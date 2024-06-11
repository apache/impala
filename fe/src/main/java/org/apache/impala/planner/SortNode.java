// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.analysis.ToSqlOptions;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSortInfo;
import org.apache.impala.thrift.TSortNode;
import org.apache.impala.thrift.TSortType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Node the implements various types of sorts:
 * - TOTAL: uses SortNode in the BE.
 * - TOPN: uses TopNNode in the BE. Must have a limit.
 * - PARTIAL: use PartialSortNode in the BE. Cannot have a limit or offset.
 *
 * Will always materialize the new tuple info_.sortTupleDesc_.
 */
public class SortNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(SortNode.class);

  // Coefficiencts for estimating scan processing cost. Derived from benchmarking.
  private static final double COST_COEFFICIENT_SORT_TOTAL_SMALL_ROW = 0.0301;
  private static final double COST_COEFFICIENT_SORT_TOTAL_ROWS = 0.4071;
  private static final double COST_COEFFICIENT_SORT_TOTAL_BYTES = 0.0435;
  private static final double COST_COEFFICIENT_SORT_TOPN = 0.5638;

  // Memory limit for partial sorts, specified in bytes. TODO: determine the value for
  // this, consider making it configurable, enforce it in the BE. (IMPALA-5669)
  private final long PARTIAL_SORT_MEM_LIMIT = 128 * 1024 * 1024;

  private final SortInfo info_;

  // if set, this SortNode requires its input to have this data partition
  private DataPartition inputPartition_;

  // if true, the output of this node feeds an AnalyticNode
  private boolean isAnalyticSort_;

  // if this is an analytic sort, this points to the corresponding
  // analytic eval node otherwise null
  private AnalyticEvalNode analyticEvalNode_;

  // info_.sortTupleSlotExprs_ substituted with the outputSmap_ for materialized slots
  // in init().
  private List<Expr> resolvedTupleExprs_;

  // The offset of the first row to return.
  protected long offset_;

  // How many of the expressions in info_ comprise the partition key.
  // Non-negative if type_ is PARTITIONED_TOPN, -1 otherwise.
  protected int numPartitionExprs_;

  // Max rows to return for each partition key value.
  // Non-negative if type_ is PARTITIONED_TOPN, -1 otherwise.
  protected long perPartitionLimit_;

  // Whether to include ties for the last place in the Top-N values.
  // Only supported if type_ is TOPN or PARTITIONED_TOPN.
  protected boolean includeTies_;

  // If includeTies_ is true, this is the limit used. We cannot use the default 'limit_'
  // because the plan node may return more than this number of rows.
  protected long limitWithTies_;

  // The predicate that the limit was derived from, if any. If non-null,
  // used for informational purposes in the explain string.
  protected Expr limitSrcPred_;

  // The type of sort. Determines the exec node used in the BE.
  private TSortType type_;

  // Estimated bytes of input that will go into this sort node across all backends.
  // Used for sorter spill estimation in backend code.
  private long estimatedFullInputSize_ = -1;

  /**
   * Creates a new SortNode that implements a partial sort.
   */
  public static SortNode createPartialSortNode(
      PlanNodeId id, PlanNode input, SortInfo info) {
    return new SortNode(id, input, info, 0, -1, -1, -1, false, TSortType.PARTIAL);
  }

  /**
   * Creates a new SortNode with a limit that is executed with either TopNNode in the BE
   * or SortNode in the backend dependent on TOPN_BYTES_LIMIT.
   */
  public static SortNode createTopNSortNode(TQueryOptions queryOptions,
      PlanNodeId id, PlanNode input, SortInfo info, long offset, long limit,
      boolean includeTies) {
    long topNBytesLimit = queryOptions.topn_bytes_limit;
    long topNCardinality = capCardinalityAtLimit(input.cardinality_, limit);
    long estimatedTopNMaterializedSize =
        info.estimateTopNMaterializedSize(topNCardinality, offset);

    SortNode result;
    if (topNBytesLimit <= 0 || estimatedTopNMaterializedSize < topNBytesLimit
            || includeTies) {
      result = new SortNode(
              id, input, info, offset, limit, -1, -1, includeTies, TSortType.TOPN);
    } else {
      result = SortNode.createTotalSortNode(id, input, info, offset);
      result.setLimit(limit);
    }
    return result;
  }

  /**
   * Creates a new SortNode with a per-partition limit that is executed with TopNNode
   * in the BE.
   */
  public static SortNode createPartitionedTopNSortNode(
      PlanNodeId id, PlanNode input, SortInfo info, int numPartitionExprs,
      long perPartitionLimit, boolean includeTies) {
    return new SortNode(id, input, info, 0, -1, numPartitionExprs, perPartitionLimit,
        includeTies, TSortType.PARTITIONED_TOPN);
  }

  /**
   * Creates a new SortNode that does a total sort, possibly with a limit.
   */
  public static SortNode createTotalSortNode(
      PlanNodeId id, PlanNode input, SortInfo info, long offset) {
    return new SortNode(id, input, info, offset, -1, -1, -1, false, TSortType.TOTAL);
  }

  protected SortNode(
      PlanNodeId id, PlanNode input, SortInfo info, long offset, long limit,
      int numPartitionExprs, long perPartitionLimit, boolean includeTies,
      TSortType type) {
    super(id, info.getSortTupleDescriptor().getId().asList(), getDisplayName(type));
    Preconditions.checkState(offset >= 0);
    if (type == TSortType.PARTITIONED_TOPN) {
      // We need to support 0 partition exprs if ties are included, because the
      // non-partitioned Top-N and sort nodes do not currently support including ties.
      Preconditions.checkState(includeTies || numPartitionExprs > 0);
      Preconditions.checkState(perPartitionLimit > 0);
    } else if (type == TSortType.TOPN) {
      Preconditions.checkArgument(type != TSortType.TOPN || limit >= 0);
    }
    info_ = info;
    children_.add(input);
    offset_ = offset;
    numPartitionExprs_ = numPartitionExprs;
    perPartitionLimit_ = perPartitionLimit;
    includeTies_ = includeTies;
    limitWithTies_ = (type == TSortType.TOPN && includeTies) ? limit : -1;
    type_ = type;
    if (!includeTies) setLimit(limit);
  }

  public long getOffset() { return offset_; }
  public void setOffset(long offset) { offset_ = offset; }
  public boolean hasOffset() { return offset_ > 0; }
  public boolean isTotalSort() { return type_ == TSortType.TOTAL; }
  public boolean isTypeTopN() { return type_ == TSortType.TOPN; }
  public boolean isPartitionedTopN() { return type_ == TSortType.PARTITIONED_TOPN; }
  public int getNumPartitionExprs() { return numPartitionExprs_; }
  public long getPerPartitionLimit() { return perPartitionLimit_; }
  public boolean isIncludeTies() { return includeTies_; }
  // Get the limit that applies to the sort, including ties or not.
  public long getSortLimit() {
    return includeTies_ ? limitWithTies_ : limit_;
  }

  public SortInfo getSortInfo() { return info_; }
  public void setInputPartition(DataPartition inputPartition) {
    inputPartition_ = inputPartition;
  }
  public DataPartition getInputPartition() { return inputPartition_; }
  public boolean isAnalyticSort() { return isAnalyticSort_; }
  public void setIsAnalyticSort(boolean v) { isAnalyticSort_ = v; }
  public void setAnalyticEvalNode(AnalyticEvalNode n) { analyticEvalNode_ = n; }
  public AnalyticEvalNode getAnalyticEvalNode() { return analyticEvalNode_; }

  /**
   * Under special cases, the planner may decide to convert a total sort or
   * partition top-N into a TopN sort with limit. This does the conversion to top-n
   * if the converted node would pass the TOPN_BYTES_LIMIT check. Otherwise does
   * not modify this node.
   */
  public void tryConvertToTopN(long limit, Analyzer analyzer, boolean includeTies) {
    Preconditions.checkArgument(type_ == TSortType.TOTAL
        || type_ == TSortType.PARTITIONED_TOPN);
    Preconditions.checkState(!hasLimit());
    Preconditions.checkState(!hasOffset());
    long topNBytesLimit = analyzer.getQueryOptions().topn_bytes_limit;
    long topNCardinality = capCardinalityAtLimit(getChild(0).cardinality_, limit);
    long estimatedTopNMaterializedSize =
        info_.estimateTopNMaterializedSize(topNCardinality, offset_);

    if (topNBytesLimit > 0 && estimatedTopNMaterializedSize >= topNBytesLimit) {
      return;
    }
    type_ = TSortType.TOPN;
    displayName_ = getDisplayName(type_);
    numPartitionExprs_ = -1;
    perPartitionLimit_ = -1;
    includeTies_ = includeTies;
    if (includeTies) {
      unsetLimit();
      limitWithTies_ = limit;
    } else {
      setLimit(limit);
    }
    computeStats(analyzer);
  }

  @Override
  public boolean allowPartitioned() {
    if (isAnalyticSort_ && hasLimit()) return true;
    return super.allowPartitioned();
  }

  public void setLimitSrcPred(Expr v) {
    this.limitSrcPred_ = v;
  }

  @Override
  public boolean isBlockingNode() { return type_ != TSortType.PARTIAL; }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    // Do not assignConjuncts() here, so that conjuncts bound by this SortNode's tuple id
    // can be placed in a downstream SelectNode. A SortNode cannot evaluate conjuncts.
    Preconditions.checkState(conjuncts_.isEmpty());
    // Compute the memory layout for the generated tuple.
    computeMemLayout(analyzer);
    computeStats(analyzer);

    // populate resolvedTupleExprs_ and outputSmap_
    List<SlotDescriptor> sortTupleSlots = info_.getSortTupleDescriptor().getSlots();
    Preconditions.checkState(sortTupleSlots.size() > 0,
        "empty sort tuple descriptor");
    List<Expr> slotExprs = info_.getMaterializedExprs();
    resolvedTupleExprs_ = new ArrayList<>();
    outputSmap_ = new ExprSubstitutionMap();
    for (int i = 0; i < slotExprs.size(); ++i) {
      if (!sortTupleSlots.get(i).isMaterialized()) continue;
      resolvedTupleExprs_.add(slotExprs.get(i));
      outputSmap_.put(slotExprs.get(i), new SlotRef(sortTupleSlots.get(i)));
    }
    ExprSubstitutionMap childSmap = getCombinedChildSmap();
    // Preserve type as resolvedTupleExprs_ will be used to materialize the tuple and the
    // layout is already calculated.
    resolvedTupleExprs_ =
        Expr.substituteList(resolvedTupleExprs_, childSmap, analyzer, true);

    // Remap the ordering exprs to the tuple materialized by this sort node. The mapping
    // is a composition of the childSmap and the outputSmap_ because the child node may
    // have also remapped its input (e.g., as in a series of (sort->analytic)* nodes).
    // Parent nodes have to do the same so set the composition as the outputSmap_.
    outputSmap_ = ExprSubstitutionMap.compose(childSmap, outputSmap_, analyzer);

    info_.substituteSortExprs(outputSmap_, analyzer);
    info_.checkConsistency();

    if (LOG.isTraceEnabled()) {
      LOG.trace("sort id " + tupleIds_.get(0).toString() + " smap: "
          + outputSmap_.debugString());
      LOG.trace("sort input exprs: " + Expr.debugString(resolvedTupleExprs_));
    }
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    if (isTypeTopN() && includeTies_) {
      cardinality_ = capCardinalityAtLimit(getChild(0).cardinality_, limitWithTies_);
    } else {
      cardinality_ = capCardinalityAtLimit(getChild(0).cardinality_);
    }
    if (type_ == TSortType.PARTITIONED_TOPN) {
      // We may be able to get a more precise estimate based on the number of
      // partitions and per-partition limits.
      List<Expr> partExprs = info_.getSortExprs().subList(0, numPartitionExprs_);
      long partNdv = numPartitionExprs_ == 0 ? 1 : Expr.getNumDistinctValues(partExprs);
      if (partNdv >= 0) {
        long maxRowsInHeaps = partNdv * getPerPartitionLimit();
        if (cardinality_ < 0 || cardinality_ > maxRowsInHeaps) {
          cardinality_ = maxRowsInHeaps;
        }
      }
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("stats Sort: cardinality=" + Long.toString(cardinality_));
    }
  }

  @Override
  protected String debugString() {
    List<String> strings = new ArrayList<>();
    for (Boolean isAsc : info_.getIsAscOrder()) {
      strings.add(isAsc ? "a" : "d");
    }
    return MoreObjects.toStringHelper(this)
        .add("type_", type_)
        .add("ordering_exprs", Expr.debugString(info_.getSortExprs()))
        .add("is_asc", "[" + Joiner.on(" ").join(strings) + "]")
        .add("nulls_first", "[" + Joiner.on(" ").join(info_.getNullsFirst()) + "]")
        .add("offset_", offset_)
        .add("includeTies_", includeTies_)
        .add("limitWithTies_", limitWithTies_)
        .add("numPartitionExprs_", numPartitionExprs_)
        .add("perPartitionLimit_", perPartitionLimit_)
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    if (isTypeTopN()) {
      Preconditions.checkState(hasLimit() ||
              (includeTies_ && limitWithTies_ >= 0), "Top-N must have limit",
              debugString());
      Preconditions.checkState(!includeTies_ || (!hasLimit() && limitWithTies_ >= 0),
              "Top-N with tie handling must set limitWithTies_ only");
    }
    Preconditions.checkState(offset_ >= 0);
    msg.node_type = TPlanNodeType.SORT_NODE;
    TSortInfo sort_info = new TSortInfo(Expr.treesToThrift(info_.getSortExprs()),
        info_.getIsAscOrder(), info_.getNullsFirst(), info_.getSortingOrder());
    sort_info.setNum_lexical_keys_in_zorder(info_.getNumLexicalKeysInZOrder());
    Preconditions.checkState(tupleIds_.size() == 1,
        "Incorrect size for tupleIds_ in SortNode");
    sort_info.setSort_tuple_slot_exprs(Expr.treesToThrift(resolvedTupleExprs_));
    TSortNode sort_node = new TSortNode(sort_info, type_);
    sort_node.setOffset(offset_);
    sort_node.setEstimated_full_input_size(estimatedFullInputSize_);

    if (type_ == TSortType.PARTITIONED_TOPN) {
      sort_node.setPer_partition_limit(perPartitionLimit_);
      List<Expr> partExprs = info_.getSortExprs().subList(0, numPartitionExprs_);
      sort_node.setPartition_exprs(Expr.treesToThrift(partExprs));
      // Remove the partition exprs for the intra-partition sort.
      int totalExprs = info_.getSortExprs().size();
      List<Expr> sortExprs = info_.getSortExprs().subList(numPartitionExprs_, totalExprs);
      sort_node.setIntra_partition_sort_info(new TSortInfo(Expr.treesToThrift(sortExprs),
          info_.getIsAscOrder().subList(numPartitionExprs_, totalExprs),
          info_.getNullsFirst().subList(numPartitionExprs_, totalExprs),
          info_.getSortingOrder()));
    }
    Preconditions.checkState(type_ == TSortType.PARTITIONED_TOPN ||
            type_ == TSortType.TOPN || !includeTies_);
    sort_node.setInclude_ties(includeTies_);
    if (includeTies_) {
      sort_node.setLimit_with_ties(limitWithTies_);
    }
    msg.sort_node = sort_node;
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s:%s%s\n", prefix, id_.toString(),
        displayName_, getNodeExplainDetail(detailLevel)));
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (type_ == TSortType.PARTITIONED_TOPN) {
        output.append(detailPrefix + "partition by:");
        List<Expr> partExprs = info_.getSortExprs().subList(0, numPartitionExprs_);
        if (partExprs.size() > 0) {
          output.append(" ");
          output.append(Expr.toSql(partExprs, ToSqlOptions.DEFAULT));
        }
        int totalExprs = info_.getSortExprs().size();
        List<Expr> sortExprs =
              info_.getSortExprs().subList(numPartitionExprs_, totalExprs);
        output.append("\n" + detailPrefix + "order by: ");
        output.append(getSortingOrderExplainString(sortExprs,
              info_.getIsAscOrder().subList(numPartitionExprs_, totalExprs),
              info_.getNullsFirstParams().subList(numPartitionExprs_, totalExprs),
              info_.getSortingOrder(), info_.getNumLexicalKeysInZOrder()));
        output.append(detailPrefix + "partition limit: " + perPartitionLimit_);
        if (includeTies_) output.append(" (include ties)");
        output.append("\n");
      } else {
        output.append(detailPrefix + "order by: ");
        output.append(getSortingOrderExplainString(info_.getSortExprs(),
            info_.getIsAscOrder(), info_.getNullsFirstParams(), info_.getSortingOrder(),
            info_.getNumLexicalKeysInZOrder()));
        if (includeTies_) {
          output.append(detailPrefix + "limit with ties: " + limitWithTies_ + "\n");
        }
      }
      if (limitSrcPred_ != null) {
        output.append(detailPrefix + "source expr: " +
                limitSrcPred_.toSql(ToSqlOptions.SHOW_IMPLICIT_CASTS) + "\n");
      }
    }

    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      List<Expr> nonSlotRefExprs = new ArrayList<>();
      for (Expr e: info_.getMaterializedExprs()) {
        if (e instanceof SlotRef) continue;
        nonSlotRefExprs.add(e);
      }
      if (!nonSlotRefExprs.isEmpty()) {
        output.append(detailPrefix + "materialized: ");
        for (int i = 0; i < nonSlotRefExprs.size(); ++i) {
          if (i > 0) output.append(", ");
          output.append(nonSlotRefExprs.get(i).toSql());
        }
        output.append("\n");
      }
    }

    return output.toString();
  }

  private String getNodeExplainDetail(TExplainLevel detailLevel) {
    if (!hasLimit()) return "";
    if (hasOffset()) {
      return String.format(" [LIMIT=%s OFFSET=%s]", limit_, offset_);
    } else {
      return String.format(" [LIMIT=%s]", limit_);
    }
  }

  @Override
  protected String getOffsetExplainString(String prefix) {
    return offset_ != 0 ? prefix + "offset: " + Long.toString(offset_) + "\n" : "";
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    // For TOTAL sorts benchmarked costs fall into two categories. For smaller row widths
    // (<= 10 bytes) the sort time is proportional to NlogN of the row count.  For larger
    // row widths the time is best predicted as a linear function of the row count and
    // byte count (likely because the time is dominated by materialization costs and not
    // the actual key sorting cost). Benchmark coefficients were derived from cases with
    // moderately low NDV and with just one or two sorting runs and minimal or no
    // spilling.  These are close to best case costs intentionally to avoid overestimating
    // costs and required resources. We currently use the same costing for partial and
    // total sorts.
    // TODO: Benchmark partial sort cost separately.
    // TODO: Improve this for larger spilling sorts.
    double totalCost = 0.0F;
    long inputCardinality = Math.max(0, getChild(0).getFilteredCardinality());
    double log2InputCardinality =
        inputCardinality <= 0 ? 0.0 : (Math.log(inputCardinality) / Math.log(2));
    if (type_ == TSortType.TOTAL || type_ == TSortType.PARTIAL) {
      if (avgRowSize_ <= 10) {
        totalCost = inputCardinality * log2InputCardinality
            * COST_COEFFICIENT_SORT_TOTAL_SMALL_ROW;
      } else {
        double fullInputSize = inputCardinality * avgRowSize_;
        totalCost = (inputCardinality * COST_COEFFICIENT_SORT_TOTAL_ROWS)
            + (fullInputSize * COST_COEFFICIENT_SORT_TOTAL_BYTES);
      }
    } else {
      Preconditions.checkState(
          type_ == TSortType.TOPN || type_ == TSortType.PARTITIONED_TOPN);
      // Benchmarked TopN sort costs were ~ NlogN rows.
      totalCost = inputCardinality * log2InputCardinality * COST_COEFFICIENT_SORT_TOPN;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Sort CPU cost estimate: " + totalCost + ", Type: " + type_
          + ", Input Card: " + inputCardinality + ", Row Size: " + avgRowSize_);
    }
    processingCost_ = ProcessingCost.basicCost(getDisplayLabel(), totalCost);
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    Preconditions.checkState(hasValidStats());
    if (type_ == TSortType.TOPN) {
      nodeResourceProfile_ = ResourceProfile.noReservation(
          getSortInfo().estimateTopNMaterializedSize(cardinality_, offset_));
      return;
    }

    double fullInputSize = getChild(0).cardinality_ * avgRowSize_;
    estimatedFullInputSize_ = fullInputSize < 0 ? -1 : (long) Math.ceil(fullInputSize);
    boolean usesVarLenBlocks = false;
    for (SlotDescriptor slotDesc: info_.getSortTupleDescriptor().getSlots()) {
      if (slotDesc.isMaterialized() && !slotDesc.getType().isFixedLengthType()) {
        usesVarLenBlocks = true;
        break;
      }
    }

    // Sort uses a single buffer size - either the default spillable buffer size or the
    // smallest buffer size required to fit the maximum row size.
    long bufferSize = computeMaxSpillableBufferSize(
        queryOptions.getDefault_spillable_buffer_size(), queryOptions.getMax_row_size());

    // The external sorter writes fixed-len and var-len data in separate sequences of
    // pages on disk and reads from both sequences when merging. This effectively
    // doubles the number of pages required when there are var-len columns present.
    // Must be kept in sync with ComputeMinReservation() in Sorter in be.
    int pageMultiplier = usesVarLenBlocks ? 2 : 1;
    long perInstanceMemEstimate;
    long perInstanceMinMemReservation;
    if (type_ == TSortType.PARTIAL) {
      // The memory limit cannot be less than the size of the required blocks.
      long mem_limit = Math.max(PARTIAL_SORT_MEM_LIMIT, bufferSize * pageMultiplier);
      // 'fullInputSize' will be negative if stats are missing, just use the limit.
      perInstanceMemEstimate = fullInputSize < 0 ?
          mem_limit :
          Math.min((long) Math.ceil(fullInputSize), mem_limit);
      perInstanceMinMemReservation = bufferSize * pageMultiplier;
    } else {
      Preconditions.checkState(type_ == TSortType.TOTAL ||
          type_ == TSortType.PARTITIONED_TOPN);
      long numInstances = fragment_.getNumInstances();
      // Data takes up most of the memory
      perInstanceMemEstimate = fullInputSize < 0 ?
          PARTIAL_SORT_MEM_LIMIT : (long) Math.ceil(fullInputSize / numInstances);
      perInstanceMinMemReservation = 3 * bufferSize * pageMultiplier;

      if (type_ == TSortType.PARTITIONED_TOPN && cardinality_ >= 0) {
        // We may be able to estimate a lower memory requirement based on the size
        // of in-memory heaps.
        long totalHeapBytes = getSortInfo().estimateMaterializedSize(cardinality_);
        perInstanceMemEstimate = Math.min(perInstanceMemEstimate, totalHeapBytes);
      }
    }
    nodeResourceProfile_ = new ResourceProfileBuilder()
        .setMemEstimateBytes(perInstanceMemEstimate)
        .setMinMemReservationBytes(perInstanceMinMemReservation)
        .setSpillableBufferBytes(bufferSize).setMaxRowBufferBytes(bufferSize).build();
  }

  private static String getDisplayName(TSortType type) {
    if (type == TSortType.TOPN || type == TSortType.PARTITIONED_TOPN) {
      // The two top-n variants can be distinguished by presence of partitioning exprs.
      return "TOP-N";
    } else if (type == TSortType.PARTIAL) {
      return "PARTIAL SORT";
    } else {
      Preconditions.checkState(type == TSortType.TOTAL);
      return "SORT";
    }
  }
}
