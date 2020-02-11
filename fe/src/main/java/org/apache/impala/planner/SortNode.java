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
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSortInfo;
import org.apache.impala.thrift.TSortNode;
import org.apache.impala.thrift.TSortType;
import org.apache.impala.thrift.TSortingOrder;
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

  // Memory limit for partial sorts, specified in bytes. TODO: determine the value for
  // this, consider making it configurable, enforce it in the BE. (IMPALA-5669)
  private final long PARTIAL_SORT_MEM_LIMIT = 128 * 1024 * 1024;

  private final SortInfo info_;

  // if set, this SortNode requires its input to have this data partition
  private DataPartition inputPartition_;

  // if true, the output of this node feeds an AnalyticNode
  private boolean isAnalyticSort_;

  // info_.sortTupleSlotExprs_ substituted with the outputSmap_ for materialized slots
  // in init().
  private List<Expr> resolvedTupleExprs_;

  // The offset of the first row to return.
  protected long offset_;

  // The type of sort. Determines the exec node used in the BE.
  private final TSortType type_;

  /**
   * Creates a new SortNode that implements a partial sort.
   */
  public static SortNode createPartialSortNode(
      PlanNodeId id, PlanNode input, SortInfo info) {
    return new SortNode(id, input, info, 0, TSortType.PARTIAL);
  }

  /**
   * Creates a new SortNode with a limit that is executed with TopNNode in the BE.
   */
  public static SortNode createTopNSortNode(
      PlanNodeId id, PlanNode input, SortInfo info, long offset) {
    return new SortNode(id, input, info, offset, TSortType.TOPN);
  }

  /**
   * Creates a new SortNode that does a total sort, possibly with a limit.
   */
  public static SortNode createTotalSortNode(
      PlanNodeId id, PlanNode input, SortInfo info, long offset) {
    return new SortNode(id, input, info, offset, TSortType.TOTAL);
  }

  private SortNode(
      PlanNodeId id, PlanNode input, SortInfo info, long offset, TSortType type) {
    super(id, info.getSortTupleDescriptor().getId().asList(), getDisplayName(type));
    info_ = info;
    children_.add(input);
    offset_ = offset;
    type_ = type;
  }

  public long getOffset() { return offset_; }
  public void setOffset(long offset) { offset_ = offset; }
  public boolean hasOffset() { return offset_ > 0; }
  public boolean useTopN() { return type_ == TSortType.TOPN; }
  public SortInfo getSortInfo() { return info_; }
  public void setInputPartition(DataPartition inputPartition) {
    inputPartition_ = inputPartition;
  }
  public DataPartition getInputPartition() { return inputPartition_; }
  public boolean isAnalyticSort() { return isAnalyticSort_; }
  public void setIsAnalyticSort(boolean v) { isAnalyticSort_ = v; }

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
  protected void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = capCardinalityAtLimit(getChild(0).cardinality_);
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
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SORT_NODE;
    TSortInfo sort_info = new TSortInfo(Expr.treesToThrift(info_.getSortExprs()),
        info_.getIsAscOrder(), info_.getNullsFirst(), info_.getSortingOrder());
    Preconditions.checkState(tupleIds_.size() == 1,
        "Incorrect size for tupleIds_ in SortNode");
    sort_info.setSort_tuple_slot_exprs(Expr.treesToThrift(resolvedTupleExprs_));
    TSortNode sort_node = new TSortNode(sort_info, type_);
    sort_node.setOffset(offset_);
    msg.sort_node = sort_node;
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s:%s%s\n", prefix, id_.toString(),
        displayName_, getNodeExplainDetail(detailLevel)));
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      output.append(detailPrefix + "order by: ");
      output.append(getSortingOrderExplainString(info_.getSortExprs(),
          info_.getIsAscOrder(), info_.getNullsFirstParams(), info_.getSortingOrder()));
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
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    Preconditions.checkState(hasValidStats());
    if (type_ == TSortType.TOPN) {
      nodeResourceProfile_ = ResourceProfile.noReservation(
          getSortInfo().estimateTopNMaterializedSize(cardinality_, offset_));
      return;
    }

    // For an external sort, set the memory cost to be what is required for a 2-phase
    // sort. If the input to be sorted would take up N blocks in memory, then the
    // memory required for a 2-phase sort is sqrt(N) blocks. A single run would be of
    // size sqrt(N) blocks, and we could merge sqrt(N) such runs with sqrt(N) blocks
    // of memory.
    double fullInputSize = getChild(0).cardinality_ * avgRowSize_;
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
      double numInputBlocks = Math.ceil(fullInputSize / (bufferSize * pageMultiplier));
      perInstanceMemEstimate =
          bufferSize * (long) Math.ceil(Math.sqrt(numInputBlocks));
      perInstanceMinMemReservation = 3 * bufferSize * pageMultiplier;
    }
    nodeResourceProfile_ = new ResourceProfileBuilder()
        .setMemEstimateBytes(perInstanceMemEstimate)
        .setMinMemReservationBytes(perInstanceMinMemReservation)
        .setSpillableBufferBytes(bufferSize).setMaxRowBufferBytes(bufferSize).build();
  }

  private static String getDisplayName(TSortType type) {
    if (type == TSortType.TOPN) {
      return "TOP-N";
    } else if (type == TSortType.PARTIAL) {
      return "PARTIAL SORT";
    } else {
      Preconditions.checkState(type == TSortType.TOTAL);
      return "SORT";
    }
  }
}
