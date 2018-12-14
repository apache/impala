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
import java.util.Collections;
import java.util.List;

import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CaseExpr;
import org.apache.impala.analysis.CaseWhenClause;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.MultiAggregateInfo.AggPhase;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.ValidTupleIdExpr;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TAggregationNode;
import org.apache.impala.thrift.TAggregator;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.BitUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Aggregation computation.
 *
 */
public class AggregationNode extends PlanNode {
  // Default per-instance memory requirement used if no valid stats are available.
  // TODO: Come up with a more useful heuristic.
  private final static long DEFAULT_PER_INSTANCE_MEM = 128L * 1024L * 1024L;

  // Conservative minimum size of hash table for low-cardinality aggregations.
  private final static long MIN_HASH_TBL_MEM = 10L * 1024L * 1024L;

  // Default skew factor to account for data skew among fragment instances.
  private final static double DEFAULT_SKEW_FACTOR = 1.5;

  private final MultiAggregateInfo multiAggInfo_;
  private final AggPhase aggPhase_;

  // Aggregation-class infos derived from 'multiAggInfo_' and 'aggPhase_' in c'tor.
  private final List<AggregateInfo> aggInfos_;

  // If true, this node produces intermediate aggregation tuples.
  private boolean useIntermediateTuple_ = false;

  // If true, this node performs the finalize step.
  private boolean needsFinalize_ = false;

  // If true, this node uses streaming preaggregation. Invalid if this is a merge agg.
  private boolean useStreamingPreagg_ = false;

  // Resource profiles for each aggregation class.
  private List<ResourceProfile> resourceProfiles_;

  public AggregationNode(
      PlanNodeId id, PlanNode input, MultiAggregateInfo multiAggInfo, AggPhase aggPhase) {
    super(id, "AGGREGATE");
    children_.add(input);
    multiAggInfo_ = multiAggInfo;
    aggInfos_ = multiAggInfo_.getMaterializedAggInfos(aggPhase);
    aggPhase_ = aggPhase;
    needsFinalize_ = true;
    computeTupleIds();
  }

  /**
   * Copy c'tor used in clone().
   */
  private AggregationNode(PlanNodeId id, AggregationNode src) {
    super(id, src, "AGGREGATE");
    multiAggInfo_ = src.multiAggInfo_;
    aggPhase_ = src.aggPhase_;
    aggInfos_ = src.aggInfos_;
    needsFinalize_ = src.needsFinalize_;
    useIntermediateTuple_ = src.useIntermediateTuple_;
  }

  @Override
  public void computeTupleIds() {
    clearTupleIds();
    for (AggregateInfo aggInfo : aggInfos_) {
      TupleId aggClassTupleId = null;
      if (useIntermediateTuple_) {
        aggClassTupleId = aggInfo.getIntermediateTupleId();
      } else {
        aggClassTupleId = aggInfo.getOutputTupleId();
      }
      tupleIds_.add(aggClassTupleId);
      tblRefIds_.add(aggClassTupleId);
      // Nullable tuples are only required to distinguish between multiple
      // aggregation classes.
      if (aggInfos_.size() > 1) {
        nullableTupleIds_.add(aggClassTupleId);
      }
    }
  }

  /**
   * Sets this node as a preaggregation.
   */
  public void setIsPreagg(PlannerContext ctx) {
    if (ctx.getQueryOptions().disable_streaming_preaggregations) {
      useStreamingPreagg_ = false;
      return;
    }
    for (AggregateInfo aggInfo : aggInfos_) {
      if (aggInfo.getGroupingExprs().size() > 0) {
        useStreamingPreagg_ = true;
        return;
      }
    }
  }

  /**
   * Unsets this node as requiring finalize. Only valid to call this if it is
   * currently marked as needing finalize.
   */
  public void unsetNeedsFinalize() {
    Preconditions.checkState(needsFinalize_);
    needsFinalize_ = false;
  }

  public void setIntermediateTuple() {
    useIntermediateTuple_ = true;
    computeTupleIds();
  }

  public MultiAggregateInfo getMultiAggInfo() { return multiAggInfo_; }
  public AggPhase getAggPhase() { return aggPhase_; }
  public boolean hasGrouping() { return multiAggInfo_.hasGrouping(); }
  public boolean isSingleClassAgg() { return aggInfos_.size() == 1; }

  public boolean isDistinctAgg() {
    for (AggregateInfo aggInfo : aggInfos_) {
      if (aggInfo.isDistinctAgg()) return true;
    }
    return false;
  }

  @Override
  public boolean isBlockingNode() { return !useStreamingPreagg_; }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    Preconditions.checkState(tupleIds_.size() == aggInfos_.size());
    // Assign conjuncts to the top-most agg in the single-node plan. They are transferred
    // to the proper place in the distributed plan via transferConjuncts().
    if (aggPhase_ == multiAggInfo_.getConjunctAssignmentPhase()) {
      conjuncts_.clear();
      // TODO: If this is the transposition phase, then we can push conjuncts that
      // reference a single aggregation class down into the aggregators of the
      // previous phase.
      conjuncts_.addAll(multiAggInfo_.collectConjuncts(analyzer, true));
      conjuncts_ = orderConjunctsByCost(conjuncts_);
    }

    // Compute the mem layout for both tuples here for simplicity.
    for (AggregateInfo aggInfo : aggInfos_) {
      aggInfo.getOutputTupleDesc().computeMemLayout();
      aggInfo.getIntermediateTupleDesc().computeMemLayout();
    }

    // Do at the end so it can take all conjuncts into account
    computeStats(analyzer);

    // don't call createDefaultSMap(), it would point our conjuncts (= Having clause)
    // to our input; our conjuncts don't get substituted because they already
    // refer to our output
    outputSmap_ = getCombinedChildSmap();

    // Substitute exprs and check consistency.
    // All of the AggregationNodes corresponding to a MultiAggregationInfo will have the
    // same outputSmap_, so just substitute it once.
    if (aggPhase_ == AggPhase.FIRST) multiAggInfo_.substitute(outputSmap_, analyzer);
    for (AggregateInfo aggInfo : aggInfos_) {
      aggInfo.substitute(outputSmap_, analyzer);
      aggInfo.checkConsistency();
    }
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    // This is prone to overflow, because we keep multiplying cardinalities,
    // even if the grouping exprs are functionally dependent (example:
    // group by the primary key of a table plus a number of other columns from that
    // same table)
    // TODO: try to recognize functional dependencies
    // TODO: as a shortcut, instead of recognizing functional dependencies,
    // limit the contribution of a single table to the number of rows
    // of that table (so that when we're grouping by the primary key col plus
    // some others, the estimate doesn't overshoot dramatically)
    // cardinality: product of # of distinct values produced by grouping exprs

    cardinality_ = 0;
    for (AggregateInfo aggInfo : aggInfos_) {
      List<Expr> groupingExprs = aggInfo.getGroupingExprs();
      if (groupingExprs.isEmpty()) {
        // Non-grouping aggregation class.
        cardinality_ += 1;
      } else {
        long ndvs = Expr.getNumDistinctValues(groupingExprs);
        // if we ended up with an overflow, the estimate is certain to be wrong
        if (ndvs < 0) {
          cardinality_ = -1;
          break;
        }
        cardinality_ += checkedAdd(cardinality_, ndvs);
      }
    }

    // Take conjuncts into account.
    if (cardinality_ > 0) {
      cardinality_ = (long) Math.round((double) cardinality_ * computeSelectivity());
    }
    // Sanity check the cardinality_ based on the input cardinality_.
    if (getChild(0).getCardinality() != -1) {
      if (cardinality_ == -1) {
        // A worst-case cardinality_ is better than an unknown cardinality_.
        cardinality_ = getChild(0).getCardinality();
      } else {
        // An AggregationNode cannot increase the cardinality_.
        cardinality_ = Math.min(getChild(0).getCardinality(), cardinality_);
      }
    }
    cardinality_ = capCardinalityAtLimit(cardinality_);
  }

  /**
   * Returns a list of exprs suitable for hash partitioning the output of this node
   * before the merge aggregation step. Only valid to call if this node is not a merge
   * or transposing aggregation. The returned exprs are bound by the intermediate tuples.
   * Takes the SHUFFLE_DISTINCT_EXPRS query option into account.
   *
   * For single-class aggregations the returned exprs are typically the grouping exprs.
   * With SHUFFLE_DISTINCT_EXPRS=true the distinct exprs are also included if this is the
   * non-merge first-phase aggregation of a distinct aggregation.
   *
   * For multi-class aggregations the returned exprs are a list of CASE exprs which you
   * can think of as a "union" of the merge partition exprs of each class. Each CASE
   * switches on the valid tuple id of an input row to determine the aggregation class,
   * and selects the corresponding partition expr.
   * The main challenges with crafting these exprs are:
   * 1. Different aggregation classes can have a different number of distinct exprs
   *    Solution: The returned list is maximally wide to accommodate the widest
   *    aggregation class. For classes that have fewer than the max distinct exprs we add
   *    constant dummy exprs in the corresponding branch of the CASE.
   * 2. A CASE expr must return a single output type, but different aggregation classes
   *    may have incompatible distinct exprs, so selecting the distinct exprs directly
   *    in the CASE branches would not always work (unless we cast everything to STRING,
   *    which we try to avoid). Instead, we call MURMUR_HASH() on the exprs to produce
   *    a hash value. That way, all branches of a CASE return the same type.
   *    Considering that the backend will do another round of hashing, there's an
   *    unnecessary double hashing here that we deemed acceptable for now and has
   *    potential for cleanup (maybe the FE should always add a MURMUR_HASH()).
   * The handling of SHUFFLE_DISTINCT_EXPRS is analogous to the single-class case.
   *
   * Example:
   * SELECT COUNT(DISTINCT a,b), COUNT(DISTINCT c) FROM t GROUP BY d
   * Suppose the two aggregation classes have intermediate tuple ids 0 and 1.
   *
   * Challenges explained on this example:
   * 1. First class has distinct exprs a,b and second class has c. We need to accommodate
   *    the widest class (a,b) and also hash on the grouping expr (d), so there will be
   *    three cases.
   * 2. The types of a and c might be incompatible
   *
   * The first-phase partition exprs are a list of the following 3 exprs:
   * CASE valid_tid()
   *   WHEN 0 THEN murmur_hash(d) <-- d SlotRef into tuple 0
   *   WHEN 1 THEN murmur_hash(d) <-- d SlotRef into tuple 1
   * END,
   * CASE valid_tid()
   *   WHEN 0 THEN murmur_hash(a)
   *   WHEN 1 THEN murmur_hash(c)
   * END,
   * CASE valid_tid()
   *   WHEN 0 THEN murmur_hash(b)
   *   WHEN 1 THEN 0 <-- dummy constant integer
   * END
   */
  public List<Expr> getMergePartitionExprs(Analyzer analyzer) {
    Preconditions.checkState(!tupleIds_.isEmpty());
    Preconditions.checkState(!aggPhase_.isMerge() && !aggPhase_.isTranspose());

    boolean shuffleDistinctExprs = analyzer.getQueryOptions().shuffle_distinct_exprs;
    if (aggInfos_.size() == 1) {
      AggregateInfo aggInfo = aggInfos_.get(0);
      List<Expr> groupingExprs = null;
      if (aggPhase_.isFirstPhase() && hasGrouping() && !shuffleDistinctExprs) {
        groupingExprs = multiAggInfo_.getSubstGroupingExprs();
      } else {
        groupingExprs = aggInfo.getPartitionExprs();
        if (groupingExprs == null) groupingExprs = aggInfo.getGroupingExprs();
      }
      return Expr.substituteList(
          groupingExprs, aggInfo.getIntermediateSmap(), analyzer, false);
    }

    int maxNumExprs = 0;
    for (AggregateInfo aggInfo : aggInfos_) {
      if (aggInfo.getGroupingExprs() == null) continue;
      maxNumExprs = Math.max(maxNumExprs, aggInfo.getGroupingExprs().size());
    }
    if (maxNumExprs == 0) return Collections.emptyList();

    List<Expr> result = new ArrayList<>();
    for (int i = 0; i < maxNumExprs; ++i) {
      List<CaseWhenClause> caseWhenClauses = new ArrayList<>();
      for (AggregateInfo aggInfo : aggInfos_) {
        TupleId tid;
        if (aggInfo.isDistinctAgg()) {
          tid = aggInfo.getOutputTupleId();
        } else {
          tid = aggInfo.getIntermediateTupleId();
        }
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        if (aggPhase_.isFirstPhase() && hasGrouping() && !shuffleDistinctExprs) {
          groupingExprs = multiAggInfo_.getSubstGroupingExprs();
        }
        Expr whenExpr = NumericLiteral.create(tid.asInt());
        Expr thenExpr;
        if (groupingExprs == null || i >= groupingExprs.size()) {
          thenExpr = NumericLiteral.create(0);
        } else {
          thenExpr = new FunctionCallExpr(
              "murmur_hash", Lists.newArrayList(groupingExprs.get(i).clone()));
          thenExpr.analyzeNoThrow(analyzer);
          thenExpr = thenExpr.substitute(aggInfo.getIntermediateSmap(), analyzer, true);
        }
        caseWhenClauses.add(new CaseWhenClause(whenExpr, thenExpr));
      }
      CaseExpr caseExpr =
          new CaseExpr(new ValidTupleIdExpr(tupleIds_), caseWhenClauses, null);
      caseExpr.analyzeNoThrow(analyzer);
      result.add(caseExpr);
    }
    return result;
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.agg_node = new TAggregationNode();
    msg.node_type = TPlanNodeType.AGGREGATION_NODE;
    boolean replicateInput = aggPhase_ == AggPhase.FIRST && aggInfos_.size() > 1;
    msg.agg_node.setReplicate_input(replicateInput);
    msg.agg_node.setEstimated_input_cardinality(getChild(0).getCardinality());
    for (int i = 0; i < aggInfos_.size(); ++i) {
      AggregateInfo aggInfo = aggInfos_.get(i);
      List<TExpr> aggregateFunctions = new ArrayList<>();
      for (FunctionCallExpr e : aggInfo.getMaterializedAggregateExprs()) {
        aggregateFunctions.add(e.treeToThrift());
      }
      TAggregator taggregator = new TAggregator(aggregateFunctions,
          aggInfo.getIntermediateTupleId().asInt(), aggInfo.getOutputTupleId().asInt(),
          needsFinalize_, useStreamingPreagg_, resourceProfiles_.get(i).toThrift());
      List<Expr> groupingExprs = aggInfo.getGroupingExprs();
      if (!groupingExprs.isEmpty()) {
        taggregator.setGrouping_exprs(Expr.treesToThrift(groupingExprs));
      }
      msg.agg_node.addToAggregators(taggregator);
    }
  }

  @Override
  protected String getDisplayLabelDetail() {
    if (useStreamingPreagg_) return "STREAMING";
    if (needsFinalize_) return "FINALIZE";
    return null;
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    String nameDetail = getDisplayLabelDetail();
    output.append(String.format("%s%s", prefix, getDisplayLabel()));
    if (nameDetail != null) output.append(" [" + nameDetail + "]");
    output.append("\n");

    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (aggInfos_.size() == 1) {
        output.append(
            getAggInfoExplainString(detailPrefix, aggInfos_.get(0), detailLevel));
      } else {
        for (int i = 0; i < aggInfos_.size(); ++i) {
          AggregateInfo aggInfo = aggInfos_.get(i);
          output.append(String.format("%sClass %d\n", detailPrefix, i));
          output.append(
              getAggInfoExplainString(detailPrefix + "  ", aggInfo, detailLevel));
        }
      }
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix)
            .append("having: ")
            .append(getExplainString(conjuncts_, detailLevel))
            .append("\n");
      }
    }
    return output.toString();
  }

  private StringBuilder getAggInfoExplainString(
      String prefix, AggregateInfo aggInfo, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    List<FunctionCallExpr> aggExprs = aggInfo.getMaterializedAggregateExprs();
    List<Expr> groupingExprs = aggInfo.getGroupingExprs();
    if (!aggExprs.isEmpty()) {
      output.append(prefix)
          .append("output: ")
          .append(getExplainString(aggExprs, detailLevel))
          .append("\n");
    }
    if (!groupingExprs.isEmpty()) {
      output.append(prefix)
          .append("group by: ")
          .append(getExplainString(groupingExprs, detailLevel))
          .append("\n");
    }
    return output;
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    resourceProfiles_ = Lists.newArrayListWithCapacity(aggInfos_.size());
    resourceProfiles_.clear();
    for (AggregateInfo aggInfo : aggInfos_) {
      resourceProfiles_.add(computeAggClassResourceProfile(queryOptions, aggInfo));
    }
    if (aggInfos_.size() == 1) {
      nodeResourceProfile_ = resourceProfiles_.get(0);
    } else {
      nodeResourceProfile_ = ResourceProfile.noReservation(0);
      for (ResourceProfile aggProfile : resourceProfiles_) {
        nodeResourceProfile_ = nodeResourceProfile_.sum(aggProfile);
      }
    }
  }

  private ResourceProfile computeAggClassResourceProfile(
      TQueryOptions queryOptions, AggregateInfo aggInfo) {
    Preconditions.checkNotNull(
        fragment_, "PlanNode must be placed into a fragment before calling this method.");
    long perInstanceCardinality =
        fragment_.getPerInstanceNdv(queryOptions.getMt_dop(), aggInfo.getGroupingExprs());
    long perInstanceMemEstimate;
    long perInstanceDataBytes = -1;
    if (perInstanceCardinality == -1) {
      perInstanceMemEstimate = DEFAULT_PER_INSTANCE_MEM;
    } else {
      // Per-instance cardinality cannot be greater than the total input cardinality.
      long inputCardinality = getChild(0).getCardinality();
      if (inputCardinality != -1) {
        // Calculate the input cardinality distributed across fragment instances.
        long numInstances = fragment_.getNumInstances(queryOptions.getMt_dop());
        long perInstanceInputCardinality;
        if (numInstances > 1) {
          perInstanceInputCardinality =
              (long) Math.ceil((inputCardinality / numInstances) * DEFAULT_SKEW_FACTOR);
        } else {
          // When numInstances is 1 or unknown(-1), perInstanceInputCardinality is the
          // same as inputCardinality.
          perInstanceInputCardinality = inputCardinality;
        }
        perInstanceCardinality =
            Math.min(perInstanceCardinality, perInstanceInputCardinality);
      }
      perInstanceDataBytes = (long)Math.ceil(perInstanceCardinality * avgRowSize_);
      perInstanceMemEstimate = (long)Math.max(perInstanceDataBytes *
          PlannerContext.HASH_TBL_SPACE_OVERHEAD, MIN_HASH_TBL_MEM);
    }

    // Must be kept in sync with GroupingAggregator::MinReservation() in backend.
    long perInstanceMinMemReservation;
    long bufferSize = queryOptions.getDefault_spillable_buffer_size();
    long maxRowBufferSize =
        computeMaxSpillableBufferSize(bufferSize, queryOptions.getMax_row_size());
    if (aggInfo.getGroupingExprs().isEmpty()) {
      perInstanceMinMemReservation = 0;
    } else {
      // This is a grouping pre-aggregation or merge aggregation.
      final int PARTITION_FANOUT = 16;
      if (perInstanceDataBytes != -1) {
        long bytesPerPartition = perInstanceDataBytes / PARTITION_FANOUT;
        // Scale down the buffer size if we think there will be excess free space with the
        // default buffer size, e.g. with small dimension tables.
        bufferSize = Math.min(bufferSize, Math.max(
            queryOptions.getMin_spillable_buffer_size(),
            BitUtil.roundUpToPowerOf2(bytesPerPartition)));
        // Recompute the max row buffer size with the smaller buffer.
        maxRowBufferSize =
            computeMaxSpillableBufferSize(bufferSize, queryOptions.getMax_row_size());
      }
      if (useStreamingPreagg_) {
        // We can execute a streaming preagg without any buffers by passing through rows,
        // but that is a very low performance mode of execution if the aggregation reduces
        // its input significantly. Instead reserve memory for one buffer per partition
        // and at least 64kb for hash tables per partition. We must reserve at least one
        // full buffer for hash tables for the suballocator to subdivide. We don't need to
        // reserve memory for large rows since they can be passed through if needed.
        perInstanceMinMemReservation = bufferSize * PARTITION_FANOUT +
            Math.max(64 * 1024 * PARTITION_FANOUT, bufferSize);
      } else {
        long minBuffers = PARTITION_FANOUT + 1 + (aggInfo.needsSerialize() ? 1 : 0);
        // Two of the buffers need to be buffers large enough to hold the maximum-sized
        // row to serve as input and output buffers while repartitioning.
        perInstanceMinMemReservation = bufferSize * (minBuffers - 2) + maxRowBufferSize * 2;
      }
    }

    return new ResourceProfileBuilder()
        .setMemEstimateBytes(perInstanceMemEstimate)
        .setMinMemReservationBytes(perInstanceMinMemReservation)
        .setSpillableBufferBytes(bufferSize)
        .setMaxRowBufferBytes(maxRowBufferSize)
        .build();
  }
}
