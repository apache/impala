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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TAggregationNode;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.BitUtil;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Aggregation computation.
 *
 */
public class AggregationNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(AggregationNode.class);

  // Default per-instance memory requirement used if no valid stats are available.
  // TODO: Come up with a more useful heuristic.
  private final static long DEFAULT_PER_INSTANCE_MEM = 128L * 1024L * 1024L;

  // Conservative minimum size of hash table for low-cardinality aggregations.
  private final static long MIN_HASH_TBL_MEM = 10L * 1024L * 1024L;

  private final AggregateInfo aggInfo_;

  // Set to true if this aggregation node needs to run the Finalize step. This
  // node is the root node of a distributed aggregation.
  private boolean needsFinalize_;

  // If true, use streaming preaggregation algorithm. Not valid if this is a merge agg.
  private boolean useStreamingPreagg_;

  /**
   * Create an agg node from aggInfo.
   */
  public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
    super(id, aggInfo.getOutputTupleId().asList(), "AGGREGATE");
    aggInfo_ = aggInfo;
    children_.add(input);
    needsFinalize_ = true;
  }

  /**
   * Copy c'tor used in clone().
   */
  private AggregationNode(PlanNodeId id, AggregationNode src) {
    super(id, src, "AGGREGATE");
    aggInfo_ = src.aggInfo_;
    needsFinalize_ = src.needsFinalize_;
  }

  public AggregateInfo getAggInfo() { return aggInfo_; }

  /**
   * Unsets this node as requiring finalize. Only valid to call this if it is
   * currently marked as needing finalize.
   */
  public void unsetNeedsFinalize() {
    Preconditions.checkState(needsFinalize_);
    needsFinalize_ = false;
  }

  /**
   * Sets this node as a preaggregation. Only valid to call this if it is not marked
   * as a preaggregation
   */
  public void setIsPreagg(PlannerContext ctx_) {
    TQueryOptions query_options = ctx_.getQueryOptions();
    useStreamingPreagg_ =  !query_options.disable_streaming_preaggregations &&
        aggInfo_.getGroupingExprs().size() > 0;
  }

  /**
   * Have this node materialize the aggregation's intermediate tuple instead of
   * the output tuple.
   */
  public void setIntermediateTuple() {
    Preconditions.checkState(!tupleIds_.isEmpty());
    Preconditions.checkState(tupleIds_.get(0).equals(aggInfo_.getOutputTupleId()));
    tupleIds_.clear();
    tupleIds_.add(aggInfo_.getIntermediateTupleId());
  }

  @Override
  public boolean isBlockingNode() { return !useStreamingPreagg_; }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    // Assign predicates to the top-most agg in the single-node plan that can evaluate
    // them, as follows: For non-distinct aggs place them in the 1st phase agg node. For
    // distinct aggs place them in the 2nd phase agg node. The conjuncts are
    // transferred to the proper place in the multi-node plan via transferConjuncts().
    if (tupleIds_.get(0).equals(aggInfo_.getResultTupleId()) && !aggInfo_.isMerge()) {
      // Ignore predicates bound by a grouping slot produced by a SlotRef grouping expr.
      // Those predicates are already evaluated below this agg node (e.g., in a scan),
      // because the grouping slot must be in the same equivalence class as another slot
      // below this agg node. We must not ignore other grouping slots in order to retain
      // conjuncts bound by those grouping slots in createEquivConjuncts() (IMPALA-2089).
      // Those conjuncts cannot be redundant because our equivalence classes do not
      // capture dependencies with non-SlotRef exprs.
      Set<SlotId> groupBySlots = Sets.newHashSet();
      for (int i = 0; i < aggInfo_.getGroupingExprs().size(); ++i) {
        if (aggInfo_.getGroupingExprs().get(i).unwrapSlotRef(true) == null) continue;
        groupBySlots.add(aggInfo_.getOutputTupleDesc().getSlots().get(i).getId());
      }
      ArrayList<Expr> bindingPredicates =
          analyzer.getBoundPredicates(tupleIds_.get(0), groupBySlots, true);
      conjuncts_.addAll(bindingPredicates);

      // also add remaining unassigned conjuncts_
      assignConjuncts(analyzer);

      analyzer.createEquivConjuncts(tupleIds_.get(0), conjuncts_, groupBySlots);
    }
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    // Compute the mem layout for both tuples here for simplicity.
    aggInfo_.getOutputTupleDesc().computeMemLayout();
    aggInfo_.getIntermediateTupleDesc().computeMemLayout();

    // do this at the end so it can take all conjuncts into account
    computeStats(analyzer);

    // don't call createDefaultSMap(), it would point our conjuncts (= Having clause)
    // to our input; our conjuncts don't get substituted because they already
    // refer to our output
    outputSmap_ = getCombinedChildSmap();
    aggInfo_.substitute(outputSmap_, analyzer);
    // assert consistent aggregate expr and slot materialization
    aggInfo_.checkConsistency();
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

    // Any non-grouping aggregation has at least one distinct value
    cardinality_ = aggInfo_.getGroupingExprs().isEmpty() ? 1 :
      Expr.getNumDistinctValues(aggInfo_.getGroupingExprs());
    // take HAVING predicate into account
    if (LOG.isTraceEnabled()) {
      LOG.trace("Agg: cardinality=" + Long.toString(cardinality_));
    }
    if (cardinality_ > 0) {
      cardinality_ = Math.round((double) cardinality_ * computeSelectivity());
      if (LOG.isTraceEnabled()) {
        LOG.trace("sel=" + Double.toString(computeSelectivity()));
      }
    }
    // if we ended up with an overflow, the estimate is certain to be wrong
    if (cardinality_ < 0) cardinality_ = -1;
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
    cardinality_ = capAtLimit(cardinality_);
    if (LOG.isTraceEnabled()) {
      LOG.trace("stats Agg: cardinality=" + Long.toString(cardinality_));
    }
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("aggInfo", aggInfo_.debugString())
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.AGGREGATION_NODE;

    List<TExpr> aggregateFunctions = Lists.newArrayList();
    // only serialize agg exprs that are being materialized
    for (FunctionCallExpr e: aggInfo_.getMaterializedAggregateExprs()) {
      aggregateFunctions.add(e.treeToThrift());
    }
    aggInfo_.checkConsistency();
    msg.agg_node = new TAggregationNode(
        aggregateFunctions,
        aggInfo_.getIntermediateTupleId().asInt(),
        aggInfo_.getOutputTupleId().asInt(), needsFinalize_,
        useStreamingPreagg_,
        getChild(0).getCardinality());
    List<Expr> groupingExprs = aggInfo_.getGroupingExprs();
    if (groupingExprs != null) {
      msg.agg_node.setGrouping_exprs(Expr.treesToThrift(groupingExprs));
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
      ArrayList<FunctionCallExpr> aggExprs = aggInfo_.getMaterializedAggregateExprs();
      if (!aggExprs.isEmpty()) {
        output.append(detailPrefix + "output: ")
            .append(getExplainString(aggExprs) + "\n");
      }
      // TODO: is this the best way to display this. It currently would
      // have DISTINCT_PC(DISTINCT_PC(col)) for the merge phase but not
      // very obvious what that means if you don't already know.

      // TODO: group by can be very long. Break it into multiple lines
      if (!aggInfo_.getGroupingExprs().isEmpty()) {
        output.append(detailPrefix + "group by: ")
            .append(getExplainString(aggInfo_.getGroupingExprs()) + "\n");
      }
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix + "having: ")
            .append(getExplainString(conjuncts_) + "\n");
      }
    }
    return output.toString();
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    Preconditions.checkNotNull(
        fragment_, "PlanNode must be placed into a fragment before calling this method.");
    long perInstanceCardinality = fragment_.getPerInstanceNdv(
        queryOptions.getMt_dop(), aggInfo_.getGroupingExprs());
    long perInstanceMemEstimate;
    long perInstanceDataBytes = -1;
    if (perInstanceCardinality == -1) {
      perInstanceMemEstimate = DEFAULT_PER_INSTANCE_MEM;
    } else {
      // Per-instance cardinality cannot be greater than the total output cardinality.
      if (cardinality_ != -1) {
        perInstanceCardinality = Math.min(perInstanceCardinality, cardinality_);
      }
      perInstanceDataBytes = (long)Math.ceil(perInstanceCardinality * avgRowSize_);
      perInstanceMemEstimate = (long)Math.max(perInstanceDataBytes *
          PlannerContext.HASH_TBL_SPACE_OVERHEAD, MIN_HASH_TBL_MEM);
    }

    // Must be kept in sync with PartitionedAggregationNode::MinReservation() in be.
    long perInstanceMinReservation;
    long bufferSize = queryOptions.getDefault_spillable_buffer_size();
    long maxRowBufferSize =
        computeMaxSpillableBufferSize(bufferSize, queryOptions.getMax_row_size());
    if (aggInfo_.getGroupingExprs().isEmpty()) {
      perInstanceMinReservation = 0;
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
        perInstanceMinReservation = bufferSize * PARTITION_FANOUT +
            Math.max(64 * 1024 * PARTITION_FANOUT, bufferSize);
      } else {
        long minBuffers = PARTITION_FANOUT + 1 + (aggInfo_.needsSerialize() ? 1 : 0);
        // Two of the buffers need to be buffers large enough to hold the maximum-sized
        // row to serve as input and output buffers while repartitioning.
        perInstanceMinReservation = bufferSize * (minBuffers - 2) + maxRowBufferSize * 2;
      }
    }

    nodeResourceProfile_ = new ResourceProfileBuilder()
        .setMemEstimateBytes(perInstanceMemEstimate)
        .setMinReservationBytes(perInstanceMinReservation)
        .setSpillableBufferBytes(bufferSize)
        .setMaxRowBufferBytes(maxRowBufferSize).build();
  }
}
