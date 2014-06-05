// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.FunctionCallExpr;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.thrift.TAggregationNode;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TQueryOptions;
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

  // Default per-host memory requirement used if no valid stats are available.
  // TODO: Come up with a more useful heuristic.
  private final static long DEFAULT_PER_HOST_MEM = 128L * 1024L * 1024L;

  // Conservative minimum size of hash table for low-cardinality aggregations.
  private final static long MIN_HASH_TBL_MEM = 10L * 1024L * 1024L;

  private final AggregateInfo aggInfo_;

  // Set to true if this aggregation node needs to run the Finalize step. This
  // node is the root node of a distributed aggregation.
  private boolean needsFinalize_;

  /**
   * Create an agg node from aggInfo.
   */
  public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
    super(id, aggInfo.getAggTupleId().asList(), "AGGREGATE");
    aggInfo_ = aggInfo;
    children_.add(input);
    nullableTupleIds_.addAll(input.getNullableTupleIds());
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

  // Unsets this node as requiring finalize. Only valid to call this if it is
  // currently marked as needing finalize.
  public void unsetNeedsFinalize() {
    Preconditions.checkState(needsFinalize_);
    needsFinalize_ = false;
  }

  @Override
  public void setCompactData(boolean on) { compactData_ = on; }

  @Override
  public boolean isBlockingNode() { return true; }

  @Override
  public void init(Analyzer analyzer) {
    // TODO: It seems wrong that the 2nd phase agg has aggInfo_.isMerge() == true.
    // The reason for this is that the non-distinct aggregate functions need to use
    // the merge function in the BE (see toThrift() of this class). Clean this up.
    boolean isSecondPhaseAgg = false;
    if (getChild(0) instanceof AggregationNode) {
      AggregationNode childAggNode = (AggregationNode) getChild(0);
      if (childAggNode.getAggInfo().getSecondPhaseDistinctAggInfo() == aggInfo_) {
        isSecondPhaseAgg = true;
      }
    }
    // Assign predicates to the top-most agg in the single-node plan that can evaluate
    // them, as follows: For non-distinct aggs place them in the 1st phase agg node. For
    // distinct aggs place them in the 2nd phase agg node. The conjuncts are
    // transferred to the proper place in the multi-node plan via transferConjuncts().
    if (tupleIds_.get(0).equals(aggInfo_.getOutputTupleId()) &&
        (isSecondPhaseAgg || !aggInfo_.isMerge())) {
      // Ignore predicates bound to a group-by slot because those
      // are already evaluated below this agg node (e.g., in a scan).
      Set<SlotId> groupBySlots = Sets.newHashSet();
      for (int i = 0; i < aggInfo_.getGroupingExprs().size(); ++i) {
        groupBySlots.add(aggInfo_.getAggTupleDesc().getSlots().get(i).getId());
      }
      ArrayList<Expr> bindingPredicates =
          analyzer.getBoundPredicates(tupleIds_.get(0), groupBySlots);
      conjuncts_.addAll(bindingPredicates);

      // also add remaining unassigned conjuncts_
      assignConjuncts(analyzer);

      analyzer.enforceSlotEquivalences(tupleIds_.get(0), conjuncts_, groupBySlots);
    }
    computeMemLayout(analyzer);
    // do this at the end so it can take all conjuncts into account
    computeStats(analyzer);

    // don't call createDefaultSMap(), it would point our conjuncts (= Having clause)
    // to our input; our conjuncts don't get substituted because they already
    // refer to our output
    Expr.SubstitutionMap combinedChildSmap = getCombinedChildSmap();
    aggInfo_.substitute(combinedChildSmap);
    baseTblSmap_ = aggInfo_.getSMap();
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
    cardinality_ = Expr.getNumDistinctValues(aggInfo_.getGroupingExprs());
    // take HAVING predicate into account
    LOG.trace("Agg: cardinality=" + Long.toString(cardinality_));
    if (cardinality_ > 0) {
      cardinality_ = Math.round((double) cardinality_ * computeSelectivity());
      LOG.trace("sel=" + Double.toString(computeSelectivity()));
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
    LOG.trace("stats Agg: cardinality=" + Long.toString(cardinality_));
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
        aggInfo_.getAggTupleId().asInt(), needsFinalize_);
    msg.agg_node.setIs_merge(aggInfo_.isMerge());
    List<Expr> groupingExprs = aggInfo_.getGroupingExprs();
    if (groupingExprs != null) {
      msg.agg_node.setGrouping_exprs(Expr.treesToThrift(groupingExprs));
    }
  }

  @Override
  protected String getDisplayLabelDetail() {
    if (aggInfo_.isMerge() || needsFinalize_) {
      if (aggInfo_.isMerge() && needsFinalize_) {
        return "MERGE FINALIZE";
      } else if (aggInfo_.isMerge()) {
        return "MERGE";
      } else {
        return "FINALIZE";
      }
    }
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
      if (aggInfo_.getAggregateExprs() != null &&
          aggInfo_.getAggregateExprs().size() > 0) {
        output.append(detailPrefix + "output: ")
        .append(getExplainString(aggInfo_.getAggregateExprs()) + "\n");
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
  public void computeCosts(TQueryOptions queryOptions) {
    Preconditions.checkNotNull(fragment_,
        "PlanNode must be placed into a fragment before calling this method.");
    perHostMemCost_ = 0;
    long perHostCardinality = fragment_.getNumDistinctValues(aggInfo_.getGroupingExprs());
    if (perHostCardinality == -1) {
      perHostMemCost_ = DEFAULT_PER_HOST_MEM;
      return;
    }

    // Per-host cardinality cannot be greater than the total output cardinality.
    if (cardinality_ != -1) {
      perHostCardinality = Math.min(perHostCardinality, cardinality_);
    }
    perHostMemCost_ += Math.max(perHostCardinality * avgRowSize_ *
        Planner.HASH_TBL_SPACE_OVERHEAD, MIN_HASH_TBL_MEM);
  }
}
