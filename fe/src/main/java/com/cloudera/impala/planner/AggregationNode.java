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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.FunctionCallExpr;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.thrift.TAggregateFunction;
import com.cloudera.impala.thrift.TAggregationNode;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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

  private final AggregateInfo aggInfo;

  // Set to true if this aggregation node needs to run the Finalize step. This
  // node is the root node of a distributed aggregation.
  private boolean needsFinalize;

  /**
   * Create an agg node from aggInfo.
   */
  public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
    super(id, aggInfo.getAggTupleId().asList(), "AGGREGATE");
    this.aggInfo = aggInfo;
    this.children.add(input);
    this.needsFinalize = true;
    updateDisplayName();
  }

  public AggregateInfo getAggInfo() { return aggInfo; }

  // Unsets this node as requiring finalize. Only valid to call this if it is
  // currently marked as needing finalize.
  public void unsetNeedsFinalize() {
    Preconditions.checkState(needsFinalize);
    needsFinalize = false;
    updateDisplayName();
  }

  @Override
  public void setCompactData(boolean on) { this.compactData = on; }

  @Override
  public boolean isBlockingNode() { return true; }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    List<Expr> groupingExprs = aggInfo.getGroupingExprs();
    cardinality = 1;
    // cardinality: product of # of distinct values produced by grouping exprs
    for (Expr groupingExpr: groupingExprs) {
      long numDistinct = groupingExpr.getNumDistinctValues();
      LOG.debug("grouping expr: " + groupingExpr.toSql() + " #distinct="
          + Long.toString(numDistinct));
      if (numDistinct == -1) {
        cardinality = -1;
        break;
      }
      // This is prone to overflow, because we keep multiplying cardinalities,
      // even if the grouping exprs are functionally dependent (example:
      // group by the primary key of a table plus a number of other columns from that
      // same table)
      // TODO: try to recognize functional dependencies
      // TODO: as a shortcut, instead of recognizing functional dependencies,
      // limit the contribution of a single table to the number of rows
      // of that table (so that when we're grouping by the primary key col plus
      // some others, the estimate doesn't overshoot dramatically)
      cardinality *= numDistinct;
    }
    // take HAVING predicate into account
    LOG.debug("Agg: cardinality=" + Long.toString(cardinality));
    if (cardinality > 0) {
      cardinality = Math.round((double) cardinality * computeSelectivity());
      LOG.debug("sel=" + Double.toString(computeSelectivity()));
    }
    // if we ended up with an overflow, the estimate is certain to be wrong
    if (cardinality < 0) cardinality = -1;
    LOG.debug("stats Agg: cardinality=" + Long.toString(cardinality));
  }

  private void updateDisplayName() {
    StringBuilder sb = new StringBuilder();
    sb.append("AGGREGATE");
    if (aggInfo.isMerge() || needsFinalize) {
      sb.append(" (");
      if (aggInfo.isMerge() && needsFinalize) {
        sb.append("merge finalize");
      } else if (aggInfo.isMerge()) {
        sb.append("merge");
      } else {
        sb.append("finalize");
      }
      sb.append(")");
    }
    setDisplayName(sb.toString());
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("aggInfo", aggInfo.debugString())
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.AGGREGATION_NODE;

    List<TAggregateFunction> aggregateFunctions = Lists.newArrayList();
    for (FunctionCallExpr e: aggInfo.getAggregateExprs()) {
      aggregateFunctions.add(e.toTAggregateFunction());
    }
    msg.agg_node = new TAggregationNode(
        aggregateFunctions,
        aggInfo.getAggTupleId().asInt(), needsFinalize);
    msg.agg_node.setIs_merge(aggInfo.isMerge());
    List<Expr> groupingExprs = aggInfo.getGroupingExprs();
    if (groupingExprs != null) {
      msg.agg_node.setGrouping_exprs(Expr.treesToThrift(groupingExprs));
    }
  }

  @Override
  protected String getNodeExplainString(String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    if (aggInfo.getAggregateExprs() != null && aggInfo.getAggregateExprs().size() > 0) {
      output.append(detailPrefix + "output: ")
        .append(getExplainString(aggInfo.getAggregateExprs()) + "\n");
    }
    // TODO: is this the best way to display this. It currently would
    // have DISTINCT_PC(DISTINCT_PC(col)) for the merge phase but not
    // very obvious what that means if you don't already know.

    // TODO: group by can be very long. Break it into multiple lines
    output.append(detailPrefix + "group by: ")
      .append(getExplainString(aggInfo.getGroupingExprs()) + "\n");
    if (!conjuncts.isEmpty()) {
      output.append(detailPrefix + "having: ")
          .append(getExplainString(conjuncts) + "\n");
    }
    return output.toString();
  }

  @Override
  public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
    super.getMaterializedIds(analyzer, ids);

    // we indirectly reference all grouping slots (because we write them)
    // so they're all materialized.
    aggInfo.getRefdSlots(ids);
  }

  @Override
  public void computeCosts() {
    Preconditions.checkNotNull(fragment,
        "PlanNode must be placed into a fragment before calling this method.");
    perHostMemCost = 0;
    long perHostCardinality = fragment.getNumDistinctValues(aggInfo.getGroupingExprs());
    if (perHostCardinality != -1) {
      // take HAVING predicate into account
      perHostCardinality =
          Math.round((double) perHostCardinality * computeSelectivity());
      perHostMemCost += Math.max(perHostCardinality * avgRowSize *
          Planner.HASH_TBL_SPACE_OVERHEAD, MIN_HASH_TBL_MEM);
    } else {
      perHostMemCost += DEFAULT_PER_HOST_MEM;
    }
  }
}
