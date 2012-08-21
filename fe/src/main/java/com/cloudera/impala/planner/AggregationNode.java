// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.AggregateExpr;
import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.thrift.TAggregationNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TExplainLevel;
import com.google.common.base.Objects;

/**
 * Aggregation computation.
 *
 */
public class AggregationNode extends PlanNode {
  private final AggregateInfo aggInfo;

  // Set to true if this aggregation node contains aggregate functions that require
  // finalization after all rows have been aggregated.
  private boolean needsFinalize;

  /**
   * Create an agg node that is not an intermediate node.
   * isIntermediate is true if it is a slave node in a 2-part agg plan.
   */
  public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo,
      boolean isIntermediate) {
    super(id, aggInfo.getAggTupleId().asList());
    this.aggInfo = aggInfo;
    this.children.add(input);
    this.rowTupleIds.add(aggInfo.getAggTupleId());
    needsFinalize = false;
    if (!isIntermediate) {
      for (AggregateExpr expr: aggInfo.getAggregateExprs()) {
        if (expr.getOp().getNeedFinalize()) {
          needsFinalize = true;
          break;
        }
      }
    }
  }

  /**
   * Create an agg node that is not an intermediate agg node. It is either an agg node in
   * a single node plan, or a coord agg node in a multi-node plan.
   */
  public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
    this(id, input, aggInfo, false);
  }

  public AggregateInfo getAggInfo() {
    return aggInfo;
  }

  @Override
  public void setCompactData(boolean on) {
    this.compactData = on;
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
    msg.agg_node = new TAggregationNode(
        Expr.treesToThrift(aggInfo.getAggregateExprs()),
        aggInfo.getAggTupleId().asInt(), needsFinalize);
    List<Expr> groupingExprs = aggInfo.getGroupingExprs();
    if (groupingExprs != null) {
      msg.agg_node.setGrouping_exprs(Expr.treesToThrift(groupingExprs));
    }
  }

  @Override
  protected String getExplainString(String prefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder()
        .append(prefix + "AGGREGATE\n")
        .append(prefix + "OUTPUT: ")
        .append(getExplainString(aggInfo.getAggregateExprs()) + "\n")
        .append(prefix + "GROUP BY: ")
        .append(getExplainString(aggInfo.getGroupingExprs()) + "\n");
    if (!conjuncts.isEmpty()) {
      output.append(prefix + "HAVING: ")
          .append(getExplainString(conjuncts) + "\n");
    }
    output.append(super.getExplainString(prefix, detailLevel))
        .append(getChild(0).getExplainString(prefix + "  ", detailLevel));
    return output.toString();
  }

  @Override
  public void getMaterializedIds(List<SlotId> ids) {
    super.getMaterializedIds(ids);

    // we indirectly reference all grouping slots (because we write them)
    // so they're all materialized.
    aggInfo.getRefdSlots(ids);
  }
}
