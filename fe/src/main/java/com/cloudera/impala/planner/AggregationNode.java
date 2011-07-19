// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.thrift.TAggregationNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.google.common.base.Objects;

/**
 * Aggregation computation.
 *
 */
public class AggregationNode extends PlanNode {
  private final AggregateInfo aggInfo;

  public AggregationNode(PlanNode input, AggregateInfo aggInfo) {
    super();
    this.aggInfo = aggInfo;
    this.children.add(input);
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
        aggInfo.getAggTupleId().asInt());
    List<Expr> groupingExprs = aggInfo.getGroupingExprs();
    if (groupingExprs != null) {
      msg.agg_node.setGrouping_exprs(Expr.treesToThrift(groupingExprs));
    }
  }

  @Override
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "AGGREGATE\n");
    output.append(prefix + "GROUP BY: ");
    output.append(getExplainString(aggInfo.getGroupingExprs()) + "\n");
    if (!conjuncts.isEmpty()) {
      output.append(prefix + "HAVING: ");
      output.append(getExplainString(conjuncts) + "\n");
    }
    output.append(getChild(0).getExplainString(prefix + "  "));
    return output.toString();
  }
}
