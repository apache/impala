// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.analysis.AggregateInfo;
import com.google.common.base.Objects;

/**
 * Aggregation computation.
 *
 */
public class AggregationNode extends PlanNode {
  private final AggregateInfo aggInfo;

  public AggregationNode(PlanNode input, AggregateInfo aggInfo) {
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
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "AGGREGATE\n");
    output.append(prefix + "GROUP BY: ");
    output.append(getExplainString(aggInfo.getGroupingExprs()) + "\n");
    if (conjuncts != null) {
      output.append(prefix + "HAVING: ");
      output.append(getExplainString(conjuncts) + "\n");
    }
    output.append(getChild(0).getExplainString(prefix + "  "));
    return output.toString();
  }
}
