// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.Iterator;
import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Sorting.
 *
 */
public class SortNode extends PlanNode {
  private final List<Expr> orderingExprs;
  private final List<Boolean> isAscOrder;

  public SortNode(PlanNode input, List<Expr> orderingExprs, List<Boolean> isAscOrder) {
    Preconditions.checkArgument(orderingExprs.size() == isAscOrder.size());
    this.orderingExprs = orderingExprs;
    this.isAscOrder = isAscOrder;
    this.children.add(input);
  }

  @Override
  protected String debugString() {
    List<String> strings = Lists.newArrayList();
    for (Boolean isAsc : isAscOrder) {
      strings.add(isAsc ? "a" : "d");
    }
    return Objects.toStringHelper(this)
        .add("ordering_exprs", Expr.debugString(orderingExprs))
        .add("is_asc", "[" + Joiner.on(" ").join(strings) + "]")
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "SORT\n");
    output.append(prefix + "ORDER BY: ");
    Iterator<Expr> expr = orderingExprs.iterator();
    Iterator<Boolean> isAsc = isAscOrder.iterator();
    boolean start = true;
    while (expr.hasNext()) {
      if (start) {
        start = false;
      } else {
        output.append(", ");
      }
      output.append(expr.next().toSql() + " ");
      output.append(isAsc.next() ? "ASC" : "DESC");
      output.append("\n");
    }
    output.append(getChild(0).getExplainString(prefix + "  "));
    return output.toString();
  }
}
