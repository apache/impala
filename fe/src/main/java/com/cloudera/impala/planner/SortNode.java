// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.Iterator;
import java.util.List;

import com.cloudera.impala.parser.Expr;
import com.google.common.base.Joiner;
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
    StringBuilder output = new StringBuilder("Sort(");
    output.append("ordering_exprs=" + Expr.debugString(orderingExprs));
    output.append(" is_asc=");
    List<String> strings = Lists.newArrayList();
    for (Boolean isAsc: isAscOrder) {
      strings.add(isAsc ? "a" : "d");
    }
    output.append("[" + Joiner.on(" ").join(strings) + "]");
    output.append(" " + super.debugString() + ")");
    return output.toString();
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
