// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.Iterator;
import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SortInfo;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TSortNode;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Sorting.
 *
 */
public class SortNode extends PlanNode {
  private final SortInfo info;
  private final boolean useTopN;

  public SortNode(PlanNode input, SortInfo info, boolean useTopN) {
    super();
    this.info = info;
    this.useTopN = useTopN;
    this.tupleIds.addAll(input.getTupleIds());
    this.children.add(input);
    this.rowTupleIds.addAll(input.getRowTupleIds());
    Preconditions.checkArgument(info.getOrderingExprs().size() == info.getIsAscOrder().size());
  }

  @Override
  public void getMaterializedIds(List<SlotId> ids) {
    super.getMaterializedIds(ids);
    Expr.getIds(info.getOrderingExprs(), null, ids);
  }

  @Override
  protected String debugString() {
    List<String> strings = Lists.newArrayList();
    for (Boolean isAsc : info.getIsAscOrder()) {
      strings.add(isAsc ? "a" : "d");
    }
    return Objects.toStringHelper(this)
        .add("ordering_exprs", Expr.debugString(info.getOrderingExprs()))
        .add("is_asc", "[" + Joiner.on(" ").join(strings) + "]")
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SORT_NODE;
    msg.sort_node = new TSortNode(
        Expr.treesToThrift(info.getOrderingExprs()), info.getIsAscOrder(), useTopN);
  }

  @Override
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    if (useTopN) {
      output.append(prefix + "TOP-N\n");
    } else {
      output.append(prefix + "SORT\n");
    }
    output.append(prefix + "ORDER BY: ");
    Iterator<Expr> expr = info.getOrderingExprs().iterator();
    Iterator<Boolean> isAsc = info.getIsAscOrder().iterator();
    boolean start = true;
    while (expr.hasNext()) {
      if (start) {
        start = false;
      } else {
        output.append(", ");
      }
      output.append(expr.next().toSql() + " ");
      output.append(isAsc.next() ? "ASC" : "DESC");
    }
    output.append("\n" + super.getExplainString(prefix));
    output.append(getChild(0).getExplainString(prefix + "  "));
    return output.toString();
  }
}
