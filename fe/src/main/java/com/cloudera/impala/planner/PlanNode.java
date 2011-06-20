// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.common.TreeNode;
import com.cloudera.impala.parser.Expr;
import com.cloudera.impala.parser.Predicate;

/**
 * Each PlanNode represents a single relational operator
 * and encapsulates the information needed by the planner to
 * make optimization decisions.
 *
 */
abstract public class PlanNode extends TreeNode<PlanNode> {
  protected long limit; // max. # of rows to be returned; 0: no limit

  /**
   * all conjuncts can be executed in the context of this node, ie,
   * they only reference tuples materialized by this node or one of
   * its children
   */
  protected List<Predicate> conjuncts;

  public long getLimit() {
    return limit;
  }

  public void setLimit(long limit) {
    this.limit = limit;
  }

  public List<Predicate> getConjuncts() {
    return conjuncts;
  }

  public void setConjuncts(List<Predicate> conjuncts) {
    this.conjuncts = conjuncts;
  }

  public String getExplainString() {
    return getExplainString("");
  }

  protected abstract String getExplainString(String prefix);

  protected String debugString() {
    // not using Objects.toStrHelper because
    // PlanNode.debugString() is embedded by debug strings of the subclasses
    StringBuilder output = new StringBuilder();
    output.append("preds=" + Expr.debugString(conjuncts));
    output.append(" limit=" + Long.toString(limit));
    return output.toString();
  }

  protected String getExplainString(List<? extends Expr> exprs) {
    StringBuilder output = new StringBuilder();
    for (int i = 0; i < exprs.size(); ++i) {
      if (i > 0) {
        output.append(", ");
      }
      output.append(exprs.get(i).toSql());
    }
    return output.toString();
  }
}
