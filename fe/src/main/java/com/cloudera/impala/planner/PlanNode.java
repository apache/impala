// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.common.TreeNode;
import com.cloudera.impala.thrift.TPlan;
import com.cloudera.impala.thrift.TPlanNode;
import com.google.common.collect.Lists;

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

  PlanNode() {
    this.conjuncts = Lists.newArrayList();
  }

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

  // Convert this plan node, including all children, to its Thrift representation.
  public TPlan treeToThrift() {
    TPlan result = new TPlan();
    treeToThriftHelper(result);
    return result;
  }

  // Append a flattened version of this plan node, including all children, to 'container'.
  private void treeToThriftHelper(TPlan container) {
    TPlanNode msg = new TPlanNode();
    msg.num_children = children.size();
    msg.limit = limit;
    for (Predicate p: conjuncts) {
      msg.addToConjuncts(p.treeToThrift());
    }
    toThrift(msg);
    container.addToNodes(msg);
    for (PlanNode child: children) {
      child.treeToThriftHelper(container);
    }
  }

  // Convert this plan node into msg (excluding children), which requires setting
  // the node type and the node-specific field.
  protected abstract void toThrift(TPlanNode msg);

  protected String debugString() {
    // not using Objects.toStrHelper because
    // PlanNode.debugString() is embedded by debug strings of the subclasses
    StringBuilder output = new StringBuilder();
    output.append("preds=" + Expr.debugString(conjuncts));
    output.append(" limit=" + Long.toString(limit));
    return output.toString();
  }

  protected String getExplainString(List<? extends Expr> exprs) {
    if (exprs == null) {
      return "";
    }
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
