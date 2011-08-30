// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TEqJoinCondition;
import com.cloudera.impala.thrift.THashJoinNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Hash join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 *
 */
public class HashJoinNode extends PlanNode {
  // predicates of the form "<lhs> = <rhs>", recorded as Pair(<lhs>, <rhs>)
  private final List<Pair<Expr, Expr> > joinPredicates;

  public HashJoinNode(PlanNode outer, PlanNode inner,
                      List<Pair<Expr, Expr> > joinPredicates) {
    super();
    tupleIds.addAll(outer.getTupleIds());
    Preconditions.checkState(inner.getTupleIds().size() == 1);
    tupleIds.add(inner.getTupleIds().get(0));
    this.joinPredicates = joinPredicates;
    children.add(outer);
    children.add(inner);
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("joinPreds", joinPredicatesDebugString())
        .addValue(super.debugString())
        .toString();
  }

  private String joinPredicatesDebugString() {
    Objects.ToStringHelper helper = Objects.toStringHelper(this);
    for (Pair<Expr, Expr> entry: joinPredicates) {
      helper.add("lhs" , entry.first).add("rhs", entry.second);
    }
    return helper.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
    msg.hash_join_node = new THashJoinNode();
    for (Pair<Expr, Expr> entry: joinPredicates) {
      TEqJoinCondition eqJoinCondition =
          new TEqJoinCondition(entry.first.treeToThrift(), entry.second.treeToThrift());
      msg.hash_join_node.addToJoin_predicates(eqJoinCondition);
    }
  }

  @Override
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "HASH JOIN\n");
    output.append(prefix + "HASH PREDICATES:");
    for (Pair<Expr, Expr> entry: joinPredicates) {
      output.append(
          "\n" + prefix + "  " + entry.first.toSql() + " = " + entry.second.toSql());
    }
    if (!conjuncts.isEmpty()) {
      output.append("\n" + prefix + "OTHER PREDICATES: ");
      output.append(getExplainString(conjuncts));
    }
    output.append("\n" + getChild(0).getExplainString(prefix + "  "));
    output.append("\n" + getChild(1).getExplainString(prefix + "  "));
    return output.toString();
  }
}
