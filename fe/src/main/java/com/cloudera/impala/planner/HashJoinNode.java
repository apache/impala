// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.JoinOperator;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.SlotId;
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
  private final JoinOperator joinOp;
  // conjuncts of the form "<lhs> = <rhs>", recorded as Pair(<lhs>, <rhs>)
  private final List<Pair<Expr, Expr> > eqJoinConjuncts;

  // join conjuncts from the JOIN clause that aren't equi-join predicates
  private final List<Predicate> otherJoinConjuncts;

  public HashJoinNode(
      PlanNode outer, PlanNode inner, JoinOperator joinOp,
      List<Pair<Expr, Expr> > eqJoinConjuncts,
      List<Predicate> otherJoinConjuncts) {
    super();
    tupleIds.addAll(outer.getTupleIds());
    Preconditions.checkState(inner.getTupleIds().size() == 1);
    tupleIds.add(inner.getTupleIds().get(0));
    this.joinOp = joinOp;
    this.eqJoinConjuncts = eqJoinConjuncts;
    this.otherJoinConjuncts = otherJoinConjuncts;
    children.add(outer);
    children.add(inner);
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("eqJoinConjuncts", eqJoinConjunctsDebugString())
        .addValue(super.debugString())
        .toString();
  }

  private String eqJoinConjunctsDebugString() {
    Objects.ToStringHelper helper = Objects.toStringHelper(this);
    for (Pair<Expr, Expr> entry: eqJoinConjuncts) {
      helper.add("lhs" , entry.first).add("rhs", entry.second);
    }
    return helper.toString();
  }

  @Override
  public void getMaterializedIds(List<SlotId> ids) {
    super.getMaterializedIds(ids);
    // we also need to materialize everything referenced by eqJoinConjuncts
    // and otherJoinConjuncts
    for (Pair<Expr, Expr> p: eqJoinConjuncts) {
      p.first.getIds(null, ids);
      p.second.getIds(null, ids);
    }
    for (Predicate p: otherJoinConjuncts) {
      p.getIds(null, ids);
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
    msg.hash_join_node = new THashJoinNode();
    msg.hash_join_node.join_op = joinOp.toThrift();
    for (Pair<Expr, Expr> entry: eqJoinConjuncts) {
      TEqJoinCondition eqJoinCondition =
          new TEqJoinCondition(entry.first.treeToThrift(), entry.second.treeToThrift());
      msg.hash_join_node.addToEq_join_conjuncts(eqJoinCondition);
    }
    for (Predicate p: otherJoinConjuncts) {
      msg.hash_join_node.addToOther_join_conjuncts(p.treeToThrift());
    }
  }

  @Override
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "HASH JOIN\n");
    output.append(prefix + "  JOIN OP: " + joinOp.toString() + "\n");
    output.append(prefix + "  HASH PREDICATES:");
    for (Pair<Expr, Expr> entry: eqJoinConjuncts) {
      output.append(
          "\n" + prefix + "  " + entry.first.toSql() + " = " + entry.second.toSql());
    }
    if (!otherJoinConjuncts.isEmpty()) {
      output.append("\n" + prefix + "  OTHER JOIN PREDICATES: ");
      output.append(getExplainString(otherJoinConjuncts));
    }
    if (!conjuncts.isEmpty()) {
      output.append("\n" + prefix + "  OTHER PREDICATES: ");
      output.append(getExplainString(conjuncts));
    }
    output.append(getLimitExplainString(prefix));
    output.append("\n" + getChild(0).getExplainString(prefix + "    "));
    output.append("\n" + getChild(1).getExplainString(prefix + "    "));
    return output.toString();
  }
}
