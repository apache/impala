// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TExprOperator;
import com.google.common.base.Preconditions;

/**
 * &&, ||, ! predicates.
 *
 */
public class CompoundPredicate extends Predicate {
  enum Operator {
    AND("AND", TExprOperator.AND),
    OR("OR", TExprOperator.OR),
    NOT("NOT", TExprOperator.NOT);

    private final String description;
    private final TExprOperator thriftOp;

    private Operator(String description, TExprOperator thriftOp) {
      this.description = description;
      this.thriftOp = thriftOp;
    }

    @Override
    public String toString() {
      return description;
    }

    public TExprOperator toThrift() {
      return thriftOp;
    }
  }
  private final Operator op;

  public CompoundPredicate(Operator op, Predicate p1, Predicate p2) {
    super();
    this.op = op;
    Preconditions.checkNotNull(p1);
    children.add(p1);
    Preconditions.checkArgument(op == Operator.NOT && p2 == null
        || op != Operator.NOT && p2 != null);
    if (p2 != null) {
      children.add(p2);
    }
  }

  public Operator getOp() {
    return op;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((CompoundPredicate) obj).op == op;
  }

  @Override
  public String toSql() {
    if (children.size() == 1) {
      Preconditions.checkState(op == Operator.NOT);
      return "NOT " + getChild(0).toSql();
    } else {
      return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.COMPOUND_PRED;
    msg.op = op.toThrift();
  }
}
