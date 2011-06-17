// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.google.common.base.Preconditions;

/**
 * &&, ||, ! predicates.
 *
 */
public class CompoundPredicate extends Predicate {
  enum Operator {
    AND("AND"),
    OR("OR"),
    NOT("NOT");

    private final String description;

    private Operator(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
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
}
