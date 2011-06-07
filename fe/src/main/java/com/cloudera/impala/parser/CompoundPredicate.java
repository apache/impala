// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.google.common.base.Preconditions;

// &&, ||, ! predicates
class CompoundPredicate extends Predicate {
  enum Operator {
    AND,
    OR,
    NOT
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

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((CompoundPredicate) obj).op == op;
  }
}
