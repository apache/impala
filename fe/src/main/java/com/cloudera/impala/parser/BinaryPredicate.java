// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.PrimitiveType;
import com.google.common.base.Preconditions;

// Most predicates with two operands.
class BinaryPredicate extends Predicate {
  enum Operator {
    EQ, // =
    NE, // !=, <>
    LE, // <=
    GE, // >=
    LT, // <
    GT, // >
  };
  private final Operator op;

  public BinaryPredicate(Operator op, Expr e1, Expr e2) {
    super();
    this.op = op;
    Preconditions.checkNotNull(e1);
    children.add(e1);
    Preconditions.checkNotNull(e2);
    children.add(e2);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((BinaryPredicate) obj).op == op;
  }

  // TODO: this only checks the operand types; we also need to insert operand casts
  // when their types aren't identical
  @Override
  public void analyze(Analyzer analyzer) throws Analyzer.Exception {
    super.analyze(analyzer);
    if (!PrimitiveType.getAssignmentCompatibleType(getChild(0).getType(), getChild(1).getType())
        .isValid()) {
      // there is no type to which both are assignment-compatible -> we can't compare them
      throw new Analyzer.Exception("operands are not comparable: " + this.toSql());
    }
  }
}
