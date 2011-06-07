// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.PrimitiveType;
import com.google.common.base.Preconditions;

class ArithmeticExpr extends Expr {
  enum Operator {
    MULTIPLY,
    DIVIDE,
    MOD,
    INT_DIVIDE,
    PLUS,
    MINUS,
    BITAND,
    BITOR,
    BITXOR,
    BITNOT;

    public boolean isBitwiseOperation() {
      return this == BITAND || this == BITOR || this == BITXOR || this == BITNOT;
    }
  }
  private final Operator op;

  public ArithmeticExpr(Operator op, Expr e1, Expr e2) {
    super();
    this.op = op;
    Preconditions.checkNotNull(e1);
    children.add(e1);
    Preconditions.checkArgument(op == Operator.BITNOT && e2 == null
        || op != Operator.BITNOT && e2 != null);
    if (e2 != null) {
      children.add(e2);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((ArithmeticExpr) obj).op == op;
  }

  // TODO: this only determines the type; we also need to insert operand casts
  // when their types aren't identical
  @Override
  public void analyze(Analyzer analyzer) throws Analyzer.Exception {
    super.analyze(analyzer);
    for (Expr child: children) {
      Expr operand = (Expr) child;
      if (!operand.type.isNumericType()) {
        throw new Analyzer.Exception("Arithmetic operation requires numeric operands: " + toSql());
      }
    }

    if (op.isBitwiseOperation()) {
      // TODO: this is what mysql does; check whether Hive has the same semantics
      type = PrimitiveType.BIGINT;
      return;
    }

    PrimitiveType t1 = getChild(0).getType();
    PrimitiveType t2 = getChild(1).getType();  // only bitnot is unary
    switch (op) {
      case MULTIPLY:
      case MOD:
      case PLUS:
      case MINUS:
        type = PrimitiveType.getAssignmentCompatibleType(t1, t2);
        return;
      case DIVIDE:
        // FLOAT in some cases?
        type = PrimitiveType.DOUBLE;
        return;
      case INT_DIVIDE:
        // the result is always an integer
        if (PrimitiveType.getAssignmentCompatibleType(t1, t2) == PrimitiveType.DOUBLE) {
          type = PrimitiveType.BIGINT;
        } else {
          type = PrimitiveType.INT;
        }
        return;
    }
  }
}
