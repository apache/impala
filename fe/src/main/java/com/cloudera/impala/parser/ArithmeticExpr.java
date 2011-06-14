// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

public class ArithmeticExpr extends Expr {
  enum Operator {
    MULTIPLY("*"),
    DIVIDE("/"),
    MOD("%"),
    INT_DIVIDE("DIV"),
    PLUS("+"),
    MINUS("-"),
    BITAND("&"),
    BITOR("|"),
    BITXOR("^"),
    BITNOT("~");

    private final String description;

    private Operator(String description) {
      this.description = description;
    }

    public boolean isBitwiseOperation() {
      return this == BITAND || this == BITOR || this == BITXOR || this == BITNOT;
    }

    @Override
    public String toString() {
      return description;
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
  public String toSql() {
    if (children.size() == 1) {
      return op.toString() + " " + getChild(0).toSql();
    } else {
      Preconditions.checkState(children.size() == 2);
      return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
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
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    for (Expr child: children) {
      Expr operand = (Expr) child;
      if (!operand.type.isNumericType()) {
        throw new AnalysisException("Arithmetic operation requires numeric operands: " + toSql());
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
