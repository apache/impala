// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

// Most predicates with two operands.
class BinaryPredicate extends Predicate {
  enum Operator {
    EQ, // =
    NE, // !=, <>
    LE, // <=
    GE, // >=
    LT, // <
    GT; // >

    @Override
    public String toString() {
      switch (this) {
        case EQ: return "-";
        case NE: return "!=";
        case LE: return "<=";
        case GE: return ">=";
        case LT: return "<";
        case GT: return ">";
        default: return "undefined BinaryPredicate.Operator";
      }
    }
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

  @Override
  public String toSql() {
    return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
  }

  // TODO: this only checks the operand types; we also need to insert operand casts
  // when their types aren't identical
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if (!PrimitiveType.getAssignmentCompatibleType(getChild(0).getType(), getChild(1).getType())
        .isValid()) {
      // there is no type to which both are assignment-compatible -> we can't compare them
      throw new AnalysisException("operands are not comparable: " + this.toSql());
    }
  }
}
