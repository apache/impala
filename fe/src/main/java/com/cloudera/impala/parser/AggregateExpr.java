// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.google.common.base.Preconditions;

class AggregateExpr extends Expr {
  enum Operator {
    COUNT,
    MIN,
    MAX,
    SUM,
    AVG;

    @Override
    public String toString() {
      switch (this) {
        case COUNT: return "COUNT";
        case MIN: return "MIN";
        case MAX: return "MAX";
        case SUM: return "SUM";
        case AVG: return "AVG";
      }
      return "";
    }
  }
  private final Operator op;
  private final boolean isStar;
  private final boolean isDistinct;

  public AggregateExpr(Operator op, boolean isStar,
                       boolean isDistinct, List<Expr> exprs) {
    super();
    this.op = op;
    // '*' precludes exprs and DISTINCT, which the grammar should catch
    Preconditions.checkArgument(
        !(isStar && (isDistinct || exprs != null)));
    this.isStar = isStar;
    this.isDistinct = isDistinct;
    if (exprs != null) {
      children.addAll(exprs);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    AggregateExpr expr = (AggregateExpr) obj;
    return op == expr.op && isStar == expr.isStar
        && isDistinct == expr.isDistinct;
  }

  @Override
  public void analyze(Analyzer analyzer) throws Analyzer.Exception {
    super.analyze(analyzer);
    if (isStar && op != Operator.COUNT) {
      throw new Analyzer.Exception(
          "'*' can only be used in conjunction with COUNT: "
          + this.toSql());
    }
    // subexprs must not contain aggregates
    for (Expr child: children) {
      if (child.contains(AggregateExpr.class)) {
        throw new Analyzer.Exception(
            "aggregate function cannot contain aggregate parameters: " + this.toSql());
      }
    }

    if (op == Operator.COUNT) {
      type = PrimitiveType.BIGINT;
      return;
    }

    // only COUNT can contain multiple exprs
    if (children.size() != 1) {
      throw new Analyzer.Exception(
          op.toString() + " requires exactly one parameter: " + this.toSql());
    }

    // determine type
    Expr arg = (Expr) getChild(0);

    // SUM and AVG cannot be applied to non-numeric types
    if ((op == Operator.AVG || op == Operator.SUM) && !arg.type.isNumericType()) {
      throw new Analyzer.Exception(
          op.toString() + " requires a numeric parameter: " + this.toSql());
    }

    if (op == Operator.AVG) {
      // division always results in a floating-point value
      // TODO: make it a float if the param type is <bigint?
      type = (arg.type == PrimitiveType.FLOAT ? PrimitiveType.FLOAT : PrimitiveType.DOUBLE);
      return;
    } else {
      // numeric types need to be accumulated at maximum precision
      if (arg.type.isFixedPointType()) {
        type = PrimitiveType.BIGINT;
      } else if (arg.type.isFloatingPointType()) {
        type = PrimitiveType.DOUBLE;
      } else {
        type = arg.type;
      }
    }
  }
}
