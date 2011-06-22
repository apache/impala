// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class AggregateExpr extends Expr {
  enum Operator {
    COUNT("COUNT"),
    MIN("MIN"),
    MAX("MAX"),
    SUM("SUM"),
    AVG("AVG");

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

  public Operator getOp() {
    return op;
  }

  public boolean isStar() {
    return isStar;
  }

  public boolean isDistinct() {
    return isDistinct;
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
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("op", op)
        .add("isStar", isStar)
        .add("isDistinct", isDistinct)
        .addValue(super.debugString())
        .toString();
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder(op.toString());
    sb.append("(");
    if (isStar) {
      sb.append("*");
    }
    if (isDistinct) {
      sb.append("DISTINCT ");
    }
    for (int i = 0; i < children.size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(getChild(i).toSql());
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if (isStar && op != Operator.COUNT) {
      throw new AnalysisException(
          "'*' can only be used in conjunction with COUNT: "
          + this.toSql());
    }
    // subexprs must not contain aggregates
    for (Expr child: children) {
      if (child.contains(AggregateExpr.class)) {
        throw new AnalysisException(
            "aggregate function cannot contain aggregate parameters: " + this.toSql());
      }
    }

    if (op == Operator.COUNT) {
      type = PrimitiveType.BIGINT;
      return;
    }

    // only COUNT can contain multiple exprs
    if (children.size() != 1) {
      throw new AnalysisException(
          op.toString() + " requires exactly one parameter: " + this.toSql());
    }

    // determine type
    Expr arg = (Expr) getChild(0);

    // SUM and AVG cannot be applied to non-numeric types
    if ((op == Operator.AVG || op == Operator.SUM) && !arg.type.isNumericType()) {
      throw new AnalysisException(
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
