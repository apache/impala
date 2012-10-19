// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAggregateExpr;
import com.cloudera.impala.thrift.TAggregationOp;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class AggregateExpr extends Expr {
  public enum Operator {
    COUNT("COUNT", TAggregationOp.COUNT, false),
    MIN("MIN", TAggregationOp.MIN, false),
    MAX("MAX", TAggregationOp.MAX, false),
    DISTINCT_PC("DISTINC_PC", TAggregationOp.DISTINCT_PC, true),
    MERGE_PC("MERGE_PC", TAggregationOp.MERGE_PC, true),
    DISTINCT_PCSA("DISTINC_PCSA", TAggregationOp.DISTINCT_PCSA,true),
    MERGE_PCSA("MERGE_PCSA", TAggregationOp.MERGE_PCSA, true),
    SUM("SUM", TAggregationOp.SUM, false),
    AVG("AVG", TAggregationOp.INVALID, false);

    private final String description;
    private final TAggregationOp thriftOp;
    private final boolean needFinalize;

    private Operator(String description, TAggregationOp thriftOp, boolean needFinalize) {
      this.description = description;
      this.thriftOp = thriftOp;
      this.needFinalize = needFinalize;
    }

    @Override
    public String toString() {
      return description;
    }

    public TAggregationOp toThrift() {
      return thriftOp;
    }

    public boolean getNeedFinalize() {
      return needFinalize;
    }
  }
  private final Operator op;
  private final boolean isStar;
  private boolean isDistinct;

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
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.AGG_EXPR;
    msg.agg_expr = new TAggregateExpr(isStar, isDistinct, op.toThrift());
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
    if (op == Operator.SUM && !arg.type.isNumericType()) {
        throw new AnalysisException(
                      "SUM requires a numeric parameter: " + this.toSql());
    }
    if (op == Operator.AVG &&
        (!arg.type.isNumericType() && arg.type != PrimitiveType.TIMESTAMP)) {
      throw new AnalysisException(
                    "AVG requires a numeric or timestamp parameter: " + this.toSql());
    }

    if ((op == Operator.MERGE_PC || op == Operator.MERGE_PCSA)
        && !arg.type.isStringType()) {
      Preconditions.checkState(false,
          "MERGEPC(SA) expects string type input but gets " +
          arg.type.toString());
    }

    if (op == Operator.DISTINCT_PC ||
        op == Operator.MERGE_PC ||
        op == Operator.DISTINCT_PCSA ||
        op == Operator.MERGE_PCSA) {
      // Distinct/Merge Estimate is a string type.
      // Although the number of distinct value is a number, we're using string as return
      // type. This is because the distinct estimation algorithm uses a bitmap as an
      // intermediate working context, which can fit nicely in a string. Also we need
      // string so intermediate from different nodes are sent as part of the
      // thrift row batch.
      type = PrimitiveType.STRING;
      return;
    }

    if (op == Operator.AVG) {
      // division always results in a double value
      type = PrimitiveType.DOUBLE;
      return;
    } else if (op == Operator.SUM) {
      // numeric types need to be accumulated at maximum precision
      type = arg.type.getMaxResolutionType();
      if (arg.type != type) {
        castChild(type, 0);
      }
    } else if (op == Operator.MIN || op == Operator.MAX) {
      type = arg.type;
      isDistinct = false;  // DISTINCT is meaningless here
    }
  }
}
