// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TExprOperator;
import com.google.common.base.Preconditions;

public class LikePredicate extends Predicate {
  enum Operator {
    LIKE("LIKE", TExprOperator.LIKE),
    RLIKE("RLIKE", TExprOperator.LIKE),
    REGEXP("REGEXP", TExprOperator.LIKE);

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

  public LikePredicate(Operator op, Expr e1, Expr e2) {
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
    return ((LikePredicate) obj).op == op;
  }

  @Override
  public String toSql() {
    return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.LIKE_PRED;
    msg.op = op.toThrift();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if (getChild(0).getType() != PrimitiveType.STRING) {
      throw new AnalysisException(
          "left operand of " + op.toString() + " must be of type STRING: " + this.toSql());
    }
    if (getChild(1).getType() != PrimitiveType.STRING) {
      throw new AnalysisException(
          "right operand of " + op.toString() + " must be of type STRING: " + this.toSql());
    }
  }
}
