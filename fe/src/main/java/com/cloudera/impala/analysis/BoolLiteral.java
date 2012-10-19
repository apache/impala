// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TBoolLiteral;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;

public class BoolLiteral extends LiteralExpr {
  private final boolean value;

  public BoolLiteral(boolean value) {
    this.value = value;
    type = PrimitiveType.BOOLEAN;
  }

  public BoolLiteral(String value) throws AnalysisException {
    this.type = PrimitiveType.BOOLEAN;
    if (value.toLowerCase().equals("true")) {
      this.value = true;
    } else if (value.toLowerCase().equals("false")) {
      this.value = false;
    } else {
      throw new AnalysisException("invalid BOOLEAN literal: " + value);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((BoolLiteral) obj).value == value;
  }

  public boolean getValue() {
    return value;
  }

  @Override
  public String toSql() {
    return value ? "TRUE" : "FALSE";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.BOOL_LITERAL;
    msg.bool_literal = new TBoolLiteral(value);
  }
}
