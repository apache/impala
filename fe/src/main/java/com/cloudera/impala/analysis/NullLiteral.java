// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;

public class NullLiteral extends LiteralExpr {
  public NullLiteral() {
    // TODO: should NULL be a type?
    type = PrimitiveType.BOOLEAN;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return obj instanceof NullLiteral;
  }

  @Override
  public String toSql() {
    return "NULL";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.NULL_LITERAL;
  }
}
