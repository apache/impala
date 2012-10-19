// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TLiteralPredicate;

public class LiteralPredicate extends Predicate {
  private final boolean value;
  private final boolean isNull;

  static public LiteralPredicate True() {
    return new LiteralPredicate(true, false);
  }

  static public LiteralPredicate False() {
    return new LiteralPredicate(false, false);
  }

  static public LiteralPredicate Null() {
    return new LiteralPredicate(false, true);
  }

  private LiteralPredicate(boolean val, boolean isNull) {
    super();
    this.value = val;
    this.isNull = isNull;
  }

  public boolean isNull() {
    return isNull;
  }

  public boolean getValue() {
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((LiteralPredicate) obj).value == value;
  }

  @Override
  public String toSql() {
    if (isNull) {
      return "NULL";
    } else {
      return (value ? "TRUE" : "FALSE");
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.LITERAL_PRED;
    msg.literal_pred = new TLiteralPredicate(value, isNull);
  }

}
