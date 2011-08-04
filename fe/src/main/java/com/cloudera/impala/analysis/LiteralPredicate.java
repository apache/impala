// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TLiteralPredicate;

public class LiteralPredicate extends Predicate {
  private final boolean value;

  static public LiteralPredicate True() {
    return new LiteralPredicate(true);
  }

  static public LiteralPredicate False() {
    return new LiteralPredicate(false);
  }

  private LiteralPredicate(boolean val) {
    super();
    this.value = val;
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
    return (value ? "TRUE" : "FALSE");
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.LITERAL_PRED;
    msg.literal_pred = new TLiteralPredicate(value);
  }

}
