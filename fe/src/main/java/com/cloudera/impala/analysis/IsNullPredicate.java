// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TIsNullPredicate;
import com.google.common.base.Preconditions;

public class IsNullPredicate extends Predicate {
  private final boolean isNotNull;

  public IsNullPredicate(Expr e, boolean isNotNull) {
    super();
    this.isNotNull = isNotNull;
    Preconditions.checkNotNull(e);
    children.add(e);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((IsNullPredicate) obj).isNotNull == isNotNull;
  }

  @Override
  public String toSql() {
    return getChild(0).toSql() + (isNotNull ? " IS NOT NULL" : " IS NULL");
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.IS_NULL_PRED;
    msg.is_null_pred = new TIsNullPredicate(isNotNull);
  }

}
