// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

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
}
