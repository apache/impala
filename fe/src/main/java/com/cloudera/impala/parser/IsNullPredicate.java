// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.google.common.base.Preconditions;

class IsNullPredicate extends Predicate {
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
}
