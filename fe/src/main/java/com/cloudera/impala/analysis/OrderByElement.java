// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;


/**
 * Combination of expr and ASC/DESC.
 *
 */
class OrderByElement {
  private final Expr expr;
  private final boolean isAsc;

  public OrderByElement(Expr expr, boolean isAsc) {
    super();
    this.expr = expr;
    this.isAsc = isAsc;
  }

  public Expr getExpr() {
    return expr;
  }

  public boolean getIsAsc() {
    return isAsc;
  }
}
