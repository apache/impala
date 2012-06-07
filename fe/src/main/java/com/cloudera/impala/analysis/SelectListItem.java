// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.google.common.base.Preconditions;

class SelectListItem {
  private final Expr expr;
  private String alias;

  // for "[name.]*"
  private final TableName tblName;
  private final boolean isStar;

  public SelectListItem(Expr expr, String alias) {
    super();
    Preconditions.checkNotNull(expr);
    this.expr = expr;
    this.alias = alias;
    this.tblName = null;
    this.isStar = false;
  }

  // select list item corresponding to "[[db.]tbl.]*"
  static public SelectListItem createStarItem(TableName tblName) {
    return new SelectListItem(tblName);
  }

  private SelectListItem(TableName tblName) {
    super();
    this.expr = null;
    this.tblName = tblName;
    this.isStar = true;
  }

  public boolean isStar() {
    return isStar;
  }

  public TableName getTblName() {
    return tblName;
  }

  public Expr getExpr() {
    return expr;
  }

  public String getAlias() {
    return alias;
  }

  public String toSql() {
    if (!isStar) {
      Preconditions.checkNotNull(expr);
      return expr.toSql();
    } else if (tblName != null) {
      return tblName.toString() + ".*";
    } else {
      return "*";
    }
  }

  /**
   * Return a column label for the select list item.
   */
  public String toColumnLabel() {
    Preconditions.checkState(!isStar());
    return expr.toColumnLabel();
  }

}
