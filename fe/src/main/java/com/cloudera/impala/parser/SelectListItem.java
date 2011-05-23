// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

import java.lang.String;

class SelectListItem {
  private Expr expr;
  private String alias;

  // for "[name.]*"
  private TableName tblName;
  private boolean isStar;

  public SelectListItem(Expr expr, String alias) {
    super();
    this.expr = expr;
    this.alias = alias;
    this.isStar = false;
  }

  // select list item corresponding to "[[db.]tbl.]*"
  static public SelectListItem createStarItem(TableName tblName) {
    return new SelectListItem(tblName);
  }

  private SelectListItem(TableName tblName) {
    super();
    this.tblName = tblName;
    this.isStar = true;
  }

  public boolean isStar() { return isStar; }
  public TableName tblName() { return tblName; }
  public Expr expr() { return expr; }
  public String alias() { return alias; }
}
