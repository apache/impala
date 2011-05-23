// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

import java.lang.String;

class SlotRef extends Expr {
  private String tbl;
  private String col;

  public SlotRef(String tbl, String col) {
    super();
    this.tbl = tbl;
    this.col = col;
  }
}
