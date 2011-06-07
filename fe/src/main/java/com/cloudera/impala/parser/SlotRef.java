// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;


class SlotRef extends Expr {
  private final TableName tblName;
  private final String col;

  // results of analysis
  private SlotId id;

  public SlotRef(TableName tblName, String col) {
    super();
    this.tblName = tblName;
    this.col = col;
  }

  // C'tor for a "pre-analyzed" ref to slot that doesn't correspond to
  // a table's column.
  public SlotRef(SlotDescriptor slotD) {
    super();
    this.tblName = null;
    this.col = null;
    this.id = slotD.getId();
    this.type = slotD.getType();
  }

  @Override
  public void analyze(Analyzer analyzer) throws Analyzer.Exception {
    SlotDescriptor slotD = analyzer.registerColumnRef(tblName, col);
    id = slotD.getId();
    type = slotD.getType();
  }

  @Override
  public String toSql() {
    return tblName.toString() + "." + col;
  }
}
