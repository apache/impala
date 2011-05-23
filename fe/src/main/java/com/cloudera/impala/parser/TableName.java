// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

import java.lang.String;

class TableName {
  private String db;
  private String tbl;

  public TableName(String db, String tbl) {
    super();
    this.db = db;
    this.tbl = tbl;
  }
}
