// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

import java.lang.String;
import java.util.ArrayList;

class TableRef {
  private TableName name;
  private String alias;

  public TableRef(TableName name, String alias) {
    super();
    this.name = name;
    this.alias = alias;
  }

  public void setJoinOperator(JoinOperator op) {
  }

  public void setOnClause(Predicate pred) {
  }

  public void setUsingClause(ArrayList<String> colNames) {
  }
}
