// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.List;

import com.cloudera.impala.catalog.Table;
import com.google.common.base.Preconditions;

public class TableRef {
  private final TableName name;
  private final String alias;
  private TupleDescriptor desc;  // analysis output

  public TableRef(TableName name, String alias) {
    super();
    Preconditions.checkArgument(!name.toString().isEmpty());
    this.name = name;
    Preconditions.checkArgument(alias == null || !alias.isEmpty());
    this.alias = alias;
  }

  public TupleDescriptor getDesc() {
    return desc;
  }

  public void setDesc(TupleDescriptor desc) {
    this.desc = desc;
  }

  public TableName getName() {
    return name;
  }

  public String getExplicitAlias() {
    return alias;
  }

  public Table getTable() {
    return desc.getTable();
  }

  public void setJoinOperator(JoinOperator op) {
  }

  public void setOnClause(Predicate pred) {
  }

  public void setUsingClause(List<String> colNames) {
  }

  // Return alias by which this table is referenced in select block.
  public String getAlias() {
    if (alias == null) {
      return name.toString().toLowerCase();
    } else {
      return alias;
    }
  }

}
