// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

/**
 * Representation of a single column:value element in the PARTITION (...) clause of an insert
 * statement.
 */
public class PartitionListItem {
  // Name of partitioning column.
  private final String colName;
  // Value of partitioning column. Set to null for dynamic inserts.
  private final LiteralExpr value;

  public PartitionListItem(String colName, LiteralExpr value) {
    this.colName = colName;
    this.value = value;
  }

  public String getColName() {
    return colName;
  }

  public LiteralExpr getValue() {
    return value;
  }
}
