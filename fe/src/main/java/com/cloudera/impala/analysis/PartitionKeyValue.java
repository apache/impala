// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

/**
 * Representation of a single column:value element in the PARTITION (...) clause of an insert
 * statement.
 */
public class PartitionKeyValue {
  // Name of partitioning column.
  private final String colName;
  // Value of partitioning column. Set to null for dynamic inserts.
  private final LiteralExpr value;

  public PartitionKeyValue(String colName, LiteralExpr value) {
    this.colName = colName.toLowerCase();
    this.value = value;
  }

  public String getColName() {
    return colName;
  }

  public LiteralExpr getValue() {
    return value;
  }

  public boolean isDynamic() {
    return value == null;
  }

  public boolean isStatic() {
    return !isDynamic();
  }
}
