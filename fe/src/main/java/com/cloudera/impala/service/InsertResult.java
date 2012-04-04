// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.util.ArrayList;
import java.util.List;

/**
 * The result of an insert query.
 */
public class InsertResult {
  // List of partitions (Hdfs folders) that were modified by an insert query.
  // Doesn't apply to, e.g., HBase tables.
  private final List<String> modifiedPartitions = new ArrayList<String>();

  // Number of appended rows per modified partition.
  private final List<Long> rowsAppended = new ArrayList<Long>();

  public void addModifiedPartition(String partition) {
    modifiedPartitions.add(partition);
  }

  public void addToRowsAppended(Long numRows) {
    rowsAppended.add(numRows);
  }

  public List<String> getModifiedPartitions() {
    return modifiedPartitions;
  }

  public List<Long> getRowsAppended() {
    return rowsAppended;
  }

  public void clear() {
    modifiedPartitions.clear();
    rowsAppended.clear();
  }
}
