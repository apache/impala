// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.catalog.Table;

/**
 * A DataSink that writes into a table.
 *
 */
public abstract class TableSink extends DataSink {
  // Table which is to be populated by this sink.
  protected final Table targetTable;

  public TableSink(Table targetTable) {
    this.targetTable = targetTable;
  }
}
