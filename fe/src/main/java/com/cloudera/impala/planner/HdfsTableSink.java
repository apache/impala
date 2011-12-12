// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.Table;

/**
 * Base class for Hdfs data sinks such as HdfsTextTableSink.
 *
 */
public abstract class HdfsTableSink extends TableSink {
  // Exprs for computing the output partition(s).
  protected final List<Expr> partitionKeyExprs;
  // Whether to overwrite the existing partition(s).
  protected final boolean overwrite;

  public HdfsTableSink(Table targetTable, List<Expr> partitionKeyExprs, boolean overwrite) {
    super(targetTable);
    this.partitionKeyExprs = partitionKeyExprs;
    this.overwrite = overwrite;
  }
}
