// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.catalog.Table;

/**
 * Scan of a single single table. Currently limited to full-table scans.
 * TODO: pass in range restrictions.
 *
 */
public class ScanNode extends PlanNode {
  private final Table tbl;

  public ScanNode(Table tbl) {
    this.tbl = tbl;
  }
}
