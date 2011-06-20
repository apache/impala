// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.catalog.Table;
import com.google.common.base.Objects;

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

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("tblName", tbl.getFullName())
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "SCAN table=" + tbl.getFullName() + "\n");
    output.append(prefix + "  PREDICATES: " + getExplainString(conjuncts));
    return output.toString();
  }
}
