// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.catalog.Table;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;

/**
 * Scan of a single single table. Currently limited to full-table scans.
 * TODO: pass in range restrictions.
 */
public class ScanNode extends PlanNode {
  private final Table tbl;
  private final List<String> filePaths;  // data files to scan

  /**
   * Constructs node to scan given data files of table 'tbl'.
   * @param tbl
   * @param filePaths
   */
  public ScanNode(Table tbl, List<String> filePaths) {
    this.tbl = tbl;
    this.filePaths = filePaths;
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("tblName", tbl.getFullName())
        .add("filePaths", Joiner.on(", ").join(filePaths))
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "SCAN table=" + tbl.getFullName() + "\n");
    output.append(prefix + "  PREDICATES: " + getExplainString(conjuncts) + "\n");
    output.append(prefix + "  FILES: " + Joiner.on(", ").join(filePaths));
    return output.toString();
  }
}
