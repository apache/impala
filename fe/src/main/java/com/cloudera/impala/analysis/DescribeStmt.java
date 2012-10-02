// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;

/**
 * Representation of a DESCRIBE table statement. 
 */
public class DescribeStmt extends ParseNodeBase {
  private TableName table;

  public DescribeStmt(TableName table) {
    this.table = table;
  }

  public String toSql() {
    return "DESCRIBE " + table; 
  }

  public TableName getTable() {
    return table;
  }

  public String debugString() {
    return toSql() + table.toString();
  }

  public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
    if (!table.isFullyQualified()) {
      table = new TableName(analyzer.getDefaultDb(), table.getTbl());
    }
  }
}
