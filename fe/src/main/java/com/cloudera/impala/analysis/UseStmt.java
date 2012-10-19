// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;

/**
 * Representation of a USE db statement. 
 */
public class UseStmt extends ParseNodeBase {
  private final String database;

  public UseStmt(String db) {
    database = db;
  }

  public String getDatabase() {
    return database;
  }

  public String toSql() {
    return "USE " + database;
  }

  public String debugString() {
    return toSql();
  }

  public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
    // USE is completely ignored for now
  }
}
