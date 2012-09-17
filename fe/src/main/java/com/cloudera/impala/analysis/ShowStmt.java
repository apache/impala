// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;

/**
 * Representation of a SHOW TABLES [pattern] statement. 
 */
public class ShowStmt extends ParseNodeBase {
  // Pattern to match tables against. | denotes choice, * matches all strings
  private final String pattern;

  public ShowStmt() {
    pattern = null;
  }

  public ShowStmt(String pattern) {
    this.pattern = pattern;
  }

  public String getPattern() {
    return pattern;
  }

  public String toSql() {
    if (pattern == null) {
      return "SHOW TABLES";
    } else {
      return "SHOW TABLES \"" + pattern + "\"";
    }
  }

  public String debugString() {
    return toSql();
  }

  public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
    // Nothing to do for analysis
  }
}
