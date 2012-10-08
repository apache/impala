// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;

/**
 * Representation of a SHOW DATABASES [pattern] statement. 
 * Acceptable syntax:
 *
 * SHOW DATABASES
 * SHOW SCHEMAS
 * SHOW DATABASES LIKE 'pattern'
 * SHOW SCHEMAS LIKE 'pattern'
 *
 */
public class ShowDbsStmt extends ParseNodeBase {
  // Pattern to match tables against. | denotes choice, * matches all strings
  private final String pattern;

  /**
   * Default constructor, which creates a show statement which returns all
   * databases.
   */
  public ShowDbsStmt() {
    this(null);
  }

  /**
   * Constructs a show statement which matches all databases against the
   * supplied pattern.
   */
  public ShowDbsStmt(String pattern) {
    this.pattern = pattern;
  }

  public String getPattern() {
    return pattern;
  }

  public String toSql() {
    if (pattern == null) {
        return "SHOW DATABASES";
    } else {
        return "SHOW DATABASES LIKE '" + pattern + "'";
    }
  }

  public String debugString() {
    return toSql();
  }

  public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
    // Nothing to do here
  }
}
