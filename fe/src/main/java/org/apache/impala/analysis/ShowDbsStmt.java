// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.analysis;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TShowDbsParams;

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
public class ShowDbsStmt extends StatementBase {
  // Pattern to match tables against. | denotes choice, * matches all strings
  private final String pattern_;

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
    this.pattern_ = pattern;
  }

  public String getPattern() { return pattern_; }

  @Override
  public String toSql(ToSqlOptions options) {
    if (pattern_ == null) {
      return "SHOW DATABASES";
    } else {
      return "SHOW DATABASES LIKE '" + pattern_ + "'";
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Nothing to do here
  }

  public TShowDbsParams toThrift() {
    TShowDbsParams params = new TShowDbsParams();
    params.setShow_pattern(getPattern());
    return params;
  }
}
