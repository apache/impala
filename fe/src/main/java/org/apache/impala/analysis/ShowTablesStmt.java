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

import org.apache.impala.authorization.Privilege;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TShowTablesParams;

import com.google.common.base.Preconditions;

/**
 * Representation of a SHOW TABLES [pattern] statement.
 * Acceptable syntax:
 *
 * SHOW TABLES
 * SHOW TABLES "pattern"
 * SHOW TABLES LIKE "pattern"
 * SHOW TABLES IN database
 * SHOW TABLES IN database "pattern"
 * SHOW TABLES IN database LIKE "pattern"
 *
 * In Hive, the 'LIKE' is optional. Also SHOW TABLES unquotedpattern is accepted
 * by the parser but returns no results. We don't support that syntax.
 */
public class ShowTablesStmt extends StatementBase {
  // Pattern to match tables against. | denotes choice, * matches all strings
  private final String pattern_;

  // DB (if any) as seen by the parser
  private final String parsedDb_;

  // Set during analysis
  private String postAnalysisDb_;

  /**
   * Default constructor, which creates a show statement with the default
   * database and no pattern (which returns all tables in the default database).
   */
  public ShowTablesStmt() {
    this(null, null);
  }

  /**
   * Constructs a show statement against the default database using the supplied
   * pattern.
   */
  public ShowTablesStmt(String pattern) {
    this(null, pattern);
  }

  /**
   * General purpose constructor which builds a show statement that matches
   * table names against a given pattern in the supplied database.
   *
   * If pattern is null, all tables in the supplied database match.
   * If database is null, the default database is searched.
   */
  public ShowTablesStmt(String database, String pattern) {
    this.parsedDb_ = database;
    this.pattern_ = pattern;
    this.postAnalysisDb_ = null;
  }

  public String getPattern() { return pattern_; }

  /**
   * Can only be called after analysis, returns the name of the database that
   * this show will search against.
   */
  public String getDb() {
    Preconditions.checkNotNull(postAnalysisDb_);
    return postAnalysisDb_;
  }

  @Override
  public String toSql() {
    if (pattern_ == null) {
      if (parsedDb_ == null) {
        return "SHOW TABLES";
      } else {
        return "SHOW TABLES IN " + parsedDb_;
      }
    } else {
      if (parsedDb_ == null) {
        return "SHOW TABLES LIKE '" + pattern_ + "'";
      } else {
        return "SHOW TABLES IN " + parsedDb_ + " LIKE '" + pattern_ + "'";
      }
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    postAnalysisDb_ = (parsedDb_ == null ? analyzer.getDefaultDb() : parsedDb_);
    if (analyzer.getDb(postAnalysisDb_, Privilege.ANY) == null) {
      throw new AnalysisException(Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + postAnalysisDb_);
    }
  }

  public TShowTablesParams toThrift() {
    TShowTablesParams params = new TShowTablesParams();
    params.setShow_pattern(getPattern());
    params.setDb(getDb());
    return params;
  }
}
