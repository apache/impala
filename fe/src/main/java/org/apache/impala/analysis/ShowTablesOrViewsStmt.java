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

public abstract class ShowTablesOrViewsStmt extends StatementBase {
  // Pattern to match tables or views against. | denotes choice, * matches all strings
  private final String pattern_;

  // DB (if any) as seen by the parser
  private final String parsedDb_;

  // Set during analysis
  private String postAnalysisDb_;

  /**
   * Default constructor, which creates a show statement with the default
   * database and no pattern which returns either a) all tables and views, or b) all
   * views in the default database depending on the instantiation (ShowTablesStmt v.s.
   * ShowViewsStmt).
   */
  public ShowTablesOrViewsStmt() {
    this(null, null);
  }

  /**
   * Constructs a show statement against the default database using the supplied
   * pattern.
   */
  public ShowTablesOrViewsStmt(String pattern) {
    this(null, pattern);
  }

  /**
   * General purpose constructor which builds a show statement that matches
   * either a) all table and view names against a given pattern, or b) all view names in
   * the supplied database depending on the instantiation (ShowTablesStmt v.s.
   * ShowViewsStmt).
   *
   * If pattern is null, all tables in the supplied database match.
   * If database is null, the default database is searched.
   */
  public ShowTablesOrViewsStmt(String database, String pattern) {
    this.parsedDb_ = database;
    this.pattern_ = pattern;
    this.postAnalysisDb_ = null;
  }

  public String getParsedDb() { return parsedDb_; }
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
