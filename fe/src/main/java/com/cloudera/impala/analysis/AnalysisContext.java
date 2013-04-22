// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TQueryGlobals;
import com.google.common.base.Preconditions;

/**
 * Wrapper class for parser and analyzer.
 *
 */
public class AnalysisContext {
  private final Catalog catalog;

  // The name of the database to use if one is not explicitly specified by a query.
  private final String defaultDatabase;

  // The user who initiated the request.
  private final String user;

  private final TQueryGlobals queryGlobals;

  public AnalysisContext(Catalog catalog, String defaultDb, String user) {
    this.catalog = catalog;
    this.defaultDatabase = defaultDb;
    this.user = user;
    this.queryGlobals = createQueryGlobals();
  }

  static public class AnalysisResult {
    private ParseNode stmt;
    private Analyzer analyzer;

    public boolean isAlterTableStmt() {
      return stmt instanceof AlterTableStmt;
    }

    public boolean isQueryStmt() {
      return stmt instanceof QueryStmt;
    }

    public boolean isInsertStmt() {
      return stmt instanceof InsertStmt;
    }

    public boolean isDropDbStmt() {
      return stmt instanceof DropDbStmt;
    }

    public boolean isDropTableStmt() {
      return stmt instanceof DropTableStmt;
    }

    public boolean isCreateTableLikeStmt() {
      return stmt instanceof CreateTableLikeStmt;
    }

    public boolean isCreateTableStmt() {
      return stmt instanceof CreateTableStmt;
    }

    public boolean isCreateDbStmt() {
      return stmt instanceof CreateDbStmt;
    }

    public boolean isUseStmt() {
      return stmt instanceof UseStmt;
    }

    public boolean isShowTablesStmt() {
      return stmt instanceof ShowTablesStmt;
    }

    public boolean isShowDbsStmt() {
      return stmt instanceof ShowDbsStmt;
    }

    public boolean isDescribeStmt() {
      return stmt instanceof DescribeStmt;
    }

    public boolean isDdlStmt() {
      return isUseStmt() || isShowTablesStmt() || isShowDbsStmt() || isDescribeStmt() ||
          isCreateTableLikeStmt() || isCreateTableStmt() || isCreateDbStmt() ||
          isDropDbStmt() || isDropTableStmt() || isAlterTableStmt();
    }

    public boolean isDmlStmt() {
      return isInsertStmt();
    }

    public AlterTableStmt getAlterTableStmt() {
      Preconditions.checkState(isAlterTableStmt());
      return (AlterTableStmt) stmt;
    }

    public CreateTableLikeStmt getCreateTableLikeStmt() {
      Preconditions.checkState(isCreateTableLikeStmt());
      return (CreateTableLikeStmt) stmt;
    }

    public CreateTableStmt getCreateTableStmt() {
      Preconditions.checkState(isCreateTableStmt());
      return (CreateTableStmt) stmt;
    }

    public CreateDbStmt getCreateDbStmt() {
      Preconditions.checkState(isCreateDbStmt());
      return (CreateDbStmt) stmt;
    }

    public DropDbStmt getDropDbStmt() {
      Preconditions.checkState(isDropDbStmt());
      return (DropDbStmt) stmt;
    }

    public DropTableStmt getDropTableStmt() {
      Preconditions.checkState(isDropTableStmt());
      return (DropTableStmt) stmt;
    }

    public QueryStmt getQueryStmt() {
      Preconditions.checkState(isQueryStmt());
      return (QueryStmt) stmt;
    }

    public InsertStmt getInsertStmt() {
      Preconditions.checkState(isInsertStmt());
      return (InsertStmt) stmt;
    }

    public UseStmt getUseStmt() {
      Preconditions.checkState(isUseStmt());
      return (UseStmt) stmt;
    }

    public ShowTablesStmt getShowTablesStmt() {
      Preconditions.checkState(isShowTablesStmt());
      return (ShowTablesStmt) stmt;
    }

    public ShowDbsStmt getShowDbsStmt() {
      Preconditions.checkState(isShowDbsStmt());
      return (ShowDbsStmt) stmt;
    }

    public DescribeStmt getDescribeStmt() {
      Preconditions.checkState(isDescribeStmt());
      return (DescribeStmt) stmt;
    }

    public ParseNode getStmt() {
      return stmt;
    }

    public Analyzer getAnalyzer() {
      return analyzer;
    }
  }

  /**
   * Parse and analyze 'stmt'.
   *
   * @param stmt
   * @return AnalysisResult
   *         containing the analyzer and the analyzed insert or select statement.
   * @throws AnalysisException
   *           on any kind of error, including parsing error.
   */
  public AnalysisResult analyze(String stmt) throws AnalysisException {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    try {
      AnalysisResult result = new AnalysisResult();
      result.stmt = (ParseNode) parser.parse().value;
      if (result.stmt == null) {
        return null;
      }
      result.analyzer = new Analyzer(catalog, defaultDatabase, user, queryGlobals);
      result.stmt.analyze(result.analyzer);
      return result;
    } catch (AnalysisException e) {
      throw e;
    } catch (Exception e) {
      throw new AnalysisException(parser.getErrorMsg(stmt), e);
    }
  }

  public TQueryGlobals getQueryGlobals() { return queryGlobals; }

  /**
   * Create query global parameters to be set in each TPlanExecRequest.
   */
  private TQueryGlobals createQueryGlobals() {
    SimpleDateFormat formatter =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    TQueryGlobals queryGlobals = new TQueryGlobals();
    Calendar currentDate = Calendar.getInstance();
    String nowStr = formatter.format(currentDate.getTime());
    queryGlobals.setNow_string(nowStr);
    return queryGlobals;
  }

}
