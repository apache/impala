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
import java.util.List;

import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAccessEvent;
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
  private final User user;

  public AnalysisContext(Catalog catalog, String defaultDb, User user) {
    this.catalog = catalog;
    this.defaultDatabase = defaultDb;
    this.user = user;
  }

  static public class AnalysisResult {
    private StatementBase stmt;
    private Analyzer analyzer;

    public boolean isAlterTableStmt() {
      return stmt instanceof AlterTableStmt;
    }

    public boolean isAlterViewStmt() {
      return stmt instanceof AlterViewStmt;
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

    public boolean isDropTableOrViewStmt() {
      return stmt instanceof DropTableOrViewStmt;
    }

    public boolean isCreateTableLikeStmt() {
      return stmt instanceof CreateTableLikeStmt;
    }

    public boolean isCreateViewStmt() {
      return stmt instanceof CreateViewStmt;
    }

    public boolean isCreateTableAsSelectStmt() {
      return stmt instanceof CreateTableAsSelectStmt;
    }

    public boolean isCreateTableStmt() {
      return stmt instanceof CreateTableStmt;
    }

    public boolean isCreateDbStmt() {
      return stmt instanceof CreateDbStmt;
    }

    public boolean isLoadDataStmt() {
      return stmt instanceof LoadDataStmt;
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

    public boolean isResetMetadataStmt() {
      return stmt instanceof ResetMetadataStmt;
    }

    public boolean isExplainStmt() {
      if (isQueryStmt()) return ((QueryStmt)stmt).isExplain();
      if (isInsertStmt()) return ((InsertStmt)stmt).isExplain();
      return false;
    }

    public boolean isDdlStmt() {
      return isUseStmt() || isShowTablesStmt() || isShowDbsStmt() || isDescribeStmt() ||
          isCreateTableLikeStmt() || isCreateTableStmt() || isCreateViewStmt() ||
          isCreateDbStmt() || isDropDbStmt() || isDropTableOrViewStmt() ||
          isResetMetadataStmt() || isAlterTableStmt() || isAlterViewStmt() ||
          isCreateTableAsSelectStmt();
    }

    public boolean isDmlStmt() {
      return isInsertStmt();
    }

    public AlterTableStmt getAlterTableStmt() {
      Preconditions.checkState(isAlterTableStmt());
      return (AlterTableStmt) stmt;
    }

    public AlterViewStmt getAlterViewStmt() {
      Preconditions.checkState(isAlterViewStmt());
      return (AlterViewStmt) stmt;
    }

    public CreateTableLikeStmt getCreateTableLikeStmt() {
      Preconditions.checkState(isCreateTableLikeStmt());
      return (CreateTableLikeStmt) stmt;
    }

    public CreateViewStmt getCreateViewStmt() {
      Preconditions.checkState(isCreateViewStmt());
      return (CreateViewStmt) stmt;
    }

    public CreateTableAsSelectStmt getCreateTableAsSelectStmt() {
      Preconditions.checkState(isCreateTableAsSelectStmt());
      return (CreateTableAsSelectStmt) stmt;
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

    public DropTableOrViewStmt getDropTableOrViewStmt() {
      Preconditions.checkState(isDropTableOrViewStmt());
      return (DropTableOrViewStmt) stmt;
    }

    public LoadDataStmt getLoadDataStmt() {
      Preconditions.checkState(isLoadDataStmt());
      return (LoadDataStmt) stmt;
    }

    public QueryStmt getQueryStmt() {
      Preconditions.checkState(isQueryStmt());
      return (QueryStmt) stmt;
    }

    public InsertStmt getInsertStmt() {
      if (isCreateTableAsSelectStmt()) {
        return getCreateTableAsSelectStmt().getInsertStmt();
      } else {
        Preconditions.checkState(isInsertStmt());
        return (InsertStmt) stmt;
      }
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

    public StatementBase getStmt() {
      return stmt;
    }

    public Analyzer getAnalyzer() {
      return analyzer;
    }

    public List<TAccessEvent> getAccessEvents() {
      return analyzer.getAccessEvents();
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
  public AnalysisResult analyze(String stmt) throws AnalysisException,
      AuthorizationException {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    try {
      AnalysisResult result = new AnalysisResult();
      result.stmt = (StatementBase) parser.parse().value;
      if (result.stmt == null) {
        return null;
      }
      result.analyzer = new Analyzer(catalog, defaultDatabase, user);
      result.stmt.analyze(result.analyzer);
      return result;
    } catch (AnalysisException e) {
      throw e;
    } catch (AuthorizationException e) {
      throw e;
    } catch (Exception e) {
      throw new AnalysisException(parser.getErrorMsg(stmt), e);
    }
  }
}
