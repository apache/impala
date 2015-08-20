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
import java.util.Map;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.AuthorizationChecker;
import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.authorization.AuthorizeableColumn;
import com.cloudera.impala.authorization.AuthorizeableTable;
import com.cloudera.impala.authorization.AuthorizeableDb;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.PrivilegeRequest;
import com.cloudera.impala.authorization.PrivilegeRequestBuilder;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.ImpaladCatalog;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAccessEvent;
import com.cloudera.impala.thrift.TDescribeTableOutputStyle;
import com.cloudera.impala.thrift.TQueryCtx;
import com.cloudera.impala.common.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Wrapper class for parser and analyzer.
 */
public class AnalysisContext {
  private final static Logger LOG = LoggerFactory.getLogger(AnalysisContext.class);
  private final ImpaladCatalog catalog_;
  private final TQueryCtx queryCtx_;
  private final AuthorizationConfig authzConfig_;

  // Set in analyze()
  private AnalysisResult analysisResult_;

  public AnalysisContext(ImpaladCatalog catalog, TQueryCtx queryCtx,
      AuthorizationConfig authzConfig) {
    catalog_ = catalog;
    queryCtx_ = queryCtx;
    authzConfig_ = authzConfig;
  }

  static public class AnalysisResult {
    private StatementBase stmt_;
    private Analyzer analyzer_;
    private CreateTableStmt tmpCreateTableStmt_;

    public boolean isAlterTableStmt() { return stmt_ instanceof AlterTableStmt; }
    public boolean isAlterViewStmt() { return stmt_ instanceof AlterViewStmt; }
    public boolean isComputeStatsStmt() { return stmt_ instanceof ComputeStatsStmt; }
    public boolean isQueryStmt() { return stmt_ instanceof QueryStmt; }
    public boolean isInsertStmt() { return stmt_ instanceof InsertStmt; }
    public boolean isDropDbStmt() { return stmt_ instanceof DropDbStmt; }
    public boolean isDropTableOrViewStmt() {
      return stmt_ instanceof DropTableOrViewStmt;
    }
    public boolean isDropFunctionStmt() { return stmt_ instanceof DropFunctionStmt; }
    public boolean isDropDataSrcStmt() { return stmt_ instanceof DropDataSrcStmt; }
    public boolean isDropStatsStmt() { return stmt_ instanceof DropStatsStmt; }
    public boolean isCreateTableLikeStmt() {
      return stmt_ instanceof CreateTableLikeStmt;
    }
    public boolean isCreateViewStmt() { return stmt_ instanceof CreateViewStmt; }
    public boolean isCreateTableAsSelectStmt() {
      return stmt_ instanceof CreateTableAsSelectStmt;
    }
    public boolean isCreateTableStmt() { return stmt_ instanceof CreateTableStmt; }
    public boolean isCreateDbStmt() { return stmt_ instanceof CreateDbStmt; }
    public boolean isCreateUdfStmt() { return stmt_ instanceof CreateUdfStmt; }
    public boolean isCreateUdaStmt() { return stmt_ instanceof CreateUdaStmt; }
    public boolean isCreateDataSrcStmt() { return stmt_ instanceof CreateDataSrcStmt; }
    public boolean isLoadDataStmt() { return stmt_ instanceof LoadDataStmt; }
    public boolean isUseStmt() { return stmt_ instanceof UseStmt; }
    public boolean isSetStmt() { return stmt_ instanceof SetStmt; }
    public boolean isShowTablesStmt() { return stmt_ instanceof ShowTablesStmt; }
    public boolean isShowDbsStmt() { return stmt_ instanceof ShowDbsStmt; }
    public boolean isShowDataSrcsStmt() { return stmt_ instanceof ShowDataSrcsStmt; }
    public boolean isShowStatsStmt() { return stmt_ instanceof ShowStatsStmt; }
    public boolean isShowFunctionsStmt() { return stmt_ instanceof ShowFunctionsStmt; }
    public boolean isShowCreateTableStmt() {
      return stmt_ instanceof ShowCreateTableStmt;
    }
    public boolean isShowFilesStmt() { return stmt_ instanceof ShowFilesStmt; }
    public boolean isDescribeStmt() { return stmt_ instanceof DescribeStmt; }
    public boolean isResetMetadataStmt() { return stmt_ instanceof ResetMetadataStmt; }
    public boolean isExplainStmt() { return stmt_.isExplain(); }
    public boolean isShowRolesStmt() { return stmt_ instanceof ShowRolesStmt; }
    public boolean isShowGrantRoleStmt() { return stmt_ instanceof ShowGrantRoleStmt; }
    public boolean isCreateDropRoleStmt() { return stmt_ instanceof CreateDropRoleStmt; }
    public boolean isGrantRevokeRoleStmt() {
      return stmt_ instanceof GrantRevokeRoleStmt;
    }
    public boolean isGrantRevokePrivStmt() {
      return stmt_ instanceof GrantRevokePrivStmt;
    }
    public boolean isTruncateStmt() { return stmt_ instanceof TruncateStmt; }

    public boolean isCatalogOp() {
      return isUseStmt() || isViewMetadataStmt() || isDdlStmt();
    }

    private boolean isDdlStmt() {
      return isCreateTableLikeStmt() || isCreateTableStmt() ||
          isCreateViewStmt() || isCreateDbStmt() || isDropDbStmt() ||
          isDropTableOrViewStmt() || isResetMetadataStmt() || isAlterTableStmt() ||
          isAlterViewStmt() || isComputeStatsStmt() || isCreateUdfStmt() ||
          isCreateUdaStmt() || isDropFunctionStmt() || isCreateTableAsSelectStmt() ||
          isCreateDataSrcStmt() || isDropDataSrcStmt() || isDropStatsStmt() ||
          isCreateDropRoleStmt() || isGrantRevokeStmt() || isTruncateStmt();
    }

    private boolean isViewMetadataStmt() {
      return isShowFilesStmt() || isShowTablesStmt() || isShowDbsStmt() ||
          isShowFunctionsStmt() || isShowRolesStmt() || isShowGrantRoleStmt() ||
          isShowCreateTableStmt() || isShowDataSrcsStmt() || isShowStatsStmt() ||
          isDescribeStmt();
    }

    private boolean isGrantRevokeStmt() {
      return isGrantRevokeRoleStmt() || isGrantRevokePrivStmt();
    }

    public boolean isDmlStmt() {
      return isInsertStmt();
    }

    public AlterTableStmt getAlterTableStmt() {
      Preconditions.checkState(isAlterTableStmt());
      return (AlterTableStmt) stmt_;
    }

    public AlterViewStmt getAlterViewStmt() {
      Preconditions.checkState(isAlterViewStmt());
      return (AlterViewStmt) stmt_;
    }

    public ComputeStatsStmt getComputeStatsStmt() {
      Preconditions.checkState(isComputeStatsStmt());
      return (ComputeStatsStmt) stmt_;
    }

    public CreateTableLikeStmt getCreateTableLikeStmt() {
      Preconditions.checkState(isCreateTableLikeStmt());
      return (CreateTableLikeStmt) stmt_;
    }

    public CreateViewStmt getCreateViewStmt() {
      Preconditions.checkState(isCreateViewStmt());
      return (CreateViewStmt) stmt_;
    }

    public CreateTableAsSelectStmt getCreateTableAsSelectStmt() {
      Preconditions.checkState(isCreateTableAsSelectStmt());
      return (CreateTableAsSelectStmt) stmt_;
    }

    public CreateTableStmt getCreateTableStmt() {
      Preconditions.checkState(isCreateTableStmt());
      return (CreateTableStmt) stmt_;
    }

    public CreateTableStmt getTmpCreateTableStmt() {
      return tmpCreateTableStmt_;
    }

    public CreateDbStmt getCreateDbStmt() {
      Preconditions.checkState(isCreateDbStmt());
      return (CreateDbStmt) stmt_;
    }

    public CreateUdfStmt getCreateUdfStmt() {
      Preconditions.checkState(isCreateUdfStmt());
      return (CreateUdfStmt) stmt_;
    }

    public CreateUdaStmt getCreateUdaStmt() {
      Preconditions.checkState(isCreateUdfStmt());
      return (CreateUdaStmt) stmt_;
    }

    public DropDbStmt getDropDbStmt() {
      Preconditions.checkState(isDropDbStmt());
      return (DropDbStmt) stmt_;
    }

    public DropTableOrViewStmt getDropTableOrViewStmt() {
      Preconditions.checkState(isDropTableOrViewStmt());
      return (DropTableOrViewStmt) stmt_;
    }

    public TruncateStmt getTruncateStmt() {
      Preconditions.checkState(isTruncateStmt());
      return (TruncateStmt) stmt_;
    }

    public DropFunctionStmt getDropFunctionStmt() {
      Preconditions.checkState(isDropFunctionStmt());
      return (DropFunctionStmt) stmt_;
    }

    public LoadDataStmt getLoadDataStmt() {
      Preconditions.checkState(isLoadDataStmt());
      return (LoadDataStmt) stmt_;
    }

    public QueryStmt getQueryStmt() {
      Preconditions.checkState(isQueryStmt());
      return (QueryStmt) stmt_;
    }

    public InsertStmt getInsertStmt() {
      if (isCreateTableAsSelectStmt()) {
        return getCreateTableAsSelectStmt().getInsertStmt();
      } else {
        Preconditions.checkState(isInsertStmt());
        return (InsertStmt) stmt_;
      }
    }

    public UseStmt getUseStmt() {
      Preconditions.checkState(isUseStmt());
      return (UseStmt) stmt_;
    }

    public SetStmt getSetStmt() {
      Preconditions.checkState(isSetStmt());
      return (SetStmt) stmt_;
    }

    public ShowTablesStmt getShowTablesStmt() {
      Preconditions.checkState(isShowTablesStmt());
      return (ShowTablesStmt) stmt_;
    }

    public ShowDbsStmt getShowDbsStmt() {
      Preconditions.checkState(isShowDbsStmt());
      return (ShowDbsStmt) stmt_;
    }

    public ShowDataSrcsStmt getShowDataSrcsStmt() {
      Preconditions.checkState(isShowDataSrcsStmt());
      return (ShowDataSrcsStmt) stmt_;
    }

    public ShowStatsStmt getShowStatsStmt() {
      Preconditions.checkState(isShowStatsStmt());
      return (ShowStatsStmt) stmt_;
    }

    public ShowFunctionsStmt getShowFunctionsStmt() {
      Preconditions.checkState(isShowFunctionsStmt());
      return (ShowFunctionsStmt) stmt_;
    }

    public ShowFilesStmt getShowFilesStmt() {
      Preconditions.checkState(isShowFilesStmt());
      return (ShowFilesStmt) stmt_;
    }

    public DescribeStmt getDescribeStmt() {
      Preconditions.checkState(isDescribeStmt());
      return (DescribeStmt) stmt_;
    }

    public ShowCreateTableStmt getShowCreateTableStmt() {
      Preconditions.checkState(isShowCreateTableStmt());
      return (ShowCreateTableStmt) stmt_;
    }

    public StatementBase getStmt() { return stmt_; }
    public Analyzer getAnalyzer() { return analyzer_; }
    public Set<TAccessEvent> getAccessEvents() { return analyzer_.getAccessEvents(); }
    public boolean requiresRewrite() {
      return analyzer_.containsSubquery() && !(stmt_ instanceof CreateViewStmt)
          && !(stmt_ instanceof AlterViewStmt);
    }
    public String getJsonLineageGraph() { return analyzer_.getSerializedLineageGraph(); }
  }

  /**
   * Parse and analyze 'stmt'. If 'stmt' is a nested query (i.e. query that
   * contains subqueries), it is also rewritten by performing subquery unnesting.
   * The transformed stmt is then re-analyzed in a new analysis context.
   *
   * The result of analysis can be retrieved by calling
   * getAnalysisResult().
   *
   * @throws AnalysisException
   *           On any other error, including parsing errors. Also thrown when any
   *           missing tables are detected as a result of running analysis.
   */
  public void analyze(String stmt) throws AnalysisException {
    Analyzer analyzer = new Analyzer(catalog_, queryCtx_, authzConfig_);
    analyze(stmt, analyzer);
  }

  /**
   * Parse and analyze 'stmt' using a specified Analyzer.
   */
  public void analyze(String stmt, Analyzer analyzer) throws AnalysisException {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    try {
      analysisResult_ = new AnalysisResult();
      analysisResult_.analyzer_ = analyzer;
      if (analysisResult_.analyzer_ == null) {
        analysisResult_.analyzer_ = new Analyzer(catalog_, queryCtx_, authzConfig_);
      }
      analysisResult_.stmt_ = (StatementBase) parser.parse().value;
      if (analysisResult_.stmt_ == null) return;

      // For CTAS, we copy the create statement in case we have to create a new CTAS
      // statement after a query rewrite.
      if (analysisResult_.stmt_ instanceof CreateTableAsSelectStmt) {
        analysisResult_.tmpCreateTableStmt_ =
            ((CreateTableAsSelectStmt)analysisResult_.stmt_).getCreateStmt().clone();
      }

      analysisResult_.stmt_.analyze(analysisResult_.analyzer_);
      boolean isExplain = analysisResult_.isExplainStmt();

      // Check if we need to rewrite the statement.
      if (analysisResult_.requiresRewrite()) {
        StatementBase rewrittenStmt = StmtRewriter.rewrite(analysisResult_);
        // Re-analyze the rewritten statement.
        Preconditions.checkNotNull(rewrittenStmt);
        analysisResult_ = new AnalysisResult();
        analysisResult_.analyzer_ = new Analyzer(catalog_, queryCtx_, authzConfig_);
        analysisResult_.stmt_ = rewrittenStmt;
        analysisResult_.stmt_.analyze(analysisResult_.analyzer_);
        LOG.trace("rewrittenStmt: " + rewrittenStmt.toSql());
        if (isExplain) analysisResult_.stmt_.setIsExplain();
        Preconditions.checkState(!analysisResult_.requiresRewrite());
      }
    } catch (AnalysisException e) {
      // Don't wrap AnalysisExceptions in another AnalysisException
      throw e;
    } catch (Exception e) {
      throw new AnalysisException(parser.getErrorMsg(stmt), e);
    }
  }

  /**
   * Authorize an analyzed statement.
   * analyze() must have already been called. Throws an AuthorizationException if the
   * user doesn't have sufficient privileges to run this statement.
   */
  public void authorize(AuthorizationChecker authzChecker) throws AuthorizationException {
    Preconditions.checkNotNull(analysisResult_);
    Analyzer analyzer = getAnalyzer();
    // Process statements for which column-level privilege requests may be registered.
    if (analysisResult_.isQueryStmt() || analysisResult_.isInsertStmt() ||
        analysisResult_.isCreateTableAsSelectStmt() ||
        analysisResult_.isCreateViewStmt() || analysisResult_.isAlterViewStmt()) {
      // Map of table name to a list of privilege requests associated with that table.
      // These include both table-level and column-level privilege requests.
      Map<String, List<PrivilegeRequest>> tablePrivReqs = Maps.newHashMap();
      // Privilege requests that are not column or table-level.
      List<PrivilegeRequest> otherPrivReqs = Lists.newArrayList();
      // Group the registered privilege requests based on the table they reference.
      for (PrivilegeRequest privReq: analyzer.getPrivilegeReqs()) {
        String tableName = privReq.getAuthorizeable().getFullTableName();
        if (tableName == null) {
          otherPrivReqs.add(privReq);
        } else {
          List<PrivilegeRequest> requests = tablePrivReqs.get(tableName);
          if (requests == null) {
            requests = Lists.newArrayList();
            tablePrivReqs.put(tableName, requests);
          }
          // The table-level SELECT must be the first table-level request, and it
          // must precede all column-level privilege requests.
          Preconditions.checkState((requests.isEmpty() ||
              !(privReq.getAuthorizeable() instanceof AuthorizeableColumn)) ||
              (requests.get(0).getAuthorizeable() instanceof AuthorizeableTable &&
              requests.get(0).getPrivilege() == Privilege.SELECT));
          requests.add(privReq);
        }
      }

      // Check any non-table, non-column privilege requests first.
      for (PrivilegeRequest request: otherPrivReqs) {
        authorizePrivilegeRequest(authzChecker, request);
      }

      // Authorize table accesses, one table at a time, by considering both table and
      // column-level privilege requests.
      for (Map.Entry<String, List<PrivilegeRequest>> entry: tablePrivReqs.entrySet()) {
        authorizeTableAccess(authzChecker, entry.getValue());
      }
    } else {
      for (PrivilegeRequest privReq: analyzer.getPrivilegeReqs()) {
        Preconditions.checkState(
            !(privReq.getAuthorizeable() instanceof AuthorizeableColumn) ||
            analysisResult_.isDescribeStmt());
        authorizePrivilegeRequest(authzChecker, privReq);
      }
    }

    // Check any masked requests.
    for (Pair<PrivilegeRequest, String> maskedReq: analyzer.getMaskedPrivilegeReqs()) {
      if (!authzChecker.hasAccess(analyzer.getUser(), maskedReq.first)) {
        throw new AuthorizationException(maskedReq.second);
      }
    }
  }

  /**
   * Authorize a privilege request.
   * Throws an AuthorizationException if the user doesn't have sufficient privileges for
   * this request. Also, checks if the request references a system database.
   */
  private void authorizePrivilegeRequest(AuthorizationChecker authzChecker,
    PrivilegeRequest request) throws AuthorizationException {
    Preconditions.checkNotNull(request);
    String dbName = null;
    if (request.getAuthorizeable() != null) {
      dbName = request.getAuthorizeable().getDbName();
    }
    // If this is a system database, some actions should always be allowed
    // or disabled, regardless of what is in the auth policy.
    if (dbName != null && checkSystemDbAccess(dbName, request.getPrivilege())) {
      return;
    }
    authzChecker.checkAccess(getAnalyzer().getUser(), request);
  }

  /**
   * Authorize a list of privilege requests associated with a single table.
   * It checks if the user has sufficient table-level privileges and if that is
   * not the case, it falls back on checking column-level privileges, if any. This
   * function requires 'SELECT' requests to be ordered by table and then by column
   * privilege requests. Throws an AuthorizationException if the user doesn't have
   * sufficient privileges.
   */
  private void authorizeTableAccess(AuthorizationChecker authzChecker,
      List<PrivilegeRequest> requests) throws AuthorizationException {
    Preconditions.checkState(!requests.isEmpty());
    Analyzer analyzer = getAnalyzer();
    boolean hasTableSelectPriv = true;
    boolean hasColumnSelectPriv = false;
    for (PrivilegeRequest request: requests) {
      if (request.getAuthorizeable() instanceof AuthorizeableTable) {
        try {
          authorizePrivilegeRequest(authzChecker, request);
        } catch (AuthorizationException e) {
          // Authorization fails if we fail to authorize any table-level request that is
          // not a SELECT privilege (e.g. INSERT).
          if (request.getPrivilege() != Privilege.SELECT) throw e;
          hasTableSelectPriv = false;
        }
      } else {
        Preconditions.checkState(
            request.getAuthorizeable() instanceof AuthorizeableColumn);
        if (hasTableSelectPriv) continue;
        if (authzChecker.hasAccess(analyzer.getUser(), request)) {
          hasColumnSelectPriv = true;
          continue;
        }
        // Make sure we don't reveal any column names in the error message.
        throw new AuthorizationException(String.format("User '%s' does not have " +
          "privileges to execute '%s' on: %s", analyzer.getUser().getName(),
          request.getPrivilege().toString(),
          request.getAuthorizeable().getFullTableName()));
      }
    }
    if (!hasTableSelectPriv && !hasColumnSelectPriv) {
       throw new AuthorizationException(String.format("User '%s' does not have " +
          "privileges to execute 'SELECT' on: %s", analyzer.getUser().getName(),
          requests.get(0).getAuthorizeable().getFullTableName()));
    }
  }

  /**
   * Throws an AuthorizationException if the dbName is a system db
   * and the user is trying to modify it.
   * Returns true if this is a system db and the action is allowed.
   */
  private boolean checkSystemDbAccess(String dbName, Privilege privilege)
      throws AuthorizationException {
    Db db = catalog_.getDb(dbName);
    if (db != null && db.isSystemDb()) {
      switch (privilege) {
        case VIEW_METADATA:
        case ANY:
          return true;
        default:
          throw new AuthorizationException("Cannot modify system database.");
      }
    }
    return false;
  }

  public AnalysisResult getAnalysisResult() { return analysisResult_; }
  public Analyzer getAnalyzer() { return getAnalysisResult().getAnalyzer(); }
}
