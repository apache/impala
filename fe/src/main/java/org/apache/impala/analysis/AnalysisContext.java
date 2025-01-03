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

import static org.apache.impala.analysis.ToSqlOptions.REWRITTEN;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.AuthorizationException;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.service.CompilerFactory;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TClientRequest;
import org.apache.impala.thrift.TLineageGraph;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.EventSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Wrapper class for parsing, analyzing and rewriting a SQL stmt.
 */
public class AnalysisContext {
  private final static Logger LOG = LoggerFactory.getLogger(AnalysisContext.class);
  private final TQueryCtx queryCtx_;
  private final AuthorizationFactory authzFactory_;
  private final EventSequence timeline_;

  // Set in analyzeAndAuthorize().
  private FeCatalog catalog_;
  private AnalysisResult analysisResult_;

  // Use Hive's scheme for auto-generating column labels. Only used for testing.
  private boolean useHiveColLabels_;

  public AnalysisContext(TQueryCtx queryCtx, AuthorizationFactory authzFactory,
      EventSequence timeline) {
    queryCtx_ = queryCtx;
    authzFactory_ = authzFactory;
    timeline_ = timeline;
  }

  public FeCatalog getCatalog() { return catalog_; }
  public TQueryCtx getQueryCtx() { return queryCtx_; }
  public AuthorizationFactory getAuthzFactory() { return authzFactory_; }
  public boolean getUseHiveColLabels() { return useHiveColLabels_; }
  public TQueryOptions getQueryOptions() {
    return queryCtx_.client_request.query_options;
  }
  public String getUser() { return queryCtx_.session.connected_user; }

  public void setUseHiveColLabels(boolean b) {
    Preconditions.checkState(RuntimeEnv.INSTANCE.isTestEnv());
    useHiveColLabels_ = b;
  }

  static public class AnalysisResult {
    // The wrapper for the parsed AST for a planner (e.g. original, Calcite).
    private final ParsedStatement parsedStmt_;
    // AST for original planner. This is retrievable via the parsedStmt_ variable
    // but in its own variable for convenience. For the Calcite planner, this will be
    // null.
    private final StatementBase stmt_;
    private final Analyzer analyzer_;
    private final ImpalaException exception_;
    private boolean userHasProfileAccess_ = true;

    public AnalysisResult(ParsedStatement parsedStmt, Analyzer analyzer) {
      this(parsedStmt, analyzer, null);
    }

    public AnalysisResult(ParsedStatement parsedStmt, Analyzer analyzer,
        ImpalaException e) {
      parsedStmt_ = parsedStmt;
      stmt_ = parsedStmt_.getTopLevelNode() instanceof StatementBase
          ? (StatementBase) parsedStmt_.getTopLevelNode()
          : null;
      analyzer_ = analyzer;
      exception_ = e;
    }

    public boolean isAlterTableStmt() { return stmt_ instanceof AlterTableStmt; }
    public boolean isAlterViewStmt() { return stmt_ instanceof AlterViewStmt; }
    public boolean isComputeStatsStmt() { return stmt_ instanceof ComputeStatsStmt; }
    public boolean isQueryStmt() { return parsedStmt_.isQueryStmt(); }
    public boolean isSetOperationStmt() { return stmt_ instanceof SetOperationStmt; }
    public boolean isInsertStmt() { return stmt_ instanceof InsertStmt; }
    public boolean isMergeStmt() { return stmt_ instanceof MergeStmt; }
    public boolean isOptimizeStmt() { return stmt_ instanceof OptimizeStmt; }
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
    public boolean isShowMetadataTablesStmt() {
      return stmt_ instanceof ShowMetadataTablesStmt;
    }
    public boolean isShowViewsStmt() { return stmt_ instanceof ShowViewsStmt; }
    public boolean isDescribeHistoryStmt() {
      return stmt_ instanceof DescribeHistoryStmt;
    }
    public boolean isShowDbsStmt() { return stmt_ instanceof ShowDbsStmt; }
    public boolean isShowDataSrcsStmt() { return stmt_ instanceof ShowDataSrcsStmt; }
    public boolean isShowStatsStmt() { return stmt_ instanceof ShowStatsStmt; }
    public boolean isShowFunctionsStmt() { return stmt_ instanceof ShowFunctionsStmt; }
    public boolean isShowCreateTableStmt() {
      return stmt_ instanceof ShowCreateTableStmt;
    }
    public boolean isShowCreateFunctionStmt() {
      return stmt_ instanceof ShowCreateFunctionStmt;
    }
    public boolean isShowFilesStmt() { return stmt_ instanceof ShowFilesStmt; }
    public boolean isAdminFnStmt() { return stmt_ instanceof AdminFnStmt; }
    public boolean isDescribeDbStmt() { return stmt_ instanceof DescribeDbStmt; }
    public boolean isDescribeTableStmt() { return stmt_ instanceof DescribeTableStmt; }
    public boolean isResetMetadataStmt() { return stmt_ instanceof ResetMetadataStmt; }
    public boolean isExplainStmt() { return parsedStmt_.isExplain(); }
    public boolean isShowRolesStmt() { return stmt_ instanceof ShowRolesStmt; }
    public boolean isShowGrantPrincipalStmt() {
      return stmt_ instanceof ShowGrantPrincipalStmt;
    }
    public boolean isCreateDropRoleStmt() { return stmt_ instanceof CreateDropRoleStmt; }
    public boolean isGrantRevokeRoleStmt() {
      return stmt_ instanceof GrantRevokeRoleStmt;
    }
    public boolean isGrantRevokePrivStmt() {
      return stmt_ instanceof GrantRevokePrivStmt;
    }
    public boolean isTruncateStmt() { return stmt_ instanceof TruncateStmt; }
    public boolean isUpdateStmt() { return stmt_ instanceof UpdateStmt; }
    public UpdateStmt getUpdateStmt() { return (UpdateStmt) stmt_; }
    public boolean isDeleteStmt() { return stmt_ instanceof DeleteStmt; }
    public DeleteStmt getDeleteStmt() { return (DeleteStmt) stmt_; }
    public boolean isCommentOnStmt() { return stmt_ instanceof CommentOnStmt; }

    public boolean isAlterDbStmt() { return stmt_ instanceof AlterDbStmt; }

    public boolean isCatalogOp() {
      return isUseStmt() || isViewMetadataStmt() || isDdlStmt();
    }

    public boolean isTestCaseStmt() { return stmt_ instanceof CopyTestCaseStmt; }

    private boolean isDdlStmt() {
      return isCreateTableLikeStmt() || isCreateTableStmt() ||
          isCreateViewStmt() || isCreateDbStmt() || isDropDbStmt() ||
          isDropTableOrViewStmt() || isResetMetadataStmt() || isAlterTableStmt() ||
          isAlterViewStmt() || isComputeStatsStmt() || isCreateUdfStmt() ||
          isCreateUdaStmt() || isDropFunctionStmt() || isCreateTableAsSelectStmt() ||
          isCreateDataSrcStmt() || isDropDataSrcStmt() || isDropStatsStmt() ||
          isCreateDropRoleStmt() || isGrantRevokeStmt() || isTruncateStmt() ||
          isCommentOnStmt() || isAlterDbStmt();
    }

    private boolean isViewMetadataStmt() {
      return isShowFilesStmt() || isShowTablesStmt() || isShowMetadataTablesStmt() ||
          isShowViewsStmt() || isShowDbsStmt() || isShowFunctionsStmt() ||
          isShowRolesStmt() || isShowGrantPrincipalStmt() || isShowCreateTableStmt() ||
          isShowDataSrcsStmt() || isShowStatsStmt() || isDescribeTableStmt() ||
          isDescribeDbStmt() || isShowCreateFunctionStmt() || isDescribeHistoryStmt();
    }

    private boolean isGrantRevokeStmt() {
      return isGrantRevokeRoleStmt() || isGrantRevokePrivStmt();
    }

    public boolean isDmlStmt() {
      return isInsertStmt() || isUpdateStmt() || isDeleteStmt()
          || isOptimizeStmt() || isMergeStmt();
    }

    /**
     * Returns true for statements that may produce several privilege requests of
     * hierarchical nature, e.g., table/column.
     */
    public boolean isHierarchicalAuthStmt() {
      return isQueryStmt() || isInsertStmt() || isUpdateStmt() || isDeleteStmt()
          || isCreateTableAsSelectStmt() || isCreateViewStmt() || isAlterViewStmt()
          || isOptimizeStmt() || isTestCaseStmt() || isMergeStmt();
    }

    /**
     * Returns true for statements that may produce a single column-level privilege
     * request without a request at the table level.
     * Example: USE functional; ALTER TABLE allcomplextypes.int_array_col [...];
     * The path 'allcomplextypes.int_array_col' table ref path resolves to
     * a column, so a column-level privilege request is registered.
     */
    public boolean isSingleColumnPrivStmt() {
      return isDescribeTableStmt() || isResetMetadataStmt() || isUseStmt()
          || isShowTablesStmt() || isShowMetadataTablesStmt() || isShowViewsStmt()
          || isAlterTableStmt() || isShowFunctionsStmt();
    }

    public boolean isConvertTableToIcebergStmt() {
      return stmt_ instanceof ConvertTableToIcebergStmt;
    }

    public boolean isKillQueryStmt() {
      return stmt_ instanceof KillQueryStmt;
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

    public OptimizeStmt getOptimizeStmt() {
      Preconditions.checkState(isOptimizeStmt());
      return (OptimizeStmt) stmt_;
    }

    public MergeStmt getMergeStmt() {
      Preconditions.checkState(isMergeStmt());
      return (MergeStmt) stmt_;
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

    public ShowMetadataTablesStmt getShowMetadataTablesStmt() {
      Preconditions.checkState(isShowMetadataTablesStmt());
      return (ShowMetadataTablesStmt) stmt_;
    }

    public ShowViewsStmt getShowViewsStmt() {
      Preconditions.checkState(isShowViewsStmt());
      return (ShowViewsStmt) stmt_;
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

    public DescribeHistoryStmt getDescribeHistoryStmt() {
      Preconditions.checkState(isDescribeHistoryStmt());
      return (DescribeHistoryStmt) stmt_;
    }

    public DescribeDbStmt getDescribeDbStmt() {
      Preconditions.checkState(isDescribeDbStmt());
      return (DescribeDbStmt) stmt_;
    }

    public DescribeTableStmt getDescribeTableStmt() {
      Preconditions.checkState(isDescribeTableStmt());
      return (DescribeTableStmt) stmt_;
    }

    public ShowCreateTableStmt getShowCreateTableStmt() {
      Preconditions.checkState(isShowCreateTableStmt());
      return (ShowCreateTableStmt) stmt_;
    }

    public ShowCreateFunctionStmt getShowCreateFunctionStmt() {
      Preconditions.checkState(isShowCreateFunctionStmt());
      return (ShowCreateFunctionStmt) stmt_;
    }

    public CommentOnStmt getCommentOnStmt() {
      Preconditions.checkState(isCommentOnStmt());
      return (CommentOnStmt) stmt_;
    }

    public AlterDbStmt getAlterDbStmt() {
      Preconditions.checkState(isAlterDbStmt());
      return (AlterDbStmt) stmt_;
    }

    public AdminFnStmt getAdminFnStmt() {
      Preconditions.checkState(isAdminFnStmt());
      return (AdminFnStmt) stmt_;
    }

    public ConvertTableToIcebergStmt getConvertTableToIcebergStmt() {
      Preconditions.checkState(isConvertTableToIcebergStmt());
      return (ConvertTableToIcebergStmt) stmt_;
    }

    public KillQueryStmt getKillQueryStmt() {
      Preconditions.checkState(isKillQueryStmt());
      return (KillQueryStmt) stmt_;
    }

    public ParsedStatement getParsedStmt() { return parsedStmt_; }
    public StatementBase getStmt() { return stmt_; }
    public Analyzer getAnalyzer() { return analyzer_; }
    public ImpalaException getException() { return exception_; }
    public Set<TAccessEvent> getAccessEvents() { return analyzer_.getAccessEvents(); }
    public TLineageGraph getThriftLineageGraph() {
      return analyzer_.getThriftSerializedLineageGraph();
    }
    public void setUserHasProfileAccess(boolean value) { userHasProfileAccess_ = value; }
    public boolean userHasProfileAccess() { return userHasProfileAccess_; }

  }

  public AnalysisResult analyzeAndAuthorize(CompilerFactory compilerFactory,
      ParsedStatement parsedStmt, StmtTableCache stmtTableCache,
      AuthorizationChecker authzChecker) throws ImpalaException {
    return analyzeAndAuthorize(compilerFactory, parsedStmt,
        stmtTableCache, authzChecker, false /*disableAuthorization*/);
  }

  /**
   * Analyzes and authorizes the given statement using the provided table cache and
   * authorization checker.
   * AuthorizationExceptions take precedence over AnalysisExceptions so as not to
   * reveal the existence/absence of objects the user is not authorized to see.
   */
  public AnalysisResult analyzeAndAuthorize(CompilerFactory compilerFactory,
      ParsedStatement parsedStmt, StmtTableCache stmtTableCache,
      AuthorizationChecker authzChecker,
      boolean disableAuthorization) throws ImpalaException {
    // TODO: Clean up the creation/setting of the analysis result.
    catalog_ = stmtTableCache.catalog;

    // Analyze statement and record exception.
    TClientRequest clientRequest = queryCtx_.getClient_request();
    AuthorizationContext authzCtx = authzChecker.createAuthorizationContext(true,
        clientRequest.isSetRedacted_stmt() ?
            clientRequest.getRedacted_stmt() : clientRequest.getStmt(),
        queryCtx_.getSession(), Optional.of(timeline_));
    Preconditions.checkState(authzCtx != null);
    AnalysisDriver analysisDriver = compilerFactory.createAnalysisDriver(this,
        parsedStmt, stmtTableCache, authzCtx);

    analysisResult_ = analysisDriver.analyze();
    authzChecker.postAnalyze(authzCtx);
    ImpalaException analysisException = analysisResult_.getException();

    // A statement that returns at most one row does not need to spool query results.
    // IMPALA-13902: returnsAtMostOneRow should be in planner interface so it is
    // accessible by the Calcite planner.
    if (analysisException == null && analysisResult_.getStmt() instanceof SelectStmt &&
        ((SelectStmt)analysisResult_.getStmt()).returnsAtMostOneRow()) {
      clientRequest.query_options.setSpool_query_results(false);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Result spooling is disabled due to the statement returning at most "
            + "one row.");
      }
    }
    long durationMs = timeline_.markEvent("Analysis finished") / 1000000;
    LOG.info("Analysis took {} ms", durationMs);

    // Authorize statement and record exception. Authorization relies on information
    // collected during analysis.
    AuthorizationException authException = null;
    if (!disableAuthorization) {
      try {
        if (analysisResult_.getAnalyzer().encounteredMVAuthException()) {
          throw new AuthorizationException(
            analysisResult_.getAnalyzer().getMVAuthExceptionMsg());
        }
        authzChecker.authorize(authzCtx, analysisResult_, catalog_);
      } catch (AuthorizationException e) {
        authException = e;
      } finally {
        authzChecker.postAuthorize(authzCtx, authException == null,
            analysisException == null);
      }
    }

    // AuthorizationExceptions take precedence over AnalysisExceptions so as not
    // to reveal the existence/absence of objects the user is not authorized to see.
    if (authException != null) throw authException;
    if (analysisException != null) throw analysisException;
    return analysisResult_;
  }

  /**
   * AnalysisDriverImpl has one main method "analyze" which drives the analysis
   * of the query. The analyze method returns an AnalysisResult which is a simple
   * class containing results from the analysis which are needed for the planning
   * stage.
   */
  public static class AnalysisDriverImpl implements AnalysisDriver {
    // the stmt variables and Analyzer are not final variables because of the
    // rewrite code. This should perhaps be refactored in the future.
    private ParsedStatement parsedStmt_;
    private StatementBase stmt_;
    private Analyzer analyzer_;
    private final AnalysisContext ctx_;
    private final StmtTableCache stmtTableCache_;
    private final AuthorizationContext authzCtx_;

    public AnalysisDriverImpl(AnalysisContext ctx, ParsedStatement parsedStmt,
        StmtTableCache stmtTableCache,
        AuthorizationContext authzCtx) {
      Preconditions.checkNotNull(parsedStmt);
      parsedStmt_ = parsedStmt;
      ctx_ = ctx;
      Preconditions.checkNotNull(parsedStmt.getTopLevelNode());
      stmt_ = (StatementBase) parsedStmt.getTopLevelNode();
      stmtTableCache_ = stmtTableCache;
      authzCtx_ = authzCtx;
      analyzer_ = createAnalyzer(ctx_, stmtTableCache, authzCtx);
    }

    /**
     * Analyzes the statement set in 'analysisResult_' with a new Analyzer based on the
     * given loaded tables. Performs expr and subquery rewrites which require re-analyzing
     * the transformed statement.
     */
    @Override
    public AnalysisResult analyze() {
      try {
        stmt_.analyze(analyzer_);
        // Enforce the statement expression limit at the end of analysis so that there is
        // an accurate count of the total number of expressions. The first analyze() call
        // is not very expensive (~seconds) even for large statements. The limit on the
        // total length of the SQL statement (max_statement_length_bytes) provides an
        // upper bound. It is important to enforce this before expression rewrites,
        // because rewrites are expensive with large expression trees. For example, a SQL
        // that takes a few seconds to analyze the first time may take 10 minutes for
        // rewrites.
        analyzer_.checkStmtExprLimit();

        // The rewrites should have no user-visible effect on query results, including
        // types and labels. Remember the original result types and column labels to
        // restore them after the rewritten stmt has been reset() and re-analyzed. For a
        // CTAS statement, the types represent column types of the table that will be
        // created, including the partition columns, if any.
        List<Type> origResultTypes = new ArrayList<>();
        for (Expr e : stmt_.getResultExprs()) {
          origResultTypes.add(e.getType());
        }
        List<String> origColLabels =
            Lists.newArrayList(stmt_.getColLabels());

        // Apply column/row masking, expr, setop, and subquery rewrites.
        boolean shouldReAnalyze = false;
        if (ctx_.getAuthzFactory().getAuthorizationConfig().isEnabled()) {
          shouldReAnalyze = stmt_.resolveTableMask(analyzer_);
          // If any catalog table/view is replaced by table masking views, we need to
          // resolve them. Also re-analyze the SlotRefs to reference the output exprs of
          // the table masking views.
          if (shouldReAnalyze) {
            // To be consistent with Hive, privilege requests introduced by the masking
            // views should also be collected (IMPALA-10728).
            reAnalyze(stmtTableCache_, authzCtx_, origResultTypes, origColLabels,
                /*collectPrivileges*/ true);
          }
          shouldReAnalyze = false;
        }
        ExprRewriter rewriter = analyzer_.getExprRewriter();
        if (requiresExprRewrite()) {
          rewriter.reset();
          stmt_.rewriteExprs(rewriter);
          shouldReAnalyze = rewriter.changed();
        }
        if (requiresSubqueryRewrite()) {
          new StmtRewriter.SubqueryRewriter().rewrite(stmt_);
          shouldReAnalyze = true;
        }
        if (requiresSetOperationRewrite()) {
          new StmtRewriter().rewrite(stmt_);
          shouldReAnalyze = true;
        }
        if (requiresAcidComplexScanRewrite()) {
          new StmtRewriter.AcidRewriter().rewrite(stmt_);
          shouldReAnalyze = true;
        }
        if (requiresZippingUnnestRewrite()) {
          new StmtRewriter.ZippingUnnestRewriter().rewrite(stmt_);
          shouldReAnalyze = true;
        }
        if (!shouldReAnalyze) {
          return new AnalysisResult(parsedStmt_, analyzer_);
        }

        // For SetOperationStmt we must replace the query statement with the rewritten
        // version before re-analysis and set the explain flag of the rewritten version
        // if the original is explain statement.
        if (requiresSetOperationRewrite()) {
          if (stmt_ instanceof SetOperationStmt) {
            if (((SetOperationStmt) stmt_).hasRewrittenStmt()) {
              boolean isExplain = stmt_.isExplain();
              stmt_ = ((SetOperationStmt) stmt_).getRewrittenStmt();
              parsedStmt_ = new ParsedStatementImpl(stmt_);
              if (isExplain) stmt_.setIsExplain();
            }
          }
        }

        reAnalyze(stmtTableCache_, authzCtx_, origResultTypes, origColLabels,
            /*collectPrivileges*/ false);
        Preconditions.checkState(!requiresSubqueryRewrite());
        return new AnalysisResult(parsedStmt_, analyzer_);
      } catch (ImpalaException e) {
        return new AnalysisResult(parsedStmt_, analyzer_, e);
      }
    }

    private void reAnalyze(
        StmtTableCache stmtTableCache, AuthorizationContext authzCtx,
        List<Type> origResultTypes, List<String> origColLabels, boolean collectPrivileges)
        throws AnalysisException {
      boolean isExplain = stmt_.isExplain();
      // Some expressions, such as function calls with constant arguments, can get
      // folded into literals. Since literals do not require privilege requests, we
      // must save the original privileges in order to not lose them during
      // re-analysis.
      ImmutableList<PrivilegeRequest> origPrivReqs = analyzer_.getPrivilegeReqs();

      // Re-analyze the stmt with a new analyzer.
      analyzer_ = createAnalyzer(ctx_, stmtTableCache, authzCtx);
      // Restore privilege requests found during the previous pass
      for (PrivilegeRequest req : origPrivReqs) {
        analyzer_.registerPrivReq(req);
      }
      // Only collect privilege requests in need.
      analyzer_.setEnablePrivChecks(collectPrivileges);
      stmt_.reset();
      try {
        stmt_.analyze(analyzer_);
        analyzer_.setEnablePrivChecks(true); // Always restore
      } catch (AnalysisException e) {
        logRewriteErrorNoThrow(stmt_, e);
        throw new AnalysisException("An error occurred after query rewrite: " +
            e.getMessage(), e);
      }
      // Restore the original result types and column labels.
      stmt_.castResultExprs(origResultTypes);
      stmt_.setColLabels(origColLabels);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Rewritten SQL: " + stmt_.toSql(REWRITTEN));
      }
      if (isExplain) stmt_.setIsExplain();
    }

    private void logRewriteErrorNoThrow(StatementBase stmt,
        AnalysisException analysisException) {
      try {
        LOG.error(String.format("Error analyzing the rewritten query.\n" +
            "Original SQL: %s\nRewritten SQL: %s", stmt.toSql(),
            stmt.toSql(REWRITTEN)), analysisException);
      } catch (Exception e) {
        LOG.error("An exception occurred during printing out " +
            "the rewritten SQL statement.", e);
      }
    }

    public boolean canRewriteStatement() {
      return !isCreateViewStmt() && !isAlterViewStmt() && !isShowCreateTableStmt();
    }
    public boolean requiresSubqueryRewrite() {
      return canRewriteStatement() && analyzer_.containsSubquery();
    }
    public boolean requiresAcidComplexScanRewrite() {
      return canRewriteStatement() && analyzer_.hasTopLevelAcidCollectionTableRef();
    }
    public boolean requiresZippingUnnestRewrite() {
      return canRewriteStatement() && isZippingUnnestInSelectList(stmt_);
    }
    public boolean requiresExprRewrite() {
      return isQueryStmt() || isInsertStmt() || isCreateTableAsSelectStmt()
          || isUpdateStmt() || isDeleteStmt() || isOptimizeStmt() || isMergeStmt();
    }
    public boolean requiresSetOperationRewrite() {
      return analyzer_.containsSetOperation() && canRewriteStatement();
    }

    // TODO: IMPALA-13840: Code would be cleaner if we had an enum for StatementType
    // rather than checking "instanceof".
    public boolean isCreateViewStmt() { return stmt_ instanceof CreateViewStmt; }
    public boolean isAlterViewStmt() { return stmt_ instanceof AlterViewStmt; }
    public boolean isShowCreateTableStmt() {
      return stmt_ instanceof ShowCreateTableStmt;
    }
    public boolean isQueryStmt() { return stmt_ instanceof QueryStmt; }
    public boolean isInsertStmt() { return stmt_ instanceof InsertStmt; }
    public boolean isMergeStmt() { return stmt_ instanceof MergeStmt; }
    public boolean isDeleteStmt() { return stmt_ instanceof DeleteStmt; }
    public boolean isOptimizeStmt() { return stmt_ instanceof OptimizeStmt; }
    public boolean isUpdateStmt() { return stmt_ instanceof UpdateStmt; }
    public boolean isCreateTableAsSelectStmt() {
      return stmt_ instanceof CreateTableAsSelectStmt;
    }

    private boolean isZippingUnnestInSelectList(StatementBase stmt) {
      if (!(stmt instanceof SelectStmt)) return false;
      if (!stmt.analyzer_.getTableRefsFromUnnestExpr().isEmpty()) return true;
      SelectStmt selectStmt = (SelectStmt)stmt;
      for (TableRef tblRef : selectStmt.fromClause_.getTableRefs()) {
        if (tblRef instanceof InlineViewRef &&
            isZippingUnnestInSelectList(((InlineViewRef)tblRef).getViewStmt())) {
          return true;
        }
      }
      return false;
    }

    public static Analyzer createAnalyzer(AnalysisContext ctx,
        StmtTableCache stmtTableCache) {
      return createAnalyzer(ctx, stmtTableCache, null);
    }

    private static Analyzer createAnalyzer(AnalysisContext ctx,
        StmtTableCache stmtTableCache, AuthorizationContext authzCtx) {
      Analyzer result = new Analyzer(stmtTableCache, ctx.getQueryCtx(),
          ctx.getAuthzFactory(), authzCtx);
      result.setUseHiveColLabels(ctx.getUseHiveColLabels());
      return result;
    }
  }
  public Analyzer getAnalyzer() { return analysisResult_.getAnalyzer(); }
  public EventSequence getTimeline() { return timeline_; }
  // This should only be called after analyzeAndAuthorize().
  public AnalysisResult getAnalysisResult() {
    Preconditions.checkNotNull(analysisResult_);
    Preconditions.checkNotNull(analysisResult_.stmt_);
    return analysisResult_;
  }
}
