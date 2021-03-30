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

package org.apache.impala.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.impala.analysis.AlterDbStmt;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.CommentOnStmt;
import org.apache.impala.analysis.CopyTestCaseStmt;
import org.apache.impala.analysis.CreateDataSrcStmt;
import org.apache.impala.analysis.CreateDropRoleStmt;
import org.apache.impala.analysis.CreateUdaStmt;
import org.apache.impala.analysis.CreateUdfStmt;
import org.apache.impala.analysis.DescribeTableStmt;
import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.DropDataSrcStmt;
import org.apache.impala.analysis.DropFunctionStmt;
import org.apache.impala.analysis.DropStatsStmt;
import org.apache.impala.analysis.DropTableOrViewStmt;
import org.apache.impala.analysis.GrantRevokePrivStmt;
import org.apache.impala.analysis.GrantRevokeRoleStmt;
import org.apache.impala.analysis.InsertStmt;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.ResetMetadataStmt;
import org.apache.impala.analysis.ShowFunctionsStmt;
import org.apache.impala.analysis.ShowGrantPrincipalStmt;
import org.apache.impala.analysis.ShowRolesStmt;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.analysis.TableName;
import org.apache.impala.analysis.TruncateStmt;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.ImpalaInternalAdminUser;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeDataSource;
import org.apache.impala.catalog.FeDataSourceTable;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ImpaladCatalog;
import org.apache.impala.catalog.ImpaladTableUsageTracker;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.local.InconsistentMetadataFetchException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.common.TransactionException;
import org.apache.impala.common.TransactionKeepalive;
import org.apache.impala.common.TransactionKeepalive.HeartbeatContext;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.hooks.QueryCompleteContext;
import org.apache.impala.hooks.QueryEventHook;
import org.apache.impala.hooks.QueryEventHookManager;
import org.apache.impala.planner.HdfsScanNode;
import org.apache.impala.planner.PlanFragment;
import org.apache.impala.planner.Planner;
import org.apache.impala.planner.ScanNode;
import org.apache.impala.thrift.TAlterDbParams;
import org.apache.impala.thrift.TCatalogOpRequest;
import org.apache.impala.thrift.TCatalogOpType;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TClientRequest;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TCommentOnParams;
import org.apache.impala.thrift.TCreateDropRoleParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDescribeOutputStyle;
import org.apache.impala.thrift.TDescribeResult;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExplainResult;
import org.apache.impala.thrift.TFinalizeParams;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.thrift.TGetCatalogMetricsResult;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.TLineageGraph;
import org.apache.impala.thrift.TLoadDataReq;
import org.apache.impala.thrift.TLoadDataResp;
import org.apache.impala.thrift.TCopyTestCaseReq;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TPlanExecInfo;
import org.apache.impala.thrift.TPlanFragment;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryExecRequest;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TShowFilesParams;
import org.apache.impala.thrift.TShowStatsOp;
import org.apache.impala.thrift.TStmtType;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUpdateCatalogCacheRequest;
import org.apache.impala.thrift.TUpdateCatalogCacheResponse;
import org.apache.impala.thrift.TUpdateExecutorMembershipRequest;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.ExecutorMembershipSnapshot;
import org.apache.impala.util.PatternMatcher;
import org.apache.impala.util.TResultRowBuilder;
import org.apache.impala.util.TSessionStateUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Frontend API for the impalad process.
 * This class allows the impala daemon to create TQueryExecRequest
 * in response to TClientRequests. Also handles management of the authorization
 * policy and query execution hooks.
 */
public class Frontend {
  private final static Logger LOG = LoggerFactory.getLogger(Frontend.class);

  // Max time to wait for a catalog update notification.
  public static final long MAX_CATALOG_UPDATE_WAIT_TIME_MS = 2 * 1000;

  // TODO: Make the reload interval configurable.
  private static final int AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS = 5 * 60;

  // Maximum number of times to retry a query if it fails due to inconsistent metadata.
  private static final int INCONSISTENT_METADATA_NUM_RETRIES =
      BackendConfig.INSTANCE.getLocalCatalogMaxFetchRetries();

  // Maximum number of threads used to check authorization for the user when executing
  // show tables/databases.
  private static final int MAX_CHECK_AUTHORIZATION_POOL_SIZE = 128;

  /**
   * Plan-time context that allows capturing various artifacts created
   * during the process.
   *
   * The context gathers an optional describe string for display to the
   * user, and the optional plan fragment nodes for use in unit tests.
   */
  public static class PlanCtx {
    // The query context.
    protected final TQueryCtx queryCtx_;
    // The explain string built from the query plan.
    protected final StringBuilder explainBuf_;
    // Flag to indicate whether to capture (return) the plan.
    protected boolean capturePlan_;
    // Flag to control whether the descriptor table is serialized. This defaults to
    // true, but some frontend tests set it to false because they are operating on
    // incomplete structures (e.g. THdfsTable without nullPartitionKeyValue) that cannot
    // be serialized.
    protected boolean serializeDescTbl_ = true;

    // The physical plan, divided by fragment, before conversion to
    // Thrift. For unit testing.
    protected List<PlanFragment> plan_;

    public PlanCtx(TQueryCtx qCtx) {
      queryCtx_ = qCtx;
      explainBuf_ = new StringBuilder();
    }

    public PlanCtx(TQueryCtx qCtx, StringBuilder describe) {
      queryCtx_ = qCtx;
      explainBuf_ = describe;
    }

    /**
     * Request to capture the plan tree for unit tests.
     */
    public void requestPlanCapture() { capturePlan_ = true; }
    public boolean planCaptureRequested() { return capturePlan_; }
    public void disableDescTblSerialization() { serializeDescTbl_ = false; }
    public boolean serializeDescTbl() { return serializeDescTbl_; }
    public TQueryCtx getQueryContext() { return queryCtx_; }

    /**
     * @return the captured plan tree. Used only for unit tests
     */
    @VisibleForTesting
    public List<PlanFragment> getPlan() { return plan_; }

    /**
     * @return the captured describe string
     */
    public String getExplainString() { return explainBuf_.toString(); }
  }

  private final FeCatalogManager catalogManager_;
  private final AuthorizationFactory authzFactory_;
  private final AuthorizationManager authzManager_;
  // Privileges in which the user should have any of them to see a database or table,
  private final EnumSet<Privilege> minPrivilegeSetForShowStmts_;
  /**
   * Authorization checker. Initialized and periodically loaded by a task
   * running on the {@link #policyReader_} thread.
   */
  private final AtomicReference<AuthorizationChecker> authzChecker_ =
      new AtomicReference<>();
  private final ScheduledExecutorService policyReader_ =
      Executors.newScheduledThreadPool(1);

  private final ImpaladTableUsageTracker impaladTableUsageTracker_;

  private final QueryEventHookManager queryHookManager_;

  // Stores metastore clients for direct accesses to HMS.
  private final MetaStoreClientPool metaStoreClientPool_;

  private final TransactionKeepalive transactionKeepalive_;

  private static ExecutorService checkAuthorizationPool_;

  public Frontend(AuthorizationFactory authzFactory, boolean isBackendTest)
      throws ImpalaException {
    this(authzFactory, FeCatalogManager.createFromBackendConfig(), isBackendTest);
  }

  /**
   * Create a frontend with a specific catalog instance which will not allow
   * updates and will be used for all requests.
   */
  @VisibleForTesting
  public Frontend(AuthorizationFactory authzFactory, FeCatalog testCatalog)
      throws ImpalaException {
    // This signature is only used for frontend tests, so pass false for isBackendTest
    this(authzFactory, FeCatalogManager.createForTests(testCatalog), false);
  }

  private Frontend(AuthorizationFactory authzFactory, FeCatalogManager catalogManager,
      boolean isBackendTest) throws ImpalaException {
    catalogManager_ = catalogManager;
    authzFactory_ = authzFactory;

    AuthorizationConfig authzConfig = authzFactory.getAuthorizationConfig();
    if (authzConfig.isEnabled()) {
      authzChecker_.set(authzFactory.newAuthorizationChecker(
          getCatalog().getAuthPolicy()));
      int numThreads = BackendConfig.INSTANCE.getNumCheckAuthorizationThreads();
      Preconditions.checkState(numThreads > 0
        && numThreads <= MAX_CHECK_AUTHORIZATION_POOL_SIZE);
      if (numThreads == 1) {
        checkAuthorizationPool_ = MoreExecutors.newDirectExecutorService();
      } else {
        LOG.info("Using a thread pool of size {} for authorization", numThreads);
        checkAuthorizationPool_ = Executors.newFixedThreadPool(numThreads);
      }
    } else {
      authzChecker_.set(authzFactory.newAuthorizationChecker());
    }
    catalogManager_.setAuthzChecker(authzChecker_);
    authzManager_ = authzFactory.newAuthorizationManager(catalogManager_,
        authzChecker_::get);
    minPrivilegeSetForShowStmts_ = getMinPrivilegeSetForShowStmts();
    impaladTableUsageTracker_ = ImpaladTableUsageTracker.createFromConfig(
        BackendConfig.INSTANCE);
    queryHookManager_ = QueryEventHookManager.createFromConfig(BackendConfig.INSTANCE);
    if (!isBackendTest) {
      metaStoreClientPool_ = new MetaStoreClientPool(1, 0);
      if (MetastoreShim.getMajorVersion() > 2) {
        transactionKeepalive_ = new TransactionKeepalive(metaStoreClientPool_);
      } else {
        transactionKeepalive_ = null;
      }
    } else {
      metaStoreClientPool_ = null;
      transactionKeepalive_ = null;
    }
  }

  /**
   * Returns the required privilege set for showing a database or table.
   */
  private EnumSet<Privilege> getMinPrivilegeSetForShowStmts() throws InternalException {
    String configStr = BackendConfig.INSTANCE.getMinPrivilegeSetForShowStmts();
    if (Strings.isNullOrEmpty(configStr)) return EnumSet.of(Privilege.ANY);
    EnumSet<Privilege> privileges = EnumSet.noneOf(Privilege.class);
    for (String pStr : configStr.toUpperCase().split(",")) {
      try {
        privileges.add(Privilege.valueOf(pStr.trim()));
      } catch (IllegalArgumentException e) {
        LOG.error("Illegal privilege name '{}'", pStr, e);
        throw new InternalException("Failed to parse privileges: " + configStr, e);
      }
    }
    return privileges.isEmpty() ? EnumSet.of(Privilege.ANY) : privileges;
  }

  public FeCatalog getCatalog() { return catalogManager_.getOrCreateCatalog(); }

  public AuthorizationChecker getAuthzChecker() { return authzChecker_.get(); }

  public AuthorizationManager getAuthzManager() { return authzManager_; }

  public ImpaladTableUsageTracker getImpaladTableUsageTracker() {
    return impaladTableUsageTracker_;
  }

  public TUpdateCatalogCacheResponse updateCatalogCache(
      TUpdateCatalogCacheRequest req) throws ImpalaException, TException {
    TUpdateCatalogCacheResponse resp = catalogManager_.updateCatalogCache(req);
    if (!req.is_delta) {
      // In the case that it was a non-delta update, the catalog might have reloaded
      // itself, and we need to reset the AuthorizationChecker accordingly.
      authzChecker_.set(authzFactory_.newAuthorizationChecker(
          getCatalog().getAuthPolicy()));
    }
    return resp;
  }

  /**
   * Constructs a TCatalogOpRequest and attaches it, plus any metadata, to the
   * result argument.
   */
  private void createCatalogOpRequest(AnalysisResult analysis,
      TExecRequest result) throws InternalException {
    TCatalogOpRequest ddl = new TCatalogOpRequest();
    TResultSetMetadata metadata = new TResultSetMetadata();
    if (analysis.isUseStmt()) {
      ddl.op_type = TCatalogOpType.USE;
      ddl.setUse_db_params(analysis.getUseStmt().toThrift());
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isShowTablesStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_TABLES;
      ddl.setShow_tables_params(analysis.getShowTablesStmt().toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("name", Type.STRING.toThrift())));
    } else if (analysis.isShowDbsStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_DBS;
      ddl.setShow_dbs_params(analysis.getShowDbsStmt().toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("name", Type.STRING.toThrift()),
          new TColumn("comment", Type.STRING.toThrift())));
    } else if (analysis.isShowDataSrcsStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_DATA_SRCS;
      ddl.setShow_data_srcs_params(analysis.getShowDataSrcsStmt().toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("name", Type.STRING.toThrift()),
          new TColumn("location", Type.STRING.toThrift()),
          new TColumn("class name", Type.STRING.toThrift()),
          new TColumn("api version", Type.STRING.toThrift())));
    } else if (analysis.isShowStatsStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_STATS;
      ddl.setShow_stats_params(analysis.getShowStatsStmt().toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("name", Type.STRING.toThrift())));
    } else if (analysis.isShowFunctionsStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_FUNCTIONS;
      ShowFunctionsStmt stmt = (ShowFunctionsStmt)analysis.getStmt();
      ddl.setShow_fns_params(stmt.toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("return type", Type.STRING.toThrift()),
          new TColumn("signature", Type.STRING.toThrift()),
          new TColumn("binary type", Type.STRING.toThrift()),
          new TColumn("is persistent", Type.STRING.toThrift())));
    } else if (analysis.isShowCreateTableStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_CREATE_TABLE;
      ddl.setShow_create_table_params(analysis.getShowCreateTableStmt().toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("result", Type.STRING.toThrift())));
    } else if (analysis.isShowCreateFunctionStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_CREATE_FUNCTION;
      ddl.setShow_create_function_params(analysis.getShowCreateFunctionStmt().toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("result", Type.STRING.toThrift())));
    } else if (analysis.isShowFilesStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_FILES;
      ddl.setShow_files_params(analysis.getShowFilesStmt().toThrift());
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isDescribeDbStmt()) {
      ddl.op_type = TCatalogOpType.DESCRIBE_DB;
      ddl.setDescribe_db_params(analysis.getDescribeDbStmt().toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("name", Type.STRING.toThrift()),
          new TColumn("location", Type.STRING.toThrift()),
          new TColumn("comment", Type.STRING.toThrift())));
    } else if (analysis.isDescribeTableStmt()) {
      ddl.op_type = TCatalogOpType.DESCRIBE_TABLE;
      DescribeTableStmt descStmt = analysis.getDescribeTableStmt();
      ddl.setDescribe_table_params(descStmt.toThrift());
      List<TColumn> columns = Lists.newArrayList(
          new TColumn("name", Type.STRING.toThrift()),
          new TColumn("type", Type.STRING.toThrift()),
          new TColumn("comment", Type.STRING.toThrift()));
      if (descStmt.getTable() instanceof FeKuduTable
          && descStmt.getOutputStyle() == TDescribeOutputStyle.MINIMAL) {
        columns.add(new TColumn("primary_key", Type.STRING.toThrift()));
        columns.add(new TColumn("nullable", Type.STRING.toThrift()));
        columns.add(new TColumn("default_value", Type.STRING.toThrift()));
        columns.add(new TColumn("encoding", Type.STRING.toThrift()));
        columns.add(new TColumn("compression", Type.STRING.toThrift()));
        columns.add(new TColumn("block_size", Type.STRING.toThrift()));
      }
      metadata.setColumns(columns);
    } else if (analysis.isAlterTableStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.ALTER_TABLE);
      req.setAlter_table_params(analysis.getAlterTableStmt().toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isAlterViewStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.ALTER_VIEW);
      req.setAlter_view_params(analysis.getAlterViewStmt().toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isCreateTableStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_TABLE);
      req.setCreate_table_params(analysis.getCreateTableStmt().toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isCreateTableAsSelectStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_TABLE_AS_SELECT);
      req.setCreate_table_params(
          analysis.getCreateTableAsSelectStmt().getCreateStmt().toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isCreateTableLikeStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_TABLE_LIKE);
      req.setCreate_table_like_params(analysis.getCreateTableLikeStmt().toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isCreateViewStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_VIEW);
      req.setCreate_view_params(analysis.getCreateViewStmt().toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isCreateDbStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_DATABASE);
      req.setCreate_db_params(analysis.getCreateDbStmt().toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isCreateUdfStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      CreateUdfStmt stmt = (CreateUdfStmt) analysis.getStmt();
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_FUNCTION);
      req.setCreate_fn_params(stmt.toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isCreateUdaStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_FUNCTION);
      CreateUdaStmt stmt = (CreateUdaStmt)analysis.getStmt();
      req.setCreate_fn_params(stmt.toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isCreateDataSrcStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_DATA_SOURCE);
      CreateDataSrcStmt stmt = (CreateDataSrcStmt)analysis.getStmt();
      req.setCreate_data_source_params(stmt.toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isComputeStatsStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.COMPUTE_STATS);
      req.setCompute_stats_params(analysis.getComputeStatsStmt().toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isDropDbStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.DROP_DATABASE);
      req.setDrop_db_params(analysis.getDropDbStmt().toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isDropTableOrViewStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      DropTableOrViewStmt stmt = analysis.getDropTableOrViewStmt();
      req.setDdl_type(stmt.isDropTable() ? TDdlType.DROP_TABLE : TDdlType.DROP_VIEW);
      req.setDrop_table_or_view_params(stmt.toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isTruncateStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      TruncateStmt stmt = analysis.getTruncateStmt();
      req.setDdl_type(TDdlType.TRUNCATE_TABLE);
      req.setTruncate_params(stmt.toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isDropFunctionStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.DROP_FUNCTION);
      DropFunctionStmt stmt = (DropFunctionStmt)analysis.getStmt();
      req.setDrop_fn_params(stmt.toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isDropDataSrcStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.DROP_DATA_SOURCE);
      DropDataSrcStmt stmt = (DropDataSrcStmt)analysis.getStmt();
      req.setDrop_data_source_params(stmt.toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isDropStatsStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.DROP_STATS);
      DropStatsStmt stmt = (DropStatsStmt) analysis.getStmt();
      req.setDrop_stats_params(stmt.toThrift());
      ddl.setDdl_params(req);
    } else if (analysis.isResetMetadataStmt()) {
      ddl.op_type = TCatalogOpType.RESET_METADATA;
      ResetMetadataStmt resetMetadataStmt = (ResetMetadataStmt) analysis.getStmt();
      TResetMetadataRequest req = resetMetadataStmt.toThrift();
      ddl.setReset_metadata_params(req);
      metadata.setColumns(Collections.emptyList());
    } else if (analysis.isShowRolesStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_ROLES;
      ShowRolesStmt showRolesStmt = (ShowRolesStmt) analysis.getStmt();
      ddl.setShow_roles_params(showRolesStmt.toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("role_name", Type.STRING.toThrift())));
    } else if (analysis.isShowGrantPrincipalStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_GRANT_PRINCIPAL;
      ShowGrantPrincipalStmt showGrantPrincipalStmt =
          (ShowGrantPrincipalStmt) analysis.getStmt();
      ddl.setShow_grant_principal_params(showGrantPrincipalStmt.toThrift());
      metadata.setColumns(Arrays.asList(new TColumn("name", Type.STRING.toThrift())));
    } else if (analysis.isCreateDropRoleStmt()) {
      CreateDropRoleStmt createDropRoleStmt = (CreateDropRoleStmt) analysis.getStmt();
      TCreateDropRoleParams params = createDropRoleStmt.toThrift();
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(params.isIs_drop() ? TDdlType.DROP_ROLE : TDdlType.CREATE_ROLE);
      req.setCreate_drop_role_params(params);
      ddl.op_type = TCatalogOpType.DDL;
      ddl.setDdl_params(req);
    } else if (analysis.isGrantRevokeRoleStmt()) {
      GrantRevokeRoleStmt grantRoleStmt = (GrantRevokeRoleStmt) analysis.getStmt();
      TGrantRevokeRoleParams params = grantRoleStmt.toThrift();
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(params.isIs_grant() ? TDdlType.GRANT_ROLE : TDdlType.REVOKE_ROLE);
      req.setGrant_revoke_role_params(params);
      ddl.op_type = TCatalogOpType.DDL;
      ddl.setDdl_params(req);
    } else if (analysis.isGrantRevokePrivStmt()) {
      GrantRevokePrivStmt grantRevokePrivStmt = (GrantRevokePrivStmt) analysis.getStmt();
      TGrantRevokePrivParams params = grantRevokePrivStmt.toThrift();
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(params.isIs_grant() ?
          TDdlType.GRANT_PRIVILEGE : TDdlType.REVOKE_PRIVILEGE);
      req.setGrant_revoke_priv_params(params);
      ddl.op_type = TCatalogOpType.DDL;
      ddl.setDdl_params(req);
    } else if (analysis.isCommentOnStmt()) {
      CommentOnStmt commentOnStmt = analysis.getCommentOnStmt();
      TCommentOnParams params = commentOnStmt.toThrift();
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.COMMENT_ON);
      req.setComment_on_params(params);
      ddl.op_type = TCatalogOpType.DDL;
      ddl.setDdl_params(req);
    } else if (analysis.isAlterDbStmt()) {
      AlterDbStmt alterDbStmt = analysis.getAlterDbStmt();
      TAlterDbParams params = alterDbStmt.toThrift();
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.ALTER_DATABASE);
      req.setAlter_db_params(params);
      ddl.op_type = TCatalogOpType.DDL;
      ddl.setDdl_params(req);
    } else if (analysis.isTestCaseStmt()){
      CopyTestCaseStmt stmt = (CopyTestCaseStmt) analysis.getStmt();
      TCopyTestCaseReq req = new TCopyTestCaseReq(stmt.getHdfsPath());
      TDdlExecRequest ddlReq = new TDdlExecRequest();
      ddlReq.setCopy_test_case_params(req);
      ddlReq.setDdl_type(TDdlType.COPY_TESTCASE);
      ddl.op_type = TCatalogOpType.DDL;
      ddl.setDdl_params(ddlReq);
    } else {
      throw new IllegalStateException("Unexpected CatalogOp statement type.");
    }
    // All DDL commands return a string summarizing the outcome of the DDL.
    if (ddl.op_type == TCatalogOpType.DDL) {
      metadata.setColumns(Arrays.asList(new TColumn("summary", Type.STRING.toThrift())));
    }
    result.setResult_set_metadata(metadata);
    ddl.setSync_ddl(result.getQuery_options().isSync_ddl());
    result.setCatalog_op_request(ddl);
    if (ddl.getOp_type() == TCatalogOpType.DDL) {
      TCatalogServiceRequestHeader header = new TCatalogServiceRequestHeader();
      header.setRequesting_user(analysis.getAnalyzer().getUser().getName());
      TQueryCtx queryCtx = analysis.getAnalyzer().getQueryCtx();
      header.setClient_ip(queryCtx.getSession().getNetwork_address().getHostname());
      TClientRequest clientRequest = queryCtx.getClient_request();
      header.setRedacted_sql_stmt(clientRequest.isSetRedacted_stmt() ?
          clientRequest.getRedacted_stmt() : clientRequest.getStmt());
      ddl.getDdl_params().setHeader(header);
      ddl.getDdl_params().setSync_ddl(ddl.isSync_ddl());
    }
    if (ddl.getOp_type() == TCatalogOpType.RESET_METADATA) {
      ddl.getReset_metadata_params().setSync_ddl(ddl.isSync_ddl());
    }
  }

  /**
   * Loads a table or partition with one or more data files. If the "overwrite" flag
   * in the request is true, all existing data in the table/partition will be replaced.
   * If the "overwrite" flag is false, the files will be added alongside any existing
   * data files.
   */
  public TLoadDataResp loadTableData(TLoadDataReq request) throws ImpalaException,
      IOException {
    TTableName tableName = request.getTable_name();
    RetryTracker retries = new RetryTracker(
        String.format("load table data %s.%s", tableName.db_name, tableName.table_name));
    while (true) {
      try {
        return doLoadTableData(request);
      } catch(InconsistentMetadataFetchException e) {
        retries.handleRetryOrThrow(e);
      }
    }
  }

  private TLoadDataResp doLoadTableData(TLoadDataReq request) throws ImpalaException,
      IOException {
    TableName tableName = TableName.fromThrift(request.getTable_name());

    // Get the destination for the load. If the load is targeting a partition,
    // this the partition location. Otherwise this is the table location.
    String destPathString = null;
    FeCatalog catalog = getCatalog();
    if (request.isSetPartition_spec()) {
      destPathString = catalog.getHdfsPartition(tableName.getDb(),
          tableName.getTbl(), request.getPartition_spec()).getLocation();
    } else {
      destPathString = catalog.getTable(tableName.getDb(), tableName.getTbl())
          .getMetaStoreTable().getSd().getLocation();
    }

    Path destPath = new Path(destPathString);
    Path sourcePath = new Path(request.source_path);
    FileSystem destFs = destPath.getFileSystem(FileSystemUtil.getConfiguration());
    FileSystem sourceFs = sourcePath.getFileSystem(FileSystemUtil.getConfiguration());

    // Create a temporary directory within the final destination directory to stage the
    // file move.
    Path tmpDestPath = FileSystemUtil.makeTmpSubdirectory(destPath);

    int filesLoaded = 0;
    if (sourceFs.isDirectory(sourcePath)) {
      filesLoaded = FileSystemUtil.relocateAllVisibleFiles(sourcePath, tmpDestPath);
    } else {
      FileSystemUtil.relocateFile(sourcePath, tmpDestPath, true);
      filesLoaded = 1;
    }

    // If this is an OVERWRITE, delete all files in the destination.
    if (request.isOverwrite()) {
      FileSystemUtil.deleteAllVisibleFiles(destPath);
    }

    // Move the files from the temporary location to the final destination.
    FileSystemUtil.relocateAllVisibleFiles(tmpDestPath, destPath);
    // Cleanup the tmp directory.
    destFs.delete(tmpDestPath, true);
    TLoadDataResp response = new TLoadDataResp();
    TColumnValue col = new TColumnValue();
    String loadMsg = String.format(
        "Loaded %d file(s). Total files in destination location: %d",
        filesLoaded, FileSystemUtil.getTotalNumVisibleFiles(destPath));
    col.setString_val(loadMsg);
    response.setLoad_summary(new TResultRow(Lists.newArrayList(col)));
    return response;
  }

  /**
   * Parses and plans a query in order to generate its explain string. This method does
   * not increase the query id counter.
   */
  public String getExplainString(TQueryCtx queryCtx) throws ImpalaException {
    PlanCtx planCtx = new PlanCtx(queryCtx);
    createExecRequest(planCtx);
    return planCtx.getExplainString();
  }

  public TGetCatalogMetricsResult getCatalogMetrics() throws ImpalaException {
    TGetCatalogMetricsResult resp = new TGetCatalogMetricsResult();
    for (FeDb db : getCatalog().getDbs(PatternMatcher.MATCHER_MATCH_ALL)) {
      resp.num_dbs++;
      resp.num_tables += db.getAllTableNames().size();
    }
    FeCatalogUtils.populateCacheMetrics(getCatalog(), resp);
    return resp;
  }

  /**
   * Keeps track of retries when handling InconsistentMetadataFetchExceptions.
   * Whenever a Catalog object is acquired (e.g., getCatalog), operations that access
   * finer-grained objects, such as tables and partitions, can throw such a runtime
   * exception. Inconsistent metadata comes up due to interleaving catalog object updates
   * with retrieving those objects. Instead of bubbling up the issue to the user, retrying
   * can get the user's operation to run on a consistent snapshot and to succeed.
   * Retries are *not* needed for accessing top-level objects such as databases, since
   * they do not have a parent, so cannot be inconsistent.
   * TODO: this class is typically used in a loop at the call-site. replace with lambdas
   *       in Java 8 to simplify the looping boilerplate.
   */
  public static class RetryTracker {
    // Number of exceptions seen
    private int attempt_ = 0;
    // Message to add when logging retries.
    private final String msg_;

    public RetryTracker(String msg) { msg_ = msg; }

    /**
     * Record a retry. If the number of retries exceeds to configured maximum, the
     * exception is thrown. Otherwise, the number of retries is incremented and logged.
     * TODO: record these retries in the profile as done for query retries.
     */
    public void handleRetryOrThrow(InconsistentMetadataFetchException exception) {
      if (attempt_++ >= INCONSISTENT_METADATA_NUM_RETRIES) throw exception;
      if (attempt_ > 1) {
        // Back-off a bit on later retries.
        Uninterruptibles.sleepUninterruptibly(200 * attempt_, TimeUnit.MILLISECONDS);
      }
      LOG.warn(String.format("Retried %s: (retry #%s of %s)", msg_,
          attempt_, INCONSISTENT_METADATA_NUM_RETRIES), exception);
    }
  }

  /**
   * A Callable wrapper used for checking authorization to tables/databases.
   */
  private class CheckAuthorization implements Callable<Boolean> {
    private final String dbName_;
    private final String tblName_;
    private final String owner_;
    private final User user_;

    public CheckAuthorization(String dbName, String tblName, String owner, User user) {
      // dbName and user cannot be null, tblName and owner can be null.
      Preconditions.checkNotNull(dbName);
      Preconditions.checkNotNull(user);
      dbName_ = dbName;
      tblName_ = tblName;
      owner_ = owner;
      user_ = user;
    }

    @Override
    public Boolean call() throws Exception {
      return new Boolean(isAccessibleToUser(dbName_, tblName_, owner_, user_));
    }
  }

  /**
   * Returns all tables in database 'dbName' that match the pattern of 'matcher' and are
   * accessible to 'user'.
   */
  public List<String> getTableNames(String dbName, PatternMatcher matcher,
      User user) throws ImpalaException {
    RetryTracker retries = new RetryTracker(
        String.format("fetching %s table names", dbName));
    while (true) {
      try {
        return doGetTableNames(dbName, matcher, user);
      } catch(InconsistentMetadataFetchException e) {
        retries.handleRetryOrThrow(e);
      }
    }
  }

  /**
   * This method filters out elements from the given list based on the the results
   * of the pendingCheckTasks.
   */
  private void filterUnaccessibleElements(List<Future<Boolean>> pendingCheckTasks,
    List<?> checkList) throws InternalException {
    int failedCheckTasks = 0;
    int index = 0;
    Iterator<?> iter = checkList.iterator();

    Preconditions.checkState(checkList.size() == pendingCheckTasks.size());
    while (iter.hasNext()) {
      iter.next();
      try {
        if (!pendingCheckTasks.get(index).get()) iter.remove();
        index++;
      } catch (ExecutionException | InterruptedException e) {
        failedCheckTasks++;
        LOG.error("Encountered an error checking access", e);
        break;
      }
    }

    if (failedCheckTasks > 0)
      throw new InternalException("Failed to check access." +
          "Check the server log for more details.");
  }

  private List<String> doGetTableNames(String dbName, PatternMatcher matcher,
      User user) throws ImpalaException {
    FeCatalog catalog = getCatalog();
    List<String> tblNames = catalog.getTableNames(dbName, matcher);

    boolean needsAuthChecks = authzFactory_.getAuthorizationConfig().isEnabled()
                              && !userHasAccessForWholeDb(user, dbName);

    if (needsAuthChecks) {
      List<Future<Boolean>> pendingCheckTasks = Lists.newArrayList();
      Iterator<String> iter = tblNames.iterator();
      while (iter.hasNext()) {
        String tblName = iter.next();
        // Get the owner information. Do not force load the table, only get it
        // from cache, if it is already loaded. This means that we cannot access
        // ownership information for unloaded tables and they will not be listed
        // here. This might result in situations like 'show tables' not listing
        // 'owned' tables for a given user just because the metadata is not loaded.
        // TODO(IMPALA-8937): Figure out a way to load Table/Database ownership
        // information when fetching the table lists from HMS.
        FeTable table = catalog.getTableIfCached(dbName, tblName);
        String tableOwner = table.getOwnerUser();
        if (tableOwner == null) {
          LOG.info("Table {} not yet loaded, ignoring it in table listing.",
            dbName + "." + tblName);
        }
        pendingCheckTasks.add(checkAuthorizationPool_.submit(
            new CheckAuthorization(dbName, tblName, tableOwner, user)));
      }

      filterUnaccessibleElements(pendingCheckTasks, tblNames);
    }

    return tblNames;
  }

  /**
   * Returns a list of columns of a table using 'matcher' and are accessible
   * to the given user.
   */
  public List<Column> getColumns(FeTable table, PatternMatcher matcher,
      User user) throws InternalException {
    Preconditions.checkNotNull(table);
    Preconditions.checkNotNull(matcher);
    List<Column> columns = Lists.newArrayList();
    for (Column column: table.getColumnsInHiveOrder()) {
      String colName = column.getName();
      if (!matcher.matches(colName)) continue;
      if (authzFactory_.getAuthorizationConfig().isEnabled()) {
        PrivilegeRequest privilegeRequest = new PrivilegeRequestBuilder(
            authzFactory_.getAuthorizableFactory())
            .any().onColumn(table.getTableName().getDb(), table.getTableName().getTbl(),
                colName, table.getOwnerUser()).build();
        if (!authzChecker_.get().hasAccess(user, privilegeRequest)) continue;
      }
      columns.add(column);
    }
    return columns;
  }

  /**
   * Returns a list of primary keys for a given table only if the user has access to all
   * the columns that form the primary key. This is because all SQLPrimaryKeys for a
   * given table together form the primary key.
   */
  public List<SQLPrimaryKey> getPrimaryKeys(FeTable table, User user)
      throws InternalException {
    Preconditions.checkNotNull(table);
    List<SQLPrimaryKey> pkList;
    pkList = table.getSqlConstraints().getPrimaryKeys();
    Preconditions.checkNotNull(pkList);
    for (SQLPrimaryKey pk : pkList) {
      if (authzFactory_.getAuthorizationConfig().isEnabled()) {
        PrivilegeRequest privilegeRequest = new PrivilegeRequestBuilder(
            authzFactory_.getAuthorizableFactory())
            .any().onColumn(table.getTableName().getDb(), table.getTableName().getTbl(),
            pk.getColumn_name(), table.getOwnerUser()).build();
        // If any of the pk columns is not accessible to the user, we return an empty
        // list.
        if (!authzChecker_.get().hasAccess(user, privilegeRequest)) {
          return new ArrayList<>();
        }
      }
    }
    return pkList;
  }

  /**
   * Returns a list of foreign keys for a given table only if both the primary key
   * column and the foreign key columns are accessible to user.
   */
  public List<SQLForeignKey> getForeignKeys(FeTable table, User user)
      throws InternalException {
    Preconditions.checkNotNull(table);
    // Consider an example:
    // A child table has the following foreign keys.
    // 1) A composite foreign key (col1, col2) referencing parent_table_1 columns (a, b).
    // 2) A foreign key (col3) referencing a different parent_table_2 column (c).
    // In the above scenario, three "SQLForeignKey" structures are stored in HMS. Two
    // SQLForiegnKey for 1) above which share the same FkName and will have key_seq 1
    // and 2 respectively and one for 2) above. In other words, within a foreign key
    // definition, we will have one "SQLForeignKey" structure for each column in the
    // definition. They share fkName but will have different key_seq numbers. For the
    // purpose of authorization, we do not want to show only a part of a sequence of
    // keys. So if any of the keys in a sequence has incorrect privileges, we omit the
    // entire sequence. For instance, in a request for all the foreign keys on
    // child_table above, if we discover that the user does not have privilege on col1
    // in the child_table, we omit both the "SQLForeignKey" associated with col1 and col2
    // but we return the "SQLFOreignKey" for col3.
    Set<String> omitList = new HashSet<>();
    List<SQLForeignKey> fkList = new ArrayList<>();
    List<SQLForeignKey> foreignKeys = table.getSqlConstraints().getForeignKeys();
    Preconditions.checkNotNull(foreignKeys);
    for (SQLForeignKey fk : foreignKeys) {
      String fkName = fk.getFk_name();
      if (!omitList.contains(fkName)) {
        if (authzFactory_.getAuthorizationConfig().isEnabled()) {
          PrivilegeRequest fkPrivilegeRequest = new PrivilegeRequestBuilder(
              authzFactory_.getAuthorizableFactory())
              .any()
              .onColumn(table.getTableName().getDb(), table.getTableName().getTbl(),
              fk.getFkcolumn_name(), table.getOwnerUser()).build();

          // Build privilege request for PK table.
          FeTable pkTable =
              getCatalog().getTableNoThrow(fk.getPktable_db(), fk.getPktable_name());
          PrivilegeRequest pkPrivilegeRequest = new PrivilegeRequestBuilder(
              authzFactory_.getAuthorizableFactory())
              .any().onColumn(pkTable.getTableName().getDb(),
              pkTable.getTableName().getTbl(), fk.getPkcolumn_name(),
              pkTable.getOwnerUser()).build();
          if (!authzChecker_.get().hasAccess(user, fkPrivilegeRequest) ||
              !authzChecker_.get().hasAccess(user, pkPrivilegeRequest)) {
            omitList.add(fkName);
          }
        }
      }
    }
    for (SQLForeignKey fk : foreignKeys) {
      if (!omitList.contains(fk.getFk_name())) {
        fkList.add(fk);
      }
    }
    return fkList;
  }

  /**
   * Returns all databases in catalog cache that match the pattern of 'matcher' and are
   * accessible to 'user'.
   */
  public List<? extends FeDb> getDbs(PatternMatcher matcher, User user)
      throws InternalException {
    List<? extends FeDb> dbs = getCatalog().getDbs(matcher);

    boolean needsAuthChecks = authzFactory_.getAuthorizationConfig().isEnabled()
                              && !userHasAccessForWholeServer(user);

    // Filter out the databases the user does not have permissions on.
    if (needsAuthChecks) {
      Iterator<? extends FeDb> iter = dbs.iterator();
      List<Future<Boolean>> pendingCheckTasks = Lists.newArrayList();
      while (iter.hasNext()) {
        FeDb db = iter.next();
        pendingCheckTasks.add(checkAuthorizationPool_.submit(
            new CheckAuthorization(db.getName(), null, db.getOwnerUser(), user)));
      }

      filterUnaccessibleElements(pendingCheckTasks, dbs);
    }

    return dbs;
  }

  /**
   * Check whether table/database is accessible to given user.
   */
  private boolean isAccessibleToUser(String dbName, String tblName,
      String owner, User user) throws InternalException {
    Preconditions.checkNotNull(dbName);
    if (tblName == null &&
        dbName.toLowerCase().equals(Catalog.DEFAULT_DB.toLowerCase())) {
      // Default DB should always be shown.
      return true;
    }

    PrivilegeRequestBuilder builder = new PrivilegeRequestBuilder(
        authzFactory_.getAuthorizableFactory())
        .anyOf(minPrivilegeSetForShowStmts_);
    if (tblName == null) {
      // Check database
      builder = builder.onAnyColumn(dbName, owner);
    } else {
      // Check table
      builder = builder.onAnyColumn(dbName, tblName, owner);
    }

    return authzChecker_.get().hasAnyAccess(user, builder.buildSet());
  }

  /**
   * Check whether the whole server is accessible to given user.
   */
  private boolean userHasAccessForWholeServer(User user)
      throws InternalException {
    if (authEngineSupportsDenyRules()) return false;
    PrivilegeRequestBuilder builder = new PrivilegeRequestBuilder(
        authzFactory_.getAuthorizableFactory()).anyOf(minPrivilegeSetForShowStmts_)
        .onServer(authzFactory_.getAuthorizationConfig().getServerName());
    return authzChecker_.get().hasAnyAccess(user, builder.buildSet());
  }

  /**
   * Check whether the whole database is accessible to given user.
   */
  private boolean userHasAccessForWholeDb(User user, String dbName)
      throws InternalException, DatabaseNotFoundException {
    if (authEngineSupportsDenyRules()) return false;
    // FeDb is needed to respect ownership in Ranger, dbName would be enough for
    // the privilege request otherwise.
    FeDb db = getCatalog().getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    PrivilegeRequestBuilder builder = new PrivilegeRequestBuilder(
        authzFactory_.getAuthorizableFactory()).anyOf(minPrivilegeSetForShowStmts_)
        .onDb(db);
    return authzChecker_.get().hasAnyAccess(user, builder.buildSet());
  }

  /**
   * Returns whether the authorization engine supports deny rules. If it does,
   * then a privilege on a higher level object does not imply privilege on lower
   * level objects in the hierarchy.
   */
  private boolean authEngineSupportsDenyRules() {
    // Sentry did not support deny rules, but Ranger does. So, this now returns true.
    // TODO: could check config for Ranger and return true if deny rules are disabled
    return true;
  }

  /**
   * Returns all data sources that match the pattern. If pattern is null,
   * matches all data sources.
   */
  public List<? extends FeDataSource> getDataSrcs(String pattern) {
    // TODO: handle InconsistentMetadataException for data sources.
    return getCatalog().getDataSources(
        PatternMatcher.createHivePatternMatcher(pattern));
  }

  /**
   * Generate result set and schema for a SHOW COLUMN STATS command.
   */
  public TResultSet getColumnStats(String dbName, String tableName)
      throws ImpalaException {
    RetryTracker retries = new RetryTracker(
        String.format("fetching column stats from %s.%s", dbName, tableName));
    while (true) {
      try {
        return doGetColumnStats(dbName, tableName);
      } catch(InconsistentMetadataFetchException e) {
        retries.handleRetryOrThrow(e);
      }
    }
  }

  private TResultSet doGetColumnStats(String dbName, String tableName)
      throws ImpalaException {
    FeTable table = getCatalog().getTable(dbName, tableName);
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);
    resultSchema.addToColumns(new TColumn("Column", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Type", Type.STRING.toThrift()));
    resultSchema.addToColumns(
        new TColumn("#Distinct Values", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("#Nulls", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("Max Size", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("Avg Size", Type.DOUBLE.toThrift()));
    resultSchema.addToColumns(new TColumn("#Trues", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("#Falses", Type.BIGINT.toThrift()));

    for (Column c: table.getColumnsInHiveOrder()) {
      TResultRowBuilder rowBuilder = new TResultRowBuilder();
      // Add name, type, NDVs, numNulls, max size and avg size.
      rowBuilder.add(c.getName())
          .add(c.getType().toSql())
          .add(c.getStats().getNumDistinctValues())
          .add(c.getStats().getNumNulls())
          .add(c.getStats().getMaxSize())
          .add(c.getStats().getAvgSize())
          .add(c.getStats().getNumTrues())
          .add(c.getStats().getNumFalses());
      result.addToRows(rowBuilder.get());
    }
    return result;
  }

  /**
   * Generate result set and schema for a SHOW TABLE STATS command.
   */
  public TResultSet getTableStats(String dbName, String tableName, TShowStatsOp op)
      throws ImpalaException {
    RetryTracker retries = new RetryTracker(
        String.format("fetching table stats from %s.%s", dbName, tableName));
    while (true) {
      try {
        return doGetTableStats(dbName, tableName, op);
      } catch(InconsistentMetadataFetchException e) {
        retries.handleRetryOrThrow(e);
      }
    }
  }

  private TResultSet doGetTableStats(String dbName, String tableName, TShowStatsOp op)
      throws ImpalaException {
    FeTable table = getCatalog().getTable(dbName, tableName);
    if (table instanceof FeFsTable) {
      return ((FeFsTable) table).getTableStats();
    } else if (table instanceof FeHBaseTable) {
      return ((FeHBaseTable) table).getTableStats();
    } else if (table instanceof FeDataSourceTable) {
      return ((FeDataSourceTable) table).getTableStats();
    } else if (table instanceof FeKuduTable) {
      if (op == TShowStatsOp.RANGE_PARTITIONS) {
        return FeKuduTable.Utils.getRangePartitions((FeKuduTable) table);
      } else if (op == TShowStatsOp.PARTITIONS) {
        return FeKuduTable.Utils.getPartitions((FeKuduTable) table);
      } else {
        Preconditions.checkState(op == TShowStatsOp.TABLE_STATS);
        return FeKuduTable.Utils.getTableStats((FeKuduTable) table);
      }
    } else {
      throw new InternalException("Invalid table class: " + table.getClass());
    }
  }

  /**
   * Returns all function signatures that match the pattern. If pattern is null,
   * matches all functions. If exactMatch is true, treats fnPattern as a function
   * name instead of pattern and returns exact match only.
   */
  public List<Function> getFunctions(TFunctionCategory category,
      String dbName, String fnPattern, boolean exactMatch)
      throws DatabaseNotFoundException {
    RetryTracker retries = new RetryTracker(
        String.format("fetching functions from %s", dbName));
    while (true) {
      try {
        return doGetFunctions(category, dbName, fnPattern, exactMatch);
      } catch(InconsistentMetadataFetchException e) {
        retries.handleRetryOrThrow(e);
      }
    }
  }

  private List<Function> doGetFunctions(TFunctionCategory category,
      String dbName, String fnPattern, boolean exactMatch)
      throws DatabaseNotFoundException {
    FeDb db = getCatalog().getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    List<Function> fns;
    if (exactMatch) {
      Preconditions.checkNotNull(fnPattern, "Invalid function name");
      fns = db.getFunctions(category, fnPattern);
    } else {
      fns = db.getFunctions(
        category, PatternMatcher.createHivePatternMatcher(fnPattern));
    }
    Collections.sort(fns,
        new Comparator<Function>() {
          @Override
          public int compare(Function f1, Function f2) {
            return f1.signatureString().compareTo(f2.signatureString());
          }
        });
    return fns;
  }

  /**
   * Returns database metadata, in the specified database. Throws an exception if db is
   * not found or if there is an error loading the db metadata.
   */
  public TDescribeResult describeDb(String dbName, TDescribeOutputStyle outputStyle)
      throws ImpalaException {
    FeDb db = getCatalog().getDb(dbName);
    return DescribeResultFactory.buildDescribeDbResult(db, outputStyle);
  }

  /**
   * Returns table metadata, such as the column descriptors, in the specified table.
   * Throws an exception if the table or db is not found or if there is an error loading
   * the table metadata.
   */
  public TDescribeResult describeTable(TTableName tableName,
      TDescribeOutputStyle outputStyle, User user) throws ImpalaException {
    RetryTracker retries = new RetryTracker(
        String.format("fetching table %s.%s", tableName.db_name, tableName.table_name));
    while (true) {
      try {
        return doDescribeTable(tableName, outputStyle, user);
      } catch(InconsistentMetadataFetchException e) {
        retries.handleRetryOrThrow(e);
      }
    }
  }

  private TDescribeResult doDescribeTable(TTableName tableName,
      TDescribeOutputStyle outputStyle, User user) throws ImpalaException {
    FeTable table = getCatalog().getTable(tableName.db_name,
        tableName.table_name);
    List<Column> filteredColumns;
    if (authzFactory_.getAuthorizationConfig().isEnabled()) {
      // First run a table check
      PrivilegeRequest privilegeRequest = new PrivilegeRequestBuilder(
          authzFactory_.getAuthorizableFactory())
          .allOf(Privilege.VIEW_METADATA).onTable(table).build();
      if (!authzChecker_.get().hasAccess(user, privilegeRequest)) {
        // Filter out columns that the user is not authorized to see.
        filteredColumns = new ArrayList<Column>();
        for (Column col: table.getColumnsInHiveOrder()) {
          String colName = col.getName();
          privilegeRequest = new PrivilegeRequestBuilder(
              authzFactory_.getAuthorizableFactory())
              .allOf(Privilege.VIEW_METADATA)
              .onColumn(table.getDb().getName(),
                  table.getName(), colName, table.getOwnerUser()).build();
          if (authzChecker_.get().hasAccess(user, privilegeRequest)) {
            filteredColumns.add(col);
          }
        }
      } else {
        // User has table-level access
        filteredColumns = table.getColumnsInHiveOrder();
      }
    } else {
      // Authorization is disabled
      filteredColumns = table.getColumnsInHiveOrder();
    }
    if (outputStyle == TDescribeOutputStyle.MINIMAL) {
      if (!(table instanceof FeKuduTable)) {
        return DescribeResultFactory.buildDescribeMinimalResult(
            Column.columnsToStruct(filteredColumns));
      }
      return DescribeResultFactory.buildKuduDescribeMinimalResult(filteredColumns);
    } else {
      Preconditions.checkArgument(outputStyle == TDescribeOutputStyle.FORMATTED ||
          outputStyle == TDescribeOutputStyle.EXTENDED);
      TDescribeResult result = DescribeResultFactory.buildDescribeFormattedResult(table,
          filteredColumns);
      // Filter out LOCATION text
      if (authzFactory_.getAuthorizationConfig().isEnabled()) {
        PrivilegeRequest privilegeRequest = new PrivilegeRequestBuilder(
            authzFactory_.getAuthorizableFactory())
            .allOf(Privilege.VIEW_METADATA).onTable(table).build();
        // Only filter if the user doesn't have table access.
        if (!authzChecker_.get().hasAccess(user, privilegeRequest)) {
          List<TResultRow> results = new ArrayList<>();
          for(TResultRow row: result.getResults()) {
            String stringVal = row.getColVals().get(0).getString_val();
            if (!stringVal.contains("Location")) {
              results.add(row);
            }
          }
          result.setResults(results);
        }
      }
      return result;
    }
  }

  /**
   * Waits indefinitely for the local catalog to be ready. The catalog is "ready" after
   * the first catalog update with a version > INITIAL_CATALOG_VERSION is received from
   * the statestore.
   *
   * @see ImpaladCatalog#isReady()
   */
  public void waitForCatalog() {
    LOG.info("Waiting for first catalog update from the statestore.");
    int numTries = 0;
    long startTimeMs = System.currentTimeMillis();
    while (true) {
      if (getCatalog().isReady()) {
        LOG.info("Local catalog initialized after: " +
            (System.currentTimeMillis() - startTimeMs) + " ms.");
        return;
      }
      LOG.info("Waiting for local catalog to be initialized, attempt: " + numTries);
      getCatalog().waitForCatalogUpdate(MAX_CATALOG_UPDATE_WAIT_TIME_MS);
      ++numTries;
    }
  }

  /**
   * Return a TPlanExecInfo corresponding to the plan with root fragment 'planRoot'.
   */
  private TPlanExecInfo createPlanExecInfo(PlanFragment planRoot, Planner planner,
      TQueryCtx queryCtx, TQueryExecRequest queryExecRequest) {
    TPlanExecInfo result = new TPlanExecInfo();
    List<PlanFragment> fragments = planRoot.getFragmentsInPlanPreorder();

    // collect ScanNodes
    List<ScanNode> scanNodes = Lists.newArrayList();
    for (PlanFragment fragment : fragments) {
      fragment.collectPlanNodes(
        Predicates.instanceOf(ScanNode.class), scanNodes);
    }

    // Set scan ranges/locations for scan nodes.
    LOG.trace("get scan range locations");
    Set<TTableName> tablesMissingStats = Sets.newTreeSet();
    Set<TTableName> tablesWithCorruptStats = Sets.newTreeSet();
    Set<TTableName> tablesWithMissingDiskIds = Sets.newTreeSet();
    for (ScanNode scanNode: scanNodes) {
      result.putToPer_node_scan_ranges(
          scanNode.getId().asInt(), scanNode.getScanRangeSpecs());

      TTableName tableName = scanNode.getTupleDesc().getTableName().toThrift();
      if (scanNode.isTableMissingStats()) tablesMissingStats.add(tableName);
      if (scanNode.hasCorruptTableStats()) tablesWithCorruptStats.add(tableName);
      if (scanNode instanceof HdfsScanNode &&
          ((HdfsScanNode) scanNode).hasMissingDiskIds()) {
        tablesWithMissingDiskIds.add(tableName);
      }
    }

    // Clear pre-existing lists to avoid adding duplicate entries in FE tests.
    queryCtx.unsetTables_missing_stats();
    queryCtx.unsetTables_with_corrupt_stats();
    for (TTableName tableName: tablesMissingStats) {
      queryCtx.addToTables_missing_stats(tableName);
    }
    for (TTableName tableName: tablesWithCorruptStats) {
      queryCtx.addToTables_with_corrupt_stats(tableName);
    }
    for (TTableName tableName: tablesWithMissingDiskIds) {
      queryCtx.addToTables_missing_diskids(tableName);
    }

    // The fragment at this point has all state set, serialize it to thrift.
    for (PlanFragment fragment: fragments) {
      TPlanFragment thriftFragment = fragment.toThrift();
      result.addToFragments(thriftFragment);
    }

    return result;
  }

  /**
   * Create a populated TQueryExecRequest, corresponding to the supplied planner.
   */
  private TQueryExecRequest createExecRequest(
      Planner planner, PlanCtx planCtx) throws ImpalaException {
    TQueryCtx queryCtx = planner.getQueryCtx();
    List<PlanFragment> planRoots = planner.createPlans();
    if (planCtx.planCaptureRequested()) {
      planCtx.plan_ = planRoots;
    }

    // Compute resource requirements of the final plans.
    TQueryExecRequest result = new TQueryExecRequest();
    planner.computeResourceReqs(planRoots, queryCtx, result);

    // create per-plan exec info;
    // also assemble list of names of tables with missing or corrupt stats for
    // assembling a warning message
    for (PlanFragment planRoot: planRoots) {
      result.addToPlan_exec_info(
          createPlanExecInfo(planRoot, planner, queryCtx, result));
    }

    // Optionally disable spilling in the backend. Allow spilling if there are plan hints
    // or if all tables have stats.
    boolean disableSpilling =
        queryCtx.client_request.query_options.isDisable_unsafe_spills()
          && queryCtx.isSetTables_missing_stats()
          && !queryCtx.tables_missing_stats.isEmpty()
          && !planner.getAnalysisResult().getAnalyzer().hasPlanHints();
    queryCtx.setDisable_spilling(disableSpilling);

    // assign fragment idx
    int idx = 0;
    for (TPlanExecInfo planExecInfo: result.plan_exec_info) {
      for (TPlanFragment fragment: planExecInfo.fragments) fragment.setIdx(idx++);
    }

    // create EXPLAIN output after setting everything else
    result.setQuery_ctx(queryCtx);  // needed by getExplainString()
    List<PlanFragment> allFragments = planRoots.get(0).getNodesPreOrder();
    planCtx.explainBuf_.append(planner.getExplainString(allFragments, result));
    result.setQuery_plan(planCtx.getExplainString());

    return result;
  }

  /**
   * Create a TExecRequest for the query and query context provided in the plan
   * context. Fills in the EXPLAIN string and optionally the internal plan tree.
   */
  public TExecRequest createExecRequest(PlanCtx planCtx)
      throws ImpalaException {
    // Timeline of important events in the planning process, used for debugging
    // and profiling.
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      EventSequence timeline = new EventSequence("Query Compilation");
      TExecRequest result = getTExecRequest(planCtx, timeline);
      timeline.markEvent("Planning finished");
      result.setTimeline(timeline.toThrift());
      result.setProfile(FrontendProfile.getCurrent().emitAsThrift());
      return result;
    }
  }

  /**
   * Marks 'timeline' with the number of query planning retries that were needed.
   * Includes a 'msg' that explains the cause of retries. If there were no retries, then
   * 'timeline' is not written.
   */
  private void markTimelineRetries(int numRetries, String msg, EventSequence timeline) {
    if (numRetries == 0) return;
    timeline.markEvent(
        String.format("Retried query planning due to inconsistent metadata "
            + "%s of %s times: ",numRetries, INCONSISTENT_METADATA_NUM_RETRIES) + msg);
  }

  private TExecRequest getTExecRequest(PlanCtx planCtx, EventSequence timeline)
      throws ImpalaException {
    TQueryCtx queryCtx = planCtx.getQueryContext();
    LOG.info("Analyzing query: " + queryCtx.client_request.stmt + " db: "
        + queryCtx.session.database);

    int attempt = 0;
    String retryMsg = "";
    while (true) {
      try {
        TExecRequest req = doCreateExecRequest(planCtx, timeline);
        markTimelineRetries(attempt, retryMsg, timeline);
        return req;
      } catch (InconsistentMetadataFetchException e) {
        if (attempt++ == INCONSISTENT_METADATA_NUM_RETRIES) {
          markTimelineRetries(attempt, e.getMessage(), timeline);
          throw e;
        }
        if (attempt > 1) {
          // Back-off a bit on later retries.
          Uninterruptibles.sleepUninterruptibly(200 * attempt, TimeUnit.MILLISECONDS);
        }
        retryMsg = e.getMessage();
        LOG.warn("Retrying plan of query {}: {} (retry #{} of {})",
            queryCtx.client_request.stmt, retryMsg, attempt,
            INCONSISTENT_METADATA_NUM_RETRIES);
      }
    }
  }

  private TExecRequest doCreateExecRequest(PlanCtx planCtx,
      EventSequence timeline) throws ImpalaException {
    TQueryCtx queryCtx = planCtx.getQueryContext();
    // Parse stmt and collect/load metadata to populate a stmt-local table cache
    StatementBase stmt = Parser.parse(
        queryCtx.client_request.stmt, queryCtx.client_request.query_options);
    StmtMetadataLoader metadataLoader =
        new StmtMetadataLoader(this, queryCtx.session.database, timeline);
    //TODO (IMPALA-8788): should load table write ids in transaction context.
    StmtTableCache stmtTableCache = metadataLoader.loadTables(stmt);

    // Analyze and authorize stmt
    AnalysisContext analysisCtx = new AnalysisContext(queryCtx, authzFactory_, timeline);
    AnalysisResult analysisResult = analysisCtx.analyzeAndAuthorize(stmt, stmtTableCache,
        authzChecker_.get());
    LOG.info("Analysis and authorization finished.");
    Preconditions.checkNotNull(analysisResult.getStmt());
    TExecRequest result = createBaseExecRequest(queryCtx, analysisResult);

    try {
      TQueryOptions queryOptions = queryCtx.client_request.query_options;
      if (analysisResult.isCatalogOp()) {
        result.stmt_type = TStmtType.DDL;
        createCatalogOpRequest(analysisResult, result);
        TLineageGraph thriftLineageGraph = analysisResult.getThriftLineageGraph();
        if (thriftLineageGraph != null && thriftLineageGraph.isSetQuery_text()) {
          result.catalog_op_request.setLineage_graph(thriftLineageGraph);
        }
        setMtDopForCatalogOp(analysisResult, queryOptions);
        // All DDL operations except for CTAS are done with analysis at this point.
        if (!analysisResult.isCreateTableAsSelectStmt()) {
          return result;
        }
      } else if (analysisResult.isInsertStmt() ||
          analysisResult.isCreateTableAsSelectStmt()) {
        InsertStmt insertStmt = analysisResult.getInsertStmt();
        FeTable targetTable = insertStmt.getTargetTable();
        if (AcidUtils.isTransactionalTable(
            targetTable.getMetaStoreTable().getParameters())) {
          long txnId = openTransaction(queryCtx);
          timeline.markEvent("Transaction opened (" + String.valueOf(txnId) + ")");
          Collection<FeTable> tables = stmtTableCache.tables.values();
          createLockForInsert(txnId, tables, targetTable, insertStmt.isOverwrite());
          long writeId = allocateWriteId(queryCtx, targetTable);
          insertStmt.setWriteId(writeId);
        }
      } else if (analysisResult.isLoadDataStmt()) {
        result.stmt_type = TStmtType.LOAD;
        result.setResult_set_metadata(new TResultSetMetadata(
            Collections.singletonList(new TColumn("summary", Type.STRING.toThrift()))));
        result.setLoad_data_request(analysisResult.getLoadDataStmt().toThrift());
        return result;
      } else if (analysisResult.isSetStmt()) {
        result.stmt_type = TStmtType.SET;
        result.setResult_set_metadata(new TResultSetMetadata(Arrays.asList(
            new TColumn("option", Type.STRING.toThrift()),
            new TColumn("value", Type.STRING.toThrift()),
            new TColumn("level", Type.STRING.toThrift()))));
        result.setSet_query_option_request(analysisResult.getSetStmt().toThrift());
        return result;
      } else if (analysisResult.isAdminFnStmt()) {
        result.stmt_type = TStmtType.ADMIN_FN;
        result.setResult_set_metadata(new TResultSetMetadata(Arrays.asList(
            new TColumn("summary", Type.STRING.toThrift()))));
        result.setAdmin_request(analysisResult.getAdminFnStmt().toThrift());
        return result;
      } else if (analysisResult.isTestCaseStmt()) {
        CopyTestCaseStmt testCaseStmt = ((CopyTestCaseStmt) stmt);
        if (testCaseStmt.isTestCaseExport()) {
          result.setStmt_type(TStmtType.TESTCASE);
          result.setResult_set_metadata(new TResultSetMetadata(Arrays.asList(
            new TColumn("Test case data output path", Type.STRING.toThrift()))));
          result.setTestcase_data_path(testCaseStmt.writeTestCaseData());
        } else {
          // Mimic it as a DDL.
          result.setStmt_type(TStmtType.DDL);
          createCatalogOpRequest(analysisResult, result);
        }
        return result;
      }

      // If unset, set MT_DOP to 0 to simplify the rest of the code.
      if (!queryOptions.isSetMt_dop()) queryOptions.setMt_dop(0);

      // create TQueryExecRequest
      TQueryExecRequest queryExecRequest =
          getPlannedExecRequest(planCtx, analysisResult, timeline);

      TLineageGraph thriftLineageGraph = analysisResult.getThriftLineageGraph();
      if (thriftLineageGraph != null && thriftLineageGraph.isSetQuery_text()) {
        queryExecRequest.setLineage_graph(thriftLineageGraph);
      }

      // Override the per_host_mem_estimate sent to the backend if needed. The explain
      // string is already generated at this point so this does not change the estimate
      // shown in the plan.
      checkAndOverrideMemEstimate(queryExecRequest, queryOptions);

      if (analysisResult.isExplainStmt()) {
        // Return the EXPLAIN request
        createExplainRequest(planCtx.getExplainString(), result);
        return result;
      }

      result.setQuery_exec_request(queryExecRequest);
      if (analysisResult.isQueryStmt()) {
        result.stmt_type = TStmtType.QUERY;
        result.query_exec_request.stmt_type = result.stmt_type;
        // fill in the metadata
        result.setResult_set_metadata(createQueryResultSetMetadata(analysisResult));
      } else if (analysisResult.isInsertStmt() ||
          analysisResult.isCreateTableAsSelectStmt()) {
        // For CTAS the overall TExecRequest statement type is DDL, but the
        // query_exec_request should be DML
        result.stmt_type =
            analysisResult.isCreateTableAsSelectStmt() ? TStmtType.DDL : TStmtType.DML;
        result.query_exec_request.stmt_type = TStmtType.DML;
        // create finalization params of insert stmt
        addFinalizationParamsForInsert(
            queryCtx, queryExecRequest, analysisResult.getInsertStmt());
      } else {
        Preconditions.checkState(
            analysisResult.isUpdateStmt() || analysisResult.isDeleteStmt());
        result.stmt_type = TStmtType.DML;
        result.query_exec_request.stmt_type = TStmtType.DML;
      }
      return result;
    } catch (Exception e) {
      if (queryCtx.isSetTransaction_id()) {
        try {
          abortTransaction(queryCtx.getTransaction_id());
          timeline.markEvent("Transaction aborted");
        } catch (TransactionException te) {
          LOG.error("Could not abort transaction because: " + te.getMessage());
        }
      }
      throw e;
    }
  }

  /**
   * Set MT_DOP based on the analysis result
   */
  private static void setMtDopForCatalogOp(
      AnalysisResult analysisResult, TQueryOptions queryOptions) {
    // Set MT_DOP=4 for COMPUTE STATS, unless the user has already provided another
    // value for MT_DOP.
    if (!queryOptions.isSetMt_dop() && analysisResult.isComputeStatsStmt()) {
      queryOptions.setMt_dop(4);
    }
    // If unset, set MT_DOP to 0 to simplify the rest of the code.
    if (!queryOptions.isSetMt_dop()) queryOptions.setMt_dop(0);
  }

  /**
   * Create the TExecRequest and initialize it
   */
  private static TExecRequest createBaseExecRequest(
      TQueryCtx queryCtx, AnalysisResult analysisResult) {
    TExecRequest result = new TExecRequest();
    result.setQuery_options(queryCtx.client_request.getQuery_options());
    result.setAccess_events(Lists.newArrayList(analysisResult.getAccessEvents()));
    result.analysis_warnings = analysisResult.getAnalyzer().getWarnings();
    result.setUser_has_profile_access(analysisResult.userHasProfileAccess());
    return result;
  }

  /**
   * Add the finalize params for an insert statement to the queryExecRequest
   */
  private static void addFinalizationParamsForInsert(
      TQueryCtx queryCtx, TQueryExecRequest queryExecRequest, InsertStmt insertStmt) {
    if (insertStmt.getTargetTable() instanceof FeFsTable) {
      TFinalizeParams finalizeParams = new TFinalizeParams();
      finalizeParams.setIs_overwrite(insertStmt.isOverwrite());
      finalizeParams.setTable_name(insertStmt.getTargetTableName().getTbl());
      finalizeParams.setTable_id(DescriptorTable.TABLE_SINK_ID);
      String db = insertStmt.getTargetTableName().getDb();
      finalizeParams.setTable_db(db == null ? queryCtx.session.database : db);
      FeFsTable hdfsTable = (FeFsTable) insertStmt.getTargetTable();
      finalizeParams.setHdfs_base_dir(hdfsTable.getHdfsBaseDir());
      if (insertStmt.getWriteId() != -1) {
        Preconditions.checkState(queryCtx.isSetTransaction_id());
        finalizeParams.setTransaction_id(queryCtx.getTransaction_id());
        finalizeParams.setWrite_id(insertStmt.getWriteId());
      } else {
        // TODO: Currently this flag only controls the removal of the query-level staging
        // directory. HdfsTableSink (that creates the staging dir) calculates the path
        // independently. So it'd be better to either remove this option, or make it used
        // everywhere where the staging directory is referenced.
        finalizeParams.setStaging_dir(
            hdfsTable.getHdfsBaseDir() + "/_impala_insert_staging");
      }
      queryExecRequest.setFinalize_params(finalizeParams);
    }
  }

  /**
   * Add the metadata for the result set
   */
  private static TResultSetMetadata createQueryResultSetMetadata(
      AnalysisResult analysisResult) {
    LOG.trace("create result set metadata");
    TResultSetMetadata metadata = new TResultSetMetadata();
    QueryStmt queryStmt = analysisResult.getQueryStmt();
    int colCnt = queryStmt.getColLabels().size();
    for (int i = 0; i < colCnt; ++i) {
      TColumn colDesc = new TColumn();
      colDesc.columnName = queryStmt.getColLabels().get(i);
      colDesc.columnType = queryStmt.getResultExprs().get(i).getType().toThrift();
      metadata.addToColumns(colDesc);
    }
    return metadata;
  }

  /**
   * Get the TQueryExecRequest and use it to populate the query context
   */
  private TQueryExecRequest getPlannedExecRequest(PlanCtx planCtx,
      AnalysisResult analysisResult, EventSequence timeline)
      throws ImpalaException {
    Preconditions.checkState(analysisResult.isQueryStmt() || analysisResult.isDmlStmt()
        || analysisResult.isCreateTableAsSelectStmt());
    TQueryCtx queryCtx = planCtx.getQueryContext();
    Planner planner = new Planner(analysisResult, queryCtx, timeline);
    TQueryExecRequest queryExecRequest = createExecRequest(planner, planCtx);
    if (planCtx.serializeDescTbl()) {
      queryCtx.setDesc_tbl_serialized(
          planner.getAnalysisResult().getAnalyzer().getDescTbl().toSerializedThrift());
    } else {
      queryCtx.setDesc_tbl_testonly(
          planner.getAnalysisResult().getAnalyzer().getDescTbl().toThrift());
    }
    queryExecRequest.setQuery_ctx(queryCtx);
    queryExecRequest.setHost_list(analysisResult.getAnalyzer().getHostIndex().getList());
    return queryExecRequest;
  }

  /**
   * The MAX_MEM_ESTIMATE_FOR_ADMISSION query option can override the planner memory
   * estimate if set. Sets queryOptions.per_host_mem_estimate if the override is
   * effective.
   */
  private void checkAndOverrideMemEstimate(TQueryExecRequest queryExecRequest,
      TQueryOptions queryOptions) {
    if (queryOptions.isSetMax_mem_estimate_for_admission()
        && queryOptions.getMax_mem_estimate_for_admission() > 0) {
      long effectivePerHostMemEstimate = Math.min(
          queryExecRequest.getPer_host_mem_estimate(),
              queryOptions.getMax_mem_estimate_for_admission());
      queryExecRequest.setPer_host_mem_estimate(effectivePerHostMemEstimate);
      long effectiveCoordinatorMemEstimate = Math.min(
          queryExecRequest.getDedicated_coord_mem_estimate(),
              queryOptions.getMax_mem_estimate_for_admission());
      queryExecRequest.setDedicated_coord_mem_estimate(effectiveCoordinatorMemEstimate);
    }
  }

  /**
   * Attaches the explain result to the TExecRequest.
   */
  private void createExplainRequest(String explainString, TExecRequest result) {
    // update the metadata - one string column
    TColumn colDesc = new TColumn("Explain String", Type.STRING.toThrift());
    TResultSetMetadata metadata = new TResultSetMetadata(Lists.newArrayList(colDesc));
    result.setResult_set_metadata(metadata);

    // create the explain result set - split the explain string into one line per row
    String[] explainStringArray = explainString.split("\n");
    TExplainResult explainResult = new TExplainResult();
    explainResult.results = Lists.newArrayList();
    for (int i = 0; i < explainStringArray.length; ++i) {
      TColumnValue col = new TColumnValue();
      col.setString_val(explainStringArray[i]);
      TResultRow row = new TResultRow(Lists.newArrayList(col));
      explainResult.results.add(row);
    }
    result.setExplain_result(explainResult);
    result.stmt_type = TStmtType.EXPLAIN;
  }

  /**
   * Executes a HiveServer2 metadata operation and returns a TResultSet
   */
  public TResultSet execHiveServer2MetadataOp(TMetadataOpRequest request)
      throws ImpalaException {
    User user = request.isSetSession() ?
        new User(TSessionStateUtil.getEffectiveUser(request.session)) :
        ImpalaInternalAdminUser.getInstance();
    switch (request.opcode) {
      case GET_TYPE_INFO: return MetadataOp.getTypeInfo();
      case GET_SCHEMAS: return MetastoreShim.execGetSchemas(this, request, user);
      case GET_TABLES: return MetastoreShim.execGetTables(this, request, user);
      case GET_COLUMNS: return MetastoreShim.execGetColumns(this, request, user);
      case GET_CATALOGS: return MetadataOp.getCatalogs();
      case GET_TABLE_TYPES: return MetadataOp.getTableTypes();
      case GET_FUNCTIONS: return MetastoreShim.execGetFunctions(this, request, user);
      case GET_PRIMARY_KEYS: return MetadataOp.getPrimaryKeys(this, request,
          user);
      case GET_CROSS_REFERENCE: return MetadataOp.getCrossReference(this,
          request, user);
      default:
        throw new NotImplementedException(request.opcode + " has not been implemented.");
    }
  }

  /**
   * Returns all files info of a table or partition.
   */
  public TResultSet getTableFiles(TShowFilesParams request)
      throws ImpalaException {
    TTableName tableName = request.getTable_name();
    RetryTracker retries = new RetryTracker(
        String.format("getting table files %s.%s", tableName.db_name,
            tableName.table_name));
    while (true) {
      try {
        return doGetTableFiles(request);
      } catch(InconsistentMetadataFetchException e) {
        retries.handleRetryOrThrow(e);
      }
    }
  }

  private TResultSet doGetTableFiles(TShowFilesParams request)
      throws ImpalaException{
    FeTable table = getCatalog().getTable(request.getTable_name().getDb_name(),
        request.getTable_name().getTable_name());
    if (table instanceof FeFsTable) {
      return FeFsTable.Utils.getFiles((FeFsTable)table, request.getPartition_set());
    } else {
      throw new InternalException("SHOW FILES only supports Hdfs table. " +
          "Unsupported table class: " + table.getClass());
    }
  }

  /**
   * Executes the {@link QueryEventHook#onQueryComplete(QueryCompleteContext)}
   * execution hooks for each hook registered in this instance's
   * {@link QueryEventHookManager}.
   *
   * <h3>Service Guarantees</h3>
   *
   * Impala makes the following guarantees about how this method executes hooks:
   *
   * <h4>Hooks are executed asynchronously</h4>
   *
   * All hook execution happens asynchronously of the query that triggered
   * them.  Hooks may still be executing after the query response has returned
   * to the caller.  Additionally, hooks may execute concurrently if the
   * hook executor thread size is configured appropriately.
   *
   * <h4>Hook Invocation is in Configuration Order</h4>
   *
   * The <i>submission</i> of the hook execution tasks occurs in the order
   * that the hooks were defined in configuration.  This generally means that
   * hooks will <i>start</i> executing in order, but there are no guarantees
   * about finishing order.
   * <p>
   * For example, if configured with {@code query_event_hook_classes=hook1,hook2,hook3},
   * then hook1 will start before hook2, and hook2 will start before hook3.
   * If you need to guarantee that hook1 <i>completes</i> before hook2 starts, then
   * you should specify {@code query_event_hook_nthreads=1} for serial hook
   * execution.
   * </p>
   *
   * <h4>Hook Execution Blocks</h4>
   *
   * A hook will block the thread it executes on until it completes.  If a hook hangs,
   * then the thread also hangs.  Impala (currently) will not check for hanging hooks to
   * take any action.  This means that if you have {@code query_event_hook_nthreads}
   * less than the number of hooks, then 1 hook may effectively block others from
   * executing.
   *
   * <h4>Hook Exceptions are non-fatal</h4>
   *
   * Any exception thrown from this hook method will be logged and ignored.  Therefore,
   * an exception in 1 hook will not affect another hook (when no shared resources are
   * involved).
   *
   * <h4>Hook Execution may end abruptly at Impala shutdown</h4>
   *
   * If a hook is still executing when Impala is shutdown, there are no guarantees
   * that it will complete execution before being killed.
   *
   * @see QueryCompleteContext
   * @see QueryEventHookManager
   *
   * @param context the execution context of the query
   */
  public void callQueryCompleteHooks(QueryCompleteContext context) {
    // TODO (IMPALA-8571): can we make use of the futures to implement better
    // error-handling?  Currently, the queryHookManager simply
    // logs-then-rethrows any exception thrown from a hook.postQueryExecute
    final List<Future<QueryEventHook>> futures
        = this.queryHookManager_.executeQueryCompleteHooks(context);
  }

  /**
   * Opens a new transaction and registers it to the keepalive object.
   * @param queryCtx context of the query that requires the transaction.
   * @return the transaction id.
   * @throws TransactionException
   */
  private long openTransaction(TQueryCtx queryCtx) throws TransactionException {
    try (MetaStoreClient client = metaStoreClientPool_.getClient()) {
      IMetaStoreClient hmsClient = client.getHiveClient();
      long transactionId = MetastoreShim.openTransaction(hmsClient);
      HeartbeatContext ctx = new HeartbeatContext(queryCtx, System.nanoTime());
      transactionKeepalive_.addTransaction(transactionId, ctx);
      LOG.info("Opened transaction: " + Long.toString(transactionId));
      queryCtx.setTransaction_id(transactionId);
      return transactionId;
    }
  }

  /**
   * Aborts a transaction.
   * @param txnId is the id of the transaction to abort.
   * @throws TransactionException
   * TODO: maybe we should make it async.
   */
  public void abortTransaction(long txnId) throws TransactionException {
    LOG.error("Aborting transaction: " + Long.toString(txnId));
    try (MetaStoreClient client = metaStoreClientPool_.getClient()) {
      IMetaStoreClient hmsClient = client.getHiveClient();
      transactionKeepalive_.deleteTransaction(txnId);
      MetastoreShim.abortTransaction(hmsClient, txnId);
    }
  }

  /**
   * Unregisters an already committed transaction.
   * @param txnId is the id of the transaction to clear.
   */
  public void unregisterTransaction(long txnId) {
    LOG.info("Unregistering already committed transaction: " + Long.toString(txnId));
    transactionKeepalive_.deleteTransaction(txnId);
  }

  /**
   * Allocates write id for transactional table.
   * @param queryCtx the query context that contains the transaction id.
   * @param table the target table of the write operation.
   * @return the allocated write id
   * @throws TransactionException
   */
  private long allocateWriteId(TQueryCtx queryCtx, FeTable table)
      throws TransactionException {
    Preconditions.checkState(queryCtx.isSetTransaction_id());
    Preconditions.checkState(table instanceof FeFsTable);
    Preconditions.checkState(
        AcidUtils.isTransactionalTable(table.getMetaStoreTable().getParameters()));
    try (MetaStoreClient client = metaStoreClientPool_.getClient()) {
      IMetaStoreClient hmsClient = client.getHiveClient();
      long txnId = queryCtx.getTransaction_id();
      return MetastoreShim.allocateTableWriteId(hmsClient, txnId, table.getDb().getName(),
          table.getName());
    }
  }

  /**
   * Creates Lock object for the insert statement.
   * @param txnId the transaction id to be used.
   * @param tables the tables in the query.
   * @param targetTable the target table of INSERT. Must be transactional.
   * @param isOverwrite
   * @throws TransactionException
   */
  private void createLockForInsert(Long txnId, Collection<FeTable> tables,
      FeTable targetTable, boolean isOverwrite) throws TransactionException {
    Preconditions.checkState(
        AcidUtils.isTransactionalTable(targetTable.getMetaStoreTable().getParameters()));
    List<LockComponent> lockComponents = new ArrayList<>(tables.size());
    for (FeTable table : tables) {
      if (!AcidUtils.isTransactionalTable(table.getMetaStoreTable().getParameters())) {
        continue;
      }
      LockComponent lockComponent = new LockComponent();
      lockComponent.setDbname(table.getDb().getName());
      lockComponent.setTablename(table.getName());
      lockComponent.setLevel(LockLevel.TABLE);
      if (table == targetTable) {
        if (isOverwrite) {
          lockComponent.setType(LockType.EXCLUSIVE);
        } else {
          lockComponent.setType(LockType.SHARED_READ);
        }
        lockComponent.setOperationType(DataOperationType.INSERT);
      } else {
        lockComponent.setType(LockType.SHARED_READ);
        lockComponent.setOperationType(DataOperationType.SELECT);
      }
      lockComponents.add(lockComponent);
    }
    try (MetaStoreClient client = metaStoreClientPool_.getClient()) {
      IMetaStoreClient hmsClient = client.getHiveClient();
      MetastoreShim.acquireLock(hmsClient, txnId, lockComponents);
    }
  }
}
