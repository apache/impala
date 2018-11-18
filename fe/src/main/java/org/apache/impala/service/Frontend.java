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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.AlterDbStmt;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.CommentOnStmt;
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
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.ResetMetadataStmt;
import org.apache.impala.analysis.ShowFunctionsStmt;
import org.apache.impala.analysis.ShowGrantPrincipalStmt;
import org.apache.impala.analysis.ShowRolesStmt;
import org.apache.impala.analysis.SqlParser;
import org.apache.impala.analysis.SqlScanner;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.analysis.TableName;
import org.apache.impala.analysis.TruncateStmt;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizeableTable;
import org.apache.impala.authorization.ImpalaInternalAdminUser;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.CatalogException;
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
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.local.InconsistentMetadataFetchException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.planner.HdfsScanNode;
import org.apache.impala.planner.PlanFragment;
import org.apache.impala.planner.Planner;
import org.apache.impala.planner.ScanNode;
import org.apache.impala.thrift.TAdminRequest;
import org.apache.impala.thrift.TAlterDbParams;
import org.apache.impala.thrift.TCatalogOpRequest;
import org.apache.impala.thrift.TCatalogOpType;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
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
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TPlanExecInfo;
import org.apache.impala.thrift.TPlanFragment;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryExecRequest;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TShowFilesParams;
import org.apache.impala.thrift.TShowStatsOp;
import org.apache.impala.thrift.TShutdownParams;
import org.apache.impala.thrift.TStmtType;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUpdateCatalogCacheRequest;
import org.apache.impala.thrift.TUpdateCatalogCacheResponse;
import org.apache.impala.thrift.TUpdateExecutorMembershipRequest;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Frontend API for the impalad process.
 * This class allows the impala daemon to create TQueryExecRequest
 * in response to TClientRequests. Also handles management of the authorization
 * policy.
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

  private final FeCatalogManager catalogManager_;
  private final AuthorizationConfig authzConfig_;
  /**
   * Authorization checker. Initialized and periodically loaded by a task
   * running on the {@link #policyReader_} thread.
   */
  private final AtomicReference<AuthorizationChecker> authzChecker_ =
      new AtomicReference<>();
  private final ScheduledExecutorService policyReader_ =
      Executors.newScheduledThreadPool(1);

  private final ImpaladTableUsageTracker impaladTableUsageTracker_;

  public Frontend(AuthorizationConfig authorizationConfig) {
    this(authorizationConfig, FeCatalogManager.createFromBackendConfig());
  }

  /**
   * Create a frontend with a specific catalog instance which will not allow
   * updates and will be used for all requests.
   */
  @VisibleForTesting
  public Frontend(AuthorizationConfig authorizationConfig,
      FeCatalog testCatalog) {
    this(authorizationConfig, FeCatalogManager.createForTests(testCatalog));
  }

  private Frontend(AuthorizationConfig authorizationConfig,
      FeCatalogManager catalogManager) {
    authzConfig_ = authorizationConfig;
    catalogManager_ = catalogManager;

    // Load the authorization policy once at startup, initializing
    // authzChecker_. This ensures that, if the policy fails to load,
    // we will throw an exception and fail to start.
    AuthorizationPolicyReader policyReaderTask =
        new AuthorizationPolicyReader(authzConfig_);
    policyReaderTask.run();

    // If authorization is enabled, reload the policy on a regular basis.
    if (authzConfig_.isEnabled() && authzConfig_.isFileBasedPolicy()) {
      // Stagger the reads across nodes
      Random randomGen = new Random(UUID.randomUUID().hashCode());
      int delay = AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS + randomGen.nextInt(60);

      policyReader_.scheduleAtFixedRate(policyReaderTask,
          delay, AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS, TimeUnit.SECONDS);
    }
    impaladTableUsageTracker_ = ImpaladTableUsageTracker.createFromConfig(
        BackendConfig.INSTANCE);
  }

  /**
   * Reads (and caches) an authorization policy from HDFS.
   */
  private class AuthorizationPolicyReader implements Runnable {
    private final AuthorizationConfig config_;

    public AuthorizationPolicyReader(AuthorizationConfig config) {
      config_ = config;
    }

    @Override
    public void run() {
      try {
        LOG.info("Reloading authorization policy file from: " + config_.getPolicyFile());
        authzChecker_.set(new AuthorizationChecker(config_,
            getCatalog().getAuthPolicy()));
      } catch (Exception e) {
        LOG.error("Error reloading policy file: ", e);
      }
    }
  }

  public FeCatalog getCatalog() { return catalogManager_.getOrCreateCatalog(); }

  public AuthorizationChecker getAuthzChecker() { return authzChecker_.get(); }

  public ImpaladTableUsageTracker getImpaladTableUsageTracker() {
    return impaladTableUsageTracker_;
  }

  public TUpdateCatalogCacheResponse updateCatalogCache(
      TUpdateCatalogCacheRequest req) throws CatalogException, TException {
    TUpdateCatalogCacheResponse resp = catalogManager_.updateCatalogCache(req);
    if (!req.is_delta) {
      // In the case that it was a non-delta update, the catalog might have reloaded
      // itself, and we need to reset the AuthorizationChecker accordingly.
      authzChecker_.set(new AuthorizationChecker(
          authzConfig_, getCatalog().getAuthPolicy()));
    }
    return resp;
  }

  /**
   * Update the cluster membership snapshot with the latest snapshot from the backend.
   */
  public void updateExecutorMembership(TUpdateExecutorMembershipRequest req) {
    ExecutorMembershipSnapshot.update(req);
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
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isShowRolesStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_ROLES;
      ShowRolesStmt showRolesStmt = (ShowRolesStmt) analysis.getStmt();
      ddl.setShow_roles_params(showRolesStmt.toThrift());
      Set<String> groupNames =
          getAuthzChecker().getUserGroups(analysis.getAnalyzer().getUser());
      // Check if the user is part of the group (case-sensitive) this SHOW ROLE
      // statement is targeting. If they are already a member of the group,
      // the admin requirement can be removed.
      Preconditions.checkState(ddl.getShow_roles_params().isSetIs_admin_op());
      if (ddl.getShow_roles_params().isSetGrant_group() &&
          groupNames.contains(ddl.getShow_roles_params().getGrant_group())) {
        ddl.getShow_roles_params().setIs_admin_op(false);
      }
      metadata.setColumns(Arrays.asList(
          new TColumn("role_name", Type.STRING.toThrift())));
    } else if (analysis.isShowGrantPrincipalStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_GRANT_PRINCIPAL;
      ShowGrantPrincipalStmt showGrantPrincipalStmt =
          (ShowGrantPrincipalStmt) analysis.getStmt();
      ddl.setShow_grant_principal_params(showGrantPrincipalStmt.toThrift());
      Set<String> groupNames =
          getAuthzChecker().getUserGroups(analysis.getAnalyzer().getUser());
      // User must be an admin to execute this operation if they have not been granted
      // this principal, or the same user as the request.
      boolean requiresAdmin;
      if (showGrantPrincipalStmt.getPrincipal().getPrincipalType()
          == TPrincipalType.USER) {
        requiresAdmin = !showGrantPrincipalStmt.getPrincipal().getName().equals(
            analysis.getAnalyzer().getUser().getShortName());
      } else {
        requiresAdmin = Sets.intersection(groupNames, showGrantPrincipalStmt
            .getPrincipal().getGrantGroups()).isEmpty();
      }
      ddl.getShow_grant_principal_params().setIs_admin_op(requiresAdmin);
      metadata.setColumns(Arrays.asList(
          new TColumn("name", Type.STRING.toThrift())));
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
    StringBuilder stringBuilder = new StringBuilder();
    createExecRequest(queryCtx, stringBuilder);
    return stringBuilder.toString();
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

  private List<String> doGetTableNames(String dbName, PatternMatcher matcher,
      User user) throws ImpalaException {
    List<String> tblNames = getCatalog().getTableNames(dbName, matcher);
    if (authzConfig_.isEnabled()) {
      Iterator<String> iter = tblNames.iterator();
      while (iter.hasNext()) {
        String tblName = iter.next();
        PrivilegeRequest privilegeRequest = new PrivilegeRequestBuilder()
            .any().onAnyColumn(dbName, tblName).toRequest();
        if (!authzChecker_.get().hasAccess(user, privilegeRequest)) {
          iter.remove();
        }
      }
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
      if (authzConfig_.isEnabled()) {
        PrivilegeRequest privilegeRequest = new PrivilegeRequestBuilder()
            .any().onColumn(table.getTableName().getDb(), table.getTableName().getTbl(),
            colName).toRequest();
        if (!authzChecker_.get().hasAccess(user, privilegeRequest)) continue;
      }
      columns.add(column);
    }
    return columns;
  }

  /**
   * Returns all databases in catalog cache that match the pattern of 'matcher' and are
   * accessible to 'user'.
   */
  public List<? extends FeDb> getDbs(PatternMatcher matcher, User user)
      throws InternalException {
    List<? extends FeDb> dbs = getCatalog().getDbs(matcher);
    // If authorization is enabled, filter out the databases the user does not
    // have permissions on.
    if (authzConfig_.isEnabled()) {
      Iterator<? extends FeDb> iter = dbs.iterator();
      while (iter.hasNext()) {
        FeDb db = iter.next();
        if (!isAccessibleToUser(db, user)) iter.remove();
      }
    }
    return dbs;
  }

  /**
   * Check whether database is accessible to given user.
   */
  private boolean isAccessibleToUser(FeDb db, User user)
      throws InternalException {
    if (db.getName().toLowerCase().equals(Catalog.DEFAULT_DB.toLowerCase())) {
      // Default DB should always be shown.
      return true;
    }
    PrivilegeRequest request = new PrivilegeRequestBuilder()
        .any().onAnyColumn(db.getName(), AuthorizeableTable.ANY_TABLE_NAME).toRequest();
    return authzChecker_.get().hasAccess(user, request);
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

    for (Column c: table.getColumnsInHiveOrder()) {
      TResultRowBuilder rowBuilder = new TResultRowBuilder();
      // Add name, type, NDVs, numNulls, max size and avg size.
      rowBuilder.add(c.getName()).add(c.getType().toSql())
          .add(c.getStats().getNumDistinctValues()).add(c.getStats().getNumNulls())
          .add(c.getStats().getMaxSize()).add(c.getStats().getAvgSize());
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
      } else {
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
    if (authzConfig_.isEnabled()) {
      // First run a table check
      PrivilegeRequest privilegeRequest = new PrivilegeRequestBuilder()
          .allOf(Privilege.VIEW_METADATA).onTable(table.getDb().getName(),
              table.getName())
          .toRequest();
      if (!authzChecker_.get().hasAccess(user, privilegeRequest)) {
        // Filter out columns that the user is not authorized to see.
        filteredColumns = new ArrayList<Column>();
        for (Column col: table.getColumnsInHiveOrder()) {
          String colName = col.getName();
          privilegeRequest = new PrivilegeRequestBuilder()
              .allOf(Privilege.VIEW_METADATA)
              .onColumn(table.getDb().getName(), table.getName(), colName)
              .toRequest();
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
      if (authzConfig_.isEnabled()) {
        PrivilegeRequest privilegeRequest = new PrivilegeRequestBuilder()
            .allOf(Privilege.VIEW_METADATA)
            .onTable(table.getDb().getName(),table.getName())
            .toRequest();
        // Only filter if the user doesn't have table access.
        if (!authzChecker_.get().hasAccess(user, privilegeRequest)) {
          List<TResultRow> results = new ArrayList<TResultRow>();
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
    List<PlanFragment> fragments = planRoot.getNodesPreOrder();

    // collect ScanNodes
    List<ScanNode> scanNodes = Lists.newArrayList();
    for (PlanFragment fragment: fragments) {
      Preconditions.checkNotNull(fragment.getPlanRoot());
      fragment.getPlanRoot().collect(Predicates.instanceOf(ScanNode.class), scanNodes);
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
      Planner planner, StringBuilder explainString) throws ImpalaException {
    TQueryCtx queryCtx = planner.getQueryCtx();
    AnalysisResult analysisResult = planner.getAnalysisResult();
    boolean isMtExec = analysisResult.isQueryStmt()
        && queryCtx.client_request.query_options.isSetMt_dop()
        && queryCtx.client_request.query_options.mt_dop > 0;

    List<PlanFragment> planRoots = Lists.newArrayList();
    TQueryExecRequest result = new TQueryExecRequest();
    if (isMtExec) {
      LOG.trace("create mt plan");
      planRoots.addAll(planner.createParallelPlans());
    } else {
      LOG.trace("create plan");
      planRoots.add(planner.createPlan().get(0));
    }

    // Compute resource requirements of the final plans.
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
          && !analysisResult.getAnalyzer().hasPlanHints();
    queryCtx.setDisable_spilling(disableSpilling);

    // assign fragment idx
    int idx = 0;
    for (TPlanExecInfo planExecInfo: result.plan_exec_info) {
      for (TPlanFragment fragment: planExecInfo.fragments) fragment.setIdx(idx++);
    }

    // create EXPLAIN output after setting everything else
    result.setQuery_ctx(queryCtx);  // needed by getExplainString()
    List<PlanFragment> allFragments = planRoots.get(0).getNodesPreOrder();
    explainString.append(planner.getExplainString(allFragments, result));
    result.setQuery_plan(explainString.toString());

    return result;
  }

  @VisibleForTesting
  public StatementBase parse(String stmt) throws AnalysisException {
    return parse(stmt, new TQueryOptions());
  }

  public StatementBase parse(String stmt, TQueryOptions options)
      throws AnalysisException {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    Preconditions.checkArgument(options != null);
    parser.setQueryOptions(options);
    try {
      return (StatementBase) parser.parse().value;
    } catch (Exception e) {
      throw new AnalysisException(parser.getErrorMsg(stmt), e);
    }
  }

  /**
   * Create a populated TExecRequest corresponding to the supplied TQueryCtx.
   */
  public TExecRequest createExecRequest(TQueryCtx queryCtx, StringBuilder explainString)
      throws ImpalaException {
    // Timeline of important events in the planning process, used for debugging
    // and profiling.
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      EventSequence timeline = new EventSequence("Query Compilation");
      TExecRequest result = getTExecRequest(queryCtx, timeline, explainString);
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

  private TExecRequest getTExecRequest(TQueryCtx queryCtx, EventSequence timeline,
      StringBuilder explainString) throws ImpalaException {
    LOG.info("Analyzing query: " + queryCtx.client_request.stmt);

    int attempt = 0;
    String retryMsg = "";
    while (true) {
      try {
        TExecRequest req = doCreateExecRequest(queryCtx, timeline, explainString);
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

  private TExecRequest doCreateExecRequest(TQueryCtx queryCtx, EventSequence timeline,
      StringBuilder explainString) throws ImpalaException {
    // Parse stmt and collect/load metadata to populate a stmt-local table cache
    StatementBase stmt =
        parse(queryCtx.client_request.stmt, queryCtx.client_request.query_options);
    StmtMetadataLoader metadataLoader =
        new StmtMetadataLoader(this, queryCtx.session.database, timeline);
    StmtTableCache stmtTableCache = metadataLoader.loadTables(stmt);

    // Analyze and authorize stmt
    AnalysisContext analysisCtx = new AnalysisContext(queryCtx, authzConfig_, timeline);
    AnalysisResult analysisResult =
        analysisCtx.analyzeAndAuthorize(stmt, stmtTableCache, authzChecker_.get());
    LOG.info("Analysis finished.");
    timeline.markEvent("Analysis finished");
    Preconditions.checkNotNull(analysisResult.getStmt());

    TExecRequest result = createBaseExecRequest(queryCtx, analysisResult);

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
    }
    // If unset, set MT_DOP to 0 to simplify the rest of the code.
    if (!queryOptions.isSetMt_dop()) queryOptions.setMt_dop(0);

    // create TQueryExecRequest
    TQueryExecRequest queryExecRequest =
        getPlannedExecRequest(queryCtx, analysisResult, timeline, explainString);

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
      createExplainRequest(explainString.toString(), result);
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
  }

  /**
   * Set MT_DOP based on the analysis result
   */
  private static void setMtDopForCatalogOp(
      AnalysisResult analysisResult, TQueryOptions queryOptions) {
    // Set MT_DOP=4 for COMPUTE STATS on Parquet/ORC tables, unless the user has already
    // provided another value for MT_DOP.
    if (!queryOptions.isSetMt_dop() && analysisResult.isComputeStatsStmt()
        && analysisResult.getComputeStatsStmt().isColumnar()) {
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
    result.setAccess_events(analysisResult.getAccessEvents());
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
      finalizeParams.setStaging_dir(
          hdfsTable.getHdfsBaseDir() + "/_impala_insert_staging");
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
  private TQueryExecRequest getPlannedExecRequest(TQueryCtx queryCtx,
      AnalysisResult analysisResult, EventSequence timeline, StringBuilder explainString)
      throws ImpalaException {
    Preconditions.checkState(analysisResult.isQueryStmt() || analysisResult.isDmlStmt()
        || analysisResult.isCreateTableAsSelectStmt() || analysisResult.isUpdateStmt()
        || analysisResult.isDeleteStmt());
    Planner planner = new Planner(analysisResult, queryCtx, timeline);
    TQueryExecRequest queryExecRequest = createExecRequest(planner, explainString);
    queryCtx.setDesc_tbl(
        planner.getAnalysisResult().getAnalyzer().getDescTbl().toThrift());
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
      long effectiveMemEstimate = Math.min(queryExecRequest.getPer_host_mem_estimate(),
              queryOptions.getMax_mem_estimate_for_admission());
      queryExecRequest.setPer_host_mem_estimate(effectiveMemEstimate);
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
}
