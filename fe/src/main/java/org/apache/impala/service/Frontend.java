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
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
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
import org.apache.impala.analysis.ShowGrantRoleStmt;
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
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.ImpaladCatalog;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
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
import org.apache.impala.thrift.TCatalogOpRequest;
import org.apache.impala.thrift.TCatalogOpType;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TCreateDropRoleParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDescribeOutputStyle;
import org.apache.impala.thrift.TDescribeResult;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExplainResult;
import org.apache.impala.thrift.TFinalizeParams;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.TLineageGraph;
import org.apache.impala.thrift.TLoadDataReq;
import org.apache.impala.thrift.TLoadDataResp;
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
import org.apache.impala.thrift.TUpdateMembershipRequest;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.MembershipSnapshot;
import org.apache.impala.util.PatternMatcher;
import org.apache.impala.util.TResultRowBuilder;
import org.apache.impala.util.TSessionStateUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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

  //TODO: Make the reload interval configurable.
  private static final int AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS = 5 * 60;

  private final AtomicReference<ImpaladCatalog> impaladCatalog_ =
      new AtomicReference<ImpaladCatalog>();
  private final AuthorizationConfig authzConfig_;
  private final AtomicReference<AuthorizationChecker> authzChecker_;
  private final ScheduledExecutorService policyReader_ =
      Executors.newScheduledThreadPool(1);
  private final String defaultKuduMasterHosts_;

  public Frontend(AuthorizationConfig authorizationConfig,
      String defaultKuduMasterHosts) {
    this(authorizationConfig, new ImpaladCatalog(defaultKuduMasterHosts));
  }

  /**
   * C'tor used by tests to pass in a custom ImpaladCatalog.
   */
  public Frontend(AuthorizationConfig authorizationConfig, ImpaladCatalog catalog) {
    authzConfig_ = authorizationConfig;
    impaladCatalog_.set(catalog);
    defaultKuduMasterHosts_ = catalog.getDefaultKuduMasterHosts();
    authzChecker_ = new AtomicReference<AuthorizationChecker>(
        new AuthorizationChecker(authzConfig_, impaladCatalog_.get().getAuthPolicy()));
    // If authorization is enabled, reload the policy on a regular basis.
    if (authzConfig_.isEnabled() && authzConfig_.isFileBasedPolicy()) {
      // Stagger the reads across nodes
      Random randomGen = new Random(UUID.randomUUID().hashCode());
      int delay = AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS + randomGen.nextInt(60);

      policyReader_.scheduleAtFixedRate(
          new AuthorizationPolicyReader(authzConfig_),
          delay, AUTHORIZATION_POLICY_RELOAD_INTERVAL_SECS, TimeUnit.SECONDS);
    }
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

  public ImpaladCatalog getCatalog() { return impaladCatalog_.get(); }
  public AuthorizationChecker getAuthzChecker() { return authzChecker_.get(); }

  public TUpdateCatalogCacheResponse updateCatalogCache(
      TUpdateCatalogCacheRequest req) throws CatalogException, TException {
    if (req.is_delta) return impaladCatalog_.get().updateCatalog(req);

    // If this is not a delta, this update should replace the current
    // Catalog contents so create a new catalog and populate it.
    ImpaladCatalog catalog = new ImpaladCatalog(defaultKuduMasterHosts_);

    TUpdateCatalogCacheResponse response = catalog.updateCatalog(req);

    // Now that the catalog has been updated, replace the references to
    // impaladCatalog_/authzChecker_. This ensures that clients don't see the catalog
    // disappear. The catalog is guaranteed to be ready since updateCatalog() has a
    // postcondition of isReady() == true.
    impaladCatalog_.set(catalog);
    authzChecker_.set(new AuthorizationChecker(authzConfig_, catalog.getAuthPolicy()));
    return response;
  }

  /**
   * Update the cluster membership snapshot with the latest snapshot from the backend.
   */
  public void updateMembership(TUpdateMembershipRequest req) {
    MembershipSnapshot.update(req);
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
      if (descStmt.getTable() instanceof KuduTable
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
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isAlterViewStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.ALTER_VIEW);
      req.setAlter_view_params(analysis.getAlterViewStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isCreateTableStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_TABLE);
      req.setCreate_table_params(analysis.getCreateTableStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isCreateTableAsSelectStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_TABLE_AS_SELECT);
      req.setCreate_table_params(
          analysis.getCreateTableAsSelectStmt().getCreateStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Arrays.asList(
          new TColumn("summary", Type.STRING.toThrift())));
    } else if (analysis.isCreateTableLikeStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_TABLE_LIKE);
      req.setCreate_table_like_params(analysis.getCreateTableLikeStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isCreateViewStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_VIEW);
      req.setCreate_view_params(analysis.getCreateViewStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isCreateDbStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_DATABASE);
      req.setCreate_db_params(analysis.getCreateDbStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isCreateUdfStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      CreateUdfStmt stmt = (CreateUdfStmt) analysis.getStmt();
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_FUNCTION);
      req.setCreate_fn_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isCreateUdaStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_FUNCTION);
      CreateUdaStmt stmt = (CreateUdaStmt)analysis.getStmt();
      req.setCreate_fn_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isCreateDataSrcStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_DATA_SOURCE);
      CreateDataSrcStmt stmt = (CreateDataSrcStmt)analysis.getStmt();
      req.setCreate_data_source_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isComputeStatsStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.COMPUTE_STATS);
      req.setCompute_stats_params(analysis.getComputeStatsStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isDropDbStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.DROP_DATABASE);
      req.setDrop_db_params(analysis.getDropDbStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isDropTableOrViewStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      DropTableOrViewStmt stmt = analysis.getDropTableOrViewStmt();
      req.setDdl_type(stmt.isDropTable() ? TDdlType.DROP_TABLE : TDdlType.DROP_VIEW);
      req.setDrop_table_or_view_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isTruncateStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      TruncateStmt stmt = analysis.getTruncateStmt();
      req.setDdl_type(TDdlType.TRUNCATE_TABLE);
      req.setTruncate_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isDropFunctionStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.DROP_FUNCTION);
      DropFunctionStmt stmt = (DropFunctionStmt)analysis.getStmt();
      req.setDrop_fn_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isDropDataSrcStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.DROP_DATA_SOURCE);
      DropDataSrcStmt stmt = (DropDataSrcStmt)analysis.getStmt();
      req.setDrop_data_source_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isDropStatsStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.DROP_STATS);
      DropStatsStmt stmt = (DropStatsStmt) analysis.getStmt();
      req.setDrop_stats_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
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
    } else if (analysis.isShowGrantRoleStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_GRANT_ROLE;
      ShowGrantRoleStmt showGrantRoleStmt = (ShowGrantRoleStmt) analysis.getStmt();
      ddl.setShow_grant_role_params(showGrantRoleStmt.toThrift());
      Set<String> groupNames =
          getAuthzChecker().getUserGroups(analysis.getAnalyzer().getUser());
      // User must be an admin to execute this operation if they have not been granted
      // this role.
      ddl.getShow_grant_role_params().setIs_admin_op(Sets.intersection(groupNames,
          showGrantRoleStmt.getRole().getGrantGroups()).isEmpty());
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
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isGrantRevokeRoleStmt()) {
      GrantRevokeRoleStmt grantRoleStmt = (GrantRevokeRoleStmt) analysis.getStmt();
      TGrantRevokeRoleParams params = grantRoleStmt.toThrift();
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(params.isIs_grant() ? TDdlType.GRANT_ROLE : TDdlType.REVOKE_ROLE);
      req.setGrant_revoke_role_params(params);
      ddl.op_type = TCatalogOpType.DDL;
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else if (analysis.isGrantRevokePrivStmt()) {
      GrantRevokePrivStmt grantRevokePrivStmt = (GrantRevokePrivStmt) analysis.getStmt();
      TGrantRevokePrivParams params = grantRevokePrivStmt.toThrift();
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(params.isIs_grant() ?
          TDdlType.GRANT_PRIVILEGE : TDdlType.REVOKE_PRIVILEGE);
      req.setGrant_revoke_priv_params(params);
      ddl.op_type = TCatalogOpType.DDL;
      ddl.setDdl_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    } else {
      throw new IllegalStateException("Unexpected CatalogOp statement type.");
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
    TableName tableName = TableName.fromThrift(request.getTable_name());

    // Get the destination for the load. If the load is targeting a partition,
    // this the partition location. Otherwise this is the table location.
    String destPathString = null;
    ImpaladCatalog catalog = impaladCatalog_.get();
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

  /**
   * Returns all tables in database 'dbName' that match the pattern of 'matcher' and are
   * accessible to 'user'.
   */
  public List<String> getTableNames(String dbName, PatternMatcher matcher,
      User user) throws ImpalaException {
    List<String> tblNames = impaladCatalog_.get().getTableNames(dbName, matcher);
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
  public List<Column> getColumns(Table table, PatternMatcher matcher,
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
  public List<Db> getDbs(PatternMatcher matcher, User user)
      throws InternalException {
    List<Db> dbs = impaladCatalog_.get().getDbs(matcher);
    // If authorization is enabled, filter out the databases the user does not
    // have permissions on.
    if (authzConfig_.isEnabled()) {
      Iterator<Db> iter = dbs.iterator();
      while (iter.hasNext()) {
        Db db = iter.next();
        if (!isAccessibleToUser(db, user)) iter.remove();
      }
    }
    return dbs;
  }

  /**
   * Check whether database is accessible to given user.
   */
  private boolean isAccessibleToUser(Db db, User user)
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
  public List<DataSource> getDataSrcs(String pattern) {
    return impaladCatalog_.get().getDataSources(
        PatternMatcher.createHivePatternMatcher(pattern));
  }

  /**
   * Generate result set and schema for a SHOW COLUMN STATS command.
   */
  public TResultSet getColumnStats(String dbName, String tableName)
      throws ImpalaException {
    Table table = impaladCatalog_.get().getTable(dbName, tableName);
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
    Table table = impaladCatalog_.get().getTable(dbName, tableName);
    if (table instanceof HdfsTable) {
      return ((HdfsTable) table).getTableStats();
    } else if (table instanceof HBaseTable) {
      return ((HBaseTable) table).getTableStats();
    } else if (table instanceof DataSourceTable) {
      return ((DataSourceTable) table).getTableStats();
    } else if (table instanceof KuduTable) {
      if (op == TShowStatsOp.RANGE_PARTITIONS) {
        return ((KuduTable) table).getRangePartitions();
      } else {
        return ((KuduTable) table).getTableStats();
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
    Db db = impaladCatalog_.get().getDb(dbName);
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
    Db db = impaladCatalog_.get().getDb(dbName);
    return DescribeResultFactory.buildDescribeDbResult(db, outputStyle);
  }

  /**
   * Returns table metadata, such as the column descriptors, in the specified table.
   * Throws an exception if the table or db is not found or if there is an error loading
   * the table metadata.
   */
  public TDescribeResult describeTable(TTableName tableName,
      TDescribeOutputStyle outputStyle) throws ImpalaException {
    Table table = impaladCatalog_.get().getTable(tableName.db_name, tableName.table_name);
    if (outputStyle == TDescribeOutputStyle.MINIMAL) {
      return DescribeResultFactory.buildDescribeMinimalResult(table);
    } else {
      Preconditions.checkArgument(outputStyle == TDescribeOutputStyle.FORMATTED ||
          outputStyle == TDescribeOutputStyle.EXTENDED);
      return DescribeResultFactory.buildDescribeFormattedResult(table);
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
    ArrayList<PlanFragment> fragments = planRoot.getNodesPreOrder();

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
          scanNode.getId().asInt(), scanNode.getScanRangeLocations());

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
    ArrayList<PlanFragment> allFragments = planRoots.get(0).getNodesPreOrder();
    explainString.append(planner.getExplainString(allFragments, result));
    result.setQuery_plan(explainString.toString());

    return result;
  }

  public StatementBase parse(String stmt) throws AnalysisException {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
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
    EventSequence timeline = new EventSequence("Query Compilation");
    LOG.info("Analyzing query: " + queryCtx.client_request.stmt);

    // Parse stmt and collect/load metadata to populate a stmt-local table cache
    StatementBase stmt = parse(queryCtx.client_request.stmt);
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

    TExecRequest result = new TExecRequest();
    result.setQuery_options(queryCtx.client_request.getQuery_options());
    result.setAccess_events(analysisResult.getAccessEvents());
    result.analysis_warnings = analysisResult.getAnalyzer().getWarnings();
    result.setUser_has_profile_access(analysisResult.userHasProfileAccess());

    TQueryOptions queryOptions = queryCtx.client_request.query_options;
    if (analysisResult.isCatalogOp()) {
      result.stmt_type = TStmtType.DDL;
      createCatalogOpRequest(analysisResult, result);
      TLineageGraph thriftLineageGraph = analysisResult.getThriftLineageGraph();
      if (thriftLineageGraph != null && thriftLineageGraph.isSetQuery_text()) {
        result.catalog_op_request.setLineage_graph(thriftLineageGraph);
      }
      // Set MT_DOP=4 for COMPUTE STATS on Parquet tables, unless the user has already
      // provided another value for MT_DOP.
      if (!queryOptions.isSetMt_dop() &&
          analysisResult.isComputeStatsStmt() &&
          analysisResult.getComputeStatsStmt().isParquetOnly()) {
        queryOptions.setMt_dop(4);
      }
      // If unset, set MT_DOP to 0 to simplify the rest of the code.
      if (!queryOptions.isSetMt_dop()) queryOptions.setMt_dop(0);
      // All DDL operations except for CTAS are done with analysis at this point.
      if (!analysisResult.isCreateTableAsSelectStmt()) return result;
    } else if (analysisResult.isLoadDataStmt()) {
      result.stmt_type = TStmtType.LOAD;
      result.setResult_set_metadata(new TResultSetMetadata(Arrays.asList(
          new TColumn("summary", Type.STRING.toThrift()))));
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
    }
    // If unset, set MT_DOP to 0 to simplify the rest of the code.
    if (!queryOptions.isSetMt_dop()) queryOptions.setMt_dop(0);

    // create TQueryExecRequest
    Preconditions.checkState(analysisResult.isQueryStmt() || analysisResult.isDmlStmt()
        || analysisResult.isCreateTableAsSelectStmt() || analysisResult.isUpdateStmt()
        || analysisResult.isDeleteStmt());

    Planner planner = new Planner(analysisResult, queryCtx, timeline);
    TQueryExecRequest queryExecRequest = createExecRequest(planner, explainString);
    queryCtx.setDesc_tbl(
        planner.getAnalysisResult().getAnalyzer().getDescTbl().toThrift());
    queryExecRequest.setQuery_ctx(queryCtx);
    queryExecRequest.setHost_list(analysisResult.getAnalyzer().getHostIndex().getList());

    TLineageGraph thriftLineageGraph = analysisResult.getThriftLineageGraph();
    if (thriftLineageGraph != null && thriftLineageGraph.isSetQuery_text()) {
      queryExecRequest.setLineage_graph(thriftLineageGraph);
    }

    if (analysisResult.isExplainStmt()) {
      // Return the EXPLAIN request
      createExplainRequest(explainString.toString(), result);
      return result;
    }

    result.setQuery_exec_request(queryExecRequest);
    if (analysisResult.isQueryStmt()) {
      // fill in the metadata
      LOG.trace("create result set metadata");
      result.stmt_type = TStmtType.QUERY;
      result.query_exec_request.stmt_type = result.stmt_type;
      TResultSetMetadata metadata = new TResultSetMetadata();
      QueryStmt queryStmt = analysisResult.getQueryStmt();
      int colCnt = queryStmt.getColLabels().size();
      for (int i = 0; i < colCnt; ++i) {
        TColumn colDesc = new TColumn();
        colDesc.columnName = queryStmt.getColLabels().get(i);
        colDesc.columnType = queryStmt.getResultExprs().get(i).getType().toThrift();
        metadata.addToColumns(colDesc);
      }
      result.setResult_set_metadata(metadata);
    } else if (analysisResult.isInsertStmt() ||
        analysisResult.isCreateTableAsSelectStmt()) {
      // For CTAS the overall TExecRequest statement type is DDL, but the
      // query_exec_request should be DML
      result.stmt_type =
          analysisResult.isCreateTableAsSelectStmt() ? TStmtType.DDL : TStmtType.DML;
      result.query_exec_request.stmt_type = TStmtType.DML;

      // create finalization params of insert stmt
      InsertStmt insertStmt = analysisResult.getInsertStmt();
      if (insertStmt.getTargetTable() instanceof HdfsTable) {
        TFinalizeParams finalizeParams = new TFinalizeParams();
        finalizeParams.setIs_overwrite(insertStmt.isOverwrite());
        finalizeParams.setTable_name(insertStmt.getTargetTableName().getTbl());
        finalizeParams.setTable_id(DescriptorTable.TABLE_SINK_ID);
        String db = insertStmt.getTargetTableName().getDb();
        finalizeParams.setTable_db(db == null ? queryCtx.session.database : db);
        HdfsTable hdfsTable = (HdfsTable) insertStmt.getTargetTable();
        finalizeParams.setHdfs_base_dir(hdfsTable.getHdfsBaseDir());
        finalizeParams.setStaging_dir(
            hdfsTable.getHdfsBaseDir() + "/_impala_insert_staging");
        queryExecRequest.setFinalize_params(finalizeParams);
      }
    } else {
      Preconditions.checkState(analysisResult.isUpdateStmt() || analysisResult.isDeleteStmt());
      result.stmt_type = TStmtType.DML;
      result.query_exec_request.stmt_type = TStmtType.DML;
    }

    timeline.markEvent("Planning finished");
    result.setTimeline(timeline.toThrift());
    return result;
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
    String[] explainStringArray = explainString.toString().split("\n");
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
      throws ImpalaException{
    Table table = impaladCatalog_.get().getTable(request.getTable_name().getDb_name(),
        request.getTable_name().getTable_name());
    if (table instanceof HdfsTable) {
      return ((HdfsTable) table).getFiles(request.getPartition_set());
    } else {
      throw new InternalException("SHOW FILES only supports Hdfs table. " +
          "Unsupported table class: " + table.getClass());
    }
  }
}
