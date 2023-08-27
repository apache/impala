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

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.AcidUtils;

import com.google.common.base.Preconditions;

/**
 * Representation of the following statements:
 * INVALIDATE METADATA
 * INVALIDATE METADATA <table>
 * REFRESH <table>
 * REFRESH <table> PARTITION <partition>
 * REFRESH FUNCTIONS <database>
 * REFRESH AUTHORIZATION
 */
public class ResetMetadataStmt extends StatementBase {

  public enum Action {
    INVALIDATE_METADATA_ALL(false),
    INVALIDATE_METADATA_TABLE(false),
    REFRESH_TABLE(true),
    REFRESH_PARTITION(true),
    REFRESH_FUNCTIONS(true),
    REFRESH_AUTHORIZATION(true);

    private final boolean isRefresh_;

    Action(boolean isRefresh) {
      isRefresh_ = isRefresh;
    }

    public boolean isRefresh() { return isRefresh_; }
  }

  // Updated during analysis. Null if invalidating the entire catalog or refreshing
  // database functions.
  private TableName tableName_;

  // not null when refreshing a single partition
  private final PartitionSpec partitionSpec_;

  // not null when refreshing functions in a database.
  private final String database_;

  // The type of action.
  private final Action action_;

  // Set during analysis.
  private User requestingUser_;

  // Set during analysis.
  private TUniqueId queryId_;

  // Set during analysis.
  private String clientIp_;

  private ResetMetadataStmt(Action action, String db, TableName tableName,
      PartitionSpec partitionSpec) {
    Preconditions.checkNotNull(action);
    action_ = action;
    database_ = db;
    tableName_ = tableName;
    partitionSpec_ = partitionSpec;
    if (partitionSpec_ != null) partitionSpec_.setTableName(tableName_);
  }

  public static ResetMetadataStmt createInvalidateStmt() {
    return new ResetMetadataStmt(Action.INVALIDATE_METADATA_ALL, /*db*/ null,
        /*table*/ null, /*partition*/ null);
  }

  public static ResetMetadataStmt createInvalidateStmt(TableName tableName) {
    return new ResetMetadataStmt(Action.INVALIDATE_METADATA_TABLE, /*db*/ null,
        /*table*/ Preconditions.checkNotNull(tableName), /*partition*/ null);
  }

  public static ResetMetadataStmt createRefreshTableStmt(TableName tableName) {
    return new ResetMetadataStmt(Action.REFRESH_TABLE, /*db*/ null,
        Preconditions.checkNotNull(tableName), /*partition*/ null);
  }

  public static ResetMetadataStmt createRefreshPartitionStmt(TableName tableName,
      PartitionSpec partitionSpec) {
    return new ResetMetadataStmt(Action.REFRESH_PARTITION, /*db*/ null,
        Preconditions.checkNotNull(tableName), Preconditions.checkNotNull(partitionSpec));
  }

  public static ResetMetadataStmt createRefreshFunctionsStmt(String db) {
    return new ResetMetadataStmt(Action.REFRESH_FUNCTIONS, Preconditions.checkNotNull(db),
        /*table*/ null, /*partition*/ null);
  }

  public static ResetMetadataStmt createRefreshAuthorizationStmt() {
    return new ResetMetadataStmt(Action.REFRESH_AUTHORIZATION, /*db*/ null,
        /*table*/ null, /*partition*/ null);
  }

  public TableName getTableName() { return tableName_; }

  public PartitionSpec getPartitionSpec() { return partitionSpec_; }

  @VisibleForTesting
  protected Action getAction() { return action_; }

  @VisibleForTesting
  public void setRequestingUser(User user) {
    requestingUser_ = user;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    if (tableName_ != null && partitionSpec_ != null) {
      tblRefs.add(new TableRef(tableName_.toPath(), null));
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    requestingUser_ = analyzer.getUser();
    queryId_ = analyzer.getQueryCtx().getQuery_id();
    clientIp_ = analyzer.getQueryCtx().getSession().getNetwork_address().getHostname();
    switch (action_) {
      case INVALIDATE_METADATA_TABLE:
      case REFRESH_TABLE:
      case REFRESH_PARTITION:
        Preconditions.checkNotNull(tableName_);
        String dbName = analyzer.getTargetDbName(tableName_);
        tableName_ = new TableName(dbName, tableName_.getTbl());
        if (action_.isRefresh()) {
          // Verify the user has privileges to access this table. Will throw if the parent
          // database does not exists. Don't call getTable() to avoid loading the table
          // metadata if it is not yet in this impalad's catalog cache.
          if (!analyzer.dbContainsTable(dbName, tableName_.getTbl(), Privilege.REFRESH)) {
            // Only throw an exception when the table does not exist for refresh
            // statements since 'invalidate metadata' should add/remove tables
            // created/dropped external to Impala.
            throw new AnalysisException(Analyzer.TBL_DOES_NOT_EXIST_ERROR_MSG +
                tableName_);
          }
          if (partitionSpec_ != null) {
            try {
              // Get local table info without reaching out to HMS
              FeTable table = analyzer.getTable(dbName, tableName_.getTbl(),
                  /* must_exist */ true);
              if (AcidUtils.isTransactionalTable(
                      table.getMetaStoreTable().getParameters())) {
                throw new AnalysisException("Refreshing a partition is not allowed on " +
                    "transactional tables. Try to refresh the whole table instead.");
              }
            } catch (TableLoadingException e) {
              throw new AnalysisException(e);
            }
            partitionSpec_.setPrivilegeRequirement(Privilege.ANY);
            partitionSpec_.analyze(analyzer);
          }
        } else {
          FeTable tbl = analyzer.getTableNoThrow(dbName, tableName_.getTbl());
          // Verify the user has privileges to access this table.
          if (tbl == null) {
            analyzer.registerPrivReq(builder ->
                builder.onTableUnknownOwner(
                  dbName, tableName_.getTbl()).allOf(Privilege.REFRESH).build());
          } else {
            analyzer.registerPrivReq(
                builder -> builder.onTable(dbName, tableName_.getTbl(),
                    tbl.getOwnerUser()).allOf(Privilege.REFRESH).build());
          }
        }
        break;
      case REFRESH_AUTHORIZATION:
        if (!analyzer.isAuthzEnabled()) {
          throw new AnalysisException("Authorization is not enabled. To enable " +
              "authorization restart Impala with the --server_name=<name> flag.");
        }
        analyzer.registerPrivReq(builder ->
            builder.onServer(analyzer.getServerName())
                .allOf(Privilege.REFRESH)
                .build());
        break;
      case REFRESH_FUNCTIONS:
        FeDb db = analyzer.getDb(database_, /*throwIfDoesNotExist*/ false);
        String dbOwner = db == null ? null : db.getOwnerUser();
        analyzer.registerPrivReq(builder ->
            builder.onDb(database_, dbOwner)
                .allOf(Privilege.REFRESH)
                .build());
        break;
      case INVALIDATE_METADATA_ALL:
        analyzer.registerPrivReq(builder ->
            builder.onServer(analyzer.getServerName())
                .allOf(Privilege.REFRESH)
                .build());
        break;
      default:
        throw new IllegalStateException("Invalid reset metadata action: " + action_);
    }
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder result = new StringBuilder();
    switch (action_) {
      case REFRESH_AUTHORIZATION:
        result.append("REFRESH AUTHORIZATION");
        break;
      case REFRESH_FUNCTIONS:
        result.append("REFRESH FUNCTIONS ").append(database_);
        break;
      case REFRESH_TABLE:
        result.append("REFRESH ").append(tableName_.toSql());
        break;
      case REFRESH_PARTITION:
        result.append("REFRESH ").append(tableName_.toSql()).append(" ")
            .append(partitionSpec_.toSql(options));
        break;
      case INVALIDATE_METADATA_ALL:
        result.append("INVALIDATE METADATA");
        break;
      case INVALIDATE_METADATA_TABLE:
        result.append("INVALIDATE METADATA ").append(tableName_.toSql());
        break;
      default:
        throw new IllegalStateException("Invalid reset metadata action: " + action_);
    }
    return result.toString();
  }

  public TResetMetadataRequest toThrift() throws InternalException {
    TResetMetadataRequest params = new TResetMetadataRequest();
    params.setHeader(new TCatalogServiceRequestHeader());
    params.header.setRequesting_user(requestingUser_.getShortName());
    params.header.setQuery_id(queryId_);
    params.header.setCoordinator_hostname(BackendConfig.INSTANCE.getHostname());
    params.header.setClient_ip(clientIp_);
    params.setIs_refresh(action_.isRefresh());
    if (tableName_ != null) {
      params.setTable_name(new TTableName(tableName_.getDb(), tableName_.getTbl()));
    }
    if (partitionSpec_ != null) params.setPartition_spec(partitionSpec_.toThrift());
    if (database_ != null) params.setDb_name(database_);
    if (action_ == Action.REFRESH_AUTHORIZATION) {
      params.setAuthorization(true);
    }
    return params;
  }
}
