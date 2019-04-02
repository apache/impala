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

import com.google.common.collect.Iterables;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.io.IOUtils;
import org.apache.impala.analysis.AlterTableSortByStmt;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationDelta;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnNotFoundException;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.HiveStorageDescriptorFactory;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.PartitionNotFoundException;
import org.apache.impala.catalog.PartitionStatsUtil;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.View;
import org.apache.impala.catalog.events.MetastoreEvents;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.JniCatalogConstants;
import org.apache.impala.thrift.TAlterDbParams;
import org.apache.impala.thrift.TAlterDbSetOwnerParams;
import org.apache.impala.thrift.TAlterTableAddColsParams;
import org.apache.impala.thrift.TAlterTableAddDropRangePartitionParams;
import org.apache.impala.thrift.TAlterTableAddPartitionParams;
import org.apache.impala.thrift.TAlterTableAlterColParams;
import org.apache.impala.thrift.TAlterTableDropColParams;
import org.apache.impala.thrift.TAlterTableDropPartitionParams;
import org.apache.impala.thrift.TAlterTableOrViewSetOwnerParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableReplaceColsParams;
import org.apache.impala.thrift.TAlterTableSetCachedParams;
import org.apache.impala.thrift.TAlterTableSetFileFormatParams;
import org.apache.impala.thrift.TAlterTableSetLocationParams;
import org.apache.impala.thrift.TAlterTableSetRowFormatParams;
import org.apache.impala.thrift.TAlterTableSetTblPropertiesParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TCatalogUpdateResult;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnName;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TCommentOnParams;
import org.apache.impala.thrift.TCreateDataSourceParams;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TCreateDropRoleParams;
import org.apache.impala.thrift.TCreateFunctionParams;
import org.apache.impala.thrift.TCreateOrAlterViewParams;
import org.apache.impala.thrift.TCreateTableLikeParams;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDropDataSourceParams;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.thrift.TDropFunctionParams;
import org.apache.impala.thrift.TDropStatsParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.THdfsCachingOp;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TCopyTestCaseReq;
import org.apache.impala.thrift.TPartitionDef;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.thrift.TRangePartitionOperationType;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TResetMetadataResponse;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableRowFormat;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTestCaseData;
import org.apache.impala.thrift.TTruncateParams;
import org.apache.impala.thrift.TUpdateCatalogRequest;
import org.apache.impala.thrift.TUpdateCatalogResponse;
import org.apache.impala.util.CompressionUtil;
import org.apache.impala.util.FunctionUtils;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.MetaStoreUtil;
import org.slf4j.Logger;
import org.apache.thrift.TException;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.LoggerFactory;

/**
 * Class used to execute Catalog Operations, including DDL and refresh/invalidate
 * metadata requests. Acts as a bridge between the Thrift catalog operation requests
 * and the non-thrift Java Catalog objects.
 *
 * Updates are applied first to the Hive Metastore and only if they succeed, are then
 * applied to the catalog objects. To ensure consistency in the presence of failed HMS
 * updates, DDL operations should not directly modify the HMS objects of the catalog
 * objects but should operate on copies instead.
 *
 * The CatalogOpExecutor uses table-level locking to protect table metadata during
 * concurrent modifications and is responsible for assigning a new catalog version when
 * a table is modified (e.g. alterTable()).
 *
 * The following locking protocol is employed to ensure that modifying
 * the table metadata and assigning a new catalog version is performed atomically and
 * consistently in the presence of concurrent DDL operations. The following pattern
 * ensures that the catalog lock is never held for a long period of time, preventing
 * other DDL operations from making progress. This pattern only applies to single-table
 * update operations and requires the use of fair table locks to prevent starvation.
 *
 *   DO {
 *     Acquire the catalog lock (see CatalogServiceCatalog.versionLock_)
 *     Try to acquire a table lock
 *     IF the table lock acquisition fails {
 *       Release the catalog lock
 *       YIELD()
 *     ELSE
 *       BREAK
 *   } WHILE (TIMEOUT);
 *
 *   If (TIMEOUT) report error
 *
 *   Increment and get a new catalog version
 *   Release the catalog lock
 *   Modify table metadata
 *   Release table lock
 *
 * Note: The getCatalogObjects() function is the only case where this locking pattern is
 * not used since it accesses multiple catalog entities in order to compute a snapshot
 * of catalog metadata.
 *
 * Operations that CREATE/DROP catalog objects such as tables and databases employ the
 * following locking protocol:
 * 1. Acquire the metastoreDdlLock_
 * 2. Update the Hive Metastore
 * 3. Increment and get a new catalog version
 * 4. Update the catalog
 * 5. Grant/revoke owner privilege if authorization with ownership is enabled.
 * 6. Release the metastoreDdlLock_
 *
 * It is imperative that other operations that need to hold both the catalog lock and
 * table locks at the same time follow the same locking protocol and acquire these
 * locks in that particular order. Also, operations that modify table metadata
 * (e.g. alter table statements) should not acquire the metastoreDdlLock_.
 *
 * TODO: Refactor the CatalogOpExecutor and CatalogServiceCatalog classes and consolidate
 * the locking protocol into a single class.
 *
 * TODO: Improve catalog's consistency guarantees by using a hierarchical locking scheme.
 * Currently, only concurrent modidications to table metadata are guaranteed to be
 * serialized. Concurrent DDL operations that DROP/ADD catalog objects,
 * especially in the presence of INVALIDATE METADATA and REFRESH, are not guaranteed to
 * be consistent (see IMPALA-2774).
 *
 * TODO: Create a Hive Metastore utility class to move code that interacts with the
 * metastore out of this class.
 */
public class CatalogOpExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogOpExecutor.class);
  // Format string for exceptions returned by Hive Metastore RPCs.
  private final static String HMS_RPC_ERROR_FORMAT_STR =
      "Error making '%s' RPC to Hive Metastore: ";

  // The maximum number of partitions to update in one Hive Metastore RPC.
  // Used when persisting the results of COMPUTE STATS statements.
  // It is also used as an upper limit for the number of partitions allowed in one ADD
  // PARTITION statement.
  public final static short MAX_PARTITION_UPDATES_PER_RPC = 500;

  private final CatalogServiceCatalog catalog_;
  private final AuthorizationConfig authzConfig_;
  private final AuthorizationManager authzManager_;

  // Lock used to ensure that CREATE[DROP] TABLE[DATABASE] operations performed in
  // catalog_ and the corresponding RPC to apply the change in HMS are atomic.
  private final Object metastoreDdlLock_ = new Object();

  public CatalogOpExecutor(CatalogServiceCatalog catalog, AuthorizationConfig authzConfig,
      AuthorizationManager authzManager) throws ImpalaException {
    Preconditions.checkNotNull(authzManager);
    catalog_ = Preconditions.checkNotNull(catalog);
    authzConfig_ = Preconditions.checkNotNull(authzConfig);
    authzManager_ = Preconditions.checkNotNull(authzManager);
  }

  public CatalogServiceCatalog getCatalog() { return catalog_; }

  public AuthorizationManager getAuthzManager() { return authzManager_; }

  public TDdlExecResponse execDdlRequest(TDdlExecRequest ddlRequest)
      throws ImpalaException {
    TDdlExecResponse response = new TDdlExecResponse();
    response.setResult(new TCatalogUpdateResult());
    response.getResult().setCatalog_service_id(JniCatalog.getServiceId());
    User requestingUser = null;
    if (ddlRequest.isSetHeader()) {
      requestingUser = new User(ddlRequest.getHeader().getRequesting_user());
    }

    boolean syncDdl = ddlRequest.isSync_ddl();
    switch (ddlRequest.ddl_type) {
      case ALTER_DATABASE:
        alterDatabase(ddlRequest.getAlter_db_params(), response);
        break;
      case ALTER_TABLE:
        alterTable(ddlRequest.getAlter_table_params(), response);
        break;
      case ALTER_VIEW:
        alterView(ddlRequest.getAlter_view_params(), response);
        break;
      case CREATE_DATABASE:
        createDatabase(ddlRequest.getCreate_db_params(), response);
        break;
      case CREATE_TABLE_AS_SELECT:
        response.setNew_table_created(createTable(
            ddlRequest.getCreate_table_params(), response, syncDdl));
        break;
      case CREATE_TABLE:
        createTable(ddlRequest.getCreate_table_params(), response, syncDdl);
        break;
      case CREATE_TABLE_LIKE:
        createTableLike(ddlRequest.getCreate_table_like_params(), response, syncDdl);
        break;
      case CREATE_VIEW:
        createView(ddlRequest.getCreate_view_params(), response);
        break;
      case CREATE_FUNCTION:
        createFunction(ddlRequest.getCreate_fn_params(), response);
        break;
      case CREATE_DATA_SOURCE:
        createDataSource(ddlRequest.getCreate_data_source_params(), response);
        break;
      case COMPUTE_STATS:
        Preconditions.checkState(false, "Compute stats should trigger an ALTER TABLE.");
        break;
      case DROP_STATS:
        dropStats(ddlRequest.getDrop_stats_params(), response);
        break;
      case DROP_DATABASE:
        dropDatabase(ddlRequest.getDrop_db_params(), response);
        break;
      case DROP_TABLE:
      case DROP_VIEW:
        dropTableOrView(ddlRequest.getDrop_table_or_view_params(), response);
        break;
      case TRUNCATE_TABLE:
        truncateTable(ddlRequest.getTruncate_params(), response);
        break;
      case DROP_FUNCTION:
        dropFunction(ddlRequest.getDrop_fn_params(), response);
        break;
      case DROP_DATA_SOURCE:
        dropDataSource(ddlRequest.getDrop_data_source_params(), response);
        break;
      case CREATE_ROLE:
        createRole(requestingUser, ddlRequest.getCreate_drop_role_params(), response);
        break;
      case DROP_ROLE:
        dropRole(requestingUser, ddlRequest.getCreate_drop_role_params(), response);
        break;
      case GRANT_ROLE:
        grantRoleToGroup(requestingUser, ddlRequest.getGrant_revoke_role_params(),
            response);
        break;
      case REVOKE_ROLE:
        revokeRoleFromGroup(requestingUser, ddlRequest.getGrant_revoke_role_params(),
            response);
        break;
      case GRANT_PRIVILEGE:
        grantPrivilege(requestingUser, ddlRequest.getGrant_revoke_priv_params(),
            response);
        break;
      case REVOKE_PRIVILEGE:
        revokePrivilege(requestingUser, ddlRequest.getGrant_revoke_priv_params(),
            response);
        break;
      case COMMENT_ON:
        alterCommentOn(ddlRequest.getComment_on_params(), response);
        break;
      case COPY_TESTCASE:
        copyTestCaseData(ddlRequest.getCopy_test_case_params(), response);
        break;
      default: throw new IllegalStateException("Unexpected DDL exec request type: " +
          ddlRequest.ddl_type);
    }

    // If SYNC_DDL is set, set the catalog update that contains the results of this DDL
    // operation. The version of this catalog update is returned to the requesting
    // impalad which will wait until this catalog update has been broadcast to all the
    // coordinators.
    if (syncDdl) {
      response.getResult().setVersion(
          catalog_.waitForSyncDdlVersion(response.getResult()));
    }

    // At this point, the operation is considered successful. If any errors occurred
    // during execution, this function will throw an exception and the CatalogServer
    // will handle setting a bad status code.
    response.getResult().setStatus(new TStatus(TErrorCode.OK, new ArrayList<String>()));
    return response;
  }

  /**
   * Loads the testcase metadata from the request into the catalog cache and returns
   * the query statement this input testcase corresponds to. When loading the table and
   * database objects, this method overwrites any existing tables or databases with the
   * same name. However, these overwrites are *not* persistent. The old table/db
   * states can be recovered by blowing away the cache using INVALIDATE METADATA.
   */
  @VisibleForTesting
  public String copyTestCaseData(
      TCopyTestCaseReq request, TDdlExecResponse response)
      throws ImpalaException {
    Path inputPath = new Path(Preconditions.checkNotNull(request.input_path));
    // Read the data from the source FS.
    FileSystem fs;
    FSDataInputStream in;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      fs = FileSystemUtil.getFileSystemForPath(inputPath);
      in = fs.open(inputPath);
      IOUtils.copyBytes(in, out, fs.getConf(), /*close streams*/true);
    } catch (IOException e) {
      throw new ImpalaRuntimeException(String.format("Error reading test case data from" +
          " path: %s", inputPath), e);
    }
    byte[] decompressedBytes = CompressionUtil.deflateDecompress(out.toByteArray());
    TTestCaseData testCaseData = new TTestCaseData();
    try {
      JniUtil.deserializeThrift(testCaseData, decompressedBytes);
    } catch (ImpalaException e) {
      throw new CatalogException(String.format("Error deserializing the testcase data " +
          "at path %s. File data may be corrupt or incompatible with the current version "
          + "of Impala.", inputPath.toString()),e);
    }

    // Add the databases first, followed by the table and views information.
    // Overwrites any existing Db/Table objects with name clashes. Since we overwrite
    // the state in-memory and do not flush it to HMS, the older state can be recovered
    // by loading everything back from HMS. For ex: INVALIDATE METADATA.
    int numDbsAdded = 0;
    for (TDatabase thriftDb: testCaseData.getDbs()) {
      Db db = Db.fromTDatabase(thriftDb);
      // Set a new version to force an overwrite if a Db already exists with the same
      // name.
      db.setCatalogVersion(catalog_.incrementAndGetCatalogVersion());
      Db ret = catalog_.addDb(db.getName(), db.getMetaStoreDb());
      if (ret != null) {
        ++numDbsAdded;
        response.result.addToUpdated_catalog_objects(db.toTCatalogObject());
      }
    }

    int numTblsAdded = 0;
    int numViewsAdded = 0;
    for(TTable tTable: testCaseData.tables_and_views) {
      Db db = catalog_.getDb(tTable.db_name);
      // Db should have been created by now.
      Preconditions.checkNotNull(db, String.format("Missing db %s", tTable.db_name));
      Table t = Table.fromThrift(db, tTable);
      // Set a new version to force an overwrite if a table already exists with the same
      // name.
      t.setCatalogVersion(catalog_.incrementAndGetCatalogVersion());
      catalog_.addTable(db, t);
      if (t instanceof View) {
        ++numViewsAdded;
      } else {
        ++numTblsAdded;
      }
      // The table lock is needed here since toTCatalogObject() calls Table#toThrift()
      // which expects the current thread to hold this lock. For more details refer
      // to IMPALA-4092.
      t.getLock().lock();
      try {
        response.result.addToUpdated_catalog_objects(t.toTCatalogObject());
      } finally {
        t.getLock().unlock();
      }
    }
    StringBuilder responseStr = new StringBuilder();
    responseStr.append(String.format("Testcase generated using Impala version %s. ",
        testCaseData.getImpala_version()));
    responseStr.append(String.format(
        "%d db(s), %d table(s) and %d view(s) imported for query: ", numDbsAdded,
        numTblsAdded, numViewsAdded));
    responseStr.append("\n\n").append(testCaseData.getQuery_stmt());
    LOG.info(String.format("%s. Testcase path: %s", responseStr, inputPath));
    addSummary(response, responseStr.toString());
    return testCaseData.getQuery_stmt();
  }

  /**
   * Create result set from string 'summary', and attach it to 'response'.
   */
  private static void addSummary(TDdlExecResponse response, String summary) {
    TColumnValue resultColVal = new TColumnValue();
    resultColVal.setString_val(summary);
    TResultSet resultSet = new TResultSet();
    resultSet.setSchema(new TResultSetMetadata(Lists.newArrayList(new TColumn(
        "summary", Type.STRING.toThrift()))));
    TResultRow resultRow = new TResultRow();
    resultRow.setColVals(Lists.newArrayList(resultColVal));
    resultSet.setRows(Lists.newArrayList(resultRow));
    response.setResult_set(resultSet);
  }

  /**
   * Execute the ALTER TABLE command according to the TAlterTableParams and refresh the
   * table metadata, except for RENAME, ADD PARTITION and DROP PARTITION. This call is
   * thread-safe, i.e. concurrent operations on the same table are serialized.
   */
  private void alterTable(TAlterTableParams params, TDdlExecResponse response)
      throws ImpalaException {
    // When true, loads the file/block metadata.
    boolean reloadFileMetadata = false;
    // When true, loads the table schema and the column stats from the Hive Metastore.
    boolean reloadTableSchema = false;

    Reference<Long> numUpdatedPartitions = new Reference<>(0L);

    TableName tableName = TableName.fromThrift(params.getTable_name());
    Table tbl = getExistingTable(tableName.getDb(), tableName.getTbl());
    tryLock(tbl);
    // Get a new catalog version to assign to the table being altered.
    long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    addCatalogServiceIdentifiers(tbl, catalog_.getCatalogServiceId(), newCatalogVersion);
    final Timer.Context context
        = tbl.getMetrics().getTimer(Table.ALTER_DURATION_METRIC).time();
    try {
      if (params.getAlter_type() == TAlterTableType.RENAME_VIEW
          || params.getAlter_type() == TAlterTableType.RENAME_TABLE) {
        // RENAME is implemented as an ADD + DROP, so we need to execute it as we hold
        // the catalog lock.
        try {
          alterTableOrViewRename(tbl,
              TableName.fromThrift(params.getRename_params().getNew_table_name()),
              response);
          return;
        } finally {
          // release the version taken in the tryLock call above
          catalog_.getLock().writeLock().unlock();
        }
      }

      Table refreshedTable = null;
      boolean reloadMetadata = true;
      catalog_.getLock().writeLock().unlock();

      if (tbl instanceof KuduTable && altersKuduTable(params.getAlter_type())) {
        alterKuduTable(params, response, (KuduTable) tbl, newCatalogVersion);
        return;
      }
      switch (params.getAlter_type()) {
        case ADD_COLUMNS:
          TAlterTableAddColsParams addColParams = params.getAdd_cols_params();
          boolean added = alterTableAddCols(tbl, addColParams.getColumns(),
              addColParams.isIf_not_exists());
          reloadTableSchema = true;
          if (added) {
            addSummary(response, "New column(s) have been added to the table.");
          } else {
            addSummary(response, "No new column(s) have been added to the table.");
          }
          break;
        case REPLACE_COLUMNS:
          TAlterTableReplaceColsParams replaceColParams = params.getReplace_cols_params();
          alterTableReplaceCols(tbl, replaceColParams.getColumns());
          reloadTableSchema = true;
          addSummary(response, "Table columns have been replaced.");
          break;
        case ADD_PARTITION:
          // Create and add HdfsPartition objects to the corresponding HdfsTable and load
          // their block metadata. Get the new table object with an updated catalog
          // version.
          refreshedTable = alterTableAddPartitions(tbl, params.getAdd_partition_params());
          if (refreshedTable != null) {
            refreshedTable.setCatalogVersion(newCatalogVersion);
            addTableToCatalogUpdate(refreshedTable, response.result);
          }
          reloadMetadata = false;
          addSummary(response, "New partition has been added to the table.");
          break;
        case DROP_COLUMN:
          TAlterTableDropColParams dropColParams = params.getDrop_col_params();
          alterTableDropCol(tbl, dropColParams.getCol_name());
          reloadTableSchema = true;
          addSummary(response, "Column has been dropped.");
          break;
        case ALTER_COLUMN:
          TAlterTableAlterColParams alterColParams = params.getAlter_col_params();
          alterTableAlterCol(tbl, alterColParams.getCol_name(),
              alterColParams.getNew_col_def());
          reloadTableSchema = true;
          addSummary(response, "Column has been altered.");
          break;
        case DROP_PARTITION:
          TAlterTableDropPartitionParams dropPartParams =
              params.getDrop_partition_params();
          // Drop the partition from the corresponding table. Get the table object
          // with an updated catalog version. If the partition does not exist and
          // "IfExists" is true, null is returned. If "purge" option is specified
          // partition data is purged by skipping Trash, if configured.
          refreshedTable = alterTableDropPartition(
              tbl, dropPartParams.getPartition_set(),
              dropPartParams.isIf_exists(),
              dropPartParams.isPurge(), numUpdatedPartitions);
          if (refreshedTable != null) {
            refreshedTable.setCatalogVersion(newCatalogVersion);
            addTableToCatalogUpdate(refreshedTable, response.result);
          }
          addSummary(response,
              "Dropped " + numUpdatedPartitions.getRef() + " partition(s).");
          reloadMetadata = false;
          break;
        case RENAME_TABLE:
        case RENAME_VIEW:
          Preconditions.checkState(false,
              "RENAME TABLE/VIEW operation has been processed");
          break;
        case SET_FILE_FORMAT:
          TAlterTableSetFileFormatParams fileFormatParams =
              params.getSet_file_format_params();
          reloadFileMetadata = alterTableSetFileFormat(
              tbl, fileFormatParams.getPartition_set(),
                  fileFormatParams.getFile_format(), numUpdatedPartitions);

          if (fileFormatParams.isSetPartition_set()) {
            addSummary(response,
                "Updated " + numUpdatedPartitions.getRef() + " partition(s).");
          } else {
            addSummary(response, "Updated table.");
          }
          break;
        case SET_ROW_FORMAT:
          TAlterTableSetRowFormatParams rowFormatParams =
              params.getSet_row_format_params();
          reloadFileMetadata = alterTableSetRowFormat(tbl,
              rowFormatParams.getPartition_set(), rowFormatParams.getRow_format(),
              numUpdatedPartitions);
          if (rowFormatParams.isSetPartition_set()) {
            addSummary(response,
                "Updated " + numUpdatedPartitions.getRef() + " partition(s).");
          } else {
            addSummary(response, "Updated table.");
          }
          break;
        case SET_LOCATION:
          TAlterTableSetLocationParams setLocationParams =
              params.getSet_location_params();
          List<TPartitionKeyValue> partitionSpec = setLocationParams.getPartition_spec();
          reloadFileMetadata = alterTableSetLocation(tbl, partitionSpec,
               setLocationParams.getLocation());
          if (partitionSpec == null) {
            addSummary(response, "New location has been set.");
          } else {
            addSummary(response, "New location has been set for the specified partition.");
          }
          break;
        case SET_TBL_PROPERTIES:
          alterTableSetTblProperties(tbl, params.getSet_tbl_properties_params(),
              numUpdatedPartitions);
          if (params.getSet_tbl_properties_params().isSetPartition_set()) {
            addSummary(response,
                "Updated " + numUpdatedPartitions.getRef() + " partition(s).");
          } else {
            addSummary(response, "Updated table.");
          }
          break;
        case UPDATE_STATS:
          Preconditions.checkState(params.isSetUpdate_stats_params());
          Reference<Long> numUpdatedColumns = new Reference<>(0L);
          alterTableUpdateStats(tbl, params.getUpdate_stats_params(),
              numUpdatedPartitions, numUpdatedColumns);
          reloadTableSchema = true;
          addSummary(response, "Updated " + numUpdatedPartitions.getRef() +
              " partition(s) and " + numUpdatedColumns.getRef() + " column(s).");
          break;
        case SET_CACHED:
          Preconditions.checkState(params.isSetSet_cached_params());
          String op = params.getSet_cached_params().getCache_op().isSet_cached() ?
              "Cached " : "Uncached ";
          if (params.getSet_cached_params().getPartition_set() == null) {
            reloadFileMetadata =
                alterTableSetCached(tbl, params.getSet_cached_params());
            addSummary(response, op + "table.");
          } else {
            alterPartitionSetCached(tbl, params.getSet_cached_params(),
                numUpdatedPartitions);
            addSummary(response,
                op + numUpdatedPartitions.getRef() + " partition(s).");
          }
          break;
        case RECOVER_PARTITIONS:
          alterTableRecoverPartitions(tbl);
          addSummary(response, "Partitions have been recovered.");
          break;
        case SET_OWNER:
          Preconditions.checkState(params.isSetSet_owner_params());
          alterTableOrViewSetOwner(tbl, params.getSet_owner_params(), response);
          addSummary(response, "Updated table/view.");
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown ALTER TABLE operation type: " + params.getAlter_type());
      }

      if (reloadMetadata) {
        loadTableMetadata(tbl, newCatalogVersion, reloadFileMetadata,
            reloadTableSchema, null);
        addTableToCatalogUpdate(tbl, response.result);
      }
      // now that HMS alter operation has succeeded, add this version to list of inflight
      // events in catalog table if event processing is enabled
      catalog_.addVersionsForInflightEvents(tbl, newCatalogVersion);
    } finally {
      context.stop();
      Preconditions.checkState(!catalog_.getLock().isWriteLockedByCurrentThread());
      tbl.getLock().unlock();
    }
  }

  /**
   * Returns true if the given alteration type changes the underlying table stored in
   * Kudu in addition to the HMS table.
   */
  private boolean altersKuduTable(TAlterTableType type) {
    return type == TAlterTableType.ADD_COLUMNS
        || type == TAlterTableType.REPLACE_COLUMNS
        || type == TAlterTableType.DROP_COLUMN
        || type == TAlterTableType.ALTER_COLUMN
        || type == TAlterTableType.ADD_DROP_RANGE_PARTITION;
  }

  /**
   * Executes the ALTER TABLE command for a Kudu table and reloads its metadata.
   */
  private void alterKuduTable(TAlterTableParams params, TDdlExecResponse response,
      KuduTable tbl, long newCatalogVersion) throws ImpalaException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());
    switch (params.getAlter_type()) {
      case ADD_COLUMNS:
        TAlterTableAddColsParams addColParams = params.getAdd_cols_params();
        KuduCatalogOpExecutor.addColumn(tbl, addColParams.getColumns());
        addSummary(response, "Column(s) have been added.");
        break;
      case REPLACE_COLUMNS:
        TAlterTableReplaceColsParams replaceColParams = params.getReplace_cols_params();
        KuduCatalogOpExecutor.addColumn(tbl, replaceColParams.getColumns());
        addSummary(response, "Column(s) have been replaced.");
        break;
      case DROP_COLUMN:
        TAlterTableDropColParams dropColParams = params.getDrop_col_params();
        KuduCatalogOpExecutor.dropColumn(tbl, dropColParams.getCol_name());
        addSummary(response, "Column has been dropped.");
        break;
      case ALTER_COLUMN:
        TAlterTableAlterColParams alterColParams = params.getAlter_col_params();
        KuduCatalogOpExecutor.alterColumn(tbl, alterColParams.getCol_name(),
            alterColParams.getNew_col_def());
        addSummary(response, "Column has been altered.");
        break;
      case ADD_DROP_RANGE_PARTITION:
        TAlterTableAddDropRangePartitionParams partParams =
            params.getAdd_drop_range_partition_params();
        KuduCatalogOpExecutor.addDropRangePartition(tbl, partParams);
        addSummary(response, "Range partition has been " +
            (partParams.type == TRangePartitionOperationType.ADD ?
            "added." : "dropped."));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported ALTER TABLE operation for Kudu tables: " +
            params.getAlter_type());
    }

    loadTableMetadata(tbl, newCatalogVersion, true, true, null);
    addTableToCatalogUpdate(tbl, response.result);
  }

  /**
   * Loads the metadata of a table 'tbl' and assigns a new catalog version.
   * 'reloadFileMetadata', 'reloadTableSchema', and 'partitionsToUpdate'
   * are used only for HdfsTables and control which metadata to reload.
   * Throws a CatalogException if there is an error loading table metadata.
   */
  private void loadTableMetadata(Table tbl, long newCatalogVersion,
      boolean reloadFileMetadata, boolean reloadTableSchema,
      Set<String> partitionsToUpdate) throws CatalogException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          getMetaStoreTable(msClient, tbl);
      if (tbl instanceof HdfsTable) {
        ((HdfsTable) tbl).load(true, msClient.getHiveClient(), msTbl,
            reloadFileMetadata, reloadTableSchema, partitionsToUpdate);
      } else {
        tbl.load(true, msClient.getHiveClient(), msTbl);
      }
    }
    tbl.setCatalogVersion(newCatalogVersion);
  }

  /**
   * Serializes and adds table 'tbl' to a TCatalogUpdateResult object. Uses the
   * version of the serialized table as the version of the catalog update result.
   */
  private static void addTableToCatalogUpdate(Table tbl, TCatalogUpdateResult result) {
    Preconditions.checkNotNull(tbl);
    TCatalogObject updatedCatalogObject = tbl.toTCatalogObject();
    // TODO(todd): if client is a 'v2' impalad, only send back invalidation
    result.addToUpdated_catalog_objects(updatedCatalogObject);
    result.setVersion(updatedCatalogObject.getCatalog_version());
  }

  private Table addHdfsPartitions(Table tbl, List<Partition> partitions)
      throws CatalogException {
    Preconditions.checkNotNull(tbl);
    Preconditions.checkNotNull(partitions);
    if (!(tbl instanceof HdfsTable)) {
      throw new CatalogException("Table " + tbl.getFullName() + " is not an HDFS table");
    }
    HdfsTable hdfsTable = (HdfsTable) tbl;
    List<HdfsPartition> hdfsPartitions = hdfsTable.createAndLoadPartitions(partitions);
    for (HdfsPartition hdfsPartition: hdfsPartitions) {
      catalog_.addPartition(hdfsPartition);
    }
    return hdfsTable;
  }

  /**
   * Alters an existing view's definition in the metastore. Throws an exception
   * if the view does not exist or if the existing metadata entry is
   * a table instead of a a view.
   */
   private void alterView(TCreateOrAlterViewParams params, TDdlExecResponse resp)
      throws ImpalaException {
    TableName tableName = TableName.fromThrift(params.getView_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
        params.getColumns().size() > 0,
          "Null or empty column list given as argument to DdlExecutor.alterView");
    Table tbl = getExistingTable(tableName.getDb(), tableName.getTbl());
    Preconditions.checkState(tbl instanceof View, "Expected view: %s",
        tableName);
    tryLock(tbl);
    try {
      long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      catalog_.getLock().writeLock().unlock();
      addCatalogServiceIdentifiers(tbl, catalog_.getCatalogServiceId(),
          newCatalogVersion);
      // Operate on a copy of the metastore table to avoid prematurely applying the
      // alteration to our cached table in case the actual alteration fails.
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      if (!msTbl.getTableType().equalsIgnoreCase(
          (TableType.VIRTUAL_VIEW.toString()))) {
        throw new ImpalaRuntimeException(
            String.format("ALTER VIEW not allowed on a table: %s",
                tableName.toString()));
      }

      // Set the altered view attributes and update the metastore.
      setAlterViewAttributes(params, msTbl);
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Altering view %s", tableName));
      }
      applyAlterTable(msTbl, true);
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        tbl.load(true, msClient.getHiveClient(), msTbl);
      }
      addSummary(resp, "View has been altered.");
      tbl.setCatalogVersion(newCatalogVersion);
      addTableToCatalogUpdate(tbl, resp.result);
    } finally {
      Preconditions.checkState(!catalog_.getLock().isWriteLockedByCurrentThread());
      tbl.getLock().unlock();
    }
  }

  /**
   * Adds the catalog service id and the given catalog version to the table
   * parameters. No-op if event processing is disabled
   */
  private void addCatalogServiceIdentifiers(Table tbl, String catalogServiceId,
      long newCatalogVersion) {
    if (!catalog_.isExternalEventProcessingEnabled()) return;
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable();
    msTbl.putToParameters(
        MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(),
        catalogServiceId);
    msTbl.putToParameters(
        MetastoreEventPropertyKey.CATALOG_VERSION.getKey(),
        String.valueOf(newCatalogVersion));
  }

  /**
   * Alters an existing table's table and/or column statistics. Partitions are updated
   * in batches of size 'MAX_PARTITION_UPDATES_PER_RPC'.
   * This function is used by COMPUTE STATS, COMPUTE INCREMENTAL STATS and
   * ALTER TABLE SET COLUMN STATS.
   * Updates table property 'impala.lastComputeStatsTime' for COMPUTE (INCREMENTAL) STATS,
   * but not for ALTER TABLE SET COLUMN STATS.
   * Returns the number of updated partitions and columns in 'numUpdatedPartitions'
   * and 'numUpdatedColumns', respectively.
   */
  private void alterTableUpdateStats(Table table, TAlterTableUpdateStatsParams params,
      Reference<Long> numUpdatedPartitions, Reference<Long> numUpdatedColumns)
      throws ImpalaException {
    Preconditions.checkState(table.getLock().isHeldByCurrentThread());
    Preconditions.checkState(params.isSetTable_stats() || params.isSetColumn_stats());

    TableName tableName = table.getTableName();
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    if (LOG.isInfoEnabled()) {
      int numPartitions =
          params.isSetPartition_stats() ? params.partition_stats.size() : 0;
      int numColumns =
          params.isSetColumn_stats() ? params.column_stats.size() : 0;
      LOG.info(String.format(
          "Updating stats for table %s: table-stats=%s partitions=%d column-stats=%d",
          tableName, params.isSetTable_stats(), numPartitions, numColumns));
    }

    // Update column stats.
    ColumnStatistics colStats = null;
    numUpdatedColumns.setRef(0L);
    if (params.isSetColumn_stats()) {
      colStats = createHiveColStats(params, table);
      if (colStats.getStatsObjSize() > 0) {
        try(MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
          msClient.getHiveClient().updateTableColumnStatistics(colStats);
        } catch (Exception e) {
          throw new ImpalaRuntimeException(String.format(HMS_RPC_ERROR_FORMAT_STR,
              "updateTableColumnStatistics"), e);
        }
      }
      numUpdatedColumns.setRef((long) colStats.getStatsObjSize());
    }

    // Deep copy the msTbl to avoid updating our cache before successfully persisting
    // the results to the metastore.
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        table.getMetaStoreTable().deepCopy();

    // Update partition-level row counts and incremental column stats for
    // partitioned Hdfs tables.
    List<HdfsPartition> modifiedParts = null;
    if (params.isSetPartition_stats() && table.getNumClusteringCols() > 0) {
      Preconditions.checkState(table instanceof HdfsTable);
      modifiedParts = updatePartitionStats(params, (HdfsTable) table);
      bulkAlterPartitions(table, modifiedParts);
    }

    if (params.isSetTable_stats()) {
      // Update table row count and total file bytes.
      updateTableStats(params, msTbl);
      // Set impala.lastComputeStatsTime just before alter_table to ensure that it is as
      // accurate as possible.
      Table.updateTimestampProperty(msTbl, HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME);
    }

    applyAlterTable(msTbl, false);
    numUpdatedPartitions.setRef(0L);
    if (modifiedParts != null) {
      numUpdatedPartitions.setRef((long) modifiedParts.size());
    } else if (params.isSetTable_stats()) {
      numUpdatedPartitions.setRef(1L);
    }
  }

  /**
   * Updates the row counts and incremental column stats of the partitions in the given
   * Impala table based on the given update stats parameters. Returns the modified Impala
   * partitions.
   * Row counts for missing or new partitions as a result of concurrent table alterations
   * are set to 0.
   */
  private List<HdfsPartition> updatePartitionStats(TAlterTableUpdateStatsParams params,
      HdfsTable table) throws ImpalaException {
    Preconditions.checkState(params.isSetPartition_stats());
    List<HdfsPartition> modifiedParts = Lists.newArrayList();
    // TODO(todd) only load the partitions that were modified in 'params'.
    Collection<? extends FeFsPartition> parts =
        FeCatalogUtils.loadAllPartitions(table);
    for (FeFsPartition fePartition: parts) {
      // TODO(todd): avoid downcast to implementation class
      HdfsPartition partition = (HdfsPartition)fePartition;

      // NULL keys are returned as 'NULL' in the partition_stats map, so don't substitute
      // this partition's keys with Hive's replacement value.
      List<String> partitionValues = partition.getPartitionValuesAsStrings(false);
      TPartitionStats partitionStats = params.partition_stats.get(partitionValues);
      if (partitionStats == null) {
        // No stats were collected for this partition. This means that it was not included
        // in the original computation statements. If the backend does not find any rows
        // for a partition that should be included, it will generate an empty
        // TPartitionStats object.
        if (params.expect_all_partitions == false) continue;

        // If all partitions are expected, fill in any missing stats with an empty entry.
        partitionStats = new TPartitionStats();
        if (params.is_incremental) {
          partitionStats.intermediate_col_stats = Maps.newHashMap();
        }
        partitionStats.stats = new TTableStats();
        partitionStats.stats.setNum_rows(0L);
      }

      // Unconditionally update the partition stats and row count, even if the partition
      // already has identical ones. This behavior results in possibly redundant work,
      // but it is predictable and easy to reason about because it does not depend on the
      // existing state of the metadata. See IMPALA-2201.
      long numRows = partitionStats.stats.num_rows;
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Updating stats for partition %s: numRows=%d",
            partition.getValuesAsString(), numRows));
      }
      PartitionStatsUtil.partStatsToPartition(partitionStats, partition);
      partition.putToParameters(StatsSetupConst.ROW_COUNT, String.valueOf(numRows));
      // HMS requires this param for stats changes to take effect.
      partition.putToParameters(MetastoreShim.statsGeneratedViaStatsTaskParam());
      modifiedParts.add(partition);
    }
    return modifiedParts;
  }

  /**
   * Updates the row count and total file bytes of the given HMS table based on the
   * the update stats parameters.
   */
  private void updateTableStats(TAlterTableUpdateStatsParams params,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws ImpalaException {
    Preconditions.checkState(params.isSetTable_stats());
    long numRows = params.table_stats.num_rows;
    // Update the table's ROW_COUNT and TOTAL_SIZE parameters.
    msTbl.putToParameters(StatsSetupConst.ROW_COUNT, String.valueOf(numRows));
    if (params.getTable_stats().isSetTotal_file_bytes()) {
      msTbl.putToParameters(StatsSetupConst.TOTAL_SIZE,
          String.valueOf(params.getTable_stats().total_file_bytes));
    }
    // HMS requires this param for stats changes to take effect.
    Pair<String, String> statsTaskParam = MetastoreShim.statsGeneratedViaStatsTaskParam();
    msTbl.putToParameters(statsTaskParam.first, statsTaskParam.second);
  }

  /**
   * Create HMS column statistics for the given table based on the give map from column
   * name to column stats. Missing or new columns as a result of concurrent table
   * alterations are ignored.
   */
  private static ColumnStatistics createHiveColStats(
      TAlterTableUpdateStatsParams params, Table table) {
    Preconditions.checkState(params.isSetColumn_stats());
    // Collection of column statistics objects to be returned.
    ColumnStatistics colStats = new ColumnStatistics();
    colStats.setStatsDesc(
        new ColumnStatisticsDesc(true, table.getDb().getName(), table.getName()));
    // Generate Hive column stats objects from the update stats params.
    for (Map.Entry<String, TColumnStats> entry: params.getColumn_stats().entrySet()) {
      String colName = entry.getKey();
      Column tableCol = table.getColumn(entry.getKey());
      // Ignore columns that were dropped in the meantime.
      if (tableCol == null) continue;
      // If we know the number of rows in the table, cap NDV of the column appropriately.
      long ndvCap = params.isSetTable_stats() ? params.table_stats.num_rows : -1;
      ColumnStatisticsData colStatsData = ColumnStats.createHiveColStatsData(
              ndvCap, entry.getValue(), tableCol.getType());
      if (colStatsData == null) continue;
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Updating column stats for %s: numDVs=%d numNulls=%d " +
            "maxSize=%d avgSize=%.2f", colName, entry.getValue().getNum_distinct_values(),
            entry.getValue().getNum_nulls(), entry.getValue().getMax_size(),
            entry.getValue().getAvg_size()));
      }
      ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj(colName,
          tableCol.getType().toString().toLowerCase(), colStatsData);
      colStats.addToStatsObj(colStatsObj);
    }
    return colStats;
  }

  /**
   * Creates a new database in the metastore and adds the db name to the internal
   * metadata cache, marking its metadata to be lazily loaded on the next access.
   * Re-throws any Hive Meta Store exceptions encountered during the create, these
   * may vary depending on the Meta Store connection type (thrift vs direct db).
   */
  private void createDatabase(TCreateDbParams params, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(params);
    String dbName = params.getDb();
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name passed as argument to Catalog.createDatabase");
    Db existingDb = catalog_.getDb(dbName);
    if (params.if_not_exists && existingDb != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Skipping database creation because " + dbName + " already exists "
            + "and IF NOT EXISTS was specified.");
      }
      Preconditions.checkNotNull(existingDb);
      // TODO(todd): if client is a 'v2' impalad, only send back invalidation
      resp.getResult().addToUpdated_catalog_objects(existingDb.toTCatalogObject());
      resp.getResult().setVersion(existingDb.getCatalogVersion());
      addSummary(resp, "Database already exists.");
      return;
    }
    org.apache.hadoop.hive.metastore.api.Database db =
        new org.apache.hadoop.hive.metastore.api.Database();
    db.setName(dbName);
    if (params.getComment() != null) {
      db.setDescription(params.getComment());
    }
    if (params.getLocation() != null) {
      db.setLocationUri(params.getLocation());
    }
    db.setOwnerName(params.getOwner());
    db.setOwnerType(PrincipalType.USER);
    if (LOG.isTraceEnabled()) LOG.trace("Creating database " + dbName);
    Db newDb = null;
    synchronized (metastoreDdlLock_) {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        try {
          msClient.getHiveClient().createDatabase(db);
          // Load the database back from the HMS. It's unfortunate we need two
          // RPCs here, but otherwise we can't populate the location field of the
          // DB properly. We'll take the slight chance of a race over the incorrect
          // behavior of showing no location in 'describe database' (IMPALA-7439).
          db = msClient.getHiveClient().getDatabase(dbName);
          newDb = catalog_.addDb(dbName, db);
          addSummary(resp, "Database has been created.");
        } catch (AlreadyExistsException e) {
          if (!params.if_not_exists) {
            throw new ImpalaRuntimeException(
                String.format(HMS_RPC_ERROR_FORMAT_STR, "createDatabase"), e);
          }
          addSummary(resp, "Database already exists.");
          if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Ignoring '%s' when creating database %s because " +
                "IF NOT EXISTS was specified.", e, dbName));
          }
          newDb = catalog_.getDb(dbName);
          if (newDb == null) {
            try {
              org.apache.hadoop.hive.metastore.api.Database msDb =
                  msClient.getHiveClient().getDatabase(dbName);
              newDb = catalog_.addDb(dbName, msDb);
            } catch (TException e1) {
              throw new ImpalaRuntimeException(
                  String.format(HMS_RPC_ERROR_FORMAT_STR, "createDatabase"), e1);
            }
          }
        } catch (TException e) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "createDatabase"), e);
        }
      }

      Preconditions.checkNotNull(newDb);
      // TODO(todd): if client is a 'v2' impalad, only send back invalidation
      resp.result.addToUpdated_catalog_objects(newDb.toTCatalogObject());
      if (authzConfig_.isEnabled()) {
        authzManager_.updateDatabaseOwnerPrivilege(params.server_name, newDb.getName(),
            /* oldOwner */ null, /* oldOwnerType */ null,
            newDb.getMetaStoreDb().getOwnerName(), newDb.getMetaStoreDb().getOwnerType(),
            resp);
      }
    }
    resp.result.setVersion(newDb.getCatalogVersion());
  }

  private void createFunction(TCreateFunctionParams params, TDdlExecResponse resp)
      throws ImpalaException {
    Function fn = Function.fromThrift(params.getFn());
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Adding %s: %s",
          fn.getClass().getSimpleName(), fn.signatureString()));
    }
    boolean isPersistentJavaFn =
        (fn.getBinaryType() == TFunctionBinaryType.JAVA) && fn.isPersistent();
    synchronized (metastoreDdlLock_) {
      Db db = catalog_.getDb(fn.dbName());
      if (db == null) {
        throw new CatalogException("Database: " + fn.dbName() + " does not exist.");
      }
      // Search for existing functions with the same name or signature that would
      // conflict with the function being added.
      for (Function function: db.getFunctions(fn.functionName())) {
        if (isPersistentJavaFn || (function.isPersistent() &&
            (function.getBinaryType() == TFunctionBinaryType.JAVA)) ||
                function.compare(fn, Function.CompareMode.IS_INDISTINGUISHABLE)) {
          if (!params.if_not_exists) {
            throw new CatalogException("Function " + fn.functionName() +
                " already exists.");
          }
          addSummary(resp, "Function already exists.");
          return;
        }
      }

      List<TCatalogObject> addedFunctions = Lists.newArrayList();
      if (isPersistentJavaFn) {
        // For persistent Java functions we extract all supported function signatures from
        // the corresponding Jar and add each signature to the catalog.
        Preconditions.checkState(fn instanceof ScalarFunction);
        org.apache.hadoop.hive.metastore.api.Function hiveFn =
            ((ScalarFunction)fn).toHiveFunction();
        List<Function> funcs = FunctionUtils.extractFunctions(fn.dbName(), hiveFn,
            BackendConfig.INSTANCE.getBackendCfg().local_library_path);
        if (funcs.isEmpty()) {
          throw new CatalogException(
            "No compatible function signatures found in class: " + hiveFn.getClassName());
        }
        if (addJavaFunctionToHms(fn.dbName(), hiveFn, params.if_not_exists)) {
          for (Function addedFn: funcs) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(String.format("Adding function: %s.%s", addedFn.dbName(),
                  addedFn.signatureString()));
            }
            Preconditions.checkState(catalog_.addFunction(addedFn));
            addedFunctions.add(addedFn.toTCatalogObject());
          }
        }
      } else {
        if (catalog_.addFunction(fn)) {
          // Flush DB changes to metastore
          applyAlterDatabase(db.getMetaStoreDb());
          addedFunctions.add(fn.toTCatalogObject());
        }
      }

      if (!addedFunctions.isEmpty()) {
        resp.result.setUpdated_catalog_objects(addedFunctions);
        resp.result.setVersion(catalog_.getCatalogVersion());
        addSummary(resp, "Function has been created.");
      } else {
        addSummary(resp, "Function already exists.");
      }
    }
  }

  private void createDataSource(TCreateDataSourceParams params, TDdlExecResponse resp)
      throws ImpalaException {
    // TODO(IMPALA-7131): support data sources with LocalCatalog.
    if (LOG.isTraceEnabled()) { LOG.trace("Adding DATA SOURCE: " + params.toString()); }
    DataSource dataSource = DataSource.fromThrift(params.getData_source());
    DataSource existingDataSource = catalog_.getDataSource(dataSource.getName());
    if (existingDataSource != null) {
      if (!params.if_not_exists) {
        throw new ImpalaRuntimeException("Data source " + dataSource.getName() +
            " already exists.");
      }
      addSummary(resp, "Data source already exists.");
      resp.result.addToUpdated_catalog_objects(existingDataSource.toTCatalogObject());
      resp.result.setVersion(existingDataSource.getCatalogVersion());
      return;
    }
    catalog_.addDataSource(dataSource);
    resp.result.addToUpdated_catalog_objects(dataSource.toTCatalogObject());
    resp.result.setVersion(dataSource.getCatalogVersion());
    addSummary(resp, "Data source has been created.");
  }

  private void dropDataSource(TDropDataSourceParams params, TDdlExecResponse resp)
      throws ImpalaException {
    // TODO(IMPALA-7131): support data sources with LocalCatalog.
    if (LOG.isTraceEnabled()) LOG.trace("Drop DATA SOURCE: " + params.toString());
    DataSource dataSource = catalog_.removeDataSource(params.getData_source());
    if (dataSource == null) {
      if (!params.if_exists) {
        throw new ImpalaRuntimeException("Data source " + params.getData_source() +
            " does not exists.");
      }
      addSummary(resp, "Data source does not exist.");
      // No data source was removed.
      resp.result.setVersion(catalog_.getCatalogVersion());
      return;
    }
    resp.result.addToRemoved_catalog_objects(dataSource.toTCatalogObject());
    resp.result.setVersion(dataSource.getCatalogVersion());
    addSummary(resp, "Data source has been dropped.");
  }

  /**
   * Drops all table and column stats from the target table in the HMS and
   * updates the Impala catalog. Throws an ImpalaException if any errors are
   * encountered as part of this operation. Acquires a lock on the modified table
   * to protect against concurrent modifications.
   */
  private void dropStats(TDropStatsParams params, TDdlExecResponse resp)
      throws ImpalaException {
    Table table = getExistingTable(params.getTable_name().getDb_name(),
        params.getTable_name().getTable_name());
    Preconditions.checkNotNull(table);
    if (!catalog_.tryLockTable(table)) {
      throw new InternalException(String.format("Error dropping stats for table %s " +
          "due to lock contention", table.getFullName()));
    }
    try {
      long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      catalog_.getLock().writeLock().unlock();
      if (params.getPartition_set() == null) {
        // TODO: Report the number of updated partitions/columns to the user?
        // TODO: bulk alter the partitions.
        dropColumnStats(table);
        dropTableStats(table);
      } else {
        HdfsTable hdfsTbl = (HdfsTable) table;
        List<HdfsPartition> partitions =
            hdfsTbl.getPartitionsFromPartitionSet(params.getPartition_set());
        if (partitions.isEmpty()) {
          addSummary(resp, "No partitions found for table.");
          return;
        }

        for(HdfsPartition partition : partitions) {
          if (partition.getPartitionStatsCompressed() != null) {
            partition.dropPartitionStats();
            try {
              applyAlterPartition(table, partition);
            } finally {
              partition.markDirty();
            }
          }
        }
      }
      loadTableMetadata(table, newCatalogVersion, false, true, null);
      addTableToCatalogUpdate(table, resp.result);
      addSummary(resp, "Stats have been dropped.");
    } finally {
      Preconditions.checkState(!catalog_.getLock().isWriteLockedByCurrentThread());
      table.getLock().unlock();
    }
  }

  /**
   * Drops all column stats from the table in the HMS. Returns the number of columns
   * that were updated as part of this operation.
   */
  private int dropColumnStats(Table table) throws ImpalaRuntimeException {
    Preconditions.checkState(table.getLock().isHeldByCurrentThread());
    int numColsUpdated = 0;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      for (Column col: table.getColumns()) {
        // Skip columns that don't have stats.
        if (!col.getStats().hasStats()) continue;

        try {
          msClient.getHiveClient().deleteTableColumnStatistics(
              table.getDb().getName(), table.getName(), col.getName());
          ++numColsUpdated;
        } catch (NoSuchObjectException e) {
          // We don't care if the column stats do not exist, just ignore the exception.
          // We would only expect to make it here if the Impala and HMS metadata
          // diverged.
        } catch (TException e) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR,
                  "delete_table_column_statistics"), e);
        }
      }
    }
    return numColsUpdated;
  }

  /**
   * Drops all table and partition stats from this table in the HMS.
   * Partitions are updated in batches of MAX_PARTITION_UPDATES_PER_RPC. Returns
   * the number of partitions updated as part of this operation, or 1 if the table
   * is unpartitioned.
   */
  private int dropTableStats(Table table) throws ImpalaException {
    Preconditions.checkState(table.getLock().isHeldByCurrentThread());
    // Delete the ROW_COUNT from the table (if it was set).
    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getMetaStoreTable();
    int numTargetedPartitions = 0;
    boolean droppedRowCount =
        msTbl.getParameters().remove(StatsSetupConst.ROW_COUNT) != null;
    boolean droppedTotalSize =
        msTbl.getParameters().remove(StatsSetupConst.TOTAL_SIZE) != null;
    if (droppedRowCount || droppedTotalSize) {
      applyAlterTable(msTbl, false);
      ++numTargetedPartitions;
    }

    if (!(table instanceof HdfsTable) || table.getNumClusteringCols() == 0) {
      // If this is not an HdfsTable or if the table is not partitioned, there
      // is no more work to be done so just return.
      return numTargetedPartitions;
    }

    // Now clear the stats for all partitions in the table.
    HdfsTable hdfsTable = (HdfsTable) table;
    Preconditions.checkNotNull(hdfsTable);

    // List of partitions that were modified as part of this operation.
    List<HdfsPartition> modifiedParts = Lists.newArrayList();
    Collection<? extends FeFsPartition> parts =
        FeCatalogUtils.loadAllPartitions(hdfsTable);
    for (FeFsPartition fePart: parts) {
      // TODO(todd): avoid downcast
      HdfsPartition part = (HdfsPartition) fePart;
      boolean isModified = false;
      if (part.getPartitionStatsCompressed() != null) {
        part.dropPartitionStats();
        isModified = true;
      }

      // Remove the ROW_COUNT parameter if it has been set.
      if (part.getParameters().remove(StatsSetupConst.ROW_COUNT) != null) {
        isModified = true;
      }

      if (isModified) modifiedParts.add(part);
    }

    bulkAlterPartitions(table, modifiedParts);
    return modifiedParts.size();
  }

  /**
   * Drops a database from the metastore and removes the database's metadata from the
   * internal cache. Attempts to remove the HDFS cache directives of the underlying
   * tables. Re-throws any HMS exceptions encountered during the drop.
   */
  private void dropDatabase(TDropDbParams params, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(params);
    Preconditions.checkState(params.getDb() != null && !params.getDb().isEmpty(),
        "Null or empty database name passed as argument to Catalog.dropDatabase");

    LOG.trace("Dropping database " + params.getDb());
    Db db = catalog_.getDb(params.db);
    if (db != null && db.numFunctions() > 0 && !params.cascade) {
      throw new CatalogException("Database " + db.getName() + " is not empty");
    }

    TCatalogObject removedObject = null;
    synchronized (metastoreDdlLock_) {
      // Remove all the Kudu tables of 'db' from the Kudu storage engine.
      if (db != null && params.cascade) dropTablesFromKudu(db);

      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        msClient.getHiveClient().dropDatabase(
            params.getDb(), true, params.if_exists, params.cascade);
        addSummary(resp, "Database has been dropped.");
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "dropDatabase"), e);
      }
      Db removedDb = catalog_.removeDb(params.getDb());

      if (removedDb == null) {
        // Nothing was removed from the catalogd's cache.
        resp.result.setVersion(catalog_.getCatalogVersion());
        return;
      }
      // Make sure the cache directives, if any, of the underlying tables are removed
      for (String tableName: removedDb.getAllTableNames()) {
        uncacheTable(removedDb.getTable(tableName));
      }
      removedObject = removedDb.toTCatalogObject();
      if (authzConfig_.isEnabled()) {
        authzManager_.updateDatabaseOwnerPrivilege(params.server_name, db.getName(),
            db.getMetaStoreDb().getOwnerName(), db.getMetaStoreDb().getOwnerType(),
            /* newOwner */ null, /* newOwnerType */ null, resp);
      }
    }

    Preconditions.checkNotNull(removedObject);
    resp.result.setVersion(removedObject.getCatalog_version());
    resp.result.addToRemoved_catalog_objects(removedObject);
    addSummary(resp, "Database has been dropped.");
  }

  /**
   * Drops all the Kudu tables of database 'db' from the Kudu storage engine. Retrieves
   * the Kudu table name of each table in 'db' from HMS. Throws an ImpalaException if
   * metadata for Kudu tables cannot be loaded from HMS or if an error occurs while
   * trying to drop a table from Kudu.
   */
  private void dropTablesFromKudu(Db db) throws ImpalaException {
    // If the table format isn't available, because the table hasn't been loaded yet,
    // the metadata must be fetched from the Hive Metastore.
    List<String> incompleteTableNames = Lists.newArrayList();
    List<org.apache.hadoop.hive.metastore.api.Table> msTables = Lists.newArrayList();
    for (Table table: db.getTables()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = table.getMetaStoreTable();
      if (msTable == null) {
        incompleteTableNames.add(table.getName());
      } else {
        msTables.add(msTable);
      }
    }
    if (!incompleteTableNames.isEmpty()) {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        msTables.addAll(msClient.getHiveClient().getTableObjectsByName(
            db.getName(), incompleteTableNames));
      } catch (TException e) {
        LOG.error(String.format(HMS_RPC_ERROR_FORMAT_STR, "getTableObjectsByName") +
            e.getMessage());
      }
    }
    for (org.apache.hadoop.hive.metastore.api.Table msTable: msTables) {
      if (!KuduTable.isKuduTable(msTable) || Table.isExternalTable(msTable)) continue;
      // The operation will be aborted if the Kudu table cannot be dropped. If for
      // some reason Kudu is permanently stuck in a non-functional state, the user is
      // expected to ALTER TABLE to either set the table to UNMANAGED or set the format
      // to something else.
      KuduCatalogOpExecutor.dropTable(msTable, /*if exists*/ true);
    }
  }

  /**
   * Drops a table or view from the metastore and removes it from the catalog.
   * Also drops all associated caching requests on the table and/or table's partitions,
   * uncaching all table data. If params.purge is true, table data is permanently
   * deleted.
   */
  private void dropTableOrView(TDropTableOrViewParams params, TDdlExecResponse resp)
      throws ImpalaException {
    TableName tableName = TableName.fromThrift(params.getTable_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    LOG.trace(String.format("Dropping table/view %s", tableName));

    // If the table exists, ensure that it is loaded before we try to operate on it.
    // We do this up here rather than down below to avoid doing too much table-loading
    // work while holding the DDL lock. We can't simply use 'getExistingTable' because
    // we rely on more granular checks to provide the correct summary message for
    // the 'IF EXISTS' case.
    //
    // In the standard catalogd implementation, the table will most likely already
    // be loaded because the planning phase on the impalad side triggered the loading.
    // In the LocalCatalog configuration, however, this is often necessary.
    try {
      catalog_.getOrLoadTable(params.getTable_name().db_name,
          params.getTable_name().table_name);
    } catch (CatalogException e) {
      // Ignore exceptions -- the above was just to trigger loading. Failure to load
      // or non-existence of the database will be handled down below.
    }

    TCatalogObject removedObject = new TCatalogObject();
    synchronized (metastoreDdlLock_) {
      Db db = catalog_.getDb(params.getTable_name().db_name);
      if (db == null) {
        String dbNotExist = "Database does not exist: " + params.getTable_name().db_name;
        if (params.if_exists) {
          addSummary(resp, dbNotExist);
          return;
        }
        throw new CatalogException(dbNotExist);
      }
      Table existingTbl = db.getTable(params.getTable_name().table_name);
      if (existingTbl == null) {
        if (params.if_exists) {
          addSummary(resp, (params.is_table ? "Table " : "View ") + "does not exist.");
          return;
        }
        throw new CatalogException("Table/View does not exist.");
      }

      // Retrieve the HMS table to determine if this is a Kudu table.
      org.apache.hadoop.hive.metastore.api.Table msTbl = existingTbl.getMetaStoreTable();
      if (msTbl == null) {
        Preconditions.checkState(existingTbl instanceof IncompleteTable);
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
          msTbl = msClient.getHiveClient().getTable(tableName.getDb(),
              tableName.getTbl());
        } catch (TException e) {
          LOG.error(String.format(HMS_RPC_ERROR_FORMAT_STR, "getTable") + e.getMessage());
        }
      }
      if (msTbl != null && KuduTable.isKuduTable(msTbl)
          && !Table.isExternalTable(msTbl)) {
        KuduCatalogOpExecutor.dropTable(msTbl, /* if exists */ true);
      }

      // Check to make sure we don't drop a view with "drop table" statement and
      // vice versa. is_table field is marked optional in TDropTableOrViewParams to
      // maintain catalog api compatibility.
      // TODO: Remove params.isSetIs_table() check once catalog api compatibility is
      // fixed.
      if (params.isSetIs_table() && ((params.is_table && existingTbl instanceof View)
          || (!params.is_table && !(existingTbl instanceof View)))) {
        String errorMsg = "DROP " + (params.is_table ? "TABLE " : "VIEW ") +
            "not allowed on a " + (params.is_table ? "view: " : "table: ") + tableName;
        if (params.if_exists) {
          addSummary(resp, "Drop " + (params.is_table ? "table " : "view ") +
              "is not allowed on a " + (params.is_table ? "view." : "table."));
          return;
        }
        throw new CatalogException(errorMsg);
      }
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        msClient.getHiveClient().dropTable(
            tableName.getDb(), tableName.getTbl(), true, params.if_exists, params.purge);
      } catch (NoSuchObjectException e) {
        throw new ImpalaRuntimeException(String.format("Table %s no longer exists in " +
            "the Hive MetaStore. Run 'invalidate metadata %s' to update the Impala " +
            "catalog.", tableName, tableName));
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "dropTable"), e);
      }
      addSummary(resp, (params.is_table ? "Table " : "View ") + "has been dropped.");

      Table table = catalog_.removeTable(params.getTable_name().db_name,
          params.getTable_name().table_name);
      if (table == null) {
        // Nothing was removed from the catalogd's cache.
        resp.result.setVersion(catalog_.getCatalogVersion());
        return;
      }
      resp.result.setVersion(table.getCatalogVersion());
      uncacheTable(table);
      if (table.getMetaStoreTable() != null) {
        if (authzConfig_.isEnabled()) {
          authzManager_.updateTableOwnerPrivilege(params.server_name,
              table.getDb().getName(), table.getName(),
              table.getMetaStoreTable().getOwner(),
              table.getMetaStoreTable().getOwnerType(), /* newOwner */ null,
              /* newOwnerType */ null, resp);
        }
      }
    }
    removedObject.setType(TCatalogObjectType.TABLE);
    removedObject.setTable(new TTable());
    removedObject.getTable().setTbl_name(tableName.getTbl());
    removedObject.getTable().setDb_name(tableName.getDb());
    removedObject.setCatalog_version(resp.result.getVersion());
    resp.result.addToRemoved_catalog_objects(removedObject);
  }

  /**
   * Drops all associated caching requests on the table and/or table's partitions,
   * uncaching all table data, if applicable. Throws no exceptions, only logs errors.
   * Does not update the HMS.
   */
  private static void uncacheTable(FeTable table) {
    if (!(table instanceof FeFsTable)) return;
    FeFsTable hdfsTable = (FeFsTable) table;
    if (hdfsTable.isMarkedCached()) {
      try {
        HdfsCachingUtil.removeTblCacheDirective(table.getMetaStoreTable());
      } catch (Exception e) {
        LOG.error("Unable to uncache table: " + table.getFullName(), e);
      }
    }
    if (table.getNumClusteringCols() > 0) {
      Collection<? extends FeFsPartition> parts =
          FeCatalogUtils.loadAllPartitions(hdfsTable);
      for (FeFsPartition part: parts) {
        if (part.isMarkedCached()) {
          try {
            HdfsCachingUtil.removePartitionCacheDirective(part);
          } catch (Exception e) {
            LOG.error("Unable to uncache partition: " + part.getPartitionName(), e);
          }
        }
      }
    }
  }

  /**
   * Truncate a table by deleting all files in its partition directories, and dropping
   * all column and table statistics. Acquires a table lock to protect against
   * concurrent table modifications.
   * TODO truncate specified partitions.
   */
  private void truncateTable(TTruncateParams params, TDdlExecResponse resp)
      throws ImpalaException {
    TTableName tblName = params.getTable_name();
    Table table = null;
    try {
      table = getExistingTable(tblName.getDb_name(), tblName.getTable_name());
    } catch (TableNotFoundException e) {
      if (params.if_exists) {
        addSummary(resp, "Table does not exist.");
        return;
      }
      throw e;
    }
    Preconditions.checkNotNull(table);
    if (!(table instanceof HdfsTable)) {
      throw new CatalogException(
          String.format("TRUNCATE TABLE not supported on non-HDFS table: %s",
          table.getFullName()));
    }
    if (!catalog_.tryLockTable(table)) {
      throw new InternalException(String.format("Error truncating table %s due to lock " +
          "contention", table.getFullName()));
    }
    try {
      long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      catalog_.getLock().writeLock().unlock();
      try {
        HdfsTable hdfsTable = (HdfsTable)table;
        Collection<? extends FeFsPartition> parts =
            FeCatalogUtils.loadAllPartitions(hdfsTable);
        for (FeFsPartition part: parts) {
          FileSystemUtil.deleteAllVisibleFiles(new Path(part.getLocation()));
        }

        dropColumnStats(table);
        dropTableStats(table);
      } catch (Exception e) {
        String fqName = tblName.db_name + "." + tblName.table_name;
        throw new CatalogException(String.format("Failed to truncate table: %s.\n" +
            "Table may be in a partially truncated state.", fqName), e);
      }
      addSummary(resp, "Table has been truncated.");

      loadTableMetadata(table, newCatalogVersion, true, true, null);
      addTableToCatalogUpdate(table, resp.result);
    } finally {
      Preconditions.checkState(!catalog_.getLock().isWriteLockedByCurrentThread());
      table.getLock().unlock();
    }
  }

  private void dropFunction(TDropFunctionParams params, TDdlExecResponse resp)
      throws ImpalaException {
    FunctionName fName = FunctionName.fromThrift(params.fn_name);
    synchronized (metastoreDdlLock_) {
      Db db = catalog_.getDb(fName.getDb());
      if (db == null) {
        if (!params.if_exists) {
            throw new CatalogException("Database: " + fName.getDb()
                + " does not exist.");
        }
        addSummary(resp, "Database does not exist.");
        return;
      }
      List<TCatalogObject> removedFunctions = Lists.newArrayList();
      if (!params.isSetSignature()) {
        dropJavaFunctionFromHms(fName.getDb(), fName.getFunction(), params.if_exists);
        for (Function fn: db.getFunctions(fName.getFunction())) {
          if (fn.getBinaryType() != TFunctionBinaryType.JAVA
              || !fn.isPersistent()) {
            continue;
          }
          Preconditions.checkNotNull(catalog_.removeFunction(fn));
          removedFunctions.add(fn.toTCatalogObject());
        }
      } else {
        ArrayList<Type> argTypes = Lists.newArrayList();
        for (TColumnType t: params.arg_types) {
          argTypes.add(Type.fromThrift(t));
        }
        Function desc = new Function(fName, argTypes, Type.INVALID, false);
        Function fn = catalog_.removeFunction(desc);
        if (fn == null) {
          if (!params.if_exists) {
            throw new CatalogException(
                "Function: " + desc.signatureString() + " does not exist.");
          }
        } else {
          // Flush DB changes to metastore
          applyAlterDatabase(db.getMetaStoreDb());
          removedFunctions.add(fn.toTCatalogObject());
        }
      }

      if (!removedFunctions.isEmpty()) {
        addSummary(resp, "Function has been dropped.");
        resp.result.setRemoved_catalog_objects(removedFunctions);
      } else {
        addSummary(resp, "Function does not exist.");
      }
      resp.result.setVersion(catalog_.getCatalogVersion());
    }
  }

  /**
   * Creates a new table in the metastore and adds an entry to the metadata cache to
   * lazily load the new metadata on the next access. If this is a managed Kudu table,
   * the table is also created in the Kudu storage engine. Re-throws any HMS or Kudu
   * exceptions encountered during the create.
   * @param  syncDdl tells if SYNC_DDL option is enabled on this DDL request.
   * @return true if a new table has been created with the given params, false
   * otherwise.
   */
  private boolean createTable(TCreateTableParams params, TDdlExecResponse response,
      boolean syncDdl) throws ImpalaException {
    Preconditions.checkNotNull(params);
    TableName tableName = TableName.fromThrift(params.getTable_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null,
        "Null column list given as argument to Catalog.createTable");

    Table existingTbl = catalog_.getTableNoThrow(tableName.getDb(), tableName.getTbl());
    if (params.if_not_exists && existingTbl != null) {
      addSummary(response, "Table already exists.");
      LOG.trace("Skipping table creation because {} already exists and " +
          "IF NOT EXISTS was specified.", tableName);
      tryLock(existingTbl);
      try {
        if (syncDdl) {
          // When SYNC_DDL is enabled and the table already exists, we force a version
          // bump on it so that it is added to the next statestore update. Without this
          // we could potentially be referring to a table object that has already been
          // GC'ed from the TopicUpdateLog and waitForSyncDdlVersion() cannot find a
          // covering topic version (IMPALA-7961).
          //
          // This is a conservative hack to not break the SYNC_DDL semantics and could
          // possibly result in false-positive invalidates on this table. However, that is
          // better than breaking the SYNC_DDL semantics and the subsequent queries
          // referring to this table failing with "table not found" errors.
          long newVersion = catalog_.incrementAndGetCatalogVersion();
          existingTbl.setCatalogVersion(newVersion);
          LOG.trace("Table {} version bumped to {} because SYNC_DDL is enabled.",
              tableName, newVersion);
        }
        addTableToCatalogUpdate(existingTbl, response.result);
      } finally {
        // Release the locks held in tryLock().
        catalog_.getLock().writeLock().unlock();
        existingTbl.getLock().unlock();
      }
      return false;
    }
    org.apache.hadoop.hive.metastore.api.Table tbl = createMetaStoreTable(params);
    LOG.trace("Creating table {}", tableName);
    if (KuduTable.isKuduTable(tbl)) return createKuduTable(tbl, params, response);
    Preconditions.checkState(params.getColumns().size() > 0,
        "Empty column list given as argument to Catalog.createTable");
    return createTable(tbl, params.if_not_exists, params.getCache_op(),
        params.server_name, response);
  }

  /**
   * Utility function that creates a hive.metastore.api.Table object based on the given
   * TCreateTableParams.
   * TODO: Extract metastore object creation utility functions into a separate
   * helper/factory class.
   */
  public static org.apache.hadoop.hive.metastore.api.Table createMetaStoreTable(
      TCreateTableParams params) {
    Preconditions.checkNotNull(params);
    TableName tableName = TableName.fromThrift(params.getTable_name());
    org.apache.hadoop.hive.metastore.api.Table tbl =
        new org.apache.hadoop.hive.metastore.api.Table();
    tbl.setDbName(tableName.getDb());
    tbl.setTableName(tableName.getTbl());
    tbl.setOwner(params.getOwner());
    if (params.isSetTable_properties()) {
      tbl.setParameters(params.getTable_properties());
    } else {
      tbl.setParameters(new HashMap<String, String>());
    }
    if (params.isSetSort_columns() && !params.sort_columns.isEmpty()) {
      tbl.getParameters().put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS,
          Joiner.on(",").join(params.sort_columns));
    }
    if (params.getComment() != null) {
      tbl.getParameters().put("comment", params.getComment());
    }
    if (params.is_external) {
      tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      tbl.putToParameters("EXTERNAL", "TRUE");
    } else {
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
    }

    tbl.setSd(createSd(params));
    if (params.getPartition_columns() != null) {
      // Add in any partition keys that were specified
      tbl.setPartitionKeys(buildFieldSchemaList(params.getPartition_columns()));
    } else {
      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
    }
    return tbl;
  }

  private static StorageDescriptor createSd(TCreateTableParams params) {
    StorageDescriptor sd = HiveStorageDescriptorFactory.createSd(
        params.getFile_format(), RowFormat.fromThrift(params.getRow_format()));
    if (params.isSetSerde_properties()) {
      if (sd.getSerdeInfo().getParameters() == null) {
        sd.getSerdeInfo().setParameters(params.getSerde_properties());
      } else {
        sd.getSerdeInfo().getParameters().putAll(params.getSerde_properties());
      }
    }

    if (params.getLocation() != null) sd.setLocation(params.getLocation());

    // Add in all the columns
    sd.setCols(buildFieldSchemaList(params.getColumns()));
    return sd;
  }

  /**
   * Creates a new Kudu table. The Kudu table is first created in the Kudu storage engine
   * (only applicable to managed tables), then in HMS and finally in the catalog cache.
   * Failure to add the table in HMS results in the table being dropped from Kudu.
   * 'response' is populated with the results of this operation. Returns true if a new
   * table was created as part of this call, false otherwise.
   */
  private boolean createKuduTable(org.apache.hadoop.hive.metastore.api.Table newTable,
      TCreateTableParams params, TDdlExecResponse response) throws ImpalaException {
    Preconditions.checkState(KuduTable.isKuduTable(newTable));
    if (Table.isExternalTable(newTable)) {
      KuduCatalogOpExecutor.populateColumnsFromKudu(newTable);
    } else {
      KuduCatalogOpExecutor.createManagedTable(newTable, params);
    }
    try {
      // Add the table to the HMS and the catalog cache. Aquire metastoreDdlLock_ to
      // ensure the atomicity of these operations.
      synchronized (metastoreDdlLock_) {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
          msClient.getHiveClient().createTable(newTable);
        }
        // Add the table to the catalog cache
        Table newTbl = catalog_.addTable(newTable.getDbName(), newTable.getTableName());
        addTableToCatalogUpdate(newTbl, response.result);
      }
    } catch (Exception e) {
      try {
        // Error creating the table in HMS, drop the managed table from Kudu.
        if (!Table.isExternalTable(newTable)) {
          KuduCatalogOpExecutor.dropTable(newTable, false);
        }
      } catch (Exception logged) {
        String kuduTableName = newTable.getParameters().get(KuduTable.KEY_TABLE_NAME);
        LOG.error(String.format("Failed to drop Kudu table '%s'", kuduTableName),
            logged);
        throw new RuntimeException(String.format("Failed to create the table '%s' in " +
            " the Metastore and the newly created Kudu table '%s' could not be " +
            " dropped. The log contains more information.", newTable.getTableName(),
            kuduTableName), e);
      }
      if (e instanceof AlreadyExistsException && params.if_not_exists) {
        addSummary(response, "Table already exists.");
        return false;
      }
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "createTable"), e);
    }
    addSummary(response, "Table has been created.");
    return true;
  }

  /**
   * Creates a new table. The table is initially created in HMS and, if that operation
   * succeeds, it is then added in the catalog cache. It also sets HDFS caching if
   * 'cacheOp' is not null. 'response' is populated with the results of this operation.
   * Returns true if a new table was created as part of this call, false otherwise.
   */
  private boolean createTable(org.apache.hadoop.hive.metastore.api.Table newTable,
      boolean if_not_exists, THdfsCachingOp cacheOp, String serverName,
      TDdlExecResponse response) throws ImpalaException {
    Preconditions.checkState(!KuduTable.isKuduTable(newTable));
    synchronized (metastoreDdlLock_) {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        msClient.getHiveClient().createTable(newTable);
        addSummary(response, "Table has been created.");
        // If this table should be cached, and the table location was not specified by
        // the user, an extra step is needed to read the table to find the location.
        if (cacheOp != null && cacheOp.isSet_cached() &&
            newTable.getSd().getLocation() == null) {
          newTable = msClient.getHiveClient().getTable(
              newTable.getDbName(), newTable.getTableName());
        }
      } catch (Exception e) {
        if (e instanceof AlreadyExistsException && if_not_exists) {
          addSummary(response, "Table already exists");
          return false;
        }
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "createTable"), e);
      }
      // Submit the cache request and update the table metadata.
      if (cacheOp != null && cacheOp.isSet_cached()) {
        short replication = cacheOp.isSetReplication() ? cacheOp.getReplication() :
            JniCatalogConstants.HDFS_DEFAULT_CACHE_REPLICATION_FACTOR;
        long id = HdfsCachingUtil.submitCacheTblDirective(newTable,
            cacheOp.getCache_pool_name(), replication);
        catalog_.watchCacheDirs(Lists.<Long>newArrayList(id),
            new TTableName(newTable.getDbName(), newTable.getTableName()));
        applyAlterTable(newTable, true);
      }
      Table newTbl = catalog_.addTable(newTable.getDbName(), newTable.getTableName());
      addTableToCatalogUpdate(newTbl, response.result);
      if (authzConfig_.isEnabled()) {
        authzManager_.updateTableOwnerPrivilege(serverName, newTable.getDbName(),
            newTable.getTableName(), /* oldOwner */ null,
            /* oldOwnerType */ null, newTable.getOwner(), newTable.getOwnerType(),
            response);
      }
    }
    return true;
  }

  /**
   * Creates a new view in the metastore and adds an entry to the metadata cache to
   * lazily load the new metadata on the next access. Re-throws any Metastore
   * exceptions encountered during the create.
   */
  private void createView(TCreateOrAlterViewParams params, TDdlExecResponse response)
      throws ImpalaException {
    TableName tableName = TableName.fromThrift(params.getView_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
        params.getColumns().size() > 0,
          "Null or empty column list given as argument to DdlExecutor.createView");
    if (params.if_not_exists &&
        catalog_.containsTable(tableName.getDb(), tableName.getTbl())) {
      LOG.trace(String.format("Skipping view creation because %s already exists and " +
          "ifNotExists is true.", tableName));
    }

    // Create new view.
    org.apache.hadoop.hive.metastore.api.Table view =
        new org.apache.hadoop.hive.metastore.api.Table();
    setCreateViewAttributes(params, view);
    LOG.trace(String.format("Creating view %s", tableName));
    if (!createTable(view, params.if_not_exists, null, params.server_name, response)) {
      addSummary(response, "View already exists.");
    } else {
      addSummary(response, "View has been created.");
    }
  }

  /**
   * Creates a new table in the metastore based on the definition of an existing table.
   * No data is copied as part of this process, it is a metadata only operation. If the
   * creation succeeds, an entry is added to the metadata cache to lazily load the new
   * table's metadata on the next access.
   * @param  syncDdl tells is SYNC_DDL is enabled for this DDL request.
   */
  private void createTableLike(TCreateTableLikeParams params, TDdlExecResponse response
      , boolean syncDdl)
      throws ImpalaException {
    Preconditions.checkNotNull(params);

    THdfsFileFormat fileFormat =
        params.isSetFile_format() ? params.getFile_format() : null;
    String comment = params.isSetComment() ? params.getComment() : null;
    TableName tblName = TableName.fromThrift(params.getTable_name());
    TableName srcTblName = TableName.fromThrift(params.getSrc_table_name());
    Preconditions.checkState(tblName != null && tblName.isFullyQualified());
    Preconditions.checkState(srcTblName != null && srcTblName.isFullyQualified());

    Table existingTbl = catalog_.getTableNoThrow(tblName.getDb(), tblName.getTbl());
    if (params.if_not_exists && existingTbl != null) {
      addSummary(response, "Table already exists.");
      LOG.trace(String.format("Skipping table creation because %s already exists and " +
          "IF NOT EXISTS was specified.", tblName));
      tryLock(existingTbl);
      try {
        if (syncDdl) {
          // When SYNC_DDL is enabled and the table already exists, we force a version
          // bump on it so that it is added to the next statestore update. Without this
          // we could potentially be referring to a table object that has already been
          // GC'ed from the TopicUpdateLog and waitForSyncDdlVersion() cannot find a
          // covering topic version (IMPALA-7961).
          //
          // This is a conservative hack to not break the SYNC_DDL semantics and could
          // possibly result in false-positive invalidates on this table. However, that is
          // better than breaking the SYNC_DDL semantics and the subsequent queries
          // referring to this table failing with "table not found" errors.
          long newVersion = catalog_.incrementAndGetCatalogVersion();
          existingTbl.setCatalogVersion(newVersion);
          LOG.trace("Table {} version bumped to {} because SYNC_DDL is enabled.",
              existingTbl.getFullName(), newVersion);
        }
        addTableToCatalogUpdate(existingTbl, response.result);
      } finally {
        // Release the locks held in tryLock().
        catalog_.getLock().writeLock().unlock();
        existingTbl.getLock().unlock();
      }
      return;
    }
    Table srcTable = getExistingTable(srcTblName.getDb(), srcTblName.getTbl());
    org.apache.hadoop.hive.metastore.api.Table tbl =
        srcTable.getMetaStoreTable().deepCopy();
    Preconditions.checkState(!KuduTable.isKuduTable(tbl),
        "CREATE TABLE LIKE is not supported for Kudu tables.");
    tbl.setDbName(tblName.getDb());
    tbl.setTableName(tblName.getTbl());
    tbl.setOwner(params.getOwner());
    if (tbl.getParameters() == null) {
      tbl.setParameters(new HashMap<String, String>());
    }
    if (params.isSetSort_columns() && !params.sort_columns.isEmpty()) {
      tbl.getParameters().put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS,
          Joiner.on(",").join(params.sort_columns));
    }
    if (comment != null) {
      tbl.getParameters().put("comment", comment);
    }
    // The EXTERNAL table property should not be copied from the old table.
    if (params.is_external) {
      tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      tbl.putToParameters("EXTERNAL", "TRUE");
    } else {
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
      if (tbl.getParameters().containsKey("EXTERNAL")) {
        tbl.getParameters().remove("EXTERNAL");
      }
    }

    // We should not propagate hdfs caching parameters to the new table.
    if (tbl.getParameters().containsKey(
        HdfsCachingUtil.CACHE_DIR_ID_PROP_NAME)) {
      tbl.getParameters().remove(HdfsCachingUtil.CACHE_DIR_ID_PROP_NAME);
    }
    if (tbl.getParameters().containsKey(
        HdfsCachingUtil.CACHE_DIR_REPLICATION_PROP_NAME)) {
      tbl.getParameters().remove(
        HdfsCachingUtil.CACHE_DIR_REPLICATION_PROP_NAME);
    }

    // The LOCATION property should not be copied from the old table. If the location
    // is null (the caller didn't specify a custom location) this will clear the value
    // and the table will use the default table location from the parent database.
    tbl.getSd().setLocation(params.getLocation());
    if (fileFormat != null) {
      setStorageDescriptorFileFormat(tbl.getSd(), fileFormat);
    } else if (srcTable instanceof View) {
      // Here, source table is a view which has no input format. So to be
      // consistent with CREATE TABLE, default input format is assumed to be
      // TEXT unless otherwise specified.
      setStorageDescriptorFileFormat(tbl.getSd(), THdfsFileFormat.TEXT);
    }
    // Set the row count of this table to unknown.
    tbl.putToParameters(StatsSetupConst.ROW_COUNT, "-1");
    LOG.trace(String.format("Creating table %s LIKE %s", tblName, srcTblName));
    createTable(tbl, params.if_not_exists, null, params.server_name, response);
  }

  /**
   * Sets the given params in the metastore table as appropriate for a
   * create view operation.
   */
  private void setCreateViewAttributes(TCreateOrAlterViewParams params,
      org.apache.hadoop.hive.metastore.api.Table view) {
    view.setTableType(TableType.VIRTUAL_VIEW.toString());
    view.setViewOriginalText(params.getOriginal_view_def());
    view.setViewExpandedText(params.getExpanded_view_def());
    view.setDbName(params.getView_name().getDb_name());
    view.setTableName(params.getView_name().getTable_name());
    view.setOwner(params.getOwner());
    if (view.getParameters() == null) view.setParameters(new HashMap<String, String>());
    if (params.isSetComment() && params.getComment() != null) {
      view.getParameters().put("comment", params.getComment());
    }
    StorageDescriptor sd = new StorageDescriptor();
    // Add all the columns to a new storage descriptor.
    sd.setCols(buildFieldSchemaList(params.getColumns()));
    // Set a dummy SerdeInfo for Hive.
    sd.setSerdeInfo(new SerDeInfo());
    view.setSd(sd);
  }

  /**
   * Sets the given params in the metastore table as appropriate for an
   * alter view operation.
   */
  private void setAlterViewAttributes(TCreateOrAlterViewParams params,
      org.apache.hadoop.hive.metastore.api.Table view) {
    view.setViewOriginalText(params.getOriginal_view_def());
    view.setViewExpandedText(params.getExpanded_view_def());
    if (params.isSetComment() && params.getComment() != null) {
      view.getParameters().put("comment", params.getComment());
    }
    // Add all the columns to a new storage descriptor.
    view.getSd().setCols(buildFieldSchemaList(params.getColumns()));
  }

  /**
   * Appends one or more columns to the given table. Returns true if there a column was
   * added; false otherwise.
   */
  private boolean alterTableAddCols(Table tbl, List<TColumn> columns, boolean ifNotExists)
      throws ImpalaException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    List<TColumn> colsToAdd = new ArrayList<>();
    for (TColumn column: columns) {
      Column col = tbl.getColumn(column.getColumnName());
      if (ifNotExists && col != null) continue;
      if (col != null) {
        throw new CatalogException(
            String.format("Column '%s' in table '%s' already exists.",
            col.getName(), tbl.getName()));
      }
      colsToAdd.add(column);
    }
    // Only add columns that do not exist.
    if (!colsToAdd.isEmpty()) {
      // Append the new column to the existing list of columns.
      msTbl.getSd().getCols().addAll(buildFieldSchemaList(colsToAdd));
      applyAlterTable(msTbl, true);
      return true;
    }
    return false;
  }

  /**
   * Replaces all existing columns to the given table.
   */
  private void alterTableReplaceCols(Table tbl, List<TColumn> columns)
      throws ImpalaException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    List<FieldSchema> newColumns = buildFieldSchemaList(columns);
    msTbl.getSd().setCols(newColumns);
    String sortByKey = AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS;
    if (msTbl.getParameters().containsKey(sortByKey)) {
      String oldColumns = msTbl.getParameters().get(sortByKey);
      String alteredColumns = MetaStoreUtil.intersectCsvListWithColumNames(oldColumns,
          columns);
      msTbl.getParameters().put(sortByKey, alteredColumns);
    }
    applyAlterTable(msTbl, true);
  }

  /**
   * Changes the column definition of an existing column. This can be used to rename a
   * column, add a comment to a column, or change the datatype of a column.
   */
  private void alterTableAlterCol(Table tbl, String colName,
      TColumn newCol) throws ImpalaException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    // Find the matching column name and change it.
    Iterator<FieldSchema> iterator = msTbl.getSd().getColsIterator();
    while (iterator.hasNext()) {
      FieldSchema fs = iterator.next();
      if (fs.getName().toLowerCase().equals(colName.toLowerCase())) {
        fs.setName(newCol.getColumnName());
        Type type = Type.fromThrift(newCol.getColumnType());
        fs.setType(type.toSql().toLowerCase());
        // Don't overwrite the existing comment unless a new comment is given
        if (newCol.getComment() != null) {
          fs.setComment(newCol.getComment());
        }
        String sortByKey = AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS;
        if (msTbl.getParameters().containsKey(sortByKey)) {
          String oldColumns = msTbl.getParameters().get(sortByKey);
          String alteredColumns = MetaStoreUtil.replaceValueInCsvList(oldColumns, colName,
              newCol.getColumnName());
          msTbl.getParameters().put(sortByKey, alteredColumns);
        }
        break;
      }
      if (!iterator.hasNext()) {
        throw new ColumnNotFoundException(String.format(
            "Column name %s not found in table %s.", colName, tbl.getFullName()));
      }
    }
    applyAlterTable(msTbl, true);
  }

  /**
   * Adds new partitions to the given table in HMS. Also creates and adds new
   * HdfsPartitions to the corresponding HdfsTable. Returns the table object with an
   * updated catalog version or null if the table is not altered because all the
   * partitions already exist and IF NOT EXISTS is specified.
   * If IF NOT EXISTS is not used and there is a conflict with the partitions that already
   * exist in HMS or catalog cache, then:
   * - HMS and catalog cache are left intact, and
   * - ImpalaRuntimeException is thrown.
   * If IF NOT EXISTS is used, conflicts are handled as follows:
   * 1. If a partition exists in catalog cache, ignore it.
   * 2. If a partition exists in HMS but not in catalog cache, reload partition from HMS.
   * Caching directives are only applied to new partitions that were absent from both the
   * catalog cache and the HMS.
   */
  private Table alterTableAddPartitions(Table tbl,
      TAlterTableAddPartitionParams addPartParams) throws ImpalaException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());

    TableName tableName = tbl.getTableName();
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    boolean ifNotExists = addPartParams.isIf_not_exists();
    List<Partition> allHmsPartitionsToAdd = Lists.newArrayList();
    Map<List<String>, THdfsCachingOp> partitionCachingOpMap = Maps.newHashMap();
    for (TPartitionDef partParams: addPartParams.getPartitions()) {
      List<TPartitionKeyValue> partitionSpec = partParams.getPartition_spec();
      if (catalog_.containsHdfsPartition(tableName.getDb(), tableName.getTbl(),
          partitionSpec)) {
        String partitionSpecStr = Joiner.on(", ").join(partitionSpec);
        if (!ifNotExists) {
          throw new ImpalaRuntimeException(String.format("Partition already " +
              "exists: (%s)", partitionSpecStr));
        }
        LOG.trace(String.format("Skipping partition creation because (%s) already " +
            "exists and IF NOT EXISTS was specified.", partitionSpecStr));
        continue;
      }

      Partition hmsPartition =
          createHmsPartition(partitionSpec, msTbl, tableName, partParams.getLocation());
      allHmsPartitionsToAdd.add(hmsPartition);

      THdfsCachingOp cacheOp = partParams.getCache_op();
      if (cacheOp != null) partitionCachingOpMap.put(hmsPartition.getValues(), cacheOp);
    }

    if (allHmsPartitionsToAdd.isEmpty()) return null;

    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      List<Partition> addedHmsPartitions = Lists.newArrayList();

      for (List<Partition> hmsSublist :
          Lists.partition(allHmsPartitionsToAdd, MAX_PARTITION_UPDATES_PER_RPC)) {
        try {
          addedHmsPartitions.addAll(msClient.getHiveClient().add_partitions(hmsSublist,
              ifNotExists, true));
        } catch (TException e) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "add_partitions"), e);
        }
      }
      // Handle HDFS cache. This is done in a separate round bacause we have to apply
      // caching only to newly added partitions.
      alterTableCachePartitions(msTbl, msClient, tableName, addedHmsPartitions,
          partitionCachingOpMap);

      // If 'ifNotExists' is true, add_partitions() may fail to add all the partitions to
      // HMS because some of them may already exist there. In that case, we load in the
      // catalog the partitions that already exist in HMS but aren't in the catalog yet.
      if (allHmsPartitionsToAdd.size() != addedHmsPartitions.size()) {
        List<Partition> difference = computeDifference(allHmsPartitionsToAdd,
            addedHmsPartitions);
        addedHmsPartitions.addAll(
            getPartitionsFromHms(msTbl, msClient, tableName, difference));
      }
      addHdfsPartitions(tbl, addedHmsPartitions);
    }
    return tbl;
  }

  /**
   * Returns the list of Partition objects from 'aList' that cannot be found in 'bList'.
   * Partition objects are distinguished by partition values only.
   */
  private List<Partition> computeDifference(List<Partition> aList,
      List<Partition> bList) {
    Set<List<String>> bSet = Sets.newHashSet();
    for (Partition b: bList) bSet.add(b.getValues());

    List<Partition> diffList = Lists.newArrayList();
    for (Partition a: aList) {
      if (!bSet.contains(a.getValues())) diffList.add(a);
    }
    return diffList;
  }

  /**
   * Returns a list of partitions retrieved from HMS for each 'hmsPartitions' element.
   */
  private List<Partition> getPartitionsFromHms(
      org.apache.hadoop.hive.metastore.api.Table msTbl, MetaStoreClient msClient,
      TableName tableName, List<Partition> hmsPartitions)
      throws ImpalaException {
    List<String> partitionCols = Lists.newArrayList();
    for (FieldSchema fs: msTbl.getPartitionKeys()) partitionCols.add(fs.getName());

    List<String> partitionNames = Lists.newArrayListWithCapacity(hmsPartitions.size());
    for (Partition part: hmsPartitions) {
      String partName = org.apache.hadoop.hive.common.FileUtils.makePartName(
          partitionCols, part.getValues());
      partitionNames.add(partName);
    }
    try {
      return msClient.getHiveClient().getPartitionsByNames(tableName.getDb(),
          tableName.getTbl(), partitionNames);
    } catch (TException e) {
      throw new ImpalaRuntimeException("Metadata inconsistency has occured. Please run "
          + "'invalidate metadata <tablename>' to resolve the problem.", e);
    }
  }

  /**
   * Applies HDFS caching ops on 'hmsPartitions' and updates their metadata in Hive
   * Metastore.
   * 'partitionCachingOpMap' maps partitions (identified by their partition values) to
   * their corresponding HDFS caching ops.
   */
  private void alterTableCachePartitions(org.apache.hadoop.hive.metastore.api.Table msTbl,
      MetaStoreClient msClient, TableName tableName, List<Partition> hmsPartitions,
      Map<List<String>, THdfsCachingOp> partitionCachingOpMap)
      throws ImpalaException {
    // Handle HDFS cache
    List<Long> cacheIds = Lists.newArrayList();
    List<Partition> hmsPartitionsToCache = Lists.newArrayList();
    Long parentTblCacheDirId = HdfsCachingUtil.getCacheDirectiveId(msTbl.getParameters());
    for (Partition partition: hmsPartitions) {
      THdfsCachingOp cacheOp = partitionCachingOpMap.get(partition.getValues());
      String cachePoolName = null;
      Short replication = null;
      if (cacheOp == null && parentTblCacheDirId != null) {
        // The user didn't specify an explicit caching operation, inherit the value
        // from the parent table.
        cachePoolName = HdfsCachingUtil.getCachePool(parentTblCacheDirId);
        Preconditions.checkNotNull(cachePoolName);
        replication = HdfsCachingUtil.getCacheReplication(parentTblCacheDirId);
        Preconditions.checkNotNull(replication);
      } else if (cacheOp != null && cacheOp.isSet_cached()) {
        // The user explicitly stated that this partition should be cached.
        cachePoolName = cacheOp.getCache_pool_name();

        // When the new partition should be cached and and no replication factor
        // was specified, inherit the replication factor from the parent table if
        // it is cached. If the parent is not cached and no replication factor is
        // explicitly set, use the default value.
        if (!cacheOp.isSetReplication() && parentTblCacheDirId != null) {
          replication = HdfsCachingUtil.getCacheReplication(parentTblCacheDirId);
        } else {
          replication = HdfsCachingUtil.getReplicationOrDefault(cacheOp);
        }
      }
      // If cache pool name is not null, it indicates this partition should be cached.
      if (cachePoolName != null) {
        long id = HdfsCachingUtil.submitCachePartitionDirective(partition,
            cachePoolName, replication);
        cacheIds.add(id);
        hmsPartitionsToCache.add(partition);
      }
    }

    // Update the partition metadata to include the cache directive id.
    if (!cacheIds.isEmpty()) {
      applyAlterHmsPartitions(msTbl, msClient, tableName, hmsPartitionsToCache);
      catalog_.watchCacheDirs(cacheIds, tableName.toThrift());
    }
  }

  /**
   * Drops existing partitions from the given table in Hive. If a partition is cached,
   * the associated cache directive will also be removed.
   * Also drops the corresponding partitions from its Hdfs table.
   * Returns the table object with an updated catalog version. If none of the partitions
   * exists and "IfExists" is true, null is returned. If purge is true, partition data is
   * permanently deleted. numUpdatedPartitions is used to inform the client how many
   * partitions being dropped in this operation.
   */
  private Table alterTableDropPartition(Table tbl,
      List<List<TPartitionKeyValue>> partitionSet,
      boolean ifExists, boolean purge, Reference<Long> numUpdatedPartitions)
      throws ImpalaException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());
    Preconditions.checkNotNull(partitionSet);

    TableName tableName = tbl.getTableName();
    if (!ifExists) {
      Preconditions.checkState(!partitionSet.isEmpty());
    } else {
      if (partitionSet.isEmpty()) {
        LOG.trace(String.format("Ignoring empty partition list when dropping " +
            "partitions from %s because ifExists is true.", tableName));
        return tbl;
      }
    }

    Preconditions.checkArgument(tbl instanceof HdfsTable);
    List<HdfsPartition> parts =
        ((HdfsTable) tbl).getPartitionsFromPartitionSet(partitionSet);

    if (!ifExists && parts.isEmpty()) {
      throw new PartitionNotFoundException(
          "The partitions being dropped don't exist any more");
    }

    PartitionDropOptions dropOptions = PartitionDropOptions.instance();
    dropOptions.purgeData(purge);
    long numTargetedPartitions = 0L;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      for (HdfsPartition part : parts) {
        try {
          msClient.getHiveClient().dropPartition(tableName.getDb(), tableName.getTbl(),
              part.getPartitionValuesAsStrings(true), dropOptions);
          ++numTargetedPartitions;
        } catch (NoSuchObjectException e) {
          if (!ifExists) {
            throw new ImpalaRuntimeException(
                String.format(HMS_RPC_ERROR_FORMAT_STR, "dropPartition"), e);
          }
          LOG.trace(
              String.format("Ignoring '%s' when dropping partitions from %s because" +
              " ifExists is true.", e, tableName));
        }
      }
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "dropPartition"), e);
    }
    numUpdatedPartitions.setRef(numTargetedPartitions);
    return catalog_.dropPartitions(tbl, partitionSet);
  }

  /**
   * Removes a column from the given table.
   */
  private void alterTableDropCol(Table tbl, String colName) throws ImpalaException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    // Find the matching column name and remove it.
    Iterator<FieldSchema> iterator = msTbl.getSd().getColsIterator();
    while (iterator.hasNext()) {
      FieldSchema fs = iterator.next();
      if (fs.getName().toLowerCase().equals(colName.toLowerCase())) {
        iterator.remove();
        break;
      }
      if (!iterator.hasNext()) {
        throw new ColumnNotFoundException(String.format(
            "Column name %s not found in table %s.", colName, tbl.getFullName()));
      }
    }
    String sortByKey = AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS;
    if (msTbl.getParameters().containsKey(sortByKey)) {
      String oldColumns = msTbl.getParameters().get(sortByKey);
      String alteredColumns = MetaStoreUtil.removeValueFromCsvList(oldColumns, colName);
      msTbl.getParameters().put(sortByKey, alteredColumns);
    }
    applyAlterTable(msTbl, true);
  }

  /**
   * Renames an existing table or view.
   * After renaming the table/view, its metadata is marked as invalid and will be
   * reloaded on the next access.
   */
  private void alterTableOrViewRename(Table oldTbl, TableName newTableName,
      TDdlExecResponse response) throws ImpalaException {
    Preconditions.checkState(oldTbl.getLock().isHeldByCurrentThread()
        && catalog_.getLock().isWriteLockedByCurrentThread());
    TableName tableName = oldTbl.getTableName();
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        oldTbl.getMetaStoreTable().deepCopy();
    msTbl.setDbName(newTableName.getDb());
    msTbl.setTableName(newTableName.getTbl());

    // If oldTbl is a managed Kudu table, rename the underlying Kudu table
    if (oldTbl instanceof KuduTable && !Table.isExternalTable(msTbl)) {
      Preconditions.checkState(KuduTable.isKuduTable(msTbl));
      renameKuduTable((KuduTable) oldTbl, msTbl, newTableName);
    }

    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().alter_table(tableName.getDb(), tableName.getTbl(), msTbl);
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_table"), e);
    }
    // Rename the table in the Catalog and get the resulting catalog object.
    // ALTER TABLE/VIEW RENAME is implemented as an ADD + DROP.
    Pair<Table, Table> result =
        catalog_.renameTable(tableName.toThrift(), newTableName.toThrift());
    if (result.first == null || result.second == null) {
      // The rename succeeded in the HMS but failed in the catalog cache. The cache is in
      // an inconsistent state, but can likely be fixed by running "invalidate metadata".
      throw new ImpalaRuntimeException(String.format(
          "Table/view rename succeeded in the Hive Metastore, but failed in Impala's " +
          "Catalog Server. Running 'invalidate metadata <tbl>' on the old table name " +
          "'%s' and the new table name '%s' may fix the problem." , tableName.toString(),
          newTableName.toString()));
    }

    // TODO(todd): if client is a 'v2' impalad, only send back invalidation
    response.result.addToRemoved_catalog_objects(result.first.toMinimalTCatalogObject());
    response.result.addToUpdated_catalog_objects(result.second.toTCatalogObject());
    response.result.setVersion(result.second.getCatalogVersion());
    addSummary(response, "Renaming was successful.");
  }

  /**
   * Renames the underlying Kudu table for the given Impala table. If the new Kudu
   * table name is the same as the old Kudu table name, this method does nothing.
   */
  private void renameKuduTable(KuduTable oldTbl,
      org.apache.hadoop.hive.metastore.api.Table oldMsTbl, TableName newTableName)
      throws ImpalaRuntimeException {
    String newKuduTableName = KuduUtil.getDefaultCreateKuduTableName(
        newTableName.getDb(), newTableName.getTbl());

    // If the name of the Kudu table has not changed, do nothing
    if (oldTbl.getKuduTableName().equals(newKuduTableName)) return;

    KuduCatalogOpExecutor.renameTable(oldTbl, newKuduTableName);

    // Add the name of the new Kudu table to the HMS table parameters
    oldMsTbl.getParameters().put(KuduTable.KEY_TABLE_NAME, newKuduTableName);
  }

  /**
   * Changes the file format for the given table or partitions. This is a metadata only
   * operation, existing table data will not be converted to the new format. Returns
   * true if the file metadata to be reloaded.
   */
  private boolean alterTableSetFileFormat(Table tbl,
      List<List<TPartitionKeyValue>> partitionSet, THdfsFileFormat fileFormat,
      Reference<Long> numUpdatedPartitions)
      throws ImpalaException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());
    boolean reloadFileMetadata = false;
    if (partitionSet == null) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      setStorageDescriptorFileFormat(msTbl.getSd(), fileFormat);
      // The prototype partition must be updated if the file format is changed so that new
      // partitions are created with the new file format.
      if (tbl instanceof HdfsTable) ((HdfsTable) tbl).setPrototypePartition(msTbl.getSd());
      applyAlterTable(msTbl, true);
      reloadFileMetadata = true;
    } else {
      Preconditions.checkArgument(tbl instanceof HdfsTable);
      List<HdfsPartition> partitions =
          ((HdfsTable) tbl).getPartitionsFromPartitionSet(partitionSet);
      List<HdfsPartition> modifiedParts = Lists.newArrayList();
      for(HdfsPartition partition: partitions) {
        partition.setFileFormat(HdfsFileFormat.fromThrift(fileFormat));
        modifiedParts.add(partition);
      }
      bulkAlterPartitions(tbl, modifiedParts);
      numUpdatedPartitions.setRef((long) modifiedParts.size());
    }
    return reloadFileMetadata;
  }

  /**
   * Changes the row format for the given table or partitions. This is a metadata only
   * operation, existing table data will not be converted to the new format. Returns
   * true if the file metadata to be reloaded.
   */
  private boolean alterTableSetRowFormat(Table tbl,
      List<List<TPartitionKeyValue>> partitionSet, TTableRowFormat tRowFormat,
      Reference<Long> numUpdatedPartitions)
      throws ImpalaException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());
    Preconditions.checkArgument(tbl instanceof HdfsTable);
    boolean reloadFileMetadata = false;
    RowFormat rowFormat = RowFormat.fromThrift(tRowFormat);
    if (partitionSet == null) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      StorageDescriptor sd = msTbl.getSd();
      HiveStorageDescriptorFactory.setSerdeInfo(rowFormat, sd.getSerdeInfo());
      // The prototype partition must be updated if the row format is changed so that new
      // partitions are created with the new file format.
      ((HdfsTable) tbl).setPrototypePartition(msTbl.getSd());
      applyAlterTable(msTbl, true);
      reloadFileMetadata = true;
    } else {
      List<HdfsPartition> partitions =
          ((HdfsTable) tbl).getPartitionsFromPartitionSet(partitionSet);
      List<HdfsPartition> modifiedParts = Lists.newArrayList();
      for(HdfsPartition partition: partitions) {
        HiveStorageDescriptorFactory.setSerdeInfo(rowFormat, partition.getSerdeInfo());
        modifiedParts.add(partition);
      }
      bulkAlterPartitions(tbl, modifiedParts);
      numUpdatedPartitions.setRef((long) modifiedParts.size());
    }
    return reloadFileMetadata;
  }

  /**
   * Helper method for setting the file format on a given storage descriptor.
   */
  private static void setStorageDescriptorFileFormat(StorageDescriptor sd,
      THdfsFileFormat fileFormat) {
    StorageDescriptor tempSd =
        HiveStorageDescriptorFactory.createSd(fileFormat, RowFormat.DEFAULT_ROW_FORMAT);
    sd.setInputFormat(tempSd.getInputFormat());
    sd.setOutputFormat(tempSd.getOutputFormat());
    sd.getSerdeInfo().setSerializationLib(tempSd.getSerdeInfo().getSerializationLib());
  }

  /**
   * Changes the HDFS storage location for the given table. This is a metadata only
   * operation, existing table data will not be as part of changing the location.
   */
  private boolean alterTableSetLocation(Table tbl,
      List<TPartitionKeyValue> partitionSpec, String location) throws ImpalaException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());
    boolean reloadFileMetadata = false;
    if (partitionSpec == null) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      if (msTbl.getPartitionKeysSize() == 0) reloadFileMetadata = true;
      msTbl.getSd().setLocation(location);
      applyAlterTable(msTbl, true);
    } else {
      TableName tableName = tbl.getTableName();
      HdfsPartition partition = catalog_.getHdfsPartition(
          tableName.getDb(), tableName.getTbl(), partitionSpec);
      partition.setLocation(location);
      try {
        applyAlterPartition(tbl, partition);
      } finally {
        partition.markDirty();
      }
    }
    return reloadFileMetadata;
  }

  /**
   * Appends to the table or partitions property metadata for the given table, replacing
   * the values of any keys that already exist.
   */
  private void alterTableSetTblProperties(Table tbl,
      TAlterTableSetTblPropertiesParams params, Reference<Long> numUpdatedPartitions)
      throws ImpalaException {
    Preconditions.checkState(tbl.getLock().isHeldByCurrentThread());
    Map<String, String> properties = params.getProperties();
    Preconditions.checkNotNull(properties);
    if (params.isSetPartition_set()) {
      Preconditions.checkArgument(tbl instanceof HdfsTable);
      List<HdfsPartition> partitions =
          ((HdfsTable) tbl).getPartitionsFromPartitionSet(params.getPartition_set());

      List<HdfsPartition> modifiedParts = Lists.newArrayList();
      for(HdfsPartition partition: partitions) {
        switch (params.getTarget()) {
          case TBL_PROPERTY:
            partition.getParameters().putAll(properties);
            break;
          case SERDE_PROPERTY:
            partition.getSerdeInfo().getParameters().putAll(properties);
            break;
          default:
            throw new UnsupportedOperationException(
                "Unknown target TTablePropertyType: " + params.getTarget());
        }
        modifiedParts.add(partition);
      }
      try {
        bulkAlterPartitions(tbl, modifiedParts);
      } finally {
        for (HdfsPartition modifiedPart : modifiedParts) {
          modifiedPart.markDirty();
        }
      }
      numUpdatedPartitions.setRef((long) modifiedParts.size());
    } else {
      // Alter table params.
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      switch (params.getTarget()) {
        case TBL_PROPERTY:
          if (KuduTable.isKuduTable(msTbl)) {
            // If 'kudu.table_name' is specified and this is a managed table, rename
            // the underlying Kudu table.
            if (properties.containsKey(KuduTable.KEY_TABLE_NAME)
                && !properties.get(KuduTable.KEY_TABLE_NAME).equals(
                    msTbl.getParameters().get(KuduTable.KEY_TABLE_NAME))
                && !Table.isExternalTable(msTbl)) {
              KuduCatalogOpExecutor.renameTable((KuduTable) tbl,
                  properties.get(KuduTable.KEY_TABLE_NAME));
            }
            msTbl.getParameters().putAll(properties);
            // Validate that the new table properties are valid and that
            // the Kudu table is accessible.
            KuduCatalogOpExecutor.validateKuduTblExists(msTbl);
          } else {
            msTbl.getParameters().putAll(properties);
          }
          break;
        case SERDE_PROPERTY:
          msTbl.getSd().getSerdeInfo().getParameters().putAll(properties);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown target TTablePropertyType: " + params.getTarget());
      }
      applyAlterTable(msTbl, true);
    }
  }

  /**
   * Caches or uncaches the HDFS location of the target table and updates the
   * table's metadata in Hive Metastore Store. If this is a partitioned table,
   * all uncached partitions will also be cached. The table/partition metadata
   * will be updated to include the ID of each cache directive that was submitted.
   * If the table is being uncached, any outstanding cache directives will be dropped
   * and the cache directive ID property key will be cleared. For partitioned tables,
   * marks the partitions that are affected as 'dirty'. For unpartitioned tables, it
   * returns true to indicate that the file metadata of the table must be reloaded.
   */
  private boolean alterTableSetCached(Table tbl, TAlterTableSetCachedParams params)
      throws ImpalaException {
    Preconditions.checkArgument(tbl.getLock().isHeldByCurrentThread());
    THdfsCachingOp cacheOp = params.getCache_op();
    Preconditions.checkNotNull(cacheOp);
    // Alter table params.
    if (!(tbl instanceof HdfsTable)) {
      throw new ImpalaRuntimeException("ALTER TABLE SET CACHED/UNCACHED must target " +
          "an HDFS table.");
    }
    boolean loadFileMetadata = false;
    TableName tableName = tbl.getTableName();
    HdfsTable hdfsTable = (HdfsTable) tbl;
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        tbl.getMetaStoreTable().deepCopy();
    Long cacheDirId = HdfsCachingUtil.getCacheDirectiveId(msTbl.getParameters());
    if (cacheOp.isSet_cached()) {
      // List of cache directive IDs that were submitted as part of this
      // ALTER TABLE operation.
      List<Long> cacheDirIds = Lists.newArrayList();
      short cacheReplication = HdfsCachingUtil.getReplicationOrDefault(cacheOp);
      // If the table was not previously cached (cacheDirId == null) we issue a new
      // directive for this table. If the table was already cached, we validate
      // the pool name and update the cache replication factor if necessary
      if (cacheDirId == null) {
        cacheDirIds.add(HdfsCachingUtil.submitCacheTblDirective(msTbl,
            cacheOp.getCache_pool_name(), cacheReplication));
      } else {
        // Check if the cache directive needs to be changed
        if (HdfsCachingUtil.isUpdateOp(cacheOp, msTbl.getParameters())) {
          HdfsCachingUtil.validateCachePool(cacheOp, cacheDirId, tableName);
          cacheDirIds.add(HdfsCachingUtil.modifyCacheDirective(cacheDirId, msTbl,
              cacheOp.getCache_pool_name(), cacheReplication));
        }
      }

      if (tbl.getNumClusteringCols() > 0) {
        // If this is a partitioned table, submit cache directives for all uncached
        // partitions.
        Collection<? extends FeFsPartition> parts =
            FeCatalogUtils.loadAllPartitions(hdfsTable);
        for (FeFsPartition fePartition: parts) {
          // TODO(todd): avoid downcast
          HdfsPartition partition = (HdfsPartition) fePartition;
          // Only issue cache directives if the data is uncached or the cache directive
          // needs to be updated
          if (!partition.isMarkedCached() ||
              HdfsCachingUtil.isUpdateOp(cacheOp, partition.getParameters())) {
            try {
              // If the partition was already cached, update the directive otherwise
              // issue new cache directive
              if (!partition.isMarkedCached()) {
                cacheDirIds.add(HdfsCachingUtil.submitCachePartitionDirective(
                    partition, cacheOp.getCache_pool_name(), cacheReplication));
              } else {
                Long directiveId = HdfsCachingUtil.getCacheDirectiveId(
                    partition.getParameters());
                cacheDirIds.add(HdfsCachingUtil.modifyCacheDirective(directiveId,
                    partition, cacheOp.getCache_pool_name(), cacheReplication));
              }
            } catch (ImpalaRuntimeException e) {
              if (partition.isMarkedCached()) {
                LOG.error("Unable to modify cache partition: " +
                    partition.getPartitionName(), e);
              } else {
                LOG.error("Unable to cache partition: " +
                    partition.getPartitionName(), e);
              }
            }

            // Update the partition metadata.
            try {
              applyAlterPartition(tbl, partition);
            } finally {
              partition.markDirty();
            }
          }
        }
      } else {
        loadFileMetadata = true;
      }

      // Nothing to do.
      if (cacheDirIds.isEmpty()) return loadFileMetadata;

      // Submit a request to watch these cache directives. The TableLoadingMgr will
      // asynchronously refresh the table metadata once the directives complete.
      catalog_.watchCacheDirs(cacheDirIds, tableName.toThrift());
    } else {
      // Uncache the table.
      if (cacheDirId != null) HdfsCachingUtil.removeTblCacheDirective(msTbl);
      // Uncache all table partitions.
      if (tbl.getNumClusteringCols() > 0) {
        Collection<? extends FeFsPartition> parts =
            FeCatalogUtils.loadAllPartitions(hdfsTable);
        for (FeFsPartition fePartition: parts) {
          // TODO(todd): avoid downcast
          HdfsPartition partition = (HdfsPartition) fePartition;
          if (partition.isMarkedCached()) {
            HdfsCachingUtil.removePartitionCacheDirective(partition);
            try {
              applyAlterPartition(tbl, partition);
            } finally {
              partition.markDirty();
            }
          }
        }
      } else {
        loadFileMetadata = true;
      }
    }

    // Update the table metadata.
    applyAlterTable(msTbl, true);
    return loadFileMetadata;
  }

  /**
   * Caches or uncaches the HDFS location of the target partitions and updates the
   * partitions' metadata in Hive Metastore Store. If a partition is being cached, the
   * partition properties will have the ID of the cache directive added. If the partition
   * is being uncached, any outstanding cache directive will be dropped and the cache
   * directive ID property key will be cleared.
   */
  private void alterPartitionSetCached(Table tbl,
      TAlterTableSetCachedParams params, Reference<Long> numUpdatedPartitions)
      throws ImpalaException {
    Preconditions.checkArgument(tbl.getLock().isHeldByCurrentThread());
    THdfsCachingOp cacheOp = params.getCache_op();
    Preconditions.checkNotNull(cacheOp);
    Preconditions.checkNotNull(params.getPartition_set());
    TableName tableName = tbl.getTableName();
    Preconditions.checkArgument(tbl instanceof HdfsTable);
    List<HdfsPartition> partitions =
        ((HdfsTable) tbl).getPartitionsFromPartitionSet(params.getPartition_set());
    List<HdfsPartition> modifiedParts = Lists.newArrayList();
    if (cacheOp.isSet_cached()) {
      for (HdfsPartition partition : partitions) {
        // The directive is null if the partition is not cached
        Long directiveId =
            HdfsCachingUtil.getCacheDirectiveId(partition.getParameters());
        short replication = HdfsCachingUtil.getReplicationOrDefault(cacheOp);
        List<Long> cacheDirs = Lists.newArrayList();
        if (directiveId == null) {
          cacheDirs.add(HdfsCachingUtil.submitCachePartitionDirective(
              partition, cacheOp.getCache_pool_name(), replication));
        } else {
          if (HdfsCachingUtil.isUpdateOp(cacheOp, partition.getParameters())) {
            HdfsCachingUtil.validateCachePool(cacheOp, directiveId, tableName, partition);
            cacheDirs.add(HdfsCachingUtil.modifyCacheDirective(
                directiveId, partition, cacheOp.getCache_pool_name(),
                replication));
          }
        }

        // Once the cache directives are submitted, observe the status of the caching
        // until no more progress is made -- either fully cached or out of cache memory
        if (!cacheDirs.isEmpty()) {
          catalog_.watchCacheDirs(cacheDirs, tableName.toThrift());
        }
        if (!partition.isMarkedCached()) {
          modifiedParts.add(partition);
        }
      }
    } else {
      for (HdfsPartition partition : partitions) {
        if (partition.isMarkedCached()) {
          HdfsCachingUtil.removePartitionCacheDirective(partition);
          modifiedParts.add(partition);
        }
      }
    }
    try {
      bulkAlterPartitions(tbl, modifiedParts);
    } finally {
      for (HdfsPartition modifiedPart : modifiedParts) {
        modifiedPart.markDirty();
      }
    }
    numUpdatedPartitions.setRef((long) modifiedParts.size());
  }

  /**
   * Recover partitions of specified table.
   * Add partitions to metastore which exist in HDFS but not in metastore.
   */
  private void alterTableRecoverPartitions(Table tbl) throws ImpalaException {
    Preconditions.checkArgument(tbl.getLock().isHeldByCurrentThread());
    if (!(tbl instanceof HdfsTable)) {
      throw new CatalogException("Table " + tbl.getFullName() + " is not an HDFS table");
    }
    HdfsTable hdfsTable = (HdfsTable) tbl;
    List<List<String>> partitionsNotInHms = hdfsTable.getPathsWithoutPartitions();
    if (partitionsNotInHms.isEmpty()) return;

    List<Partition> hmsPartitions = Lists.newArrayList();
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        tbl.getMetaStoreTable().deepCopy();
    TableName tableName = tbl.getTableName();
    for (List<String> partitionSpecValues: partitionsNotInHms) {
      hmsPartitions.add(createHmsPartitionFromValues(
          partitionSpecValues, msTbl, tableName, null));
    }

    String cachePoolName = null;
    Short replication = null;
    List<Long> cacheIds = Lists.newArrayList();
    Long parentTblCacheDirId =
        HdfsCachingUtil.getCacheDirectiveId(msTbl.getParameters());
    if (parentTblCacheDirId != null) {
      // Inherit the HDFS cache value from the parent table.
      cachePoolName = HdfsCachingUtil.getCachePool(parentTblCacheDirId);
      Preconditions.checkNotNull(cachePoolName);
      replication = HdfsCachingUtil.getCacheReplication(parentTblCacheDirId);
      Preconditions.checkNotNull(replication);
    }

    // Add partitions to metastore.
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      // Apply the updates in batches of 'MAX_PARTITION_UPDATES_PER_RPC'.
      for (List<Partition> hmsSublist :
          Lists.partition(hmsPartitions, MAX_PARTITION_UPDATES_PER_RPC)) {
        // ifNotExists and needResults are true.
        List<Partition> hmsAddedPartitions =
            msClient.getHiveClient().add_partitions(hmsSublist, true, true);
        addHdfsPartitions(tbl, hmsAddedPartitions);
        // Handle HDFS cache.
        if (cachePoolName != null) {
          for (Partition partition: hmsAddedPartitions) {
            long id = HdfsCachingUtil.submitCachePartitionDirective(partition,
                cachePoolName, replication);
            cacheIds.add(id);
          }
          // Update the partition metadata to include the cache directive id.
          MetastoreShim.alterPartitions(msClient.getHiveClient(), tableName.getDb(),
              tableName.getTbl(), hmsAddedPartitions);
        }
      }
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "add_partition"), e);
    }
    if (!cacheIds.isEmpty()) {
      catalog_.watchCacheDirs(cacheIds, tableName.toThrift());
    }
  }

  private void alterTableOrViewSetOwner(Table tbl, TAlterTableOrViewSetOwnerParams params,
      TDdlExecResponse response) throws ImpalaException {
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    String oldOwner = msTbl.getOwner();
    PrincipalType oldOwnerType = msTbl.getOwnerType();
    msTbl.setOwner(params.owner_name);
    msTbl.setOwnerType(PrincipalType.valueOf(params.owner_type.name()));
    applyAlterTable(msTbl, true);
    if (authzConfig_.isEnabled()) {
      authzManager_.updateTableOwnerPrivilege(params.server_name, msTbl.getDbName(),
          msTbl.getTableName(), oldOwner, oldOwnerType, msTbl.getOwner(),
          msTbl.getOwnerType(), response);
    }
  }

  /**
   * Create a new HMS Partition.
   */
  private Partition createHmsPartition(List<TPartitionKeyValue> partitionSpec,
      org.apache.hadoop.hive.metastore.api.Table msTbl, TableName tableName,
      String location) {
    List<String> values = Lists.newArrayList();
    // Need to add in the values in the same order they are defined in the table.
    for (FieldSchema fs: msTbl.getPartitionKeys()) {
      for (TPartitionKeyValue kv: partitionSpec) {
        if (fs.getName().toLowerCase().equals(kv.getName().toLowerCase())) {
          values.add(kv.getValue());
        }
      }
    }
    return createHmsPartitionFromValues(values, msTbl, tableName, location);
  }

  /**
   * Create a new HMS Partition from partition values.
   */
  private Partition createHmsPartitionFromValues(List<String> partitionSpecValues,
      org.apache.hadoop.hive.metastore.api.Table msTbl, TableName tableName,
      String location) {
    // Create HMS Partition.
    org.apache.hadoop.hive.metastore.api.Partition partition =
        new org.apache.hadoop.hive.metastore.api.Partition();
    partition.setDbName(tableName.getDb());
    partition.setTableName(tableName.getTbl());
    partition.setValues(partitionSpecValues);
    StorageDescriptor sd = msTbl.getSd().deepCopy();
    sd.setLocation(location);
    partition.setSd(sd);
    // if external event processing is enabled, add the catalog service identifiers
    // from table to the partition
    addCatalogServiceIdentifiers(msTbl, partition);
    return partition;
  }

  /**
   * Adds this catalog service id and the given catalog version to the partition
   * parameters from table parameters. No-op if event processing is disabled
   */
  private void addCatalogServiceIdentifiers(
      org.apache.hadoop.hive.metastore.api.Table msTbl, Partition partition) {
    if (!catalog_.isExternalEventProcessingEnabled())
      return;
    Preconditions.checkState(msTbl.isSetParameters());
    Map<String, String> tblParams = msTbl.getParameters();
    Preconditions
        .checkState(tblParams.containsKey(
            MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey()),
            "Table parameters must have catalog service identifier before "
                + "adding it to partition parameters");
    Preconditions
        .checkState(tblParams.containsKey(
            MetastoreEventPropertyKey.CATALOG_VERSION.getKey()),
            "Table parameters must contain catalog version before adding "
                + "it to partition parameters");
    partition.putToParameters(
        MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(),
        tblParams.get(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey()));
    partition.putToParameters(
        MetastoreEventPropertyKey.CATALOG_VERSION.getKey(),
        tblParams.get(MetastoreEventPropertyKey.CATALOG_VERSION.getKey()));
  }

  /**
   * Creates a new function in the Hive metastore. Returns true if successful
   * and false if the call fails and ifNotExists is true.
   */
  public boolean addJavaFunctionToHms(String db,
      org.apache.hadoop.hive.metastore.api.Function fn, boolean ifNotExists)
      throws ImpalaRuntimeException{
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().createFunction(fn);
    } catch(AlreadyExistsException e) {
      if (!ifNotExists) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "createFunction"), e);
      }
      return false;
    } catch (Exception e) {
      LOG.error("Error executing createFunction() metastore call: " +
          fn.getFunctionName(), e);
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "createFunction"), e);
    }
    return true;
  }

  /**
   * Drops the given function from Hive metastore. Returns true if successful
   * and false if the function does not exist and ifExists is true.
   */
  public boolean dropJavaFunctionFromHms(String db, String fn, boolean ifExists)
      throws ImpalaRuntimeException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().dropFunction(db, fn);
    } catch (NoSuchObjectException e) {
      if (!ifExists) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "dropFunction"), e);
      }
      return false;
    } catch (TException e) {
      LOG.error("Error executing dropFunction() metastore call: " + fn, e);
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "dropFunction"), e);
    }
    return true;
  }

  /**
   * Updates the database object in the metastore.
   */
  private void applyAlterDatabase(Database msDb)
      throws ImpalaRuntimeException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().alterDatabase(msDb.getName(), msDb);
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "alterDatabase"), e);
    }
  }

  /**
   * Applies an ALTER TABLE command to the metastore table.
   * Note: The metastore interface is not very safe because it only accepts
   * an entire metastore.api.Table object rather than a delta of what to change. This
   * means an external modification to the table could be overwritten by an ALTER TABLE
   * command if the metadata is not completely in-sync. This affects both Hive and
   * Impala, but is more important in Impala because the metadata is cached for a
   * longer period of time.
   * If 'overwriteLastDdlTime' is true, then table property 'transient_lastDdlTime'
   * is updated to current time so that metastore does not update it in the alter_table
   * call.
   */
  private void applyAlterTable(org.apache.hadoop.hive.metastore.api.Table msTbl,
      boolean overwriteLastDdlTime) throws ImpalaRuntimeException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      if (overwriteLastDdlTime) {
        // It would be enough to remove this table property, as HMS would fill it, but
        // this would make it necessary to reload the table after alter_table in order to
        // remain consistent with HMS.
        Table.updateTimestampProperty(msTbl, Table.TBL_PROP_LAST_DDL_TIME);
      }
      // Avoid computing/setting stats on the HMS side because that may reset the
      // 'numRows' table property (see HIVE-15653). The DO_NOT_UPDATE_STATS flag
      // tells the HMS not to recompute/reset any statistics on its own. Any
      // stats-related alterations passed in the RPC will still be applied.
      msTbl.putToParameters(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
      msClient.getHiveClient().alter_table(
          msTbl.getDbName(), msTbl.getTableName(), msTbl);
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_table"), e);
    }
  }

  private void applyAlterPartition(Table tbl, HdfsPartition partition)
      throws ImpalaException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      Partition hmsPartition = partition.toHmsPartition();
      addCatalogServiceIdentifiers(tbl.getMetaStoreTable(), hmsPartition);
      applyAlterHmsPartitions(tbl.getMetaStoreTable().deepCopy(), msClient,
          tbl.getTableName(), Arrays.asList(hmsPartition));
    }
  }

  private void applyAlterHmsPartitions(org.apache.hadoop.hive.metastore.api.Table msTbl,
      MetaStoreClient msClient, TableName tableName, List<Partition> hmsPartitions)
      throws ImpalaException {
    try {
      MetastoreShim.alterPartitions(
          msClient.getHiveClient(), tableName.getDb(), tableName.getTbl(), hmsPartitions);
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_partitions"), e);
    }
  }

  /**
   * Creates a role on behalf of the requestingUser.
   */
  private void createRole(User requestingUser,
      TCreateDropRoleParams createDropRoleParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    Preconditions.checkNotNull(createDropRoleParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(!createDropRoleParams.isIs_drop());
    authzManager_.createRole(requestingUser, createDropRoleParams, resp);
    addSummary(resp, "Role has been created.");
  }

  /**
   * Drops a role on behalf of the requestingUser.
   */
  private void dropRole(User requestingUser,
      TCreateDropRoleParams createDropRoleParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    Preconditions.checkNotNull(createDropRoleParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(createDropRoleParams.isIs_drop());
    authzManager_.dropRole(requestingUser, createDropRoleParams, resp);
    addSummary(resp, "Role has been dropped.");
  }

  /**
   * Grants a role to the given group on behalf of the requestingUser.
   */
  private void grantRoleToGroup(User requestingUser,
      TGrantRevokeRoleParams grantRevokeRoleParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    Preconditions.checkNotNull(grantRevokeRoleParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(grantRevokeRoleParams.isIs_grant());
    authzManager_.grantRoleToGroup(requestingUser, grantRevokeRoleParams, resp);
    addSummary(resp, "Role has been granted.");
  }

  /**
   * Revokes a role from the given group on behalf of the requestingUser.
   */
  private void revokeRoleFromGroup(User requestingUser,
      TGrantRevokeRoleParams grantRevokeRoleParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    Preconditions.checkNotNull(grantRevokeRoleParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(!grantRevokeRoleParams.isIs_grant());
    authzManager_.revokeRoleFromGroup(requestingUser, grantRevokeRoleParams, resp);
    addSummary(resp, "Role has been revoked.");
  }

  /**
   * Grants one or more privileges to role on behalf of the requestingUser.
   */
  private void grantPrivilege(User requestingUser,
      TGrantRevokePrivParams grantRevokePrivParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    Preconditions.checkNotNull(grantRevokePrivParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(grantRevokePrivParams.isIs_grant());

    switch (grantRevokePrivParams.principal_type) {
      case ROLE:
        authzManager_.grantPrivilegeToRole(requestingUser, grantRevokePrivParams, resp);
        break;
      case USER:
        authzManager_.grantPrivilegeToUser(requestingUser, grantRevokePrivParams, resp);
        break;
      case GROUP:
        authzManager_.grantPrivilegeToGroup(requestingUser, grantRevokePrivParams, resp);
        break;
      default:
        throw new IllegalArgumentException("Unexpected principal type: " +
            grantRevokePrivParams.principal_type);
    }

    addSummary(resp, "Privilege(s) have been granted.");
  }

  /**
   * Revokes one or more privileges to role on behalf of the requestingUser.
   */
  private void revokePrivilege(User requestingUser,
      TGrantRevokePrivParams grantRevokePrivParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    Preconditions.checkNotNull(grantRevokePrivParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(!grantRevokePrivParams.isIs_grant());

    switch (grantRevokePrivParams.principal_type) {
      case ROLE:
        authzManager_.revokePrivilegeFromRole(requestingUser, grantRevokePrivParams,
            resp);
        break;
      case USER:
        authzManager_.revokePrivilegeFromUser(requestingUser, grantRevokePrivParams,
            resp);
        break;
      case GROUP:
        authzManager_.revokePrivilegeFromGroup(requestingUser, grantRevokePrivParams,
            resp);
        break;
      default:
        throw new IllegalArgumentException("Unexpected principal type: " +
            grantRevokePrivParams.principal_type);
    }

    addSummary(resp, "Privilege(s) have been revoked.");
  }

  /**
   * Alters partitions in batches of size 'MAX_PARTITION_UPDATES_PER_RPC'. This
   * reduces the time spent in a single update and helps avoid metastore client
   * timeouts.
   */
  private void bulkAlterPartitions(Table tbl, List<HdfsPartition> modifiedParts)
      throws ImpalaException {
    List<org.apache.hadoop.hive.metastore.api.Partition> hmsPartitions =
        Lists.newArrayList();
    for (HdfsPartition p: modifiedParts) {
      org.apache.hadoop.hive.metastore.api.Partition msPart = p.toHmsPartition();
      if (msPart != null) {
        addCatalogServiceIdentifiers(tbl.getMetaStoreTable(), msPart);
        hmsPartitions.add(msPart);
      }
    }
    if (hmsPartitions.isEmpty()) return;

    String dbName = tbl.getDb().getName();
    String tableName = tbl.getName();
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      // Apply the updates in batches of 'MAX_PARTITION_UPDATES_PER_RPC'.
      for (List<Partition> hmsPartitionsSubList :
        Lists.partition(hmsPartitions, MAX_PARTITION_UPDATES_PER_RPC)) {
        try {
          // Alter partitions in bulk.
          MetastoreShim.alterPartitions(msClient.getHiveClient(), dbName, tableName,
              hmsPartitionsSubList);
          // Mark the corresponding HdfsPartition objects as dirty
          for (org.apache.hadoop.hive.metastore.api.Partition msPartition:
              hmsPartitionsSubList) {
            try {
              catalog_.getHdfsPartition(dbName, tableName, msPartition).markDirty();
            } catch (PartitionNotFoundException e) {
              LOG.error(String.format("Partition of table %s could not be found: %s",
                  tableName, e.getMessage()));
              continue;
            }
          }
        } catch (TException e) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_partitions"), e);
        }
      }
    }
  }

  /**
   * Returns the metastore.api.Table object from the Hive Metastore for an existing
   * fully loaded table.
   */
  private org.apache.hadoop.hive.metastore.api.Table getMetaStoreTable(
      MetaStoreClient msClient, Table tbl) throws CatalogException {
    Preconditions.checkState(!(tbl instanceof IncompleteTable));
    Preconditions.checkNotNull(msClient);
    Db db = tbl.getDb();
    org.apache.hadoop.hive.metastore.api.Table msTbl = null;
    try {
      msTbl = msClient.getHiveClient().getTable(db.getName(), tbl.getName());
    } catch (Exception e) {
      throw new TableLoadingException("Error loading metadata for table: " +
          db.getName() + "." + tbl.getName(), e);
    }
    return msTbl;
  }

  private static List<FieldSchema> buildFieldSchemaList(List<TColumn> columns) {
    List<FieldSchema> fsList = Lists.newArrayList();
    // Add in all the columns
    for (TColumn col: columns) {
      Type type = Type.fromThrift(col.getColumnType());
      // The type string must be lowercase for Hive to read the column metadata properly.
      String typeSql = type.toSql().toLowerCase();
      FieldSchema fs = new FieldSchema(col.getColumnName(), typeSql, col.getComment());
      fsList.add(fs);
    }
    return fsList;
  }

  /**
   * Executes a TResetMetadataRequest and returns the result as a
   * TResetMetadataResponse. Based on the request parameters, this operation
   * may do one of three things:
   * 1) invalidate the entire catalog, forcing the metadata for all catalog
   *    objects to be reloaded.
   * 2) invalidate a specific table, forcing the metadata to be reloaded
   *    on the next access.
   * 3) perform a synchronous incremental refresh of a specific table.
   * 4) perform a refresh on authorization metadata.
   *
   * For details on the specific commands see comments on their respective
   * methods in CatalogServiceCatalog.java.
   */
  public TResetMetadataResponse execResetMetadata(TResetMetadataRequest req)
      throws CatalogException {
    TResetMetadataResponse resp = new TResetMetadataResponse();
    resp.setResult(new TCatalogUpdateResult());
    resp.getResult().setCatalog_service_id(JniCatalog.getServiceId());

    if (req.isSetDb_name()) {
      // This is a "refresh functions" operation.
      synchronized (metastoreDdlLock_) {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
          List<TCatalogObject> addedFuncs = Lists.newArrayList();
          List<TCatalogObject> removedFuncs = Lists.newArrayList();
          catalog_.refreshFunctions(msClient, req.getDb_name(), addedFuncs, removedFuncs);
          resp.result.setUpdated_catalog_objects(addedFuncs);
          resp.result.setRemoved_catalog_objects(removedFuncs);
          resp.result.setVersion(catalog_.getCatalogVersion());
          for (TCatalogObject removedFn: removedFuncs) {
            catalog_.getDeleteLog().addRemovedObject(removedFn);
          }
        }
      }
    } else if (req.isSetTable_name()) {
      // Results of an invalidate operation, indicating whether the table was removed
      // from the Metastore, and whether a new database was added to Impala as a result
      // of the invalidate operation. Always false for refresh.
      Reference<Boolean> tblWasRemoved = new Reference<Boolean>(false);
      Reference<Boolean> dbWasAdded = new Reference<Boolean>(false);
      // Thrift representation of the result of the invalidate/refresh operation.
      TCatalogObject updatedThriftTable = null;
      if (req.isIs_refresh()) {
        TableName tblName = TableName.fromThrift(req.getTable_name());
        // Quick check to see if the table exists in the catalog without triggering
        // a table load.
        Table tbl = catalog_.getTable(tblName.getDb(), tblName.getTbl());
        if (tbl != null) {
          // If the table is not loaded, no need to perform refresh after the initial
          // metadata load.
          boolean needsRefresh = tbl.isLoaded();
          tbl = getExistingTable(tblName.getDb(), tblName.getTbl());
          if (tbl != null) {
            if (needsRefresh) {
              if (req.isSetPartition_spec()) {
                updatedThriftTable = catalog_.reloadPartition(tbl, req.getPartition_spec());
              } else {
                updatedThriftTable = catalog_.reloadTable(tbl);
              }
            } else {
              // Table was loaded from scratch, so it's already "refreshed".
              tbl.getLock().lock();
              try {
                updatedThriftTable = tbl.toTCatalogObject();
              } finally {
                tbl.getLock().unlock();
              }
            }
          }
        }
      } else {
        updatedThriftTable = catalog_.invalidateTable(
            req.getTable_name(), tblWasRemoved, dbWasAdded);
      }

      if (updatedThriftTable == null) {
        // Table does not exist in the Metastore and Impala catalog, throw error.
        throw new TableNotFoundException("Table not found: " +
            req.getTable_name().getDb_name() + "." +
            req.getTable_name().getTable_name());
      }

      // Return the TCatalogObject in the result to indicate this request can be
      // processed as a direct DDL operation.
      if (tblWasRemoved.getRef()) {
        resp.getResult().addToRemoved_catalog_objects(updatedThriftTable);
      } else {
        resp.getResult().addToUpdated_catalog_objects(updatedThriftTable);
      }

      if (dbWasAdded.getRef()) {
        Db addedDb = catalog_.getDb(updatedThriftTable.getTable().getDb_name());
        if (addedDb == null) {
          throw new CatalogException("Database " +
              updatedThriftTable.getTable().getDb_name() + " was removed by a " +
              "concurrent operation. Try invalidating the table again.");
        }
        resp.getResult().addToUpdated_catalog_objects(addedDb.toTCatalogObject());
      }
      resp.getResult().setVersion(updatedThriftTable.getCatalog_version());
    } else if (req.isAuthorization()) {
      AuthorizationDelta authzDelta = catalog_.refreshAuthorization(false);
      resp.result.setUpdated_catalog_objects(authzDelta.getCatalogObjectsAdded());
      resp.result.setRemoved_catalog_objects(authzDelta.getCatalogObjectsRemoved());
      resp.result.setVersion(catalog_.getCatalogVersion());
    } else {
      // Invalidate the entire catalog if no table name is provided.
      Preconditions.checkArgument(!req.isIs_refresh());
      resp.getResult().setVersion(catalog_.reset());
      resp.getResult().setIs_invalidate(true);
    }
    if (req.isSync_ddl()) {
      resp.getResult().setVersion(catalog_.waitForSyncDdlVersion(resp.getResult()));
    }
    resp.getResult().setStatus(new TStatus(TErrorCode.OK, new ArrayList<String>()));
    return resp;
  }

  /**
   * Create any new partitions required as a result of an INSERT statement and refreshes
   * the table metadata after every INSERT statement. Any new partitions will inherit
   * their cache configuration from the parent table. That is, if the parent is cached
   * new partitions created will also be cached and will be put in the same pool as the
   * parent.
   * If the insert touched any pre-existing partitions that were cached, a request to
   * watch the associated cache directives will be submitted. This will result in an
   * async table refresh once the cache request completes.
   */
  public TUpdateCatalogResponse updateCatalog(TUpdateCatalogRequest update)
      throws ImpalaException {
    TUpdateCatalogResponse response = new TUpdateCatalogResponse();
    // Only update metastore for Hdfs tables.
    Table table = getExistingTable(update.getDb_name(), update.getTarget_table());
    if (!(table instanceof HdfsTable)) {
      throw new InternalException("Unexpected table type: " +
          update.getTarget_table());
    }

    if (!catalog_.tryLockTable(table)) {
      throw new InternalException("Error updating the catalog due to lock contention.");
    }
    final Timer.Context context
        = table.getMetrics().getTimer(HdfsTable.CATALOG_UPDATE_DURATION_METRIC).time();
    try {
      // Get new catalog version for table in insert.
      long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      catalog_.getLock().writeLock().unlock();
      // Collects the cache directive IDs of any cached table/partitions that were
      // targeted. A watch on these cache directives is submitted to the
      // TableLoadingMgr and the table will be refreshed asynchronously after all
      // cache directives complete.
      List<Long> cacheDirIds = Lists.<Long>newArrayList();

      // If the table is cached, get its cache pool name and replication factor. New
      // partitions will inherit this property.
      Pair<String, Short> cacheInfo = table.getTableCacheInfo(cacheDirIds);
      String cachePoolName = cacheInfo.first;
      Short cacheReplication = cacheInfo.second;

      TableName tblName = new TableName(table.getDb().getName(), table.getName());
      List<String> errorMessages = Lists.newArrayList();
      HashSet<String> partsToLoadMetadata = null;
      Collection<? extends FeFsPartition> parts =
          FeCatalogUtils.loadAllPartitions((HdfsTable) table);
      List<FeFsPartition> affectedExistingPartitions = new ArrayList<>();
      if (table.getNumClusteringCols() > 0) {
        // Set of all partition names targeted by the insert that need to be created
        // in the Metastore (partitions that do not currently exist in the catalog).
        // In the BE, we don't currently distinguish between which targeted partitions
        // are new and which already exist, so initialize the set with all targeted
        // partition names and remove the ones that are found to exist.
        HashSet<String> partsToCreate =
            Sets.newHashSet(update.getCreated_partitions());
        partsToLoadMetadata = Sets.newHashSet(partsToCreate);
        for (FeFsPartition partition: parts) {
          // TODO: In the BE we build partition names without a trailing char. In FE
          // we build partition name with a trailing char. We should make this
          // consistent.
          String partName = partition.getPartitionName() + "/";

          // Attempt to remove this partition name from from partsToCreate. If remove
          // returns true, it indicates the partition already exists.
          if (partsToCreate.remove(partName)) {
            affectedExistingPartitions.add(partition);
            if (partition.isMarkedCached()) {
              // The partition was targeted by the insert and is also a cached. Since
              // data was written to the partition, a watch needs to be placed on the
              // cache cache directive so the TableLoadingMgr can perform an async
              // refresh once all data becomes cached.
              cacheDirIds.add(HdfsCachingUtil.getCacheDirectiveId(
                  partition.getParameters()));
            }
          }
          if (partsToCreate.size() == 0) break;
        }

        if (!partsToCreate.isEmpty()) {
          try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
            org.apache.hadoop.hive.metastore.api.Table msTbl =
                table.getMetaStoreTable().deepCopy();
            List<org.apache.hadoop.hive.metastore.api.Partition> hmsParts =
                Lists.newArrayList();
            HiveConf hiveConf = new HiveConf(this.getClass());
            Warehouse warehouse = new Warehouse(hiveConf);
            for (String partName: partsToCreate) {
              org.apache.hadoop.hive.metastore.api.Partition partition =
                  new org.apache.hadoop.hive.metastore.api.Partition();
              hmsParts.add(partition);

              partition.setDbName(tblName.getDb());
              partition.setTableName(tblName.getTbl());
              partition.setValues(MetaStoreUtil.getPartValsFromName(msTbl, partName));
              partition.setParameters(new HashMap<String, String>());
              partition.setSd(msTbl.getSd().deepCopy());
              partition.getSd().setSerdeInfo(msTbl.getSd().getSerdeInfo().deepCopy());
              partition.getSd().setLocation(msTbl.getSd().getLocation() + "/" +
                  partName.substring(0, partName.length() - 1));
              MetastoreShim.updatePartitionStatsFast(partition, msTbl, warehouse);
            }

            // First add_partitions and then alter_partitions the successful ones with
            // caching directives. The reason is that some partitions could have been
            // added concurrently, and we want to avoid caching a partition twice and
            // leaking a caching directive.
            List<org.apache.hadoop.hive.metastore.api.Partition> addedHmsParts =
                msClient.getHiveClient().add_partitions(hmsParts, true, true);

            if (addedHmsParts.size() > 0) {
              if (cachePoolName != null) {
                List<org.apache.hadoop.hive.metastore.api.Partition> cachedHmsParts =
                    Lists.newArrayList();
                // Submit a new cache directive and update the partition metadata with
                // the directive id.
                for (org.apache.hadoop.hive.metastore.api.Partition part: addedHmsParts) {
                  try {
                    cacheDirIds.add(HdfsCachingUtil.submitCachePartitionDirective(
                        part, cachePoolName, cacheReplication));
                    cachedHmsParts.add(part);
                  } catch (ImpalaRuntimeException e) {
                    String msg = String.format("Partition %s.%s(%s): State: Not " +
                        "cached. Action: Cache manully via 'ALTER TABLE'.",
                        part.getDbName(), part.getTableName(), part.getValues());
                    LOG.error(msg, e);
                    errorMessages.add(msg);
                  }
                }
                try {
                  MetastoreShim.alterPartitions(msClient.getHiveClient(), tblName.getDb(),
                      tblName.getTbl(), cachedHmsParts);
                } catch (Exception e) {
                  LOG.error("Failed in alter_partitions: ", e);
                  // Try to uncache the partitions when the alteration in the HMS
                  // failed.
                  for (org.apache.hadoop.hive.metastore.api.Partition part:
                      cachedHmsParts) {
                    try {
                      HdfsCachingUtil.removePartitionCacheDirective(part);
                    } catch (ImpalaException e1) {
                      String msg = String.format(
                          "Partition %s.%s(%s): State: Leaked caching directive. " +
                          "Action: Manually uncache directory %s via hdfs " +
                          "cacheAdmin.", part.getDbName(), part.getTableName(),
                          part.getValues(), part.getSd().getLocation());
                      LOG.error(msg, e);
                      errorMessages.add(msg);
                    }
                  }
                }
              }
            }
          } catch (AlreadyExistsException e) {
            throw new InternalException(
                "AlreadyExistsException thrown although ifNotExists given", e);
          } catch (Exception e) {
            throw new InternalException("Error adding partitions", e);
          }
        }
      } else {
        // For non-partitioned table, only single part exists
        FeFsPartition singlePart = Iterables.getOnlyElement((List<FeFsPartition>) parts);
        affectedExistingPartitions.add(singlePart);
      }
      // Submit the watch request for the given cache directives.
      if (!cacheDirIds.isEmpty()) {
        catalog_.watchCacheDirs(cacheDirIds, tblName.toThrift());
      }

      response.setResult(new TCatalogUpdateResult());
      response.getResult().setCatalog_service_id(JniCatalog.getServiceId());
      if (errorMessages.size() > 0) {
        errorMessages.add("Please refer to the catalogd error log for details " +
            "regarding the failed un/caching operations.");
        response.getResult().setStatus(
            new TStatus(TErrorCode.INTERNAL_ERROR, errorMessages));
      } else {
        response.getResult().setStatus(
            new TStatus(TErrorCode.OK, new ArrayList<String>()));
      }

      loadTableMetadata(table, newCatalogVersion, true, false, partsToLoadMetadata);
      // After loading metadata, fire insert events if external event processing is
      // enabled.
      createInsertEvents(table, affectedExistingPartitions, update.is_overwrite);
      addTableToCatalogUpdate(table, response.result);
    } finally {
      context.stop();
      Preconditions.checkState(!catalog_.getLock().isWriteLockedByCurrentThread());
      table.getLock().unlock();
    }

    if (update.isSync_ddl()) {
      response.getResult().setVersion(
          catalog_.waitForSyncDdlVersion(response.getResult()));
    }
    return response;
  }

  /**
   * Populates insert event data and calls fireInsertEvent() if external event processing
   * is enabled. This is no-op if event processing is disabled or there are no existing
   * partitions affected by this insert.
   *
   * @param affectedExistingPartitions List of existing partitions touched by the insert.
   * @param isInsertOverwrite indicates if the operation was an insert overwrite. If it
   *     is not, all the new files added by this insert is calculated.
   */
  private void createInsertEvents(Table table,
      List<FeFsPartition> affectedExistingPartitions, boolean isInsertOverwrite) {
    if (!catalog_.isExternalEventProcessingEnabled() ||
        affectedExistingPartitions.size() == 0) return;
    // Map of partition ids to file names of all existing partitions touched by the
    // insert.
    Map<Long, Set<String>> partitionFilesMap = new HashMap<>();
    if (!isInsertOverwrite) {
      for (FeFsPartition partition : affectedExistingPartitions) {
        partitionFilesMap.put(partition.getId(),
            ((HdfsPartition) partition).getFileNames());
      }
    }
    // If table is partitioned, we add all existing partitions touched by this insert
    // to the insert event. If it is not an insert overwrite operation, we find new
    // files added by this insert.
    Collection<? extends FeFsPartition> partsPostInsert;
    partsPostInsert = table.getNumClusteringCols() > 0 ?
        ((FeFsTable)table).loadPartitions(partitionFilesMap.keySet()) :
            FeCatalogUtils.loadAllPartitions((HdfsTable) table);
    for (FeFsPartition part : partsPostInsert) {
      // Find the delta of the files added by the insert if it is not an overwrite
      // operation. HMS fireListenerEvent() expects an empty list if no new files are
      // added or if the operation is an insert overwrite.
      Set<String> deltaFiles = new HashSet<>();
      List<String> partVals = null;
      if (!isInsertOverwrite) {
        Set<String> filesPostInsert = ((HdfsPartition) part).getFileNames();
        if (table.getNumClusteringCols() > 0) {
          Set<String> filesBeforeInsert = partitionFilesMap.get(part.getId());
          deltaFiles = Sets.difference(filesBeforeInsert, filesPostInsert);
          partVals = part.getPartitionValuesAsStrings(true);
        } else {
          Map.Entry<Long, Set<String>> entry =
              partitionFilesMap.entrySet().iterator().next();
          deltaFiles = Sets.difference(entry.getValue(), filesPostInsert);
        }
        LOG.info("{} new files detected for table {} partition {}.",
            filesPostInsert.size(), table.getTableName(), part.getPartitionName());
      }
      if (deltaFiles != null || isInsertOverwrite) {
        fireInsertEvent(table, partVals, deltaFiles, isInsertOverwrite);
      }
      else {
        LOG.info("No new files were created, and is not a replace. Skipping "
            + "generating INSERT event.");
      }
    }
  }

  /**
   *  Fires an insert event to HMS notification log. For partitioned table, each
   *  existing partition touched by the insert will fire a separate insert event.
   *
   * @param newFiles Set of all the 'new' files added by this insert. This is empty in
   * case of insert overwrite.
   * @param partVals List of partition values corresponding to the partition keys in
   * a partitioned table. This is null for non-partitioned table.
   * @param isOverwrite If true, sets the 'replace' flag to true indicating that the
   * operation was an insert overwrite in the notification log. Will set the same to
   * false otherwise.
   */
  private void fireInsertEvent(Table tbl, List<String> partVals,
      Set<String> newFiles, boolean isOverwrite) {
    LOG.debug("Firing an insert event for {}.", tbl.getName());
    FireEventRequestData data = new FireEventRequestData();
    InsertEventRequestData insertData = new InsertEventRequestData();
    data.setInsertData(insertData);
    FireEventRequest rqst = new FireEventRequest(true, data);
    rqst.setDbName(tbl.getDb().getName());
    rqst.setTableName(tbl.getName());
    insertData.setFilesAdded(new ArrayList<>(newFiles));
    insertData.setReplace(isOverwrite);
    rqst.setPartitionVals(partVals);

    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().fireListenerEvent(rqst);
    } catch (Exception e) {
      LOG.error("Failed to fire insert event. Some tables might not be"
          + " refreshed on other impala clusters.", e);
    }
  }

  /**
   * Returns an existing, loaded table from the Catalog. Throws an exception if any
   * of the following are true:
   * - The table does not exist
   * - There was an error loading the table metadata.
   * - The table is missing (not yet loaded).
   * This is to help protect against certain scenarios where the table was
   * modified or dropped between the time analysis completed and the the catalog op
   * started executing. However, even with these checks it is possible the table was
   * modified or dropped/re-created without us knowing. This function also updates the
   * table usage counter.
   *
   * TODO: Track object IDs to
   * know when a table has been dropped and re-created with the same name.
   */
  private Table getExistingTable(String dbName, String tblName) throws CatalogException {
    Table tbl = catalog_.getOrLoadTable(dbName, tblName);
    if (tbl == null) {
      throw new TableNotFoundException("Table not found: " + dbName + "." + tblName);
    }
    tbl.incrementMetadataOpsCount();

    if (!tbl.isLoaded()) {
      throw new CatalogException(String.format("Table '%s.%s' was modified while " +
          "operation was in progress, aborting execution.", dbName, tblName));
    }

    if (tbl instanceof IncompleteTable && tbl.isLoaded()) {
      // The table loading failed. Throw an exception.
      ImpalaException e = ((IncompleteTable) tbl).getCause();
      if (e instanceof TableLoadingException) {
        throw (TableLoadingException) e;
      }
      throw new TableLoadingException(e.getMessage(), e);
    }
    Preconditions.checkNotNull(tbl);
    Preconditions.checkState(tbl.isLoaded());
    return tbl;
  }

  private void alterCommentOn(TCommentOnParams params, TDdlExecResponse response)
      throws ImpalaRuntimeException, CatalogException, InternalException {
    if (params.getDb() != null) {
      Preconditions.checkArgument(!params.isSetTable_name() &&
          !params.isSetColumn_name());
      alterCommentOnDb(params.getDb(), params.getComment(), response);
    } else if (params.getTable_name() != null) {
      Preconditions.checkArgument(!params.isSetDb() && !params.isSetColumn_name());
      alterCommentOnTableOrView(TableName.fromThrift(params.getTable_name()),
          params.getComment(), response);
    } else if (params.getColumn_name() != null) {
      Preconditions.checkArgument(!params.isSetDb() && !params.isSetTable_name());
      TColumnName columnName = params.getColumn_name();
      alterCommentOnColumn(TableName.fromThrift(columnName.getTable_name()),
          columnName.getColumn_name(), params.getComment(), response);
    } else {
      throw new UnsupportedOperationException("Unsupported COMMENT ON operation");
    }
  }

  private void alterCommentOnDb(String dbName, String comment, TDdlExecResponse response)
      throws ImpalaRuntimeException, CatalogException {
    Db db = catalog_.getDb(dbName);
    if (db == null) {
      throw new CatalogException("Database: " + dbName + " does not exist.");
    }
    synchronized (metastoreDdlLock_) {
      Database msDb = db.getMetaStoreDb().deepCopy();
      msDb.setDescription(comment);
      try {
        applyAlterDatabase(msDb);
      } catch (ImpalaRuntimeException e) {
        throw e;
      }
      Db updatedDb = catalog_.updateDb(msDb);
      addDbToCatalogUpdate(updatedDb, response.result);
    }
    addSummary(response, "Updated database.");
  }

  private void alterDatabase(TAlterDbParams params, TDdlExecResponse response)
      throws ImpalaException {
    Preconditions.checkNotNull(params);
    String dbName = params.getDb();
    Db db = catalog_.getDb(dbName);
    if (db == null) {
      throw new CatalogException("Database: " + dbName + " does not exist.");
    }
    switch (params.getAlter_type()) {
      case SET_OWNER:
        alterDatabaseSetOwner(db, params.getSet_owner_params(), response);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown ALTER DATABASE operation type: " + params.getAlter_type());
    }
  }

  private void alterDatabaseSetOwner(Db db, TAlterDbSetOwnerParams params,
      TDdlExecResponse response) throws ImpalaException {
    Preconditions.checkNotNull(params.owner_name);
    Preconditions.checkNotNull(params.owner_type);
    synchronized (metastoreDdlLock_) {
      // Get a new catalog version to assign to the database being altered.
      long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      addCatalogServiceIdentifiers(db, catalog_.getCatalogServiceId(), newCatalogVersion);
      Database msDb = db.getMetaStoreDb().deepCopy();
      String originalOwnerName = msDb.getOwnerName();
      PrincipalType originalOwnerType = msDb.getOwnerType();
      msDb.setOwnerName(params.owner_name);
      msDb.setOwnerType(PrincipalType.valueOf(params.owner_type.name()));
      try {
        applyAlterDatabase(msDb);
      } catch (ImpalaRuntimeException e) {
        throw e;
      }
      if (authzConfig_.isEnabled()) {
        authzManager_.updateDatabaseOwnerPrivilege(params.server_name, db.getName(),
            originalOwnerName, originalOwnerType, msDb.getOwnerName(),
            msDb.getOwnerType(), response);
      }
      Db updatedDb = catalog_.updateDb(msDb);
      addDbToCatalogUpdate(updatedDb, response.result);
      // now that HMS alter operation has succeeded, add this version to list of inflight
      // events in catalog database if event processing is enabled
      catalog_.addVersionsForInflightEvents(db, newCatalogVersion);
    }
    addSummary(response, "Updated database.");
  }

  /**
   * Adds the catalog service id and the given catalog version to the database parameters.
   * No-op if event processing is disabled
   */
  private void addCatalogServiceIdentifiers(
      Db db, String catalogServiceId, long newCatalogVersion) {
    if (!catalog_.isExternalEventProcessingEnabled()) return;
    org.apache.hadoop.hive.metastore.api.Database msDb = db.getMetaStoreDb();
    msDb.putToParameters(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(),
        catalogServiceId);
    msDb.putToParameters(MetastoreEventPropertyKey.CATALOG_VERSION.getKey(),
        String.valueOf(newCatalogVersion));
  }

  private void addDbToCatalogUpdate(Db db, TCatalogUpdateResult result) {
    Preconditions.checkNotNull(db);
    TCatalogObject updatedCatalogObject = db.toTCatalogObject();
    updatedCatalogObject.setCatalog_version(updatedCatalogObject.getCatalog_version());
    // TODO(todd): if client is a 'v2' impalad, only send back invalidation
    result.addToUpdated_catalog_objects(updatedCatalogObject);
    result.setVersion(updatedCatalogObject.getCatalog_version());
  }

  private void alterCommentOnTableOrView(TableName tableName, String comment,
      TDdlExecResponse response) throws CatalogException, InternalException,
      ImpalaRuntimeException {
    Table tbl = getExistingTable(tableName.getDb(), tableName.getTbl());
    tryLock(tbl);
    try {
      long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      catalog_.getLock().writeLock().unlock();
      addCatalogServiceIdentifiers(tbl, catalog_.getCatalogServiceId(),
          newCatalogVersion);
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      boolean isView = msTbl.getTableType().equalsIgnoreCase(
          TableType.VIRTUAL_VIEW.toString());
      if (comment == null) {
        msTbl.getParameters().remove("comment");
      } else {
        msTbl.getParameters().put("comment", comment);
      }
      applyAlterTable(msTbl, true);
      loadTableMetadata(tbl, newCatalogVersion, false, false, null);
      addTableToCatalogUpdate(tbl, response.result);
      addSummary(response, String.format("Updated %s.", (isView) ? "view" : "table"));
    } finally {
      tbl.getLock().unlock();
    }
  }

  private void alterCommentOnColumn(TableName tableName, String columnName,
      String comment, TDdlExecResponse response) throws CatalogException,
      InternalException, ImpalaRuntimeException {
    Table tbl = getExistingTable(tableName.getDb(), tableName.getTbl());
    tryLock(tbl);
    try {
      long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      catalog_.getLock().writeLock().unlock();
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      if (!updateColumnComment(msTbl.getSd().getColsIterator(), columnName, comment)) {
        if (!updateColumnComment(msTbl.getPartitionKeysIterator(), columnName, comment)) {
          throw new ColumnNotFoundException(String.format(
              "Column name %s not found in table %s.", columnName, tbl.getFullName()));
        }
      }
      applyAlterTable(msTbl, true);
      loadTableMetadata(tbl, newCatalogVersion, false, true, null);
      addTableToCatalogUpdate(tbl, response.result);
      addSummary(response, "Column has been altered.");
    } finally {
      tbl.getLock().unlock();
    }
  }

  /**
   * Find the matching column name in the iterator and update its comment. Return
   * true if found; false otherwise.
   */
  private static boolean updateColumnComment(Iterator<FieldSchema> iterator,
      String columnName, String comment) {
    while (iterator.hasNext()) {
      FieldSchema fs = iterator.next();
      if (fs.getName().equalsIgnoreCase(columnName)) {
        fs.setComment(comment);
        return true;
      }
    }
    return false;
  }

  /**
   * Try to lock a table in the catalog. Throw an InternalException if the catalog is
   * unable to lock the given table.
   */
  private void tryLock(Table tbl) throws InternalException {
    String type = tbl instanceof View ? "view" : "table";
    if (!catalog_.tryLockTable(tbl)) {
      throw new InternalException(String.format("Error altering %s %s due to " +
          "lock contention.", type, tbl.getFullName()));
    }
  }
}
