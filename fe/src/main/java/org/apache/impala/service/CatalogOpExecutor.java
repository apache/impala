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

import static org.apache.impala.analysis.Analyzer.ACCESSTYPE_READWRITE;
import static org.apache.impala.catalog.CatalogServiceCatalog.TABLE_ID_UNAVAILABLE;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.mr.Catalogs;
import org.apache.impala.analysis.AlterTableSortByStmt;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.KuduPartitionParam;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationDelta;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogObject;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnNotFoundException;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FileMetadataLoadOpts;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.IcebergTableLoadingException;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.HiveStorageDescriptorFactory;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.PartitionNotFoundException;
import org.apache.impala.catalog.PartitionStatsUtil;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.Transaction;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.View;
import org.apache.impala.catalog.events.DeleteEventLog;
import org.apache.impala.catalog.events.MetastoreEvents.AddPartitionEvent;
import org.apache.impala.catalog.events.MetastoreEvents.AlterTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.CreateDatabaseEvent;
import org.apache.impala.catalog.events.MetastoreEvents.CreateTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.DropDatabaseEvent;
import org.apache.impala.catalog.events.MetastoreEvents.DropPartitionEvent;
import org.apache.impala.catalog.events.MetastoreEvents.DropTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.catalog.events.MetastoreEventsProcessor;
import org.apache.impala.catalog.events.MetastoreEventsProcessor.EventProcessorStatus;
import org.apache.impala.catalog.events.MetastoreNotificationException;
import org.apache.impala.catalog.monitor.CatalogMonitor;
import org.apache.impala.catalog.monitor.CatalogOperationTracker;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.common.TransactionException;
import org.apache.impala.common.TransactionKeepalive.HeartbeatContext;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.hive.common.MutableValidWriteIdList;
import org.apache.impala.hive.executor.HiveJavaFunction;
import org.apache.impala.hive.executor.HiveJavaFunctionFactory;
import org.apache.impala.thrift.JniCatalogConstants;
import org.apache.impala.thrift.TAlterDbParams;
import org.apache.impala.thrift.TAlterDbSetOwnerParams;
import org.apache.impala.thrift.TAlterTableAddColsParams;
import org.apache.impala.thrift.TAlterTableAddDropRangePartitionParams;
import org.apache.impala.thrift.TAlterTableAddPartitionParams;
import org.apache.impala.thrift.TAlterTableAlterColParams;
import org.apache.impala.thrift.TAlterTableDropColParams;
import org.apache.impala.thrift.TAlterTableDropPartitionParams;
import org.apache.impala.thrift.TAlterTableExecuteParams;
import org.apache.impala.thrift.TAlterTableOrViewSetOwnerParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableReplaceColsParams;
import org.apache.impala.thrift.TAlterTableSetCachedParams;
import org.apache.impala.thrift.TAlterTableSetFileFormatParams;
import org.apache.impala.thrift.TAlterTableSetLocationParams;
import org.apache.impala.thrift.TAlterTableSetPartitionSpecParams;
import org.apache.impala.thrift.TAlterTableSetRowFormatParams;
import org.apache.impala.thrift.TAlterTableSetTblPropertiesParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TAlterTableUnSetTblPropertiesParams;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TBucketType;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TCatalogUpdateResult;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnName;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TCommentOnParams;
import org.apache.impala.thrift.TCopyTestCaseReq;
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
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropDataSourceParams;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.thrift.TDropFunctionParams;
import org.apache.impala.thrift.TDropStatsParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TFunctionName;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.THdfsCachingOp;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TIcebergPartitionSpec;
import org.apache.impala.thrift.TKuduPartitionParam;
import org.apache.impala.thrift.TPartitionDef;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.thrift.TRangePartitionOperationType;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TResetMetadataResponse;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTablePropertyType;
import org.apache.impala.thrift.TTableRowFormat;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTestCaseData;
import org.apache.impala.thrift.TTruncateParams;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateCatalogRequest;
import org.apache.impala.thrift.TUpdateCatalogResponse;
import org.apache.impala.thrift.TUpdatedPartition;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.AcidUtils.TblTransaction;
import org.apache.impala.util.CatalogOpUtil;
import org.apache.impala.util.CompressionUtil;
import org.apache.impala.util.DebugUtils;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.util.MetaStoreUtil.TableInsertEventInfo;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.impala.util.ThreadNameAnnotator;
import org.apache.thrift.TException;
import org.slf4j.Logger;
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
 * The CatalogOpExecutor uses table-level or Db object level locking to protect table
 * metadata or database metadata respectively during concurrent modifications and is
 * responsible for assigning a new catalog version when a table/Db is modified
 * (e.g. alterTable() or alterDb()).
 *
 * The following locking protocol is employed to ensure that modifying
 * the table/Db metadata and assigning a new catalog version is performed atomically and
 * consistently in the presence of concurrent DDL operations. The following pattern
 * ensures that the catalog lock is never held for a long period of time, preventing
 * other DDL operations from making progress. This pattern only applies to single-table/Db
 * update operations and requires the use of fair table locks to prevent starvation.
 * Additionally, this locking protocol is also followed in case of CREATE/DROP
 * FUNCTION. In case of CREATE/DROP FUNCTION, we take the Db object lock since
 * certain FUNCTION are stored in the HMS database parameters. Using this approach
 * also makes sure that adding or removing functions from different databases do not
 * block each other.
 *
 *   DO {
 *     Acquire the catalog lock (see CatalogServiceCatalog.versionLock_)
 *     Try to acquire a table/Db lock
 *     IF the table/Db lock acquisition fails {
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
 *   Modify table/Db metadata
 *   Release table/Db lock
 *
 * Note: The getCatalogObjects() function is the only case where this locking pattern is
 * not used since it accesses multiple catalog entities in order to compute a snapshot
 * of catalog metadata.
 *
 * Operations that CREATE/DROP catalog objects such as tables and databases
 * (except for functions, see above) employ the following locking protocol:
 * 1. Acquire the metastoreDdlLock_
 * 2. Update the Hive Metastore
 * 3. Increment and get a new catalog version
 * 4. Update the catalog
 * 5. Grant/revoke owner privilege if authorization with ownership is enabled.
 * 6. Release the metastoreDdlLock_
 *
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
  public final static String HMS_RPC_ERROR_FORMAT_STR =
      "Error making '%s' RPC to Hive Metastore: ";
  // Error string for inconsistent blacklisted dbs/tables configs between catalogd and
  // coordinators.
  private final static String BLACKLISTED_DBS_INCONSISTENT_ERR_STR =
      "--blacklisted_dbs may be inconsistent between catalogd and coordinators";
  private final static String BLACKLISTED_TABLES_INCONSISTENT_ERR_STR =
      "--blacklisted_tables may be inconsistent between catalogd and coordinators";
  private final static String ALTER_TBL_UNSET_NON_EXIST_PROPERTY =
      "Please use the following syntax if not sure whether the property existed" +
      " or not:\nALTER TABLE tableName UNSET (TBLPROPERTIES|SERDEPROPERTIES) IF EXISTS" +
      " (key1, key2, ...)\n";
  private final static String ALTER_VIEW_UNSET_NON_EXIST_PROPERTY =
      "Please use the following syntax if not sure whether the property existed" +
      " or not:\nALTER VIEW viewName UNSET TBLPROPERTIES IF EXISTS" +
      " (key1, key2, ...)\n";

  // Table default capabilities
  private static final String ACIDINSERTONLY_CAPABILITIES =
      "HIVEMANAGEDINSERTREAD,HIVEMANAGEDINSERTWRITE";
  private static final String FULLACID_CAPABILITIES =
      "HIVEFULLACIDREAD";
  private static final String NONACID_CAPABILITIES = "EXTREAD,EXTWRITE";

  // The maximum number of partitions to update in one Hive Metastore RPC.
  // Used when persisting the results of COMPUTE STATS statements.
  // It is also used as an upper limit for the number of partitions allowed in one ADD
  // PARTITION statement.
  public final static short MAX_PARTITION_UPDATES_PER_RPC = 500;

  // Table capabilities property name
  public static final String CAPABILITIES_KEY = "OBJCAPABILITIES";

  // Labels used in catalog timelines
  private static final String CATALOG_TIMELINE_NAME = "Catalog Server Operation";
  private static final String CHECKED_HMS_TABLE_EXISTENCE =
      "Checked table existence in Metastore";
  private static final String CREATED_HMS_TABLE = "Created table in Metastore";
  private static final String CREATED_CATALOG_TABLE = "Created table in catalog cache";
  private static final String CREATED_ICEBERG_TABLE =
      "Created table using Iceberg Catalog ";
  private static final String DDL_FINISHED = "DDL finished";
  public static final String FETCHED_LATEST_HMS_EVENT_ID =
      "Got current Metastore event id ";
  public static final String FETCHED_HMS_EVENT_BATCH =
      "Fetched event batch from Metastore";
  public static final String FETCHED_HMS_TABLE = "Fetched table from Metastore";
  public static final String FETCHED_HMS_DB = "Fetched database from Metastore";
  public static final String FETCHED_HMS_PARTITION = "Fetched partition from Metastore";
  private static final String GOT_METASTORE_DDL_LOCK = "Got metastoreDdlLock";
  public static final String GOT_TABLE_WRITE_LOCK =
      "Got catalog version write lock and table write lock";
  public static final String GOT_TABLE_READ_LOCK = "Got table read lock";
  private static final String LOADED_ICEBERG_TABLE = "Loaded iceberg table";
  private static final String SENT_CATALOG_FOR_SYNC_DDL =
      "Sent catalog update for sync_ddl";

  private final CatalogServiceCatalog catalog_;
  private final AuthorizationConfig authzConfig_;
  private final AuthorizationManager authzManager_;
  private final HiveJavaFunctionFactory hiveJavaFuncFactory_;

  // A singleton monitoring class that keeps track of the catalog operations.
  private final CatalogOperationTracker catalogOpTracker_ =
      CatalogMonitor.INSTANCE.getCatalogOperationTracker();

  // Lock used to ensure that CREATE[DROP] TABLE[DATABASE] operations performed in
  // catalog_ and the corresponding RPC to apply the change in HMS are atomic.
  private final ReentrantLock metastoreDdlLock_ = new ReentrantLock();

  public CatalogOpExecutor(CatalogServiceCatalog catalog, AuthorizationConfig authzConfig,
      AuthorizationManager authzManager,
      HiveJavaFunctionFactory hiveJavaFuncFactory) throws ImpalaException {
    Preconditions.checkNotNull(authzManager);
    catalog_ = Preconditions.checkNotNull(catalog);
    authzConfig_ = Preconditions.checkNotNull(authzConfig);
    authzManager_ = Preconditions.checkNotNull(authzManager);
    hiveJavaFuncFactory_ = Preconditions.checkNotNull(hiveJavaFuncFactory);
  }

  public CatalogServiceCatalog getCatalog() { return catalog_; }

  public AuthorizationManager getAuthzManager() { return authzManager_; }

  public TDdlExecResponse execDdlRequest(TDdlExecRequest ddlRequest)
      throws ImpalaException {
    EventSequence catalogTimeline = new EventSequence(CATALOG_TIMELINE_NAME);
    TDdlExecResponse response = new TDdlExecResponse();
    response.setResult(new TCatalogUpdateResult());
    response.getResult().setCatalog_service_id(JniCatalog.getServiceId());
    User requestingUser = null;
    TUniqueId queryId = null;
    boolean wantMinimalResult = false;
    if (ddlRequest.isSetHeader()) {
      TCatalogServiceRequestHeader header = ddlRequest.getHeader();
      if (header.isSetRequesting_user()) {
        requestingUser = new User(ddlRequest.getHeader().getRequesting_user());
      }
      wantMinimalResult = ddlRequest.getHeader().isWant_minimal_response();
      queryId = header.getQuery_id();
    }
    Optional<TTableName> tTableName = Optional.empty();
    TDdlType ddlType = ddlRequest.ddl_type;
    try {
      boolean syncDdl = ddlRequest.getQuery_options().isSync_ddl();
      String debugAction = ddlRequest.getQuery_options().getDebug_action();
      switch (ddlType) {
        case ALTER_DATABASE:
          TAlterDbParams alter_db_params = ddlRequest.getAlter_db_params();
          tTableName = Optional.of(new TTableName(alter_db_params.db, ""));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          alterDatabase(alter_db_params, wantMinimalResult, response, catalogTimeline);
          break;
        case ALTER_TABLE:
          TAlterTableParams alter_table_params = ddlRequest.getAlter_table_params();
          tTableName = Optional.of(alter_table_params.getTable_name());
          catalogOpTracker_.increment(ddlRequest, tTableName);
          alterTable(alter_table_params, debugAction, wantMinimalResult, response,
              catalogTimeline);
          break;
        case ALTER_VIEW:
          TCreateOrAlterViewParams alter_view_params = ddlRequest.getAlter_view_params();
          tTableName = Optional.of(alter_view_params.getView_name());
          catalogOpTracker_.increment(ddlRequest, tTableName);
          alterView(alter_view_params, wantMinimalResult, response, catalogTimeline);
          break;
        case CREATE_DATABASE:
          TCreateDbParams create_db_params = ddlRequest.getCreate_db_params();
          tTableName = Optional.of(new TTableName(create_db_params.db, ""));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          createDatabase(create_db_params, response, catalogTimeline, syncDdl,
              wantMinimalResult);
          break;
        case CREATE_TABLE_AS_SELECT:
          TCreateTableParams create_table_as_select_params =
              ddlRequest.getCreate_table_params();
          tTableName = Optional.of(create_table_as_select_params.getTable_name());
          catalogOpTracker_.increment(ddlRequest, tTableName);
          response.setNew_table_created(createTable(create_table_as_select_params,
              response, catalogTimeline, syncDdl, wantMinimalResult, debugAction));
          break;
        case CREATE_TABLE:
          TCreateTableParams create_table_params = ddlRequest.getCreate_table_params();
          tTableName = Optional.of((create_table_params.getTable_name()));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          createTable(ddlRequest.getCreate_table_params(), response, catalogTimeline,
              syncDdl, wantMinimalResult, debugAction);
          break;
        case CREATE_TABLE_LIKE:
          TCreateTableLikeParams create_table_like_params =
              ddlRequest.getCreate_table_like_params();
          tTableName = Optional.of(create_table_like_params.getTable_name());
          catalogOpTracker_.increment(ddlRequest, tTableName);
          createTableLike(create_table_like_params, response, catalogTimeline, syncDdl,
              wantMinimalResult, debugAction);
          break;
        case CREATE_VIEW:
          TCreateOrAlterViewParams create_view_params =
              ddlRequest.getCreate_view_params();
          tTableName = Optional.of(create_view_params.getView_name());
          catalogOpTracker_.increment(ddlRequest, tTableName);
          createView(create_view_params, wantMinimalResult, response, catalogTimeline);
          break;
        case CREATE_FUNCTION:
          TCreateFunctionParams create_func_params = ddlRequest.getCreate_fn_params();
          TFunctionName fnName = create_func_params.getFn().getName();
          tTableName = Optional.of(new TTableName(fnName.db_name, fnName.function_name));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          createFunction(ddlRequest.getCreate_fn_params(), response, catalogTimeline);
          break;
        case CREATE_DATA_SOURCE:
          TCreateDataSourceParams create_ds_params =
              ddlRequest.getCreate_data_source_params();
          tTableName = Optional.of(
              new TTableName(create_ds_params.getData_source().name, ""));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          createDataSource(create_ds_params, response);
          break;
        case COMPUTE_STATS:
          Preconditions.checkState(false, "Compute stats should trigger an ALTER TABLE.");
          break;
        case DROP_STATS:
          TDropStatsParams drop_stats_params = ddlRequest.getDrop_stats_params();
          tTableName = Optional.of(drop_stats_params.getTable_name());
          catalogOpTracker_.increment(ddlRequest, tTableName);
          dropStats(drop_stats_params, wantMinimalResult, response, catalogTimeline,
              ddlRequest.getQuery_options().getDebug_action());
          break;
        case DROP_DATABASE:
          TDropDbParams drop_db_params = ddlRequest.getDrop_db_params();
          tTableName = Optional.of(new TTableName(drop_db_params.getDb(), ""));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          dropDatabase(drop_db_params, response,
              ddlRequest.getQuery_options().getKudu_table_reserve_seconds(),
              catalogTimeline);
          break;
        case DROP_TABLE:
        case DROP_VIEW:
          TDropTableOrViewParams drop_table_or_view_params =
              ddlRequest.getDrop_table_or_view_params();
          tTableName = Optional.of(drop_table_or_view_params.getTable_name());
          catalogOpTracker_.increment(ddlRequest, tTableName);
          // Dropped tables and views are already returned as minimal results, so don't
          // need to pass down wantMinimalResult here.
          dropTableOrView(drop_table_or_view_params, response,
              ddlRequest.getQuery_options().getLock_max_wait_time_s(),
              ddlRequest.getQuery_options().getKudu_table_reserve_seconds(),
              catalogTimeline);
          break;
        case TRUNCATE_TABLE:
          TTruncateParams truncate_params = ddlRequest.getTruncate_params();
          tTableName = Optional.of(truncate_params.getTable_name());
          catalogOpTracker_.increment(ddlRequest, tTableName);
          truncateTable(truncate_params, wantMinimalResult, response,
              ddlRequest.getQuery_options().getLock_max_wait_time_s(), catalogTimeline,
              ddlRequest.getQuery_options().getDebug_action());
          break;
        case DROP_FUNCTION:
          TDropFunctionParams drop_func_params = ddlRequest.getDrop_fn_params();
          TFunctionName dropFnName = drop_func_params.getFn_name();
          tTableName = Optional.of(
              new TTableName(dropFnName.db_name, dropFnName.function_name));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          dropFunction(ddlRequest.getDrop_fn_params(), response, catalogTimeline);
          break;
        case DROP_DATA_SOURCE:
          TDropDataSourceParams drop_ds_params = ddlRequest.getDrop_data_source_params();
          tTableName = Optional.of(new TTableName(drop_ds_params.data_source, ""));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          dropDataSource(drop_ds_params, response);
          break;
        case CREATE_ROLE:
          TCreateDropRoleParams create_role_params =
              ddlRequest.getCreate_drop_role_params();
          tTableName = Optional.of(new TTableName(create_role_params.role_name, ""));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          createRole(requestingUser, create_role_params, response);
          break;
        case DROP_ROLE:
          TCreateDropRoleParams drop_role_params =
            ddlRequest.getCreate_drop_role_params();
          tTableName = Optional.of(new TTableName(drop_role_params.role_name, ""));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          dropRole(requestingUser, drop_role_params, response);
          break;
        case GRANT_ROLE:
          TGrantRevokeRoleParams grant_role_params =
              ddlRequest.getGrant_revoke_role_params();
          tTableName = Optional.of(new TTableName(
              StringUtils.join(",", grant_role_params.group_names), ""));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          grantRoleToGroup(requestingUser, grant_role_params, response);
          break;
        case REVOKE_ROLE:
          TGrantRevokeRoleParams revoke_role_params =
              ddlRequest.getGrant_revoke_role_params();
          tTableName = Optional.of(new TTableName(
              StringUtils.join(",", revoke_role_params.group_names), ""));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          revokeRoleFromGroup(
              requestingUser, revoke_role_params, response);
          break;
        case GRANT_PRIVILEGE:
          TGrantRevokePrivParams grant_priv_params =
              ddlRequest.getGrant_revoke_priv_params();
          tTableName = Optional.of(new TTableName(grant_priv_params.principal_name, ""));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          grantPrivilege(ddlRequest.getHeader(), grant_priv_params, response);
          break;
        case REVOKE_PRIVILEGE:
          TGrantRevokePrivParams revoke_priv_params =
              ddlRequest.getGrant_revoke_priv_params();
          tTableName = Optional.of(new TTableName(revoke_priv_params.principal_name, ""));
          catalogOpTracker_.increment(ddlRequest, tTableName);
          revokePrivilege(ddlRequest.getHeader(), revoke_priv_params, response);
          break;
        case COMMENT_ON:
          tTableName = Optional.of(new TTableName("", ""));
          alterCommentOn(ddlRequest, response, tTableName, wantMinimalResult,
              catalogTimeline);
          break;
        case COPY_TESTCASE:
          catalogOpTracker_.increment(ddlRequest, Optional.empty());
          copyTestCaseData(ddlRequest.getCopy_test_case_params(), response,
              wantMinimalResult);
          break;
        default:
          catalogOpTracker_.increment(ddlRequest, Optional.empty());
          throw new IllegalStateException(
              "Unexpected DDL exec request type: " + ddlType);
      }
      catalogTimeline.markEvent(DDL_FINISHED);

      // If SYNC_DDL is set, set the catalog update that contains the results of this DDL
      // operation. The version of this catalog update is returned to the requesting
      // impalad which will wait until this catalog update has been broadcast to all the
      // coordinators.
      if (syncDdl) {
        response.getResult().setVersion(
            catalog_.waitForSyncDdlVersion(response.getResult()));
        catalogTimeline.markEvent(SENT_CATALOG_FOR_SYNC_DDL);
      }

      TRuntimeProfileNode profile = Frontend.createTRuntimeProfileNode("CatalogOp");
      profile.addToEvent_sequences(catalogTimeline.toThrift());
      response.setProfile(profile);
      // At this point, the operation is considered successful. If any errors occurred
      // during execution, this function will throw an exception and the CatalogServer
      // will handle setting a bad status code.
      response.getResult().setStatus(new TStatus(TErrorCode.OK, new ArrayList<>()));
      catalogOpTracker_.decrement(ddlType, queryId, tTableName, /*exception*/null);
    } catch (Exception e) {
      catalogOpTracker_.decrement(ddlType, queryId, tTableName,
          JniUtil.throwableToString(e));
      throw e;
    }
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
      TCopyTestCaseReq request, TDdlExecResponse response, boolean wantMinimalResult)
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
    if (testCaseData.getDbs() != null) {
      for (TDatabase thriftDb : testCaseData.getDbs()) {
        Db db = Db.fromTDatabase(thriftDb);
        // Set a new version to force an overwrite if a Db already exists with the same
        // name.
        db.setCatalogVersion(catalog_.incrementAndGetCatalogVersion());
        Db ret = catalog_.addDb(db.getName(), db.getMetaStoreDb());
        if (ret != null) {
          ++numDbsAdded;
          addDbToCatalogUpdate(db, wantMinimalResult, response.result);
        }
      }
    }

    int numTblsAdded = 0;
    int numViewsAdded = 0;
    if (testCaseData.getTables_and_views() != null) {
      for (TTable tTable : testCaseData.tables_and_views) {
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
        t.takeReadLock();
        try {
          addTableToCatalogUpdate(t, wantMinimalResult, response.result);
        } finally {
          t.releaseReadLock();
        }
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
   * This method checks if the write lock of 'catalog_' is unlocked. If it's still locked
   * then it logs an error and unlocks it.
   */
  public void UnlockWriteLockIfErronouslyLocked() {
    if(catalog_.getLock().isWriteLockedByCurrentThread()) {
      LOG.error("Write lock should have been released.");
      catalog_.getLock().writeLock().unlock();
    }
  }

  /**
   * Remove a catalog table based on the given metastore table if it exists and its
   * id matches with the id of the table in Catalog.
   *
   * @param eventId Event Id being processed.
   * @param dbName Database name of the table to be removed.
   * @param tblName Name of the table to be removed.
   * @param tblAddedLater is set to true if the table was not removed because it was
   *                      created after the event id being processed.
   * @return True if the table was removed; False otherwise.
   */
  public boolean removeTableIfNotAddedLater(long eventId,
      String dbName, String tblName, Reference<Boolean> tblAddedLater) {
    tblAddedLater.setRef(false);
    getMetastoreDdlLock().lock();
    try {
      Db db = catalog_.getDb(dbName);
      if (db == null) {
        LOG.debug("EventId: {} Not removing the table since database {} does not exist",
            eventId, dbName);
        return false;
      }

      Table tblToBeRemoved = db.getTable(tblName);
      if (tblToBeRemoved == null) {
        LOG.debug("EventId: {} Not removing the table since table {} does not exist",
            eventId, tblName);
        return false;
      }
      // if the table exists, we must make sure that it was not created by this catalog
      // since the event was generated.
      if (eventId <= tblToBeRemoved.getCreateEventId()) {
        LOG.debug(
            "EventId: {} Not removing the table {} table's create event id is {} ",
            eventId, new TableName(dbName, tblName), tblToBeRemoved.getCreateEventId());
        tblAddedLater.setRef(true);
        return false;
      }
      Table removedTbl = db.removeTable(tblToBeRemoved.getName());
      removedTbl.setCatalogVersion(catalog_.incrementAndGetCatalogVersion());
      catalog_.getDeleteLog().addRemovedObject(removedTbl.toMinimalTCatalogObject());
      return true;
    } finally {
      getMetastoreDdlLock().unlock();
    }
  }

  /**
   * Adds the given table to the catalog if it does not exists and if there is
   * no removal event found the deleteEventLog which is greater than the given eventId.
   * @return true if the table was successfully added; Otherwise returns false.
   * @throws DatabaseNotFoundException if the db is not found.
   */
  public boolean addTableIfNotRemovedLater(long eventId,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws DatabaseNotFoundException {
    getMetastoreDdlLock().lock();
    try {
      String dbName = msTbl.getDbName();
      Db db = catalog_.getDb(dbName);
      DeleteEventLog deleteEventLog = catalog_.getMetastoreEventProcessor()
          .getDeleteEventLog();
      if (db == null) {
        // if db is not found in the catalog, check if it was removed since the event
        // was generated.
        if (!deleteEventLog.wasRemovedAfter(eventId,
            DeleteEventLog.getDbKey(msTbl.getDbName()))) {
          throw new DatabaseNotFoundException(dbName + " not found");
        }
        LOG.debug(
            "EventId: {} Table was not added since the database {} was removed later",
            eventId, dbName);
        return false;
      }
      String tblName = msTbl.getTableName();
      Table existingTable = db.getTable(tblName);
      if (existingTable != null) {
        LOG.debug("EventId: {} Table {} was not added since "
                + "it already exists in catalog.", eventId, existingTable.getFullName());
        if (existingTable.getCreateEventId() != eventId) {
          LOG.warn("Existing table {} create event Id: {} does not match the "
                  + "event id: {}", existingTable.getFullName(),
              existingTable.getCreateEventId(), eventId);
        }
        return false;
      }
      // table does not exist in catalog. We must make sure that the table was
      // not removed since the event was generated.
      if (deleteEventLog.wasRemovedAfter(eventId, DeleteEventLog.getKey(msTbl))) {
        LOG.debug(
            "EventId: {} Table was not added since it was removed later", eventId);
        return false;
      }
      Table incompleteTable = IncompleteTable.createUninitializedTable(db, tblName,
          MetastoreShim.mapToInternalTableType(msTbl.getTableType()),
          MetadataOp.getTableComment(msTbl));
      incompleteTable.setCatalogVersion(catalog_.incrementAndGetCatalogVersion());
      // set the createEventId of the table to eventId since we are adding table
      // due to the given eventId.
      incompleteTable.setCreateEventId(eventId);
      db.addTable(incompleteTable);
      return true;
    } finally {
      getMetastoreDdlLock().unlock();
    }
  }

  /**
   * Renames a table based on the event. The rename operation is implemented by
   * removing the old table and adding an IncompleteTable with the new name.
   *
   * @param eventId The eventId which is being processed.
   * @param msTblBefore The table object before the rename was done.
   * @param msTblAfter The table object after the rename was processed by metastore.
   * @param oldTblRemoved This reference is set if the old table was found and removed
   *                      from catalogd.
   * @param newTblAdded This reference is set if new table is added to the catalogd.
   * @throws CatalogException If the rename event could not processed because the
   * table lock could not acquired.
   */
  public void renameTableFromEvent(long eventId,
      org.apache.hadoop.hive.metastore.api.Table msTblBefore,
      org.apache.hadoop.hive.metastore.api.Table msTblAfter,
      Reference<Boolean> oldTblRemoved, Reference<Boolean> newTblAdded)
      throws CatalogException {
    getMetastoreDdlLock().lock();
    try {
      Table tblBefore = null;
      try {
        tblBefore = catalog_
            .getTable(msTblBefore.getDbName(), msTblBefore.getTableName());
      } catch (DatabaseNotFoundException e) {
        // ignore if the database is not found; we consider it same as table
        // not found and we don't remove it.
      }
      boolean beforeTblLocked = false;
      try {
        if (tblBefore != null) {
          // if the before table exists, then we must take a lock on it so that
          // we block any other concurrent operations on it.
          tryWriteLock(tblBefore, "ALTER_TABLE RENAME EVENT",
              NoOpEventSequence.INSTANCE);
          beforeTblLocked = true;
          catalog_.getLock().writeLock().unlock();
        }
        Reference<Boolean> tableAddedLater = new Reference<>();
        boolean tblRemoved = removeTableIfNotAddedLater(eventId,
            msTblBefore.getDbName(), msTblBefore.getTableName(), tableAddedLater);
        oldTblRemoved.setRef(tblRemoved);
        if (!tblRemoved) {
          LOG.debug("EventId: {} original table not removed since {}", eventId,
              (tableAddedLater.getRef() ? "it is added later"
                  : "it doesn't exist anymore"));
        }
        boolean tblAdded = addTableIfNotRemovedLater(eventId, msTblAfter);
        newTblAdded.setRef(tblAdded);
      } catch (InternalException e) {
        throw new CatalogException(
            "Unable to process rename table event " + eventId, e);
      } finally {
        UnlockWriteLockIfErronouslyLocked();
        if (beforeTblLocked) tblBefore.releaseWriteLock();
      }
    } finally {
      getMetastoreDdlLock().unlock();
    }
  }

  /**
   * Adds a database to the catalogd if it does not exists or if it was not removed
   * since the event was generated.
   * @param eventId The eventId being processed.
   * @param msDb the {@link Database} object to be added to catalogd.
   * @return True if the database was added, false otherwise.
   */
  public boolean addDbIfNotRemovedLater(
      long eventId, org.apache.hadoop.hive.metastore.api.Database msDb) {
    getMetastoreDdlLock().lock();
    try {
      String dbName = msDb.getName();
      Db db = catalog_.getDb(dbName);
      DeleteEventLog deleteEventLog = catalog_.getMetastoreEventProcessor()
          .getDeleteEventLog();
      if (db == null) {
        if (!deleteEventLog.wasRemovedAfter(eventId, DeleteEventLog.getKey(msDb))) {
          catalog_.addDb(dbName, msDb, eventId);
          return true;
        }
      }
      return false;
    } finally {
      getMetastoreDdlLock().unlock();
    }
  }

  /**
   * Removes the database from catalogd if it exists and has not been added since the
   * eventId was generated.
   * @param eventId The eventId being processed.
   * @param dbName Metastore db name used to remove Db from Catalog
   * @return true if the database was removed; else false.
   */
  public boolean removeDbIfNotAddedLater(long eventId, String dbName) {
    getMetastoreDdlLock().lock();
    try {
      Db catalogDb = catalog_.getDb(dbName);
      if (catalogDb == null) {
        LOG.info(
            "EventId: {} Skipping the event since database {} does not exist anymore",
            eventId, dbName);
        return false;
      }
      // if this database has been created after this drop database event is generated
      // the createdEventId of the database will be higher than eventId. In such case
      // if means that catalog has recreated this database again and events processor
      // is just receiving the earlier drop database event. We should ignore such event.
      if (catalogDb.getCreateEventId() > eventId) {
        LOG.info(
            "EventId: {} Not removing the database {} since the create event id is {}",
            eventId, dbName, catalogDb.getCreateEventId());
        return false;
      }
      catalog_.removeDb(dbName);
      return true;
    } finally {
      getMetastoreDdlLock().unlock();
    }
  }

  /**
   * Updates the catalog db with alteredMsDb. To do so, first acquire lock on catalog db
   * and then metastore db is updated. Also update the event id in the db.
   * No update is done if the catalog db is already synced till this event id
   * @param eventId: HMS event id for this alter db operation
   * @param alteredMsDb: metastore db to update in catalogd
   * @return: true if metastore db was updated in catalog's db
   *          false otherwise
   */
  public boolean alterDbIfExists(long eventId,
      org.apache.hadoop.hive.metastore.api.Database alteredMsDb) {
    Preconditions.checkNotNull(alteredMsDb);
    String dbName = alteredMsDb.getName();
    Db dbToAlter = catalog_.getDb(dbName);
    if (dbToAlter == null) {
      LOG.debug("Event id: {}, not altering db {} since it does not exist in catalogd",
          eventId, dbName);
      return false;
    }
    boolean syncToLatestEventId =
        BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();
    boolean dbLocked = false;
    try {
      tryLock(dbToAlter, String.format("alter db from event id: %s", eventId),
          NoOpEventSequence.INSTANCE);
      catalog_.getLock().writeLock().unlock();
      dbLocked = true;
      if (syncToLatestEventId && dbToAlter.getLastSyncedEventId() >= eventId) {
        LOG.debug("Not altering db {} from event id: {} since db is already synced "
                + "till event id: {}", dbName, eventId, dbToAlter.getLastSyncedEventId());
        return false;
      }
      boolean success = catalog_.updateDbIfExists(alteredMsDb);
      if (success) {
        dbToAlter.setLastSyncedEventId(eventId);
      }
      return success;
    } catch (Exception e) {
      LOG.error("Event id: {}, failed to alter db {}. Error message: {}", eventId, dbName,
          e.getMessage());
      return false;
    } finally {
      if (dbLocked) {
        dbToAlter.getLock().unlock();
      }
    }
  }

  /**
   * Helper class to keep track of all in-progress table modification.
   * Catalog version number will be increased upon instantiation of this class,
   * but will not be applied towards 'table' yet.
   * Call updateTableCatalogVersion() to apply the new catalog version to the 'table'.
   * The new version can be accessed through newVersionNumber() method.
   */
  private static class InProgressTableModification {
    private final CatalogServiceCatalog catalog_;
    private final Table table_;
    private final long newVersionNumber_;

    // Marked to True if call to catalog_.addVersionsForInflightEvents() returns true.
    private boolean inflightEventAdded_ = false;

    // Marked to True if call to catalog_.addVersionsForInflightEvents() returns true.
    // Afterwards, marked back to False through either of cancelInflightEventIfExist() or
    // markInflightEventRegistrationComplete().
    private boolean inflightEventRegistrationOngoing_ = false;

    public InProgressTableModification(CatalogServiceCatalog catalog, Table table) {
      catalog_ = catalog;
      table_ = table;
      newVersionNumber_ = catalog.incrementAndGetCatalogVersion();
    }

    /**
     * Return the new version number when this object is created.
     */
    public long newVersionNumber() { return newVersionNumber_; }

    /**
     * Register the new version number into table's in-flight events.
     * Must be called before the HMS API for non-insert events, except in few specific
     * alter partition operation. If called multiple times,
     * only the first successful one will go through and the rest become no-op.
     */
    public void registerInflightEvent() {
      if (inflightEventAdded_) return;
      if (catalog_.addVersionsForInflightEvents(false, table_, newVersionNumber_)) {
        inflightEventAdded_ = true;
        inflightEventRegistrationOngoing_ = true;
      }
    }

    /**
     * Cancel the new version number registration out of table's in-flight events if it
     * exist.
     */
    public void cancelInflightEventIfExist() {
      // If inflightEventAdded_ is True, isRemoved should also be True.
      // Unless, if the table_.removeFromVersionsForInflightEvents() has been
      // concurrently called by other call site that does not track using this
      // InProgressTableModification, such as MetastoreEvent.isSelfEvent() or
      // CatalogServiceCatalog.evaluateSelfEventForTable().
      boolean isRemoved =
          table_.removeFromVersionsForInflightEvents(false, newVersionNumber_);
      LOG.info("Cancel in-progress in-flight event of table " + table_.getFullName()
          + ". versionNumber=" + newVersionNumber_ + " isRemoved=" + isRemoved
          + " inflightEventAdded_=" + inflightEventAdded_);
      markInflightEventRegistrationComplete();
    }

    /**
     * Mark the new version number registration to table's in-flight events as complete.
     */
    public void markInflightEventRegistrationComplete() {
      inflightEventRegistrationOngoing_ = false;
    }

    /**
     * Set table's catalog version to the new version number.
     */
    public void updateTableCatalogVersion() {
      table_.setCatalogVersion(newVersionNumber_);
    }

    /**
     * Adds the catalog service id and the given catalog version to the table
     * parameters. No-op if event processing is disabled.
     */
    private void addCatalogServiceIdentifiersToTable() {
      if (!catalog_.isEventProcessingActive()) return;
      org.apache.hadoop.hive.metastore.api.Table msTbl = table_.getMetaStoreTable();
      msTbl.putToParameters(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(),
          catalog_.getCatalogServiceId());
      msTbl.putToParameters(MetastoreEventPropertyKey.CATALOG_VERSION.getKey(),
          String.valueOf(newVersionNumber_));
    }

    /**
     * Return true if table has in-progress modification or this object has in-flight
     * event registration that is not marked as complete or canceled.
     */
    public boolean isInProgress() {
      return table_.hasInProgressModification() || inflightEventRegistrationOngoing_;
    }

    /**
     * Validate that all in-progress table modification is done.
     * TODO: Try implement AutoCloseable interface and make the close() method call
     * this method. It will allow using this class in try-with-resources pattern.
     */
    public void validateInProgressModificationComplete() {
      Preconditions.checkState(!table_.hasInProgressModification(),
          "Table %s still has in-progress modification.", table_.getFullName());
      Preconditions.checkState(!inflightEventRegistrationOngoing_,
          "Table %s still has incomplete self-event registration.", table_.getFullName());
    }
  }

  /**
   * Execute the ALTER TABLE command according to the TAlterTableParams and refresh the
   * table metadata, except for RENAME, ADD PARTITION and DROP PARTITION. This call is
   * thread-safe, i.e. concurrent operations on the same table are serialized.
   */
  private void alterTable(TAlterTableParams params, @Nullable String debugAction,
      boolean wantMinimalResult, TDdlExecResponse response, EventSequence catalogTimeline)
      throws ImpalaException {
    // When true, loads the file/block metadata.
    boolean reloadFileMetadata = false;
    // When true, loads the table schema and the column stats from the Hive Metastore.
    boolean reloadTableSchema = false;

    Reference<Long> numUpdatedPartitions = new Reference<>(0L);

    TableName tableName = TableName.fromThrift(params.getTable_name());
    Table tbl = getExistingTable(tableName.getDb(), tableName.getTbl(),
        "Load for ALTER TABLE", catalogTimeline);
    if (params.getAlter_type() == TAlterTableType.RENAME_VIEW
        || params.getAlter_type() == TAlterTableType.RENAME_TABLE) {
      TableName newTableName = TableName.fromThrift(
          params.getRename_params().getNew_table_name());
      Preconditions.checkState(!catalog_.isBlacklistedTable(newTableName),
          String.format("Can't rename to blacklisted table name: %s. %s", newTableName,
              BLACKLISTED_DBS_INCONSISTENT_ERR_STR));
    }
    tryWriteLock(tbl, catalogTimeline);
    // get table's catalogVersion before altering it
    long oldCatalogVersion = tbl.getCatalogVersion();
    // Get a new catalog version, wrap it in InProgressTableModification, and assign new
    // version to the table being altered.
    InProgressTableModification modification =
        new InProgressTableModification(catalog_, tbl);
    modification.addCatalogServiceIdentifiersToTable();
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
              modification, wantMinimalResult, response, catalogTimeline);
          modification.validateInProgressModificationComplete();
          return;
        } finally {
          // release the version taken in the tryLock call above
          catalog_.getLock().writeLock().unlock();
        }
      }

      String responseSummaryMsg = null;
      catalog_.getLock().writeLock().unlock();

      if (tbl instanceof KuduTable && altersKuduTable(params.getAlter_type())) {
        alterKuduTable(params, response, (KuduTable) tbl, modification, wantMinimalResult,
            catalogTimeline, debugAction);
        modification.validateInProgressModificationComplete();
        return;
      } else if (tbl instanceof IcebergTable &&
          altersIcebergTable(params.getAlter_type())) {
        boolean needToUpdateHms = alterIcebergTable(params, response, (IcebergTable) tbl,
            wantMinimalResult, debugAction, catalogTimeline, modification);
        if (!needToUpdateHms) {
          modification.validateInProgressModificationComplete();
          return;
        }
      }

      boolean reloadMetadata = true;
      switch (params.getAlter_type()) {
        case ADD_COLUMNS:
          boolean added = false;
          // Columns could be ignored/cleared in AlterTableAddColsStmt,
          // that may cause columns to be empty.
          if (params.getAdd_cols_params() != null
              && params.getAdd_cols_params().getColumnsSize() != 0) {
            TAlterTableAddColsParams addColParams = params.getAdd_cols_params();
            added = alterTableAddCols(tbl, addColParams.getColumns(),
                addColParams.isIf_not_exists(), catalogTimeline, modification);
            reloadTableSchema = true;
          }
          if (added) {
            responseSummaryMsg = "New column(s) have been added to the table.";
          } else {
            responseSummaryMsg = "No new column(s) have been added to the table.";
          }
          break;
        case REPLACE_COLUMNS:
          TAlterTableReplaceColsParams replaceColParams = params.getReplace_cols_params();
          alterTableReplaceCols(
              tbl, replaceColParams.getColumns(), catalogTimeline, modification);
          reloadTableSchema = true;
          responseSummaryMsg = "Table columns have been replaced.";
          break;
        case ADD_PARTITION:
          // Create and add HdfsPartition objects to the corresponding HdfsTable and load
          // their block metadata. Update the table object with the new catalog version.
          THdfsFileFormat format = null;
          if(params.isSetSet_file_format_params()) {
            format = params.getSet_file_format_params().file_format;
          }
          alterTableAddPartitions(tbl, params.getAdd_partition_params(), format,
              catalogTimeline, modification, debugAction);
          reloadMetadata = false;
          responseSummaryMsg = "New partition has been added to the table.";
          break;
        case DROP_COLUMN:
          TAlterTableDropColParams dropColParams = params.getDrop_col_params();
          alterTableDropCol(
              tbl, dropColParams.getCol_name(), catalogTimeline, modification);
          reloadTableSchema = true;
          responseSummaryMsg = "Column has been dropped.";
          break;
        case ALTER_COLUMN:
          TAlterTableAlterColParams alterColParams = params.getAlter_col_params();
          alterTableAlterCol(tbl, alterColParams.getCol_name(),
              alterColParams.getNew_col_def(), catalogTimeline, modification);
          reloadTableSchema = true;
          responseSummaryMsg = "Column has been altered.";
          break;
        case DROP_PARTITION:
          TAlterTableDropPartitionParams dropPartParams =
              params.getDrop_partition_params();
          // Drop the partition from the corresponding table. If "purge" option is
          // specified partition data is purged by skipping Trash, if configured.
          alterTableDropPartition(tbl, dropPartParams.getPartition_set(),
              dropPartParams.isIf_exists(), dropPartParams.isPurge(),
              numUpdatedPartitions, catalogTimeline, modification);
          responseSummaryMsg =
              "Dropped " + numUpdatedPartitions.getRef() + " partition(s).";
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
          reloadFileMetadata = alterTableSetFileFormat(tbl,
              fileFormatParams.getPartition_set(), fileFormatParams.getFile_format(),
              numUpdatedPartitions, catalogTimeline, modification);

          if (fileFormatParams.isSetPartition_set()) {
            responseSummaryMsg =
                "Updated " + numUpdatedPartitions.getRef() + " partition(s).";
          } else {
            responseSummaryMsg = "Updated table.";
          }
          break;
        case SET_ROW_FORMAT:
          TAlterTableSetRowFormatParams rowFormatParams =
              params.getSet_row_format_params();
          reloadFileMetadata = alterTableSetRowFormat(tbl,
              rowFormatParams.getPartition_set(), rowFormatParams.getRow_format(),
              numUpdatedPartitions, catalogTimeline, modification);
          if (rowFormatParams.isSetPartition_set()) {
            responseSummaryMsg =
                "Updated " + numUpdatedPartitions.getRef() + " partition(s).";
          } else {
            responseSummaryMsg = "Updated table.";
          }
          break;
        case SET_LOCATION:
          TAlterTableSetLocationParams setLocationParams =
              params.getSet_location_params();
          List<TPartitionKeyValue> partitionSpec = setLocationParams.getPartition_spec();
          reloadFileMetadata = alterTableSetLocation(tbl, partitionSpec,
              setLocationParams.getLocation(), catalogTimeline, modification);
          if (partitionSpec == null) {
            responseSummaryMsg = "New location has been set.";
          } else {
            responseSummaryMsg = "New location has been set for the specified partition.";
          }
          break;
        case SET_TBL_PROPERTIES:
          alterTableSetTblProperties(tbl, params.getSet_tbl_properties_params(),
              numUpdatedPartitions, catalogTimeline, modification);
          reloadTableSchema = true;
          if (params.getSet_tbl_properties_params().isSetPartition_set()) {
            responseSummaryMsg =
                "Updated " + numUpdatedPartitions.getRef() + " partition(s).";
          } else {
            responseSummaryMsg = "Updated table.";
          }
          break;
        case UNSET_TBL_PROPERTIES:
          alterTableUnSetTblProperties(tbl, params.getUnset_tbl_properties_params(),
              numUpdatedPartitions, catalogTimeline, modification);
          reloadTableSchema = true;
          if (params.getUnset_tbl_properties_params().isSetPartition_set()) {
            responseSummaryMsg =
                "Updated " + numUpdatedPartitions.getRef() + " partition(s).";
          } else {
            responseSummaryMsg = "Updated table.";
          }
          break;
        case SET_VIEW_PROPERTIES:
          alterViewSetTblProperties(
              tbl, params.getSet_tbl_properties_params(), catalogTimeline, modification);
          reloadTableSchema = true;
          responseSummaryMsg = "Updated view.";
          break;
        case UNSET_VIEW_PROPERTIES:
          alterViewUnSetTblProperties(tbl, params.getUnset_tbl_properties_params(),
              catalogTimeline, modification);
          reloadTableSchema = true;
          responseSummaryMsg = "Updated view.";
          break;
        case UPDATE_STATS:
          Preconditions.checkState(params.isSetUpdate_stats_params());
          Reference<Long> numUpdatedColumns = new Reference<>(0L);
          alterTableUpdateStats(tbl, params.getUpdate_stats_params(),
              numUpdatedPartitions, numUpdatedColumns, debugAction, catalogTimeline,
              modification);
          reloadTableSchema = true;
          responseSummaryMsg = "Updated " + numUpdatedPartitions.getRef() +
              " partition(s) and " + numUpdatedColumns.getRef() + " column(s).";
          break;
        case SET_CACHED:
          Preconditions.checkState(params.isSetSet_cached_params());
          String op = params.getSet_cached_params().getCache_op().isSet_cached() ?
              "Cached " : "Uncached ";
          if (params.getSet_cached_params().getPartition_set() == null) {
            reloadFileMetadata = alterTableSetCached(
                tbl, params.getSet_cached_params(), catalogTimeline, modification);
            responseSummaryMsg = op + "table.";
          } else {
            alterPartitionSetCached(tbl, params.getSet_cached_params(),
                numUpdatedPartitions, catalogTimeline, modification);
            responseSummaryMsg = op + numUpdatedPartitions.getRef() + " partition(s).";
          }
          break;
        case RECOVER_PARTITIONS:
          alterTableRecoverPartitions(tbl, debugAction, catalogTimeline, modification);
          responseSummaryMsg = "Partitions have been recovered.";
          break;
        case SET_OWNER:
          Preconditions.checkState(params.isSetSet_owner_params());
          alterTableOrViewSetOwner(
              tbl, params.getSet_owner_params(), response, catalogTimeline, modification);
          responseSummaryMsg = "Updated table/view.";
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown ALTER TABLE operation type: " + params.getAlter_type());
      }

      // Make sure we won't forget finalizing the modification.
      if (modification.isInProgress()) {
        Preconditions.checkState(reloadMetadata,
            "In-progress modification of table %s must be followed by metadata reload.",
            tbl.getFullName());
      }
      if (reloadMetadata) {
        loadTableMetadata(tbl, modification.newVersionNumber(), reloadFileMetadata,
            reloadTableSchema, "ALTER TABLE " + params.getAlter_type().name(),
            debugAction, catalogTimeline);
        modification.markInflightEventRegistrationComplete();
      }
      addSummary(response, responseSummaryMsg);
      // add table to catalog update if its old and existing versions do not match
      if (tbl.getCatalogVersion() != oldCatalogVersion) {
        addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
      }
      // Make sure all the modifications are done.
      modification.validateInProgressModificationComplete();
    } catch (Exception ex) {
      modification.cancelInflightEventIfExist();
      throw ex;
    } finally {
      context.stop();
      UnlockWriteLockIfErronouslyLocked();
      // Clear in-progress modifications in case of exceptions.
      tbl.resetInProgressModification();
      tbl.releaseWriteLock();
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
      KuduTable tbl, InProgressTableModification modification, boolean wantMinimalResult,
      EventSequence catalogTimeline, @Nullable String debugAction)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    switch (params.getAlter_type()) {
      case ADD_COLUMNS:
        if (params.getAdd_cols_params() != null
            && params.getAdd_cols_params().getColumnsSize() != 0) {
          TAlterTableAddColsParams addColParams = params.getAdd_cols_params();
          KuduCatalogOpExecutor.addColumn(tbl, addColParams.getColumns(),
              catalogTimeline);
          addSummary(response, "Column(s) have been added.");
        } else {
          addSummary(response, "No new column(s) have been added to the table.");
        }
        break;
      case REPLACE_COLUMNS:
        TAlterTableReplaceColsParams replaceColParams = params.getReplace_cols_params();
        KuduCatalogOpExecutor.addColumn(tbl, replaceColParams.getColumns(),
            catalogTimeline);
        addSummary(response, "Column(s) have been replaced.");
        break;
      case DROP_COLUMN:
        TAlterTableDropColParams dropColParams = params.getDrop_col_params();
        KuduCatalogOpExecutor.dropColumn(tbl, dropColParams.getCol_name(),
            catalogTimeline);
        addSummary(response, "Column has been dropped.");
        break;
      case ALTER_COLUMN:
        TAlterTableAlterColParams alterColParams = params.getAlter_col_params();
        KuduCatalogOpExecutor.alterColumn(tbl, alterColParams.getCol_name(),
            alterColParams.getNew_col_def(), catalogTimeline);
        addSummary(response, "Column has been altered.");
        break;
      case ADD_DROP_RANGE_PARTITION:
        TAlterTableAddDropRangePartitionParams partParams =
            params.getAdd_drop_range_partition_params();
        KuduCatalogOpExecutor.addDropRangePartition(tbl, partParams, catalogTimeline);
        addSummary(response, "Range partition has been " +
            (partParams.type == TRangePartitionOperationType.ADD ?
            "added." : "dropped."));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported ALTER TABLE operation for Kudu tables: " +
            params.getAlter_type());
    }

    loadTableMetadata(tbl, modification.newVersionNumber(), true, true,
        "ALTER KUDU TABLE " + params.getAlter_type().name(), debugAction,
        catalogTimeline);
    addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
  }

  /**
   * Returns true if the given alteration type changes the underlying table stored in
   * Iceberg in addition to the HMS table.
   */
  private boolean altersIcebergTable(TAlterTableType type) {
    return type == TAlterTableType.ADD_COLUMNS
        || type == TAlterTableType.REPLACE_COLUMNS
        || type == TAlterTableType.EXECUTE
        || type == TAlterTableType.DROP_COLUMN
        || type == TAlterTableType.ALTER_COLUMN
        || type == TAlterTableType.SET_PARTITION_SPEC
        || type == TAlterTableType.SET_TBL_PROPERTIES
        || type == TAlterTableType.UNSET_TBL_PROPERTIES
        || type == TAlterTableType.DROP_PARTITION;
  }

  /**
   * Executes the ALTER TABLE command for an Iceberg table and reloads its metadata.
   * Returns true if we also need to update the table definition in HMS. Returns false
   * if the HMS table is already updated by Iceberg, or there is nothing to update in
   * HMS (the change is internal to Iceberg).
   */
  private boolean alterIcebergTable(TAlterTableParams params, TDdlExecResponse response,
      IcebergTable tbl, boolean wantMinimalResult, @Nullable String debugAction,
      EventSequence catalogTimeline, InProgressTableModification modification)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    boolean needsToUpdateHms = !IcebergUtil.isHiveCatalog(tbl.getMetaStoreTable());
    try {
      org.apache.iceberg.Transaction iceTxn = IcebergUtil.getIcebergTransaction(tbl);
      catalogTimeline.markEvent("Got Iceberg transaction");
      switch (params.getAlter_type()) {
        case ADD_COLUMNS:
          if (params.getAdd_cols_params() != null
              && params.getAdd_cols_params().getColumnsSize() != 0) {
            TAlterTableAddColsParams addColParams = params.getAdd_cols_params();
            IcebergCatalogOpExecutor.addColumns(iceTxn, addColParams.getColumns());
            addSummary(response, "Column(s) have been added.");
          } else {
            addSummary(response, "No new column(s) have been added to the table.");
          }
          break;
        case DROP_COLUMN:
          TAlterTableDropColParams dropColParams = params.getDrop_col_params();
          IcebergCatalogOpExecutor.dropColumn(iceTxn, dropColParams.getCol_name());
          addSummary(response, "Column has been dropped.");
          break;
        case ALTER_COLUMN:
          TAlterTableAlterColParams alterColParams = params.getAlter_col_params();
          IcebergCatalogOpExecutor.alterColumn(iceTxn, alterColParams.getCol_name(),
               alterColParams.getNew_col_def());
          addSummary(response, "Column has been altered.");
          break;
        case EXECUTE:
          Preconditions.checkState(params.isSetSet_execute_params());
          // All the EXECUTE functions operate only on Iceberg data.
          needsToUpdateHms = false;
          TAlterTableExecuteParams setExecuteParams = params.getSet_execute_params();
          if (setExecuteParams.isSetExecute_rollback_params()) {
            String rollbackSummary = IcebergCatalogOpExecutor.alterTableExecuteRollback(
                iceTxn, tbl, setExecuteParams.getExecute_rollback_params());
            addSummary(response, rollbackSummary);
          } else if (setExecuteParams.isSetExpire_snapshots_params()) {
            String expireSummary =
                IcebergCatalogOpExecutor.alterTableExecuteExpireSnapshots(
                    iceTxn, setExecuteParams.getExpire_snapshots_params());
            addSummary(response, expireSummary);
          } else {
            // Cannot happen, but throw just in case.
            throw new IllegalStateException(
                "Alter table execute statement is not implemented.");
          }
          break;
        case SET_PARTITION_SPEC:
          // Partition spec is not stored in HMS.
          needsToUpdateHms = false;
          TAlterTableSetPartitionSpecParams setPartSpecParams =
              params.getSet_partition_spec_params();
          IcebergCatalogOpExecutor.alterTableSetPartitionSpec(tbl,
              setPartSpecParams.getPartition_spec(), iceTxn);
          addSummary(response, "Updated partition spec.");
          break;
        case SET_TBL_PROPERTIES:
          needsToUpdateHms |= !setIcebergTblProperties(tbl, params, iceTxn);
          addSummary(response, "Updated table.");
          break;
        case UNSET_TBL_PROPERTIES:
          needsToUpdateHms |= !unsetIcebergTblProperties(tbl, params, iceTxn);
          addSummary(response, "Updated table.");
          break;
        case DROP_PARTITION:
          // Metadata change only
          needsToUpdateHms = false;
          long droppedPartitions = IcebergCatalogOpExecutor.alterTableDropPartition(
              iceTxn, params.getDrop_partition_params());
          addSummary(
              response, String.format("Dropped %d partition(s)", droppedPartitions));
          break;
        case REPLACE_COLUMNS:
          // It doesn't make sense to replace all the columns of an Iceberg table as it
          // would basically make all existing data inaccessible.
        default:
          throw new UnsupportedOperationException(
              "Unsupported ALTER TABLE operation for Iceberg tables: " +
              params.getAlter_type());
      }
      catalogTimeline.markEvent("Iceberg operations are prepared for commit");
      if (!needsToUpdateHms) {
        // registerInflightEvent() before committing transaction.
        modification.registerInflightEvent();
        IcebergCatalogOpExecutor.addCatalogVersionToTxn(
            iceTxn, catalog_.getCatalogServiceId(), modification.newVersionNumber());
      }
      if (debugAction != null) {
        DebugUtils.executeDebugAction(debugAction, DebugUtils.ICEBERG_COMMIT);
      }
      iceTxn.commitTransaction();
      catalogTimeline.markEvent("Committed Iceberg transaction");
      if (!needsToUpdateHms) modification.markInflightEventRegistrationComplete();
    } catch (IllegalArgumentException | ValidationException ex) {
      modification.cancelInflightEventIfExist();
      throw new ImpalaRuntimeException(String.format(
          "Failed to ALTER table '%s': %s", params.getTable_name().table_name,
          ex.getMessage()));
    }

    if (!needsToUpdateHms) {
      // We don't need to update HMS because either it is already done by Iceberg's
      // HiveCatalog, or we modified the Iceberg data which is not stored in HMS.
      loadTableMetadata(tbl, modification.newVersionNumber(), true, true,
          "ALTER Iceberg TABLE " + params.getAlter_type().name(), debugAction,
          catalogTimeline);
      addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
    }
    return needsToUpdateHms;
  }

  /**
   * Sets table properties for an Iceberg table. Returns true on success, returns false
   * if the operation is not applicable at the Iceberg table level, e.g. setting SERDE
   * properties.
   */
  private boolean setIcebergTblProperties(IcebergTable tbl, TAlterTableParams params,
      org.apache.iceberg.Transaction iceTxn) throws ImpalaException {
    TAlterTableSetTblPropertiesParams setPropsParams =
        params.getSet_tbl_properties_params();
    if (setPropsParams.getTarget() != TTablePropertyType.TBL_PROPERTY) return false;

    addMergeOnReadPropertiesIfNeeded(tbl, setPropsParams.getProperties());
    IcebergCatalogOpExecutor.setTblProperties(iceTxn, setPropsParams.getProperties());
    return true;
  }

  /**
   * Iceberg format from V2 supports row-level modifications. We set write modes to
   * "merge-on-read" which is the write mode Impala will eventually
   * support (IMPALA-11664). Unless the user specified otherwise in the table properties.
   */
  private void addMergeOnReadPropertiesIfNeeded(IcebergTable tbl,
      Map<String, String> properties) {
    String formatVersion = properties.get(TableProperties.FORMAT_VERSION);
    if (formatVersion == null ||
        Integer.valueOf(formatVersion) < IcebergTable.ICEBERG_FORMAT_V2) {
      return;
    }
    if (!IcebergUtil.isAnyWriteModeSet(properties) &&
        !IcebergUtil.isAnyWriteModeSet(tbl.getMetaStoreTable().getParameters())) {
      final String MERGE_ON_READ = IcebergTable.MERGE_ON_READ;
      properties.put(TableProperties.DELETE_MODE, MERGE_ON_READ);
      properties.put(TableProperties.UPDATE_MODE, MERGE_ON_READ);
      properties.put(TableProperties.MERGE_MODE, MERGE_ON_READ);
    }
  }

  /**
   * Unsets table properties for an Iceberg table. Returns true on success, returns false
   * if the operation is not applicable at the Iceberg table level, e.g. setting SERDE
   * properties.
   */
  private boolean unsetIcebergTblProperties(IcebergTable tbl, TAlterTableParams params,
      org.apache.iceberg.Transaction iceTxn) throws ImpalaException {
    TAlterTableUnSetTblPropertiesParams unsetParams =
        params.getUnset_tbl_properties_params();
    if (unsetParams.getTarget() != TTablePropertyType.TBL_PROPERTY) return false;
    IcebergCatalogOpExecutor.unsetTblProperties(iceTxn, unsetParams.getProperty_keys());
    return true;
  }

  /**
   * Loads the metadata of a table 'tbl' and assigns a new catalog version.
   * 'reloadFileMetadata' and 'reloadTableSchema'
   * are used only for HdfsTables and control which metadata to reload.
   * Throws a CatalogException if there is an error loading table metadata.
   */
  private void loadTableMetadata(Table tbl, long newCatalogVersion,
      boolean reloadFileMetadata, boolean reloadTableSchema, String reason,
      @Nullable String debugAction, EventSequence catalogTimeline)
      throws CatalogException {
    loadTableMetadata(tbl, newCatalogVersion, reloadFileMetadata, reloadTableSchema, null,
        null, reason, debugAction, catalogTimeline);
  }

  /**
   * Same as the above method but takes in a Set of partition name to update and
   * Map of partition name to event id which is passed down to the table load method.
   */
  private void loadTableMetadata(Table tbl, long newCatalogVersion,
      boolean reloadFileMetadata, boolean reloadTableSchema,
      @Nullable Set<String> partitionsToUpdate,
      @Nullable Map<String, Long> partitionToEventId, String reason,
      @Nullable String debugAction, EventSequence catalogTimeline)
      throws CatalogException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    long eventId = -1L;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          getMetaStoreTable(msClient, tbl);

      catalogTimeline.markEvent(FETCHED_HMS_TABLE);
      if (debugAction != null) {
        DebugUtils.executeDebugAction(debugAction, DebugUtils.LOAD_METADATA_DELAY);
      }
      if (msTbl.getPartitionKeysSize() == 0) {
        eventId = MetastoreEventsProcessor.getCurrentEventIdNoThrow(
            msClient.getHiveClient());
        catalogTimeline.markEvent(FETCHED_LATEST_HMS_EVENT_ID + eventId);
      }
      if (tbl instanceof HdfsTable) {
        ((HdfsTable) tbl).load(true, msClient.getHiveClient(), msTbl,
            reloadFileMetadata, reloadTableSchema, false, partitionsToUpdate,
            debugAction, partitionToEventId, reason, catalogTimeline);
      } else {
        tbl.load(true, msClient.getHiveClient(), msTbl, reason, catalogTimeline);
      }
      // Update the lastRefreshEventId at the table level if it is unpartitioned table
      // if it is partitioned table, partitions are updated in HdfsTable#load() method
      if (msTbl.getPartitionKeysSize() == 0 && eventId > tbl.getLastRefreshEventId()
          && reloadFileMetadata && reloadTableSchema) {
        tbl.setLastRefreshEventId(eventId);
      }
      catalogTimeline.markEvent("Reloaded table metadata");
    }
    tbl.setCatalogVersion(newCatalogVersion);
  }

  /**
   * Serializes and adds table 'tbl' to a TCatalogUpdateResult object. Uses the
   * version of the serialized table as the version of the catalog update result.
   */
  private static void addTableToCatalogUpdate(Table tbl, boolean wantMinimalResult,
      TCatalogUpdateResult result) {
    Preconditions.checkNotNull(tbl);
    // TODO(IMPALA-9937): if client is a 'v1' impalad, only send back incremental updates
    TCatalogObject updatedCatalogObject = wantMinimalResult ?
        tbl.toInvalidationObject() : tbl.toTCatalogObject();
    result.addToUpdated_catalog_objects(updatedCatalogObject);
    result.setVersion(updatedCatalogObject.getCatalog_version());
  }

  private Table addHdfsPartitions(MetaStoreClient msClient, Table tbl,
      List<Partition> addedPartitions, Map<String, Long> partitionToEventId,
      EventSequence catalogTimeline) throws CatalogException {
    Preconditions.checkNotNull(tbl);
    Preconditions.checkNotNull(addedPartitions);
    if (!(tbl instanceof HdfsTable)) {
      throw new CatalogException("Table " + tbl.getFullName() + " is not an HDFS table");
    }
    HdfsTable hdfsTable = (HdfsTable) tbl;
    List<HdfsPartition> hdfsPartitions = hdfsTable.createAndLoadPartitions(
        msClient.getHiveClient(), addedPartitions, partitionToEventId, catalogTimeline);
    for (HdfsPartition hdfsPartition : hdfsPartitions) {
      catalog_.addPartition(hdfsPartition);
    }
    return hdfsTable;
  }

  /**
   * Alters an existing view's definition in the metastore. Throws an exception
   * if the view does not exist or if the existing metadata entry is
   * a table instead of a a view.
   */
   private void alterView(TCreateOrAlterViewParams params, boolean wantMinimalResult,
       TDdlExecResponse resp, EventSequence catalogTimeline) throws ImpalaException {
    TableName tableName = TableName.fromThrift(params.getView_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
        params.getColumns().size() > 0,
          "Null or empty column list given as argument to DdlExecutor.alterView");
    Table tbl = getExistingTable(tableName.getDb(), tableName.getTbl(),
        "Load for ALTER VIEW", catalogTimeline);
    Preconditions.checkState(tbl instanceof View, "Expected view: %s",
        tableName);
    tryWriteLock(tbl, catalogTimeline);
    try {
      InProgressTableModification modification =
          new InProgressTableModification(catalog_, tbl);
      catalog_.getLock().writeLock().unlock();
      modification.addCatalogServiceIdentifiersToTable();
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
      // In-flight event registration needed here. As for a view, reloading the HMS object
      // is a full reload. Thus, we can safely set setLastRefreshEventId below, which will
      // lead to ignoring the event.
      applyAlterTable(msTbl, true, null, catalogTimeline);
      long eventId = -1L;
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
        eventId = MetastoreEventsProcessor.getCurrentEventIdNoThrow(
            msClient.getHiveClient());
        tbl.load(true, msClient.getHiveClient(), msTbl, "ALTER VIEW", catalogTimeline);
      }
      addSummary(resp, "View has been altered.");
      modification.updateTableCatalogVersion();
      // Update the last refresh event id at table level
      if (eventId > tbl.getLastRefreshEventId()) {
        tbl.setLastRefreshEventId(eventId);
      }
      addTableToCatalogUpdate(tbl, wantMinimalResult, resp.result);
      modification.validateInProgressModificationComplete();
    } finally {
      UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  private void addCatalogServiceIdentifiers(
      org.apache.hadoop.hive.metastore.api.Table msTbl, String catalogServiceId,
      long catalogVersion) {
    if (!catalog_.isEventProcessingActive()) return;
    msTbl.putToParameters(
        MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(),
        catalogServiceId);
    msTbl.putToParameters(
        MetastoreEventPropertyKey.CATALOG_VERSION.getKey(),
        String.valueOf(catalogVersion));
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
      Reference<Long> numUpdatedPartitions, Reference<Long> numUpdatedColumns,
      @Nullable String debugAction, EventSequence catalogTimeline,
      InProgressTableModification modification) throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
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

    // Deep copy the msTbl to avoid updating our cache before successfully persisting
    // the results to the metastore.
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        table.getMetaStoreTable().deepCopy();

    // TODO: Transaction committing / aborting seems weird for stat update, but I don't
    //       see other ways to get a new write id (which is needed to update
    //       transactional tables). Hive seems to use internal API for this.
    //       See IMPALA-8865 about plans to improve this.
    TblTransaction tblTxn = null;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      modification.registerInflightEvent();
      try {
        if (AcidUtils.isTransactionalTable(msTbl.getParameters())) {
          tblTxn = MetastoreShim.createTblTransaction(
              msClient.getHiveClient(), msTbl, -1 /* opens new transaction */);
          catalogTimeline.markEvent("Opened new transaction");
        }
        alterTableUpdateStatsInner(table, msTbl, params,
            numUpdatedPartitions, numUpdatedColumns, msClient, tblTxn, catalogTimeline);
        if (tblTxn != null) {
          MetastoreShim.commitTblTransactionIfNeeded(msClient.getHiveClient(), tblTxn);
          catalogTimeline.markEvent("Committed transaction");
        }
      } catch (Exception ex) {
        modification.cancelInflightEventIfExist();
        if (tblTxn != null) {
          MetastoreShim.abortTblTransactionIfNeeded(msClient.getHiveClient(), tblTxn);
          catalogTimeline.markEvent("Aborted transaction");
        }
        throw ex;
      }
      modification.markInflightEventRegistrationComplete();
    }
    DebugUtils.executeDebugAction(debugAction, DebugUtils.UPDATE_STATS_DELAY);
  }

  private void alterTableUpdateStatsInner(Table table,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      TAlterTableUpdateStatsParams params,
      Reference<Long> numUpdatedPartitions, Reference<Long> numUpdatedColumns,
      MetaStoreClient msClient, TblTransaction tblTxn, EventSequence catalogTimeline)
      throws ImpalaException {
    // Update column stats.
    numUpdatedColumns.setRef(0L);
    if (params.isSetColumn_stats()) {
      ColumnStatistics colStats = createHiveColStats(params, table);
      if (colStats.getStatsObjSize() > 0) {
        if (tblTxn != null) {
          MetastoreShim.setTableColumnStatsTransactional(
              msClient.getHiveClient(), msTbl, colStats, tblTxn);
        } else {
          try {
            msClient.getHiveClient().updateTableColumnStatistics(colStats);
          } catch (Exception e) {
            throw new ImpalaRuntimeException(String.format(HMS_RPC_ERROR_FORMAT_STR,
                "updateTableColumnStatistics"), e);
          }
        }
        catalogTimeline.markEvent("Updated column stats");
      }
      numUpdatedColumns.setRef((long) colStats.getStatsObjSize());
    }

    // Update partition-level row counts and incremental column stats for
    // partitioned Hdfs tables.
    List<HdfsPartition.Builder> modifiedParts = null;
    if (params.isSetPartition_stats() && table.getNumClusteringCols() > 0) {
      Preconditions.checkState(table instanceof HdfsTable);
      modifiedParts = updatePartitionStats(params, (HdfsTable) table);
      // TODO: IMPALA-10203: avoid reloading modified partitions when updating stats.
      // in-flight event registration is handled by alterTableUpdateStats().
      bulkAlterPartitions(table, modifiedParts, tblTxn, UpdatePartitionMethod.MARK_DIRTY,
          catalogTimeline);
    }

    if (params.isSetTable_stats()) {
      // Update table row count and total file bytes.
      updateTableStats(params, msTbl);
      // Set impala.lastComputeStatsTime just before alter_table to ensure that it is as
      // accurate as possible.
      Table.updateTimestampProperty(msTbl, HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME);
    }

    if (IcebergTable.isIcebergTable(msTbl) && isIcebergHmsIntegrationEnabled(msTbl)) {
      updateTableStatsViaIceberg((IcebergTable)table, msTbl);
      catalogTimeline.markEvent("Updated table stats via Iceberg");
    } else {
      // Apply property changes like numRows.
      msTbl.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
      applyAlterTable(msTbl, false, tblTxn, catalogTimeline);
    }
    numUpdatedPartitions.setRef(0L);
    if (modifiedParts != null) {
      numUpdatedPartitions.setRef((long) modifiedParts.size());
    } else if (params.isSetTable_stats()) {
      numUpdatedPartitions.setRef(1L);
    }
  }

  /**
   * For Iceberg tables using HiveCatalog we must avoid updating the HMS table directly to
   * avoid overriding concurrent modifications to the table. See IMPALA-11583.
   * Table-level stats (numRows, totalSize) should not be set as Iceberg keeps them
   * up-to-date.
   * 'impala.lastComputeStatsTime' still needs to be set, so we'll know when we executed
   * COMPUTE STATS the last time.
   * We need to set catalog service id and catalog version to detect self-events.
   */
  private void updateTableStatsViaIceberg(IcebergTable iceTbl,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws ImpalaException {
    String CATALOG_SERVICE_ID = MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey();
    String CATALOG_VERSION    = MetastoreEventPropertyKey.CATALOG_VERSION.getKey();
    String COMPUTE_STATS_TIME = HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME;

    Preconditions.checkState(msTbl.getParameters().containsKey(CATALOG_SERVICE_ID));
    Preconditions.checkState(msTbl.getParameters().containsKey(CATALOG_VERSION));

    Map<String, String> props = new HashMap<>();
    props.put(CATALOG_SERVICE_ID, msTbl.getParameters().get(CATALOG_SERVICE_ID));
    props.put(CATALOG_VERSION,    msTbl.getParameters().get(CATALOG_VERSION));
    if (msTbl.getParameters().containsKey(COMPUTE_STATS_TIME)) {
      props.put(COMPUTE_STATS_TIME, msTbl.getParameters().get(COMPUTE_STATS_TIME));
    }

    org.apache.iceberg.Transaction iceTxn = IcebergUtil.getIcebergTransaction(iceTbl);
    IcebergCatalogOpExecutor.setTblProperties(iceTxn, props);
    iceTxn.commitTransaction();
  }


  /**
   * Updates the row counts and incremental column stats of the partitions in the given
   * Impala table based on the given update stats parameters. Returns the modified Impala
   * partitions.
   * Row counts for missing or new partitions as a result of concurrent table alterations
   * are set to 0.
   */
  private List<HdfsPartition.Builder> updatePartitionStats(
      TAlterTableUpdateStatsParams params, HdfsTable table) throws ImpalaException {
    Preconditions.checkState(params.isSetPartition_stats());
    List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
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

      // Update the partition in HMS only if something has changed.
      // Note that previously all partitions were updated to avoid an HMS bug.
      // See IMPALA-2201.
      boolean updatedPartition = false;
      TPartitionStats existingPartStats = partition.getPartitionStats();
      // Update the partition stats if: either there are no existing stats for this
      // partition, or they're different.
      if (existingPartStats == null || !existingPartStats.equals(partitionStats)) {
        updatedPartition = true;
      }

      long numRows = partitionStats.stats.num_rows;
      String existingRowCount =
          partition.getParameters().get(StatsSetupConst.ROW_COUNT);
      if (existingRowCount == null ||
          !existingRowCount.equals(String.valueOf(numRows))) {
        // The existing row count value wasn't set or has changed.
        updatedPartition = true;
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("{} stats for partition {}: numRows={}",
            updatedPartition ? "Updating" : "Skip updating",
            partition.getValuesAsString(), numRows);
      }

      if (!updatedPartition) continue;

      HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
      PartitionStatsUtil.partStatsToPartition(partitionStats, partBuilder);
      partBuilder.setRowCountParam(numRows);
      // HMS requires this param for stats changes to take effect.
      partBuilder.putToParameters(MetastoreShim.statsGeneratedViaStatsTaskParam());
      partBuilder.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
      modifiedParts.add(partBuilder);
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
    ColumnStatistics colStats = MetastoreShim.createNewHiveColStats();
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
        LOG.trace(String.format("Updating column stats for %s: numDVs=%d numNulls=%d "
                + "maxSize=%d avgSize=%.2f minValue=%s maxValue=%s",
            colName, entry.getValue().getNum_distinct_values(),
            entry.getValue().getNum_nulls(), entry.getValue().getMax_size(),
            entry.getValue().getAvg_size(), entry.getValue().getLow_value() != null ?
            entry.getValue().getLow_value().toString() : -1,
            entry.getValue().getHigh_value() != null ?
            entry.getValue().getHigh_value().toString() : -1));
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
   * @param  syncDdl tells if SYNC_DDL option is enabled on this DDL request.
   */
  private void createDatabase(TCreateDbParams params, TDdlExecResponse resp,
      EventSequence catalogTimeline, boolean syncDdl, boolean wantMinimalResult)
      throws ImpalaException {
    Preconditions.checkNotNull(params);
    String dbName = params.getDb();
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name passed as argument to Catalog.createDatabase");
    Preconditions.checkState(!catalog_.isBlacklistedDb(dbName),
        String.format("Can't create blacklisted database: %s. %s", dbName,
            BLACKLISTED_DBS_INCONSISTENT_ERR_STR));
    Db existingDb = catalog_.getDb(dbName);
    if (params.if_not_exists && existingDb != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Skipping database creation because " + dbName + " already exists "
            + "and IF NOT EXISTS was specified.");
      }
      Preconditions.checkNotNull(existingDb);
      if (syncDdl) {
        tryLock(existingDb, "create database", catalogTimeline);
        try {
          // When SYNC_DDL is enabled and the database already exists, we force a version
          // bump on it so that it is added to the next statestore update. Without this
          // we could potentially be referring to a database object that has already been
          // GC'ed from the TopicUpdateLog and waitForSyncDdlVersion() cannot find a
          // covering topic version (IMPALA-7961).
          //
          // This is a conservative hack to not break the SYNC_DDL semantics and could
          // possibly result in false-positive invalidates on this database. However,
          // that is better than breaking the SYNC_DDL semantics and the subsequent
          // queries referring to this database failing with "database not found" errors.
          long newVersion = catalog_.incrementAndGetCatalogVersion();
          existingDb.setCatalogVersion(newVersion);
          LOG.trace("Database {} version bumped to {} because SYNC_DDL is enabled.",
              dbName, newVersion);
        } finally {
          // Release the locks held in tryLock().
          catalog_.getLock().writeLock().unlock();
          existingDb.getLock().unlock();
        }
      }
      addDbToCatalogUpdate(existingDb, wantMinimalResult, resp.result);
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
    if (params.getManaged_location() != null) {
      MetastoreShim.setManagedLocationUri(db, params.getManaged_location());
    }
    db.setOwnerName(params.getOwner());
    db.setOwnerType(PrincipalType.USER);
    if (LOG.isTraceEnabled()) LOG.trace("Creating database " + dbName);
    Db newDb = null;
    acquireMetastoreDdlLock(catalogTimeline);
    try {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
        try {
          long eventId = getCurrentEventId(msClient, catalogTimeline);
          msClient.getHiveClient().createDatabase(db);
          catalogTimeline.markEvent("Created database in Metastore");
          List<NotificationEvent> events = getNextMetastoreEventsForDbIfEnabled(
              catalogTimeline, eventId, dbName, CreateDatabaseEvent.EVENT_TYPE);
          Pair<Long, Database> eventDbPair = getDatabaseFromEvents(events,
              params.if_not_exists);
          if (eventDbPair == null) {
            // if events processor is not turned on we get it from HMS.
            // Load the database back from the HMS. It's unfortunate we need two
            // RPCs here, but otherwise we can't populate the location field of the
            // DB properly. We'll take the slight chance of a race over the incorrect
            // behavior of showing no location in 'describe database' (IMPALA-7439).
            eventDbPair = new Pair<>(-1L, msClient.getHiveClient().getDatabase(dbName));
          } else {
            // Due to HIVE-24899 we cannot rely on the database object present in the
            // event which may or may not include the managed location uri. Once
            // HIVE-24899 is fixed we can rely on using the Database object from the event
            // directly and avoid this extra HMS call.
            eventDbPair.second = msClient.getHiveClient().getDatabase(dbName);
          }
          newDb = catalog_.addDb(dbName, eventDbPair.second, eventDbPair.first);
          catalogTimeline.markEvent("Created database in catalog cache");
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
              catalogTimeline.markEvent("Fetched database from Metastore");
              newDb = catalog_.addDb(dbName, msDb);
              catalogTimeline.markEvent("Created database in catalog cache");
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

      addDbToCatalogUpdate(newDb, wantMinimalResult, resp.result);
      if (authzConfig_.isEnabled()) {
        authzManager_.updateDatabaseOwnerPrivilege(params.server_name, newDb.getName(),
            /* oldOwner */ null, /* oldOwnerType */ null,
            newDb.getMetaStoreDb().getOwnerName(), newDb.getMetaStoreDb().getOwnerType(),
            resp);
      }
    } finally {
      getMetastoreDdlLock().unlock();
    }
  }

  /**
   * Wrapper around
   * {@code MetastoreEventsProcessor#getNextMetastoreEventsInBatchesForTable} with the
   * addition that it checks if events processing is active or not. If not active,
   * returns an empty list. Also updates the given 'catalogTimeline'.
   */
  private List<NotificationEvent> getNextMetastoreEventsForTableIfEnabled(
      EventSequence catalogTimeline, long eventId, String dbName, String tblName,
      String eventType) throws MetastoreNotificationException {
    if (!catalog_.isEventProcessingActive()) return Collections.emptyList();
    List<NotificationEvent> events = MetastoreEventsProcessor
        .getNextMetastoreEventsInBatchesForTable(catalog_, eventId, dbName, tblName,
            eventType);
    catalogTimeline.markEvent(FETCHED_HMS_EVENT_BATCH);
    return events;
  }

  /**
   * Wrapper around
   * {@code MetastoreEventsProcessor#getNextMetastoreEventsInBatchesForDb} with the
   * addition that it checks if events processing is active or not. If not active,
   * returns an empty list. Also updates the given 'catalogTimeline'.
   */
  private List<NotificationEvent> getNextMetastoreEventsForDbIfEnabled(
      EventSequence catalogTimeline, long eventId, String dbName, String eventType)
      throws MetastoreNotificationException {
    if (!catalog_.isEventProcessingActive()) return Collections.emptyList();
    List<NotificationEvent> events = MetastoreEventsProcessor
        .getNextMetastoreEventsInBatchesForDb(catalog_, eventId, dbName, eventType);
    catalogTimeline.markEvent(FETCHED_HMS_EVENT_BATCH);
    return events;
  }

  /**
   * Fetches CreateDatabase and CreateTable events of a db if events processing is active.
   * Returns an empty list if not active. Also updates the given 'catalogTimeline'.
   */
  private List<NotificationEvent> getNextMetastoreDropEventsForDbIfEnabled(
      EventSequence catalogTimeline, long eventId, String dbName)
      throws MetastoreNotificationException {
    if (!catalog_.isEventProcessingActive()) return Collections.emptyList();
    List<String> eventTypes = Lists.newArrayList(
        DropDatabaseEvent.EVENT_TYPE, DropTableEvent.EVENT_TYPE);
    NotificationFilter filter = e -> dbName.equalsIgnoreCase(e.getDbName())
        && MetastoreShim.isDefaultCatalog(e.getCatName())
        && eventTypes.contains(e.getEventType());
    List<NotificationEvent> events = MetastoreEventsProcessor
        .getNextMetastoreEventsInBatches(catalog_, eventId, filter,
            DropDatabaseEvent.EVENT_TYPE, DropTableEvent.EVENT_TYPE);
    catalogTimeline.markEvent(FETCHED_HMS_EVENT_BATCH);
    return events;
  }

  /**
   * Extracts the database object from the {@link CreateDatabaseEvent} present in the
   * events.
   * @param events The metastore events which are already filtered for CREATE_DATABASE
   *               event type and for the right database name.
   * @param useLatestEvent If useLatestEvent is true we don't care if there is only one
   *                       event events or not. We just use the latest event id. This is
   *                       used when the create database was created with if not exists
   *                       clause.
   * @return Pair of eventId and the database object from the events.
   * @throws CatalogException If the database could not be parsed from the events.
   */
  private Pair<Long, Database> getDatabaseFromEvents(List<NotificationEvent> events,
      boolean useLatestEvent) throws CatalogException {
    if (events == null || events.isEmpty()) return null;
    // this means that the database was recreated from some other application while
    // this create database operation was in progress. We bail out by throwing an error
    // in this case because it is possible the database which the user was trying to
    // create was not the one which was eventually recreated in the metastore.
    Preconditions.checkState(useLatestEvent || events.size() == 1,
        "Database was recreated in metastore while "
            + "createDatabase operation was in progress");
    try {
      MetastoreEvent event = catalog_
          .getMetastoreEventProcessor().getEventsFactory()
          .get(events.get(events.size() - 1), null);
      Preconditions.checkState(event instanceof CreateDatabaseEvent,
          "Expects CreateDatabaseEvent but got %s. All events: %s",
          event, events);
      return new Pair<>(events.get(0).getEventId(),
          ((CreateDatabaseEvent) event).getDatabase());
    } catch (MetastoreNotificationException e) {
      throw new CatalogException("Unable to create a metastore event ", e);
    }
  }

  /**
   * Similar to {@code getDatabaseFromEvents} but finds the table instead of database
   * by parsing a CREATE_TABLE event.
   * @param events Filtered list of events of the type CREATE_TABLE and for the correct
   *               table name.
   * @param useLatestEvent if this flag is set then we use the latest event otherwise
   *                       we make sure that there are only one events. This is used
   *                       when the table is create with if not exists clause.
   * @return Pair of eventId and the table object from the event.
   * @throws CatalogException
   */
  private Pair<Long, org.apache.hadoop.hive.metastore.api.Table> getTableFromEvents(
      List<NotificationEvent> events, boolean useLatestEvent) throws CatalogException {
    if (events == null || events.isEmpty()) return null;
    // we bail out by throwing an error here because if the table has been recreated
    // from another application while this create table was in progress, it is possible
    // that the schema is different than what user was trying to create.
    Preconditions.checkState(useLatestEvent || events.size() == 1,
        "Table was recreated in metastore while createTable operation "
            + "was in progress.");
    try {
      MetastoreEvent event = catalog_
          .getMetastoreEventProcessor().getEventsFactory()
          .get(events.get(events.size() - 1), null);
      Preconditions.checkState(event instanceof CreateTableEvent);
      return new Pair<>(events.get(0).getEventId(),
          ((CreateTableEvent) event).getTable());
    } catch (MetastoreNotificationException e) {
      throw new CatalogException("Unable to create a metastore event", e);
    }
  }

  /**
   * Processing the given list of events which are prefiltered appropriate to include
   * on the ALTER_TABLE event types on the target table name.
   * @param events list of events which are already filtered for ALTER_TABLE type
   *               and on renamed table names.
   * @return Pair of eventId and the table object from the event which pertain to the
   * rename event. If events processing is not active or if rename event is not found
   * returns null.
   * @throws CatalogException if the event was found but could not be parsed.
   */
  private Pair<Long, Pair<org.apache.hadoop.hive.metastore.api.Table,
      org.apache.hadoop.hive.metastore.api.Table>> getRenamedTableFromEvents(
      List<NotificationEvent> events) throws CatalogException {
    if (events == null || events.isEmpty()) return null;
    for (NotificationEvent notificationEvent : events) {
      try {
        MetastoreEvent event = catalog_
            .getMetastoreEventProcessor().getEventsFactory().get(notificationEvent, null);
        Preconditions.checkState(event instanceof AlterTableEvent);
        AlterTableEvent alterEvent = (AlterTableEvent) event;
        if (!alterEvent.isRename()) continue;
        return new Pair<>(events.get(0).getEventId(),
            new Pair<>(alterEvent.getBeforeTable(), alterEvent.getAfterTable()));
      } catch (MetastoreNotificationException e) {
        throw new CatalogException("Unable to create a metastore event", e);
      }
    }
    return null;
  }

  /**
   * Processes the list of events which contain of the events of the type ADD_PARTITION
   * and on the target table. The method then extracts the partition objects from the
   * events and adds to the partitionToEventId along with the event Id which added that
   * partition in the metastore.
   * @param events Events which are pre-filtered by type (ADD_PARTITION) and on the target
   *               table.
   * @param partitionToEventId Map of Partition to the eventId which is populated by
   *                           this method.
   * @throws CatalogException If the event information cannot be parsed.
   */
  private void getPartitionsFromEvent(
      List<NotificationEvent> events, Map<Partition, Long> partitionToEventId)
      throws CatalogException {
    if (events == null || events.isEmpty()) return;
    for (NotificationEvent event : events) {
      try {
        MetastoreEvent metastoreEvent = catalog_
            .getMetastoreEventProcessor().getEventsFactory().get(event, null);
        Preconditions.checkState(metastoreEvent instanceof AddPartitionEvent);
        Long eventId = metastoreEvent.getEventId();
        for (Partition part : ((AddPartitionEvent) metastoreEvent).getPartitions()) {
          partitionToEventId.put(part, eventId);
        }
      } catch (MetastoreNotificationException e) {
        throw new CatalogException("Unable to create a metastore event", e);
      }
    }
  }

  /**
   *
   * @param partColNames The partition column names of the table whose partitions were
   *                     dropped.
   * @param events The pre-filtered list of events which contain events of the type
   *               DROP_PARTITION and on the target table.
   * @param eventIdToPartVals Map of eventId to a list of list of partition values. The
   *                          map is populated by this method to include a mapping of the
   *                          eventId to the partition values from the event.
   * @throws CatalogException If the event cannot be parsed.
   */
  private void addDroppedPartitionsFromEvent(
      List<String> partColNames, List<NotificationEvent> events,
      Map<Long, List<List<String>>> eventIdToPartVals) throws CatalogException {
    if (events == null || events.isEmpty()) return;
    // in case of DROP partitions, catalog drops the partitions one by one
    // eventId to list of partition names in the event which are dropped.
    for (NotificationEvent notificationEvent : events) {
      try {
        MetastoreEvent event = catalog_
            .getMetastoreEventProcessor().getEventsFactory().get(notificationEvent, null);
        Preconditions.checkState(event instanceof DropPartitionEvent);
        Long eventId = notificationEvent.getEventId();
        List<Map<String, String>> droppedPartitions = ((DropPartitionEvent) event)
            .getDroppedPartitions();
        // it is important that we create the partition key in the order of partition cols
        for (Map<String, String> partKeyVals : droppedPartitions) {
          List<String> partVals = Lists.newArrayList();
          for (String partColName : partColNames) {
            String val = Preconditions.checkNotNull(partKeyVals.get(partColName));
            partVals.add(val);
          }
          eventIdToPartVals.computeIfAbsent(eventId, l -> new ArrayList<>())
              .add(partVals);
        }
      } catch (MetastoreNotificationException e) {
        throw new CatalogException("Unable to create a metastore event", e);
      }
    }
  }

  /**
   * Returns the latest notification event id from the Hive metastore.
   */
  private long getCurrentEventId(MetaStoreClient msClient) throws ImpalaRuntimeException {
    return getCurrentEventId(msClient, null);
  }

  /**
   * Same as the above but also update the given 'catalogTimeline'.
   */
  private long getCurrentEventId(MetaStoreClient msClient, EventSequence catalogTimeline)
      throws ImpalaRuntimeException {
    try {
      long id = msClient.getHiveClient().getCurrentNotificationEventId().getEventId();
      if (catalogTimeline != null) {
        catalogTimeline.markEvent(FETCHED_LATEST_HMS_EVENT_ID + id);
      }
      return id;
    } catch (TException e) {
      throw new ImpalaRuntimeException(String.format(HMS_RPC_ERROR_FORMAT_STR,
          "getCurrentNotificationEventId") + e
          .getMessage());
    }
  }

  private void createFunction(TCreateFunctionParams params, TDdlExecResponse resp,
      EventSequence catalogTimeline) throws ImpalaException {
    Function fn = Function.fromThrift(params.getFn());
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Adding %s: %s",
          fn.getClass().getSimpleName(), fn.signatureString()));
    }
    boolean isPersistentJavaFn =
        (fn.getBinaryType() == TFunctionBinaryType.JAVA) && fn.isPersistent();
    HiveJavaFunction hiveJavaFunction = (fn.getBinaryType() == TFunctionBinaryType.JAVA) ?
        hiveJavaFuncFactory_.create((ScalarFunction) fn) :
        null;
    Db db = catalog_.getDb(fn.dbName());
    if (db == null) {
      throw new CatalogException("Database: " + fn.dbName() + " does not exist.");
    }

    tryLock(db, "creating function " + fn.getClass().getSimpleName(), catalogTimeline);
    // Get a new catalog version to assign to the database being altered. This is
    // needed for events processor as this method creates alter database events.
    long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
    try {
      // Search for existing functions with the same name or signature that would
      // conflict with the function being added.
      for (Function function : db.getFunctions(fn.functionName())) {
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
        List<ScalarFunction> funcs = hiveJavaFunction.extract();
        if (addJavaFunctionToHms(hiveJavaFunction.getHiveFunction(),
            params.if_not_exists, catalogTimeline)) {
          for (Function addedFn : funcs) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(String.format("Adding function: %s.%s", addedFn.dbName(),
                  addedFn.signatureString()));
            }
            Preconditions.checkState(catalog_.addFunction(addedFn));
            addedFunctions.add(addedFn.toTCatalogObject());
          }
        }
      } else {
        //TODO(Vihang): addFunction method below directly updates the database
        // parameters. If the applyAlterDatabase method below throws an exception,
        // catalog might end up in a inconsistent state. Ideally, we should make a copy
        // of hms Database object and then update the Db once the HMS operation succeeds
        // similar to what happens in alterDatabaseSetOwner method.
        if (catalog_.addFunction(fn)) {
          addCatalogServiceIdentifiers(db.getMetaStoreDb(),
              catalog_.getCatalogServiceId(), newCatalogVersion);
          // Flush DB changes to metastore
          applyAlterDatabase(db.getMetaStoreDb(), catalogTimeline);
          addedFunctions.add(fn.toTCatalogObject());
          // now that HMS alter database has succeeded, add this version to list of
          // inflight events in catalog database if event processing is enabled.
          catalog_.addVersionsForInflightEvents(db, newCatalogVersion);
        }
      }

      if (!addedFunctions.isEmpty()) {
        resp.result.setUpdated_catalog_objects(addedFunctions);
        resp.result.setVersion(catalog_.getCatalogVersion());
        addSummary(resp, "Function has been created.");
      } else {
        addSummary(resp, "Function already exists.");
      }
    } finally {
      db.getLock().unlock();
    }
  }

  private void createDataSource(TCreateDataSourceParams params, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(params);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Adding DATA SOURCE: " + params.toString());
    }
    DataSource dataSource = DataSource.fromThrift(params.getData_source());
    Preconditions.checkNotNull(dataSource);
    String dataSrcName = dataSource.getName();
    Preconditions.checkState(!Strings.isNullOrEmpty(dataSrcName),
        "Null or empty DataSource name passed as argument to " +
        "CatalogOpExecutor.createDataSource");
    DataSource existingDataSource = catalog_.getDataSource(dataSrcName);
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
    // Create DataSource object in HMS.
    addDataSourceToHms(dataSource, params.if_not_exists);
    // Add DataSource object to memory cache,
    catalog_.addDataSource(dataSource);
    resp.result.addToUpdated_catalog_objects(dataSource.toTCatalogObject());
    resp.result.setVersion(dataSource.getCatalogVersion());
    addSummary(resp, "Data source has been created.");
  }

  private void dropDataSource(TDropDataSourceParams params, TDdlExecResponse resp)
      throws ImpalaException {
    if (LOG.isTraceEnabled()) LOG.trace("Drop DATA SOURCE: " + params.toString());
    String dataSrcName = params.getData_source();
    Preconditions.checkState(!Strings.isNullOrEmpty(dataSrcName),
        "Null or empty DataSource name passed as argument to " +
        "CatalogOpExecutor.dropDataSource");
    DataSource dataSource = catalog_.removeDataSource(dataSrcName);
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
    // Drop DataSource object from HMS.
    dropDataSourceFromHms(dataSrcName, /* ifExists */ false);
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
  private void dropStats(TDropStatsParams params, boolean wantMinimalResult,
      TDdlExecResponse resp, EventSequence catalogTimeline, @Nullable String debugAction)
      throws ImpalaException {
    Table table = getExistingTable(params.getTable_name().getDb_name(),
        params.getTable_name().getTable_name(), "Load for DROP STATS", catalogTimeline);
    Preconditions.checkNotNull(table);
    // There is no transactional HMS API to drop stats at the moment (HIVE-22104).
    Preconditions.checkState(!AcidUtils.isTransactionalTable(
        table.getMetaStoreTable().getParameters()));

    tryWriteLock(table, "dropping stats", catalogTimeline);
    InProgressTableModification modification = null;
    try {
      modification = new InProgressTableModification(catalog_, table);
      catalog_.getLock().writeLock().unlock();
      modification.addCatalogServiceIdentifiersToTable();
      modification.registerInflightEvent();
      if (params.getPartition_set() == null) {
        // TODO: Report the number of updated partitions/columns to the user?
        // TODO: bulk alter the partitions.
        dropColumnStats(table, catalogTimeline);
        dropTableStats(table, catalogTimeline);
      } else {
        HdfsTable hdfsTbl = (HdfsTable) table;
        List<HdfsPartition> partitions =
            hdfsTbl.getPartitionsFromPartitionSet(params.getPartition_set());
        if (partitions.isEmpty()) {
          addSummary(resp, "No partitions found for table.");
          return;
        }

        for (HdfsPartition partition : partitions) {
          if (partition.getPartitionStatsCompressed() != null) {
            HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
            partBuilder.dropPartitionStats();
            applyAlterPartition(table, partBuilder, catalogTimeline);
            hdfsTbl.updatePartition(partBuilder);
          }
        }
      }
      loadTableMetadata(table, modification.newVersionNumber(),
          /*reloadFileMetadata=*/false,
          /*reloadTableSchema=*/true, "DROP STATS", debugAction, catalogTimeline);
      addTableToCatalogUpdate(table, wantMinimalResult, resp.result);
      addSummary(resp, "Stats have been dropped.");
      modification.markInflightEventRegistrationComplete();
      modification.validateInProgressModificationComplete();
    } catch (ImpalaException ex) {
      if (modification != null) modification.cancelInflightEventIfExist();
      throw ex;
    } finally {
      UnlockWriteLockIfErronouslyLocked();
      table.releaseWriteLock();
    }
  }

  /**
   * Drops all column stats from the table in the HMS. Returns the number of columns
   * that were updated as part of this operation.
   */
  private int dropColumnStats(Table table, EventSequence catalogTimeline)
      throws ImpalaRuntimeException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    int numColsUpdated = 0;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      for (Column col: table.getColumns()) {
        // Skip columns that don't have stats.
        if (!col.getStats().hasStats()) continue;

        try {
          MetastoreShim.deleteTableColumnStatistics(msClient.getHiveClient(),
              table.getDb().getName(), table.getName(), col.getName());
          catalogTimeline.markEvent("Deleted column statistics in Metastore");
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
  private int dropTableStats(Table table, EventSequence catalogTimeline)
      throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    // Delete the ROW_COUNT from the table (if it was set).
    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getMetaStoreTable();
    boolean isIntegratedIcebergTbl =
        IcebergTable.isIcebergTable(msTbl) && isIcebergHmsIntegrationEnabled(msTbl);
    if (isIntegratedIcebergTbl) {
      // We shouldn't modify table-level stats of HMS-integrated Iceberg tables as these
      // stats are managed by Iceberg.
      return 0;
    }
    int numTargetedPartitions = 0;
    boolean droppedRowCount =
        msTbl.getParameters().remove(StatsSetupConst.ROW_COUNT) != null;
    boolean droppedTotalSize =
        msTbl.getParameters().remove(StatsSetupConst.TOTAL_SIZE) != null;

    if (droppedRowCount || droppedTotalSize) {
      applyAlterTable(msTbl, false, null, catalogTimeline);
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
    List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
    Collection<? extends FeFsPartition> parts =
        FeCatalogUtils.loadAllPartitions(hdfsTable);
    for (FeFsPartition fePart: parts) {
      // TODO(todd): avoid downcast
      HdfsPartition part = (HdfsPartition) fePart;
      HdfsPartition.Builder partBuilder = null;
      if (part.getPartitionStatsCompressed() != null) {
        partBuilder = new HdfsPartition.Builder(part).dropPartitionStats();
      }

      // We need to update the partition if it has a ROW_COUNT parameter.
      if (part.getParameters().containsKey(StatsSetupConst.ROW_COUNT)) {
        if (partBuilder == null) {
          partBuilder = new HdfsPartition.Builder(part);
        }
        partBuilder.removeRowCountParam();
      }

      if (partBuilder != null) modifiedParts.add(partBuilder);
    }

    // in-flight event registration is handled by caller of this method.
    bulkAlterPartitions(table, modifiedParts, null, UpdatePartitionMethod.IN_PLACE,
        catalogTimeline);
    return modifiedParts.size();
  }

  /**
   * Drops a database from the metastore and removes the database's metadata from the
   * internal cache. Attempts to remove the HDFS cache directives of the underlying
   * tables. Re-throws any HMS exceptions encountered during the drop.
   */
  private void dropDatabase(TDropDbParams params, TDdlExecResponse resp,
      int kudu_table_reserve_seconds, EventSequence catalogTimeline)
      throws ImpalaException {
    Preconditions.checkNotNull(params);
    String dbName = params.getDb();
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name passed as argument to Catalog.dropDatabase");
    Preconditions.checkState(!catalog_.isBlacklistedDb(dbName) || params.if_exists,
        String.format("Can't drop blacklisted database: %s. %s", dbName,
            BLACKLISTED_DBS_INCONSISTENT_ERR_STR));
    if (catalog_.isBlacklistedDb(dbName)) {
      // It's expected to go here if "if_exists" is set to true.
      addSummary(resp, "Can't drop blacklisted database: " + dbName);
      return;
    }

    LOG.trace("Dropping database " + dbName);
    Db db = catalog_.getDb(dbName);
    if (db != null && db.numFunctions() > 0 && !params.cascade) {
      throw new CatalogException("Database " + db.getName() + " is not empty");
    }

    TCatalogObject removedObject = null;
    acquireMetastoreDdlLock(catalogTimeline);
    try {
      // Remove all the Kudu tables of 'db' from the Kudu storage engine.
      if (db != null && params.cascade) {
        dropTablesFromKudu(db, kudu_table_reserve_seconds, catalogTimeline);
      }

      // The Kudu tables in the HMS should have been dropped at this point
      // with the Hive Metastore integration enabled.
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
        long eventId = getCurrentEventId(msClient, catalogTimeline);
        // HMS client does not have a way to identify if the database was dropped or
        // not if the ignoreIfUnknown flag is true. Hence we always pass the
        // ignoreIfUnknown as false and catch the NoSuchObjectFoundException and
        // determine if we should throw or not
        msClient.getHiveClient().dropDatabase(
            dbName, /* deleteData */true, /* ignoreIfUnknown */false,
            params.cascade);
        catalogTimeline.markEvent("Dropped database in Metastore");
        List<NotificationEvent> events = getNextMetastoreDropEventsForDbIfEnabled(
            catalogTimeline, eventId, dbName);
        addToDeleteEventLog(events);
        addSummary(resp, "Database has been dropped.");
      } catch (TException e) {
        if (e instanceof NoSuchObjectException && params.if_exists) {
          // if_exists param was set; we ignore the NoSuchObjectFoundException
          addSummary(resp, "Database does not exist.");
        } else {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "dropDatabase"), e);
        }
      }
      Db removedDb = catalog_.removeDb(dbName);
      catalogTimeline.markEvent("Deleted database in catalog cache");

      if (removedDb == null) {
        // Nothing was removed from the catalogd's cache.
        resp.result.setVersion(catalog_.getCatalogVersion());
        return;
      }
      // Make sure the cache directives, if any, of the underlying tables are removed
      for (String tableName: removedDb.getAllTableNames()) {
        uncacheTable(removedDb.getTable(tableName), catalogTimeline);
      }
      removedObject = removedDb.toTCatalogObject();
      if (authzConfig_.isEnabled()) {
        authzManager_.updateDatabaseOwnerPrivilege(params.server_name, dbName,
            db.getMetaStoreDb().getOwnerName(), db.getMetaStoreDb().getOwnerType(),
            /* newOwner */ null, /* newOwnerType */ null, resp);
      }
    } finally {
      getMetastoreDdlLock().unlock();
    }

    Preconditions.checkNotNull(removedObject);
    resp.result.setVersion(removedObject.getCatalog_version());
    resp.result.addToRemoved_catalog_objects(removedObject);
    // it is possible that HMS database has been removed out of band externally. In
    // such a case we still would want to add the summary of the operation as database
    // has been dropped since we cleaned up state from CatalogServer
    addSummary(resp, "Database has been dropped.");
  }

  /**
   * Adds the events to the deleteEventLog if the event processing is active.
   */
  public void addToDeleteEventLog(List<NotificationEvent> events) {
    if (events == null || events.isEmpty()) return;
    for (NotificationEvent event : events) {
      String eventType = event.getEventType();
      Preconditions.checkState(
          eventType.equals(DropDatabaseEvent.EVENT_TYPE) ||
          eventType.equals(DropTableEvent.EVENT_TYPE) ||
          eventType.equals(DropPartitionEvent.EVENT_TYPE), "Can not add event type: " +
              "%s to deleteEventLog", eventType);
      String key;
      if (DropDatabaseEvent.EVENT_TYPE.equals(event.getEventType())) {
        key = DeleteEventLog.getDbKey(event.getDbName());
      } else {
        Preconditions.checkNotNull(event.getTableName());
        key = DeleteEventLog.getTblKey(event.getDbName(), event.getTableName());
      }
      addToDeleteEventLog(event.getEventId(), key);
    }
  }

  /**
   * Drops all the Kudu tables of database 'db' from the Kudu storage engine. Retrieves
   * the Kudu table name of each table in 'db' from HMS. Throws an ImpalaException if
   * metadata for Kudu tables cannot be loaded from HMS or if an error occurs while
   * trying to drop a table from Kudu.
   */
  private void dropTablesFromKudu(Db db, int kudu_table_reserve_seconds,
      EventSequence catalogTimeline) throws ImpalaException {
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
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
        msTables.addAll(msClient.getHiveClient().getTableObjectsByName(
            db.getName(), incompleteTableNames));
        catalogTimeline.markEvent("Fetched unloaded tables from Metastore");
      } catch (TException e) {
        LOG.error(String.format(HMS_RPC_ERROR_FORMAT_STR, "getTableObjectsByName") +
            e.getMessage());
      }
    }
    for (org.apache.hadoop.hive.metastore.api.Table msTable: msTables) {
      if (!KuduTable.isKuduTable(msTable) || !KuduTable
          .isSynchronizedTable(msTable)) continue;
      // The operation will be aborted if the Kudu table cannot be dropped. If for
      // some reason Kudu is permanently stuck in a non-functional state, the user is
      // expected to ALTER TABLE to either set the table to UNMANAGED or set the format
      // to something else.
      KuduCatalogOpExecutor.dropTable(
        msTable, /*if exists*/ true, kudu_table_reserve_seconds, catalogTimeline);
    }
  }

  private boolean isHmsIntegrationAutomatic(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws ImpalaRuntimeException {
    if (KuduTable.isKuduTable(msTbl)) {
      return isKuduHmsIntegrationEnabled(msTbl);
    }
    if (IcebergTable.isIcebergTable(msTbl)) {
      return isIcebergHmsIntegrationEnabled(msTbl);
    }
    return false;
  }

  private boolean isKuduHmsIntegrationEnabled(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws ImpalaRuntimeException {
    // Check if Kudu's integration with the Hive Metastore is enabled, and validate
    // the configuration.
    Preconditions.checkState(KuduTable.isKuduTable(msTbl));
    String masterHosts = msTbl.getParameters().get(KuduTable.KEY_MASTER_HOSTS);
    String hmsUris = MetaStoreUtil.getHiveMetastoreUris();
    return KuduTable.isHMSIntegrationEnabledAndValidate(masterHosts, hmsUris);
  }

  private boolean isIcebergHmsIntegrationEnabled(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws ImpalaRuntimeException {
    // Check if Iceberg's integration with the Hive Metastore is enabled, and validate
    // the configuration.
    Preconditions.checkState(IcebergTable.isIcebergTable(msTbl));
    // Only synchronized tables can be integrated.
    if (!IcebergTable.isSynchronizedTable(msTbl)) return false;
    return IcebergUtil.isHiveCatalog(msTbl);
  }

  /**
   * Drops a table or view from the metastore and removes it from the catalog.
   * Also drops all associated caching requests on the table and/or table's partitions,
   * uncaching all table data. If params.purge is true, table data is permanently
   * deleted.
   * In case of transactional tables acquires an exclusive HMS table lock before
   * executing the drop operation.
   */
  private void dropTableOrView(TDropTableOrViewParams params, TDdlExecResponse resp,
      int lockMaxWaitTime, int kudu_table_reserve_seconds, EventSequence catalogTimeline)
      throws ImpalaException {
    TableName tableName = TableName.fromThrift(params.getTable_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(!catalog_.isBlacklistedTable(tableName) || params.if_exists,
        String.format("Can't drop blacklisted table: %s. %s", tableName,
            BLACKLISTED_TABLES_INCONSISTENT_ERR_STR));
    if (catalog_.isBlacklistedTable(tableName)) {
      // It's expected to go here if "if_exists" is set to true.
      addSummary(resp, "Can't drop blacklisted table: " + tableName);
      return;
    }
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
      // we pass null validWriteIdList here since we don't really care what version of
      // table is loaded, eventually its going to be dropped below.
      catalog_.getOrLoadTable(params.getTable_name().db_name,
          params.getTable_name().table_name, "Load for DROP TABLE/VIEW", null,
          TABLE_ID_UNAVAILABLE, catalogTimeline);
      catalogTimeline.markEvent("Loaded catalog table");
    } catch (CatalogException e) {
      // Ignore exceptions -- the above was just to trigger loading. Failure to load
      // or non-existence of the database will be handled down below.
    }

    Table tbl = catalog_.getTableIfCachedNoThrow(tableName.getDb(), tableName.getTbl());
    long lockId = -1;
    if (tbl != null && !(tbl instanceof IncompleteTable) &&
        AcidUtils.isTransactionalTable(tbl.getMetaStoreTable().getParameters())) {
      HeartbeatContext ctx = new HeartbeatContext(
          String.format("Drop table/view %s.%s", tableName.getDb(), tableName.getTbl()),
          System.nanoTime());
      lockId = catalog_.lockTableStandalone(tableName.getDb(), tableName.getTbl(), ctx,
          lockMaxWaitTime);
      catalogTimeline.markEvent("Acquired Metastore lock on the table");
    }

    try {
      dropTableOrViewInternal(params, tableName, resp, kudu_table_reserve_seconds,
          catalogTimeline);
    } finally {
      if (lockId > 0) catalog_.releaseTableLock(lockId);
    }
  }

  /**
   * Helper function for dropTableOrView().
   */
  private void dropTableOrViewInternal(TDropTableOrViewParams params,
      TableName tableName, TDdlExecResponse resp, int kudu_table_reserve_seconds,
      EventSequence catalogTimeline) throws ImpalaException {
    TCatalogObject removedObject = new TCatalogObject();
    acquireMetastoreDdlLock(catalogTimeline);
    try {
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

      // Retrieve the HMS table to determine if this is a Kudu or Iceberg table.
      org.apache.hadoop.hive.metastore.api.Table msTbl = existingTbl.getMetaStoreTable();
      if (msTbl == null) {
        Preconditions.checkState(existingTbl instanceof IncompleteTable);
        Stopwatch hmsLoadSW = Stopwatch.createStarted();
        long hmsLoadTime;
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
          msTbl = msClient.getHiveClient().getTable(tableName.getDb(),
              tableName.getTbl());
          catalogTimeline.markEvent(FETCHED_HMS_TABLE);
        } catch (TException e) {
          LOG.error(String.format(HMS_RPC_ERROR_FORMAT_STR, "getTable") + e.getMessage());
        } finally {
          hmsLoadTime = hmsLoadSW.elapsed(TimeUnit.NANOSECONDS);
        }
        existingTbl.updateHMSLoadTableSchemaTime(hmsLoadTime);
      }
      boolean isSynchronizedKuduTable = msTbl != null &&
              KuduTable.isKuduTable(msTbl) && KuduTable.isSynchronizedTable(msTbl);
      if (isSynchronizedKuduTable) {
        KuduCatalogOpExecutor.dropTable(
            msTbl, /* if exists */ true, kudu_table_reserve_seconds, catalogTimeline);
      }

      long eventId;
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
        eventId = getCurrentEventId(msClient, catalogTimeline);
      }
      boolean isSynchronizedIcebergTable = msTbl != null &&
          IcebergTable.isIcebergTable(msTbl) &&
          IcebergTable.isSynchronizedTable(msTbl);
      // When HMS integration is automatic, the table is dropped automatically. In all
      // other cases, we need to drop the HMS table entry ourselves.
      boolean isSynchronizedTable = isSynchronizedKuduTable || isSynchronizedIcebergTable;
      boolean needsHmsDropTable =
          (existingTbl instanceof IncompleteTable && isSynchronizedIcebergTable) ||
              !isSynchronizedTable ||
              !isHmsIntegrationAutomatic(msTbl);

      if (!(existingTbl instanceof IncompleteTable) && isSynchronizedIcebergTable) {
        Preconditions.checkState(existingTbl instanceof IcebergTable);
        try {
          IcebergCatalogOpExecutor.dropTable((IcebergTable)existingTbl, params.if_exists);
          catalogTimeline.markEvent("Dropped table using Iceberg");
        } catch (TableNotFoundException e) {
          // This is unusual as normally this would have already shown up as
          // (existingTbl instanceof IncompleteTable), but this can happen if
          // for example the Iceberg metadata is removed.
          if (!needsHmsDropTable) {
            // There is no more work to be done, so throw exception.
            throw e;
          }
          // Although dropTable() failed in Iceberg we need to also drop the table in
          // HMS, so we continue here.
          LOG.warn(String.format("Could not drop Iceberg table %s.%s " +
                  "proceeding to drop table in HMS", tableName.getDb(),
              tableName.getTbl()), e);
        }
      }


      if (needsHmsDropTable) {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
          msClient.getHiveClient().dropTable(
              tableName.getDb(), tableName.getTbl(), true,
              params.if_exists, params.purge);
          catalogTimeline.markEvent("Dropped table in Metastore");
        } catch (NoSuchObjectException e) {
          throw new ImpalaRuntimeException(String.format("Table %s no longer exists " +
              "in the Hive MetaStore. Run 'invalidate metadata %s' to update the " +
              "Impala catalog.", tableName, tableName));
        } catch (TException e) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "dropTable"), e);
        }
      }
      List<NotificationEvent> events = getNextMetastoreEventsForTableIfEnabled(
          catalogTimeline, eventId, tableName.getDb(), tableName.getTbl(),
          DropTableEvent.EVENT_TYPE);
      addSummary(resp, (params.is_table ? "Table " : "View ") + "has been dropped.");
      addToDeleteEventLog(events);
      Table table = catalog_.removeTable(params.getTable_name().db_name,
          params.getTable_name().table_name);
      catalogTimeline.markEvent("Deleted table in catalog cache");
      if (table == null) {
        // Nothing was removed from the catalogd's cache.
        resp.result.setVersion(catalog_.getCatalogVersion());
        return;
      }
      resp.result.setVersion(table.getCatalogVersion());
      uncacheTable(table, catalogTimeline);
      if (table.getMetaStoreTable() != null) {
        if (authzConfig_.isEnabled()) {
          authzManager_.updateTableOwnerPrivilege(params.server_name,
              table.getDb().getName(), table.getName(),
              table.getMetaStoreTable().getOwner(),
              table.getMetaStoreTable().getOwnerType(), /* newOwner */ null,
              /* newOwnerType */ null, resp);
        }
      }
    } finally {
      getMetastoreDdlLock().unlock();
    }
    removedObject.setType(params.is_table ?
        TCatalogObjectType.TABLE : TCatalogObjectType.VIEW);
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
  private static void uncacheTable(FeTable table, EventSequence catalogTimeline) {
    if (!(table instanceof FeFsTable)) return;
    FeFsTable hdfsTable = (FeFsTable) table;
    if (hdfsTable.isMarkedCached()) {
      try {
        HdfsCachingUtil.removeTblCacheDirective(table.getMetaStoreTable());
        // This is also used in dropDatabase() so add the table name.
        catalogTimeline.markEvent("Removed table cache directive of " +
            table.getFullName());
      } catch (Exception e) {
        LOG.error("Unable to uncache table: " + table.getFullName(), e);
      }
    }
    if (table.getNumClusteringCols() > 0) {
      Collection<? extends FeFsPartition> parts =
          FeCatalogUtils.loadAllPartitions(hdfsTable);
      boolean hasTasks = false;
      for (FeFsPartition part: parts) {
        if (part.isMarkedCached()) {
          hasTasks = true;
          HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(
              (HdfsPartition) part);
          try {
            HdfsCachingUtil.removePartitionCacheDirective(partBuilder);
            // We are dropping the table. Don't need to update the existing partition so
            // ignore the partBuilder here.
          } catch (Exception e) {
            LOG.error("Unable to uncache partition: " + part.getPartitionName(), e);
          }
        }
      }
      if (hasTasks) {
        // This is also used in dropDatabase() so add the table name.
        catalogTimeline.markEvent("Removed partition cache directives of " +
            table.getFullName());
      }
    }
  }

  /**
   * Truncate a table by deleting all files in its partition directories, and dropping
   * all column and table statistics. Acquires a table lock to protect against
   * concurrent table modifications.
   * TODO truncate specified partitions.
   */
  private void truncateTable(TTruncateParams params, boolean wantMinimalResult,
      TDdlExecResponse resp, int lockMaxWaitTime, EventSequence catalogTimeline,
      @Nullable String debugAction) throws ImpalaException {
    TTableName tblName = params.getTable_name();
    Table table = null;
    try {
      table = getExistingTable(tblName.getDb_name(), tblName.getTable_name(),
          "Load for TRUNCATE TABLE", catalogTimeline);
    } catch (TableNotFoundException e) {
      if (params.if_exists) {
        addSummary(resp, "Table does not exist.");
        return;
      }
      throw e;
    }
    Preconditions.checkNotNull(table);
    if (!(table instanceof FeFsTable)) {
      throw new CatalogException(
          String.format("TRUNCATE TABLE not supported on non-HDFS table: %s",
          table.getFullName()));
    }
    // Lock table to check transactional properties.
    // If non-transactional, the lock will be held during truncation.
    // If transactional, the lock will be released for some time to acquire the HMS Acid
    // lock. It's safe because transactional -> non-transactional conversion is not
    // allowed.
    tryWriteLock(table, "truncating", catalogTimeline);
    try {
      long newCatalogVersion = 0;
      try {
        if (AcidUtils.isTransactionalTable(table.getMetaStoreTable().getParameters())) {
          newCatalogVersion = truncateTransactionalTable(params, table, lockMaxWaitTime,
              catalogTimeline);
        } else if (table instanceof FeIcebergTable) {
          newCatalogVersion = truncateIcebergTable(params, table, catalogTimeline);
        } else {
          newCatalogVersion = truncateNonTransactionalTable(params, table,
              catalogTimeline);
        }
        catalogTimeline.markEvent("Truncated table");
      } catch (Exception e) {
        String fqName = tblName.db_name + "." + tblName.table_name;
        throw new CatalogException(String.format("Failed to truncate table: %s.\n" +
            "Table may be in a partially truncated state.", fqName), e);
      }
      Preconditions.checkState(
          newCatalogVersion > 0, "newCatalogVersion must be greater than 0!");
      addSummary(resp, "Table has been truncated.");
      loadTableMetadata(
          table, newCatalogVersion, true, true, "TRUNCATE", debugAction, catalogTimeline);
      addTableToCatalogUpdate(table, wantMinimalResult, resp.result);
    } finally {
      UnlockWriteLockIfErronouslyLocked();
      if (table.isWriteLockedByCurrentThread()) {
        table.releaseWriteLock();
      }
    }
  }

  /**
   * Truncates a transactional table. It creates new empty base directories in all
   * partitions of the table. That way queries started earlier can still read a
   * valid snapshot version of the data. HMS's cleaner should remove obsolete
   * directories later.
   * After that empty directory creation it removes stats-related parameters of
   * the table and its partitions.
   */
  private long truncateTransactionalTable(TTruncateParams params, Table table,
      int lockMaxWaitTime, EventSequence catalogTimeline) throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    Preconditions.checkState(catalog_.getLock().isWriteLockedByCurrentThread());
    InProgressTableModification modification = null;
    catalog_.getLock().writeLock().unlock();
    TableName tblName = TableName.fromThrift(params.getTable_name());
    Stopwatch sw = Stopwatch.createStarted();
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      IMetaStoreClient hmsClient = msClient.getHiveClient();
      HeartbeatContext ctx = new HeartbeatContext(
          String.format("Truncate table %s.%s", tblName.getDb(), tblName.getTbl()),
          System.nanoTime());
      try (Transaction txn = catalog_.openTransaction(hmsClient, ctx)) {
        catalogTimeline.markEvent("Opened transaction in Metastore");
        Preconditions.checkState(txn.getId() > 0);
        // We need to release catalog table lock here, because HMS Acid table lock
        // must be locked in advance to avoid dead lock.
        table.releaseWriteLock();
        //TODO: if possible, set DataOperationType to something better than NO_TXN.
        catalog_.lockTableInTransaction(tblName.getDb(), tblName.getTbl(), txn,
            DataOperationType.NO_TXN, ctx, lockMaxWaitTime);
        tryWriteLock(table, "truncating", catalogTimeline);
        LOG.trace("Time elapsed after taking write lock on table {}: {} msec",
            table.getFullName(), sw.elapsed(TimeUnit.MILLISECONDS));

        // TODO: revisit this in-flight event registration for scenario where
        // table is not being replicated and COLUMN_STATS_ACCURATE is not in the table
        // property. In that scenario, probably no HMS APIs will be invoked thus no events
        // will be generated.
        modification = new InProgressTableModification(catalog_, table);
        catalog_.getLock().writeLock().unlock();

        modification.addCatalogServiceIdentifiersToTable();
        modification.registerInflightEvent();
        TblTransaction tblTxn = MetastoreShim.createTblTransaction(hmsClient,
            table.getMetaStoreTable(), txn.getId());
        HdfsTable hdfsTable = (HdfsTable) table;
        // if the table is replicated we should use the HMS API to truncate it so that
        // if moves the files into in replication change manager location which is later
        // used for replication.
        if (isTableBeingReplicated(hmsClient, hdfsTable)) {
          String dbName = Preconditions.checkNotNull(hdfsTable.getDb()).getName();
          MetastoreShim.truncateTable(hmsClient, dbName, hdfsTable.getName(), null,
              tblTxn.validWriteIds, tblTxn.writeId);
          LOG.trace("Time elapsed to truncate table {} using HMS API: {} msec",
              hdfsTable.getFullName(), sw.elapsed(TimeUnit.MILLISECONDS));
        } else {
          Collection<? extends FeFsPartition> parts =
              FeCatalogUtils.loadAllPartitions(hdfsTable);
          createEmptyBaseDirectories(parts, tblTxn.writeId);
          LOG.trace("Time elapsed after creating empty base directories for table {}: {} "
                  + "msec", table.getFullName(), sw.elapsed(TimeUnit.MILLISECONDS));
          // Currently Impala cannot update the statistics properly. So instead of
          // writing correct stats, let's just remove COLUMN_STATS_ACCURATE parameter from
          // each partition.
          // TODO(IMPALA-8883): properly update statistics
          List<org.apache.hadoop.hive.metastore.api.Partition> hmsPartitions =
              Lists.newArrayListWithCapacity(parts.size());
          if (table.getNumClusteringCols() > 0) {
            for (FeFsPartition part : parts) {
              org.apache.hadoop.hive.metastore.api.Partition hmsPart =
                  ((HdfsPartition) part).toHmsPartition();
              Preconditions.checkNotNull(hmsPart);
              if (hmsPart.getParameters() != null) {
                hmsPart.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
                hmsPartitions.add(hmsPart);
              }
            }
          }
          // For partitioned tables we need to alter all the partitions in HMS.
          if (!hmsPartitions.isEmpty()) {
            unsetPartitionsColStats(table.getMetaStoreTable(), hmsPartitions, tblTxn,
                catalogTimeline);
          }
          // Remove COLUMN_STATS_ACCURATE property from the table.
          unsetTableColStats(table.getMetaStoreTable(), tblTxn, catalogTimeline);
          LOG.trace("Time elapsed after unset partition and column statistics for table "
              + "{}: {} msec", table.getFullName(), sw.elapsed(TimeUnit.MILLISECONDS));
        }
        txn.commit();
      }
      modification.markInflightEventRegistrationComplete();
      modification.validateInProgressModificationComplete();
      return modification.newVersionNumber();
    } catch (Exception e) {
      if (modification != null) modification.cancelInflightEventIfExist();
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "truncateTable"), e);
    } finally {
      LOG.trace("Time taken to do metastore and file system operations for"
              + " truncating table {}: {} msec", table.getFullName(),
          sw.stop().elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Helper method to check if the database which table belongs to is a source of
   * Hive replication. We cannot rely on the Db object here due to eventual nature of
   * cache updates.
   */
  private boolean isTableBeingReplicated(IMetaStoreClient metastoreClient,
      HdfsTable tbl) throws CatalogException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    String dbName = tbl.getDb().getName();
    try {
      Database db = metastoreClient.getDatabase(dbName);
      if (!db.isSetParameters()) return false;
      return org.apache.commons.lang.StringUtils
          .isNotEmpty(db.getParameters().get("repl.source.for"));
    } catch (TException tException) {
      throw new CatalogException(
          String.format("Could not determine if the table %s is a replication source",
          tbl.getFullName()), tException);
    }
  }

  private long truncateIcebergTable(TTruncateParams params, Table table,
      EventSequence catalogTimeline) throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    Preconditions.checkState(catalog_.getLock().isWriteLockedByCurrentThread());
    Preconditions.checkState(table instanceof FeIcebergTable);
    InProgressTableModification modification =
        new InProgressTableModification(catalog_, table);
    catalog_.getLock().writeLock().unlock();
    modification.addCatalogServiceIdentifiersToTable();
    try {
      FeIcebergTable iceTbl = (FeIcebergTable) table;
      if (params.isDelete_stats()) {
        // TODO: The following methods could already generate events. Investigate if
        // calling modification.registerInflightEvent() here is necessary ahead of them.
        modification.registerInflightEvent();
        dropColumnStats(table, catalogTimeline);
        dropTableStats(table, catalogTimeline);
      }
      org.apache.iceberg.Transaction iceTxn = IcebergUtil.getIcebergTransaction(iceTbl);
      IcebergCatalogOpExecutor.truncateTable(iceTxn);
      if (isIcebergHmsIntegrationEnabled(iceTbl.getMetaStoreTable())) {
        modification.registerInflightEvent();
        IcebergCatalogOpExecutor.addCatalogVersionToTxn(
            iceTxn, catalog_.getCatalogServiceId(), modification.newVersionNumber());
      }
      iceTxn.commitTransaction();
      modification.markInflightEventRegistrationComplete();
      modification.validateInProgressModificationComplete();
    } catch (ImpalaException ex) {
      modification.cancelInflightEventIfExist();
      throw ex;
    }
    return modification.newVersionNumber();
  }

  private long truncateNonTransactionalTable(TTruncateParams params, Table table,
      EventSequence catalogTimeline) throws Exception {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    Preconditions.checkState(catalog_.getLock().isWriteLockedByCurrentThread());
    InProgressTableModification modification =
        new InProgressTableModification(catalog_, table);
    catalog_.getLock().writeLock().unlock();
    modification.addCatalogServiceIdentifiersToTable();
    HdfsTable hdfsTable = (HdfsTable) table;
    boolean isTableBeingReplicated = false;
    Stopwatch sw = Stopwatch.createStarted();
    try {
      // if the table is being replicated we issue the HMS API to truncate the table
      // since it generates additional events which are used by Hive Replication.
      try (MetaStoreClient client = catalog_.getMetaStoreClient(catalogTimeline)) {
        if (isTableBeingReplicated(client.getHiveClient(), hdfsTable)) {
          isTableBeingReplicated = true;
          // We will issue HMS API in these cases. Register in-flight event before we do.
          modification.registerInflightEvent();
          String dbName = Preconditions.checkNotNull(hdfsTable.getDb()).getName();
          client.getHiveClient()
              .truncateTable(dbName, hdfsTable.getName(), null);
          catalogTimeline.markEvent("Truncated table in Metastore");
          LOG.trace("Time elapsed after truncating table {} using HMS API: {} msec",
              hdfsTable.getFullName(), sw.elapsed(TimeUnit.MILLISECONDS));
        }
      }
      if (!isTableBeingReplicated) {
        // when table is replicated we let the HMS API handle the file deletion logic
        // otherwise we delete the files.
        Collection<? extends FeFsPartition> parts = FeCatalogUtils
            .loadAllPartitions(hdfsTable);
        for (FeFsPartition part : parts) {
          FileSystemUtil.deleteAllVisibleFiles(new Path(part.getLocation()));
        }
        catalogTimeline.markEvent("Deleted all visible files");
        LOG.trace("Time elapsed after deleting files for table {}: {} msec",
            table.getFullName(), sw.elapsed(TimeUnit.MILLISECONDS));
      }
      if (params.isDelete_stats()) {
        // We will issue HMS API in these cases. Register in-flight event before we do.
        // TODO: revisit this in-flight event registration.
        modification.registerInflightEvent();
        dropColumnStats(table, catalogTimeline);
        dropTableStats(table, catalogTimeline);
        LOG.trace("Time elapsed after deleting statistics for table {}: {} msec ",
            table.getFullName(), sw.elapsed(TimeUnit.MILLISECONDS));
      }
      modification.markInflightEventRegistrationComplete();
      modification.validateInProgressModificationComplete();
      return modification.newVersionNumber();
    } catch (Exception ex) {
      modification.cancelInflightEventIfExist();
      throw ex;
    } finally {
      LOG.debug("Time taken for metastore and filesystem operations for truncating "
              + "table {}: {} msec", table.getFullName(),
          sw.stop().elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Creates new empty base directories for an ACID table. The directories won't be
   * really empty, they will contain the "empty" file. It's needed because
   * FileSystemUtil.listFiles() doesn't see empty directories. See IMPALA-8739.
   * @param partitions the partitions in which we create new directories.
   * @param writeId the write id of the new base directory.
   * @throws IOException
   */
  private void createEmptyBaseDirectories(
      Collection<? extends FeFsPartition> partitions, long writeId) throws IOException {
    for (FeFsPartition part: partitions) {
      Path partPath = new Path(part.getLocation());
      FileSystem fs = FileSystemUtil.getFileSystemForPath(partPath);
      String baseDirStr =
          part.getLocation() + Path.SEPARATOR + "base_" + String.valueOf(writeId);
      fs.mkdirs(new Path(baseDirStr));
      String emptyFile = baseDirStr + Path.SEPARATOR + "empty";
      fs.create(new Path(emptyFile)).close();
    }
  }

  private void dropFunction(TDropFunctionParams params, TDdlExecResponse resp,
      EventSequence catalogTimeline) throws ImpalaException {
    FunctionName fName = FunctionName.fromThrift(params.fn_name);
    Db db = catalog_.getDb(fName.getDb());
    if (db == null) {
      if (!params.if_exists) {
        throw new CatalogException("Database: " + fName.getDb()
            + " does not exist.");
      }
      addSummary(resp, "Database does not exist.");
      return;
    }

    tryLock(db, "dropping function " + fName, catalogTimeline);
    // Get a new catalog version to assign to the database being altered. This is
    // needed for events processor as this method creates alter database events.
    long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
    try {
      List<TCatalogObject> removedFunctions = Lists.newArrayList();
      if (!params.isSetSignature()) {
        dropJavaFunctionFromHms(fName.getDb(), fName.getFunction(), params.if_exists,
            catalogTimeline);
        for (Function fn : db.getFunctions(fName.getFunction())) {
          if (fn.getBinaryType() != TFunctionBinaryType.JAVA
              || !fn.isPersistent()) {
            continue;
          }
          Preconditions.checkNotNull(catalog_.removeFunction(fn));
          removedFunctions.add(fn.toTCatalogObject());
        }
      } else {
        ArrayList<Type> argTypes = Lists.newArrayList();
        for (TColumnType t : params.arg_types) {
          argTypes.add(Type.fromThrift(t));
        }
        Function desc = new Function(fName, argTypes, Type.INVALID, false);
        Function fn = catalog_.removeFunction(desc);
        catalogTimeline.markEvent("Removed function in catalog cache");
        if (fn == null) {
          if (!params.if_exists) {
            throw new CatalogException(
                "Function: " + desc.signatureString() + " does not exist.");
          }
        } else {
          addCatalogServiceIdentifiers(db.getMetaStoreDb(),
              catalog_.getCatalogServiceId(), newCatalogVersion);
          // Flush DB changes to metastore
          applyAlterDatabase(db.getMetaStoreDb(), catalogTimeline);
          removedFunctions.add(fn.toTCatalogObject());
          // now that HMS alter operation has succeeded, add this version to list of
          // inflight events in catalog database if event processing is enabled.
          catalog_.addVersionsForInflightEvents(db, newCatalogVersion);
        }
      }

      if (!removedFunctions.isEmpty()) {
        addSummary(resp, "Function has been dropped.");
        resp.result.setRemoved_catalog_objects(removedFunctions);
      } else {
        addSummary(resp, "Function does not exist.");
      }
      resp.result.setVersion(catalog_.getCatalogVersion());
    } finally {
      db.getLock().unlock();
    }
  }

  /**
   * Creates a new table in the metastore and adds an entry to the metadata cache to
   * lazily load the new metadata on the next access. If this is a Synchronized Kudu
   * table, the table is also created in the Kudu storage engine. Re-throws any HMS or
   * Kudu exceptions encountered during the create.
   * @param  syncDdl tells if SYNC_DDL option is enabled on this DDL request.
   * @return true if a new table has been created with the given params, false
   * otherwise.
   */
  private boolean createTable(TCreateTableParams params, TDdlExecResponse response,
      EventSequence catalogTimeline, boolean syncDdl, boolean wantMinimalResult,
      @Nullable String debugAction) throws ImpalaException {
    Preconditions.checkNotNull(params);
    TableName tableName = TableName.fromThrift(params.getTable_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null,
        "Null column list given as argument to Catalog.createTable");
    Preconditions.checkState(!catalog_.isBlacklistedTable(tableName),
        String.format("Can't create blacklisted table: %s. %s", tableName,
            BLACKLISTED_TABLES_INCONSISTENT_ERR_STR));

    Table existingTbl = catalog_.getTableNoThrow(tableName.getDb(), tableName.getTbl());
    if (params.if_not_exists && existingTbl != null) {
      addSummary(response, "Table already exists.");
      LOG.trace("Skipping table creation because {} already exists and " +
          "IF NOT EXISTS was specified.", tableName);
      tryWriteLock(existingTbl, catalogTimeline);
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
        addTableToCatalogUpdate(existingTbl, wantMinimalResult, response.result);
      } finally {
        // Release the locks held in tryLock().
        catalog_.getLock().writeLock().unlock();
        existingTbl.releaseWriteLock();
      }
      return false;
    }
    org.apache.hadoop.hive.metastore.api.Table tbl = createMetaStoreTable(params);
    LOG.trace("Creating table {}", tableName);
    if (KuduTable.isKuduTable(tbl)) {
      return createKuduTable(tbl, params, wantMinimalResult, response, catalogTimeline);
    } else if (IcebergTable.isIcebergTable(tbl)) {
      return createIcebergTable(tbl, wantMinimalResult, response, catalogTimeline,
          params.if_not_exists, params.getColumns(), params.getPartition_spec(),
          params.getPrimary_key_column_names(), params.getTable_properties(),
          params.getComment(), debugAction);
    }
    Preconditions.checkState(params.getColumns().size() > 0,
        "Empty column list given as argument to Catalog.createTable");
    MetastoreShim.setTableLocation(catalog_.getDb(tbl.getDbName()), tbl);
    return createTable(tbl, params.if_not_exists, params.getCache_op(),
        params.server_name, params.getPrimary_keys(), params.getForeign_keys(),
        wantMinimalResult, response, catalogTimeline);
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
      TSortingOrder sortingOrder = params.isSetSorting_order() ?
          params.sorting_order : TSortingOrder.LEXICAL;
      tbl.getParameters().put(AlterTableSortByStmt.TBL_PROP_SORT_ORDER,
          sortingOrder.toString());
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

    // Set bucketing_version to table parameter
    if (params.getBucket_info() != null
        && params.getBucket_info().getBucket_type() != TBucketType.NONE) {
      tbl.getParameters().put("bucketing_version", "2");
    }
    tbl.setSd(createSd(params));
    if (params.getPartition_columns() != null) {
      // Add in any partition keys that were specified
      tbl.setPartitionKeys(buildFieldSchemaList(params.getPartition_columns()));
    } else {
      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
    }

    setDefaultTableCapabilities(tbl);
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

    // Add bucket desc
    if (params.getBucket_info() != null
        && params.getBucket_info().getBucket_type() != TBucketType.NONE) {
      sd.setBucketCols(params.getBucket_info().getBucket_columns());
      sd.setNumBuckets(params.getBucket_info().getNum_bucket());
    }

    // Add in all the columns
    sd.setCols(buildFieldSchemaList(params.getColumns()));
    return sd;
  }

  /**
   * Creates a new Kudu table. It should be noted that since HIVE-22158, HMS transforms
   * a create managed Kudu table request to a create external Kudu table with
   * <code>external.table.purge</code> property set to true. Such transformed Kudu
   * tables should be treated as managed (synchronized) tables to keep the user facing
   * behavior consistent.
   *
   * For synchronized tables (managed or external tables with external.table.purge=true
   * in tblproperties):
   *  1. If Kudu's integration with the Hive Metastore is not enabled, the Kudu
   *     table is first created in Kudu, then in the HMS.
   *  2. Otherwise, when the table is created in Kudu, we rely on Kudu to have
   *     created the table in the HMS.
   * For external tables:
   *  1. We only create the table in the HMS (regardless of Kudu's integration
   *     with the Hive Metastore).
   *
   * After the above is complete, we create the table in the catalog cache.
   *
   * 'response' is populated with the results of this operation. Returns true if a new
   * table was created as part of this call, false otherwise.
   */
  private boolean createKuduTable(org.apache.hadoop.hive.metastore.api.Table newTable,
      TCreateTableParams params, boolean wantMinimalResult, TDdlExecResponse response,
      EventSequence catalogTimeline) throws ImpalaException {
    Preconditions.checkState(KuduTable.isKuduTable(newTable));
    boolean createHMSTable;
    long eventId;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      eventId = getCurrentEventId(msClient, catalogTimeline);
    }
    if (!KuduTable.isSynchronizedTable(newTable)) {
      // if this is not a synchronized table, we assume that the table must be existing
      // in kudu and use the column spec from Kudu
      KuduCatalogOpExecutor.populateExternalTableColsFromKudu(catalogTimeline, newTable);
      createHMSTable = true;
    } else {
      // if this is a synchronized table (managed or external.purge table) then we
      // create it in Kudu first
      KuduCatalogOpExecutor.createSynchronizedTable(catalogTimeline, newTable, params);
      createHMSTable = !isKuduHmsIntegrationEnabled(newTable);
    }
    try {
      // Add the table to the HMS and the catalog cache. Acquire metastoreDdlLock_ to
      // ensure the atomicity of these operations.
      List<NotificationEvent> events = Collections.emptyList();
      acquireMetastoreDdlLock(catalogTimeline);
      if (createHMSTable) {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
          boolean tableInMetastore =
              msClient.getHiveClient().tableExists(newTable.getDbName(),
                                                   newTable.getTableName());
          catalogTimeline.markEvent(CHECKED_HMS_TABLE_EXISTENCE);
          if (!tableInMetastore) {
            msClient.getHiveClient().createTable(newTable);
            catalogTimeline.markEvent(CREATED_HMS_TABLE);
          } else {
            addSummary(response, "Table already exists.");
            return false;
          }
          events = getNextMetastoreEventsForTableIfEnabled(catalogTimeline, eventId,
              newTable.getDbName(), newTable.getTableName(), CreateTableEvent.EVENT_TYPE);
        }
      }
      // in case of synchronized tables it is possible that Kudu doesn't generate
      // any metastore events.
      long createEventId = -1;
      Pair<Long, org.apache.hadoop.hive.metastore.api.Table> eventTblPair =
          getTableFromEvents(events, params.if_not_exists);
      createEventId = eventTblPair == null ? -1 : eventTblPair.first;
      org.apache.hadoop.hive.metastore.api.Table msTable =
          eventTblPair == null ? null : eventTblPair.second;
      setTableNameAndCreateTimeInResponse(msTable,
          newTable.getDbName(), newTable.getTableName(), response, catalogTimeline);

      // Add the table to the catalog cache
      Table newTbl = catalog_
          .addIncompleteTable(newTable.getDbName(), newTable.getTableName(),
              TImpalaTableType.TABLE, params.getComment(), createEventId);
      catalogTimeline.markEvent(CREATED_CATALOG_TABLE);
      LOG.debug("Created a Kudu table {} with create event id {}", newTbl.getFullName(),
          createEventId);
      addTableToCatalogUpdate(newTbl, wantMinimalResult, response.result);
    } catch (Exception e) {
      try {
        // Error creating the table in HMS, drop the synchronized table from Kudu.
        if (!KuduTable.isSynchronizedTable(newTable)) {
          KuduCatalogOpExecutor.dropTable(newTable, /* if exists */ false,
              /* kudu_table_reserve_seconds */ 0, catalogTimeline);
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
    } finally {
      getMetastoreDdlLock().unlock();
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
      List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
      boolean wantMinimalResult, TDdlExecResponse response, EventSequence catalogTimeline)
      throws ImpalaException {
    Preconditions.checkState(!KuduTable.isKuduTable(newTable));
    acquireMetastoreDdlLock(catalogTimeline);
    try {
      org.apache.hadoop.hive.metastore.api.Table msTable;
      Pair<Long, org.apache.hadoop.hive.metastore.api.Table> eventIdTblPair = null;
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
        long eventId = getCurrentEventId(msClient, catalogTimeline);
        if (primaryKeys == null && foreignKeys == null) {
          msClient.getHiveClient().createTable(newTable);
        } else {
          MetastoreShim.createTableWithConstraints(
              msClient.getHiveClient(), newTable,
              primaryKeys == null ? Collections.emptyList() : primaryKeys,
              foreignKeys == null ? Collections.emptyList() : foreignKeys);
        }
        catalogTimeline.markEvent(CREATED_HMS_TABLE);
        addSummary(response, "Table has been created.");
        List<NotificationEvent> events = getNextMetastoreEventsForTableIfEnabled(
            catalogTimeline, eventId, newTable.getDbName(), newTable.getTableName(),
            CreateTableEvent.EVENT_TYPE);
        eventIdTblPair = getTableFromEvents(events, if_not_exists);
        if (eventIdTblPair == null) {
          // TODO (HIVE-21807): Creating a table and retrieving the table information is
          // not atomic.
          eventIdTblPair = new Pair<>(-1L, msClient.getHiveClient()
              .getTable(newTable.getDbName(), newTable.getTableName()));
          catalogTimeline.markEvent(FETCHED_HMS_TABLE);
        }
        msTable = eventIdTblPair.second;
        setTableNameAndCreateTimeInResponse(msTable, newTable.getDbName(),
            newTable.getTableName(), response, catalogTimeline);
        // For external tables set table location needed for lineage generation.
        if (newTable.getTableType() == TableType.EXTERNAL_TABLE.toString()) {
          String tableLocation = newTable.getSd().getLocation();
          // If location was not specified in the query, get it from newly created
          // metastore table.
          if (tableLocation == null) {
            tableLocation = msTable.getSd().getLocation();
          }
          response.setTable_location(tableLocation);
        }
      } catch (Exception e) {
        if (e instanceof AlreadyExistsException && if_not_exists) {
          addSummary(response, "Table already exists");
          return false;
        }
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "createTable"), e);
      }
      Table newTbl = catalog_.addIncompleteTable(msTable.getDbName(),
          msTable.getTableName(),
          MetastoreShim.mapToInternalTableType(msTable.getTableType()),
          MetadataOp.getTableComment(msTable),
          eventIdTblPair.first);
      Preconditions.checkNotNull(newTbl);
      catalogTimeline.markEvent(CREATED_CATALOG_TABLE);
      LOG.debug("Created catalog table {} with create event id {}", newTbl.getFullName(),
          eventIdTblPair.first);
      // Submit the cache request and update the table metadata.
      if (cacheOp != null && cacheOp.isSet_cached()) {
        short replication = cacheOp.isSetReplication() ? cacheOp.getReplication() :
            JniCatalogConstants.HDFS_DEFAULT_CACHE_REPLICATION_FACTOR;
        long id = HdfsCachingUtil.submitCacheTblDirective(msTable,
            cacheOp.getCache_pool_name(), replication);
        catalog_.watchCacheDirs(Lists.<Long>newArrayList(id),
            new TTableName(msTable.getDbName(), msTable.getTableName()),
                "CREATE TABLE CACHED");
        // in this case we don't really bump up the catalog version and apply the alter
        // we know that the table is just created above and we hold the lock. We
        // reuse the table's catalog version to add to the in-flight events to avoid
        // reloading the table when the alter_table event is received later.
        addCatalogServiceIdentifiers(msTable, catalog_.getCatalogServiceId(),
            newTbl.getCatalogVersion());
        applyAlterTable(msTable, true, null, catalogTimeline);
        newTbl.addToVersionsForInflightEvents(false, newTbl.getCatalogVersion());
      }
      addTableToCatalogUpdate(newTbl, wantMinimalResult, response.result);
      if (authzConfig_.isEnabled()) {
        authzManager_.updateTableOwnerPrivilege(serverName, msTable.getDbName(),
            msTable.getTableName(), /* oldOwner */ null,
            /* oldOwnerType */ null, msTable.getOwner(), msTable.getOwnerType(),
            response);
      }
    } finally {
      getMetastoreDdlLock().unlock();
    }
    return true;
  }

  /**
   * Sets table name and creation time in 'response' based on 'msTable'.
   * If 'msTable' is null, then it loads the table from HMS.
   * Throws exception if table is not found.
   */
  private void setTableNameAndCreateTimeInResponse(
      org.apache.hadoop.hive.metastore.api.Table msTable, String dbName, String tblName,
      TDdlExecResponse response, EventSequence catalogTimeline)
      throws org.apache.thrift.TException {
    if (msTable == null) {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
        msTable = msClient.getHiveClient().getTable(dbName, tblName);
        catalogTimeline.markEvent(FETCHED_HMS_TABLE + " to get create time");
      }
    }
    response.setTable_name(dbName + "." + tblName);
    response.setTable_create_time(msTable.getCreateTime());
  }

  /**
   * Creates a new view in the metastore and adds an entry to the metadata cache to
   * lazily load the new metadata on the next access. Re-throws any Metastore
   * exceptions encountered during the create.
   */
  private void createView(TCreateOrAlterViewParams params, boolean wantMinimalResult,
      TDdlExecResponse response, EventSequence catalogTimeline) throws ImpalaException {
    TableName tableName = TableName.fromThrift(params.getView_name());
    Preconditions.checkState(tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
        params.getColumns().size() > 0,
          "Null or empty column list given as argument to DdlExecutor.createView");
    Preconditions.checkState(!catalog_.isBlacklistedTable(tableName),
        String.format("Can't create view with blacklisted table name: %s. %s", tableName,
            BLACKLISTED_TABLES_INCONSISTENT_ERR_STR));
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
    if (!createTable(view, params.if_not_exists, /*cacheOp*/null, params.server_name,
        /*primaryKeys*/null, /*foreignKeys*/null, wantMinimalResult, response,
        catalogTimeline)) {
      addSummary(response, "View already exists.");
    } else {
      addSummary(response, "View has been created.");
    }
  }

  /**
   * Creates a new Iceberg table.
   */
  private boolean createIcebergTable(org.apache.hadoop.hive.metastore.api.Table newTable,
      boolean wantMinimalResult, TDdlExecResponse response, EventSequence catalogTimeline,
      boolean ifNotExists, List<TColumn> columns, TIcebergPartitionSpec partitionSpec,
      List<String> primaryKeyColumnNames, Map<String, String> tableProperties,
      String tblComment, @Nullable String debugAction) throws ImpalaException {
    Preconditions.checkState(IcebergTable.isIcebergTable(newTable));

    acquireMetastoreDdlLock(catalogTimeline);
    try {
      // Add the table to the HMS and the catalog cache. Acquire metastoreDdlLock_ to
      // ensure the atomicity of these operations.
      List<NotificationEvent> events;
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
        boolean tableInMetastore =
            msClient.getHiveClient().tableExists(newTable.getDbName(),
                newTable.getTableName());
        catalogTimeline.markEvent(CHECKED_HMS_TABLE_EXISTENCE);
        if (!tableInMetastore) {
          long eventId = getCurrentEventId(msClient, catalogTimeline);
          TIcebergCatalog catalog = IcebergUtil.getTIcebergCatalog(newTable);
          String location = newTable.getSd().getLocation();
          //Create table in iceberg if necessary
          if (IcebergTable.isSynchronizedTable(newTable)) {
            //Set location here if not been specified in sql
            if (location == null) {
              if (catalog == TIcebergCatalog.HADOOP_CATALOG) {
                // Using catalog location to create table
                // We cannot set location for 'hadoop.catalog' table in SQL
                location = IcebergUtil.getIcebergCatalogLocation(newTable);
              } else {
                // Using normal location as 'hadoop.tables' table location and create
                // table
                location = MetastoreShim.getPathForNewTable(
                    msClient.getHiveClient().getDatabase(newTable.getDbName()),
                    newTable);
              }
            }
            if (debugAction != null) {
              DebugUtils.executeDebugAction(debugAction, DebugUtils.ICEBERG_CREATE);
            }
            String tableLoc = IcebergCatalogOpExecutor.createTable(catalog,
                IcebergUtil.getIcebergTableIdentifier(newTable), location, columns,
                partitionSpec, primaryKeyColumnNames, newTable.getOwner(),
                tableProperties).location();
            newTable.getSd().setLocation(tableLoc);
            catalogTimeline.markEvent(CREATED_ICEBERG_TABLE + catalog.name());
          } else {
            // If this is not a synchronized table, we assume that the table must be
            // existing in an Iceberg Catalog.
            TIcebergCatalog underlyingCatalog =
                IcebergUtil.getUnderlyingCatalog(newTable);
            String locationToLoadFrom;
            if (underlyingCatalog == TIcebergCatalog.HADOOP_TABLES) {
              if (location == null) {
                addSummary(response,
                    "Location is necessary for external iceberg table.");
                return false;
              }
              locationToLoadFrom = location;
            } else {
              // For HadoopCatalog tables 'locationToLoadFrom' is the location of the
              // hadoop catalog. For HiveCatalog tables it remains null.
              locationToLoadFrom = IcebergUtil.getIcebergCatalogLocation(newTable);
            }
            TableIdentifier identifier = IcebergUtil.getIcebergTableIdentifier(newTable);
            org.apache.iceberg.Table iceTable = IcebergUtil.loadTable(
                catalog, identifier, locationToLoadFrom, newTable.getParameters());
            catalogTimeline.markEvent(LOADED_ICEBERG_TABLE);
            // Populate the HMS table schema based on the Iceberg table's schema because
            // the Iceberg metadata is the source of truth. This also avoids an
            // unnecessary ALTER TABLE.
            IcebergCatalogOpExecutor.populateExternalTableCols(newTable, iceTable);
            if (location == null) {
              // Using the location of the loaded Iceberg table we can also get the
              // correct location for tables stored in nested namespaces.
              newTable.getSd().setLocation(iceTable.location());
            }
          }

          // Iceberg tables are always unpartitioned. The partition columns are
          // derived from the TCreateTableParams.partition_spec field, and could
          // include one or more of the table columns
          Preconditions.checkState(newTable.getPartitionKeys() == null ||
              newTable.getPartitionKeys().isEmpty());
          if (!isIcebergHmsIntegrationEnabled(newTable)) {
            msClient.getHiveClient().createTable(newTable);
            catalogTimeline.markEvent(CREATED_HMS_TABLE);
          }
          events = getNextMetastoreEventsForTableIfEnabled(catalogTimeline, eventId,
              newTable.getDbName(), newTable.getTableName(), CreateTableEvent.EVENT_TYPE);
        } else {
          addSummary(response, "Table already exists.");
          return false;
        }
      }
      Pair<Long, org.apache.hadoop.hive.metastore.api.Table> eventTblPair
          = getTableFromEvents(events, ifNotExists);
      long createEventId = eventTblPair == null ? -1 : eventTblPair.first;
      org.apache.hadoop.hive.metastore.api.Table msTable =
          eventTblPair == null ? null : eventTblPair.second;
      setTableNameAndCreateTimeInResponse(msTable,
          newTable.getDbName(), newTable.getTableName(), response, catalogTimeline);
      // Add the table to the catalog cache
      Table newTbl = catalog_.addIncompleteTable(newTable.getDbName(),
          newTable.getTableName(), TImpalaTableType.TABLE, tblComment,
          createEventId);
      catalogTimeline.markEvent(CREATED_CATALOG_TABLE);
      LOG.debug("Created an iceberg table {} in catalog with create event Id {} ",
          newTbl.getFullName(), createEventId);
      addTableToCatalogUpdate(newTbl, wantMinimalResult, response.result);
    } catch (Exception e) {
      if (ifNotExists && (e instanceof AlreadyExistsException ||
          e instanceof org.apache.iceberg.exceptions.AlreadyExistsException)) {
        addSummary(response, "Table already exists.");
        return false;
      }
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "createTable"), e);
    } finally {
      getMetastoreDdlLock().unlock();
    }

    addSummary(response, "Table has been created.");
    return true;
  }

  /**
   * Creates a new table in the metastore based on the definition of an existing table.
   * No data is copied as part of this process, it is a metadata only operation. If the
   * creation succeeds, an entry is added to the metadata cache to lazily load the new
   * table's metadata on the next access.
   * @param  syncDdl tells is SYNC_DDL is enabled for this DDL request.
   */
  private void createTableLike(TCreateTableLikeParams params, TDdlExecResponse response,
      EventSequence catalogTimeline, boolean syncDdl, boolean wantMinimalResult,
      @Nullable String debugAction) throws ImpalaException {
    Preconditions.checkNotNull(params);
    THdfsFileFormat fileFormat =
        params.isSetFile_format() ? params.getFile_format() : null;
    String comment = params.isSetComment() ? params.getComment() : null;
    TableName tblName = TableName.fromThrift(params.getTable_name());
    TableName srcTblName = TableName.fromThrift(params.getSrc_table_name());
    Preconditions.checkState(tblName != null && tblName.isFullyQualified());
    Preconditions.checkState(srcTblName != null && srcTblName.isFullyQualified());
    Preconditions.checkState(!catalog_.isBlacklistedTable(tblName),
        String.format("Can't create blacklisted table: %s. %s", tblName,
            BLACKLISTED_TABLES_INCONSISTENT_ERR_STR));

    Table existingTbl = catalog_.getTableNoThrow(tblName.getDb(), tblName.getTbl());
    if (params.if_not_exists && existingTbl != null) {
      addSummary(response, "Table already exists.");
      LOG.trace(String.format("Skipping table creation because %s already exists and " +
          "IF NOT EXISTS was specified.", tblName));
      tryWriteLock(existingTbl, catalogTimeline);
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
        addTableToCatalogUpdate(existingTbl, wantMinimalResult, response.result);
      } finally {
        // Release the locks held in tryLock().
        catalog_.getLock().writeLock().unlock();
        existingTbl.releaseWriteLock();
      }
      return;
    }
    Table srcTable = getExistingTable(srcTblName.getDb(), srcTblName.getTbl(),
        "Load source for CREATE TABLE LIKE", catalogTimeline);
    org.apache.hadoop.hive.metastore.api.Table tbl =
        srcTable.getMetaStoreTable().deepCopy();
    tbl.setDbName(tblName.getDb());
    tbl.setTableName(tblName.getTbl());
    tbl.setOwner(params.getOwner());
    if (tbl.getParameters() == null) {
      tbl.setParameters(new HashMap<String, String>());
    }
    if (params.isSetSort_columns() && !params.sort_columns.isEmpty()) {
      tbl.getParameters().put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS,
          Joiner.on(",").join(params.sort_columns));
      TSortingOrder sortingOrder = params.isSetSorting_order() ?
          params.sorting_order : TSortingOrder.LEXICAL;
      tbl.getParameters().put(AlterTableSortByStmt.TBL_PROP_SORT_ORDER,
          sortingOrder.toString());
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
    setDefaultTableCapabilities(tbl);
    LOG.trace(String.format("Creating table %s LIKE %s", tblName, srcTblName));

    if (srcTable instanceof IcebergTable && IcebergTable.isIcebergTable(tbl)) {
      IcebergTable srcIceTable = (IcebergTable) srcTable;
      Map<String, String> tableProperties = Maps
          .newHashMap(srcIceTable.getIcebergApiTable().properties());
      tableProperties.remove(Catalogs.NAME);
      tableProperties.remove(Catalogs.LOCATION);
      tableProperties.remove(IcebergTable.ICEBERG_CATALOG);
      tableProperties.remove(IcebergTable.ICEBERG_TABLE_IDENTIFIER);

      // The table identifier of the new table will be 'database.table'
      TableIdentifier identifier = IcebergUtil
          .getIcebergTableIdentifier(tbl.getDbName(), tbl.getTableName());
      if (tbl.getParameters().containsKey(Catalogs.NAME)) {
        tbl.getParameters().put(Catalogs.NAME, identifier.toString());
        tableProperties.put(Catalogs.NAME, identifier.toString());
      }
      if (tbl.getParameters().containsKey(IcebergTable.ICEBERG_CATALOG)) {
        tableProperties.put(IcebergTable.ICEBERG_CATALOG,
            tbl.getParameters().get(IcebergTable.ICEBERG_CATALOG));
      }
      if (tbl.getParameters().containsKey(IcebergTable.ICEBERG_TABLE_IDENTIFIER)) {
        tbl.getParameters()
            .put(IcebergTable.ICEBERG_TABLE_IDENTIFIER, identifier.toString());
        tableProperties.put(IcebergTable.ICEBERG_TABLE_IDENTIFIER, identifier.toString());
      }
      List<TColumn> columns = new ArrayList<>();
      for (Column col: srcIceTable.getColumns()) columns.add(col.toThrift());
      TIcebergPartitionSpec partitionSpec = srcIceTable.getDefaultPartitionSpec()
          .toThrift();
      createIcebergTable(tbl, wantMinimalResult, response, catalogTimeline,
          params.if_not_exists, columns, partitionSpec,
          Lists.newArrayList(srcIceTable.getIcebergSchema().identifierFieldNames()),
          tableProperties, params.getComment(), debugAction);
    } else if (srcTable instanceof KuduTable && KuduTable.isKuduTable(tbl)) {
      TCreateTableParams createTableParams =
          extractKuduCreateTableParams(params, tblName, (KuduTable) srcTable, tbl);
      createKuduTable(tbl, createTableParams, wantMinimalResult, response,
          catalogTimeline);
    } else {
      MetastoreShim.setTableLocation(catalog_.getDb(tbl.getDbName()), tbl);
      createTable(tbl, params.if_not_exists, null, params.server_name, null, null,
          wantMinimalResult, response, catalogTimeline);
    }
  }

  /**
   * Build TCreateTableParams by source
   */
  private TCreateTableParams extractKuduCreateTableParams(TCreateTableLikeParams params,
      TableName tblName, KuduTable kuduTable,
      org.apache.hadoop.hive.metastore.api.Table tbl) throws ImpalaRuntimeException {
    TCreateTableParams createTableParams = new TCreateTableParams();
    createTableParams.if_not_exists = params.if_not_exists;
    createTableParams.setComment(params.getComment());
    List<TColumn> columns = new ArrayList<>();
    for (Column col : kuduTable.getColumns()) {
      // Omit cloning auto-incrementing column of Kudu table since the column will be
      // created by Kudu engine.
      if (((KuduColumn) col).isAutoIncrementing()) continue;
      columns.add(col.toThrift());
    }
    createTableParams.setColumns(columns);
    // Omit auto-incrementing column as primary key.
    List<String> primaryColumnNames =
        new ArrayList<>(kuduTable.getPrimaryKeyColumnNames());
    if (kuduTable.hasAutoIncrementingColumn()) {
      primaryColumnNames.remove(KuduUtil.getAutoIncrementingColumnName());
    }
    createTableParams.setPrimary_key_column_names(primaryColumnNames);

    List<TKuduPartitionParam> partitionParams = new ArrayList<>();
    for (KuduPartitionParam kuduPartitionParam : kuduTable.getPartitionBy()) {
      partitionParams.add(kuduPartitionParam.toThrift());
    }
    createTableParams.setPartition_by(partitionParams);

    Map<String, String> tableProperties = tbl.getParameters();
    tableProperties.remove(KuduTable.KEY_TABLE_NAME);
    tableProperties.remove(KuduTable.KEY_TABLE_ID);

    String kuduMasters = tbl.getParameters().get(KuduTable.KEY_MASTER_HOSTS);
    boolean isKuduHmsIntegrationEnabled = KuduTable.isHMSIntegrationEnabled(kuduMasters);
    tableProperties.put(KuduTable.KEY_TABLE_NAME,
        KuduUtil.getDefaultKuduTableName(
            tblName.getDb(), tblName.getTbl(), isKuduHmsIntegrationEnabled));
    tbl.setParameters(tableProperties);
    return createTableParams;
  }

  private static void setDefaultTableCapabilities(
      org.apache.hadoop.hive.metastore.api.Table tbl) {
    if (MetastoreShim.getMajorVersion() > 2) {
      // This hms table is for create table.
      // It needs read/write access type,  not default value(0, undefined)
      MetastoreShim.setTableAccessType(tbl, ACCESSTYPE_READWRITE);
      // Set table default capabilities in HMS
      if (tbl.getParameters().containsKey(CAPABILITIES_KEY)) return;
      if (AcidUtils.isTransactionalTable(tbl.getParameters())) {
        if (AcidUtils.isFullAcidTable(tbl.getParameters())) {
          tbl.getParameters().put(CAPABILITIES_KEY, FULLACID_CAPABILITIES);
        } else {
          tbl.getParameters().put(CAPABILITIES_KEY, ACIDINSERTONLY_CAPABILITIES);
        }
      } else {
        // Managed KUDU table has issues with extra table properties:
        // 1. The property is not stored. 2. The table cannot be found after created.
        // Related jira: IMPALA-8751
        // Skip adding default capabilities for KUDU tables before the issues are fixed.
        if (!KuduTable.isKuduTable(tbl)) {
          tbl.getParameters().put(CAPABILITIES_KEY, NONACID_CAPABILITIES);
        }
      }
    }
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
    if (params.getTblproperties() != null && params.getTblpropertiesSize() != 0) {
      view.getParameters().putAll(params.getTblproperties());
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
    if (params.getTblproperties() != null && params.getTblpropertiesSize() != 0) {
      view.getParameters().putAll(params.getTblproperties());
    }
    // Add all the columns to a new storage descriptor.
    view.getSd().setCols(buildFieldSchemaList(params.getColumns()));
  }

  /**
   * Appends one or more columns to the given table. Returns true if there a column was
   * added; false otherwise.
   */
  private boolean alterTableAddCols(Table tbl, List<TColumn> columns, boolean ifNotExists,
      EventSequence catalogTimeline, InProgressTableModification modification)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
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
      applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
      return true;
    }
    return false;
  }

  /**
   * Replaces all existing columns to the given table.
   */
  private void alterTableReplaceCols(Table tbl, List<TColumn> columns,
      EventSequence catalogTimeline, InProgressTableModification modification)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
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
    applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
  }

  /**
   * Changes the column definition of an existing column. This can be used to rename a
   * column, add a comment to a column, or change the datatype of a column.
   */
  private void alterTableAlterCol(Table tbl, String colName, TColumn newCol,
      EventSequence catalogTimeline, InProgressTableModification modification)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
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
    applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
  }

  /**
   * Adds new partitions to the given table in HMS. Also creates and adds new
   * HdfsPartitions to the corresponding HdfsTable.
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
  private void alterTableAddPartitions(Table tbl,
      TAlterTableAddPartitionParams addPartParams, THdfsFileFormat fileFormat,
      EventSequence catalogTimeline, InProgressTableModification modification,
      String debugAction) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());

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

      if (fileFormat != null) {
        setStorageDescriptorFileFormat(hmsPartition.getSd(), fileFormat);
      }

      THdfsCachingOp cacheOp = partParams.getCache_op();
      if (cacheOp != null) partitionCachingOpMap.put(hmsPartition.getValues(), cacheOp);
    }

    if (allHmsPartitionsToAdd.isEmpty()) return;

    List<Partition> difference = null;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      Map<String, Long> partitionToEventId = Maps.newHashMap();
      List<Partition> addedHmsPartitions = addHmsPartitionsInTransaction(msClient,
          tbl, allHmsPartitionsToAdd, partitionToEventId, ifNotExists, catalogTimeline,
          debugAction);
      // Handle HDFS cache. This is done in a separate round bacause we have to apply
      // caching only to newly added partitions.
      alterTableCachePartitions(msTbl, msClient, tableName, addedHmsPartitions,
          partitionCachingOpMap, catalogTimeline);

      // If 'ifNotExists' is true, add_partitions() may fail to add all the partitions to
      // HMS because some of them may already exist there. In that case, we load in the
      // catalog the partitions that already exist in HMS but aren't in the catalog yet.
      if (allHmsPartitionsToAdd.size() != addedHmsPartitions.size()) {
        difference = computeDifference(allHmsPartitionsToAdd, addedHmsPartitions);
        addedHmsPartitions.addAll(
            getPartitionsFromHms(msTbl, msClient, difference));
      }
      if (addedHmsPartitions.isEmpty() && difference != null) {
        // getPartitionsFromHms() must have silently failed if addedHmsPartitions stays
        // empty. No need to continue.
        LOG.info("{} partitions already exist in HMS, but unable to be retrieved.",
            difference.size());
        return;
      }
      addHdfsPartitions(msClient, tbl, addedHmsPartitions, partitionToEventId,
          catalogTimeline);
    }

    // the alter table event is only generated when we add the partition. For
    // instance if not exists clause is provided and the partition is
    // pre-existing there is no alter table event generated. Hence we should
    // only add the versions for in-flight events when we are sure that the
    // partition was really added.
    modification.registerInflightEvent();
    modification.updateTableCatalogVersion();
    // mark completion here because there will be no metadata reload after this.
    modification.markInflightEventRegistrationComplete();
  }

  /**
   * Adds partition if table exists, is loaded and if the partitions have not been
   * removed since the event is generated. This method uses the deleteEventLog to
   * determine if the partitions which need to be added have not been removed since
   * the event was generated.
   * @param eventId the EventId being processed
   * @param dbName The database name for the table where partition needs to be added
   * @param tblName The table for which partitions need to be added.
   * @param partitions List of partitions from the event to be added.
   * @param reason Reason for adding the partitions. Useful for logging purposes.
   * @return The number of partitions which were added.
   * @throws CatalogException if partition reload threw an error.
   * @throws DatabaseNotFoundException if Db doesn't exist.
   * @throws TableNotFoundException if table doesn't exist.
   */
  public int addPartitionsIfNotRemovedLater(long eventId, String dbName,
      String tblName, List<Partition> partitions, String reason)
      throws CatalogException {
    Table table;
    try {
      table = catalog_.getTable(dbName, tblName);
    } catch (DatabaseNotFoundException e) {
      LOG.info("EventId: {} Not adding partitions since the database {} "
              + "does not exist anymore.", eventId, dbName);
      return 0;
    }
    if (table == null) {
      LOG.info("EventId: {} Not adding partitions since the table {}.{} "
              + "does not exist anymore.", eventId, dbName, tblName);
      return 0;
    }
    if (table instanceof IncompleteTable) {
      LOG.info("EventId: {} Table {} is not loaded. Skipping add partitions", eventId,
          table.getFullName());
      return 0;
    }
    if (table instanceof IcebergTable) {
      LOG.info("EventId: {} Table {} is an Iceberg table. Partitioning is handled by " +
          "Iceberg. Skipping add partitions", eventId, table.getFullName());
      return 0;
    }
    if(!(table instanceof HdfsTable)) {
      throw new CatalogException(
          "Partition event " + eventId + " received on a non-hdfs table");
    }
    boolean syncToLatestEventId =
        BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();
    try {
      tryWriteLock(table, reason, NoOpEventSequence.INSTANCE);
      if (syncToLatestEventId && table.getLastSyncedEventId() >= eventId) {
        LOG.info("Not adding partitions from event id: {} since table {} is already "
                + "synced till event id {}", eventId, table.getFullName(),
            table.getLastSyncedEventId());
        return 0;
      }
      InProgressTableModification modification =
          new InProgressTableModification(catalog_, table);
      catalog_.getLock().writeLock().unlock();
      HdfsTable hdfsTable = (HdfsTable) table;
      List<Partition> partitionsToAdd = filterPartitionsToAddFromEvent(eventId, hdfsTable,
          partitions);
      int partitionsAdded = 0;
      if (!partitionsToAdd.isEmpty()) {
        LOG.debug("Found {}/{} partitions to add in table {} from event {}",
            partitionsToAdd.size(), partitions.size(), table.getFullName(), eventId);
        Map<String, Long> partToEventId = Maps.newHashMap();
        for (Partition part : partitionsToAdd) {
          partToEventId
              .put(FeCatalogUtils.getPartitionName(hdfsTable, part.getValues()), eventId);
        }
        try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
          addHdfsPartitions(metaStoreClient, table, partitionsToAdd, partToEventId,
              NoOpEventSequence.INSTANCE);
        }
        modification.updateTableCatalogVersion();
        partitionsAdded = partitionsToAdd.size();
      }
      if (syncToLatestEventId) {
        table.setLastSyncedEventId(eventId);
      }
      modification.validateInProgressModificationComplete();
      return partitionsAdded;
    } catch (InternalException | UnsupportedEncodingException e) {
      throw new CatalogException(
          "Unable to add partition for table " + table.getFullName(), e);
    } finally {
      UnlockWriteLockIfErronouslyLocked();
      if (table.isWriteLockedByCurrentThread()) {
        table.releaseWriteLock();
      }
    }
  }

  /**
   * Filters out the partitions which need to be added in catalogd based on the delete
   * event log. If the some of the partitions which are being added have been removed
   * since the event was generated, those are filtered out.
   * @return List of partitions which need to be added.
   */
  private List<Partition> filterPartitionsToAddFromEvent(long eventId,
      HdfsTable hdfsTable, List<Partition> partitions)
      throws UnsupportedEncodingException, CatalogException {
    Preconditions.checkNotNull(hdfsTable);
    // it is possible that when the event is received, the table was dropped
    // and recreated. Hence we should make sure that the deleteEventLog
    // does not have the table removed after this eventid
    List<Partition> partsToBeAdded = Lists.newArrayList();
    org.apache.hadoop.hive.metastore.api.Table msTbl = hdfsTable.getMetaStoreTable();
    DeleteEventLog deleteEventLog = catalog_.getMetastoreEventProcessor()
        .getDeleteEventLog();
    if (deleteEventLog.wasRemovedAfter(eventId,
        DeleteEventLog.getDbKey(msTbl.getDbName()))) {
      LOG.info(
          "EventId: {} Not adding partitions since the database {} was removed later",
          eventId, msTbl.getDbName());
      return partsToBeAdded;
    }
    // check if the table is removed since the event was generated.
    if (deleteEventLog.wasRemovedAfter(eventId,
        DeleteEventLog.getTblKey(msTbl.getDbName(), msTbl.getTableName()))) {
      LOG.info(
          "EventId: {} Not adding partitions since the table {} was removed later",
          eventId, hdfsTable.getFullName());
      return partsToBeAdded;
    }
    Preconditions.checkState(!partitions.isEmpty());
    for (Partition part : partitions) {
      List<LiteralExpr> partExprs = FeCatalogUtils
          .parsePartitionKeyValues(hdfsTable, part.getValues());
      HdfsPartition hdfsPartition = hdfsTable.getPartition(partExprs);
      // if partition is not present and if it was not removed later
      // it can be added.
      if (hdfsPartition == null) {
        boolean removed = deleteEventLog.wasRemovedAfter(eventId,
            DeleteEventLog.getPartitionKey(hdfsTable, part.getValues()));
        // it is possible that the partition doesn't exist anymore because it was removed
        // later
        if (removed) {
          LOG.info(
              "EventId: {} Skipping addition of partition {} since it was removed later"
                  + " in catalog for table {}",
              eventId,
              FileUtils.makePartName(hdfsTable.getClusteringColNames(), part.getValues()),
              hdfsTable.getFullName());
        } else {
          partsToBeAdded.add(part);
        }
      }
    }
    return partsToBeAdded;
  }

  /**
   * Remove the partitions from the event if the table exists, is loaded and if the
   * partitions have not been added again since the event was generated.
   * @param eventId The eventId being processed.
   * @param dbName Database name of the table whose partitions need to be removed.
   * @param tblName Table name whose partitions need to be removed.
   * @param droppedPartitions List of mapping of partition key to value for all the
   *                          partitions in the event.
   * @param reason Reason for removing the partitions for logging.
   * @return Number of partitions which were removed from the table.
   * @throws CatalogException
   */
  public int removePartitionsIfNotAddedLater(long eventId,
      String dbName, String tblName, List<Map<String, String>> droppedPartitions,
      String reason) throws CatalogException {
    Table table;
    try {
      table = catalog_.getTable(dbName, tblName);
    } catch (DatabaseNotFoundException e) {
      LOG.info("EventId: {} Not removing partitions since the database {}"
              + " does not exist anymore.", eventId, dbName);
      return 0;
    }
    if (table == null) {
      LOG.info("EventId: {} Not dropping partitions since the table {}.{} "
              + "does not exist anymore", eventId, dbName, tblName);
      return 0;
    }
    if (table instanceof IncompleteTable) {
      LOG.info("EventId: {} Table {} is not loaded. Not processing the event.",
          eventId, table.getFullName());
      return 0;
    }
    if (!(table instanceof HdfsTable)) {
      throw new CatalogException("Partition event received on a non-hdfs table");
    }
    boolean syncToLatestEventId =
        BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();

    boolean errorOccured = false;
    try {
      tryWriteLock(table, reason, NoOpEventSequence.INSTANCE);
      if (syncToLatestEventId && table.getLastSyncedEventId() >= eventId) {
        LOG.info("Not dropping partitions from event id: {} since table {} is already "
                + "synced till event id {}", eventId, table.getFullName(),
            table.getLastSyncedEventId());
        return 0;
      }
      InProgressTableModification modification =
          new InProgressTableModification(catalog_, table);
      catalog_.getLock().writeLock().unlock();
      HdfsTable hdfsTable = (HdfsTable) table;
      // check if the table or database has been dropped and recreated since the
      // eventId, if yes, we should not process this event.
      if (eventId <= hdfsTable.getCreateEventId()) {
        LOG.info(
            "EventId: {} Not dropping partitions table's create event id is {}",
            eventId, hdfsTable.getCreateEventId());
        return 0;
      }
      List<Map<String, String>> skippedPartitions = Lists.newArrayList();
      for (Map<String, String> partKeyVals : droppedPartitions) {
        if (!canDropPartitionFromEvent(eventId, hdfsTable,
            Lists.newArrayList(partKeyVals.values()))) {
          skippedPartitions.add(partKeyVals);
        }
      }
      droppedPartitions.removeAll(skippedPartitions);
      if (droppedPartitions.isEmpty()) {
        return 0;
      } else {
        LOG.info(
            "EventId: {} Skipping removal of {}/{} partitions since they don't exist or"
                + " were created later in table {}.", eventId, skippedPartitions.size(),
            droppedPartitions.size(), table.getFullName());
      }
      List<List<TPartitionKeyValue>> allTPartKeyVals = Lists
          .newArrayListWithCapacity(droppedPartitions.size());
      List<Column> partitionCols = hdfsTable.getClusteringColumns();
      for (Map<String, String> droppedPartitionKeyVals : droppedPartitions) {
        List<TPartitionKeyValue> tPartKeyVals = Lists
            .newArrayListWithCapacity(partitionCols.size());
        for (Column partitionCol : partitionCols) {
          String val = droppedPartitionKeyVals.get(partitionCol.getName());
          if (val == null) {
            // the event doesn't have the partition value for the key
            throw new CatalogException(
                String.format(
                    "Event does not contain partition value for key %s. Event contains "
                        + "%s", partitionCol.getName(),
                    Joiner.on(",").withKeyValueSeparator("=")
                        .join(droppedPartitionKeyVals)));
          }
          tPartKeyVals.add(new TPartitionKeyValue(partitionCol.getName(), val));
        }
        allTPartKeyVals.add(tPartKeyVals);
      }
      Preconditions.checkState(!allTPartKeyVals.isEmpty());
      catalog_.dropPartitions(table, allTPartKeyVals);
      modification.updateTableCatalogVersion();
      modification.validateInProgressModificationComplete();
      return allTPartKeyVals.size();
    } catch (InternalException e) {
      errorOccured = true;
      throw new CatalogException(
          "Unable to add partition for table " + table.getFullName(), e);
    } finally {
      //  set table's last sycned event id  if no error occurred and
      //  table's last synced event id < current event id
      if (!errorOccured && syncToLatestEventId &&
          table.getLastSyncedEventId() < eventId) {
        table.setLastSyncedEventId(eventId);
      }
      UnlockWriteLockIfErronouslyLocked();
      table.releaseWriteLock();
    }
  }

  /**
   * Checks if the partition represented by its values from the event can be dropped
   * from catalog or not based on it's createEventId.
   * @return True if the partition can be dropped, else false.
   */
  private boolean canDropPartitionFromEvent(long eventId, HdfsTable hdfsTable,
      List<String> values) throws CatalogException {
    List<LiteralExpr> partExprs = FeCatalogUtils
        .parsePartitionKeyValues(hdfsTable, values);
    HdfsPartition hdfsPartition = hdfsTable.getPartition(partExprs);
    // partition doesn't exist
    if (hdfsPartition == null) {
      return false;
    }
    // if the partition has been created since the event was generated, skip
    // the stale event.
    boolean isStale = hdfsPartition.getCreateEventId() > eventId;
    LOG.info("{} partition {} of table {} since it's create event id {} is {} than " +
            "eventid {}",
        isStale ? "Not dropping" : "Dropping",
        hdfsPartition.getPartitionName(), hdfsTable.getFullName(),
        hdfsPartition.getCreateEventId(), isStale ? "higher" : "not higher",
        eventId);
    return !isStale;
  }

  /**
   * Reloads the given partitions if they exist and have not been removed since the event
   * was generated.
   *
   * @param eventId EventId of the event being processed.
   * @param eventType EventType of the event being processed.
   * @param dbName Database name for the partition
   * @param tblName Table name for the partition
   * @param partsFromEvent List of {@link Partition} objects from the events to be
   *                       reloaded.
   * @param reason Reason for reloading the partitions for logging purposes.
   * @param fileMetadataLoadOpts describes how to reload file metadata for partsFromEvent
   * @return the number of partitions which were reloaded. If the table does not exist,
   * returns 0. Some partitions could be skipped if they don't exist anymore.
   */
  public int reloadPartitionsIfExist(long eventId, String eventType, String dbName,
      String tblName, List<Partition> partsFromEvent, String reason,
      FileMetadataLoadOpts fileMetadataLoadOpts) throws CatalogException {
    List<String> partNames = new ArrayList<>();
    Table table = catalog_.getTable(dbName, tblName);
    if (table instanceof HdfsTable) {
      HdfsTable hdfsTable = (HdfsTable) table;
      for (Partition part : partsFromEvent) {
        partNames.add(FileUtils.makePartName(hdfsTable.getClusteringColNames(),
            part.getValues(), null));
      }
    }
    return reloadPartitionsFromNamesIfExists(eventId, eventType, dbName, tblName,
        partNames, reason, fileMetadataLoadOpts);
  }

  /**
   * Reloads the given partitions from partiton names if they exist and have not been
   * removed since the event was generated.
   *
   * @param eventId EventId being processed.
   * @param dbName Database name for the partition
   * @param tblName Table name for the partition
   * @param partNames List of partition names from the events to be reloaded.
   * @param reason Reason for reloading the partitions for logging purposes.
   * @param fileMetadataLoadOpts describes how to reload file metadata for partsFromEvent
   * @return the number of partitions which were reloaded. If the table does not exist,
   * returns 0. Some partitions could be skipped if they don't exist anymore.
   */
  public int reloadPartitionsFromNamesIfExists(long eventId, String eventType,
      String dbName, String tblName, List<String> partNames, String reason,
      FileMetadataLoadOpts fileMetadataLoadOpts) throws CatalogException {
    Table table = catalog_.getTable(dbName, tblName);
    if (table == null) {
      DeleteEventLog deleteEventLog = catalog_.getMetastoreEventProcessor()
          .getDeleteEventLog();
      if (deleteEventLog
          .wasRemovedAfter(eventId, DeleteEventLog.getTblKey(dbName, tblName))) {
        LOG.info("EventId: {} EventType: {} Not reloading the partition of table {}.{} " +
            "since it was removed later in catalog", eventId, eventType, dbName, tblName);
        return 0;
      } else {
        throw new TableNotFoundException(
            "Table " + dbName + "." + tblName + " not found");
      }
    }
    if (table instanceof IncompleteTable) {
      LOG.info("Table {} is not loaded. Skipping {} event {}",
          table.getFullName(), eventType, eventId);
      return 0;
    }
    if (!(table instanceof HdfsTable)) {
      throw new CatalogException("Partition event received on a non-hdfs table");
    }
    boolean syncToLatestEventId =
        BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();

    boolean errorOccured = false;
    try {
      tryWriteLock(table, reason, NoOpEventSequence.INSTANCE);
      InProgressTableModification modification =
          new InProgressTableModification(catalog_, table);
      catalog_.getLock().writeLock().unlock();
      if (syncToLatestEventId && table.getLastSyncedEventId() >= eventId) {
        LOG.info("EventId: {} EventType: {} Not reloading partition since table {} is " +
                "already synced till event id {}",
            eventId, eventType, table.getFullName(), table.getLastSyncedEventId());
        return 0;
      }
      HdfsTable hdfsTable = (HdfsTable) table;
      // some partitions from the event or the table itself
      // may not exist in HMS anymore. Hence, we re-fetch
      // the partitions from HMS.
      int numOfPartsReloaded;
      try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
        numOfPartsReloaded = hdfsTable.reloadPartitionsFromNames(
            metaStoreClient.getHiveClient(), partNames, reason, fileMetadataLoadOpts);
      }
      modification.updateTableCatalogVersion();
      modification.validateInProgressModificationComplete();
      return numOfPartsReloaded;
    } catch (TableLoadingException e) {
      LOG.info("EventId: {} EventType: {} Could not reload {} partitions of table {}",
          eventId, eventType, partNames.size(), table.getFullName(), e);
    } catch (InternalException e) {
      errorOccured = true;
      throw new CatalogException(
          "Could not acquire lock on the table " + table.getFullName(), e);
    } finally {
      //  set table's last sycned event id  if no error occurred and
      //  table's last synced event id < current event id
      if (!errorOccured && syncToLatestEventId &&
          table.getLastSyncedEventId() < eventId) {
        table.setLastSyncedEventId(eventId);
      }
      UnlockWriteLockIfErronouslyLocked();
      table.releaseWriteLock();
    }
    return 0;
  }

  /**
   * Reloads the given partitions if they exist and have not been removed since the event
   * was generated. We don't retrieve partitions from HMS but use partitions from event.
   * This api does NOT reload file metadata when reloading partitions
   *
   * @param eventId EventId being processed.
   * @param dbName Database name for the partition
   * @param tblName Table name for the partition
   * @param partsFromEvent List of {@link Partition} objects from the events to be
   *                       reloaded.
   * @param reason Reason for reloading the partitions for logging purposes.
   * @return the number of partitions which were reloaded. If the table does not exist,
   * returns 0. Some partitions could be skipped if they don't exist anymore.
   */
  public int reloadPartitionsFromEvent(long eventId, String dbName, String tblName,
      List<Partition> partsFromEvent, String reason)
      throws CatalogException {
    Table table = catalog_.getTable(dbName, tblName);
    if (table == null) {
      DeleteEventLog deleteEventLog = catalog_.getMetastoreEventProcessor()
          .getDeleteEventLog();
      if (deleteEventLog
          .wasRemovedAfter(eventId, DeleteEventLog.getTblKey(dbName, tblName))) {
        LOG.info(
            "Not reloading the partition of table {} since it was removed "
                + "later in catalog", new TableName(dbName, tblName));
        return 0;
      } else {
        throw new TableNotFoundException(
            "Table " + dbName + "." + tblName + " not found");
      }
    }
    if (table instanceof IncompleteTable) {
      LOG.info("Table {} is not loaded. Skipping drop partition event {}",
          table.getFullName(), eventId);
      return 0;
    }
    if (!(table instanceof HdfsTable)) {
      throw new CatalogException("Partition event received on a non-hdfs table");
    }
    if (eventId > 0 && eventId <= table.getCreateEventId()) {
      LOG.debug("Not reloading partitions of table {}.{} for event {} since it is " +
          "recreated at event {}.", dbName, tblName, eventId, table.getCreateEventId());
      return 0;
    }
    try {
      tryWriteLock(table, reason, NoOpEventSequence.INSTANCE);
      InProgressTableModification modification =
          new InProgressTableModification(catalog_, table);
      catalog_.getLock().writeLock().unlock();
      HdfsTable hdfsTable = (HdfsTable) table;
      int numOfPartsReloaded;
      try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
        numOfPartsReloaded = hdfsTable.reloadPartitionsFromEvent(
            metaStoreClient.getHiveClient(), partsFromEvent, false, reason);
      }
      modification.updateTableCatalogVersion();
      modification.validateInProgressModificationComplete();
      return numOfPartsReloaded;
    } catch (InternalException e) {
      throw new CatalogException(
          "Could not acquire lock on the table " + table.getFullName(), e);
    } finally {
      UnlockWriteLockIfErronouslyLocked();
      table.releaseWriteLock();
    }
  }

  /**
   * This function is only used by CommitTxnEvent to mark write ids as committed and
   * reload partitions from events atomically.
   *
   * @param eventId EventId being processed
   * @param dbName Database name for the partition
   * @param tblName Table name for the partition
   * @param writeIds List of write ids for this transaction
   * @param partsFromEvent List of Partition objects from the events to be reloaded
   * @param reason Reason for reloading the partitions for logging purposes.
   * @return the number of partitions which were reloaded. Some partitions can be
   * skipped if they don't exist anymore, or they have stale write ids.
   */
  public int addCommittedWriteIdsAndReloadPartitionsIfExist(long eventId, String dbName,
      String tblName, List<Long> writeIds, List<Partition> partsFromEvent, String reason)
      throws CatalogException {
    Table table = catalog_.getTable(dbName, tblName);
    if (table == null) {
      DeleteEventLog deleteEventLog = catalog_.getMetastoreEventProcessor()
          .getDeleteEventLog();
      if (deleteEventLog
          .wasRemovedAfter(eventId, DeleteEventLog.getTblKey(dbName, tblName))) {
        LOG.info(
            "Not reloading partitions of table {} for event {} since it was removed "
                + "later in catalog", new TableName(dbName, tblName), eventId);
        return 0;
      } else {
        throw new TableNotFoundException(
            "Table " + dbName + "." + tblName + " not found");
      }
    }
    if (table instanceof IncompleteTable) {
      LOG.info("Table {} is not loaded. Skipping partition event {}",
          table.getFullName(), eventId);
      return 0;
    }
    if (!(table instanceof HdfsTable)) {
      throw new CatalogException("Partition event received on a non-hdfs table");
    }

    HdfsTable hdfsTable = (HdfsTable) table;
    ValidWriteIdList previousWriteIdList = hdfsTable.getValidWriteIds();
    try {
      tryWriteLock(table, reason, NoOpEventSequence.INSTANCE);
      InProgressTableModification modification =
          new InProgressTableModification(catalog_, hdfsTable);
      catalog_.getLock().writeLock().unlock();
      boolean syncToLatestEvent =
          BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();
      if (hdfsTable.getLastSyncedEventId() > eventId) {
        LOG.info("EventId: {}, Skipping addition of committed writeIds and partitions"
            + " reload for table {} since it is already synced till eventId: {}",
            eventId, hdfsTable.getFullName(), hdfsTable.getLastSyncedEventId());
        return 0;
      }
      Preconditions.checkState(previousWriteIdList != null,
          "Write id list of table %s should not be null", table.getFullName());
      // get a copy of previous write id list
      previousWriteIdList = MetastoreShim.getValidWriteIdListFromString(
          previousWriteIdList.toString());
      // some partitions from the event or the table itself
      // may not exist in HMS anymore. Hence, we collect the names here and re-fetch
      // the partitions from HMS.
      List<Partition> partsToRefresh = new ArrayList<>();
      List<Long> writeIdsToRefresh = new ArrayList<>();
      ListIterator<Partition> it = partsFromEvent.listIterator();
      while (it.hasNext()) {
        // The partition objects from HMS event was persisted when transaction was not
        // committed, so its write id is smaller than the write id of the write event.
        // Since the event is committed at this point, we need to update the partition
        // object's write id by event's write id.
        long writeId = writeIds.get(it.nextIndex());
        Partition part = it.next();
        // Aborted write id is not allowed. The write id can be committed if the table
        // in cache is ahead of this commit event.
        Preconditions.checkState(!previousWriteIdList.isWriteIdAborted(writeId),
            "Write id %d of Table %s should not be aborted",
            writeId, table.getFullName());
        // Valid write id means committed write id here.
        if (!previousWriteIdList.isWriteIdValid(writeId)) {
          MetastoreShim.setWriteIdToMSPartition(part, writeId);
          partsToRefresh.add(part);
          writeIdsToRefresh.add(writeId);
        }
      }
      if (partsToRefresh.isEmpty()) {
        LOG.info("Not reloading partitions of table {} for event {} since the cache is "
            + "already up-to-date", table.getFullName(), eventId);
        if (syncToLatestEvent) {
          hdfsTable.setLastSyncedEventId(eventId);
        }
        return 0;
      }
      // set write id as committed before reload the partitions so that we can get
      // up-to-date filemetadata.
      hdfsTable.addWriteIds(writeIdsToRefresh,
          MutableValidWriteIdList.WriteIdStatus.COMMITTED);
      int numOfPartsReloaded;
      try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
        numOfPartsReloaded = hdfsTable.reloadPartitionsFromEvent(
            metaStoreClient.getHiveClient(), partsToRefresh, true, reason);
      }
      modification.updateTableCatalogVersion();
      if (syncToLatestEvent) {
        hdfsTable.setLastSyncedEventId(eventId);
      }
      modification.validateInProgressModificationComplete();
      return numOfPartsReloaded;
    } catch (InternalException e) {
      throw new CatalogException(
          "Could not acquire lock on the table " + table.getFullName(), e);
    } catch (Exception e) {
      LOG.info("Rolling back the write id list of table {} because reloading "
          + "for event {} is failed: {}", table.getFullName(), eventId, e.getMessage());
      // roll back the original writeIdList
      hdfsTable.setValidWriteIds(previousWriteIdList);
      throw e;
    } finally {
      UnlockWriteLockIfErronouslyLocked();
      table.releaseWriteLock();
    }
  }

  public ReentrantLock getMetastoreDdlLock() {
    return metastoreDdlLock_;
  }

  /**
   * Acquires 'metastoreDdlLock_' and also updates 'catalogTimeline' when it's done.
   */
  private void acquireMetastoreDdlLock(EventSequence catalogTimeline) {
    metastoreDdlLock_.lock();
    catalogTimeline.markEvent(GOT_METASTORE_DDL_LOCK);
  }

  /**
   * Adds partitions in 'allHmsPartitionsToAdd' in batches via 'msClient'.
   * Returns the created partitions.
   */
  private List<Partition> addHmsPartitions(MetaStoreClient msClient,
      Table tbl, List<Partition> allHmsPartitionsToAdd,
      Map<String, Long> partitionToEventId, boolean ifNotExists,
      EventSequence catalogTimeline) throws ImpalaRuntimeException, CatalogException {
    long eventId = getCurrentEventId(msClient, catalogTimeline);
    List<Partition> addedHmsPartitions = Lists
        .newArrayListWithCapacity(allHmsPartitionsToAdd.size());
    long numDone = 0;
    for (List<Partition> hmsSublist :
        Lists.partition(allHmsPartitionsToAdd, MAX_PARTITION_UPDATES_PER_RPC)) {
      try {
        List<Partition> addedPartitions = MetaStoreUtil.addPartitions(
            msClient.getHiveClient(), tbl.getMetaStoreTable(),
            hmsSublist, ifNotExists, true);
        numDone += hmsSublist.size();
        catalogTimeline.markEvent(String.format(
            "Added %d partitions in Metastore", numDone));
        LOG.info("Added {}/{} partitions in HMS for table {}", numDone,
            allHmsPartitionsToAdd.size(), tbl.getFullName());
        org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable();
        List<NotificationEvent> events = getNextMetastoreEventsForTableIfEnabled(
            catalogTimeline, eventId, msTbl.getDbName(), msTbl.getTableName(),
            AddPartitionEvent.EVENT_TYPE);
        Map<Partition, Long> partitionToEventSubMap = Maps.newHashMap();
        getPartitionsFromEvent(events, partitionToEventSubMap);
        // set the eventId to last one which we received so the we fetch the next
        // set of events correctly
        if (!events.isEmpty()) {
          eventId = events.get(events.size() - 1).getEventId();
        }
        if (partitionToEventSubMap.isEmpty()) {
          // if partitions couldn't be fetched from events, use the one returned by
          // add_partitions call above.
          addedHmsPartitions.addAll(addedPartitions);
        } else {
          addedHmsPartitions.addAll(partitionToEventSubMap.keySet());
          // we cannot keep a mapping of Partition to event ids because the
          // partition objects are changed later in the cachePartitions code path.
          // hence it better to map the partitionName to eventId since partitionName
          // remains unchanged.
          for (Partition part : partitionToEventSubMap.keySet()) {
            partitionToEventId
                .put(FeCatalogUtils.getPartitionName((FeFsTable) tbl, part.getValues()),
                    partitionToEventSubMap.get(part));
          }
        }
      } catch (MetastoreNotificationException | TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "add_partitions"), e);
      }
    }
    return addedHmsPartitions;
  }

  /**
   * Invokes addHmsPartitions() in transaction for transactional tables. For
   * non-transactional tables it just simply invokes addHmsPartitions().
   * Please note that once addHmsPartitions() succeeded, then even if the transaction
   * fails, the HMS table modification won't be reverted.
   * Returns the list of the newly added partitions.
   */
  private List<Partition> addHmsPartitionsInTransaction(MetaStoreClient msClient,
      Table tbl, List<Partition> partitions, Map<String, Long> partitionToEventId,
      boolean ifNotExists, EventSequence catalogTimeline, String debugAction)
      throws ImpalaException {
    if (!AcidUtils.isTransactionalTable(tbl.getMetaStoreTable().getParameters())) {
      if (DebugUtils.hasDebugAction(debugAction, DebugUtils.ENABLE_EVENT_PROCESSOR)) {
        catalog_.startEventsProcessor();
      }
      return addHmsPartitions(msClient, tbl, partitions, partitionToEventId,
          ifNotExists, catalogTimeline);
    }
    try (Transaction txn = new Transaction(
        msClient.getHiveClient(),
        catalog_.getAcidUserId(),
        String.format("ADD PARTITION for %s", tbl.getFullName()))) {
      MetastoreShim.allocateTableWriteId(msClient.getHiveClient(), txn.getId(),
          tbl.getDb().getName(), tbl.getName());
      List<Partition> ret = addHmsPartitions(msClient, tbl, partitions,
          partitionToEventId, ifNotExists, catalogTimeline);
      txn.commit();
      return ret;
    }
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
      List<Partition> hmsPartitions) throws ImpalaException {
    List<String> partitionCols = Lists.newArrayList();
    for (FieldSchema fs: msTbl.getPartitionKeys()) partitionCols.add(fs.getName());

    List<String> partitionNames = Lists.newArrayListWithCapacity(hmsPartitions.size());
    for (Partition part: hmsPartitions) {
      String partName = org.apache.hadoop.hive.common.FileUtils.makePartName(
          partitionCols, part.getValues());
      partitionNames.add(partName);
    }
    try {
      return MetaStoreUtil.fetchPartitionsByName(msClient.getHiveClient(),
          partitionNames, msTbl);
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
      Map<List<String>, THdfsCachingOp> partitionCachingOpMap,
      EventSequence catalogTimeline) throws ImpalaException {
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
      for (Partition part : hmsPartitionsToCache) {
        addCatalogServiceIdentifiers(msTbl, part);
      }
      applyAlterHmsPartitions(msClient, tableName, hmsPartitionsToCache, catalogTimeline);
      catalog_.watchCacheDirs(cacheIds, tableName.toThrift(),
         "ALTER TABLE CACHE PARTITIONS");
    }
  }

  /**
   * Drops existing partitions from the given table in Hive. If a partition is cached,
   * the associated cache directive will also be removed.
   * Also drops the corresponding partitions from its Hdfs table.
   * If purge is true, partition data is
   * permanently deleted. numUpdatedPartitions is used to inform the client how many
   * partitions being dropped in this operation.
   */
  private void alterTableDropPartition(Table tbl,
      List<List<TPartitionKeyValue>> partitionSet, boolean ifExists, boolean purge,
      Reference<Long> numUpdatedPartitions, EventSequence catalogTimeline,
      InProgressTableModification modification) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    Preconditions.checkNotNull(partitionSet);

    TableName tableName = tbl.getTableName();
    if (!ifExists) {
      Preconditions.checkState(!partitionSet.isEmpty());
    } else {
      if (partitionSet.isEmpty()) {
        LOG.trace(String.format("Ignoring empty partition list when dropping " +
            "partitions from %s because ifExists is true.", tableName));
        return;
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
    // droppedPartitionsFromEvent maps the eventId to the list of partition values
    // for all the partitions which are received in that event.
    Map<Long, List<List<String>>> droppedPartsFromEvent = Maps.newHashMap();
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      long currentEventId = getCurrentEventId(msClient, catalogTimeline);
      //TODO batch drop the partitions instead of one-by-one.
      for (HdfsPartition part : parts) {
        try {
          msClient.getHiveClient().dropPartition(tableName.getDb(), tableName.getTbl(),
              part.getPartitionValuesAsStrings(true), dropOptions);
          LOG.info("Dropped partition {}.{}:{} in Metastore",
              tableName.getDb(), tableName.getTbl(), part.getPartitionName());
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
        catalogTimeline.markEvent("Dropped partitions in Metastore");
      }
      List<NotificationEvent> events = getNextMetastoreEventsForTableIfEnabled(
          catalogTimeline, currentEventId, tableName.getDb(), tableName.getTbl(),
          DropPartitionEvent.EVENT_TYPE);
      addDroppedPartitionsFromEvent(
          ((HdfsTable) tbl).getClusteringColNames(), events, droppedPartsFromEvent);
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "dropPartition"), e);
    }
    numUpdatedPartitions.setRef(numTargetedPartitions);
    catalog_.dropPartitions(tbl, partitionSet);
    for (Entry<Long, List<List<String>>> eventToPartitionNames : droppedPartsFromEvent
        .entrySet()) {
      //TODO we add partitions one by one above and hence we expect each event to contain
      // one partition. If this changes, we should change the eventLog logic as well
      // to support multiple partitions removed per event.
      Preconditions.checkState(eventToPartitionNames.getValue().size() == 1);
      addToDeleteEventLog(eventToPartitionNames.getKey(),
          DeleteEventLog.getPartitionKey((HdfsTable) tbl,
                  eventToPartitionNames.getValue().get(0)));
    }
    // we don't need to add catalog versions in partition's InflightEvents here
    // since by the time the event is received, the partition is already
    // removed from catalog and there is nothing to compare against during
    // self-event evaluation
    modification.updateTableCatalogVersion();
  }

  /**
   * Removes a column from the given table.
   */
  private void alterTableDropCol(Table tbl, String colName, EventSequence catalogTimeline,
      InProgressTableModification modification) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
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
    applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
  }

  /**
   * Renames an existing table or view.
   * After renaming the table/view, its metadata is marked as invalid and will be
   * reloaded on the next access.
   */
  private void alterTableOrViewRename(Table oldTbl, TableName newTableName,
      InProgressTableModification modification, boolean wantMinimalResult,
      TDdlExecResponse response, EventSequence catalogTimeline) throws ImpalaException {
    Preconditions.checkState(oldTbl.isWriteLockedByCurrentThread()
        && catalog_.getLock().isWriteLockedByCurrentThread());
    TableName tableName = oldTbl.getTableName();
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        oldTbl.getMetaStoreTable().deepCopy();
    msTbl.setDbName(newTableName.getDb());
    msTbl.setTableName(newTableName.getTbl());
    long eventId = -1;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      eventId = getCurrentEventId(msClient);
      catalogTimeline.markEvent(FETCHED_LATEST_HMS_EVENT_ID + eventId);
    }
    // If oldTbl is a synchronized Kudu table, rename the underlying Kudu table.
    boolean isSynchronizedKuduTable = (oldTbl instanceof KuduTable) &&
                                 KuduTable.isSynchronizedTable(msTbl);
    boolean integratedHmsTable = isHmsIntegrationAutomatic(msTbl);
    if (isSynchronizedKuduTable) {
      Preconditions.checkState(KuduTable.isKuduTable(msTbl));
      renameManagedKuduTable((KuduTable) oldTbl, msTbl, newTableName, integratedHmsTable,
          catalogTimeline);
    }

    // If oldTbl is a synchronized Iceberg table, rename the underlying Iceberg table.
    boolean isSynchronizedIcebergTable = (oldTbl instanceof IcebergTable) &&
        IcebergTable.isSynchronizedTable(msTbl);
    if (isSynchronizedIcebergTable) {
      renameManagedIcebergTable((IcebergTable) oldTbl, msTbl, newTableName);
      catalogTimeline.markEvent("Renamed table in Iceberg");
    }

    boolean isSynchronizedTable = isSynchronizedKuduTable || isSynchronizedIcebergTable;
    // Update the HMS table, unless the table is synchronized and the HMS integration
    // is automatic.
    boolean needsHmsAlterTable = !isSynchronizedTable || !integratedHmsTable;
    if (needsHmsAlterTable) {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
        Table.updateTimestampProperty(msTbl, Table.TBL_PROP_LAST_DDL_TIME);
        msClient.getHiveClient().alter_table(
            tableName.getDb(), tableName.getTbl(), msTbl);
        catalogTimeline.markEvent("Renamed table in Metastore");
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_table"), e);
      }
    }
    List<NotificationEvent> events = null;
    // the alter table event is generated on the renamed table
    events = getNextMetastoreEventsForTableIfEnabled(catalogTimeline, eventId,
        msTbl.getDbName(), msTbl.getTableName(), AlterTableEvent.EVENT_TYPE);
    Pair<Long, Pair<org.apache.hadoop.hive.metastore.api.Table,
        org.apache.hadoop.hive.metastore.api.Table>> renamedTable =
        getRenamedTableFromEvents(events);
    // Rename the table in the Catalog and get the resulting catalog object.
    // ALTER TABLE/VIEW RENAME is implemented as an ADD + DROP.
    Pair<Table, Table> result =
        catalog_.renameTable(tableName.toThrift(), newTableName.toThrift());
    if (renamedTable != null) {
      org.apache.hadoop.hive.metastore.api.Table tblBefore = renamedTable.second.first;
      addToDeleteEventLog(renamedTable.first, DeleteEventLog
          .getTblKey(tblBefore.getDbName(), tblBefore.getTableName()));
      if (result.second != null) {
        result.second.setCreateEventId(renamedTable.first);
      }
    }
    if (result.first == null || result.second == null) {
      // The rename succeeded in the HMS but failed in the catalog cache. The cache is in
      // an inconsistent state, but can likely be fixed by running "invalidate metadata".
      throw new ImpalaRuntimeException(String.format(
          "Table/view rename succeeded in the Hive Metastore, but failed in Impala's " +
          "Catalog Server. Running 'invalidate metadata <tbl>' on the old table name " +
          "'%s' and the new table name '%s' may fix the problem." , tableName,
          newTableName));
    }
    // TODO: call addVersionsForInflightEvents using InProgressTableModification object
    // that is passed into catalog_.renameTable()
    catalog_.addVersionsForInflightEvents(
        false, result.second, modification.newVersionNumber());
    if (wantMinimalResult) {
      response.result.addToRemoved_catalog_objects(result.first.toInvalidationObject());
      response.result.addToUpdated_catalog_objects(result.second.toInvalidationObject());
    } else {
      response.result.addToRemoved_catalog_objects(
          result.first.toMinimalTCatalogObject());
      response.result.addToUpdated_catalog_objects(result.second.toTCatalogObject());
    }
    response.result.setVersion(result.second.getCatalogVersion());
    addSummary(response, "Renaming was successful.");
  }

  /**
   * Adds the eventId and it's associated object key to the
   * {@link MetastoreEventsProcessor}'s delete event log. The event information is
   * not added to the delete event log if events processor is not active in order
   * to make sure that we don't keep adding events when they are not being garbage
   * collected.
   */
  public void addToDeleteEventLog(long eventId, String objectKey) {
    if (!catalog_.isEventProcessingActive()) {
      LOG.trace("Not adding event {}:{} since events processing is not active", eventId,
          objectKey);
      return;
    }
    catalog_.getMetastoreEventProcessor().getDeleteEventLog()
        .addRemovedObject(eventId, objectKey);
  }

  /**
   * Renames the underlying Kudu table for the given managed table. If the new Kudu
   * table name is the same as the old Kudu table name, this method does nothing.
   */
  private void renameManagedKuduTable(KuduTable oldTbl,
      org.apache.hadoop.hive.metastore.api.Table oldMsTbl,
      TableName newTableName, boolean isHMSIntegrationEanbled,
      EventSequence catalogTimeline) throws ImpalaRuntimeException {
    String newKuduTableName = KuduUtil.getDefaultKuduTableName(
        newTableName.getDb(), newTableName.getTbl(),
        isHMSIntegrationEanbled);

    // If the name of the Kudu table has not changed, do nothing
    if (oldTbl.getKuduTableName().equals(newKuduTableName)) return;

    KuduCatalogOpExecutor.renameTable(oldTbl, newKuduTableName, catalogTimeline);

    // Add the name of the new Kudu table to the HMS table parameters
    oldMsTbl.getParameters().put(KuduTable.KEY_TABLE_NAME, newKuduTableName);
  }

  /**
   * Renames the underlying Iceberg table for the given managed table. If the new Iceberg
   * table name is the same as the old Iceberg table name, this method does nothing.
   */
  private void renameManagedIcebergTable(IcebergTable oldTbl,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      TableName newTableName) throws ImpalaRuntimeException {
    TableIdentifier tableId = TableIdentifier.of(newTableName.getDb(),
        newTableName.getTbl());
    IcebergCatalogOpExecutor.renameTable(oldTbl, tableId);

    if (msTbl.getParameters().get(IcebergTable.ICEBERG_TABLE_IDENTIFIER) != null) {
      // We need update table identifier for HadoopCatalog managed table if exists.
      msTbl.getParameters().put(IcebergTable.ICEBERG_TABLE_IDENTIFIER,
          tableId.toString());
    }
  }

  /**
   * Changes the file format for the given table or partitions. This is a metadata only
   * operation, existing table data will not be converted to the new format. Returns
   * true if the file metadata to be reloaded.
   */
  private boolean alterTableSetFileFormat(Table tbl,
      List<List<TPartitionKeyValue>> partitionSet, THdfsFileFormat fileFormat,
      Reference<Long> numUpdatedPartitions, EventSequence catalogTimeline,
      InProgressTableModification modification) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    boolean reloadFileMetadata = false;
    if (partitionSet == null) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      setStorageDescriptorFileFormat(msTbl.getSd(), fileFormat);
      // The prototype partition must be updated if the file format is changed so that new
      // partitions are created with the new file format.
      if (tbl instanceof HdfsTable) ((HdfsTable) tbl).setPrototypePartition(msTbl.getSd());
      applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
      reloadFileMetadata = true;
    } else {
      Preconditions.checkArgument(tbl instanceof HdfsTable);
      List<HdfsPartition> partitions =
          ((HdfsTable) tbl).getPartitionsFromPartitionSet(partitionSet);
      List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
      for(HdfsPartition partition: partitions) {
        modifiedParts.add(new HdfsPartition.Builder(partition).setFileFormat(
            HdfsFileFormat.fromThrift(fileFormat)));
      }
      bulkAlterPartitions(tbl, modifiedParts, null, UpdatePartitionMethod.MARK_DIRTY,
          catalogTimeline, modification);
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
      Reference<Long> numUpdatedPartitions, EventSequence catalogTimeline,
      InProgressTableModification modification) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
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
      applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
      reloadFileMetadata = true;
    } else {
      List<HdfsPartition> partitions =
          ((HdfsTable) tbl).getPartitionsFromPartitionSet(partitionSet);
      List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
      for(HdfsPartition partition: partitions) {
        HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
        HiveStorageDescriptorFactory.setSerdeInfo(rowFormat, partBuilder.getSerdeInfo());
        modifiedParts.add(partBuilder);
      }
      bulkAlterPartitions(tbl, modifiedParts, null, UpdatePartitionMethod.MARK_DIRTY,
          catalogTimeline, modification);
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
  private boolean alterTableSetLocation(Table tbl, List<TPartitionKeyValue> partitionSpec,
      String location, EventSequence catalogTimeline,
      InProgressTableModification modification) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    boolean reloadFileMetadata = false;
    if (partitionSpec == null) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      if (msTbl.getPartitionKeysSize() == 0) reloadFileMetadata = true;
      msTbl.getSd().setLocation(location);
      applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
    } else {
      TableName tableName = tbl.getTableName();
      HdfsPartition partition = catalog_.getHdfsPartition(
          tableName.getDb(), tableName.getTbl(), partitionSpec);
      HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
      partBuilder.setLocation(location);
      try {
        applyAlterPartition(tbl, partBuilder, catalogTimeline);
      } finally {
        ((HdfsTable) tbl).markDirtyPartition(partBuilder);
      }
      // Only add the versions for in-flight event when we are sure that all
      // partition HMS API calls are successful. Registering in-flight event at the end
      // is OK since partition event processing still require acquiring table lock.
      modification.registerInflightEvent();
    }
    return reloadFileMetadata;
  }

  /**
   * Appends to the table or partitions property metadata for the given table, replacing
   * the values of any keys that already exist.
   */
  private void alterTableSetTblProperties(Table tbl,
      TAlterTableSetTblPropertiesParams params, Reference<Long> numUpdatedPartitions,
      EventSequence catalogTimeline, InProgressTableModification modification)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    Map<String, String> properties = params.getProperties();
    Preconditions.checkNotNull(properties);
    if (params.isSetPartition_set()) {
      Preconditions.checkArgument(tbl instanceof HdfsTable);
      List<HdfsPartition> partitions =
          ((HdfsTable) tbl).getPartitionsFromPartitionSet(params.getPartition_set());

      List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
      for(HdfsPartition partition: partitions) {
        HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
        switch (params.getTarget()) {
          case TBL_PROPERTY:
            partBuilder.getParameters().putAll(properties);
            break;
          case SERDE_PROPERTY:
            partBuilder.getSerdeInfo().getParameters().putAll(properties);
            break;
          default:
            throw new UnsupportedOperationException(
                "Unknown target TTablePropertyType: " + params.getTarget());
        }
        modifiedParts.add(partBuilder);
      }
      try {
        // Do not mark the partitions dirty here since it's done in finally clause.
        bulkAlterPartitions(tbl, modifiedParts, null, UpdatePartitionMethod.NONE,
            catalogTimeline, modification);
      } finally {
        ((HdfsTable) tbl).markDirtyPartitions(modifiedParts);
      }
      numUpdatedPartitions.setRef((long) modifiedParts.size());
    } else {
      // Alter table params.
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      switch (params.getTarget()) {
        case TBL_PROPERTY:
          if (KuduTable.isKuduTable(msTbl)) {
            // If 'kudu.table_name' is specified and this is a synchronized table, rename
            // the underlying Kudu table.
            // TODO(IMPALA-8618): this should be disallowed since IMPALA-5654
            if (properties.containsKey(KuduTable.KEY_TABLE_NAME)
                && !properties.get(KuduTable.KEY_TABLE_NAME).equals(
                    msTbl.getParameters().get(KuduTable.KEY_TABLE_NAME))
                && KuduTable.isSynchronizedTable(msTbl)) {
              KuduCatalogOpExecutor.renameTable((KuduTable) tbl,
                  properties.get(KuduTable.KEY_TABLE_NAME), catalogTimeline);
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
      applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
    }
  }

  private void alterTableUnSetTblProperties(Table tbl,
      TAlterTableUnSetTblPropertiesParams params, Reference<Long> numUpdatedPartitions,
      EventSequence catalogTimeline, InProgressTableModification modification)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    List<String> removeProperties = params.getProperty_keys();
    boolean ifExists = params.isIf_exists();
    Preconditions.checkNotNull(removeProperties);

    if (params.isSetPartition_set()) {
      Preconditions.checkArgument(tbl instanceof HdfsTable,
          "Partition spec not allowed for non-HDFS table");
      List<HdfsPartition> partitions =
          ((HdfsTable) tbl).getPartitionsFromPartitionSet(params.getPartition_set());

      List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
      for(HdfsPartition partition: partitions) {
        HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
        Set<String> keys;
        switch (params.getTarget()) {
          case TBL_PROPERTY:
            keys = partBuilder.getParameters().keySet();
            break;
          case SERDE_PROPERTY:
            keys = partBuilder.getSerdeInfo().getParameters().keySet();
            break;
          default:
            throw new UnsupportedOperationException(
                "Unknown target TTablePropertyType: " + params.getTarget());
        }
        removeKeys(removeProperties, ifExists, keys, "partition " +
            partition.getPartitionName(), ALTER_TBL_UNSET_NON_EXIST_PROPERTY);
        modifiedParts.add(partBuilder);
      }
      try {
        // Do not mark the partitions dirty here since it's done in finally clause.
        bulkAlterPartitions(tbl, modifiedParts, null, UpdatePartitionMethod.NONE,
            catalogTimeline, modification);
      } finally {
        ((HdfsTable) tbl).markDirtyPartitions(modifiedParts);
      }
      numUpdatedPartitions.setRef((long) modifiedParts.size());
    } else {
      // Alter table params.
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      Set<String> keys;
      switch (params.getTarget()) {
        case TBL_PROPERTY:
          keys = msTbl.getParameters().keySet();
          break;
        case SERDE_PROPERTY:
          keys = msTbl.getSd().getSerdeInfo().getParameters().keySet();
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown target TTablePropertyType: " + params.getTarget());
      }
      removeKeys(removeProperties, ifExists, keys,
          "table " + tbl.getFullName(), ALTER_TBL_UNSET_NON_EXIST_PROPERTY);
      // Validate that the new table properties are valid and that
      // the Kudu table is accessible.
      if (KuduTable.isKuduTable(msTbl)) {
        KuduCatalogOpExecutor.validateKuduTblExists(msTbl);
      }
      applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
    }
  }

  /**
   * Appends to the view property metadata for the given view, replacing
   * the values of any keys that already exist.
   */
  private void alterViewSetTblProperties(Table tbl,
      TAlterTableSetTblPropertiesParams params, EventSequence catalogTimeline,
      InProgressTableModification modification) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    Map<String, String> properties = params.getProperties();
    Preconditions.checkNotNull(properties);

    // Alter view params.
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        tbl.getMetaStoreTable().deepCopy();
    msTbl.getParameters().putAll(properties);
    applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
  }

  private void alterViewUnSetTblProperties(Table tbl,
      TAlterTableUnSetTblPropertiesParams params, EventSequence catalogTimeline,
      InProgressTableModification modification) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    List<String> removeProperties = params.getProperty_keys();
    boolean ifExists = params.isIf_exists();
    Preconditions.checkNotNull(removeProperties);
    // Alter view params.
    org.apache.hadoop.hive.metastore.api.Table msTbl =
            tbl.getMetaStoreTable().deepCopy();
    Set<String> keys = msTbl.getParameters().keySet();
    removeKeys(removeProperties, ifExists, keys,
        "view " + tbl.getFullName(), ALTER_VIEW_UNSET_NON_EXIST_PROPERTY);
    applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
  }

  private void removeKeys(List<String> removeProperties, boolean ifExists,
      Set<String> keys, String fullName, String excepInfo) throws CatalogException {
    if (ifExists || keys.containsAll(removeProperties)) {
      keys.removeAll(removeProperties);
    } else {
      List<String> removeCopy = new ArrayList(removeProperties);
      removeCopy.removeAll(keys);
      throw new CatalogException(
          String.format("These properties do not exist for %s: %s.\n%s",
              fullName,
              String.join(",", removeCopy),
              excepInfo));
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
  private boolean alterTableSetCached(Table tbl, TAlterTableSetCachedParams params,
      EventSequence catalogTimeline, InProgressTableModification modification)
      throws ImpalaException {
    Preconditions.checkArgument(tbl.isWriteLockedByCurrentThread());
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
            HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
            try {
              // If the partition was already cached, update the directive otherwise
              // issue new cache directive
              if (!partition.isMarkedCached()) {
                cacheDirIds.add(HdfsCachingUtil.submitCachePartitionDirective(
                    partBuilder, cacheOp.getCache_pool_name(), cacheReplication));
              } else {
                Long directiveId = HdfsCachingUtil.getCacheDirectiveId(
                    partition.getParameters());
                cacheDirIds.add(HdfsCachingUtil.modifyCacheDirective(directiveId,
                    partBuilder, cacheOp.getCache_pool_name(), cacheReplication));
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
              applyAlterPartition(tbl, partBuilder, catalogTimeline);
            } finally {
              ((HdfsTable) tbl).markDirtyPartition(partBuilder);
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
      catalog_.watchCacheDirs(cacheDirIds, tableName.toThrift(),
          "ALTER TABLE SET CACHED");
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
            HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
            HdfsCachingUtil.removePartitionCacheDirective(partBuilder);
            try {
              applyAlterPartition(tbl, partBuilder, catalogTimeline);
            } finally {
              ((HdfsTable) tbl).markDirtyPartition(partBuilder);
            }
          }
        }
      } else {
        loadFileMetadata = true;
      }
    }

    // Update the table metadata.
    applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
    return loadFileMetadata;
  }

  /**
   * Caches or uncaches the HDFS location of the target partitions and updates the
   * partitions' metadata in Hive Metastore Store. If a partition is being cached, the
   * partition properties will have the ID of the cache directive added. If the partition
   * is being uncached, any outstanding cache directive will be dropped and the cache
   * directive ID property key will be cleared.
   */
  private void alterPartitionSetCached(Table tbl, TAlterTableSetCachedParams params,
      Reference<Long> numUpdatedPartitions, EventSequence catalogTimeline,
      InProgressTableModification modification) throws ImpalaException {
    Preconditions.checkArgument(tbl.isWriteLockedByCurrentThread());
    THdfsCachingOp cacheOp = params.getCache_op();
    Preconditions.checkNotNull(cacheOp);
    Preconditions.checkNotNull(params.getPartition_set());
    TableName tableName = tbl.getTableName();
    Preconditions.checkArgument(tbl instanceof HdfsTable);
    List<HdfsPartition> partitions =
        ((HdfsTable) tbl).getPartitionsFromPartitionSet(params.getPartition_set());
    List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
    if (cacheOp.isSet_cached()) {
      for (HdfsPartition partition : partitions) {
        // The directive is null if the partition is not cached
        Long directiveId =
            HdfsCachingUtil.getCacheDirectiveId(partition.getParameters());
        HdfsPartition.Builder partBuilder = null;
        short replication = HdfsCachingUtil.getReplicationOrDefault(cacheOp);
        List<Long> cacheDirs = Lists.newArrayList();
        if (directiveId == null) {
          partBuilder = new HdfsPartition.Builder(partition);
          cacheDirs.add(HdfsCachingUtil.submitCachePartitionDirective(
              partBuilder, cacheOp.getCache_pool_name(), replication));
        } else {
          if (HdfsCachingUtil.isUpdateOp(cacheOp, partition.getParameters())) {
            partBuilder = new HdfsPartition.Builder(partition);
            HdfsCachingUtil.validateCachePool(cacheOp, directiveId, tableName, partition);
            cacheDirs.add(HdfsCachingUtil.modifyCacheDirective(
                directiveId, partBuilder, cacheOp.getCache_pool_name(), replication));
          }
        }

        // Once the cache directives are submitted, observe the status of the caching
        // until no more progress is made -- either fully cached or out of cache memory
        if (!cacheDirs.isEmpty()) {
          catalog_.watchCacheDirs(cacheDirs, tableName.toThrift(),
              "ALTER PARTITION SET CACHED");
        }
        if (partBuilder != null) modifiedParts.add(partBuilder);
      }
    } else {
      for (HdfsPartition partition : partitions) {
        if (partition.isMarkedCached()) {
          HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
          HdfsCachingUtil.removePartitionCacheDirective(partBuilder);
          modifiedParts.add(partBuilder);
        }
      }
    }
    try {
      // Do not mark the partitions dirty here since it's done in finally clause.
      bulkAlterPartitions(tbl, modifiedParts, null, UpdatePartitionMethod.NONE,
          catalogTimeline, modification);
    } finally {
      ((HdfsTable) tbl).markDirtyPartitions(modifiedParts);
    }
    numUpdatedPartitions.setRef((long) modifiedParts.size());
  }

  /**
   * Recover partitions of specified table.
   * Add partitions to metastore which exist in HDFS but not in metastore.
   */
  private void alterTableRecoverPartitions(Table tbl, @Nullable String debugAction,
      EventSequence catalogTimeline, InProgressTableModification modification)
      throws ImpalaException {
    Preconditions.checkArgument(tbl.isWriteLockedByCurrentThread());
    if (!(tbl instanceof HdfsTable)) {
      throw new CatalogException("Table " + tbl.getFullName() + " is not an HDFS table");
    }
    HdfsTable hdfsTable = (HdfsTable) tbl;
    List<List<String>> partitionsNotInHms = hdfsTable
        .getPathsWithoutPartitions(debugAction);
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
    Map<String, Long> partitionToEventId = Maps.newHashMap();
    String annotation = String.format("Recovering %d partitions for %s",
        hmsPartitions.size(), tbl.getFullName());
    if (DebugUtils.hasDebugAction(debugAction, DebugUtils.ENABLE_EVENT_PROCESSOR)) {
      catalog_.startEventsProcessor();
    }
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(annotation);
        MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      List<Partition> addedPartitions = addHmsPartitions(msClient, tbl, hmsPartitions,
          partitionToEventId, true, catalogTimeline);
      addHdfsPartitions(msClient, tbl, addedPartitions, partitionToEventId,
          catalogTimeline);
      // Handle HDFS cache.
      if (cachePoolName != null) {
        int numDone = 0;
        for (List<Partition> hmsSublist :
            Lists.partition(addedPartitions, MAX_PARTITION_UPDATES_PER_RPC)) {
          for (Partition partition: hmsSublist) {
            long id = HdfsCachingUtil.submitCachePartitionDirective(partition,
                cachePoolName, replication);
            cacheIds.add(id);
          }
          // Update the partition metadata to include the cache directive id.
          MetastoreShim.alterPartitions(msClient.getHiveClient(), tableName.getDb(),
              tableName.getTbl(), hmsSublist);
          numDone += hmsSublist.size();
          LOG.info("Updated cache directive id for {}/{} partitions for table {}",
              numDone, addedPartitions.size(), tableName);
        }
      }
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "add_partition"), e);
    }
    if (!cacheIds.isEmpty()) {
      catalog_.watchCacheDirs(cacheIds, tableName.toThrift(),
          "ALTER TABLE RECOVER PARTITIONS");
    }
    // Only add the versions for in-flight event when we are sure that all
    // partition HMS API calls are successful. Registering in-flight event at the end
    // is OK since partition event processing still require acquiring table lock.
    modification.registerInflightEvent();
  }

  private void alterTableOrViewSetOwner(Table tbl, TAlterTableOrViewSetOwnerParams params,
      TDdlExecResponse response, EventSequence catalogTimeline,
      InProgressTableModification modification) throws ImpalaException {
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    String oldOwner = msTbl.getOwner();
    PrincipalType oldOwnerType = msTbl.getOwnerType();
    msTbl.setOwner(params.owner_name);
    msTbl.setOwnerType(PrincipalType.valueOf(params.owner_type.name()));

    // A KuduTable is synchronized if it is a managed KuduTable, or an external table
    // with the property of 'external.table.purge' being true.
    boolean isSynchronizedKuduTable = (tbl instanceof KuduTable) &&
        KuduTable.isSynchronizedTable(msTbl);
    boolean altersHMSTable = true;
    if (isSynchronizedKuduTable) {
      boolean isKuduHmsIntegrationEnabled = isKuduHmsIntegrationEnabled(msTbl);
      // We need to update HMS when the integration between Kudu and HMS is not enabled.
      altersHMSTable = !isKuduHmsIntegrationEnabled;
      KuduCatalogOpExecutor.alterSetOwner((KuduTable) tbl, params.owner_name,
          catalogTimeline);
    }

    if (altersHMSTable)
      applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);

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
    StorageDescriptor sd = MetaStoreUtil.shallowCopyStorageDescriptor(msTbl.getSd());
    sd.setLocation(location);
    partition.setSd(sd);
    return partition;
  }

  /**
   * No-op if event processing is disabled. Adds this catalog service id and the given
   * catalog version to the partition parameters from table parameters.
   */
  private void addCatalogServiceIdentifiers(
      org.apache.hadoop.hive.metastore.api.Table msTbl, Partition partition) {
    if (!catalog_.isEventProcessingActive()) return;
    Preconditions.checkState(msTbl.isSetParameters());
    Preconditions.checkNotNull(partition, "Partition is null");
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
    // make sure that the service id from the table matches with our own service id to
    // avoid issues where the msTbl has an older (other catalogs' service identifiers)
    String serviceIdFromTbl =
        tblParams.get(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey());
    String version = tblParams.get(MetastoreEventPropertyKey.CATALOG_VERSION.getKey());
    if (catalog_.getCatalogServiceId().equals(serviceIdFromTbl)) {
      partition.putToParameters(
          MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(), serviceIdFromTbl);
      partition.putToParameters(
          MetastoreEventPropertyKey.CATALOG_VERSION.getKey(), version);
    }
  }

  /**
   * This method extracts the catalog version from the tbl parameters and adds it to
   * the HdfsPartition's inflight events. This information is used by event
   * processor to skip the event generated on the partition.
   */
  private void addToInflightVersionsOfPartition(
      Map<String, String> partitionParams, HdfsPartition.Builder partBuilder) {
    if (!catalog_.isEventProcessingActive()) return;
    Preconditions.checkState(partitionParams != null);
    String version = partitionParams
        .get(MetastoreEventPropertyKey.CATALOG_VERSION.getKey());
    String serviceId = partitionParams
        .get(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey());

    // make sure that we are adding the catalog version from our own instance of
    // catalog service identifiers
    if (catalog_.getCatalogServiceId().equals(serviceId)) {
      Preconditions.checkNotNull(version);
      partBuilder.addToVersionsForInflightEvents(false, Long.parseLong(version));
    }
  }

  /**
   * Creates a new function in the Hive metastore. Returns true if successful
   * and false if the call fails and ifNotExists is true.
   */
  public boolean addJavaFunctionToHms(
      org.apache.hadoop.hive.metastore.api.Function fn, boolean ifNotExists,
      EventSequence catalogTimeline) throws ImpalaRuntimeException{
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      msClient.getHiveClient().createFunction(fn);
      catalogTimeline.markEvent("Created function in Metastore");
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
  public boolean dropJavaFunctionFromHms(String db, String fn, boolean ifExists,
      EventSequence catalogTimeline) throws ImpalaRuntimeException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      msClient.getHiveClient().dropFunction(db, fn);
      catalogTimeline.markEvent("Dropped function in Metastore");
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
   * Creates a new DataSource in the Hive metastore. Returns true if successful
   * and false if the call fails and ifNotExists is true.
   * Note that DataSource object is saved as DataConnector object with type
   * "impalaDataSource"in HMS.
   */
  private boolean addDataSourceToHms(DataSource dataSource, boolean ifNotExists)
      throws ImpalaRuntimeException{
    getMetastoreDdlLock().lock();
    try {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        MetastoreShim.createDataSource(msClient.getHiveClient(), dataSource);
      } catch(AlreadyExistsException e) {
        if (!ifNotExists) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "createDataConnector"), e);
        }
        return false;
      } catch (TException e) {
        LOG.error("Error executing createDataConnector() metastore call: " +
            dataSource.getName(), e);
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "createDataConnector"), e);
      }
    } finally {
      getMetastoreDdlLock().unlock();
    }
    return true;
  }

  /**
   * Drops the DataSource with given name from Hive metastore. Returns true if
   * successful and false if the DataSource does not exist and ifExists is true.
   */
  private boolean dropDataSourceFromHms(String name, boolean ifExists)
      throws ImpalaRuntimeException {
    getMetastoreDdlLock().lock();
    try {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        try {
          MetastoreShim.dropDataSource(msClient.getHiveClient(), name, ifExists);
        } catch (NoSuchObjectException e) {
          if (!ifExists) {
            throw new ImpalaRuntimeException(
                String.format(HMS_RPC_ERROR_FORMAT_STR, "dropDataConnector"), e);
          }
          return false;
        } catch (TException e) {
          LOG.error("Error executing dropDataConnector HMS call: " + name, e);
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "dropDataDataConnector"), e);
        }
      }
    } finally {
      getMetastoreDdlLock().unlock();
    }
    return true;
  }

  /**
   * Updates the database object in the metastore.
   */
  private void applyAlterDatabase(Database msDb, EventSequence catalogTimeline)
      throws ImpalaRuntimeException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      msClient.getHiveClient().alterDatabase(msDb.getName(), msDb);
      catalogTimeline.markEvent("Altered database in Metastore");
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "alterDatabase"), e);
    }
  }

  /**
   * Applies an ALTER TABLE command to the metastore table and register in-progress
   * table modification. Table must set in-progress table modification prior to calling
   * this method. This method call applyAlterTable(5) internally.
   */
  private void applyAlterAndInProgressTableModification(
      org.apache.hadoop.hive.metastore.api.Table msTbl, EventSequence catalogTimeline,
      @Nullable InProgressTableModification modification) throws ImpalaRuntimeException {
    applyAlterTable(msTbl, true, null, catalogTimeline, modification);
  }

  /**
   * Convenience function to call applyAlterTable(5) without InProgressTableModification
   * argument.
   */
  private void applyAlterTable(org.apache.hadoop.hive.metastore.api.Table msTbl,
      boolean overwriteLastDdlTime, @Nullable TblTransaction tblTxn,
      EventSequence catalogTimeline) throws ImpalaRuntimeException {
    applyAlterTable(msTbl, overwriteLastDdlTime, tblTxn, catalogTimeline, null);
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
      boolean overwriteLastDdlTime, @Nullable TblTransaction tblTxn,
      EventSequence catalogTimeline, @Nullable InProgressTableModification modification)
      throws ImpalaRuntimeException {
    if (modification != null) modification.registerInflightEvent();
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
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

      if (tblTxn != null) {
        MetastoreShim.alterTableWithTransaction(msClient.getHiveClient(), msTbl, tblTxn);
      } else {
        try {
          msClient.getHiveClient().alter_table(
              msTbl.getDbName(), msTbl.getTableName(), msTbl);
        } catch (TException e) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_table"), e);
        }
      }
      catalogTimeline.markEvent("Altered table in Metastore");
    } catch (Exception ex) {
      if (modification != null) modification.cancelInflightEventIfExist();
      throw ex;
    }
    if (modification != null) modification.markInflightEventRegistrationComplete();
  }

  private void applyAlterPartition(Table tbl, HdfsPartition.Builder partBuilder,
      EventSequence catalogTimeline) throws ImpalaException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      Partition hmsPartition = partBuilder.toHmsPartition();
      addCatalogServiceIdentifiers(tbl.getMetaStoreTable(), hmsPartition);
      applyAlterHmsPartitions(msClient, tbl.getTableName(),
          Arrays.asList(hmsPartition), catalogTimeline);
      addToInflightVersionsOfPartition(hmsPartition.getParameters(), partBuilder);
    }
  }

  private void applyAlterHmsPartitions(
      MetaStoreClient msClient, TableName tableName, List<Partition> hmsPartitions,
      EventSequence catalogTimeline) throws ImpalaException {
    try {
      MetastoreShim.alterPartitions(
          msClient.getHiveClient(), tableName.getDb(), tableName.getTbl(), hmsPartitions);
      catalogTimeline.markEvent(String.format("Altered %d partitions in Metastore",
          hmsPartitions.size()));
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
  private void grantPrivilege(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams grantRevokePrivParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(header);
    Preconditions.checkNotNull(grantRevokePrivParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(grantRevokePrivParams.isIs_grant());

    switch (grantRevokePrivParams.principal_type) {
      case ROLE:
        authzManager_.grantPrivilegeToRole(header, grantRevokePrivParams, resp);
        break;
      case USER:
        authzManager_.grantPrivilegeToUser(header, grantRevokePrivParams,
            resp);
        break;
      case GROUP:
        authzManager_.grantPrivilegeToGroup(header, grantRevokePrivParams,
            resp);
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
  private void revokePrivilege(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams grantRevokePrivParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(header);
    Preconditions.checkNotNull(grantRevokePrivParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(!grantRevokePrivParams.isIs_grant());

    switch (grantRevokePrivParams.principal_type) {
      case ROLE:
        authzManager_.revokePrivilegeFromRole(header, grantRevokePrivParams, resp);
        break;
      case USER:
        authzManager_.revokePrivilegeFromUser(header, grantRevokePrivParams, resp);
        break;
      case GROUP:
        authzManager_.revokePrivilegeFromGroup(header, grantRevokePrivParams, resp);
        break;
      default:
        throw new IllegalArgumentException("Unexpected principal type: " +
            grantRevokePrivParams.principal_type);
    }

    addSummary(resp, "Privilege(s) have been revoked.");
  }

  private static enum UpdatePartitionMethod {
    // Do not apply updates to the partition. The caller is responsible for updating
    // the state of any modified partitions to reflect changes applied.
    NONE,
    // Update the state of the Partition objects in place in the catalog.
    IN_PLACE,
    // Mark the partition dirty so that it will be later reloaded from scratch when
    // the table is reloaded.
    MARK_DIRTY,
  }
  ;

  /**
   * Convenience method to call bulkAlterPartitions(6) without modification parameter.
   */
  private void bulkAlterPartitions(Table tbl, List<HdfsPartition.Builder> modifiedParts,
      TblTransaction tblTxn, UpdatePartitionMethod updateMethod,
      EventSequence catalogTimeline) throws ImpalaException {
    bulkAlterPartitions(tbl, modifiedParts, tblTxn, updateMethod, catalogTimeline, null);
  }

  /**
   * Alters partitions in the HMS in batches of size 'MAX_PARTITION_UPDATES_PER_RPC'.
   * This reduces the time spent in a single update and helps avoid metastore client
   * timeouts.
   * @param updateMethod controls how the same updates are applied to 'tbl' to reflect
   *                     the changes written to the HMS.
   */
  private void bulkAlterPartitions(Table tbl, List<HdfsPartition.Builder> modifiedParts,
      TblTransaction tblTxn, UpdatePartitionMethod updateMethod,
      EventSequence catalogTimeline, @Nullable InProgressTableModification modification)
      throws ImpalaException {
    // Map from msPartitions to the partition builders. Use IdentityHashMap since
    // modifications will change hash codes of msPartitions.
    Map<Partition, HdfsPartition.Builder> msPartitionToBuilders =
        Maps.newIdentityHashMap();
    for (HdfsPartition.Builder p: modifiedParts) {
      Partition msPart = p.toHmsPartition();
      if (msPart != null) {
        addCatalogServiceIdentifiers(tbl.getMetaStoreTable(), msPart);
        msPartitionToBuilders.put(msPart, p);
      }
    }
    if (msPartitionToBuilders.isEmpty()) return;

    String dbName = tbl.getDb().getName();
    String tableName = tbl.getName();
    int numDone = 0;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      // Apply the updates in batches of 'MAX_PARTITION_UPDATES_PER_RPC'.
      for (List<Partition> msPartitionsSubList : Iterables.partition(
          msPartitionToBuilders.keySet(), MAX_PARTITION_UPDATES_PER_RPC)) {
        try {
          // Alter partitions in bulk.
          if (tblTxn != null) {
            MetastoreShim.alterPartitionsWithTransaction(msClient.getHiveClient(), dbName,
                tableName, msPartitionsSubList, tblTxn);
          } else {
            MetastoreShim.alterPartitions(msClient.getHiveClient(), dbName, tableName,
                msPartitionsSubList);
          }
          numDone += msPartitionsSubList.size();
          catalogTimeline.markEvent(
              String.format("Altered %d partitions in Metastore", numDone));
          LOG.info("HMS alterPartitions done on {}/{} partitions of table {}", numDone,
              msPartitionToBuilders.size(), tbl.getFullName());
          // Mark the corresponding HdfsPartition objects as dirty
          for (Partition msPartition : msPartitionsSubList) {
            HdfsPartition.Builder partBuilder = msPartitionToBuilders.get(msPartition);
            Preconditions.checkNotNull(partBuilder);
            // The partition either needs to be reloaded or updated in place to apply
            // the modifications.
            if (updateMethod == UpdatePartitionMethod.MARK_DIRTY) {
              ((HdfsTable) tbl).markDirtyPartition(partBuilder);
            } else if (updateMethod == UpdatePartitionMethod.IN_PLACE) {
              ((HdfsTable) tbl).updatePartition(partBuilder);
            }
            // If event processing is turned on add the version number from partition
            // parameters to the HdfsPartition's list of in-flight events.
            addToInflightVersionsOfPartition(msPartition.getParameters(), partBuilder);
          }
        } catch (TException e) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_partitions"), e);
        }
      }
    }
    // Only add the versions for in-flight event when we are sure that all
    // partition HMS API calls are successful. Registering in-flight event at the end
    // is OK since partition event processing still require acquiring table lock.
    if (modification != null) modification.registerInflightEvent();
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
    Stopwatch hmsLoadSW = Stopwatch.createStarted();
    long hmsLoadTime;
    try {
      msTbl = msClient.getHiveClient().getTable(db.getName(), tbl.getName());
    } catch (Exception e) {
      throw new TableLoadingException("Error loading metadata for table: " +
          db.getName() + "." + tbl.getName(), e);
    } finally {
      hmsLoadTime = hmsLoadSW.elapsed(TimeUnit.NANOSECONDS);
    }
    tbl.updateHMSLoadTableSchemaTime(hmsLoadTime);
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

  public TResetMetadataResponse execResetMetadata(TResetMetadataRequest req)
      throws CatalogException {
    catalogOpTracker_.increment(req);
    try {
      TResetMetadataResponse response = execResetMetadataImpl(req);
      catalogOpTracker_.decrement(req, /*errorMsg*/null);
      return response;
    } catch (Exception e) {
      catalogOpTracker_.decrement(req, JniUtil.throwableToString(e));
      throw e;
    }
  }

  /**
   * Executes a TResetMetadataRequest and returns the result as a
   * TResetMetadataResponse. Based on the request parameters, this operation
   * may do one of these things:
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
  public TResetMetadataResponse execResetMetadataImpl(TResetMetadataRequest req)
      throws CatalogException {
    EventSequence catalogTimeline = new EventSequence(CATALOG_TIMELINE_NAME);
    String cmdString = CatalogOpUtil.getShortDescForReset(req);
    TResetMetadataResponse resp = new TResetMetadataResponse();
    resp.setResult(new TCatalogUpdateResult());
    resp.getResult().setCatalog_service_id(JniCatalog.getServiceId());

    if (req.isSetDb_name()) {
      Preconditions.checkState(!catalog_.isBlacklistedDb(req.getDb_name()),
          String.format("Can't refresh functions in blacklisted database: %s. %s",
              req.getDb_name(), BLACKLISTED_DBS_INCONSISTENT_ERR_STR));
      // This is a "refresh functions" operation.
      acquireMetastoreDdlLock(catalogTimeline);
      try {
        List<TCatalogObject> addedFuncs = Lists.newArrayList();
        List<TCatalogObject> removedFuncs = Lists.newArrayList();
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
          catalog_.refreshFunctions(msClient, req.getDb_name(), addedFuncs, removedFuncs);
        }
        resp.result.setUpdated_catalog_objects(addedFuncs);
        resp.result.setRemoved_catalog_objects(removedFuncs);
        resp.result.setVersion(catalog_.getCatalogVersion());
        for (TCatalogObject removedFn: removedFuncs) {
          catalog_.getDeleteLog().addRemovedObject(removedFn);
        }
      } finally {
        getMetastoreDdlLock().unlock();
      }
    } else if (req.isSetTable_name()) {
      // Results of an invalidate operation, indicating whether the table was removed
      // from the Metastore, and whether a new database was added to Impala as a result
      // of the invalidate operation. Always false for refresh.
      Reference<Boolean> tblWasRemoved = new Reference<Boolean>(false);
      Reference<Boolean> dbWasAdded = new Reference<Boolean>(false);
      // Thrift representation of the result of the invalidate/refresh operation.
      TCatalogObject updatedThriftTable = null;
      // Result table of the invalidate/refresh operation.
      Table tbl = null;
      TableName tblName = TableName.fromThrift(req.getTable_name());
      long eventId = -1L;
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
        eventId = MetastoreEventsProcessor.getCurrentEventIdNoThrow(
            msClient.getHiveClient());
      }
      if (!req.isIs_refresh()) {
        // For INVALIDATE METADATA <db>.<table>, the db might be unloaded.
        // So we can't update 'tbl' here.
        updatedThriftTable = catalog_.invalidateTable(
            req.getTable_name(), tblWasRemoved, dbWasAdded, catalogTimeline, eventId);
        catalogTimeline.markEvent("Invalidated table in catalog cache");
      } else {
        // Quick check to see if the table exists in the catalog without triggering
        // a table load.
        tbl = catalog_.getTable(tblName.getDb(), tblName.getTbl());
        if (tbl != null) {
          // If the table is not loaded, no need to perform refresh after the initial
          // metadata load.
          boolean isTableLoadedInCatalog = tbl.isLoaded();
          tbl = getExistingTable(tblName.getDb(), tblName.getTbl(),
              "Load triggered by " + cmdString, catalogTimeline);
          CatalogObject.ThriftObjectType resultType =
              req.header.want_minimal_response ?
                  CatalogObject.ThriftObjectType.INVALIDATION :
                  CatalogObject.ThriftObjectType.FULL;
          if (isTableLoadedInCatalog) {
            if (req.isSetPartition_spec()) {
              boolean isTransactional = AcidUtils.isTransactionalTable(
                  tbl.getMetaStoreTable().getParameters());
              Preconditions.checkArgument(!isTransactional);
              Reference<Boolean> wasPartitionRefreshed = new Reference<>(false);
              // TODO if the partition was not really refreshed because the partSpec
              // was wrong, do we still need to send back the table?
              updatedThriftTable = catalog_.reloadPartition(tbl, req.getPartition_spec(),
                  wasPartitionRefreshed, resultType, cmdString, catalogTimeline);
            } else {
              // TODO IMPALA-8809: Optimisation for partitioned tables:
              //   1: Reload the whole table if schema change happened. Identify
              //     such scenario by checking Table.TBL_PROP_LAST_DDL_TIME property.
              //     Note, table level writeId is not updated by HMS for partitioned
              //     ACID tables, there is a Jira to cover this: HIVE-22062.
              //   2: If no need for a full table reload then fetch partition level
              //     writeIds and reload only the ones that changed.
              try {
                updatedThriftTable = catalog_.reloadTable(tbl, req, resultType, cmdString,
                    /*eventId*/ -1, catalogTimeline);
              } catch (IcebergTableLoadingException e) {
                updatedThriftTable = catalog_.invalidateTable(req.getTable_name(),
                    tblWasRemoved, dbWasAdded, catalogTimeline, eventId);
              }
            }
          } else {
            catalogTimeline.markEvent("Loaded table from scratch");
            // Table was loaded from scratch, so it's already "refreshed".
            tbl.takeReadLock();
            catalogTimeline.markEvent("Got table read lock");
            try {
              updatedThriftTable = tbl.toTCatalogObject(resultType);
            } finally {
              tbl.releaseReadLock();
            }
          }
        }
      }

      if (updatedThriftTable == null) {
        // Table does not exist in the Metastore and Impala catalog, throw error.
        throw new TableNotFoundException("Table not found: " +
            req.getTable_name().getDb_name() + "." +
            req.getTable_name().getTable_name());
      }

      if (BackendConfig.INSTANCE.enableReloadEvents()) {
        // For INVALIDATE METADATA <table>, 'tbl' can only be got after it succeeds.
        if (!req.isIs_refresh()) {
          tbl = catalog_.getTable(tblName.getDb(), tblName.getTbl());
        }
        Preconditions.checkNotNull(tbl, "tbl is null in " + cmdString);
        // fire event for refresh event and update the last refresh event id
        fireReloadEventAndUpdateRefreshEventId(req, tblName, tbl);
        catalogTimeline.markEvent("Fired reload events in Metastore");
      }

      // Return the TCatalogObject in the result to indicate this request can be
      // processed as a direct DDL operation.
      if (tblWasRemoved.getRef()) {
        resp.getResult().addToRemoved_catalog_objects(updatedThriftTable);
      } else {
        // TODO(IMPALA-9937): if client is a 'v1' impalad, only send back incremental
        //  updates
        resp.getResult().addToUpdated_catalog_objects(updatedThriftTable);
      }

      if (dbWasAdded.getRef()) {
        Db addedDb = catalog_.getDb(updatedThriftTable.getTable().getDb_name());
        if (addedDb == null) {
          throw new CatalogException("Database " +
              updatedThriftTable.getTable().getDb_name() + " was removed by a " +
              "concurrent operation. Try invalidating the table again.");
        }
        addDbToCatalogUpdate(addedDb, req.header.want_minimal_response, resp.getResult());
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
      resp.getResult().setVersion(catalog_.reset(catalogTimeline));
      resp.getResult().setIs_invalidate(true);
    }
    catalogTimeline.markEvent("Finished resetMetadata request");
    if (req.isSync_ddl()) {
      resp.getResult().setVersion(catalog_.waitForSyncDdlVersion(resp.getResult()));
      catalogTimeline.markEvent(SENT_CATALOG_FOR_SYNC_DDL);
    }
    resp.getResult().setStatus(new TStatus(TErrorCode.OK, new ArrayList<String>()));

    TRuntimeProfileNode profile = Frontend.createTRuntimeProfileNode("CatalogOp");
    profile.addToEvent_sequences(catalogTimeline.toThrift());
    resp.setProfile(profile);
    return resp;
  }

  /**
   * Helper class for refresh event.
   * This class invokes metastore shim's fireReloadEvent to fire event to HMS
   * and update the last refresh event id in the cache
   */
  private void fireReloadEventAndUpdateRefreshEventId(
      TResetMetadataRequest req, TableName tblName, Table tbl) {
    List<String> partVals = null;
    if (req.isSetPartition_spec()) {
      partVals = req.getPartition_spec().stream().
          map(TPartitionKeyValue::getValue).collect(Collectors.toList());
    }
    try {
      List<Long> eventIds = MetastoreShim.fireReloadEventHelper(
          catalog_.getMetaStoreClient(), req.isIs_refresh(), partVals, tblName.getDb(),
          tblName.getTbl(), Collections.emptyMap());
      LOG.info("Fired {} RELOAD events for table {}: {}", eventIds.size(),
          tbl.getFullName(), StringUtils.join(",", eventIds));
      // Update the lastRefreshEventId accordingly
      if (!req.isIs_refresh() || eventIds.isEmpty()) return;
      if (!catalog_.tryLock(tbl, true, 600000)) {
        LOG.warn("Couldn't obtain a version lock for the table: {}. " +
                "Self events may go undetected in that case",
            tbl.getFullName());
        return;
      }
      if (req.isSetPartition_spec()) {
        HdfsTable hdfsTbl = (HdfsTable) tbl;
        HdfsPartition partition = hdfsTbl
            .getPartitionFromThriftPartitionSpec(req.getPartition_spec());
        if (partition != null) {
          HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
          partBuilder.setLastRefreshEventId(eventIds.get(0));
          hdfsTbl.updatePartition(partBuilder);
        } else {
          LOG.warn("Partition {} no longer exists in table {}. It might be " +
              "dropped by a concurrent operation.",
              FeCatalogUtils.getPartitionName(hdfsTbl, partVals),
              hdfsTbl.getFullName());
        }
      } else {
        tbl.setLastRefreshEventId(eventIds.get(0));
      }
    } catch (TException | CatalogException e) {
      LOG.error(String.format(HMS_RPC_ERROR_FORMAT_STR,
          "fireReloadEvent") + e.getMessage());
    } finally {
      if (tbl.isWriteLockedByCurrentThread()) {
        tbl.releaseWriteLock();
        catalog_.getLock().writeLock().unlock();
      }
    }
  }

  public TUpdateCatalogResponse updateCatalog(TUpdateCatalogRequest update)
      throws ImpalaException {
    catalogOpTracker_.increment(update);
    try {
      TUpdateCatalogResponse response = updateCatalogImpl(update);
      catalogOpTracker_.decrement(update, /*errorMsg*/null);
      return response;
    } catch (Exception e) {
      catalogOpTracker_.decrement(update, JniUtil.throwableToString(e));
      throw e;
    }
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
  public TUpdateCatalogResponse updateCatalogImpl(TUpdateCatalogRequest update)
      throws ImpalaException {
    EventSequence catalogTimeline = new EventSequence(CATALOG_TIMELINE_NAME);
    TUpdateCatalogResponse response = new TUpdateCatalogResponse();
    // Only update metastore for Hdfs tables.
    Table table = getExistingTable(update.getDb_name(), update.getTarget_table(),
        "Load for INSERT", catalogTimeline);
    if (!(table instanceof FeFsTable)) {
      throw new InternalException("Unexpected table type: " +
          update.getTarget_table());
    }

    tryWriteLock(table, "updating the catalog", catalogTimeline);
    final Timer.Context context
        = table.getMetrics().getTimer(HdfsTable.CATALOG_UPDATE_DURATION_METRIC).time();

    InProgressTableModification modification = null;
    try {
      // Get new catalog version for table in insert.
      modification = new InProgressTableModification(catalog_, table);
      catalog_.getLock().writeLock().unlock();

      TblTransaction tblTxn = null;
      if (update.isSetTransaction_id()) {
        long transactionId = update.getTransaction_id();
        Preconditions.checkState(transactionId > 0);
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
          if (DebugUtils.hasDebugAction(update.getDebug_action(),
              DebugUtils.UPDATE_CATALOG_ABORT_INSERT_TXN)) {
            MetastoreShim.abortTransaction(msClient.getHiveClient(), transactionId);
            LOG.info("Aborted txn due to the debug action.");
          }
          // Setup transactional parameters needed to do alter table/partitions later.
          // TODO: Could be optimized to possibly save some RPCs, as these parameters are
          //       not always needed + the writeId of the INSERT could be probably reused.
          tblTxn = MetastoreShim.createTblTransaction(
              msClient.getHiveClient(), table.getMetaStoreTable(), transactionId);
          catalogTimeline.markEvent("Created Metastore transaction");
        }
      }

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
          FeCatalogUtils.loadAllPartitions((FeFsTable)table);
      List<FeFsPartition> affectedExistingPartitions = new ArrayList<>();
      List<org.apache.hadoop.hive.metastore.api.Partition> hmsPartitionsStatsUnset =
          Lists.newArrayList();
      // Names of the partitions that are added with add_partitions() RPC.
      // add_partitions() fires events for these partitions, so we don't need to
      // fire an insert event. Collect the partition name both as a single string and
      // as a list of values for convenience.
      Map<String, List<String>> addedPartitionNames = new HashMap<>();
      // if event processing is enabled we collect the events ids generated for the added
      // partitions in this map. This is used later on when table is reloaded to set
      // the createEventId for the partitions.
      Map<String, Long> partitionToEventId = new HashMap<>();
      modification.addCatalogServiceIdentifiersToTable();
      if (table.getNumClusteringCols() > 0) {
        // Set of all partition names targeted by the insert that need to be created
        // in the Metastore (partitions that do not currently exist in the catalog).
        // In the BE, we don't currently distinguish between which targeted partitions
        // are new and which already exist, so initialize the set with all targeted
        // partition names and remove the ones that are found to exist.
        HashSet<String> partsToCreate =
            Sets.newHashSet(update.getUpdated_partitions().keySet());
        partsToLoadMetadata = Sets.newHashSet(partsToCreate);
        for (FeFsPartition partition: parts) {
          String partName = partition.getPartitionName();
          // Attempt to remove this partition name from partsToCreate. If remove
          // returns true, it indicates the partition already exists.
          if (partsToCreate.remove(partName)) {
            affectedExistingPartitions.add(partition);
            // For existing partitions, we need to unset column_stats_accurate to
            // tell hive the statistics is not accurate any longer.
            if (partition.getParameters() != null &&  partition.getParameters()
                .containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
              org.apache.hadoop.hive.metastore.api.Partition hmsPartition =
                  ((HdfsPartition) partition).toHmsPartition();
              hmsPartition.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
              hmsPartitionsStatsUnset.add(hmsPartition);
            }
            if (partition.isMarkedCached()) {
              // The partition was targeted by the insert and is also cached. Since
              // data was written to the partition, a watch needs to be placed on the
              // cache directive so the TableLoadingMgr can perform an async
              // refresh once all data becomes cached.
              cacheDirIds.add(HdfsCachingUtil.getCacheDirectiveId(
                  partition.getParameters()));
            }
          }
          if (partsToCreate.size() == 0) break;
        }

        if (!partsToCreate.isEmpty()) {
          try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
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
              partition.setSd(MetaStoreUtil.shallowCopyStorageDescriptor(msTbl.getSd()));
              partition.getSd().setLocation(msTbl.getSd().getLocation() + "/" + partName);
              if (AcidUtils.isTransactionalTable(msTbl.getParameters())) {
                // Self event detection is deprecated for non-transactional tables add
                // partition. So we add catalog service identifiers only for
                // transactional tables
                addCatalogServiceIdentifiers(msTbl, partition);
              }
              MetastoreShim.updatePartitionStatsFast(partition, msTbl, warehouse);
            }

            // First add_partitions and then alter_partitions the successful ones with
            // caching directives. The reason is that some partitions could have been
            // added concurrently, and we want to avoid caching a partition twice and
            // leaking a caching directive.
            List<Partition> addedHmsParts = addHmsPartitions(
                msClient, table, hmsParts, partitionToEventId, true, catalogTimeline);
            for (Partition part: addedHmsParts) {
              String part_name =
                  FeCatalogUtils.getPartitionName((FeFsTable)table, part.getValues());
              addedPartitionNames.put(part_name, part.getValues());
            }
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
                    StatsSetupConst.setBasicStatsState(part.getParameters(), "false");
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
                      HdfsCachingUtil.removePartitionCacheDirective(part.getParameters());
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
          } catch (Exception e) {
            throw new InternalException("Error adding partitions", e);
          }
        }

        // Unset COLUMN_STATS_ACCURATE by calling alter partition to hms.
        if (!hmsPartitionsStatsUnset.isEmpty()) {
          unsetPartitionsColStats(table.getMetaStoreTable(), hmsPartitionsStatsUnset,
              tblTxn, catalogTimeline);
        }
      } else {
        // For non-partitioned table, only single part exists
        FeFsPartition singlePart = Iterables.getOnlyElement((List<FeFsPartition>) parts);
        affectedExistingPartitions.add(singlePart);
      }
      unsetTableColStats(table.getMetaStoreTable(), tblTxn, catalogTimeline);
      // Submit the watch request for the given cache directives.
      if (!cacheDirIds.isEmpty()) {
        catalog_.watchCacheDirs(cacheDirIds, tblName.toThrift(),
            "INSERT into cached partitions");
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

      // Before commit fire insert events if external event processing is
      // enabled. This is best-effort. Any errors in it should not fail the INSERT.
      try {
        createInsertEvents((FeFsTable) table, update.getUpdated_partitions(),
            addedPartitionNames, update.is_overwrite, tblTxn, catalogTimeline);
      } catch (Exception e) {
        LOG.error("Failed to fire insert events for table {}", table.getFullName(), e);
      }

      // Commit transactional inserts on success. We don't abort the transaction
      // here in case of failures, because the client, i.e. query coordinator, is
      // always responsible for aborting transactions when queries hit errors.
      if (update.isSetTransaction_id()) {
        if (response.getResult().getStatus().getStatus_code() == TErrorCode.OK) {
          commitTransaction(update.getTransaction_id(), catalogTimeline);
        }
      }

      if (table instanceof FeIcebergTable && update.isSetIceberg_operation()) {
        FeIcebergTable iceTbl = (FeIcebergTable)table;
        org.apache.iceberg.Transaction iceTxn = IcebergUtil.getIcebergTransaction(iceTbl);
        IcebergCatalogOpExecutor.execute(iceTbl, iceTxn,
            update.getIceberg_operation());
        catalogTimeline.markEvent("Executed Iceberg operation " +
            update.getIceberg_operation().getOperation());
        if (isIcebergHmsIntegrationEnabled(iceTbl.getMetaStoreTable())) {
          // Add catalog service id and the 'newCatalogVersion' to the table parameters.
          // This way we can avoid reloading the table on self-events (Iceberg generates
          // an ALTER TABLE statement to set the new metadata_location).
          modification.registerInflightEvent();
          IcebergCatalogOpExecutor.addCatalogVersionToTxn(
              iceTxn, catalog_.getCatalogServiceId(), modification.newVersionNumber());
          catalogTimeline.markEvent("Updated table properties");
        }

        if (update.isSetDebug_action()) {
          String debugAction = update.getDebug_action();
          DebugUtils.executeDebugAction(debugAction, DebugUtils.ICEBERG_COMMIT);
        }
        iceTxn.commitTransaction();
        modification.markInflightEventRegistrationComplete();
      }

      loadTableMetadata(table, modification.newVersionNumber(), true, false,
          partsToLoadMetadata, partitionToEventId, "INSERT", update.getDebug_action(),
          catalogTimeline);
      addTableToCatalogUpdate(table, update.header.want_minimal_response,
          response.result);
      modification.validateInProgressModificationComplete();
    } catch (ImpalaException ex) {
      if (modification != null) modification.cancelInflightEventIfExist();
      throw ex;
    } finally {
      context.stop();
      UnlockWriteLockIfErronouslyLocked();
      table.releaseWriteLock();
    }
    catalogTimeline.markEvent("Finished updateCatalog request");

    if (update.isSync_ddl()) {
      response.getResult().setVersion(
          catalog_.waitForSyncDdlVersion(response.getResult()));
      catalogTimeline.markEvent(SENT_CATALOG_FOR_SYNC_DDL);
    }

    if (update.isSetDebug_action()) {
      DebugUtils.executeDebugAction(update.getDebug_action(),
          DebugUtils.INSERT_FINISH_DELAY);
    }
    TRuntimeProfileNode profile = Frontend.createTRuntimeProfileNode("CatalogOp");
    profile.addToEvent_sequences(catalogTimeline.toThrift());
    response.setProfile(profile);
    return response;
  }

  /**
   * Populates insert event data and calls fireInsertEvents() if external event
   * processing is enabled.
   * This method is replicating what Hive does in case a table or partition is inserts
   * into. There are 2 cases:
   * 1. If the table is transactional, we should first generate ADD_PARTITION events
   * for new partitions which are generated. This is taken care of in the updateCatalog
   * method. Additionally, for each partition including existing partitions which were
   * inserted into, this method creates an ACID_WRITE event using the HMS API
   * addWriteNotificationLog.
   * 2. If the table is not transactional, this method generates INSERT_EVENT for only
   * the pre-existing partitions which were inserted into. This is in-line with what hive
   * does, see:
   * https://github.com/apache/hive/blob/25892ea409/ql/src/java/org/apache/hadoop/hive/ql/metadata/Hive.java#L3251
   * @param table The target table.
   * @param updatedPartitions All affected partitions with the list of new files
   *                          inserted.
   * @param addedPartitionNames List of new partitions created during the insert.
   * @param isInsertOverwrite Indicates if the operation was an insert overwrite.
   * @param tblTxn Contains the transactionId and the writeId for the insert.
   */
  private void createInsertEvents(FeFsTable table,
      Map<String, TUpdatedPartition> updatedPartitions,
      Map<String, List<String>> addedPartitionNames,
      boolean isInsertOverwrite, TblTransaction tblTxn, EventSequence catalogTimeline)
      throws CatalogException, MetaException {
    if (!shouldGenerateInsertEvents(table)) return;
    long txnId = tblTxn == null ? -1 : tblTxn.txnId;
    long writeId = tblTxn == null ? -1: tblTxn.writeId;
    // If the table is transaction table we should generate a transactional
    // insert event type. This would show up in HMS as an ACID_WRITE event.
    boolean isTransactional = AcidUtils.isTransactionalTable(table.getMetaStoreTable()
        .getParameters());
    if (isTransactional) {
      Preconditions.checkState(txnId > 0, "Invalid transaction id %s for table %s",
          txnId, table.getFullName());
      Preconditions.checkState(writeId > 0, "Invalid write id %s for table %s",
          writeId, table.getFullName());
    }

    boolean isPartitioned = table.getNumClusteringCols() > 0;
    // List of all insert events that we call HMS fireInsertEvent() on.
    List<InsertEventRequestData> insertEventReqDatas = new ArrayList<>();
    // The partition val list corresponding to insertEventReqDatas for Apache Hive-3
    List<List<String>> insertEventPartVals = new ArrayList<>();
    // List of all existing partition names in HMS that we insert into. It's possible
    // that the partition doesn't exist in catalogd's cache.
    // Use LinkedHashSet to preserve the order.
    LinkedHashSet<String> existingPartSetInHms = new LinkedHashSet<>();
    if (isPartitioned) {
      existingPartSetInHms.addAll(updatedPartitions.keySet());
      existingPartSetInHms.removeAll(addedPartitionNames.keySet());
      // Create events for existing partitions in partitioned tables.
      for (String partName : existingPartSetInHms) {
        List<String> partVals = MetaStoreUtil.getPartValsFromName(
            table.getMetaStoreTable(), partName);
        prepareInsertEventData(table, partName, partVals, updatedPartitions,
            isInsertOverwrite, isPartitioned, insertEventReqDatas, insertEventPartVals);
      }
    } else {
      Preconditions.checkState(updatedPartitions.size() == 1);
      prepareInsertEventData(table, HdfsTable.DEFAULT_PARTITION_NAME,
          Collections.emptyList(), updatedPartitions, isInsertOverwrite, isPartitioned,
          insertEventReqDatas, insertEventPartVals);
    }

    // Create events for new partitions only in ACID tables.
    if (isTransactional) {
      for (Map.Entry<String, List<String>> part : addedPartitionNames.entrySet()) {
        prepareInsertEventData(table, part.getKey(), part.getValue(), updatedPartitions,
            isInsertOverwrite, isPartitioned, insertEventReqDatas, insertEventPartVals);
      }
    }

    if (insertEventReqDatas.isEmpty()) return;

    MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient(catalogTimeline);
    TableInsertEventInfo insertEventInfo = new TableInsertEventInfo(
        insertEventReqDatas, insertEventPartVals, isTransactional, txnId, writeId);
    List<Long> eventIds = MetastoreShim.fireInsertEvents(metaStoreClient,
        insertEventInfo, table.getDb().getName(), table.getName());
    catalogTimeline.markEvent("Fired Metastore events");
    if (isTransactional) {
      // ACID inserts do not generate INSERT events so there is nothing we need to track
      // here for self-event detection. Note that It is enough to listen to the commit
      // events fired by HMS, i.e. ALLOC_WRITE_ID_EVENT and COMMIT_TXN events.
      // For commit events triggered by Impala itself, it skips reloads for them based
      // on the writeIds. Commit events triggered by external systems (e.g. Hive) will
      // bring new writeIds which causes Impala to reload the table.
      // See more in addCommittedWriteIdsAndReloadPartitionsIfExist().
      Preconditions.checkState(eventIds.isEmpty());
      return;
    }
    trackInsertEvents((HdfsTable)table, eventIds, existingPartSetInHms);
  }

  /**
   * Helper method to prepare InsertEventRequestData. Also collect the list of
   * partition values.
   */
  private void prepareInsertEventData(FeFsTable table,
      String partName, List<String> partVals,
      Map<String, TUpdatedPartition> updatedPartitions,
      boolean isInsertOverwrite, boolean isPartitioned,
      List<InsertEventRequestData> insertEventReqDatas,
      List<List<String>> insertEventPartVals) throws CatalogException {
    List<String> newFiles = updatedPartitions.get(partName).getFiles();
    if (!newFiles.isEmpty() || isInsertOverwrite) {
      LOG.info("{} new files detected for table {}{}",
          newFiles.size(), table.getFullName(),
          isPartitioned ? " partition " + partName : "");
      insertEventReqDatas.add(
          makeInsertEventData(table, partVals, newFiles, isInsertOverwrite));
      insertEventPartVals.add(partVals);
    }
  }

  /**
   * Keeps track of the self-generated INSERT events for self-event detection.
   * Each item in 'eventIds' corresponds to the partition in 'existingPartsInHms'
   * based on the iteration order.
   * Note that this is not called for transactional tables since INSERT on them
   * don't trigger INSERT events.
   */
  private void trackInsertEvents(HdfsTable table, List<Long> eventIds,
      LinkedHashSet<String> existingPartsInHms) {
    if (eventIds == null || eventIds.isEmpty()) return;
    if (table.getNumClusteringCols() == 0) { // insert into table
      Preconditions.checkState(eventIds.size() == 1);
      catalog_.addVersionsForInflightEvents(true, table, eventIds.get(0));
    } else { // insert into partition
      Preconditions.checkState(existingPartsInHms.size() == eventIds.size());
      int par_idx = 0;
      for (String partName : existingPartsInHms) {
        long eventId = eventIds.get(par_idx++);
        if (!table.addInflightInsertEventToPartition(partName, eventId)) {
          LOG.warn("INSERT event {} on partition {} of table {} are not tracked since " +
              "it doesn't exist in catalogd cache", eventId, partName,
              table.getFullName());
        }
      }
    }
  }

  private boolean shouldGenerateInsertEvents(FeFsTable table) {
    if (table instanceof FeIcebergTable) return false;
    return BackendConfig.INSTANCE.isInsertEventsEnabled();
  }

  private InsertEventRequestData makeInsertEventData(FeFsTable tbl, List<String> partVals,
      List<String> newFiles, boolean isInsertOverwrite) throws CatalogException {
    Preconditions.checkNotNull(newFiles);
    Preconditions.checkNotNull(partVals);
    InsertEventRequestData insertEventRequestData = new InsertEventRequestData(
        Lists.newArrayListWithCapacity(
            newFiles.size()));
    boolean isTransactional = AcidUtils
        .isTransactionalTable(tbl.getMetaStoreTable().getParameters());
    // in case of unpartitioned table, partVals will be empty
    boolean isPartitioned = !partVals.isEmpty();
    if (isPartitioned) {
      MetastoreShim.setPartitionVal(insertEventRequestData, partVals);
    }
    // Get table file system with table location.
    FileSystem tableFs = tbl.getFileSystem();
    FileSystem fs;
    for (String file : newFiles) {
      try {
        Path filePath = new Path(file);
        if (!isPartitioned) {
          fs = tableFs;
        } else {
          // Partitions may be in different file systems.
          fs = FeFsTable.getFileSystem(filePath);
        }
        FileChecksum checkSum = fs.getFileChecksum(filePath);
        String checksumStr = checkSum == null ? ""
            : StringUtils.byteToHexString(checkSum.getBytes(), 0, checkSum.getLength());
        insertEventRequestData.addToFilesAdded(file);
        insertEventRequestData.addToFilesAddedChecksum(checksumStr);
        if (isTransactional) {
          String acidDirPath = AcidUtils.getFirstLevelAcidDirPath(filePath, fs);
          if (acidDirPath != null) {
            MetastoreShim.addToSubDirectoryList(insertEventRequestData, acidDirPath);
          }
        }
        insertEventRequestData.setReplace(isInsertOverwrite);
      } catch (IOException e) {
        if (tbl instanceof FeIcebergTable) {
          // TODO IMPALA-10254: load data files via Iceberg API. Currently we load
          // Iceberg data files via file listing, so we might see files being written.
          continue;
        }
        throw new CatalogException("Could not get the file checksum for file " + file, e);
      }
    }
    return insertEventRequestData;
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
  public Table getExistingTable(String dbName, String tblName, String reason,
      EventSequence catalogTimeline) throws CatalogException {
    // passing null validWriteIdList makes sure that we return the table if it is
    // already loaded.
    Table tbl = catalog_.getOrLoadTable(dbName, tblName, reason, null,
        TABLE_ID_UNAVAILABLE, catalogTimeline);
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

  private void alterCommentOn(TDdlExecRequest ddlRequest, TDdlExecResponse response,
      Optional<TTableName> tTableName, boolean wantMinimalResult,
      EventSequence catalogTimeline)
      throws ImpalaRuntimeException, CatalogException, InternalException {
    Preconditions.checkState(tTableName.isPresent());
    TCommentOnParams params = ddlRequest.getComment_on_params();
    if (params.getDb() != null) {
      Preconditions.checkArgument(!params.isSetTable_name() &&
          !params.isSetColumn_name());
      tTableName.get().setDb_name(params.db);
      catalogOpTracker_.increment(ddlRequest, tTableName);
      alterCommentOnDb(params.getDb(), params.getComment(), wantMinimalResult, response,
          catalogTimeline);
    } else if (params.getTable_name() != null) {
      Preconditions.checkArgument(!params.isSetDb() && !params.isSetColumn_name());
      tTableName.get().setDb_name(params.table_name.db_name);
      tTableName.get().setTable_name(params.table_name.table_name);
      catalogOpTracker_.increment(ddlRequest, tTableName);
      alterCommentOnTableOrView(TableName.fromThrift(params.getTable_name()),
          params.getComment(), wantMinimalResult, response, catalogTimeline,
          ddlRequest.getQuery_options().getDebug_action());
    } else if (params.getColumn_name() != null) {
      Preconditions.checkArgument(!params.isSetDb() && !params.isSetTable_name());
      TColumnName columnName = params.getColumn_name();
      tTableName.get().setDb_name(columnName.table_name.table_name);
      tTableName.get().setTable_name(columnName.table_name.table_name);
      catalogOpTracker_.increment(ddlRequest, tTableName);
      alterCommentOnColumn(TableName.fromThrift(columnName.getTable_name()),
          columnName.getColumn_name(), params.getComment(), wantMinimalResult, response,
          catalogTimeline, ddlRequest.getQuery_options().getDebug_action());
    } else {
      throw new UnsupportedOperationException("Unsupported COMMENT ON operation");
    }
  }

  private void alterCommentOnDb(String dbName, String comment, boolean wantMinimalResult,
      TDdlExecResponse response, EventSequence catalogTimeline)
      throws ImpalaRuntimeException, CatalogException, InternalException {
    Db db = catalog_.getDb(dbName);
    if (db == null) {
      throw new CatalogException("Database: " + dbName + " does not exist.");
    }
    tryLock(db, "altering the comment", catalogTimeline);
    // Get a new catalog version to assign to the database being altered.
    long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
    try {
      Database msDb = db.getMetaStoreDb().deepCopy();
      addCatalogServiceIdentifiers(msDb, catalog_.getCatalogServiceId(),
          newCatalogVersion);
      msDb.setDescription(comment);
      try {
        applyAlterDatabase(msDb, catalogTimeline);
      } catch (ImpalaRuntimeException e) {
        throw e;
      }
      Db updatedDb = catalog_.updateDb(msDb);
      catalogTimeline.markEvent("Updated database in catalog cache");
      addDbToCatalogUpdate(updatedDb, wantMinimalResult, response.result);
      // now that HMS alter operation has succeeded, add this version to list of inflight
      // events in catalog database if event processing is enabled
      catalog_.addVersionsForInflightEvents(db, newCatalogVersion);
    } finally {
      db.getLock().unlock();
    }
    addSummary(response, "Updated database.");
  }

  private void alterDatabase(TAlterDbParams params, boolean wantMinimalResult,
      TDdlExecResponse response, EventSequence catalogTimeline) throws ImpalaException {
    Preconditions.checkNotNull(params);
    String dbName = params.getDb();
    Db db = catalog_.getDb(dbName);
    if (db == null) {
      throw new CatalogException("Database: " + dbName + " does not exist.");
    }
    switch (params.getAlter_type()) {
      case SET_OWNER:
        alterDatabaseSetOwner(db, params.getSet_owner_params(), wantMinimalResult,
            response, catalogTimeline);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown ALTER DATABASE operation type: " + params.getAlter_type());
    }
  }

  private void alterDatabaseSetOwner(Db db, TAlterDbSetOwnerParams params,
      boolean wantMinimalResult, TDdlExecResponse response,
      EventSequence catalogTimeline) throws ImpalaException {
    Preconditions.checkNotNull(params.owner_name);
    Preconditions.checkNotNull(params.owner_type);
    tryLock(db, "altering the owner", catalogTimeline);
    // Get a new catalog version to assign to the database being altered.
    long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
    try {
      Database msDb = db.getMetaStoreDb().deepCopy();
      addCatalogServiceIdentifiers(msDb, catalog_.getCatalogServiceId(),
          newCatalogVersion);
      String originalOwnerName = msDb.getOwnerName();
      PrincipalType originalOwnerType = msDb.getOwnerType();
      msDb.setOwnerName(params.owner_name);
      msDb.setOwnerType(PrincipalType.valueOf(params.owner_type.name()));
      try {
        applyAlterDatabase(msDb, catalogTimeline);
      } catch (ImpalaRuntimeException e) {
        throw e;
      }
      if (authzConfig_.isEnabled()) {
        authzManager_.updateDatabaseOwnerPrivilege(params.server_name, db.getName(),
            originalOwnerName, originalOwnerType, msDb.getOwnerName(),
            msDb.getOwnerType(), response);
      }
      Db updatedDb = catalog_.updateDb(msDb);
      catalogTimeline.markEvent("Updated database in catalog cache");
      addDbToCatalogUpdate(updatedDb, wantMinimalResult, response.result);
      // now that HMS alter operation has succeeded, add this version to list of inflight
      // events in catalog database if event processing is enabled
      catalog_.addVersionsForInflightEvents(db, newCatalogVersion);
    } finally {
      db.getLock().unlock();
    }
    addSummary(response, "Updated database.");
  }

  /**
   * Adds the catalog service id and the given catalog version to the database parameters.
   * No-op if event processing is disabled
   */
  private void addCatalogServiceIdentifiers(
      Database msDb, String catalogServiceId, long newCatalogVersion) {
    if (!catalog_.isEventProcessingActive()) return;
    Preconditions.checkNotNull(msDb);
    msDb.putToParameters(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(),
        catalogServiceId);
    msDb.putToParameters(MetastoreEventPropertyKey.CATALOG_VERSION.getKey(),
        String.valueOf(newCatalogVersion));
  }

  private void addDbToCatalogUpdate(Db db, boolean wantMinimalResult,
      TCatalogUpdateResult result) {
    Preconditions.checkNotNull(db);
    TCatalogObject updatedCatalogObject = wantMinimalResult ?
        db.toMinimalTCatalogObject() : db.toTCatalogObject();
    result.addToUpdated_catalog_objects(updatedCatalogObject);
    result.setVersion(updatedCatalogObject.getCatalog_version());
  }

  private void alterCommentOnTableOrView(TableName tableName, String comment,
      boolean wantMinimalResult, TDdlExecResponse response, EventSequence catalogTimeline,
      @Nullable String debugAction)
      throws CatalogException, InternalException, ImpalaRuntimeException {
    Table tbl = getExistingTable(tableName.getDb(), tableName.getTbl(),
        "Load for ALTER COMMENT", catalogTimeline);
    tryWriteLock(tbl, catalogTimeline);
    try {
      InProgressTableModification modification =
          new InProgressTableModification(catalog_, tbl);
      catalog_.getLock().writeLock().unlock();
      modification.addCatalogServiceIdentifiersToTable();
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      boolean isView = msTbl.getTableType().equalsIgnoreCase(
          TableType.VIRTUAL_VIEW.toString());
      if (comment == null) {
        msTbl.getParameters().remove("comment");
      } else {
        msTbl.getParameters().put("comment", comment);
      }
      applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
      loadTableMetadata(tbl, modification.newVersionNumber(), false, false,
          "ALTER COMMENT", debugAction, catalogTimeline);
      addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
      addSummary(response, String.format("Updated %s.", (isView) ? "view" : "table"));
      modification.validateInProgressModificationComplete();
    } finally {
      tbl.releaseWriteLock();
    }
  }

  private void alterCommentOnColumn(TableName tableName, String columnName,
      String comment, boolean wantMinimalResult, TDdlExecResponse response,
      EventSequence catalogTimeline, @Nullable String debugAction)
      throws CatalogException, InternalException, ImpalaRuntimeException {
    Table tbl = getExistingTable(tableName.getDb(), tableName.getTbl(),
        "Load for ALTER COLUMN COMMENT", catalogTimeline);
    tryWriteLock(tbl, catalogTimeline);
    try {
      InProgressTableModification modification =
          new InProgressTableModification(catalog_, tbl);
      catalog_.getLock().writeLock().unlock();
      modification.addCatalogServiceIdentifiersToTable();
      if (tbl instanceof KuduTable) {
        TColumn new_col = new TColumn(columnName,
            tbl.getColumn(columnName).getType().toThrift());
        new_col.setComment(comment != null ? comment : "");
        KuduCatalogOpExecutor.alterColumn((KuduTable) tbl, columnName, new_col,
            catalogTimeline);
      } else {
        org.apache.hadoop.hive.metastore.api.Table msTbl =
            tbl.getMetaStoreTable().deepCopy();
        if (!updateColumnComment(msTbl.getSd().getColsIterator(), columnName, comment)) {
          if (!updateColumnComment(msTbl.getPartitionKeysIterator(), columnName,
              comment)) {
            throw new ColumnNotFoundException(String.format(
                "Column name %s not found in table %s.", columnName, tbl.getFullName()));
          }
        }
        applyAlterAndInProgressTableModification(msTbl, catalogTimeline, modification);
      }
      loadTableMetadata(tbl, modification.newVersionNumber(), false, true,
          "ALTER COLUMN COMMENT", debugAction, catalogTimeline);
      addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
      addSummary(response, "Column has been altered.");
      modification.validateInProgressModificationComplete();
    } finally {
      tbl.releaseWriteLock();
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
   * Tries to take the write lock of the table in the catalog. Throws an InternalException
   * if the catalog is unable to lock the given table.
   */
  private void tryWriteLock(Table tbl, EventSequence catalogTimeline)
      throws InternalException {
    tryWriteLock(tbl, "altering", catalogTimeline);
  }

  /**
   * Try to lock a table in the catalog for a given operation. Throw an InternalException
   * if the catalog is unable to lock the given table.
   */
  private void tryWriteLock(Table tbl, String operation, EventSequence catalogTimeline)
      throws InternalException {
    String type = tbl instanceof View ? "view" : "table";
    if (!catalog_.tryWriteLock(tbl, catalogTimeline)) {
      throw new InternalException(String.format("Error %s (for) %s %s due to " +
          "lock contention.", operation, type, tbl.getFullName()));
    }
  }

  /**
   * Try to lock the given Db in the catalog for the given operation. Throws
   * InternalException if catalog is unable to lock the database.
   */
  private void tryLock(Db db, String operation, EventSequence catalogTimeline)
      throws InternalException {
    if (!catalog_.tryLockDb(db)) {
      throw new InternalException(String.format("Error %s of database %s due to lock "
          + "contention.", operation, db.getName()));
    }
    catalogTimeline.markEvent("Got database lock");
  }

  /**
   * Commits ACID transaction with given transaction id.
   * @param transactionId is the id of the transaction.
   * @throws TransactionException
   */
  private void commitTransaction(long transactionId, EventSequence catalogTimeline)
      throws TransactionException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      MetastoreShim.commitTransaction(msClient.getHiveClient(), transactionId);
      catalogTimeline.markEvent("Committed Metastore transaction " + transactionId);
      LOG.info("Committed transaction: " + transactionId);
    }
  }

  /**
   * Update table properties to remove the COLUMN_STATS_ACCURATE entry if it exists.
   */
  private void unsetTableColStats(org.apache.hadoop.hive.metastore.api.Table msTable,
      TblTransaction tblTxn, EventSequence catalogTimeline)
      throws ImpalaRuntimeException {
    Map<String, String> params = msTable.getParameters();
    if (params != null && params.containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
      params.remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
      applyAlterTable(msTable, false, tblTxn, catalogTimeline);
    }
  }

  /**
   * Update partitions properties to remove the COLUMN_STATS_ACCURATE entry from HMS.
   * This method assumes the partitions in the input hmsPartitionsStatsUnset already
   * had the COLUMN_STATS_ACCURATE removed from their properties.
   */
  private void unsetPartitionsColStats(org.apache.hadoop.hive.metastore.api.Table msTable,
      List<org.apache.hadoop.hive.metastore.api.Partition> hmsPartitionsStatsUnset,
      TblTransaction tblTxn, EventSequence catalogTimeline)
      throws ImpalaRuntimeException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      try {
        if (tblTxn != null) {
          MetastoreShim.alterPartitionsWithTransaction(
              msClient.getHiveClient(), msTable.getDbName(), msTable.getTableName(),
              hmsPartitionsStatsUnset,  tblTxn);
        } else {
          MetastoreShim.alterPartitions(msClient.getHiveClient(), msTable.getDbName(),
              msTable.getTableName(), hmsPartitionsStatsUnset);
        }
        catalogTimeline.markEvent("Unset partition stats in Metastore");
      } catch (TException te) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_partitions"), te);
      }
    }
  }

}
