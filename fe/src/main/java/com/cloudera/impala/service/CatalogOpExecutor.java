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

package com.cloudera.impala.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.TableName;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.CatalogServiceCatalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.ColumnNotFoundException;
import com.cloudera.impala.catalog.DataSource;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.HiveStorageDescriptorFactory;
import com.cloudera.impala.catalog.IncompleteTable;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.catalog.PartitionStatsUtil;
import com.cloudera.impala.catalog.Role;
import com.cloudera.impala.catalog.RolePrivilege;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.TableLoadingException;
import com.cloudera.impala.catalog.TableNotFoundException;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;
import com.cloudera.impala.thrift.JniCatalogConstants;
import com.cloudera.impala.thrift.TAlterTableAddPartitionParams;
import com.cloudera.impala.thrift.TAlterTableAddReplaceColsParams;
import com.cloudera.impala.thrift.TAlterTableChangeColParams;
import com.cloudera.impala.thrift.TAlterTableDropColParams;
import com.cloudera.impala.thrift.TAlterTableDropPartitionParams;
import com.cloudera.impala.thrift.TAlterTableOrViewRenameParams;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableSetCachedParams;
import com.cloudera.impala.thrift.TAlterTableSetFileFormatParams;
import com.cloudera.impala.thrift.TAlterTableSetLocationParams;
import com.cloudera.impala.thrift.TAlterTableSetTblPropertiesParams;
import com.cloudera.impala.thrift.TAlterTableUpdateStatsParams;
import com.cloudera.impala.thrift.TCatalogObject;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TCatalogUpdateResult;
import com.cloudera.impala.thrift.TColumn;
import com.cloudera.impala.thrift.TColumnStats;
import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TCreateDataSourceParams;
import com.cloudera.impala.thrift.TCreateDbParams;
import com.cloudera.impala.thrift.TCreateDropRoleParams;
import com.cloudera.impala.thrift.TCreateFunctionParams;
import com.cloudera.impala.thrift.TCreateOrAlterViewParams;
import com.cloudera.impala.thrift.TCreateTableLikeParams;
import com.cloudera.impala.thrift.TCreateTableParams;
import com.cloudera.impala.thrift.TDatabase;
import com.cloudera.impala.thrift.TDdlExecRequest;
import com.cloudera.impala.thrift.TDdlExecResponse;
import com.cloudera.impala.thrift.TDropDataSourceParams;
import com.cloudera.impala.thrift.TDropDbParams;
import com.cloudera.impala.thrift.TDropFunctionParams;
import com.cloudera.impala.thrift.TDropStatsParams;
import com.cloudera.impala.thrift.TDropTableOrViewParams;
import com.cloudera.impala.thrift.TErrorCode;
import com.cloudera.impala.thrift.TGrantRevokePrivParams;
import com.cloudera.impala.thrift.TGrantRevokeRoleParams;
import com.cloudera.impala.thrift.THdfsCachingOp;
import com.cloudera.impala.thrift.THdfsFileFormat;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.cloudera.impala.thrift.TPartitionStats;
import com.cloudera.impala.thrift.TPrivilege;
import com.cloudera.impala.thrift.TResetMetadataRequest;
import com.cloudera.impala.thrift.TResetMetadataResponse;
import com.cloudera.impala.thrift.TResultRow;
import com.cloudera.impala.thrift.TResultSet;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.cloudera.impala.thrift.TStatus;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.thrift.TTableStats;
import com.cloudera.impala.thrift.TTruncateParams;
import com.cloudera.impala.thrift.TUpdateCatalogRequest;
import com.cloudera.impala.thrift.TUpdateCatalogResponse;
import com.cloudera.impala.util.HdfsCachingUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Class used to execute Catalog Operations, including DDL and refresh/invalidate
 * metadata requests. Acts as a bridge between the Thrift catalog operation requests
 * and the non-thrift Java Catalog objects.
 * TODO: Create a Hive Metastore utility class to move code that interacts with the
 * metastore out of this class.
 */
public class CatalogOpExecutor {
  // Format string for exceptions returned by Hive Metastore RPCs.
  private final static String HMS_RPC_ERROR_FORMAT_STR =
      "Error making '%s' RPC to Hive Metastore: ";

  private final CatalogServiceCatalog catalog_;

  // Lock used to ensure that in-place modifications to cached table/db objects in
  // catalog_ and the corresponding RPC to apply the change in the HMS are atomic.
  // Such modifications are done for CREATE/DROP/ALTER TABLE/DATABASE requests.
  private final Object metastoreDdlLock_ = new Object();
  private static final Logger LOG = Logger.getLogger(CatalogOpExecutor.class);

  // The maximum number of partitions to update in one Hive Metastore RPC.
  // Used when persisting the results of COMPUTE STATS statements.
  private final static short MAX_PARTITION_UPDATES_PER_RPC = 500;

  public CatalogOpExecutor(CatalogServiceCatalog catalog) {
    catalog_ = catalog;
  }

  public TDdlExecResponse execDdlRequest(TDdlExecRequest ddlRequest)
      throws ImpalaException {
    TDdlExecResponse response = new TDdlExecResponse();
    response.setResult(new TCatalogUpdateResult());
    response.getResult().setCatalog_service_id(JniCatalog.getServiceId());
    User requestingUser = null;
    if (ddlRequest.isSetHeader()) {
      requestingUser = new User(ddlRequest.getHeader().getRequesting_user());
    }

    switch (ddlRequest.ddl_type) {
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
        response.setNew_table_created(
            createTable(ddlRequest.getCreate_table_params(), response));
        break;
      case CREATE_TABLE:
        createTable(ddlRequest.getCreate_table_params(), response);
        break;
      case CREATE_TABLE_LIKE:
        createTableLike(ddlRequest.getCreate_table_like_params(), response);
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
      case DROP_ROLE:
        createDropRole(requestingUser, ddlRequest.getCreate_drop_role_params(),
            response);
        break;
      case GRANT_ROLE:
      case REVOKE_ROLE:
        grantRevokeRoleGroup(requestingUser, ddlRequest.getGrant_revoke_role_params(),
            response);
        break;
      case GRANT_PRIVILEGE:
      case REVOKE_PRIVILEGE:
        grantRevokeRolePrivilege(requestingUser,
            ddlRequest.getGrant_revoke_priv_params(), response);
        break;
      default: throw new IllegalStateException("Unexpected DDL exec request type: " +
          ddlRequest.ddl_type);
    }
    // At this point, the operation is considered successful. If any errors occurred
    // during execution, this function will throw an exception and the CatalogServer
    // will handle setting a bad status code.
    response.getResult().setStatus(new TStatus(TErrorCode.OK, new ArrayList<String>()));
    return response;
  }

  /**
   * Execute the ALTER TABLE command according to the TAlterTableParams and refresh the
   * table metadata, except for RENAME, ADD PARTITION and DROP PARTITION.
   */
  private void alterTable(TAlterTableParams params, TDdlExecResponse response)
      throws ImpalaException {
    switch (params.getAlter_type()) {
      case ADD_REPLACE_COLUMNS:
        TAlterTableAddReplaceColsParams addReplaceColParams =
            params.getAdd_replace_cols_params();
        alterTableAddReplaceCols(TableName.fromThrift(params.getTable_name()),
            addReplaceColParams.getColumns(),
            addReplaceColParams.isReplace_existing_cols());
        break;
      case ADD_PARTITION:
        TAlterTableAddPartitionParams addPartParams = params.getAdd_partition_params();
        // Create and add HdfsPartition object to the corresponding HdfsTable and load
        // its block metadata. Get the new table object with an updated catalog version.
        // If the partition already exists in Hive and "IfNotExists" is true, then null
        // is returned.
        Table refreshedTable = alterTableAddPartition(TableName.fromThrift(
            params.getTable_name()), addPartParams.getPartition_spec(),
            addPartParams.isIf_not_exists(), addPartParams.getLocation(),
            addPartParams.getCache_op());
        response.result.setUpdated_catalog_object(TableToTCatalogObject(refreshedTable));
        response.result.setVersion(
            response.result.getUpdated_catalog_object().getCatalog_version());
        return;
      case DROP_COLUMN:
        TAlterTableDropColParams dropColParams = params.getDrop_col_params();
        alterTableDropCol(TableName.fromThrift(params.getTable_name()),
            dropColParams.getCol_name());
        break;
      case CHANGE_COLUMN:
        TAlterTableChangeColParams changeColParams = params.getChange_col_params();
        alterTableChangeCol(TableName.fromThrift(params.getTable_name()),
            changeColParams.getCol_name(), changeColParams.getNew_col_def());
        break;
      case DROP_PARTITION:
        TAlterTableDropPartitionParams dropPartParams = params.getDrop_partition_params();
        // Drop the partition from the corresponding table. Get the table object
        // with an updated catalog version. If the partition does not exist and
        // "IfExists" is true, null is returned. If "purge" option is specified
        // partition data is purged by skipping Trash, if configured.
        refreshedTable = alterTableDropPartition(TableName.fromThrift(
            params.getTable_name()), dropPartParams.getPartition_spec(),
            dropPartParams.isIf_exists(), dropPartParams.isPurge());
        response.result.setUpdated_catalog_object(TableToTCatalogObject(refreshedTable));
        response.result.setVersion(
            response.result.getUpdated_catalog_object().getCatalog_version());
        return;
      case RENAME_TABLE:
      case RENAME_VIEW:
        TAlterTableOrViewRenameParams renameParams = params.getRename_params();
        alterTableOrViewRename(TableName.fromThrift(params.getTable_name()),
            TableName.fromThrift(renameParams.getNew_table_name()),
            response);
        // Renamed table can't be fast refreshed anyway. Return now.
        return;
      case SET_FILE_FORMAT:
        TAlterTableSetFileFormatParams fileFormatParams =
            params.getSet_file_format_params();
        List<TPartitionKeyValue> fileFormatPartitionSpec = null;
        if (fileFormatParams.isSetPartition_spec()) {
          fileFormatPartitionSpec = fileFormatParams.getPartition_spec();
        }
        alterTableSetFileFormat(TableName.fromThrift(params.getTable_name()),
            fileFormatPartitionSpec, fileFormatParams.getFile_format());
        break;
      case SET_LOCATION:
        TAlterTableSetLocationParams setLocationParams = params.getSet_location_params();
        List<TPartitionKeyValue> partitionSpec = null;
        if (setLocationParams.isSetPartition_spec()) {
          partitionSpec = setLocationParams.getPartition_spec();
        }
        alterTableSetLocation(TableName.fromThrift(params.getTable_name()),
            partitionSpec, setLocationParams.getLocation());
        break;
      case SET_TBL_PROPERTIES:
        alterTableSetTblProperties(TableName.fromThrift(params.getTable_name()),
            params.getSet_tbl_properties_params());
        break;
      case UPDATE_STATS:
        Preconditions.checkState(params.isSetUpdate_stats_params());
        alterTableUpdateStats(params.getUpdate_stats_params(), response);
        break;
      case SET_CACHED:
        Preconditions.checkState(params.isSetSet_cached_params());
        if (params.getSet_cached_params().getPartition_spec() == null) {
          alterTableSetCached(TableName.fromThrift(params.getTable_name()),
              params.getSet_cached_params());
        } else {
          alterPartitionSetCached(TableName.fromThrift(params.getTable_name()),
              params.getSet_cached_params());
        }
        break;
      case RECOVER_PARTITIONS:
        alterTableRecoverPartitions(TableName.fromThrift(params.getTable_name()));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown ALTER TABLE operation type: " + params.getAlter_type());
    }

    Table refreshedTable = catalog_.reloadTable(params.getTable_name());
    response.result.setUpdated_catalog_object(TableToTCatalogObject(refreshedTable));
    response.result.setVersion(
        response.result.getUpdated_catalog_object().getCatalog_version());
  }

  /**
   * Creates a new HdfsPartition object and adds it to the corresponding HdfsTable.
   * Does not create the object in the Hive metastore.
   */
  private Table addHdfsPartition(TableName tableName, Partition partition)
      throws CatalogException {
    Preconditions.checkNotNull(partition);
    Table tbl = getExistingTable(tableName.getDb(), tableName.getTbl());
    if (!(tbl instanceof HdfsTable)) {
      throw new CatalogException("Table " + tbl.getFullName() + " is not an HDFS table");
    }
    HdfsTable hdfsTable = (HdfsTable) tbl;
    HdfsPartition hdfsPartition = hdfsTable.createPartition(partition.getSd(), partition);
    return catalog_.addPartition(hdfsPartition);
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

    synchronized (metastoreDdlLock_) {
      // Operate on a copy of the metastore table to avoid prematurely applying the
      // alteration to our cached table in case the actual alteration fails.
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      if (!msTbl.getTableType().equalsIgnoreCase((TableType.VIRTUAL_VIEW.toString()))) {
        throw new ImpalaRuntimeException(
            String.format("ALTER VIEW not allowed on a table: %s",
                tableName.toString()));
      }

      // Set the altered view attributes and update the metastore.
      setViewAttributes(params, msTbl);
      LOG.debug(String.format("Altering view %s", tableName));
      applyAlterTable(msTbl);
    }

    Table refreshedTbl = catalog_.reloadTable(tableName.toThrift());
    resp.result.setUpdated_catalog_object(TableToTCatalogObject(refreshedTbl));
    resp.result.setVersion(resp.result.getUpdated_catalog_object().getCatalog_version());
  }

  /**
   * Alters an existing table's table and column statistics. Partitions are updated
   * in batches of size 'MAX_PARTITION_UPDATES_PER_RPC'.
   */
  private void alterTableUpdateStats(TAlterTableUpdateStatsParams params,
      TDdlExecResponse resp) throws ImpalaException {
    Preconditions.checkState(params.isSetPartition_stats() && params.isSetTable_stats());

    TableName tableName = TableName.fromThrift(params.getTable_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    LOG.info(String.format("Updating table stats for: %s", tableName));

    Table table = getExistingTable(tableName.getDb(), tableName.getTbl());
    // Deep copy the msTbl to avoid updating our cache before successfully persisting
    // the results to the metastore.
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        table.getMetaStoreTable().deepCopy();
    List<HdfsPartition> partitions = Lists.newArrayList();
    if (table instanceof HdfsTable) {
      // Build a list of non-default partitions to update.
      HdfsTable hdfsTable = (HdfsTable) table;
      for (HdfsPartition p: hdfsTable.getPartitions()) {
        if (!p.isDefaultPartition()) partitions.add(p);
      }
    }

    MetaStoreClient msClient = catalog_.getMetaStoreClient();
    int numTargetedPartitions;
    int numUpdatedColumns = 0;
    try {
      // Update the table and partition row counts based on the query results.
      List<HdfsPartition> modifiedParts = Lists.newArrayList();
      numTargetedPartitions = updateTableStats(table, params, msTbl, partitions,
          modifiedParts);

      ColumnStatistics colStats = null;
      if (params.isSetColumn_stats()) {
        // Create Hive column stats from the query results.
        colStats = createHiveColStats(params.getColumn_stats(), table);
        numUpdatedColumns = colStats.getStatsObjSize();
      }

      // Update all partitions.
      bulkAlterPartitions(table.getDb().getName(), table.getName(), modifiedParts);

      synchronized (metastoreDdlLock_) {
        if (numUpdatedColumns > 0) {
          Preconditions.checkNotNull(colStats);
          // Update column stats.
          try {
            msClient.getHiveClient().updateTableColumnStatistics(colStats);
          } catch (Exception e) {
            throw new ImpalaRuntimeException(String.format(HMS_RPC_ERROR_FORMAT_STR,
                    "updateTableColumnStatistics"), e);
          }
        }
        // Update the table stats. Apply the table alteration last to ensure the
        // lastDdlTime is as accurate as possible.
        applyAlterTable(msTbl);
      }
    } finally {
      msClient.release();
    }

    // Set the results to be reported to the client.
    TResultSet resultSet = new TResultSet();
    resultSet.setSchema(new TResultSetMetadata(Lists.newArrayList(
        new TColumn("summary", Type.STRING.toThrift()))));
    TColumnValue resultColVal = new TColumnValue();
    resultColVal.setString_val("Updated " + numTargetedPartitions + " partition(s) and " +
        numUpdatedColumns + " column(s).");
    TResultRow resultRow = new TResultRow();
    resultRow.setColVals(Lists.newArrayList(resultColVal));
    resultSet.setRows(Lists.newArrayList(resultRow));
    resp.setResult_set(resultSet);
  }

  /**
   * Updates the row counts of the given Hive partitions and the total row count of the
   * given Hive table based on the given update stats parameters. The partitions whose
   * row counts have not changed are skipped. The modified partitions are returned
   * in the modifiedParts parameter.
   * Row counts for missing or new partitions as a result of concurrent table
   * alterations are set to 0.
   * Returns the number of partitions that were targeted for update (includes partitions
   * whose row counts have not changed).
   */
  private int updateTableStats(Table table, TAlterTableUpdateStatsParams params,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      List<HdfsPartition> partitions, List<HdfsPartition> modifiedParts)
      throws ImpalaException {
    Preconditions.checkState(params.isSetPartition_stats());
    Preconditions.checkState(params.isSetTable_stats());
    // Update the partitions' ROW_COUNT parameter.
    int numTargetedPartitions = 0;
    for (HdfsPartition partition: partitions) {
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
      LOG.debug(String.format("Updating stats for partition %s: numRows=%s",
          partition.getValuesAsString(), numRows));
      PartitionStatsUtil.partStatsToParameters(partitionStats, partition);
      partition.putToParameters(StatsSetupConst.ROW_COUNT, String.valueOf(numRows));
      partition.putToParameters(StatsSetupConst.STATS_GENERATED_VIA_STATS_TASK,
          StatsSetupConst.TRUE);
      ++numTargetedPartitions;
      modifiedParts.add(partition);
    }

    // For unpartitioned tables and HBase tables report a single updated partition.
    if (table.getNumClusteringCols() == 0 || table instanceof HBaseTable) {
      numTargetedPartitions = 1;
    }

    // Update the table's ROW_COUNT parameter.
    msTbl.putToParameters(StatsSetupConst.ROW_COUNT,
        String.valueOf(params.getTable_stats().num_rows));
    msTbl.putToParameters(StatsSetupConst.STATS_GENERATED_VIA_STATS_TASK,
        StatsSetupConst.TRUE);
    return numTargetedPartitions;
  }

  /**
   * Create Hive column statistics for the given table based on the give map from column
   * name to column stats. Missing or new columns as a result of concurrent table
   * alterations are ignored.
   */
  private static ColumnStatistics createHiveColStats(
      Map<String, TColumnStats> columnStats, Table table) {
    // Collection of column statistics objects to be returned.
    ColumnStatistics colStats = new ColumnStatistics();
    colStats.setStatsDesc(
        new ColumnStatisticsDesc(true, table.getDb().getName(), table.getName()));
    // Generate Hive column stats objects from the update stats params.
    for (Map.Entry<String, TColumnStats> entry: columnStats.entrySet()) {
      String colName = entry.getKey();
      Column tableCol = table.getColumn(entry.getKey());
      // Ignore columns that were dropped in the meantime.
      if (tableCol == null) continue;
      ColumnStatisticsData colStatsData =
          createHiveColStatsData(entry.getValue(), tableCol.getType());
      if (colStatsData == null) continue;
      LOG.debug(String.format("Updating column stats for %s: numDVs=%s numNulls=%s " +
          "maxSize=%s avgSize=%s", colName, entry.getValue().getNum_distinct_values(),
          entry.getValue().getNum_nulls(), entry.getValue().getMax_size(),
          entry.getValue().getAvg_size()));
      ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj(colName,
          tableCol.getType().toString(), colStatsData);
      colStats.addToStatsObj(colStatsObj);
    }
    return colStats;
  }

  private static ColumnStatisticsData createHiveColStatsData(TColumnStats colStats,
      Type colType) {
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();
    long ndvs = colStats.getNum_distinct_values();
    long numNulls = colStats.getNum_nulls();
    switch(colType.getPrimitiveType()) {
      case BOOLEAN:
        // TODO: Gather and set the numTrues and numFalse stats as well. The planner
        // currently does not rely on them.
        colStatsData.setBooleanStats(new BooleanColumnStatsData(1, -1, numNulls));
        break;
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
      case TIMESTAMP: // Hive and Impala use LongColumnStatsData for timestamps.
        // TODO: Gather and set the min/max values stats as well. The planner
        // currently does not rely on them.
        colStatsData.setLongStats(new LongColumnStatsData(numNulls, ndvs));
        break;
      case FLOAT:
      case DOUBLE:
        // TODO: Gather and set the min/max values stats as well. The planner
        // currently does not rely on them.
        colStatsData.setDoubleStats(new DoubleColumnStatsData(numNulls, ndvs));
        break;
      case CHAR:
      case VARCHAR:
      case STRING:
        long maxStrLen = colStats.getMax_size();
        double avgStrLen = colStats.getAvg_size();
        colStatsData.setStringStats(
            new StringColumnStatsData(maxStrLen, avgStrLen, numNulls, ndvs));
        break;
      case DECIMAL:
        // TODO: Gather and set the min/max values stats as well. The planner
        // currently does not rely on them.
        colStatsData.setDecimalStats(
            new DecimalColumnStatsData(numNulls, ndvs));
        break;
      default:
        return null;
    }
    return colStatsData;
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
    if (params.if_not_exists && catalog_.getDb(dbName) != null) {
      LOG.debug("Skipping database creation because " + dbName + " already exists and " +
          "IF NOT EXISTS was specified.");
      resp.getResult().setVersion(catalog_.getCatalogVersion());
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
    LOG.debug("Creating database " + dbName);
    Db newDb = null;
    synchronized (metastoreDdlLock_) {
      MetaStoreClient msClient = catalog_.getMetaStoreClient();
      try {
        msClient.getHiveClient().createDatabase(db);
        newDb = catalog_.addDb(dbName);
      } catch (AlreadyExistsException e) {
        if (!params.if_not_exists) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "createDatabase"), e);
        }
        LOG.debug(String.format("Ignoring '%s' when creating database %s because " +
            "IF NOT EXISTS was specified.", e, dbName));
        newDb = catalog_.getDb(dbName);
        if (newDb == null) newDb = catalog_.addDb(dbName);
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "createDatabase"), e);
      } finally {
        msClient.release();
      }

      Preconditions.checkNotNull(newDb);
      TCatalogObject thriftDb = new TCatalogObject(TCatalogObjectType.DATABASE,
          Catalog.INITIAL_CATALOG_VERSION);
      thriftDb.setDb(newDb.toThrift());
      thriftDb.setCatalog_version(newDb.getCatalogVersion());
      resp.result.setUpdated_catalog_object(thriftDb);
    }
    resp.result.setVersion(resp.result.getUpdated_catalog_object().getCatalog_version());
  }

  private void createFunction(TCreateFunctionParams params, TDdlExecResponse resp)
      throws ImpalaException {
    Function fn = Function.fromThrift(params.getFn());
    LOG.debug(String.format("Adding %s: %s",
        fn.getClass().getSimpleName(), fn.signatureString()));
    Function existingFn =
        catalog_.getFunction(fn, Function.CompareMode.IS_INDISTINGUISHABLE);
    if (existingFn != null && !params.if_not_exists) {
      throw new CatalogException("Function " + fn.signatureString() +
          " already exists.");
    }
    catalog_.addFunction(fn);
    TCatalogObject addedObject = new TCatalogObject();
    addedObject.setType(TCatalogObjectType.FUNCTION);
    addedObject.setFn(fn.toThrift());
    addedObject.setCatalog_version(fn.getCatalogVersion());
    resp.result.setUpdated_catalog_object(addedObject);
    resp.result.setVersion(fn.getCatalogVersion());
  }

  private void createDataSource(TCreateDataSourceParams params, TDdlExecResponse resp)
      throws ImpalaException {
    if (LOG.isDebugEnabled()) { LOG.debug("Adding DATA SOURCE: " + params.toString()); }
    DataSource dataSource = DataSource.fromThrift(params.getData_source());
    if (catalog_.getDataSource(dataSource.getName()) != null) {
      if (!params.if_not_exists) {
        throw new ImpalaRuntimeException("Data source " + dataSource.getName() +
            " already exists.");
      }
      // The user specified IF NOT EXISTS and the data source exists, just
      // return the current catalog version.
      resp.result.setVersion(catalog_.getCatalogVersion());
      return;
    }
    catalog_.addDataSource(dataSource);
    TCatalogObject addedObject = new TCatalogObject();
    addedObject.setType(TCatalogObjectType.DATA_SOURCE);
    addedObject.setData_source(dataSource.toThrift());
    addedObject.setCatalog_version(dataSource.getCatalogVersion());
    resp.result.setUpdated_catalog_object(addedObject);
    resp.result.setVersion(dataSource.getCatalogVersion());
  }

  private void dropDataSource(TDropDataSourceParams params, TDdlExecResponse resp)
      throws ImpalaException {
    if (LOG.isDebugEnabled()) { LOG.debug("Drop DATA SOURCE: " + params.toString()); }
    DataSource dataSource = catalog_.getDataSource(params.getData_source());
    if (dataSource == null) {
      if (!params.if_exists) {
        throw new ImpalaRuntimeException("Data source " + params.getData_source() +
            " does not exists.");
      }
      // The user specified IF EXISTS and the data source didn't exist, just
      // return the current catalog version.
      resp.result.setVersion(catalog_.getCatalogVersion());
      return;
    }
    catalog_.removeDataSource(params.getData_source());
    TCatalogObject removedObject = new TCatalogObject();
    removedObject.setType(TCatalogObjectType.DATA_SOURCE);
    removedObject.setData_source(dataSource.toThrift());
    removedObject.setCatalog_version(dataSource.getCatalogVersion());
    resp.result.setRemoved_catalog_object(removedObject);
    resp.result.setVersion(dataSource.getCatalogVersion());
  }

  /**
   * Drops all table and column stats from the target table in the HMS and
   * updates the Impala catalog. Throws an ImpalaException if any errors are
   * encountered as part of this operation.
   */
  private void dropStats(TDropStatsParams params, TDdlExecResponse resp)
      throws ImpalaException {
    Table table = getExistingTable(params.getTable_name().getDb_name(),
        params.getTable_name().getTable_name());
    Preconditions.checkNotNull(table);

    if (params.getPartition_spec() == null) {
      // TODO: Report the number of updated partitions/columns to the user?
      dropColumnStats(table);
      dropTableStats(table);
    } else {
      HdfsPartition partition =
          ((HdfsTable)table).getPartitionFromThriftPartitionSpec(
              params.getPartition_spec());
      if (partition == null) {
        List<String> partitionDescription = Lists.newArrayList();
        for (TPartitionKeyValue v: params.getPartition_spec()) {
          partitionDescription.add(v.name + " = " + v.value);
        }
        throw new ImpalaRuntimeException("Could not find partition: " +
            Joiner.on("/").join(partitionDescription));
      }

      if (partition.getPartitionStats() != null)  {
        synchronized (metastoreDdlLock_) {
          PartitionStatsUtil.deletePartStats(partition);
          try {
            applyAlterPartition(table.getTableName(), partition);
          } finally {
            partition.markDirty();
          }
        }
      }
    }

    Table refreshedTable = catalog_.reloadTable(params.getTable_name());
    resp.result.setUpdated_catalog_object(TableToTCatalogObject(refreshedTable));
    resp.result.setVersion(
        resp.result.getUpdated_catalog_object().getCatalog_version());
  }

  /**
   * Drops all column stats from the table in the HMS. Returns the number of columns
   * that were updated as part of this operation.
   */
  private int dropColumnStats(Table table) throws ImpalaRuntimeException {
    MetaStoreClient msClient = null;
    int numColsUpdated = 0;
    try {
      msClient = catalog_.getMetaStoreClient();

      for (Column col: table.getColumns()) {
        // Skip columns that don't have stats.
        if (!col.getStats().hasStats()) continue;

        try {
          synchronized (metastoreDdlLock_) {
            msClient.getHiveClient().deleteTableColumnStatistics(
                table.getDb().getName(), table.getName(), col.getName());
            ++numColsUpdated;
          }
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
    } finally {
      if (msClient != null) msClient.release();
    }
    return numColsUpdated;
  }

  /**
   * Drops all table and partition stats from this table in the HMS.
   * Partitions are updated in batches of MAX_PARTITION_UPDATES_PER_RPC. Returns
   * the number of partitions updated as part of this operation, or 1 if the table
   * is unpartitioned.
   */
  private int dropTableStats(Table table) throws ImpalaRuntimeException {
    // Delete the ROW_COUNT from the table (if it was set).
    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getMetaStoreTable();
    int numTargetedPartitions = 0;
    if (msTbl.getParameters().remove(StatsSetupConst.ROW_COUNT) != null) {
      applyAlterTable(msTbl);
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
    for (HdfsPartition part: hdfsTable.getPartitions()) {
      boolean isModified = false;
      // The default partition is an Impala-internal abstraction and is not
      // represented in the Hive Metastore.
      if (part.getId() == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
        continue;
      }
      if (part.getPartitionStats() != null) {
        PartitionStatsUtil.deletePartStats(part);
        isModified = true;
      }

      // Remove the ROW_COUNT parameter if it has been set.
      if (part.getParameters().remove(StatsSetupConst.ROW_COUNT) != null) {
        isModified = true;
      }

      if (isModified) modifiedParts.add(part);
    }

    bulkAlterPartitions(table.getDb().getName(), table.getName(), modifiedParts);
    return modifiedParts.size();
  }

  /**
   * Drops a database from the metastore and removes the database's metadata from the
   * internal cache. Re-throws any Hive Meta Store exceptions encountered during
   * the drop.
   */
  private void dropDatabase(TDropDbParams params, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(params);
    Preconditions.checkState(params.getDb() != null && !params.getDb().isEmpty(),
        "Null or empty database name passed as argument to Catalog.dropDatabase");

    LOG.debug("Dropping database " + params.getDb());
    Db db = catalog_.getDb(params.db);
    if (db != null && db.numFunctions() > 0 && !params.cascade) {
      throw new CatalogException("Database " + db.getName() + " is not empty");
    }

    TCatalogObject removedObject = new TCatalogObject();
    MetaStoreClient msClient = catalog_.getMetaStoreClient();
    synchronized (metastoreDdlLock_) {
      try {
        msClient.getHiveClient().dropDatabase(
            params.getDb(), true, params.if_exists, params.cascade);
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "dropDatabase"), e);
      } finally {
        msClient.release();
      }
      Db removedDb = catalog_.removeDb(params.getDb());
      // If no db was removed as part of this operation just return the current catalog
      // version.
      if (removedDb == null) {
        removedObject.setCatalog_version(catalog_.getCatalogVersion());
      } else {
        removedObject.setCatalog_version(removedDb.getCatalogVersion());
      }
    }
    removedObject.setType(TCatalogObjectType.DATABASE);
    removedObject.setDb(new TDatabase());
    removedObject.getDb().setDb_name(params.getDb());
    resp.result.setVersion(removedObject.getCatalog_version());
    resp.result.setRemoved_catalog_object(removedObject);
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
    LOG.debug(String.format("Dropping table/view %s", tableName));

    TCatalogObject removedObject = new TCatalogObject();
    synchronized (metastoreDdlLock_) {
      MetaStoreClient msClient = catalog_.getMetaStoreClient();
      try {
        msClient.getHiveClient().dropTable(
            tableName.getDb(), tableName.getTbl(), true, params.if_exists, params.purge);
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "dropTable"), e);
      } finally {
        msClient.release();
      }

      Table table = catalog_.removeTable(params.getTable_name().db_name,
          params.getTable_name().table_name);
      if (table != null) {
        resp.result.setVersion(table.getCatalogVersion());
        if (table instanceof HdfsTable) {
          HdfsTable hdfsTable = (HdfsTable) table;
          if (hdfsTable.isMarkedCached()) {
            try {
              HdfsCachingUtil.uncacheTbl(table.getMetaStoreTable());
            } catch (Exception e) {
              LOG.error("Unable to uncache table: " + table.getFullName(), e);
            }
          }
          if (table.getNumClusteringCols() > 0) {
            for (HdfsPartition partition: hdfsTable.getPartitions()) {
              if (partition.isMarkedCached()) {
                try {
                  HdfsCachingUtil.uncachePartition(partition);
                } catch (Exception e) {
                  LOG.error("Unable to uncache partition: " +
                      partition.getPartitionName(), e);
                }
              }
            }
          }
        }
      } else {
        resp.result.setVersion(catalog_.getCatalogVersion());
      }
    }
    removedObject.setType(TCatalogObjectType.TABLE);
    removedObject.setTable(new TTable());
    removedObject.getTable().setTbl_name(tableName.getTbl());
    removedObject.getTable().setDb_name(tableName.getDb());
    removedObject.setCatalog_version(resp.result.getVersion());
    resp.result.setRemoved_catalog_object(removedObject);
  }

  /**
   * Truncate a table by deleting all files in its partition directories, and dropping
   * all column and table statistics.
   * TODO truncate specified partitions.
   */
  private void truncateTable(TTruncateParams params, TDdlExecResponse resp)
      throws ImpalaException {
    synchronized (metastoreDdlLock_) {
      TTableName tblName = params.getTable_name();
      try {
        Table table = getExistingTable(tblName.getDb_name(), tblName.getTable_name());
        Preconditions.checkNotNull(table);
        if (!(table instanceof HdfsTable)) {
          throw new CatalogException(
              String.format("TRUNCATE TABLE not supported on non-HDFS table: %s",
              table.getFullName()));
        }

        HdfsTable hdfsTable = (HdfsTable)table;
        for (HdfsPartition part: hdfsTable.getPartitions()) {
          if (part.isDefaultPartition()) continue;
          FileSystemUtil.deleteAllVisibleFiles(new Path(part.getLocation()));
        }

        dropColumnStats(table);
        dropTableStats(table);
      } catch (Exception e) {
        String fqName = tblName.db_name + "." + tblName.table_name;
        throw new CatalogException(String.format("Failed to truncate table: %s.\n" +
            "Table may be in a partially truncated state.", fqName), e);
      }
    }

    // Reload the table to pick up on the now empty directories.
    Table refreshedTbl = catalog_.reloadTable(params.getTable_name());
    resp.getResult().setUpdated_catalog_object(TableToTCatalogObject(refreshedTbl));
    resp.getResult().setVersion(
        resp.getResult().getUpdated_catalog_object().getCatalog_version());
  }

  private void dropFunction(TDropFunctionParams params, TDdlExecResponse resp)
      throws ImpalaException {
    ArrayList<Type> argTypes = Lists.newArrayList();
    for (TColumnType t: params.arg_types) {
      argTypes.add(Type.fromThrift(t));
    }
    Function desc = new Function(FunctionName.fromThrift(params.fn_name),
        argTypes, Type.INVALID, false);
    LOG.debug(String.format("Dropping Function %s", desc.signatureString()));
    Function fn = catalog_.removeFunction(desc);
    if (fn == null) {
      if (!params.if_exists) {
        throw new CatalogException(
            "Function: " + desc.signatureString() + " does not exist.");
      }
      // The user specified IF NOT EXISTS and the function didn't exist, just
      // return the current catalog version.
      resp.result.setVersion(catalog_.getCatalogVersion());
    } else {
      TCatalogObject removedObject = new TCatalogObject();
      removedObject.setType(TCatalogObjectType.FUNCTION);
      removedObject.setFn(fn.toThrift());
      removedObject.setCatalog_version(fn.getCatalogVersion());
      resp.result.setRemoved_catalog_object(removedObject);
      resp.result.setVersion(fn.getCatalogVersion());
    }
  }

  /**
   * Creates a new table in the metastore and adds an entry to the metadata cache to
   * lazily load the new metadata on the next access. Re-throws any Hive Meta Store
   * exceptions encountered during the create.
   */
  private boolean createTable(TCreateTableParams params, TDdlExecResponse response)
      throws ImpalaException {
    Preconditions.checkNotNull(params);
    TableName tableName = TableName.fromThrift(params.getTable_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
        params.getColumns().size() > 0,
        "Null or empty column list given as argument to Catalog.createTable");

    if (params.if_not_exists &&
        catalog_.containsTable(tableName.getDb(), tableName.getTbl())) {
      LOG.debug(String.format("Skipping table creation because %s already exists and " +
          "IF NOT EXISTS was specified.", tableName));
      response.getResult().setVersion(catalog_.getCatalogVersion());
      return false;
    }
    org.apache.hadoop.hive.metastore.api.Table tbl =
        createMetaStoreTable(params);
    LOG.debug(String.format("Creating table %s", tableName));
    return createTable(tbl, params.if_not_exists, params.getCache_op(), response);
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
      LOG.debug(String.format("Skipping view creation because %s already exists and " +
          "ifNotExists is true.", tableName));
    }

    // Create new view.
    org.apache.hadoop.hive.metastore.api.Table view =
        new org.apache.hadoop.hive.metastore.api.Table();
    setViewAttributes(params, view);
    LOG.debug(String.format("Creating view %s", tableName));
    createTable(view, params.if_not_exists, null, response);
  }

  /**
   * Creates a new table in the metastore based on the definition of an existing table.
   * No data is copied as part of this process, it is a metadata only operation. If the
   * creation succeeds, an entry is added to the metadata cache to lazily load the new
   * table's metadata on the next access.
   */
  private void createTableLike(TCreateTableLikeParams params, TDdlExecResponse response)
      throws ImpalaException {
    Preconditions.checkNotNull(params);

    THdfsFileFormat fileFormat =
        params.isSetFile_format() ? params.getFile_format() : null;
    String comment = params.isSetComment() ? params.getComment() : null;
    TableName tblName = TableName.fromThrift(params.getTable_name());
    TableName srcTblName = TableName.fromThrift(params.getSrc_table_name());
    Preconditions.checkState(tblName != null && tblName.isFullyQualified());
    Preconditions.checkState(srcTblName != null && srcTblName.isFullyQualified());

    if (params.if_not_exists &&
        catalog_.containsTable(tblName.getDb(), tblName.getTbl())) {
      LOG.debug(String.format("Skipping table creation because %s already exists and " +
          "IF NOT EXISTS was specified.", tblName));
      response.getResult().setVersion(catalog_.getCatalogVersion());
      return;
    }
    Table srcTable = getExistingTable(srcTblName.getDb(), srcTblName.getTbl());
    org.apache.hadoop.hive.metastore.api.Table tbl =
        srcTable.getMetaStoreTable().deepCopy();
    tbl.setDbName(tblName.getDb());
    tbl.setTableName(tblName.getTbl());
    tbl.setOwner(params.getOwner());
    if (tbl.getParameters() == null) {
      tbl.setParameters(new HashMap<String, String>());
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
    // The LOCATION property should not be copied from the old table. If the location
    // is null (the caller didn't specify a custom location) this will clear the value
    // and the table will use the default table location from the parent database.
    tbl.getSd().setLocation(params.getLocation());
    if (fileFormat != null) {
      setStorageDescriptorFileFormat(tbl.getSd(), fileFormat);
    } else if (fileFormat == null && srcTable instanceof View) {
      // Here, source table is a view which has no input format. So to be
      // consistent with CREATE TABLE, default input format is assumed to be
      // TEXT unless otherwise specified.
      setStorageDescriptorFileFormat(tbl.getSd(), THdfsFileFormat.TEXT);
    }
    // Set the row count of this table to unknown.
    tbl.putToParameters(StatsSetupConst.ROW_COUNT, "-1");
    LOG.debug(String.format("Creating table %s LIKE %s", tblName, srcTblName));
    createTable(tbl, params.if_not_exists, null, response);
  }

  /**
   * Creates a new table in the HMS. If ifNotExists=true, no error will be thrown if
   * the table already exists, otherwise an exception will be thrown.
   * Accepts an optional 'cacheOp' param, which if specified will cache the table's
   * HDFS location according to the 'cacheOp' spec after creation.
   * Stores details of the operations (such as the resulting catalog version) in
   * 'response' output parameter.
   * Returns true if a new table was created as part of this call, false otherwise.
   */
  private boolean createTable(org.apache.hadoop.hive.metastore.api.Table newTable,
      boolean ifNotExists, THdfsCachingOp cacheOp, TDdlExecResponse response)
      throws ImpalaException {
    MetaStoreClient msClient = catalog_.getMetaStoreClient();
    synchronized (metastoreDdlLock_) {
      try {
        msClient.getHiveClient().createTable(newTable);
        // If this table should be cached, and the table location was not specified by
        // the user, an extra step is needed to read the table to find the location.
        if (cacheOp != null && cacheOp.isSet_cached() &&
            newTable.getSd().getLocation() == null) {
          newTable = msClient.getHiveClient().getTable(newTable.getDbName(),
              newTable.getTableName());
        }
      } catch (AlreadyExistsException e) {
        if (!ifNotExists) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "createTable"), e);
        }
        LOG.debug(String.format("Ignoring '%s' when creating table %s.%s because " +
            "IF NOT EXISTS was specified.", e,
            newTable.getDbName(), newTable.getTableName()));
        return false;
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "createTable"), e);
      }
        finally {
        msClient.release();
      }
    }

    // Submit the cache request and update the table metadata.
    if (cacheOp != null && cacheOp.isSet_cached()) {
      short replication = cacheOp.isSetReplication() ? cacheOp.getReplication() :
          JniCatalogConstants.HDFS_DEFAULT_CACHE_REPLICATION_FACTOR;
      long id = HdfsCachingUtil.submitCacheTblDirective(newTable,
          cacheOp.getCache_pool_name(), replication);
      catalog_.watchCacheDirs(Lists.<Long>newArrayList(id),
          new TTableName(newTable.getDbName(), newTable.getTableName()));
      applyAlterTable(newTable);
    }

    Table newTbl = catalog_.addTable(newTable.getDbName(), newTable.getTableName());
    response.result.setUpdated_catalog_object(TableToTCatalogObject(newTbl));
    response.result.setVersion(
        response.result.getUpdated_catalog_object().getCatalog_version());
    return true;
  }

  /**
   * Sets the given params in the metastore table as appropriate for a view.
   */
  private void setViewAttributes(TCreateOrAlterViewParams params,
      org.apache.hadoop.hive.metastore.api.Table view) {
    view.setTableType(TableType.VIRTUAL_VIEW.toString());
    view.setViewOriginalText(params.getOriginal_view_def());
    view.setViewExpandedText(params.getExpanded_view_def());
    view.setDbName(params.getView_name().getDb_name());
    view.setTableName(params.getView_name().getTable_name());
    view.setOwner(params.getOwner());
    if (view.getParameters() == null) view.setParameters(new HashMap<String, String>());
    if (params.isSetComment() &&  params.getComment() != null) {
      view.getParameters().put("comment", params.getComment());
    }

    // Add all the columns to a new storage descriptor.
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(buildFieldSchemaList(params.getColumns()));
    // Set a dummy SerdeInfo for Hive.
    sd.setSerdeInfo(new SerDeInfo());
    view.setSd(sd);
  }

  /**
   * Appends one or more columns to the given table, optionally replacing all existing
   * columns.
   */
  private void alterTableAddReplaceCols(TableName tableName, List<TColumn> columns,
      boolean replaceExistingCols) throws ImpalaException {
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);

    List<FieldSchema> newColumns = buildFieldSchemaList(columns);
    if (replaceExistingCols) {
      msTbl.getSd().setCols(newColumns);
    } else {
      // Append the new column to the existing list of columns.
      for (FieldSchema fs: buildFieldSchemaList(columns)) {
        msTbl.getSd().addToCols(fs);
      }
    }
    applyAlterTable(msTbl);
  }

  /**
   * Changes the column definition of an existing column. This can be used to rename a
   * column, add a comment to a column, or change the datatype of a column.
   */
  private void alterTableChangeCol(TableName tableName, String colName,
      TColumn newCol) throws ImpalaException {
    synchronized (metastoreDdlLock_) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      // Find the matching column name and change it.
      Iterator<FieldSchema> iterator = msTbl.getSd().getColsIterator();
      while (iterator.hasNext()) {
        FieldSchema fs = iterator.next();
        if (fs.getName().toLowerCase().equals(colName.toLowerCase())) {
          fs.setName(newCol.getColumnName());
          Type type = Type.fromThrift(newCol.getColumnType());
          fs.setType(type.toString().toLowerCase());
          // Don't overwrite the existing comment unless a new comment is given
          if (newCol.getComment() != null) {
            fs.setComment(newCol.getComment());
          }
          break;
        }
        if (!iterator.hasNext()) {
          throw new ColumnNotFoundException(String.format(
              "Column name %s not found in table %s.", colName, tableName));
        }
      }
      applyAlterTable(msTbl);
    }
  }

  /**
   * Adds a new partition to the given table in Hive. Also creates and adds
   * a new HdfsPartition to the corresponding HdfsTable.
   * If cacheOp is not null, the partition's location will be cached according
   * to the cacheOp. If cacheOp is null, the new partition will inherit the
   * the caching properties of the parent table.
   * Returns null if the partition already exists in Hive and "IfNotExists"
   * is true. Otherwise, returns the table object with an updated catalog version.
   */
  private Table alterTableAddPartition(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, boolean ifNotExists, String location,
      THdfsCachingOp cacheOp) throws ImpalaException {
    if (ifNotExists && catalog_.containsHdfsPartition(tableName.getDb(),
        tableName.getTbl(), partitionSpec)) {
      LOG.debug(String.format("Skipping partition creation because (%s) already exists" +
          " and ifNotExists is true.", Joiner.on(", ").join(partitionSpec)));
      return null;
    }

    org.apache.hadoop.hive.metastore.api.Partition partition = null;
    Table result = null;
    List<Long> cacheIds = null;
    synchronized (metastoreDdlLock_) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      Long parentTblCacheDirId =
          HdfsCachingUtil.getCacheDirectiveId(msTbl.getParameters());

      MetaStoreClient msClient = catalog_.getMetaStoreClient();
      partition = createHmsPartition(partitionSpec, msTbl, tableName, location);

      try {
        // Add the new partition.
        partition = msClient.getHiveClient().add_partition(partition);
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
          cacheIds = Lists.<Long>newArrayList(id);
          // Update the partition metadata to include the cache directive id.
          msClient.getHiveClient().alter_partition(partition.getDbName(),
              partition.getTableName(), partition);
        }
        updateLastDdlTime(msTbl, msClient);
      } catch (AlreadyExistsException e) {
        if (!ifNotExists) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "add_partition"), e);
        }
        LOG.debug(String.format("Ignoring '%s' when adding partition to %s because" +
            " ifNotExists is true.", e, tableName));
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "add_partition"), e);
      } finally {
        msClient.release();
      }
    }
    if (cacheIds != null) catalog_.watchCacheDirs(cacheIds, tableName.toThrift());
    // Return the table object with an updated catalog version after creating the
    // partition.
    result = addHdfsPartition(tableName, partition);
    return result;
  }

  /**
   * Drops an existing partition from the given table in Hive. If the partition is cached,
   * the associated cache directive will also be removed.
   * Also drops the partition from its Hdfs table.
   * Returns the table object with an updated catalog version. If the partition does not
   * exist and "IfExists" is true, null is returned. If purge is true, partition data is
   * permanently deleted.
   */
  private Table alterTableDropPartition(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, boolean ifExists, boolean purge)
      throws ImpalaException {
    if (ifExists && !catalog_.containsHdfsPartition(tableName.getDb(), tableName.getTbl(),
        partitionSpec)) {
      LOG.debug(String.format("Skipping partition drop because (%s) does not exist " +
          "and ifExists is true.", Joiner.on(", ").join(partitionSpec)));
      return null;
    }

    HdfsPartition part = catalog_.getHdfsPartition(tableName.getDb(),
        tableName.getTbl(), partitionSpec);
    synchronized (metastoreDdlLock_) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      List<String> values = Lists.newArrayList();
      // Need to add in the values in the same order they are defined in the table.
      for (FieldSchema fs: msTbl.getPartitionKeys()) {
        for (TPartitionKeyValue kv: partitionSpec) {
          if (fs.getName().toLowerCase().equals(kv.getName().toLowerCase())) {
            values.add(kv.getValue());
          }
        }
      }
      MetaStoreClient msClient = catalog_.getMetaStoreClient();
      PartitionDropOptions dropOptions = PartitionDropOptions.instance();
      dropOptions.purgeData(purge);
      try {
        msClient.getHiveClient().dropPartition(tableName.getDb(),
            tableName.getTbl(), values, dropOptions);
        updateLastDdlTime(msTbl, msClient);
        if (part.isMarkedCached()) {
          HdfsCachingUtil.uncachePartition(part);
        }
      } catch (NoSuchObjectException e) {
        if (!ifExists) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "dropPartition"), e);
        }
        LOG.debug(String.format("Ignoring '%s' when dropping partition from %s because" +
            " ifExists is true.", e, tableName));
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "dropPartition"), e);
      } finally {
        msClient.release();
      }
    }
    return catalog_.dropPartition(tableName, partitionSpec);
  }

  /**
   * Removes a column from the given table.
   */
  private void alterTableDropCol(TableName tableName, String colName)
      throws ImpalaException {
    synchronized (metastoreDdlLock_) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);

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
              "Column name %s not found in table %s.", colName, tableName));
        }
      }
      applyAlterTable(msTbl);
    }
  }

  /**
   * Renames an existing table or view. Saves, drops and restores the column stats for
   * tables renamed across databases to work around HIVE-9720/IMPALA-1711.
   * After renaming the table/view, its metadata is marked as invalid and will be
   * reloaded on the next access.
   */
  private void alterTableOrViewRename(TableName tableName, TableName newTableName,
      TDdlExecResponse response)
      throws ImpalaException {
    synchronized (metastoreDdlLock_) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      msTbl.setDbName(newTableName.getDb());
      msTbl.setTableName(newTableName.getTbl());

      MetaStoreClient msClient = catalog_.getMetaStoreClient();
      try {
        // Workaround for HIVE-9720/IMPALA-1711: When renaming a table with column stats
        // across databases, we save, drop and restore the column stats because the HMS
        // does not properly move them to the new table via alteration. The following
        // block needs to be protected by the metastoreDdlLock_ to avoid conflicts with
        // concurrent DDL on this same table (e.g., drop+add table with same db/name).
        ColumnStatistics hmsColStats = null;
        if (!msTbl.getTableType().equalsIgnoreCase(TableType.VIRTUAL_VIEW.toString()) &&
            !tableName.getDb().equalsIgnoreCase(newTableName.getDb())) {
          Table oldTbl = getExistingTable(tableName.getDb(), tableName.getTbl());
          Map<String, TColumnStats> colStats = Maps.newHashMap();
          for (Column c: oldTbl.getColumns()) {
            colStats.put(c.getName(), c.getStats().toThrift());
          }
          hmsColStats = createHiveColStats(colStats, oldTbl);
          // Set the new db/table.
          hmsColStats.setStatsDesc(
              new ColumnStatisticsDesc(true, newTableName.getDb(), newTableName.getTbl()));

          LOG.trace(String.format("Dropping column stats for table %s being " +
              "renamed to %s to workaround HIVE-9720.",
              tableName.toString(), newTableName.toString()));
          // Delete all column stats of the original table from the HMS.
          msClient.getHiveClient().deleteTableColumnStatistics(
              tableName.getDb(), tableName.getTbl(), null);
        }

        // Perform the table rename in any case.
        msClient.getHiveClient().alter_table(
            tableName.getDb(), tableName.getTbl(), msTbl);

        if (hmsColStats != null) {
          LOG.trace(String.format("Restoring column stats for table %s being " +
              "renamed to %s to workaround HIVE-9720.",
              tableName.toString(), newTableName.toString()));
          msClient.getHiveClient().updateTableColumnStatistics(hmsColStats);
        }
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_table"), e);
      } finally {
        msClient.release();
      }
    }

    // Rename the table in the Catalog and get the resulting catalog object.
    // ALTER TABLE/VIEW RENAME is implemented as an ADD + DROP.
    TCatalogObject newTable = TableToTCatalogObject(
        catalog_.renameTable(tableName.toThrift(), newTableName.toThrift()));
    TCatalogObject removedObject = new TCatalogObject();
    removedObject.setType(TCatalogObjectType.TABLE);
    removedObject.setTable(new TTable());
    removedObject.getTable().setTbl_name(tableName.getTbl());
    removedObject.getTable().setDb_name(tableName.getDb());
    removedObject.setCatalog_version(newTable.getCatalog_version());
    response.result.setRemoved_catalog_object(removedObject);
    response.result.setUpdated_catalog_object(newTable);
    response.result.setVersion(newTable.getCatalog_version());
  }

  /**
   * Changes the file format for the given table or partition. This is a metadata only
   * operation, existing table data will not be converted to the new format. After
   * changing the file format the table metadata is marked as invalid and will be
   * reloaded on the next access.
   */
  private void alterTableSetFileFormat(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, THdfsFileFormat fileFormat)
      throws ImpalaException {
    Preconditions.checkState(partitionSpec == null || !partitionSpec.isEmpty());
    if (partitionSpec == null) {
      synchronized (metastoreDdlLock_) {
        org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
        setStorageDescriptorFileFormat(msTbl.getSd(), fileFormat);
        applyAlterTable(msTbl);
      }
    } else {
      synchronized (metastoreDdlLock_) {
        HdfsPartition partition = catalog_.getHdfsPartition(
            tableName.getDb(), tableName.getTbl(), partitionSpec);
        Preconditions.checkNotNull(partition);
        partition.setFileFormat(HdfsFileFormat.fromThrift(fileFormat));
        try {
          applyAlterPartition(tableName, partition);
        } finally {
          partition.markDirty();
        }
      }
    }
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
  private void alterTableSetLocation(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, String location) throws ImpalaException {
    Preconditions.checkState(partitionSpec == null || !partitionSpec.isEmpty());
    if (partitionSpec == null) {
      synchronized (metastoreDdlLock_) {
        org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
        msTbl.getSd().setLocation(location);
        applyAlterTable(msTbl);
      }
    } else {
      synchronized (metastoreDdlLock_) {
        HdfsPartition partition = catalog_.getHdfsPartition(
            tableName.getDb(), tableName.getTbl(), partitionSpec);
        partition.setLocation(location);
        try {
          applyAlterPartition(tableName, partition);
        } finally {
          partition.markDirty();
        }
      }
    }
  }

  /**
   * Appends to the table or partition property metadata for the given table, replacing
   * the values of any keys that already exist.
   */
  private void alterTableSetTblProperties(TableName tableName,
      TAlterTableSetTblPropertiesParams params) throws ImpalaException {
    Map<String, String> properties = params.getProperties();
    Preconditions.checkNotNull(properties);
    synchronized (metastoreDdlLock_) {
      if (params.isSetPartition_spec()) {
        // Alter partition params.
        HdfsPartition partition = catalog_.getHdfsPartition(
            tableName.getDb(), tableName.getTbl(), params.getPartition_spec());
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
        try {
          applyAlterPartition(tableName, partition);
        } finally {
          partition.markDirty();
        }
      } else {
        // Alter table params.
        org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
        switch (params.getTarget()) {
          case TBL_PROPERTY:
            msTbl.getParameters().putAll(properties);
            break;
          case SERDE_PROPERTY:
            msTbl.getSd().getSerdeInfo().getParameters().putAll(properties);
            break;
          default:
            throw new UnsupportedOperationException(
                "Unknown target TTablePropertyType: " + params.getTarget());
        }
        applyAlterTable(msTbl);
      }
    }
  }

  /**
   * Caches or uncaches the HDFS location of the target table and updates the
   * table's metadata in Hive Metastore Store. If this is a partitioned table,
   * all uncached partitions will also be cached. The table/partition metadata
   * will be updated to include the ID of each cache directive that was submitted.
   * If the table is being uncached, any outstanding cache directives will be dropped
   * and the cache directive ID property key will be cleared.
   */
  private void alterTableSetCached(TableName tableName,
      TAlterTableSetCachedParams params) throws ImpalaException {
    THdfsCachingOp cacheOp = params.getCache_op();
    Preconditions.checkNotNull(cacheOp);
    // Alter table params.
    Table table = getExistingTable(tableName.getDb(), tableName.getTbl());
    if (!(table instanceof HdfsTable)) {
      throw new ImpalaRuntimeException("ALTER TABLE SET CACHED/UNCACHED must target " +
          "an HDFS table.");
    }
    HdfsTable hdfsTable = (HdfsTable) table;
    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getMetaStoreTable();
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

      if (table.getNumClusteringCols() > 0) {
        // If this is a partitioned table, submit cache directives for all uncached
        // partitions.
        for (HdfsPartition partition: hdfsTable.getPartitions()) {
          // No need to cache the default partition because it contains no files and is
          // not referred to by scan nodes.
          if (partition.getId() == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
            continue;
          }
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
                Long directiveId =
                    HdfsCachingUtil.getCacheDirectiveId(partition.getParameters());
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
              applyAlterPartition(tableName, partition);
            } finally {
              partition.markDirty();
            }
          }
        }
      }

      // Nothing to do.
      if (cacheDirIds.isEmpty()) return;

      // Submit a request to watch these cache directives. The TableLoadingMgr will
      // asynchronously refresh the table metadata once the directives complete.
      catalog_.watchCacheDirs(cacheDirIds, tableName.toThrift());
    } else {
      // Uncache the table.
      if (cacheDirId != null) HdfsCachingUtil.uncacheTbl(msTbl);
      // Uncache all table partitions.
      if (table.getNumClusteringCols() > 0) {
        for (HdfsPartition partition: ((HdfsTable) table).getPartitions()) {
          if (partition.getId() == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
            continue;
          }
          if (partition.isMarkedCached()) {
            HdfsCachingUtil.uncachePartition(partition);
            try {
              applyAlterPartition(tableName, partition);
            } finally {
              partition.markDirty();
            }
          }
        }
      }
    }

    // Update the table metadata.
    applyAlterTable(msTbl);
  }

  /**
   * Caches or uncaches the HDFS location of the target partition and updates the
   * partition's metadata in Hive Metastore Store. If a partition is being cached, the
   * partition properties will have the ID of the cache directive added. If the partition
   * is being uncached, any outstanding cache directive will be dropped and the cache
   * directive ID property key will be cleared.
   */
  private void alterPartitionSetCached(TableName tableName,
      TAlterTableSetCachedParams params) throws ImpalaException {
    THdfsCachingOp cacheOp = params.getCache_op();
    Preconditions.checkNotNull(cacheOp);
    Preconditions.checkNotNull(params.getPartition_spec());
    synchronized (metastoreDdlLock_) {
      // Alter partition params.
      HdfsPartition partition = catalog_.getHdfsPartition(
          tableName.getDb(), tableName.getTbl(), params.getPartition_spec());
      if (cacheOp.isSet_cached()) {

        // The directive is null if the partition is not cached
        Long directiveId = HdfsCachingUtil.getCacheDirectiveId(
            partition.getParameters());
        short replication = HdfsCachingUtil.getReplicationOrDefault(cacheOp);
        List<Long> cacheDirs = Lists.newArrayList();

        if (directiveId == null) {
          cacheDirs.add(HdfsCachingUtil.submitCachePartitionDirective(partition,
              cacheOp.getCache_pool_name(), replication));
        } else {
          if (HdfsCachingUtil.isUpdateOp(cacheOp, partition.getParameters())) {
            HdfsCachingUtil.validateCachePool(cacheOp, directiveId, tableName, partition);
            cacheDirs.add(HdfsCachingUtil.modifyCacheDirective(directiveId, partition,
                cacheOp.getCache_pool_name(), replication));
          }
        }

        // Once the cache directives are sbumitted, observe the status of the caching
        // until no more progress is made -- either fully cached or out of cache memory
        if (!cacheDirs.isEmpty()) {
          catalog_.watchCacheDirs(cacheDirs, tableName.toThrift());
        }

      } else {
        // Partition is not cached, just return.
        if (!partition.isMarkedCached()) return;
        HdfsCachingUtil.uncachePartition(partition);
      }
      try {
        applyAlterPartition(tableName, partition);
      } finally {
        partition.markDirty();
      }
    }
  }

  /**
   * Recover partitions of specified table.
   * Add partitions to metastore which exist in HDFS but not in metastore.
   */
  private void alterTableRecoverPartitions(TableName tableName)
      throws ImpalaException {
    Table table = getExistingTable(tableName.getDb(), tableName.getTbl());
    if (!(table instanceof HdfsTable)) {
      throw new CatalogException("Table " + tableName + " is not an HDFS table");
    }

    HdfsTable hdfsTable = (HdfsTable)table;
    List<List<String>> partitionsNotInHms = hdfsTable.getPathsWithoutPartitions();
    if (partitionsNotInHms.isEmpty()) return;

    List<Partition> hmsPartitions = Lists.newArrayList();
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
    for (List<String> partitionSpecValues: partitionsNotInHms) {
      hmsPartitions.add(createHmsPartitionFromValues(
          partitionSpecValues, msTbl, tableName, null));
    }

    synchronized (metastoreDdlLock_) {
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
      MetaStoreClient msClient = catalog_.getMetaStoreClient();
      try {
        // ifNotExists and needResults are true.
        hmsPartitions = msClient.getHiveClient().add_partitions(hmsPartitions,
            true, true);

        for (Partition partition: hmsPartitions) {
          // Create and add the HdfsPartition. Return the table object with an updated
          // catalog version.
          addHdfsPartition(tableName, partition);
        }

        // Handle HDFS cache.
        if (cachePoolName != null) {
          for (Partition partition: hmsPartitions) {
            long id = HdfsCachingUtil.submitCachePartitionDirective(partition,
                cachePoolName, replication);
            cacheIds.add(id);
          }
          // Update the partition metadata to include the cache directive id.
          msClient.getHiveClient().alter_partitions(tableName.getDb(),
              tableName.getTbl(), hmsPartitions);
        }

        updateLastDdlTime(msTbl, msClient);
      } catch (AlreadyExistsException e) {
        // This may happen when another client of HMS has added the partitions.
        LOG.debug(String.format("Ignoring '%s' when adding partition to %s.", e,
            tableName));
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "add_partition"), e);
      } finally {
        msClient.release();
      }

      if (!cacheIds.isEmpty()) {
        catalog_.watchCacheDirs(cacheIds, tableName.toThrift());
      }
    }
  }

  /**
   * Create a new HMS Partition.
   */
  private static Partition createHmsPartition(List<TPartitionKeyValue> partitionSpec,
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
  private static Partition createHmsPartitionFromValues(List<String> partitionSpecValues,
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

    return partition;
  }

  /**
   * Applies an ALTER TABLE command to the metastore table. The caller should take the
   * metastoreDdlLock before calling this method.
   * Note: The metastore interface is not very safe because it only accepts
   * an entire metastore.api.Table object rather than a delta of what to change. This
   * means an external modification to the table could be overwritten by an ALTER TABLE
   * command if the metadata is not completely in-sync. This affects both Hive and
   * Impala, but is more important in Impala because the metadata is cached for a
   * longer period of time.
   */
  private void applyAlterTable(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws ImpalaRuntimeException {
    MetaStoreClient msClient = catalog_.getMetaStoreClient();
    long lastDdlTime = -1;
    try {
      lastDdlTime = calculateDdlTime(msTbl);
      msTbl.putToParameters("transient_lastDdlTime", Long.toString(lastDdlTime));
      msClient.getHiveClient().alter_table(
          msTbl.getDbName(), msTbl.getTableName(), msTbl);
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_table"), e);
    } finally {
      msClient.release();
      catalog_.updateLastDdlTime(
          new TTableName(msTbl.getDbName(), msTbl.getTableName()), lastDdlTime);
    }
  }

  private void applyAlterPartition(TableName tableName, HdfsPartition partition)
      throws ImpalaException {
    MetaStoreClient msClient = catalog_.getMetaStoreClient();
    try {
      msClient.getHiveClient().alter_partition(
          tableName.getDb(), tableName.getTbl(), partition.toHmsPartition());
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      updateLastDdlTime(msTbl, msClient);
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_partition"), e);
    } finally {
      msClient.release();
    }
  }

  /**
   * Creates or drops a Sentry role on behalf of the requestingUser.
   */
  private void createDropRole(User requestingUser,
      TCreateDropRoleParams createDropRoleParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    verifySentryServiceEnabled();

    Role role;
    if (createDropRoleParams.isIs_drop()) {
      role = catalog_.getSentryProxy().dropRole(requestingUser,
          createDropRoleParams.getRole_name());
      if (role == null) {
        role = new Role(createDropRoleParams.getRole_name(), Sets.<String>newHashSet());
        role.setCatalogVersion(catalog_.getCatalogVersion());
      }
    } else {
      role = catalog_.getSentryProxy().createRole(requestingUser,
          createDropRoleParams.getRole_name());
    }
    Preconditions.checkNotNull(role);

    TCatalogObject catalogObject = new TCatalogObject();
    catalogObject.setType(role.getCatalogObjectType());
    catalogObject.setRole(role.toThrift());
    catalogObject.setCatalog_version(role.getCatalogVersion());
    if (createDropRoleParams.isIs_drop()) {
      resp.result.setRemoved_catalog_object(catalogObject);
    } else {
      resp.result.setUpdated_catalog_object(catalogObject);
    }
    resp.result.setVersion(role.getCatalogVersion());
  }

  /**
   * Grants or revokes a Sentry role to/from the given group on behalf of the
   * requestingUser.
   */
  private void grantRevokeRoleGroup(User requestingUser,
      TGrantRevokeRoleParams grantRevokeRoleParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    verifySentryServiceEnabled();

    String roleName = grantRevokeRoleParams.getRole_names().get(0);
    String groupName = grantRevokeRoleParams.getGroup_names().get(0);
    Role role = null;
    if (grantRevokeRoleParams.isIs_grant()) {
      role = catalog_.getSentryProxy().grantRoleGroup(requestingUser, roleName,
          groupName);
    } else {
      role = catalog_.getSentryProxy().revokeRoleGroup(requestingUser, roleName,
          groupName);
    }
    Preconditions.checkNotNull(role);
    TCatalogObject catalogObject = new TCatalogObject();
    catalogObject.setType(role.getCatalogObjectType());
    catalogObject.setRole(role.toThrift());
    catalogObject.setCatalog_version(role.getCatalogVersion());
    resp.result.setUpdated_catalog_object(catalogObject);
    resp.result.setVersion(role.getCatalogVersion());
  }

  /**
   * Grants or revokes one or more privileges to/from a Sentry role on behalf of the
   * requestingUser.
   */
  private void grantRevokeRolePrivilege(User requestingUser,
      TGrantRevokePrivParams grantRevokePrivParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    verifySentryServiceEnabled();
    String roleName = grantRevokePrivParams.getRole_name();
    List<TPrivilege> privileges = grantRevokePrivParams.getPrivileges();
    List<RolePrivilege> rolePrivileges = null;
    if (grantRevokePrivParams.isIs_grant()) {
      rolePrivileges = catalog_.getSentryProxy().grantRolePrivileges(requestingUser,
          roleName, privileges);
    } else {
      rolePrivileges = catalog_.getSentryProxy().revokeRolePrivileges(requestingUser,
          roleName, privileges, grantRevokePrivParams.isHas_grant_opt());
    }
    Preconditions.checkNotNull(rolePrivileges);
    List<TCatalogObject> updatedPrivs = Lists.newArrayList();
    for (RolePrivilege rolePriv: rolePrivileges) {
      TCatalogObject catalogObject = new TCatalogObject();
      catalogObject.setType(rolePriv.getCatalogObjectType());
      catalogObject.setPrivilege(rolePriv.toThrift());
      catalogObject.setCatalog_version(rolePriv.getCatalogVersion());
      updatedPrivs.add(catalogObject);
    }

    // TODO: Currently we only support sending back 1 catalog object in a "direct DDL"
    // response. If multiple privileges have been updated, just send back the
    // catalog version so subscribers can wait for the statestore heartbeat that contains
    // all updates.
    if (updatedPrivs.size() == 1) {
      // If this is a REVOKE statement with hasGrantOpt, only the GRANT OPTION is revoked
      // from the privilege.
      if (grantRevokePrivParams.isIs_grant() ||
          privileges.get(0).isHas_grant_opt()) {
        resp.result.setUpdated_catalog_object(updatedPrivs.get(0));
      } else {
        resp.result.setRemoved_catalog_object(updatedPrivs.get(0));
      }
      resp.result.setVersion(updatedPrivs.get(0).getCatalog_version());
    } else if (updatedPrivs.size() > 1) {
      resp.result.setVersion(
          updatedPrivs.get(updatedPrivs.size() - 1).getCatalog_version());
    }
  }

  /**
   * Throws a CatalogException if the Sentry Service is not enabled.
   */
  private void verifySentryServiceEnabled() throws CatalogException {
    if (catalog_.getSentryProxy() == null) {
      throw new CatalogException("Sentry Service is not enabled on the " +
          "CatalogServer.");
    }
  }

  /**
   * Alters partitions in batches of size 'MAX_PARTITION_UPDATES_PER_RPC'. This
   * reduces the time spent in a single update and helps avoid metastore client
   * timeouts.
   */
  private void bulkAlterPartitions(String dbName, String tableName,
      List<HdfsPartition> modifiedParts) throws ImpalaRuntimeException {
    MetaStoreClient msClient = null;
    List<org.apache.hadoop.hive.metastore.api.Partition> hmsPartitions =
        Lists.newArrayList();
    for (HdfsPartition p: modifiedParts) {
      org.apache.hadoop.hive.metastore.api.Partition msPart = p.toHmsPartition();
      if (msPart != null) hmsPartitions.add(msPart);
    }
    if (hmsPartitions.size() == 0) return;
    try {
      msClient = catalog_.getMetaStoreClient();

      // Apply the updates in batches of 'MAX_PARTITION_UPDATES_PER_RPC'.
      for (int i = 0; i < hmsPartitions.size(); i += MAX_PARTITION_UPDATES_PER_RPC) {
        int numPartitionsToUpdate =
            Math.min(i + MAX_PARTITION_UPDATES_PER_RPC, hmsPartitions.size());
        synchronized (metastoreDdlLock_) {
          try {
            // Alter partitions in bulk.
            msClient.getHiveClient().alter_partitions(dbName, tableName,
                hmsPartitions.subList(i, numPartitionsToUpdate));
          } catch (TException e) {
            throw new ImpalaRuntimeException(
                String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_partitions"), e);
          }
        }
      }
    } finally {
      if (msClient != null) msClient.release();
    }
  }

  /**
   * Returns a deep copy of the metastore.api.Table object for the given TableName.
   */
  private org.apache.hadoop.hive.metastore.api.Table getMetaStoreTable(
      TableName tableName) throws CatalogException {
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    return getExistingTable(tableName.getDb(), tableName.getTbl())
        .getMetaStoreTable().deepCopy();
  }

  public static List<FieldSchema> buildFieldSchemaList(List<TColumn> columns) {
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

  private static TCatalogObject TableToTCatalogObject(Table table) {
    if (table != null) return table.toTCatalogObject();
    return new TCatalogObject(TCatalogObjectType.TABLE,
        Catalog.INITIAL_CATALOG_VERSION);
  }

   /**
   * Sets the table parameter 'transient_lastDdlTime' to System.currentTimeMillis()/1000
   * in the given msTbl. 'transient_lastDdlTime' is guaranteed to be changed.
   * If msClient is not null then this method applies alter_table() to update the
   * Metastore. Otherwise, the caller is responsible for the final update.
   */
  public long updateLastDdlTime(org.apache.hadoop.hive.metastore.api.Table msTbl,
      MetaStoreClient msClient) throws MetaException, NoSuchObjectException, TException {
    Preconditions.checkNotNull(msTbl);
    LOG.debug("Updating lastDdlTime for table: " + msTbl.getTableName());
    Map<String, String> params = msTbl.getParameters();
    long lastDdlTime = calculateDdlTime(msTbl);
    params.put("transient_lastDdlTime", Long.toString(lastDdlTime));
    msTbl.setParameters(params);
    if (msClient != null) {
      msClient.getHiveClient().alter_table(
          msTbl.getDbName(), msTbl.getTableName(), msTbl);
    }
    catalog_.updateLastDdlTime(
        new TTableName(msTbl.getDbName(), msTbl.getTableName()), lastDdlTime);
    return lastDdlTime;
  }

  /**
   * Calculates the next transient_lastDdlTime value.
   */
  private static long calculateDdlTime(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    long existingLastDdlTime = CatalogServiceCatalog.getLastDdlTime(msTbl);
    long currentTime = System.currentTimeMillis() / 1000;
    if (existingLastDdlTime == currentTime) ++currentTime;
    return currentTime;
  }

  /**
   * Utility function that creates a hive.metastore.api.Table object based on the given
   * TCreateTableParams.
   * TODO: Extract metastore object creation utility functions into a separate
   * helper/factory class.
   */
  public static org.apache.hadoop.hive.metastore.api.Table
      createMetaStoreTable(TCreateTableParams params) {
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

    if (params.getComment() != null) {
      tbl.getParameters().put("comment", params.getComment());
    }
    if (params.is_external) {
      tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      tbl.putToParameters("EXTERNAL", "TRUE");
    } else {
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
    }

    StorageDescriptor sd = HiveStorageDescriptorFactory.createSd(
        params.getFile_format(), RowFormat.fromThrift(params.getRow_format()));

    if (params.isSetSerde_properties()) {
      if (sd.getSerdeInfo().getParameters() == null) {
        sd.getSerdeInfo().setParameters(params.getSerde_properties());
      } else {
        sd.getSerdeInfo().getParameters().putAll(params.getSerde_properties());
      }
    }

    if (params.getLocation() != null) {
      sd.setLocation(params.getLocation());
    }
    // Add in all the columns
    sd.setCols(buildFieldSchemaList(params.getColumns()));
    tbl.setSd(sd);
    if (params.getPartition_columns() != null) {
      // Add in any partition keys that were specified
      tbl.setPartitionKeys(buildFieldSchemaList(params.getPartition_columns()));
    } else {
      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
    }
    return tbl;
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
   *
   * For details on the specific commands see comments on their respective
   * methods in CatalogServiceCatalog.java.
   */
  public TResetMetadataResponse execResetMetadata(TResetMetadataRequest req)
      throws CatalogException {
    TResetMetadataResponse resp = new TResetMetadataResponse();
    resp.setResult(new TCatalogUpdateResult());
    resp.getResult().setCatalog_service_id(JniCatalog.getServiceId());

    if (req.isSetTable_name()) {
      // Tracks any CatalogObjects updated/added/removed as a result of
      // the invalidate metadata or refresh call. For refresh() it is only expected
      // that a table be modified, but for invalidateTable() the table's parent database
      // may have also been added if it did not previously exist in the catalog.
      Pair<Db, Table> modifiedObjects = new Pair<Db, Table>(null, null);

      boolean wasRemoved = false;
      if (req.isIs_refresh()) {
        modifiedObjects.second = catalog_.reloadTable(req.getTable_name());
      } else {
        wasRemoved = catalog_.invalidateTable(req.getTable_name(), modifiedObjects);
      }

      if (modifiedObjects.first == null) {
        TCatalogObject thriftTable = TableToTCatalogObject(modifiedObjects.second);
        if (modifiedObjects.second != null) {
          // Return the TCatalogObject in the result to indicate this request can be
          // processed as a direct DDL operation.
          if (wasRemoved) {
            resp.getResult().setRemoved_catalog_object(thriftTable);
          } else {
            resp.getResult().setUpdated_catalog_object(thriftTable);
          }
        } else {
          // Table does not exist in the meta store and Impala catalog, throw error.
          throw new TableNotFoundException("Table not found: " +
              req.getTable_name().getDb_name() + "."
              + req.getTable_name().getTable_name());
        }
        resp.getResult().setVersion(thriftTable.getCatalog_version());
      } else {
        // If there were two catalog objects modified it indicates there was an
        // "invalidateTable()" call that added a new table AND database to the catalog.
        Preconditions.checkState(!req.isIs_refresh());
        Preconditions.checkNotNull(modifiedObjects.first);
        Preconditions.checkNotNull(modifiedObjects.second);

        // The database should always have a lower catalog version than the table because
        // it needs to be created before the table can be added.
        Preconditions.checkState(modifiedObjects.first.getCatalogVersion() <
            modifiedObjects.second.getCatalogVersion());

        // Since multiple catalog objects were modified, don't treat this as a direct DDL
        // operation. Just set the overall catalog version and the impalad will wait for
        // a statestore heartbeat that contains the update.
        resp.getResult().setVersion(modifiedObjects.second.getCatalogVersion());
      }
    } else {
      // Invalidate the entire catalog if no table name is provided.
      Preconditions.checkArgument(!req.isIs_refresh());
      catalog_.reset();
      resp.result.setVersion(catalog_.getCatalogVersion());
    }
    resp.getResult().setStatus(
        new TStatus(TErrorCode.OK, new ArrayList<String>()));
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
   * Updates the lastDdlTime of the table if new partitions were created.
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

    // Collects the cache directive IDs of any cached table/partitions that were
    // targeted. A watch on these cache directives is submitted to the TableLoadingMgr
    // and the table will be refreshed asynchronously after all cache directives
    // complete.
    List<Long> cacheDirIds = Lists.<Long>newArrayList();

    // If the table is cached, get its cache pool name and replication factor. New
    // partitions will inherit this property.
    String cachePoolName = null;
    Short cacheReplication = 0;
    Long cacheDirId = HdfsCachingUtil.getCacheDirectiveId(
        table.getMetaStoreTable().getParameters());
    if (cacheDirId != null) {
      try {
        cachePoolName = HdfsCachingUtil.getCachePool(cacheDirId);
        cacheReplication = HdfsCachingUtil.getCacheReplication(cacheDirId);
        Preconditions.checkNotNull(cacheReplication);
        if (table.getNumClusteringCols() == 0) cacheDirIds.add(cacheDirId);
      } catch (ImpalaRuntimeException e) {
        // Catch the error so that the actual update to the catalog can progress,
        // this resets caching for the table though
        LOG.error(
            String.format("Cache directive %d was not found, uncache the table %s.%s" +
                "to remove this message.", cacheDirId, update.getDb_name(),
                update.getTarget_table()));
        cacheDirId = null;
      }

    }

    TableName tblName = new TableName(table.getDb().getName(), table.getName());
    List<String> errorMessages = Lists.newArrayList();
    if (table.getNumClusteringCols() > 0) {
      // Set of all partition names targeted by the insert that that need to be created
      // in the Metastore (partitions that do not currently exist in the catalog).
      // In the BE, we don't currently distinguish between which targeted partitions are
      // new and which already exist, so initialize the set with all targeted partition
      // names and remove the ones that are found to exist.
      Set<String> partsToCreate = Sets.newHashSet(update.getCreated_partitions());
      for (HdfsPartition partition: ((HdfsTable) table).getPartitions()) {
        // Skip dummy default partition.
        if (partition.getId() == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
          continue;
        }
        // TODO: In the BE we build partition names without a trailing char. In FE we
        // build partition name with a trailing char. We should make this consistent.
        String partName = partition.getPartitionName() + "/";

        // Attempt to remove this partition name from from partsToCreate. If remove
        // returns true, it indicates the partition already exists.
        if (partsToCreate.remove(partName) && partition.isMarkedCached()) {
          // The partition was targeted by the insert and is also a cached. Since data
          // was written to the partition, a watch needs to be placed on the cache
          // cache directive so the TableLoadingMgr can perform an async refresh once
          // all data becomes cached.
          cacheDirIds.add(HdfsCachingUtil.getCacheDirectiveId(
              partition.getParameters()));
        }
        if (partsToCreate.size() == 0) break;
      }

      if (!partsToCreate.isEmpty()) {
        MetaStoreClient msClient = catalog_.getMetaStoreClient();
        try {
          org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tblName);
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
            partition.setValues(getPartValsFromName(msTbl, partName));
            partition.setParameters(new HashMap<String, String>());
            partition.setSd(msTbl.getSd().deepCopy());
            partition.getSd().setSerdeInfo(msTbl.getSd().getSerdeInfo().deepCopy());
            partition.getSd().setLocation(msTbl.getSd().getLocation() + "/" +
                partName.substring(0, partName.length() - 1));
            MetaStoreUtils.updatePartitionStatsFast(partition, warehouse);
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
                  String msg = String.format("Partition %s.%s(%s): State: Not cached." +
                      " Action: Cache manully via 'ALTER TABLE'.",
                      part.getDbName(), part.getTableName(), part.getValues());
                  LOG.error(msg, e);
                  errorMessages.add(msg);
                }
              }
              try {
                msClient.getHiveClient().alter_partitions(tblName.getDb(),
                    tblName.getTbl(), cachedHmsParts);
              } catch (Exception e) {
                LOG.error("Failed in alter_partitions: ", e);
                // Try to uncache the partitions when the alteration in the HMS failed.
                for (org.apache.hadoop.hive.metastore.api.Partition part:
                    cachedHmsParts) {
                  try {
                    HdfsCachingUtil.uncachePartition(part);
                  } catch (ImpalaException e1) {
                    String msg = String.format(
                        "Partition %s.%s(%s): State: Leaked caching directive. " +
                        "Action: Manually uncache directory %s via hdfs cacheAdmin.",
                        part.getDbName(), part.getTableName(), part.getValues(),
                        part.getSd().getLocation());
                    LOG.error(msg, e);
                    errorMessages.add(msg);
                  }
                }
              }
            }
            updateLastDdlTime(msTbl, msClient);
          }
        } catch (AlreadyExistsException e) {
          throw new InternalException(
              "AlreadyExistsException thrown although ifNotExists given", e);
        } catch (Exception e) {
          throw new InternalException("Error adding partitions", e);
        } finally {
          msClient.release();
        }
      }
    }

    // Submit the watch request for the given cache directives.
    if (!cacheDirIds.isEmpty()) catalog_.watchCacheDirs(cacheDirIds, tblName.toThrift());

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
    // Perform an incremental refresh to load new/modified partitions and files.
    Table refreshedTbl = catalog_.reloadTable(tblName.toThrift());
    response.getResult().setUpdated_catalog_object(TableToTCatalogObject(refreshedTbl));
    response.getResult().setVersion(
        response.getResult().getUpdated_catalog_object().getCatalog_version());
    return response;
  }

  private List<String> getPartValsFromName(org.apache.hadoop.hive.metastore.api.Table
      msTbl, String partName) throws MetaException, CatalogException {
    Preconditions.checkNotNull(msTbl);
    LinkedHashMap<String, String> hm =
        org.apache.hadoop.hive.metastore.Warehouse.makeSpecFromName(partName);
    List<String> partVals = Lists.newArrayList();
    for (FieldSchema field: msTbl.getPartitionKeys()) {
      String key = field.getName();
      String val = hm.get(key);
      if (val == null) {
        throw new CatalogException("Incomplete partition name - missing " + key);
      }
      partVals.add(val);
    }
    return partVals;
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
   * modified or dropped/re-created without us knowing. TODO: Track object IDs to
   * know when a table has been dropped and re-created with the same name.
   */
  public Table getExistingTable(String dbName, String tblName) throws CatalogException {
    Table tbl = catalog_.getOrLoadTable(dbName, tblName);
    if (tbl == null) {
      throw new TableNotFoundException("Table not found: " + dbName + "." + tblName);
    }

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
}
