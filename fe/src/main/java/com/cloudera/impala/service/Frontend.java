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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hive.service.cli.thrift.TGetColumnsReq;
import org.apache.hive.service.cli.thrift.TGetFunctionsReq;
import org.apache.hive.service.cli.thrift.TGetSchemasReq;
import org.apache.hive.service.cli.thrift.TGetTablesReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.CreateDataSrcStmt;
import com.cloudera.impala.analysis.CreateUdaStmt;
import com.cloudera.impala.analysis.CreateUdfStmt;
import com.cloudera.impala.analysis.DropDataSrcStmt;
import com.cloudera.impala.analysis.DropFunctionStmt;
import com.cloudera.impala.analysis.DropTableOrViewStmt;
import com.cloudera.impala.analysis.InsertStmt;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.analysis.ResetMetadataStmt;
import com.cloudera.impala.analysis.ShowFunctionsStmt;
import com.cloudera.impala.analysis.TableName;
import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.authorization.ImpalaInternalAdminUser;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.catalog.DataSourceTable;
import com.cloudera.impala.catalog.DatabaseNotFoundException;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.ImpaladCatalog;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.planner.PlanFragment;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.planner.ScanNode;
import com.cloudera.impala.thrift.TCatalogOpRequest;
import com.cloudera.impala.thrift.TCatalogOpType;
import com.cloudera.impala.thrift.TColumn;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TDdlExecRequest;
import com.cloudera.impala.thrift.TDdlType;
import com.cloudera.impala.thrift.TDescribeTableOutputStyle;
import com.cloudera.impala.thrift.TDescribeTableResult;
import com.cloudera.impala.thrift.TExecRequest;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TExplainResult;
import com.cloudera.impala.thrift.TFinalizeParams;
import com.cloudera.impala.thrift.TFunctionType;
import com.cloudera.impala.thrift.TLoadDataReq;
import com.cloudera.impala.thrift.TLoadDataResp;
import com.cloudera.impala.thrift.TMetadataOpRequest;
import com.cloudera.impala.thrift.TPlanFragment;
import com.cloudera.impala.thrift.TQueryContext;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TResetMetadataRequest;
import com.cloudera.impala.thrift.TResultRow;
import com.cloudera.impala.thrift.TResultSet;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.cloudera.impala.thrift.TStatus;
import com.cloudera.impala.thrift.TStatusCode;
import com.cloudera.impala.thrift.TStmtType;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.thrift.TUpdateCatalogCacheRequest;
import com.cloudera.impala.thrift.TUpdateCatalogCacheResponse;
import com.cloudera.impala.util.PatternMatcher;
import com.cloudera.impala.util.TResultRowBuilder;
import com.cloudera.impala.util.TSessionStateUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Frontend API for the impalad process.
 * This class allows the impala daemon to create TQueryExecRequest
 * in response to TClientRequests.
 */
public class Frontend {
  private final static Logger LOG = LoggerFactory.getLogger(Frontend.class);
  // Time to wait for missing tables to be loaded before timing out.
  private final long MISSING_TBL_LOAD_WAIT_TIMEOUT_MS = 2 * 60 * 1000;

  // Max time to wait for a catalog update notification.
  private final long MAX_CATALOG_UPDATE_WAIT_TIME_MS = 2 * 1000;

  private ImpaladCatalog impaladCatalog_;
  private final AuthorizationConfig authzConfig_;

  public Frontend(AuthorizationConfig authorizationConfig) {
    this(authorizationConfig, new ImpaladCatalog(authorizationConfig));
  }

  /**
   * C'tor used by tests to pass in a custom ImpaladCatalog.
   */
  public Frontend(AuthorizationConfig authorizationConfig, ImpaladCatalog catalog) {
    authzConfig_ = authorizationConfig;
    impaladCatalog_ = catalog;
  }

  public ImpaladCatalog getCatalog() { return impaladCatalog_; }

  public TUpdateCatalogCacheResponse updateCatalogCache(
      TUpdateCatalogCacheRequest req) throws CatalogException {
    ImpaladCatalog catalog = impaladCatalog_;

    // If this is not a delta, this update should replace the current
    // Catalog contents so create a new catalog and populate it.
    if (!req.is_delta) catalog = new ImpaladCatalog(authzConfig_);

    TUpdateCatalogCacheResponse response = catalog.updateCatalog(req);
    if (!req.is_delta) impaladCatalog_ = catalog;
    return response;
  }

  /**
   * Constructs a TCatalogOpRequest and attaches it, plus any metadata, to the
   * result argument.
   */
  private void createCatalogOpRequest(AnalysisContext.AnalysisResult analysis,
      TExecRequest result) {
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
          new TColumn("name", ColumnType.STRING.toThrift())));
    } else if (analysis.isShowDbsStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_DBS;
      ddl.setShow_dbs_params(analysis.getShowDbsStmt().toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("name", ColumnType.STRING.toThrift())));
    } else if (analysis.isShowStatsStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_STATS;
      ddl.setShow_stats_params(analysis.getShowStatsStmt().toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("name", ColumnType.STRING.toThrift())));
    } else if (analysis.isShowFunctionsStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_FUNCTIONS;
      ShowFunctionsStmt stmt = (ShowFunctionsStmt)analysis.getStmt();
      ddl.setShow_fns_params(stmt.toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("return type", ColumnType.STRING.toThrift()),
          new TColumn("signature", ColumnType.STRING.toThrift())));
    } else if (analysis.isShowCreateTableStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_CREATE_TABLE;
      ddl.setShow_create_table_params(analysis.getShowCreateTableStmt().toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("result", ColumnType.STRING.toThrift())));
    } else if (analysis.isDescribeStmt()) {
      ddl.op_type = TCatalogOpType.DESCRIBE;
      ddl.setDescribe_table_params(analysis.getDescribeStmt().toThrift());
      metadata.setColumns(Arrays.asList(
          new TColumn("name", ColumnType.STRING.toThrift()),
          new TColumn("type", ColumnType.STRING.toThrift()),
          new TColumn("comment", ColumnType.STRING.toThrift())));
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
          new TColumn("summary", ColumnType.STRING.toThrift())));
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
    } else if (analysis.isResetMetadataStmt()) {
      ddl.op_type = TCatalogOpType.RESET_METADATA;
      ResetMetadataStmt resetMetadataStmt = (ResetMetadataStmt) analysis.getStmt();
      TResetMetadataRequest req = resetMetadataStmt.toThrift();
      ddl.setReset_metadata_params(req);
      metadata.setColumns(Collections.<TColumn>emptyList());
    }
    result.setResult_set_metadata(metadata);
    result.setCatalog_op_request(ddl);
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
    if (request.isSetPartition_spec()) {
      destPathString = impaladCatalog_.getHdfsPartition(tableName.getDb(),
          tableName.getTbl(), request.getPartition_spec()).getLocation();
    } else {
      destPathString = impaladCatalog_.getTable(tableName.getDb(), tableName.getTbl(),
          ImpalaInternalAdminUser.getInstance(), Privilege.INSERT)
          .getMetaStoreTable().getSd().getLocation();
    }

    Path destPath = new Path(destPathString);
    DistributedFileSystem dfs = FileSystemUtil.getDistributedFileSystem(destPath);

    // Create a temporary directory within the final destination directory to stage the
    // file move.
    Path tmpDestPath = FileSystemUtil.makeTmpSubdirectory(destPath);

    Path sourcePath = new Path(request.source_path);
    int filesLoaded = 0;
    if (dfs.isDirectory(sourcePath)) {
      filesLoaded = FileSystemUtil.moveAllVisibleFiles(sourcePath, tmpDestPath);
    } else {
      FileSystemUtil.moveFile(sourcePath, tmpDestPath, true);
      filesLoaded = 1;
    }

    // If this is an OVERWRITE, delete all files in the destination.
    if (request.isOverwrite()) {
      FileSystemUtil.deleteAllVisibleFiles(destPath);
    }

    // Move the files from the temporary location to the final destination.
    FileSystemUtil.moveAllVisibleFiles(tmpDestPath, destPath);
    // Cleanup the tmp directory.
    dfs.delete(tmpDestPath, true);
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
  public String getExplainString(TQueryContext queryCtxt) throws ImpalaException {
    StringBuilder stringBuilder = new StringBuilder();
    createExecRequest(queryCtxt, stringBuilder);
    return stringBuilder.toString();
  }

  /**
   * Returns all tables that match the specified database and pattern that are accessible
   * to the given user. If pattern is null, matches all tables. If db is null, all
   * databases are searched for matches.
   */
  public List<String> getTableNames(String dbName, String tablePattern, User user)
      throws ImpalaException {
    return impaladCatalog_.getTableNames(dbName, tablePattern, user);
  }

  /**
   * Returns all database names that match the pattern and
   * are accessible to the given user. If pattern is null, matches all dbs.
   */
  public List<String> getDbNames(String dbPattern, User user) {
    return impaladCatalog_.getDbNames(dbPattern, user);
  }

  /**
   * Generate result set and schema for a SHOW COLUMN STATS command.
   */
  public TResultSet getColumnStats(String dbName, String tableName)
      throws ImpalaException {
    Table table = impaladCatalog_.getTable(dbName, tableName,
        ImpalaInternalAdminUser.getInstance(), Privilege.ALL);
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);
    resultSchema.addToColumns(new TColumn("Column", ColumnType.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Type", ColumnType.STRING.toThrift()));
    resultSchema.addToColumns(
        new TColumn("#Distinct Values", ColumnType.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("#Nulls", ColumnType.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("Max Size", ColumnType.DOUBLE.toThrift()));
    resultSchema.addToColumns(new TColumn("Avg Size", ColumnType.DOUBLE.toThrift()));

    for (Column c: table.getColumnsInHiveOrder()) {
      TResultRowBuilder rowBuilder = new TResultRowBuilder();
      // Add name, type, NDVs, numNulls, max size and avg size.
      rowBuilder.add(c.getName()).add(c.getType().toString())
          .add(c.getStats().getNumDistinctValues()).add(c.getStats().getNumNulls())
          .add(c.getStats().getMaxSize()).add(c.getStats().getAvgSize());
      result.addToRows(rowBuilder.get());
    }
    return result;
  }

  /**
   * Generate result set and schema for a SHOW TABLE STATS command.
   */
  public TResultSet getTableStats(String dbName, String tableName)
      throws ImpalaException {
    Table table = impaladCatalog_.getTable(dbName, tableName,
        ImpalaInternalAdminUser.getInstance(), Privilege.ALL);
    if (table instanceof HdfsTable) {
      return ((HdfsTable) table).getTableStats();
    } else if (table instanceof HBaseTable) {
      return ((HBaseTable) table).getTableStats();
    } else if (table instanceof DataSourceTable) {
      return ((DataSourceTable) table).getTableStats();
    } else {
      throw new InternalException("Invalid table class: " + table.getClass());
    }
  }

  /**
   * Returns all function signatures that match the pattern. If pattern is null,
   * matches all functions.
   */
  public List<Function> getFunctions(TFunctionType type, String dbName, String fnPattern)
      throws DatabaseNotFoundException {
    Db db = impaladCatalog_.getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    return db.getFunctions(type, PatternMatcher.createHivePatternMatcher(fnPattern));
  }

  /**
   * Returns table metadata, such as the column descriptors, in the specified table.
   * Throws an exception if the table or db is not found or if there is an error
   * loading the table metadata.
   */
  public TDescribeTableResult describeTable(String dbName, String tableName,
      TDescribeTableOutputStyle outputStyle) throws ImpalaException {
    Table table = impaladCatalog_.getTable(dbName, tableName,
        ImpalaInternalAdminUser.getInstance(), Privilege.ALL);
    return DescribeResultFactory.buildDescribeTableResult(table, outputStyle);
  }

  /**
   * Given a set of table names, returns the set of table names that are missing
   * metadata (are not yet loaded).
   */
  private Set<TableName> getMissingTbls(Set<TableName> tableNames) {
    Set<TableName> missingTbls = new HashSet<TableName>();
    for (TableName tblName: tableNames) {
      Db db = getCatalog().getDb(tblName.getDb());
      if (db == null) continue;
      Table tbl = db.getTable(tblName.getTbl());
      if (tbl == null) continue;
      if (!tbl.isLoaded()) missingTbls.add(tblName);
    }
    return missingTbls;
  }

  /**
   * Requests the catalog server load the given set of tables and waits until
   * these tables show up in the local catalog, or the given timeout has been reached.
   * The timeout is specified in milliseconds, with a value <= 0 indicating no timeout.
   * The exact steps taken are:
   * 1) Collect the tables that are missing (not yet loaded locally).
   * 2) Make an RPC to the CatalogServer to prioritize the loading of these tables.
   * 3) Wait until the local catalog contains all missing tables by (re)checking the
   *    catalog each time a new catalog update is received.
   *
   * Returns true if all missing tables were received before timing out and false if
   * the timeout was reached before all tables were received.
   */
  private boolean requestTblLoadAndWait(Set<TableName> requestedTbls, long timeoutMs)
      throws InternalException {
    Set<TableName> missingTbls = getMissingTbls(requestedTbls);
    // There are no missing tables, return and avoid making an RPC to the CatalogServer.
    if (missingTbls.isEmpty()) return true;

    // Call into the CatalogServer and request the required tables be loaded.
    LOG.info(String.format("Requesting prioritized load of table(s): %s",
        Joiner.on(", ").join(missingTbls)));
    TStatus status = FeSupport.PrioritizeLoad(missingTbls);
    if (status.getStatus_code() != TStatusCode.OK) {
      throw new InternalException("Error requesting prioritized load: " +
          Joiner.on("\n").join(status.getError_msgs()));
    }

    long startTimeMs = System.currentTimeMillis();
    // Wait until all the required tables are loaded in the Impalad's catalog cache.
    while (!missingTbls.isEmpty()) {
      // Check if the timeout has been reached.
      if (timeoutMs > 0 && System.currentTimeMillis() - startTimeMs > timeoutMs) {
        return false;
      }

      LOG.trace(String.format("Waiting for table(s) to complete loading: %s",
          Joiner.on(", ").join(missingTbls)));
      getCatalog().waitForCatalogUpdate(MAX_CATALOG_UPDATE_WAIT_TIME_MS);
      missingTbls = getMissingTbls(missingTbls);
      // TODO: Check for query cancellation here.
    }
    return true;
  }

  /**
   * Overload of requestTblLoadAndWait that uses the default timeout.
   */
  public boolean requestTblLoadAndWait(Set<TableName> requestedTbls)
      throws InternalException {
    return requestTblLoadAndWait(requestedTbls, MISSING_TBL_LOAD_WAIT_TIMEOUT_MS);
  }

  /**
   * Analyzes the SQL statement included in queryCtxt and returns the AnalysisResult.
   * If a statement fails analysis because table/view metadata was not loaded, an
   * RPC to the CatalogServer will be executed to request loading the missing metadata
   * and analysis will be restarted once the required tables have been loaded
   * in the local Impalad Catalog or the MISSING_TBL_LOAD_WAIT_TIMEOUT_MS timeout
   * is reached.
   * The goal of this timeout is not to analysis, but to restart the analysis/missing
   * table collection process. This helps ensure a statement never waits indefinitely
   * for a table to be loaded in event the table metadata was invalidated.
   * TODO: Also consider adding an overall timeout that fails analysis.
   */
  private AnalysisContext.AnalysisResult analyzeStmt(TQueryContext queryCtxt)
      throws AnalysisException, InternalException, AuthorizationException {
    AnalysisContext analysisCtxt = new AnalysisContext(impaladCatalog_, queryCtxt);
    LOG.debug("analyze query " + queryCtxt.request.stmt);

    // Run analysis in a loop until it either:
    // 1) Completes successfully
    // 2) Fails with an AnalysisException AND there are no missing tables.
    while (true) {
      try {
        analysisCtxt.analyze(queryCtxt.request.stmt);
        Preconditions.checkState(analysisCtxt.getAnalyzer().getMissingTbls().isEmpty());
        return analysisCtxt.getAnalysisResult();
      } catch (AnalysisException e) {
        Set<TableName> missingTbls = analysisCtxt.getAnalyzer().getMissingTbls();
        // Only re-throw the AnalysisException if there were no missing tables.
        if (missingTbls.isEmpty()) throw e;

        // Some tables/views were missing, request and wait for them to load.
        if (!requestTblLoadAndWait(missingTbls, MISSING_TBL_LOAD_WAIT_TIMEOUT_MS)) {
          LOG.info(String.format("Missing tables were not received in %dms. Load " +
              "request will be retried.", MISSING_TBL_LOAD_WAIT_TIMEOUT_MS));
        }
      }
    }
  }

  /**
   * Create a populated TExecRequest corresponding to the supplied TQueryContext.
   */
  public TExecRequest createExecRequest(
      TQueryContext queryCtxt, StringBuilder explainString)
      throws ImpalaException {
    // Analyze the statement
    AnalysisContext.AnalysisResult analysisResult = analyzeStmt(queryCtxt);
    Preconditions.checkNotNull(analysisResult.getStmt());

    TExecRequest result = new TExecRequest();
    result.setQuery_options(queryCtxt.request.getQuery_options());
    result.setAccess_events(analysisResult.getAccessEvents());

    if (analysisResult.isCatalogOp()) {
      result.stmt_type = TStmtType.DDL;
      createCatalogOpRequest(analysisResult, result);

      // All DDL operations except for CTAS are done with analysis at this point.
      if (!analysisResult.isCreateTableAsSelectStmt()) return result;
    } else if (analysisResult.isLoadDataStmt()) {
      result.stmt_type = TStmtType.LOAD;
      result.setResult_set_metadata(new TResultSetMetadata(Arrays.asList(
          new TColumn("summary", ColumnType.STRING.toThrift()))));
      result.setLoad_data_request(analysisResult.getLoadDataStmt().toThrift());
      return result;
    }

    // create TQueryExecRequest
    Preconditions.checkState(analysisResult.isQueryStmt() || analysisResult.isDmlStmt()
        || analysisResult.isCreateTableAsSelectStmt());

    TQueryExecRequest queryExecRequest = new TQueryExecRequest();
    // create plan
    LOG.debug("create plan");
    Planner planner = new Planner();
    ArrayList<PlanFragment> fragments =
        planner.createPlanFragments(analysisResult, queryCtxt.request.query_options);
    List<ScanNode> scanNodes = Lists.newArrayList();
    // map from fragment to its index in queryExecRequest.fragments; needed for
    // queryExecRequest.dest_fragment_idx
    Map<PlanFragment, Integer> fragmentIdx = Maps.newHashMap();
    for (PlanFragment fragment: fragments) {
      TPlanFragment thriftFragment = fragment.toThrift();
      queryExecRequest.addToFragments(thriftFragment);
      Preconditions.checkNotNull(fragment.getPlanRoot());
      fragment.getPlanRoot().collect(Predicates.instanceOf(ScanNode.class), scanNodes);
      fragmentIdx.put(fragment, queryExecRequest.fragments.size() - 1);
    }

    // set fragment destinations
    for (int i = 1; i < fragments.size(); ++i) {
      PlanFragment dest = fragments.get(i).getDestFragment();
      Integer idx = fragmentIdx.get(dest);
      Preconditions.checkState(idx != null);
      queryExecRequest.addToDest_fragment_idx(idx.intValue());
    }

    // Set scan ranges/locations for scan nodes.
    // Also assemble list of tables names missing stats for assembling a warning message.
    LOG.debug("get scan range locations");
    Set<TTableName> tablesMissingStats = Sets.newTreeSet();
    for (ScanNode scanNode: scanNodes) {
      queryExecRequest.putToPer_node_scan_ranges(
          scanNode.getId().asInt(),
          scanNode.getScanRangeLocations(
              queryCtxt.request.query_options.getMax_scan_range_length()));
      if (scanNode.isTableMissingStats()) {
        tablesMissingStats.add(scanNode.getTupleDesc().getTableName().toThrift());
      }
    }
    for (TTableName tableName: tablesMissingStats) {
      queryCtxt.addToTables_missing_stats(tableName);
    }

    // Compute resource requirements after scan range locations because the cost
    // estimates of scan nodes rely on them.
    planner.computeResourceReqs(fragments, true, queryCtxt.request.query_options,
        queryExecRequest);

    // Use VERBOSE by default for all non-explain statements.
    TExplainLevel explainLevel = TExplainLevel.VERBOSE;
    if (queryCtxt.request.query_options.isSetExplain_level()) {
      explainLevel = queryCtxt.request.query_options.getExplain_level();
    } else if (analysisResult.isExplainStmt()) {
      // Use the STANDARD by default for explain statements.
      explainLevel = TExplainLevel.STANDARD;
    }
    // Global query parameters to be set in each TPlanExecRequest.
    queryExecRequest.setQuery_ctxt(queryCtxt);

    explainString.append(planner.getExplainString(fragments, queryExecRequest,
        explainLevel));
    queryExecRequest.setQuery_plan(explainString.toString());
    queryExecRequest.setDesc_tbl(analysisResult.getAnalyzer().getDescTbl().toThrift());

    if (analysisResult.isExplainStmt()) {
      // Return the EXPLAIN request
      createExplainRequest(explainString.toString(), result);
      return result;
    }

    result.setQuery_exec_request(queryExecRequest);

    if (analysisResult.isQueryStmt()) {
      // fill in the metadata
      LOG.debug("create result set metadata");
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
    } else {
      Preconditions.checkState(analysisResult.isInsertStmt() ||
          analysisResult.isCreateTableAsSelectStmt());

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
        String db = insertStmt.getTargetTableName().getDb();
        finalizeParams.setTable_db(db == null ? queryCtxt.session.database : db);
        HdfsTable hdfsTable = (HdfsTable) insertStmt.getTargetTable();
        finalizeParams.setHdfs_base_dir(hdfsTable.getHdfsBaseDir());
        finalizeParams.setStaging_dir(
            hdfsTable.getHdfsBaseDir() + "/.impala_insert_staging");
        queryExecRequest.setFinalize_params(finalizeParams);
      }
    }
    return result;
  }

  /**
   * Attaches the explain result to the TExecRequest.
   */
  private void createExplainRequest(String explainString, TExecRequest result) {
    // update the metadata - one string column
    TColumn colDesc = new TColumn("Explain String", ColumnType.STRING.toThrift());
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
      case GET_SCHEMAS:
      {
        TGetSchemasReq req = request.getGet_schemas_req();
        return MetadataOp.getSchemas(
            impaladCatalog_, req.getCatalogName(), req.getSchemaName(), user);
      }
      case GET_TABLES:
      {
        TGetTablesReq req = request.getGet_tables_req();
        return MetadataOp.getTables(impaladCatalog_, req.getCatalogName(),
            req.getSchemaName(), req.getTableName(), req.getTableTypes(), user);
      }
      case GET_COLUMNS:
      {
        TGetColumnsReq req = request.getGet_columns_req();
        return MetadataOp.getColumns(this, req.getCatalogName(),
            req.getSchemaName(), req.getTableName(), req.getColumnName(), user);
      }
      case GET_CATALOGS: return MetadataOp.getCatalogs();
      case GET_TABLE_TYPES: return MetadataOp.getTableTypes();
      case GET_FUNCTIONS:
      {
        TGetFunctionsReq req = request.getGet_functions_req();
        return MetadataOp.getFunctions(impaladCatalog_, req.getCatalogName(),
            req.getSchemaName(), req.getFunctionName(), user);
      }
      default:
        throw new NotImplementedException(request.opcode + " has not been implemented.");
    }
  }
}
