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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hive.service.cli.thrift.TGetColumnsReq;
import org.apache.hive.service.cli.thrift.TGetFunctionsReq;
import org.apache.hive.service.cli.thrift.TGetSchemasReq;
import org.apache.hive.service.cli.thrift.TGetTablesReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.DropTableOrViewStmt;
import com.cloudera.impala.analysis.InsertStmt;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.analysis.ResetMetadataStmt;
import com.cloudera.impala.analysis.TableName;
import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.authorization.ImpalaInternalAdminUser;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.DatabaseNotFoundException;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.TableNotFoundException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.planner.PlanFragment;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.planner.ScanNode;
import com.cloudera.impala.thrift.TCatalogUpdate;
import com.cloudera.impala.thrift.TClientRequest;
import com.cloudera.impala.thrift.TColumnDesc;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TDdlExecRequest;
import com.cloudera.impala.thrift.TDdlType;
import com.cloudera.impala.thrift.TDescribeTableOutputStyle;
import com.cloudera.impala.thrift.TDescribeTableResult;
import com.cloudera.impala.thrift.TExecRequest;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TExplainResult;
import com.cloudera.impala.thrift.TFinalizeParams;
import com.cloudera.impala.thrift.TLoadDataReq;
import com.cloudera.impala.thrift.TLoadDataResp;
import com.cloudera.impala.thrift.TMetadataOpRequest;
import com.cloudera.impala.thrift.TMetadataOpResponse;
import com.cloudera.impala.thrift.TPlanFragment;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TResetMetadataParams;
import com.cloudera.impala.thrift.TResultRow;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.cloudera.impala.thrift.TStmtType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Frontend API for the impalad process.
 * This class allows the impala daemon to create TQueryExecRequest
 * in response to TClientRequests.
 */
public class Frontend {
  private final static Logger LOG = LoggerFactory.getLogger(Frontend.class);
  private final boolean lazyCatalog;

  private Catalog catalog;
  private DdlExecutor ddlExecutor;
  private final AuthorizationConfig authzConfig;

  public Frontend(boolean lazy, AuthorizationConfig authorizationConfig) {
    this.lazyCatalog = lazy;
    this.authzConfig = authorizationConfig;
    this.catalog = new Catalog(lazy, false, authzConfig);
    ddlExecutor = new DdlExecutor(catalog);
  }

  public DdlExecutor getDdlExecutor() {
    return ddlExecutor;
  }

  /**
   * Invalidates all catalog metadata, forcing a reload.
   */
  private void resetCatalog() {
    catalog.close();
    catalog = new Catalog(lazyCatalog, true, authzConfig);
    ddlExecutor = new DdlExecutor(catalog);
  }

  public Catalog getCatalog() {
    return catalog;
  }

  /**
   * If isRefresh is false, invalidates a specific table's metadata, forcing the
   * metadata to be reloaded on the next access.
   * If isRefresh is true, performs an immediate incremental refresh.
   */
  private void resetTable(String dbName, String tableName, boolean isRefresh)
      throws CatalogException {
    Db db = catalog.getDb(dbName, ImpalaInternalAdminUser.getInstance(), Privilege.ANY);
    if (db == null) {
      throw new DatabaseNotFoundException("Database not found: " + dbName);
    }
    if (!db.containsTable(tableName)) {
      throw new TableNotFoundException(
          "Table not found: " + dbName + "." + tableName);
    }
    if (isRefresh) {
      LOG.info("Refreshing table metadata: " + dbName + "." + tableName);
      db.refreshTable(tableName);
    } else {
      LOG.info("Invalidating table metadata: " + dbName + "." + tableName);
      db.invalidateTable(tableName);
    }
  }

  public void close() {
    this.catalog.close();
  }

  /**
   * Constructs a TDdlExecRequest and attaches it, plus any metadata, to the
   * result argument.
   */
  private void createDdlExecRequest(AnalysisContext.AnalysisResult analysis,
      TExecRequest result) {
    TDdlExecRequest ddl = new TDdlExecRequest();
    TResultSetMetadata metadata = new TResultSetMetadata();
    if (analysis.isUseStmt()) {
      ddl.ddl_type = TDdlType.USE;
      ddl.setUse_db_params(analysis.getUseStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isShowTablesStmt()) {
      ddl.ddl_type = TDdlType.SHOW_TABLES;
      ddl.setShow_tables_params(analysis.getShowTablesStmt().toThrift());
      metadata.setColumnDescs(Arrays.asList(
          new TColumnDesc("name", TPrimitiveType.STRING)));
    } else if (analysis.isShowDbsStmt()) {
      ddl.ddl_type = TDdlType.SHOW_DBS;
      ddl.setShow_dbs_params(analysis.getShowDbsStmt().toThrift());
      metadata.setColumnDescs(Arrays.asList(
          new TColumnDesc("name", TPrimitiveType.STRING)));
    } else if (analysis.isDescribeStmt()) {
      ddl.ddl_type = TDdlType.DESCRIBE;
      ddl.setDescribe_table_params(analysis.getDescribeStmt().toThrift());
      metadata.setColumnDescs(Arrays.asList(
          new TColumnDesc("name", TPrimitiveType.STRING),
          new TColumnDesc("type", TPrimitiveType.STRING),
          new TColumnDesc("comment", TPrimitiveType.STRING)));
    } else if (analysis.isAlterTableStmt()) {
      ddl.ddl_type = TDdlType.ALTER_TABLE;
      ddl.setAlter_table_params(analysis.getAlterTableStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isAlterViewStmt()) {
      ddl.ddl_type = TDdlType.ALTER_VIEW;
      ddl.setAlter_view_params(analysis.getAlterViewStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateTableStmt()) {
      ddl.ddl_type = TDdlType.CREATE_TABLE;
      ddl.setCreate_table_params(analysis.getCreateTableStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateTableLikeStmt()) {
      ddl.ddl_type = TDdlType.CREATE_TABLE_LIKE;
      ddl.setCreate_table_like_params(analysis.getCreateTableLikeStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateViewStmt()) {
      ddl.ddl_type = TDdlType.CREATE_VIEW;
      ddl.setCreate_view_params(analysis.getCreateViewStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateDbStmt()) {
      ddl.ddl_type = TDdlType.CREATE_DATABASE;
      ddl.setCreate_db_params(analysis.getCreateDbStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isDropDbStmt()) {
      ddl.ddl_type = TDdlType.DROP_DATABASE;
      ddl.setDrop_db_params(analysis.getDropDbStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isDropTableOrViewStmt()) {
      DropTableOrViewStmt stmt = analysis.getDropTableOrViewStmt();
      ddl.ddl_type = (stmt.isDropTable()) ? TDdlType.DROP_TABLE : TDdlType.DROP_VIEW;
      ddl.setDrop_table_or_view_params(stmt.toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isResetMetadataStmt()) {
      ddl.ddl_type = TDdlType.RESET_METADATA;
      ResetMetadataStmt resetMetadataStmt = (ResetMetadataStmt) analysis.getStmt();
      ddl.setReset_metadata_params(resetMetadataStmt.toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    }

    result.setResult_set_metadata(metadata);
    result.setDdl_exec_request(ddl);
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
      destPathString = catalog.getHdfsPartition(tableName.getDb(), tableName.getTbl(),
          request.getPartition_spec()).getLocation();
    } else {
      destPathString = catalog.getTable(tableName.getDb(), tableName.getTbl(),
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
    resetTable(tableName.getDb(), tableName.getTbl(), true);

    TLoadDataResp response = new TLoadDataResp();
    TColumnValue col = new TColumnValue();
    String loadMsg = String.format(
        "Loaded %d file(s). Total files in destination location: %d",
        filesLoaded, FileSystemUtil.getTotalNumVisibleFiles(destPath));
    col.setStringVal(loadMsg);
    response.setLoad_summary(new TResultRow(Lists.newArrayList(col)));
    return response;
  }

  /**
   * Parses and plans a query in order to generate its explain string. This method does
   * not increase the query id counter.
   */
  public String getExplainString(TClientRequest request) throws ImpalaException {
    StringBuilder stringBuilder = new StringBuilder();
    createExecRequest(request, stringBuilder);
    return stringBuilder.toString();
  }

  /**
   * Returns all tables that match the specified database and pattern that are accessible
   * to the given user. If pattern is null, matches all tables. If db is null, all
   * databases are searched for matches.
   */
  public List<String> getTableNames(String dbName, String tablePattern, User user)
      throws ImpalaException {
    return catalog.getTableNames(dbName, tablePattern, user);
  }

  /**
   * Returns all database names that match the specified database and pattern that
   * are accessible to the given user. If pattern is null, matches all dbs.
   */
  public List<String> getDbNames(String dbPattern, User user)
      throws ImpalaException {
    return catalog.getDbNames(dbPattern, user);
  }

  /**
   * Returns table metadata, such as the column descriptors, in the specified table.
   * Throws an exception if the table or db is not found or if there is an error
   * loading the table metadata.
   */
  public TDescribeTableResult describeTable(String dbName, String tableName,
      TDescribeTableOutputStyle outputStyle) throws ImpalaException {
    Table table = catalog.getTable(dbName, tableName,
        ImpalaInternalAdminUser.getInstance(), Privilege.ALL);
    return DescribeResultFactory.buildDescribeTableResult(table, outputStyle);
  }

  /**
   * new planner interface:
   * Create a populated TExecRequest corresponding to the supplied
   * TClientRequest.
   */
  public TExecRequest createExecRequest(
      TClientRequest request, StringBuilder explainString)
      throws AuthorizationException, InternalException, AnalysisException,
      NotImplementedException {
    AnalysisContext analysisCtxt = new AnalysisContext(catalog,
        request.sessionState.database,
        new User(request.sessionState.user));
    AnalysisContext.AnalysisResult analysisResult = null;
    LOG.info("analyze query " + request.stmt);
    analysisResult = analysisCtxt.analyze(request.stmt);
    Preconditions.checkNotNull(analysisResult.getStmt());

    TExecRequest result = new TExecRequest();
    result.setQuery_options(request.getQueryOptions());

    if (analysisResult.isDdlStmt()) {
      result.stmt_type = TStmtType.DDL;
      createDdlExecRequest(analysisResult, result);
      return result;
    } else if (analysisResult.isLoadDataStmt()) {
      result.stmt_type = TStmtType.LOAD;
      result.setResult_set_metadata(new TResultSetMetadata(Arrays.asList(
          new TColumnDesc("summary", TPrimitiveType.STRING))));
      result.setLoad_data_request(analysisResult.getLoadDataStmt().toThrift());
      return result;
    }

    // create TQueryExecRequest
    Preconditions.checkState(
        analysisResult.isQueryStmt() || analysisResult.isDmlStmt());
    TQueryExecRequest queryExecRequest = new TQueryExecRequest();

    // create plan
    LOG.info("create plan");
    Planner planner = new Planner();
    ArrayList<PlanFragment> fragments =
        planner.createPlanFragments(analysisResult, request.queryOptions);
    List<ScanNode> scanNodes = Lists.newArrayList();
    // map from fragment to its index in queryExecRequest.fragments; needed for
    // queryExecRequest.dest_fragment_idx
    Map<PlanFragment, Integer> fragmentIdx = Maps.newHashMap();
    for (PlanFragment fragment: fragments) {
      TPlanFragment thriftFragment = fragment.toThrift();
      queryExecRequest.addToFragments(thriftFragment);
      Preconditions.checkNotNull(fragment.getPlanRoot());
      fragment.getPlanRoot().collectSubclasses(ScanNode.class, scanNodes);
      fragmentIdx.put(fragment, queryExecRequest.fragments.size() - 1);
    }
    explainString.append(planner.getExplainString(fragments, TExplainLevel.VERBOSE));
    queryExecRequest.setQuery_plan(explainString.toString());
    queryExecRequest.setDesc_tbl(analysisResult.getAnalyzer().getDescTbl().toThrift());

    if (analysisResult.isExplainStmt()) {
      // Return the EXPLAIN request
      createExplainRequest(explainString.toString(), result);
      return result;
    }

    result.setQuery_exec_request(queryExecRequest);

    // set fragment destinations
    for (int i = 1; i < fragments.size(); ++i) {
      PlanFragment dest = fragments.get(i).getDestFragment();
      Integer idx = fragmentIdx.get(dest);
      Preconditions.checkState(idx != null);
      queryExecRequest.addToDest_fragment_idx(idx.intValue());
    }

    // set scan ranges/locations for scan nodes
    LOG.info("get scan range locations");
    for (ScanNode scanNode: scanNodes) {
      queryExecRequest.putToPer_node_scan_ranges(
          scanNode.getId().asInt(),
          scanNode.getScanRangeLocations(
            request.queryOptions.getMax_scan_range_length()));
    }

    // Global query parameters to be set in each TPlanExecRequest.
    queryExecRequest.query_globals = analysisCtxt.getQueryGlobals();

    if (analysisResult.isQueryStmt()) {
      // fill in the metadata
      LOG.info("create result set metadata");
      result.stmt_type = TStmtType.QUERY;
      TResultSetMetadata metadata = new TResultSetMetadata();
      QueryStmt queryStmt = analysisResult.getQueryStmt();
      int colCnt = queryStmt.getColLabels().size();
      for (int i = 0; i < colCnt; ++i) {
        TColumnDesc colDesc = new TColumnDesc();
        colDesc.columnName = queryStmt.getColLabels().get(i);
        colDesc.columnType = queryStmt.getResultExprs().get(i).getType().toThrift();
        metadata.addToColumnDescs(colDesc);
      }
      result.setResult_set_metadata(metadata);
    } else {
      Preconditions.checkState(analysisResult.isInsertStmt());
      result.stmt_type = TStmtType.DML;
      // create finalization params of insert stmt
      InsertStmt insertStmt = analysisResult.getInsertStmt();
      if (insertStmt.getTargetTable() instanceof HdfsTable) {
        TFinalizeParams finalizeParams = new TFinalizeParams();
        finalizeParams.setIs_overwrite(insertStmt.isOverwrite());
        finalizeParams.setTable_name(insertStmt.getTargetTableName().getTbl());
        String db = insertStmt.getTargetTableName().getDb();
        finalizeParams.setTable_db(db == null ? request.sessionState.database : db);
        HdfsTable hdfsTable = (HdfsTable) insertStmt.getTargetTable();
        finalizeParams.setHdfs_base_dir(hdfsTable.getHdfsBaseDir());
        queryExecRequest.setFinalize_params(finalizeParams);
      }
    }

    // Copy the statement type into the TQueryExecRequest so that it
    // is visible to the coordinator.
    result.query_exec_request.stmt_type = result.stmt_type;
    return result;
  }

  /**
   * Attaches the explain result to the TExecRequest.
   */
  private void createExplainRequest(String explainString, TExecRequest result) {
    // update the metadata - one string column
    TColumnDesc colDesc = new TColumnDesc("Explain String", TPrimitiveType.STRING);
    TResultSetMetadata metadata = new TResultSetMetadata(Lists.newArrayList(colDesc));
    result.setResult_set_metadata(metadata);

    // create the explain result set - split the explain string into one line per row
    String[] explainStringArray = explainString.toString().split("\n");
    TExplainResult explainResult = new TExplainResult();
    explainResult.results = Lists.newArrayList();
    for (int i = 0; i < explainStringArray.length; ++i) {
      TColumnValue col = new TColumnValue();
      col.setStringVal(explainStringArray[i]);
      TResultRow row = new TResultRow(Lists.newArrayList(col));
      explainResult.results.add(row);
    }
    result.setExplain_result(explainResult);
    result.stmt_type = TStmtType.EXPLAIN;
  }

  /**
   * Executes a HiveServer2 metadata operation and returns a TMetadataOpResponse
   */
  public TMetadataOpResponse execHiveServer2MetadataOp(TMetadataOpRequest request)
      throws ImpalaException {
    User user = request.isSetSession() ?
        new User(request.session.getUser()) : ImpalaInternalAdminUser.getInstance();
    switch (request.opcode) {
      case GET_TYPE_INFO: return MetadataOp.getTypeInfo();
      case GET_SCHEMAS:
      {
        TGetSchemasReq req = request.getGet_schemas_req();
        return MetadataOp.getSchemas(
            catalog, req.getCatalogName(), req.getSchemaName(), user);
      }
      case GET_TABLES:
      {
        TGetTablesReq req = request.getGet_tables_req();
        return MetadataOp.getTables(catalog, req.getCatalogName(), req.getSchemaName(),
            req.getTableName(), req.getTableTypes(), user);
      }
      case GET_COLUMNS:
      {
        TGetColumnsReq req = request.getGet_columns_req();
        return MetadataOp.getColumns(catalog, req.getCatalogName(), req.getSchemaName(),
            req.getTableName(), req.getColumnName(), user);
      }
      case GET_CATALOGS: return MetadataOp.getCatalogs();
      case GET_TABLE_TYPES: return MetadataOp.getTableTypes();
      case GET_FUNCTIONS:
      {
        TGetFunctionsReq req = request.getGet_functions_req();
        return MetadataOp.getFunctions(req.getCatalogName(), req.getSchemaName(),
            req.getFunctionName());
      }
      default:
        throw new NotImplementedException(request.opcode + " has not been implemented.");
    }
  }

  /**
   * Create any new partitions required as a result of an INSERT statement.
   * Updates the lastDdlTime of the table if new partitions were created.
   */
  public void updateMetastore(TCatalogUpdate update) throws ImpalaException {
    // Only update metastore for Hdfs tables.
    Table table = catalog.getTable(update.getDb_name(), update.getTarget_table(),
        ImpalaInternalAdminUser.getInstance(), Privilege.ALL);
    if (!(table instanceof HdfsTable)) {
      LOG.warn("Unexpected table type in updateMetastore: "
          + update.getTarget_table());
      return;
    }

    String dbName = table.getDb().getName();
    String tblName = table.getName();
    MetaStoreClient msClient = catalog.getMetaStoreClient();
    try {
      boolean addedNewPartition = false;
      if (table.getNumClusteringCols() > 0) {
        // Add all partitions to metastore.
        for (String partName: update.getCreated_partitions()) {
          try {
            LOG.info("Creating partition: " + partName + " in table: " + tblName);
            msClient.getHiveClient().appendPartitionByName(dbName, tblName, partName);
            addedNewPartition = true;
          } catch (AlreadyExistsException e) {
            LOG.info("Ignoring partition " + partName + ", since it already exists");
            // Ignore since partition already exists.
          } catch (Exception e) {
            throw new InternalException("Error updating metastore", e);
          }
        }
      }
      if (addedNewPartition) {
        try {
          // Operate on a copy of msTbl to prevent our cached msTbl becoming inconsistent
          // if the alteration fails in the metastore.
          org.apache.hadoop.hive.metastore.api.Table msTbl =
              table.getMetaStoreTable().deepCopy();
          DdlExecutor.updateLastDdlTime(msTbl, msClient);
        } catch (Exception e) {
          throw new InternalException("Error updating lastDdlTime", e);
        }
      }
    } finally {
      msClient.release();
    }

    // Refresh the table metadata.
    resetTable(dbName, tblName, true);
  }

  /**
   * Execute a reset metadata statement.
   */
  public void execResetMetadata(TResetMetadataParams params)
      throws CatalogException {
    if (params.isSetTable_name()) {
      resetTable(params.getTable_name().getDb_name(),
          params.getTable_name().getTable_name(), params.isIs_refresh());
    } else {
      // Invalidate the catalog if no table name is provided.
      Preconditions.checkArgument(!params.isIs_refresh());
      resetCatalog();
    }
  }
}
