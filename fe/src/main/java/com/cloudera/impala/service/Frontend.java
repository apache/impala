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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hive.service.cli.thrift.TGetColumnsReq;
import org.apache.hive.service.cli.thrift.TGetFunctionsReq;
import org.apache.hive.service.cli.thrift.TGetSchemasReq;
import org.apache.hive.service.cli.thrift.TGetTablesReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.InsertStmt;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.analysis.TableName;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Catalog.DatabaseNotFoundException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Db.TableLoadingException;
import com.cloudera.impala.catalog.FileFormat;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.planner.PlanFragment;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.planner.ScanNode;
import com.cloudera.impala.thrift.TCatalogUpdate;
import com.cloudera.impala.thrift.TClientRequest;
import com.cloudera.impala.thrift.TColumnDef;
import com.cloudera.impala.thrift.TColumnDesc;
import com.cloudera.impala.thrift.TDdlExecRequest;
import com.cloudera.impala.thrift.TDdlType;
import com.cloudera.impala.thrift.TExecRequest;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TFinalizeParams;
import com.cloudera.impala.thrift.TMetadataOpRequest;
import com.cloudera.impala.thrift.TMetadataOpResponse;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.cloudera.impala.thrift.TPlanFragment;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.cloudera.impala.thrift.TStmtType;
import com.cloudera.impala.thrift.TUniqueId;
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

  public Frontend() {
    // Default to lazy loading
    this(true);
  }

  public Frontend(boolean lazy) {
    this.lazyCatalog = lazy;
    this.catalog = new Catalog(lazy, false);
  }

  /**
   * Invalidates all catalog metadata, forcing a reload.
   */
  public void resetCatalog() {
    this.catalog.close();
    this.catalog = new Catalog(lazyCatalog, true);
  }

  /**
   * Invalidates a specific table's metadata, forcing the metadata to be reloaded on
   * the next access.
   * @throws DatabaseNotFoundException - If the specified database does not exist.
   * @throws TableNotFoundException - If the specified table does not exist.
   */
  public void resetTable(String dbName, String tableName)
      throws Catalog.DatabaseNotFoundException, Catalog.TableNotFoundException {
    Db db = catalog.getDb(dbName);
    if (db == null) {
      throw new Catalog.DatabaseNotFoundException("Database not found: " + dbName);
    }
    if (!db.containsTable(tableName)) {
      throw new Catalog.TableNotFoundException(
          "Table not found: " + dbName + "." + tableName);
    }
    LOG.info("Invalidating table metadata: " + dbName + "." + tableName);
    db.invalidateTable(tableName, true);
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
    } else if (analysis.isCreateTableStmt()) {
      ddl.ddl_type = TDdlType.CREATE_TABLE;
      ddl.setCreate_table_params(analysis.getCreateTableStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateTableLikeStmt()) {
      ddl.ddl_type = TDdlType.CREATE_TABLE_LIKE;
      ddl.setCreate_table_like_params(analysis.getCreateTableLikeStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateDbStmt()) {
      ddl.ddl_type = TDdlType.CREATE_DATABASE;
      ddl.setCreate_db_params(analysis.getCreateDbStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isDropDbStmt()) {
      ddl.ddl_type = TDdlType.DROP_DATABASE;
      ddl.setDrop_db_params(analysis.getDropDbStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isDropTableStmt()) {
      ddl.ddl_type = TDdlType.DROP_TABLE;
      ddl.setDrop_table_params(analysis.getDropTableStmt().toThrift());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    }

    result.setResult_set_metadata(metadata);
    result.setDdl_exec_request(ddl);
  }

  /**
   * Appends one or more columns to the given table, optionally replacing all existing
   * columns.
   */
  public void alterTableAddReplaceCols(TableName tableName, List<TColumnDef> columns,
      boolean replaceExistingCols)
      throws MetaException, org.apache.thrift.TException, InvalidObjectException,
      ImpalaException, TableLoadingException {
    catalog.alterTableAddReplaceCols(tableName, columns, replaceExistingCols);
  }

  /**
   * Adds a new partition to an existing table.
   */
  public void alterTableAddPartition(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, String location, boolean ifNotExists)
      throws MetaException, org.apache.thrift.TException, InvalidObjectException,
      ImpalaException, TableLoadingException {
    catalog.alterTableAddPartition(tableName, partitionSpec, location, ifNotExists);
  }

  /**
   * Drops a column from an existing table.
   */
  public void alterTableDropCol(TableName tableName, String colName)
      throws MetaException, org.apache.thrift.TException, InvalidObjectException,
      ImpalaException, TableLoadingException {
    catalog.alterTableDropCol(tableName, colName);
  }

  /**
   * Drops a partition from an existing table.
   */
  public void alterTableDropPartition(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, boolean ifExists)
      throws MetaException, org.apache.thrift.TException, InvalidObjectException,
      ImpalaException, TableLoadingException {
    catalog.alterTableDropPartition(tableName, partitionSpec, ifExists);
  }

  /**
   * Changes the column definition of an existing column.
   */
  public void alterTableChangeCol(TableName tableName, String colName,
      TColumnDef newColDef) throws MetaException, org.apache.thrift.TException,
      InvalidObjectException, ImpalaException, TableLoadingException {
    catalog.alterTableChangeCol(tableName, colName, newColDef);
  }

  /**
   * Renames a table.
   */
  public void alterTableRename(TableName tableName, TableName newTableName)
      throws MetaException, org.apache.thrift.TException, InvalidObjectException,
      ImpalaException, TableLoadingException {
    catalog.alterTableRename(tableName, newTableName);
  }

  /**
   * Changes the file format for the given table.
   */
  public void alterTableSetFileFormat(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, FileFormat fileFormat) throws MetaException,
      org.apache.thrift.TException, InvalidObjectException, ImpalaException,
      TableLoadingException {
    catalog.alterTableSetFileFormat(tableName, partitionSpec, fileFormat);
  }

  /**
   * Changes the HDFS storage location for the given table.
   */
  public void alterTableSetLocation(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, String location) throws MetaException,
      org.apache.thrift.TException, InvalidObjectException, ImpalaException,
      TableLoadingException {
    catalog.alterTableSetLocation(tableName, partitionSpec, location);
  }

  /**
   * Creates a new database in the metastore.
   */
  public void createDatabase(String dbName, String comment, String locationUri,
      boolean ifNotExists) throws MetaException, org.apache.thrift.TException,
      AlreadyExistsException, InvalidObjectException {
    catalog.createDatabase(dbName, comment, locationUri, ifNotExists);
  }

  /**
   * Creates a new table in the metastore.
   */
  public void createTable(TableName tableName, List<TColumnDef> columns,
      List<TColumnDef> partitionColumns, String owner, boolean isExternal, String comment,
      RowFormat rowFormat, FileFormat fileFormat, String location, boolean ifNotExists)
      throws MetaException, NoSuchObjectException, org.apache.thrift.TException,
      AlreadyExistsException, InvalidObjectException {
    catalog.createTable(tableName, columns, partitionColumns, owner, isExternal, comment,
        rowFormat, fileFormat, location, ifNotExists);
  }

  /**
   * Creates a new table in the metastore.
   */
  public void createTableLike(TableName tableName, TableName oldTableName, String owner,
      boolean isExternal, String comment, FileFormat fileFormat, String location,
      boolean ifNotExists) throws MetaException, NoSuchObjectException,
      org.apache.thrift.TException, AlreadyExistsException, InvalidObjectException,
      ImpalaException, TableLoadingException {
    catalog.createTableLike(tableName, oldTableName, owner, isExternal, comment,
        fileFormat, location, ifNotExists);
  }

  /**
   * Drops the specified table.
   */
  public void dropTable(TableName tableName, boolean ifExists)
      throws MetaException, NoSuchObjectException, org.apache.thrift.TException,
      AlreadyExistsException, InvalidObjectException, InvalidOperationException {
    catalog.dropTable(tableName, ifExists);
  }

  /**
   * Drops the specified database.
   */
  public void dropDatabase(String dbName, boolean ifExists)
    throws MetaException, NoSuchObjectException, org.apache.thrift.TException,
    AlreadyExistsException, InvalidObjectException, InvalidOperationException {
    catalog.dropDatabase(dbName, ifExists);
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
   * Returns all tables that match the specified database and pattern.  If
   * pattern is null, matches all tables. If db is null, all databases are
   * searched for matches.
   */
  public List<String> getTableNames(String dbName, String tablePattern)
      throws ImpalaException {
    return catalog.getTableNames(dbName, tablePattern);
  }

  /**
   * Returns all tables that match the specified database and pattern.  If
   * pattern is null, matches all tables. If db is null, all databases are
   * searched for matches.
   */
  public List<String> getDbNames(String dbPattern)
      throws ImpalaException {
    return catalog.getDbNames(dbPattern);
  }

  /**
   * Returns a list of column descriptors describing a the columns in the
   * specified table. Throws an AnalysisException if the table or db is not
   * found.
   */
  public List<TColumnDef> describeTable(String dbName, String tableName)
      throws ImpalaException {
    Db db = catalog.getDb(dbName);
    if (db == null) {
      throw new AnalysisException("Unknown database: " + dbName);
    }

    Table table = null;
    try {
      table = db.getTable(tableName);
    } catch (TableLoadingException e) {
      throw new AnalysisException("Failed to load table metadata for: " + tableName, e);
    }

    if (table == null) {
      throw new AnalysisException("Unknown table: " + db.getName() + "." + tableName);
    }

    List<TColumnDef> columns = Lists.newArrayList();
    for (Column column: table.getColumnsInHiveOrder()) {
      TColumnDef colDef = new TColumnDef(
          new TColumnDesc(column.getName(), column.getType().toThrift()));
      colDef.setComment(column.getComment());
      columns.add(colDef);
    }

    return columns;
  }

  /**
   * new planner interface:
   * Create a populated TExecRequest corresponding to the supplied
   * TClientRequest.
   */
  public TExecRequest createExecRequest(
      TClientRequest request, StringBuilder explainString)
      throws InternalException, AnalysisException, NotImplementedException {
    AnalysisContext analysisCtxt = new AnalysisContext(catalog,
        request.sessionState.database, request.sessionState.user);
    AnalysisContext.AnalysisResult analysisResult = null;
    LOG.info("analyze query " + request.stmt);
    analysisResult = analysisCtxt.analyze(request.stmt);
    Preconditions.checkNotNull(analysisResult.getStmt());

    TExecRequest result = new TExecRequest();
    result.setSql_stmt(request.stmt);
    result.setQuery_options(request.getQueryOptions());

    // assign request_id
    UUID requestId = UUID.randomUUID();
    result.setRequest_id(
        new TUniqueId(requestId.getMostSignificantBits(),
                      requestId.getLeastSignificantBits()));

    if (analysisResult.isDdlStmt()) {
      result.stmt_type = TStmtType.DDL;
      createDdlExecRequest(analysisResult, result);
      return result;
    }

    // create TQueryExecRequest
    Preconditions.checkState(
        analysisResult.isQueryStmt() || analysisResult.isDmlStmt());
    TQueryExecRequest queryExecRequest = new TQueryExecRequest();
    result.setQuery_exec_request(queryExecRequest);

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
      if (fragment.getPlanRoot() != null) {
        fragment.getPlanRoot().collectSubclasses(ScanNode.class, scanNodes);
      }
      fragmentIdx.put(fragment, queryExecRequest.fragments.size() - 1);
    }
    explainString.append(planner.getExplainString(fragments, TExplainLevel.VERBOSE));
    queryExecRequest.setQuery_plan(explainString.toString());
    if (fragments.get(0).getPlanRoot() != null) {
      // a SELECT without FROM clause will only have a single fragment, which won't
      // have a plan tree
      queryExecRequest.setDesc_tbl(analysisResult.getAnalyzer().getDescTbl().toThrift());
    }

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
   * Executes a HiveServer2 metadata operation and returns a TMetadataOpResponse
   */
  public TMetadataOpResponse execHiveServer2MetadataOp(TMetadataOpRequest request)
      throws ImpalaException {
    switch (request.opcode) {
      case GET_TYPE_INFO: return MetadataOp.getTypeInfo();
      case GET_SCHEMAS:
      {
        TGetSchemasReq req = request.getGet_schemas_req();
        return MetadataOp.getSchemas(catalog, req.getCatalogName(), req.getSchemaName());
      }
      case GET_TABLES:
      {
        TGetTablesReq req = request.getGet_tables_req();
        return MetadataOp.getTables(catalog, req.getCatalogName(), req.getSchemaName(),
            req.getTableName(), req.getTableTypes());
      }
      case GET_COLUMNS:
      {
        TGetColumnsReq req = request.getGet_columns_req();
        return MetadataOp.getColumns(catalog, req.getCatalogName(), req.getSchemaName(),
            req.getTableName(), req.getColumnName());
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
   * Create any new partitions required as a result of an INSERT statement
   */
  public void updateMetastore(TCatalogUpdate update) throws ImpalaException {
    // Only update metastore for Hdfs tables.
    Table table = catalog.getDb(update.getDb_name()).getTable(update.getTarget_table());
    if (!(table instanceof HdfsTable)) {
      LOG.warn("Unexpected table type in updateMetastore: "
          + update.getTarget_table());
      return;
    }

    String dbName = table.getDb().getName();
    String tblName = table.getName();
    if (table.getNumClusteringCols() > 0) {
      MetaStoreClient msClient = catalog.getMetaStoreClient();
      try {
        // Add all partitions to metastore.
        for (String partName: update.getCreated_partitions()) {
          try {
            LOG.info("Creating partition: " + partName + " in table: " + tblName);
            msClient.getHiveClient().appendPartitionByName(dbName, tblName, partName);
          } catch (AlreadyExistsException e) {
            LOG.info("Ignoring partition " + partName + ", since it already exists");
            // Ignore since partition already exists.
          } catch (Exception e) {
            throw new InternalException("Error updating metastore", e);
          }
        }
      } finally {
        msClient.release();
      }
    }
    // Mark the table metadata as invalid so it will be reloaded on the next access.
    LOG.info("Invalidating table metadata: " + dbName + "." + tblName);
    catalog.invalidateTable(dbName, tblName, true);
  }
}
