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
import org.apache.hive.service.cli.thrift.TGetColumnsReq;
import org.apache.hive.service.cli.thrift.TGetFunctionsReq;
import org.apache.hive.service.cli.thrift.TGetSchemasReq;
import org.apache.hive.service.cli.thrift.TGetTablesReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.CreateUdaStmt;
import com.cloudera.impala.analysis.CreateUdfStmt;
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
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.DatabaseNotFoundException;
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
import com.cloudera.impala.thrift.TFunctionType;
import com.cloudera.impala.thrift.TInternalCatalogUpdateRequest;
import com.cloudera.impala.thrift.TInternalCatalogUpdateResponse;
import com.cloudera.impala.thrift.TLoadDataReq;
import com.cloudera.impala.thrift.TLoadDataResp;
import com.cloudera.impala.thrift.TMetadataOpRequest;
import com.cloudera.impala.thrift.TMetadataOpResponse;
import com.cloudera.impala.thrift.TPlanFragment;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TResetMetadataRequest;
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
  private ImpaladCatalog impaladCatalog_;
  private final AuthorizationConfig authzConfig_;

  public Frontend(AuthorizationConfig authorizationConfig) {
    this(Catalog.CatalogInitStrategy.EMPTY, authorizationConfig);
  }

  // C'tor used by some tests.
  public Frontend(Catalog.CatalogInitStrategy initStrategy,
      AuthorizationConfig authorizationConfig) {
    authzConfig_ = authorizationConfig;
    impaladCatalog_ = new ImpaladCatalog(initStrategy, authzConfig_);
  }

  public ImpaladCatalog getCatalog() { return impaladCatalog_; }

  public TInternalCatalogUpdateResponse updateInternalCatalog(
      TInternalCatalogUpdateRequest req) throws CatalogException {
    ImpaladCatalog catalog = impaladCatalog_;

    // If this is not a delta, this update should replace the current
    // Catalog contents so create a new catalog and populate it.
    if (!req.is_delta) {
      catalog = new ImpaladCatalog(Catalog.CatalogInitStrategy.EMPTY,
          authzConfig_);
    }
    TInternalCatalogUpdateResponse response = catalog.updateCatalog(req);
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
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isShowTablesStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_TABLES;
      ddl.setShow_tables_params(analysis.getShowTablesStmt().toThrift());
      metadata.setColumnDescs(Arrays.asList(
          new TColumnDesc("name", TPrimitiveType.STRING)));
    } else if (analysis.isShowDbsStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_DBS;
      ddl.setShow_dbs_params(analysis.getShowDbsStmt().toThrift());
      metadata.setColumnDescs(Arrays.asList(
          new TColumnDesc("name", TPrimitiveType.STRING)));
    } else if (analysis.isShowFunctionsStmt()) {
      ddl.op_type = TCatalogOpType.SHOW_FUNCTIONS;
      ShowFunctionsStmt stmt = (ShowFunctionsStmt)analysis.getStmt();
      ddl.setShow_fns_params(stmt.toThrift());
      metadata.setColumnDescs(Arrays.asList(
          new TColumnDesc("name", TPrimitiveType.STRING)));
    } else if (analysis.isDescribeStmt()) {
      ddl.op_type = TCatalogOpType.DESCRIBE;
      ddl.setDescribe_table_params(analysis.getDescribeStmt().toThrift());
      metadata.setColumnDescs(Arrays.asList(
          new TColumnDesc("name", TPrimitiveType.STRING),
          new TColumnDesc("type", TPrimitiveType.STRING),
          new TColumnDesc("comment", TPrimitiveType.STRING)));
    } else if (analysis.isAlterTableStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.ALTER_TABLE);
      req.setAlter_table_params(analysis.getAlterTableStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isAlterViewStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.ALTER_VIEW);
      req.setAlter_view_params(analysis.getAlterViewStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateTableStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_TABLE);
      req.setCreate_table_params(analysis.getCreateTableStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateTableAsSelectStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_TABLE_AS_SELECT);
      req.setCreate_table_params(
          analysis.getCreateTableAsSelectStmt().getCreateStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Arrays.asList(
          new TColumnDesc("summary", TPrimitiveType.STRING)));
    } else if (analysis.isCreateTableLikeStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_TABLE_LIKE);
      req.setCreate_table_like_params(analysis.getCreateTableLikeStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateViewStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_VIEW);
      req.setCreate_view_params(analysis.getCreateViewStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateDbStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_DATABASE);
      req.setCreate_db_params(analysis.getCreateDbStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateUdfStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      CreateUdfStmt stmt = (CreateUdfStmt) analysis.getStmt();
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_FUNCTION);
      req.setCreate_fn_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isCreateUdaStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.CREATE_FUNCTION);
      CreateUdaStmt stmt = (CreateUdaStmt)analysis.getStmt();
      req.setCreate_fn_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isDropDbStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.DROP_DATABASE);
      req.setDrop_db_params(analysis.getDropDbStmt().toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isDropTableOrViewStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      DropTableOrViewStmt stmt = analysis.getDropTableOrViewStmt();
      req.setDdl_type(stmt.isDropTable() ? TDdlType.DROP_TABLE : TDdlType.DROP_VIEW);
      req.setDrop_table_or_view_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isDropFunctionStmt()) {
      ddl.op_type = TCatalogOpType.DDL;
      TDdlExecRequest req = new TDdlExecRequest();
      req.setDdl_type(TDdlType.DROP_FUNCTION);
      DropFunctionStmt stmt = (DropFunctionStmt)analysis.getStmt();
      req.setDrop_fn_params(stmt.toThrift());
      ddl.setDdl_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isResetMetadataStmt()) {
      ddl.op_type = TCatalogOpType.RESET_METADATA;
      ResetMetadataStmt resetMetadataStmt = (ResetMetadataStmt) analysis.getStmt();
      TResetMetadataRequest req = resetMetadataStmt.toThrift();
      ddl.setReset_metadata_params(req);
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
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
   * Returns all function signatures that match the pattern. If pattern is null,
   * matches all functions.
   */
  public List<String> getFunctions(TFunctionType type, String dbName, String fnPattern)
      throws DatabaseNotFoundException {
    return impaladCatalog_.getFunctionSignatures(type, dbName, fnPattern);
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
   * Create a populated TExecRequest corresponding to the supplied TClientRequest.
   */
  public TExecRequest createExecRequest(
      TClientRequest request, StringBuilder explainString)
      throws AnalysisException, AuthorizationException, NotImplementedException,
      InternalException {
    AnalysisContext analysisCtxt = new AnalysisContext(impaladCatalog_,
        request.sessionState.database,
        new User(request.sessionState.user));
    AnalysisContext.AnalysisResult analysisResult = null;
    LOG.debug("analyze query " + request.stmt);
    analysisResult = analysisCtxt.analyze(request.stmt);

    Preconditions.checkNotNull(analysisResult.getStmt());

    TExecRequest result = new TExecRequest();
    result.setQuery_options(request.getQueryOptions());
    result.setAccess_events(analysisResult.getAccessEvents());

    if (analysisResult.isDdlStmt()) {
      result.stmt_type = TStmtType.DDL;
      createCatalogOpRequest(analysisResult, result);

      // All DDL operations except for CTAS are done with analysis at this point.
      if (!analysisResult.isCreateTableAsSelectStmt()) return result;
    } else if (analysisResult.isLoadDataStmt()) {
      result.stmt_type = TStmtType.LOAD;
      result.setResult_set_metadata(new TResultSetMetadata(Arrays.asList(
          new TColumnDesc("summary", TPrimitiveType.STRING))));
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

    // set fragment destinations
    for (int i = 1; i < fragments.size(); ++i) {
      PlanFragment dest = fragments.get(i).getDestFragment();
      Integer idx = fragmentIdx.get(dest);
      Preconditions.checkState(idx != null);
      queryExecRequest.addToDest_fragment_idx(idx.intValue());
    }

    // set scan ranges/locations for scan nodes
    LOG.debug("get scan range locations");
    for (ScanNode scanNode: scanNodes) {
      queryExecRequest.putToPer_node_scan_ranges(
          scanNode.getId().asInt(),
          scanNode.getScanRangeLocations(
            request.queryOptions.getMax_scan_range_length()));
    }

    // Compute resource requirements after scan range locations because the cost
    // estimates of scan nodes rely on them.
    planner.computeResourceReqs(fragments, true, request.queryOptions, queryExecRequest);

    // Use VERBOSE by default for all non-explain statements.
    TExplainLevel explainLevel = TExplainLevel.VERBOSE;
    if (request.queryOptions.isSetExplain_level()) {
      explainLevel = request.queryOptions.getExplain_level();
    } else if (analysisResult.isExplainStmt()) {
      // Use the NORMAL by default for explain statements.
      explainLevel = TExplainLevel.NORMAL;
    }

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

    // Global query parameters to be set in each TPlanExecRequest.
    queryExecRequest.query_globals = analysisResult.getAnalyzer().getQueryGlobals();

    if (analysisResult.isQueryStmt()) {
      // fill in the metadata
      LOG.debug("create result set metadata");
      result.stmt_type = TStmtType.QUERY;
      result.query_exec_request.stmt_type = result.stmt_type;
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
        finalizeParams.setTable_db(db == null ? request.sessionState.database : db);
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
        return MetadataOp.getColumns(impaladCatalog_, req.getCatalogName(),
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
