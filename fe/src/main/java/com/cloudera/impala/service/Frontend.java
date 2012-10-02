// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.InsertStmt;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.planner.NewPlanner;
import com.cloudera.impala.planner.PlanFragment;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.planner.ScanNode;
import com.cloudera.impala.thrift.TCatalogUpdate;
import com.cloudera.impala.thrift.TClientRequest;
import com.cloudera.impala.thrift.TColumnDesc;
import com.cloudera.impala.thrift.TDdlExecRequest;
import com.cloudera.impala.thrift.TDdlType;
import com.cloudera.impala.thrift.TExecRequest;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TFinalizeParams;
import com.cloudera.impala.thrift.TPlanExecParams;
import com.cloudera.impala.thrift.TPlanFragment;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TQueryExecRequest2;
import com.cloudera.impala.thrift.TQueryGlobals;
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
  private Catalog catalog;
  final boolean lazyCatalog;

  // For generating a string of the current time.
  private final SimpleDateFormat formatter =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

  public Frontend() {
    // Default to eager loading
    this(false);
  }

  public Frontend(boolean lazy) {
    this.catalog = new Catalog(lazy);
    this.lazyCatalog = lazy;
  }

  /**
   * Invalidates catalog metadata, forcing a reload.
   */
  public void resetCatalog() {
    this.catalog.close();
    this.catalog = new Catalog(lazyCatalog);
  }


  public void close() {
    this.catalog.close();
  }

  /**
   * Assigns request id and id of the coordinator fragment, which is set to be the same
   * as the request id. Fragment ids need to be globally unique across all plan fragment
   * executions, and therefore need to be assigned by the coordinator when initiating
   * fragment execution.
   * Also sets TPlanExecParams.dest_fragment_id.
   */
  private void assignIds(TExecRequest request) {
    UUID requestId = UUID.randomUUID();
    request.setRequest_id(
        new TUniqueId(requestId.getMostSignificantBits(),
                      requestId.getLeastSignificantBits()));

    if (request.getQueryExecRequest() != null) {
      TQueryExecRequest queryExecRequest = request.getQueryExecRequest();
      queryExecRequest.setQuery_id(request.getRequest_id());
      for (int fragmentNum = 0; fragmentNum < queryExecRequest.fragment_requests.size();
           ++fragmentNum) {
        // we only need to assign the coordinator fragment's id (= query id), the
        // rest is done by the coordinator as it issues fragment execution rpcs,
        // but fragment_id is required, so we give it a dummy value
        queryExecRequest.fragment_requests.get(fragmentNum).
            setFragment_id(request.request_id);
        queryExecRequest.fragment_requests.get(fragmentNum).
            setQuery_id(request.request_id);
      }

      if (queryExecRequest.node_request_params != null &&
          queryExecRequest.node_request_params.size() == 2) {
        Preconditions.checkState(queryExecRequest.has_coordinator_fragment);
        // we only have two fragments (1st one: coord); the destination
        // of the 2nd fragment is the coordinator fragment
        TUniqueId coordFragmentId = queryExecRequest.fragment_requests.get(0).fragment_id;
        for (TPlanExecParams execParams: queryExecRequest.node_request_params.get(1)) {
          execParams.setDest_fragment_id(coordFragmentId);
        }
      }
    }
  }

  /**
   * Create a populated TExecRequest corresponding to the supplied
   * TClientRequest.
   * @param request query request
   * @param explainString if not null, it will contain the explain plan string
   * @return a TExecRequest based on request
   */
  public TExecRequest createExecRequest(TClientRequest request,
      StringBuilder explainString) throws ImpalaException {
    AnalysisContext analysisCtxt = 
        new AnalysisContext(catalog, request.sessionState.database);

    AnalysisContext.AnalysisResult analysisResult = null;
    LOG.info("createExecRequest for query " + request.stmt);
    try {
      analysisResult = analysisCtxt.analyze(request.stmt);
    } catch (AnalysisException e) {
      LOG.info(e.getMessage());
      throw e;
    }
    Preconditions.checkNotNull(analysisResult.getStmt());

    LOG.info("desc tbl:\n" + analysisResult.getAnalyzer().getDescTbl().debugString());

    TExecRequest result = new TExecRequest();

    if (analysisResult.isDdlStmt()) {
      result.stmt_type = TStmtType.DDL;
      createDdlExecRequest(analysisResult, result);
    } else {
      // create plan
      Planner planner = new Planner();
      result.setQueryExecRequest(
          planner.createPlanFragments(analysisResult, request.queryOptions,
                                      explainString));
      result.queryExecRequest.sql_stmt = request.stmt;

      // fill the metadata (for query statement)
      if (analysisResult.isQueryStmt()) {
        TResultSetMetadata metadata = new TResultSetMetadata();
        QueryStmt queryStmt = analysisResult.getQueryStmt();
        int colCnt = queryStmt.getColLabels().size();
        for (int i = 0; i < colCnt; ++i) {
          TColumnDesc colDesc = new TColumnDesc();
          colDesc.columnName = queryStmt.getColLabels().get(i);
          colDesc.columnType = queryStmt.getResultExprs().get(i).getType().toThrift();
          metadata.addToColumnDescs(colDesc);
        }
        result.resultSetMetadata = metadata;
        result.stmt_type = TStmtType.QUERY;
      } else {
        Preconditions.checkState(analysisResult.isDmlStmt());
        result.stmt_type = TStmtType.DML;
      }
    }

    result.setQuery_options(request.getQueryOptions());
    assignIds(result);
    return result;
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
      ddl.setDatabase(analysis.getUseStmt().getDatabase());
      metadata.setColumnDescs(Collections.<TColumnDesc>emptyList());
    } else if (analysis.isShowStmt()) {
      ddl.ddl_type = TDdlType.SHOW;
      ddl.setShow_pattern(analysis.getShowStmt().getPattern());
      ddl.setDatabase(analysis.getShowStmt().getDb());
      metadata.setColumnDescs(Arrays.asList(
          new TColumnDesc("name", TPrimitiveType.STRING)));
    } else if (analysis.isDescribeStmt()) {
      ddl.ddl_type = TDdlType.DESCRIBE;
      ddl.setDescribe_table(analysis.getDescribeStmt().getTable().getTbl());
      ddl.setDatabase(analysis.getDescribeStmt().getTable().getDb());
      metadata.setColumnDescs(Arrays.asList(
          new TColumnDesc("name", TPrimitiveType.STRING),
          new TColumnDesc("type", TPrimitiveType.STRING)));
    }

    result.setResultSetMetadata(metadata);
    result.setDdlExecRequest(ddl);
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
   * Returns a list of column descriptors describing a the columns in the
   * specified table. Throws an AnalysisException if the table or db is not
   * found.
   */
  public List<TColumnDesc> describeTable(String dbName, String tableName)
      throws ImpalaException {
    Db db = catalog.getDb(dbName);
    if (db == null) {
      throw new AnalysisException("Unknown database: " + dbName);
    }

    Table table = db.getTable(tableName);
    if (table == null) {
      throw new AnalysisException("Unknown table: " + db.getName() + "." + tableName);
    }

    List<TColumnDesc> columns = Lists.newArrayList();
    for (Column column: table.getColumnsInHiveOrder()) {
      TColumnDesc columnDesc = new TColumnDesc();
      columnDesc.setColumnName(column.getName());
      columnDesc.setColumnType(column.getType().toThrift());
      columns.add(columnDesc);
    }

    return columns;
  }

  /**
   * new planner interface:
   * Create a populated TExecRequest corresponding to the supplied
   * TClientRequest.
   */
  public TExecRequest createExecRequest2(
      TClientRequest request, StringBuilder explainString)
      throws InternalException, AnalysisException, NotImplementedException {
    AnalysisContext analysisCtxt = new AnalysisContext(catalog);
    AnalysisContext.AnalysisResult analysisResult = null;
    LOG.info("createExecRequest for query " + request.stmt);
    try {
      analysisResult = analysisCtxt.analyze(request.stmt);
    } catch (AnalysisException e) {
      LOG.info(e.getMessage());
      throw e;
    }
    Preconditions.checkNotNull(analysisResult.getStmt());

    TExecRequest result = new TExecRequest();
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

    // create TQueryExecRequest2
    Preconditions.checkState(
        analysisResult.isQueryStmt() || analysisResult.isDmlStmt());
    TQueryExecRequest2 queryExecRequest = new TQueryExecRequest2();
    result.setQueryExecRequest2(queryExecRequest);

    // create plan
    NewPlanner planner = new NewPlanner();
    ArrayList<PlanFragment> fragments =
        planner.createPlanFragments(analysisResult, request.queryOptions);
    List<ScanNode> scanNodes = Lists.newArrayList();
    // map from fragment to its index in queryExecRequest.fragments; needed for
    // queryExecRequest.dest_fragment_idx
    Map<PlanFragment, Integer> fragmentIdx = Maps.newHashMap();
    for (PlanFragment fragment: fragments) {
      TPlanFragment thriftFragment = fragment.toThrift();
      queryExecRequest.addToFragments(thriftFragment);
      fragment.getPlanRoot().collectSubclasses(ScanNode.class, scanNodes);
      fragmentIdx.put(fragment, queryExecRequest.fragments.size() - 1);
    }
    explainString.append(planner.getExplainString(fragments, TExplainLevel.VERBOSE));
    LOG.info("desc tbl:\n" + analysisResult.getAnalyzer().getDescTbl().debugString());
    queryExecRequest.desc_tbl = analysisResult.getAnalyzer().getDescTbl().toThrift();

    // set fragment destinations
    for (int i = 1; i < fragments.size(); ++i) {
      PlanFragment dest = fragments.get(i).getDestFragment();
      Integer idx = fragmentIdx.get(dest);
      Preconditions.checkState(idx != null);
      queryExecRequest.addToDest_fragment_idx(idx.intValue());
    }

    // set scan ranges/locations for scan nodes
    for (ScanNode scanNode: scanNodes) {
      queryExecRequest.putToPer_node_scan_ranges(
          scanNode.getId().asInt(),
          scanNode.getScanRangeLocations(request.queryOptions.getMax_scan_range_length()));
    }

    // Global query parameters to be set in each TPlanExecRequest.
    queryExecRequest.query_globals = createQueryGlobals();

    if (analysisResult.isQueryStmt()) {
      // fill in the metadata
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
      result.resultSetMetadata = metadata;
    } else {
      Preconditions.checkState(analysisResult.isInsertStmt());
      result.stmt_type = TStmtType.DML;
      // create finalization params of insert stmt
      InsertStmt insertStmt = analysisResult.getInsertStmt();
      TFinalizeParams finalizeParams = new TFinalizeParams();
      finalizeParams.setIs_overwrite(insertStmt.isOverwrite());
      finalizeParams.setTable_name(insertStmt.getTargetTableName().getTbl());
      String db = insertStmt.getTargetTableName().getDb();
      // TODO: fix up after Henry's session cl goes in
      finalizeParams.setTable_db(db == null ? "" : db);
      finalizeParams.setHdfs_base_dir(
        ((HdfsTable)insertStmt.getTargetTable()).getHdfsBaseDir());
      queryExecRequest.setFinalize_params(finalizeParams);
    }

    return result;
  }

  /**
   * Create query global parameters to be set in each TPlanExecRequest.
   */
  private TQueryGlobals createQueryGlobals() {
    TQueryGlobals queryGlobals = new TQueryGlobals();
    Calendar currentDate = Calendar.getInstance();
    String nowStr = formatter.format(currentDate.getTime());
    queryGlobals.setNow_string(nowStr);
    return queryGlobals;
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
    HiveMetaStoreClient msClient = catalog.getMetaStoreClient();
    if (table.getNumClusteringCols() > 0) {
      // Add all partitions to metastore.
      for (String partName: update.getCreated_partitions()) {
        try {
          LOG.info("Creating partition: " + partName + " in table: " + tblName);
          msClient.appendPartitionByName(dbName, tblName, partName);
        } catch (AlreadyExistsException e) {
          LOG.info("Ignoring partition " + partName + ", since it already exists");
          // Ignore since partition already exists.
        } catch (Exception e) {
          throw new InternalException("Error updating metastore", e);
        }
      }
    }
  }
}
