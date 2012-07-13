// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.util.UUID;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.thrift.TCatalogUpdate;
import com.cloudera.impala.thrift.TColumnDesc;
import com.cloudera.impala.thrift.TCreateQueryExecRequestResult;
import com.cloudera.impala.thrift.TPlanExecParams;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TQueryRequest;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.cloudera.impala.thrift.TUniqueId;
import com.google.common.base.Preconditions;

/**
 * Frontend API for the impalad process.
 * This class allows the impala daemon to create TQueryExecRequest
 * in response to TQueryRequests.
 * TODO: make this thread-safe by making updates to nextQueryId and catalog thread-safe
 */
public class Frontend {
  private final static Logger LOG = LoggerFactory.getLogger(Frontend.class);
  private Catalog catalog;
  private int nextQueryId;
  final boolean lazyCatalog;

  public Frontend() {
    // Default to eager loading
    this(false);
  }

  public Frontend(boolean lazy) {
    this.catalog = new Catalog(lazy);
    this.nextQueryId = 0;
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
   * Assigns query id and id of the coordinator fragment, which is set to be the same
   * as the query id. Fragment ids need to be globally unique across all plan fragment
   * executions, and therefore need to be assigned by the coordinator when initiating
   * fragment execution.
   * Also sets TPlanExecParams.dest_fragment_id.
   */
  private void assignIds(TQueryExecRequest request) {
    UUID queryId = new UUID(nextQueryId++, 0);
    request.setQuery_id(
        new TUniqueId(queryId.getMostSignificantBits(),
                      queryId.getLeastSignificantBits()));

    for (int fragmentNum = 0; fragmentNum < request.fragment_requests.size();
         ++fragmentNum) {
      // we only need to assign the coordinator fragment's id (= query id), the
      // rest is done by the coordinator as it issues fragment execution rpcs,
      // but fragment_id is required, so we give it a dummy value
      request.fragment_requests.get(fragmentNum).setFragment_id(request.query_id);
      request.fragment_requests.get(fragmentNum).setQuery_id(request.query_id);
    }

    if (request.node_request_params != null && request.node_request_params.size() == 2) {
      // we only have two fragments (1st one: coord); the destination
      // of the 2nd fragment is the coordinator fragment
      TUniqueId coordFragmentId = request.fragment_requests.get(0).fragment_id;
      for (TPlanExecParams execParams: request.node_request_params.get(1)) {
        execParams.setDest_fragment_id(coordFragmentId);
      }
    }
  }

  /**
   * Create a populated TCreateQueryExecRequestResult corresponding to the supplied
   * TQueryRequest.
   * @param request query request
   * @param explainString if not null, it will contain the explain plan string
   * @return a TCreateQueryExecRequestResult based on request
   * TODO: make updates to nextQueryId thread-safe
   */
  public TCreateQueryExecRequestResult createQueryExecRequest(TQueryRequest request,
      StringBuilder explainString) throws ImpalaException {
    AnalysisContext analysisCtxt = new AnalysisContext(catalog);
    AnalysisContext.AnalysisResult analysisResult = null;
    try {
      analysisResult = analysisCtxt.analyze(request.stmt);
    } catch (AnalysisException e) {
      LOG.info(e.getMessage());
      throw e;
    }
    Preconditions.checkNotNull(analysisResult.getStmt());

    // create plan
    Planner planner = new Planner();
    TCreateQueryExecRequestResult result = new TCreateQueryExecRequestResult();
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
    }

    assignIds(result.queryExecRequest);
    return result;
  }

  /**
   * Parses and plans a query in order to generate its explain string. This method does
   * not increase the query id counter.
   */
  public String getExplainString(TQueryRequest request) throws ImpalaException {
    StringBuilder stringBuilder = new StringBuilder();
    createQueryExecRequest(request, stringBuilder);
    return stringBuilder.toString();
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
      for (String partName : update.getCreated_partitions()) {
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
