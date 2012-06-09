// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.thrift.TPlanExecRequest;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TQueryRequest;
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
   * Run parsing, semantic analysis and plan generation for the given query request and
   * return the TQueryExecRequest and optionally, an explain plan string.
   * TODO: make updates to nextQueryId thread-safe
   * @param request query request
   * @param explainString if not null, it will contain the explain plan string
   * @return the serialized form of a TQueryExecRequest based on thriftQueryRequest
   */
  private TQueryExecRequest generateExecRequest(TQueryRequest request,
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
    TQueryExecRequest result;
    result = planner.createPlanFragments(analysisResult, request.numNodes, explainString);
    return result;
  }

  private void assignQueryId(TQueryExecRequest request) {
    // Set query id
    UUID queryId = new UUID(nextQueryId++, 0);
    request.setQueryId(
        new TUniqueId(queryId.getMostSignificantBits(),
                      queryId.getLeastSignificantBits()));
    for (TPlanExecRequest planRequest: request.fragmentRequests) {
      planRequest.setQueryId(request.queryId);
    }
  }

  /**
   * Create a populated TQueryExecRequest corresponding to the supplied TQueryRequest.
   */
  public TQueryExecRequest createExecRequest(TQueryRequest request,
      StringBuilder explainString) throws ImpalaException {
    TQueryExecRequest execRequest = generateExecRequest(request, explainString);
    assignQueryId(execRequest);
    return execRequest;
  }

  /**
   * Parses and plans a query in order to generate its explain string. This method does
   * not increase the query id counter.
   */
  public String getExplainString(TQueryRequest request) throws ImpalaException {
    StringBuilder stringBuilder = new StringBuilder();
    generateExecRequest(request, stringBuilder);
    return stringBuilder.toString();
  }
}
