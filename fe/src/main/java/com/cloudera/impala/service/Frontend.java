// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.planner.DataSink;
import com.cloudera.impala.planner.PlanNode;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.thrift.TPlanExecRequest;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TQueryRequest;
import com.cloudera.impala.thrift.TUniqueId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

// Frontend API for the impalad process.
// This class allows the impala daemon to create TQueryExecRequest
// in response to TQueryRequests.
public class Frontend {
  private final static Logger LOG = LoggerFactory.getLogger(Frontend.class);
  private final Catalog catalog;
  private int nextQueryId;

  public Frontend() throws MetaException {
    HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf(Frontend.class));
    this.catalog = new Catalog(client);
    this.nextQueryId = 0;
  }

  /**
   * Create a TQueryExecRequest based on the provided TQueryRequest.
   * This call is thread-safe.
   * TODO: make updates to nextQueryId thread-safe
   */
  public TQueryExecRequest GetExecRequest(TQueryRequest request)
      throws ImpalaException {
    LOG.info("creating TQueryExecRequest for " + request.toString());

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
    List<PlanNode> planFragments = Lists.newArrayList();
    List<DataSink> dataSinks = Lists.newArrayList();
    TQueryExecRequest result;
    result = planner.createPlanFragments(
        analysisResult, request.numNodes, planFragments, dataSinks);

    UUID queryId = new UUID(nextQueryId++, 0);
    result.setQueryId(
        new TUniqueId(queryId.getMostSignificantBits(),
                      queryId.getLeastSignificantBits()));
    for (TPlanExecRequest planRequest: result.fragmentRequests) {
      planRequest.setQueryId(result.queryId);
    }

    // Generate explain string and print it.
    Preconditions.checkState(planFragments.size() == dataSinks.size());
    for (int i = 0; i < planFragments.size(); ++i) {
      if (i > 0) {
        System.out.println("----");
      }
      String explainStr = null;
      // First data sink may be null.
      if (dataSinks.get(i) == null) {
        Preconditions.checkState(i == 0);
        explainStr = planFragments.get(i).getExplainString();
      } else {
        explainStr = dataSinks.get(i).getExplainString()
            + planFragments.get(i).getExplainString();
      }
      LOG.info(explainStr);
    }

    LOG.info("returned exec request: " + request.toString());
    return result;
  }
}
