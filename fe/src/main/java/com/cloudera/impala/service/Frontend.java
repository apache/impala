// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.util.UUID;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.planner.DataSink;
import com.cloudera.impala.planner.PlanNode;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.thrift.TPlanExecRequest;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TQueryRequest;
import com.cloudera.impala.thrift.TUniqueId;
import com.google.common.base.Preconditions;

// Frontend API for the impalad process.
// This class allows the impala daemon to create TQueryExecRequest
// in response to TQueryRequests.
// TODO: make this thread-safe by making updates to nextQueryId and catalog
// thread-safe
public class Frontend {
  private final static Logger LOG = LoggerFactory.getLogger(Frontend.class);
  private final Catalog catalog;
  private int nextQueryId;

  public Frontend() throws MetaException {
    this.catalog = new Catalog();
    this.nextQueryId = 0;
  }

  /**
   * Create the serialized form of a TQueryExecRequest based on thriftQueryRequest,
   * a serialized TQueryRequest.
   * This call is thread-safe.
   * TODO: make updates to nextQueryId thread-safe
   */
  public byte[] GetExecRequest(byte[] thriftQueryRequest) throws ImpalaException {
    // TODO: avoid creating factory and deserializer for each query?
    TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
    TDeserializer deserializer = new TDeserializer(protocolFactory);

    TQueryRequest request = new TQueryRequest();
    try {
      deserializer.deserialize(request, thriftQueryRequest);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
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
    StringBuilder explainString = new StringBuilder();
    TQueryExecRequest result;
    result = planner.createPlanFragments(
        analysisResult, request.numNodes, explainString);

    UUID queryId = new UUID(nextQueryId++, 0);
    result.setQueryId(
        new TUniqueId(queryId.getMostSignificantBits(),
                      queryId.getLeastSignificantBits()));
    for (TPlanExecRequest planRequest: result.fragmentRequests) {
      planRequest.setQueryId(result.queryId);
    }

    // Print explain string.
    LOG.info(explainString.toString());

    LOG.info("returned exec request: " + result.toString());
    // TODO: avoid creating serializer for each query?
    TSerializer serializer = new TSerializer(protocolFactory);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }
}
