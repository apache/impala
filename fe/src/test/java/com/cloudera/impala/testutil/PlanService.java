// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.testutil;

import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.thrift.ImpalaPlanService;
import com.cloudera.impala.thrift.TPlanExecRequest;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TUniqueId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Service to construct a TPlanExecRequest for a given query string.
 * We're implementing that as a stand-alone service, rather than having
 * the backend call the corresponding Coordinator function, because
 * the process would crash somewhere during metastore setup when running
 * under gdb.
 */
public class PlanService {
  private static class PlanServiceHandler implements ImpalaPlanService.Iface {
    private final AtomicReference<Catalog> catalog = new AtomicReference<Catalog>();
    private int nextQueryId;

    public PlanServiceHandler(Catalog catalog) {
      Preconditions.checkNotNull(catalog);
      setCatalog(catalog);
      this.nextQueryId = 0;
    }

    protected void setCatalog(Catalog catalog) {
      this.catalog.set(catalog);
    }

    public TQueryExecRequest GetExecRequest(String stmt, int numNodes) throws TException {
      System.out.println(
          "Executing '" + stmt + "' for " + Integer.toString(numNodes) + " nodes");
      AnalysisContext analysisCtxt = new AnalysisContext(catalog.get());
      AnalysisContext.AnalysisResult analysisResult = null;
      try {
        analysisResult = analysisCtxt.analyze(stmt);
      } catch (Exception e) {
        System.out.println(e.getMessage());
        throw new TException(e.getMessage());
      }
      Preconditions.checkNotNull(analysisResult.getStmt());

      // populate colTypes
      List<PrimitiveType> colTypes = Lists.newArrayList();
      if (analysisResult.isSelectStmt()) {
        SelectStmt selectStmt = analysisResult.getSelectStmt();
        for (Expr expr : selectStmt.getSelectListExprs()) {
          colTypes.add(expr.getType());
        }
      }

      // create plan
      Planner planner = new Planner();
      StringBuilder explainString = new StringBuilder();

      TQueryExecRequest request;
      try {
        request = planner.createPlanFragments(analysisResult, numNodes, explainString);
      } catch (NotImplementedException e) {
        throw new TException(e.getMessage());
      } catch (InternalException e) {
        throw new TException(e.getMessage());
      } catch (Exception e) {
        System.out.println(e.getMessage());
        throw new TException(e.getMessage());
      }

      UUID queryId = new UUID(nextQueryId++, 0);
      request.setQueryId(
          new TUniqueId(queryId.getMostSignificantBits(),
                        queryId.getLeastSignificantBits()));
      request.setAsAscii(false);
      request.setAbortOnError(false);
      request.setMaxErrors(100);
      request.setBatchSize(0);

      for (TPlanExecRequest planRequest: request.fragmentRequests) {
        planRequest.setQueryId(request.queryId);
      }

      // Print explain string.
      System.out.println(explainString.toString());

      System.out.println("returned exec request: " + request.toString());
      return request;
    }

    public void ShutdownServer() {
      System.exit(0);
    }

    /**
     * Loads an updated catalog from the metastore. Is thread-safe.
     */
    @Override
    public void RefreshMetadata() throws TException {
      setCatalog(new Catalog());
    }
  }

  // Note: query-executor.cc has a hardcoded port of 20000, be cautious if overriding this
  // value
  public static final int DEFAULT_PORT = 20000;
  final PlanServiceHandler handler;
  final TServer server;

  public PlanService(Catalog catalog, int port) throws TTransportException {
    handler = new PlanServiceHandler(catalog);
    ImpalaPlanService.Processor proc = new ImpalaPlanService.Processor(handler);
    TServerTransport transport = new TServerSocket(port);
    server =
      new TThreadPoolServer(new TThreadPoolServer.Args(transport).processor(proc));
  }

  public PlanService() throws TTransportException, MetaException {
    this(new Catalog(), DEFAULT_PORT);
  }

  /**
   * Waits for some length of time for the plan server to come up, and returns true if it
   * does, and false if not.
   */
  public boolean waitForServer(long timeoutMs) {
    long start = System.currentTimeMillis();
    while (!server.isServing()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {

      }
      long now = System.currentTimeMillis();
      if (now - start > timeoutMs) {
        return false;
      }
    }
    return true;
  }

  public void serve() throws MetaException, TTransportException {
    server.serve();
  }

  public void shutdown() {
    server.stop();
  }

  public static void main(String[] args) throws TTransportException, MetaException {
    PlanService service = new PlanService();
    try {
      service.serve();
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.exit(2);
    }

    Properties prop = System.getProperties();
    Enumeration<Object> keys = prop.keys();
    while (keys.hasMoreElements()) {
      String key = (String)keys.nextElement();
      String value = (String)prop.get(key);
      System.out.println(key + ": " + value);
    }
  }

}
