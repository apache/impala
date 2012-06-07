// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.testutil;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.service.Frontend;
import com.cloudera.impala.thrift.ImpalaPlanService;
import com.cloudera.impala.thrift.TImpalaPlanServiceException;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TQueryRequest;
import com.cloudera.impala.thrift.TQueryRequestResult;
import com.google.common.collect.Sets;

/**
 * Service to construct a TPlanExecRequest for a given query string.
 * We're implementing that as a stand-alone service, rather than having
 * the backend call the corresponding Coordinator function, because
 * the process would crash somewhere during metastore setup when running
 * under gdb.
 */
public class PlanService {
  // Note: query-executor.cc has a hardcoded port of 20000, be cautious if overriding this
  // value
  public static final int DEFAULT_PORT = 20000;
  final PlanServiceHandler handler;
  final TServer server;

  private static class PlanServiceHandler implements ImpalaPlanService.Iface {
    private static final Logger LOG = Logger.getLogger(PlanService.class);
    private final Frontend frontend;

    public PlanServiceHandler(boolean lazy) {
      frontend = new Frontend(lazy);
    }

    public TQueryRequestResult GetQueryRequestResult(String stmt, int numNodes)
        throws TImpalaPlanServiceException {
      LOG.info(
          "Executing '" + stmt + "' for " + Integer.toString(numNodes) + " nodes");
      TQueryRequest tRequest = new TQueryRequest(stmt, false, numNodes);
      StringBuilder explainStringBuilder = new StringBuilder();
      TQueryRequestResult result;
      try {
        result = frontend.createQueryExecRequest(tRequest, explainStringBuilder);
      } catch (ImpalaException e) {
        LOG.warn("Error creating query request result", e);
        throw new TImpalaPlanServiceException(e.getMessage());
      }

      result.queryExecRequest.setAsAscii(false);
      result.queryExecRequest.setAbortOnError(false);
      result.queryExecRequest.setMaxErrors(100);
      result.queryExecRequest.setBatchSize(0);

      // Print explain string.
      LOG.info(explainStringBuilder.toString());

      LOG.info("returned TQueryRequestResult: " + result.toString());
      return result;
    }

    public void ShutdownServer() {
      frontend.close();
      System.exit(0);
    }

    /**
     * Loads an updated catalog from the metastore. Is thread-safe.
     */
    @Override
    public void RefreshMetadata() {
      frontend.resetCatalog();
    }

    @Override
    public String GetExplainString(String query, int numNodes)
        throws TImpalaPlanServiceException {
      try {
        return frontend.getExplainString(new TQueryRequest(query, false, numNodes));
      } catch (ImpalaException e) {
        LOG.warn("Error getting explain string", e);
        throw new TImpalaPlanServiceException(e.getMessage());
      }
    }
  }

  public PlanService(int port, boolean lazy) throws TTransportException {
    handler = new PlanServiceHandler(lazy);
    ImpalaPlanService.Processor proc = new ImpalaPlanService.Processor(handler);
    TServerTransport transport = new TServerSocket(port);
    server =
      new TThreadPoolServer(new TThreadPoolServer.Args(transport).processor(proc));
  }

  /**
   * If lazy is true, load the catalog lazily rather than eagerly at start-up
   */
  public PlanService(boolean lazy) throws TTransportException, MetaException {
    this(DEFAULT_PORT, lazy);
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

  private static final String NON_LAZY_ARG = "-nonlazy";

  public static void main(String[] args) throws TTransportException, MetaException {
    Set<String> argSet = Sets.newHashSet(args);
    PlanService service = new PlanService(!argSet.contains(NON_LAZY_ARG));
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
