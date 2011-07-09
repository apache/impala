// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.planner.PlanNode;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TExecutePlanRequest;
import com.cloudera.impala.thrift.TQueryRequest;
import com.cloudera.impala.thrift.TResultRow;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class Coordinator {
  private final Catalog catalog;

  // for async execution
  private Thread execThread;
  private String errorMsg;

  public Coordinator(Catalog catalog) {
    this.catalog = catalog;
    init();
  }

  private void init() {
    this.execThread = null;
    this.errorMsg = null;
  }

  // Run the query synchronously (ie, this function blocks until the query
  // is finished) and place the results in resultQueue, followed by an empty
  // row that acts as an end-of-stream marker.
  // Also populates 'colTypes'.
  public void runQuery(TQueryRequest request,
                       List<PrimitiveType> colTypes,
                       List<String> colNames,
                       BlockingQueue<TResultRow> resultQueue) throws ImpalaException {
    init();
    AnalysisContext.AnalysisResult analysisResult = analyzeQuery(request, colTypes, colNames);
    execQuery(analysisResult, request.returnAsAscii, resultQueue);
    addSentinelRow(resultQueue);
  }

  // Run the query asynchronously, returning immediately after starting
  // query execution and populating 'colTypes'. Places an empty row as
  // a marker at the end of the queue once all result rows have been added to the
  // queue or an error occured.
  // getErrorMsg() will return a non-empty string if an error occured, otherwise
  // null.
  public void asyncRunQuery(
      final TQueryRequest request, List<PrimitiveType> colTypes, List<String> colNames,
      final BlockingQueue<TResultRow> resultQueue) throws ImpalaException {
    init();
    final AnalysisContext.AnalysisResult analysisResult = analyzeQuery(request, colTypes, colNames);
    Runnable execCall = new Runnable() {
      public void run() {
        try {
          execQuery(analysisResult, request.returnAsAscii, resultQueue);
        } catch (ImpalaException e) {
          errorMsg = e.getMessage();
        }
        addSentinelRow(resultQueue);
      }
    };
    execThread = new Thread(execCall);
    execThread.start();
  }

  // When executing asynchronously, this waits for execution to finish and returns
  // an error message if an error occured, otherwise null.
  public String getErrorMsg() {
    if (execThread != null) {
      try {
        execThread.join();
      } catch (InterruptedException e) {
        assert false : "unexpected interrupt: execThread.join()";
      }
      return errorMsg;
    } else {
      return null;
    }
  }

  private void addSentinelRow(BlockingQueue<TResultRow> resultQueue) {
    try {
      resultQueue.put(new TResultRow());
    } catch (InterruptedException e) {
      // we don't expect to get interrupted
      assert false : "unexpected blockingqueueinterrupt";
    }
  }

  // Analyze query and return analysis result and types of select list exprs.
  private AnalysisContext.AnalysisResult analyzeQuery(
      TQueryRequest request, List<PrimitiveType> colTypes, List<String> colNames) throws ImpalaException {
    AnalysisContext analysisCtxt = new AnalysisContext(catalog);
    AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(request.stmt);
    Preconditions.checkNotNull(analysisResult.selectStmt);

    // TODO: handle EXPLAIN SELECT

    // populate colTypes and colNames
    colTypes.clear();
    for (Expr expr : analysisResult.selectStmt.getSelectListExprs()) {
      colTypes.add(expr.getType());
    }

    // populate colnames
    for (String s : analysisResult.selectStmt.getColLabels()) {
      colNames.add(s);
    }

    return analysisResult;
  }

  // Execute query contained in 'analysisResult' and return result in
  // 'resultQueue'. If 'returnAsAscii' is true, returns results as printable
  // strings.
  private void execQuery(
      AnalysisContext.AnalysisResult analysisResult, boolean returnAsAscii,
      BlockingQueue<TResultRow> resultQueue) throws ImpalaException {
    // create plan
    Planner planner = new Planner();
    PlanNode plan = planner.createPlan(analysisResult.selectStmt, analysisResult.analyzer);
    // execute locally
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    TExecutePlanRequest execRequest = new TExecutePlanRequest(
        plan.treeToThrift(),
        analysisResult.analyzer.getDescTbl().toThrift(),
        Expr.treesToThrift(analysisResult.selectStmt.getSelectListExprs()));
    try {
      NativePlanExecutor.ExecPlan(
          serializer.serialize(execRequest), returnAsAscii, resultQueue);
    } catch (TException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static byte[] getThriftPlan(String queryStr)
      throws MetaException, NotImplementedException, AnalysisException, TException {
    HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf(Coordinator.class));
    Catalog catalog = new Catalog(client);
    AnalysisContext analysisCtxt = new AnalysisContext(catalog);
    AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(queryStr);
    // create plan
    Planner planner = new Planner();
    PlanNode plan = planner.createPlan(analysisResult.selectStmt, analysisResult.analyzer);
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    TExecutePlanRequest execRequest = new TExecutePlanRequest(
        plan.treeToThrift(),
        analysisResult.analyzer.getDescTbl().toThrift(),
        Expr.treesToThrift(analysisResult.selectStmt.getSelectListExprs()));
    return serializer.serialize(execRequest);
  }

  // Run single query against test instance
  public static void main(String args[]) {
    if (args.length != 1) {
      for (int i = 0; i < args.length; ++i) {
        System.err.println(args[i]);
      }
      System.err.println("coordinator \"query string\"");
      System.exit(1);
    }
    HiveMetaStoreClient client = null;
    try {
      client = new HiveMetaStoreClient(new HiveConf(Coordinator.class));
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.exit(2);
    }
    Catalog catalog = new Catalog(client);

    System.out.println("Running query '" + args[0] + "'");
    int numRows = 0;
    TQueryRequest request = new TQueryRequest(args[0], true);
    List<PrimitiveType> colTypes = Lists.newArrayList();
    List<String> colNames = Lists.newArrayList();
    BlockingQueue<TResultRow> resultQueue = new LinkedBlockingQueue<TResultRow>();
    Coordinator coordinator = new Coordinator(catalog);
    try {
      coordinator.asyncRunQuery(request, colTypes, colNames, resultQueue);
    } catch (ImpalaException e) {
      System.err.println(e.getMessage());
      System.exit(2);
    }
    while (true) {
      TResultRow resultRow = null;
      try {
        resultRow = resultQueue.take();
      } catch (InterruptedException e) {
        assert false : "unexpected interrupt";
      }
      if (resultRow.colVals == null) {
        break;
      }
      ++numRows;
      StringBuilder output = new StringBuilder();
      for (TColumnValue val: resultRow.colVals) {
        output.append('\t');
        output.append(val.stringVal);
      }
      System.out.println(output.toString());
    }
    String errorMsg = coordinator.getErrorMsg();
    if (errorMsg != null) {
      System.err.println("encountered error:\n" + errorMsg);
    } else {
      System.out.println("TOTAL ROWS: " + numRows);
    }
  }
}
