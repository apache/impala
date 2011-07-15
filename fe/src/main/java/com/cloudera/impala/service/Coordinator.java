// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

  public Coordinator(Catalog catalog) {
    this.catalog = catalog;
  }

  // Run the query synchronously (ie, this function blocks until the query
  // is finished) and place the results in resultQueue.
  // Also populates 'colTypes' before actually running the query.
  public void RunQuery(TQueryRequest request,
                       List<PrimitiveType> colTypes,
                       BlockingQueue<TResultRow> resultQueue) throws ImpalaException {
    AnalysisContext analysisCtxt = new AnalysisContext(catalog);
    AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(request.stmt);
    Preconditions.checkNotNull(analysisResult.selectStmt);

    // TODO: handle EXPLAIN SELECT

    // populate colTypes
    colTypes.clear();
    for (Expr expr : analysisResult.selectStmt.getSelectListExprs()) {
      colTypes.add(expr.getType());
    }

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
          serializer.serialize(execRequest), request.returnAsAscii, resultQueue);
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
    Coordinator coordinator = new Coordinator(catalog);

    System.out.println("Running query '" + args[0] + "'");
    int numRows = 0;
    TQueryRequest request = new TQueryRequest(args[0], true);
    List<PrimitiveType> colTypes = Lists.newArrayList();
    BlockingQueue<TResultRow> resultQueue = new LinkedBlockingQueue<TResultRow>();
    try {
      coordinator.RunQuery(request, colTypes, resultQueue);
      while (true) {
        TResultRow resultRow = null;
        try {
          // We use a timeout here, because it is conceivable that the c++ backend
          // has not written anything to the queue yet by the time we reach this piece of code.
          // In that case we would report an empty query result.
          // By adding a timeout we poll the queue until the timeout is reached.
          resultRow = resultQueue.poll(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (resultRow == null) {
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
    } catch (ImpalaException e) {
      System.err.println(e.getMessage());
      System.exit(2);
    }
    System.out.println("TOTAL ROWS: " + numRows);
  }
}
