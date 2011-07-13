// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.ImpalaException;
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
    TQueryRequest request = new TQueryRequest(args[0], true);
    List<PrimitiveType> colTypes = Lists.newArrayList();
    BlockingQueue<TResultRow> resultQueue = new LinkedBlockingQueue<TResultRow>();
    try {
      coordinator.RunQuery(request, colTypes, resultQueue);
      while (true) {
        TResultRow resultRow = resultQueue.poll();
        if (resultRow == null) {
          break;
        }

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
  }

}
