// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.planner.PlanNode;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.thrift.TExecutePlanRequest;
import com.cloudera.impala.thrift.TPrimitiveType;
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
    for (Expr expr: analysisResult.selectStmt.getSelectListExprs()) {
      colTypes.add(expr.getType());
    }

    // create plan
    Planner planner = new Planner();
    PlanNode plan = planner.createPlan(analysisResult.selectStmt, analysisResult.analyzer);

    // execute locally
    List<TPrimitiveType> selectListTypes = Lists.newArrayList();
    List<Expr> selectListExprs = analysisResult.selectStmt.getSelectListExprs();
    for (Expr expr: selectListExprs) {
      selectListTypes.add(expr.getType().toThrift());
    }
    TExecutePlanRequest execRequest = new TExecutePlanRequest(plan.treeToThrift(),
        analysisResult.analyzer.getDescTbl().toThrift(),
        Expr.treesToThrift(selectListExprs), selectListTypes);
    NativePlanExecutor.ExecPlan(
        thriftToByteArray(execRequest), request.returnAsAscii, resultQueue);
  }

  byte[] thriftToByteArray(Object thriftMsg) {
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    try {
      ObjectOutputStream o = new ObjectOutputStream(s);
      o.writeObject(thriftMsg);
      o.close();
    } catch (IOException e) {
      // TODO: print object debug string
      throw new RuntimeException("couldn't serialize object: " + e.getMessage());
    }
    return s.toByteArray();
  }

}
