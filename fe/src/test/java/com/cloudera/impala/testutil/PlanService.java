// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.testutil;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.planner.PlanNode;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.thrift.ImpalaPlanService;
import com.cloudera.impala.thrift.TAnalysisException;
import com.cloudera.impala.thrift.TExecutePlanRequest;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

// Service to construct a TPlanExecRequest for a given query string.
// We're implementing that as a stand-alone service, rather than having
// the backend call the corresponding Coordinator function, because
// the process would crash somewhere during metastore setup when running
// under gdb.
public class PlanService {
  public static class PlanServiceHandler implements ImpalaPlanService.Iface {
    private final Catalog catalog;

    public PlanServiceHandler(Catalog catalog) {
      this.catalog = catalog;
    }

    public TExecutePlanRequest GetExecRequest(String stmt) throws TAnalysisException {
      System.out.println("Executing '" + stmt + "'");
      AnalysisContext analysisCtxt = new AnalysisContext(catalog);
      AnalysisContext.AnalysisResult analysisResult = null;
      try {
        analysisResult = analysisCtxt.analyze(stmt);
      } catch (AnalysisException e) {
        System.out.println(e.getMessage());
        throw new TAnalysisException(e.getMessage());
      }
      Preconditions.checkNotNull(analysisResult.selectStmt);

      // populate colTypes
      List<PrimitiveType> colTypes = Lists.newArrayList();
      for (Expr expr : analysisResult.selectStmt.getSelectListExprs()) {
        colTypes.add(expr.getType());
      }

      // create plan
      Planner planner = new Planner();

      PlanNode plan = null;
      try {
        plan = planner.createPlan(analysisResult.selectStmt, analysisResult.analyzer);
      } catch (NotImplementedException e) {
        throw new TAnalysisException(e.getMessage());
      }
      if (plan != null) {
        System.out.println(plan.getExplainString());
      }

      TExecutePlanRequest execRequest = new TExecutePlanRequest(
          Expr.treesToThrift(analysisResult.selectStmt.getSelectListExprs()));
      if (plan != null) {
        execRequest.setPlan(plan.treeToThrift());
        execRequest.setDescTbl(analysisResult.analyzer.getDescTbl().toThrift());
      }
      System.out.println("returned exec request: " + execRequest.toString());
      return execRequest;
    }

    public void ShutdownServer() {
      System.exit(0);
    }
  }

  public static void main(String[] args) {
    HiveMetaStoreClient client = null;
    try {
      client = new HiveMetaStoreClient(new HiveConf(PlanService.class));
      Catalog catalog = new Catalog(client);

      PlanServiceHandler handler = new PlanServiceHandler(catalog);
      ImpalaPlanService.Processor proc = new ImpalaPlanService.Processor(handler);
      TServerTransport transport = new TServerSocket(20000);
      TServer server = new TSimpleServer(new Args(transport).processor(proc));
      server.serve();
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.exit(2);
    }
  }

}
