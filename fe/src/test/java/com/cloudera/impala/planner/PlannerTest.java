// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.TestSchemaUtils;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestUtils;
import com.google.common.collect.Lists;

public class PlannerTest {
  private final static Logger LOG = LoggerFactory.getLogger(PlannerTest.class);

  private static Catalog catalog;
  private static AnalysisContext analysisCtxt;
  private final String testDir = "PlannerTest";
  private static StringBuilder testErrorLog;

  @BeforeClass public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createClient();
    catalog = new Catalog(client);
    analysisCtxt = new AnalysisContext(catalog);
    testErrorLog = new StringBuilder();
  }

  private void RunQuery(String query, ArrayList<String> expectedPlan,
                        StringBuilder errorLog) {
    try {
      LOG.info("running query " + query);
      AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(query);
      Planner planner = new Planner();
      List<PlanNode> planFragments = Lists.newArrayList();
      planner.createPlanFragments(
          analysisResult.selectStmt, analysisResult.analyzer, 1, planFragments);
      PlanNode plan = planFragments.get(0);
      LOG.info(plan.getExplainString());
      String result = TestUtils.compareOutput(plan.getExplainString().split("\n"), expectedPlan);
      if (!result.isEmpty()) {
        errorLog.append("query:\n" + query + "\n" + result);
      }
    } catch (AnalysisException e) {
      errorLog.append("query:\n" + query + "\nanalysis error: " + e.getMessage() + "\n");
    } catch (InternalException e) {
      errorLog.append("query:\n" + query + "\ninternal error: " + e.getMessage() + "\n");
    } catch (NotImplementedException e) {
      errorLog.append("query:\n" + query + "\nplan not implemented");
    }
  }

  private void RunUnimplementedQuery(String query,
                                     StringBuilder errorLog) {
    try {
      AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(query);
      Planner planner = new Planner();
      List<PlanNode> planFragments = Lists.newArrayList();
      planner.createPlanFragments(
          analysisResult.selectStmt, analysisResult.analyzer, 1, planFragments);
      PlanNode plan = planFragments.get(0);
      errorLog.append(
          "query produced a plan\nquery=" + query + "\nplan=\n"
          + plan.getExplainString());
    } catch (AnalysisException e) {
      errorLog.append("query:\n" + query + "\nanalysis error: " + e.getMessage() + "\n");
    } catch (InternalException e) {
      errorLog.append("query:\n" + query + "\ninternal error: " + e.getMessage() + "\n");
    } catch (NotImplementedException e) {
      // expected
    }
  }

  private void RunTests(String testCase) {
    String fileName = testDir + "/" + testCase + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    queryFileParser.open();
    StringBuilder errorLog = new StringBuilder();
    while (queryFileParser.hasNext()) {
      queryFileParser.next();
      String query = queryFileParser.getQuery();
      ArrayList<String> plan = queryFileParser.getExpectedResult(0);
      if (plan.size() > 0 && plan.get(0).toLowerCase().startsWith("not implemented")) {
        RunUnimplementedQuery(query, errorLog);
      } else {
        RunQuery(query, plan, errorLog);
      }
    }
    queryFileParser.close();
    if (errorLog.length() != 0) {
      testErrorLog.append("\n\n" + testCase + "\n");
      testErrorLog.append(errorLog);
    }
  }

  @Test public void Test() {
    RunTests("aggregation");
    RunTests("hbase");
    RunTests("hdfs");
    RunTests("joins");
    RunTests("order");
    RunTests("topn");

    // check whether any of the tests had errors
    if (testErrorLog.length() != 0) {
      fail(testErrorLog.toString());
    }
  }
}
