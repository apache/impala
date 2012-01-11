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
import com.cloudera.impala.thrift.Constants;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class PlannerTest {
  private final static Logger LOG = LoggerFactory.getLogger(PlannerTest.class);

  private static Catalog catalog;
  private static AnalysisContext analysisCtxt;
  private final String testDir = "PlannerTest";

  @BeforeClass
  public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createClient();
    catalog = new Catalog(client);
    analysisCtxt = new AnalysisContext(catalog);
  }

  private void RunQuery(String query, int numNodes, TestFileParser parser,
                        int sectionStartIdx, StringBuilder errorLog) {
    try {
      LOG.info("running query " + query);
      AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(query);
      Planner planner = new Planner();
      List<PlanNode> planFragments = Lists.newArrayList();
      List<DataSink> dataSinks = Lists.newArrayList();
      planner.createPlanFragments(analysisResult, numNodes, planFragments, dataSinks);
      Preconditions.checkState(planFragments.size() == dataSinks.size());
      for (int i = 0; i < planFragments.size(); ++i) {
        DataSink dataSink = dataSinks.get(i);
        PlanNode fragment = planFragments.get(i);
        String explainString;
        // Coordinator fragment might not have an associated sink.
        if (dataSink == null) {
          explainString = fragment.getExplainString();
        } else {
          explainString = dataSink.getExplainString() + fragment.getExplainString();
        }
        LOG.info(explainString);
        ArrayList<String> expectedPlan = parser.getExpectedResult(sectionStartIdx + i);
        String result = TestUtils.compareOutput(explainString.split("\n"), expectedPlan, true);
        if (!result.isEmpty()) {
          errorLog.append(
              "section " + Integer.toString(sectionStartIdx + i) + " of query:\n"
              + query + "\n" + result);
        }
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
      List<DataSink> dataSinks = Lists.newArrayList();
      planner.createPlanFragments(analysisResult, 1, planFragments, dataSinks);
      String explainString = null;
      PlanNode plan = planFragments.get(0);
      if (analysisResult.isInsertStmt()) {
        DataSink dataSink = dataSinks.get(0);
        explainString = dataSink.getExplainString() + plan.getExplainString("  ");
      } else {
        explainString = plan.getExplainString();
      }
      errorLog.append(
          "query produced a plan\nquery=" + query + "\nplan=\n"
          + explainString);
    } catch (AnalysisException e) {
      errorLog.append("query:\n" + query + "\nanalysis error: " + e.getMessage() + "\n");
    } catch (InternalException e) {
      errorLog.append("query:\n" + query + "\ninternal error: " + e.getMessage() + "\n");
    } catch (NotImplementedException e) {
      // expected
    }
  }

  private void runPlannerTestFile(String testCase) {
    String fileName = testDir + "/" + testCase + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    queryFileParser.open();
    StringBuilder errorLog = new StringBuilder();
    while (queryFileParser.hasNext()) {
      queryFileParser.next();
      String query = queryFileParser.getQuery();
      // each planner test case contains multiple result sections:
      // - the first one is for the single-node plan
      // - the subsequent ones are for distributed plans; there is one
      //   section per plan fragment produced by the planner
      ArrayList<String> plan = queryFileParser.getExpectedResult(0);
      if (plan.size() > 0 && plan.get(0).toLowerCase().startsWith("not implemented")) {
        RunUnimplementedQuery(query, errorLog);
      } else {
        RunQuery(query, 1, queryFileParser, 0, errorLog);
        RunQuery(query, Constants.NUM_NODES_ALL, queryFileParser, 1, errorLog);
      }
    }
    queryFileParser.close();
    if (errorLog.length() != 0) {
      fail(errorLog.toString());
    }
  }

  @Test
  public void testAggregation() {
    runPlannerTestFile("aggregation");
  }

  @Test
  public void testHBase() {
    runPlannerTestFile("hbase");
  }

  @Test
  public void testInsert() {
    runPlannerTestFile("insert");
  }

  @Test
  public void testHdfs() {
    runPlannerTestFile("hdfs");
  }

  @Test
  public void testJoins() {
    runPlannerTestFile("joins");
  }

  @Test
  public void testOrder() {
    runPlannerTestFile("order");
  }

  @Test
  public void testTopN() {
    runPlannerTestFile("topn");
  }
}
