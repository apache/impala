// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

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

public class PlannerTest {
  private final static Logger LOG = LoggerFactory.getLogger(PlannerTest.class);
  private final static boolean GENERATE_OUTPUT_FILE = false;

  private static Catalog catalog;
  private static AnalysisContext analysisCtxt;
  private final String testDir = "PlannerTest";
  private final String outDir = "/tmp/PlannerTest/";

  private final StringBuilder explainStringBuilder = new StringBuilder();

  @BeforeClass
  public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createClient();
    catalog = new Catalog(client);
    analysisCtxt = new AnalysisContext(catalog);
  }

  private void RunQuery(String query, int numNodes, TestFileParser parser,
                        int sectionStartIdx, StringBuilder errorLog,
                        StringBuilder actualOutput) {
    try {
      LOG.info("running query " + query);
      AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(query);
      Planner planner = new Planner();
      explainStringBuilder.setLength(0);
      planner.createPlanFragments(analysisResult, numNodes, explainStringBuilder);

      String explainStr = explainStringBuilder.toString();
      actualOutput.append(explainStr);
      LOG.info(explainStr);
      ArrayList<String> expectedPlan = parser.getExpectedResult(sectionStartIdx);
      String result = TestUtils.compareOutput(explainStr.split("\n"), expectedPlan, true);
      if (!result.isEmpty()) {
        errorLog.append(
            "section " + Integer.toString(sectionStartIdx) + " of query:\n"
            + query + "\n" + result);
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
      explainStringBuilder.setLength(0);

      planner.createPlanFragments(analysisResult, 1, explainStringBuilder);

      errorLog.append(
          "query produced a plan\nquery=" + query + "\nplan=\n"
          + explainStringBuilder.toString());
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
    StringBuilder actualOutput = new StringBuilder();

    queryFileParser.open();
    StringBuilder errorLog = new StringBuilder();
    while (queryFileParser.hasNext()) {
      queryFileParser.next();
      String query = queryFileParser.getQuery();
      actualOutput.append(queryFileParser.getQuerySection());
      actualOutput.append("----\n");
      // each planner test case contains multiple result sections:
      // - the first one is for the single-node plan
      // - the subsequent ones are for distributed plans; there is one
      //   section per plan fragment produced by the planner
      ArrayList<String> plan = queryFileParser.getExpectedResult(0);
      if (plan.size() > 0 && plan.get(0).toLowerCase().startsWith("not implemented")) {
        RunUnimplementedQuery(query, errorLog);
        actualOutput.append("not implemented\n");
      } else {
        RunQuery(query, 1, queryFileParser, 0, errorLog, actualOutput);
        actualOutput.append("------------\n");
        RunQuery(query, Constants.NUM_NODES_ALL, queryFileParser, 1, errorLog,
                 actualOutput);
      }
      actualOutput.append("====\n");
    }

    // Create the actual output file
    if (GENERATE_OUTPUT_FILE) {
      try {
        File outDirFile = new File(outDir);
        outDirFile.mkdirs();
        FileWriter fw = new FileWriter(outDir + testCase + ".test");
        fw.write(actualOutput.toString());
        fw.close();
      } catch (IOException e) {
        errorLog.append("Unable to create output file: " + e.getMessage());
      }
    }

    queryFileParser.close();
    if (errorLog.length() != 0) {
      fail(errorLog.toString());
    }
  }

  @Test
  public void testDistinct() {
    runPlannerTestFile("distinct");
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

  @Test
  public void testSubquery() {
    runPlannerTestFile("subquery");
  }
}
