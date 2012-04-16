// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
import com.cloudera.impala.testutil.TestFileParser.Section;
import com.cloudera.impala.testutil.TestFileParser.TestCase;
import com.cloudera.impala.testutil.TestUtils;
import com.cloudera.impala.thrift.Constants;
import com.google.common.collect.Lists;

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

  private void RunQuery(String query, int numNodes, TestCase testCase,
                        Section section, StringBuilder errorLog,
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
      ArrayList<String> expectedPlan = testCase.getSectionContents(section);
      String result =
        TestUtils.compareOutput(Lists.newArrayList(explainStr.split("\n")), expectedPlan,
            true);
      if (!result.isEmpty()) {
        errorLog.append(
            "section " + section + " of query:\n"
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

  private void runPlannerTestFile(String testFile) {
    String fileName = testDir + "/" + testFile + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    StringBuilder actualOutput = new StringBuilder();

    queryFileParser.parseFile();
    StringBuilder errorLog = new StringBuilder();
    for (TestCase testCase : queryFileParser.getTestCases()) {
      String query = testCase.getQuery();
      actualOutput.append(testCase.getSectionAsString(Section.QUERY, true, "\n"));
      actualOutput.append("----\n");
      // each planner test case contains multiple result sections:
      // - the first one is for the single-node plan
      // - the subsequent one is for distributed plans; there is one
      //   section for all plan fragments produced by the planner
      List<String> plan = testCase.getSectionContents(Section.PLAN);
      if (plan.size() > 0 && plan.get(0).toLowerCase().startsWith("not implemented")) {
        RunUnimplementedQuery(query, errorLog);
        actualOutput.append("not implemented\n");
      } else {
        RunQuery(query, 1, testCase, Section.PLAN, errorLog, actualOutput);
        actualOutput.append("------------\n");
        RunQuery(query, Constants.NUM_NODES_ALL, testCase, Section.DISTRIBUTEDPLAN,
                 errorLog, actualOutput);
      }
      actualOutput.append("====\n");
    }

    // Create the actual output file
    if (GENERATE_OUTPUT_FILE) {
      try {
        File outDirFile = new File(outDir);
        outDirFile.mkdirs();
        FileWriter fw = new FileWriter(outDir + testFile + ".test");
        fw.write(actualOutput.toString());
        fw.close();
      } catch (IOException e) {
        errorLog.append("Unable to create output file: " + e.getMessage());
      }
    }

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
