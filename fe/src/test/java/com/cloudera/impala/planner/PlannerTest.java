// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.catalog.Catalog;
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
  private final static boolean GENERATE_OUTPUT_FILE = true;

  private static Catalog catalog;
  private static AnalysisContext analysisCtxt;
  private final String testDir = "functional-planner/queries/PlannerTest";
  private final String outDir = "/tmp/PlannerTest/";

  private final StringBuilder explainStringBuilder = new StringBuilder();

  @BeforeClass
  public static void setUp() throws Exception {
    catalog = new Catalog(true);
    analysisCtxt = new AnalysisContext(catalog);
  }

  @AfterClass
  public static void cleanUp() {
    catalog.close();
  }

  private void RunQuery(String query, int numNodes, TestCase testCase,
                        Section section, StringBuilder errorLog,
                        StringBuilder actualOutput, PlanNode.ExplainPlanLevel level) {
    try {
      LOG.info("running query " + query);
      AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(query);
      Planner planner = new Planner();
      planner.setExplainPlanDetailLevel(level);
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
      errorLog.append("query:\n" + query + "\nplan not implemented: " +
          e.getMessage() + "\n");
    }
  }

  private void RunUnimplementedQuery(String query, int numNodes,
                                     StringBuilder errorLog) {
    try {
      AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(query);
      Planner planner = new Planner();
      explainStringBuilder.setLength(0);

      planner.createPlanFragments(analysisResult, numNodes, explainStringBuilder);

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

  private void runPlannerTestFile(String testFile, PlanNode.ExplainPlanLevel level) {
    String fileName = testDir + "/" + testFile + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    StringBuilder actualOutput = new StringBuilder();

    queryFileParser.parseFile();
    StringBuilder errorLog = new StringBuilder();
    for (TestCase testCase : queryFileParser.getTestCases()) {
      String query = testCase.getQuery();
      actualOutput.append(testCase.getSectionAsString(Section.QUERY, true, "\n"));
      actualOutput.append("\n");
      actualOutput.append("---- PLAN\n");
      // each planner test case contains multiple result sections:
      // - the first one is for the single-node plan
      // - the subsequent ones are for distributed plans; there is one
      //   section per plan fragment produced by the planner
      // only execute the multi-node plan if the single-node plan is implemented.
      // the multi-node plan may not be implemented.
      ArrayList<String> singleNodePlan = testCase.getSectionContents(Section.PLAN);
      if (singleNodePlan.size() > 0 &&
          singleNodePlan.get(0).toLowerCase().startsWith("not implemented")) {
        RunUnimplementedQuery(query, 1, errorLog);
        actualOutput.append("not implemented\n");
      } else {
        // Run single-node query,
        RunQuery(query, 1, testCase, Section.PLAN, errorLog, actualOutput, level);
        // Check if multi-node query is implemented.
        ArrayList<String> multiNodePlan = testCase.getSectionContents(Section.DISTRIBUTEDPLAN);
        if (multiNodePlan.size() > 0 &&
            multiNodePlan.get(0).toLowerCase().startsWith("not implemented")) {
          RunUnimplementedQuery(query, Constants.NUM_NODES_ALL, errorLog);
          actualOutput.append("not implemented\n");
        } else {
          actualOutput.append("------------ DISTRIBUTEDPLAN\n");
          // Run multi-node query
          // TODO: use Constants.NUM_NODES_ALL when multi-node planning is done (IMP-77)
          // Using all nodes will cause unstable plan because data location is
          // non-deterministic. To see incorrect multi-node planning, change 2 to
          // 0 (use all nodes)
          RunQuery(query, 2, testCase, Section.DISTRIBUTEDPLAN, errorLog,
                   actualOutput, level);
        }
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
    runPlannerTestFile("distinct", PlanNode.ExplainPlanLevel.NORMAL);
  }

  @Test
  public void testAggregation() {
    runPlannerTestFile("aggregation", PlanNode.ExplainPlanLevel.NORMAL);
  }

  @Test
  public void testHBase() {
    runPlannerTestFile("hbase", PlanNode.ExplainPlanLevel.NORMAL);
  }

  @Test
  public void testInsert() {
    runPlannerTestFile("insert", PlanNode.ExplainPlanLevel.NORMAL);
  }

  @Test
  public void testHdfs() {
    runPlannerTestFile("hdfs", PlanNode.ExplainPlanLevel.NORMAL);
  }

  @Test
  public void testJoins() {
    runPlannerTestFile("joins", PlanNode.ExplainPlanLevel.HIGH);
  }

  @Test
  public void testOrder() {
    runPlannerTestFile("order", PlanNode.ExplainPlanLevel.NORMAL);
  }

  @Test
  public void testTopN() {
    runPlannerTestFile("topn", PlanNode.ExplainPlanLevel.NORMAL);
  }

  @Test
  public void testSubquery() {
    runPlannerTestFile("subquery", PlanNode.ExplainPlanLevel.HIGH);
  }

  @Test
  public void testUnion() {
    runPlannerTestFile("union", PlanNode.ExplainPlanLevel.HIGH);
  }

  @Test
  public void testTpch() {
    // TODO: Q20-Q22 are disabled due to IMP-137. Once that bug is resolved they should
    // be re-enabled.
    runPlannerTestFile("tpch-all", PlanNode.ExplainPlanLevel.HIGH);
  }
}
