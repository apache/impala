// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.common.RuntimeEnv;
import com.cloudera.impala.service.Frontend;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestFileParser.Section;
import com.cloudera.impala.testutil.TestFileParser.TestCase;
import com.cloudera.impala.testutil.TestUtils;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;
import com.cloudera.impala.thrift.TExecRequest;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.THBaseKeyRange;
import com.cloudera.impala.thrift.THdfsFileSplit;
import com.cloudera.impala.thrift.TQueryContext;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TScanRangeLocations;
import com.cloudera.impala.thrift.TStmtType;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class PlannerTest {
  private final static Logger LOG = LoggerFactory.getLogger(PlannerTest.class);
  private final static boolean GENERATE_OUTPUT_FILE = true;

  private static Frontend frontend_;
  private final String testDir_ = "functional-planner/queries/PlannerTest";
  private final String outDir_ = "/tmp/PlannerTest/";

  @BeforeClass
  public static void setUp() throws Exception {
    // Use 8 cores for resource estimation.
    RuntimeEnv.INSTANCE.setNumCores(8);
    frontend_ = new Frontend(Catalog.CatalogInitStrategy.LAZY,
        AuthorizationConfig.createAuthDisabledConfig());
  }

  @AfterClass
  public static void cleanUp() {
    RuntimeEnv.INSTANCE.reset();
  }

  private StringBuilder PrintScanRangeLocations(TQueryExecRequest execRequest) {
    StringBuilder result = new StringBuilder();
    if (execRequest.per_node_scan_ranges == null) {
      return result;
    }
    for (Map.Entry<Integer, List<TScanRangeLocations>> entry:
        execRequest.per_node_scan_ranges.entrySet()) {
      result.append("NODE " + entry.getKey().toString() + ":\n");
      if (entry.getValue() == null) {
        continue;
      }

      for (TScanRangeLocations locations: entry.getValue()) {
        // print scan range
        result.append("  ");
        if (locations.scan_range.isSetHdfs_file_split()) {
          THdfsFileSplit split = locations.scan_range.getHdfs_file_split();
          result.append("HDFS SPLIT " + split.path + " "
              + Long.toString(split.offset) + ":" + Long.toString(split.length));
        }
        if (locations.scan_range.isSetHbase_key_range()) {
          THBaseKeyRange keyRange = locations.scan_range.getHbase_key_range();
          result.append("HBASE KEYRANGE ");
          result.append("port=" + locations.locations.get(0).server.port+" ");
          if (keyRange.isSetStartKey()) {
            result.append(HBaseScanNode.printKey(keyRange.getStartKey().getBytes()));
          } else {
            result.append("<unbounded>");
          }
          result.append(":");
          if (keyRange.isSetStopKey()) {
            result.append(HBaseScanNode.printKey(keyRange.getStopKey().getBytes()));
          } else {
            result.append("<unbounded>");
          }
        }
        result.append("\n");
      }
    }
    return result;
  }

  /**
   * Extracts and returns the expected error message from expectedPlan.
   * Returns null if expectedPlan is empty or its first element is not an error message.
   * The accepted format for error messages is 'not implemented: expected error message'
   * Returns the empty string if expectedPlan starts with 'not implemented' but no
   * expected error message was given.
   */
  private String getExpectedErrorMessage(ArrayList<String> expectedPlan) {
    if (expectedPlan.isEmpty()) return null;
    if (!expectedPlan.get(0).toLowerCase().startsWith("not implemented")) return null;
    // Find first ':' and extract string on right hand side as error message.
    int ix = expectedPlan.get(0).indexOf(":");
    if (ix + 1 > 0) {
      return expectedPlan.get(0).substring(ix + 1).trim();
    } else {
      return "";
    }
  }

  private void handleNotImplException(String query, String expectedErrorMsg,
      StringBuilder errorLog, StringBuilder actualOutput, Throwable e) {
    boolean isImplemented = expectedErrorMsg == null;
    actualOutput.append("not implemented: " + e.getMessage() + "\n");
    if (isImplemented) {
      errorLog.append("query:\n" + query + "\nPLAN not implemented: "
          + e.getMessage() + "\n");
    } else {
      // Compare actual and expected error messages.
      if (expectedErrorMsg != null && !expectedErrorMsg.isEmpty()) {
        if (!e.getMessage().toLowerCase().equals(expectedErrorMsg.toLowerCase())) {
          errorLog.append("query:\n" + query + "\nExpected error message: '"
              + expectedErrorMsg + "'\nActual error message: '"
              + e.getMessage() + "'\n");
        }
      }
    }
  }

  /**
   * Produces single-node and distributed plans for testCase and compares
   * plan and scan range results.
   * Appends the actual single-node and distributed plan as well as the printed
   * scan ranges to actualOutput, along with the requisite section header.
   * locations to actualScanRangeLocations; compares both to the appropriate sections
   * of 'testCase'.
   */
  private void RunTestCase(TestCase testCase, StringBuilder errorLog,
      StringBuilder actualOutput, String dbName)
      throws CatalogException {
    String query = testCase.getQuery();
    LOG.info("running query " + query);
    TQueryContext queryCtxt = TestUtils.createQueryContext(
        dbName, System.getProperty("user.name"));
    queryCtxt.request.query_options.setExplain_level(TExplainLevel.STANDARD);
    queryCtxt.request.query_options.allow_unsupported_formats = true;
    // single-node plan and scan range locations
    testSingleNodePlan(testCase, queryCtxt, errorLog, actualOutput);
    // distributed plan
    testDistributedPlan(testCase, queryCtxt, errorLog, actualOutput);
  }

  /**
   * Produces single-node plan for testCase and compares actual plan with expected plan,
   * as well as the scan range locations.
   * If testCase contains no expected single-node plan then this function is a no-op.
   */
  private void testSingleNodePlan(TestCase testCase, TQueryContext queryCtxt,
      StringBuilder errorLog, StringBuilder actualOutput) throws CatalogException {
    ArrayList<String> expectedPlan = testCase.getSectionContents(Section.PLAN);
    // Test case has no expected single-node plan. Do not test it.
    if (expectedPlan == null || expectedPlan.isEmpty()) return;
    String query = testCase.getQuery();
    String expectedErrorMsg = getExpectedErrorMessage(expectedPlan);
    queryCtxt.request.getQuery_options().setNum_nodes(1);
    queryCtxt.request.setStmt(query);
    boolean isImplemented = expectedErrorMsg == null;
    StringBuilder explainBuilder = new StringBuilder();

    TExecRequest execRequest = null;
    String locationsStr = null;
    actualOutput.append(Section.PLAN.getHeader() + "\n");
    try {
      execRequest = frontend_.createExecRequest(queryCtxt, explainBuilder);
      Preconditions.checkState(execRequest.stmt_type == TStmtType.DML
          || execRequest.stmt_type == TStmtType.QUERY);
      String explainStr = removeResourceEstimates(explainBuilder.toString());
      actualOutput.append(explainStr);
      if (!isImplemented) {
        errorLog.append(
            "query produced PLAN\nquery=" + query + "\nplan=\n" + explainStr);
      } else {
        LOG.info("single-node plan: " + explainStr);
        String result = TestUtils.compareOutput(
            Lists.newArrayList(explainStr.split("\n")), expectedPlan, true);
        if (!result.isEmpty()) {
          errorLog.append("section " + Section.PLAN.toString() + " of query:\n" + query
              + "\n" + result);
        }
        locationsStr =
            PrintScanRangeLocations(execRequest.query_exec_request).toString();
      }
    } catch (ImpalaException e) {
      if (e instanceof AnalysisException) {
        errorLog.append("query:\n" + query + "\nanalysis error: " + e.getMessage() + "\n");
        return;
      } else if (e instanceof InternalException) {
        errorLog.append("query:\n" + query + "\ninternal error: " + e.getMessage() + "\n");
        return;
      } if (e instanceof NotImplementedException) {
        handleNotImplException(query, expectedErrorMsg, errorLog, actualOutput, e);
      } else if (e instanceof CatalogException) {
        // TODO: do we need to rethrow?
        throw (CatalogException) e;
      } else {
        errorLog.append(
            "query:\n" + query + "\nunhandled exception: " + e.getMessage() + "\n");
      }
    }

    // compare scan range locations
    LOG.info("scan range locations: " + locationsStr);
    ArrayList<String> expectedLocations =
        testCase.getSectionContents(Section.SCANRANGELOCATIONS);

    if (expectedLocations.size() > 0 && locationsStr != null) {
      // Locations' order does not matter.
      String result = TestUtils.compareOutput(
          Lists.newArrayList(locationsStr.split("\n")), expectedLocations, false);
      if (!result.isEmpty()) {
        errorLog.append("section " + Section.SCANRANGELOCATIONS + " of query:\n"
            + query + "\n" + result);
      }
      actualOutput.append(Section.SCANRANGELOCATIONS.getHeader() + "\n");
      actualOutput.append(locationsStr);
      // TODO: check that scan range locations are identical in both cases
    }
  }


  /**
  * Produces distributed plan for testCase and compares actual plan with expected plan.
  * If testCase contains no expected distributed plan then this function is a no-op.
  */
 private void testDistributedPlan(TestCase testCase, TQueryContext queryCtxt,
     StringBuilder errorLog, StringBuilder actualOutput) throws CatalogException {
   ArrayList<String> expectedPlan =
       testCase.getSectionContents(Section.DISTRIBUTEDPLAN);
   // Test case has no expected distributed plan. Do not test it.
   if (expectedPlan == null || expectedPlan.isEmpty()) return;
   String query = testCase.getQuery();
   String expectedErrorMsg = getExpectedErrorMessage(expectedPlan);
   queryCtxt.request.getQuery_options().setNum_nodes(
       ImpalaInternalServiceConstants.NUM_NODES_ALL);
   queryCtxt.request.setStmt(query);
   boolean isImplemented = expectedErrorMsg == null;
   StringBuilder explainBuilder = new StringBuilder();
   actualOutput.append(Section.DISTRIBUTEDPLAN.getHeader() + "\n");
   TExecRequest execRequest = null;
   try {
     // distributed plan
     execRequest = frontend_.createExecRequest(queryCtxt, explainBuilder);
     Preconditions.checkState(execRequest.stmt_type == TStmtType.DML
         || execRequest.stmt_type == TStmtType.QUERY);
     String explainStr = removeResourceEstimates(explainBuilder.toString());
     actualOutput.append(explainStr);
     if (!isImplemented) {
       errorLog.append(
           "query produced DISTRIBUTEDPLAN\nquery=" + query + "\nplan=\n"
           + explainStr);
     } else {
       LOG.info("distributed plan: " + explainStr);
       String result = TestUtils.compareOutput(
           Lists.newArrayList(explainStr.split("\n")), expectedPlan, true);
       if (!result.isEmpty()) {
         errorLog.append("section " + Section.DISTRIBUTEDPLAN.toString()
             + " of query:\n" + query + "\n" + result);
       }
     }
   } catch (ImpalaException e) {
     if (e instanceof AnalysisException) {
       errorLog.append(
           "query:\n" + query + "\nanalysis error: " + e.getMessage() + "\n");
       return;
     } else if (e instanceof InternalException) {
       errorLog.append(
           "query:\n" + query + "\ninternal error: " + e.getMessage() + "\n");
       return;
     } if (e instanceof NotImplementedException) {
       handleNotImplException(query, expectedErrorMsg, errorLog, actualOutput, e);
     } else if (e instanceof CatalogException) {
       throw (CatalogException) e;
     } else {
       errorLog.append(
           "query:\n" + query + "\nunhandled exception: " + e.getMessage() + "\n");
     }
   }
  }

  /**
   * Strips out the header containing resource estimates from the given explain plan,
   * because the estimates can change easily with stats/cardinality.
   */
  private String removeResourceEstimates(String explain) {
    if (explain.startsWith("Estimated Per-Host Requirements:")) {
      String[] lines = explain.split("\n");
      return Joiner.on("\n").join(Arrays.copyOfRange(lines, 2, lines.length)) + "\n";
    }
    return explain;
  }

  private void runPlannerTestFile(String testFile, String dbName) {
    String fileName = testDir_ + "/" + testFile + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    StringBuilder actualOutput = new StringBuilder();

    queryFileParser.parseFile();
    StringBuilder errorLog = new StringBuilder();
    for (TestCase testCase : queryFileParser.getTestCases()) {
      actualOutput.append(testCase.getSectionAsString(Section.QUERY, true, "\n"));
      actualOutput.append("\n");
      try {
        RunTestCase(testCase, errorLog, actualOutput, dbName);
      } catch (CatalogException e) {
        errorLog.append(String.format("Failed to plan query\n%s\n%s",
            testCase.getQuery(), e.getMessage()));
      }
      actualOutput.append("====\n");
    }

    // Create the actual output file
    if (GENERATE_OUTPUT_FILE) {
      try {
        File outDirFile = new File(outDir_);
        outDirFile.mkdirs();
        FileWriter fw = new FileWriter(outDir_ + testFile + ".test");
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

  private void runPlannerTestFile(String testFile) {
    runPlannerTestFile(testFile, "default");
  }

  @Test
  public void testPredicatePropagation() {
    runPlannerTestFile("predicate-propagation");
  }

  @Test
  public void testConstant() {
    runPlannerTestFile("constant");
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
  public void testHbase() {
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
  public void testJoinOrder() {
    runPlannerTestFile("join-order");
  }

  @Test
  public void testOuterJoins() {
    runPlannerTestFile("outer-joins");
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

  @Test
  public void testSubqueryLimit() {
    runPlannerTestFile("subquery-limit");
  }

  @Test
  public void testUnion() {
    runPlannerTestFile("union");
  }

  @Test
  public void testValues() {
    runPlannerTestFile("values");
  }

  @Test
  public void testViews() {
    runPlannerTestFile("views");
  }

  @Test
  public void testWithClause() {
    runPlannerTestFile("with-clause");
  }

  @Test
  public void testDistinctEstimate() {
    runPlannerTestFile("distinct-estimate");
  }

  @Test
  public void testTpch() {
    runPlannerTestFile("tpch-all");
  }

  @Test
  public void testTpcds() {
    // Join order has been optimized for Impala. Uses ss_date as partition key.
    runPlannerTestFile("tpcds-all", "tpcds");
  }
}
