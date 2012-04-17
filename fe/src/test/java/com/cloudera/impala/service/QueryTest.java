// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestUtils;
import com.cloudera.impala.testutil.TestFileParser.Section;
import com.cloudera.impala.testutil.TestFileParser.TestCase;

public class QueryTest {
  private static Catalog catalog;
  private static Executor executor;
  private final String testDir = "QueryTest";
  private static ArrayList<String> tableSubsitutionList;

  @BeforeClass
  public static void setUp() throws Exception {
    catalog = new Catalog();
    executor = new Executor(catalog);
    tableSubsitutionList = new ArrayList<String>();
    tableSubsitutionList.add("");
    tableSubsitutionList.add("_rc");
    tableSubsitutionList.add("_seq");
    tableSubsitutionList.add("_seq_def");
    tableSubsitutionList.add("_seq_gzip");
    tableSubsitutionList.add("_seq_bzip");
    tableSubsitutionList.add("_seq_snap");
    tableSubsitutionList.add("_seq_record_def");
    tableSubsitutionList.add("_seq_record_gzip");
    tableSubsitutionList.add("_seq_record_bzip");
    tableSubsitutionList.add("_seq_record_snap");
  }

  private void runQueryTestFile(String testFile, boolean abortOnError, int maxErrors) {
    runQueryTestFile(testFile, abortOnError, maxErrors, null);
  }

  private void runQueryTestFile(String testFile, boolean abortOnError, int maxErrors,
      ArrayList<String> tables) {
    String fileName = testDir + "/" + testFile + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    for (int f = 0; f < (tables == null ? 1 : tables.size()); f++) {
        queryFileParser.parseFile(tables == null ? null : tables.get(f));
      StringBuilder errorLog = new StringBuilder();
      for (TestCase testCase : queryFileParser.getTestCases()) {
        ArrayList<String> expectedTypes =
          testCase.getSectionContents(Section.TYPES);
        ArrayList<String> expectedResults =
          testCase.getSectionContents(Section.RESULTS);
        // run each test against all possible combinations of batch sizes and
        // number of execution nodes
        int[] batchSizes = {0, 16, 1};
        int[] numNodes = {1, 2, 3, 0};
        for (int i = 0; i < batchSizes.length; ++i) {
          for (int j = 0; j < numNodes.length; ++j) {
            TestUtils.runQuery(
                executor, testCase.getSectionAsString(Section.QUERY, false, " "),
                numNodes[j], batchSizes[i], abortOnError, maxErrors,
                testCase.getStartingLineNum(), null, expectedTypes,
                expectedResults, null, null, errorLog);
          }
        }
      }
      if (errorLog.length() != 0) {
        fail(errorLog.toString());
      }
    }
  }


  @Test
  public void TestDistinct() {
    runQueryTestFile("distinct", false, 1000);
  }

  @Test
  public void TestAggregation() {
    runQueryTestFile("aggregation", false, 1000);
  }

  @Test
  public void TestExprs() {
    runQueryTestFile("exprs", false, 1000);
  }

  @Test
  public void TestHdfsScanNode() {
    runQueryTestFile("hdfs-scan-node", false, 1000, tableSubsitutionList);
  }

  @Test
  public void TestFilePartions() {
    runQueryTestFile("hdfs-partitions", false, 1000, tableSubsitutionList);
  }

  @Test
  public void TestHBaseScanNode() {
    runQueryTestFile("hbase-scan-node", false, 1000);
  }

  @Test
  public void TestHBaseRowKeys() {
    runQueryTestFile("hbase-rowkeys", false, 1000);
  }

  @Test
  public void TestHBaseFilters() {
    runQueryTestFile("hbase-filters", false, 1000);
  }

  @Test
  public void TestJoins() {
    runQueryTestFile("joins", false, 1000);
  }

  @Test
  public void TestOuterJoins() {
    runQueryTestFile("outer-joins", false, 1000);
  }

  @Test
  public void TestLimit() {
    runQueryTestFile("limit", false, 1000);
  }

  @Test
  public void TestTopN() {
    runQueryTestFile("top-n", false, 1000);
  }

  @Test
  public void TestEmpty() {
    runQueryTestFile("empty", false, 1000);
  }

  @Test
  public void TestSubquery() {
    runQueryTestFile("subquery", false, 1000);
  }

}
