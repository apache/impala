// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.dataerror;

import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.service.Executor;
import com.cloudera.impala.testutil.QueryExecTestResult;
import com.cloudera.impala.testutil.TestExecContext;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestFileParser.Section;
import com.cloudera.impala.testutil.TestFileParser.TestCase;
import com.cloudera.impala.testutil.TestUtils;

public class DataErrorsTest {
  private static Catalog catalog;
  private static Executor executor;
  private static StringBuilder testErrorLog;
  private final String testDir = "DataErrorsTest";
  private static ArrayList<String>  tableList;

  @BeforeClass
  public static void setUp() throws Exception {
    catalog = new Catalog(true);
    executor = new Executor(catalog);
    testErrorLog = new StringBuilder();
    tableList = new ArrayList<String>();
    tableList.add("");
    tableList.add("_rc");
    tableList.add("_seq");
    tableList.add("_seq_def");
    tableList.add("_seq_gzip");
    tableList.add("_seq_bzip");
    tableList.add("_seq_snap");
    tableList.add("_seq_record_def");
    tableList.add("_seq_record_gzip");
    tableList.add("_seq_record_bzip");
    tableList.add("_seq_record_snap");
  }

  @AfterClass
  public static void cleanUp() {
    catalog.close();
  }

  private void runErrorTestFile(String testFile, boolean abortOnError, int maxErrors,
      boolean disableCodegen, ArrayList<String> tables) {
    StringBuilder errorLog = new StringBuilder();
    String fileName = testDir + "/" + testFile + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    for (int f = 0; f < (tables == null ? 1 : tables.size()); f++) {
      queryFileParser.parseFile(tables == null ? null : tables.get(f));
      for (TestCase testCase : queryFileParser.getTestCases()) {
        ArrayList<String> expectedErrors = testCase.getSectionContents(Section.ERRORS);
        // The test file is assumed to contain all errors.
        // We may only want to compare a few of them.
        int errorsToCompare = Math.min(expectedErrors.size(), maxErrors);
        int lastLine = 0;
        int errorCount = 0;
        for (String line : expectedErrors) {
          // Indicates the last line of one error message.
          // The final line of an Hdfs error message starts with "line:",
          // and for Hbase tables with "row key:".
          if (line.startsWith("line:") || line.startsWith("row key:")) {
            errorCount++;
          }
          lastLine++;
          if (errorCount >= errorsToCompare) {
            break;
          }
          while (expectedErrors.size() > lastLine) {
            expectedErrors.remove(expectedErrors.size() - 1);
          }
          // File error entries must be sorted by filename within .test file.
          ArrayList<String> expectedFileErrors =
              testCase.getSectionContents(Section.FILEERRORS);
          if (abortOnError && !expectedFileErrors.isEmpty()) {
            String[] fileErrSplits = expectedFileErrors.get(0).split(",");
            // We are expecting only a single file with a single error.
            String expectedFileError = fileErrSplits[0] + ",1";
            expectedFileErrors.clear();
            expectedFileErrors.add(expectedFileError);
          }
          // run query 3 ways: with backend's default batch size, with small batch size,
          // and with batch size of 1, which should trigger a lot of corner cases
          // in the execution engine code
          String query = testCase.getQuery();
          QueryExecTestResult expectedResults = new QueryExecTestResult();
          expectedResults.getErrors().addAll(expectedErrors);
          expectedResults.getFileErrors().addAll(expectedFileErrors);
          TestUtils.runQueryUsingExecutor(executor, query,
              new TestExecContext(1, 0, disableCodegen, abortOnError, maxErrors),
              testCase.getStartingLineNum(), expectedResults, testErrorLog);
          TestUtils.runQueryUsingExecutor(executor, query,
              new TestExecContext(1, 16, disableCodegen, abortOnError, maxErrors),
              testCase.getStartingLineNum(), expectedResults, testErrorLog);
          TestUtils.runQueryUsingExecutor(executor, query,
              new TestExecContext(1, 1, disableCodegen, abortOnError, maxErrors),
              testCase.getStartingLineNum(), expectedResults, testErrorLog);
        }
      }

      if (errorLog.length() != 0) {
        fail(errorLog.toString());
      }
    }
  }

  @Test
  public void TestHdfsScanNodeErrors() {
    runErrorTestFile("hdfs-scan-node-errors", false, 100, true, tableList);
    runErrorTestFile("hdfs-scan-node-errors", false, 5, true, tableList);
    runErrorTestFile("hdfs-scan-node-errors", true, 1, true, tableList);
  }

  @Test
  public void TestHBaseScanNodeErrors() {
    runErrorTestFile("hbase-scan-node-errors", false, 100, true, null);
    runErrorTestFile("hbase-scan-node-errors", false, 5, true, null);
    runErrorTestFile("hbase-scan-node-errors", true, 1, true, null);
  }
}
