// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.dataerror;

import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.TestSchemaUtils;
import com.cloudera.impala.service.Executor;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestUtils;

public class DataErrorsTest {
  private static Catalog catalog;
  private static Executor executor;
  private static StringBuilder testErrorLog;
  private final String testDir = "DataErrorsTest";

  @BeforeClass
  public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createClient();
    catalog = new Catalog(client);
    executor = new Executor(catalog);
    testErrorLog = new StringBuilder();
  }

  private void runErrorTestFile(String testCase, boolean abortOnError, int maxErrors) {
    StringBuilder errorLog = new StringBuilder();
    String fileName = testDir + "/" + testCase + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    queryFileParser.open();
    while (queryFileParser.hasNext()) {
      queryFileParser.next();
      ArrayList<String> expectedErrors = queryFileParser.getExpectedResult(0);
      // The test file is assumed to contain all errors. We may only want to compare a few of them.
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
      }
      while (expectedErrors.size() > lastLine) {
        expectedErrors.remove(expectedErrors.size() - 1);
      }
      // File error entries must be sorted by filename within .test file.
      ArrayList<String> expectedFileErrors = queryFileParser.getExpectedResult(1);
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
      TestUtils.runQuery(executor, queryFileParser.getQuery(),
          queryFileParser.getLineNum(), 0, abortOnError, maxErrors, null, null, null,
          expectedErrors, expectedFileErrors, testErrorLog);
      TestUtils.runQuery(executor, queryFileParser.getQuery(),
          queryFileParser.getLineNum(), 16, abortOnError, maxErrors, null, null, null,
          expectedErrors, expectedFileErrors, testErrorLog);
      TestUtils.runQuery(executor, queryFileParser.getQuery(),
          queryFileParser.getLineNum(), 1, abortOnError, maxErrors, null, null, null,
          expectedErrors, expectedFileErrors, testErrorLog);
    }
    queryFileParser.close();

    if (errorLog.length() != 0) {
      fail(errorLog.toString());
    }
  }

  @Test
  public void TestHdfsScanNodeErrors() {
    runErrorTestFile("hdfs-scan-node-errors", false, 100);
    runErrorTestFile("hdfs-scan-node-errors", false, 5);
    runErrorTestFile("hdfs-scan-node-errors", true, 1);
  }

  @Test
  public void TestHdfsRCFileScanNodeErrors() {
    runErrorTestFile("hdfs-rcfile-scan-node-errors", false, 100);
    runErrorTestFile("hdfs-rcfile-scan-node-errors", false, 5);
    runErrorTestFile("hdfs-rcfile-scan-node-errors", true, 1);
  }
  
  @Test
  public void TestHBaseScanNodeErrors() {
    runErrorTestFile("hbase-scan-node-errors", false, 100);
    runErrorTestFile("hbase-scan-node-errors", false, 5);
    runErrorTestFile("hbase-scan-node-errors", true, 1);
  }
}
