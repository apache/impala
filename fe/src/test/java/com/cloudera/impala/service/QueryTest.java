// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.TestSchemaUtils;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestUtils;

public class QueryTest {
  private static Catalog catalog;
  private static Executor coordinator;
  private static StringBuilder testErrorLog;
  private final String testDir = "QueryTest";

  @BeforeClass
  public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createClient();
    catalog = new Catalog(client);
    coordinator = new Executor(catalog);
    testErrorLog = new StringBuilder();
  }

  private void runTests(String testCase, boolean abortOnError, int maxErrors) {
    String fileName = testDir + "/" + testCase + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    queryFileParser.open();
    StringBuilder errorLog = new StringBuilder();
    while (queryFileParser.hasNext()) {
      queryFileParser.next();
      ArrayList<String> expectedTypes = queryFileParser.getExpectedResult(0);
      ArrayList<String> expectedResults = queryFileParser.getExpectedResult(1);
      // run query 3 ways: with backend's default batch size, with small batch size,
      // and with batch size of 1, which should trigger a lot of corner cases
      // in the execution engine code
      TestUtils.runQuery(
          coordinator, queryFileParser.getQuery(), queryFileParser.getLineNum(),
          0, abortOnError, maxErrors, null, expectedTypes, expectedResults, null, null,
          errorLog);
      TestUtils.runQuery(
          coordinator, queryFileParser.getQuery(), queryFileParser.getLineNum(),
          16, abortOnError, maxErrors, null, expectedTypes, expectedResults, null, null,
          errorLog);
      TestUtils.runQuery(
          coordinator, queryFileParser.getQuery(), queryFileParser.getLineNum(),
          1, abortOnError, maxErrors, null, expectedTypes, expectedResults, null, null,
          errorLog);
    }
    queryFileParser.close();
    if (errorLog.length() != 0) {
      testErrorLog.append("\n\n" + testCase + "\n");
      testErrorLog.append(errorLog);
    }
  }

  @Test
  public void Test() {
    runTests("aggregation", false, 1000);
    runTests("exprs", false, 1000);
    runTests("hdfs-scan-node", false, 1000);
    runTests("hdfs-partitions", false, 1000);
    runTests("hbase-scan-node", false, 1000);
    runTests("hbase-rowkeys", false, 1000);
    runTests("hbase-filters", false, 1000);
    runTests("joins", false, 1000);
    runTests("outer-joins", false, 1000);
    runTests("limit", false, 1000);

    // check whether any of the tests had errors
    if (testErrorLog.length() != 0) {
      fail(testErrorLog.toString());
      //fail(Integer.toString(testErrorLog.length()) + "\n" + testErrorLog.toString());
    }
  }
}
