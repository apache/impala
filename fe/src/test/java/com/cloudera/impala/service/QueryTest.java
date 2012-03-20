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
  private static Executor executor;
  private final String testDir = "QueryTest";

  @BeforeClass
  public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createClient();
    catalog = new Catalog(client);
    executor = new Executor(catalog);
  }

  private void runQueryTestFile(String testCase, boolean abortOnError, int maxErrors) {
    String fileName = testDir + "/" + testCase + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    queryFileParser.open();
    StringBuilder errorLog = new StringBuilder();
    while (queryFileParser.hasNext()) {
      queryFileParser.next();
      ArrayList<String> expectedTypes = queryFileParser.getExpectedResult(0);
      ArrayList<String> expectedResults = queryFileParser.getExpectedResult(1);
      // run each test against all possible combinations of batch sizes and
      // number of execution nodes
      int[] batchSizes = {0, 16, 1};
      int[] numNodes = {1, 2, 3, 0};
      for (int i = 0; i < batchSizes.length; ++i) {
        for (int j = 0; j < numNodes.length; ++j) {
          TestUtils.runQuery(
              executor, queryFileParser.getQuery(), queryFileParser.getLineNum(),
              numNodes[j], batchSizes[i], abortOnError, maxErrors, null, expectedTypes,
              expectedResults, null, null, errorLog);
        }
      }
    }
    queryFileParser.close();
    if (errorLog.length() != 0) {
      fail(errorLog.toString());
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
  public void TestHdfsTextScanNode() {
    runQueryTestFile("hdfs-scan-node", false, 1000);
  }

  @Test
  public void TestHdfsTextPartitions() {
    runQueryTestFile("hdfs-partitions", false, 1000);
  }

  @Test
  public void TestHdfsRCFileScanNode() {
    runQueryTestFile("hdfs-rcfile-scan-node", false, 1000);
  }

  @Test
  public void TestHdfsRCFilePartitions() {
    runQueryTestFile("hdfs-rcfile-partitions", false, 1000);
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
