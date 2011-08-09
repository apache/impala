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
  private static Coordinator coordinator;
  private static StringBuilder testErrorLog;
  private final String testDir = "ServiceTest";

  @BeforeClass
  public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createClient();
    catalog = new Catalog(client);
    coordinator = new Coordinator(catalog);
    testErrorLog = new StringBuilder();
  }

  private void runTests(String testCase, boolean abortOnError, int maxErrors) {
    String fileName = testDir + "/" + testCase + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    queryFileParser.open();
    while (queryFileParser.hasNext()) {
      queryFileParser.next();
      String query = queryFileParser.getQuery();
      ArrayList<String> expectedTypes = queryFileParser.getExpectedResult(0);
      ArrayList<String> expectedResults = queryFileParser.getExpectedResult(1);
      TestUtils.runQuery(coordinator, query, abortOnError, maxErrors, expectedTypes,
                         expectedResults, null, null, testErrorLog);
    }
    queryFileParser.close();
  }

  @Test
  public void Test() {
    runTests("aggregation", false, 1000);
    // TODO: enable this test in follow-on change (something is broken right now)
    //runTests("exprs", false, 1000);
    runTests("textscannode", false, 1000);

    // check whether any of the tests had errors
    if (testErrorLog.length() != 0) {
      fail(testErrorLog.toString());
    }
  }
}
