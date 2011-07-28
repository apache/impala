// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.dataerror;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.TestSchemaUtils;
import com.cloudera.impala.service.Coordinator;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestUtils;

public class DataErrorsTest {
  private static Catalog catalog;
  private static Coordinator coordinator;
  private static StringBuilder testErrorLog;
  private final String testDir = "DataErrorsTest";

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
      ArrayList<String> expectedErrors = queryFileParser.getExpectedResult(0);
      // The test file is assumed to contain all errors. We may only want to compare a few of them.
      int errorsToCompare = Math.min(expectedErrors.size(), maxErrors);
      int lastLine = 0;
      int errorCount = 0;
      for (String line : expectedErrors) {
        // Indicates the last line of one error message.
        if (line.startsWith("line:")) {
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
      // Parse expected file errors.
      ArrayList<String> expectedFileErrorStrings = queryFileParser.getExpectedResult(1);
      Map<String, Integer> expectedFileErrors = new HashMap<String, Integer>();
      for (String fileError : expectedFileErrorStrings) {
        String[] fileErrorSplits = fileError.split(",");
        Integer numErrors = null;
        try {
          numErrors = Integer.parseInt(fileErrorSplits[1]);
          // We expect only one error for the first file if abort is requested.
          if (abortOnError) {
            expectedFileErrors.put(fileErrorSplits[0], 1);
            break;
          }
          expectedFileErrors.put(fileErrorSplits[0], numErrors);
        } catch (Exception e) {
          fail("Could not parse file error string: " + fileError + "\n");
        }
      }
      TestUtils.runQuery(coordinator, query, abortOnError, maxErrors, null, null, expectedErrors,
          expectedFileErrors, testErrorLog);
    }
    queryFileParser.close();
  }

  @Test
  public void Test() {
    runTests("textscannode-errors", false, 100);
    runTests("textscannode-errors", false, 5);
    runTests("textscannode-errors", true, 1);
    if (testErrorLog.length() != 0) {
      fail(testErrorLog.toString());
    }
  }
}
