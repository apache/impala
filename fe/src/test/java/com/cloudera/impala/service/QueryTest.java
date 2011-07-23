// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.TestSchemaUtils;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestUtils;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TQueryRequest;
import com.cloudera.impala.thrift.TResultRow;

public class QueryTest {
  private final static Logger LOG = LoggerFactory.getLogger(QueryTest.class);

  private static Catalog catalog;
  private static Coordinator coordinator;
  private final String testDir = "ServiceTest";
  private static StringBuilder errorLog;

  @BeforeClass
  public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createClient();
    catalog = new Catalog(client);
    coordinator = new Coordinator(catalog);
    errorLog = new StringBuilder();
  }

  private void runQuery(String query, ArrayList<String> expectedTypes,
                        ArrayList<String> expectedResults) {
    LOG.info("running query " + query);
    TQueryRequest request = new TQueryRequest(query, true);
    List<PrimitiveType> colTypes = new ArrayList<PrimitiveType>();
    List<String> colLabels = new ArrayList<String>();
    BlockingQueue<TResultRow> resultQueue = new LinkedBlockingQueue<TResultRow>();
    ArrayList<String> actualResults = new ArrayList<String>();
    try {
      coordinator.runQuery(request, colTypes, colLabels, resultQueue);
    } catch (ImpalaException e) {
      errorLog.append("error executing query '" + query + "':\n" + e.getMessage());
      return;
    }

    // Check types filled in by RunQuery()
    String[] expectedTypesArr = expectedTypes.get(0).split(",");
    String typeResult = TestUtils.compareOutputTypes(colTypes, expectedTypesArr);
    if (!typeResult.isEmpty()) {
      errorLog.append("query:\n" + query + "\n" + typeResult);
      return;
    }

    while (true) {
      TResultRow resultRow = null;
      try {
        resultRow = resultQueue.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
        errorLog.append("unexpected interrupt");
        return;
      }
      if (resultRow.colVals == null) {
        break;
      }

      // Concatenate columns separated by ","
      StringBuilder line = new StringBuilder();
      for (TColumnValue val : resultRow.colVals) {
        line.append(val.stringVal);
        line.append(',');
      }
      // remove trailing ','
      line.deleteCharAt(line.length()-1);
      actualResults.add(line.toString());
    }
    String[] actualResultsArray = new String[actualResults.size()];
    actualResults.toArray(actualResultsArray);
    String result = TestUtils.compareOutput(actualResultsArray, expectedResults);
    if (!result.isEmpty()) {
      errorLog.append("query:\n" + query + "\n" + result);
      return;
    }
  }


  private void runTests(String testCase) {
    String fileName = testDir + "/" + testCase + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    queryFileParser.open();
    while (queryFileParser.hasNext()) {
      queryFileParser.next();
      String query = queryFileParser.getQuery();
      ArrayList<String> expectedTypes = queryFileParser.getExpectedResult(0);
      ArrayList<String> expectedResults = queryFileParser.getExpectedResult(1);
      runQuery(query, expectedTypes, expectedResults);
    }
    queryFileParser.close();
  }

  @Test
  public void Test() {
    runTests("aggregation");
    runTests("textscannode");
    if (errorLog.length() != 0) {
      fail(errorLog.toString());
    }
  }
}
