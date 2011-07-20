// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.TestSchemaUtils;
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

  @BeforeClass
  public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createClient();
    catalog = new Catalog(client);
    coordinator = new Coordinator(catalog);
  }

  private void RunQuery(String query, ArrayList<String> expectedTypes, ArrayList<String> expectedResults) {
    try {
      LOG.info("running query " + query);
      TQueryRequest request = new TQueryRequest(query, true);
      List<PrimitiveType> colTypes = new ArrayList<PrimitiveType>();
      BlockingQueue<TResultRow> resultQueue = new LinkedBlockingQueue<TResultRow>();
      ArrayList<String> actualResults = new ArrayList<String>();
      coordinator.RunQuery(request, colTypes, resultQueue);
      // Check types filled in by RunQuery()
      String[] expectedTypesArr = expectedTypes.get(0).split(",");
      String typeResult = TestUtils.compareOutputTypes(colTypes, expectedTypesArr);
      if (!typeResult.isEmpty()) {
        fail("query:\n" + query + "\n" + typeResult);
      }
      while (true) {
        TResultRow resultRow = null;
        try {
          // We use a timeout here, because it is conceivable that the c++ backend
          // has not written anything to the queue yet by the time we reach this piece of code.
          // In that case we would report an empty query result.
          // By adding a timeout we poll the queue until the timeout is reached.
          resultRow = resultQueue.poll(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (resultRow == null) {
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
        fail("query:\n" + query + "\n" + result);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void RunTests(String testCase) {
    String fileName = testDir + "/" + testCase + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    queryFileParser.open();
    while (queryFileParser.hasNext()) {
      queryFileParser.next();
      String query = queryFileParser.getQuery();
      ArrayList<String> expectedTypes = queryFileParser.getExpectedResult(0);
      ArrayList<String> expectedResults = queryFileParser.getExpectedResult(1);
      RunQuery(query, expectedTypes, expectedResults);
    }
    queryFileParser.close();
  }

  @Test
  public void Test() {
    RunTests("textscannode");
  }
}
