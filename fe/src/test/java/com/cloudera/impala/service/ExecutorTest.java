// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.common.ImpalaException;

public class ExecutorTest {

  // For buffering query results.
  private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
  private final PrintStream printStream = new PrintStream(outputStream);
  private final Catalog catalog = Executor.createCatalog();

  private void runTestSuccess(String query, int expectedRows)
      throws ImpalaException {
    // start at the beginning of the output stream for every test
    outputStream.reset();
    int syncNumRows = Executor.runQuery(query, catalog, false,
        Executor.DEFAULT_BATCH_SIZE, printStream);
    if (expectedRows != -1) {
      Assert.assertEquals(expectedRows, syncNumRows);
    }
    int asyncNumRows = Executor.runQuery(query, catalog, true,
        Executor.DEFAULT_BATCH_SIZE, printStream);
    if (expectedRows != -1) {
      Assert.assertEquals(expectedRows, asyncNumRows);
    }
  }

  private void runTestFailure(String query, int expectedRows) {
    // start at the beginning of the output stream for every test
    outputStream.reset();
    try {
      Executor.runQuery(query, catalog, false, Executor.DEFAULT_BATCH_SIZE, printStream);
      fail("Expected query to fail: " + query);
    } catch (Exception e) {
    }
    outputStream.reset();
    try {
      Executor.runQuery(query, catalog, true, Executor.DEFAULT_BATCH_SIZE, printStream);
      fail("Expected query to fail: " + query);
    } catch (Exception e) {
    }
  }

  @Test
  public void runTest() throws ImpalaException {
    runTestSuccess("select substring(\"Hello World\", 0)", 1);
    runTestSuccess("select int_col+bigint_col from alltypessmall limit 1", 1);
    runTestSuccess("select year, tinyint_col, int_col, id from alltypessmall", 100);
    runTestSuccess("select sum(double_col), count(double_col), avg(double_col) " +
                   "from alltypessmall", 1);

    // Syntax error.
    runTestFailure("slect tinyint_col from alltypessmall", 100);
    // Unknown column.
    runTestFailure("select tiny from alltypessmall", 100);
    // Different number of results.
    runTestFailure("select tiny from alltypessmall", 80);
  }
}
