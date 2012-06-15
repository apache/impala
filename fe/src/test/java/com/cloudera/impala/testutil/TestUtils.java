// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.testutil;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.service.Executor;
import com.cloudera.impala.service.InsertResult;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TQueryRequest;
import com.cloudera.impala.thrift.TResultRow;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class TestUtils {
  private final static Logger LOG = LoggerFactory.getLogger(TestUtils.class);
  private final static String[] expectedFilePrefix = { "hdfs:" };
  private final static String[] ignoreContentAfter = { "HOST:" };

  // Maps from uppercase type name to PrimitiveType
  private static Map<String, PrimitiveType> typeNameMap =
      new HashMap<String, PrimitiveType>();
  static {
    for(PrimitiveType type: PrimitiveType.values()) {
      typeNameMap.put(type.toString(), type);
    }
  }

  /**
   * Do a line-by-line comparison of actual and expected output.
   * Comparison of the individual lines ignores whitespace.
   * If an expected line starts with expectedFilePrefix,
   * then the expected vs. actual comparison is successful if the actual string contains
   * the expected line (ignoring the expectedFilePrefix prefix).
   * If orderMatters is false, we consider actual to match expected if they
   * both contains the same output lines regardless of order.
   *
   * @return an error message if actual does not match expected, "" otherwise.
   */
  public static String compareOutput(
      ArrayList<String> actual, ArrayList<String> expected, boolean orderMatters) {
    if (!orderMatters) {
      Collections.sort(actual);
      Collections.sort(expected);
    }
    int mismatch = -1; // line in actual w/ mismatch
    int maxLen = Math.min(actual.size(), expected.size());
    for (int i = 0; i < maxLen; ++i) {
      String expectedStr = expected.get(i);
      String actualStr = actual.get(i);
      // Look for special prefixes in containsPrefixes.
      boolean containsPrefix = false;
      for (int prefixIdx = 0; prefixIdx < expectedFilePrefix.length; ++prefixIdx) {
        containsPrefix = expectedStr.trim().startsWith(expectedFilePrefix[prefixIdx]);
        if (containsPrefix) {
          expectedStr = expectedStr.replaceFirst(expectedFilePrefix[prefixIdx], "");
          actualStr = actualStr.replaceFirst(expectedFilePrefix[prefixIdx], "");
          break;
        }
      }

      boolean ignoreAfter = false;
      for (int icIdx = 0; icIdx < ignoreContentAfter.length; ++icIdx) {
        ignoreAfter |= expectedStr.trim().startsWith(ignoreContentAfter[icIdx]);
      }

      // do a whitespace-insensitive comparison
      Scanner e = new Scanner(expectedStr);
      Scanner a = new Scanner(actualStr);
      while (a.hasNext() && e.hasNext()) {
        if (containsPrefix) {
          if (!a.next().contains(e.next())) {
            mismatch = i;
            break;
          }
        } else {
          if (!a.next().equals(e.next())) {
            mismatch = i;
            break;
          }
        }
      }
      if (mismatch != -1) {
        break;
      }

      if (ignoreAfter) {
        if (e.hasNext() && !a.hasNext()) {
          mismatch = i;
          break;
        }
      } else if (a.hasNext() != e.hasNext()) {
        mismatch = i;
        break;
      }
    }
    if (mismatch == -1 && actual.size() < expected.size()) {
      // actual is a prefix of expected
      StringBuilder output =
          new StringBuilder("actual result is missing lines:\n");
      for (int i = 0; i < actual.size(); ++i) {
        output.append(actual.get(i)).append("\n");
      }
      output.append("missing:\n");
      for (int i = actual.size(); i < expected.size(); ++i) {
        output.append(expected.get(i)).append("\n");
      }
      return output.toString();
    }

    if (mismatch != -1) {
      // print actual and expected, highlighting mismatch
      StringBuilder output =
          new StringBuilder("actual result doesn't match expected result:\n");
      for (int i = 0; i <= mismatch; ++i) {
        output.append(actual.get(i)).append("\n");
      }
      // underline mismatched line with "^^^..."
      for (int i = 0; i < actual.get(mismatch).length(); ++i) {
        output.append('^');
      }
      output.append("\n");
      for (int i = mismatch + 1; i < actual.size(); ++i) {
        output.append(actual.get(i)).append("\n");
      }
      output.append("\nexpected:\n");
      for (String str : expected) {
        output.append(str).append("\n");
      }
      return output.toString();
    }

    if (actual.size() > expected.size()) {
      // print actual and expected
      StringBuilder output =
          new StringBuilder("actual result contains extra output:\n");
      for (String str : actual) {
        output.append(str).append("\n");
      }
      output.append("\nexpected:\n");
      for (String str : expected) {
        output.append(str).append("\n");
      }
      return output.toString();
    }

    return "";
  }

  /**
   * Do an element-by-element comparison of actual and expected types.
   * @return an error message if actual does not match expected, "" otherwise.
   */
  private static String compareOutputTypes(
      List<PrimitiveType> actual, String[] expectedStrTypes) {
    if (actual.size() != expectedStrTypes.length) {
      return "Unequal number of output types.\nFound: " + actual.toString()
          + ".\nExpected: " + Arrays.toString(expectedStrTypes) + "\n";
    }
    for (int i = 0; i < expectedStrTypes.length; ++i) {
      String upperCaseTypeStr = expectedStrTypes[i].toUpperCase();
      PrimitiveType expectedType = typeNameMap.get(upperCaseTypeStr.trim());
      if (actual.get(i) != expectedType) {
        return "Mismatched output types.\nFound: " + actual.toString() +
               ".\nExpected: " + Arrays.toString(expectedStrTypes) + "\n";
      }
    }
    return "";
  }

  /**
   * Executes query, retrieving query results and error messages.
   * Compares the following actual vs. expected outputs:
   * 1. Actual and expected types of exprs in the query's select list.
   * 2. Actual and expected query results if expectedResults is non-null.
   * 3. Actual and expected errors if expectedErrors and expectedFileErrors are non-null.
   * 4. Actual and expected partitions and number of appended rows if they are non-null.
   *
   * @param executor
   *          Coordinator to run query with.
   * @param query
   *          Query to be executed.
   * @param context
   *          The context of how to execute the query. Contains details such as the
   *          number of execution nodes, if codegen is enabled, etc...
   * @param abortOnError
   *          Indicates whether the query should abort if data errors are encountered.
   *          If abortOnError is true and expectedErrors is not null, then an
   *          ImpalaException is expected to occur during query execution. The actual
   *          and expected errors will be compared in a catch clause.
   * @param maxErrors
   *          Indicates the maximum number of errors to gather.
   * @param lineNum
   *          Line number in test file on which this test query started.
   * @param expectedColLabels
   *          Expected column labels.
   * @param expectedTypes
   *          Expected types in query's select list. Ignored if null.
   * @param expectedResults
   *          Expected query results. Ignored if null.
   * @param expectedErrors
   *          Expected messages in error log. Ignored if null.
   * @param expectedFileErrors
   *          Expected number of errors per data file read from query. Ignored if null.
   * @param expectedPartitions
   *          Expected partitions created by an insert.
   * @param expectedNumAppendedRows
   *          Expected number of rows per partition file.
   * @param testErrorLog
   *          Records error messages of failed tests to be reported at the very end of
   *          a test run.
   * @return an error message if actual does not match expected, "" otherwise.
   */
  public static void runQueryUsingExecutor(Object executor, String query,
      TestExecContext context, int lineNum,
      ArrayList<String> expectedColLabels, ArrayList<String> expectedTypes,
      ArrayList<String> expectedResults, ArrayList<String> expectedErrors,
      ArrayList<String> expectedFileErrors, ArrayList<String> expectedPartitions,
      ArrayList<String> expectedNumAppendedRows,
      StringBuilder testErrorLog) {

    Preconditions.checkNotNull(executor);
    if (executor instanceof ImpaladClientExecutor) {
      runImpaladQuery(
          (ImpaladClientExecutor) executor, query, context, lineNum, expectedColLabels,
          expectedTypes, expectedResults, expectedErrors, expectedFileErrors,
          expectedPartitions, expectedNumAppendedRows, testErrorLog);
    } else if (executor instanceof Executor) {
      runInProcessQuery(
          (Executor) executor, query, context, lineNum, expectedColLabels, expectedTypes,
          expectedResults, expectedErrors, expectedFileErrors, expectedPartitions,
          expectedNumAppendedRows, testErrorLog);
    } else {
      fail(String.format("Unknown executor type: '%s'", executor.getClass().getName()));
    }
  }

  /**
   * Runs the given query against an Impalad instance and validates expected results.
   */
  private static void runImpaladQuery(ImpaladClientExecutor executor, String query,
      TestExecContext context, int lineNum,
      ArrayList<String> expectedColLabels, ArrayList<String> expectedTypes,
      ArrayList<String> expectedResults, ArrayList<String> expectedErrors,
      ArrayList<String> expectedFileErrors, ArrayList<String> expectedPartitions,
      ArrayList<String> expectedNumAppendedRows, StringBuilder testErrorLog) {
    String queryReportString = buildQueryDetailString(query, context);
    LOG.info("running query targeting impalad " + queryReportString);

    BlockingQueue<String> resultQueue = new LinkedBlockingQueue<String>();
    ArrayList<String> colLabels = new ArrayList<String>();
    ArrayList<String> colTypes = new ArrayList<String>();

    try {
      executor.runQuery(query, resultQueue, colTypes, colLabels);
    } catch(Exception e) {
      e.printStackTrace();
      if (context.getAbortOnError()) {
        fail(String.format("Query failed. Message: %s", e.getMessage()));
      } else {
        testErrorLog.append("Error executing query '" +
                            query + "' on line " + lineNum + ":\n" + e.getMessage());
      }
      return;
    }

    ArrayList<String> actualResults = Lists.newArrayList();

    while (!resultQueue.isEmpty()) {
      String resultRow = null;
      try {
        resultRow = resultQueue.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
        testErrorLog.append("unexpected interrupt");
        return;
      }

      parseResultRow(resultRow, actualResults, colTypes);
    }

    verifyTypesLabelsAndResults(queryReportString, testErrorLog, colLabels,
        expectedColLabels, stringTypeToPrimitiveType(colTypes),
        expectedTypes, actualResults, expectedResults);
  }



  /**
   * Executes the given query using the given in-process Executor.
   */
  private static void runInProcessQuery(
      Executor inProcessExecutor, String query, TestExecContext context, int lineNum,
      ArrayList<String> expectedColLabels, ArrayList<String> expectedTypes,
      ArrayList<String> expectedResults, ArrayList<String> expectedErrors,
      ArrayList<String> expectedFileErrors, ArrayList<String> expectedPartitions,
      ArrayList<String> expectedNumAppendedRows, StringBuilder testErrorLog) {

    String queryReportString = buildQueryDetailString(query, context);
    LOG.info("running query " + queryReportString);
    TQueryRequest request = new TQueryRequest(query, true, context.getNumNodes());
    ArrayList<String> errors = new ArrayList<String>();
    SortedMap<String, Integer> fileErrors = new TreeMap<String, Integer>();
    ArrayList<PrimitiveType> colTypes = new ArrayList<PrimitiveType>();
    ArrayList<String> colLabels = new ArrayList<String>();
    AtomicBoolean containsOrderBy = new AtomicBoolean();
    BlockingQueue<TResultRow> resultQueue = new LinkedBlockingQueue<TResultRow>();
    InsertResult insertResult = new InsertResult();
    ArrayList<String> actualResults = new ArrayList<String>();
    try {
      inProcessExecutor.runQuery(request, colTypes, colLabels, containsOrderBy,
          context.getBatchSize(), context.getAbortOnError(), context.getMaxErrors(),
          context.isCodegenDisabled(), errors, fileErrors, resultQueue,
          insertResult);
    } catch (ImpalaException e) {
      // Compare errors if we are expecting some.
      if (context.getAbortOnError() && expectedErrors != null) {
        compareErrors(queryReportString, errors, fileErrors, expectedErrors,
                      expectedFileErrors, testErrorLog);
      } else {
        testErrorLog.append(
            "Error executing query '" + query + "' on line " + lineNum
            + ":\n" + e.getMessage());
      }
      return;
    }

    // Check insert results for insert statements.
    // We check the num rows and the partitions independently,
    // because not all table types create new partition files (e.g., HBase tables).
    if (expectedNumAppendedRows != null) {
      ArrayList<String> actualResultsArray = Lists.newArrayList();
      for (int i = 0; i < insertResult.getRowsAppended().size(); ++i) {
        actualResultsArray.add(insertResult.getRowsAppended().get(i).toString());
      }
      String result =
        TestUtils.compareOutput(actualResultsArray, expectedNumAppendedRows, true);
      if (!result.isEmpty()) {
        testErrorLog.append("query:\n" + queryReportString  + "\n" + result);
        return;
      }
    }
    if (expectedPartitions != null) {
      ArrayList<String> modifiedPartitions =
        Lists.newArrayList(insertResult.getModifiedPartitions());
      String result =
        TestUtils.compareOutput(modifiedPartitions, expectedPartitions, false);
      if (!result.isEmpty()) {
        testErrorLog.append("query:\n" + queryReportString  + "\n" + result);
        return;
      }
    }
    // We only expect partitions and num rows for insert statements,
    // and we compared them above.
    if (expectedNumAppendedRows != null || expectedPartitions != null) {
      return;
    }

    while (true) {
      TResultRow resultRow = null;
      try {
        resultRow = resultQueue.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
        testErrorLog.append("unexpected interrupt");
        return;
      }
      if (resultRow.colVals == null) {
        break;
      }

      // Concatenate columns separated by ","
      StringBuilder line = new StringBuilder();
      Iterator<TColumnValue> colVal = resultRow.colVals.iterator();
      for (int i = 0; i < colTypes.size(); ++i) {
        if (i > 0) {
          line.append(',');
        }

        line.append(parseColumnValue(colVal.next().stringVal, colTypes.get(i)));
      }
      actualResults.add(line.toString());
    }

    verifyTypesLabelsAndResults(queryReportString, testErrorLog, colLabels,
        expectedColLabels, colTypes, expectedTypes, actualResults, expectedResults);

    compareErrors(queryReportString, errors, fileErrors, expectedErrors,
                  expectedFileErrors, testErrorLog);
  }

  private static void parseResultRow(String rawResultRow, List<String> results,
      List<String> colTypes) {
    // Concatenate columns separated by ","
    String[] resultColumns = rawResultRow.split("\t");
    Assert.assertEquals(resultColumns.length, colTypes.size());
    StringBuilder line = new StringBuilder();
    for (int i = 0; i < colTypes.size(); ++i) {
      if (i > 0) {
        line.append(',');
      }

      PrimitiveType columnType = typeNameMap.get(colTypes.get(i).toUpperCase().trim());
      line.append(parseColumnValue(resultColumns[i], columnType));
    }

    results.add(line.toString());
  }

  /**
   * Parses column value and returns the result in the format used by the results
   * file.
   */
  private static String parseColumnValue(String columnValue, PrimitiveType columnType) {
    switch(columnType) {
      case STRING:
        return String.format("'%s'", columnValue);
      default:
        return columnValue;
    }
  }

  private static String buildQueryDetailString(String queryString,
                                               TestExecContext context) {
    return String.format("%s (%s)", queryString, context.toString());
  }

  /**
   * Performs common verification between different types of Executors.
   */
  private static void verifyTypesLabelsAndResults(
      String queryReportString, StringBuilder testErrorLog,
      ArrayList<String> actualColLabels, ArrayList<String> expectedColLabels,
      ArrayList<PrimitiveType> actualColTypes, ArrayList<String> expectedColTypes,
      ArrayList<String> actualResults, ArrayList<String> expectedResults) {

    String result = compareColumnLabels(actualColLabels, expectedColLabels);
    if (!result.isEmpty()) {
      testErrorLog.append("query:\n" + queryReportString + "\n" + result);
      return;
    }

    result = compareExpectedTypes(actualColTypes, expectedColTypes);
    if (!result.isEmpty()) {
      testErrorLog.append("query:\n" + queryReportString + "\n" + result);
      return;
    }

    result = compareExpectedResults(actualResults, expectedResults, false);
    if (!result.isEmpty()) {
      testErrorLog.append("query:\n" + queryReportString  + "\n" + result);
    }
  }

  /**
   * Compares the actual and expected column labels, returning an error string
   * if they do not match.
   */
  private static String compareColumnLabels(ArrayList<String> actualColLabels,
                                            ArrayList<String> expectedColLabels) {
    return (expectedColLabels != null) ?
        TestUtils.compareOutput(actualColLabels, expectedColLabels, true) : "";
  }

  private static String compareExpectedTypes(ArrayList<PrimitiveType> actualTypes,
      ArrayList<String> expectedTypes) {
    // Check types filled in by RunQuery()
    if (expectedTypes != null) {
      String[] expectedTypesArr;
      if (expectedTypes.isEmpty()) {
        expectedTypesArr = new String[0];
      } else {
        expectedTypesArr = expectedTypes.get(0).split(",");
      }

      return TestUtils.compareOutputTypes(actualTypes, expectedTypesArr);
    }
    return "";
  }

  private static ArrayList<PrimitiveType> stringTypeToPrimitiveType(List<String> types) {
    ArrayList<PrimitiveType> typeList = new ArrayList<PrimitiveType>();
    for (String typeString : types) {
      typeList.add(typeNameMap.get(typeString.toUpperCase().trim()));
    }
    return typeList;
  }

  /**
   * Compares actual and expected results of a query execution.
   *
   * @param actualResults
   *        The actual results of executing the query
   * @param expectedResults
   *        The expected results of executing the query
   * @param containsOrderBy
   *        Whether the query contains an ORDER BY clause. If it does not then
   *        the results are sorted before comparison.
   * @return
   *        Returns an empty string if the results are the same, otherwise a string
   *        describing how the results differ.
   */
  private static String compareExpectedResults(ArrayList<String> actualResults,
      ArrayList<String> expectedResults, boolean containsOrderBy) {
    return (expectedResults != null) ?
        TestUtils.compareOutput(actualResults, expectedResults, containsOrderBy) : "";
  }

  private static void compareErrors(String query,
      ArrayList<String> actualErrors, SortedMap<String, Integer> actualFileErrors,
      ArrayList<String> expectedErrors, ArrayList<String> expectedFileErrors,
      StringBuilder testErrorLog) {
    // Compare expected messages in error log.
    if (expectedErrors != null) {
      // Split the error messages by newline to compare them against the expected errors.
      ArrayList<String> splitErrors = new ArrayList<String>();
      for (String err : actualErrors) {
        String[] lines = err.split("\n");
        for (String line : lines) {
          splitErrors.add(line);
        }
      }
      String result = TestUtils.compareOutput(splitErrors, expectedErrors, true);
      if (!result.isEmpty()) {
        testErrorLog.append("query:\n" + query + "\n" + result);
        return;
      }
    }
    // Compare expected errors per file.
    if (expectedFileErrors != null) {
      ArrayList<String> actualFileErrorsArray = Lists.newArrayList();
      for(SortedMap.Entry<String, Integer> entry : actualFileErrors.entrySet()) {
        actualFileErrorsArray.add("file: " + entry.getKey() + "," + entry.getValue());
      }
      String fileErrorsResult =
          TestUtils.compareOutput(actualFileErrorsArray, expectedFileErrors, true);
      if (!fileErrorsResult.isEmpty()) {
        fail("query:\n" + query + "\n" + fileErrorsResult);
      }
    }
  }
}