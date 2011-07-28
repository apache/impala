package com.cloudera.impala.testutil;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.service.Coordinator;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TQueryRequest;
import com.cloudera.impala.thrift.TResultRow;

public class TestUtils {
  private final static Logger LOG = LoggerFactory.getLogger(TestUtils.class);

  // Maps from uppercase type name to PrimitiveType
  private static Map<String, PrimitiveType> typeNameMap = new HashMap<String, PrimitiveType>();
  static {
    for(PrimitiveType type: PrimitiveType.values()) {
      typeNameMap.put(type.toString(), type);
    }
  }

  /**
   * Do a line-by-line comparison of actual and expected output.
   * Comparison of the individual lines ignores whitespace.
   *
   * @param actual
   * @param expected
   * @return an error message if actual does not match expected, "" otherwise.
   */
  public static String compareOutput(String[] actual, ArrayList<String> expected) {
    int mismatch = -1; // line w/ mismatch
    int maxLen = Math.min(actual.length, expected.size());
    for (int i = 0; i < maxLen; ++i) {
      // do a whitespace-insensitive comparison
      Scanner a = new Scanner(actual[i]);
      Scanner e = new Scanner(expected.get(i));
      while (a.hasNext() && e.hasNext()) {
        if (!a.next().equals(e.next())) {
          mismatch = i;
          break;
        }
      }
      if (mismatch != -1) {
        break;
      }
      if (a.hasNext() != e.hasNext()) {
        mismatch = i;
        break;
      }
    }
    if (mismatch == -1 && actual.length < expected.size()) {
      mismatch = actual.length;
    }

    if (mismatch != -1) {
      // print actual and expected, highlighting mismatch
      StringBuilder output =
          new StringBuilder("actual result doesn't match expected result:\n");
      for (int i = 0; i <= mismatch; ++i) {
        output.append(actual[i]).append("\n");
      }
      // underline mismatched line with "^^^..."
      for (int i = 0; i < actual[mismatch].length(); ++i) {
        output.append('^');
      }
      output.append("\n");
      for (int i = mismatch + 1; i < actual.length; ++i) {
        output.append(actual[i]).append("\n");
      }
      output.append("\nexpected:\n");
      for (String str : expected) {
        output.append(str).append("\n");
      }
      return output.toString();
    }

    if (actual.length > expected.size()) {
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
   *
   * @param actual
   * @param expected
   * @return an error message if actual does not match expected, "" otherwise.
   */
  public static String compareOutputTypes(List<PrimitiveType> actual, String[] expectedStrTypes) {
    if (actual.size() != expectedStrTypes.length) {
      return "Unequal number of types. Found: " + actual.size() + ". Expected: "
          + expectedStrTypes.length;
    }
    for (int i = 0; i < expectedStrTypes.length; ++i) {
      String upperCaseTypeStr = expectedStrTypes[i].toUpperCase();
      PrimitiveType expectedType = typeNameMap.get(upperCaseTypeStr.trim());
      if (actual.get(i) != expectedType) {
        return "Slot: " + i + ". Found: " + actual.get(i).toString() + ". Expected: "
            + upperCaseTypeStr;
      }
    }
    return "";
  }

  /**
   * Do an element-by-element comparison of actual and expected number of failures per file.
   *
   * @param actual
   * @param expected
   * @return an error message if actual does not match expected, "" otherwise.
   */
  public static String compareFileErrors(Map<String, Integer> actual, Map<String, Integer> expected) {
    if (actual.size() != expected.size()) {
      StringBuilder expectedMapStr = new StringBuilder();
      for (Map.Entry<String, Integer> entry : expected.entrySet()) {
        expectedMapStr.append(entry.getKey() + " " + entry.getValue() + "\n");
      }
      StringBuilder actualMapStr = new StringBuilder();
      for (Map.Entry<String, Integer> entry : actual.entrySet()) {
        actualMapStr.append(entry.getKey() + " " + entry.getValue() + "\n");
      }
      return "Unequal number of file errors. Found: " + actual.size() + ". Expected: "
          + expected.size() + "\nActual Map:\n" + actualMapStr.toString() +
          "\nExpected Map:\n" + expectedMapStr.toString();
    }

    // Iterate over expected and find actual.
    for (Map.Entry<String, Integer> entry : expected.entrySet()) {
      Integer numErrors = actual.get(entry.getKey());
      if (numErrors == null) {
        return "Expected a file error entry for: " + entry.getKey() + " but non actually found.";
      }
      if (numErrors.intValue() != entry.getValue().intValue()) {
        return "Expected " + entry.getValue().intValue() + " errors in file " + entry.getKey() +
        " but found " + numErrors.intValue() + " errors";
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
   *
   * @param coordinator
   *          Coordinator to run query with.
   * @param query
   *          Query to be executed.
   * @param abortOnError
   *          Indicates whether the query should abort if data errors are encountered.
   *          If abortOnError is true and expectedErrors is not null, then an ImpalaException is
   *          expected to occur during query execution. The actual and expected errors
   *          will be compared in a catch clause.
   * @param maxErrors
   *          Indicates the maximum number of errors to gather.
   * @param expectedTypes
   *          Expected types in query's select list. Ignored if null.
   * @param expectedResults
   *          Expected query results. Ignored if null.
   * @param expectedErrors
   *          Expected messages in error log. Ignored if null.
   * @param expectedFileErrors
   *          Expected number of errors per data file read from query. Ignored if null.
   * @param testErrorLog
   *          Records error messages of failed tests to be reported at the very end of a test run.
   * @return an error message if actual does not match expected, "" otherwise.
   */
  public static void runQuery(Coordinator coordinator, String query, boolean abortOnError, int maxErrors,
      ArrayList<String> expectedTypes, ArrayList<String> expectedResults,
      ArrayList<String> expectedErrors, Map<String, Integer> expectedFileErrors,
      StringBuilder testErrorLog) {
    LOG.info("running query " + query);
    TQueryRequest request = new TQueryRequest(query, true);
    ArrayList<String> errors = new ArrayList<String>();
    Map<String, Integer> fileErrors = new HashMap<String, Integer>();
    ArrayList<PrimitiveType> colTypes = new ArrayList<PrimitiveType>();
    ArrayList<String> colLabels = new ArrayList<String>();
    BlockingQueue<TResultRow> resultQueue = new LinkedBlockingQueue<TResultRow>();
    ArrayList<String> actualResults = new ArrayList<String>();
    try {
      coordinator.runQuery(request, colTypes, colLabels, abortOnError, maxErrors,
          errors, fileErrors, resultQueue);
    } catch (ImpalaException e) {
      // Compare errors if we are expecting some.
      if (abortOnError && expectedErrors != null) {
        compareErrors(query, errors, fileErrors, expectedErrors, expectedFileErrors, testErrorLog);
      } else {
        testErrorLog.append("error executing query '" + query + "':\n" + e.getMessage());
      }
      return;
    }

    // Check types filled in by RunQuery()
    if (expectedTypes != null) {
      String[] expectedTypesArr = expectedTypes.get(0).split(",");
      String typeResult = TestUtils.compareOutputTypes(colTypes, expectedTypesArr);
      if (!typeResult.isEmpty()) {
        testErrorLog.append("query:\n" + query + "\n" + typeResult);
        return;
      }
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
      for (TColumnValue val : resultRow.colVals) {
        line.append(val.stringVal);
        line.append(',');
      }
      // remove trailing ','
      line.deleteCharAt(line.length()-1);
      actualResults.add(line.toString());
    }
    // Compare expected results.
    if (expectedResults != null) {
      String[] actualResultsArray = new String[actualResults.size()];
      actualResults.toArray(actualResultsArray);
      String result = TestUtils.compareOutput(actualResultsArray, expectedResults);
      if (!result.isEmpty()) {
        testErrorLog.append("query:\n" + query + "\n" + result);
        return;
      }
    }
    compareErrors(query, errors, fileErrors, expectedErrors, expectedFileErrors, testErrorLog);
  }

  private static void compareErrors(String query,
      ArrayList<String> actualErrors, Map<String, Integer> actualFileErrors,
      ArrayList<String> expectedErrors, Map<String, Integer> expectedFileErrors,
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
      String[] actualErrorsArray = new String[splitErrors.size()];
      splitErrors.toArray(actualErrorsArray);
      String result = TestUtils.compareOutput(actualErrorsArray, expectedErrors);
      if (!result.isEmpty()) {
        testErrorLog.append("query:\n" + query + "\n" + result);
        return;
      }
    }
    // Compare expected errors per file.
    if (expectedFileErrors != null) {
      String fileErrorsResult = TestUtils.compareFileErrors(actualFileErrors, expectedFileErrors);
      if (!fileErrorsResult.isEmpty()) {
        fail("query:\n" + query + "\n" + fileErrorsResult);
      }
    }
  }
}
