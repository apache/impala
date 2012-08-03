package com.cloudera.impala.testutil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Joiner;

/**
 * Miscellaneous utility methods related to interacting with Test Files. This is a more
 * specialized utility class than the more generic 'TestUtils' class.
 */
public class TestFileUtils {

  private static void appendTestCaseString(StringBuilder sb,
      QueryExecTestResult queryResult) {
    sb.append(Joiner.on("\n").join(queryResult.getQuery()) + "\n");

    if(queryResult.getSetup().size() > 0) {
      sb.append("---- SETUP\n");
      sb.append(Joiner.on("\n").join(queryResult.getSetup()) + "\n");
    }

    if (queryResult.getColTypes().size() > 0) {
      sb.append("---- TYPES\n");
      sb.append(Joiner.on(", ").join(queryResult.getColTypes()) + "\n");
    }

    sb.append("---- RESULTS\n");
    sb.append(Joiner.on("\n").join(queryResult.getResultSet()));
    if (queryResult.getResultSet().size() > 0) {
      sb.append("\n");
    }

    if(queryResult.getModifiedPartitions().size() > 0) {
      sb.append("---- PARTITIONS\n");
      sb.append(Joiner.on("\n").join(queryResult.getModifiedPartitions()) + "\n");
    }
    if(queryResult.getNumAppendedRows().size() > 0) {
      sb.append("---- NUMROWS\n");
      sb.append(Joiner.on("\n").join(queryResult.getNumAppendedRows()) + "\n");
    }
  }

  /**
   * Utility for updating a test result file.
   * TODO: Currently only supports query test. Could be extended in the future to
   * support Planner and Data Error tests.
   */
  public static void saveUpdatedResults(String outputFilePath,
      List<QueryExecTestResult> newResults) throws IOException {

    StringBuilder sb = new StringBuilder();
    for (QueryExecTestResult newQueryResult : newResults) {
      appendTestCaseString(sb, newQueryResult);
      sb.append("====\n");
    }
    File outputFile = new File(outputFilePath);
    // Make the parent directories
    new File(outputFile.getParent()).mkdirs();

    FileWriter fw = new FileWriter(outputFile);
    try {
      fw.write(sb.toString());
    } finally {
      fw.close();
    }
  }

  /**
   * Returns the base directory for test files.
   */
  public static String getTestFileBaseDir() {
    return new File(System.getenv("IMPALA_HOME"), "testdata/workloads").getPath();
  }
}