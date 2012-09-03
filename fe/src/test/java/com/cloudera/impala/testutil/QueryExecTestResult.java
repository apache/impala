package com.cloudera.impala.testutil;

import java.util.ArrayList;

import com.google.common.collect.Lists;

/**
 * This class describes the result of an Impala query. It contains information such as
 * the result set returned, the column types, etc. It is useful for tests to describe an
 * expected and an actual result. Currently, all fields are stored as Lists of Strings
 * because it helps map this file to lines in a query test file (each element is a line).
 *
 * TODO: Consider pushing the actual comparison logic of results into this class.
 */
public class QueryExecTestResult {
  private final ArrayList<String> colLabels;
  private final ArrayList<String> colTypes;
  private final ArrayList<String> errors;
  private final ArrayList<String> fileErrors;
  private final ArrayList<String> numAppendedRows;
  private final ArrayList<String> modifiedPartitions;
  private final ArrayList<String> query;
  private final ArrayList<String> resultSet;
  private final ArrayList<String> setup;

  public QueryExecTestResult() {
    this.colTypes = Lists.newArrayList();
    this.colLabels = Lists.newArrayList();
    this.errors = Lists.newArrayList();
    this.fileErrors = Lists.newArrayList();
    this.numAppendedRows = Lists.newArrayList();
    this.modifiedPartitions = Lists.newArrayList();
    this.query = Lists.newArrayList();
    this.resultSet = Lists.newArrayList();
    this.setup = Lists.newArrayList();
  }

  /**
   * The column types from a SELECT
   */
  public ArrayList<String> getColTypes() {
    return colTypes;
  }

  /**
   * The column labels from a SELECT
   */
  public ArrayList<String> getColLabels() {
    return colLabels;
  }

  /**
   * Expected messages in error log.
   */
  public ArrayList<String> getErrors() {
    return errors;
  }

  /**
   * Expected number of errors per data file read from query
   */
  public ArrayList<String> getFileErrors() {
    return fileErrors;
  }

  /**
   * Number of partitions affected by an insert, including the number of rows written,
   * in the form /k1=v1/k2=v2: <num rows>
   */
  public ArrayList<String> getModifiedPartitions() {
    return modifiedPartitions;
  }

  /**
   * The query the results are associated with
   */
  public ArrayList<String> getQuery() {
    return query;
  }

  /**
   * The result set from a SELECT statement
   */
  public ArrayList<String> getResultSet() {
    return resultSet;
  }

  /**
   * Setup section
   */
  public ArrayList<String> getSetup() {
    return setup;
  }
}
