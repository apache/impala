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
  private final ArrayList<String> colLabels_;
  private final ArrayList<String> colTypes_;
  private final ArrayList<String> errors_;
  private final ArrayList<String> fileErrors_;
  private final ArrayList<String> numAppendedRows_;
  private final ArrayList<String> modifiedPartitions_;
  private final ArrayList<String> query_;
  private final ArrayList<String> resultSet_;
  private final ArrayList<String> setup_;

  public QueryExecTestResult() {
    this.colTypes_ = Lists.newArrayList();
    this.colLabels_ = Lists.newArrayList();
    this.errors_ = Lists.newArrayList();
    this.fileErrors_ = Lists.newArrayList();
    this.numAppendedRows_ = Lists.newArrayList();
    this.modifiedPartitions_ = Lists.newArrayList();
    this.query_ = Lists.newArrayList();
    this.resultSet_ = Lists.newArrayList();
    this.setup_ = Lists.newArrayList();
  }

  /**
   * The column types from a SELECT
   */
  public ArrayList<String> getColTypes() { return colTypes_; }

  /**
   * The column labels from a SELECT
   */
  public ArrayList<String> getColLabels() { return colLabels_; }

  /**
   * Expected messages in error log.
   */
  public ArrayList<String> getErrors() { return errors_; }

  /**
   * Expected number of errors per data file read from query
   */
  public ArrayList<String> getFileErrors() { return fileErrors_; }

  /**
   * Number of partitions affected by an insert, including the number of rows written,
   * in the form /k1=v1/k2=v2: <num rows>
   */
  public ArrayList<String> getModifiedPartitions() { return modifiedPartitions_; }

  /**
   * The query the results are associated with
   */
  public ArrayList<String> getQuery() { return query_; }

  /**
   * The result set from a SELECT statement
   */
  public ArrayList<String> getResultSet() { return resultSet_; }

  /**
   * Setup section
   */
  public ArrayList<String> getSetup() { return setup_; }
}
