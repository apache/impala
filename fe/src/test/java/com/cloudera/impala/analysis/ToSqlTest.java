// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.analysis;

import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.service.Executor;
import com.cloudera.impala.thrift.TQueryRequest;

public class ToSqlTest {

  private static Executor executor;

  @BeforeClass
  public static void setUp() throws Exception {
    Catalog catalog = new Catalog();
    executor = new Executor(catalog);
  }

  private static AnalysisContext.AnalysisResult analyze(String query) {
    try {
      TQueryRequest tqueryRequest = new TQueryRequest(query, false, 1);
      ArrayList<PrimitiveType> colTypes = new ArrayList<PrimitiveType>();
      ArrayList<String> colLabels = new ArrayList<String>();
      return executor.analyzeQuery(tqueryRequest, colTypes, colLabels);
    } catch (Exception e) {
      fail("Failed to analyze query: " + query + "\n" + e.getMessage());
    }
    return null;
  }

  private void testToSql(String query, String expected) {
    AnalysisContext.AnalysisResult analysisResult = analyze(query);
    String actual = analysisResult.getStmt().toSql();
    if (!actual.equals(expected)) {
      fail("Expected: " + expected + "\n Actual: " + actual + "\n");
    }
  }

  // Test the toSql() output of select list expressions.
  @Test
  public void selectListTest() {
    testToSql("select 1234, 1234.0, 1234.0 + 1, 1234.0 + 1.0, 1 + 1, \"abc\" from alltypes",
        "SELECT 1234, 1234.0, 1234.0 + 1.0, 1234.0 + 1.0, 1 + 1, 'abc' FROM alltypes");
    testToSql("select null, 1234 < 5678, 1234.0 < 5678.0, 1234 < null from alltypes",
        "SELECT NULL, 1234 < 5678, 1234.0 < 5678.0, 1234 < NULL FROM alltypes");
    testToSql("select int_col + int_col, " +
        "tinyint_col + int_col, " +
        "float_col + double_col, " +
        "float_col + bigint_col, " +
        "cast(float_col as int), " +
        "bool_col " +
        "from alltypes",
        "SELECT int_col + int_col, " +
        "tinyint_col + int_col, " +
        "float_col + double_col, " +
        "float_col + bigint_col, " +
        "CAST(float_col AS INT), " +
        "bool_col " +
        "FROM alltypes");
    // TODO: test boolean expressions and date literals
  }

  // Test the toSql() output of the where clause.
  @Test
  public void whereTest() {
    testToSql("select id from alltypes where tinyint_col < 40 OR int_col = 4 AND float_col > 1.4",
        "SELECT id FROM alltypes WHERE tinyint_col < 40 OR int_col = 4 AND float_col > 1.4");
    testToSql("select id from alltypes where string_col = \"abc\"",
        "SELECT id FROM alltypes WHERE string_col = 'abc'");
    testToSql("select id from alltypes where string_col = 'abc'",
        "SELECT id FROM alltypes WHERE string_col = 'abc'");
  }

  // Test the toSql() output of aggregate and group by expressions.
  @Test
  public void aggregationTest() {
    testToSql("select COUNT(*), count(id), COUNT(id), SUM(id), AVG(id) from alltypes " +
        "group by tinyint_col",
        "SELECT COUNT(*), COUNT(id), COUNT(id), SUM(id), AVG(id) FROM alltypes " +
        "GROUP BY tinyint_col");
    testToSql("select avg(float_col / id) from alltypes group by tinyint_col",
        "SELECT AVG(float_col / id) " +
        "FROM alltypes GROUP BY tinyint_col");
    testToSql("select avg(double_col) from alltypes group by int_col, tinyint_col, bigint_col",
        "SELECT AVG(double_col) FROM alltypes GROUP BY int_col, tinyint_col, bigint_col");
    // Group by with having clause
    testToSql("select avg(id) from alltypes group by tinyint_col having count(tinyint_col) > 10",
        "SELECT AVG(id) FROM alltypes GROUP BY tinyint_col HAVING COUNT(tinyint_col) > 10");
    testToSql("select sum(id) from alltypes group by tinyint_col " +
        "having avg(tinyint_col) > 10 AND count(tinyint_col) > 5",
        "SELECT SUM(id) FROM alltypes GROUP BY tinyint_col " +
        "HAVING AVG(tinyint_col) > 10 AND COUNT(tinyint_col) > 5");
  }

  // Test the toSql() output of the order by clause.
  @Test
  public void orderByTest() {
    testToSql("select id, string_col from alltypes " +
        "order by string_col ASC, float_col DESC, int_col ASC",
        "SELECT id, string_col FROM alltypes " +
        "ORDER BY string_col ASC, float_col DESC, int_col ASC");
    testToSql("select id, string_col from alltypes " +
        "order by string_col DESC, float_col ASC, int_col DESC",
        "SELECT id, string_col FROM alltypes " +
        "ORDER BY string_col DESC, float_col ASC, int_col DESC");
  }

  // Test the toSql() output of queries with all clauses.
  @Test
  public void allTest() {
    testToSql("select bigint_col, avg(double_col), sum(tinyint_col) from alltypes " +
        "where double_col > 2.5 AND string_col != \"abc\"" +
        "group by bigint_col, int_col " +
        "having count(int_col) > 10 OR sum(bigint_col) > 20 " +
        "order by 2 DESC, 3 ASC",
        "SELECT bigint_col, AVG(double_col), SUM(tinyint_col) FROM alltypes " +
        "WHERE double_col > 2.5 AND string_col != 'abc' " +
        "GROUP BY bigint_col, int_col " +
        "HAVING COUNT(int_col) > 10 OR SUM(bigint_col) > 20 " +
        "ORDER BY 2 DESC, 3 ASC");
  }

  // Test the toSql() output of insert queries.
  @Test
  public void insertTest() {
    // Insert into unpartitioned table without partition clause.
    testToSql("insert into table alltypesnopart " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col from alltypes",
        "INSERT INTO TABLE alltypesnopart SELECT id, bool_col, tinyint_col, " +
        "smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, " +
        "string_col FROM alltypes");
    // Insert into overwrite unpartitioned table without partition clause.
    testToSql("insert overwrite table alltypesnopart " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col from alltypes",
        "INSERT OVERWRITE TABLE alltypesnopart SELECT id, bool_col, tinyint_col, " +
        "smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, " +
        "string_col FROM alltypes");
    // Static partition.
    testToSql("insert into table alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes",
        "INSERT INTO TABLE alltypessmall PARTITION (year=2009, month=4) SELECT id, " +
        "bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, " +
        "double_col, date_string_col, string_col, timestamp_col FROM alltypes");
    // Fully dynamic partitions.
    testToSql("insert into table alltypessmall " +
        "partition (year, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year, month from alltypes",
        "INSERT INTO TABLE alltypessmall PARTITION (year, month) SELECT id, bool_col, " +
        "tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, " +
        "date_string_col, string_col, timestamp_col, year, month FROM alltypes");
    // Partially dynamic partitions.
    testToSql("insert into table alltypessmall " +
        "partition (year=2009, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, month from alltypes",
        "INSERT INTO TABLE alltypessmall PARTITION (year=2009, month) SELECT id, " +
        "bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, " +
        "double_col, date_string_col, string_col, timestamp_col, month FROM alltypes");
  }
}
