// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.analysis;

import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.google.common.base.Preconditions;

public class ToSqlTest {

  private static Catalog catalog;
  @BeforeClass
  public static void setUp() throws Exception {
    catalog = new Catalog();
  }

  @AfterClass
  public static void cleanUp() {
    catalog.close();
  }

  private static AnalysisContext.AnalysisResult analyze(String query) {
    try {
      AnalysisContext analysisCtxt = new AnalysisContext(catalog);
      AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(query);
      Preconditions.checkNotNull(analysisResult.getStmt());
      return analysisResult;
    } catch (Exception e) {
      e.printStackTrace();
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
    testToSql("select id from alltypes where 5 between smallint_col and int_col",
        "SELECT id FROM alltypes WHERE 5 BETWEEN smallint_col AND int_col");
    testToSql("select id from alltypes where 5 not between smallint_col and int_col",
        "SELECT id FROM alltypes WHERE 5 NOT BETWEEN smallint_col AND int_col");
    testToSql("select id from alltypes where 5 in (smallint_col, int_col)",
        "SELECT id FROM alltypes WHERE 5 IN (smallint_col, int_col)");
    testToSql("select id from alltypes where 5 not in (smallint_col, int_col)",
        "SELECT id FROM alltypes WHERE 5 NOT IN (smallint_col, int_col)");
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

  @Test
  public void unionTest() {
    testToSql("select bool_col, int_col from alltypes " +
        "union select bool_col, int_col from alltypessmall " +
        "union select bool_col, bigint_col from alltypes",
        "SELECT bool_col, int_col FROM alltypes " +
        "UNION SELECT bool_col, int_col FROM alltypessmall " +
        "UNION SELECT bool_col, bigint_col FROM alltypes");
    testToSql("select bool_col, int_col from alltypes " +
        "union all select bool_col, int_col from alltypessmall " +
        "union all select bool_col, bigint_col from alltypes",
        "SELECT bool_col, int_col FROM alltypes " +
        "UNION ALL SELECT bool_col, int_col FROM alltypessmall " +
        "UNION ALL SELECT bool_col, bigint_col FROM alltypes");
    // With 'order by' and 'limit' on union, and also on last select.
    testToSql("(select bool_col, int_col from alltypes) " +
        "union all (select bool_col, int_col from alltypessmall) " +
        "union all (select bool_col, bigint_col from alltypes order by 1 limit 1) " +
        "order by int_col, bool_col limit 10",
        "SELECT bool_col, int_col FROM alltypes " +
        "UNION ALL SELECT bool_col, int_col FROM alltypessmall " +
        "UNION ALL SELECT bool_col, bigint_col FROM alltypes ORDER BY 1 ASC LIMIT 1 " +
        "ORDER BY int_col ASC, bool_col ASC LIMIT 10");
    // With 'order by' and 'limit' on union but not on last select.
    testToSql("select bool_col, int_col from alltypes " +
        "union all select bool_col, int_col from alltypessmall " +
        "union all (select bool_col, bigint_col from alltypes) " +
        "order by int_col, bool_col limit 10",
        "SELECT bool_col, int_col FROM alltypes " +
        "UNION ALL SELECT bool_col, int_col FROM alltypessmall " +
        "UNION ALL (SELECT bool_col, bigint_col FROM alltypes) " +
        "ORDER BY int_col ASC, bool_col ASC LIMIT 10");
    // Nested unions require parenthesis.
    testToSql("select bool_col, int_col from alltypes " +
        "union all (select bool_col, int_col from alltypessmall " +
        "union distinct (select bool_col, bigint_col from alltypes)) " +
        "order by int_col, bool_col limit 10",
        "SELECT bool_col, int_col FROM alltypes UNION ALL " +
        "(SELECT bool_col, int_col FROM alltypessmall " +
        "UNION SELECT bool_col, bigint_col FROM alltypes) " +
        "ORDER BY int_col ASC, bool_col ASC LIMIT 10");
  }

  // Test the toSql() output of insert queries.
  @Test
  public void insertTest() {
    // Insert into unpartitioned table without partition clause.
    testToSql("insert into table alltypesnopart " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes",
        "INSERT INTO TABLE alltypesnopart SELECT id, bool_col, tinyint_col, " +
        "smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, " +
        "string_col, timestamp_col FROM alltypes");
    // Insert into overwrite unpartitioned table without partition clause.
    testToSql("insert overwrite table alltypesnopart " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes",
        "INSERT OVERWRITE TABLE alltypesnopart SELECT id, bool_col, tinyint_col, " +
        "smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, " +
        "string_col, timestamp_col FROM alltypes");
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
