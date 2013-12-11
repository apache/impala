// Copyright 2013 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.Catalog;
import com.google.common.base.Preconditions;

// TODO: Expand this test, in particular, because view creation relies
// on producing correct SQL.
public class ToSqlTest extends AnalyzerTest {

  private static AnalysisContext.AnalysisResult analyze(String query) {
    try {
      AnalysisContext analysisCtxt = new AnalysisContext(catalog,
          Catalog.DEFAULT_DB, new User(System.getProperty("user.name")));

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
    // Try to parse and analyze the resulting SQL to ensure its validity.
    AnalyzesOk(actual);
  }

 @Test
 public void selectListTest() {
   testToSql("select 1234, 1234.0, 1234.0 + 1, 1234.0 + 1.0, 1 + 1, \"abc\" " +
       "from functional.alltypes",
       "SELECT 1234, 1234.0, 1234.0 + 1.0, 1234.0 + 1.0, 1 + 1, 'abc' " +
       "FROM functional.alltypes");
   // Test aliases.
   testToSql("select 1234 i, 1234.0 as j, (1234.0 + 1) k, (1234.0 + 1.0) as l " +
       "from functional.alltypes",
       "SELECT 1234 i, 1234.0 j, (1234.0 + 1.0) k, (1234.0 + 1.0) l " +
       "FROM functional.alltypes");
   // Test select without from.
   testToSql("select 1234 i, 1234.0 as j, (1234.0 + 1) k, (1234.0 + 1.0) as l",
       "SELECT 1234 i, 1234.0 j, (1234.0 + 1.0) k, (1234.0 + 1.0) l");
   // Test select without from.
   testToSql("select null, 1234 < 5678, 1234.0 < 5678.0, 1234 < null " +
       "from functional.alltypes",
       "SELECT NULL, 1234 < 5678, 1234.0 < 5678.0, 1234 < NULL " +
       "FROM functional.alltypes");
 }

  /**
   * Tests quoting of identifiers for view compatibility with Hive.
   */
  @Test
  public void TestIdentifierQuoting() {
    // The quotes of quoted identifiers will be removed if they are unnecessary.
    testToSql("select 1 as `abc`, 2.0 as 'xyz'", "SELECT 1 abc, 2.0 xyz");

    // These identifiers are lexable by Impala but not Hive. For view compatibility
    // we enclose the idents in quotes.
    testToSql("select 1 as _c0, 2.0 as $abc", "SELECT 1 `_c0`, 2.0 `$abc`");

    // Quoted identifiers that require quoting in both Impala and Hive.
    testToSql("select 1 as `???`, 2.0 as '^^^'", "SELECT 1 `???`, 2.0 `^^^`");

    // Test quoting of inline view aliases.
    testToSql("select a from (select 1 as a) as _t",
        "SELECT a FROM (SELECT 1 a) `_t`");

    // Test quoting of WITH-clause views.
    testToSql("with _t as (select 1 as a) select * from _t",
        "WITH `_t` AS (SELECT 1 a) SELECT * FROM `_t`");

    // Test quoting of non-SlotRef exprs in inline views.
    testToSql("select `1 + 10`, `trim('abc')` from (select 1 + 10, trim('abc')) as t",
        "SELECT `1 + 10`, `trim('abc')` FROM (SELECT 1 + 10, trim('abc')) t");
  }

  // Test the toSql() output of the where clause.
  @Test
  public void whereTest() {
    testToSql("select id from functional.alltypes " +
        "where tinyint_col < 40 OR int_col = 4 AND float_col > 1.4",
        "SELECT id FROM functional.alltypes " +
        "WHERE tinyint_col < 40 OR int_col = 4 AND float_col > 1.4");
    testToSql("select id from functional.alltypes where string_col = \"abc\"",
        "SELECT id FROM functional.alltypes WHERE string_col = 'abc'");
    testToSql("select id from functional.alltypes where string_col = 'abc'",
        "SELECT id FROM functional.alltypes WHERE string_col = 'abc'");
    testToSql("select id from functional.alltypes " +
        "where 5 between smallint_col and int_col",
        "SELECT id FROM functional.alltypes WHERE 5 BETWEEN smallint_col AND int_col");
    testToSql("select id from functional.alltypes " +
        "where 5 not between smallint_col and int_col",
        "SELECT id FROM functional.alltypes " +
        "WHERE 5 NOT BETWEEN smallint_col AND int_col");
    testToSql("select id from functional.alltypes where 5 in (smallint_col, int_col)",
        "SELECT id FROM functional.alltypes WHERE 5 IN (smallint_col, int_col)");
    testToSql("select id from functional.alltypes " +
        "where 5 not in (smallint_col, int_col)",
        "SELECT id FROM functional.alltypes WHERE 5 NOT IN (smallint_col, int_col)");
  }

  // Test the toSql() output of aggregate and group by expressions.
  @Test
  public void aggregationTest() {
    testToSql("select COUNT(*), count(id), COUNT(id), SUM(id), AVG(id) " +
        "from functional.alltypes group by tinyint_col",
        "SELECT COUNT(*), COUNT(id), COUNT(id), SUM(id), AVG(id) " +
        "FROM functional.alltypes GROUP BY tinyint_col");
    testToSql("select avg(float_col / id) from functional.alltypes group by tinyint_col",
        "SELECT AVG(float_col / id) " +
        "FROM functional.alltypes GROUP BY tinyint_col");
    testToSql("select avg(double_col) from functional.alltypes " +
        "group by int_col, tinyint_col, bigint_col",
        "SELECT AVG(double_col) FROM functional.alltypes " +
        "GROUP BY int_col, tinyint_col, bigint_col");
    // Group by with having clause
    testToSql("select avg(id) from functional.alltypes " +
        "group by tinyint_col having count(tinyint_col) > 10",
        "SELECT AVG(id) FROM functional.alltypes " +
        "GROUP BY tinyint_col HAVING COUNT(tinyint_col) > 10");
    testToSql("select sum(id) from functional.alltypes group by tinyint_col " +
        "having avg(tinyint_col) > 10 AND count(tinyint_col) > 5",
        "SELECT SUM(id) FROM functional.alltypes GROUP BY tinyint_col " +
        "HAVING AVG(tinyint_col) > 10 AND COUNT(tinyint_col) > 5");
  }

  // Test the toSql() output of the order by clause.
  @Test
  public void orderByTest() {
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col ASC, float_col DESC, int_col ASC",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col ASC, float_col DESC, int_col ASC");
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col DESC, float_col ASC, int_col DESC",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col DESC, float_col ASC, int_col DESC");
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC");
    // Test limit/offset
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC LIMIT 10 OFFSET 5",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC LIMIT 10 OFFSET 5");
    // Offset shouldn't be printed if it's not necessary
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC LIMIT 10 OFFSET 0",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC LIMIT 10");

    // Check we do not print NULLS FIRST/LAST unless necessary
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col DESC NULLS FIRST, float_col ASC NULLS LAST, " +
        "int_col DESC",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col DESC, float_col ASC, " +
        "int_col DESC");
  }

  // Test the toSql() output of queries with all clauses.
  @Test
  public void allTest() {
    testToSql("select bigint_col, avg(double_col), sum(tinyint_col) " +
        "from functional.alltypes " +
        "where double_col > 2.5 AND string_col != \"abc\"" +
        "group by bigint_col, int_col " +
        "having count(int_col) > 10 OR sum(bigint_col) > 20 " +
        "order by 2 DESC NULLS LAST, 3 ASC",
        "SELECT bigint_col, AVG(double_col), SUM(tinyint_col) " +
        "FROM functional.alltypes " +
        "WHERE double_col > 2.5 AND string_col != 'abc' " +
        "GROUP BY bigint_col, int_col " +
        "HAVING COUNT(int_col) > 10 OR SUM(bigint_col) > 20 " +
        "ORDER BY 2 DESC NULLS LAST, 3 ASC");
  }

  @Test
  public void unionTest() {
    testToSql("select bool_col, int_col from functional.alltypes " +
        "union select bool_col, int_col from functional.alltypessmall " +
        "union select bool_col, bigint_col from functional.alltypes",
        "SELECT bool_col, int_col FROM functional.alltypes " +
        "UNION SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION SELECT bool_col, bigint_col FROM functional.alltypes");
    testToSql("select bool_col, int_col from functional.alltypes " +
        "union all select bool_col, int_col from functional.alltypessmall " +
        "union all select bool_col, int_col from functional.alltypessmall " +
        "union all select bool_col, int_col from functional.alltypessmall " +
        "union all select bool_col, bigint_col from functional.alltypes",
        "SELECT bool_col, int_col FROM functional.alltypes " +
        "UNION ALL SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION ALL SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION ALL SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION ALL SELECT bool_col, bigint_col FROM functional.alltypes");
    // With 'order by' and 'limit' on union, and also on last select.
    testToSql("(select bool_col, int_col from functional.alltypes) " +
        "union all (select bool_col, int_col from functional.alltypessmall) " +
        "union all (select bool_col, bigint_col " +
        "from functional.alltypes order by 1 nulls first limit 1) " +
        "order by int_col nulls first, bool_col limit 5 + 5",
        "SELECT bool_col, int_col FROM functional.alltypes " +
        "UNION ALL SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION ALL SELECT bool_col, bigint_col " +
        "FROM functional.alltypes ORDER BY 1 ASC NULLS FIRST LIMIT 1 " +
        "ORDER BY int_col ASC NULLS FIRST, bool_col ASC LIMIT 5 + 5");
    // With 'order by' and 'limit' on union but not on last select.
    testToSql("select bool_col, int_col from functional.alltypes " +
        "union all select bool_col, int_col from functional.alltypessmall " +
        "union all (select bool_col, bigint_col from functional.alltypes) " +
        "order by int_col nulls first, bool_col limit 10",
        "SELECT bool_col, int_col FROM functional.alltypes " +
        "UNION ALL SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION ALL (SELECT bool_col, bigint_col FROM functional.alltypes) " +
        "ORDER BY int_col ASC NULLS FIRST, bool_col ASC LIMIT 10");
    // Nested unions require parenthesis.
    testToSql("select bool_col, int_col from functional.alltypes " +
        "union all (select bool_col, int_col from functional.alltypessmall " +
        "union distinct (select bool_col, bigint_col from functional.alltypes)) " +
        "order by int_col, bool_col limit 10",
        "SELECT bool_col, int_col FROM functional.alltypes UNION ALL " +
        "(SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION SELECT bool_col, bigint_col FROM functional.alltypes) " +
        "ORDER BY int_col ASC, bool_col ASC LIMIT 10");
  }

  @Test
  public void valuesTest() {
    testToSql("values(1, 'a', 1.0)", "VALUES(1, 'a', 1.0)");
    testToSql("values(1 as x, 'a' y, 1.0 as z)", "VALUES(1 x, 'a' y, 1.0 z)");
    testToSql("values(1, 'a'), (2, 'b'), (3, 'c')",
        "VALUES((1, 'a'), (2, 'b'), (3, 'c'))");
    testToSql("values(1 x, 'a' as y), (2 as y, 'b'), (3, 'c' x)",
        "VALUES((1 x, 'a' y), (2 y, 'b'), (3, 'c' x))");
    testToSql("select * from (values(1, 'a'), (2, 'b')) as t",
        "SELECT * FROM (VALUES((1, 'a'), (2, 'b'))) t");
    testToSql("values(1, 'a'), (2, 'b') union all values(3, 'c')",
        "VALUES((1, 'a'), (2, 'b')) UNION ALL (VALUES(3, 'c'))");
    testToSql("insert into table functional.alltypessmall " +
        "partition (year=2009, month=4) " +
        "values(1, true, 1, 1, 10, 10, 10.0, 10.0, 'a', 'a', cast (0 as timestamp))",
        "INSERT INTO TABLE functional.alltypessmall PARTITION (year=2009, month=4) " +
        "VALUES(1, TRUE, 1, 1, 10, 10, 10.0, 10.0, 'a', 'a', CAST(0 AS TIMESTAMP))");
  }

  /**
   * Tests that toSql() properly handles inline views and their expression substitutions.
   */
  @Test
  public void subqueryTest() {
    // Test undoing expr substitution in select-list exprs and on clause.
    testToSql("select t1.int_col, t2.int_col from " +
        "(select int_col from functional.alltypes) t1 inner join " +
        "(select int_col from functional.alltypes) t2 on (t1.int_col = t2.int_col)",
        "SELECT t1.int_col, t2.int_col FROM " +
        "(SELECT int_col FROM functional.alltypes) t1 INNER JOIN " +
        "(SELECT int_col FROM functional.alltypes) t2 ON (t1.int_col = t2.int_col)");
    // Test undoing expr substitution in aggregates and group by and having clause.
    testToSql("select count(t1.string_col), sum(t2.float_col) from " +
        "(select id, string_col from functional.alltypes) t1 inner join " +
        "(select id, float_col from functional.alltypes) t2 on (t1.id = t2.id) " +
        "group by t1.id, t2.id having count(t2.float_col) > 2",
        "SELECT COUNT(t1.string_col), SUM(t2.float_col) FROM " +
        "(SELECT id, string_col FROM functional.alltypes) t1 INNER JOIN " +
        "(SELECT id, float_col FROM functional.alltypes) t2 ON (t1.id = t2.id) " +
        "GROUP BY t1.id, t2.id HAVING COUNT(t2.float_col) > 2");
    // Test undoing expr substitution in order by clause.
    testToSql("select t1.id, t2.id from " +
        "(select id, string_col from functional.alltypes) t1 inner join " +
        "(select id, float_col from functional.alltypes) t2 on (t1.id = t2.id) " +
        "order by t1.id, t2.id nulls first",
        "SELECT t1.id, t2.id FROM " +
        "(SELECT id, string_col FROM functional.alltypes) t1 INNER JOIN " +
        "(SELECT id, float_col FROM functional.alltypes) t2 ON (t1.id = t2.id) " +
        "ORDER BY t1.id ASC, t2.id ASC NULLS FIRST");
    // Test undoing expr substitution in where-clause conjuncts.
    testToSql("select t1.id, t2.id from " +
        "(select id, string_col from functional.alltypes) t1, " +
        "(select id, float_col from functional.alltypes) t2 " +
        "where t1.id = t2.id and t1.string_col = 'abc' and t2.float_col < 10",
        "SELECT t1.id, t2.id FROM " +
        "(SELECT id, string_col FROM functional.alltypes) t1, " +
        "(SELECT id, float_col FROM functional.alltypes) t2 " +
        "WHERE t1.id = t2.id AND t1.string_col = 'abc' AND t2.float_col < 10.0");
  }

  @Test
  public void withClauseTest() {
    // WITH clause in select stmt.
    testToSql("with t as (select * from functional.alltypes) select * from t",
        "WITH t AS (SELECT * FROM functional.alltypes) SELECT * FROM t");
    // WITH clause in select stmt with a join and an ON clause.
    testToSql("with t as (select * from functional.alltypes) " +
        "select * from t a inner join t b on (a.int_col = b.int_col)",
        "WITH t AS (SELECT * FROM functional.alltypes) " +
        "SELECT * FROM t a INNER JOIN t b ON (a.int_col = b.int_col)");
    // WITH clause in select stmt with a join and a USING clause.
    testToSql("with t as (select * from functional.alltypes) " +
        "select * from t a inner join t b using(int_col)",
        "WITH t AS (SELECT * FROM functional.alltypes) " +
        "SELECT * FROM t a INNER JOIN t b USING (int_col)");
    // WITH clause in a union stmt.
    testToSql("with t1 as (select * from functional.alltypes)" +
        "select * from t1 union all select * from t1",
        "WITH t1 AS (SELECT * FROM functional.alltypes) " +
        "SELECT * FROM t1 UNION ALL SELECT * FROM t1");
    // WITH clause in values stmt.
    testToSql("with t1 as (select * from functional.alltypes) values(1, 2), (3, 4)",
        "WITH t1 AS (SELECT * FROM functional.alltypes) VALUES((1, 2), (3, 4))");
    // WITH clause in insert stmt.
    testToSql("with t1 as (select * from functional.alltypes) " +
        "insert into functional.alltypes partition(year, month) select * from t1",
        "WITH t1 AS (SELECT * FROM functional.alltypes) " +
        "INSERT INTO TABLE functional.alltypes PARTITION (year, month) " +
        "SELECT * FROM t1");
    // WITH clause in complex query with joins and and order by + limit.
    testToSql("with t as (select int_col x, bigint_col y from functional.alltypestiny " +
        "order by id nulls first limit 2) " +
        "select * from t t1 left outer join t t2 on t1.y = t2.x " +
        "full outer join t t3 on t2.y = t3.x order by t1.x nulls first limit 5 * 2",
        "WITH t AS (SELECT int_col x, bigint_col y FROM functional.alltypestiny " +
        "ORDER BY id ASC NULLS FIRST LIMIT 2) " +
        "SELECT * FROM t t1 LEFT OUTER JOIN t t2 ON t1.y = t2.x " +
        "FULL OUTER JOIN t t3 ON t2.y = t3.x ORDER BY t1.x ASC NULLS FIRST LIMIT 5 * 2");
  }

  // Test the toSql() output of insert queries.
  @Test
  public void insertTest() {
    // Insert into unpartitioned table without partition clause.
    testToSql("insert into table functional.alltypesnopart " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "INSERT INTO TABLE functional.alltypesnopart " +
        "SELECT id, bool_col, tinyint_col, " +
        "smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, " +
        "string_col, timestamp_col FROM functional.alltypes");
    // Insert into overwrite unpartitioned table without partition clause.
    testToSql("insert overwrite table functional.alltypesnopart " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "INSERT OVERWRITE TABLE functional.alltypesnopart " +
        "SELECT id, bool_col, tinyint_col, " +
        "smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, " +
        "string_col, timestamp_col FROM functional.alltypes");
    // Static partition.
    testToSql("insert into table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "INSERT INTO TABLE functional.alltypessmall " +
        "PARTITION (year=2009, month=4) SELECT id, " +
        "bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, " +
        "double_col, date_string_col, string_col, timestamp_col " +
        "FROM functional.alltypes");
    // Fully dynamic partitions.
    testToSql("insert into table functional.alltypessmall " +
        "partition (year, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year, " +
        "month from functional.alltypes",
        "INSERT INTO TABLE functional.alltypessmall " +
        "PARTITION (year, month) SELECT id, bool_col, " +
        "tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, " +
        "date_string_col, string_col, timestamp_col, year, month " +
        "FROM functional.alltypes");
    // Partially dynamic partitions.
    testToSql("insert into table functional.alltypessmall " +
        "partition (year=2009, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, month " +
        "from functional.alltypes",
        "INSERT INTO TABLE functional.alltypessmall " +
        "PARTITION (year=2009, month) SELECT id, " +
        "bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, " +
        "double_col, date_string_col, string_col, timestamp_col, month " +
        "FROM functional.alltypes");

    // Permutations
    testToSql("insert into table functional.alltypesnopart(id, bool_col, tinyint_col) " +
        " values(1, true, 0)",
        "INSERT INTO TABLE functional.alltypesnopart(id, bool_col, tinyint_col) " +
        "VALUES(1, TRUE, 0)");

    // Permutations that mention partition column
    testToSql("insert into table functional.alltypes(id, year, month) " +
        " values(1, 1990, 12)",
        "INSERT INTO TABLE functional.alltypes(id, year, month) " +
        "VALUES(1, 1990, 12)");

    // Empty permutation with no select statement
    testToSql("insert into table functional.alltypesnopart()",
              "INSERT INTO TABLE functional.alltypesnopart()");

    // Permutation and explicit partition clause
    testToSql("insert into table functional.alltypes(id) " +
        " partition (year=2009, month) values(1, 12)",
        "INSERT INTO TABLE functional.alltypes(id) " +
        "PARTITION (year=2009, month) VALUES(1, 12)");
  }

  /**
   * Tests all expressions including whether their toSql() is properly
   * enclosed in parentheses.
   */
  @Test
  public void testExprs() {
    // AggregateExpr.
    testToSql("select count(*), (count(*)), avg(int_col), (avg(int_col)), " +
        "sum(int_col), (sum(int_col)), min(int_col), (min(int_col)), " +
        "max(int_col), (max(int_col)) from functional.alltypes",
        "SELECT COUNT(*), (COUNT(*)), AVG(int_col), (AVG(int_col)), " +
        "SUM(int_col), (SUM(int_col)), MIN(int_col), (MIN(int_col)), " +
        "MAX(int_col), (MAX(int_col)) FROM functional.alltypes");
    // ArithmeticExpr.
    testToSql("select 1 * 1, (1 * 1), 2 / 2, (2 / 2), 3 % 3, (3 % 3), " +
        "4 DIV 4, (4 DIV 4), 5 + 5, (5 + 5), 6 - 6, (6 - 6), 7 & 7, (7 & 7), " +
        "8 | 8, (8 | 8), 9 ^ 9, (9 ^ 9), ~10, (~10)",
        "SELECT 1 * 1, (1 * 1), 2.0 / 2.0, (2.0 / 2.0), 3 % 3, (3 % 3), " +
         "4 DIV 4, (4 DIV 4), 5 + 5, (5 + 5), 6 - 6, (6 - 6), 7 & 7, (7 & 7), " +
        "8 | 8, (8 | 8), 9 ^ 9, (9 ^ 9), ~10, (~10)");
    testToSql("select (((1 + 2) * (3 - 4) + 6) / 7)",
        "SELECT (((1 + 2) * (3 - 4) + 6) / 7.0)");

    // CaseExpr.
    // Single case without else clause. No case expr.
    testToSql("select case when true then 1 end, " +
        "(case when true then 1 end)",
        "SELECT CASE WHEN TRUE THEN 1 END, " +
        "(CASE WHEN TRUE THEN 1 END)");
    // Multiple cases with else clause. No case expr.
    testToSql("select case when true then 1 when false then 2 else 3 end, " +
        "(case when true then 1 when false then 2 else 3 end)",
        "SELECT CASE WHEN TRUE THEN 1 WHEN FALSE THEN 2 ELSE 3 END, " +
        "(CASE WHEN TRUE THEN 1 WHEN FALSE THEN 2 ELSE 3 END)");
    // Multiple cases with else clause with case expr.
    testToSql("select case true when true then 1 when false then 2 else 3 end, " +
        "(case true when true then 1 when false then 2 else 3 end)",
        "SELECT CASE TRUE WHEN TRUE THEN 1 WHEN FALSE THEN 2 ELSE 3 END, " +
        "(CASE TRUE WHEN TRUE THEN 1 WHEN FALSE THEN 2 ELSE 3 END)");

    // CastExpr.
    testToSql("select cast(NULL as INT), (cast(NULL as INT))",
        "SELECT CAST(NULL AS INT), (CAST(NULL AS INT))");
    // FunctionCallExpr.
    testToSql("select pi(), (pi()), trim('a'), (trim('a'))",
        "SELECT pi(), (pi()), trim('a'), (trim('a'))");
    // LiteralExpr.
    testToSql("select 10, (10), 20.0, (20.0), NULL, (NULL), 'abc', ('abc')",
        "SELECT 10, (10), 20.0, (20.0), NULL, (NULL), 'abc', ('abc')");
    // BetweenPredicate.
    testToSql("select 5 between 10 and 20, (5 between 10 and 20)",
        "SELECT 5 BETWEEN 10 AND 20, (5 BETWEEN 10 AND 20)");
    testToSql("select 5 not between 10 and 20, (5 not between 10 and 20)",
        "SELECT 5 NOT BETWEEN 10 AND 20, (5 NOT BETWEEN 10 AND 20)");
    // BinaryPredicate.
    testToSql("select 'a' = 'b', ('a' = 'b'), 'a' != 'b', ('a' != 'b'), " +
        "1 < 2, (1 < 2), 1 <= 2, (1 <= 2), 1 > 2, (1 > 2), 1 >= 2, (1 >= 2)",
        "SELECT 'a' = 'b', ('a' = 'b'), 'a' != 'b', ('a' != 'b'), " +
        "1 < 2, (1 < 2), 1 <= 2, (1 <= 2), 1 > 2, (1 > 2), 1 >= 2, (1 >= 2)");
    // CompoundPredicate.
    testToSql("select true and false, (true and false), " +
        "false or true, (false or true), " +
        "!true, (!true), not false, (not false)",
        "SELECT TRUE AND FALSE, (TRUE AND FALSE), " +
         "FALSE OR TRUE, (FALSE OR TRUE), " +
        "NOT TRUE, (NOT TRUE), NOT FALSE, (NOT FALSE)");
    testToSql("select ((true and (false or false) or true) and (false or true))",
        "SELECT ((TRUE AND (FALSE OR FALSE) OR TRUE) AND (FALSE OR TRUE))");
    // InPredicate.
    testToSql("select 5 in (4, 6, 7, 5), (5 in (4, 6, 7, 5))," +
        "5 not in (4, 6, 7, 5), (5 not In (4, 6, 7, 5))",
        "SELECT 5 IN (4, 6, 7, 5), (5 IN (4, 6, 7, 5)), " +
        "5 NOT IN (4, 6, 7, 5), (5 NOT IN (4, 6, 7, 5))");
    // IsNullPredicate.
    testToSql("select 5 is null, (5 is null), 10 is not null, (10 is not null)",
        "SELECT 5 IS NULL, (5 IS NULL), 10 IS NOT NULL, (10 IS NOT NULL)");
    // LikePredicate.
    testToSql("select 'a' LIKE '%b.', ('a' LIKE '%b.'), " +
        "'b' RLIKE '.c%', ('b' RLIKE '.c%')," +
        "'d' REGEXP '.e%', ('d' REGEXP '.e%')",
        "SELECT 'a' LIKE '%b.', ('a' LIKE '%b.'), " +
         "'b' RLIKE '.c%', ('b' RLIKE '.c%'), " +
        "'d' REGEXP '.e%', ('d' REGEXP '.e%')");
    // SlotRef.
    testToSql("select bool_col, (bool_col), int_col, (int_col) " +
        "string_col, (string_col), timestamp_col, (timestamp_col) " +
        "from functional.alltypes",
        "SELECT bool_col, (bool_col), int_col, (int_col) " +
         "string_col, (string_col), timestamp_col, (timestamp_col) " +
        "FROM functional.alltypes");

    // TimestampArithmeticExpr.
    // Non-function-call like version.
    testToSql("select timestamp_col + interval 10 years, " +
        "(timestamp_col + interval 10 years) from functional.alltypes",
        "SELECT timestamp_col + INTERVAL 10 years, " +
        "(timestamp_col + INTERVAL 10 years) FROM functional.alltypes");
    testToSql("select timestamp_col - interval 20 months, " +
        "(timestamp_col - interval 20 months) from functional.alltypes",
        "SELECT timestamp_col - INTERVAL 20 months, " +
        "(timestamp_col - INTERVAL 20 months) FROM functional.alltypes");
    // Reversed interval and timestamp using addition.
    testToSql("select interval 30 weeks + timestamp_col, " +
        "(interval 30 weeks + timestamp_col) from functional.alltypes",
        "SELECT INTERVAL 30 weeks + timestamp_col, " +
        "(INTERVAL 30 weeks + timestamp_col) FROM functional.alltypes");
    // Function-call like version.
    testToSql("select date_add(timestamp_col, interval 40 days), " +
        "(date_add(timestamp_col, interval 40 days)) from functional.alltypes",
        "SELECT DATE_ADD(timestamp_col, INTERVAL 40 days), " +
        "(DATE_ADD(timestamp_col, INTERVAL 40 days)) FROM functional.alltypes");
    testToSql("select date_sub(timestamp_col, interval 40 hours), " +
        "(date_sub(timestamp_col, interval 40 hours)) from functional.alltypes",
        "SELECT DATE_SUB(timestamp_col, INTERVAL 40 hours), " +
        "(DATE_SUB(timestamp_col, INTERVAL 40 hours)) FROM functional.alltypes");
  }
}
