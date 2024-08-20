// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Set;

import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.AnalysisSessionFixture;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.QueryFixture.SelectFixture;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.planner.CardinalityTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Tests expression cardinality and selectivity, both of which are
 * important inputs to scan and join cardinality estimates.
 *
 * In the comments below, the notation |x| means the cardinality of
 * x. If x is a table, then it is the row count of x. If x is a column,
 * then it is the number of distinct values (the cardinality of the
 * domain of the column), also known as NDV.
 *
 * This test focuses on cardinality and the selectivity that determines
 * derived cardinality. If |T| is the cardinality of table T, then
 * |T'| is defined as the cardinality of table T after applying a selection s.
 * Selectivity is defined as:
 *
 * sel(s) = |T'|/|T|
 *
 * Or
 *
 * |T'| = |T| * sel(s)
 *
 * Though not used here, it can be helpful to think of the selectivity as
 * the probability p that some row r appears in the output after selection:
 *
 * sel(s) = p(r in |T'|)
 *
 * Tests here focus on the entire cardinality and NDV lifecycle up to an
 * expression, ensuring that we produce proper overall estimates. See also:
 *
 * * {@link ExprNdvTest} which focuses on the actual NDV calculation
 *   method,
 * * {@link CardinalityTest} which examines cardinality output from the
 *   planner.
 *
 * The tests here illustrate a number of known bugs, typically marked by
 * their ticket number. IMPALA-7601 is a roll-up for the general case that
 * Impala does not estimate selectivity except in the narrow (col = const)
 * case.
 */
public class ExprCardinalityTest {
  private static AnalysisSessionFixture session_ = new AnalysisSessionFixture();

  @BeforeClass
  public static void setUp() {
    RuntimeEnv.INSTANCE.setTestEnv(true);
  }

  @AfterClass
  public static void cleanUp() {
    RuntimeEnv.INSTANCE.reset();
  }

  private void verifyTableCol(Table table, String colName,
      long expectedNdv, long expectedNullCount) {
    Column col = table.getColumn(colName);
    assertNotNull(col);
    ColumnStats stats = col.getStats();
    assertNotNull(stats);
    assertEquals(expectedNdv, stats.getNumDistinctValues());
    assertEquals(expectedNullCount, stats.getNumNulls());
  }

  /**
   * Baseline test of metadata cardinality, NDVs and null count.
   * Locks down the values used in later tests to catch external changes
   * easily.
   *
   * Cases:
   * - With stats
   *   - Columns without nulls
   *   - Columns with nulls
   * - Without stats, estimated from file size and schema
   *
   * (The last bit is not yet available.)
   */

  @Test
  public void testMetadata() throws DatabaseNotFoundException, InternalException {
    Catalog catalog = session_.catalog();
    Db db = catalog.getDb("functional");
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(session_.frontend(), "functional", null);
    Set<TableName> tables = Sets.newHashSet(
        new TableName("functional", "alltypes"),
        new TableName("functional", "nullrows"),
        new TableName("functional", "manynulls"));
    mdLoader.loadTables(tables);

    // Table with stats, no nulls
    Table allTypes = db.getTable("alltypes");
    assertEquals(7300, allTypes.getTTableStats().getNum_rows());
    verifyTableCol(allTypes, "id", 7300, 0);
    verifyTableCol(allTypes, "bool_col", 2, 0);
    verifyTableCol(allTypes, "int_col", 10, 0);
    // Bug: NDV of partition columns is -1 though it is listed as
    // 2 in the shell with: SHOW COLUMN STATS alltypes
    //verifyTableCol(allTypes, "year", 2, 0);
    // Bug: When tests are run in Eclipse we get the result above.
    // But, when the same test is run using maven from the command line,
    // we get the result shown below.
    // Unit test in Eclipse see the above, unit tests run from the
    // Disabling both to avoid a flaky test,
    // Same issue for the next three tests.
    //verifyTableCol(allTypes, "year", -1, -1);
    //verifyTableCol(allTypes, "month", 12, 0);
    //verifyTableCol(allTypes, "month", -1, -1);

    // Table with stats and nulls
    Table nullrows = db.getTable("nullrows");
    assertEquals(26, nullrows.getTTableStats().getNum_rows());
    verifyTableCol(nullrows, "id", 26, 0);
    // Bug: NDV should be 1 to include nulls
    verifyTableCol(nullrows, "null_str", 0, 26);
    verifyTableCol(nullrows, "group_str", 6, 0);
    verifyTableCol(nullrows, "some_nulls", 6, 20);
    // Oddly, boolean columns DO include nulls in NDV.
    verifyTableCol(nullrows, "bool_nulls", 3, 15);

    // Table without stats
    Table manynulls = db.getTable("manynulls");
    // Bug: Table cardinality should be guessed from schema & file size.
    assertEquals(-1, manynulls.getTTableStats().getNum_rows());
    verifyTableCol(manynulls, "id", -1, -1);
  }

  public void verifySelectCol(String table, String col,
      long expectedNdv, long expectedNullCount) throws ImpalaException {
    SelectFixture select = new SelectFixture(session_)
        .table("functional." + table)
        .exprSql(col);
    Expr expr = select.analyzeExpr();
    SlotRef colRef = (SlotRef) expr;
    assertEquals(expectedNdv, expr.getNumDistinctValues());
    assertEquals(expectedNullCount, colRef.getDesc().getStats().getNumNulls());
    // Columns don't have selectivity, only expressions on columns
    assertEquals(-1, expr.getSelectivity(), 0.001);
  }

  /**
   * Test cardinality of the column references within an AST.
   * Ensures that the metadata cardinality was propagated into the
   * AST, along with possible adjustments.
   *
   * Cases:
   * - With stats
   *   - Normal NDV
   *   - Small NDV
   *   - Small NDV with nulls
   *   - NDV with all nulls
   *   - Constants
   * - Without stats
   * @throws ImpalaException
   */
  @Test
  public void testColumnCardinality() throws ImpalaException {
    // Stats, no null values
    verifySelectCol("alltypes", "id", 7300, 0);
    verifySelectCol("alltypes", "bool_col", 2, 0);
    verifySelectCol("alltypes", "int_col", 10, 0);
    // Bug: Stats not available for partition columns
    //verifySelectExpr("alltypes", "year", 2, 0);
    // Bug: Unit test in Eclipse see the above, unit tests run from the
    // command line see the below. Disabling to avoid a flaky test,
    // here and below.
    //verifySelectExpr("alltypes", "year", -1, -1);
    //verifySelectExpr("alltypes", "month", 12, 0);
    //verifySelectExpr("alltypes", "month", -1, -1);

    // Stats, with null values
    verifySelectCol("nullrows", "id", 26, 0);
    verifySelectCol("nullrows", "null_str", 1, 26);
    verifySelectCol("nullrows", "group_str", 6, 0);
    verifySelectCol("nullrows", "some_nulls", 6, 20);
    // Oddly, boolean columns DO include nulls in NDV.
    verifySelectCol("nullrows", "bool_nulls", 3, 15);

    // No stats
    verifySelectCol("manynulls", "id", -1, -1);
  }

  public void verifySelectExpr(String db, String table, String exprSql, long expectedNdv,
      double expectedSel) throws ImpalaException {
    SelectFixture select =
        new SelectFixture(session_).table(db + "." + table).exprSql(exprSql);
    Expr expr = select.analyzeExpr();
    assertEquals(expectedNdv, expr.getNumDistinctValues());
    assertEquals(expectedSel, expr.getSelectivity(), 0.00001);
  }

  public void verifySelectExpr(String table, String exprSql, long expectedNdv,
      double expectedSel) throws ImpalaException {
    verifySelectExpr("functional", table, exprSql, expectedNdv, expectedSel);
  }

  /**
   * Constants have an NDV of 1, selectivity of -1.
   */
  @Test
  public void testConstants() throws ImpalaException {
    verifySelectExpr("alltypes", "10", 1, -1);
    verifySelectExpr("allTypes", "'foo'", 1, -1);
    // Note that the constant NULL has an NDV = 1, but
    // Null-only columns have an NDV=0...
    // See IMPALA-8058
    verifySelectExpr("alltypes", "NULL", 1, 0);
    verifySelectExpr("alltypes", "true", 1, 1);
    verifySelectExpr("alltypes", "false", 1, 0);
  }

  // Expression selectivity
  // - Test for each expression type
  // - Test for variety of situations
  //   - Valid/invalid table cardinality
  //   - Valid/invalid NDV
  //   - Valid/invalid null count

  /**
   * Test col = const
   *
   * selectivity = 1 / |col|
   */
  @Test
  public void testEqSelectivity() throws ImpalaException {
    verifySelectExpr("alltypes", "id = 10", 3, 1.0/7300);
    verifySelectExpr("alltypes", "bool_col = true", 3, 1.0/2);
    verifySelectExpr("alltypes", "int_col = 10", 3, 1.0/10);

    verifySelectExpr("nullrows", "id = 'foo'", 3, 1.0/26);
    verifySelectExpr("nullrows", "null_str = 'foo'", 3, 0);
    verifySelectExpr("nullrows", "group_str = 'foo'", 3, 1.0/6);

    verifySelectExpr("nullrows", "some_nulls = 'foo'", 3, 1.0/6 * 6/26);
    verifySelectExpr("nullrows", "some_nulls = null", 3, 0);

    // Bug: Sel should default to good old 0.1
    verifySelectExpr("manynulls", "id = 10", 3, -1);
  }

  /**
   * Test col IS NOT DISTINCT FROM x
   *
   * Sel should be same as = if x is non-null, otherwise
   * same as IS NULL
   */
  @Test
  public void testNotDistinctSelectivity() throws ImpalaException {
    verifySelectExpr("alltypes", "id is not distinct from 10", 3, 1.0/7300);

    verifySelectExpr("alltypes", "id is not distinct from null", 3, 0);
    verifySelectExpr("alltypes", "bool_col is not distinct from true", 3, 1.0/2);
    verifySelectExpr("alltypes", "bool_col is not distinct from null", 3, 0);
    verifySelectExpr("alltypes", "int_col is not distinct from 10", 3, 1.0/10);

    verifySelectExpr("alltypes", "int_col is not distinct from null", 3, 0);

    verifySelectExpr("nullrows", "id is not distinct from 'foo'", 3, 1.0/26);

    verifySelectExpr("nullrows", "id is not distinct from null", 3, 0);

    verifySelectExpr("nullrows", "null_str is not distinct from 'foo'", 3, 0);
    verifySelectExpr("nullrows", "null_str is not distinct from null", 3, 1.0/1);
    verifySelectExpr("nullrows", "group_str is not distinct from 'foo'", 3, 1.0/6);

    verifySelectExpr("nullrows", "group_str is not distinct from null", 3, 0);

    verifySelectExpr("nullrows", "some_nulls is not distinct from 'foo'", 3, 1.0/6*6/26);

    // Bug: Sel should default to good old 0.1
    verifySelectExpr("manynulls", "id is not distinct from 10", 3, -1);
  }

  /**
   * Test col != const
   */
  @Test
  public void testNeSelectivity() throws ImpalaException {
    verifySelectExpr("alltypes", "id != 10", 3, 1 - 1.0/7300);
    verifySelectExpr("alltypes", "bool_col != true", 3, 1 - 1.0/2);
    verifySelectExpr("alltypes", "int_col != 10", 3, 1 - 1.0/10);
    verifySelectExpr("nullrows", "id != 'foo'", 3, 1 - 1.0/26);
    verifySelectExpr("nullrows", "null_str != 'foo'", 3, 1 - 1.0/1);
    verifySelectExpr("nullrows", "group_str != 'foo'", 3, 1 - 1.0/6);
    verifySelectExpr("nullrows", "some_nulls != 'foo'", 3, (1 - 1.0/6)*6/26);
    verifySelectExpr("nullrows", "some_nulls != null", 3, 0);
    // field has no statistics.
    verifySelectExpr("emptytable", "field != 'foo'", 3, -1);
    verifySelectExpr("emptytable", "f2 != 10", 3, 0.0);

    // Bug: Sel should default to 1 - good old 0.1
    verifySelectExpr("manynulls", "id != 10", 3, -1);
  }

  /**
   * Test col IS DISTINCT FROM x
   *
   * Sel should be 1 - Sel(col IS NOT DISTINCT FROM x)
   */
  @Test
  public void testDistinctSelectivity() throws ImpalaException {

    verifySelectExpr("alltypes", "id is distinct from 10", 3, 1 - 1.0/7300);
    // Bug: does not treat NULL specially
    // Bug: NDV sould be 2 since IS DISTINCT won't return NULL
    //verifySelectExpr("alltypes", "id is distinct from null", 2, 1);
    verifySelectExpr("alltypes", "id is distinct from null", 3, 1);
    verifySelectExpr("alltypes", "bool_col is distinct from true", 3, 1 - 1.0/2);

    verifySelectExpr("alltypes", "bool_col is distinct from null", 3, 1);
    verifySelectExpr("alltypes", "int_col is distinct from 10", 3, 1 - 1.0/10);

    verifySelectExpr("alltypes", "int_col is distinct from null", 3, 1);
    verifySelectExpr("nullrows", "id is distinct from 'foo'", 3, 1 - 1.0/26);

    verifySelectExpr("nullrows", "id is distinct from null", 3, 1);
    // For is distinct from non-null, all null values are true
    verifySelectExpr("nullrows", "null_str is distinct from 'foo'", 3, 1);
    verifySelectExpr("nullrows", "null_str is distinct from null", 3, 0);
    verifySelectExpr("nullrows", "group_str is distinct from 'foo'", 3, 1 - 1.0/6);
    verifySelectExpr("nullrows", "group_str is distinct from null", 3, 1);
    verifySelectExpr("nullrows", "group_str is distinct from null", 3, 1);
    verifySelectExpr("nullrows", "some_nulls is not distinct from 'foo'", 3, 1.0/6*6/26);
    verifySelectExpr("nullrows", "some_nulls is distinct from null", 3, 6.0/26.0);

    // Bug: Sel should default to 1 - good old 0.1
    verifySelectExpr("manynulls", "id is distinct from 10", 3, -1);
  }

  public static final double INEQUALITY_SEL = 0.33;

  private void verifyInequalitySel(String table, String col, String value)
      throws ImpalaException {
    for (String op : new String[] { "<", "<=", ">", ">="}) {
      // Bug: No estimated selectivity for >, >=, <, <= (IMPALA-7603)
      //verifySelectExpr(table, col + " " + op + " " + value, 3, INEQUALITY_SEL);
      verifySelectExpr(table, col + " " + op + " " + value, 3, -1);
    }
  }

  @Test
  public void testInequalitySelectivity() throws ImpalaException {
    verifyInequalitySel("alltypes", "id", "10");
    verifyInequalitySel("alltypes", "int_col", "10");

    verifyInequalitySel("nullrows", "id", "'foo'");
    verifyInequalitySel("nullrows", "null_str", "'foo'");
    verifyInequalitySel("nullrows", "group_str", "'foo'");
    verifyInequalitySel("nullrows", "some_nulls", "'foo'");

    // Bug: Sel should default to 1 - good old 0.1
    verifyInequalitySel("manynulls", "id", "10");
  }

  /**
   * Test col IS NULL
   * Selectivity should be null_count / |table|
   */
  @Test
  public void testIsNullSelectivity() throws ImpalaException {
    // TODO: IMPALA-9915: NDV of IS NULL should be 2
    verifySelectExpr("alltypes", "id is null", 3, 0);
    verifySelectExpr("alltypes", "bool_col is null", 3, 0);
    verifySelectExpr("alltypes", "int_col is null", 3, 0);

    verifySelectExpr("nullrows", "id is null", 3, 0);
    verifySelectExpr("nullrows", "null_str is null", 3, 1);
    verifySelectExpr("nullrows", "group_str is null", 3, 0);
    verifySelectExpr("nullrows", "some_nulls is null", 3, 20.0/26);
    verifySelectExpr("nullrows", "bool_nulls is not null", 3, 1 - 15.0/26);

    // Bug: Sel should default to good old 0.1
    verifySelectExpr("manynulls", "id is null", 3, -1);
  }

  /**
   * Test col IS NOT NULL
   * Selectivity should be 1 - null_count / |table|
   */
  @Test
  public void testNotNullSelectivity() throws ImpalaException {
    // TODO: IMPALA-9915: NDV of IS NOT NULL should be 2
    verifySelectExpr("alltypes", "id is not null", 3, 1);
    verifySelectExpr("alltypes", "bool_col is not null", 3, 1);
    verifySelectExpr("alltypes", "int_col is not null", 3, 1);

    verifySelectExpr("nullrows", "id is not null", 3, 1);
    verifySelectExpr("nullrows", "null_str is not null", 3, 0);
    verifySelectExpr("nullrows", "group_str is not null", 3, 1);
    verifySelectExpr("nullrows", "some_nulls is not null", 3, 1 - 20.0/26);
    verifySelectExpr("nullrows", "bool_nulls is not null", 3, 1 - 15.0/26);

    // Bug: Sel should default to good old 0.1
    verifySelectExpr("manynulls", "id is not null", 3, -1);
  }

  /**
   * Test col IN (a, b, c)
   *
   * The code should check only distinct values, so that
   * |in| = NDV(in clause)
   *
   * Expected selectivity is |in| / |col|
   *
   * Where |col| = ndv(col)
   *
   * Estimate should be based on the "Containment" assumption: that the
   * in-clause values are contained in the set of column values
   */
  @Test
  public void testInSelectivity() throws ImpalaException {
    verifySelectExpr("alltypes", "id in (1, 2, 3)", 3, 3.0/7300);
    // Bug: Does not use NDV, just simple value count
    //verifySelectExpr("alltypes", "id in (1, 2, 3, 2, 3, 1)", 3, 3.0/7300);
    verifySelectExpr("alltypes", "id in (1, 2, 3, 2, 3, 1)", 3, 6.0/7300);
    verifySelectExpr("alltypes", "bool_col in (true)", 3, 1.0/2);
    verifySelectExpr("alltypes", "bool_col in (true, false)", 3, 2.0/2);
    verifySelectExpr("alltypes", "int_col in (1, 2, 3)", 3, 3.0/10);
    verifySelectExpr("alltypes",
        "int_col in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)", 3, 1);

    verifySelectExpr("nullrows", "id in ('a', 'b', 'c')", 3, 3.0/26);
    verifySelectExpr("nullrows", "null_str in ('a', 'b', 'c')", 3, 1);
    verifySelectExpr("nullrows", "group_str in ('a', 'b', 'c')", 3, 3.0/6);
    //verifySelectExpr("nullrows", "some_nulls in ('a', 'b', 'c')", 3, 3.0/7);
    verifySelectExpr("nullrows", "some_nulls in ('a', 'b', 'c')", 3, 3.0/6);

    // Bug: Sel should default to good old 0.1
    verifySelectExpr("manynulls", "id in (1, 3, 3)", 3, -1);
  }

  /**
   * Test col NOT IN (a, b, c)
   *
   * Should be 1 = sel(col IN (a, b, c))
   */
  @Test
  public void testNotInSelectivity() throws ImpalaException {
    verifySelectExpr("alltypes", "id not in (1, 2, 3)", 3, 1 - 3.0/7300);
    // Bug: Does not use NDV, just simple value count
    //verifySelectExpr("alltypes", "id not in (1, 2, 3, 2, 3, 1)", 3, 1 - 3.0/7300);
    verifySelectExpr("alltypes", "id not in (1, 2, 3, 2, 3, 1)", 3, 1 - 6.0/7300);
    verifySelectExpr("alltypes", "bool_col not in (true)", 3, 1 - 1.0/2);
    verifySelectExpr("alltypes", "bool_col not in (true, false)", 3, 1 - 2.0/2);
    verifySelectExpr("alltypes", "int_col not in (1, 2, 3)", 3, 1 - 3.0/10);
    verifySelectExpr("alltypes",
        "int_col not in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)", 3, 0);

    verifySelectExpr("nullrows", "id not in ('a', 'b', 'c')", 3, 1 - 3.0/26);
    verifySelectExpr("nullrows", "null_str not in ('a', 'b', 'c')", 3, 0);
    verifySelectExpr("nullrows", "group_str not in ('a', 'b', 'c')", 3, 1 - 3.0/6);
    // Bug: NULL should count as ndv
    //verifySelectExpr("nullrows", "some_nulls not in ('a', 'b', 'c')", 3, 1 - 3.0/7);
    verifySelectExpr("nullrows", "some_nulls not in ('a', 'b', 'c')", 3, 1 - 3.0/6);

    // Bug: Sel should default to 1 - good old 0.1
    verifySelectExpr("manynulls", "id not in (1, 3, 3)", 3, -1);
  }

  @Test
  public void testNotSelectivity() throws ImpalaException {
    verifySelectExpr("alltypes", "not id in (1, 2, 3)", 3, 1 - 3.0/7300);
    verifySelectExpr("alltypes", "not int_col in (1, 2)", 3, 1 - 2.0/10);
    verifySelectExpr("alltypes", "not int_col = 10", 3, 1 - 1.0/10);

    // Bug: Sel should default to 1 - good old 0.1
    //verifySelectExpr("manynulls", "not id = 10", 3, 0.9);
    verifySelectExpr("manynulls", "not id = 10", 3, -1);
  }

  @Test
  public void testAndSelectivity() throws ImpalaException {
    verifySelectExpr("alltypes", "bool_col = true", 3, 1.0/2);
    verifySelectExpr("alltypes", "int_col = 10", 3, 1.0/10);
    // Note: This is NOT the logic used in plan nodes!
    verifySelectExpr("alltypes", "bool_col = true and int_col = 10", 3, 1.0/2 * 1.0/10);
    // Bug: should be something like (1/3)^2
    //verifySelectExpr("alltypes", "int_col >= 10 and int_col <= 20", 3, 0.11);
    verifySelectExpr("alltypes", "int_col >= 10 and int_col <= 20", 3, -1);

    // Bug: Should be a product of two estimates.
    // But, the -1 from the inequality poisons the whole expression
    //verifySelectExpr("alltypes", "int_col = 10 AND smallint_col > 20",
    //      3, 1.0/10 * 0.33);
    verifySelectExpr("alltypes", "int_col = 10 AND smallint_col > 20", 3, -1);
  }

  @Test
  public void testOrSelectivity() throws ImpalaException {
    verifySelectExpr("alltypes", "bool_col = true or int_col = 10",
        3, 1.0/2 + 1.0/10 - 1.0/2 * 1.0/10);
    // Chain of OR rewritten to IN
    verifySelectExpr("alltypes", "int_col = 10 or int_col = 20", 3, 2.0/10);
    // Or with literals
    // 'int_col = 10 or true' rewritten to 'true', expected NDV = 1
    verifySelectExpr("alltypes", "int_col = 10 or true", 1, 1.0);
    verifySelectExpr("alltypes", "int_col = 10 or false", 3, 0.1);
    verifySelectExpr("alltypes", "int_col = 10 or null", 3, 0.1);
  }

  /**
   * Test col BETWEEN x and y. Rewritten to
   * col >= x AND col <= y. Inequality should have an estimate. Since
   * the expression is an AND, we multiply the two estimates.
   * So, regardless of NDV and null count, selectivity should be
   * something like 0.33^2.
   * However, for a sufficiently unique column, selectivity can be approximated as ratio
   * of values that fall in that range over NDV
   * (see BetweenToCompoundRule.computeBetweenSelectivity()).
   */
  @Test
  public void testBetweenSelectivity() throws ImpalaException {
    // Bug: NO selectivity for Between because it is rewritten to
    // use inequalities, and there no selectivities for those, except for Between
    // predicate over a sufficiently unique column. See IMPALA-8042
    // verifySelectExpr("alltypes", "id between 30 and 60", 3, 0.33 * 0.33);
    verifySelectExpr("alltypes", "id between 30 and 60", 3, 31.0 / 7300.0);
    // Fallback to -1 if selectivity is too high.
    verifySelectExpr("alltypes", "id between 1 and 7298", 3, -1);
    //verifySelectExpr("alltypes", "int_col between 30 and 60", 3, 0.33 * 0.33);
    verifySelectExpr("alltypes", "int_col between 30 and 60", 3, -1);

    // Should work over date column.
    // TODO: Add new unique date column in functional.date_tbl and use it here.
    verifySelectExpr("tpcds_partitioned_parquet_snap", "date_dim",
        "d_date between DATE '2001-03-13' and (DATE '2001-03-13' + interval 90 days)", 3,
        91.0 / 73049.0);

    // Should not matter that there are no stats
    //verifySelectExpr("manynulls", "id between 30 and 60", 3, 0.33 * 0.33);
    verifySelectExpr("manynulls", "id between 30 and 60", 3, -1);
  }

  /**
   * Test col NOT BETWEEN x and y. Should be 1 - sel(col BETWEEN x and y).
   */
  @Test
  public void testNotBetweenSelectivity() throws ImpalaException {
    // Bug: NO selectivity for Not Between because it is rewritten to
    // use inequalities, and there no selectivities for those
    // verifySelectExpr("alltypes", "id not between 30 and 60", 3, 1 - 0.33 * 0.33);
    // Fallback to -1 if selectivity is too high.
    verifySelectExpr("alltypes", "id not between 30 and 60", 3, -1);
    // only 2 value not in between.
    verifySelectExpr("alltypes", "id not between 1 and 7298", 3, 2.0 / 7300.0);
    //verifySelectExpr("alltypes", "int_col not between 30 and 60", 3, 1 - 0.33 * 0.33);
    verifySelectExpr("alltypes", "int_col not between 30 and 60", 3, -1);

    // Should work over date column. Fallback to -1 if selectivity is too high.
    // TODO: Add new unique date column in functional.date_tbl and use it here.
    verifySelectExpr("tpcds_partitioned_parquet_snap", "date_dim",
        "d_date not between DATE '2001-03-13' and (DATE '2001-03-13' + interval 90 days)",
        3, -1);

    // Should not matter that there are no stats
    //verifySelectExpr("manynulls", "id not between 30 and 60", 3, 1 - 0.33 * 0.33);
    verifySelectExpr("manynulls", "id not between 30 and 60", 3, -1);
  }
}
