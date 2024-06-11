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

import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests computeNumDistinctValues() estimates for Exprs
 */
public class ExprNdvTest extends FrontendTestBase {
  @BeforeClass
  public static void setUpClass() throws Exception {
    RuntimeEnv.INSTANCE.setTestEnv(true);
  }

  @AfterClass
  public static void cleanUpClass() {
    RuntimeEnv.INSTANCE.reset();
  }

  public void verifyNdv(String expr, long expectedNdv)
      throws ImpalaException {
    String stmtStr = "select " + expr + " from functional.alltypes";
    verifyNdvStmt(stmtStr, expectedNdv);
  }

  /**
   * This test queries two tables to allow testing missing statistics.
   * functional.alltypes (a) has statistics
   * functional.tinytable (tiny) does not
   */
  public void verifyNdvTwoTable(String expr, long expectedNdv)
      throws ImpalaException {
    String stmtStr = "select " + expr + " from functional.alltypes a, " +
                     "functional.tinytable tiny";
    verifyNdvStmt(stmtStr, expectedNdv);
  }

  public void verifyNdvStmt(String stmt, long expectedNdv) throws ImpalaException {
    AnalysisContext ctx = createAnalysisCtx();
    AnalysisResult result = parseAndAnalyze(stmt, ctx);
    SelectStmt parsedStmt = (SelectStmt) result.getStmt();
    Expr analyzedExpr = parsedStmt.getSelectList().getItems().get(0).getExpr();
    long calculatedNdv = analyzedExpr.getNumDistinctValues();
    assertEquals(expectedNdv, calculatedNdv);
  }

  /**
   * Helper for prettier error messages than what JUnit.Assert provides.
   */
  private void assertEquals(long expected, long actual) {
    if (actual != expected) {
      Assert.fail(String.format("\nActual: %d\nExpected: %d\n", actual, expected));
    }
  }

  /**
   * Constants have an NDV.
   */
  @Test
  public void testConsts() throws ImpalaException {
    // Would expect 1, but is 2.
    verifyNdv("case when 0 = 1 then 'yes' else 'no' end", 2);

    // Constants have NDV=1. This is set in the base LiteralExpr class,
    // so only an INT constant is tested, all others are the same.
    verifyNdv("10", 1);

    // Propagation of const NDV. All expressions save CASE use
    // the same max logic.
    verifyNdv("10 * 3", 1);

    // Planner defines NDV as "number of distinct values
    // including nulls", but the NDV function (and the stats
    // from tables) define it as "number of distinct non-null
    // values".
    verifyNdv("null", 1);
  }

  @Test
  public void TestCaseExprBasic() throws ImpalaException {
    // All constants tests
    verifyNdv("case when id = 1 then 'yes' else 'no' end", 2);
    verifyNdv("case when id = 1 then 'yes' " +
              "when id = 2 then 'maybe' else 'no' end", 3);
    verifyNdv("decode(id, 1, 'yes', 'no')", 2);
    // Duplicate constants are counted once
    verifyNdv("case when id = 1 then 'yes' " +
              "when id = 2 then 'yes' else 'yes' end", 1);
    // When else not specified, it is NULL, verify it is counted
    verifyNdv("case when id = 1 then 'yes' end", 2);

    // Basic cases where the output includes a SlotRef
    // Expect number of constants + max over output SlotRefs
    verifyNdv("case when id = 1 then 0 else id end", 7301);
    verifyNdv("case when id = 1 then 0 else int_col end", 11);
    verifyNdv("case when id = 1 then 'foo' else date_string_col end", 737);

    // Verify max
    verifyNdv("case when id = 1 then int_col else id end", 7300);
    verifyNdv("case when id = 1 then date_string_col " +
              "when id = 2 then date_string_col " +
              "else date_string_col end", 736);
  }

  @Test
  public void testExprBasic() throws ImpalaException {
    // Baseline
    verifyNdv("id", 7300);

    // Produces a constant, but not worth worrying about.
    // Actual NDV = 1 (or 2 if nullable)
    verifyNdv("id * 0", 7300);

    // Should not change NDV
    verifyNdv("CAST(id AS VARCHAR)", 7300);

    // All expressions save CASE use the max logic.
    verifyNdv("id + 2", 7300);
    verifyNdv("id * 2", 7300);

    // IMPALA-7603: Should multiply NDVs, but does Max instead
    verifyNdv("id + int_col", 7300);
    verifyNdv("id * int_col", 7300);

    // nullValue returns a boolean, so should be NDV=2
    // Actual is wrong because it uses a generic calc:
    // NDV(f(x)) = NDV(x).
    // Should be:
    // NDV(f(x)) = max(NDV(x), NDV(type(f)))
    verifyNdv("nullValue(id)", 7300);
  }

  @Test
  public void TestCaseExprMissingStats() throws ImpalaException {
    // Consts still work
    verifyNdvTwoTable("case when a.id = 1 then 'yes' " +
                      "when tiny.a = 'whatever' then 'maybe' " +
                      "else 'no' end", 3);

    // Input has stats, output does not
    verifyNdvTwoTable("case when a.id = 1 then tiny.a else tiny.b end", -1);

    // Input has stats, some outputs do not (tiny)
    verifyNdvTwoTable("case when a.id = 1 then tiny.a " +
                      "else date_string_col end", -1);

    // Outputs has stats, input does not
    verifyNdvTwoTable("case when tiny.a = 'whatever' then a.id " +
                      "else 0 end", 7301);
  }

  /**
   * Test null count handling. After IMPALA-7659, Impala computes a null count,
   * when gathering stats, but the NDV does not include nulls (except for Boolean
   * columns) if stats are computed by Impala, but does include nulls if stats are
   * computed by Hive. This leads to rather bizarre outcomes such as the NDV of a
   * column = 0 when the null count is greater than zero. This is clearly a bug to
   * be fixed, but a complex one because of Hive and backward compatibility
   * considerations. This test simply illustrates the current (unfortunate)
   * behavior. See IMPALA-8094.
   */
  @Test
  public void testNulls() throws ImpalaException {
    // A table with nulls for which stats have been computed
    // NDV(id) = 26
    verifyNdvStmt("SELECT id FROM functional.nullrows", 26);
    // NDV(some_nulls) = 6
    verifyNdvStmt("SELECT some_nulls FROM functional.nullrows", 6);
    // NDV(null_str) = 0 (all nulls), but add 1 for nulls
    verifyNdvStmt("SELECT null_str FROM functional.nullrows", 1);
    // NDV(blanks) = 1, add 1 for nulls
    // Bug: See IMPALA-7310, IMPALA-8094
    //verifyNdvStmt("SELECT blanks FROM functional.nullrows", 2);
    verifyNdvStmt("SELECT blank FROM functional.nullrows", 1);

    // Same schema, one row
    verifyNdvStmt("SELECT a FROM functional.nulltable", 1);
    verifyNdvStmt("SELECT c FROM functional.nulltable", 1);

    // 11K rows, no stats
    // Bug: Should come up with some estimate from size
    verifyNdvStmt("SELECT id FROM functional.manynulls", -1);

    // Table with 8 rows, NDV(year) = 1,
    // null count for year is 0, so no adjustment.
    verifyNdvStmt("SELECT year FROM functional.alltypestiny", 1);

    // Test with non-nullable columns.
    // NDV value from stats not increased by one here.
    verifyNdvStmt("SELECT id FROM functional_kudu.alltypestiny", 8);
    // But, is increased for a nullable column.
    // Bug: Same as above
    //verifyNdvStmt("SELECT year FROM functional_kudu.alltypestiny", 2);
    verifyNdvStmt("SELECT year FROM functional_kudu.alltypestiny", 1);
  }
}
