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
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests computeNumDistinctValues() estimates for Exprs
 */
public class ExprNdvTest extends FrontendTestBase {

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
}
