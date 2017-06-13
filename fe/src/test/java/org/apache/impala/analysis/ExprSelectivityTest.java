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

import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests selectivity estimates for Exprs
 */
public class ExprSelectivityTest extends FrontendTestBase {

  public void verifySelectivityStmt(String stmtStr, double expectedSel)
      throws AnalysisException {
    SelectStmt stmt = (SelectStmt) AnalyzesOk(stmtStr);
    Expr selectClause = stmt.getSelectList().getItems().get(0).getExpr();
    double calculatedSel = selectClause.getSelectivity();
    assertEquals(calculatedSel, expectedSel);
  }

  public void verifySel(String predicate, double expectedSel)
      throws AnalysisException {
    String stmtStr = "select " + predicate + " from functional.alltypes";
    verifySelectivityStmt(stmtStr, expectedSel);
  }

  /**
   * Helper for prettier error messages than what JUnit.Assert provides.
   */
  private void assertEquals(double actual, double expected) {
    // Direct comparison of a calculated double and a hard-coded double proves
    // to be difficult. So we are setting the delta to essentially ignores
    // precision beyond 6.
    if (Math.abs(actual - expected) > 0.000001) {
      Assert.fail(
          String.format("\nActual: %.7f\nExpected: %.7f\n", actual, expected));
    }
  }

  @Test
  public void TestBasicPredicateSel() throws AnalysisException {
    // Testing selectivity of IN an NOT IN predicates
    verifySel("id in (1,3,5,7)", 0.000548);
    verifySel("id not in (1,3,9)", 0.999589);
  }
}
