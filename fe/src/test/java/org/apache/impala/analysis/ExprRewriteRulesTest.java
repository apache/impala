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

import java.util.List;

import org.apache.impala.catalog.Catalog;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.rewrite.BetweenToCompoundRule;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests ExprRewriteRules.
 */
public class ExprRewriteRulesTest extends FrontendTestBase {

  public Expr RewritesOk(String expr, ExprRewriteRule rule, String expectedExpr)
      throws AnalysisException {
    String stmtStr = "select " + expr + " from functional.alltypes";
    SelectStmt stmt = (SelectStmt) ParsesOk(stmtStr);
    Analyzer analyzer = createAnalyzer(Catalog.DEFAULT_DB);
    stmt.analyze(analyzer);
    Expr origExpr = stmt.getResultExprs().get(0);
    String origSql = origExpr.toSql();
    // Create a rewriter with only a single rule.
    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(rule);
    ExprRewriter rewriter = new ExprRewriter(rules);
    Expr rewrittenExpr = rewriter.rewrite(origExpr, analyzer);
    String rewrittenSql = rewrittenExpr.toSql();
    boolean expectChange = expectedExpr != null;
    if (expectedExpr != null) {
      assertEquals(expectedExpr, rewrittenSql);
    } else {
      assertEquals(origSql, rewrittenSql);
    }
    Assert.assertEquals(expectChange, rewriter.changed());
    return rewrittenExpr;
  }

  /**
   * Helper for prettier error messages than what JUnit.Assert provides.
   */
  private void assertEquals(String expected, String actual) {
    if (!actual.equals(expected)) {
      Assert.fail(String.format("\nActual: %s\nExpected: %s\n", actual, expected));
    }
  }

  @Test
  public void TestBetweenToCompoundRule() throws AnalysisException {
    ExprRewriteRule rule = BetweenToCompoundRule.INSTANCE;

    // Basic BETWEEN predicates.
    RewritesOk("int_col between float_col and double_col", rule,
        "int_col >= float_col AND int_col <= double_col");
    RewritesOk("int_col not between float_col and double_col", rule,
        "int_col < float_col OR int_col > double_col");
    RewritesOk("50.0 between null and 5000", rule,
        "50.0 >= NULL AND 50.0 <= 5000");
    // Basic NOT BETWEEN predicates.
    RewritesOk("int_col between 10 and 20", rule,
        "int_col >= 10 AND int_col <= 20");
    RewritesOk("int_col not between 10 and 20", rule,
        "int_col < 10 OR int_col > 20");
    RewritesOk("50.0 not between null and 5000", rule,
        "50.0 < NULL OR 50.0 > 5000");

    // Nested BETWEEN predicates.
    RewritesOk(
        "int_col between if(tinyint_col between 1 and 2, 10, 20) " +
        "and cast(smallint_col between 1 and 2 as int)", rule,
        "int_col >= if(tinyint_col >= 1 AND tinyint_col <= 2, 10, 20) " +
        "AND int_col <= CAST(smallint_col >= 1 AND smallint_col <= 2 AS INT)");
    // Nested NOT BETWEEN predicates.
    RewritesOk(
        "int_col not between if(tinyint_col not between 1 and 2, 10, 20) " +
        "and cast(smallint_col not between 1 and 2 as int)", rule,
        "int_col < if(tinyint_col < 1 OR tinyint_col > 2, 10, 20) " +
        "OR int_col > CAST(smallint_col < 1 OR smallint_col > 2 AS INT)");
    // Mixed nested BETWEEN and NOT BETWEEN predicates.
    RewritesOk(
        "int_col between if(tinyint_col between 1 and 2, 10, 20) " +
        "and cast(smallint_col not between 1 and 2 as int)", rule,
        "int_col >= if(tinyint_col >= 1 AND tinyint_col <= 2, 10, 20) " +
        "AND int_col <= CAST(smallint_col < 1 OR smallint_col > 2 AS INT)");
  }
}
