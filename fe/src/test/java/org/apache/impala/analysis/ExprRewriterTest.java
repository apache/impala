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
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;

/**
 * Tests that the ExprRewriter framework covers all clauses as well as nested statements.
 * Does not test specific rewrite rules.
 */
public class ExprRewriterTest extends AnalyzerTest {

  /**
   * Replaces any Expr that does not contain a Subquery with a TRUE BoolLiteral.
   */
  static class ExprToBoolRule implements ExprRewriteRule {
    public static ExprToBoolRule INSTANCE = new ExprToBoolRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
      if (expr.contains(Subquery.class)) return expr;
      if (Predicate.IS_TRUE_LITERAL.apply(expr)) return expr;
      return new BoolLiteral(true);
    }

    private ExprToBoolRule() {}
  }

  /**
   * Replaces a TRUE BoolLiteral with a FALSE BoolLiteral.
   */
  static class TrueToFalseRule implements ExprRewriteRule {
    public static TrueToFalseRule INSTANCE = new TrueToFalseRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
      if (Predicate.IS_TRUE_LITERAL.apply(expr)) return new BoolLiteral(false);
      return expr;
    }
    private TrueToFalseRule() {}
  }

  private final ExprRewriter exprToTrue_ = new ExprRewriter(ExprToBoolRule.INSTANCE);
  private final ExprRewriter trueToFalse_ = new ExprRewriter(TrueToFalseRule.INSTANCE);

  /**
   * Analyzes 'stmt' and rewrites Exprs with 'exprToTrue_' and validates the following:
   * 1. The actual number of changed Exprs should be equal to 'expectedNumChanges'.
   * 2. Checks that the Exprs were actually rewritten by doing another round of
   *    rewriting using 'trueToFalse_'. The expected number of changes for this
   *    second rewriting is 'expectedNumExprTrees'.
   * Does not use an AnalysisContext to avoid rewriting subqueries which might alter the
   * number of expressions and complicate validation.
   */
  public void RewritesOk(String stmt, int expectedNumChanges,
      int expectedNumExprTrees) throws ImpalaException {
    // Analyze without rewrites since that's what we want to test here.
    StatementBase parsedStmt = (StatementBase) ParsesOk(stmt);
    AnalyzesOkNoRewrite(parsedStmt);
    exprToTrue_.reset();
    parsedStmt.rewriteExprs(exprToTrue_);
    Assert.assertEquals(expectedNumChanges, exprToTrue_.getNumChanges());

    // Verify that the Exprs were actually replaced.
    trueToFalse_.reset();
    parsedStmt.rewriteExprs(trueToFalse_);
    Assert.assertEquals(expectedNumExprTrees, trueToFalse_.getNumChanges());

    // Make sure the stmt can be successfully re-analyzed.
    parsedStmt.reset();
    AnalyzesOkNoRewrite(parsedStmt);
  }

  /**
   * Asserts that no rewrites are performed on the given stmt.
   */
  public void CheckNoRewrite(String stmt) throws ImpalaException {
    exprToTrue_.reset();
    AnalysisContext analysisCtx = createAnalysisCtx();
    AnalysisResult result = parseAndAnalyze(stmt, analysisCtx);
    Preconditions.checkNotNull(result.getStmt());
    Assert.assertEquals(0, exprToTrue_.getNumChanges());
  }

  // Select statement with all clauses that has 11 rewritable Expr trees.
  // We expect a total of 23 exprs to be changed.
  private final String stmt_ =
      "select a.int_col a, 10 b, 20.2 c, count(b.int_col) cnt from " +
      "functional.alltypes a join functional.alltypes b on (a.id = b.id)" +
      "where b.float_col > 1 and b.double_col > 2 " +
      "group by 1, a.string_col " +
      "having count(b.int_col) < 3 " +
      "order by a.int_col, 4 limit 10";

  @Test
  public void TestQueryStmts() throws ImpalaException {
    RewritesOk(stmt_, 23, 11);
    // Test rewriting in inline views. The view stmt is the same as the query above
    // but with an order by + limit. Expanded star exprs are not rewritten.
    RewritesOk("select * from (" + stmt_ + ") v", 23, 11);
    // Test union, 11 + 11 + 1 rewritable Expr trees.
    RewritesOk(String.format("%s union all (%s) order by cnt", stmt_, stmt_), 47, 23);
    // Test union inside an inline view.
    RewritesOk(String.format("select * from (%s union all (%s) order by cnt limit 10) v",
        stmt_, stmt_), 47, 23);
    // Constant select.
    RewritesOk("select 1, 2, 3, 4", 4, 4);
    // Values stmt.
    RewritesOk("values(1, '2', 3, 4.1), (1, '2', 3, 4.1)", 8, 8);
    // Test WHERE-clause subqueries.
    RewritesOk("select id, int_col from functional.alltypes a " +
        "where exists (select 1 from functional.alltypes " +
        "where string_col = 'test' having count(*) < 10)", 9, 5);
    RewritesOk("select id, int_col from functional.alltypes a " +
        "where a.id in (select count(*) from functional.alltypes " +
        "where string_col = 'test' having count(*) < 10)", 10, 6);
  }

  @Test
  public void TestDdlStmts() throws ImpalaException {
    RewritesOk("create table ctas_test as " + stmt_, 23, 11);
    // Create/alter view stmts are not rewritten to preserve the original SQL.
    CheckNoRewrite("create view view_test as " + stmt_);
    CheckNoRewrite("alter view functional.alltypes_view as " + stmt_);
  }

  @Test
  public void TestDmlStmts() throws ImpalaException {
    // Insert.
    RewritesOk("insert into functional.alltypes (id, int_col, float_col, bigint_col) " +
      "partition(year=2009,month=10) " + stmt_, 23, 11);

    if (RuntimeEnv.INSTANCE.isKuduSupported()) {
      // Update.
      RewritesOk("update t2 set name = 'test' from " +
          "functional.alltypes t1 join functional_kudu.dimtbl t2 on (t1.id = t2.id) " +
          "where t2.id < 10", 10, 5);
      RewritesOk("update functional_kudu.dimtbl set name = 'test', zip = 4711 " +
          "where exists (" + stmt_ + ")", 28, 16);
      // Delete.
      RewritesOk("delete a from " +
          "functional_kudu.testtbl a join functional.testtbl b on a.zip = b.zip", 4, 2);
      RewritesOk("delete functional_kudu.testtbl where exists (" + stmt_ + ")", 24, 12);
    }
  }
}
