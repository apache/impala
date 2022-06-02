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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.rewrite.EqualityDisjunctsToInRule;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;
import static org.apache.impala.analysis.ToSqlOptions.REWRITTEN;
import static org.apache.impala.analysis.ToSqlOptions.SHOW_IMPLICIT_CASTS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests that the ExprRewriter framework covers all clauses as well as nested statements.
 * It also tests some specific rewrite rules.
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
      if (Expr.IS_TRUE_LITERAL.apply(expr)) return expr;
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
      if (Expr.IS_TRUE_LITERAL.apply(expr)) return new BoolLiteral(false);
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

  private Expr analyze(String query) {
    AnalysisContext ctx = createAnalysisCtx();
    ctx.getQueryOptions().setDecimal_v2(true);
    ctx.getQueryOptions().setEnable_expr_rewrites(false);
    return ((SelectStmt) AnalyzesOk(query, ctx)).getSelectList()
        .getItems().get(0).getExpr();
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
    // Values stmt - expression rewrites are not required in this test cases.
    RewritesOk("values(1, '2', 3, 4.1), (1, '2', 3, 4.1),"
            + "(CAST(true OR false AS INT), '2', 3*1+2-4, 1.1%1)",
        0, 0);
    RewritesOk("values(CONCAT('a', 'b'), true OR true)", 0, 0);
    // Values stmt - expression rewrites are required for || and Between predicate.
    RewritesOk("values(1 <= 2 || 'impala' <> 'IMPALA'), (0.5 BETWEEN 0 AND 1),"
            + "('a' NOT BETWEEN 'b' AND 'c')",
        3, 0);
    // Values stmt - expression rewrites are required for || and Between predicate that
    // is not at root Expr.
    RewritesOk("values(1 <= 2 AND ((0.5 BETWEEN 0 AND 1) AND "
            + "(('a' || 'b') = 'ab' AND (true || false))))",
        3, 0);
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

  /**
   * construct an in-list: string_col in [offset ... offset + length)
   */
  private void CreateInList(int offset, int length, StringBuilder stmtSb) {
    stmtSb.append("string_col in(");
    for (int j = 0; j < length - 1; ++j) {
      stmtSb.append("'c").append(offset + j).append("',");
    }
    stmtSb.append("'c").append(offset + length - 1).append("')");
  }

  private void CheckNumChangesByEqualityDisjunctsToInRule(
      String stmt, int expectedNumChanges) throws ImpalaException {
    StatementBase parsedStmt = (StatementBase) ParsesOk(stmt);
    AnalyzesOkNoRewrite(parsedStmt);
    ExprRewriter rewriter = new ExprRewriter(EqualityDisjunctsToInRule.INSTANCE);
    parsedStmt.rewriteExprs(rewriter);
    Assert.assertEquals(expectedNumChanges, rewriter.getNumChanges());
  }

  @Test
  public void TestEqualityDisjunctsToInRuleSizeLimit() throws ImpalaException {
    String stmtPrefix = "select count(*) from functional.alltypes where ( ";
    // Test that EqualityDisjunctsToInRule doesn't create an expr with a number of
    // children exceeding the limit.
    {
      // Create a disjunct with 2 in-lists of length Expr.EXPR_CHILDREN_LIMIT - 1.
      StringBuilder stmtSb = new StringBuilder(stmtPrefix);
      for (int i = 0; i < 2; ++i) {
        int offset = (Expr.EXPR_CHILDREN_LIMIT - 1) * i;
        CreateInList(offset, Expr.EXPR_CHILDREN_LIMIT - 1, stmtSb);
        if (i != 1) stmtSb.append(" or ");
      }
      stmtSb.append(")");
      CheckNumChangesByEqualityDisjunctsToInRule(stmtSb.toString(), 0);
    }
    {
      // Create a disjunct with an in-list of length Expr.EXPR_CHILDREN_LIMIT - 1 and a
      // EQ predicate.
      StringBuilder stmtSb = new StringBuilder(stmtPrefix);
      CreateInList(0, Expr.EXPR_CHILDREN_LIMIT - 1, stmtSb);
      stmtSb.append("or string_col='").append(Expr.EXPR_CHILDREN_LIMIT - 1).append("')");
      CheckNumChangesByEqualityDisjunctsToInRule(stmtSb.toString(), 0);
    }
    {
      // Create a disjunct with an in-list of length Expr.EXPR_CHILDREN_LIMIT - 2 and 2
      // EQ predicates.
      StringBuilder stmtSb = new StringBuilder(stmtPrefix);
      CreateInList(0, Expr.EXPR_CHILDREN_LIMIT - 2, stmtSb);
      stmtSb.append("or string_col='").append(Expr.EXPR_CHILDREN_LIMIT - 2)
          .append("' or string_col='").append(Expr.EXPR_CHILDREN_LIMIT - 1).append("')");
      CheckNumChangesByEqualityDisjunctsToInRule(stmtSb.toString(), 1);
    }
  }

  @Test
  public void TestToSql() {
    TQueryOptions options = new TQueryOptions();
    options.setEnable_expr_rewrites(true);
    AnalysisContext ctx = createAnalysisCtx(options);

    //----------------------
    // Test query rewrites.
    //----------------------
    assertToSql(ctx, "select 1 + 1", "SELECT 1 + 1", "SELECT 2");

    assertToSql(ctx,
        "select (case when true then 1 else id end) from functional.alltypes " +
        "union " +
        "select 1 + 1",
        "SELECT (CASE WHEN TRUE THEN 1 ELSE id END) FROM functional.alltypes " +
        "UNION " +
        "SELECT 1 + 1",
        "SELECT 1 FROM functional.alltypes UNION SELECT 2");

    assertToSql(ctx,
        "values(1, '2', 3, 4.1), (1, '2', 3, 4.1)",
        "VALUES((1, '2', 3, 4.1), (1, '2', 3, 4.1))",
        "SELECT 1, '2', 3, 4.1 UNION ALL SELECT 1, '2', 3, 4.1");

    assertToSql(ctx,
        "select case when 1 = 1 then 1 else 2.0 end from functional.alltypes",
        "SELECT CASE WHEN 1 = 1 THEN 1 ELSE 2.0 END FROM functional.alltypes",
        "SELECT 1.0 FROM functional.alltypes");

    assertToSql(ctx,
        "select case when false then 1.0 else 2 end from functional.alltypes",
        "SELECT CASE WHEN FALSE THEN 1.0 ELSE 2 END FROM functional.alltypes",
        "SELECT 2.0 FROM functional.alltypes");

    assertToSql(ctx,
        "select * from functional.alltypes where case " +
        "when true = true then year < 2019 " +
        "when false then year > 2010 end",
        "SELECT * FROM functional.alltypes WHERE CASE " +
        "WHEN TRUE = TRUE THEN `year` < 2019 " +
        "WHEN FALSE THEN `year` > 2010 END",
        "SELECT * FROM functional.alltypes WHERE `year` < 2019");

    //-------------------------
    // Test subquery rewrites.
    //-------------------------
    assertToSql(ctx, "select * from (" +
        "select * from functional.alltypes where id = (select 1 + 1)) a",
        "SELECT * FROM (SELECT * FROM functional.alltypes WHERE id = (SELECT 1 + 1)) a",
        "SELECT * FROM (SELECT * FROM functional.alltypes LEFT SEMI JOIN " +
        "(SELECT 2) `$a$1` (`$c$1`) ON id = `$a$1`.`$c$1`) a");

    assertToSql(ctx,
        "select * from (select * from functional.alltypes where id = (select 1 + 1)) a " +
        "union " +
        "select * from (select * from functional.alltypes where id = (select 1 + 1)) b",
        "SELECT * FROM (SELECT * FROM functional.alltypes WHERE id = (SELECT 1 + 1)) a " +
        "UNION " +
        "SELECT * FROM (SELECT * FROM functional.alltypes WHERE id = (SELECT 1 + 1)) b",
        "SELECT * FROM (SELECT * FROM functional.alltypes LEFT SEMI JOIN (SELECT 2) " +
        "`$a$1` (`$c$1`) ON id = `$a$1`.`$c$1`) a " +
        "UNION " +
        "SELECT * FROM (SELECT * FROM functional.alltypes LEFT SEMI JOIN (SELECT 2) " +
        "`$a$1` (`$c$1`) ON id = `$a$1`.`$c$1`) b");

    assertToSql(ctx, "select * from " +
        "(select (case when true then 1 else id end) from functional.alltypes " +
        "union select 1 + 1) v",
        "SELECT * FROM (SELECT (CASE WHEN TRUE THEN 1 ELSE id END) " +
        "FROM functional.alltypes UNION SELECT 1 + 1) v",
        "SELECT * FROM (SELECT 1 FROM functional.alltypes " +
        "UNION SELECT 2) v");

    //---------------------
    // Test CTAS rewrites.
    //---------------------
    assertToSql(ctx,
        "create table ctas_test as select 1 + 1",
        "CREATE TABLE default.ctas_test\n" +
        "STORED AS TEXTFILE\n" +
        " AS SELECT 1 + 1",
        "CREATE TABLE default.ctas_test\n" +
        "STORED AS TEXTFILE\n" +
        " AS SELECT 2");

    //--------------------
    // Test DML rewrites.
    //--------------------
    // Insert
    assertToSql(ctx,
        "insert into functional.alltypes(id) partition(year=2009, month=10) " +
        "select 1 + 1",
        "INSERT INTO TABLE functional.alltypes(id) " +
        "PARTITION (`year`=2009, `month`=10) SELECT 1 + 1",
        "INSERT INTO TABLE functional.alltypes(id) " +
        "PARTITION (`year`=2009, `month`=10) SELECT 2");

    // Update.
    assertToSql(ctx,
        "update functional_kudu.alltypes "
            + "set string_col = 'test' where id = (select 1 + 1)",
        "UPDATE functional_kudu.alltypes SET string_col = 'test' "
            + "FROM functional_kudu.alltypes WHERE id = (SELECT 1 + 1)",
        "UPDATE functional_kudu.alltypes SET string_col = 'test' "
            + "FROM functional_kudu.alltypes LEFT SEMI JOIN (SELECT 2) `$a$1` (`$c$1`) "
            + "ON id = `$a$1`.`$c$1` WHERE id = (SELECT 2)");

    // Delete
    assertToSql(ctx,
        "delete functional_kudu.alltypes "
            + "where id = (select 1 + 1)",
        "DELETE FROM functional_kudu.alltypes "
            + "WHERE id = (SELECT 1 + 1)",
        "DELETE functional_kudu.alltypes "
            + "FROM functional_kudu.alltypes LEFT SEMI JOIN (SELECT 2) `$a$1` (`$c$1`) "
            + "ON id = `$a$1`.`$c$1` WHERE id = (SELECT 2)");

    //-------------------------
    // Test || rewrites.
    //-------------------------
    assertToSql(ctx,
        "select * from functional.alltypes where "
            + "int_col = 1 || int_col = 2 "
            + "|| tinyint_col > 5 || (string_col || string_col) = 'testtest'",
        "SELECT * FROM functional.alltypes WHERE "
            + "int_col = 1 OR int_col = 2 "
            + "OR tinyint_col > 5 OR (concat(string_col, string_col)) = 'testtest'",
        "SELECT * FROM functional.alltypes WHERE "
            + "int_col IN (1, 2) "
            + "OR tinyint_col > 5 OR concat(string_col, string_col) = 'testtest'");

    assertToSql(ctx,
        "select int_col = 1 || int_col = 2, string_col || 'test' "
            + "from functional.alltypes where "
            + "(bool_col || id = 2) || (string_col || 'test') = 'testtest'",
        "SELECT int_col = 1 OR int_col = 2, concat(string_col, 'test') "
            + "FROM functional.alltypes WHERE "
            + "(bool_col OR id = 2) OR (concat(string_col, 'test')) = 'testtest'",
        "SELECT int_col IN (1, 2), concat(string_col, 'test') "
            + "FROM functional.alltypes WHERE "
            + "bool_col OR id = 2 OR concat(string_col, 'test') = 'testtest'");

    // We don't do any rewrite for WITH clause.
    StatementBase stmt = (StatementBase) AnalyzesOk("with t as (select 1 + 1) " +
        "select id from functional.alltypes union select id from functional.alltypesagg",
        ctx);
    Assert.assertEquals(stmt.toSql(), stmt.toSql());
  }

  @Test
  /**
   * Test printing of implicit casts
   */
  public void TestToSqlWithImplicitCasts() {
    TQueryOptions options = new TQueryOptions();
    options.setEnable_expr_rewrites(true);
    AnalysisContext ctx = createAnalysisCtx(options);

    assertToSqlWithImplicitCasts(ctx,
        "select * from functional_kudu.alltypestiny where bigint_col < "
            + "1000 / 100",
        "SELECT * FROM functional_kudu.alltypestiny WHERE "
            + "CAST(bigint_col AS DOUBLE) < CAST(10 AS DOUBLE)");
    assertToSqlWithImplicitCasts(ctx,
        "select float_col + 1.1 from functional.alltypestiny",
        "SELECT CAST(float_col AS DECIMAL(38,9)) + CAST(1.1 AS DECIMAL(2,1)) "
            + "FROM functional.alltypestiny");
    assertToSqlWithImplicitCasts(
        ctx, "select cast(2 as bigint)", "SELECT CAST(2 AS BIGINT)");
    assertToSqlWithImplicitCasts(ctx, "select cast(2 as decimal(38,37))",
        "SELECT CAST(2.0000000000000000000000000000000000000 AS DECIMAL(38,37))");
    assertToSqlWithImplicitCasts(ctx, "select d1 - 1.1 from functional.decimal_tbl",
        "SELECT d1 - CAST(1.1 AS DECIMAL(2,1)) FROM functional.decimal_tbl");
    assertToSqlWithImplicitCasts(ctx, "select * from functional.date_tbl "
        + "where date_col = '2017-11-28'",
        "SELECT * FROM functional.date_tbl "
        + "WHERE date_col = DATE '2017-11-28'");
    assertToSqlWithImplicitCasts(ctx, "select * from functional.alltypes, "
        + "functional.date_tbl where timestamp_col = date_col",
        "SELECT * FROM functional.alltypes, functional.date_tbl "
        + "WHERE timestamp_col = CAST(date_col AS TIMESTAMP)");
    assertToSqlWithImplicitCasts(ctx, "select round(1.2345, 2) * pow(10, 10)",
        "SELECT CAST(12300000000 AS DOUBLE)");
    assertToSqlWithImplicitCasts(ctx,
        "select * from functional.alltypes where "
            + "double_col in (int_col, bigint_col)",
        "SELECT * FROM functional.alltypes WHERE double_col IN "
            + "(CAST(int_col AS DOUBLE), CAST(bigint_col AS DOUBLE))");
    assertToSqlWithImplicitCasts(ctx,
        "select * from functional.alltypes "
            + "where double_col between smallint_col and int_col",
        "SELECT * FROM functional.alltypes WHERE double_col >= "
            + "CAST(smallint_col AS DOUBLE) AND double_col <= CAST(int_col AS DOUBLE)");
    assertToSqlWithImplicitCasts(ctx,
        "select * from "
            + "(select 10 as i, 2 as j, 2013 as s) as t "
            + "where t.i < 10",
        "SELECT * FROM "
            + "(SELECT CAST(10 AS TINYINT) i, CAST(2 AS TINYINT) j, "
            + "CAST(2013 AS SMALLINT) s) t "
            + "WHERE t.i < CAST(10 AS TINYINT)");
    assertToSqlWithImplicitCasts(ctx,
        "select * from (select id, int_col, year,  sum(int_col) "
            + " over(partition by year order by id) as s from functional.alltypes) v "
            + " where year = 2009 and id = 1 and"
            + " int_col < 10 and s = 4",
        "SELECT * FROM (SELECT id, int_col, `year`, sum(int_col)"
            + " OVER (PARTITION BY `year` ORDER BY id ASC) s FROM functional.alltypes) v"
            + " WHERE `year` = CAST(2009 AS INT) AND id = CAST(1 AS INT) AND"
            + " int_col < CAST(10 AS INT) AND s = CAST(4 AS BIGINT)");
    assertToSqlWithImplicitCasts(ctx,
        "select * from functional.alltypes where "
            + "int_col = 1 or int_col = 2 "
            + "or tinyint_col > 5 AND "
            + "(float_col = 5 or double_col = 6)",
        "SELECT * FROM functional.alltypes WHERE "
            + "int_col IN (CAST(1 AS INT), CAST(2 AS INT)) "
            + "OR tinyint_col > CAST(5 AS TINYINT) AND "
            + "(float_col = CAST(5 AS FLOAT) OR double_col = CAST(6 AS DOUBLE))");

    checkNumericLiteralCasts(ctx, "tinyint_col", "1", "TINYINT");
    checkNumericLiteralCasts(ctx, "smallint_col", "1", "TINYINT");
    checkNumericLiteralCasts(ctx, "smallint_col", "1000", "SMALLINT");
    checkNumericLiteralCasts(ctx, "int_col", "1", "TINYINT");
    checkNumericLiteralCasts(ctx, "int_col", "1000", "SMALLINT");
    checkNumericLiteralCasts(ctx, "int_col", "1000000", "INT");
    checkNumericLiteralCasts(ctx, "bigint_col", "1", "TINYINT");
    checkNumericLiteralCasts(ctx, "bigint_col", "1000", "SMALLINT");
    checkNumericLiteralCasts(ctx, "bigint_col", "1000000", "INT");
    checkNumericLiteralCasts(ctx, "bigint_col", "10000000000", "BIGINT");
    checkNumericLiteralCasts(ctx, "float_col", "1", "TINYINT");
    checkNumericLiteralCasts(ctx, "float_col", "1.0", "DECIMAL(2,1)");
    checkNumericLiteralCasts(ctx, "float_col", "100000.001", "DECIMAL(9,3)");
    checkNumericLiteralCasts(ctx, "double_col", "1", "TINYINT");
    checkNumericLiteralCasts(ctx, "double_col", "1.0", "DECIMAL(2,1)");
    checkNumericLiteralCasts(ctx, "double_col", "100000.001", "DECIMAL(9,3)");
  }

  /**
   * Generate an insert query into a column and check that the toSql() with implicit casts
   * looks as expected.
   * columnName is the name of a column in functional.alltypesnopart.
   * data is the literal value to insert.
   * castColumn is the type to which the literal is expected to be cast.
   */
  private void checkNumericLiteralCasts(
      AnalysisContext ctx, String columnName, String data, String castColumn) {
    String query = "insert into table functional.alltypesnopart (" + columnName + ") "
        + "values(" + data + ")";
    String expectedToSql = "INSERT INTO TABLE "
        + "functional.alltypesnopart(" + columnName + ") "
        + "SELECT CAST(" + data + " AS " + castColumn + ")";
    assertToSqlWithImplicitCasts(ctx, query, expectedToSql);
  }

  private void assertToSql(AnalysisContext ctx, String query, String expectedToSql,
      String expectedToRewrittenSql) {
    StatementBase stmt = (StatementBase) AnalyzesOk(query, ctx);
    Assert.assertEquals(expectedToSql, stmt.toSql(DEFAULT));
    Assert.assertEquals(expectedToSql, stmt.toSql());
    Assert.assertEquals(expectedToRewrittenSql, stmt.toSql(REWRITTEN));
  }

  private void assertToSqlWithImplicitCasts(
      AnalysisContext ctx, String query, String expectedToSqlWithImplicitCasts) {
    StatementBase stmt = (StatementBase) AnalyzesOk(query, ctx);
    String actual = stmt.toSql(SHOW_IMPLICIT_CASTS);
    Assert.assertEquals("Bad sql with implicit casts from original query:\n" + query,
        expectedToSqlWithImplicitCasts, actual);
  }

  @Test
  public void TestToSqlWithAppxCountDistinctAndDefaultNdvs() {
    TQueryOptions options = new TQueryOptions();
    options.setEnable_expr_rewrites(true);

    AnalysisContext ctx = createAnalysisCtx(options);

    //----------------------
    // Test query rewrites.
    //----------------------
    String countDistinctSql = "SELECT count(DISTINCT id) FROM functional.alltypes";

    // No rewrite
    assertToSql(ctx, countDistinctSql, countDistinctSql, countDistinctSql);

    // Rewrite to ndv
    options.setAppx_count_distinct(true);
    assertToSql(createAnalysisCtx(options), countDistinctSql, countDistinctSql,
            "SELECT ndv(id) FROM functional.alltypes");

    // Rewrite to ndv(10)
    options.setDefault_ndv_scale(10);
    assertToSql(createAnalysisCtx(options), countDistinctSql, countDistinctSql,
            "SELECT ndv(id, 10) FROM functional.alltypes");

    String ndvSql = "SELECT ndv(id) FROM functional.alltypes";

    // No rewrite
    options.setDefault_ndv_scale(2);
    assertToSql(createAnalysisCtx(options), ndvSql, ndvSql, ndvSql);

    // Rewrite ndv scale
    options.setDefault_ndv_scale(9);
    assertToSql(createAnalysisCtx(options), ndvSql, ndvSql,
            "SELECT ndv(id, 9) FROM functional.alltypes");

    //-----------------------------------------------------------------------------------
    // Test complex sql which has all 3 types of functions.
    // NDV(<expr>) | NDV(<expr>, <scale>) | COUNT(DISTINCT <expr>)
    //
    // For coverage, we have these scenarios:
    // CASE 1: DEFAULT_NDV_SCALE=5(same as the value in original sql),
    //         APPX_COUNT_DISTINCT=True
    // CASE 2: DEFAULT_NDV_SCALE=5, APPX_COUNT_DISTINCT=False
    // CASE 3: DEFAULT_NDV_SCALE=9(different with original), APPX_COUNT_DISTINCT=True
    // CASE 4: DEFAULT_NDV_SCALE=3, APPX_COUNT_DISTINCT=False
    // CASE 5: DEFAULT_NDV_SCALE=2, APPX_COUNT_DISTINCT=True
    // CASE 6: DEFAULT_NDV_SCALE=2, APPX_COUNT_DISTINCT=False
    //-----------------------------------------------------------------------------------
    String sql1 = "SELECT ndv(id), ndv(id, 5), count(DISTINCT id) FROM " +
            "functional.alltypes";

    // CASE 1
    options.setDefault_ndv_scale(5).setAppx_count_distinct(true);
    assertToSql(createAnalysisCtx(options), sql1, sql1,
            "SELECT ndv(id, 5), ndv(id, 5), ndv(id, 5) FROM functional.alltypes");

    // CASE 2
    options.setDefault_ndv_scale(5).setAppx_count_distinct(false);
    assertToSql(createAnalysisCtx(options), sql1, sql1,
            "SELECT ndv(id, 5), ndv(id, 5), count(DISTINCT id)" +
                    " FROM functional.alltypes");

    // CASE 3
    options.setDefault_ndv_scale(9).setAppx_count_distinct(true);
    assertToSql(createAnalysisCtx(options), sql1, sql1,
            "SELECT ndv(id, 9), ndv(id, 5), ndv(id, 9) FROM functional.alltypes");

    // CASE 4
    options.setDefault_ndv_scale(3).setAppx_count_distinct(false);
    assertToSql(createAnalysisCtx(options), sql1, sql1,
            "SELECT ndv(id, 3), ndv(id, 5), count(DISTINCT id) " +
                    "FROM functional.alltypes");

    // CASE 5
    options.setDefault_ndv_scale(2).setAppx_count_distinct(true);
    assertToSql(createAnalysisCtx(options), sql1, sql1,
            "SELECT ndv(id), ndv(id, 5), ndv(id) FROM functional.alltypes");

    // CASE 6
    options.setDefault_ndv_scale(2).setAppx_count_distinct(false);
    assertToSql(createAnalysisCtx(options), sql1, sql1,
            "SELECT ndv(id), ndv(id, 5), count(DISTINCT id) FROM functional.alltypes");

  }

  @Test
  public void TestShouldConvertToCNF() {
    TQueryOptions options = new TQueryOptions();
    options.setEnable_expr_rewrites(false);
    AnalysisContext ctx = createAnalysisCtx(options);

    // Positive tests
    List<String> convertablePredicates = Arrays.asList("select (1=cast(1 as int))",
        "select (cast(d_date_sk as int) = 10) from tpcds_parquet.date_dim",
        "select (d_date_sk = d_year) from tpcds_parquet.date_dim",
        "select (d_date_sk between 1 and 10) from tpcds_parquet.date_dim",
        "select (d_date_sk in (1,2,10)) from tpcds_parquet.date_dim",
        "select (d_date_sk is null) from tpcds_parquet.date_dim", "select (cos(1) = 1.1)",
        "select (cast(d_date_sk as int) * 2 = 10) from tpcds_parquet.date_dim",
        "select ((2 = cast(1 as int)) and (cos(1) = 1))",
        "select ((2 = cast(1 as int)) or (cast(0 as int) is not null))",
        "select (sin(cos(2*pi())))");

    for (String query: convertablePredicates) {
      Expr expr = analyze(query);
      assertTrue("Should convert to CNF: "+query, expr.shouldConvertToCNF());
    }

    // Negative tests
    List<String> inconvertablePredicates = Arrays.asList(
        "select (upper(d_day_name) = 'A') from tpcds_parquet.date_dim",
        "select (d_day_name like '%A') from tpcds_parquet.date_dim",
        "select (coalesce(d_date_sk, -1) = d_year) from tpcds_parquet.date_dim",
        "select (log10(cast(1 + length(upper(d_day_name)) as double)) > 1.0) from "
         + "tpcds_parquet.date_dim");

    for (String query: inconvertablePredicates) {
      Expr expr = analyze(query);
      assertFalse("Should not convert to CNF: "+query, expr.shouldConvertToCNF());
    }
  }
}
