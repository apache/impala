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

import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.rewrite.BetweenToCompoundRule;
import org.apache.impala.rewrite.EqualityDisjunctsToInRule;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.rewrite.ExtractCommonConjunctRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.rewrite.NormalizeBinaryPredicatesRule;
import org.apache.impala.rewrite.NormalizeCountStarRule;
import org.apache.impala.rewrite.NormalizeExprsRule;
import org.apache.impala.rewrite.RemoveRedundantStringCast;
import org.apache.impala.rewrite.SimplifyConditionalsRule;
import org.apache.impala.rewrite.SimplifyDistinctFromRule;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests ExprRewriteRules.
 */
public class ExprRewriteRulesTest extends FrontendTestBase {

  /** Wraps an ExprRewriteRule to count how many times it's been applied. */
  private static class CountingRewriteRuleWrapper implements ExprRewriteRule {
    int rewrites;
    ExprRewriteRule wrapped;

    CountingRewriteRuleWrapper(ExprRewriteRule wrapped) {
      this.wrapped = wrapped;
    }

    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
      Expr ret = wrapped.apply(expr, analyzer);
      if (expr != ret) rewrites++;
      return ret;
    }
  }

  public Expr RewritesOk(String exprStr, ExprRewriteRule rule, String expectedExprStr)
      throws ImpalaException {
    return RewritesOk("functional.alltypessmall", exprStr, rule, expectedExprStr);
  }

  public Expr RewritesOk(String tableName, String exprStr, ExprRewriteRule rule, String expectedExprStr)
      throws ImpalaException {
    return RewritesOk(tableName, exprStr, Lists.newArrayList(rule), expectedExprStr);
  }

  public Expr RewritesOk(String exprStr, List<ExprRewriteRule> rules, String expectedExprStr)
      throws ImpalaException {
    return RewritesOk("functional.alltypessmall", exprStr, rules, expectedExprStr);
  }

  public Expr RewritesOk(String tableName, String exprStr, List<ExprRewriteRule> rules,
      String expectedExprStr) throws ImpalaException {
    String stmtStr = "select " + exprStr + " from " + tableName;
    // Analyze without rewrites since that's what we want to test here.
    SelectStmt stmt = (SelectStmt) ParsesOk(stmtStr);
    AnalyzesOkNoRewrite(stmt);
    Expr origExpr = stmt.getSelectList().getItems().get(0).getExpr();
    Expr rewrittenExpr =
        verifyExprEquivalence(origExpr, expectedExprStr, rules, stmt.getAnalyzer());
    return rewrittenExpr;
  }

  public Expr RewritesOkWhereExpr(String exprStr, ExprRewriteRule rule, String expectedExprStr)
      throws ImpalaException {
    return RewritesOkWhereExpr("functional.alltypessmall", exprStr, rule, expectedExprStr);
  }

  public Expr RewritesOkWhereExpr(String tableName, String exprStr, ExprRewriteRule rule, String expectedExprStr)
      throws ImpalaException {
    return RewritesOkWhereExpr(tableName, exprStr, Lists.newArrayList(rule), expectedExprStr);
  }

  public Expr RewritesOkWhereExpr(String tableName, String exprStr, List<ExprRewriteRule> rules,
      String expectedExprStr) throws ImpalaException {
    String stmtStr = "select count(1)  from " + tableName + " where " + exprStr;
    // Analyze without rewrites since that's what we want to test here.
    SelectStmt stmt = (SelectStmt) ParsesOk(stmtStr);
    AnalyzesOkNoRewrite(stmt);
    Expr origExpr = stmt.getWhereClause();
    Expr rewrittenExpr =
        verifyExprEquivalence(origExpr, expectedExprStr, rules, stmt.getAnalyzer());
    return rewrittenExpr;
  }

  private Expr verifyExprEquivalence(Expr origExpr, String expectedExprStr,
      List<ExprRewriteRule> rules, Analyzer analyzer) throws AnalysisException {
    String origSql = origExpr.toSql();

    List<ExprRewriteRule> wrappedRules = Lists.newArrayList();
    for (ExprRewriteRule r : rules) {
      wrappedRules.add(new CountingRewriteRuleWrapper(r));
    }
    ExprRewriter rewriter = new ExprRewriter(wrappedRules);

    Expr rewrittenExpr = rewriter.rewrite(origExpr, analyzer);
    String rewrittenSql = rewrittenExpr.toSql();
    boolean expectChange = expectedExprStr != null;
    if (expectedExprStr != null) {
      assertEquals(expectedExprStr, rewrittenSql);
      // Asserts that all specified rules fired at least once. This makes sure that the
      // rules being tested are, in fact, being executed. A common mistake is to write
      // an expression that's re-written by the constant folder before getting to the
      // rule that is intended for the test.
      for (ExprRewriteRule r : wrappedRules) {
        CountingRewriteRuleWrapper w = (CountingRewriteRuleWrapper) r;
        Assert.assertTrue("Rule " + w.wrapped.toString() + " didn't fire.",
          w.rewrites > 0);
      }
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
  public void TestBetweenToCompoundRule() throws ImpalaException {
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

  @Test
  public void TestExtractCommonConjunctsRule() throws ImpalaException {
    ExprRewriteRule rule = ExtractCommonConjunctRule.INSTANCE;

    // One common conjunct: int_col < 10
    RewritesOk(
        "(int_col < 10 and bigint_col < 10) or " +
        "(string_col = '10' and int_col < 10)", rule,
        "int_col < 10 AND ((bigint_col < 10) OR (string_col = '10'))");
    // One common conjunct in multiple disjuncts: int_col < 10
    RewritesOk(
        "(int_col < 10 and bigint_col < 10) or " +
        "(string_col = '10' and int_col < 10) or " +
        "(id < 20 and int_col < 10) or " +
        "(int_col < 10 and float_col > 3.14)", rule,
        "int_col < 10 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Same as above but with a bushy OR tree.
    RewritesOk(
        "((int_col < 10 and bigint_col < 10) or " +
        " (string_col = '10' and int_col < 10)) or " +
        "((id < 20 and int_col < 10) or " +
        " (int_col < 10 and float_col > 3.14))", rule,
        "int_col < 10 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Multiple common conjuncts: int_col < 10, bool_col is null
    RewritesOk(
        "(int_col < 10 and bigint_col < 10 and bool_col is null) or " +
        "(bool_col is null and string_col = '10' and int_col < 10)", rule,
        "int_col < 10 AND bool_col IS NULL AND " +
        "((bigint_col < 10) OR (string_col = '10'))");
    // Negated common conjunct: !(int_col=5 or tinyint_col > 9)
    RewritesOk(
        "(!(int_col=5 or tinyint_col > 9) and double_col = 7) or " +
        "(!(int_col=5 or tinyint_col > 9) and double_col = 8)", rule,
        "NOT (int_col = 5 OR tinyint_col > 9) AND " +
        "((double_col = 7) OR (double_col = 8))");

    // Test common BetweenPredicate: int_col between 10 and 30
    RewritesOk(
        "(int_col between 10 and 30 and bigint_col < 10) or " +
        "(string_col = '10' and int_col between 10 and 30) or " +
        "(id < 20 and int_col between 10 and 30) or " +
        "(int_col between 10 and 30 and float_col > 3.14)", rule,
        "int_col BETWEEN 10 AND 30 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Test common NOT BetweenPredicate: int_col not between 10 and 30
    RewritesOk(
        "(int_col not between 10 and 30 and bigint_col < 10) or " +
        "(string_col = '10' and int_col not between 10 and 30) or " +
        "(id < 20 and int_col not between 10 and 30) or " +
        "(int_col not between 10 and 30 and float_col > 3.14)", rule,
        "int_col NOT BETWEEN 10 AND 30 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Test mixed BetweenPredicates are not common.
    RewritesOk(
        "(int_col not between 10 and 30 and bigint_col < 10) or " +
        "(string_col = '10' and int_col between 10 and 30) or " +
        "(id < 20 and int_col not between 10 and 30) or " +
        "(int_col between 10 and 30 and float_col > 3.14)", rule,
        null);

    // All conjuncts are common.
    RewritesOk(
        "(int_col < 10 and id between 5 and 6) or " +
        "(id between 5 and 6 and int_col < 10) or " +
        "(int_col < 10 and id between 5 and 6)", rule,
        "int_col < 10 AND id BETWEEN 5 AND 6");
    // Complex disjuncts are redundant.
    RewritesOk(
        "(int_col < 10) or " +
        "(int_col < 10 and bigint_col < 10 and bool_col is null) or " +
        "(int_col < 10) or " +
        "(bool_col is null and int_col < 10)", rule,
        "int_col < 10");

    // Due to the shape of the original OR tree we are left with redundant
    // disjuncts after the extraction.
    RewritesOk(
        "(int_col < 10 and bigint_col < 10) or " +
        "(string_col = '10' and int_col < 10) or " +
        "(id < 20 and int_col < 10) or " +
        "(int_col < 10 and id < 20)", rule,
        "int_col < 10 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR (id < 20) OR (id < 20))");
  }

  /**
   * Only contains very basic tests for a few interesting cases. More thorough
   * testing is done in expr-test.cc.
   */
  @Test
  public void TestFoldConstantsRule() throws ImpalaException {
    ExprRewriteRule rule = FoldConstantsRule.INSTANCE;

    RewritesOk("1 + 1", rule, "2");
    RewritesOk("1 + 1 + 1 + 1 + 1", rule, "5");
    RewritesOk("10 - 5 - 2 - 1 - 8", rule, "-6");
    RewritesOk("cast('2016-11-09' as timestamp)", rule,
        "TIMESTAMP '2016-11-09 00:00:00'");
    RewritesOk("cast('2016-11-09' as timestamp) + interval 1 year", rule,
        "TIMESTAMP '2017-11-09 00:00:00'");

    // Tests correct handling of strings with escape sequences.
    RewritesOk("'_' LIKE '\\\\_'", rule, "TRUE");
    RewritesOk("base64decode(base64encode('\\047\\001\\132\\060')) = " +
      "'\\047\\001\\132\\060'", rule, "TRUE");

    // Tests correct handling of strings with chars > 127. Should not be folded.
    RewritesOk("hex(unhex(hex(unhex('D3'))))", rule, null);
    // Tests that non-deterministic functions are not folded.
    RewritesOk("rand()", rule, null);
    RewritesOk("random()", rule, null);
    RewritesOk("uuid()", rule, null);
    // Tests that exprs that warn during their evaluation are not folded.
    RewritesOk("coalesce(1.8, cast(int_col as decimal(38,38)))", rule, null);
  }

  @Test
  public void TestSimplifyConditionalsRule() throws ImpalaException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;

    // IF
    RewritesOk("if(true, id, id+1)", rule, "id");
    RewritesOk("if(false, id, id+1)", rule, "id + 1");
    RewritesOk("if(null, id, id+1)", rule, "id + 1");
    RewritesOk("if(id = 0, true, false)", rule, null);

    // IFNULL and its aliases
    for (String f : ImmutableList.of("ifnull", "isnull", "nvl")) {
      RewritesOk(f + "(null, id)", rule, "id");
      RewritesOk(f + "(null, null)", rule, "NULL");
      RewritesOk(f + "(id, id + 1)", rule, null);

      RewritesOk(f + "(1, 2)", rule, "1");
      RewritesOk(f + "(0, id)", rule, "0");
      // non literal constants shouldn't be simplified by the rule
      RewritesOk(f + "(1 + 1, id)", rule, null);
      RewritesOk(f + "(NULL + 1, id)", rule, null);
    }

    // CompoundPredicate
    RewritesOk("false || id = 0", rule, "id = 0");
    RewritesOk("true || id = 0", rule, "TRUE");
    RewritesOk("false && id = 0", rule, "FALSE");
    RewritesOk("true && id = 0", rule, "id = 0");
    // NULL with a non-constant other child doesn't get rewritten.
    RewritesOk("null && id = 0", rule, null);
    RewritesOk("null || id = 0", rule, null);

    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);
    // CASE with caseExpr
    // Single TRUE case with no preceding non-constant cases.
    RewritesOk("case 1 when 0 then id when 1 then id + 1 when 2 then id + 2 end", rule,
        "id + 1");
    // SINGLE TRUE case with preceding non-constant case.
    RewritesOk("case 1 when id then id when 1 then id + 1 end", rule,
        "CASE 1 WHEN id THEN id ELSE id + 1 END");
    // Single FALSE case.
    RewritesOk("case 0 when 1 then 1 when id then id + 1 end", rule,
        "CASE 0 WHEN id THEN id + 1 END");
    // All FALSE, return ELSE.
    RewritesOk("case 2 when 0 then id when 1 then id * 2 else 0 end", rule, "0");
    // All FALSE, return implicit NULL ELSE.
    RewritesOk("case 3 when 0 then id when 1 then id + 1 end", rule, "NULL");
    // Multiple TRUE, first one becomes ELSE.
    RewritesOk("case 1 when id then id when 2 - 1 then id + 1 when 1 then id + 2 end",
        rules, "CASE 1 WHEN id THEN id ELSE id + 1 END");
    // When NULL.
    RewritesOk("case 0 when null then id else 1 end", rule, "1");
    // All non-constant, don't rewrite.
    RewritesOk("case id when 1 then 1 when 2 then 2 else 3 end", rule, null);

    // CASE without caseExpr
    // Single TRUE case with no predecing non-constant case.
    RewritesOk("case when FALSE then 0 when TRUE then 1 end", rule, "1");
    // Single TRUE case with preceding non-constant case.
    RewritesOk("case when id = 0 then 0 when true then 1 when id = 2 then 2 end", rule,
        "CASE WHEN id = 0 THEN 0 ELSE 1 END");
    // Single FALSE case.
    RewritesOk("case when id = 0 then 0 when false then 1 when id = 2 then 2 end", rule,
        "CASE WHEN id = 0 THEN 0 WHEN id = 2 THEN 2 END");
    // All FALSE, return ELSE.
    RewritesOk(
        "case when false then 1 when false then 2 else id + 1 end", rule, "id + 1");
    // All FALSE, return implicit NULL ELSE.
    RewritesOk("case when false then 0 end", rule, "NULL");
    // Multiple TRUE, first one becomes ELSE.
    RewritesOk("case when id = 1 then 0 when 2 = 1 + 1 then 1 when true then 2 end",
        rules, "CASE WHEN id = 1 THEN 0 ELSE 1 END");
    // When NULL.
    RewritesOk("case when id = 0 then 0 when null then 1 else 2 end", rule,
        "CASE WHEN id = 0 THEN 0 ELSE 2 END");
    // All non-constant, don't rewrite.
    RewritesOk("case when id = 0 then 0 when id = 1 then 1 end", rule, null);

    // DECODE
    // Single TRUE case with no preceding non-constant case.
    RewritesOk("decode(1, 0, id, 1, id + 1, 2, id + 2)", rules, "id + 1");
    // Single TRUE case with predecing non-constant case.
    RewritesOk("decode(1, id, id, 1, id + 1, 0)", rules,
        "CASE WHEN 1 = id THEN id ELSE id + 1 END");
    // Single FALSE case.
    RewritesOk("decode(1, 0, id, tinyint_col, id + 1)", rules,
        "CASE WHEN 1 = tinyint_col THEN id + 1 END");
    // All FALSE, return ELSE.
    RewritesOk("decode(1, 0, id, 2, 2, 3)", rules, "3");
    // All FALSE, return implicit NULL ELSE.
    RewritesOk("decode(1, 1 + 1, id, 1 + 2, 3)", rules, "NULL");
    // Multiple TRUE, first one becomes ELSE.
    RewritesOk("decode(1, id, id, 1 + 1, 0, 1 * 1, 1, 2 - 1, 2)", rules,
        "CASE WHEN 1 = id THEN id ELSE 1 END");
    // When NULL - DECODE allows the decodeExpr to equal NULL (see CaseExpr.java), so the
    // NULL case is not treated as a constant FALSE and removed.
    RewritesOk("decode(id, null, 0, 1)", rules, null);
    // All non-constant, don't rewrite.
    RewritesOk("decode(id, 1, 1, 2, 2)", rules, null);

    // IMPALA-5125: Exprs containing aggregates should not be rewritten if the rewrite
    // eliminates all aggregates.
    RewritesOk("if(true, 0, sum(id))", rule, null);
    RewritesOk("if(false, max(id), min(id))", rule, "min(id)");
    RewritesOk("true || sum(id) = 0", rule, null);
    RewritesOk("ifnull(null, max(id))", rule, "max(id)");
    RewritesOk("ifnull(1, max(id))", rule, null);
    RewritesOk("case when true then 0 when false then sum(id) end", rule, null);
    RewritesOk(
        "case when true then count(id) when false then sum(id) end", rule, "count(id)");

    // IMPALA-5016: Simplify COALESCE function
    // Test skipping leading nulls.
    RewritesOk("coalesce(null, id, year)", rule, "coalesce(id, year)");
    RewritesOk("coalesce(null, 1, id)", rule, "1");
    RewritesOk("coalesce(null, null, id)", rule, "id");
    // If the leading parameter is a non-NULL constant, rewrite to that constant.
    RewritesOk("coalesce(1, id, year)", rule, "1");
    // If COALESCE has only one parameter, rewrite to the parameter.
    RewritesOk("coalesce(id)", rule, "id");
    // If all parameters are NULL, rewrite to NULL.
    RewritesOk("coalesce(null, null)", rule, "NULL");
    // Do not rewrite non-literal constant exprs, rely on constant folding.
    RewritesOk("coalesce(null is null, id)", rule, null);
    RewritesOk("coalesce(10 + null, id)", rule, null);
    // Combine COALESCE rule with FoldConstantsRule.
    RewritesOk("coalesce(1 + 2, id, year)", rules, "3");
    RewritesOk("coalesce(null is null, bool_col)", rules, "TRUE");
    RewritesOk("coalesce(10 + null, id, year)", rules, "coalesce(id, year)");
    // Don't rewrite based on nullability of slots. TODO (IMPALA-5753).
    RewritesOk("coalesce(year, id)", rule, null);
    RewritesOk("functional_kudu.alltypessmall", "coalesce(id, year)", rule, null);
  }

  @Test
  public void TestNormalizeExprsRule() throws ImpalaException {
    ExprRewriteRule rule = NormalizeExprsRule.INSTANCE;

    // CompoundPredicate
    RewritesOk("id = 0 OR false", rule, "FALSE OR id = 0");
    RewritesOk("null AND true", rule, "TRUE AND NULL");
    // The following already have a BoolLiteral left child and don't get rewritten.
    RewritesOk("true and id = 0", rule, null);
    RewritesOk("false or id = 1", rule, null);
    RewritesOk("false or true", rule, null);
  }

  @Test
  public void TestNormalizeBinaryPredicatesRule() throws ImpalaException {
    ExprRewriteRule rule = NormalizeBinaryPredicatesRule.INSTANCE;

    RewritesOk("0 = id", rule, "id = 0");
    RewritesOk("cast(0 as double) = id", rule, "id = CAST(0 AS DOUBLE)");
    RewritesOk("1 + 1 = cast(id as int)", rule, "CAST(id AS INT) = 1 + 1");
    RewritesOk("5 = id + 2", rule, "id + 2 = 5");
    RewritesOk("5 + 3 = id", rule, "id = 5 + 3");
    RewritesOk("tinyint_col + smallint_col = int_col", rule,
        "int_col = tinyint_col + smallint_col");


    // Verify that these don't get rewritten.
    RewritesOk("5 = 6", rule, null);
    RewritesOk("id = 5", rule, null);
    RewritesOk("cast(id as int) = int_col", rule, null);
    RewritesOk("int_col = cast(id as int)", rule, null);
    RewritesOk("int_col = tinyint_col", rule, null);
    RewritesOk("tinyint_col = int_col", rule, null);
  }

  @Test
  public void TestEqualityDisjunctsToInRule() throws ImpalaException {
    ExprRewriteRule edToInrule = EqualityDisjunctsToInRule.INSTANCE;
    ExprRewriteRule normalizeRule = NormalizeBinaryPredicatesRule.INSTANCE;
    List<ExprRewriteRule> comboRules = Lists.newArrayList(normalizeRule,
        edToInrule);

    RewritesOk("int_col = 1 or int_col = 2", edToInrule, "int_col IN (1, 2)");
    RewritesOk("int_col = 1 or int_col = 2 or int_col = 3", edToInrule,
        "int_col IN (1, 2, 3)");
    RewritesOk("(int_col = 1 or int_col = 2) or (int_col = 3 or int_col = 4)", edToInrule,
        "int_col IN (1, 2, 3, 4)");
    RewritesOk("float_col = 1.1 or float_col = 2.2 or float_col = 3.3",
        edToInrule, "float_col IN (1.1, 2.2, 3.3)");
    RewritesOk("string_col = '1' or string_col = '2' or string_col = '3'",
        edToInrule, "string_col IN ('1', '2', '3')");
    RewritesOk("bool_col = true or bool_col = false or bool_col = true", edToInrule,
        "bool_col IN (TRUE, FALSE, TRUE)");
    RewritesOk("bool_col = null or bool_col = null or bool_col is null", edToInrule,
        "bool_col IN (NULL, NULL) OR bool_col IS NULL");
    RewritesOk("int_col * 3 = 6 or int_col * 3 = 9 or int_col * 3 = 12",
        edToInrule, "int_col * 3 IN (6, 9, 12)");

    // cases where rewrite should happen partially
    RewritesOk("(int_col = 1 or int_col = 2) or (int_col = 3 and int_col = 4)",
        edToInrule, "int_col IN (1, 2) OR (int_col = 3 AND int_col = 4)");
    RewritesOk(
        "1 = int_col or 2 = int_col or 3 = int_col AND (float_col = 5 or float_col = 6)",
        edToInrule,
        "1 = int_col OR 2 = int_col OR 3 = int_col AND float_col IN (5, 6)");
    RewritesOk("int_col * 3 = 6 or int_col * 3 = 9 or int_col * 3 <= 12",
        edToInrule, "int_col * 3 IN (6, 9) OR int_col * 3 <= 12");

    // combo rules
    RewritesOk(
        "1 = int_col or 2 = int_col or 3 = int_col AND (float_col = 5 or float_col = 6)",
        comboRules, "int_col IN (1, 2) OR int_col = 3 AND float_col IN (5, 6)");

    // existing in predicate
    RewritesOk("int_col in (1,2) or int_col = 3", edToInrule,
        "int_col IN (1, 2, 3)");
    RewritesOk("int_col = 1 or int_col in (2, 3)", edToInrule,
        "int_col IN (2, 3, 1)");
    RewritesOk("int_col in (1, 2) or int_col in (3, 4)", edToInrule,
        "int_col IN (1, 2, 3, 4)");

    // no rewrite
    RewritesOk("int_col = smallint_col or int_col = bigint_col ", edToInrule, null);
    RewritesOk("int_col = 1 or int_col = int_col ", edToInrule, null);
    RewritesOk("int_col = 1 or int_col = int_col + 3 ", edToInrule, null);
    RewritesOk("int_col in (1, 2) or int_col = int_col + 3 ", edToInrule, null);
    RewritesOk("int_col not in (1,2) or int_col = 3", edToInrule, null);
    RewritesOk("int_col = 3 or int_col not in (1,2)", edToInrule, null);
    RewritesOk("int_col not in (1,2) or int_col not in (3, 4)", edToInrule, null);
    RewritesOk("int_col in (1,2) or int_col not in (3, 4)", edToInrule, null);

    // TODO if subqueries are supported in OR clause in future, add tests to cover the same.
    RewritesOkWhereExpr(
        "int_col = 1 and int_col in "
            + "(select smallint_col from functional.alltypessmall where smallint_col<10)",
        edToInrule, null);
  }

  @Test
  public void TestNormalizeCountStarRule() throws ImpalaException {
    ExprRewriteRule rule = NormalizeCountStarRule.INSTANCE;

    RewritesOk("count(1)", rule, "count(*)");
    RewritesOk("count(5)", rule, "count(*)");

    // Verify that these don't get rewritten.
    RewritesOk("count(null)", rule, null);
    RewritesOk("count(id)", rule, null);
    RewritesOk("count(1 + 1)", rule, null);
    RewritesOk("count(1 + null)", rule, null);
  }

  @Test
  public void TestSimplifyDistinctFromRule() throws ImpalaException {
    ExprRewriteRule rule = SimplifyDistinctFromRule.INSTANCE;

    // Can be simplified
    RewritesOk("bool_col IS DISTINCT FROM bool_col", rule, "FALSE");
    RewritesOk("bool_col IS NOT DISTINCT FROM bool_col", rule, "TRUE");
    RewritesOk("bool_col <=> bool_col", rule, "TRUE");

    // Verify nothing happens
    RewritesOk("bool_col IS NOT DISTINCT FROM int_col", rule, null);
    RewritesOk("bool_col IS DISTINCT FROM int_col", rule, null);

    // IF with distinct and distinct from
    List<ExprRewriteRule> rules = Lists.newArrayList(
        SimplifyConditionalsRule.INSTANCE,
        SimplifyDistinctFromRule.INSTANCE);
    RewritesOk("if(bool_col is distinct from bool_col, 1, 2)", rules, "2");
    RewritesOk("if(bool_col is not distinct from bool_col, 1, 2)", rules, "1");
    RewritesOk("if(bool_col <=> bool_col, 1, 2)", rules, "1");
    RewritesOk("if(bool_col <=> NULL, 1, 2)", rules, null);
  }

  @Test
  public void TestRemoveRedundantStringCastRule() throws ImpalaException {
    ExprRewriteRule removeRule = RemoveRedundantStringCast.INSTANCE;
    ExprRewriteRule foldConstantRule = FoldConstantsRule.INSTANCE;
    List<ExprRewriteRule> comboRules = Lists.newArrayList(removeRule, foldConstantRule);

    // Can be simplified.
    RewritesOk("cast(tinyint_col as string) = '100'", comboRules, "tinyint_col = 100");
    RewritesOk("cast(smallint_col as string) = '1000'", comboRules,
        "smallint_col = 1000");
    RewritesOk("cast(int_col as string) = '123456'", comboRules, "int_col = 123456");
    RewritesOk("cast(bigint_col as string) = '9223372036854775807'", comboRules,
        "bigint_col = 9223372036854775807");
    RewritesOk("cast(float_col as string) = '1000.5'", comboRules, "float_col = 1000.5");
    RewritesOk("cast(double_col as string) = '1000.5'", comboRules,
        "double_col = 1000.5");
    RewritesOk("cast(timestamp_col as string) = '2009-01-01 00:01:00'", comboRules,
        "timestamp_col = TIMESTAMP '2009-01-01 00:01:00'");
    RewritesOk("functional.decimal_tiny", "cast(c1 as string) = '2.2222'", comboRules,
        "c1 = 2.2222");

    RewritesOk("cast(tinyint_col as string) = '-100'", comboRules, "tinyint_col = -100");
    RewritesOk("cast(smallint_col as string) = '-1000'", comboRules,
        "smallint_col = -1000");
    RewritesOk("cast(int_col as string) = '-123456'", comboRules, "int_col = -123456");
    RewritesOk("cast(bigint_col as string) = '-9223372036854775807'", comboRules,
        "bigint_col = -9223372036854775807");
    RewritesOk("cast(float_col as string) = '-1000.5'", comboRules,
        "float_col = -1000.5");
    RewritesOk("cast(double_col as string) = '-1000.5'", comboRules,
        "double_col = -1000.5");
    RewritesOk("functional.decimal_tiny", "cast(c1 as string) = '-2.2222'", comboRules,
        "c1 = -2.2222");

    // Works for VARCHAR/CHAR.
    RewritesOk("cast(tinyint_col as char(3)) = '100'", comboRules, "tinyint_col = 100");
    RewritesOk("cast(tinyint_col as varchar(3)) = '100'", comboRules,
        "tinyint_col = 100");
    RewritesOk("functional.chars_tiny", "cast(cs as string) = 'abc'",
        comboRules, "cs = 'abc  '"); // column 'cs' is char(5), hence the trailing spaces.
    RewritesOk("functional.chars_tiny", "cast(vc as string) = 'abc'",
        comboRules, "vc = 'abc'");

    // Works with complex expressions on both sides.
    RewritesOk("cast(cast(int_col + 1 as double) as string) = '123456'", comboRules,
        "CAST(int_col + 1 AS DOUBLE) = 123456");
    RewritesOk("cast(int_col + 1 as string) = concat('123', '456')", comboRules,
        "int_col + 1 = 123456");
    RewritesOk("cast(int_col as string) = ltrim(concat('     123', '456'))", comboRules,
        "int_col = 123456");
    RewritesOk("cast(int_col as string) = strleft('123456789', 6)", comboRules,
        "int_col = 123456");
    RewritesOk("cast(tinyint_col as char(3)) = cast(100 as char(3))", comboRules,
        "tinyint_col = 100");
    RewritesOk("cast(tinyint_col as char(3)) = cast(100 as varchar(3))", comboRules,
        "tinyint_col = 100");

    // Verify nothing happens.
    RewritesOk("cast(tinyint_col as string) = '0100'", comboRules, null);
    RewritesOk("cast(tinyint_col as string) = '01000'", comboRules, null);
    RewritesOk("cast(smallint_col as string) = '01000'", comboRules, null);
    RewritesOk("cast(smallint_col as string) = '1000000000'", comboRules, null);
    RewritesOk("cast(int_col as string) = '02147483647'", comboRules, null);
    RewritesOk("cast(int_col as string) = '21474836470'", comboRules, null);
    RewritesOk("cast(bigint_col as string) = '09223372036854775807'", comboRules, null);
    RewritesOk("cast(bigint_col as string) = '92233720368547758070'", comboRules, null);
    RewritesOk("cast(float_col as string) = '01000.5'", comboRules, null);
    RewritesOk("cast(double_col as string) = '01000.5'", comboRules, null);
    RewritesOk("functional.decimal_tiny", "cast(c1 as string) = '02.2222'", comboRules,
        null);
    RewritesOk("functional.decimal_tiny", "cast(c1 as string) = '2.22'",
        comboRules, null);
    RewritesOk("cast(timestamp_col as string) = '2009-15-01 00:01:00'", comboRules, null);
    RewritesOk("cast(tinyint_col as char(4)) = '0100'", comboRules, null);
    RewritesOk("cast(tinyint_col as varchar(4)) = '0100'", comboRules, null);
    RewritesOk("cast(tinyint_col as char(2)) = '100'", comboRules, null);
    RewritesOk("cast(tinyint_col as varchar(2)) = '100'", comboRules, null);

    // 'NULL' is treated like any other string, so no conversion should take place.
    RewritesOk("cast(tinyint_col as string) = 'NULL'", comboRules, null);
    RewritesOk("cast(smallint_col as string) = 'NULL'", comboRules, null);
    RewritesOk("cast(int_col as string) = 'NULL'", comboRules, null);
    RewritesOk("cast(bigint_col as string) = 'NULL'", comboRules, null);
    RewritesOk("cast(float_col as string) = 'NULL'", comboRules, null);
    RewritesOk("cast(double_col as string) = 'NULL'", comboRules, null);
    RewritesOk("functional.decimal_tiny", "cast(c1 as string) = 'NULL'",
        comboRules, null);
    RewritesOk("cast(timestamp_col as string) = 'NULL'", comboRules, null);
  }

  /**
   * NULLIF gets converted to an IF, and has cases where
   * it can be further simplified via SimplifyDistinctFromRule.
   */
  @Test
  public void TestNullif() throws ImpalaException {
    List<ExprRewriteRule> rules = Lists.newArrayList(
        SimplifyConditionalsRule.INSTANCE,
        SimplifyDistinctFromRule.INSTANCE);

    // nullif: converted to if and simplified
    RewritesOk("nullif(bool_col, bool_col)", rules, "NULL");

    // works because the expression tree is identical;
    // more complicated things like nullif(int_col + 1, 1 + int_col)
    // are not simplified
    RewritesOk("nullif(1 + int_col, 1 + int_col)", rules, "NULL");
  }
}
