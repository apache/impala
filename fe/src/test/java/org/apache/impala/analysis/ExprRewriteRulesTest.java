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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.AnalysisContext.AnalysisDriverImpl;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.AnalysisSessionFixture;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.QueryFixture;
import org.apache.impala.common.SqlCastException;
import org.apache.impala.rewrite.BetweenToCompoundRule;
import org.apache.impala.rewrite.SimplifyCastExprRule;
import org.apache.impala.rewrite.ConvertToCNFRule;
import org.apache.impala.rewrite.EqualityDisjunctsToInRule;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.rewrite.ExtractCommonConjunctRule;
import org.apache.impala.rewrite.ExtractCompoundVerticalBarExprRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.rewrite.NormalizeBinaryPredicatesRule;
import org.apache.impala.rewrite.NormalizeCountStarRule;
import org.apache.impala.rewrite.NormalizeExprsRule;
import org.apache.impala.rewrite.SimplifyCastStringToTimestamp;
import org.apache.impala.rewrite.SimplifyConditionalsRule;
import org.apache.impala.rewrite.SimplifyDistinctFromRule;
import org.apache.impala.service.CompilerFactory;
import org.apache.impala.service.CompilerFactoryImpl;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests ExprRewriteRules.
 */
public class ExprRewriteRulesTest extends FrontendTestBase {
  /**
   * Wraps an ExprRewriteRule to count how many times it's been applied.
   */
  public static class CountingRewriteRuleWrapper implements ExprRewriteRule {
    int rewrites_;
    final ExprRewriteRule wrapped_;

    CountingRewriteRuleWrapper(ExprRewriteRule wrapped) {
      this.wrapped_ = wrapped;
    }

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
      Expr ret = wrapped_.apply(expr, analyzer);
      if (expr != ret) { rewrites_++; }
      return ret;
    }
  }

  /**
   * Specialized form of the Select fixture which analyzes a query without
   * rewrites. Use this to invoke the rewrite engine within the test itself.
   * Note: no analysis context is created in this case.
   */
  public static class SelectRewriteFixture extends QueryFixture.SelectFixture {
    private Analyzer analyzer_;

    public SelectRewriteFixture(AnalysisSessionFixture analysisFixture) {
      super(analysisFixture);
    }

    @Override
    public StatementBase analyze() throws AnalysisException {
      Preconditions.checkState(analyzer_ == null, "Already analyzed");
      stmt_ = parse();
      analysisCtx_ = makeAnalysisContext();
      analyzer_ = AnalysisDriverImpl.createAnalyzer(analysisCtx_, makeTableCache(stmt_));
      StatementBase stmtBase = (StatementBase) stmt_.getTopLevelNode();
      stmtBase.analyze(analyzer_);
      return stmtBase;
    }

    @Override
    public Analyzer analyzer() {
      Preconditions.checkState(analyzer_ != null, "Not yet analyzed");
      return analyzer_;
    }

    /**
     * Given an analyzed expression and a set of rules, optionally ensure
     * that each of the rules fires, then return the rewritten result.
     */
    public Expr rewrite(Expr origExpr, List<ExprRewriteRule> rules,
        boolean requireFire) throws AnalysisException {
      // Wrap the rules in a (stateful) rule that counts the
      // number of times each wrapped rule fires.
      List<ExprRewriteRule> wrappedRules = new ArrayList<>();
      for (ExprRewriteRule r : rules) {
        wrappedRules.add(new CountingRewriteRuleWrapper(r));
      }
      ExprRewriter rewriter = new ExprRewriter(wrappedRules);
      Expr rewrittenExpr = rewriter.rewrite(origExpr, analyzer());
      if (requireFire) {
        // Asserts that all specified rules fired at least once. This makes sure that
        // the rules being tested are, in fact, being executed. A common mistake is
        // to write an expression that's re-written by the constant folder before
        // getting to the rule that is intended for the test.
        for (ExprRewriteRule r : wrappedRules) {
          CountingRewriteRuleWrapper w = (CountingRewriteRuleWrapper) r;
          assertTrue("Rule " + w.wrapped_.toString() + " didn't fire.",
            w.rewrites_ > 0);
        }
      }
      assertEquals(requireFire, rewriter.changed());
      return rewrittenExpr;
    }

    public Expr verifyExprEquivalence(Expr origExpr, String expectedExprStr,
        List<ExprRewriteRule> rules) throws AnalysisException {
      String origSql = origExpr.toSql();
      boolean expectChange = expectedExprStr != null;
      Expr rewrittenExpr = rewrite(origExpr, rules, expectChange);
      String rewrittenSql = rewrittenExpr.toSql();
      if (expectedExprStr != null) {
        assertEquals(expectedExprStr, rewrittenSql);
      } else {
        assertEquals(origSql, rewrittenSql);
      }
      return rewrittenExpr;
    }

    public Expr verifySelectRewrite(
        List<ExprRewriteRule> rules, String expectedExprStr)
        throws AnalysisException {
      return verifyExprEquivalence(selectExpr(), expectedExprStr, rules);
    }

    public Expr verifyWhereRewrite(
        List<ExprRewriteRule> rules, String expectedExprStr)
        throws AnalysisException {
      return verifyExprEquivalence(whereExpr(), expectedExprStr, rules);
    }
  }

  public static AnalysisSessionFixture session = new AnalysisSessionFixture();

  @BeforeClass
  public static void setup() {
    session.options().setEnable_expr_rewrites(false);
  }

  public Expr RewritesOk(String exprStr, ExprRewriteRule rule, String expectedExprStr)
      throws ImpalaException {
    return RewritesOk("functional.alltypessmall", exprStr, rule, expectedExprStr);
  }

  public Expr RewritesOk(String tableName, String exprStr, ExprRewriteRule rule,
      String expectedExprStr)
      throws ImpalaException {
    return RewritesOk(tableName, exprStr, Lists.newArrayList(rule), expectedExprStr);
  }

  /**
   * Given a list of `rule`, this function checks whether the rewritten <expr> is as
   * expected.
   *
   * If no rule in `rules` is expected to be fired, callers should set expectedExprStr
   * to null or "NULL". Otherwise, this function would throw an exception like
   * "Rule xxx didn't fire"
   *
   * @param exprStr: origin expr
   * @param rules: list of rewrite rules
   * @param expectedExprStr: expected expr
   * @return: Expr: rewritten expr
   */
  public Expr RewritesOk(String exprStr, List<ExprRewriteRule> rules,
      String expectedExprStr)
      throws ImpalaException {
    return RewritesOk("functional.alltypessmall", exprStr, rules, expectedExprStr);
  }

  public Expr RewritesOk(String tableName, String exprStr, List<ExprRewriteRule> rules,
      String expectedExprStr) throws ImpalaException {
    ExprRewriteRulesTest.SelectRewriteFixture qf =
        new ExprRewriteRulesTest.SelectRewriteFixture(session);
    qf.table(tableName);
    qf.exprSql(exprStr);
    qf.analyze();
    return qf.verifySelectRewrite(rules, expectedExprStr);
  }

  public Expr RewritesOkWhereExpr(String exprStr, ExprRewriteRule rule, String expectedExprStr)
      throws ImpalaException {
    return RewritesOkWhereExpr(exprStr, Lists.newArrayList(rule), expectedExprStr);
  }

  public Expr RewritesOkWhereExpr(String exprStr, List<ExprRewriteRule> rules,
      String expectedExprStr) throws ImpalaException {
    ExprRewriteRulesTest.SelectRewriteFixture qf =
        new ExprRewriteRulesTest.SelectRewriteFixture(session);
    qf.table("functional.alltypessmall");
    qf.whereSql(exprStr);
    qf.analyze();
    return qf.verifyWhereRewrite(rules, expectedExprStr);
  }

  public String repeat(String givenStr, long numberOfRepetitions) {
    StringBuilder resultStr = new StringBuilder();
    resultStr.append("'");
    for (long i = 0; i < numberOfRepetitions; i = i + 1) {
      resultStr.append(givenStr);
    }
    resultStr.append("'");
    System.out.println("resultStr.length(): " + resultStr.length());
    return resultStr.toString();
  }

  @Test
  public void testBetweenToCompoundRule() throws ImpalaException {
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
  public void testExtractCommonConjunctsRule() throws ImpalaException {
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
  public void testFoldConstantsRule() throws ImpalaException {
    ExprRewriteRule rule = FoldConstantsRule.INSTANCE;

    RewritesOk("1 + 1", rule, "2");
    RewritesOk("1 + 1 + 1 + 1 + 1", rule, "5");
    RewritesOk("10 - 5 - 2 - 1 - 8", rule, "-6");
    RewritesOk("cast('2016-11-09' as date)", rule, "DATE '2016-11-09'");
    RewritesOk("cast('2016-11-09' as timestamp)", rule,
        "TIMESTAMP '2016-11-09 00:00:00'");
    RewritesOk("cast('2016-11-09' as timestamp) + interval 1 year", rule,
        "TIMESTAMP '2017-11-09 00:00:00'");
    // Tests that exprs that warn during their evaluation are not folded.
    RewritesOk("CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 1 DAYS", rule,
        "TIMESTAMP '9999-12-31 21:00:00' + INTERVAL 1 DAYS");

    // Tests correct handling of strings with escape sequences.
    RewritesOk("'_' LIKE '\\\\_'", rule, "TRUE");
    RewritesOk("base64decode(base64encode('\\047\\001\\132\\060')) = " +
      "'\\047\\001\\132\\060'", rule, "TRUE");

    // Tests correct handling of strings with chars > 127.
    RewritesOk("concat('á', '你好')", rule, "'á你好'");
    RewritesOk("hex(unhex(hex(unhex('D3'))))", rule, "'D3'");
    // Tests that non-deterministic functions are not folded.
    RewritesOk("rand()", rule, null);
    RewritesOk("random()", rule, null);
    RewritesOk("uuid()", rule, null);

    RewritesOk("null + 1", rule, "NULL");
    RewritesOk("(1 + 1) is null", rule, "FALSE");
    RewritesOk("(null + 1) is null", rule, "TRUE");

    // Test if the rewrite would be rejected if the result size is larger than
    // the predefined threshold, i.e., 65_536
    RewritesOk("repeat('AZ', 2)", rule, "'AZAZ'");
    RewritesOk("repeat('A', 65536)", rule, repeat("A", 65_536));
    RewritesOk("repeat('A', 4294967296)", rule, null);

    // Check that constant folding can handle binary results.
    RewritesOk("cast(concat('a', 'b') as binary)", rule, "'ab'");
  }

  @Test
  public void testIf() throws ImpalaException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;

    RewritesOk("if(true, id, id+1)", rule, "id");
    RewritesOk("if(false, id, id+1)", rule, "id + 1");
    RewritesOk("if(null, id, id+1)", rule, "id + 1");
    RewritesOk("if(id = 0, true, false)", rule, null);
  }

  @Test
  public void testIfNull() throws ImpalaException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;

    for (String f : ImmutableList.of("ifnull", "isnull", "nvl")) {
      RewritesOk(f + "(null, id)", rule, "id");
      RewritesOk(f + "(null, null)", rule, "NULL");
      RewritesOk(f + "(id, id + 1)", rule, null);

      RewritesOk(f + "(1, 2)", rule, "1");
      RewritesOk(f + "(0, id)", rule, "0");

      // TODO: IMPALA-7769
      //RewritesOk(f + "(1 + 1, id)", rule, "2");
      //RewritesOk(f + "(NULL + 1, id)", rule, "id");
      //RewritesOk(f + "(cast(null as int), id)", rule, "id");
    }
  }

  @Test
  public void testCompoundPredicate() throws ImpalaException {
    List<ExprRewriteRule> rules = Lists.newArrayList(NormalizeExprsRule.INSTANCE,
        SimplifyConditionalsRule.INSTANCE);

    RewritesOk("id = 0 OR false", rules, "id = 0");
    RewritesOk("id = 0 OR true", rules, "TRUE");
    RewritesOk("id = 0 && false", rules, "FALSE");
    RewritesOk("id = 0 && true", rules, "id = 0");
    RewritesOk("false OR id = 0 AND true", rules, "id = 0");
    RewritesOk("true AND id = 0 OR false", rules, "id = 0");
  }

  /**
   * Sets up a framework for re-analysis that triggers conjunct ID conflicts if rewrite
   * rules don't analyze new predicates. It requires a union of SELECT WHERE clauses,
   * with one of the clauses requiring multiple rewrite passes.
   */
  private String rewriteTemplate(String whereClause) {
    return "SELECT 1 FROM functional.alltypes t WHERE t.id = 1 UNION ALL " +
        "SELECT 1 FROM functional.alltypes t WHERE " + whereClause;
  }

  /**
   * IMPALA-13302: Test that compound predicate rewrites - combined with rewrites that
   * produce a simplifiable predicate - don't leave unanalyzed conjuncts for re-analysis.
   */
  @Test
  public void testCompoundPredicateWithRewriteAndReanalyze() throws ImpalaException {
    AnalysisContext ctx = createAnalysisCtx();
    ctx.getQueryOptions().setEnable_expr_rewrites(true);

    String[] cases = {
      // NormalizeExprsRule should simplify to FALSE AND ...
      rewriteTemplate("t.id = 1 AND t.id = 1 AND false"),
      // ExtractCommonConjunctRule subset should simplify to FALSE AND ...
      rewriteTemplate("((t.id = 1 AND false) or (t.id = 1 AND false)) AND t.id = 1"),
      // ExtractCommonConjunctRule distjunct should simplify to FALSE AND ...
      rewriteTemplate("((t.id = 1 AND false) or (t.id = 2 AND false)) AND t.id = 1"),
      // BetweenToCompoundRule should simplify to FALSE AND ...
      rewriteTemplate("(1 BETWEEN 2 AND 3) AND t.id = 1 AND t.id = 1"),
      // SimplifyDistinctFromRule should simplify to FALSE AND ...
      rewriteTemplate("t.id IS DISTINCT FROM t.id AND t.id = 1")
    };

    for (String sql : cases) {
      AnalyzesOk(sql, ctx);
    }
  }

  @Test
  public void testCaseWithExpr() throws ImpalaException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;
    List<ExprRewriteRule> rules = new ArrayList<>();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);

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
    // IMPALA-12770: Fix infinite loop for nested Case expressions.
    // Case NULL, don't rewrite.
    RewritesOk("case NULL when id then id else 1 end", rule, null);
    // Nested Case expressions.
    RewritesOk("case case 1 when 0 then 0 when 1 then id end "
        + "when 2 then id + 2 else id + 3 end",
        rule, "CASE id WHEN 2 THEN id + 2 ELSE id + 3 END");
    // 'when' are all FALSE for inner Case expression, set 'case' expr as NULL for outer
    // Case expression.
    RewritesOk("case case 3 when 0 then id when 1 then id + 1 end "
        + "when 2 then id + 2 end",
        rule, "CASE NULL WHEN 2 THEN id + 2 END");
  }

  @Test
  public void testCaseWithoutExpr() throws ImpalaException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;
    List<ExprRewriteRule> rules = new ArrayList<>();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);

    // Single TRUE case with no preceding non-constant case.
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
    RewritesOk("case when id = 1 then 10 when false then 20 " +
        "when true then 30 else 40 end", rule,
        "CASE WHEN id = 1 THEN 10 ELSE 30 END");
    // IMPALA-9023: Fix IllegalStateException in SimplifyConditionalsRule
    // Test case function appears in where clause
    // Single TRUE case
    RewritesOkWhereExpr("case when true then id < 50 end", rule, "id < 50");
    // Single TRUE case and the preceding FALSE case should be removed
    RewritesOkWhereExpr("case when false then id > 50 "
        + "when true then id < 50 END", rule, "id < 50");
    // Multiple TRUE cases and it should only take first one
    RewritesOkWhereExpr("case when true then id > 30 when true then id = 30 "
        + "when true then id < 30 "
        + "END", rule, "id > 30");
  }

  @Test
  public void testDecode() throws ImpalaException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;
    List<ExprRewriteRule> rules = new ArrayList<>();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);

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
  }

  /**
   * IMPALA-5125: Exprs containing aggregates should not be rewritten if the rewrite
   * eliminates all aggregates.
   */
  @Test
  public void testExcludeAggregates() throws ImpalaException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;

    RewritesOk("if(true, 0, sum(id))", rule, null);
    RewritesOk("if(false, max(id), min(id))", rule, "min(id)");
    RewritesOk("true || sum(id) = 0", rule, null);
    RewritesOk("ifnull(null, max(id))", rule, "max(id)");
    RewritesOk("ifnull(1, max(id))", rule, null);
    RewritesOk("case when true then 0 when false then sum(id) end", rule, null);
    RewritesOk(
        "case when true then count(id) when false then sum(id) end",
        rule, "count(id)");
    RewritesOk("sum(id) is distinct from null", rule, null);
    RewritesOk("sum(id) is distinct from sum(id)", rule, null);
  }

  @Test
  public void testCoalesce() throws ImpalaException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;
    List<ExprRewriteRule> rules = new ArrayList<>();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);

    // IMPALA-5016: Simplify COALESCE function
    // Test skipping leading nulls.
    RewritesOk("coalesce(null, id, year)", rule, "coalesce(id, `year`)");
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
    RewritesOk("coalesce(10 + null, id, year)", rules, "coalesce(id, `year`)");
    // Don't rewrite based on nullability of slots. TODO (IMPALA-5753).
    RewritesOk("coalesce(year, id)", rule, null);
    RewritesOk("functional_kudu.alltypessmall", "coalesce(id, `year`)", rule, null);
    // IMPALA-7419: coalesce that gets simplified and contains an aggregate
    RewritesOk("coalesce(null, min(distinct tinyint_col), 42)", rule,
        "coalesce(min(tinyint_col), 42)");
  }

  /**
   * Special case in which a numeric literal is explicitly cast to an
   * incompatible type. In this case, no constant folding should be done.
   * Runtime relies on the fact that a DECIMAL numeric overflow in V1
   * resulted in a NULL.
   */
  @Test
  public void TestCoalesceDecimal() throws ImpalaException {
    String query =
        "SELECT coalesce(1.8, CAST(0 AS DECIMAL(38,38))) AS c " +
        " FROM functional.alltypestiny";
    // Try both with and without rewrites
    for (int i = 0; i < 2; i++) {
      boolean rewrite = i == 1;

      // Analyze the expression with the rewrite option and
      // with Decimal V2 disabled.
      AnalysisContext ctx = createAnalysisCtx();
      ctx.getQueryOptions().setEnable_expr_rewrites(rewrite);
      ctx.getQueryOptions().setDecimal_v2(false);
      SelectStmt stmt = (SelectStmt) AnalyzesOk(query, ctx);

      // Select list expr takes widest type
      Expr expr = stmt.getSelectList().getItems().get(0).getExpr();
      assertTrue(expr instanceof FunctionCallExpr);
      assertEquals(ScalarType.createDecimalType(38,38), expr.getType());

      // First arg to coalesce should be an implicit cast
      Expr arg = expr.getChild(0);
      assertTrue(arg instanceof CastExpr);
      assertEquals(ScalarType.createDecimalType(38,38), arg.getType());

      // Input to the cast is the numeric literal with its original type
      Expr num = arg.getChild(0);
      assertTrue(num instanceof NumericLiteral);
      assertEquals(ScalarType.createDecimalType(2,1), num.getType());
    }

    // In V2, the query fails with a cast exception
    try {
      AnalysisContext ctx = createAnalysisCtx();
      ctx.getQueryOptions().setEnable_expr_rewrites(true);
      ctx.getQueryOptions().setDecimal_v2(true);
      parseAndAnalyze(query, ctx);
      fail();
    } catch (SqlCastException e) {
      // Expected
    }
  }

  @Test
  public void testNormalizeExprsRule() throws ImpalaException {
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
  public void testNormalizeBinaryPredicatesRule() throws ImpalaException {
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
  public void testEqualityDisjunctsToInRule() throws ImpalaException {
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
  public void testNormalizeCountStarRule() throws ImpalaException {
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
  public void testSimplifyDistinctFromRule() throws ImpalaException {
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
  public void testCountDistinctToNdvRule() throws ImpalaException {
    List<ExprRewriteRule> rules = Lists.newArrayList(
            org.apache.impala.rewrite.CountDistinctToNdvRule.INSTANCE
    );
    session.options().setAppx_count_distinct(true);
    RewritesOk("count(distinct bool_col)", rules, "ndv(bool_col)");
  }

  @Test
  public void testDefaultNdvScaleRule() throws ImpalaException {
    List<ExprRewriteRule> rules = Lists.newArrayList(
            org.apache.impala.rewrite.DefaultNdvScaleRule.INSTANCE
    );
    session.options().setDefault_ndv_scale(10);
    RewritesOk("ndv(bool_col)", rules, "ndv(bool_col, 10)");
  }

  @Test
  public void testDefaultNdvScaleRuleNotSet() throws ImpalaException {
    List<ExprRewriteRule> rules = Lists.newArrayList(
            org.apache.impala.rewrite.DefaultNdvScaleRule.INSTANCE
    );
    RewritesOk("ndv(bool_col)", rules, null);
  }

  @Test
  public void testDefaultNdvScaleRuleSetDefault() throws ImpalaException {
    List<ExprRewriteRule> rules = Lists.newArrayList(
            org.apache.impala.rewrite.DefaultNdvScaleRule.INSTANCE
    );
    session.options().setDefault_ndv_scale(2);
    RewritesOk("ndv(bool_col)", rules, null);
  }


  @Test
  public void testCountDistinctToNdvAndDefaultNdvScaleRule() throws ImpalaException {
    List<ExprRewriteRule> rules = Lists.newArrayList(
            org.apache.impala.rewrite.CountDistinctToNdvRule.INSTANCE,
            org.apache.impala.rewrite.DefaultNdvScaleRule.INSTANCE
    );
    session.options().setAppx_count_distinct(true);
    session.options().setDefault_ndv_scale(10);
    RewritesOk("count(distinct bool_col)", rules, "ndv(bool_col, 10)");
  }

  @Test
  public void testSimplifyCastStringToTimestamp() throws ImpalaException {
    ExprRewriteRule rule = SimplifyCastStringToTimestamp.INSTANCE;

    // Can be simplified
    RewritesOk("cast(unix_timestamp(date_string_col) as timestamp)", rule,
        "CAST(date_string_col AS TIMESTAMP)");
    RewritesOk("cast(unix_timestamp(date_string_col, 'yyyy-MM-dd') as timestamp)", rule,
        "to_timestamp(date_string_col, 'yyyy-MM-dd')");

    // Verify nothing happens
    RewritesOk("cast(unix_timestamp(timestamp_col) as timestamp)", rule, null);
    RewritesOk("cast(unix_timestamp() as timestamp)", rule, null);
  }

  /**
   * NULLIF gets converted to an IF, and has cases where
   * it can be further simplified via SimplifyDistinctFromRule.
   */
  @Test
  public void testNullif() throws ImpalaException {
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

  @Test
  public void testConvertToCNFRule() throws ImpalaException {
    ExprRewriteRule rule = new ConvertToCNFRule(-1, false);

    RewritesOk("(int_col > 10 AND int_col < 20) OR float_col < 5.0", rule,
            "int_col < 20 OR float_col < 5.0 AND int_col > 10 OR float_col < 5.0");
    RewritesOk("float_col < 5.0 OR (int_col > 10 AND int_col < 20)", rule,
            "float_col < 5.0 OR int_col < 20 AND float_col < 5.0 OR int_col > 10");
    RewritesOk("(int_col > 10 AND float_col < 5.0) OR " +
            "(int_col < 20 AND float_col > 15.0)", rule,
            "float_col < 5.0 OR float_col > 15.0 AND " +
                    "float_col < 5.0 OR int_col < 20 " +
                    "AND int_col > 10 OR float_col > 15.0 AND int_col > 10 " +
                    "OR int_col < 20");
    RewritesOk("NOT(int_col > 10 OR int_col < 20)", rule,
            "NOT int_col < 20 AND NOT int_col > 10");
  }

  @Test
  public void testExtractCompoundVerticalBarExprRule() throws ImpalaException {
    ExprRewriteRule extractCompoundVerticalBarExprRule =
        ExtractCompoundVerticalBarExprRule.INSTANCE;
    ExprRewriteRule simplifyConditionalsRule = SimplifyConditionalsRule.INSTANCE;
    List<ExprRewriteRule> rules = new ArrayList<>();
    rules.add(extractCompoundVerticalBarExprRule);
    rules.add(simplifyConditionalsRule);

    RewritesOk("string_col || string_col", extractCompoundVerticalBarExprRule,
        "concat(string_col, string_col)");

    RewritesOk("bool_col || bool_col", extractCompoundVerticalBarExprRule,
        "bool_col OR bool_col");

    RewritesOk("string_col || 'TEST'", extractCompoundVerticalBarExprRule,
        "concat(string_col, 'TEST')");

    RewritesOk("functional.chars_tiny", "cl || cs", extractCompoundVerticalBarExprRule,
        "concat(cl, cs)");

    RewritesOk("FALSE || id = 0", extractCompoundVerticalBarExprRule, "FALSE OR id = 0");

    RewritesOk("FALSE || id = 0", rules, "id = 0");

    RewritesOk("'' || ''", extractCompoundVerticalBarExprRule, "concat('', '')");

    RewritesOk(
        "NULL || bool_col", extractCompoundVerticalBarExprRule, "NULL OR bool_col");
  }

  @Test
  public void testSimplifyCastExprRule() throws ImpalaException {
    ExprRewriteRule rule = SimplifyCastExprRule.INSTANCE;

    //Inner expr is a column
    RewritesOk("functional.alltypes", "CAST(int_col AS INT)", rule, "int_col");
    RewritesOk("functional.alltypes", "CAST(date_string_col AS STRING)", rule,
        "date_string_col");
    RewritesOk("functional.alltypes", "CAST(timestamp_col AS TIMESTAMP)", rule,
        "timestamp_col");
    //Multi-layer cast
    RewritesOk("functional.alltypes", "CAST(CAST(int_col AS INT) AS INT)", rule,
        "int_col");
    //No rewrite
    RewritesOk("functional.alltypes", "CAST(int_col AS BIGINT)", rule, null);

    //Inner expr is an arithmetic expr
    RewritesOk("functional.alltypes", "CAST(bigint_col+bigint_col AS BIGINT)", rule,
        "bigint_col + bigint_col");
    //Multi-layer cast
    RewritesOk("functional.alltypes", "SUM(CAST(bigint_col+bigint_col AS BIGINT))", rule,
        "sum(bigint_col + bigint_col)");
    //No rewrite
    RewritesOk("functional.alltypes", "CAST(bigint_col+bigint_col AS DOUBLE)", rule,
        null);

    //Inner expr is also a cast expr
    RewritesOk("functional.alltypes", "CAST(CAST(int_col AS BIGINT) AS BIGINT)", rule,
        "CAST(int_col AS BIGINT)");
    //Multi-layer cast
    RewritesOk("functional.alltypes",
        "CAST(CAST(CAST(int_col AS BIGINT) AS BIGINT) AS BIGINT)", rule,
        "CAST(int_col AS BIGINT)");
    //No rewrite
    RewritesOk("functional.alltypes", "CAST(CAST(int_col AS BIGINT) AS INT)", rule, null);

    //Decimal type, d3 type is DECIMAL(20,10)
    RewritesOk("functional.decimal_tbl", "CAST(d3 AS DECIMAL(20,10))", rule, "d3");
    RewritesOk("functional.decimal_tbl",
        "CAST(CAST(d3 AS DECIMAL(10,5)) AS DECIMAL(10,5))", rule,
        "CAST(d3 AS DECIMAL(10,5))");
    //No rewrite
    RewritesOk("functional.decimal_tbl", "CAST(d3 AS DECIMAL(10,5))", rule, null);
    RewritesOk("functional.decimal_tbl", "CAST(d3 AS DECIMAL(25,15))", rule, null);
    RewritesOk("functional.decimal_tbl",
        "CAST(CAST(d3 AS DECIMAL(10,5)) AS DECIMAL(15,5))", rule, null);

    //Varchar type, vc type is VARCHAR(32)
    RewritesOk("functional.chars_formats", "CAST(vc AS VARCHAR(32))", rule, "vc");
    RewritesOk("functional.chars_formats",
        "CAST(CAST(vc AS VARCHAR(16)) AS VARCHAR(16))", rule, "CAST(vc AS VARCHAR(16))");
    //No rewrite
    RewritesOk("functional.chars_formats", "CAST(vc AS VARCHAR(16))", rule, null);
    RewritesOk("functional.chars_formats", "CAST(vc AS VARCHAR(48))", rule, null);
    RewritesOk("functional.chars_formats",
        "CAST(CAST(vc AS VARCHAR(48)) AS VARCHAR(16))", rule, null);

    //Array type
    RewritesOk("functional.allcomplextypes.int_array_col",
        "CAST(int_array_col.item AS INT)", rule, "int_array_col.item");
    RewritesOk("functional.allcomplextypes.int_array_col",
        "CAST(CAST(int_array_col.item AS BIGINT) AS BIGINT)", rule,
        "CAST(int_array_col.item AS BIGINT)");

    //Map type
    RewritesOk("functional.allcomplextypes.int_map_col",
        "CAST(int_map_col.key AS STRING)", rule, "int_map_col.`key`");
    RewritesOk("functional.allcomplextypes.int_map_col",
        "CAST(CAST(int_map_col.value AS STRING) AS STRING)", rule,
        "CAST(int_map_col.value AS STRING)");

    //Struct type
    RewritesOk("functional.allcomplextypes", "CAST(int_struct_col.f1 AS INT)", rule,
        "int_struct_col.f1");
    RewritesOk("functional.allcomplextypes",
        "CAST(CAST(int_struct_col.f1 AS BIGINT) AS BIGINT)", rule,
        "CAST(int_struct_col.f1 AS BIGINT)");
  }
}
