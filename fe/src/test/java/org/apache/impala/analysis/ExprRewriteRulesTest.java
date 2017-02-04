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
import org.apache.impala.rewrite.SimplifyConditionalsRule;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.rewrite.ExtractCommonConjunctRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.rewrite.NormalizeBinaryPredicatesRule;
import org.apache.impala.rewrite.NormalizeExprsRule;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests ExprRewriteRules.
 */
public class ExprRewriteRulesTest extends FrontendTestBase {

  public Expr RewritesOk(String expr, ExprRewriteRule rule, String expectedExpr)
      throws AnalysisException {
    return RewritesOk(expr, Lists.newArrayList(rule), expectedExpr);
  }

  public Expr RewritesOk(String expr, List<ExprRewriteRule> rules, String expectedExpr)
      throws AnalysisException {
    String stmtStr = "select " + expr + " from functional.alltypessmall";
    SelectStmt stmt = (SelectStmt) ParsesOk(stmtStr);
    Analyzer analyzer = createAnalyzer(Catalog.DEFAULT_DB);
    stmt.analyze(analyzer);
    Expr origExpr = stmt.getResultExprs().get(0);
    String origSql = origExpr.toSql();
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

  @Test
  public void TestExtractCommonConjunctsRule() throws AnalysisException {
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
  public void TestFoldConstantsRule() throws AnalysisException {
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
  public void TestSimplifyConditionalsRule() throws AnalysisException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;

    // IF
    RewritesOk("if(true, id, id+1)", rule, "id");
    RewritesOk("if(false, id, id+1)", rule, "id + 1");
    RewritesOk("if(null, id, id+1)", rule, "id + 1");
    RewritesOk("if(id = 0, true, false)", rule, null);

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
    RewritesOk("case 1 when 0 then id when 1 then id + 1 when 2 then id + 2 end", rules,
        "id + 1");
    // SINGLE TRUE case with preceding non-constant case.
    RewritesOk("case 1 when id then id when 1 then id + 1 end", rules,
        "CASE 1 WHEN id THEN id ELSE id + 1 END");
    // Single FALSE case.
    RewritesOk("case 0 when 1 then 1 when id then id + 1 end", rules,
        "CASE 0 WHEN id THEN id + 1 END");
    // All FALSE, return ELSE.
    RewritesOk("case 2 when 0 then id when 1 then id * 2 else 0 end", rules, "0");
    // All FALSE, return implicit NULL ELSE.
    RewritesOk("case 3 when 0 then id when 1 then id + 1 end", rules, "NULL");
    // Multiple TRUE, first one becomes ELSE.
    RewritesOk("case 1 when id then id when 2 - 1 then id + 1 when 1 then id + 2 end",
        rules, "CASE 1 WHEN id THEN id ELSE id + 1 END");
    // When NULL.
    RewritesOk("case 0 when null then 0 else 1 end", rules, "1");
    // All non-constant, don't rewrite.
    RewritesOk("case id when 1 then 1 when 2 then 2 else 3 end", rules, null);

    // CASE without caseExpr
    // Single TRUE case with no predecing non-constant case.
    RewritesOk("case when FALSE then 0 when TRUE then 1 end", rules, "1");
    // Single TRUE case with preceding non-constant case.
    RewritesOk("case when id = 0 then 0 when true then 1 when id = 2 then 2 end", rules,
        "CASE WHEN id = 0 THEN 0 ELSE 1 END");
    // Single FALSE case.
    RewritesOk("case when id = 0 then 0 when false then 1 when id = 2 then 2 end", rules,
        "CASE WHEN id = 0 THEN 0 WHEN id = 2 THEN 2 END");
    // All FALSE, return ELSE.
    RewritesOk(
        "case when false then 1 when false then 2 else id + 1 end", rules, "id + 1");
    // All FALSE, return implicit NULL ELSE.
    RewritesOk("case when false then 0 end", rules, "NULL");
    // Multiple TRUE, first one becomes ELSE.
    RewritesOk("case when id = 1 then 0 when 2 = 1 + 1 then 1 when true then 2 end",
        rules, "CASE WHEN id = 1 THEN 0 ELSE 1 END");
    // When NULL.
    RewritesOk("case when id = 0 then 0 when null then 1 else 2 end", rules,
        "CASE WHEN id = 0 THEN 0 ELSE 2 END");
    // All non-constant, don't rewrite.
    RewritesOk("case when id = 0 then 0 when id = 1 then 1 end", rules, null);

    // DECODE
    // SIngle TRUE case with no preceding non-constant case.
    RewritesOk("decode(1, 0, id, 1, id + 1, 2, id + 2)", rules, "id + 1");
    // Single TRUE case with predecing non-constant case.
    RewritesOk("decode(1, id, id, 1, id + 1, 0)", rules,
        "CASE WHEN 1 = id THEN id ELSE id + 1 END");
    // Single FALSE case.
    RewritesOk("decode(1, 0, id, tinyint_col, id + 1)", rules,
        "CASE WHEN 1 = tinyint_col THEN id + 1 END");
    // All FALSE, return ELSE.
    RewritesOk("decode(1, 0, 0, 2, 2, 3)", rules, "3");
    // All FALSE, return implicit NULL ELSE.
    RewritesOk("decode(1, 1 + 1, 2, 1 + 2, 3)", rules, "NULL");
    // Multiple TRUE, first one becomes ELSE.
    RewritesOk("decode(1, id, id, 1 + 1, 0, 1 * 1, 1, 2 - 1, 2)", rules,
        "CASE WHEN 1 = id THEN id ELSE 1 END");
    // When NULL - DECODE allows the decodeExpr to equal NULL (see CaseExpr.java), so the
    // NULL case is not treated as a constant FALSE and removed.
    RewritesOk("decode(id, null, 0, 1)", rules, null);
    // All non-constant, don't rewrite.
    RewritesOk("decode(id, 1, 1, 2, 2)", rules, null);
  }

  @Test
  public void TestNormalizeExprsRule() throws AnalysisException {
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
  public void TestNormalizeBinaryPredicatesRule() throws AnalysisException {
    ExprRewriteRule rule = NormalizeBinaryPredicatesRule.INSTANCE;

    RewritesOk("0 = id", rule, "id = 0");
    RewritesOk("cast(0 as double) = id", rule, "id = CAST(0 AS DOUBLE)");
    RewritesOk("1 + 1 = cast(id as int)", rule, "CAST(id AS INT) = 1 + 1");

    // Verify that these don't get rewritten.
    RewritesOk("id = 5", rule, null);
    RewritesOk("5 = id + 2", rule, null);
    RewritesOk("cast(id as int) = int_col", rule, null);
    RewritesOk("int_col = cast(id as int)", rule, null);
    RewritesOk("int_col = tinyint_col", rule, null);
    RewritesOk("tinyint_col = int_col", rule, null);
  }
}
