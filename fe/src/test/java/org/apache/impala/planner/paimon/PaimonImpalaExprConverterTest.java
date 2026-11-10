/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.planner.paimon;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.analysis.TimestampLiteral;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.paimon.FePaimonTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.paimon.table.Table;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.EndsWith;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.NotIn;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.StartsWith;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/*
 * Unit test for paimon predicate pushdown.
 */
public class PaimonImpalaExprConverterTest extends FrontendTestBase {
  private FePaimonTable feTable_;
  private final String TBL = "functional_parquet.paimon_primitive_alltypes";
  private final String PARTITIONED_TBL = "functional_parquet.paimon_partitioned";

  @Before
  public void setup() throws DatabaseNotFoundException {
    AnalyzesOk(String.format("select * from %s", TBL));
    AnalyzesOk(String.format("select * from %s", PARTITIONED_TBL));
    String[] db_table = TBL.split("\\.");
    feTable_ =
        (FePaimonTable) frontend_.getCatalog().getTableNoThrow(db_table[0], db_table[1]);
  }

  private SelectStmt analyzeSelect(String sql) throws AnalysisException {
    SelectStmt selectStmt = (SelectStmt) AnalyzesOkWithoutWarnings(sql);
    selectStmt.rewriteExprs(selectStmt.getAnalyzer().getConstantFolder());
    return selectStmt;
  }

  private SelectStmt analyzeSelect(String sql, AnalysisContext ctx)
      throws AnalysisException {
    SelectStmt selectStmt = (SelectStmt) AnalyzesOkWithoutWarnings(sql, ctx);
    selectStmt.rewriteExprs(selectStmt.getAnalyzer().getConstantFolder());
    return selectStmt;
  }

  private ImpalaExprConverter newConverter(Analyzer analyzer) {
    return new ImpalaExprConverter(feTable_.getPaimonSchema(), analyzer);
  }

  private Expr getTestExpr(String sql) throws AnalysisException {
    return analyzeSelect(sql).getWhereClause();
  }

  private ImpalaExprConverter.ConverterResult convertSql(String sql)
      throws AnalysisException {
    SelectStmt selectStmt = analyzeSelect(sql);
    return newConverter(selectStmt.getAnalyzer())
        .convertExpr(selectStmt.getWhereClause());
  }

  private void checkAcceptableExpr(String sql) throws AnalysisException {
    SelectStmt selectStmt = analyzeSelect(sql);
    Predicate predicate = newConverter(selectStmt.getAnalyzer())
                              .convertIgnoreFailure(selectStmt.getWhereClause());
    Assert.assertNotNull(String.format("sql %s fails predicate test", sql), predicate);
  }

  private static enum PREDICATE_OP {
    NONE,
    STARTS_WITH,
    ENDS_WITH,
    EQUALS,
    STARTS_ENDS_WITH
  }

  /*
   * Check valid impala like/regexp expressions that can be pushed down.
   */
  private void checkAcceptableLikePredicate(
      Predicate predicate, PREDICATE_OP op, String... expected_values) {
    switch (op) {
      case STARTS_WITH: {
        LeafPredicate leafPredicate = (LeafPredicate) predicate;
        Assert.assertTrue(leafPredicate.function() instanceof StartsWith);
        Assert.assertEquals(
            leafPredicate.literals().get(0).toString(), expected_values[0]);
      } break;
      case ENDS_WITH: {
        LeafPredicate leafPredicate = (LeafPredicate) predicate;
        Assert.assertTrue(leafPredicate.function() instanceof EndsWith);
        Assert.assertEquals(
            leafPredicate.literals().get(0).toString(), expected_values[0]);
      } break;
      case EQUALS: {
        LeafPredicate leafPredicate = (LeafPredicate) predicate;
        Assert.assertTrue(leafPredicate.function() instanceof Equal);
        Assert.assertEquals(
            leafPredicate.literals().get(0).toString(), expected_values[0]);
      } break;
      case STARTS_ENDS_WITH: {
        CompoundPredicate andPredicate = (CompoundPredicate) predicate;
        Assert.assertTrue(andPredicate.function() instanceof And);
        Assert.assertEquals(2, expected_values.length);
        List<Predicate> child = andPredicate.children();
        checkAcceptableLikePredicate(
            child.get(0), PREDICATE_OP.STARTS_WITH, expected_values[0]);
        checkAcceptableLikePredicate(
            child.get(1), PREDICATE_OP.ENDS_WITH, expected_values[1]);
      } break;
    }
  }
  /*
   * Check valid impala expressions that can be pushed down for strict validation
   * of like/regexp expression.
   */
  private void checkAcceptableLikeExpr(
      String sql, PREDICATE_OP op, String... expected_values) throws AnalysisException {
    SelectStmt selectStmt = analyzeSelect(sql);
    Predicate predicate = newConverter(selectStmt.getAnalyzer())
                              .convertIgnoreFailure(selectStmt.getWhereClause());
    Assert.assertNotNull(String.format("sql %s fails predicate test", sql), predicate);
    checkAcceptableLikePredicate(predicate, op, expected_values);
  }

  /*
   * Check invalid impala expressions that can't be pushed down.
   */
  private void checkUnAcceptableExpr(String sql) throws AnalysisException {
    ImpalaExprConverter.ConverterResult result = convertSql(sql);
    Assert.assertTrue(String.format("sql %s should not push down", sql),
        result.isFailed());
  }

  private void checkEqualStringLiteral(String sql, String expected)
      throws AnalysisException {
    ImpalaExprConverter.ConverterResult result = convertSql(sql);
    Assert.assertFalse(result.isFailed());
    LeafPredicate leaf = (LeafPredicate) result.getPredicate();
    Assert.assertTrue(leaf.function() instanceof Equal);
    Assert.assertEquals(expected, leaf.literals().get(0).toString());
  }
  /*
   * boolean type test cases.
   */
  @Test
  public void testBooleanPushdown() throws AnalysisException {
    checkAcceptableExpr("select * from "
        + "functional_parquet.paimon_primitive_alltypes where bool_value = true");
    checkAcceptableExpr("select * from "
        + "functional_parquet.paimon_primitive_alltypes where bool_value = false");
    checkUnAcceptableExpr("select * from "
        + "functional_parquet.paimon_primitive_alltypes where bool_value = null");
    checkAcceptableExpr("select * from "
        + "functional_parquet.paimon_primitive_alltypes where bool_value is null");
    checkAcceptableExpr("select * from "
        + "functional_parquet.paimon_primitive_alltypes where bool_value is not null");
    checkAcceptableExpr("select * from "
        + "functional_parquet.paimon_primitive_alltypes where bool_value <=> true");
    checkAcceptableExpr("select * from "
        + "functional_parquet.paimon_primitive_alltypes where bool_value <=> null");
    checkAcceptableExpr("select * from "
        + "functional_parquet.paimon_primitive_alltypes where bool_value is distinct "
        + "from true");
    checkAcceptableExpr("select * from "
        + "functional_parquet.paimon_primitive_alltypes where bool_value is not distinct "
        + "from null");
    checkAcceptableExpr("select * from "
        + "functional_parquet.paimon_primitive_alltypes where not (bool_value = true)");
  }
  /*
   * numberic type test cases.
   */
  @Test
  public void testNumericPushdown() throws AnalysisException {
    String[] numericCols = {"tiny_value", "small_value", "int_value", "big_value",
        "decimal_value"};
    for (String col : numericCols) {
      checkAcceptableExpr(String.format("select * from %s where %s = 1", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s <=> 1", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s > 1", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s >= 1", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s < 1", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s <= 1", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where 1 >= %s", TBL, col));
      checkAcceptableExpr(
          String.format("select * from %s where %s in (1, 2, 3)", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s is null", TBL, col));
      checkAcceptableExpr(
          String.format("select * from %s where %s is not null", TBL, col));
      checkAcceptableExpr(
          String.format("select * from %s where %s > 0 and %s < 10", TBL, col, col));
      checkAcceptableExpr(
          String.format("select * from %s where %s < 0 or %s > 10", TBL, col, col));
      checkAcceptableExpr(String.format("select * from %s where not (%s = 1)", TBL, col));
    }
  }
  /*
   * string type test cases.
   */
  @Test
  public void testStringPushdown() throws AnalysisException {
    String col = "char_value";
    checkUnAcceptableExpr(String.format("select * from %s where %s = 'a'", TBL, col));
    checkUnAcceptableExpr(String.format("select * from %s where %s > 'a'", TBL, col));
    checkUnAcceptableExpr(String.format("select * from %s where %s in ('a')", TBL, col));
    checkAcceptableExpr(String.format("select * from %s where %s is null", TBL, col));
    checkAcceptableExpr(
        String.format("select * from %s where %s is not null", TBL, col));

    col = "varchar_value";
    {
      checkAcceptableExpr(String.format("select * from %s where %s = 'a'", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s <=> 'a'", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s > 'a'", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s >= 'a'", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s < 'a'", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s <= 'a'", TBL, col));
      checkAcceptableExpr(
          String.format("select * from %s where %s in ('a', 'b', 'c')", TBL, col));
      checkAcceptableExpr(String.format("select * from %s where %s is null", TBL, col));
      checkAcceptableExpr(
          String.format("select * from %s where %s is not null", TBL, col));
      checkAcceptableExpr(
          String.format("select * from %s where %s > 'a' and %s < 'z'", TBL, col, col));
      checkAcceptableExpr(
          String.format("select * from %s where %s < 'a' or %s > 'z'", TBL, col, col));
      checkAcceptableExpr(
          String.format("select * from %s where not (%s = 'a')", TBL, col));
    }
  }

  @Test
  public void testPredicatePushdownRegressionCases() throws AnalysisException {
    checkUnAcceptableExpr(String.format(
        "select * from %s where userid < 1.5", PARTITIONED_TBL));
    checkUnAcceptableExpr(
        String.format("select * from %s where tiny_value < 200", TBL));
    checkUnAcceptableExpr(String.format(
        "select * from %s where decimal_value < 100000000", TBL));
    checkEqualStringLiteral(String.format(
        "select * from %s where varchar_value = 'O\\'Brien'", TBL), "O'Brien");

    checkUnAcceptableExpr(String.format(
        "select * from %s where varchar_value like '%%ar%%' or int_value = 1", TBL));
    checkUnAcceptableExpr(String.format(
        "select * from %s where bool_value or int_value = 1", TBL));
    checkUnAcceptableExpr(String.format(
        "select * from %s where varchar_value like '%%ar%%' and int_value = 1", TBL));

    ImpalaExprConverter.ConverterResult likeOrInt = convertSql(String.format(
        "select * from %s where varchar_value like 'var%%' or int_value = 1", TBL));
    Assert.assertFalse(likeOrInt.isFailed());
    Assert.assertTrue(likeOrInt.isPartiallyConverted()
        || likeOrInt.isFullyConverted());
  }

  @Test
  public void testPartitionPredicatesOmittedFromPostScan() throws AnalysisException {
    Table paimonTable = feTable_.getPaimonApiTable();
    SelectStmt selectStmt = analyzeSelect(String.format(
        "select * from %s where int_value = 1", TBL));
    Expr where = selectStmt.getWhereClause();
    PredicateExtractor.ExtractedPredicates extracted =
        PredicateExtractor.extractPaimonPredicates(
            paimonTable, java.util.Collections.singletonList(where),
            selectStmt.getAnalyzer());
    Assert.assertEquals(1, extracted.getPushedPredicates().size());
    Assert.assertEquals(1, extracted.getPostScanExprs().size());
    // Row filters are not reflected in split row counts, so they still count.
    Assert.assertEquals(1, extracted.getSelectivityConjuncts().size());

    selectStmt = analyzeSelect(String.format(
        "select * from %s where userid = 1", PARTITIONED_TBL));
    where = selectStmt.getWhereClause();
    FePaimonTable partitioned = (FePaimonTable) frontend_.getCatalog()
        .getTableNoThrow("functional_parquet", "paimon_partitioned");
    extracted = PredicateExtractor.extractPaimonPredicates(
        partitioned.getPaimonApiTable(), java.util.Collections.singletonList(where),
        selectStmt.getAnalyzer());
    Assert.assertEquals(1, extracted.getPushedPredicates().size());
    Assert.assertEquals(1, extracted.getPartitionPredicates().size());
    Assert.assertEquals(0, extracted.getPostScanExprs().size());
    Assert.assertEquals(1, extracted.getReadFilters().getPartitionPredicates().size());
    Assert.assertTrue(extracted.getReadFilters().getDataFilters().isEmpty());
    // userid is not field 0 of the table row. Partition pruning must see index 0.
    org.apache.paimon.predicate.LeafPredicate partitionLeaf =
        (org.apache.paimon.predicate.LeafPredicate)
            extracted.getReadFilters().getPartitionPredicates().get(0);
    Assert.assertEquals(0, partitionLeaf.index());
  }

  /*
   * NOT IN pushdown and IN-list edge cases.
   */
  @Test
  public void testNotInPushdown() throws AnalysisException {
    // NOT IN with up to 20 values pushes as an AND of notEquals (Paimon's
    // PredicateBuilder.notIn expands short lists); more than 20 values push as a
    // single notIn leaf.
    ImpalaExprConverter.ConverterResult result = convertSql(String.format(
        "select * from %s where int_value not in (1, 2)", TBL));
    Assert.assertTrue(result.isFullyConverted());
    CompoundPredicate and = (CompoundPredicate) result.getPredicate();
    Assert.assertTrue(and.function() instanceof And);
    Assert.assertTrue(
        ((LeafPredicate) and.children().get(0)).function() instanceof NotEqual);
    StringBuilder values = new StringBuilder();
    for (int i = 1; i <= 21; ++i) {
      if (i > 1) values.append(", ");
      values.append(i);
    }
    result = convertSql(String.format(
        "select * from %s where int_value not in (%s)", TBL, values));
    Assert.assertTrue(result.isFullyConverted());
    Assert.assertTrue(
        ((LeafPredicate) result.getPredicate()).function() instanceof NotIn);
    result = convertSql(String.format(
        "select * from %s where int_value in (%s)", TBL, values));
    Assert.assertTrue(result.isFullyConverted());
    Assert.assertTrue(
        ((LeafPredicate) result.getPredicate()).function() instanceof In);
    checkAcceptableExpr(String.format(
        "select * from %s where varchar_value not in ('a', 'b')", TBL));
    // IN lists containing NULL have three-valued semantics in Impala that a
    // Paimon in/notIn predicate does not replicate: they must not be pushed.
    checkUnAcceptableExpr(String.format(
        "select * from %s where int_value in (1, null)", TBL));
    checkUnAcceptableExpr(String.format(
        "select * from %s where int_value not in (1, null)", TBL));
    // Non-literal IN members are not pushed.
    checkUnAcceptableExpr(String.format(
        "select * from %s where int_value in (1, big_value)", TBL));
    // Single-element IN is still pushed.
    result = convertSql(String.format(
        "select * from %s where int_value in (1)", TBL));
    Assert.assertFalse(result.isFailed());
  }

  /*
   * IS [NOT] DISTINCT FROM conversions for non-boolean types.
   */
  @Test
  public void testDistinctFromOperators() throws AnalysisException {
    // IS DISTINCT FROM with a non-null literal: IsNull OR NotEqual.
    ImpalaExprConverter.ConverterResult result = convertSql(String.format(
        "select * from %s where int_value is distinct from 1", TBL));
    Assert.assertTrue(result.isFullyConverted());
    CompoundPredicate or = (CompoundPredicate) result.getPredicate();
    Assert.assertTrue(or.function() instanceof Or);
    Assert.assertTrue(
        ((LeafPredicate) or.children().get(0)).function() instanceof IsNull);
    Assert.assertTrue(
        ((LeafPredicate) or.children().get(1)).function() instanceof NotEqual);

    // IS NOT DISTINCT FROM with a non-null literal: IsNotNull AND Equal.
    result = convertSql(String.format(
        "select * from %s where int_value is not distinct from 1", TBL));
    Assert.assertTrue(result.isFullyConverted());
    CompoundPredicate and = (CompoundPredicate) result.getPredicate();
    Assert.assertTrue(and.function() instanceof And);
    Assert.assertTrue(
        ((LeafPredicate) and.children().get(0)).function() instanceof IsNotNull);
    Assert.assertTrue(
        ((LeafPredicate) and.children().get(1)).function() instanceof Equal);

    // IS [NOT] DISTINCT FROM NULL degenerate to IS [NOT] NULL.
    result = convertSql(String.format(
        "select * from %s where int_value is distinct from null", TBL));
    Assert.assertTrue(result.isFullyConverted());
    Assert.assertTrue(
        ((LeafPredicate) result.getPredicate()).function() instanceof IsNotNull);
    result = convertSql(String.format(
        "select * from %s where int_value is not distinct from null", TBL));
    Assert.assertTrue(result.isFullyConverted());
    Assert.assertTrue(
        ((LeafPredicate) result.getPredicate()).function() instanceof IsNull);
    checkAcceptableExpr(String.format(
        "select * from %s where varchar_value is distinct from 'a'", TBL));
    checkAcceptableExpr(String.format(
        "select * from %s where varchar_value is not distinct from 'a'", TBL));
  }

  /*
   * Negation and nesting of compound predicates.
   */
  @Test
  public void testCompoundNegationAndNesting() throws AnalysisException {
    // NOT over OR/AND/IN pushes via Paimon's De Morgan negation.
    ImpalaExprConverter.ConverterResult result = convertSql(String.format(
        "select * from %s where not (int_value = 1 or int_value = 2)", TBL));
    Assert.assertTrue(result.isFullyConverted());
    CompoundPredicate and = (CompoundPredicate) result.getPredicate();
    Assert.assertTrue(and.function() instanceof And);
    Assert.assertTrue(
        ((LeafPredicate) and.children().get(0)).function() instanceof NotEqual);
    checkAcceptableExpr(String.format(
        "select * from %s where not (int_value = 1 and int_value > 0)", TBL));
    result = convertSql(String.format(
        "select * from %s where not (int_value in (1, 2))", TBL));
    Assert.assertTrue(result.isFullyConverted());
    // IN expands to an OR of equals for short lists, so its negation is an AND
    // of notEquals.
    CompoundPredicate negatedIn = (CompoundPredicate) result.getPredicate();
    Assert.assertTrue(negatedIn.function() instanceof And);
    Assert.assertTrue(
        ((LeafPredicate) negatedIn.children().get(0)).function() instanceof NotEqual);
    // A three-way AND converts through nested compounds.
    checkAcceptableExpr(String.format(
        "select * from %s where int_value > 0 and int_value < 100 "
            + "and bool_value = true", TBL));
    // NOT of a tree with an unconvertible child is rejected: the bare boolean
    // column cannot be converted, so the whole NOT stays in Impala.
    checkUnAcceptableExpr(String.format(
        "select * from %s where not (bool_value or int_value = 1)", TBL));
    // NOT of a partially converted LIKE superset must not be pushed: negating
    // the weaker predicate would drop rows the original NOT LIKE keeps.
    checkUnAcceptableExpr(String.format(
        "select * from %s where not (int_value = 1 or varchar_value like 'a%%b')",
        TBL));
  }

  /*
   * Comparisons without a plain literal operand are not pushed.
   */
  @Test
  public void testNonLiteralOperandRejection() throws AnalysisException {
    // Column to column.
    checkUnAcceptableExpr(String.format(
        "select * from %s where int_value = big_value", TBL));
    // Function/arithmetic result to literal.
    checkUnAcceptableExpr(String.format(
        "select * from %s where int_value + 1 = 5", TBL));
    checkUnAcceptableExpr(String.format(
        "select * from %s where abs(int_value) = 1", TBL));
    // Explicit cast on the column side is not pushed.
    checkUnAcceptableExpr(String.format(
        "select * from %s where cast(int_value as bigint) = 1", TBL));
    // A constant-folded BIGINT literal does not match INT storage type.
    checkUnAcceptableExpr(String.format(
        "select * from %s where int_value = cast(1 as bigint)", TBL));
    // A literal out of range of the column type is not pushed.
    checkUnAcceptableExpr(String.format(
        "select * from %s where tiny_value = 200", TBL));
  }

  /*
   * NULL literal handling in comparisons.
   */
  @Test
  public void testNullLiteralHandling() throws AnalysisException {
    checkUnAcceptableExpr(String.format(
        "select * from %s where int_value = null", TBL));
    checkUnAcceptableExpr(String.format(
        "select * from %s where int_value != null", TBL));
    // <=> NULL is IS NULL.
    ImpalaExprConverter.ConverterResult result = convertSql(String.format(
        "select * from %s where int_value <=> null", TBL));
    Assert.assertTrue(result.isFullyConverted());
    Assert.assertTrue(
        ((LeafPredicate) result.getPredicate()).function() instanceof IsNull);
  }

  /*
   * Decimal scale/precision edge cases, including extreme decimal types.
   */
  @Test
  public void testDecimalEdgeCases() throws AnalysisException {
    // An integer literal against a decimal column pushes.
    checkAcceptableExpr(String.format(
        "select * from %s where decimal_value > 100", TBL));
    // A literal scale below the column scale is padded.
    ImpalaExprConverter.ConverterResult result = convertSql(String.format(
        "select * from %s where decimal_value = 123.4", TBL));
    Assert.assertTrue(result.isFullyConverted());
    Assert.assertEquals("123.40",
        ((LeafPredicate) result.getPredicate()).literals().get(0).toString());
    // A literal scale above the column scale cannot be represented exactly and
    // is not pushed; Impala still evaluates the predicate after the scan.
    checkUnAcceptableExpr(String.format(
        "select * from %s where decimal_value = 123.450", TBL));
    // 9 integer digits do not fit DECIMAL(10,2): not pushed.
    checkUnAcceptableExpr(String.format(
        "select * from %s where decimal_value = 123456789.12", TBL));
    // The maximum precision that fits pushes.
    checkAcceptableExpr(String.format(
        "select * from %s where decimal_value = 12345678.12", TBL));
    // Extreme decimals (DECIMAL(38,38) etc.) are covered end-to-end on
    // functional_parquet.paimon_decimal_tbl in QueryTest/paimon-predicate-push-down;
    // that table is not loadable in the FE test catalog.
  }

  /*
   * Routine float and double comparison ops that should push to Paimon.
   */
  @Test
  public void testFloatDoublePushdown() throws AnalysisException {
    String[][] colsAndVals = {
        {"float_value", "cast(1.1 as float)", "cast(2.2 as float)",
            "cast(10.5 as float)"},
        {"double_value", "cast(1.1 as double)", "cast(2.2 as double)",
            "cast(10.5 as double)"}};
    for (String[] spec : colsAndVals) {
      String col = spec[0];
      String val = spec[1];
      String val2 = spec[2];
      String upper = spec[3];
      checkAcceptableExpr(String.format("select * from %s where %s = %s", TBL, col, val));
      checkAcceptableExpr(String.format(
          "select * from %s where %s <=> %s", TBL, col, val));
      checkAcceptableExpr(String.format("select * from %s where %s > %s", TBL, col, val));
      checkAcceptableExpr(String.format(
          "select * from %s where %s >= %s", TBL, col, val));
      checkAcceptableExpr(String.format(
          "select * from %s where %s < %s", TBL, col, upper));
      checkAcceptableExpr(String.format(
          "select * from %s where %s <= %s", TBL, col, upper));
      checkAcceptableExpr(String.format(
          "select * from %s where %s in (%s, %s)", TBL, col, val, val2));
      checkAcceptableExpr(String.format("select * from %s where %s is null", TBL, col));
      checkAcceptableExpr(
          String.format("select * from %s where %s is not null", TBL, col));
      checkAcceptableExpr(String.format(
          "select * from %s where %s > %s and %s < %s", TBL, col, val, col, upper));
      checkAcceptableExpr(String.format(
          "select * from %s where %s < %s or %s > %s", TBL, col, val, col, upper));
    }

    ImpalaExprConverter.ConverterResult gt = convertSql(String.format(
        "select * from %s where double_value > cast(10.1 as double)", TBL));
    Assert.assertTrue(gt.isFullyConverted());
    CompoundPredicate and = (CompoundPredicate) gt.getPredicate();
    Assert.assertTrue(and.function() instanceof And);
    LeafPredicate comparison = (LeafPredicate) and.children().get(0);
    Assert.assertTrue(comparison.function() instanceof GreaterThan);
    Assert.assertEquals(10.1d, ((Number) comparison.literals().get(0)).doubleValue(), 0);
  }

  /*
   * Float/double pushdown matches Impala's -0.0 and NaN comparison rules.
   * Paimon compareTo orders -0.0 before 0.0 and NaN above +Infinity.
   */
  @Test
  public void testFloatDoubleEdgeCases() throws AnalysisException {
    // d = 0 becomes d IN (0, -0.0). Paimon expands a short IN to OR of equals.
    ImpalaExprConverter.ConverterResult eqZero = convertSql(String.format(
        "select * from %s where double_value = cast(0 as double)", TBL));
    Assert.assertTrue(eqZero.isFullyConverted());
    Assert.assertFalse(eqZero.isPartitionOnly());
    assertHasPositiveAndNegativeZero(eqZero.getPredicate());
    Assert.assertFalse(hasPositiveInfinity(eqZero.getPredicate()));

    // A non-zero equality is unchanged.
    ImpalaExprConverter.ConverterResult eqOne = convertSql(String.format(
        "select * from %s where double_value = cast(1 as double)", TBL));
    Assert.assertTrue(eqOne.isFullyConverted());
    LeafPredicate eqOneLeaf = (LeafPredicate) eqOne.getPredicate();
    Assert.assertTrue(eqOneLeaf.function() instanceof Equal);
    Assert.assertEquals(1.0d, ((Number) eqOneLeaf.literals().get(0)).doubleValue(), 0);

    checkAcceptableExpr(String.format(
        "select * from %s where float_value = cast(1.1 as float)", TBL));
    // A DOUBLE literal does not match a FLOAT column and is not pushed.
    checkUnAcceptableExpr(String.format(
        "select * from %s where float_value = cast(1.1 as double)", TBL));

    // d >= 0 becomes d >= -0.0 AND d <= +Infinity.
    assertGreaterBound(String.format(
        "select * from %s where double_value >= cast(0 as double)", TBL), true);
    // d > x and d >= x for a non-zero x keep the literal and exclude NaN.
    assertGreaterBound(String.format(
        "select * from %s where double_value > cast(1 as double)", TBL), false);
    assertGreaterBound(String.format(
        "select * from %s where double_value >= cast(1 as double)", TBL), false);

    // d < 0 becomes d < -0.0. NaN is already above every finite literal.
    ImpalaExprConverter.ConverterResult ltZero = convertSql(String.format(
        "select * from %s where double_value < cast(0 as double)", TBL));
    Assert.assertTrue(ltZero.isFullyConverted());
    LeafPredicate ltZeroLeaf = (LeafPredicate) ltZero.getPredicate();
    Assert.assertTrue(ltZeroLeaf.function() instanceof LessThan);
    Assert.assertTrue(isNegativeZero(ltZeroLeaf.literals().get(0)));

    ImpalaExprConverter.ConverterResult leZero = convertSql(String.format(
        "select * from %s where double_value <= cast(0 as double)", TBL));
    Assert.assertTrue(leZero.isFullyConverted());
    LeafPredicate leZeroLeaf = (LeafPredicate) leZero.getPredicate();
    Assert.assertTrue(leZeroLeaf.function() instanceof LessOrEqual);
    Assert.assertTrue(isPositiveZero(leZeroLeaf.literals().get(0)));

    // IN lists gain -0.0 next to a zero literal.
    ImpalaExprConverter.ConverterResult inZero = convertSql(String.format(
        "select * from %s where double_value in (cast(0 as double), cast(1 as double))",
        TBL));
    Assert.assertTrue(inZero.isFullyConverted());
    Assert.assertFalse(inZero.isPartitionOnly());
    assertHasPositiveAndNegativeZero(inZero.getPredicate());
    Assert.assertTrue(hasLiteral(inZero.getPredicate(), 1.0d));

    // != and NOT IN are not data filters. A non-float partition != still pushes.
    ImpalaExprConverter.ConverterResult ne = convertSql(String.format(
        "select * from %s where double_value != cast(1 as double)", TBL));
    Assert.assertTrue(ne.isFullyConverted());
    Assert.assertTrue(ne.isPartitionOnly());
    ImpalaExprConverter.ConverterResult notIn = convertSql(String.format(
        "select * from %s where double_value not in "
            + "(cast(0 as double), cast(1 as double))",
        TBL));
    Assert.assertTrue(notIn.isFullyConverted());
    Assert.assertTrue(notIn.isPartitionOnly());
    assertHasPositiveAndNegativeZero(notIn.getPredicate());

    checkUnAcceptableExpr(String.format(
        "select * from %s where not (double_value < cast(1 as double))", TBL));
    checkUnAcceptableExpr(String.format(
        "select * from %s where double_value = cast('NaN' as double)", TBL));
    checkUnAcceptableExpr(String.format(
        "select * from %s where double_value = cast('inf' as double)", TBL));

    // Narrowing to FLOAT can produce -0.0. Push +0.0 and -0.0, not a lone -0.0.
    // The frontend does not fold this cast. Narrowing -1e-46 to FLOAT is -0.0,
    // which is pushed as +0.0 and -0.0 rather than a lone negative zero.
    ImpalaExprConverter.ConverterResult narrowed = convertSql(String.format(
        "select * from %s where float_value = cast(-1e-46 as float)", TBL));
    Assert.assertTrue(narrowed.isFullyConverted());
    assertHasPositiveAndNegativeZero(narrowed.getPredicate());

    FePaimonTable partitioned = (FePaimonTable) frontend_.getCatalog()
        .getTableNoThrow("functional_parquet", "paimon_partitioned");
    Assert.assertNotNull(partitioned);
    SelectStmt ratingNe = analyzeSelect(String.format(
        "select * from %s where rating != cast(1 as float)", PARTITIONED_TBL));
    PredicateExtractor.ExtractedPredicates extracted =
        PredicateExtractor.extractPaimonPredicates(partitioned.getPaimonApiTable(),
            java.util.Collections.singletonList(ratingNe.getWhereClause()),
            ratingNe.getAnalyzer());
    Assert.assertTrue(extracted.getPushedPredicates().isEmpty());
    Assert.assertTrue(extracted.getReadFilters().getDataFilters().isEmpty());
    Assert.assertTrue(extracted.getReadFilters().getPartitionPredicates().isEmpty());
    Assert.assertEquals(1, extracted.getPostScanExprs().size());

    SelectStmt useridNe = analyzeSelect(String.format(
        "select * from %s where userid != 1", PARTITIONED_TBL));
    extracted = PredicateExtractor.extractPaimonPredicates(
        partitioned.getPaimonApiTable(),
        java.util.Collections.singletonList(useridNe.getWhereClause()),
        useridNe.getAnalyzer());
    Assert.assertEquals(1, extracted.getPartitionPredicates().size());
    Assert.assertTrue(extracted.getReadFilters().getDataFilters().isEmpty());
    Assert.assertEquals(1, extracted.getReadFilters().getPartitionPredicates().size());
    Assert.assertTrue(extracted.getPostScanExprs().isEmpty());
  }

  private void assertGreaterBound(String sql, boolean zeroBound)
      throws AnalysisException {
    ImpalaExprConverter.ConverterResult result = convertSql(sql);
    Assert.assertTrue(result.isFullyConverted());
    Assert.assertFalse(result.isPartitionOnly());
    CompoundPredicate and = (CompoundPredicate) result.getPredicate();
    Assert.assertTrue(and.function() instanceof And);
    LeafPredicate comparison = (LeafPredicate) and.children().get(0);
    LeafPredicate notNan = (LeafPredicate) and.children().get(1);
    if (zeroBound) {
      Assert.assertTrue(comparison.function() instanceof GreaterOrEqual);
      Assert.assertTrue(isNegativeZero(comparison.literals().get(0)));
    } else {
      Assert.assertTrue(comparison.function() instanceof GreaterThan
          || comparison.function() instanceof GreaterOrEqual);
      Assert.assertEquals(1.0d, ((Number) comparison.literals().get(0)).doubleValue(), 0);
    }
    Assert.assertTrue(notNan.function() instanceof LessOrEqual);
    Assert.assertTrue(isPositiveInfinity(notNan.literals().get(0)));
  }

  private static void assertHasPositiveAndNegativeZero(Predicate predicate) {
    List<Object> literals = new ArrayList<>();
    collectLiterals(predicate, literals);
    boolean positive = false;
    boolean negative = false;
    for (Object literal : literals) {
      if (isPositiveZero(literal)) positive = true;
      if (isNegativeZero(literal)) negative = true;
    }
    Assert.assertTrue(positive);
    Assert.assertTrue(negative);
  }

  private static boolean hasLiteral(Predicate predicate, double expected) {
    List<Object> literals = new ArrayList<>();
    collectLiterals(predicate, literals);
    for (Object literal : literals) {
      if (literal instanceof Number
          && ((Number) literal).doubleValue() == expected
          && !isNegativeZero(literal)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasPositiveInfinity(Predicate predicate) {
    List<Object> literals = new ArrayList<>();
    collectLiterals(predicate, literals);
    for (Object literal : literals) {
      if (isPositiveInfinity(literal)) return true;
    }
    return false;
  }

  private static void collectLiterals(Predicate predicate, List<Object> out) {
    if (predicate instanceof LeafPredicate) {
      out.addAll(((LeafPredicate) predicate).literals());
    } else if (predicate instanceof CompoundPredicate) {
      for (Predicate child : ((CompoundPredicate) predicate).children()) {
        collectLiterals(child, out);
      }
    }
  }

  private static boolean isPositiveZero(Object value) {
    if (value instanceof Double) {
      return Double.doubleToRawLongBits((Double) value)
          == Double.doubleToRawLongBits(0.0d);
    }
    if (value instanceof Float) {
      return Float.floatToRawIntBits((Float) value) == Float.floatToRawIntBits(0.0f);
    }
    return false;
  }

  private static boolean isNegativeZero(Object value) {
    if (value instanceof Double) {
      return Double.doubleToRawLongBits((Double) value)
          == Double.doubleToRawLongBits(-0.0d);
    }
    if (value instanceof Float) {
      return Float.floatToRawIntBits((Float) value) == Float.floatToRawIntBits(-0.0f);
    }
    return false;
  }

  private static boolean isPositiveInfinity(Object value) {
    if (value instanceof Double) {
      return ((Double) value).isInfinite() && (Double) value > 0;
    }
    if (value instanceof Float) {
      return ((Float) value).isInfinite() && (Float) value > 0;
    }
    return false;
  }

  private TimestampLiteral timestampLiteralFromWhere(String sql)
      throws AnalysisException {
    SelectStmt selectStmt = analyzeSelect(sql);
    return (TimestampLiteral) ((BinaryPredicate) selectStmt.getWhereClause()).getChild(1);
  }

  @Test
  public void testHasSubMicroFraction() throws AnalysisException {
    String ts = "cast('%s' as timestamp)";
    Assert.assertFalse(ImpalaExprConverter.hasSubMicroFraction(timestampLiteralFromWhere(
        String.format("select * from %s where ts_value = " + ts, TBL,
            "2009-01-02 00:11:00"))));
    Assert.assertFalse(ImpalaExprConverter.hasSubMicroFraction(timestampLiteralFromWhere(
        String.format("select * from %s where ts_value = " + ts, TBL,
            "2009-01-02 00:11:00.45"))));
    Assert.assertFalse(ImpalaExprConverter.hasSubMicroFraction(timestampLiteralFromWhere(
        String.format("select * from %s where ts_value = " + ts, TBL,
            "2009-01-02 00:13:00.123456"))));
    Assert.assertFalse(ImpalaExprConverter.hasSubMicroFraction(timestampLiteralFromWhere(
        String.format("select * from %s where ts_value = " + ts, TBL,
            "2009-01-02 00:13:00.480000"))));

    Assert.assertTrue(ImpalaExprConverter.hasSubMicroFraction(timestampLiteralFromWhere(
        String.format("select * from %s where ts_value = " + ts, TBL,
            "2009-01-02 00:11:00.450000123"))));
    Assert.assertTrue(ImpalaExprConverter.hasSubMicroFraction(timestampLiteralFromWhere(
        String.format("select * from %s where ts_value < " + ts, TBL,
            "2024-01-01 00:00:00.0000005"))));
    Assert.assertTrue(ImpalaExprConverter.hasSubMicroFraction(timestampLiteralFromWhere(
        String.format("select * from %s where ts_value = " + ts, TBL,
            "2009-01-02 00:13:00.480000001"))));
  }

  /*
   * Timestamp literal precision handling. Only literals with at most six fractional
   * digits are pushed; sub-micro literals stay in Impala.
   */
  @Test
  public void testTimestampLiteralPrecision()
      throws AnalysisException, ImpalaRuntimeException {
    checkAcceptableExpr(String.format(
        "select * from %s where ts_value = cast('2009-01-02 00:11:00.45' as timestamp)",
        TBL));
    checkAcceptableExpr(String.format(
        "select * from %s where ts_value = cast('2009-01-02 00:13:00.480000' "
            + "as timestamp)",
        TBL));
    checkAcceptableExpr(String.format(
        "select * from %s where ts_value = cast('2009-01-02 00:13:00.123456' "
            + "as timestamp)",
        TBL));
    checkUnAcceptableExpr(String.format(
        "select * from %s where ts_value = cast('2009-01-02 00:11:00.450000123' "
            + "as timestamp)", TBL));
    checkUnAcceptableExpr(String.format(
        "select * from %s where ts_value < cast('2024-01-01 00:00:00.0000005' "
            + "as timestamp)", TBL));

    SelectStmt selectStmt = analyzeSelect(String.format(
        "select * from %s where ts_value = cast('2009-01-02 00:13:00.480000' "
            + "as timestamp)",
        TBL));
    ImpalaExprConverter converter = newConverter(selectStmt.getAnalyzer());
    Object literalValue = converter.convertLiteral(
        "ts_value", (LiteralExpr) ((BinaryPredicate) selectStmt.getWhereClause())
            .getChild(1));
    Assert.assertEquals(
        org.apache.paimon.data.Timestamp.fromLocalDateTime(
            LocalDateTime.of(2009, 1, 2, 0, 13, 0, 480000000)),
        literalValue);
  }

  @Test
  public void testJulianCalendarDateLiteral()
      throws AnalysisException, ImpalaRuntimeException {
    SelectStmt selectStmt = analyzeSelect(String.format(
        "select * from %s where date_value = cast('1582-10-10' as date)", TBL));
    ImpalaExprConverter converter = newConverter(selectStmt.getAnalyzer());
    Object literalValue = converter.convertLiteral(
        "date_value", (org.apache.impala.analysis.LiteralExpr)
            ((org.apache.impala.analysis.BinaryPredicate) selectStmt.getWhereClause())
                .getChild(1));
    // Paimon stores DATE as days since the Unix epoch. Decoding that int must
    // recover the civil date, including days inside the Julian cutover.
    Assert.assertEquals(LocalDate.of(1582, 10, 10),
        LocalDate.ofEpochDay(((Number) literalValue).longValue()));
  }

  @Test
  public void testLocalZonedTimestampNoPushWhenAmbiguous()
      throws AnalysisException {
    String[] timestamps = {
        "2024-10-27 02:30:00", // ambiguous DST fall-back
        "2024-03-31 02:30:00", // invalid spring-forward gap
    };
    for (String timestamp : timestamps) {
      AnalysisContext ctx = createAnalysisCtx();
      ctx.getQueryCtx().setLocal_time_zone("Europe/Budapest");
      SelectStmt selectStmt = analyzeSelect(String.format(
          "select * from %s where ts_ltz_value = cast('%s' as timestamp)",
          TBL, timestamp), ctx);
      ImpalaExprConverter.ConverterResult result =
          newConverter(selectStmt.getAnalyzer())
              .convertExpr(selectStmt.getWhereClause());
      Assert.assertTrue(String.format("Expected no push for %s", timestamp),
          result.isFailed());
    }
  }

  @Test
  public void testLocalZonedTimestampPushWhenUnambiguous()
      throws AnalysisException {
    AnalysisContext ctx = createAnalysisCtx();
    ctx.getQueryCtx().setLocal_time_zone("Europe/Budapest");
    String sql = String.format(
        "select * from %s where ts_ltz_value = cast('2024-06-15 12:00:00' as timestamp)",
        TBL);
    SelectStmt selectStmt = analyzeSelect(sql, ctx);
    Assert.assertNotNull(newConverter(selectStmt.getAnalyzer())
        .convertIgnoreFailure(selectStmt.getWhereClause()));
  }

  @Test
  public void testTimestampConversionRespectsUtcNotJvmZone()
      throws AnalysisException, ImpalaRuntimeException {
    TimeZone previous = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Europe/Budapest"));
      SelectStmt selectStmt = analyzeSelect(String.format(
          "select * from %s where ts_value = cast('2024-01-01 00:00:00' as timestamp)",
          TBL));
      ImpalaExprConverter converter = newConverter(selectStmt.getAnalyzer());
      Object literalValue = converter.convertLiteral(
          "ts_value", (org.apache.impala.analysis.LiteralExpr)
              ((org.apache.impala.analysis.BinaryPredicate) selectStmt.getWhereClause())
                  .getChild(1));
      Assert.assertEquals(
          org.apache.paimon.data.Timestamp.fromLocalDateTime(
              LocalDateTime.of(2024, 1, 1, 0, 0, 0)),
          literalValue);
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  /*
   * Selectivity conjuncts: exact partition predicates are omitted (split row
   * counts already reflect them); data filters and unconvertible conjuncts keep
   * contributing selectivity.
   */
  @Test
  public void testSelectivityConjuncts() throws AnalysisException {
    FePaimonTable partitioned = (FePaimonTable) frontend_.getCatalog()
        .getTableNoThrow("functional_parquet", "paimon_partitioned");
    Assert.assertNotNull(partitioned);

    // The partition predicate is exact: only the data filter keeps selectivity.
    SelectStmt selectStmt = analyzeSelect(String.format(
        "select * from %s where userid = 1 and movieid = 3", PARTITIONED_TBL));
    PredicateExtractor.ExtractedPredicates extracted =
        PredicateExtractor.extractPaimonPredicates(partitioned.getPaimonApiTable(),
            java.util.Collections.singletonList(selectStmt.getWhereClause()),
            selectStmt.getAnalyzer());
    Assert.assertEquals(1, extracted.getPartitionPredicates().size());
    Assert.assertEquals(1, extracted.getSelectivityConjuncts().size());

    // Partition-only WHERE: nothing needs selectivity.
    selectStmt = analyzeSelect(String.format(
        "select * from %s where userid = 1", PARTITIONED_TBL));
    extracted = PredicateExtractor.extractPaimonPredicates(
        partitioned.getPaimonApiTable(),
        java.util.Collections.singletonList(selectStmt.getWhereClause()),
        selectStmt.getAnalyzer());
    Assert.assertTrue(extracted.getSelectivityConjuncts().isEmpty());

    // Data-only predicate keeps its selectivity.
    selectStmt = analyzeSelect(String.format(
        "select * from %s where movieid = 3", PARTITIONED_TBL));
    extracted = PredicateExtractor.extractPaimonPredicates(
        partitioned.getPaimonApiTable(),
        java.util.Collections.singletonList(selectStmt.getWhereClause()),
        selectStmt.getAnalyzer());
    Assert.assertEquals(1, extracted.getSelectivityConjuncts().size());

    // A compound mixing partition and data columns is re-evaluated post-scan.
    selectStmt = analyzeSelect(String.format(
        "select * from %s where (userid = 1 and movieid = 2) or userid = 3",
        PARTITIONED_TBL));
    extracted = PredicateExtractor.extractPaimonPredicates(
        partitioned.getPaimonApiTable(),
        java.util.Collections.singletonList(selectStmt.getWhereClause()),
        selectStmt.getAnalyzer());
    Assert.assertEquals(0, extracted.getPartitionPredicates().size());
    Assert.assertEquals(1, extracted.getPostScanExprs().size());
    Assert.assertEquals(1, extracted.getSelectivityConjuncts().size());
  }

  /*
   * The read filters split pushed predicates into exact partition predicates and
   * re-checked data filters: partition-only conjuncts prune partitions, data-only
   * and mixed conjuncts become data filters.
   */
  @Test
  public void testReadFilterSplit() throws AnalysisException {
    FePaimonTable partitioned = (FePaimonTable) frontend_.getCatalog()
        .getTableNoThrow("functional_parquet", "paimon_partitioned");
    Assert.assertNotNull(partitioned);
    SelectStmt selectStmt = analyzeSelect(String.format(
        "select * from %s where userid = 1 and movieid = 3 "
            + "and (userid = 2 or movieid = 4)", PARTITIONED_TBL));
    PredicateExtractor.ExtractedPredicates extracted =
        PredicateExtractor.extractPaimonPredicates(partitioned.getPaimonApiTable(),
            java.util.Collections.singletonList(selectStmt.getWhereClause()),
            selectStmt.getAnalyzer());
    Assert.assertEquals(3, extracted.getPushedPredicates().size());
    PaimonJniReadFilter readFilters = extracted.getReadFilters();
    Assert.assertEquals(1, readFilters.getPartitionPredicates().size());
    Assert.assertEquals(2, readFilters.getDataFilters().size());
  }
  /*
   * like/rlike/regexp test cases.
   */
  @Test
  public void testLikePushdown() throws AnalysisException {
    String col = "varchar_value";
    //  StartsWith poshdown test.
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'a%%'", TBL, col),
        PREDICATE_OP.STARTS_WITH, "a");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s rlike '^a.*'", TBL, col),
        PREDICATE_OP.STARTS_WITH, "a");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s regexp '^a.*'", TBL, col),
        PREDICATE_OP.STARTS_WITH, "a");
    checkUnAcceptableExpr(
        String.format("select * from %s where %s rlike '^a*b'", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s regexp '^a*b'", TBL, col));
    // EndsWith poshdown test.
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like '%%a'", TBL, col),
        PREDICATE_OP.ENDS_WITH, "a");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s rlike '.*a$'", TBL, col),
        PREDICATE_OP.ENDS_WITH, "a");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s regexp '.*a$'", TBL, col),
        PREDICATE_OP.ENDS_WITH, "a");
    // Exact match patterns.
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'exact'", TBL, col),
        PREDICATE_OP.EQUALS, "exact");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s rlike '^exact$'", TBL, col),
        PREDICATE_OP.EQUALS, "exact");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s regexp '^exact$'", TBL, col),
        PREDICATE_OP.EQUALS, "exact");
    // StartsWith&Endswith
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'aaa%%bbb'", TBL, col),
        PREDICATE_OP.STARTS_ENDS_WITH, "aaa", "bbb");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s rlike '^aaa.*bbb$'", TBL, col),
        PREDICATE_OP.STARTS_ENDS_WITH, "aaa", "bbb");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s regexp '^aaa.*bbb$'", TBL, col),
        PREDICATE_OP.STARTS_ENDS_WITH, "aaa", "bbb");
    // Wildcards in the middle are ignored, only the prefix before the first wildcard
    // and the suffix after the last wildcard are pushed down.
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'aaa%%bbb%%ccc'", TBL, col),
        PREDICATE_OP.STARTS_ENDS_WITH, "aaa", "ccc");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'aaa_bbb_ccc'", TBL, col),
        PREDICATE_OP.STARTS_ENDS_WITH, "aaa", "ccc");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'aaa_b%%c_d%%e_f'", TBL, col),
        PREDICATE_OP.STARTS_ENDS_WITH, "aaa", "f");

    // Empty LIKE pattern is an exact match of the empty string.
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like ''", TBL, col),
        PREDICATE_OP.EQUALS, "");
    checkUnAcceptableExpr(
        String.format("select * from %s where %s rlike ''", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s regexp ''", TBL, col));
    // Only wildcard
    checkUnAcceptableExpr(
        String.format("select * from %s where %s like '%%'", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s rlike '.*'", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s regexp '.*'", TBL, col));
    // Multiple consecutive wildcards
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'a%%%%b'", TBL, col),
        PREDICATE_OP.STARTS_ENDS_WITH, "a", "b");
    checkUnAcceptableExpr(
        String.format("select * from %s where %s rlike '^a.*.*b$'", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s regexp 'a.*.*b'", TBL, col));
    // Underscore patterns (single character wildcard)
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'a_b'", TBL, col),
        PREDICATE_OP.STARTS_ENDS_WITH, "a", "b");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'a_%%'", TBL, col),
        PREDICATE_OP.STARTS_WITH, "a");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like '%%_a'", TBL, col),
        PREDICATE_OP.ENDS_WITH, "a");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'a_%%b'", TBL, col),
        PREDICATE_OP.STARTS_ENDS_WITH, "a", "b");
    checkUnAcceptableExpr(
        String.format("select * from %s where %s like '_'", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s like '%%_%%'", TBL, col));
    // Escaped patterns. The SQL literal 'a\\%b' is unescaped to the LIKE pattern
    // 'a\%b', which matches the string 'a%b'.
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'a\\\\%%b'", TBL, col),
        PREDICATE_OP.EQUALS, "a%b");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'a\\\\_b'", TBL, col),
        PREDICATE_OP.EQUALS, "a_b");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'a\\\\%%b%%'", TBL, col),
        PREDICATE_OP.STARTS_WITH, "a%b");
    // The SQL literal 'a\\\\%b' is unescaped to the LIKE pattern 'a\\%b', which is an
    // escaped backslash followed by a wildcard.
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'a\\\\\\\\%%b'", TBL, col),
        PREDICATE_OP.STARTS_ENDS_WITH, "a\\", "b");
    // Patterns with special characters and spaces
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'test string%%'", TBL, col),
        PREDICATE_OP.STARTS_WITH, "test string");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like '%%test string'", TBL, col),
        PREDICATE_OP.ENDS_WITH, "test string");
    checkAcceptableLikeExpr(
        String.format("select * from %s where %s like 'email%%@domain.com'", TBL, col),
        PREDICATE_OP.STARTS_ENDS_WITH, "email", "@domain.com");

    // CHAR values are space-padded by Impala but may be stored unpadded by paimon,
    // so like/rlike/regexp predicates on CHAR columns are not pushed down.
    col = "char_value";
    checkUnAcceptableExpr(
        String.format("select * from %s where %s like 'a%%'", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s like '%%a'", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s like 'exact'", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s rlike '^a.*'", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s regexp '^aaa.*bbb$'", TBL, col));

    col = "varchar_value";
    // A trailing '%' is equivalent to startsWith, so the conversion is exact.
    ImpalaExprConverter.ConverterResult prefix = convertSql(
        String.format("select * from %s where %s like 'a%%'", TBL, col));
    Assert.assertTrue(prefix.isFullyConverted());
    // Prefix and suffix together are a superset of LIKE, so the conversion is partial.
    ImpalaExprConverter.ConverterResult bothEnds = convertSql(
        String.format("select * from %s where %s like 'a%%b'", TBL, col));
    Assert.assertTrue(bothEnds.isPartiallyConverted());
    ImpalaExprConverter.ConverterResult underscore = convertSql(
        String.format("select * from %s where %s like 'a_b'", TBL, col));
    Assert.assertTrue(underscore.isPartiallyConverted());
    ImpalaExprConverter.ConverterResult regexpBoth = convertSql(
        String.format("select * from %s where %s rlike '^ab.*bc$'", TBL, col));
    Assert.assertTrue(regexpBoth.isPartiallyConverted());

    // NOT of a partial pattern must not be pushed. Negating the weaker predicate drops
    // rows that the original NOT LIKE would keep.
    checkUnAcceptableExpr(
        String.format("select * from %s where not (%s like 'a_b')", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where not (%s rlike '^ab.*bc$')", TBL, col));
    // startsWith cannot be negated by Paimon, so NOT LIKE 'a%' stays in Impala.
    checkUnAcceptableExpr(
        String.format("select * from %s where not (%s like 'a%%')", TBL, col));
    // LIKE without a wildcard is equality, and equality can be negated.
    ImpalaExprConverter.ConverterResult notExact = convertSql(
        String.format("select * from %s where not (%s like 'exact')", TBL, col));
    Assert.assertTrue(notExact.isFullyConverted());
    Assert.assertTrue(((LeafPredicate) notExact.getPredicate()).function()
        instanceof NotEqual);
  }
  /*
   * binary type test cases.
   */
  @Test
  public void testBinaryPushdown() throws AnalysisException {
    // binary type doesn't allow predicate pushdown.
    String col = "binary_value";
    checkUnAcceptableExpr(
        String.format("select * from %s where %s = cast('a' as binary)", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s <=> cast('a' as binary)", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s > cast('a' as binary)", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s >= cast('a' as binary)", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s < cast('a' as binary)", TBL, col));
    checkUnAcceptableExpr(
        String.format("select * from %s where %s <= cast('a' as binary)", TBL, col));
    checkUnAcceptableExpr(String.format(
        "select * from %s where %s in (cast('a' as binary), cast('b' as binary))", TBL,
        col));
    checkAcceptableExpr(String.format("select * from %s where %s is null", TBL, col));
    checkAcceptableExpr(String.format("select * from %s where %s is not null", TBL, col));
    checkAcceptableExpr(
        String.format("select * from %s where not (%s is null)", TBL, col));

    checkUnAcceptableExpr(
        String.format("select * from %s where not (%s = cast('a' as binary))", TBL, col));
  }
  /*
   * date type test cases.
   */
  @Test
  public void testDatePushdown() throws AnalysisException {
    String col = "date_value";
    String val = "cast('2024-01-01' as date)";
    checkAcceptableExpr(String.format("select * from %s where %s = %s", TBL, col, val));
    checkAcceptableExpr(
        String.format("select * from %s where %s <=> %s", TBL, col, val));
    checkAcceptableExpr(String.format("select * from %s where %s > %s", TBL, col, val));
    checkAcceptableExpr(String.format("select * from %s where %s >= %s", TBL, col, val));
    checkAcceptableExpr(String.format("select * from %s where %s < %s", TBL, col, val));
    checkAcceptableExpr(String.format("select * from %s where %s <= %s", TBL, col, val));
    checkAcceptableExpr(String.format(
        "select * from %s where %s in (%s, cast('2024-01-02' as date))", TBL, col, val));
    checkAcceptableExpr(String.format("select * from %s where %s is null", TBL, col));
    checkAcceptableExpr(String.format("select * from %s where %s is not null", TBL, col));
    checkAcceptableExpr(String.format(
        "select * from %s where %s > %s and %s < cast('2024-01-31' as date)", TBL, col,
        val, col));
    checkAcceptableExpr(
        String.format("select * from %s where %s < %s or %s > cast('2024-01-31' as date)",
            TBL, col, val, col));
    checkAcceptableExpr(
        String.format("select * from %s where not (%s = %s)", TBL, col, val));
  }
  /*
   * timestamp type test cases.
   */
  @Test
  public void testTimestampPushdown() throws AnalysisException {
    String[] tsCols = {"ts_value", "ts_ltz_value"};
    String val = "cast('2024-01-01 12:00:00' as timestamp)";
    for (String col : tsCols) {
      checkAcceptableExpr(String.format("select * from %s where %s = %s", TBL, col, val));
      checkAcceptableExpr(
          String.format("select * from %s where %s <=> %s", TBL, col, val));
      checkAcceptableExpr(String.format("select * from %s where %s > %s", TBL, col, val));
      checkAcceptableExpr(
          String.format("select * from %s where %s >= %s", TBL, col, val));
      checkAcceptableExpr(String.format("select * from %s where %s < %s", TBL, col, val));
      checkAcceptableExpr(
          String.format("select * from %s where %s <= %s", TBL, col, val));
      checkAcceptableExpr(String.format(
          "select * from %s where %s in (%s, cast('2024-01-01 13:00:00' as timestamp))",
          TBL, col, val));
      checkAcceptableExpr(String.format("select * from %s where %s is null", TBL, col));
      checkAcceptableExpr(
          String.format("select * from %s where %s is not null", TBL, col));
      checkAcceptableExpr(String.format(
          "select * from %s where %s > %s and %s < cast('2024-01-01 13:00:00' "
              + "as timestamp)",
          TBL, col, val, col));
      checkAcceptableExpr(String.format(
          "select * from %s where %s < %s or %s > cast('2024-01-01 13:00:00' as "
              + "timestamp)",
          TBL, col, val, col));
      checkAcceptableExpr(
          String.format("select * from %s where not (%s = %s)", TBL, col, val));
    }
  }
  @Test
  public void testTimestampConvertLiteralMicroPrecision()
      throws AnalysisException, ImpalaRuntimeException {
    String[][] cases = {
        {"2024-01-01 12:00:00", "0, 0, 0, 0"},
        {"2024-01-01 12:00:00.111", "0, 0, 0, 111000000"},
        {"2024-01-01 12:00:00.222222", "0, 0, 0, 222222000"},
        {"2024-01-01 12:00:00.3333", "0, 0, 0, 333300000"}};
    for (String[] testCase : cases) {
      String tsStr = testCase[0];
      String[] parts = testCase[1].split(", ");
      SelectStmt selectStmt = analyzeSelect(String.format(
          "select * from %s where ts_value = cast('%s' as timestamp)", TBL, tsStr));
      ImpalaExprConverter converter = newConverter(selectStmt.getAnalyzer());
      Object literalValue = converter.convertLiteral(
          "ts_value", (LiteralExpr) ((BinaryPredicate) selectStmt.getWhereClause())
              .getChild(1));
      LocalDateTime expected = LocalDateTime.of(
          2024, 1, 1, 12, 0, 0, Integer.parseInt(parts[3]));
      Assert.assertEquals(
          org.apache.paimon.data.Timestamp.fromLocalDateTime(expected), literalValue);
    }
  }
}
