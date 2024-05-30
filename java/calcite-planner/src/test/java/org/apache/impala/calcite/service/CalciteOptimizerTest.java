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

package org.apache.impala.calcite.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.NoOpEventSequence;
import org.junit.Before;
import org.junit.Test;

public class CalciteOptimizerTest extends FrontendTestBase {
  private static final String TEST_DB = "calcite_optimizer_test";
  private static final String TEST_TABLE = TEST_DB + ".alltypes";
  private static final String TEST_COMPLEX_TABLE = TEST_DB + ".complex_types";

  private static final List<SupportedQuery> SUPPORTED_QUERIES = ImmutableList.of(
      new SupportedQuery("values",
          "select 1 as int_literal, cast(null as varchar(5)) as maybe_text",
          "int_literal", "maybe_text"),
      new SupportedQuery("filter",
          "select int_col as filtered_int from " + TEST_TABLE + " "
              + "where bigint_col > 0",
          "filtered_int"),
      new SupportedQuery("aggregate",
          "select int_col as group_key, count(*) as row_count "
              + "from " + TEST_TABLE + " group by int_col",
          "group_key", "row_count"),
      new SupportedQuery("sort",
          "select int_col as sorted_int from " + TEST_TABLE + " "
              + "order by int_col limit 5",
          "sorted_int"),
      new SupportedQuery("join",
          "select l.id as left_id, r.int_col as right_int "
              + "from " + TEST_TABLE + " l join " + TEST_TABLE + " r on l.id = r.id",
          "left_id", "right_int"),
      new SupportedQuery("union",
          "select int_col as unioned_int from " + TEST_TABLE + " "
              + "union all select int_col from " + TEST_TABLE,
          "unioned_int"),
      new SupportedQuery("analytic",
          "select id, row_number() over (order by int_col) as row_num "
              + "from " + TEST_TABLE,
          "id", "row_num"));

  @Before
  public void addSyntheticTestTable() {
    addTestDb(TEST_DB, "Synthetic Calcite optimizer test database");
    addTestTable("create table " + TEST_TABLE
        + " (id int, int_col int, bigint_col bigint) stored as parquet");
    addTestTable("create table " + TEST_COMPLEX_TABLE
        + " (id int, int_array_col array<int>) stored as parquet");
  }

  @Test
  public void testPreservesClientOutputSchemaAtLogicalSeam() throws ImpalaException {
    String sql =
        "select 1 as Required_Id, cast(12.34 as decimal(9, 2)) as Amount, "
        + "cast(null as varchar(17)) as Maybe_Text";
    PreImpalaConvertResult result = createPreImpalaConvertPlan(sql);

    assertEquals(3, result.outputLabels().size());
    assertEquals("required_id", result.outputLabels().get(0));
    assertEquals("amount", result.outputLabels().get(1));
    assertEquals("maybe_text", result.outputLabels().get(2));

    RelDataType literalType = result.outputTypes().get(0);
    assertEquals(SqlTypeName.TINYINT, literalType.getSqlTypeName());
    assertFalse(literalType.isNullable());

    RelDataType decimalType = result.outputTypes().get(1);
    assertEquals(SqlTypeName.DECIMAL, decimalType.getSqlTypeName());
    assertEquals(9, decimalType.getPrecision());
    assertEquals(2, decimalType.getScale());
    assertTrue(decimalType.isNullable());

    RelDataType varcharType = result.outputTypes().get(2);
    assertEquals(SqlTypeName.VARCHAR, varcharType.getSqlTypeName());
    assertEquals(17, varcharType.getPrecision());
    assertTrue(varcharType.isNullable());

    OptimizationResult optimizationResult = optimizeWithCalciteOnly(sql);
    assertNotNull(optimizationResult.plan());
    assertEquals(result.outputLabels(), optimizationResult.outputLabels());
  }

  @Test
  public void testPercentRankIsRewrittenBeforeLogicalSeam() throws ImpalaException {
    String sql =
        "select percent_rank() over (order by int_col) as pr "
        + "from " + TEST_TABLE;
    PreImpalaConvertResult result = createPreImpalaConvertPlan(sql);

    assertEquals(1, result.outputLabels().size());
    assertEquals("pr", result.outputLabels().get(0));
    assertEquals(SqlTypeName.DOUBLE,
        result.outputTypes().get(0).getSqlTypeName());

    Set<String> overFunctions = collectOverFunctions(result.plan());
    assertFalse(overFunctions.contains("percent_rank"));
    assertTrue(overFunctions.contains("rank"));
    assertTrue(overFunctions.contains("count"));
    assertEquals(ImmutableList.of(LogicalProject.class, LogicalTableScan.class),
        collectRelNodeHierarchy(result.plan()));

    OptimizationResult optimizationResult = optimizeWithCalciteOnly(sql);
    assertNotNull(optimizationResult.plan());
    assertEquals(result.outputLabels(), optimizationResult.outputLabels());
  }

  @Test
  public void testSupportedQueryCorpusReachesImpalaPlan() throws ImpalaException {
    for (SupportedQuery query : SUPPORTED_QUERIES) {
      try {
        OptimizationResult result =
            optimizeWithCalciteOnly(query.sql_);
        assertNotNull(query.name_, result.plan());
        assertEquals(query.name_, query.outputLabels_, result.outputLabels());
      } catch (UnsupportedFeatureException e) {
        throw new AssertionError(
            "Previously supported Calcite query became unsupported: " + query.name_, e);
      } catch (ImpalaException | RuntimeException e) {
        throw new AssertionError(
            "Calcite planning failed for supported query: " + query.name_, e);
      }
    }
  }

  @Test
  public void testUnsupportedQueryIsNotHiddenByFallback() throws ImpalaException {
    try {
      optimizeWithCalciteOnly(
          "select int_array_col from " + TEST_COMPLEX_TABLE);
      fail("Expected complex type projection to remain explicitly unsupported");
    } catch (UnsupportedFeatureException expected) {
      // The direct harness must preserve the classification rather than invoke fallback.
    }
  }

  /**
   * Runs the Calcite parser, analyzer, RelNode converter, and optimizer directly. This
   * deliberately avoids Frontend.createExecRequest(), so the Original planner fallback
   * cannot satisfy these tests.
   */
  private PreImpalaConvertResult createPreImpalaConvertPlan(String sql)
      throws ImpalaException {
    CalcitePlanInput input = createCalcitePlanInput(sql);
    RelNode plan = input.optimizer().createPreImpalaConvertPlan(input.logicalPlan());
    return new PreImpalaConvertResult(
        plan, input.outputLabels(), collectOutputTypes(plan));
  }

  private OptimizationResult optimizeWithCalciteOnly(String sql)
      throws ImpalaException {
    CalcitePlanInput input = createCalcitePlanInput(sql);
    return new OptimizationResult(
        input.optimizer().optimize(input.logicalPlan()), input.outputLabels());
  }

  private CalcitePlanInput createCalcitePlanInput(String sql) throws ImpalaException {
    CalciteAnalysisResult analysisResult = (CalciteAnalysisResult) parseAndAnalyze(
        sql, feFixture_.createAnalysisCtx(), new CalciteCompilerFactory());
    CalciteRelNodeConverter relNodeConverter =
        new CalciteRelNodeConverter(analysisResult);
    RelNode logicalPlan =
        relNodeConverter.convert(analysisResult.getValidatedNode());
    CalciteOptimizer optimizer = new CalciteOptimizer(
        analysisResult, NoOpEventSequence.INSTANCE, new TQueryOptions());
    ImmutableList<String> outputLabels = ImmutableList.copyOf(
        relNodeConverter.getFieldNames(analysisResult.getValidatedNode()));
    return new CalcitePlanInput(optimizer, logicalPlan, outputLabels);
  }

  private static ImmutableList<RelDataType> collectOutputTypes(RelNode plan) {
    ImmutableList.Builder<RelDataType> outputTypes = ImmutableList.builder();
    for (RelDataTypeField field : plan.getRowType().getFieldList()) {
      outputTypes.add(field.getType());
    }
    return outputTypes.build();
  }

  private record CalcitePlanInput(CalciteOptimizer optimizer, RelNode logicalPlan,
      ImmutableList<String> outputLabels) {}

  private record PreImpalaConvertResult(RelNode plan,
      ImmutableList<String> outputLabels,
      ImmutableList<RelDataType> outputTypes) {
    private PreImpalaConvertResult {
      if (outputLabels.size() != outputTypes.size()) {
        throw new IllegalArgumentException(String.format(
            "Output label count %s does not match output type count %s",
            outputLabels.size(), outputTypes.size()));
      }
    }
  }

  private record OptimizationResult(
      ImpalaPlanRel plan, ImmutableList<String> outputLabels) {}

  private static final class SupportedQuery {
    private final String name_;
    private final String sql_;
    private final ImmutableList<String> outputLabels_;

    private SupportedQuery(String name, String sql, String... outputLabels) {
      name_ = name;
      sql_ = sql;
      outputLabels_ = ImmutableList.copyOf(outputLabels);
    }
  }

  private static Set<String> collectOverFunctions(RelNode plan) {
    Set<String> functions = new HashSet<>();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        node.accept(new RexShuttle() {
          @Override
          public RexNode visitOver(RexOver over) {
            functions.add(over.getOperator().getName().toLowerCase());
            return super.visitOver(over);
          }
        });
        super.visit(node, ordinal, parent);
      }
    }.go(plan);
    return functions;
  }

  private static List<Class<? extends RelNode>> collectRelNodeHierarchy(RelNode plan) {
    ImmutableList.Builder<Class<? extends RelNode>> hierarchy =
        ImmutableList.builder();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        hierarchy.add(node.getClass());
        super.visit(node, ordinal, parent);
      }
    }.go(plan);
    return hierarchy.build();
  }
}
