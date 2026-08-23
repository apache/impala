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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.calcite.rel.node.ImpalaCTEConsumer;
import org.apache.impala.calcite.rel.node.ImpalaCTEProducer;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rel.node.ImpalaSequence;
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
  private static final String TEST_DIM_TABLE = TEST_DB + ".dim";

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
          "id", "row_num"),
      new SupportedQuery("analytic_lead_lag",
          "select lead(int_col, 1) over (order by id) as next_int, "
              + "lag(int_col, 1) over (order by id) as previous_int "
              + "from " + TEST_TABLE,
          "next_int", "previous_int"),
      new SupportedQuery("conditional",
          "select case when int_col > 1 then 'big' else 'small' end as bucket, "
              + "coalesce(int_col, 0) as maybe_int "
              + "from " + TEST_TABLE,
          "bucket", "maybe_int"),
      new SupportedQuery("string_functions",
          "select concat(string_col, 'x') as concatenated, "
              + "length(string_col) as string_length "
              + "from " + TEST_TABLE,
          "concatenated", "string_length"),
      new SupportedQuery("decimal_arithmetic",
          "select decimal_col * cast(2 as decimal(3, 1)) as scaled "
              + "from " + TEST_TABLE,
          "scaled"),
      new SupportedQuery("distinct",
          "select distinct int_col as distinct_int from " + TEST_TABLE,
          "distinct_int"),
      new SupportedQuery("having",
          "select int_col as group_key, count(*) as row_count "
              + "from " + TEST_TABLE + " group by int_col having count(*) > 1",
          "group_key", "row_count"),
      new SupportedQuery("sort_with_offset",
          "select int_col as sorted_int from " + TEST_TABLE + " "
              + "order by int_col limit 5 offset 3",
          "sorted_int"),
      new SupportedQuery("intersect",
          "select int_col as intersected_int from " + TEST_TABLE + " "
              + "intersect select int_col from " + TEST_TABLE,
          "intersected_int"),
      new SupportedQuery("except",
          "select int_col as remaining_int from " + TEST_TABLE + " "
              + "except select int_col from " + TEST_TABLE,
          "remaining_int"),
      new SupportedQuery("outer_join",
          "select l.id as left_id, r.name as right_name "
              + "from " + TEST_TABLE + " l left outer join " + TEST_DIM_TABLE + " r "
              + "on l.id = r.id",
          "left_id", "right_name"),
      new SupportedQuery("cross_join",
          "select l.id as left_id, r.name as right_name "
              + "from " + TEST_TABLE + " l, " + TEST_DIM_TABLE + " r",
          "left_id", "right_name"),
      new SupportedQuery("semi_join",
          "select id as semi_id from " + TEST_TABLE + " "
              + "where id in (select id from " + TEST_DIM_TABLE + ")",
          "semi_id"),
      new SupportedQuery("anti_join",
          "select id as anti_id from " + TEST_TABLE + " "
              + "where id not in (select id from " + TEST_DIM_TABLE + ")",
          "anti_id"),
      new SupportedQuery("exists_subquery",
          "select l.id as exists_id from " + TEST_TABLE + " l "
              + "where exists (select 1 from " + TEST_DIM_TABLE + " r "
              + "where r.id = l.id)",
          "exists_id"),
      new SupportedQuery("scalar_subquery",
          "select id as scalar_id, "
              + "(select count(*) from " + TEST_DIM_TABLE + ") as dim_rows "
              + "from " + TEST_TABLE,
          "scalar_id", "dim_rows"),
      new SupportedQuery("inline_view",
          "select v.projected_int from "
              + "(select int_col as projected_int from " + TEST_TABLE + ") v "
              + "where v.projected_int > 2",
          "projected_int"),
      new SupportedQuery("common_table_expression",
          "with c as (select int_col as cte_int from " + TEST_TABLE + ") "
              + "select cte_int from c",
          "cte_int"));

  @Before
  public void addSyntheticTestTable() {
    addTestDb(TEST_DB, "Synthetic Calcite optimizer test database");
    addTestTable("create table " + TEST_TABLE
        + " (id int, int_col int, bigint_col bigint, string_col string, "
        + "decimal_col decimal(9, 2), wide_decimal_col decimal(38, 2)) "
        + "stored as parquet");
    addTestTable("create table " + TEST_DIM_TABLE
        + " (id int, name string) stored as parquet");
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
  public void testCumeDistAndNtileAreRewrittenBeforeLogicalSeam()
      throws ImpalaException {
    PreImpalaConvertResult cumeDist = createPreImpalaConvertPlan(
        "select cume_dist() over (order by int_col) as cd from " + TEST_TABLE);

    assertEquals(ImmutableList.of("cd"), cumeDist.outputLabels());
    assertEquals(SqlTypeName.DOUBLE,
        cumeDist.outputTypes().get(0).getSqlTypeName());
    Set<String> cumeDistFunctions = collectOverFunctions(cumeDist.plan());
    assertFalse(cumeDistFunctions.contains("cume_dist"));
    assertTrue(cumeDistFunctions.contains("rank"));
    assertTrue(cumeDistFunctions.contains("count"));

    PreImpalaConvertResult ntile = createPreImpalaConvertPlan(
        "select ntile(4) over (order by int_col) as tile from " + TEST_TABLE);

    assertEquals(ImmutableList.of("tile"), ntile.outputLabels());
    assertEquals(SqlTypeName.BIGINT, ntile.outputTypes().get(0).getSqlTypeName());
    Set<String> ntileFunctions = collectOverFunctions(ntile.plan());
    assertFalse(ntileFunctions.contains("ntile"));
    assertTrue(ntileFunctions.contains("row_number"));
    assertTrue(ntileFunctions.contains("count"));
  }

  @Test
  public void testNestedProjectsAreMergedBeforeLogicalSeam() throws ImpalaException {
    PreImpalaConvertResult result = createPreImpalaConvertPlan(
        "select projected_int + 1 as shifted_int from "
            + "(select int_col as projected_int from " + TEST_TABLE + ") v");

    assertEquals(ImmutableList.of("shifted_int"), result.outputLabels());
    assertEquals(SqlTypeName.BIGINT, result.outputTypes().get(0).getSqlTypeName());
    // PROJECT_MERGE runs last in the pre-conversion program, so the inner and
    // outer projects reach the seam as a single one.
    assertEquals(ImmutableList.of(LogicalProject.class, LogicalTableScan.class),
        collectRelNodeHierarchy(result.plan()));
  }

  @Test
  public void testFloorAndCeilCarryImpalaDecimalTypeToLogicalSeam()
      throws ImpalaException {
    for (String function : ImmutableList.of("floor", "ceil")) {
      // Impala infers decimal(p + 1, 0) for these, capped at the maximum
      // precision, which is why they are not plain Calcite operators.
      RelDataType narrow = createPreImpalaConvertPlan(
          "select " + function + "(decimal_col) as rounded from " + TEST_TABLE)
          .outputTypes().get(0);
      assertEquals(function, SqlTypeName.DECIMAL, narrow.getSqlTypeName());
      assertEquals(function, 10, narrow.getPrecision());
      assertEquals(function, 0, narrow.getScale());

      RelDataType wide = createPreImpalaConvertPlan(
          "select " + function + "(wide_decimal_col) as rounded from " + TEST_TABLE)
          .outputTypes().get(0);
      assertEquals(function, SqlTypeName.DECIMAL, wide.getSqlTypeName());
      assertEquals(function, 38, wide.getPrecision());
      assertEquals(function, 0, wide.getScale());
    }
  }

  @Test
  public void testClientLabelsSurviveARenamingProjection() throws ImpalaException {
    // The labels come from the validated statement, not from the field names of
    // the plan, which the pre-conversion rules are free to rewrite.
    PreImpalaConvertResult result = createPreImpalaConvertPlan(
        "select v.projected_int as client_label from "
            + "(select int_col as projected_int from " + TEST_TABLE + ") v "
            + "where v.projected_int > 5");

    assertEquals(ImmutableList.of("client_label"), result.outputLabels());
    assertEquals(1, result.outputTypes().size());
    assertEquals(SqlTypeName.INTEGER, result.outputTypes().get(0).getSqlTypeName());
  }

  @Test
  public void testUnusedColumnsAreTrimmedBeforeLogicalSeam() throws ImpalaException {
    PreImpalaConvertResult result = createPreImpalaConvertPlan(
        "select v.projected_int from (select int_col as projected_int, "
            + "bigint_col as unused_long from " + TEST_TABLE + ") v "
            + "where v.projected_int > 5");

    assertEquals(ImmutableList.of(LogicalFilter.class, LogicalProject.class,
        LogicalTableScan.class), collectRelNodeHierarchy(result.plan()));
    // The inline view selects two columns and the statement uses one, so the
    // field trimmer is what decides how wide the projection at the seam is.
    assertEquals(1, findOnly(result.plan(), LogicalProject.class).getProjects().size());
  }

  @Test
  public void testValuesRowsAreCombinedBeforeLogicalSeam() throws ImpalaException {
    PreImpalaConvertResult result = createPreImpalaConvertPlan(
        "select * from (values (1, 'a'), (2, 'b')) as v(number_col, text_col)");

    assertEquals(ImmutableList.of("number_col", "text_col"), result.outputLabels());
    // Each row starts as its own relation; they reach the seam as one Values.
    assertEquals(ImmutableList.of(LogicalValues.class),
        collectRelNodeHierarchy(result.plan()));
    assertEquals(2, findOnly(result.plan(), LogicalValues.class).getTuples().size());
  }

  @Test
  public void testCteThresholdMaterializesRepeatedSubtreesAtLogicalSeam()
      throws ImpalaException {
    String twoUses = "with c as (select id, int_col from " + TEST_TABLE + " "
        + "where int_col > 1) select a.id as left_id, b.int_col as right_int "
        + "from c a join c b on a.id = b.id";
    String threeUses = "with c as (select id, int_col from " + TEST_TABLE + " "
        + "where int_col > 1) select a.id as left_id from c a "
        + "join c b on a.id = b.id join c d on a.id = d.id";

    // CTE planning is off by default, so the seam sees the expanded plan.
    assertTrue(collectCteRels(createPreImpalaConvertPlan(twoUses).plan()).isEmpty());

    // With cte_threshold set, the repeated subtree becomes a producer, one
    // consumer per use, and a sequence at the root. Those are Impala relations,
    // so the seam stops being a plan of standard Calcite nodes.
    assertEquals(
        ImmutableList.of(ImpalaSequence.class, ImpalaCTEConsumer.class,
            ImpalaCTEConsumer.class, ImpalaCTEProducer.class),
        collectCteRels(
            createPreImpalaConvertPlan(twoUses, cteThreshold(1)).plan()));

    // The option counts references: two uses stay expanded below a threshold of
    // two, three uses do not.
    assertTrue(collectCteRels(
        createPreImpalaConvertPlan(twoUses, cteThreshold(2)).plan()).isEmpty());
    assertEquals(
        ImmutableList.of(ImpalaSequence.class, ImpalaCTEConsumer.class,
            ImpalaCTEConsumer.class, ImpalaCTEConsumer.class,
            ImpalaCTEProducer.class),
        collectCteRels(
            createPreImpalaConvertPlan(threeUses, cteThreshold(2)).plan()));
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
    List<String> unsupportedQueries = ImmutableList.of(
        "select int_array_col from " + TEST_COMPLEX_TABLE,
        "select a.item from " + TEST_COMPLEX_TABLE + " t, t.int_array_col a");
    for (String sql : unsupportedQueries) {
      try {
        optimizeWithCalciteOnly(sql);
        fail("Expected complex types to remain explicitly unsupported: " + sql);
      } catch (UnsupportedFeatureException expected) {
        // The direct harness must preserve the classification rather than invoke
        // fallback.
      }
    }
  }

  /**
   * Runs the Calcite parser, analyzer, RelNode converter, and optimizer directly. This
   * deliberately avoids Frontend.createExecRequest(), so the Original planner fallback
   * cannot satisfy these tests.
   */
  private PreImpalaConvertResult createPreImpalaConvertPlan(String sql)
      throws ImpalaException {
    return createPreImpalaConvertPlan(sql, new TQueryOptions());
  }

  private PreImpalaConvertResult createPreImpalaConvertPlan(String sql,
      TQueryOptions queryOptions) throws ImpalaException {
    CalcitePlanInput input = createCalcitePlanInput(sql, queryOptions);
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
    return createCalcitePlanInput(sql, new TQueryOptions());
  }

  private CalcitePlanInput createCalcitePlanInput(String sql,
      TQueryOptions queryOptions) throws ImpalaException {
    CalciteAnalysisResult analysisResult = (CalciteAnalysisResult) parseAndAnalyze(
        sql, feFixture_.createAnalysisCtx(), new CalciteCompilerFactory());
    CalciteRelNodeConverter relNodeConverter =
        new CalciteRelNodeConverter(analysisResult);
    RelNode logicalPlan =
        relNodeConverter.convert(analysisResult.getValidatedNode());
    CalciteOptimizer optimizer = new CalciteOptimizer(
        analysisResult, NoOpEventSequence.INSTANCE, queryOptions);
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

  private static TQueryOptions cteThreshold(int referenceThreshold) {
    TQueryOptions queryOptions = new TQueryOptions();
    queryOptions.setCte_threshold(referenceThreshold);
    return queryOptions;
  }

  private static List<Class<? extends RelNode>> collectCteRels(RelNode plan) {
    ImmutableList.Builder<Class<? extends RelNode>> cteRels = ImmutableList.builder();
    for (Class<? extends RelNode> relClass : collectRelNodeHierarchy(plan)) {
      if (relClass.equals(ImpalaSequence.class)
          || relClass.equals(ImpalaCTEProducer.class)
          || relClass.equals(ImpalaCTEConsumer.class)) {
        cteRels.add(relClass);
      }
    }
    return cteRels.build();
  }

  private static <T extends RelNode> T findOnly(RelNode plan, Class<T> relClass) {
    List<T> matches = new ArrayList<>();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (relClass.isInstance(node)) matches.add(relClass.cast(node));
        super.visit(node, ordinal, parent);
      }
    }.go(plan);
    assertEquals("Unexpected number of " + relClass.getSimpleName() + " nodes",
        1, matches.size());
    return matches.get(0);
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
