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

package org.apache.impala.planner;


import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Set;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.impala.analysis.Expr;
import org.apache.impala.calcite.operators.ImpalaOperatorTable;
import org.apache.impala.calcite.rel.util.PrunedPartitionHelper;
import org.apache.impala.calcite.schema.ImpalaRelMetadataProvider;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.calcite.schema.FilterSelectivityEstimator;
import org.apache.impala.calcite.service.CalciteJniFrontend.QueryContext;
import org.apache.impala.calcite.service.CalciteMetadataHandler;
import org.apache.impala.calcite.service.CalciteQueryParser;
import org.apache.impala.calcite.service.CalciteRelNodeConverter;
import org.apache.impala.calcite.service.CalciteValidator;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.FeFsPartition;
import com.google.common.base.Preconditions;

import static org.junit.Assert.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCalciteStats extends PlannerTestBase {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestCalciteStats.class.getName());

  // Planner test option to run each planner test.
  private static Set<PlannerTestOption> testOptions = tpcdsParquetTestOptions();

  private static final double ALL_TYPES_CARD = 7300.0;

  private static final double ALL_TYPES_TINY_CARD = 8.0;

  private static final double BIGINT_NDV = 10.0;

  private static final double BIGINT_TINY_NDV = 2.0;

  private static final double DOUBLE_ERR = .000001;

  private static final double PARTITIONED_MONTH_ROWS = 560.0;

  // Query option to run each planner test.
  private static TQueryOptions options =
      tpcdsParquetQueryOptions();

  @BeforeClass
  public static void setUpClass() throws Exception {
    RuntimeEnv.INSTANCE.setTestEnv(true);
    ImpalaOperatorTable.create(BuiltinsDb.getInstance());
    RelMetadataQuery.THREAD_PROVIDERS.set(
        JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE));
  }

  @AfterClass
  public static void cleanUpClass() {
    RuntimeEnv.INSTANCE.reset();
  }

  private RelNode getRelNodeForQuery(String query) throws ImpalaException {
    QueryContext queryCtx = new QueryContext(options, frontend_, query);
    TSessionState session = new TSessionState();
    session.setConnected_user("dummy");
    queryCtx.getTQueryCtx().setSession(session);
    CalciteQueryParser queryParser = new CalciteQueryParser(queryCtx);
    SqlNode parsedSqlNode = queryParser.parse();

    // Make sure the metadata cache has all the info for the query.
    CalciteMetadataHandler mdHandler =
        new CalciteMetadataHandler(parsedSqlNode, queryCtx);
    CalciteValidator validator = new CalciteValidator(mdHandler, queryCtx);
    SqlNode validatedNode = validator.validate(parsedSqlNode);
    CalciteRelNodeConverter relNodeConverter = new CalciteRelNodeConverter(validator);
    return relNodeConverter.convert(validatedNode);
  }

  private RelMetadataQuery getMQ() {
    RelDataTypeFactory typeFactory_ = new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl());
    RelOptPlanner planner_ = new VolcanoPlanner();
    RelOptCluster cluster = RelOptCluster.create(planner_, new RexBuilder(typeFactory_));
    cluster.setMetadataProvider(ImpalaRelMetadataProvider.DEFAULT);
    return cluster.getMetadataQuery();
  }

  @Test
  public void testSimpleQuery() {
    try {
      RelNode logicalPlan =
          getRelNodeForQuery("SELECT id FROM functional.alltypes");
      RelMetadataQuery mq = getMQ();
      assertEquals(ALL_TYPES_CARD, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      assertEquals(1.0, (double) mq.getSelectivity(logicalPlan, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFilterNotEquals() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT bigint_col " +
          "FROM functional.alltypes where bigint_col != 10");
      RelMetadataQuery mq = getMQ();
      double cardinality = (10.0 - 1.0) * ALL_TYPES_CARD/10.0;
      assertEquals(cardinality, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      assertEquals(9.0, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
      assertEquals(1.0, (double) mq.getSelectivity(logicalPlan, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFilterEquals() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT bigint_col, id " +
          "FROM functional.alltypes where bigint_col = 10");
      RelMetadataQuery mq = getMQ();
      double cardinality = ALL_TYPES_CARD/10.0;
      assertEquals(cardinality, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      assertEquals(1.0, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
      bitSet = ImmutableBitSet.of(1);
      double distinctId = ALL_TYPES_CARD / BIGINT_NDV;
      assertEquals(distinctId, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFilterAnd() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT bigint_col " +
          "FROM functional.alltypes where bigint_col = 10 and smallint_col = 10");
      RelMetadataQuery mq = getMQ();
      double cardinality = ALL_TYPES_CARD/100.0;
      assertEquals(cardinality, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      assertEquals(1.0, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFilterOr() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT bigint_col " +
          "FROM functional.alltypes where bigint_col = 10 or smallint_col = 10");
      RelMetadataQuery mq = getMQ();
      Double cardinality = ALL_TYPES_CARD * (1.0 - .9 * .9);
      assertEquals(cardinality, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      Double distinctRows = 10.0 * cardinality / ALL_TYPES_CARD;
      assertEquals(distinctRows, (double) mq.getDistinctRowCount(logicalPlan,
          bitSet, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFilterIsNull() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT some_nulls " +
          "FROM functional.nullrows where some_nulls is null");
      RelMetadataQuery mq = getMQ();
      assertEquals(20.0, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      // TODO: incorrect value here...it should be 1, but it is taking a percentage of
      // distinct rows
      Double incorrectDistinctRowCount = 6.0 * 20.0/26.0;
      assertEquals(incorrectDistinctRowCount, (double) mq.getDistinctRowCount(
          logicalPlan, bitSet, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFilterIsNotNull() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT some_nulls " +
          "FROM functional.nullrows where some_nulls is not null");
      RelMetadataQuery mq = getMQ();
      assertEquals(6.0, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      Double distinctRowCount = 6.0 * 6.0/26.0;
      assertEquals(distinctRowCount, (double) mq.getDistinctRowCount(
          logicalPlan, bitSet, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFilterBoolean() {
    try {
      RelNode logicalPlan =
          getRelNodeForQuery("SELECT bool_col FROM functional.alltypes where bool_col");
      RelMetadataQuery mq = getMQ();
      assertEquals((double) mq.getRowCount(logicalPlan), ALL_TYPES_CARD/2.0, DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      assertEquals(1.0, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFilterInequality() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT bigint_col " +
          "FROM functional.alltypes where bigint_col < 5");
      RelMetadataQuery mq = getMQ();
      double cardinality =
          ALL_TYPES_CARD * FilterSelectivityEstimator.RANGE_COMPARISON_SELECTIVITY;
      assertEquals(cardinality, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      double distinctRows =
          BIGINT_NDV * FilterSelectivityEstimator.RANGE_COMPARISON_SELECTIVITY;
      assertEquals(distinctRows, (double) mq.getDistinctRowCount(logicalPlan,
          bitSet, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFilterBetween() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT bigint_col " +
          "FROM functional.alltypes where bigint_col between 1 and 2");
      // TODO: Need to apply Impala selectivity logic for between
      RelMetadataQuery mq = getMQ();
      double cardinality =
          ALL_TYPES_CARD * FilterSelectivityEstimator.BETWEEN_SELECTIVITY;
      assertEquals(cardinality, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      double distinctRows =
          BIGINT_NDV * FilterSelectivityEstimator.BETWEEN_SELECTIVITY;
      assertEquals(distinctRows, (double) mq.getDistinctRowCount(logicalPlan,
          bitSet, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFilterIn() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT bigint_col " +
          "FROM functional.alltypes where bigint_col in (1,2)");
      // TODO: It's using "OR" logic, not "IN" logic
      RelMetadataQuery mq = getMQ();
      Double cardinality = ALL_TYPES_CARD * (1.0 - .9 * .9);
      assertEquals(cardinality, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      Double distinctRows = 10.0 * cardinality / ALL_TYPES_CARD;
      assertEquals(distinctRows, (double) mq.getDistinctRowCount(logicalPlan,
          bitSet, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  // Simple Join test.
  // alltypes has 7300 rows, all unique on id
  // alltypestiny  has 8 rows, all unique on id
  // So a good guess on the join is 8
  // (<num rows alltypes> * <num rows alltypetiny> / <distinct rows alltypes>
  // The numerator is the number of rows if it were a cross join. Dividing
  // by the number of distinct rows in this join gives us the
  // number of rows in the tiny table.
  @Test
  public void testSimpleInnerJoin() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT a.id FROM functional.alltypes a " +
          "inner join functional.alltypestiny b  on (a.id= b.id)");
      RelMetadataQuery mq = getMQ();
      assertEquals(ALL_TYPES_TINY_CARD, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      // value not exactly 8 because we're using Calcite's estimation for distinct row
      // count
      assertEquals(7.9961654, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
      assertEquals(1.0, (double) mq.getSelectivity(logicalPlan, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  // Slightly more complicated Join test.
  // alltypes has 7300 rows,  10 distinct bigint_col rows
  // alltypestiny  has 8 rows, 2 distinct bigint_col rows
  // The number of total rows calculated uses the below formula
  // (<num rows alltypes> * <num rows alltypetiny> / <distinct rows alltypes>
  // There are 730 rows for every distinct bigint_col in the big table. Each
  // of the 8 rows will be joined with 730 rows, giving us 730 * 8 = 5840 rows.
  // (which matches the rows if you did the select within impala-shell)
  @Test
  public void testInnerJoinSlightCross() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT a.bigint_col FROM " +
          "functional.alltypes a inner join functional.alltypestiny b " +
          "on (a.bigint_col= b.bigint_col)");
      RelMetadataQuery mq = getMQ();
      double cardinality = ALL_TYPES_CARD * ALL_TYPES_TINY_CARD / BIGINT_NDV;
      assertEquals(cardinality, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      // value not exactly 8 because we're using Calcite's estimation for distinct row
      // count
      assertEquals(10.0, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testInnerJoinSlightCrossWithFilter() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT a.bigint_col FROM " +
          "functional.alltypes a inner join functional.alltypestiny b  on " +
          "(a.bigint_col= b.bigint_col) and b.bigint_col = 10");
      logicalPlan = pushFilterIntoOrBelowJoin(logicalPlan);
      RelMetadataQuery mq = getMQ();
      // half of the number of rows from testSimpleInnerJoinSlightCross()
      double cardinality = .5 * ALL_TYPES_CARD * ALL_TYPES_TINY_CARD / BIGINT_NDV;
      assertEquals(cardinality, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      // value not exactly 8 because we're using Calcite's estimation for distinct row
      // count
      assertEquals(10.0, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testInnerJoinWithMultipleConditionFilter() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT a.year, a.month FROM " +
          "functional.alltypestiny a, functional.alltypes b where " +
          "a.year = b.year and a.month = b.month");
      logicalPlan = pushFilterIntoOrBelowJoin(logicalPlan);
      RelMetadataQuery mq = getMQ();
      // The approximate number of rows is calculated as follows:
      // There are 2 unique years in alltypes and 12 unique months, giving us 24 distinct
      // rows. This gives us a distribution of 7300/24 rows for each row in the
      // alltypestiny table. So the stats return 8 * 7300/24 rows, or 2433.333333 rows.
      // Note: The actual select in impala-shell gives us 2400 rows, which isn't exact,
      // but the approximation here is pretty good.
      double cardinality = ALL_TYPES_CARD * ALL_TYPES_TINY_CARD / (2.0 * 12.0);
      assertEquals(cardinality, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      assertEquals(1.0, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
      bitSet = ImmutableBitSet.of(1);
      assertEquals(4.0, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testLeftJoin() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT a.id, b.id FROM " +
          "functional.alltypes a left join functional.alltypestiny b  on ( a.id = b.id)");
      RelMetadataQuery mq = getMQ();
      assertEquals(ALL_TYPES_CARD, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      // Using Calcite's distinct row count, can prolly do better here.
      assertEquals(4614.6640296686, (double) mq.getDistinctRowCount(logicalPlan,
          bitSet, null), DOUBLE_ERR);
      bitSet = ImmutableBitSet.of(1);
      assertEquals(8.0, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testRightJoin() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT b.id, a.id FROM " +
          "functional.alltypestiny a right join functional.alltypes b " +
          "on ( a.id = b.id)");
      RelMetadataQuery mq = getMQ();
      assertEquals(ALL_TYPES_CARD, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      // Using Calcite's distinct row count, can prolly do better here.
      assertEquals(4614.6640296686, (double) mq.getDistinctRowCount(logicalPlan,
          bitSet, null), DOUBLE_ERR);
      bitSet = ImmutableBitSet.of(1);
      assertEquals(8.0, (double) mq.getDistinctRowCount(logicalPlan, bitSet, null),
          DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSimpleQueryNoStats() {
    try {
      RelMetadataQuery mq = getMQ();
      assertEquals(17545.0, (double) mq.getRowCount(getRelNodeForQuery(
          "SELECT id FROM functional_parquet.alltypesagg_hive_13_1")), DOUBLE_ERR);
      // Even though there are no stats, filter estimator will still return 1 since
      // this is an exact match.
      assertEquals(1.0, (double) mq.getRowCount(getRelNodeForQuery("SELECT" +
          " bigint_col, id FROM functional_parquet.alltypes where bigint_col = 10")),
          DOUBLE_ERR);
      assertEquals(758.0, (double) mq.getRowCount(getRelNodeForQuery(
           "SELECT a.id FROM functional_parquet.alltypes a inner join " +
           "functional_parquet.alltypestiny b on (a.id= b.id)")), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAggregate() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT bigint_col FROM " +
          "functional.alltypes group by bigint_col");
      RelMetadataQuery mq = getMQ();
      assertEquals(BIGINT_NDV, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      // Using Calcite's distinct row count, can prolly do better here.
      assertEquals(BIGINT_NDV, (double) mq.getDistinctRowCount(logicalPlan,
          bitSet, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAggregateWithHaving() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT bigint_col FROM " +
          "functional.alltypes group by bigint_col HAVING bigint_col >= 5");
      RelMetadataQuery mq = getMQ();
      double selectivity =
          BIGINT_NDV * FilterSelectivityEstimator.RANGE_COMPARISON_SELECTIVITY;
      assertEquals(selectivity, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      // Using Calcite's distinct row count, can prolly do better here.
      assertEquals(selectivity, (double) mq.getDistinctRowCount(logicalPlan,
          bitSet, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testUnion() {
    try {
      RelNode logicalPlan = getRelNodeForQuery("SELECT bigint_col FROM " +
          "functional.alltypes union all SELECT bigint_col FROM " +
          "functional.alltypestiny");
      RelMetadataQuery mq = getMQ();
      double cardinality = ALL_TYPES_CARD + ALL_TYPES_TINY_CARD;
      assertEquals(cardinality, (double) mq.getRowCount(logicalPlan), DOUBLE_ERR);
      ImmutableBitSet bitSet = ImmutableBitSet.of(0);
      // Using Calcite's distinct row count, can prolly do better here.
      double distinctRows = BIGINT_NDV + BIGINT_TINY_NDV;
      assertEquals(distinctRows, (double) mq.getDistinctRowCount(logicalPlan,
          bitSet, null), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSize() {
    try {
      RelNode logicalPlan =
          getRelNodeForQuery("select * from functional.alltypes");
      RelMetadataQuery mq = getMQ();
      List<Double> avgColumnSizes = mq.getAverageColumnSizes(logicalPlan);
      // id
      assertEquals(4.0, (double) avgColumnSizes.get(0), DOUBLE_ERR);
      // bool_col
      assertEquals(1.0, (double) avgColumnSizes.get(1), DOUBLE_ERR);
      // tinyint_col
      assertEquals(1.0, (double) avgColumnSizes.get(2), DOUBLE_ERR);
      // smallint_col
      assertEquals(2.0,(double) avgColumnSizes.get(3), DOUBLE_ERR);
      // int_col
      assertEquals(4.0, (double) avgColumnSizes.get(4), DOUBLE_ERR);
      // bigint_col
      assertEquals(8.0, (double) avgColumnSizes.get(5), DOUBLE_ERR);
      // float_col
      assertEquals(4.0, (double) avgColumnSizes.get(6), DOUBLE_ERR);
      // double_col
      assertEquals(8.0, (double) avgColumnSizes.get(7), DOUBLE_ERR);
      // datestring_col
      assertEquals(8.0, (double) avgColumnSizes.get(8), DOUBLE_ERR);
      // string_col
      assertEquals(1.0, (double) avgColumnSizes.get(9), DOUBLE_ERR);
      // timestamp_col
      assertEquals(16.0, (double) avgColumnSizes.get(10), DOUBLE_ERR);
      // year
      assertEquals(4.0, (double) avgColumnSizes.get(11), DOUBLE_ERR);
      // month
      assertEquals(4.0, (double) avgColumnSizes.get(12), DOUBLE_ERR);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }


  private RelNode pushFilterIntoOrBelowJoin(RelNode logicalPlan) {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
    builder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
    HepPlanner planner = new HepPlanner(builder.build(),
        logicalPlan.getCluster().getPlanner().getContext(), true, null,
        RelOptCostImpl.FACTORY);
    planner.setRoot(logicalPlan);
    return planner.findBestExp();
  }

  // very basic method to get table, only looks at first input.
  private RexNode getFirstFilterCondition(RelNode relNode) {
    while (relNode.getInputs().size() > 0) {
      relNode = relNode.getInput(0);
      if (relNode instanceof Filter) {
        break;
      }
    }
    if (!(relNode instanceof Filter)) {
      return null;
    }
    Filter filter = (Filter) relNode;
    return filter.getCondition();
  }

  // very basic method to get table, only looks at first input.
  private CalciteTable getTable(RelNode relNode) {
    while (relNode.getInputs().size() > 0) {
      relNode = relNode.getInput(0);
    }
    return (CalciteTable) relNode.getTable();
  }
}
