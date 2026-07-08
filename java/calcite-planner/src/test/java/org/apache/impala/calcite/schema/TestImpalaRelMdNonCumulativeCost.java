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

package org.apache.impala.calcite.schema;

import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.impala.calcite.rel.node.ImpalaCTEConsumer;
import org.apache.impala.calcite.rel.node.ImpalaCTEProducer;
import org.apache.impala.calcite.rel.node.ImpalaSequence;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestImpalaRelMdNonCumulativeCost {
  private static final double DOUBLE_ERR = .000001;

  @BeforeClass
  public static void setUpClass() {
    RelMetadataQuery.THREAD_PROVIDERS.set(ImpalaRelMetadataProvider.DEFAULT);
  }

  @Test
  public void testAverageRowSizeUsesSubsetInputForMetadata() {
    RelOptCluster cluster = createCluster();
    RelNode values = createValues(cluster, 3);
    RelSubset subset = (RelSubset) cluster.getPlanner().register(values, null);
    RelMetadataQuery mq = cluster.getMetadataQuery();

    Double directAverageSize = mq.getAverageRowSize(values);

    assertNotNull(directAverageSize);
    assertEquals(directAverageSize, mq.getAverageRowSize(subset), DOUBLE_ERR);
  }

  @Test
  public void testAverageRowSizeUsesCTEInputsForMetadata() {
    RelOptCluster cluster = createCluster();
    RelNode values = createValues(cluster, 3);
    RelNode consumer = new ImpalaCTEConsumer(values, "test_cte");
    RelNode producer = new ImpalaCTEProducer(values, "test_cte");
    RelNode sequence = new ImpalaSequence(ImmutableList.of(values, producer));
    RelMetadataQuery mq = cluster.getMetadataQuery();

    Double directAverageSize = mq.getAverageRowSize(values);

    assertNotNull(directAverageSize);
    assertEquals(directAverageSize, mq.getAverageRowSize(consumer), DOUBLE_ERR);
    assertEquals(directAverageSize, mq.getAverageRowSize(producer), DOUBLE_ERR);
    assertEquals(directAverageSize, mq.getAverageRowSize(sequence), DOUBLE_ERR);
  }

  @Test
  public void testJoinCostUsesCTEAndSubsetInputsForMetadata() {
    RelOptCluster cluster = createCluster();
    RelNode values = createValues(cluster, 3);
    RelNode consumer = new ImpalaCTEConsumer(values, "test_cte");
    RelNode producer = new ImpalaCTEProducer(values, "test_cte");
    RelSubset consumerSubset =
        (RelSubset) cluster.getPlanner().register(consumer, null);
    RelMetadataQuery mq = cluster.getMetadataQuery();

    assertNotNull(ImpalaRelMdNonCumulativeCost.getJoinCost(
        consumerSubset, producer, mq));
  }

  @Test
  public void testJoinCostUsesNestedSubsetSizeMetadata() {
    RelOptCluster cluster = createCluster();
    RelSubset leftSubset = (RelSubset) cluster.getPlanner().register(
        createValues(cluster, 3), null);
    RelSubset rightSubset = (RelSubset) cluster.getPlanner().register(
        createValues(cluster, 2), null);
    LogicalJoin nestedJoin = LogicalJoin.create(
        leftSubset, rightSubset, ImmutableList.of(),
        cluster.getRexBuilder().makeLiteral(true), ImmutableSet.of(),
        JoinRelType.INNER);
    RelSubset nestedJoinSubset =
        (RelSubset) cluster.getPlanner().register(nestedJoin, null);
    RelMetadataQuery mq = cluster.getMetadataQuery();

    assertNotNull(mq.getAverageRowSize(nestedJoinSubset));
    assertNotNull(ImpalaRelMdNonCumulativeCost.getJoinCost(
        nestedJoinSubset, createValues(cluster, 1), mq));
  }

  @Test
  public void testAggregateCostUsesCTEAndSubsetInputsForMetadata() {
    RelOptCluster cluster = createCluster();
    RelNode values = createValues(cluster, 3);
    RelNode consumer = new ImpalaCTEConsumer(values, "test_cte");
    RelSubset consumerSubset =
        (RelSubset) cluster.getPlanner().register(consumer, null);
    LogicalAggregate aggregate = LogicalAggregate.create(
        consumerSubset, ImmutableBitSet.of(0), null, ImmutableList.of());
    RelMetadataQuery mq = cluster.getMetadataQuery();

    RelOptCost cost =
        new ImpalaRelMdNonCumulativeCost().getNonCumulativeCost(aggregate, mq);

    assertNotNull(cost);
  }

  private static RelOptCluster createCluster() {
    RelDataTypeFactory typeFactory =
        new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl());
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(
        planner, new RexBuilder(typeFactory));
    cluster.setMetadataProvider(ImpalaRelMetadataProvider.DEFAULT);
    return cluster;
  }

  private static LogicalValues createValues(RelOptCluster cluster, int rowCount) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataType intType = rexBuilder.getTypeFactory()
        .createSqlType(SqlTypeName.INTEGER);
    RelDataType rowType = rexBuilder.getTypeFactory().builder()
        .add("int_col", intType)
        .build();

    ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
    for (int i = 0; i < rowCount; ++i) {
      tuples.add(ImmutableList.of(
          rexBuilder.makeExactLiteral(BigDecimal.valueOf(i), intType)));
    }
    return LogicalValues.create(cluster, rowType, tuples.build());
  }
}
