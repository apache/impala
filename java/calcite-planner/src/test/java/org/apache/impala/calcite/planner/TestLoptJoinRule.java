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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.LoptJoinTree;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.impala.calcite.operators.ImpalaOperatorTable;
import org.apache.impala.calcite.rules.ImpalaLoptOptimizeJoinRule;
import org.apache.impala.calcite.schema.ImpalaRelMetadataProvider;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.calcite.schema.ImpalaCost;
import org.apache.impala.calcite.service.CalciteAnalysisResult;
import org.apache.impala.calcite.service.CalciteCompilerFactory;
import org.apache.impala.calcite.service.CalciteMetadataHandler;
import org.apache.impala.calcite.service.CalciteQueryParser;
import org.apache.impala.calcite.service.CalciteRelNodeConverter;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSessionState;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.Set;




public class TestLoptJoinRule extends PlannerTestBase {
  // Planner test option to run each planner test.
  private static Set<PlannerTestOption> testOptions = tpcdsParquetTestOptions();

  public static final ImpalaLoptOptimizeJoinRule.Config MJ_CONFIG =
        ImpalaLoptOptimizeJoinRule.Config.DEFAULT;

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
    CalciteAnalysisResult analysisResult = (CalciteAnalysisResult) parseAndAnalyze(query,
        feFixture_.createAnalysisCtx(), new CalciteCompilerFactory());
    CalciteRelNodeConverter relNodeConverter =
        new CalciteRelNodeConverter(analysisResult);
    return relNodeConverter.convert(analysisResult.getValidatedNode());
  }

  private RelMetadataQuery getMQ() {
    RelDataTypeFactory typeFactory_ = new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl());
    RelOptPlanner planner_ = new VolcanoPlanner();
    RelOptCluster cluster = RelOptCluster.create(planner_, new RexBuilder(typeFactory_));
    cluster.setMetadataProvider(ImpalaRelMetadataProvider.DEFAULT);
    return cluster.getMetadataQuery();
  }

  @Test
  /**
   * Use a defined swap inputs method that returns true if the alltypestiny
   * table is on the right. This will cause swapInputs to return true
   * when alltypestiny is on the left and false when alltypes is on the left
   */
  public void testWithSwapTinyRight() {
    try {
      RelNode logicalPlan =
          getRelNodeForQuery("SELECT a.id, b.id FROM functional.alltypes a, " +
              "functional.alltypestiny b where a.id = b.id");
      ImpalaLoptOptimizeJoinRule ruleWithSwap =
          MJ_CONFIG.withSwapInputsFunction((mq, mj, left, right, sj, cond, rexB, adjust)
              -> swapInputsTinyRight(mq, mj, left, right, cond, rexB, adjust))
          .toRule();

      RelNode relNode = optimizeJoin(logicalPlan, ruleWithSwap);
      TableScan ts = getTableScan(relNode);
      assertEquals("alltypes", ((CalciteTable)ts.getTable()).getName());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  /**
   * Use a defined swap inputs method that returns true if the alltypestiny
   * table is on the left. This will cause swapInputs to return true
   * when alltypes is on the left and false when alltypestiny is on the left
   */
  public void testWithSwapTinyLeft() {
    try {
      RelNode logicalPlan =
          getRelNodeForQuery("SELECT a.id, b.id FROM functional.alltypes a, " +
              "functional.alltypestiny b where a.id = b.id");
      ImpalaLoptOptimizeJoinRule ruleWithSwap =
          MJ_CONFIG.withSwapInputsFunction((mq, mj, left, right, sj, cond, rexB, adjust)
              -> swapInputsTinyLeft(mq, mj, left, right, cond, rexB, adjust))
          .toRule();

      RelNode relNode = optimizeJoin(logicalPlan, ruleWithSwap);
      TableScan ts = getTableScan(relNode);
      assertEquals("alltypestiny", ((CalciteTable)ts.getTable()).getName());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  private RelNode optimizeJoin(RelNode logicalPlan, RelOptRule rule) {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
    HepPlanner planner = new HepPlanner(builder.build(),
        logicalPlan.getCluster().getPlanner().getContext(), true, null,
        ImpalaCost.FACTORY);
    planner.setRoot(logicalPlan);
    RelNode intermediateNode = planner.findBestExp();
    HepProgramBuilder builder2 = new HepProgramBuilder();
    builder2.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    builder2.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
    builder2.addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN);
    builder2.addRuleInstance(rule);
    HepPlanner planner2 = new HepPlanner(builder2.build(),
        logicalPlan.getCluster().getPlanner().getContext(), true, null,
        ImpalaCost.FACTORY);
    planner2.setRoot(intermediateNode);
    return planner2.findBestExp();
  }

  private static boolean swapInputsTinyRight(RelMetadataQuery mq, LoptMultiJoin multiJoin,
      LoptJoinTree leftTree, LoptJoinTree rightTree, RexNode condition,
      RexBuilder rexBuilder, boolean adjust) {
    RelNode leftNode = leftTree.getJoinTree();
    TableScan ts = getTableScan(leftNode);
    return ((CalciteTable)ts.getTable()).getName().equals("alltypestiny");
  }

  private static boolean swapInputsTinyLeft(RelMetadataQuery mq, LoptMultiJoin multiJoin,
      LoptJoinTree leftTree, LoptJoinTree rightTree, RexNode condition,
      RexBuilder rexBuilder, boolean adjust) {
    RelNode leftNode = leftTree.getJoinTree();
    TableScan ts = getTableScan(leftNode);
    return !((CalciteTable)ts.getTable()).getName().equals("alltypestiny");
  }

  private static TableScan getTableScan(RelNode relNode) {
    if (relNode instanceof HepRelVertex) {
      relNode = ((HepRelVertex) relNode).getCurrentRel();
    }
    while (!(relNode instanceof TableScan)) {
      relNode = relNode.getInputs().get(0);
      if (relNode instanceof HepRelVertex) {
        relNode = ((HepRelVertex) relNode).getCurrentRel();
      }
    }
    return (TableScan) relNode;
  }
}
