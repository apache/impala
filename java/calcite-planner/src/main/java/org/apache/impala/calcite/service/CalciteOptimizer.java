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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.impala.calcite.coercenodes.CoerceNodes;
import org.apache.impala.calcite.rel.node.ConvertToImpalaRelRules;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rules.CombineValuesNodesRule;
import org.apache.impala.calcite.rules.ExtractLiteralAgg;
import org.apache.impala.calcite.rules.ImpalaJoinProjectTransposeRule;
import org.apache.impala.calcite.rules.ImpalaMinusToDistinctRule;
import org.apache.impala.calcite.rules.RewriteRexOverRule;
import org.apache.impala.calcite.util.LogUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.util.EventSequence;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CalciteOptimizer. Responsible for optimizing the plan into its final
 * Calcite form. The final Calcite form will be an ImpalaPlanRel node which
 * will contain code that maps the node into a physical Impala PlanNode.
 */
public class CalciteOptimizer implements CompilerStep {
  protected static final Logger LOG =
      LoggerFactory.getLogger(CalciteOptimizer.class.getName());

  private final CalciteCatalogReader reader_;

  private final SqlValidator validator_;

  private final EventSequence timeline_;

  JoinProjectTransposeRule.Config JOIN_PROJECT_LEFT =
      JoinProjectTransposeRule.Config.LEFT_OUTER
          .withOperandSupplier(b0 ->
              b0.operand(LogicalJoin.class).inputs(
                  b1 -> b1.operand(LogicalProject.class).inputs(
                  b2 -> b2.operand(LogicalJoin.class).anyInputs())))
          .withDescription("JoinProjectTransposeRule(Project-Other)")
          .as(JoinProjectTransposeRule.Config.class);

  JoinProjectTransposeRule.Config JOIN_PROJECT_RIGHT =
      JoinProjectTransposeRule.Config.RIGHT_OUTER
          .withOperandSupplier(b0 ->
                b0.operand(LogicalJoin.class).inputs(
                    b1 -> b1.operand(RelNode.class).anyInputs(),
                    b2 -> b2.operand(LogicalProject.class).inputs(
                        b3 -> b3.operand(LogicalJoin.class).anyInputs())))
            .withDescription("JoinProjectTransposeRule(Other-Project)")
            .as(JoinProjectTransposeRule.Config.class);

  public CalciteOptimizer(CalciteAnalysisResult analysisResult,
      EventSequence timeline) {
    this.reader_ = analysisResult.getCatalogReader();
    this.validator_ = analysisResult.getSqlValidator();
    this.timeline_ = timeline;
  }

  public CalciteOptimizer(CalciteValidator validator, EventSequence timeline) {
    this.reader_ = validator.getCatalogReader();
    this.validator_ = validator.getSqlValidator();
    this.timeline_ = timeline;
  }

  public ImpalaPlanRel optimize(RelNode logPlan) throws ImpalaException {
    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(logPlan.getCluster(),
        reader_);

    RexBuilder rexBuilder = logPlan.getCluster().getRexBuilder();

    // Run some essential rules needed to create working RelNodes before doing
    // optimization
    timeline_.markEvent("Starting optimization");
    RelNode expandedNodesPlan = runExpandNodesProgram(logPlan);
    timeline_.markEvent("Expanded plan");
    LogUtil.logDebug(expandedNodesPlan, "Plan after expanded plan phase.");

    // The initial parse and validate steps have some issues finding the correct
    // Impala datatypes for various functions and expressions. For instance,
    // string literals are treated as 'char' by Calcite and 'string' as Impala.
    // The coerceNodes step changes all the expressions and types to something that
    // is compatible with Impala.
    RelNode coercedNodesPlan =
        CoerceNodes.coerceNodes(expandedNodesPlan, rexBuilder);
    timeline_.markEvent("Coerced plan");
    LogUtil.logDebug(coercedNodesPlan, "Plan after it has been coerced.");

    // Run rules that swap RelNodes and optimize the expressions within a RelNode
    RelNode preJoinOptimizedPlan = runOptimizeNodesProgram(relBuilder, coercedNodesPlan);
    timeline_.markEvent("Created optimized plan pre join");
    LogUtil.logDebug(preJoinOptimizedPlan, "Optimized plan before join rules " +
        "have been applied.");

    // Run join optimization
    RelNode optimizedJoinPlan = runJoinProgram(preJoinOptimizedPlan);
    timeline_.markEvent("Created optimized join plan");
    LogUtil.logDebug(optimizedJoinPlan, "Optimized plan after join optimization.");

    // rerun rules that swap RelNodes and optimize the expressions within a RelNode,
    // since the join optimization may have enabled some more rules that can be applied.
    RelNode postOptimizedJoinPlan =
        runOptimizeNodesProgram(relBuilder, optimizedJoinPlan);
    timeline_.markEvent("Created optimized plan post join");
    LogUtil.logDebug(postOptimizedJoinPlan, "Optimized plan after a second pass of "
        + "rules applied after join optimization.");

    // Run some essential rules needed to create working RelNodes after
    // optimization
    RelNode preImpalaConvertPlan =
        runPreImpalaConvertProgram(postOptimizedJoinPlan);
    LogUtil.logDebug(preImpalaConvertPlan, "Optimized plan after final "
        + "preparation done before conversion to physical nodes.");

    // Change the Calcite RelNodes into ImpalaPlanRel RelNodes, all of which
    // contain a method that converts the RelNodes into Impala PlanNodes.
    ImpalaPlanRel finalOptimizedPlan =
        runImpalaConvertProgram(preImpalaConvertPlan);
    timeline_.markEvent("Created final Impala convert plan");
    LogUtil.logDebug(finalOptimizedPlan, "Final Impala optimized plan");

    return finalOptimizedPlan;
  }

  private RelNode runExpandNodesProgram(RelNode plan) throws ImpalaException {

    HepProgramBuilder builder = new HepProgramBuilder();

    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    builder.addRuleCollection(ImmutableList.of(
        CoreRules.INTERSECT_TO_DISTINCT,
        CoreRules.UNION_TO_DISTINCT,
        ImpalaMinusToDistinctRule.Config.DEFAULT.toRule(),
        new CombineValuesNodesRule(),
        new ExtractLiteralAgg(),
        CoreRules.SORT_REMOVE_CONSTANT_KEYS
        ));

    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

    return runProgram(plan, builder.build());
  }

  private RelNode runOptimizeNodesProgram(RelBuilder relBuilder,
      RelNode plan) throws ImpalaException {

    RelFieldTrimmer trimmer =
        new RelFieldTrimmer(validator_, relBuilder);
    RelNode trimmedPlan = trimmer.trim(plan);

    HepProgramBuilder builder = new HepProgramBuilder();

    List<RelOptRule> interRules = ImmutableList.of(
        CoreRules.UNION_PULL_UP_CONSTANTS,
        CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,
        CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.FILTER_SET_OP_TRANSPOSE,
        CoreRules.JOIN_CONDITION_PUSH,
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.FILTER_AGGREGATE_TRANSPOSE,
        CoreRules.PROJECT_JOIN_TRANSPOSE,
        CoreRules.UNION_REMOVE,
        CoreRules.PROJECT_TO_SEMI_JOIN,
        CoreRules.FILTER_VALUES_MERGE,
        CoreRules.FILTER_MERGE,
        CoreRules.PROJECT_MERGE,
        PruneEmptyRules.PROJECT_INSTANCE,
        PruneEmptyRules.AGGREGATE_INSTANCE,
        PruneEmptyRules.SORT_INSTANCE,
        PruneEmptyRules.FILTER_INSTANCE,
        PruneEmptyRules.UNION_INSTANCE,
        PruneEmptyRules.JOIN_LEFT_INSTANCE,
        PruneEmptyRules.JOIN_RIGHT_INSTANCE
        );
    builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    builder.addRuleCollection(interRules);

    return trimmer.trim(runProgram(plan, builder.build()));
  }

  /**
   * Run the rules specifically for join ordering.
   *
   */
  private RelNode runJoinProgram(RelNode plan) throws ImpalaException {

    HepProgramBuilder builder = new HepProgramBuilder();
    // has to be in a separate program or else there is an infinite loop
    builder.addRuleInstance(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES);

    // XXX: add comment about project
    // Merge the filter nodes into the Join. Also include
    // The filter/project transpose in case the Filter
    // exists above the Project in the RelNode so it can
    // then be merged into the Join. The idea is to place
    // joins next to each other if possible for the join
    // optimization step.
    builder.addRuleCollection(ImmutableList.of(
        ImpalaJoinProjectTransposeRule.LEFT_OUTER,
        ImpalaJoinProjectTransposeRule.RIGHT_OUTER,
        CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.PROJECT_MERGE,
        CoreRules.FILTER_INTO_JOIN
        ));

    // Join rules work in a two step process.  The first step
    // is to merge all adjacent joins into one big "multijoin"
    // RelNode (the JOIN_TO_MULTIJOIN rule). Then the
    // MULTI_JOIN_OPTIMIZE rule is used to determine the join
    // ordering.
    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    builder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
    builder.addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN);
    builder.addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE);

    return runProgram(plan, builder.build());
  }

  /**
   * Run the convert program that does one last step to prepare the Logical
   * plan to conversion into the Physical plan. The two current rules in
   * this method are:
   *
   * RewriteRexOverRule: This rule changes analytic expressions similar to
   * the changes made in the "AnalyticExpr.rewrite" method
   *
   * FILTER_PROJECT_TRANSPOSE: One last transpose is done since the physical
   * conversion needs the Logical RelNodes ordered in this way.
   */
  private RelNode runPreImpalaConvertProgram(RelNode plan) throws ImpalaException {

    HepProgramBuilder builder = new HepProgramBuilder();

    RelNode retRelNode = plan;
    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    builder.addRuleCollection(ImmutableList.of(
        RewriteRexOverRule.INSTANCE,
        CoreRules.FILTER_PROJECT_TRANSPOSE
        ));


    return runProgram(retRelNode, builder.build());
  }

  private ImpalaPlanRel runImpalaConvertProgram(RelNode plan) throws ImpalaException {
    HepProgramBuilder builder = new HepProgramBuilder();

    builder.addRuleCollection(ImmutableList.of(
        new ConvertToImpalaRelRules.ImpalaScanRule(),
        new ConvertToImpalaRelRules.ImpalaSortRule(),
        new ConvertToImpalaRelRules.ImpalaProjectRule(),
        new ConvertToImpalaRelRules.ImpalaAggRule(),
        new ConvertToImpalaRelRules.ImpalaJoinRule(),
        new ConvertToImpalaRelRules.ImpalaFilterRule(),
        new ConvertToImpalaRelRules.ImpalaUnionRule(),
        new ConvertToImpalaRelRules.ImpalaValuesRule()
        ));

    return (ImpalaPlanRel) runProgram(plan, builder.build());
  }

  private RelNode runProgram(RelNode currentNode, HepProgram program) {
    HepPlanner planner = new HepPlanner(program,
        currentNode.getCluster().getPlanner().getContext(), true, null,
        RelOptCostImpl.FACTORY);
    planner.setRoot(currentNode);

    return planner.findBestExp();
  }

  public String getDebugString(Object optimizedPlan, String planString) {
    return RelOptUtil.dumpPlan("[" + planString + "]", (RelNode) optimizedPlan,
        SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES);
  }

  @Override
  public void logDebug(Object resultObject) {
    LogUtil.logDebug(resultObject, "Optimized Plan");
  }
}
