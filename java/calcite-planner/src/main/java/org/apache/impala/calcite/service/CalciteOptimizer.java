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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.calcite.coercenodes.CoerceNodes;
import org.apache.impala.calcite.operators.ImpalaRexSimplify;
import org.apache.impala.calcite.rel.node.ConvertToImpalaRelRules;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rules.ImpalaCoreRules;
import org.apache.impala.calcite.rules.ImpalaFilterSimplifyRule;
import org.apache.impala.calcite.rules.ImpalaProjectSimplifyRule;
import org.apache.impala.calcite.rules.ImpalaMQContext;
import org.apache.impala.calcite.rules.ImpalaRexExecutor;
import org.apache.impala.calcite.schema.ImpalaCost;
import org.apache.impala.calcite.util.LogUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TQueryCtx;
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

  private final Analyzer analyzer_;

  private final TQueryCtx queryCtx_;

  public CalciteOptimizer(CalciteAnalysisResult analysisResult,
      EventSequence timeline) {
    this.reader_ = analysisResult.getCatalogReader();
    this.validator_ = analysisResult.getSqlValidator();
    this.timeline_ = timeline;
    this.analyzer_ = analysisResult.getAnalyzer();
    this.queryCtx_ = analyzer_.getQueryCtx();
  }

  public ImpalaPlanRel optimize(RelNode logPlan) throws ImpalaException {
    RelBuilder relBuilder = ImpalaCoreRules.LOGICAL_BUILDER_NO_SIMPLIFY.create(
        logPlan.getCluster(), reader_);

    RexBuilder rexBuilder = logPlan.getCluster().getRexBuilder();

    ImpalaRexExecutor rexExecutor = new ImpalaRexExecutor(analyzer_, queryCtx_,
        new ImpalaRexExecutor.ReducerImpl());
    ImpalaRexSimplify simplifier = new ImpalaRexSimplify(rexBuilder, rexExecutor);

    // Run some essential rules needed to create working RelNodes before doing
    // optimization
    timeline_.markEvent("Starting optimization");
    RelNode expandedNodesPlan = runExpandNodesProgram(logPlan, simplifier);
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
    RelNode preJoinOptimizedPlan = runOptimizeNodesProgram(relBuilder, rexBuilder,
        coercedNodesPlan, simplifier);
    timeline_.markEvent("Created optimized plan pre join");
    LogUtil.logDebug(preJoinOptimizedPlan, "Optimized plan before join rules " +
        "have been applied.");

    // Run join optimization
    RelNode optimizedJoinPlan = runJoinProgram(preJoinOptimizedPlan, simplifier);
    timeline_.markEvent("Created optimized join plan");
    LogUtil.logDebug(optimizedJoinPlan, "Optimized plan after join optimization.");

    // rerun rules that swap RelNodes and optimize the expressions within a RelNode,
    // since the join optimization may have enabled some more rules that can be applied.
    RelNode postOptimizedJoinPlan =
        runOptimizeNodesProgram(relBuilder, rexBuilder, optimizedJoinPlan, simplifier);
    timeline_.markEvent("Created optimized plan post join");
    LogUtil.logDebug(postOptimizedJoinPlan, "Optimized plan after a second pass of "
        + "rules applied after join optimization.");

    // Run some essential rules needed to create working RelNodes after
    // optimization
    RelNode preImpalaConvertPlan =
        runPreImpalaConvertProgram(postOptimizedJoinPlan, simplifier);
    LogUtil.logDebug(preImpalaConvertPlan, "Optimized plan after final "
        + "preparation done before conversion to physical nodes.");

    // Change the Calcite RelNodes into ImpalaPlanRel RelNodes, all of which
    // contain a method that converts the RelNodes into Impala PlanNodes.
    ImpalaPlanRel finalOptimizedPlan =
        runImpalaConvertProgram(preImpalaConvertPlan, simplifier);
    timeline_.markEvent("Created final Impala convert plan");
    LogUtil.logDebug(finalOptimizedPlan, "Final Impala optimized plan");

    return finalOptimizedPlan;
  }

  private RelNode runExpandNodesProgram(RelNode plan,
      ImpalaRexSimplify simplifier) throws ImpalaException {

    HepProgramBuilder builder = new HepProgramBuilder();

    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    builder.addRuleCollection(ImmutableList.of(
        ImpalaCoreRules.INTERSECT_TO_DISTINCT,
        ImpalaCoreRules.UNION_TO_DISTINCT,
        ImpalaCoreRules.IMPALA_MINUS_TO_DISTINCT,
        ImpalaCoreRules.COMBINE_VALUES_NODES,
        ImpalaCoreRules.EXTRACT_LITERAL_AGG,
        ImpalaCoreRules.SORT_REMOVE_CONSTANT_KEYS
        ));

    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

    return runProgram(plan, builder.build(), simplifier);
  }

  private RelNode runOptimizeNodesProgram(RelBuilder relBuilder, RexBuilder rexBuilder,
      RelNode plan, ImpalaRexSimplify simplifier) throws ImpalaException {

    RelFieldTrimmer trimmer =
        new RelFieldTrimmer(validator_, relBuilder);
    RelNode trimmedPlan = trimmer.trim(plan);

    HepProgramBuilder builder = new HepProgramBuilder();

    List<RelOptRule> interRules = ImmutableList.of(
        new ImpalaFilterSimplifyRule(simplifier),
        new ImpalaProjectSimplifyRule(simplifier),
        ImpalaCoreRules.UNION_PULL_UP_CONSTANTS,
        ImpalaCoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,
        ImpalaCoreRules.FILTER_PROJECT_TRANSPOSE,
        ImpalaCoreRules.FILTER_SET_OP_TRANSPOSE,
        ImpalaCoreRules.JOIN_CONDITION_PUSH,
        ImpalaCoreRules.FILTER_INTO_JOIN,
        ImpalaCoreRules.FILTER_AGGREGATE_TRANSPOSE,
        ImpalaCoreRules.UNION_REMOVE,
        ImpalaCoreRules.PROJECT_TO_SEMI_JOIN,
        ImpalaCoreRules.FILTER_VALUES_MERGE,
        ImpalaCoreRules.FILTER_MERGE,
        ImpalaCoreRules.PROJECT_MERGE,
        ImpalaCoreRules.JOIN_PUSH_EXPRESSIONS,
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

    return trimmer.trim(runProgram(trimmedPlan, builder.build(), simplifier));
  }

  /**
   * Run the rules specifically for join ordering.
   *
   */
  private RelNode runJoinProgram(RelNode plan,
      ImpalaRexSimplify simplifier) throws ImpalaException {

    HepProgramBuilder builder = new HepProgramBuilder();
    // has to be in a separate program or else there is an infinite loop
    builder.addRuleInstance(ImpalaCoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES);
    builder.addRuleInstance(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE);

    // XXX: add comment about project
    // Merge the filter nodes into the Join. Also include
    // The filter/project transpose in case the Filter
    // exists above the Project in the RelNode so it can
    // then be merged into the Join. The idea is to place
    // joins next to each other if possible for the join
    // optimization step.
    builder.addRuleCollection(ImmutableList.of(
        ImpalaCoreRules.JOIN_PROJECT_TRANSPOSE_LEFT_OUTER,
        ImpalaCoreRules.JOIN_PROJECT_TRANSPOSE_RIGHT_OUTER,
        ImpalaCoreRules.FILTER_PROJECT_TRANSPOSE,
        ImpalaCoreRules.PROJECT_MERGE,
        ImpalaCoreRules.FILTER_INTO_JOIN
        ));

    // Join rules work in a two step process.  The first step
    // is to merge all adjacent joins into one big "multijoin"
    // RelNode (the JOIN_TO_MULTIJOIN rule). Then the
    // MULTI_JOIN_OPTIMIZE rule is used to determine the join
    // ordering.
    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    builder.addRuleInstance(ImpalaCoreRules.JOIN_CONDITION_PUSH);
    builder.addRuleInstance(ImpalaCoreRules.JOIN_TO_MULTI_JOIN);
    builder.addRuleInstance(ImpalaCoreRules.MULTI_JOIN_OPTIMIZE);

    return runProgram(plan, builder.build(), simplifier);
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
  private RelNode runPreImpalaConvertProgram(RelNode plan,
      ImpalaRexSimplify simplifier) throws ImpalaException {

    HepProgramBuilder builder = new HepProgramBuilder();

    RelNode retRelNode = plan;
    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    builder.addRuleCollection(ImmutableList.of(
        ImpalaCoreRules.REWRITE_REX_OVER,
        ImpalaCoreRules.FILTER_PROJECT_TRANSPOSE
        ));


    return runProgram(retRelNode, builder.build(), simplifier);
  }

  private ImpalaPlanRel runImpalaConvertProgram(RelNode plan,
      ImpalaRexSimplify simplifier) throws ImpalaException {
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

    return (ImpalaPlanRel) runProgram(plan, builder.build(), simplifier);
  }

  private RelNode runProgram(RelNode currentNode, HepProgram program,
      ImpalaRexSimplify simplifier) {
    HepPlanner planner = new HepPlanner(program, new ImpalaMQContext(),
        true, null,
        ImpalaCost.FACTORY);
    planner.setRoot(currentNode);
    planner.setExecutor(simplifier.getRexExecutor());

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
