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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.RelBuilder;
import org.apache.impala.calcite.coercenodes.CoerceNodes;
import org.apache.impala.calcite.rel.node.ConvertToImpalaRelRules;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rules.ConvertToCNFRules;
import org.apache.impala.calcite.rules.ExtractLiteralAgg;
import org.apache.impala.calcite.rules.ImpalaMinusToDistinctRule;
import org.apache.impala.calcite.rules.RewriteRexOverRule;
import org.apache.impala.common.ImpalaException;

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

  private final CalciteValidator validator_;

  public CalciteOptimizer(CalciteValidator validator) {
    this.validator_ = validator;
  }

  public ImpalaPlanRel optimize(RelNode logPlan) throws ImpalaException {
    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(logPlan.getCluster(),
        validator_.getCatalogReader());

    // Run some essential rules needed to create working RelNodes before doing
    // optimization
    RelNode expandedNodesPlan = runExpandNodesProgram(relBuilder, logPlan);

    // Run essential Join Node rules
    RelNode optimizedPlan = runJoinProgram(relBuilder, expandedNodesPlan);

    logDebug(optimizedPlan);

    // Run some essential rules needed to create working RelNodes after
    // optimization
    optimizedPlan = runPreImpalaConvertProgram(relBuilder, optimizedPlan);

    logDebug(optimizedPlan);

    ImpalaPlanRel finalOptimizedPlan =
        runImpalaConvertProgram(relBuilder, optimizedPlan);

    return finalOptimizedPlan;
  }

  public RelNode runExpandNodesProgram(RelBuilder relBuilder,
      RelNode plan) throws ImpalaException {

    HepProgramBuilder builder = new HepProgramBuilder();

    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    builder.addRuleCollection(ImmutableList.of(
        CoreRules.INTERSECT_TO_DISTINCT,
        CoreRules.UNION_TO_DISTINCT,
        new ConvertToCNFRules.FilterConvertToCNFRule(),
        new ConvertToCNFRules.JoinConvertToCNFRule(),
        new ConvertToCNFRules.ProjectConvertToCNFRule(),
        ImpalaMinusToDistinctRule.Config.DEFAULT.toRule()
        ));

    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

    return runProgram(plan, builder.build());
  }

  /**
   * Run the rules specifically for join ordering.
   *
   */
  public RelNode runJoinProgram(RelBuilder relBuilder,
      RelNode plan) throws ImpalaException {

    HepProgramBuilder builder = new HepProgramBuilder();

    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

    // Merge the filter nodes into the Join. Also include
    // The filter/project transpose in case the Filter
    // exists above the Project in the RelNode so it can
    // then be merged into the Join. The idea is to place
    // joins next to each other if possible for the join
    // optimization step.
    builder.addRuleCollection(ImmutableList.of(
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.FILTER_PROJECT_TRANSPOSE
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
    builder.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);

    return runProgram(plan, builder.build());
  }

  public RelNode runPreImpalaConvertProgram(RelBuilder relBuilder,
      RelNode plan) throws ImpalaException {

    HepProgramBuilder builder = new HepProgramBuilder();

    // Impala cannot handle the LITERAL_AGG method so we need to create
    // an equivalent plan
    RelNode retRelNode = plan;
    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    builder.addRuleCollection(ImmutableList.of(
        RewriteRexOverRule.INSTANCE,
        new ExtractLiteralAgg()
        ));


    retRelNode = runProgram(retRelNode, builder.build());

    // Fix up the operands for the nodes which may also change some return types that
    // propagate upwards in the plan.
    return CoerceNodes.coerceNodes(retRelNode, plan.getCluster().getRexBuilder());

  }

  public ImpalaPlanRel runImpalaConvertProgram(RelBuilder relBuilder,
      RelNode plan) throws ImpalaException {
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

  public String getDebugString(Object optimizedPlan) {
    return RelOptUtil.dumpPlan("[Impala plan]", (RelNode) optimizedPlan,
        SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES);
  }

  @Override
  public void logDebug(Object resultObject) {
    if (!(resultObject instanceof RelNode)) {
      LOG.debug("Finished optimizer step, but unknown result: " + resultObject);
      return;
    }
    LOG.info(getDebugString(resultObject));
  }
}
