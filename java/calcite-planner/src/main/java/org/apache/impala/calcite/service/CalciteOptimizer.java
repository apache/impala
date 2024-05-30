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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RuleEventLogger;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCommonExpressionSuggester;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.calcite.coercenodes.CoerceNodes;
import org.apache.impala.calcite.operators.ImpalaRexBuilder;
import org.apache.impala.calcite.operators.ImpalaRexSimplify;
import org.apache.impala.calcite.rel.node.ConvertToImpalaRelRules;
import org.apache.impala.calcite.rel.node.ImpalaCTEConsumer;
import org.apache.impala.calcite.rel.node.ImpalaCTEProducer;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rel.node.ImpalaSequence;
import org.apache.impala.calcite.rules.CTERuleConfig;
import org.apache.impala.calcite.rules.IcebergCountStarOptimizer;
import org.apache.impala.calcite.rules.ImpalaCoreRules;
import org.apache.impala.calcite.rules.ImpalaFilterSimplifyRule;
import org.apache.impala.calcite.rules.ImpalaProjectSimplifyRule;
import org.apache.impala.calcite.rules.ImpalaMQContext;
import org.apache.impala.calcite.rules.ImpalaRexExecutor;
import org.apache.impala.calcite.rules.RemoveInfrequentCTERule;
import org.apache.impala.calcite.schema.ImpalaCost;
import org.apache.impala.calcite.util.LogUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.EventSequence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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

  private final TQueryOptions queryOptions_;

  public CalciteOptimizer(CalciteAnalysisResult analysisResult,
      EventSequence timeline, TQueryOptions queryOptions) {
    this.reader_ = analysisResult.getCatalogReader();
    this.validator_ = analysisResult.getSqlValidator();
    this.timeline_ = timeline;
    this.analyzer_ = analysisResult.getAnalyzer();
    this.queryCtx_ = analyzer_.getQueryCtx();
    this.queryOptions_ = queryOptions;
  }

  public ImpalaPlanRel optimize(RelNode logPlan) throws ImpalaException {
    return convertToImpalaPlan(createPreImpalaConvertPlan(logPlan));
  }

  private ImpalaPlanRel convertToImpalaPlan(RelNode preImpalaConvertPlan)
      throws ImpalaException {

    // Change the Calcite RelNodes into ImpalaPlanRel RelNodes, all of which
    // contain a method that converts the RelNodes into Impala PlanNodes.
    ImpalaPlanRel finalOptimizedPlan = runImpalaConvertProgram(preImpalaConvertPlan);
    timeline_.markEvent("Created final Impala convert plan");
    LogUtil.logDebug(finalOptimizedPlan, "Final Impala optimized plan");

    return finalOptimizedPlan;
  }

  @VisibleForTesting
  RelNode createPreImpalaConvertPlan(RelNode logPlan) throws ImpalaException {
    RelBuilder relBuilder = ImpalaCoreRules.LOGICAL_BUILDER_NO_SIMPLIFY.create(
        logPlan.getCluster(), reader_);

    ImpalaRexBuilder rexBuilder = (ImpalaRexBuilder) logPlan.getCluster().getRexBuilder();

    ImpalaRexExecutor rexExecutor = new ImpalaRexExecutor(analyzer_, queryCtx_,
        new ImpalaRexExecutor.ReducerImpl());
    ImpalaRexSimplify simplifier = new ImpalaRexSimplify(rexBuilder, rexExecutor);

    // Run some essential rules needed to create working RelNodes before doing
    // optimization
    timeline_.markEvent("Starting optimization");
    RelNode expandedNodesPlan = runExpandNodesProgram(logPlan, simplifier);
    timeline_.markEvent("Expanded plan");
    LogUtil.logDebug(expandedNodesPlan, "Plan after expanded plan phase.");

    rexBuilder.setPostAnalysis();

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
        coercedNodesPlan, simplifier, false);
    timeline_.markEvent("Created optimized plan pre join");
    LogUtil.logDebug(preJoinOptimizedPlan, "Optimized plan before join rules " +
        "have been applied.");

    // Run join optimization
    RelNode optimizedJoinPlan = runJoinProgram(preJoinOptimizedPlan, simplifier);
    timeline_.markEvent("Created optimized join plan");
    LogUtil.logDebug(optimizedJoinPlan, "Optimized plan after join optimization.");

    // rerun rules that swap RelNodes and optimize the expressions within a RelNode,
    // since the join optimization may have enabled some more rules that can be applied.
    RelNode postOptimizedJoinPlan = runOptimizeNodesProgram(relBuilder, rexBuilder,
        optimizedJoinPlan, simplifier, true);
    timeline_.markEvent("Created optimized plan post join");
    LogUtil.logDebug(postOptimizedJoinPlan, "Optimized plan after a second pass of "
        + "rules applied after join optimization.");

    RelNode optimizedCTEPlan = runCTEProgram(relBuilder, postOptimizedJoinPlan);
    timeline_.markEvent("Created optimized CTE plan");
    LogUtil.logDebug(optimizedCTEPlan, "Optimized plan after CTE substitution.");

    // Run some essential rules needed to create working RelNodes after
    // optimization
    RelNode preImpalaConvertPlan =
        runPreImpalaConvertProgram(optimizedCTEPlan, simplifier);
    LogUtil.logDebug(preImpalaConvertPlan, "Optimized plan after final "
        + "preparation done before conversion to physical nodes.");

    return preImpalaConvertPlan;
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
      RelNode plan, ImpalaRexSimplify simplifier, boolean post) throws ImpalaException {

    RelFieldTrimmer trimmer =
        new RelFieldTrimmer(validator_, relBuilder);
    RelNode trimmedPlan = trimmer.trim(plan);

    HepProgramBuilder builder = new HepProgramBuilder();

    final ImmutableList.Builder<RelOptRule> interRulesBuilder = ImmutableList.builder();

    interRulesBuilder.add(new ImpalaFilterSimplifyRule(simplifier));
    interRulesBuilder.add(new ImpalaProjectSimplifyRule(simplifier));
    interRulesBuilder.add(ImpalaCoreRules.UNION_PULL_UP_CONSTANTS);
    interRulesBuilder.add(ImpalaCoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS);
    interRulesBuilder.add(ImpalaCoreRules.FILTER_PROJECT_TRANSPOSE);
    interRulesBuilder.add(ImpalaCoreRules.FILTER_SET_OP_TRANSPOSE);
    interRulesBuilder.add(ImpalaCoreRules.JOIN_CONDITION_PUSH);
    interRulesBuilder.add(ImpalaCoreRules.FILTER_INTO_JOIN);
    interRulesBuilder.add(ImpalaCoreRules.FILTER_AGGREGATE_TRANSPOSE);
    interRulesBuilder.add(ImpalaCoreRules.UNION_REMOVE);
    // The Semi-join should only be created after the join optimizer kicks in.
    // It creates a semi-join which could prevent the join optimizer from
    // making an optimal choice.
    if (post) {
      interRulesBuilder.add(ImpalaCoreRules.PROJECT_TO_SEMI_JOIN);
    }
    interRulesBuilder.add(ImpalaCoreRules.FILTER_VALUES_MERGE);
    interRulesBuilder.add(ImpalaCoreRules.PROJECT_VALUES_MERGE);
    interRulesBuilder.add(ImpalaCoreRules.FILTER_MERGE);
    interRulesBuilder.add(ImpalaCoreRules.PROJECT_MERGE);
    interRulesBuilder.add(ImpalaCoreRules.JOIN_PUSH_EXPRESSIONS);
    interRulesBuilder.add(PruneEmptyRules.PROJECT_INSTANCE);
    interRulesBuilder.add(PruneEmptyRules.AGGREGATE_INSTANCE);
    interRulesBuilder.add(PruneEmptyRules.SORT_INSTANCE);
    interRulesBuilder.add(PruneEmptyRules.FILTER_INSTANCE);
    interRulesBuilder.add(PruneEmptyRules.UNION_INSTANCE);
    interRulesBuilder.add(PruneEmptyRules.JOIN_LEFT_INSTANCE);
    interRulesBuilder.add(PruneEmptyRules.JOIN_RIGHT_INSTANCE);
    builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    builder.addRuleCollection(interRulesBuilder.build());

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
   * ProjectMerge: The RelFieldTrimmer adds some extra Projects that need
   * to be merged.
   */
  private RelNode runPreImpalaConvertProgram(RelNode plan,
      ImpalaRexSimplify simplifier) throws ImpalaException {

    HepProgramBuilder builder = new HepProgramBuilder();

    RelNode retRelNode = plan.accept(new IcebergCountStarOptimizer());
    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    builder.addRuleCollection(ImmutableList.of(
        ImpalaCoreRules.REWRITE_REX_OVER,
        ImpalaCoreRules.PROJECT_MERGE
        ));

    return runProgram(retRelNode, builder.build(), simplifier);
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

    return (ImpalaPlanRel) runProgram(plan, builder.build(), null);
  }

  private RelNode runProgram(RelNode currentNode, HepProgram program,
      ImpalaRexSimplify simplifier) {
    HepPlanner planner = new HepPlanner(program, new ImpalaMQContext(),
        true, null,
        ImpalaCost.FACTORY);
    planner.setRoot(currentNode);
    // Within Calcite, the RuleEventLogger logs at DEBUG level, but that would
    // be too much logging for the Impala DEBUG level.
    if (LOG.isTraceEnabled()) {
      planner.addListener(new RuleEventLogger());
    }
    if (simplifier != null) planner.setExecutor(simplifier.getRexExecutor());

    return planner.findBestExp();
  }

  public RelNode runCTEProgram(RelBuilder relBuilder,
      RelNode plan) throws ImpalaException {
    if (queryOptions_.num_nodes != 1) {
      // CTEs currently only supported in SingleNodePlanner.
      return plan;
    }

    final int referenceThreshold = queryOptions_.cte_threshold;
    if (referenceThreshold <= 0) {
      // Disable CTE planning.
      return plan;
    }

    Configuration conf = new Configuration();
    conf.setInt(CTESuggesterFactory.CTE_THRESHOLD, referenceThreshold);
    conf.set(CTESuggesterFactory.CTE_SUGGESTER_CLASS,
        BackendConfig.INSTANCE.getCTESuggesterClass());
    RelCommonExpressionSuggester suggester = CTESuggesterFactory.create(conf);
    Collection<RelNode> ctes = suggester.suggest(plan,
        plan.getCluster().getPlanner().getContext());
    if (ctes.isEmpty()) {
      return plan;
    }

    List<RelOptMaterialization> cteMVs = new ArrayList<>();
    int i = 0;
    for (RelNode cte : ctes) {
      final String name = "cte_suggestion_" + i++;
      ImpalaCTEConsumer consumer = new ImpalaCTEConsumer(cte, name);
      cteMVs.add(new RelOptMaterialization(
          consumer, cte, null, consumer.getQualifiedName()));
    }

    final RelNode ctePlan = rewriteUsingViews(plan, cteMVs);

    // Remove infrequent CTEs.
    Map<List<String>, Integer> tableOccurrences =
        findAll(ImpalaCTEConsumer.class, ctePlan)
        .stream().map(ImpalaCTEConsumer::getQualifiedName)
        .collect(Collectors.toMap(Function.identity(), v -> 1, Integer::sum));
    CTERuleConfig cteConfig = CTERuleConfig.create(referenceThreshold, tableOccurrences);
    HepProgram spoolProgram = HepProgram.builder()
        .addRuleInstance(new RemoveInfrequentCTERule(cteConfig))
        .build();
    HepPlanner planner = new HepPlanner(spoolProgram,
        ctePlan.getCluster().getPlanner().getContext(), true, null,
        ImpalaCost.FACTORY);
    cteMVs.forEach(planner::addMaterialization);
    planner.setRoot(ctePlan);
    final RelNode spoolPlan = planner.findBestExp();

    // If no CTEs were added, or all were removed as infrequent, return the original.
    Map<String, List<ImpalaCTEConsumer>> consumers = findAll(
        ImpalaCTEConsumer.class, spoolPlan).stream()
        .collect(Collectors.groupingBy(ImpalaCTEConsumer::getName));
    if (consumers.isEmpty()) {
      return plan;
    }

    // Add producers under a sequence node.
    List<RelNode> inputs = new ArrayList<>();
    inputs.add(spoolPlan);
    for (Map.Entry<String, List<ImpalaCTEConsumer>> entry : consumers.entrySet()) {
      ImpalaCTEConsumer one = entry.getValue().get(0);
      ImpalaCTEProducer producer = new ImpalaCTEProducer(one.getCTE(), one.getName());
      inputs.add(producer);
    }
    return new ImpalaSequence(inputs);
  }

  private RelNode rewriteUsingViews(RelNode basePlan,
      List<RelOptMaterialization> materializations) {
    final RelOptCluster optCluster = basePlan.getCluster();
    RelOptPlanner planner = optCluster.getPlanner();
    if (planner instanceof VolcanoPlanner) {
      // Force calculating costs for logical RelNodes.
      VolcanoPlanner vPlanner = (VolcanoPlanner) planner;
      vPlanner.setNoneConventionHasInfiniteCost(false);
    }

    // We use Volcano planner as the decision on whether to use MVs or not and which MVs
    // to use should be cost-based.
    optCluster.invalidateMetadataQuery();

    // Add materializations to planner.
    for (RelOptMaterialization materialization : materializations) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding materialization {} to the planner; the plan is:\n{}",
            materialization.qualifiedTableName,
            RelOptUtil.toString(materialization.queryRel));
      }
      planner.addMaterialization(materialization);
    }

    // Add MaterializedView rewrite rules.
    RelOptRules.MATERIALIZATION_RULES.forEach(planner::addRule);

    // Optimize plan.
    planner.setRoot(basePlan);
    basePlan = planner.findBestExp();

    // Remove view-based rewriting rules from planner.
    planner.clear();
    // Restore default cost model.
    optCluster.invalidateMetadataQuery();

    return basePlan;
  }

  private <N extends RelNode> List<N> findAll(Class<N> cls, RelNode rel) {
    final Multimap<Class<? extends RelNode>, RelNode> nodes =
        rel.getCluster().getMetadataQuery().getNodeTypes(rel);
    final List<N> results = new ArrayList<>();
    if (nodes == null) {
      return results;
    }
    for (Map.Entry<Class<? extends RelNode>, Collection<RelNode>> e
        : nodes.asMap().entrySet()) {
      if (e.getKey().isAssignableFrom(cls)) {
        for (RelNode node : e.getValue()) {
          if (cls.isInstance(node)) {
            results.add(cls.cast(node));
          }
        }
      }
    }
    return results;
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
