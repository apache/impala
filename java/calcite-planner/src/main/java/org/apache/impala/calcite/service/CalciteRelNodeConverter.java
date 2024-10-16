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
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.impala.calcite.operators.ImpalaConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CalciteRelNodeConverter. Responsible for converting a Calcite AST SqlNode into
 * a logical (pre-optimized) plan.
 */
public class CalciteRelNodeConverter implements CompilerStep {
  protected static final Logger LOG =
      LoggerFactory.getLogger(CalciteRelNodeConverter.class.getName());

  private static final RelOptTable.ViewExpander NOOP_EXPANDER =
      (type, query, schema, path) -> null;

  private final CalciteValidator validator_;

  private final RelOptCluster cluster_;

  private final RelOptPlanner planner_;

  public CalciteRelNodeConverter(CalciteValidator validator) {
    this.validator_ = validator;
    this.planner_ = new VolcanoPlanner();
    planner_.addRelTraitDef(ConventionTraitDef.INSTANCE);
    cluster_ =
        RelOptCluster.create(planner_, new RexBuilder(validator_.getTypeFactory()));
  }

  public RelNode convert(SqlNode validatedNode) {
    SqlToRelConverter relConverter = new SqlToRelConverter(
        NOOP_EXPANDER,
        validator_.getSqlValidator(),
        validator_.getCatalogReader(),
        cluster_,
        ImpalaConvertletTable.INSTANCE,
        SqlToRelConverter.config().withCreateValuesRel(false));

    // Convert the valid AST into a logical plan
    RelRoot root = relConverter.convertQuery(validatedNode, false, true);
    RelNode relNode = root.project();
    logDebug(relNode);

    RelNode subQueryRemovedPlan =
        runProgram(
            ImmutableList.of(
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE,
                CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                CoreRules.FILTER_SUB_QUERY_TO_CORRELATE
            ),
            relNode);
    logDebug(subQueryRemovedPlan);

    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster_,
        validator_.getCatalogReader());
    RelNode decorrelatedPlan =
        RelDecorrelator.decorrelateQuery(subQueryRemovedPlan, relBuilder);

    logDebug(decorrelatedPlan);
    return decorrelatedPlan;
  }

  public RelOptCluster getCluster() {
    return cluster_;
  }

  public CalciteValidator getValidator() {
    return validator_;
  }

  @Override
  public void logDebug(Object resultObject) {
    if (!(resultObject instanceof RelNode)) {
      LOG.debug("RelNodeConverter produced an unknown output: " + resultObject);
      return;
    }
    LOG.info(RelOptUtil.dumpPlan("[Logical plan]", (RelNode) resultObject,
        SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES));
  }

  private RelNode runProgram(List<RelOptRule> rules, RelNode currentNode) {
    HepProgramBuilder builder = new HepProgramBuilder();
    // rules to convert Calcite nodes into ImpalaPlanRel nodes
    builder.addRuleCollection(rules);
    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

    HepPlanner planner = new HepPlanner(builder.build(),
        currentNode.getCluster().getPlanner().getContext(),
            false, null, RelOptCostImpl.FACTORY);
    planner.setRoot(currentNode);
    return planner.findBestExp();
  }
}
