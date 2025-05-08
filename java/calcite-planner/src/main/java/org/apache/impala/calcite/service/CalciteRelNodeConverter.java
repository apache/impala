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

import org.apache.calcite.rel.type.RelDataTypeFactory;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.Quoting;
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
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.impala.calcite.operators.ImpalaConvertletTable;
import org.apache.impala.calcite.operators.ImpalaRexBuilder;
import org.apache.impala.calcite.schema.ImpalaRelMetadataProvider;
import org.apache.impala.calcite.util.LogUtil;

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

  private final RelOptTable.ViewExpander viewExpander_;

  private final RelOptCluster cluster_;

  private final RelOptPlanner planner_;

  private final RelDataTypeFactory typeFactory_;

  private final SqlValidator sqlValidator_;

  private final CalciteCatalogReader reader_;

  public CalciteRelNodeConverter(CalciteAnalysisResult analysisResult) {
    this.typeFactory_ = analysisResult.getTypeFactory();
    this.reader_ = analysisResult.getCatalogReader();
    this.sqlValidator_ = analysisResult.getSqlValidator();
    this.planner_ = new VolcanoPlanner();
    planner_.addRelTraitDef(ConventionTraitDef.INSTANCE);
    cluster_ =
        RelOptCluster.create(planner_, new ImpalaRexBuilder(typeFactory_));
    viewExpander_ = createViewExpander(
        analysisResult.getSqlValidator().getCatalogReader().getRootSchema().plus());
    cluster_.setMetadataProvider(ImpalaRelMetadataProvider.DEFAULT);
  }

  public CalciteRelNodeConverter(CalciteValidator validator) {
    this.typeFactory_ = validator.getTypeFactory();
    this.reader_ = validator.getCatalogReader();
    this.sqlValidator_ = validator.getSqlValidator();
    this.planner_ = new VolcanoPlanner();
    planner_.addRelTraitDef(ConventionTraitDef.INSTANCE);
    cluster_ =
        RelOptCluster.create(planner_, new ImpalaRexBuilder(typeFactory_));
    viewExpander_ = createViewExpander(validator.getCatalogReader()
        .getRootSchema().plus());
    cluster_.setMetadataProvider(ImpalaRelMetadataProvider.DEFAULT);
  }

  private static RelOptTable.ViewExpander createViewExpander(SchemaPlus schemaPlus) {
    SqlParser.Config parserConfig =
        SqlParser.configBuilder().setCaseSensitive(false).build()
            // This makes SqlParser expect identifiers that require quoting to be
            // enclosed by backticks.
            .withQuoting(Quoting.BACK_TICK);
    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(schemaPlus)
        // This makes 'connectionConfig' in PlannerImpl case-insensitive, which in turn
        // makes the CalciteCatalogReader used to validate the view in
        // PlannerImpl#expandView() case-insensitive. Otherwise,
        // CalciteRelNodeConverter#convert() would fail.
        .parserConfig(parserConfig)
        // We need to add ConventionTraitDef.INSTANCE to avoid the call to
        // table.getStatistic() in LogicalTableScan#create().
        .traitDefs(ConventionTraitDef.INSTANCE)
        .build();
    return new PlannerImpl(config);
  }

  public RelNode convert(SqlNode validatedNode) {
    SqlToRelConverter relConverter = new SqlToRelConverter(
        viewExpander_,
        sqlValidator_,
        reader_,
        cluster_,
        ImpalaConvertletTable.INSTANCE,
        SqlToRelConverter.config().withCreateValuesRel(false));

    // Convert the valid AST into a logical plan
    RelRoot root = relConverter.convertQuery(validatedNode, false, true);
    RelNode relNode = root.project();
    LogUtil.logDebug(relNode, "Plan after conversion from Abstract Syntax Tree");

    RelNode subQueryRemovedPlan =
        runProgram(
            ImmutableList.of(
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE,
                CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                CoreRules.FILTER_SUB_QUERY_TO_CORRELATE
            ),
            relNode);
    LogUtil.logDebug(subQueryRemovedPlan, "Plan after subquery removal phase");

    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster_,
        reader_);
    RelNode decorrelatedPlan =
        RelDecorrelator.decorrelateQuery(subQueryRemovedPlan, relBuilder);

    LogUtil.logDebug(decorrelatedPlan, "Plan after subquery decorrelation phase");
    return decorrelatedPlan;
  }

  public RelOptCluster getCluster() {
    return cluster_;
  }

  @Override
  public void logDebug(Object resultObject) {
    LogUtil.logDebug(resultObject, "RelNodeConverter plan");
  }

  private RelNode runProgram(List<RelOptRule> rules, RelNode currentNode) {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleCollection(rules);
    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

    HepPlanner planner = new HepPlanner(builder.build(),
        currentNode.getCluster().getPlanner().getContext(),
            false, null, RelOptCostImpl.FACTORY);
    planner.setRoot(currentNode);
    return planner.findBestExp();
  }
}
