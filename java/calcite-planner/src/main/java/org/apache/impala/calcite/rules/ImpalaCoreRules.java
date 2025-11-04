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

package org.apache.impala.calcite.rules;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.IntersectToDistinctRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.SemiJoinRule.ProjectToSemiJoinRule;
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.UnionEliminatorRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.UnionPullUpConstantsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * ImpalaCoreRules are the Calcite core rules manipulated to use
 * an Impala custom RelBuilder which does not call 'simplify' for
 * the RexNodes. The 'simplify' method rewrites certain rules that
 * can differ from what Impala needs. See ImpalaRexSimplify for details.
 */

public class ImpalaCoreRules {

  // The RelBuilder config without the simplify
  // TODO: Ideally, there should be an interface within RelBuilder
  // which allows our own RexSimplify to be used. Once this happens,
  // we can add our own RexSimplify to RelBuilder and remove all
  // the rules here that use this NO_SIMPLIFY configuration.
  private static final RelBuilder.Config CONFIG_NO_SIMPLIFY =
      RelBuilder.Config.DEFAULT
          .withSimplify(false);

  public static final RelBuilderFactory LOGICAL_BUILDER_NO_SIMPLIFY =
      RelBuilder.proto(Contexts.of(RelFactories.DEFAULT_STRUCT,
      CONFIG_NO_SIMPLIFY));

  public static RelOptRule PROJECT_SUB_QUERY_TO_CORRELATE =
      SubQueryRemoveRule.Config.PROJECT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(SubQueryRemoveRule.Config.class).toRule();

  public static RelOptRule FILTER_SUB_QUERY_TO_CORRELATE =
      SubQueryRemoveRule.Config.FILTER
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(SubQueryRemoveRule.Config.class).toRule();

  public static RelOptRule JOIN_SUB_QUERY_TO_CORRELATE =
      SubQueryRemoveRule.Config.JOIN
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(SubQueryRemoveRule.Config.class).toRule();

  // Rules from Calcite modified to avoid using simplify.
  // Simplify will be run through its own rule with a custom
  // derived RexSimplify

  public static JoinPushTransitivePredicatesRule JOIN_PUSH_TRANSITIVE_PREDICATES =
      JoinPushTransitivePredicatesRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
          .as(JoinPushTransitivePredicatesRule.Config.class).toRule();

  public static FilterJoinRule JOIN_CONDITION_PUSH =
      FilterJoinRule.JoinConditionPushRule.JoinConditionPushRuleConfig.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
          .as(FilterJoinRule.JoinConditionPushRule.
              JoinConditionPushRuleConfig.class).toRule();

  public static JoinPushExpressionsRule JOIN_PUSH_EXPRESSIONS =
      JoinPushExpressionsRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
          .as(JoinPushExpressionsRule.Config.class).toRule();

  public static FilterJoinRule FILTER_INTO_JOIN =
      FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
          .as(FilterJoinRule.FilterIntoJoinRule.
              FilterIntoJoinRuleConfig.class).toRule();

  public static IntersectToDistinctRule INTERSECT_TO_DISTINCT =
      IntersectToDistinctRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(IntersectToDistinctRule.Config.class).toRule();

  public static UnionToDistinctRule UNION_TO_DISTINCT =
      UnionToDistinctRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(UnionToDistinctRule.Config.class).toRule();

  public static UnionMergeRule UNION_MERGE =
      UnionMergeRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(UnionMergeRule.Config.class).toRule();

  public static UnionPullUpConstantsRule UNION_PULL_UP_CONSTANTS =
      UnionPullUpConstantsRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(UnionPullUpConstantsRule.Config.class).toRule();

  public static AggregateProjectPullUpConstantsRule AGGREGATE_ANY_PULL_UP_CONSTANTS =
      AggregateProjectPullUpConstantsRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(AggregateProjectPullUpConstantsRule.Config.class).toRule();

  public static FilterProjectTransposeRule FILTER_PROJECT_TRANSPOSE =
      FilterProjectTransposeRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(FilterProjectTransposeRule.Config.class).toRule();

  public static FilterSetOpTransposeRule FILTER_SET_OP_TRANSPOSE =
      FilterSetOpTransposeRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(FilterSetOpTransposeRule.Config.class).toRule();

  public static FilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE =
      FilterAggregateTransposeRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(FilterAggregateTransposeRule.Config.class).toRule();

  public static ProjectJoinTransposeRule PROJECT_JOIN_TRANSPOSE =
      ProjectJoinTransposeRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(ProjectJoinTransposeRule.Config.class).toRule();

  public static UnionEliminatorRule UNION_REMOVE =
      UnionEliminatorRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(UnionEliminatorRule.Config.class).toRule();

  public static ProjectToSemiJoinRule PROJECT_TO_SEMI_JOIN =
      ProjectToSemiJoinRule.ProjectToSemiJoinRuleConfig.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(ProjectToSemiJoinRule.ProjectToSemiJoinRuleConfig.class).toRule();

  public static ValuesReduceRule FILTER_VALUES_MERGE =
      ValuesReduceRule.Config.FILTER
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(ValuesReduceRule.Config.class).toRule();

  public static FilterMergeRule FILTER_MERGE =
      FilterMergeRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(FilterMergeRule.Config.class).toRule();

  public static ProjectMergeRule PROJECT_MERGE =
      ProjectMergeRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(ProjectMergeRule.Config.class).toRule();

  public static JoinToMultiJoinRule JOIN_TO_MULTI_JOIN =
      JoinToMultiJoinRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(JoinToMultiJoinRule.Config.class).toRule();

  public static SortRemoveConstantKeysRule SORT_REMOVE_CONSTANT_KEYS =
      SortRemoveConstantKeysRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(SortRemoveConstantKeysRule.Config.class).toRule();

  // Impala defined rules
  public static RelOptRule IMPALA_MINUS_TO_DISTINCT =
      ImpalaMinusToDistinctRule.Config.DEFAULT
          .withRelBuilderFactory(LOGICAL_BUILDER_NO_SIMPLIFY)
           .as(ImpalaMinusToDistinctRule.Config.class).toRule();

  public static RelOptRule COMBINE_VALUES_NODES =
      new CombineValuesNodesRule();

  public static RelOptRule EXTRACT_LITERAL_AGG =
      new ExtractLiteralAgg();

  public static RelOptRule REWRITE_REX_OVER = RewriteRexOverRule.INSTANCE;

  public static RelOptRule JOIN_PROJECT_TRANSPOSE_LEFT_OUTER =
      ImpalaJoinProjectTransposeRule.LEFT_OUTER;

  public static RelOptRule JOIN_PROJECT_TRANSPOSE_RIGHT_OUTER =
      ImpalaJoinProjectTransposeRule.RIGHT_OUTER;

}
