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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.impala.calcite.operators.ImpalaOperator;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaJoinProjectTransposeRule is an extension of the JoinProjectTransposeRule in
 * Calcite with an additional restriction, but this needs a bit of explaining...
 *
 * Calcite needs all the joins to be smushed into one big MultiJoin RelNode in order
 * to optimize join ordering. It can only smush parent/child Join RelNodes. So all
 * Projects (and Filters but irrelevant to this rule) must be pushed above the Join.
 * While the JoinProjectTranspose rule handles this, it has one flaw. If there is a
 * Project below the bottom-most Join, we do not want to transpose this above the join.
 * The Project below the Join could affect the row width entering the Join, so leaving
 * the Project there will give us a more accurate row width calculation. The extension
 * here thus checks to see if we don't want to transpose the join. It avoids the
 * transposition by returning from "onMatch" early without calling the parent
 * JoinProjectTransposeRule.onMatch method.
 */

@Value.Enclosing
public class ImpalaJoinProjectTransposeRule extends JoinProjectTransposeRule {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaJoinProjectTransposeRule.class.getName());

  // Rule that searches the left join-project
  public static ImpalaJoinProjectTransposeRule LEFT_OUTER =
      new ImpalaJoinProjectTransposeRule(Config.LEFT_OUTER);

  // Rule that searches the right join-project
  public static ImpalaJoinProjectTransposeRule RIGHT_OUTER =
      new ImpalaJoinProjectTransposeRule(Config.RIGHT_OUTER);

  public ImpalaJoinProjectTransposeRule(JoinProjectTransposeRule.Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);

    // Skip if it's a semi-join. Semi-joins throw an exception in the parent class due
    // to the fact that only the left side projects its columns, and transposing the
    // project on top has an issue because of that.
    if (join.getJoinType() == JoinRelType.SEMI) {
      return;
    }
    if (hasLeftChild(call) && !hasJoinChild(call.rel(1))) {
      return;
    }
    if (hasRightChild(call) && !hasJoinChild(call.rel(2))) {
      return;
    }
    super.onMatch(call);
  }

  // Check if the child has a join underneath. If the child is
  // a Project or Filter, we ignore the node and look at the sub-child,
  // since a Project will ultimately be merged with the other Project
  // and the Filter will be merged with the Join with different rules.
  private boolean hasJoinChild(RelNode relNode) {
    if (relNode instanceof HepRelVertex) {
      relNode = ((HepRelVertex)relNode).getCurrentRel();
    }

    if (relNode instanceof Join) {
      return true;
    }

    if ((relNode instanceof Filter) || (relNode instanceof Project)) {
      return hasJoinChild(relNode.getInput(0));
    }

    return false;
  }
}
