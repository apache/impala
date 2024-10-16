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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * ExtractLiteralAgg rule gets rid of the LITERAL_AGG into
 * something Impala can handle.
 *
 * Calcite can create an Aggregate RelNode with a single LITERAL_AGG AggregateCall
 * to check for existence in a subquery. In this case, the Aggregate will do a group
 * by, and just spit out a literal value for each row.
 * The LITERAL_AGG function does not exist in Impala. In order to create a RelNode
 * structure that Impala supports, this Aggregate is turned into an Aggregate that
 * still does the "group by" on the relevant groups, but places a Project RelNode
 * on top that returns the literal value.
 */
public class ExtractLiteralAgg extends RelOptRule {

  public ExtractLiteralAgg() {
      super(operand(LogicalAggregate.class, none()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalAggregate aggregate = call.rel(0);

    if (hasSingleLiteralAgg(aggregate.getAggCallList())) {
      LogicalAggregate newAggregate = LogicalAggregate.create(aggregate.getInput(),
          aggregate.getHints(), aggregate.getGroupSet(), aggregate.getGroupSets(),
          new ArrayList<>());
      RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
      List<RexNode> projects = new ArrayList<>();
      // Because we know there is only one agg call and its a literal agg, the
      // first n - 1 columns are not changed in the newly created project.
      for (int i = 0; i < aggregate.getRowType().getFieldCount() - 1; ++i) {
        projects.add(rexBuilder.makeInputRef(aggregate, i));
      }
      AggregateCall aggCall = aggregate.getAggCallList().get(0);
      Preconditions.checkState(aggCall.rexList.get(0) instanceof RexLiteral);
      // Add the nth column as the LITERAL_AGG value
      projects.add(aggCall.rexList.get(0));
      RelNode projectRelNode = LogicalProject.create(newAggregate, new ArrayList<>(),
          projects, aggregate.getRowType().getFieldNames(), new HashSet<>());
      call.transformTo(projectRelNode);
    }
  }

  private boolean hasSingleLiteralAgg(List<AggregateCall> aggCallList) {
    if (aggCallList.size() != 1) {
      return false;
    }
    AggregateCall aggCall = aggCallList.get(0);
    return (aggCall.getAggregation().getName().equals("LITERAL_AGG"));
  }
}
