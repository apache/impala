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
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Rules to convert RexNode expressions into conjunctive normal form. These
 * are required rules for tpcds queries. Without these rules, the performance for
 * tpcds slows down drastically.
 */
public class ConvertToCNFRules {

  public static class FilterConvertToCNFRule extends RelOptRule {
    public FilterConvertToCNFRule() {
      super(operand(LogicalFilter.class, none()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalFilter filter = call.rel(0);

      RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
      RexNode condition = filter.getCondition();
      RexNode newCondition = toCnf(rexBuilder, condition);
      if (!newCondition.equals(condition)) {
        LogicalFilter newFilter = LogicalFilter.create(filter.getInput(0), newCondition);
        call.transformTo(newFilter);
      }
    }
  }

  public static class ProjectConvertToCNFRule extends RelOptRule {
    public ProjectConvertToCNFRule() {
        super(operand(LogicalProject.class, none()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalProject project = call.rel(0);

      List<RexNode> newProjects = new ArrayList<>();
      boolean projectChanged = false;
      RexBuilder rexBuilder = project.getCluster().getRexBuilder();
      for (RexNode rexNode : project.getProjects()) {
        RexNode newProject = toCnf(rexBuilder, rexNode);
        newProjects.add(newProject);
        projectChanged |= (!newProject.equals(rexNode));
      }
      if (projectChanged) {
        LogicalProject newProject = LogicalProject.create(project.getInput(0),
             project.getHints(), newProjects,
             project.getRowType().getFieldNames(), project.getVariablesSet());
        call.transformTo(newProject);
      }
    }
  }

  public static class JoinConvertToCNFRule extends RelOptRule {

    public JoinConvertToCNFRule() {
        super(operand(LogicalJoin.class, none()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalJoin join = call.rel(0);
      RexBuilder rexBuilder = join.getCluster().getRexBuilder();
      RexNode condition = join.getCondition();
      RexNode newCondition = toCnf(rexBuilder, condition);
      if (!newCondition.equals(condition)) {
        LogicalJoin newJoin = LogicalJoin.create(join.getInput(0), join.getInput(1),
            join.getHints(), newCondition, new HashSet<>(), join.getJoinType(),
            join.isSemiJoinDone(), ImmutableList.copyOf(join.getSystemFieldList()));
        call.transformTo(newJoin);
      }
    }
  }

  private static class ConvertToCNFShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;
    public boolean fixedOperator = false;

    public ConvertToCNFShuttle (RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexCall cnfCall = (RexCall) RexUtil.toCnf(rexBuilder, call);
      return super.visitCall(cnfCall);
    }
  }

  private static RexNode toCnf(RexBuilder rexBuilder, RexNode rexNode) {
    if (rexNode instanceof RexLiteral) {
      return rexNode;
    }
    RexNode expandedCondition = RexUtil.expandSearch(rexBuilder, null, rexNode);
    RexNode newCondition = RexUtil.pullFactors(rexBuilder, expandedCondition);
    // TODO: IMPALA-13436: use max_cnf_exprs query option instead of hardcoded 100.
    // The default for max_cnf_exprs is 200, but we use 100 here because tpcds
    // q41 is super slow when the value is at 200.
    return RexUtil.toCnf(rexBuilder, 100, newCondition);
  }
}
