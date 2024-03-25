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

package org.apache.impala.calcite.rel.node;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

/**
 * ConvertToImpalaRelRules.  Contains the rules used to change the Calcite RelNodes
 * to Impala RelNodes. These Impala RelNodes are responsible for creating the
 * physical PlanNode plan. The Calcite RelNode and Impala RelNodes map one to one
 * with each other.
 */
public class ConvertToImpalaRelRules {

  public static class ImpalaProjectRule extends RelOptRule {
    public ImpalaProjectRule() {
      super(operand(LogicalProject.class, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalProject project = call.rel(0);
      call.transformTo(new ImpalaProjectRel(project));
    }
  }

  public static class ImpalaFilterRule extends RelOptRule {
    public ImpalaFilterRule() {
      super(operand(LogicalFilter.class, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalFilter filter = call.rel(0);
      call.transformTo(new ImpalaFilterRel(filter));
    }
  }

  public static class ImpalaScanRule extends RelOptRule {

    public ImpalaScanRule() {
      super(operand(LogicalTableScan.class, none()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalTableScan scan = call.rel(0);
      call.transformTo(new ImpalaHdfsScanRel(scan));
    }
  }

}
