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
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.impala.calcite.rel.node.ConvertToImpalaRelRules;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;

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

  private final CalciteValidator validator_;

  public CalciteOptimizer(CalciteValidator validator) {
    this.validator_ = validator;
  }

  public ImpalaPlanRel optimize(RelNode logPlan) throws ImpalaException {
    HepProgramBuilder builder = new HepProgramBuilder();

    // rules to convert Calcite nodes into ImpalaPlanRel nodes
    builder.addRuleCollection(
        ImmutableList.of(
            new ConvertToImpalaRelRules.ImpalaScanRule(),
            new ConvertToImpalaRelRules.ImpalaFilterRule(),
            new ConvertToImpalaRelRules.ImpalaProjectRule()));

    HepPlanner planner = new HepPlanner(builder.build(),
        logPlan.getCluster().getPlanner().getContext(),
            false, null, RelOptCostImpl.FACTORY);
    logPlan.getCluster().setMetadataProvider(JaninoRelMetadataProvider.DEFAULT);
    planner.setRoot(logPlan);
    RelNode optimizedPlan = planner.findBestExp();
    if (!(optimizedPlan instanceof ImpalaPlanRel)) {
      throw new InternalException("Could not generate Impala RelNode plan. Plan " +
          "is \n" + getDebugString(optimizedPlan));
    }
    return (ImpalaPlanRel) optimizedPlan;
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
    LOG.debug(getDebugString(resultObject));
  }
}
