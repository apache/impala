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

import org.apache.impala.calcite.rel.node.NodeWithExprs;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.util.SimplifiedAnalyzer;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.calcite.rel.node.ParentPlanRelContext;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.util.AuthorizationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CalcitePhysPlanCreator. This class is responsible for turning an ImpalaPlanRel
 * Calcite plan into the Impala physical PlanNode plan for a single node.
 */
public class CalcitePhysPlanCreator implements CompilerStep {
  protected static final Logger LOG =
      LoggerFactory.getLogger(CalcitePhysPlanCreator.class.getName());

  private final Analyzer analyzer_;
  private final PlannerContext plannerContext_;

  public CalcitePhysPlanCreator(Analyzer analyzer, PlannerContext ctx) {
    analyzer_ = analyzer;
    plannerContext_ = ctx;
  }

  public CalcitePhysPlanCreator(CalciteMetadataHandler mdHandler,
      CalciteJniFrontend.QueryContext queryCtx) throws ImpalaException {
    this.analyzer_ = mdHandler.getAnalyzer();
    this.plannerContext_ =
        new PlannerContext(analyzer_, queryCtx.getTQueryCtx(), queryCtx.getTimeline());

  }

  /**
   * returns the root plan node along with its output expressions.
   */
  public NodeWithExprs create(ImpalaPlanRel optimizedPlan) throws ImpalaException {
    ParentPlanRelContext rootContext =
        ParentPlanRelContext.createRootContext(plannerContext_);
    NodeWithExprs rootNodeWithExprs = optimizedPlan.getPlanNode(rootContext);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Printing PlanNode tree...");
      printPlanNodeTree(rootNodeWithExprs.planNode_, "");
    }
    return rootNodeWithExprs;
  }

  public void printPlanNodeTree(PlanNode node, String prefix) {
    LOG.debug(prefix + node.getClass());
    for (PlanNode child : node.getChildren()) {
      printPlanNodeTree(child, "  " + prefix);
    }
  }

  public Analyzer getAnalyzer() {
    return analyzer_;
  }

  public PlannerContext getPlannerContext() {
    return plannerContext_;
  }

  @Override
  public void logDebug(Object resultObject) {
    if (!(resultObject instanceof NodeWithExprs)) {
      LOG.debug("Finished physical plan step, but unknown result: {}", resultObject);
      return;
    }
    LOG.debug("Physical Plan: {}", resultObject);
  }
}
