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
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
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

  private final CalciteJniFrontend.QueryContext queryCtx_;
  private final Analyzer analyzer_;
  private final PlannerContext plannerContext_;

  public CalcitePhysPlanCreator(CalciteMetadataHandler mdHandler,
      CalciteJniFrontend.QueryContext queryCtx) throws ImpalaException {
    this.queryCtx_ = queryCtx;
    // TODO: IMPALA-13011: Awkward call for authorization here. Authorization
    // will be done at validation time, but this is needed here for the Analyzer
    // instantiation.
    AuthorizationFactory authzFactory =
        AuthorizationUtil.authzFactoryFrom(BackendConfig.INSTANCE);
    this.analyzer_ = new Analyzer(mdHandler.getStmtTableCache(),
        queryCtx_.getTQueryCtx(), authzFactory, null);
    this.plannerContext_ =
        new PlannerContext(analyzer_, queryCtx_.getTQueryCtx(), queryCtx_.getTimeline());

  }

  /**
   * returns the root plan node along with its output expressions.
   */
  public NodeWithExprs create(ImpalaPlanRel optimizedPlan) throws ImpalaException {
    ParentPlanRelContext.Builder builder =
        new ParentPlanRelContext.Builder(plannerContext_);
    NodeWithExprs rootNodeWithExprs = optimizedPlan.getPlanNode(builder.build());
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
      LOG.debug("Finished physical plan step, but unknown result: " + resultObject);
      return;
    }
    LOG.debug("Physical Plan: " + resultObject);
  }
}
