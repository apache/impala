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


import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.impala.analysis.AnalysisDriver;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.ParsedStatement;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rel.node.NodeWithExprs;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanRootSink;
import org.apache.impala.planner.SingleNodePlannerIntf;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TResultSetMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of the SingleNodePlannerIntf which returns a PlanNode
 * to the Impala framework and provides information needed by the
 * framework after planning.
 */
public class CalciteSingleNodePlanner implements SingleNodePlannerIntf {

  protected static final Logger LOG =
      LoggerFactory.getLogger(CalciteSingleNodePlanner.class.getName());
  private final PlannerContext ctx_;
  private final CalciteAnalysisResult analysisResult_;
  private NodeWithExprs rootNode_;

  public CalciteSingleNodePlanner(PlannerContext ctx) {
    ctx_ = ctx;
    analysisResult_ = (CalciteAnalysisResult) ctx.getAnalysisResult();
  }

  public PlanNode createSingleNodePlan() throws ImpalaException {
    // Convert the query to RelNodes which can be optimized
    CalciteRelNodeConverter relNodeConverter =
        new CalciteRelNodeConverter(analysisResult_);
    RelNode logicalPlan = relNodeConverter.convert(analysisResult_.getValidatedNode());

    // Optimize the query
    CalciteOptimizer optimizer =
        new CalciteOptimizer(analysisResult_, ctx_.getTimeline());
    ImpalaPlanRel optimizedPlan = optimizer.optimize(logicalPlan);

    // Create Physical Impala PlanNodes
    CalcitePhysPlanCreator physPlanCreator =
        new CalcitePhysPlanCreator(analysisResult_.getAnalyzer(), ctx_);
    rootNode_ = physPlanCreator.create(optimizedPlan);

    analysisResult_.getAnalyzer().computeValueTransferGraph();
    return rootNode_.planNode_;
  }

  /**
   * Creates the DataSink needed by the framework. Only the original planner
   * requires the substition map passed in.
   */
  @Override
  public DataSink createDataSink(ExprSubstitutionMap rootNodeSmap) {
    return new PlanRootSink(rootNode_.outputExprs_);
  }

  @Override
  public List<String> getColLabels() {
    return rootNode_.fieldNames_;
  }

  @Override
  public TResultSetMetadata getTResultSetMetadata(ParsedStatement parsedStmt) {
    TResultSetMetadata metadata = new TResultSetMetadata();
    int colCnt = rootNode_.outputExprs_.size();
    for (int i = 0; i < colCnt; ++i) {
      TColumn colDesc = new TColumn(rootNode_.fieldNames_.get(i).toLowerCase(),
          rootNode_.outputExprs_.get(i).getType().toThrift());
      metadata.addToColumns(colDesc);
    }
    return metadata;
  }
}
