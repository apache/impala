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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Values;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.ParsedStatement;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rel.node.NodeWithExprs;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanRootSink;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.SingleNodePlannerIntf;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TResultSetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
  private List<String> fieldNames_;
  private boolean returnsMoreThanOneRow_;

  public CalciteSingleNodePlanner(PlannerContext ctx) {
    ctx_ = ctx;
    analysisResult_ = (CalciteAnalysisResult) ctx.getAnalysisResult();
  }

  public PlanNode createSingleNodePlan() throws ImpalaException {
    // Convert the query to RelNodes which can be optimized
    CalciteRelNodeConverter relNodeConverter =
        new CalciteRelNodeConverter(analysisResult_);
    RelNode logicalPlan = relNodeConverter.convert(analysisResult_.getValidatedNode());
    fieldNames_ = relNodeConverter.getFieldNames(analysisResult_.getValidatedNode());

    // Optimize the query
    CalciteOptimizer optimizer =
        new CalciteOptimizer(analysisResult_, ctx_.getTimeline());
    ImpalaPlanRel optimizedPlan = optimizer.optimize(logicalPlan);

    returnsMoreThanOneRow_ = returnsMoreThanOneRow(optimizedPlan);

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
    return PlanRootSink.create(ctx_, rootNode_.outputExprs_, returnsMoreThanOneRow_);
  }

  private boolean returnsMoreThanOneRow(RelNode logicalPlan) {
    return !isSingleRowValues(logicalPlan) && !hasOneRowAgg(logicalPlan);
  }

  private boolean isSingleRowValues(RelNode relNode) {
    // A project will keep the row count the same. Theoretically, a filter
    // can reduce the row count, but there should not be any filter
    // over a values clause because the optimization rules should take
    // care of this situation.
    while (relNode instanceof Project) {
      relNode = relNode.getInput(0);
    }

    if (!(relNode instanceof Values)) {
      return false;
    }

    Values values = (Values) relNode;
    return Values.isEmpty(values) || Values.isSingleValue(values);
  }

  /**
   * Checks if there is an aggregation at the root that guarantees there
   * will be at most one row. Avoid aggs that have groups via a group keyword
   * or a distinct keyword.
   */
  private boolean hasOneRowAgg(RelNode relNode) {
    while (ImpalaPlanRel.canPassThroughParentAggregate(relNode)) {
      relNode = relNode.getInput(0);
    }
    if (!(relNode instanceof Aggregate)) {
      return false;
    }
    Aggregate agg = (Aggregate) relNode;
    if (agg.getGroupCount() > 0 || agg.containsDistinctCall()) {
      return false;
    }
    return true;
  }

  @Override
  public List<String> getColLabels() {
    return fieldNames_;
  }

  @Override
  public TResultSetMetadata getTResultSetMetadata(ParsedStatement parsedStmt) {
    TResultSetMetadata metadata = new TResultSetMetadata();
    int colCnt = rootNode_.outputExprs_.size();
    for (int i = 0; i < colCnt; ++i) {
      TColumn colDesc = new TColumn(fieldNames_.get(i),
          rootNode_.outputExprs_.get(i).getType().toThrift());
      metadata.addToColumns(colDesc);
    }
    return metadata;
  }
}
