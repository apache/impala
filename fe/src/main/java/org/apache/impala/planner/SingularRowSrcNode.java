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

package org.apache.impala.planner;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.ExprUtil;

import com.google.common.base.Preconditions;

/**
 * A SingularRowSrcNode returns the current row that is being processed by its
 * containing SubplanNode. A SingularRowSrcNode can only appear in the plan tree
 * of a SubplanNode. A SingularRowSrcNode returns its parent's smap such that
 * substitutions are appropriately applied within the SubplanNode's second child.
 */
public class SingularRowSrcNode extends PlanNode {
  private final SubplanNode containingSubplanNode_;

  protected SingularRowSrcNode(PlanNodeId id, SubplanNode containingSubplanNode) {
    super(id, "SINGULAR ROW SRC");
    containingSubplanNode_ = containingSubplanNode;
    computeTupleIds();
  }

  @Override
  public void computeTupleIds() {
    clearTupleIds();
    tupleIds_.addAll(containingSubplanNode_.getChild(0).getTupleIds());
    tblRefIds_.addAll(containingSubplanNode_.getChild(0).getTblRefIds());
    nullableTupleIds_.addAll(containingSubplanNode_.getChild(0).getNullableTupleIds());
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    outputSmap_ = containingSubplanNode_.getChild(0).getOutputSmap();
    Preconditions.checkState(conjuncts_.isEmpty());
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = 1;
    // The containing SubplanNode has not yet been initialized, so get the number
    // of nodes from the SubplanNode's input.
    numNodes_ = containingSubplanNode_.getChild(0).getNumNodes();
    numInstances_ = containingSubplanNode_.getChild(0).getNumInstances();
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = ProcessingCost.basicCost(getDisplayLabel(),
        containingSubplanNode_.getChild(0).getCardinality(),
        ExprUtil.computeExprsTotalCost(getConjuncts()));
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // TODO: add an estimate
    nodeResourceProfile_ = ResourceProfile.noReservation(0);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s\n", prefix, getDisplayLabel()));
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(String.format(
          "%sparent-subplan=%s\n", detailPrefix, containingSubplanNode_.getId()));
    }
    return output.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SINGULAR_ROW_SRC_NODE;
  }
}
