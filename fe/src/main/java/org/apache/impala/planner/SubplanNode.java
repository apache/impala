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

import java.util.ArrayList;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TExecNodePhase;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;

import com.google.common.base.Preconditions;

/**
 * A SubplanNode evaluates its right child plan tree for every row from its left child,
 * and returns those rows produced by the right child. The right child is called the
 * 'subplan tree' and the left child the 'input'. A SubplanNode is similar to a join,
 * but different in the following respects. First, a SubplanNode does not do any real
 * work itself. It only returns rows produced by the right child plan tree, which
 * typically has a dependency on the current input row (see SingularRowSrcNode and
 * UnnestNode). Second, no join predicates are required. A SubplanNode does not
 * evaluate any conjuncts.
 */
public class SubplanNode extends PlanNode {
  private PlanNode subplan_;

  public SubplanNode(PlanNode input) {
    super("SUBPLAN");
    children_.add(input);
  }

  /**
   * Sets the subplan of this SubplanNode. Dependent plan nodes such as UnnestNodes
   * and SingularRowSrcNodes need to know their SubplanNode parent, therefore, setting
   * the subplan in this SubplanNode is deferred until the subplan tree has been
   * constructed (which requires the parent SubplanNode to have been constructed).
   */
  public void setSubplan(PlanNode subplan) {
    Preconditions.checkState(children_.size() == 1);
    subplan_ = subplan;
    children_.add(subplan);
    computeTupleIds();
  }

  @Override
  public void computeTupleIds() {
    Preconditions.checkNotNull(subplan_);
    clearTupleIds();
    tblRefIds_.addAll(subplan_.getTblRefIds());
    tupleIds_.addAll(subplan_.getTupleIds());
    nullableTupleIds_.addAll(subplan_.getNullableTupleIds());
  }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    // Subplan root must have been set.
    Preconditions.checkState(children_.size() == 2);
    // Check that there are no unassigned conjuncts that can be evaluated by this node.
    // All such conjuncts should have already been assigned in the right child.
    assignConjuncts(analyzer);
    Preconditions.checkState(conjuncts_.isEmpty());
    computeStats(analyzer);
    outputSmap_ = getChild(1).getOutputSmap();
    // Save state of assigned conjuncts for join-ordering attempts (see member comment).
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    if (getChild(0).cardinality_ != -1 && getChild(1).cardinality_ != -1) {
      cardinality_ =
          checkedMultiply(getChild(0).cardinality_, getChild(1).cardinality_);
    } else {
      cardinality_ = -1;
    }
    cardinality_ = capCardinalityAtLimit(cardinality_);
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = ProcessingCost.basicCost(getDisplayLabel(), getCardinality(), 0);
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // TODO: add an estimate
    nodeResourceProfile_ = ResourceProfile.noReservation(0);
  }

  @Override
  public ExecPhaseResourceProfiles computeTreeResourceProfiles(
      TQueryOptions queryOptions) {
    // All nodes in a subplan remain open at the same time across iterations of a subplan,
    // therefore the peak resource consumption is simply the sum of all node resources.
    ResourceProfile subplanProfile = subplanComputePeakResources(this);
    return new ExecPhaseResourceProfiles(subplanProfile, subplanProfile);
  }

  private static ResourceProfile subplanComputePeakResources(PlanNode node) {
    ResourceProfile result = node.nodeResourceProfile_;
    for (PlanNode child: node.getChildren()) {
      result = result.sum(subplanComputePeakResources(child));
    }
    return result;
  }

  @Override
  public void computePipelineMembership() {
    children_.get(0).computePipelineMembership();
    pipelines_ = new ArrayList<>();
    for (PipelineMembership leftPipeline : children_.get(0).getPipelines()) {
      if (leftPipeline.getPhase() == TExecNodePhase.GETNEXT) {
          pipelines_.add(new PipelineMembership(
              leftPipeline.getId(), leftPipeline.getHeight() + 1, TExecNodePhase.GETNEXT));
      }
    }
    children_.get(1).setPipelinesRecursive(pipelines_);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s\n", prefix, getDisplayLabel()));
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix
            + "predicates: " + Expr.getExplainString(conjuncts_, detailLevel) + "\n");
      }
    }
    return output.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) { msg.node_type = TPlanNodeType.SUBPLAN_NODE; }
}
