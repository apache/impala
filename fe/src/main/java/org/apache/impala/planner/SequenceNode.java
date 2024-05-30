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
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.thrift.TExecNodePhase;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;

/**
 * A SequenceNode contains a passthrough node, and 1 or more terminal nodes. All terminal
 * nodes are evaluated before starting the passthrough node.
 */
public class SequenceNode extends PlanNode {
  private PlanNode passthrough_;

  public SequenceNode(PlanNodeId id, PlanNode passthrough, List<PlanNode> children) {
    super(id, passthrough.getTupleIds(), "SEQUENCE");
    passthrough_ = passthrough;
    children_.add(passthrough);
    children_.addAll(children);
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = capCardinalityAtLimit(passthrough_.cardinality_);
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = ProcessingCost.basicCost(getDisplayLabel(), getCardinality(), 0);
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
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
    for (PlanNode child : getChildren()) {
      child.computePipelineMembership();
    }

    // All children but the passthrough node are pulled from fully in Open.
    pipelines_ = new ArrayList<>();
    for (PipelineMembership pipeline : getChild(0).getPipelines()) {
      if (pipeline.getPhase() == TExecNodePhase.GETNEXT) {
          pipelines_.add(new PipelineMembership(
              pipeline.getId(), pipeline.getHeight() + 1, TExecNodePhase.GETNEXT));
      }
    }
    for (int i = 1; i < getChildCount(); i++) {
      for (PipelineMembership pipeline : getChild(i).getPipelines()) {
        if (pipeline.getPhase() == TExecNodePhase.GETNEXT) {
          pipelines_.add(new PipelineMembership(
              pipeline.getId(), pipeline.getHeight() + 1, TExecNodePhase.OPEN));
        }
      }
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) { msg.node_type = TPlanNodeType.SEQUENCE_NODE; }

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
}
