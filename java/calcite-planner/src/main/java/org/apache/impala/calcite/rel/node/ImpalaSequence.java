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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;
import org.apache.impala.planner.SequenceNode;

public class ImpalaSequence extends AbstractRelNode implements ImpalaPlanRel {

  private final List<RelNode> inputs_;

  private static RelNode passthrough(List<RelNode> inputs) {
    return inputs.get(0);
  }

  // Creates an ImpalaSequence with the first input as the passthrough node and the rest
  // as terminal CTEProducer nodes.
  public ImpalaSequence(List<RelNode> inputs) {
    this(passthrough(inputs).getCluster(), passthrough(inputs).getTraitSet(), inputs);
  }

  private ImpalaSequence(
      RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs) {
    super(cluster, traitSet);
    Preconditions.checkArgument(inputs.size() > 1);
    inputs_ = inputs;
  }

  @Override
  public ImpalaSequence copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ImpalaSequence(getCluster(), traitSet, inputs);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    pw = super.explainTerms(pw);
    for (int i = 0; i < inputs_.size(); i++) {
      pw = pw.input(String.format("input #%s", i), inputs_.get(i));
    }
    return pw;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return passthrough(getInputs()).estimateRowCount(mq);
  }

  @Override
  protected RelDataType deriveRowType() {
    return passthrough(getInputs()).getRowType();
  }

  @Override
  public List<RelNode> getInputs() {
    return inputs_;
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p) {
    inputs_.set(ordinalInParent, p);
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {
    PlanNodeId nodeId = context.ctx_.getNextNodeId();
    List<PlanNode> children = new ArrayList<>(inputs_.size() - 1);

    // Visit CTE producers first. The builder accumulates the producer plan nodes so that
    // they can be passed to the CTE consumers when they are visited.
    ParentPlanRelContext.Builder builder =
        new ParentPlanRelContext.Builder(context, this);
    for (int i = 1; i < inputs_.size(); i++) {
      Preconditions.checkState(inputs_.get(i) instanceof ImpalaCTEProducer);
      ImpalaCTEProducer cteProducer = (ImpalaCTEProducer) inputs_.get(i);
      String name = cteProducer.getName();
      NodeWithExprs ctePlanNode = cteProducer.getPlanNode(builder.build());
      builder.addCTEProducer(name, ctePlanNode);
      children.add(ctePlanNode.planNode_);
    }
    NodeWithExprs passthrough =
        ((ImpalaPlanRel) inputs_.get(0)).getPlanNode(builder.build());

    PlanNode physicalNode = new SequenceNode(nodeId, passthrough.planNode_, children);
    physicalNode.init(context.ctx_.getRootAnalyzer());
    return new NodeWithExprs(physicalNode, passthrough.outputExprs_,
        getRowType().getFieldNames(), passthrough.tblRefs_);
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.SEQUENCE;
  }
}
