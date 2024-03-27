//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.impala.calcite.rel.node;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNodeId;

import java.util.ArrayList;
import java.util.List;

/**
 * Impala RelNode class for Union.
 * One note: Both "union distinct" and "union all" are handled by this class.
 * Calcite handles the "union distinct" by changing it to a "union all" plus
 * aggregation.
 */
public class ImpalaUnionRel extends Union
    implements ImpalaPlanRel {

  public ImpalaUnionRel(Union union) {
    super(union.getCluster(), union.getTraitSet(), union.getInputs(), union.all);
  }

  private ImpalaUnionRel(RelOptCluster cluster, RelTraitSet traits,
      List<RelNode> inputs, boolean all) {
    super(cluster, traits, inputs, all);
  }

  @Override
  public ImpalaUnionRel copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new ImpalaUnionRel(getCluster(), traitSet, inputs, all);
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {
    PlanNodeId nodeId = context.ctx_.getNextNodeId();

    RelDataType rowType = getRowType();

    List<NodeWithExprs> nodeWithExprsList = getChildrenPlanNodes(getInputs(), context);

    NodeWithExprs retNode = NodeCreationUtils.createUnionPlanNode(nodeId,
        context.ctx_.getRootAnalyzer(), rowType, nodeWithExprsList);

    // If there is a filter condition, a SelectNode will get added on top
    // of the retNode.
    return NodeCreationUtils.wrapInSelectNodeIfNeeded(context, retNode,
        getCluster().getRexBuilder());
  }

  private List<NodeWithExprs> getChildrenPlanNodes(List<RelNode> relInputs,
      ParentPlanRelContext context) throws ImpalaException {
    List<NodeWithExprs> childrenNodes = new ArrayList<>();
    for (RelNode input : relInputs) {
      ImpalaPlanRel inputRel = (ImpalaPlanRel) input;
      ParentPlanRelContext.Builder builder =
          new ParentPlanRelContext.Builder(context, this);
      builder.setFilterCondition(null);
      childrenNodes.add(inputRel.getPlanNode(builder.build()));
    }
    return childrenNodes;
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.UNION;
  }
}
