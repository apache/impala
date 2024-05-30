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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Spool;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.CTEProducerNode;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

public class ImpalaCTEProducer extends Spool implements ImpalaPlanRel {
  private final String name_;

  public ImpalaCTEProducer(RelNode cte, String name) {
    this(cte.getCluster(), cte.getTraitSet(), cte, name, Type.EAGER, Type.EAGER);
  }

  private ImpalaCTEProducer(RelOptCluster cluster, RelTraitSet traitSet, RelNode cte,
      String name, Type readType, Type writeType) {
    super(cluster, traitSet, cte, readType, writeType);
    name_ = name;
  }

  @Override
  protected ImpalaCTEProducer copy(
      RelTraitSet traitSet, RelNode input, Type readType, Type writeType) {
    return new ImpalaCTEProducer(
        getCluster(), traitSet, input, name_, readType, writeType);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("cteName", name_);
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {
    PlanNodeId nodeId = context.ctx_.getNextNodeId();

    ImpalaPlanRel relInput = (ImpalaPlanRel) getInput();
    ParentPlanRelContext.Builder builder =
        new ParentPlanRelContext.Builder(context, this);
    NodeWithExprs child = relInput.getPlanNode(builder.build());

    PlanNode physicalNode = new CTEProducerNode(nodeId, child.planNode_, name_);
    physicalNode.init(context.ctx_.getRootAnalyzer());
    return new NodeWithExprs(
        physicalNode, child.outputExprs_, getRowType().getFieldNames(), child.tblRefs_);
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.CTEPRODUCER;
  }

  public String getName() {
    return name_;
  }
}
