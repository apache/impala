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

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.impala.common.ImpalaException;

public class ImpalaCTEConsumer extends AbstractRelNode implements ImpalaPlanRel {

  private static final String PREFIX = "cte";

  // The CTE RelNode this consumer is referring to. It will not be updated by further
  // planning, even though the CTE itself under an ImpalaCTEProducer will be. Only use it
  // for properties that do not change during planning, like row type and estimated count.
  private final RelNode cte_;
  private final List<String> qualifiedName_;

  public ImpalaCTEConsumer(RelNode cte, String name) {
    this(cte.getCluster(), cte.getTraitSet(), cte, ImmutableList.of(PREFIX, name));
  }

  private ImpalaCTEConsumer(RelOptCluster cluster, RelTraitSet traitSet, RelNode cte,
      List<String> qualifiedName) {
    super(cluster, traitSet);
    cte_ = cte;
    qualifiedName_ = qualifiedName;
  }

  @Override
  public ImpalaCTEConsumer copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ImpalaCTEConsumer(getCluster(), traitSet, cte_, qualifiedName_);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("cteName", getName());
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return mq.getRowCount(cte_);
  }

  @Override
  public RelDataType deriveRowType() {
    return cte_.getRowType();
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {
    NodeWithExprs plan = NodeCreationUtils.createCTEConsumerPlanNode(context,
        getRowType(), getName());

    // If there is a filter condition, a SelectNode will get added on top of the retNode.
    return NodeCreationUtils.wrapInSelectNodeIfNeeded(
        context, plan, getCluster().getRexBuilder());
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.CTECONSUMER;
  }

  public RelNode getCTE() {
    return cte_;
  }

  public String getName() {
    return Iterables.getLast(getQualifiedName());
  }

  public List<String> getQualifiedName() {
    return qualifiedName_;
  }
}
