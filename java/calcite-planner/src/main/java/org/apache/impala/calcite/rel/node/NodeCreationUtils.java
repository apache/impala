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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.planner.EmptySetNode;
import org.apache.impala.planner.PlanNodeId;
import org.apache.impala.planner.SelectNode;
import org.apache.impala.planner.UnionNode;
import org.apache.impala.calcite.rel.phys.ImpalaUnionNode;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.calcite.rel.util.TupleDescriptorFactory;
import org.apache.impala.common.ImpalaException;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class NodeCreationUtils {

  /**
   * Create an Impala SelectNode. Usually created when a normal Impala node cannot
   * handle a filter expression, so we need a standalone node to do the filter.
   */
  public static NodeWithExprs createSelectNode(RexNode filterCondition, Analyzer analyzer,
      NodeWithExprs nodeWithExprs, PlanNodeId nodeId, RexBuilder rexBuilder
      ) throws ImpalaException {
    Preconditions.checkNotNull(filterCondition);
    ExprConjunctsConverter converter = new ExprConjunctsConverter(filterCondition,
        nodeWithExprs.outputExprs_, rexBuilder, analyzer);
    List<Expr> filterConjuncts = converter.getImpalaConjuncts();
    SelectNode selectNode =
        SelectNode.createFromCalcite(nodeId, nodeWithExprs.planNode_, filterConjuncts);
    selectNode.init(analyzer);
    return new NodeWithExprs(selectNode, nodeWithExprs);
  }

  public static NodeWithExprs createEmptySetPlanNode(PlanNodeId nodeId,
      Analyzer analyzer, RelDataType rowType) throws ImpalaException {
    TupleDescriptorFactory tupleDescFactory =
        new TupleDescriptorFactory("empty set", rowType);
    TupleDescriptor tupleDesc = tupleDescFactory.create(analyzer);

    EmptySetNode emptySetNode =
          new EmptySetNode(nodeId, ImmutableList.of(tupleDesc.getId()));

    emptySetNode.init(analyzer);

    List<Expr> outputExprs = createOutputExprs(tupleDesc.getSlots());

    return new NodeWithExprs(emptySetNode, outputExprs);
  }

  public static NodeWithExprs createUnionPlanNode(PlanNodeId nodeId,
      Analyzer analyzer, RelDataType rowType, List<NodeWithExprs> childrenPlanNodes
      ) throws ImpalaException {
    TupleDescriptorFactory tupleDescFactory =
        new TupleDescriptorFactory("union", rowType);
    TupleDescriptor tupleDesc = tupleDescFactory.create(analyzer);
    // The outputexprs are the SlotRef exprs passed to the parent node.
    List<Expr> outputExprs = createOutputExprs(tupleDesc.getSlots());

    UnionNode unionNode = new ImpalaUnionNode(nodeId, tupleDesc.getId(), outputExprs,
        childrenPlanNodes);

    unionNode.init(analyzer);

    return new NodeWithExprs(unionNode, outputExprs, childrenPlanNodes);
  }

  public static List<Expr> createOutputExprs(List<SlotDescriptor> slotDescs) {
    ImmutableList.Builder<Expr> builder = new ImmutableList.Builder();
    for (SlotDescriptor slotDesc : slotDescs) {
      slotDesc.setIsMaterialized(true);
      builder.add(new SlotRef(slotDesc));
    }
    return builder.build();
  }

  /**
   * wrapInSelectNodeIfNeeded is used for PlanNodes that cannot handle
   * filter conditions directly. A SelectNode with the filter is created
   * in this case.
   */
  public static NodeWithExprs wrapInSelectNodeIfNeeded(ParentPlanRelContext context,
      NodeWithExprs planNode, RexBuilder rexBuilder) throws ImpalaException {
    return context.filterCondition_ != null
      ? NodeCreationUtils.createSelectNode(context.filterCondition_,
          context.ctx_.getRootAnalyzer(), planNode,
          context.ctx_.getNextNodeId(), rexBuilder)
      : planNode;
  }
}
