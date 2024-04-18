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
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * ImpalaValuesRel handles the Values RelNode in Calcite.
 *
 * The Values node will be turned into one of the following:
 * - If the parent node is a Union RelNode, no node will be
 *   created. Instead, the list of Expr will be sent up, and
 *   the union node will incorporate the Expr list into its node.
 * - If the parent node is a filter node, a SelectNode will be
 *   created.
 * - else, a Union node will be created with the values.
 */
public class ImpalaValuesRel extends Values
    implements ImpalaPlanRel {

  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaValuesRel.class.getName());
  public ImpalaValuesRel(Values values) {
    super(values.getCluster(), values.getRowType(), values.getTuples(),
        values.getTraitSet());
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {
    // Value RelNode will generate a Union PlanNode to hold the values. There is
    // no need to create another Union node if the parent is already a Union node.
    if (context.parentType_ == RelNodeType.UNION && (getTuples().size() == 1)) {
      return getValuesExprs(context, getTuples().get(0));
    }

    PlanNodeId nodeId = context.ctx_.getNextNodeId();

    RelDataType rowType =
        context.parentRowType_ != null ? context.parentRowType_ : getRowType();

    List<NodeWithExprs> nodeWithExprsList = getValuesExprs(context);

    NodeWithExprs retNode = NodeCreationUtils.createUnionPlanNode(nodeId,
        context.ctx_.getRootAnalyzer(), rowType, nodeWithExprsList);

    // If there is a filter condition, a SelectNode will get added on top
    // of the retNode.
    return NodeCreationUtils.wrapInSelectNodeIfNeeded(context, retNode,
        getCluster().getRexBuilder());
  }

  private List<NodeWithExprs> getValuesExprs(ParentPlanRelContext context
      ) throws ImpalaException {
    List<NodeWithExprs> nodeWithExprsList = new ArrayList<>();
    for (List<RexLiteral> literals : getTuples()) {
      nodeWithExprsList.add(getValuesExprs(context, literals));
    }
    return nodeWithExprsList;
  }

  private NodeWithExprs getValuesExprs(ParentPlanRelContext context,
      List<RexLiteral> literals) throws ImpalaException {

    PlanNode retNode = null;

    List<Expr> outputExprs = new ArrayList<>();
    int i = 0;
    for (RexLiteral literal : literals) {
      ExprConjunctsConverter converter = new ExprConjunctsConverter(literal,
          new ArrayList<>(), getCluster().getRexBuilder(),
          context.ctx_.getRootAnalyzer());

      if (context.parentRowType_ != null) {
        LiteralExpr e = (LiteralExpr) converter.getImpalaConjuncts().get(0);
        LiteralExpr f = LiteralExpr.createFromUnescapedStr(e.getStringValue(),
            ImpalaTypeConverter.createImpalaType(
                context.parentRowType_.getFieldList().get(i).getType()));
        outputExprs.add(f);
      } else {
        outputExprs.addAll(converter.getImpalaConjuncts());
      }
      i++;
    }

    return new NodeWithExprs(retNode, outputExprs);
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.VALUES;
  }
}
