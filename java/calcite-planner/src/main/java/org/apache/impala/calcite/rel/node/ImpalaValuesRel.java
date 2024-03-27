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
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.impala.analysis.Expr;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

import java.util.ArrayList;
import java.util.List;

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

    RelDataType rowType = getRowType();

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
    for (RexLiteral literal : literals) {
      // TODO: IMPALA-13022: Char types are currently disabled. The problem is a bit
      // complex. Impala expects string literals to be of type STRING. Calcite does not
      // have a type STRING and instead creates literals of type CHAR<x>, where <x> is
      // the size of the char literal. This causes a couple of problems:
      // 1) If there is a Union on top of a Values (e.g. select 'hello' union select
      // 'goodbye') there will be a type mismatch (e.g. char(5) and char(7)) which will
      // cause an impalad crash. The server crash is the main reason for disabling this
      // 2) The return type of the root expression should be "string". While this really
      // only will matter once CTAS support is enabled, it still is something that should
      // be flagged as not working right now.
      if (literal.getType().getSqlTypeName().equals(SqlTypeName.CHAR)) {
        throw new AnalysisException("Char type values are not yet supported.");
      }
      ExprConjunctsConverter converter = new ExprConjunctsConverter(literal,
          new ArrayList<>(), getCluster().getRexBuilder(),
          context.ctx_.getRootAnalyzer());
      outputExprs.addAll(converter.getImpalaConjuncts());
    }

    return new NodeWithExprs(retNode, outputExprs);
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.VALUES;
  }
}
