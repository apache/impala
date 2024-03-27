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

package org.apache.impala.calcite.rel.phys;

import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.calcite.rel.node.NodeWithExprs;
import org.apache.impala.planner.PlanNodeId;
import org.apache.impala.planner.UnionNode;

import java.util.List;

/**
 * ImpalaUnionNode can be created either from a Union RelNode or a Values RelNode.
 * If it comes from a Values RelNode, there is no child node.
 */
public class ImpalaUnionNode extends UnionNode {

  public ImpalaUnionNode(PlanNodeId id, TupleId tupleId, List<Expr> resultExprs,
      List<NodeWithExprs> planNodeAndExprsList) {
    super(id, tupleId, resultExprs, false /* subPlanId */);
    for (NodeWithExprs planNodeAndExprs : planNodeAndExprsList) {
      // planNode is null when produced by a LogicalValues
      if (planNodeAndExprs.planNode_ == null) {
        addConstExprList(planNodeAndExprs.outputExprs_);
      } else {
        addChild(planNodeAndExprs.planNode_, planNodeAndExprs.outputExprs_);
      }
    }
  }
}
