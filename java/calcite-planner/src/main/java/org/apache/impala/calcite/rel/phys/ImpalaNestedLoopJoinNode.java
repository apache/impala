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

package org.apache.impala.calcite.rel.phys;

import com.google.common.base.Preconditions;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.NestedLoopJoinNode;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

import java.util.List;

/**
 * ImpalaNestedLoopJoinNode: Derived class of NestedLoopJoinNode. This is needed
 * because the HashJoinNode tracks certain conjuncts, but this has already
 * been handled by Calcite so we need to override them with the Calcite
 * conjuncts.
 */
public class ImpalaNestedLoopJoinNode extends NestedLoopJoinNode {

  public ImpalaNestedLoopJoinNode(PlanNodeId id, PlanNode leftInput, PlanNode rightInput,
      boolean isStraightJoin, DistributionMode distMode, JoinOperator joinOp,
      List<Expr> joinConjuncts,
      List<Expr> filterConjuncts, Analyzer analyzer) throws ImpalaException {
    super(leftInput, rightInput, isStraightJoin, distMode, joinOp, joinConjuncts);
    setId(id);
    init(analyzer);
    this.conjuncts_ = filterConjuncts;
  }

  @Override
  public void assignConjuncts(Analyzer analyzer) {
  }

}
