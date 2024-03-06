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

import org.apache.impala.analysis.Expr;
import org.apache.impala.planner.PlanNode;

import java.util.List;

/**
 * NodeWithExprs:  A return class that contains a PlanNode and its output exprs
 * In the case of the Calcite Project RelNode, the PlanNode stays the same, but the
 * output expressions change. This is why the outputExprs cannot be a member of
 * PlanNode
 */
public class NodeWithExprs {
  public final PlanNode planNode_;
  public final List<Expr> outputExprs_;

  public NodeWithExprs(PlanNode planNode, List<Expr> outputExprs) {
    this.planNode_ = planNode;
    this.outputExprs_ = outputExprs;
  }
}
