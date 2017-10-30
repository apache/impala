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

package org.apache.impala.planner;

import java.util.Collections;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TNestedLoopJoinNode;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Nested-loop join between left child and right child.
 * Initially, the join operator fully materializes the right input in memory.
 * Subsequently, for every row from the left input it identifies the matching rows
 * from the right hand side and produces the join result according to the join operator.
 * The nested-loop join is used when there are no equi-join predicates. Hence,
 * eqJoinConjuncts_ should be empty and all the join conjuncts are stored in
 * otherJoinConjuncts_. Currrently, all join operators are supported except for
 * null-aware anti join.
 *
 * Note: The operator does not spill to disk when there is not enough memory to hold the
 * right input.
 */
public class NestedLoopJoinNode extends JoinNode {
  public NestedLoopJoinNode(PlanNode outer, PlanNode inner, boolean isStraightJoin,
      DistributionMode distrMode, JoinOperator joinOp, List<Expr> otherJoinConjuncts) {
    super(outer, inner, isStraightJoin, distrMode, joinOp,
        Collections.<BinaryPredicate>emptyList(), otherJoinConjuncts,
        "NESTED LOOP JOIN");
  }

  @Override
  public boolean isBlockingJoinNode() { return true; }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    Preconditions.checkState(eqJoinConjuncts_.isEmpty());
    // Set the proper join operator based on whether predicates are assigned or not.
    if (conjuncts_.isEmpty() && otherJoinConjuncts_.isEmpty() && !joinOp_.isSemiJoin() &&
        !joinOp_.isOuterJoin()) {
      joinOp_ = JoinOperator.CROSS_JOIN;
    } else if (joinOp_.isCrossJoin()) {
      // A cross join with predicates is an inner join.
      joinOp_ = JoinOperator.INNER_JOIN;
    }
    orderJoinConjunctsByCost();
    computeStats(analyzer);
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    long perInstanceMemEstimate;
    if (getChild(1).getCardinality() == -1 || getChild(1).getAvgRowSize() == -1
        || numNodes_ == 0) {
      perInstanceMemEstimate = DEFAULT_PER_INSTANCE_MEM;
    } else {
      perInstanceMemEstimate =
          (long) Math.ceil(getChild(1).cardinality_ * getChild(1).avgRowSize_);
    }
    nodeResourceProfile_ = ResourceProfile.noReservation(perInstanceMemEstimate);
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    String labelDetail = getDisplayLabelDetail();
    if (labelDetail == null) {
      output.append(prefix + getDisplayLabel() + "\n");
    } else {
      output.append(String.format("%s%s:%s [%s]\n", prefix, id_.toString(),
          displayName_, getDisplayLabelDetail()));
    }
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (joinTableId_.isValid()) {
          output.append(
              detailPrefix + "join table id: " + joinTableId_.toString() + "\n");
      }
      if (!otherJoinConjuncts_.isEmpty()) {
        output.append(detailPrefix + "join predicates: ")
        .append(getExplainString(otherJoinConjuncts_) + "\n");
      }
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix + "predicates: ")
        .append(getExplainString(conjuncts_) + "\n");
      }
    }
    return output.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.NESTED_LOOP_JOIN_NODE;
    msg.nested_loop_join_node = new TNestedLoopJoinNode();
    msg.nested_loop_join_node.join_op = joinOp_.toThrift();
    for (Expr e: otherJoinConjuncts_) {
      msg.nested_loop_join_node.addToJoin_conjuncts(e.treeToThrift());
    }
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .addValue(super.debugString())
        .toString();
  }
}
