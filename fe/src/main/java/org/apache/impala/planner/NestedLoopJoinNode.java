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
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TNestedLoopJoinNode;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
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
  private static final Logger LOG = LoggerFactory.getLogger(NestedLoopJoinNode.class);

  // Coefficients for estimating NL join CPU processing cost.  Derived from benchmarking.
  private static final double COST_COEFFICIENT_NLJ_TINY_RHS = 0.2049;
  private static final double COST_COEFFICIENT_NLJ = 0.1559;

  public NestedLoopJoinNode(PlanNode outer, PlanNode inner, boolean isStraightJoin,
      DistributionMode distrMode, JoinOperator joinOp, List<Expr> otherJoinConjuncts) {
    super(outer, inner, isStraightJoin, distrMode, joinOp,
        Collections.<BinaryPredicate>emptyList(), otherJoinConjuncts,
        "NESTED LOOP JOIN");
    Preconditions.checkState(joinOp_ != JoinOperator.ICEBERG_DELETE_JOIN);
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
  public Pair<ResourceProfile, ResourceProfile> computeJoinResourceProfile(
      TQueryOptions queryOptions) {
    // TODO: This seems a bug below that the total data is not divided by numInstances_.
    long perInstanceMemEstimate;
    if (getChild(1).getCardinality() == -1 || getChild(1).getAvgRowSize() == -1
        || numNodes_ == 0) {
      perInstanceMemEstimate = DEFAULT_PER_INSTANCE_MEM;
    } else {
      perInstanceMemEstimate =
          (long) Math.ceil(getChild(1).cardinality_ * getChild(1).avgRowSize_);
    }
    ResourceProfile buildProfile = ResourceProfile.noReservation(perInstanceMemEstimate);
    // Memory requirements for the probe side are minimal - batches are just streamed
    // through.
    return Pair.create(ResourceProfile.noReservation(0), buildProfile);
  }

  @Override
  public Pair<ProcessingCost, ProcessingCost> computeJoinProcessingCost() {
    // Benchmarked cost is generally a linear function of the product of the input
    // and output cardinalities, but the coefficients are slightly different for
    // very small RHS (< 5 rows per fragment instance) vs larger RHS so we compute
    // different costs here based on that RHS threshold.
    // We return the full cost in the first element of the Pair.
    long probeCardinality = getProbeCardinalityForCosting();
    long buildCardinality = Math.max(0, getChild(1).getCardinality());
    long cardProduct = checkedMultiply(probeCardinality, buildCardinality);
    long perInstanceBuildCardinality =
        (long) Math.ceil(buildCardinality / fragment_.getNumInstancesForCosting());
    double totalCost = 0.0F;
    if (perInstanceBuildCardinality < 5) {
      totalCost = cardProduct * COST_COEFFICIENT_NLJ_TINY_RHS;
    } else {
      totalCost = cardProduct * COST_COEFFICIENT_NLJ;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Probe CPU cost estimate: " + totalCost
          + ", Probe Card: " + probeCardinality + ", Build Card: " + buildCardinality
          + ", Build Card Per Instance: " + perInstanceBuildCardinality);
    }
    ProcessingCost processingCost =
        ProcessingCost.basicCost(getDisplayLabel(), totalCost);
    return Pair.create(processingCost, ProcessingCost.zero());
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
            .append(Expr.getExplainString(otherJoinConjuncts_, detailLevel) + "\n");
      }
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix + "predicates: ")
            .append(Expr.getExplainString(conjuncts_, detailLevel) + "\n");
      }
      if (!runtimeFilters_.isEmpty()) {
        output.append(detailPrefix + "runtime filters: ");
        output.append(getRuntimeFilterExplainString(true, detailLevel));
      }
    }
    return output.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.NESTED_LOOP_JOIN_NODE;
    msg.join_node = joinNodeToThrift();
    msg.join_node.nested_loop_join_node = new TNestedLoopJoinNode();
    for (Expr e : otherJoinConjuncts_) {
      msg.join_node.nested_loop_join_node.addToJoin_conjuncts(e.treeToThrift());
    }
  }

  @Override
  protected String debugString() {
    return MoreObjects.toStringHelper(this)
        .addValue(super.debugString())
        .toString();
  }
}
