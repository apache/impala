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

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.planner.RuntimeFilterGenerator.RuntimeFilter;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TJoinBuildSink;
import org.apache.impala.thrift.TQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

/**
 * Sink to materialize the build side of a join.
 */
public class JoinBuildSink extends DataSink {
  private final static Logger LOG = LoggerFactory.getLogger(JoinBuildSink.class);

  // id of join's build-side table assigned during planning
  private final JoinTableId joinTableId_;

  // Reference to the join node that consumes the build side.
  private final JoinNode joinNode_;

  private final List<Expr> buildExprs_ = new ArrayList<>();

  private final List<RuntimeFilter> runtimeFilters_ = new ArrayList<>();

  /**
   * Creates sink for build side of 'joinNode' (extracts buildExprs_ from joinNode).
   */
  public JoinBuildSink(JoinTableId joinTableId, JoinNode joinNode) {
    Preconditions.checkState(joinTableId.isValid());
    joinTableId_ = joinTableId;
    joinNode_ = joinNode;
    Preconditions.checkNotNull(joinNode);
    Preconditions.checkState(joinNode instanceof JoinNode);
    if (joinNode instanceof HashJoinNode) {
      for (Expr eqJoinConjunct: joinNode.getEqJoinConjuncts()) {
        BinaryPredicate p = (BinaryPredicate) eqJoinConjunct;
        // by convention the build exprs are the rhs of the join conjuncts
        buildExprs_.add(p.getChild(1).clone());
      }
    }
    runtimeFilters_.addAll(joinNode.getRuntimeFilters());
  }

  public JoinTableId getJoinTableId() { return joinTableId_; }
  @Override
  public List<RuntimeFilter> getRuntimeFilters() { return runtimeFilters_; }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    TJoinBuildSink tBuildSink = new TJoinBuildSink();
    tBuildSink.setDest_node_id(joinNode_.getId().asInt());
    tBuildSink.setJoin_op(joinNode_.getJoinOp().toThrift());
    if (joinNode_ instanceof HashJoinNode) {
      tBuildSink.setEq_join_conjuncts(
          ((HashJoinNode)joinNode_).getThriftEquiJoinConjuncts());
      tBuildSink.setHash_seed(joinNode_.getFragment().getHashSeed());
    }
    if (joinNode_ instanceof IcebergDeleteNode) {
      tBuildSink.setEq_join_conjuncts(
          ((IcebergDeleteNode) joinNode_).getThriftEquiJoinConjuncts());
      tBuildSink.setHash_seed(joinNode_.getFragment().getHashSeed());
    }
    for (RuntimeFilter filter : runtimeFilters_) {
      tBuildSink.addToRuntime_filters(filter.toThrift());
    }
    tBuildSink.setShare_build(joinNode_.canShareBuild());
    tsink.setJoin_build_sink(tBuildSink);
  }

  @Override
  protected TDataSinkType getSinkType() {
    if (joinNode_ instanceof HashJoinNode) {
      return TDataSinkType.HASH_JOIN_BUILDER;
    } else if (joinNode_ instanceof NestedLoopJoinNode) {
      return TDataSinkType.NESTED_LOOP_JOIN_BUILDER;
    } else {
      Preconditions.checkState(joinNode_ instanceof IcebergDeleteNode);
      return TDataSinkType.ICEBERG_DELETE_BUILDER;
    }
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel detailLevel, StringBuilder output) {
    output.append(String.format("%s%s\n", prefix, "JOIN BUILD"));
    if (detailLevel.ordinal() > TExplainLevel.MINIMAL.ordinal()) {
      output.append(
          detailPrefix + "join-table-id=" + joinTableId_.toString()
            + " plan-id=" + fragment_.getPlanId().toString()
            + " cohort-id=" + fragment_.getCohortId().toString() + "\n");
      if (!buildExprs_.isEmpty()) {
        output.append(detailPrefix + "build expressions: ")
            .append(Expr.toSql(buildExprs_, DEFAULT) + "\n");
      }
      if (!runtimeFilters_.isEmpty()) {
        output.append(detailPrefix + "runtime filters: ");
        output.append(PlanNode.getRuntimeFilterExplainString(
            runtimeFilters_, true, joinNode_.getId(), detailLevel));
      }
    }
  }

  /**
   * Return an estimate of the number of nodes the fragment with this sink will run
   * on. This is based on the number of nodes of the join node, since they are
   * co-located.
   */
  public int getNumNodes() {
    return joinNode_.getFragment().getNumNodes();
  }

  /**
   * Return an estimate of the number of instances the fragment with this sink will run
   * on. This is based on the number of instances or nodes of the join node, since they
   * are co-located, but the build may be shared.
   */
  public int getNumInstances() {
    return joinNode_.canShareBuild() ? joinNode_.getFragment().getNumNodes() :
                                       joinNode_.getFragment().getNumInstances();
  }

  public boolean isShared() { return joinNode_.canShareBuild(); }

  @Override
  protected String getLabel() {
    return "JOIN BUILD";
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    // The processing cost to export rows.
    processingCost_ = joinNode_.computeJoinProcessingCost().second;
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    resourceProfile_ = joinNode_.computeJoinResourceProfile(queryOptions).second;
  }

  @Override
  public void collectExprs(List<Expr> exprs) {
    exprs.addAll(buildExprs_);
  }

  @Override
  public void computeRowConsumptionAndProductionToCost() {
    super.computeRowConsumptionAndProductionToCost();
    if (isShared()) {
      fragment_.setFixedInstanceCount(getNumInstances());
    }
  }
}
