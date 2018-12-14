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
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TJoinBuildSink;
import org.apache.impala.thrift.TQueryOptions;
import com.google.common.base.Preconditions;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

/**
 * Sink to materialize the build side of a join.
 */
public class JoinBuildSink extends DataSink {
  // id of join's build-side table assigned during planning
  private final JoinTableId joinTableId_;

  private final List<Expr> buildExprs_ = new ArrayList<>();

  /**
   * Creates sink for build side of 'joinNode' (extracts buildExprs_ from joinNode).
   */
  public JoinBuildSink(JoinTableId joinTableId, JoinNode joinNode) {
    Preconditions.checkState(joinTableId.isValid());
    joinTableId_ = joinTableId;
    Preconditions.checkNotNull(joinNode);
    Preconditions.checkState(joinNode instanceof JoinNode);
    if (!(joinNode instanceof HashJoinNode)) return;
    for (Expr eqJoinConjunct: joinNode.getEqJoinConjuncts()) {
      BinaryPredicate p = (BinaryPredicate) eqJoinConjunct;
      // by convention the build exprs are the rhs of the join conjuncts
      buildExprs_.add(p.getChild(1).clone());
    }
  }

  public JoinTableId getJoinTableId() { return joinTableId_; }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    TJoinBuildSink tBuildSink = new TJoinBuildSink();
    tBuildSink.setJoin_table_id(joinTableId_.asInt());
    for (Expr buildExpr: buildExprs_) {
      tBuildSink.addToBuild_exprs(buildExpr.treeToThrift());
    }
    tsink.setJoin_build_sink(tBuildSink);
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.JOIN_BUILD_SINK;
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
    }
  }

  @Override
  protected String getLabel() {
    return "JOIN BUILD";
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    // The memory consumption is counted against the join PlanNode.
    resourceProfile_ = ResourceProfile.noReservation(0);
  }
}
