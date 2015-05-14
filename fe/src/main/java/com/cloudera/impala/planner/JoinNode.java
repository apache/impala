// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.JoinOperator;
import com.cloudera.impala.analysis.TableRef;
import com.google.common.base.Preconditions;

/**
 * Superclass of all join nodes.
 */
public abstract class JoinNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(JoinNode.class);

  // Default per-host memory requirement used if no valid stats are available.
  // TODO: Come up with a more useful heuristic (e.g., based on scanned partitions).
  protected final static long DEFAULT_PER_HOST_MEM = 2L * 1024L * 1024L * 1024L;

  protected final JoinOperator joinOp_;

  // User-provided hint for the distribution mode. Set to 'NONE' if no hints were given.
  protected final DistributionMode distrModeHint_;

  protected DistributionMode distrMode_ = DistributionMode.NONE;

  // Join conjuncts from the ON clause that aren't equi-join predicates.
  protected List<Expr> otherJoinConjuncts_;

  enum DistributionMode {
    NONE("NONE"),
    BROADCAST("BROADCAST"),
    PARTITIONED("PARTITIONED");

    private final String description_;

    private DistributionMode(String description) {
      description_ = description;
    }

    @Override
    public String toString() { return description_; }
  }

  public JoinNode(PlanNode outer, PlanNode inner, TableRef tblRef,
      List<Expr> otherJoinConjuncts, String displayName) {
    super(displayName);
    Preconditions.checkNotNull(otherJoinConjuncts);
    joinOp_ = tblRef.getJoinOp();

    // Set distribution mode hint.
    if (tblRef.isBroadcastJoin()) {
      distrModeHint_ = DistributionMode.BROADCAST;
    } else if (tblRef.isPartitionedJoin()) {
      distrModeHint_ = DistributionMode.PARTITIONED;
    } else {
      distrModeHint_ = DistributionMode.NONE;
    }

    // Only retain the non-semi-joined tuples of the inputs.
    switch (joinOp_) {
      case LEFT_ANTI_JOIN:
      case LEFT_SEMI_JOIN:
      case NULL_AWARE_LEFT_ANTI_JOIN: {
        tupleIds_.addAll(outer.getTupleIds());
        break;
      }
      case RIGHT_ANTI_JOIN:
      case RIGHT_SEMI_JOIN: {
        tupleIds_.addAll(inner.getTupleIds());
        break;
      }
      default: {
        tupleIds_.addAll(outer.getTupleIds());
        tupleIds_.addAll(inner.getTupleIds());
        break;
      }
    }
    tblRefIds_.addAll(outer.getTblRefIds());
    tblRefIds_.addAll(inner.getTblRefIds());

    otherJoinConjuncts_ = otherJoinConjuncts;
    children_.add(outer);
    children_.add(inner);

    // Inherits all the nullable tuple from the children
    // Mark tuples that form the "nullable" side of the outer join as nullable.
    nullableTupleIds_.addAll(inner.getNullableTupleIds());
    nullableTupleIds_.addAll(outer.getNullableTupleIds());
    if (joinOp_.equals(JoinOperator.FULL_OUTER_JOIN)) {
      nullableTupleIds_.addAll(outer.getTupleIds());
      nullableTupleIds_.addAll(inner.getTupleIds());
    } else if (joinOp_.equals(JoinOperator.LEFT_OUTER_JOIN)) {
      nullableTupleIds_.addAll(inner.getTupleIds());
    } else if (joinOp_.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
      nullableTupleIds_.addAll(outer.getTupleIds());
    }
  }

  public JoinOperator getJoinOp() { return joinOp_; }
  public DistributionMode getDistributionModeHint() { return distrModeHint_; }
  public DistributionMode getDistributionMode() { return distrMode_; }
  public void setDistributionMode(DistributionMode distrMode) { distrMode_ = distrMode; }
}
