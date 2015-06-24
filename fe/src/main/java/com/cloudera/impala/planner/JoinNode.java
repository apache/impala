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

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.JoinOperator;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;

/**
 * Logical join operator. Subclasses correspond to implementations of the join operator
 * (e.g. hash join, nested-loop join, etc).
 */
public abstract class JoinNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(JoinNode.class);

  // Default per-host memory requirement used if no valid stats are available.
  // TODO: Come up with a more useful heuristic (e.g., based on scanned partitions).
  protected final static long DEFAULT_PER_HOST_MEM = 2L * 1024L * 1024L * 1024L;

  // Default join selectivity when we cannot come up with a better estimate.
  private final static double DEFAULT_JOIN_SELECTIVITY = 0.1;

  protected JoinOperator joinOp_;

  // User-provided hint for the distribution mode. Set to 'NONE' if no hints were given.
  protected final DistributionMode distrModeHint_;

  protected DistributionMode distrMode_ = DistributionMode.NONE;

  // Join conjuncts. eqJoinConjuncts_ are conjuncts of the form <lhs> = <rhs>;
  // otherJoinConjuncts_ are non-equi join conjuncts. For an inner join, join conjuncts
  // are conjuncts from the ON, USING or WHERE clauses. For other join types (e.g. outer
  // and semi joins) these include only conjuncts from the ON and USING clauses.
  protected List<BinaryPredicate> eqJoinConjuncts_;
  protected List<Expr> otherJoinConjuncts_;

  public enum DistributionMode {
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

  public JoinNode(PlanNode outer, PlanNode inner, DistributionMode distrMode,
      JoinOperator joinOp, List<BinaryPredicate> eqJoinConjuncts,
      List<Expr> otherJoinConjuncts, String displayName) {
    super(displayName);
    Preconditions.checkNotNull(otherJoinConjuncts);
    joinOp_ = joinOp;
    distrModeHint_ = distrMode;

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
    eqJoinConjuncts_ = eqJoinConjuncts;
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
  public List<BinaryPredicate> getEqJoinConjuncts() { return eqJoinConjuncts_; }
  public List<Expr> getOtherJoinConjuncts() { return otherJoinConjuncts_; }
  public DistributionMode getDistributionModeHint() { return distrModeHint_; }
  public DistributionMode getDistributionMode() { return distrMode_; }
  public void setDistributionMode(DistributionMode distrMode) { distrMode_ = distrMode; }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    super.init(analyzer);
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
    otherJoinConjuncts_ = Expr.substituteList(otherJoinConjuncts_,
        getCombinedChildSmap(), analyzer, false);
  }

  /**
   * Returns the estimated cardinality of an inner or outer join.
   * For a join between child(0) and child(1), we look for join conditions "L.c = R.d"
   * (with L being from child(0) and R from child(1)) and use as the cardinality
   * estimate the maximum of
   *   |child(0)| * |R| / NDV(R.d) * |child(1)| / |R|
   * across all suitable join conditions, which simplifies to
   *   |child(0)| * |child(1)| / NDV(R.d)
   * The reasoning is that
   * - each row in child(0) joins with |R| / NDV(R.d) rows in R
   * - each row in R is 'present' in |child(1)| / |R| rows in child(1)
   *
   * This handles the very frequent case of a fact table/dimension table join
   * (aka foreign key/primary key join) if the primary key is a single column, with
   * possible additional predicates against the dimension table. An example:
   * FROM FactTbl F JOIN Customers C ON (F.cust_id = C.id) ... WHERE C.region = 'US'
   * - if there are 5 regions, the selectivity of "C.region = 'US'" would be 0.2
   *   and the output cardinality of the Customers scan would be 0.2 * # rows in
   *   Customers
   * - # rows in Customers == # of distinct values for Customers.id
   * - the output cardinality of the join would be F.cardinality * 0.2
   */
  private long getJoinCardinality(Analyzer analyzer) {
    Preconditions.checkState(joinOp_.isInnerJoin() || joinOp_.isOuterJoin());
    long maxNumDistinct = 0;
    for (Expr eqJoinPredicate: eqJoinConjuncts_) {
      if (eqJoinPredicate.getChild(0).unwrapSlotRef(false) == null) continue;
      SlotRef rhsSlotRef = eqJoinPredicate.getChild(1).unwrapSlotRef(false);
      if (rhsSlotRef == null) continue;
      SlotDescriptor slotDesc = rhsSlotRef.getDesc();
      if (slotDesc == null) continue;
      ColumnStats stats = slotDesc.getStats();
      if (!stats.hasNumDistinctValues()) continue;
      long numDistinct = stats.getNumDistinctValues();
      Table rhsTbl = slotDesc.getParent().getTable();
      if (rhsTbl != null && rhsTbl.getNumRows() != -1) {
        // we can't have more distinct values than rows in the table, even though
        // the metastore stats may think so
        LOG.debug("#distinct=" + numDistinct + " #rows="
            + Long.toString(rhsTbl.getNumRows()));
        numDistinct = Math.min(numDistinct, rhsTbl.getNumRows());
      }
      if (getChild(1).cardinality_ != -1 && numDistinct != -1) {
        // The number of distinct values of a slot cannot exceed the cardinality_
        // of the plan node the slot is coming from.
        numDistinct = Math.min(numDistinct, getChild(1).cardinality_);
      }
      maxNumDistinct = Math.max(maxNumDistinct, numDistinct);
      LOG.debug("min slotref=" + rhsSlotRef.toSql() + " #distinct="
          + Long.toString(numDistinct));
    }

    long result = -1;
    if (maxNumDistinct == 0) {
      // if we didn't find any suitable join predicates or don't have stats
      // on the relevant columns, we very optimistically assume we're doing an
      // FK/PK join (which doesn't alter the cardinality of the left-hand side)
      result = getChild(0).cardinality_;
      if (eqJoinConjuncts_.isEmpty()) {
        // No equi-join conjuncts.
        result = multiplyCardinalities(getChild(0).cardinality_,
            getChild(1).cardinality_);
        if (!otherJoinConjuncts_.isEmpty() || !conjuncts_.isEmpty()) {
          // We estimate the cardinality as 10% of the multiplied cardinalities
          // from the left and right hand side.
          result = Math.round(result * DEFAULT_JOIN_SELECTIVITY);
        }
        result = result < 0 ? -1 : result;
      }
    } else if (getChild(0).cardinality_ != -1 && getChild(1).cardinality_ != -1) {
      result = multiplyCardinalities(getChild(0).cardinality_,
          getChild(1).cardinality_);
      result = Math.round((double)result / (double) maxNumDistinct);
    }
    return result;
  }

  /**
   * Returns the estimated cardinality of a semi join node.
   * For a left semi join between child(0) and child(1), we look for equality join
   * conditions "L.c = R.d" (with L being from child(0) and R from child(1)) and use as
   * the cardinality estimate the minimum of
   *   |child(0)| * Min(NDV(L.c), NDV(R.d)) / NDV(L.c)
   * over all suitable join conditions. The reasoning is that:
   * - each row in child(0) is returned at most once
   * - the probability of a row in child(0) having a match in R is
   *   Min(NDV(L.c), NDV(R.d)) / NDV(L.c)
   *
   * For a left anti join we estimate the cardinality as the minimum of:
   *   |L| * Max(NDV(L.c) - NDV(R.d), NDV(L.c)) / NDV(L.c)
   * over all suitable join conditions. The reasoning is that:
   * - each row in child(0) is returned at most once
   * - if NDV(L.c) > NDV(R.d) then the probability of row in L having a match
   *   in child(1) is (NDV(L.c) - NDV(R.d)) / NDV(L.c)
   * - otherwise, we conservatively use |L| to avoid underestimation
   *
   * We analogously estimate the cardinality for right semi/anti joins, and treat the
   * null-aware anti join like a regular anti join
   *
   * TODO: In order to take into account additional conjuncts in the child child subtrees
   * adjust NDV(L.c) by |child(0)| / |L| and the NDV(R.d) by |child(1)| / |R|.
   * The adjustment is currently too dangerous due to the other planner bugs compounding
   * to bad plans causing perf regressions (IMPALA-976).
   */
  private long getSemiJoinCardinality() {
    Preconditions.checkState(joinOp_.isSemiJoin());

    // Return -1 if the cardinality of the returned side is unknown.
    long cardinality;
    if (joinOp_ == JoinOperator.RIGHT_SEMI_JOIN
        || joinOp_ == JoinOperator.RIGHT_ANTI_JOIN) {
      if (getChild(1).cardinality_ == -1) return -1;
      cardinality = getChild(1).cardinality_;
    } else {
      if (getChild(0).cardinality_ == -1) return -1;
      cardinality = getChild(0).cardinality_;
    }
    double minSelectivity = 1.0;
    for (Expr eqJoinPredicate: eqJoinConjuncts_) {
      long lhsNdv = getNdv(eqJoinPredicate.getChild(0));
      lhsNdv = Math.min(lhsNdv, getChild(0).cardinality_);
      long rhsNdv = getNdv(eqJoinPredicate.getChild(1));
      rhsNdv = Math.min(rhsNdv, getChild(1).cardinality_);

      // Skip conjuncts with unknown NDV on either side.
      if (lhsNdv == -1 || rhsNdv == -1) continue;

      double selectivity = 1.0;
      switch (joinOp_) {
        case LEFT_SEMI_JOIN: {
          selectivity = (double) Math.min(lhsNdv, rhsNdv) / (double) (lhsNdv);
          break;
        }
        case RIGHT_SEMI_JOIN: {
          selectivity = (double) Math.min(lhsNdv, rhsNdv) / (double) (rhsNdv);
          break;
        }
        case LEFT_ANTI_JOIN:
        case NULL_AWARE_LEFT_ANTI_JOIN: {
          selectivity = (double) Math.max(lhsNdv - rhsNdv, lhsNdv) / (double) lhsNdv;
          break;
        }
        case RIGHT_ANTI_JOIN: {
          selectivity = (double) Math.max(rhsNdv - lhsNdv, rhsNdv) / (double) rhsNdv;
          break;
        }
        default: Preconditions.checkState(false);
      }
      minSelectivity = Math.min(minSelectivity, selectivity);
    }

    Preconditions.checkState(cardinality != -1);
    return Math.round(cardinality * minSelectivity);
  }

  /**
   * Unwraps the SlotRef in expr and returns the NDVs of it.
   * Returns -1 if the NDVs are unknown or if expr is not a SlotRef.
   */
  private long getNdv(Expr expr) {
    SlotRef slotRef = expr.unwrapSlotRef(false);
    if (slotRef == null) return -1;
    SlotDescriptor slotDesc = slotRef.getDesc();
    if (slotDesc == null) return -1;
    ColumnStats stats = slotDesc.getStats();
    if (!stats.hasNumDistinctValues()) return -1;
    return stats.getNumDistinctValues();
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    if (joinOp_.isSemiJoin()) {
      cardinality_ = getSemiJoinCardinality();
    } else if (joinOp_.isInnerJoin() || joinOp_.isOuterJoin()){
      cardinality_ = getJoinCardinality(analyzer);
    }

    // Impose lower/upper bounds on the cardinality based on the join type.
    long leftCard = getChild(0).cardinality_;
    long rightCard = getChild(1).cardinality_;
    switch (joinOp_) {
      case LEFT_SEMI_JOIN: {
        if (leftCard != -1) {
          cardinality_ = Math.min(leftCard, cardinality_);
        }
        break;
      }
      case RIGHT_SEMI_JOIN: {
        if (rightCard != -1) {
          cardinality_ = Math.min(rightCard, cardinality_);
        }
        break;
      }
      case LEFT_OUTER_JOIN: {
        if (leftCard != -1) {
          cardinality_ = Math.max(leftCard, cardinality_);
        }
        break;
      }
      case RIGHT_OUTER_JOIN: {
        if (rightCard != -1) {
          cardinality_ = Math.max(rightCard, cardinality_);
        }
        break;
      }
      case FULL_OUTER_JOIN: {
        if (leftCard != -1 && rightCard != -1) {
          long cardinalitySum = addCardinalities(leftCard, rightCard);
          cardinality_ = Math.max(cardinalitySum, cardinality_);
        }
        break;
      }
      case LEFT_ANTI_JOIN:
      case NULL_AWARE_LEFT_ANTI_JOIN: {
        if (leftCard != -1) {
          cardinality_ = Math.min(leftCard, cardinality_);
        }
        break;
      }
      case RIGHT_ANTI_JOIN: {
        if (rightCard != -1) {
          cardinality_ = Math.min(rightCard, cardinality_);
        }
        break;
      }
      case CROSS_JOIN: {
        if (getChild(0).cardinality_ == -1 || getChild(1).cardinality_ == -1) {
          cardinality_ = -1;
        } else {
          cardinality_ = multiplyCardinalities(getChild(0).cardinality_,
              getChild(1).cardinality_);
        }
        break;
      }
    }

    Preconditions.checkState(hasValidStats());
    LOG.debug("stats Join: cardinality=" + Long.toString(cardinality_));
  }

  @Override
  protected String getDisplayLabelDetail() {
    StringBuilder output = new StringBuilder(joinOp_.toString());
    if (distrMode_ != DistributionMode.NONE) output.append(", " + distrMode_.toString());
    return output.toString();
  }
}
