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
import com.cloudera.impala.common.ImpalaException;
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

  // Slop in percent allowed when comparing stats for the purpose of determining whether
  // an equi-join condition is a foreign/primary key join.
  protected final static double FK_PK_MAX_STATS_DELTA_PERC = 0.05;

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
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
    otherJoinConjuncts_ = Expr.substituteList(otherJoinConjuncts_,
        getCombinedChildSmap(), analyzer, false);
  }

  /**
   * Returns the estimated cardinality of an inner or outer join.
   *
   * We estimate the cardinality based on equality join predicates of the form
   * "L.c = R.d", with L being a table from child(0) and R a table from child(1).
   * For each such join predicate we try to determine whether it is a foreign/primary
   * key (FK/PK) join condition, and either use a special FK/PK estimation or a generic
   * estimation method. We maintain the minimum cardinality for each method separately,
   * and finally return in order of preference:
   * - the FK/PK estimate, if there was at least one FP/PK predicate
   * - the generic estimate, if there was at least one predicate with sufficient stats
   * - otherwise, we optimistically assume a FK/PK join with a join selectivity of 1,
   *   and return |child(0)|
   *
   * FK/PK estimation:
   * cardinality = |child(0)| * (|child(1)| / |R|) * (NDV(R.d) / NDV(L.c))
   * - the cardinality of a FK/PK must be <= |child(0)|
   * - |child(1)| / |R| captures the reduction in join cardinality due to
   *   predicates on the PK side
   * - NDV(R.d) / NDV(L.c) adjusts the join cardinality to avoid underestimation
   *   due to an independence assumption if the PK side has a higher NDV than the FK
   *   side. The rationale is that rows filtered from the PK side do not necessarily
   *   have a match on the FK side, and therefore would not affect the join cardinality.
   *   TODO: Revisit this pessimistic adjustment that tends to overestimate.
   *
   * Generic estimation:
   * cardinality = |child(0)| * |child(1)| / max(NDV(L.c), NDV(R.d))
   * - case A: NDV(L.c) <= NDV(R.d)
   *   every row from child(0) joins with |child(1)| / NDV(R.d) rows
   * - case B: NDV(L.c) > NDV(R.d)
   *   every row from child(1) joins with |child(0)| / NDV(L.c) rows
   * - we adjust the NDVs from both sides to account for predicates that may
   *   might have reduce the cardinality and NDVs
   */
  private long getJoinCardinality(Analyzer analyzer) {
    Preconditions.checkState(
        joinOp_ == JoinOperator.INNER_JOIN || joinOp_.isOuterJoin());

    long lhsCard = getChild(0).cardinality_;
    long rhsCard = getChild(1).cardinality_;
    if (lhsCard == -1 || rhsCard == -1) return -1;

    // Minimum of estimated join cardinalities for FK/PK join conditions.
    long fkPkJoinCard = -1;
    // Minimum of estimated join cardinalities for other join conditions.
    long genericJoinCard = -1;
    for (Expr eqJoinConjunct: eqJoinConjuncts_) {
      SlotStats lhsStats = SlotStats.create(eqJoinConjunct.getChild(0));
      SlotStats rhsStats = SlotStats.create(eqJoinConjunct.getChild(1));
      // Ignore the equi-join conjunct if we have no relevant table or column stats.
      if (lhsStats == null || rhsStats == null) continue;

      // We assume a FK/PK join based on the following intuitions:
      // 1. NDV(L.c) <= NDV(R.d)
      //    The reasoning is that a FK/PK join is unlikely if the foreign key
      //    side has a higher NDV than the primary key side. We may miss true
      //    FK/PK joins due to inaccurate and/or stale stats.
      // 2. R.d is probably a primary key.
      //    Requires that NDV(R.d) is very close to |R|.
      // The idea is that, by default, we assume that every join is a FK/PK join unless
      // we have compelling evidence that suggests otherwise, so by using || we give the
      // FK/PK assumption more chances to succeed.
      if (lhsStats.ndv <= rhsStats.ndv * (1.0 + FK_PK_MAX_STATS_DELTA_PERC) ||
          Math.abs(rhsStats.numRows - rhsStats.ndv) / (double) rhsStats.numRows
            <= FK_PK_MAX_STATS_DELTA_PERC) {
        // Adjust the join selectivity based on the NDV ratio to avoid underestimating
        // the cardinality if the PK side has a higher NDV than the FK side.
        double ndvRatio = (double) rhsStats.ndv / (double) lhsStats.ndv;
        double rhsSelectivity = (double) rhsCard / (double) rhsStats.numRows;
        long joinCard = (long) Math.ceil(lhsCard * rhsSelectivity * ndvRatio);
        // FK/PK join cardinality must be <= the lhs cardinality.
        joinCard = Math.min(lhsCard, joinCard);
        if (fkPkJoinCard == -1) {
          fkPkJoinCard = joinCard;
        } else {
          fkPkJoinCard = Math.min(fkPkJoinCard, joinCard);
        }
      } else {
        // Adjust the NDVs on both sides to account for predicates. Intuitively, the NDVs
        // should only decrease, so we bail if the adjustment would lead to an increase.
        // TODO: Adjust the NDVs more systematically throughout the plan tree to
        // get a more accurate NDV at this plan node.
        if (lhsCard > lhsStats.numRows || rhsCard > rhsStats.numRows) continue;
        double lhsAdjNdv = lhsStats.ndv * ((double)lhsCard / lhsStats.numRows);
        double rhsAdjNdv = rhsStats.ndv * ((double)rhsCard / rhsStats.numRows);
        // Generic join cardinality estimation.
        long joinCard = (long) Math.ceil(
            (lhsCard / Math.max(lhsAdjNdv, rhsAdjNdv)) * rhsCard);
        if (genericJoinCard == -1) {
          genericJoinCard = joinCard;
        } else {
          genericJoinCard = Math.min(genericJoinCard, joinCard);
        }
      }
    }

    if (fkPkJoinCard != -1) {
      return fkPkJoinCard;
    } else if (genericJoinCard != -1) {
      return genericJoinCard;
    } else {
      // Optimistic FK/PK assumption with join selectivity of 1.
      return lhsCard;
    }
  }

  /**
   * Class combining column and table stats for a particular slot. Contains the NDV
   * for the slot and the number of rows in the originating table.
   */
  private static class SlotStats {
    // Number of distinct values of the slot.
    public final long ndv;
    // Number of rows in the originating table.
    public final long numRows;

    public SlotStats(long ndv, long numRows) {
      // Cap NDV at num rows of the table.
      this.ndv = Math.min(ndv, numRows);
      this.numRows = numRows;
    }

    /**
     * Returns a new SlotStats object from the given expr that is guaranteed
     * to have valid stats.
     * Returns null if 'e' is not a SlotRef or a cast SlotRef, or if there are no
     * valid table/column stats for 'e'.
     */
    public static SlotStats create(Expr e) {
      // We need both the table and column stats, but 'e' might not directly reference
      // a scan slot, e.g., if 'e' references a grouping slot of an agg. So we look for
      // that source scan slot, traversing through materialization points if necessary.
      SlotDescriptor slotDesc = e.findSrcScanSlot();
      if (slotDesc == null) return null;
      Table table = slotDesc.getParent().getTable();
      if (table == null || table.getNumRows() == -1) return null;
      if (!slotDesc.getStats().hasNumDistinctValues()) return null;
      return new SlotStats(
          slotDesc.getStats().getNumDistinctValues(), table.getNumRows());
    }
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
