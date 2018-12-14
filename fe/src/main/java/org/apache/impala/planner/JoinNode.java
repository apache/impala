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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TExecNodePhase;
import org.apache.impala.thrift.TJoinDistributionMode;
import org.apache.impala.thrift.TQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Logical join operator. Subclasses correspond to implementations of the join operator
 * (e.g. hash join, nested-loop join, etc).
 */
public abstract class JoinNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(JoinNode.class);

  // Default per-instance memory requirement used if no valid stats are available.
  // TODO: Come up with a more useful heuristic (e.g., based on scanned partitions).
  protected final static long DEFAULT_PER_INSTANCE_MEM = 2L * 1024L * 1024L * 1024L;

  // Slop in percent allowed when comparing stats for the purpose of determining whether
  // an equi-join condition is a foreign/primary key join.
  protected final static double FK_PK_MAX_STATS_DELTA_PERC = 0.05;

  protected JoinOperator joinOp_;

  // Indicates if this join originates from a query block with a straight join hint.
  protected final boolean isStraightJoin_;

  // User-provided hint for the distribution mode. Set to 'NONE' if no hints were given.
  protected final DistributionMode distrModeHint_;

  protected DistributionMode distrMode_ = DistributionMode.NONE;

  // Join conjuncts. eqJoinConjuncts_ are conjuncts of the form <lhs> = <rhs>;
  // otherJoinConjuncts_ are non-equi join conjuncts. For an inner join, join conjuncts
  // are conjuncts from the ON, USING or WHERE clauses. For other join types (e.g. outer
  // and semi joins) these include only conjuncts from the ON and USING clauses.
  protected List<BinaryPredicate> eqJoinConjuncts_;
  protected List<Expr> otherJoinConjuncts_;

  // if valid, the rhs input is materialized outside of this node and is assigned
  // joinTableId_
  protected JoinTableId joinTableId_ = JoinTableId.INVALID;

  // List of equi-join conjuncts believed to be involved in a FK/PK relationship.
  // The conjuncts are grouped by the tuple ids of the joined base table refs. A conjunct
  // is only included in this list if it is of the form <SlotRef> = <SlotRef> and the
  // underlying columns and tables on both sides have stats. See getFkPkEqJoinConjuncts()
  // for more details on the FK/PK detection logic.
  // The value of this member represents three different states:
  // - null: There are eligible join conjuncts and we have high confidence that none of
  //   them represent a FK/PK relationship.
  // - non-null and empty: There are no eligible join conjuncts. We assume a FK/PK join.
  // - non-null and non-empty: There are eligible join conjuncts that could represent
  //   a FK/PK relationship.
  // Theses conjuncts are printed in the explain plan.
  protected List<EqJoinConjunctScanSlots> fkPkEqJoinConjuncts_;

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
    public static DistributionMode fromThrift(TJoinDistributionMode distrMode) {
      if (distrMode == TJoinDistributionMode.BROADCAST) return BROADCAST;
      return PARTITIONED;
    }
  }

  public JoinNode(PlanNode outer, PlanNode inner, boolean isStraightJoin,
      DistributionMode distrMode, JoinOperator joinOp,
      List<BinaryPredicate> eqJoinConjuncts, List<Expr> otherJoinConjuncts,
      String displayName) {
    super(displayName);
    Preconditions.checkNotNull(otherJoinConjuncts);
    isStraightJoin_ = isStraightJoin;
    distrModeHint_ = distrMode;
    joinOp_ = joinOp;
    children_.add(outer);
    children_.add(inner);
    eqJoinConjuncts_ = eqJoinConjuncts;
    otherJoinConjuncts_ = otherJoinConjuncts;
    computeTupleIds();
  }

  @Override
  public void computeTupleIds() {
    Preconditions.checkState(children_.size() == 2);
    clearTupleIds();
    PlanNode outer = children_.get(0);
    PlanNode inner = children_.get(1);

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

  /**
   * Returns true if the join node can be inverted. Inversions are not allowed
   * in the following cases:
   * 1. Straight join.
   * 2. The operator is a null-aware left anti-join. There is no backend support
   *    for a null-aware right anti-join because we cannot execute it efficiently.
   * 3. In the case of a distributed plan, the resulting join is a non-equi right
   *    semi-join or a non-equi right outer-join. There is no backend support.
   */
  public boolean isInvertible(boolean isLocalPlan) {
    if (isStraightJoin()) return false;
    if (joinOp_.isNullAwareLeftAntiJoin()) return false;
    if (isLocalPlan) return true;
    if (!eqJoinConjuncts_.isEmpty()) return true;
    if (joinOp_.isLeftOuterJoin()) return false;
    if (joinOp_.isLeftSemiJoin()) return false;
    return true;
  }

  public JoinOperator getJoinOp() { return joinOp_; }
  public List<BinaryPredicate> getEqJoinConjuncts() { return eqJoinConjuncts_; }
  public List<Expr> getOtherJoinConjuncts() { return otherJoinConjuncts_; }
  public boolean isStraightJoin() { return isStraightJoin_; }
  public DistributionMode getDistributionModeHint() { return distrModeHint_; }
  public DistributionMode getDistributionMode() { return distrMode_; }
  public void setDistributionMode(DistributionMode distrMode) { distrMode_ = distrMode; }
  public JoinTableId getJoinTableId() { return joinTableId_; }
  public void setJoinTableId(JoinTableId id) { joinTableId_ = id; }
  /// True if this consumes all of its right input before outputting any rows.
  abstract public boolean isBlockingJoinNode();

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    // Do not call super.init() to defer computeStats() until all conjuncts
    // have been collected.
    assignConjuncts(analyzer);
    createDefaultSmap(analyzer);
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
    otherJoinConjuncts_ = Expr.substituteList(otherJoinConjuncts_,
        getCombinedChildSmap(), analyzer, false);
  }

  /**
   * Returns the estimated cardinality of an inner or outer join.
   *
   * We estimate the cardinality based on equality join predicates of the form
   * "L.c = R.d", with L being a table from child(0) and R a table from child(1).
   * For each set of such join predicates between two tables, we try to determine whether
   * the tables might have foreign/primary key (FK/PK) relationship, and either use a
   * special FK/PK estimation or a generic estimation method. Once the estimation method
   * has been determined we compute the final cardinality based on the single most
   * selective join predicate. We do not attempt to estimate the joint selectivity of
   * multiple join predicates to avoid underestimation.
   * The FK/PK detection logic is based on the assumption that most joins are FK/PK. We
   * only use the generic estimation method if we have high confidence that there is no
   * FK/PK relationship. In the absence of relevant stats, we assume FK/PK with a join
   * selectivity of 1.
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
    Preconditions.checkState(joinOp_.isInnerJoin() || joinOp_.isOuterJoin());
    fkPkEqJoinConjuncts_ = Collections.emptyList();

    long lhsCard = getChild(0).cardinality_;
    long rhsCard = getChild(1).cardinality_;
    if (lhsCard == -1 || rhsCard == -1) {
      // Assume FK/PK with a join selectivity of 1.
      return lhsCard;
    }

    // Collect join conjuncts that are eligible to participate in cardinality estimation.
    List<EqJoinConjunctScanSlots> eqJoinConjunctSlots = new ArrayList<>();
    for (Expr eqJoinConjunct: eqJoinConjuncts_) {
      EqJoinConjunctScanSlots slots = EqJoinConjunctScanSlots.create(eqJoinConjunct);
      if (slots != null) eqJoinConjunctSlots.add(slots);
    }

    if (eqJoinConjunctSlots.isEmpty()) {
      // There are no eligible equi-join conjuncts. Optimistically assume FK/PK with a
      // join selectivity of 1.
      return lhsCard;
    }

    fkPkEqJoinConjuncts_ = getFkPkEqJoinConjuncts(eqJoinConjunctSlots);
    if (fkPkEqJoinConjuncts_ != null) {
      return getFkPkJoinCardinality(fkPkEqJoinConjuncts_, lhsCard, rhsCard);
    } else {
      return getGenericJoinCardinality(eqJoinConjunctSlots, lhsCard, rhsCard);
    }
  }

  /**
   * Returns a list of equi-join conjuncts believed to have a FK/PK relationship based on
   * whether the right-hand side might be a PK. The conjuncts are grouped by the tuple
   * ids of the joined base table refs. We prefer to include the conjuncts in the result
   * unless we have high confidence that a FK/PK relationship is not present. The
   * right-hand side columns are unlikely to form a PK if their joint NDV is less than
   * the right-hand side row count. If the joint NDV is close to or higher than the row
   * count, then it might be a PK.
   * The given list of eligible join conjuncts must be non-empty.
   */
  private List<EqJoinConjunctScanSlots> getFkPkEqJoinConjuncts(
      List<EqJoinConjunctScanSlots> eqJoinConjunctSlots) {
    Preconditions.checkState(!eqJoinConjunctSlots.isEmpty());
    Map<Pair<TupleId, TupleId>, List<EqJoinConjunctScanSlots>> scanSlotsByJoinedTids =
        EqJoinConjunctScanSlots.groupByJoinedTupleIds(eqJoinConjunctSlots);

    List<EqJoinConjunctScanSlots> result = null;
    // Iterate over all groups of conjuncts that belong to the same joined tuple id pair.
    // For each group, we compute the join NDV of the rhs slots and compare it to the
    // number of rows in the rhs table.
    for (List<EqJoinConjunctScanSlots> fkPkCandidate: scanSlotsByJoinedTids.values()) {
      double jointNdv = 1.0;
      for (EqJoinConjunctScanSlots slots: fkPkCandidate) jointNdv *= slots.rhsNdv();
      double rhsNumRows = fkPkCandidate.get(0).rhsNumRows();
      if (jointNdv >= Math.round(rhsNumRows * (1.0 - FK_PK_MAX_STATS_DELTA_PERC))) {
        // We cannot disprove that the RHS is a PK.
        if (result == null) result = new ArrayList<>();
        result.addAll(fkPkCandidate);
      }
    }
    return result;
  }

  /**
   * Returns the estimated join cardinality of a FK/PK inner or outer join based on the
   * given list of equi-join conjunct slots and the join input cardinalities.
   * The returned result is >= 0.
   * The list of join conjuncts must be non-empty and the cardinalities must be >= 0.
   */
  private long getFkPkJoinCardinality(List<EqJoinConjunctScanSlots> eqJoinConjunctSlots,
      long lhsCard, long rhsCard) {
    Preconditions.checkState(!eqJoinConjunctSlots.isEmpty());
    Preconditions.checkState(lhsCard >= 0 && rhsCard >= 0);

    long result = -1;
    for (EqJoinConjunctScanSlots slots: eqJoinConjunctSlots) {
      // Adjust the join selectivity based on the NDV ratio to avoid underestimating
      // the cardinality if the PK side has a higher NDV than the FK side.
      double ndvRatio = 1.0;
      if (slots.lhsNdv() > 0) ndvRatio = slots.rhsNdv() / slots.lhsNdv();
      double rhsSelectivity = Double.MIN_VALUE;
      if (slots.rhsNumRows() > 0) rhsSelectivity = rhsCard / slots.rhsNumRows();
      long joinCard = (long) Math.ceil(lhsCard * rhsSelectivity * ndvRatio);
      if (result == -1) {
        result = joinCard;
      } else {
        result = Math.min(result, joinCard);
      }
    }
    // FK/PK join cardinality must be <= the lhs cardinality.
    result = Math.min(result, lhsCard);
    Preconditions.checkState(result >= 0);
    return result;
  }

  /**
   * Returns the estimated join cardinality of a generic N:M inner or outer join based
   * on the given list of equi-join conjunct slots and the join input cardinalities.
   * The returned result is >= 0.
   * The list of join conjuncts must be non-empty and the cardinalities must be >= 0.
   */
  private long getGenericJoinCardinality(List<EqJoinConjunctScanSlots> eqJoinConjunctSlots,
      long lhsCard, long rhsCard) {
    Preconditions.checkState(joinOp_.isInnerJoin() || joinOp_.isOuterJoin());
    Preconditions.checkState(!eqJoinConjunctSlots.isEmpty());
    Preconditions.checkState(lhsCard >= 0 && rhsCard >= 0);

    long result = -1;
    for (EqJoinConjunctScanSlots slots: eqJoinConjunctSlots) {
      // Adjust the NDVs on both sides to account for predicates. Intuitively, the NDVs
      // should only decrease. We ignore adjustments that would lead to an increase.
      double lhsAdjNdv = slots.lhsNdv();
      if (slots.lhsNumRows() > lhsCard) lhsAdjNdv *= lhsCard / slots.lhsNumRows();
      double rhsAdjNdv = slots.rhsNdv();
      if (slots.rhsNumRows() > rhsCard) rhsAdjNdv *= rhsCard / slots.rhsNumRows();
      // A lower limit of 1 on the max Adjusted Ndv ensures we don't estimate
      // cardinality more than the max possible. This also handles the case of
      // null columns on both sides having an Ndv of zero (which would change
      // after IMPALA-7310 is fixed).
      long joinCard = Math.round((lhsCard / Math.max(1, Math.max(lhsAdjNdv, rhsAdjNdv))) *
          rhsCard);
      if (result == -1) {
        result = joinCard;
      } else {
        result = Math.min(result, joinCard);
      }
    }
    Preconditions.checkState(result >= 0);
    return result;
  }

  /**
   * Holds the source scan slots of a <SlotRef> = <SlotRef> join predicate.
   * The underlying table and column on both sides have stats.
   */
  public static final class EqJoinConjunctScanSlots {
    private final Expr eqJoinConjunct_;
    private final SlotDescriptor lhs_;
    private final SlotDescriptor rhs_;

    private EqJoinConjunctScanSlots(Expr eqJoinConjunct, SlotDescriptor lhs,
        SlotDescriptor rhs) {
      eqJoinConjunct_ = eqJoinConjunct;
      lhs_ = lhs;
      rhs_ = rhs;
    }

    // Convenience functions. They return double to avoid excessive casts in callers.
    public double lhsNdv() {
      return Math.min(lhs_.getStats().getNumDistinctValues(), lhsNumRows());
    }
    public double rhsNdv() {
      return Math.min(rhs_.getStats().getNumDistinctValues(), rhsNumRows());
    }
    public double lhsNumRows() { return lhs_.getParent().getTable().getNumRows(); }
    public double rhsNumRows() { return rhs_.getParent().getTable().getNumRows(); }

    public TupleId lhsTid() { return lhs_.getParent().getId(); }
    public TupleId rhsTid() { return rhs_.getParent().getId(); }

    /**
     * Returns a new EqJoinConjunctScanSlots for the given equi-join conjunct or null if
     * the given conjunct is not of the form <SlotRef> = <SlotRef> or if the underlying
     * table/column of at least one side is missing stats.
     */
    public static EqJoinConjunctScanSlots create(Expr eqJoinConjunct) {
      if (!Expr.IS_EQ_BINARY_PREDICATE.apply(eqJoinConjunct)) return null;
      SlotDescriptor lhsScanSlot = eqJoinConjunct.getChild(0).findSrcScanSlot();
      if (lhsScanSlot == null || !hasNumRowsAndNdvStats(lhsScanSlot)) return null;
      SlotDescriptor rhsScanSlot = eqJoinConjunct.getChild(1).findSrcScanSlot();
      if (rhsScanSlot == null || !hasNumRowsAndNdvStats(rhsScanSlot)) return null;
      return new EqJoinConjunctScanSlots(eqJoinConjunct, lhsScanSlot, rhsScanSlot);
    }

    private static boolean hasNumRowsAndNdvStats(SlotDescriptor slotDesc) {
      if (slotDesc.getColumn() == null) return false;
      if (!slotDesc.getStats().hasNumDistinctValues()) return false;
      FeTable tbl = slotDesc.getParent().getTable();
      if (tbl == null || tbl.getNumRows() == -1) return false;
      return true;
    }

    /**
     * Groups the given EqJoinConjunctScanSlots by the lhs/rhs tuple combination
     * and returns the result as a map.
     */
    public static Map<Pair<TupleId, TupleId>, List<EqJoinConjunctScanSlots>>
        groupByJoinedTupleIds(List<EqJoinConjunctScanSlots> eqJoinConjunctSlots) {
      Map<Pair<TupleId, TupleId>, List<EqJoinConjunctScanSlots>> scanSlotsByJoinedTids =
          new LinkedHashMap<>();
      for (EqJoinConjunctScanSlots slots: eqJoinConjunctSlots) {
        Pair<TupleId, TupleId> tids = Pair.create(slots.lhsTid(), slots.rhsTid());
        List<EqJoinConjunctScanSlots> scanSlots = scanSlotsByJoinedTids.get(tids);
        if (scanSlots == null) {
          scanSlots = new ArrayList<>();
          scanSlotsByJoinedTids.put(tids, scanSlots);
        }
        scanSlots.add(slots);
      }
      return scanSlotsByJoinedTids;
    }

    @Override
    public String toString() { return eqJoinConjunct_.toSql(); }
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
    } else {
      Preconditions.checkState(joinOp_.isCrossJoin());
      long leftCard = getChild(0).cardinality_;
      long rightCard = getChild(1).cardinality_;
      if (leftCard != -1 && rightCard != -1) {
        cardinality_ = checkedMultiply(leftCard, rightCard);
      }
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
          long cardinalitySum = checkedAdd(leftCard, rightCard);
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
          cardinality_ = checkedMultiply(getChild(0).cardinality_,
              getChild(1).cardinality_);
        }
        break;
      }
    }
    cardinality_ = capCardinalityAtLimit(cardinality_);
    Preconditions.checkState(hasValidStats());
    if (LOG.isTraceEnabled()) {
      LOG.trace("stats Join: cardinality=" + Long.toString(cardinality_));
    }
  }

  /**
   * Inverts the join op, swaps our children, and swaps the children
   * of all eqJoinConjuncts_. All modifications are in place.
   */
  public void invertJoin() {
    joinOp_ = joinOp_.invert();
    Collections.swap(children_, 0, 1);
    for (BinaryPredicate p: eqJoinConjuncts_) p.reverse();
  }

  @Override
  protected String getDisplayLabelDetail() {
    StringBuilder output = new StringBuilder(joinOp_.toString());
    if (distrMode_ != DistributionMode.NONE) output.append(", " + distrMode_.toString());
    return output.toString();
  }

  protected void orderJoinConjunctsByCost() {
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    eqJoinConjuncts_ = orderConjunctsByCost(eqJoinConjuncts_);
    otherJoinConjuncts_ = orderConjunctsByCost(otherJoinConjuncts_);
  }

  @Override
  public ExecPhaseResourceProfiles computeTreeResourceProfiles(
      TQueryOptions queryOptions) {
    Preconditions.checkState(isBlockingJoinNode(), "Only blocking join nodes supported");

    ExecPhaseResourceProfiles buildSideProfile =
        getChild(1).computeTreeResourceProfiles(queryOptions);
    ExecPhaseResourceProfiles probeSideProfile =
        getChild(0).computeTreeResourceProfiles(queryOptions);

    // The peak resource consumption of the build phase is either during the Open() of
    // the build side or while we're doing the join build and calling GetNext() on the
    // build side.
    ResourceProfile buildPhaseProfile = buildSideProfile.duringOpenProfile.max(
        buildSideProfile.postOpenProfile.sum(nodeResourceProfile_));

    ResourceProfile finishedBuildProfile = nodeResourceProfile_;
    if (this instanceof NestedLoopJoinNode) {
      // These exec node implementations may hold references into the build side, which
      // prevents closing of the build side in a timely manner. This means we have to
      // count the post-open resource consumption of the build side in the same way as
      // the other in-memory data structures.
      // TODO: IMPALA-4179: remove this workaround
      finishedBuildProfile = buildSideProfile.postOpenProfile.sum(nodeResourceProfile_);
    }

    // Peak resource consumption of this subtree during Open().
    ResourceProfile duringOpenProfile;
    if (queryOptions.getMt_dop() == 0) {
      // The build and probe side can be open and therefore consume resources
      // simultaneously when mt_dop = 0 because of the async build thread.
      duringOpenProfile = buildPhaseProfile.sum(probeSideProfile.duringOpenProfile);
    } else {
      // Open() of the probe side happens after the build completes.
      duringOpenProfile = buildPhaseProfile.max(
          finishedBuildProfile.sum(probeSideProfile.duringOpenProfile));
    }

    // After Open(), the probe side remains open and the join build remain in memory.
    ResourceProfile probePhaseProfile =
        finishedBuildProfile.sum(probeSideProfile.postOpenProfile);
    return new ExecPhaseResourceProfiles(duringOpenProfile, probePhaseProfile);
  }

  @Override
  public void computePipelineMembership() {
    children_.get(0).computePipelineMembership();
    children_.get(1).computePipelineMembership();
    pipelines_ = new ArrayList<>();
    for (PipelineMembership probePipeline : children_.get(0).getPipelines()) {
      if (probePipeline.getPhase() == TExecNodePhase.GETNEXT) {
          pipelines_.add(new PipelineMembership(
              probePipeline.getId(), probePipeline.getHeight() + 1, TExecNodePhase.GETNEXT));
      }
    }
    for (PipelineMembership buildPipeline : children_.get(1).getPipelines()) {
      if (buildPipeline.getPhase() == TExecNodePhase.GETNEXT) {
        pipelines_.add(new PipelineMembership(
            buildPipeline.getId(), buildPipeline.getHeight() + 1, TExecNodePhase.OPEN));
      }
    }
  }
}
