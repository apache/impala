// Copyright 2012 Cloudera Inc.
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
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.JoinOperator;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TEqJoinCondition;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.THashJoinNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TQueryOptions;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Hash join between left child (outer) and right child (inner). One child must be the
 * plan generated for a table ref. Typically, that is the right child, but due to join
 * inversion (for outer/semi/cross joins) it could also be the left child.
 */
public class HashJoinNode extends JoinNode {
  private final static Logger LOG = LoggerFactory.getLogger(HashJoinNode.class);

  // conjuncts_ of the form "<lhs> = <rhs>"
  private List<BinaryPredicate> eqJoinConjuncts_;

  // If true, this node can add filters for the probe side that can be generated
  // after reading the build side. This can be very helpful if the join is selective and
  // there are few build rows.
  private boolean addProbeFilters_;

  public HashJoinNode(
      PlanNode outer, PlanNode inner, TableRef tblRef,
      List<BinaryPredicate> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
    super(outer, inner, tblRef, otherJoinConjuncts, "HASH JOIN");
    Preconditions.checkNotNull(eqJoinConjuncts);
    eqJoinConjuncts_ = eqJoinConjuncts;
  }

  public List<BinaryPredicate> getEqJoinConjuncts() { return eqJoinConjuncts_; }
  public void setAddProbeFilters(boolean b) { addProbeFilters_ = true; }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    assignConjuncts(analyzer);

    // Set smap to the combined childrens' smaps and apply that to all conjuncts_.
    createDefaultSmap(analyzer);

    computeStats(analyzer);
    assignedConjuncts_ = analyzer.getAssignedConjuncts();

    ExprSubstitutionMap combinedChildSmap = getCombinedChildSmap();
    otherJoinConjuncts_ =
        Expr.substituteList(otherJoinConjuncts_, combinedChildSmap, analyzer, false);

    List<BinaryPredicate> newEqJoinConjuncts = Lists.newArrayList();
    for (Expr c: eqJoinConjuncts_) {
      BinaryPredicate eqPred =
          (BinaryPredicate) c.substitute(combinedChildSmap, analyzer, false);
      Type t0 = eqPred.getChild(0).getType();
      Type t1 = eqPred.getChild(1).getType();
      if (!t0.matchesType(t1)) {
        // With decimal and char types, the child types do not have to match because
        // the equality builtin handles it. However, they will not hash correctly so
        // insert a cast.
        boolean bothDecimal = t0.isDecimal() && t1.isDecimal();
        boolean bothString = t0.isStringType() && t1.isStringType();
        if (!bothDecimal && !bothString) {
          throw new InternalException("Cannot compare " +
              t0.toSql() + " to " + t1.toSql() + " in join predicate.");
        }
        Type compatibleType = Type.getAssignmentCompatibleType(t0, t1);
        Preconditions.checkState(compatibleType.isDecimal() ||
            compatibleType.isStringType());
        try {
          if (!t0.equals(compatibleType)) {
            eqPred.setChild(0, eqPred.getChild(0).castTo(compatibleType));
          }
          if (!t1.equals(compatibleType)) {
            eqPred.setChild(1, eqPred.getChild(1).castTo(compatibleType));
          }
        } catch (AnalysisException e) {
          throw new InternalException("Should not happen", e);
        }
      }
      Preconditions.checkState(
          eqPred.getChild(0).getType().matchesType(eqPred.getChild(1).getType()));
      newEqJoinConjuncts.add(new BinaryPredicate(eqPred.getOp(),
          eqPred.getChild(0), eqPred.getChild(1)));
    }
    eqJoinConjuncts_ = newEqJoinConjuncts;
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
   * FROM FactTbl F JOIN Customers C D ON (F.cust_id = C.id) ... WHERE C.region = 'US'
   * - if there are 5 regions, the selectivity of "C.region = 'US'" would be 0.2
   *   and the output cardinality of the Customers scan would be 0.2 * # rows in
   *   Customers
   * - # rows in Customers == # of distinct values for Customers.id
   * - the output cardinality of the join would be F.cardinality * 0.2
   */
  private long getJoinCardinality(Analyzer analyzer) {
    Preconditions.checkState(
        joinOp_ == JoinOperator.INNER_JOIN || joinOp_.isOuterJoin());
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
    } else {
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
    }

    Preconditions.checkState(hasValidStats());
    LOG.debug("stats HashJoin: cardinality=" + Long.toString(cardinality_));
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("eqJoinConjuncts_", eqJoinConjunctsDebugString())
        .addValue(super.debugString())
        .toString();
  }

  private String eqJoinConjunctsDebugString() {
    Objects.ToStringHelper helper = Objects.toStringHelper(this);
    for (Expr entry: eqJoinConjuncts_) {
      helper.add("lhs" , entry.getChild(0)).add("rhs", entry.getChild(1));
    }
    return helper.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
    msg.hash_join_node = new THashJoinNode();
    msg.hash_join_node.join_op = joinOp_.toThrift();
    for (Expr entry: eqJoinConjuncts_) {
      TEqJoinCondition eqJoinCondition =
          new TEqJoinCondition(entry.getChild(0).treeToThrift(),
              entry.getChild(1).treeToThrift());
      msg.hash_join_node.addToEq_join_conjuncts(eqJoinCondition);
    }
    for (Expr e: otherJoinConjuncts_) {
      msg.hash_join_node.addToOther_join_conjuncts(e.treeToThrift());
    }
    msg.hash_join_node.setAdd_probe_filters(addProbeFilters_);
  }

  @Override
  protected String getDisplayLabelDetail() {
    StringBuilder output = new StringBuilder(joinOp_.toString());
    if (distrMode_ != DistributionMode.NONE) output.append(", " + distrMode_.toString());
    return output.toString();
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s [%s]\n", prefix, getDisplayLabel(),
        getDisplayLabelDetail()));

    if (detailLevel.ordinal() > TExplainLevel.MINIMAL.ordinal()) {
      output.append(detailPrefix + "hash predicates: ");
      for (int i = 0; i < eqJoinConjuncts_.size(); ++i) {
        Expr eqConjunct = eqJoinConjuncts_.get(i);
        output.append(eqConjunct.toSql());
        if (i + 1 != eqJoinConjuncts_.size()) output.append(", ");
      }
      output.append("\n");
      if (!otherJoinConjuncts_.isEmpty()) {
        output.append(detailPrefix + "other join predicates: ")
        .append(getExplainString(otherJoinConjuncts_) + "\n");
      }
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix + "other predicates: ")
        .append(getExplainString(conjuncts_) + "\n");
      }
    }
    return output.toString();
  }

  @Override
  public void computeCosts(TQueryOptions queryOptions) {
    if (getChild(1).getCardinality() == -1 || getChild(1).getAvgRowSize() == -1
        || numNodes_ == 0) {
      perHostMemCost_ = DEFAULT_PER_HOST_MEM;
      return;
    }
    perHostMemCost_ =
        (long) Math.ceil(getChild(1).cardinality_ * getChild(1).avgRowSize_
          * PlannerContext.HASH_TBL_SPACE_OVERHEAD);
    if (distrMode_ == DistributionMode.PARTITIONED) perHostMemCost_ /= numNodes_;
  }
}
