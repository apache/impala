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
import com.cloudera.impala.common.Pair;
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
 * Hash join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 *
 */
public class HashJoinNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(HashJoinNode.class);

  // Default per-host memory requirement used if no valid stats are available.
  // TODO: Come up with a more useful heuristic (e.g., based on scanned partitions).
  private final static long DEFAULT_PER_HOST_MEM = 2L * 1024L * 1024L * 1024L;

  private final TableRef innerRef_;
  private final JoinOperator joinOp_;

  enum DistributionMode {
    NONE("NONE"),
    BROADCAST("BROADCAST"),
    PARTITIONED("PARTITIONED");

    private final String description;

    private DistributionMode(String descr) {
      this.description = descr;
    }

    @Override
    public String toString() { return description; }
  }

  private DistributionMode distrMode_;

  // conjuncts_ of the form "<lhs> = <rhs>", recorded as Pair(<lhs>, <rhs>)
  private List<Pair<Expr, Expr> > eqJoinConjuncts_;

  // join conjuncts_ from the JOIN clause that aren't equi-join predicates
  private List<Expr> otherJoinConjuncts_;

  // If true, this node can add filters for the probe side that can be generated
  // after reading the build side. This can be very helpful if the join is selective and
  // there are few build rows.
  private boolean addProbeFilters_;

  public HashJoinNode(
      PlanNode outer, PlanNode inner, TableRef innerRef,
      List<Pair<Expr, Expr> > eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
    super("HASH JOIN");
    Preconditions.checkArgument(eqJoinConjuncts != null);
    Preconditions.checkArgument(otherJoinConjuncts != null);
    tupleIds_.addAll(outer.getTupleIds());
    tupleIds_.addAll(inner.getTupleIds());
    tblRefIds_.addAll(outer.getTblRefIds());
    tblRefIds_.addAll(inner.getTblRefIds());
    innerRef_ = innerRef;
    joinOp_ = innerRef.getJoinOp();
    distrMode_ = DistributionMode.NONE;
    eqJoinConjuncts_ = eqJoinConjuncts;
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

  public List<Pair<Expr, Expr>> getEqJoinConjuncts() { return eqJoinConjuncts_; }
  public JoinOperator getJoinOp() { return joinOp_; }
  public TableRef getInnerRef() { return innerRef_; }
  public DistributionMode getDistributionMode() { return distrMode_; }
  public void setDistributionMode(DistributionMode distrMode) { distrMode_ = distrMode; }
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
        Expr.substituteList(otherJoinConjuncts_, combinedChildSmap, analyzer);

    List<Pair<Expr, Expr>> newEqJoinConjuncts = Lists.newArrayList();
    for (Pair<Expr, Expr> c: eqJoinConjuncts_) {
      Expr eqPred = new BinaryPredicate(BinaryPredicate.Operator.EQ, c.first, c.second);
      eqPred = eqPred.substitute(combinedChildSmap, analyzer);
      Type t0 = eqPred.getChild(0).getType();
      Type t1 = eqPred.getChild(1).getType();
      if (!t0.matchesType(t1)) {
        // With decimal types, the child types do not have to match because the equality
        // builtin handles it. However, they will not hash correctly so insert a cast.
        Preconditions.checkState(t0.isDecimal());
        Preconditions.checkState(t1.isDecimal());
        Type compatibleType = Type.getAssignmentCompatibleType(t0, t1);
        Preconditions.checkState(compatibleType.isDecimal());
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
      newEqJoinConjuncts.add(new Pair(eqPred.getChild(0), eqPred.getChild(1)));
    }
    eqJoinConjuncts_ = newEqJoinConjuncts;
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);

    // For a join between child(0) and child(1), we look for join conditions "L.c = R.d"
    // (with L being from child(0) and R from child(1)) and use as the cardinality
    // estimate the maximum of
    //   child(0).cardinality * R.cardinality / # distinct values for R.d
    //     * child(1).cardinality / R.cardinality
    // across all suitable join conditions, which simplifies to
    //   child(0).cardinality * child(1).cardinality / # distinct values for R.d
    // The reasoning is that
    // - each row in child(0) joins with R.cardinality/#DV_R.d rows in R
    // - each row in R is 'present' in child(1).cardinality / R.cardinality rows in
    //   child(1)
    //
    // This handles the very frequent case of a fact table/dimension table join
    // (aka foreign key/primary key join) if the primary key is a single column, with
    // possible additional predicates against the dimension table. An example:
    // FROM FactTbl F JOIN Customers C D ON (F.cust_id = C.id) ... WHERE C.region = 'US'
    // - if there are 5 regions, the selectivity of "C.region = 'US'" would be 0.2
    //   and the output cardinality of the Customers scan would be 0.2 * # rows in
    //   Customers
    // - # rows in Customers == # of distinct values for Customers.id
    // - the output cardinality of the join would be F.cardinality * 0.2

    long maxNumDistinct = 0;
    for (Pair<Expr, Expr> eqJoinPredicate: eqJoinConjuncts_) {
      if (eqJoinPredicate.first.unwrapSlotRef(false) == null) continue;
      SlotRef rhsSlotRef = eqJoinPredicate.second.unwrapSlotRef(false);
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

    if (maxNumDistinct == 0) {
      // if we didn't find any suitable join predicates or don't have stats
      // on the relevant columns, we very optimistically assume we're doing an
      // FK/PK join (which doesn't alter the cardinality of the left-hand side)
      cardinality_ = getChild(0).cardinality_;
    } else if (getChild(0).cardinality_ != -1 && getChild(1).cardinality_ != -1) {
      cardinality_ = multiplyCardinalities(getChild(0).cardinality_,
          getChild(1).cardinality_);
      cardinality_ = Math.round((double)cardinality_ / (double) maxNumDistinct);
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
      case LEFT_ANTI_JOIN: {
        if (leftCard != -1) {
          cardinality_ = leftCard;
          if (rightCard != -1) {
            cardinality_ = Math.max(0, leftCard - rightCard);
          }
        }
        break;
      }
      case RIGHT_ANTI_JOIN: {
        if (rightCard != -1) {
          cardinality_ = rightCard;
          if (leftCard != -1) {
            cardinality_ = Math.max(0, rightCard - leftCard);
          }
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
    for (Pair<Expr, Expr> entry: eqJoinConjuncts_) {
      helper.add("lhs" , entry.first).add("rhs", entry.second);
    }
    return helper.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
    msg.hash_join_node = new THashJoinNode();
    msg.hash_join_node.join_op = joinOp_.toThrift();
    for (Pair<Expr, Expr> entry: eqJoinConjuncts_) {
      TEqJoinCondition eqJoinCondition =
          new TEqJoinCondition(entry.first.treeToThrift(), entry.second.treeToThrift());
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
        Pair<Expr, Expr> eqConjunct = eqJoinConjuncts_.get(i);
        output.append(eqConjunct.first.toSql() + " = " + eqConjunct.second.toSql());
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
          * Planner.HASH_TBL_SPACE_OVERHEAD);
    if (distrMode_ == DistributionMode.PARTITIONED) perHostMemCost_ /= numNodes_;
  }
}
