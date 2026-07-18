/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.calcite.schema;

import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.impala.planner.JoinNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JoinRelationInfo contains metadata information about the Join RelNode.
 * It calculates row counts based on the conditions in the join and is used
 * at join ordering optimization time.
 */
public class JoinRelationInfo {

  protected static final Logger LOG =
      LoggerFactory.getLogger(JoinRelationInfo.class.getName());

  private final List<EqualityConjunction> equalityConjunctions_;

  private final RexNode nonEqualityConjunction_;

  private final Double DISTINCT_PERCENTAGE = (1.0 - JoinNode.FK_PK_MAX_STATS_DELTA_PERC);

  private final Join join_;

  private final RelMetadataQuery mq_;

  private final JoinRelType joinRelType_;

  public JoinRelationInfo(Join join, RexBuilder rexBuilder, RelMetadataQuery mq) {
    this(join, rexBuilder, mq, join.getJoinType());
  }

  public JoinRelationInfo(Join join, RexBuilder rexBuilder, RelMetadataQuery mq,
      JoinRelType joinRelType) {
    List<RexNode> conjunctions = RelOptUtil.conjunctions(join.getCondition());
    join_ = join;
    mq_ = mq;
    joinRelType_ = joinRelType;
    ImmutableList.Builder<EqualityConjunction> equalityConjBuilder =
        ImmutableList.builder();
    ImmutableList.Builder<RexNode> nonEqualityConjBuilder = ImmutableList.builder();
    EqualityConjunction foundConj = null;
    for (RexNode conjunction : conjunctions) {
      EqualityConjunction equalityConj = createEqualityConjunction(join, conjunction, mq);
      if (equalityConj != null) {
        equalityConjBuilder.add(equalityConj);
      } else {
        nonEqualityConjBuilder.add(conjunction);
      }
    }

    equalityConjunctions_ = equalityConjBuilder.build();
    List<RexNode> nonEqualityConjs = nonEqualityConjBuilder.build();

    nonEqualityConjunction_ = nonEqualityConjs.size() > 0
        ? RexUtil.composeConjunction(rexBuilder, nonEqualityConjs)
        : null;
  }

  public boolean hasEqualityConjunctions() {
    // if there are no equality conjunctions, just use the Calcite
    // default row count method.
    return equalityConjunctions_.size() != 0;
  }

  public boolean useDefaultRowCount() {
    // if there are no equality conjunctions, just use the Calcite
    // default row count method.
    return !hasEqualityConjunctions();
  }

  public Double getRowCount() {
    Preconditions.checkState(equalityConjunctions_.size() != 0);
    // number of rows should be the same on any conjunction so just
    // use the first.
    EqualityConjunction conj0 = equalityConjunctions_.get(0);

    Double ret = conj0.lhsNumRows_ * conj0.rhsNumRows_ / getDistinctRows();
    if (joinRelType_ == JoinRelType.LEFT ||
        joinRelType_ == JoinRelType.FULL) {
      ret = Math.max(conj0.lhsNumRows_, ret);
    }
    if (joinRelType_ == JoinRelType.RIGHT ||
        joinRelType_ == JoinRelType.FULL) {
      ret = Math.max(conj0.rhsNumRows_, ret);
    }

    return ret;
  }

  private EqualityConjunction createEqualityConjunction(Join join, RexNode conjunction,
      RelMetadataQuery mq) {

    // get all the join input refs...should be one on the left and one on the right.
    Pair<Integer, Integer> joinCols = getJoinInputRefs(join, conjunction);
    if (joinCols == null) {
      return null;
    }

    EqualityConjunction candidate = new EqualityConjunction(join, mq, joinCols);

    // one side didn't have statistics so we can't use this to analyze row counts.
    if (candidate.lhsNumRows_ == null || candidate.rhsNumRows_ == null) {
      return null;
    }

    return candidate;
  }

  /*
   * 1. Join condition must be an Equality Predicate.
   * 2. both sides must reference 1 column.
   * 3. If needed flip the columns.
   */
  private static Pair<Integer, Integer> getJoinInputRefs(Join joinRel,
      RexNode joinCond) {

    if (!(joinCond instanceof RexCall)) {
      return null;
    }

    if (((RexCall) joinCond).getOperator() != SqlStdOperatorTable.EQUALS) {
      return null;
    }

    ImmutableBitSet leftCols =
        RelOptUtil.InputFinder.bits(((RexCall) joinCond).getOperands().get(0));
    ImmutableBitSet rightCols =
        RelOptUtil.InputFinder.bits(((RexCall) joinCond).getOperands().get(1));

    // Any operator that doesn't have a column from each side of the join would
    // have been pushed down.
    Preconditions.checkState(leftCols.cardinality() == 1 && rightCols.cardinality() == 1);

    int nFieldsLeft = joinRel.getLeft().getRowType().getFieldList().size();
    int nFieldsRight = joinRel.getRight().getRowType().getFieldList().size();
    ImmutableBitSet rightFieldsBitSet = ImmutableBitSet.range(nFieldsLeft,
        nFieldsLeft + nFieldsRight);
    /*
     * flip column references if join condition specified in reverse order to
     * join sources.
     */
    if (rightFieldsBitSet.contains(leftCols)) {
      ImmutableBitSet t = leftCols;
      leftCols = rightCols;
      rightCols = t;
    }

    int leftColIdx = leftCols.nextSetBit(0);
    int rightColIdx = rightCols.nextSetBit(0) - + nFieldsLeft;

    return new Pair<Integer, Integer>(leftColIdx, rightColIdx);
  }

  public double getDistinctRows() {
    double maxLeft = 1.0;
    double maxRight = 1.0;
    // the number of distinct rows is the maximum of the number of distinct rows
    // on the left or on the right. If there are multiple columns, we make a
    // blind assumption that there is no correlation between the columns and thus
    // multiply the columns to get the number of distinct rows. We also
    // make sure that the number of distinct rows never exceeds the total number
    // of rows.
    for (EqualityConjunction equalityConj : equalityConjunctions_) {
      if (maxLeft < equalityConj.lhsNumRows_) {
        maxLeft = Math.min(equalityConj.lhsNumRows_, maxLeft * equalityConj.lhsNdv_);
      }
      if (maxRight < equalityConj.rhsNumRows_) {
        maxRight = Math.min(equalityConj.rhsNumRows_, maxRight * equalityConj.rhsNdv_);
      }
    }

    return Math.max(maxLeft, maxRight);
  }

  /**
   * If there is an outer join, return the number of rows that are unmatched with the
   * outer join side that will produce nulls on that side.  Inner joins do not have
   * unmatched rows. Full joins have unmatched rows on both sides.
   */
  public double getUnmatchedRowsToOuterJoin(boolean isLeftUnmatchedSide) {
    if (!hasEqualityConjunctions()) {
      return 0.0;
    }

    // number of rows should be the same on any conjunction so just
    // use the first.
    EqualityConjunction conj0 = equalityConjunctions_.get(0);

    double leftRows = conj0.lhsNumRows_;
    double rightRows = conj0.rhsNumRows_;
    double leftNdvs = 1.0;
    double rightNdvs = 1.0;

    for (EqualityConjunction equalityConj : equalityConjunctions_) {
      // blind assumption that there is no correlation between distinct
      // values across equi-conditions.
      // TODO: could probably do a little better and check if the same
      // column is used across equality conjunctions, but this is probably
      // rare.
      leftNdvs = Math.min(leftRows, leftNdvs * equalityConj.lhsNdv_);
      rightNdvs = Math.min(rightRows, rightNdvs * equalityConj.rhsNdv_);
    }

    // For a left join, if the number of distinct values on the left is
    // greater than the number of distinct values on the right, we obtain
    // the percentage of the right distinct values over the left distinct values
    // which will be assume to be the percentage of the number of rows that match.
    // Subtracting this percentage from 1 gives us the percentage of the
    // number of rows that do not match, and multiplying that number by the
    // number of rows on the left side gives us the number of unmatched rows.
    // Right join is similar.
    // For full join, the correct parameter should be passed in to decide which
    // unmatched side rows we are interested in.
    double overlappedNdvs = Math.min(leftNdvs, rightNdvs);
    if (isLeftUnmatchedSide) {
      if (joinRelType_ == JoinRelType.LEFT ||
          joinRelType_ == JoinRelType.FULL) {
        return leftRows * (1.0 - overlappedNdvs / leftNdvs);
      }
    } else {
      if (joinRelType_ == JoinRelType.RIGHT ||
          joinRelType_ == JoinRelType.FULL) {
        return rightRows * (1.0 - overlappedNdvs / rightNdvs);
      }
    }
    return 0.0;
  }

  public static class EqualityConjunction {

    public final Double lhsNumRows_;
    public final Double rhsNumRows_;
    public final Double lhsNdv_;
    public final Double rhsNdv_;

    public EqualityConjunction(Join join, RelMetadataQuery mq,
        Pair<Integer, Integer> joinCols) {
      RelNode leftInput = join.getLeft();
      RelNode rightInput = join.getRight();
      lhsNumRows_ = mq.getRowCount(leftInput);
      rhsNumRows_ = mq.getRowCount(rightInput);

      ImmutableBitSet leftColumnBitSet = ImmutableBitSet.of(joinCols.left);
      ImmutableBitSet rightColumnBitSet = ImmutableBitSet.of(joinCols.right);
      lhsNdv_ =
          Math.max(mq.getDistinctRowCount(leftInput, leftColumnBitSet, null), 1.0);
      rhsNdv_ =
          Math.max(mq.getDistinctRowCount(rightInput, rightColumnBitSet, null), 1.0);
    }
  }
}

