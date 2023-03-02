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

package org.apache.impala.analysis;

import com.google.common.base.Preconditions;
import org.apache.impala.common.Pair;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class to classify constant predicates and propagate them
 * to other eligible predicates.
 */
public class ConstantPredicateHandler {
  // Equality constant predicates indexed by their position in the
  // original list of conjuncts. Boolean flag tracks whether this
  // equality predicate itself was rewritten.
  private Map<Integer, Pair<Expr, Boolean>> equalityPreds_;
  // For range predicates, maintain a list of exprs to handle constant predicates on
  // the same slot, for example: mydate >= '2019-01-01' AND mydate <= '2020-01-01',
  // since both need to be propagated together
  private Map<SlotRef, List<Expr>> rangePreds_;
  // certain constant predicates are meant for date/timestamp types only
  private Map<Integer, Expr> dateTimePreds_;

  public ConstantPredicateHandler() {
    equalityPreds_ = new HashMap<>();
    rangePreds_ = new HashMap<>();
    dateTimePreds_ = new HashMap<>();
  }

  /**
   * Classify predicates of type 'slotRef <op> constant' into separate lists.
   * Here <op> can be a comparison operator of type '=, <, <=, >. >='.
   * The range predicates are kept in a separate list from the equality
   * predicates. The candidates BitSet is used to determine which members of
   * conjuncts are considered for propagation.
   */
  public void classifyPredicates(List<Expr> conjuncts, BitSet candidates) {
    for (int i = candidates.nextSetBit(0); i >= 0;
          i = candidates.nextSetBit(i+1)) {
      Expr conjunct = conjuncts.get(i);
      if (!Expr.IS_BINARY_PREDICATE.apply(conjunct)) continue;
      BinaryPredicate bp = (BinaryPredicate) conjunct;
      // equality and range operators in the constant predicate
      // are considered for propagation
      boolean isRangeOp = BinaryPredicate.IS_RANGE_PREDICATE.apply(bp);
      if (!isRangeOp && bp.getOp() != BinaryPredicate.Operator.EQ) continue;
      SlotRef slotRef = bp.getBoundSlot();
      if (slotRef == null || !bp.getChild(1).isConstant()) {
        // save the date/timestamp predicates that are not constants themselves
        // in a separate list since they may be the target of propagation later
        if (isCandidateDateTimeExpr(bp)) {
          dateTimePreds_.put(i, bp);
        }
        continue;
      }
      Expr constant = bp.getChild(1);
      // for range predicates, only date and timestamp constants are allowed
      if (isRangeOp && !(constant instanceof DateLiteral ||
            constant instanceof TimestampLiteral)) {
        continue;
      }
      if (isRangeOp) {
        List<Expr> rangePreds = rangePreds_.getOrDefault(slotRef, new ArrayList<>());
        rangePreds.add(bp);
        rangePreds_.put(slotRef, rangePreds);
      } else {
        equalityPreds_.put(i, new Pair<>(bp, false));
      }
    }
  }

  /**
   * Propagate equality constant predicates to other conjuncts.  Propagate
   * range constant predicates to conjuncts involving date and timestamp
   * columns. Populate the keepConjuncts list with the original dateTime
   * predicates that need to be preserved for correctness in certain cases.
   */
  public void propagateConstantPreds(List<Expr> conjuncts, BitSet changed,
    List<Expr> keepConjuncts, Analyzer analyzer) {
    for (Map.Entry<Integer, Pair<Expr, Boolean>> e : equalityPreds_.entrySet()) {
      Pair<Expr, Boolean> value = e.getValue();
      // skip this predicate if it itself was previously rewritten
      if (value.second) continue;
      BinaryPredicate bp = (BinaryPredicate) value.first;
      SlotRef slotRef = bp.getBoundSlot();
      Preconditions.checkNotNull(slotRef);
      Expr subst = bp.getSlotBinding(slotRef.getSlotId());
      ExprSubstitutionMap smap = new ExprSubstitutionMap();
      smap.put(slotRef, getTargetExpr(slotRef, subst));
      for (int j = 0; j < conjuncts.size(); ++j) {
        Expr toRewrite = conjuncts.get(j);
        // Don't rewrite with our own substitution!
        if (toRewrite == bp) continue;
        Expr rewritten = toRewrite.substitute(smap, analyzer, true);
        if (!rewritten.equals(toRewrite)) {
          conjuncts.set(j, rewritten);
          changed.set(j, true);
          if (equalityPreds_.containsKey(j)) {
            equalityPreds_.get(j).second = true;
          }
        }
      }
    }
    if (dateTimePreds_.size() == 0) return;
    // Outer loop is different compared to the equality predicates above.
    // Since there could be more than 1 constant range predicate that needs
    // propagation to a particular target, we start with the target date/time
    // expr and apply all the possible range predicate substitutions.
    for (Map.Entry<Integer, Expr> dtEntry : dateTimePreds_.entrySet()) {
      int index = dtEntry.getKey();
      Expr dtExpr = dtEntry.getValue();
      List<Expr> rewrittenExprs = new ArrayList<>();
      for (Map.Entry<SlotRef, List<Expr>> e : rangePreds_.entrySet()) {
        SlotRef slotRef = e.getKey();
        List<Expr> rangePredsList = e.getValue();
        if (rangePredsList.size() == 0) continue;
        for (Expr rangePred : rangePredsList) {
          // Don't rewrite with our own substitution!
          if (rangePred == dtExpr) continue;
          BinaryPredicate bp = (BinaryPredicate) rangePred;
          Expr subst = bp.getSlotBinding(slotRef.getSlotId());
          ExprSubstitutionMap smap = new ExprSubstitutionMap();
          smap.put(slotRef, getTargetExpr(slotRef, subst));
          // make a copy since we may need to re-use the original
          Expr toRewrite = dtExpr.clone();
          toRewrite = rewriteWithOp((BinaryPredicate) toRewrite, bp.getOp(), analyzer);
          Expr rewritten = toRewrite.substitute(smap, analyzer, true);
          if (!rewritten.equals(toRewrite)) {
            rewrittenExprs.add(rewritten);
          }
        }
        if (rewrittenExprs.size() > 0) {
          keepConjuncts.add(dtExpr);
          Expr finalRewritten =
              CompoundPredicate.createConjunctivePredicate(rewrittenExprs);
          conjuncts.set(index, finalRewritten);
          changed.set(index, true);
        }
      }
    }
  }

  public static boolean isCandidateDateTimeExpr(BinaryPredicate bp) {
    if (bp.getChild(0).getType().isDateOrTimeType()
        && bp.getOp() == BinaryPredicate.Operator.EQ
        && bp.getChild(1) instanceof CastExpr) {
      return true;
    }
    return false;
  }

  private Expr rewriteWithOp(BinaryPredicate pred, BinaryPredicate.Operator newOp,
    Analyzer analyzer) {
    // Allow equality to avoid problems with truncating the time-of-day part of
    // timestamps (IMPALA-11960).
    // TODO: the original (stricter) op could be kept if the upper bound is a
    //       TimestampLiteral without time component, e.g ts_col < "2000-01-01"
    if (newOp == BinaryPredicate.Operator.LT) {
      newOp = BinaryPredicate.Operator.LE;
    } else if (newOp == BinaryPredicate.Operator.GT) {
      newOp = BinaryPredicate.Operator.GE;
    }
    if (pred.getOp() == newOp) return pred;
    BinaryPredicate newPred = new BinaryPredicate(newOp, pred.getChild(0),
        pred.getChild(1));
    newPred.analyzeNoThrow(analyzer);
    return newPred;
  }

  // During constant propagation, the types also need to be propagated since a
  // rewritten expr must have compatible data type with the source expr. So,
  // when mapping the source to the target expr we add an implicit cast if needed.
  private static Expr getTargetExpr(SlotRef source, Expr target) {
    Expr expr = source.getType().equals(target.getType()) ?
        target : new CastExpr(source.getType(), target);
    return expr;
  }

}
