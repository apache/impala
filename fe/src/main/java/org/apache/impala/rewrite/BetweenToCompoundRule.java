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

package org.apache.impala.rewrite;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BetweenPredicate;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites BetweenPredicates into an equivalent conjunctive/disjunctive
 * CompoundPredicate.
 * It can be applied to pre-analysis expr trees and therefore does not reanalyze
 * the transformation output itself.
 * Examples:
 * A BETWEEN X AND Y ==> A >= X AND A <= Y
 * A NOT BETWEEN X AND Y ==> A < X OR A > Y
 */
public class BetweenToCompoundRule implements ExprRewriteRule {
  private final static Logger LOG = LoggerFactory.getLogger(BetweenToCompoundRule.class);
  public static final ExprRewriteRule INSTANCE = new BetweenToCompoundRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!(expr instanceof BetweenPredicate)) return expr;
    BetweenPredicate bp = (BetweenPredicate) expr;
    Expr value = bp.getChild(0);
    // Using a cloned value for the 'upper' binary predicate to avoid being affected by
    // changes in the 'lower' binary predicate.
    Expr clonedValue = value.clone();
    Expr lowerBound = bp.getChild(1);
    Expr upperBound = bp.getChild(2);

    BinaryPredicate.Operator lowerOperator;
    BinaryPredicate.Operator upperOperator;
    CompoundPredicate.Operator compoundOperator;

    if (bp.isNotBetween()) {
      // Rewrite into disjunction.
      lowerOperator = BinaryPredicate.Operator.LT;
      upperOperator = BinaryPredicate.Operator.GT;
      compoundOperator = CompoundPredicate.Operator.OR;
    } else {
      // Rewrite into conjunction.
      lowerOperator = BinaryPredicate.Operator.GE;
      upperOperator = BinaryPredicate.Operator.LE;
      compoundOperator = CompoundPredicate.Operator.AND;
    }
    BinaryPredicate lower = new BinaryPredicate(lowerOperator, value, lowerBound);
    BinaryPredicate upper = new BinaryPredicate(upperOperator, clonedValue, upperBound);
    double sel = computeBetweenSelectivity(analyzer, compoundOperator, lower, upper);
    CompoundPredicate pred;
    if (sel > 0) {
      lower.setBetweenSelectivity(bp.getId(), sel);
      upper.setBetweenSelectivity(bp.getId(), sel);
      pred = CompoundPredicate.createFromBetweenPredicate(
          compoundOperator, lower, upper, sel);
    } else {
      pred = new CompoundPredicate(compoundOperator, lower, upper);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Transformed " + bp.debugString() + " to " + pred.debugString());
    }
    return pred;
  }

  private static double computeBetweenSelectivity(Analyzer analyzer,
      CompoundPredicate.Operator compoundOperator, BinaryPredicate lowerBound,
      BinaryPredicate upperBound) {
    SlotRef slotRef = lowerBound.getBoundSlot();
    if (slotRef == null || !slotRef.hasDesc()) return -1;

    // TODO: handle slotDesc that is a Union from different table columns.
    SlotDescriptor slotDesc = slotRef.getDesc();
    FeTable table = slotDesc.getParent().getTable();
    if (table == null || table.getNumRows() < 0) return -1;
    if (!slotDesc.getType().isIntegerOrDateType()) return -1;

    ColumnStats stats = slotDesc.getStats();
    if (!stats.hasNumDistinctValues()) return -1;

    long numNotNulls = table.getNumRows() - Math.max(0, stats.getNumNulls());
    if ((double) stats.getNumDistinctValues() / numNotNulls < 0.9) {
      // column is not quite unique.
      return -1;
    }

    try {
      long upperVal = upperBound.getChild(1).evalToInteger(analyzer, "BETWEEN_up", true);
      long lowerVal = lowerBound.getChild(1).evalToInteger(analyzer, "BETWEEN_low", true);

      // If compute_column_minmax_stats=true, stats might have both high and low value
      // set.
      if (stats.getHighValue() != null) {
        upperVal = Math.min(upperVal, Expr.evalToInteger(stats.getHighValue(), true));
      }
      if (stats.getLowValue() != null) {
        lowerVal = Math.max(lowerVal, Expr.evalToInteger(stats.getLowValue(), true));
      }
      if (upperVal < lowerVal) return -1;

      long diff = upperVal - lowerVal + 1;
      double sel = Math.max(0.0, (double) diff / table.getNumRows());
      if (compoundOperator == CompoundPredicate.Operator.OR) sel = 1.0 - sel;

      // Only return selectivity if it is less than or equal to Expr.DEFAULT_SELECTIVITY.
      // This is to maintain previous behavior where two BinaryPredicate derived from
      // a BetweenPredicate will have unknown (-1) selectivity and
      // PlanNode.computeCombinedSelectivity() will adds Expr.DEFAULT_SELECTIVITY to
      // represent all conjunct with unknown selectivity.
      if (sel <= Expr.DEFAULT_SELECTIVITY) return sel;
    } catch (AnalysisException e) {
      LOG.error("Error evaluating bounding value of BetweenPredicate", e);
    }
    return -1;
  }

  private BetweenToCompoundRule() {}
}
