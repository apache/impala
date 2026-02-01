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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Sarg;
import org.apache.impala.analysis.Expr;
import org.apache.impala.calcite.rel.util.RexInputRefCollector;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.core.Join;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterSelectivityEstimator {

  protected static final Logger LOG =
      LoggerFactory.getLogger(FilterSelectivityEstimator.class);

  private final RelNode childRel_;

  private final double childCardinality_;

  private final RelMetadataQuery mq_;

  public FilterSelectivityEstimator(RelNode childRel, RelMetadataQuery mq) {
    this.mq_ = mq;
    this.childRel_ = (childRel instanceof HepRelVertex)
      ? ((HepRelVertex)childRel).getCurrentRel()
      : childRel;
    this.childCardinality_ = mq.getRowCount(childRel);
  }

  public Double estimateSelectivity(RexNode rexNode) {
    Double selectivity = estimateSelectivityInternal(rexNode, true);
    return selectivity == null ? Expr.DEFAULT_SELECTIVITY : selectivity;
  }

  public Double estimateSelectivityInternal(RexNode rexNode, boolean topLevel) {
    if (rexNode instanceof RexInputRef) {
      return estimateInputRefSelectivity((RexInputRef) rexNode);
    }
    if (rexNode instanceof RexCall) {
      return estimateCallSelectivity((RexCall)rexNode, topLevel);
    }
    return 1.0;
  }

  // Can only provide a selectivity estimate on an "inputRef" filter
  // condition if it is a true/false boolean.
  private Double estimateInputRefSelectivity(RexInputRef inputRef) {
    // There is no selectivity to process for anything other than
    // a boolean
    if (inputRef.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
      return 1.0;
    }

    // We can only get stats if the node directly underneath is a TableScan
    if (!(childRel_ instanceof TableScan)) {
      // For default, If it is a boolean and we assume uniform distribution,
      // it will filter half the rows
      return 0.5;
    }

    CalciteTable table = (CalciteTable) childRel_.getTable();
    Preconditions.checkNotNull(table);
    Column column = table.getColumn(inputRef.getIndex());
    if (column.getStats() != null) {
      ColumnStats stats = column.getStats();
      if (stats.getNumTrues() == 0 && stats.getNumFalses() == 0) {
        return 0.0;
      }
      return (double) stats.getNumTrues() / childCardinality_;
    }

    // For default, If it is a boolean and we assume uniform distribution,
    // it will filter half the rows
    return 0.5;
  }

  private Double estimateCallSelectivity(RexCall call, boolean topLevel) {
    switch (call.getOperator().getKind()) {
      case EQUALS:
      case IS_NOT_DISTINCT_FROM:
        return computeEqualsSelectivity(call);
      case AND:
        return computeConjunctionSelectivity(call, topLevel);
      case OR:
        return computeDisjunctionSelectivity(call);
      case NOT:
      case NOT_EQUALS:
        return computeNotEqualitySelectivity(call);
      case IS_NULL:
        return computeIsNullSelectivity(call);
      case IS_NOT_NULL:
        return computeIsNotNullSelectivity(call);
      case SEARCH:
        return computeSearchSelectivity(call);
      // This logic is similar to the Expr framework. If there is any other type function
      // call, the Expr framework doesn't know what to do with it, so it assigns a null.
      // If this call is part of a conjunction or disjunction, it will not be considered
      // (e.g. see the computeConjunctionSelectivity caller which ignores the null which
      // this method returns). If this is the only "call", the caller will assign a
      // the default selectivity of Expr.DEFAULT_SELECTIVITY.
      default:
        return null;
    }
  }

  /**
   * NDV of "f1(x, y, z) != f2(p, q, r)" ->
   * "(maxNDV(x,y,z,p,q,r) - 1)/maxNDV(x,y,z,p,q,r)".
   * <p>
   *
   * @param call
   * @return returns "not equals" selectivity for call.
   */
  private Double computeNotEqualitySelectivity(RexCall call) {
    Double tmpNDV = getMaxNDV(call);
    if (tmpNDV == null) return null;
    return tmpNDV > 1.0 ? (tmpNDV - 1.0) / tmpNDV : 1.0;
  }

  /**
   * Selectivity of f(X,y,z) -> 1/maxNDV(x,y,z).
   * <p>
   * Note that = is considered a generic function and uses this method to find its
   * selectivity.
   * @param call
   * @return returns "equals" selectivity for call.
   */
  private Double computeEqualsSelectivity(RexCall call) {
    Double tmpNDV = getMaxNDV(call);
    if (tmpNDV == null) return null;
    return 1.0 / getMaxNDV(call);
  }

  private Double computeIsNullSelectivity(RexCall call) {
    if (childRel_ instanceof TableScan
        && call.getOperands().get(0) instanceof RexInputRef) {
      TableScan tableScan = (TableScan) childRel_;
      return getNumNulls(call, tableScan) / Math.max(mq_.getRowCount(tableScan), 1);
    } else if (childRel_ instanceof Join) {
      Join join = (Join) childRel_;
      // TODO: fix this.  If it's a non-inner join, we can hopefully calculate the number
      // of null rows based on what happens with the outer join. We also only change
      // the cardinality if the input ref is on the "outer" side.
      if (join.getJoinType() != JoinRelType.INNER) {
        return 1.0 - Expr.DEFAULT_SELECTIVITY;
      }
    }
    return null;
  }

  private Double computeIsNotNullSelectivity(RexCall call) {
    if (childRel_ instanceof TableScan
        && call.getOperands().get(0) instanceof RexInputRef) {
      TableScan tableScan = (TableScan) childRel_;
      double noOfNulls = getNumNulls(call, tableScan);
      double totalNoOfTuples = mq_.getRowCount(tableScan);
      return (totalNoOfTuples - noOfNulls) / Math.max(totalNoOfTuples, 1);
    } else if (childRel_ instanceof Join) {
      Join join = (Join) childRel_;
      // TODO: fix this.  If it's a non-inner join, we can hopefully calculate the number
      // of null rows based on what happens with the outer join. We also only change
      // the cardinality if the input ref is on the "outer" side.
      if (join.getJoinType() != JoinRelType.INNER) {
        return Expr.DEFAULT_SELECTIVITY;
      }
    }
    // IMPALA-14235:  This does not seem right. We are returning an
    // unknown selectivity here if the child is not a Join or TableScan. If this is the
    // only condition, the selectivity will be the default of .1 which seems wrong.
    // Tpcds tests seemed to be good leaving this as null, but this requires further
    // investigation because we may be able to get better plans if this is changed.
    // Also, while this is actually similar to the logic in IsNullPredicate does, it is
    // possible that higher levels (where the child is not just a TableScan or Join) may
    // still be able to deduce the number of nulls.
    return null;
  }

  private Double computeSearchSelectivity(RexCall call) {
    try {
      RexLiteral literal = (RexLiteral) call.getOperands().get(1);
      Sarg<?> sarg = literal.getValueAs(Sarg.class);
      if (sarg.isPoints() || sarg.isComplementedPoints()) {
        Double selectivity = computeEqualsSelectivity(call);
        selectivity = selectivity * sarg.pointCount;
        return sarg.isPoints()
            ? Math.min(selectivity, 1.0)
            : Math.max(1.0 - selectivity, 0.0);
      } else {
        // TODO: Impala has better logic than this
        return null;
      }
    } catch (Exception e) {
      LOG.warn("Warning: Bug found when trying to calculate selectivity for search " +
          "operator, but instead of throwing an exception, a default selectivity will " +
          "be used.");
      return null;
    }
  }

  /**
   * This logic is similar to the logic found in CompoundPredicate
   */
  private Double computeDisjunctionSelectivity(RexCall call) {
    Double tmpSelectivity;
    double selectivity = 0.0;

    // Check to see there are operands, which there should be because this
    // is called when there is a disjunction.
    Preconditions.checkState(call.getOperands().size() >= 2);
    for (RexNode dje : call.getOperands()) {
      tmpSelectivity = estimateSelectivityInternal(dje, false);
      // This logic matches the logic in CompoundPredicate.computeSelectivity()
      // A null is returned when an inner conjunct has an operand where
      // the selectivity could not be calculated.
      if (tmpSelectivity == null) {
        return null;
      }
      // This logic matches the disjunction logic found in
      // CompoundPredicate.computeSelectivity().
      selectivity = selectivity + tmpSelectivity - selectivity * tmpSelectivity;
    }
    return Math.max(0.0, Math.min(1.0, selectivity));
  }

  /**
   * This logic is similar to the logic found in PlanNode.computeCombinedSelectivity()
   * The logic there is a bit more comprehensive on how to deal with missing stats
   * and overlapping clauses. TODO: Between logic still needs to be handled.
   */
  private Double computeConjunctionSelectivity(RexCall call, boolean topLevel) {
    List<Double> selectivities = new ArrayList<>();
    for (RexNode cje : call.getOperands()) {
      Double selectivity = estimateSelectivityInternal(cje, false);
      if (selectivity != null) {
        selectivities.add(selectivity);
      }
    }
    if (selectivities.size() != call.getOperands().size()) {
      // This logic matches the logic in PlanNode.computeCombinedSelectivity().
      // At the top level, if there are any underlying conjuncts where the
      // selectivity could not be calculated, one "default" selectivity is
      // added in to represent all those conjuncts.
      // If it is not the top level, the the logic matches what is found in
      // CompoundPredicate.computeSelectivity() and a null is returned when
      // an inner conjunct has an operand where the selectivity could not be
      // calculated.
      if (topLevel) {
        selectivities.add(Expr.DEFAULT_SELECTIVITY);
      } else {
        return null;
      }
    }
    // Sort the selectivities to get a consistent estimate, regardless of the original
    // conjunct order. Sort in ascending order such that the most selective conjunct
    // is fully applied. (comment copied from PlanNode.computeCombinedSelectivity)
    Collections.sort(selectivities);
    double selectivity = 1.0;
    for (int i = 0; i < selectivities.size(); ++i) {
      // Exponential backoff for each selectivity multiplied into the final result.
      selectivity *= Math.pow(selectivities.get(i), 1.0 / (double) (i + 1));
    }

    return Math.max(0.0, Math.min(1.0, selectivity));
  }

  /**
   * Given a RexCall of kind IS_NULL & TableScan find number of nulls.
   * first col which must be an input ref
   *
   * TODO: improve this
   *
   * @param call
   * @param t
   * @return estimated number of nulls from statistics
   */
  private long getNumNulls(RexCall call, TableScan t) {
    Preconditions.checkState(call.getOperands().size() == 1);
    Preconditions.checkState(call.getOperands().get(0) instanceof RexInputRef);
    RexInputRef inputRef = (RexInputRef) call.getOperands().get(0);
    CalciteTable table = (CalciteTable) t.getTable();
    Column column = table.getColumn(inputRef.getIndex());
    return column.getStats() != null
        ? Math.max(column.getStats().getNumNulls(), 0)
        : 0;
  }

  private Double getMaxNDV(RexCall call) {
    Set<Integer> inputRefs = new HashSet<>(RexInputRefCollector.getInputRefs(call));

    if (inputRefs.size() != 1) {
      return null;
    }

    double maxNDV = 1.0;
    Double ndv = getDistinctRowCount(inputRefs.toArray(new Integer[0])[0]);
    if (ndv != null) {
      maxNDV = Math.max(ndv, maxNDV);
    }

    return maxNDV;
  }

  private Double getDistinctRowCount(int indx) {
    ImmutableBitSet bitSetOfRqdProj = ImmutableBitSet.of(indx);
    return mq_.getDistinctRowCount(childRel_, bitSetOfRqdProj,
        childRel_.getCluster().getRexBuilder().makeLiteral(true));
  }
}
