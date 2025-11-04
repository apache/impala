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

import java.util.HashSet;
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
import org.apache.impala.calcite.rel.util.RexInputRefCollector;
import org.apache.impala.calcite.schema.JoinRelationInfo.EqualityConjunction;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.core.Join;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterSelectivityEstimator {

  protected static final Logger LOG =
      LoggerFactory.getLogger(FilterSelectivityEstimator.class);

  private final RelNode childRel_;

  private final double childCardinality_;

  private final RelMetadataQuery mq_;

  public static final double RANGE_COMPARISON_SELECTIVITY = 1.0 / 3.0;

  public static final double BETWEEN_SELECTIVITY = 1.0 / 9.0;

  public FilterSelectivityEstimator(RelNode childRel, RelMetadataQuery mq) {
    this.mq_ = mq;
    this.childRel_ = (childRel instanceof HepRelVertex)
      ? ((HepRelVertex)childRel).getCurrentRel()
      : childRel;
    this.childCardinality_ = mq.getRowCount(childRel);
  }

  public Double estimateSelectivity(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) {
      return estimateInputRefSelectivity((RexInputRef) rexNode);
    }
    if (rexNode instanceof RexCall) {
      return estimateCallSelectivity((RexCall)rexNode);
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

  private Double estimateCallSelectivity(RexCall call) {
    switch (call.getOperator().getKind()) {
      case EQUALS:
      case LIKE:
      case IS_NOT_DISTINCT_FROM:
        return computeEqualsSelectivity(call);
      case AND:
        return computeConjunctionSelectivity(call);
      case OR:
        return computeDisjunctionSelectivity(call);
      case NOT:
      case NOT_EQUALS:
        return computeNotEqualitySelectivity(call);
      case IS_NULL:
        return computeIsNullSelectivity(call);
      case IS_NOT_NULL:
        return computeIsNotNullSelectivity(call);
      // TODO: For inequalities, between, and in, we can give a selectivity
      // of 0 if condition is out of range. Also, histogram data would be
      // a nice feature to add :)
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case GREATER_THAN:
        return RANGE_COMPARISON_SELECTIVITY;
      case BETWEEN:
        // TODO: Impala has better logic than this
        return BETWEEN_SELECTIVITY;
      case SEARCH:
        return computeSearchSelectivity(call);
      default:
        return computeEqualsSelectivity(call);
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
        return 1.0 - RANGE_COMPARISON_SELECTIVITY;
      }
    }
    return computeEqualsSelectivity(call);
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
        return RANGE_COMPARISON_SELECTIVITY;
      }
    }

    //TODO: IMPALA-14235, let's see if we can do better.
    return computeNotEqualitySelectivity(call);
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
        return BETWEEN_SELECTIVITY;
      }
    } catch (Exception e) {
      LOG.warn("Warning: Bug found when trying to calculate selectivity for search " +
          "operator, but instead of throwing an exception, a default selectivity will " +
          "be used.");
      return computeEqualsSelectivity(call);
    }
  }

  /**
   * Disjunction Selectivity -> (1 D(1-m1/n)(1-m2/n)) where n is the total
   * number of tuples from child and m1 and m2 is the expected number of tuples
   * from each part of the disjunction predicate.
   * <p>
   * Note we compute m1. m2.. by applying selectivity of the disjunctive element
   * on the cardinality from child.
   *
   * @param call
   * @return returns "disjunction" selectivity for call.
   */
  private Double computeDisjunctionSelectivity(RexCall call) {
    Double tmpCardinality;
    Double tmpSelectivity;
    double selectivity = 1;

    for (RexNode dje : call.getOperands()) {
      tmpCardinality = childCardinality_ * estimateSelectivity(dje);
      tmpSelectivity = (tmpCardinality > 1.0 && tmpCardinality < childCardinality_)
        ? 1.0 - tmpCardinality / childCardinality_
        : 1.0;

      selectivity *= tmpSelectivity;
    }

    if (selectivity < 0.0) {
      selectivity = 0.0;
    }

    return (1.0 - selectivity);
  }

  /**
   * Selectivity of conjunctive predicate -> (selectivity of conjunctive
   * element1) * (selectivity of conjunctive element2)...
   *
   * @param call
   * @return returns "conjunction" selectivity for call.
   */
  private Double computeConjunctionSelectivity(RexCall call) {
    double selectivity = 1.0;
    for (RexNode cje : call.getOperands()) {
      selectivity *= estimateSelectivity(cje);
    }
    return selectivity;
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
    Preconditions.checkState(call.getOperator().getKind() == SqlKind.IS_NULL ||
        call.getOperator().getKind() == SqlKind.IS_NOT_NULL);
    Preconditions.checkState(call.getOperands().size() == 1);
    Preconditions.checkState(call.getOperands().get(0) instanceof RexInputRef);
    RexInputRef inputRef = (RexInputRef) call.getOperands().get(0);
    CalciteTable table = (CalciteTable) t.getTable();
    Column column = table.getColumn(inputRef.getIndex());
    return column.getStats() != null ? column.getStats().getNumNulls() : 0;
  }

  private Double getMaxNDV(RexCall call) {
    Set<Integer> inputRefs = new HashSet<>();
    for (RexNode op : call.getOperands()) {
      inputRefs.addAll(RexInputRefCollector.getInputRefs(op));
    }

    double maxNDV = 1.0;
    for (Integer index : inputRefs) {
      maxNDV = Math.max(getDistinctRowCount(index), maxNDV);
    }

    return maxNDV;
  }

  private Double getDistinctRowCount(int indx) {
    ImmutableBitSet bitSetOfRqdProj = ImmutableBitSet.of(indx);
    return mq_.getDistinctRowCount(childRel_, bitSetOfRqdProj,
        childRel_.getCluster().getRexBuilder().makeLiteral(true));
  }
}
