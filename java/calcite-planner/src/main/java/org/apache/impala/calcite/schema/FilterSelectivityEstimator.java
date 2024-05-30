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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Sarg;
import org.apache.impala.analysis.Expr;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rel.util.RexInputRefCollector;
import org.apache.impala.calcite.rules.RelUtil;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterSelectivityEstimator {

  protected static final Logger LOG =
      LoggerFactory.getLogger(FilterSelectivityEstimator.class);

  public static final double DEFAULT_IS_NULL_PERCENTAGE = .02;

  private final RelNode childRel_;

  private final double childCardinality_;

  private final RelMetadataQuery mq_;

  public FilterSelectivityEstimator(RelNode childRel, RelMetadataQuery mq) {
    this.mq_ = mq;
    this.childRel_ = RelUtil.unwrapRelNode(childRel);
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
    return getNullPercentage(childRel_, call.getOperands().get(0));
  }

  private Double computeIsNotNullSelectivity(RexCall call) {
    return 1.0 - getNullPercentage(childRel_, call.getOperands().get(0));
  }

  private Double getNullPercentage(RelNode relNode, RexNode nullOperand) {
    // Check to see if this column is on top of an outer join relNode which generates
    // null data if the row is not present on the "outer" side.
    // If a null is returned, then no outer join was detected.
    Double outerJoinEstimate = getJoinNullPercentageEstimate(relNode, nullOperand);
    if (outerJoinEstimate != null) {
      return outerJoinEstimate;
    }

    // TODO: We can probably do an approximation on most RexCalls, but
    // let's punt this for now.
    if (!(nullOperand instanceof RexInputRef)) {
      return DEFAULT_IS_NULL_PERCENTAGE;
    }

    RexInputRef inputRef = (RexInputRef) nullOperand;

    // If the origin table can be found, get the null percentage from there.
    RelColumnOrigin originCol = mq_.getColumnOrigin(relNode, inputRef.getIndex());
    // Return default percentage if we cannot retrieve information stats information
    // from the base table.
    if (originCol == null || originCol.isDerived()) {
      return DEFAULT_IS_NULL_PERCENTAGE;
    }
    int columnNum = originCol.getOriginColumnOrdinal();
    CalciteTable table = (CalciteTable) originCol.getOriginTable();
    Preconditions.checkNotNull(table);
    Double tableRowCount = table.getRowCount();
    return tableRowCount != 0.0
        ? ((double) getNumNulls(columnNum, table)) / table.getRowCount()
        : 0.0;
  }

  /**
   * Returns the join null percentage if the RelNode is on top of an
   * outer join, null if it is not.
   * This method will recursively call its children until it either finds a
   * join RelNode and returns a null percentage estimate, or it finds a node
   * that might affect the null percentage and returns null.
   */
  private Double getJoinNullPercentageEstimate(RelNode relNode, RexNode column) {
    // TODO: We can probably do an approximation on most RexCalls, but
    // let's punt this for now.
    if (!(column instanceof RexInputRef)) {
      return null;
    }
    RexInputRef inputRef = (RexInputRef) column;
    int columnNum = inputRef.getIndex();
    RelNode realRelNode = RelUtil.unwrapRelNode(relNode);
    switch (ImpalaPlanRel.getRelNodeType(realRelNode)) {
      case JOIN:
        Join join = (Join) realRelNode;
        RelNode childRelNode = join.getInput(0);
        int numFieldsOnLeft = childRelNode.getRowType().getFieldList().size();
        boolean columnOnLeft = (columnNum < numFieldsOnLeft);
        JoinRelType joinRelType = join.getJoinType();
        RexBuilder rexBuilder = childRelNode.getCluster().getRexBuilder();
        if (!columnOnLeft) {
          // If column is on the right, all the join information needs to
          // be manipulated for the right side.
          columnNum = columnNum - numFieldsOnLeft;
          inputRef = rexBuilder.makeInputRef(column.getType(), columnNum);
          childRelNode = join.getInput(1);
        }

        // For the case where it is either an inner, we recursively call
        // getNullPercentage for this column for the child.
        if (joinRelType  == JoinRelType.INNER ||
            (joinRelType == JoinRelType.LEFT && columnOnLeft) ||
            (joinRelType == JoinRelType.RIGHT && !columnOnLeft)) {
          return getNullPercentage(childRelNode, inputRef);
        }

        if (!(joinRelType == JoinRelType.LEFT || joinRelType == JoinRelType.RIGHT ||
            joinRelType == JoinRelType.FULL)) {
          return null;
        }

        // if we are here, we know the column is on the outer join side. We
        // calculate the number of rows as if there were an inner join. Then
        // we calculate the number of rows on the non-outer join side.
        // The difference is the number of rows on the outer join side that
        // won't match up and thus put in a null value for that column.

        // Retrieve the JoinRelationInfo for the outer join
        JoinRelationInfo info =
            new JoinRelationInfo(join, rexBuilder, mq_, joinRelType);
        if (!info.hasEqualityConjunctions()) {
          // TODO: If there are no equality conjunctions, we need to come up
          // with a formula to figure out what percentage of rows on the outer
          // side didn't match. For now, return the default null percentage.
          return DEFAULT_IS_NULL_PERCENTAGE;
        }

        Double outerRowCount = info.getRowCount();
        if (outerRowCount == 0.0) {
          return 0.0;
        }

        // unmatchedRows is the number of rows on the outer side of the join.
        Double unmatchedRows = info.getUnmatchedRowsToOuterJoin(!columnOnLeft);
        // (outerRowCount - totalNullRows) should represent the number of rows on
        // the non-outer side that come through the full join, since the totalNumRows is
        // currently equal to the unmatched rows. Multiplying this by the null
        // percentage should give us the number of nulls for the column as if this were
        // an inner join.
        Double totalRowsNonOuterSide = Math.max(outerRowCount - unmatchedRows, 0.0);
        // totalNullRows = the total number of unmatched Rows on the outer side +
        // the calculated number of null rows on the non-outer side.
        Double totalNullRows = unmatchedRows + (totalRowsNonOuterSide *
            getNullPercentage(childRelNode, inputRef));
        Double nullPercentage = Math.min(totalNullRows/outerRowCount, 1.0);
        return Math.max(nullPercentage, 0.0);
      case SORT:
      case FILTER:
        // For these RelNodes, we look at the RelNode child to see if it is an
        // outer join
        return getNullPercentage(realRelNode.getInput(0), column);
      case PROJECT:
        // For project, we need to get the projected column from the input.
        Project project = (Project) realRelNode;
        RexNode newColumn = project.getProjects().get(columnNum);
        return getNullPercentage(realRelNode.getInput(0), newColumn);
      case VALUES:
      case HDFSSCAN:
      case UNION:
      case AGGREGATE:
      default:
        // For these RelNodes, either there is no outer join, or the number of nulls
        // could be affected if it is on top of an outer join.
        return null;
    }
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
  private long getNumNulls(int index, CalciteTable table) {
    Column column = table.getColumn(index);
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
