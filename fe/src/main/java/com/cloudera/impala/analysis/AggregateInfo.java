// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Encapsulates all the information needed to compute the aggregate functions of a single
 * Select block.
 */
public class AggregateInfo {
  // all exprs from Group By clause, duplicates removed
  private final ArrayList<Expr> groupingExprs;
  // all agg exprs from select block, duplicates removed
  private final ArrayList<AggregateExpr> aggregateExprs;

  // The tuple into which the output of the aggregation computation is materialized;
  // contains groupingExprs.size() + aggregateExprs.size() slots, the first
  // groupingExprs.size() of which contain the values of the grouping exprs, followed by
  // slots for the values of the aggregate exprs.
  private TupleDescriptor aggTupleDesc;

  // map from all grouping and aggregate exprs to a SlotRef referencing the corresp. slot
  // in the agg tuple
  private Expr.SubstitutionMap aggTupleSubstMap;

  // c'tor takes ownership of groupingExprs and aggExprs
  public AggregateInfo(ArrayList<Expr> groupingExprs, ArrayList<AggregateExpr> aggExprs) {
    this.groupingExprs = (groupingExprs != null ? groupingExprs : new ArrayList<Expr>());
    Expr.removeDuplicates(this.groupingExprs);
    this.aggregateExprs = (aggExprs != null ? aggExprs : new ArrayList<AggregateExpr>());
    Expr.removeDuplicates(this.aggregateExprs);
    this.aggTupleSubstMap = new Expr.SubstitutionMap();
  }

  /**
   * Create the info for an aggregation node that merges its pre-aggregated inputs:
   * - tuple desc and subst map are the same as that of the input (we're materializing
   *   the same logical tuple)
   * - grouping exprs: slotrefs to the input's grouping slots
   * - aggregate exprs: aggregation of the input's aggregateExprs slots
   *   (count is mapped to sum, everything else stays the same)
   */
  static public AggregateInfo createMergeAggInfo(
      AggregateInfo inputAggInfo, Analyzer analyzer) throws InternalException {
    TupleDescriptor inputDesc = inputAggInfo.aggTupleDesc;
    // construct grouping exprs
    ArrayList<Expr> groupingExprs = Lists.newArrayList();
    for (int i = 0; i < inputAggInfo.getGroupingExprs().size(); ++i) {
      groupingExprs.add(new SlotRef(inputDesc.getSlots().get(i)));
    }

    // construct agg exprs
    ArrayList<AggregateExpr> aggExprs = Lists.newArrayList();
    for (int i = 0; i < inputAggInfo.getAggregateExprs().size(); ++i) {
      AggregateExpr inputExpr = inputAggInfo.getAggregateExprs().get(i);
      Expr aggExprParam =
          new SlotRef(inputDesc.getSlots().get(
            i + inputAggInfo.getGroupingExprs().size()));
      List<Expr> aggExprParamList = Lists.newArrayList(aggExprParam);
      AggregateExpr aggExpr = null;
      if (inputExpr.getOp() == AggregateExpr.Operator.COUNT) {
        aggExpr =
            new AggregateExpr(AggregateExpr.Operator.SUM, false, false, aggExprParamList);
      } else {
        aggExpr = new AggregateExpr(inputExpr.getOp(), false, false, aggExprParamList);
      }
      try {
        aggExpr.analyze(analyzer);
      } catch (AnalysisException e) {
        // we shouldn't see this
        throw new InternalException(
            "error constructing merge aggregation node: " + e.getMessage());
      }
      aggExprs.add(aggExpr);
    }

    AggregateInfo info = new AggregateInfo(groupingExprs, aggExprs);
    info.aggTupleDesc = inputAggInfo.aggTupleDesc;
    info.aggTupleSubstMap = inputAggInfo.aggTupleSubstMap;
    return info;
  }

  public ArrayList<Expr> getGroupingExprs() {
    return groupingExprs;
  }

  public ArrayList<AggregateExpr> getAggregateExprs() {
    return aggregateExprs;
  }

  public TupleDescriptor getAggTupleDesc() {
    return aggTupleDesc;
  }

  public TupleId getAggTupleId() {
    return aggTupleDesc.getId();
  }

  public Expr.SubstitutionMap getAggTupleSubstMap() {
    return aggTupleSubstMap;
  }

  public void createAggTuple(DescriptorTable descTbl) {
    if (aggTupleDesc != null) {
      throw new IllegalStateException("aggTupleDesc already set");
    }
    aggTupleDesc = descTbl.createTupleDescriptor();
    List<Expr> exprs = Lists.newLinkedList();
    if (groupingExprs != null) {
      exprs.addAll(groupingExprs);
    }
    int aggregateExprStartIndex = groupingExprs.size();
    exprs.addAll(aggregateExprs);
    for (int i = 0; i < exprs.size(); ++i) {
      Expr expr = exprs.get(i);
      SlotDescriptor slotD = descTbl.addSlotDescriptor(aggTupleDesc);
      Preconditions.checkArgument(expr.getType() != PrimitiveType.INVALID_TYPE);
      slotD.setType(expr.getType());
      // count(*) is non-nullable.
      if (i >= aggregateExprStartIndex) {
        Preconditions.checkArgument(expr instanceof AggregateExpr);
        AggregateExpr aggExpr = (AggregateExpr)expr;
        if (aggExpr.getOp() == AggregateExpr.Operator.COUNT) {
          slotD.setIsNullable(false);
        }
      }
      aggTupleSubstMap.lhs.add(expr.clone(null));
      aggTupleSubstMap.rhs.add(new SlotRef(slotD));
    }
  }

  public String debugString() {
    return Objects.toStringHelper(this)
        .add("grouping_exprs", Expr.debugString(groupingExprs))
        .add("aggregate_exprs", Expr.debugString(aggregateExprs))
        .add("agg_tuple", (aggTupleDesc == null ? "null" : aggTupleDesc.debugString()))
        .toString();
  }
}
