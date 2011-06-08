// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.ArrayList;

import com.cloudera.impala.catalog.PrimitiveType;
import com.google.common.base.Preconditions;

// Encapsulates all the information needed to compute the aggregate functions of a single
// Select block.
public class AggregateInfo {
  private final ArrayList<Expr> groupingExprs;  // all exprs from Group By clause
  private final ArrayList<AggregateExpr> aggregateExprs;
    // all agg exprs from select list and having clause

  // The tuple into which the output of the aggregation computation is materialized; contains
  // groupingExprs.size() + aggregateExprs.size() slots, the first groupingExprs.size() of
  // which contain the values of the grouping exprs, followed by slots for the values of the
  // aggregate exprs.
  private TupleDescriptor aggTupleDesc;

  // map from all grouping and aggregate exprs to a SlotRef referencing the corresp. slot
  // in the agg tuple
  private final Expr.SubstitutionMap aggTupleSubstMap;

  // c'tor takes ownership of groupingExprs and aggExprs
  public AggregateInfo(ArrayList<Expr> groupingExprs, ArrayList<AggregateExpr> aggExprs) {
    this.groupingExprs = groupingExprs;
    this.aggregateExprs = aggExprs;
    this.aggTupleSubstMap = new Expr.SubstitutionMap();
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

  public Expr.SubstitutionMap getAggTupleSubstMap() {
    return aggTupleSubstMap;
  }

  public void createAggTuple(DescriptorTable descTbl) {
    aggTupleDesc = descTbl.createTupleDescriptor();
    if (groupingExprs != null) {
      for (int i = 0; i < groupingExprs.size(); ++i) {
        Expr groupingExpr = groupingExprs.get(i);
        // skip this if it's a duplicate
        boolean skip = false;
        for (int j = 0; j < i; ++j) {
          if (groupingExprs.get(j).equals(groupingExpr)) {
            skip = true;
            break;
          }
        }
        if (skip) continue;

        SlotDescriptor slotD = descTbl.addSlotDescriptor(aggTupleDesc);
        Preconditions.checkArgument(groupingExpr.getType() != PrimitiveType.INVALID_TYPE);
        slotD.setType(groupingExpr.getType());
        aggTupleSubstMap.lhs.add(groupingExpr.clone(null));
        aggTupleSubstMap.rhs.add(new SlotRef(slotD));
      }
    }
    for (int i = 0; i < aggregateExprs.size(); ++i) {
      AggregateExpr aggExpr = aggregateExprs.get(i);
      // skip this if it's a duplicate
      boolean skip = false;
      for (int j = 0; j < i; ++j) {
        if (aggregateExprs.get(j).equals(aggExpr)) {
          skip = true;
          break;
        }
      }
      if (skip) continue;

      SlotDescriptor slotD = descTbl.addSlotDescriptor(aggTupleDesc);
      Preconditions.checkArgument(aggExpr.getType() != PrimitiveType.INVALID_TYPE);
      slotD.setType(aggExpr.getType());
      aggTupleSubstMap.lhs.add(aggExpr.clone(null));
      aggTupleSubstMap.rhs.add(new SlotRef(slotD));
    }
  }
}
