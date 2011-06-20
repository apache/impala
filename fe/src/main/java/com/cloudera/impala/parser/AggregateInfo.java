// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Encapsulates all the information needed to compute the aggregate functions of a single
 * Select block.
 *
 */
public class AggregateInfo {
  private final ArrayList<Expr> groupingExprs;  // all exprs from Group By clause
  private final ArrayList<AggregateExpr> aggregateExprs;
    // all agg exprs from select block

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

  public TupleId getAggTupleId() {
    return aggTupleDesc.getId();
  }

  public Expr.SubstitutionMap getAggTupleSubstMap() {
    return aggTupleSubstMap;
  }

  public void createAggTuple(DescriptorTable descTbl) {
    aggTupleDesc = descTbl.createTupleDescriptor();
    List<Expr> exprs = Lists.newLinkedList();
    if (groupingExprs != null) {
      exprs.addAll(groupingExprs);
    }
    exprs.addAll(aggregateExprs);
    for (int i = 0; i < exprs.size(); ++i) {
      Expr expr = exprs.get(i);
      // skip this if it's a duplicate
      boolean skip = false;
      for (int j = 0; j < i; ++j) {
        if (exprs.get(j).equals(expr)) {
          skip = true;
          break;
        }
      }
      if (skip) {
        continue;
      }

      SlotDescriptor slotD = descTbl.addSlotDescriptor(aggTupleDesc);
      Preconditions.checkArgument(expr.getType() != PrimitiveType.INVALID_TYPE);
      slotD.setType(expr.getType());
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
