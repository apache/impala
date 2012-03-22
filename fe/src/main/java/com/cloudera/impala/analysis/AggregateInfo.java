// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final static Logger LOG = LoggerFactory.getLogger(AggregateInfo.class);

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
  private Expr.SubstitutionMap aggTupleSMap;

  // c'tor takes ownership of groupingExprs and aggExprs
  public AggregateInfo(
      ArrayList<Expr> groupingExprs, ArrayList<AggregateExpr> aggExprs) {
    this.groupingExprs = (groupingExprs != null ? groupingExprs : new ArrayList<Expr>());
    Expr.removeDuplicates(this.groupingExprs);
    this.aggregateExprs = (aggExprs != null ? aggExprs : new ArrayList<AggregateExpr>());
    Expr.removeDuplicates(this.aggregateExprs);
    this.aggTupleSMap = new Expr.SubstitutionMap();
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

  public Expr.SubstitutionMap getSMap() {
    return aggTupleSMap;
  }

  /**
   * Append ids of all slots that are being referenced in the process
   * of performing the aggregate computation described by this AggregateInfo.
   */
  public void getRefdSlots(List<SlotId> ids) {
    Preconditions.checkState(aggTupleDesc != null);
    if (groupingExprs != null) {
      Expr.getIds(groupingExprs, null, ids);
    }
    Expr.getIds(aggregateExprs, null, ids);
    // we reference all grouping slots (because we write them)
    for (int i = 0; i < groupingExprs.size(); ++i) {
      ids.add(aggTupleDesc.getSlots().get(i).getId());
    }
  }

  /**
   * Create the info for an aggregation node that merges its pre-aggregated inputs:
   * - tuple desc and smap are the same as that of the input (we're materializing
   *   the same logical tuple)
   * - grouping exprs: slotrefs to the input's grouping slots
   * - aggregate exprs: aggregation of the input's aggregateExprs slots
   *   (count is mapped to sum, everything else stays the same)
   *
   * The return AggregateInfo shares its descriptor and smap with the input info;
   * createAggTuple() must not be called on it.
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
    info.aggTupleSMap = inputAggInfo.aggTupleSMap;
    return info;
  }

  /**
   * Create the info for an aggregation node that merges the pre-aggregation (phase 1)
   * result of DISTINCT aggregate functions.
   * (Refer to SelectStmt.createDistinctAggInfo() for an explanation of the phases.)
   * - grouping exprs are those of the original query
   * - aggregate exprs for the DISTINCT agg fns: these are aggregating the grouping
   *   slots that were added to the original grouping slots in phase 1;
   *   count is mapped to count(*) and sum is mapped to sum
   * - other aggregate exprs: these are aggregate like for the non-DISTINCT merge
   *   case
   *   (count is mapped to sum, everything else stays the same)
   *
   * This call also creates the tuple descriptor and smap for the returned AggregateInfo.
   */
  static public AggregateInfo createDistinctMergeAggInfo(
      AggregateInfo inputAggInfo, ArrayList<AggregateExpr> distinctAggExprs,
      Analyzer analyzer) throws InternalException {
    Preconditions.checkState(!distinctAggExprs.isEmpty());
    TupleDescriptor inputDesc = inputAggInfo.aggTupleDesc;
    // construct grouping exprs: we're only grouping by the original grouping
    // exprs (params of the DISTINCT aggregate function got appended in the 1st phase)
    int numDistinctParams = distinctAggExprs.get(0).getChildren().size();
    int numOriginalGroupingExprs =
        inputAggInfo.getGroupingExprs().size() - numDistinctParams;
    ArrayList<Expr> groupingExprs = Lists.newArrayList();
    // the original grouping slots are the first slots in the tuple descriptor
    // of the phase 1 output
    for (int i = 0; i < numOriginalGroupingExprs; ++i) {
      groupingExprs.add(new SlotRef(inputDesc.getSlots().get(i)));
    }

    // construct agg exprs for original DISTINCT aggregate functions
    ArrayList<AggregateExpr> aggExprs = Lists.newArrayList();
    for (AggregateExpr inputExpr: distinctAggExprs) {
      AggregateExpr aggExpr = null;
      if (inputExpr.getOp() == AggregateExpr.Operator.COUNT) {
        // COUNT(DISTINCT ...) -> COUNT(*)
        aggExpr = new AggregateExpr(AggregateExpr.Operator.COUNT, true, false, null);
      } else {
        // SUM(DISTINCT <expr>) -> SUM(<last grouping slot>);
        // (MIN(DISTINCT ...) and MAX(DISTINCT ...) have their DISTINCT turned
        // off during analysis, and AVG() is changed to SUM()/COUNT())
        Preconditions.checkState(inputExpr.getOp() == AggregateExpr.Operator.SUM);
        Expr aggExprParam =
            new SlotRef(inputDesc.getSlots().get(numOriginalGroupingExprs));
        List<Expr> aggExprParamList = Lists.newArrayList(aggExprParam);
        aggExpr = new AggregateExpr(
            AggregateExpr.Operator.SUM, false, false, aggExprParamList);
      }
      aggExprs.add(aggExpr);
    }

    // map all the remaining agg fns
    for (int i = 0; i < inputAggInfo.getAggregateExprs().size(); ++i) {
      AggregateExpr inputExpr = inputAggInfo.getAggregateExprs().get(i);
      Expr aggExprParam =
          new SlotRef(inputDesc.getSlots().get(
            i + inputAggInfo.getGroupingExprs().size()));
      List<Expr> aggExprParamList = Lists.newArrayList(aggExprParam);
      AggregateExpr aggExpr = null;
      if (inputExpr.getOp() == AggregateExpr.Operator.COUNT) {
        aggExpr = new AggregateExpr(
            AggregateExpr.Operator.SUM, false, false, aggExprParamList);
      } else {
        aggExpr = new AggregateExpr(inputExpr.getOp(), false, false, aggExprParamList);
      }
      aggExprs.add(aggExpr);
    }

    for (AggregateExpr aggExpr: aggExprs) {
      try {
        aggExpr.analyze(analyzer);
      } catch (AnalysisException e) {
        // we shouldn't see this
        throw new InternalException(
            "error constructing merge aggregation node", e);
      }
    }

    AggregateInfo info = new AggregateInfo(groupingExprs, aggExprs);
    info.createAggTuple(analyzer.getDescTbl());
    info.createDistinctMergeAggSMap(inputAggInfo, distinctAggExprs);
    return info;
  }

  /**
   * Create smap to map original grouping and aggregate exprs onto output
   * of merge agg info.
   */
  private void createDistinctMergeAggSMap(
      AggregateInfo inputAggInfo, ArrayList<AggregateExpr> distinctAggExprs) {
    aggTupleSMap.clear();
    int slotIdx = 0;
    ArrayList<SlotDescriptor> slotDescs = aggTupleDesc.getSlots();

    int numDistinctParams = distinctAggExprs.get(0).getChildren().size();
    int numOrigGroupingExprs =
        inputAggInfo.getGroupingExprs().size() - numDistinctParams;
    Preconditions.checkState(slotDescs.size() ==
        numOrigGroupingExprs + distinctAggExprs.size() +
        inputAggInfo.getAggregateExprs().size());

    // original grouping exprs -> first m slots
    for (int i = 0; i < numOrigGroupingExprs; ++i, ++slotIdx) {
      Expr groupingExpr = inputAggInfo.getGroupingExprs().get(i);
      aggTupleSMap.lhs.add(groupingExpr.clone(null));
      aggTupleSMap.rhs.add(new SlotRef(slotDescs.get(slotIdx)));
    }

    // distinct agg exprs -> next n slots
    for (int i = 0; i < distinctAggExprs.size(); ++i, ++slotIdx) {
      Expr aggExpr = distinctAggExprs.get(i);
      aggTupleSMap.lhs.add(aggExpr.clone(null));
      aggTupleSMap.rhs.add(new SlotRef(slotDescs.get(slotIdx)));
    }

    // remaining agg exprs -> remaining slots
    for (int i = 0; i < inputAggInfo.getAggregateExprs().size(); ++i, ++slotIdx) {
      Expr aggExpr = inputAggInfo.getAggregateExprs().get(i);
      aggTupleSMap.lhs.add(aggExpr.clone(null));
      aggTupleSMap.rhs.add(new SlotRef(slotDescs.get(slotIdx)));
    }
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
      aggTupleSMap.lhs.add(expr.clone(null));
      aggTupleSMap.rhs.add(new SlotRef(slotD));
    }
    LOG.debug("aggtuple=" + aggTupleDesc.debugString());
  }

  public String debugString() {
    return Objects.toStringHelper(this)
        .add("grouping_exprs", Expr.debugString(groupingExprs))
        .add("aggregate_exprs", Expr.debugString(aggregateExprs))
        .add("agg_tuple", (aggTupleDesc == null ? "null" : aggTupleDesc.debugString()))
        .toString();
  }
}
