// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;

import com.cloudera.impala.analysis.AggregateExpr;
import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.catalog.HdfsRCFileTable;
import com.cloudera.impala.catalog.HdfsTextTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.common.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * The planner is responsible for turning parse trees into plan fragments that
 * can be shipped off to backends for execution.
 *
 */
public class Planner {
  public Planner() {
  }

  /**
   * Transform '=', '<[=]' and '>[=]' comparisons for given slot into
   * ValueRange. Also removes those predicates which were used for the construction
   * of ValueRange from 'conjuncts'. Only looks at comparisons w/ constants
   * (ie, the bounds of the result can be evaluated with Expr::GetValue(NULL)).
   * If there are multiple competing comparison predicates that could be used
   * to construct a ValueRange, only the first one from each category is chosen.
   */
  private ValueRange createScanRange(SlotDescriptor d, List<Predicate> conjuncts) {
    ListIterator<Predicate> i = conjuncts.listIterator();
    ValueRange result = null;
    while (i.hasNext()) {
      Predicate p = i.next();
      if (!(p instanceof BinaryPredicate)) {
        continue;
      }
      BinaryPredicate comp = (BinaryPredicate) p;
      if (comp.getOp() == BinaryPredicate.Operator.NE) {
        continue;
      }
      Expr slotBinding = comp.getSlotBinding(d.getId());
      if (slotBinding == null || !slotBinding.isConstant()) {
        continue;
      }

      if (comp.getOp() == BinaryPredicate.Operator.EQ) {
        i.remove();
        return ValueRange.createEqRange(slotBinding);
      }

      if (result == null) {
        result = new ValueRange();
      }

      // TODO: do we need copies here?
      if (comp.getOp() == BinaryPredicate.Operator.GT
          || comp.getOp() == BinaryPredicate.Operator.GE) {
        if (result.lowerBound == null) {
          result.lowerBound = slotBinding;
          result.lowerBoundInclusive = (comp.getOp() == BinaryPredicate.Operator.GE);
          i.remove();
        }
      } else {
        if (result.upperBound == null) {
          result.upperBound = slotBinding;
          result.upperBoundInclusive = (comp.getOp() == BinaryPredicate.Operator.LE);
          i.remove();
        }
      }
    }
    return result;
  }

  /**
   * Create node for scanning all data files of a particular table.
   * @param analyzer
   * @param tblRef
   * @return
   * @throws NotImplementedException
   */
  private PlanNode createScanNode(Analyzer analyzer, TableRef tblRef) {
    ScanNode scanNode = null;
    if (tblRef.getTable() instanceof HdfsTextTable) {
      // Hive Text table
      scanNode = new HdfsTextScanNode(tblRef.getDesc(), (HdfsTextTable) tblRef.getTable());
    } else if (tblRef.getTable() instanceof HdfsRCFileTable) {
      // Hive RCFile table
      scanNode = new HdfsRCFileScanNode(tblRef.getDesc(), (HdfsRCFileTable) tblRef.getTable());
    } else {
      // HBase table
      scanNode = new HBaseScanNode(tblRef.getDesc());
    }

    List<Predicate> conjuncts = analyzer.getConjuncts(tblRef.getId().asList());
    ArrayList<ValueRange> keyRanges = Lists.newArrayList();
    boolean addedRange = false;  // added non-null range
    // determine scan predicates for clustering cols
    for (int i = 0; i < tblRef.getTable().getNumClusteringCols(); ++i) {
      SlotDescriptor slotDesc =
          analyzer.getColumnSlot(tblRef.getDesc(), tblRef.getTable().getColumns().get(i));
      if (slotDesc == null
          || (scanNode instanceof HBaseScanNode
              && slotDesc.getType() != PrimitiveType.STRING)) {
        // clustering col not referenced in this query;
        // or: the hbase row key is mapped to a non-string type
        // (since it's stored in ascii it will be lexicographically ordered,
        // and non-string comparisons won't work)
        keyRanges.add(null);
      } else {
        ValueRange keyRange = createScanRange(slotDesc, conjuncts);
        keyRanges.add(keyRange);
        addedRange = true;
      }
    }

    if (addedRange) {
      scanNode.setKeyRanges(keyRanges);
    }
    scanNode.setConjuncts(conjuncts);

    return scanNode;
  }

  /**
   * Create HashJoinNode to join outer with inner.
   */
  private PlanNode createHashJoinNode(
      Analyzer analyzer, PlanNode outer, PlanNode inner) throws NotImplementedException {
    List<Pair<Expr, Expr> > joinPredicates = Lists.newArrayList();
    List<Predicate> joinConjuncts = Lists.newArrayList();
    analyzer.getEqJoinPredicates(
        outer.getTupleIds(), inner.getTupleIds().get(0),
        joinPredicates, joinConjuncts);
    if (joinPredicates.isEmpty()) {
      throw new NotImplementedException(
          "Join requires at least one equality predicate between the two tables.");
    }
    HashJoinNode result = new HashJoinNode(outer, inner, joinPredicates);

    // All conjuncts that are join predicates are evaluated by the hash join
    // implicitly as part of the hash table lookup; all conjuncts that are bound by
    // outer.getTupleIds() are evaluated by outer (or one of its children);
    // only the remaining conjuncts that are bound by result.getTupleIds()
    // need to be evaluated explicitly by the hash join.
    ArrayList<Predicate> conjuncts =
      new ArrayList<Predicate>(analyzer.getConjuncts(result.getTupleIds()));
    conjuncts.removeAll(joinConjuncts);
    conjuncts.removeAll(analyzer.getConjuncts(outer.getTupleIds()));
    conjuncts.removeAll(analyzer.getConjuncts(inner.getTupleIds()));
    result.setConjuncts(conjuncts);
    return result;
  }

  /**
   * Mark slots that aren't being referenced by any conjuncts or select list
   * exprs as non-materialized.
   */
  public void markUnrefdSlots(PlanNode root, SelectStmt selectStmt, Analyzer analyzer) {
    PlanNode node = root;
    List<SlotId> refdIdList = Lists.newArrayList();
    while (node != null) {
      Expr.getIds(node.getConjuncts(), null, refdIdList);
      if (node.hasChild(1)) {
        Expr.getIds(node.getChild(1).getConjuncts(), null, refdIdList);
      }
      // traverse down the leftmost path
      node = node.getChild(0);
    }
    if (selectStmt.getAggInfo() != null) {
      if (selectStmt.getAggInfo().getGroupingExprs() != null) {
        Expr.getIds(selectStmt.getAggInfo().getGroupingExprs(), null, refdIdList);
      }
      Expr.getIds(selectStmt.getAggInfo().getAggregateExprs(), null, refdIdList);
    }
    Expr.getIds(selectStmt.getSelectListExprs(), null, refdIdList);

    HashSet<SlotId> refdIds = Sets.newHashSet();
    refdIds.addAll(refdIdList);
    for (TupleDescriptor tupleDesc: analyzer.getDescTbl().getTupleDescs()) {
      for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
        if (!refdIds.contains(slotDesc.getId())) {
          slotDesc.setIsMaterialized(false);
        }
      }
    }
  }

  /**
   * Create tree of PlanNodes that implements the given selectStmt.
   * Currently only supports single-table queries plus aggregation.
   * Also calls DescriptorTable.computeMemLayout().
   * @param selectStmt
   * @param analyzer
   * @return root node of plan tree
   * @throws NotImplementedException if selectStmt contains joins, order by or aggregates
   */
  public PlanNode createPlan(SelectStmt selectStmt, Analyzer analyzer)
      throws NotImplementedException, InternalException {
    if (selectStmt.getTableRefs().isEmpty()) {
      // no from clause -> nothing to plan
      return null;
    }
    // collect ids of tuples materialized by the subtree that includes all joins
    // and scans
    ArrayList<TupleId> rowTuples = Lists.newArrayList();
    for (TableRef tblRef: selectStmt.getTableRefs()) {
      rowTuples.add(tblRef.getId());
    }

    TableRef tblRef = selectStmt.getTableRefs().get(0);
    PlanNode root = createScanNode(analyzer, tblRef);
    root.rowTupleIds = rowTuples;
    for (int i = 1; i < selectStmt.getTableRefs().size(); ++i) {
      TableRef innerRef = selectStmt.getTableRefs().get(i);
      // all joins are hash joins at this point, and the rows coming from the build
      // node only need to have space for the tuple materialized by that node
      // (this might change with nested-loop joins)
      PlanNode inner = createScanNode(analyzer, innerRef);
      inner.rowTupleIds = Lists.newArrayList(innerRef.getId());
      root = createHashJoinNode(analyzer, root, inner);
      root.rowTupleIds = rowTuples;
    }

    AggregateInfo aggInfo = selectStmt.getAggInfo();
    if (aggInfo != null) {
      // we can't aggregate strings at the moment
      // TODO: fix this
      for (AggregateExpr aggExpr: aggInfo.getAggregateExprs()) {
        if (aggExpr.hasChild(0) && aggExpr.getChild(0).getType() == PrimitiveType.STRING) {
          throw new NotImplementedException(
              aggExpr.getOp().toString() + " currently not supported for strings");
        }
      }
      root = new AggregationNode(root, aggInfo);
      if (selectStmt.getHavingPred() != null) {
        root.setConjuncts(selectStmt.getHavingPred().getConjuncts());
      }
    }

    List<Expr> orderingExprs = selectStmt.getOrderingExprs();
    if (orderingExprs != null) {
      throw new NotImplementedException("ORDER BY currently not supported.");
      // TODO: Uncomment once SortNode is implemented.
      // root =
      //    new SortNode(root, orderingExprs, selectStmt.getOrderingDirections());
    }

    root.setLimit(selectStmt.getLimit());
    root.finalize(analyzer);
    markUnrefdSlots(root, selectStmt, analyzer);
    // don't compute mem layout before marking slots that aren't being referenced
    analyzer.getDescTbl().computeMemLayout();

    return root;
  }

}
