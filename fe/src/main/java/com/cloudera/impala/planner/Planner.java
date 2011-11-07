// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;

import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SortInfo;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.catalog.HdfsRCFileTable;
import com.cloudera.impala.catalog.HdfsTextTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TPlanExecParams;
import com.cloudera.impala.thrift.TPlanExecRequest;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TScanRange;
import com.google.common.base.Preconditions;
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
   * Return join conjuncts that can be used for hash table lookups.
   * - for inner joins, those are equi-join predicates in which one side is fully bound
   *   by lhsIds and the other by rhs' id;
   * - for outer joins: same type of conjuncts as inner joins, but only from the JOIN
   *   clause
   * Returns the conjuncts in 'joinConjuncts' (in which "<lhs> = <rhs>" is returned
   * as Pair(<lhs>, <rhs>)) and also in their original form in 'joinPredicates'.
   */
  private void getHashLookupJoinConjuncts(
      Analyzer analyzer,
      List<TupleId> lhsIds, TableRef rhs,
      List<Pair<Expr, Expr> > joinConjuncts,
      List<Predicate> joinPredicates) {
    joinConjuncts.clear();
    joinPredicates.clear();
    TupleId rhsId = rhs.getId();
    List<Predicate> candidates;
    if (rhs.getJoinOp().isOuterJoin()) {
      // TODO: create test for this
      Preconditions.checkState(rhs.getOnClause() != null);
      candidates = rhs.getEqJoinConjuncts();
      Preconditions.checkState(candidates != null);
    } else {
      candidates = analyzer.getEqJoinPredicates(rhsId);
    }
    if (candidates == null) {
      return;
    }
    for (Predicate p: candidates) {
      Expr rhsExpr = null;
      if (p.getChild(0).isBound(rhsId.asList())) {
        rhsExpr = p.getChild(0);
      } else {
        Preconditions.checkState(p.getChild(1).isBound(rhsId.asList()));
        rhsExpr = p.getChild(1);
      }

      Expr lhsExpr = null;
      if (p.getChild(0).isBound(lhsIds)) {
        lhsExpr = p.getChild(0);
      } else if (p.getChild(1).isBound(lhsIds)) {
        lhsExpr = p.getChild(1);
      } else {
        // not an equi-join condition between lhsIds and rhsId
        continue;
      }

      Preconditions.checkState(lhsExpr != rhsExpr);
      joinPredicates.add(p);
      Pair<Expr, Expr> entry = Pair.create(lhsExpr, rhsExpr);
      joinConjuncts.add(entry);
    }
  }

  /**
   * Create HashJoinNode to join outer with inner.
   */
  private PlanNode createHashJoinNode(
      Analyzer analyzer, PlanNode outer, TableRef innerRef)
      throws NotImplementedException {
    // the rows coming from the build node only need to have space for the tuple
    // materialized by that node
    PlanNode inner = createScanNode(analyzer, innerRef);
    inner.rowTupleIds = Lists.newArrayList(innerRef.getId());

    List<Pair<Expr, Expr> > eqJoinConjuncts = Lists.newArrayList();
    List<Predicate> eqJoinPredicates = Lists.newArrayList();
    getHashLookupJoinConjuncts(
        analyzer, outer.getTupleIds(), innerRef, eqJoinConjuncts, eqJoinPredicates);
    if (eqJoinPredicates.isEmpty()) {
      throw new NotImplementedException(
          "Join requires at least one equality predicate between the two tables.");
    }
    HashJoinNode result =
        new HashJoinNode(outer, inner, innerRef.getJoinOp(), eqJoinConjuncts,
                         innerRef.getOtherJoinConjuncts());

    // conjuncts evaluated by this node:
    // - equi-join conjuncts are evaluated as part of the hash table lookup
    // - other join conjuncts are evaluated before establishing a match
    // - all conjuncts that are bound by outer.getTupleIds() are evaluated by outer
    //   (or one of its children)
    // - the remaining conjuncts that are bound by result.getTupleIds()
    //   need to be evaluated explicitly by the hash join
    ArrayList<Predicate> conjuncts =
      new ArrayList<Predicate>(analyzer.getConjuncts(result.getTupleIds()));
    conjuncts.removeAll(eqJoinPredicates);
    if (innerRef.getOtherJoinConjuncts() != null) {
      conjuncts.removeAll(innerRef.getOtherJoinConjuncts());
    }
    conjuncts.removeAll(analyzer.getConjuncts(outer.getTupleIds()));
    conjuncts.removeAll(analyzer.getConjuncts(inner.getTupleIds()));
    result.setConjuncts(conjuncts);
    return result;
  }

  /**
   * Mark slots that aren't being referenced by any conjuncts or select list
   * exprs as non-materialized.
   */
  private void markUnrefdSlots(PlanNode root, SelectStmt selectStmt, Analyzer analyzer) {
    PlanNode node = root;
    List<SlotId> refdIdList = Lists.newArrayList();
    while (node != null) {
      node.getMaterializedIds(refdIdList);
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
   * Also calls DescriptorTable.computeMemLayout().
   * @param selectStmt
   * @param analyzer
   * @return root node of plan tree * @throws NotImplementedException if selectStmt
   * contains Order By clause
   */
  private PlanNode createPlan(SelectStmt selectStmt, Analyzer analyzer)
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

    // create left-deep sequence of binary hash joins; assign node ids as we go along
    int nodeId = 0;
    TableRef tblRef = selectStmt.getTableRefs().get(0);
    PlanNode root = createScanNode(analyzer, tblRef);
    root.id = nodeId++;
    root.rowTupleIds = rowTuples;
    for (int i = 1; i < selectStmt.getTableRefs().size(); ++i) {
      TableRef innerRef = selectStmt.getTableRefs().get(i);
      root = createHashJoinNode(analyzer, root, innerRef);
      root.getChild(1).id = nodeId++;
      root.id = nodeId++;
      root.rowTupleIds = rowTuples;
    }

    AggregateInfo aggInfo = selectStmt.getAggInfo();
    if (aggInfo != null) {
      root = new AggregationNode(root, aggInfo);
      root.id = nodeId++;
      if (selectStmt.getHavingPred() != null) {
        root.setConjuncts(selectStmt.getHavingPred().getConjuncts());
      }
    }

    SortInfo sortInfo = selectStmt.getSortInfo();
    if (sortInfo != null) {
      // Determine if we should use TopN or Sort in the backend
      // TODO: only use topN if the memory footprint is expected to be low
      // how to account for strings???
      if (selectStmt.getLimit() != -1) {
        root = new SortNode(root, sortInfo, true);
        root.id = nodeId++;
      } else {
        throw new NotImplementedException("ORDER BY without LIMIT currently not supported");
      }
    }

    root.setLimit(selectStmt.getLimit());
    root.finalize(analyzer);
    markUnrefdSlots(root, selectStmt, analyzer);
    // don't compute mem layout before marking slots that aren't being referenced
    analyzer.getDescTbl().computeMemLayout();

    return root;
  }

  /**
   * Given a selectStmt, creates a sequence of plan fragments that implement the query.
   * @param singleNodePlan result of call to createPlan()
   * @param analyzer the same analyzer that was used to create singleNodePlan
   * @param numNodes number of nodes on which to execute fragments; same semantics as
   *     TQueryRequest.numNodes;
   *     allowed values:
   *     1: single-node execution
   *     NUM_NODES_ALL: executes on all nodes that contain relevant data
   *     NUM_NODES_ALL_RACKS: executes on one node per rack that holds relevant data
   *     > 1: executes on at most that many nodes at any point in time (ie, there can be
   *     more nodes than numNodes with plan fragments for this query, but at most
   *     numNodes would be active at any point in time)
   * @param planFragments non-thrift plan fragments, for debugging purposes
   * @return query exec request containing plan fragments and all execution parameters
   */
  public TQueryExecRequest createPlanFragments(
      SelectStmt selectStmt, Analyzer analyzer, int numNodes,
      List<PlanNode> planFragments)
      throws NotImplementedException, InternalException {
    if (numNodes != 1) {
      throw new NotImplementedException("can only generate single-node plans");
    }
    PlanNode plan = createPlan(selectStmt, analyzer);

    TQueryExecRequest request = new TQueryExecRequest();
    TPlanExecRequest fragmentRequest = new TPlanExecRequest(
        new TupleId().asInt(), Expr.treesToThrift(selectStmt.getSelectListExprs()));
    if (plan != null) {
      planFragments.add(plan);
      fragmentRequest.setPlanFragment(plan.treeToThrift());
      fragmentRequest.setDescTbl(analyzer.getDescTbl().toThrift());
    }
    request.addToFragmentRequests(fragmentRequest);
    if (plan == null) {
      return request;
    }

    // unpartitioned scans executing on local host
    TPlanExecParams execParams = new TPlanExecParams();
    List<ScanNode> scanNodes = Lists.newArrayList();
    plan.collectSubclasses(ScanNode.class, scanNodes);
    for (ScanNode scan: scanNodes) {
      List<TScanRange> scanRanges = Lists.newArrayList();
      scan.getScanParams(scanRanges, null);
      Preconditions.checkState(scanRanges.size() == 1);
      execParams.setScanRanges(scanRanges);
      execParams.setDestHosts(Lists.newArrayList("localhost"));
    }
    request.addToNodeRequestParams(Lists.newArrayList(execParams));

    return request;
}

}
