// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.AnalysisContext;
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
import com.cloudera.impala.thrift.Constants;
import com.cloudera.impala.thrift.THostPort;
import com.cloudera.impala.thrift.TPlanExecParams;
import com.cloudera.impala.thrift.TPlanExecRequest;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TScanRange;
import com.cloudera.impala.thrift.TUniqueId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * The planner is responsible for turning parse trees into plan fragments that
 * can be shipped off to backends for execution.
 *
 */
public class Planner {
  private final static Logger LOG = LoggerFactory.getLogger(Planner.class);

  // counter to assign sequential node ids
  private int nextNodeId = 0;

  private int getNextNodeId() {
    return nextNodeId++;
  }

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
   * Mark slots that are being referenced by any conjuncts or select list
   * exprs as materialized.
   */
  private void markRefdSlots(PlanNode root, SelectStmt selectStmt, Analyzer analyzer) {
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
        if (refdIds.contains(slotDesc.getId())) {
          slotDesc.setIsMaterialized(true);
        }
      }
    }
  }

  /**
   * Create tree of PlanNodes that implements the Select/Project/Join part of the
   * given selectStmt.
   * Also calls DescriptorTable.computeMemLayout().
   * @param selectStmt
   * @param analyzer
   * @return root node of plan tree * @throws NotImplementedException if selectStmt
   * contains Order By clause
   */
  private PlanNode createSpjPlan(SelectStmt selectStmt, Analyzer analyzer)
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
    TableRef tblRef = selectStmt.getTableRefs().get(0);
    PlanNode root = createScanNode(analyzer, tblRef);
    root.id = getNextNodeId();
    root.rowTupleIds = rowTuples;
    for (int i = 1; i < selectStmt.getTableRefs().size(); ++i) {
      TableRef innerRef = selectStmt.getTableRefs().get(i);
      root = createHashJoinNode(analyzer, root, innerRef);
      root.getChild(1).id = getNextNodeId();
      root.id = getNextNodeId();
      root.rowTupleIds = rowTuples;
    }

    if (selectStmt.getSortInfo() != null && selectStmt.getLimit() == -1) {
      // TODO: only use topN if the memory footprint is expected to be low
      // how to account for strings???
      throw new NotImplementedException(
          "ORDER BY without LIMIT currently not supported");
    }

    return root;
  }

  /**
   * Create AggregationNode as parent of inputNode that merges the aggregation
   * output described by inputAggInfo:
   * - grouping exprs: slotrefs to inputAggInfo's grouping slots
   * - aggregate exprs: aggregation of inputAggInfo's aggregateExprs slots
   *   (count is mapped to sum, everything else stays the same)
   */
  private PlanNode createMergeAggNode(
      Analyzer analyzer, PlanNode inputNode, AggregateInfo inputAggInfo)
      throws InternalException {
    AggregateInfo mergeAggInfo =
        AggregateInfo.createMergeAggInfo(inputAggInfo, analyzer);
    AggregationNode result = new AggregationNode(inputNode, mergeAggInfo);
    return result;
  }

  private ScanNode getLeftmostScan(PlanNode root) {
    if (root instanceof ScanNode) {
      return (ScanNode) root;
    }
    if (root.getChildren().isEmpty()) {
      return null;
    }
    return getLeftmostScan(root.getChildren().get(0));
  }

  /**
   * Given an analysisResult, creates a sequence of plan fragments that implement the query.
   *
   * @param analysisResult
   *          result of query analysis
   * @param planFragments
   *          non-thrift plan fragments for testing/debugging
   * @param dataSinks
   *          non-thrift data sinks for testing/debugging.
   *          the i-th sink corresponds to the i-th plan fragment.
   *          the 0-th sink could possibly be null,
   *          if the last fragment does not feed into anything.
   * @param numNodes
   *          number of nodes on which to execute fragments; same semantics as
   *          TQueryRequest.numNodes;
   *          allowed values:
   *          1: single-node execution
   *          NUM_NODES_ALL: executes on all nodes that contain relevant data
   *          NUM_NODES_ALL_RACKS: executes on one node per rack that holds relevant data
   *          > 1: executes on at most that many nodes at any point in time (ie, there can be
   *          more nodes than numNodes with plan fragments for this query, but at most
   *          numNodes would be active at any point in time)
   * @return query exec request containing plan fragments and all execution parameters
   */
  public TQueryExecRequest createPlanFragments(
      AnalysisContext.AnalysisResult analysisResult, int numNodes,
      List<PlanNode> planFragments, List<DataSink> dataSinks)
      throws NotImplementedException, InternalException {

    // Set selectStmt from analyzed SELECT or INSERT query.
    SelectStmt selectStmt = null;
    if (analysisResult.isInsertStmt()) {
      selectStmt = analysisResult.getInsertStmt().getSelectStmt();
    } else {
      Preconditions.checkState(analysisResult.isSelectStmt());
      selectStmt = analysisResult.getSelectStmt();
    }
    Analyzer analyzer = analysisResult.getAnalyzer();

    // distributed execution: plan that does the bulk of the execution
    // (select-project-join; pre-aggregation is added later) and sends results back to
    // the coordinator;
    // single-node execution: the single plan
    PlanNode slavePlan = createSpjPlan(selectStmt, analyzer);

    if (slavePlan == null) {
      // SELECT without FROM clause
      TQueryExecRequest request = new TQueryExecRequest();
      TPlanExecRequest fragmentRequest = new TPlanExecRequest(
          new TUniqueId(), Expr.treesToThrift(selectStmt.getSelectListExprs()));
      request.addToFragmentRequests(fragmentRequest);
      return request;
    }

    // distributed execution: plan fragment executed by the coordinator, which
    // does merging and post-aggregation, if applicable
    // single-node execution: always null
    PlanNode coordPlan = null;

    // add aggregation, if required, but without having predicate
    AggregateInfo aggInfo = selectStmt.getAggInfo();
    if (aggInfo != null) {
      slavePlan = new AggregationNode(slavePlan, aggInfo);
      slavePlan.id = getNextNodeId();
    }

    int exchNodeId = -1;
    // node that is currently the root of the PlanNode tree(s)
    PlanNode currentPlanRoot = null;
    ExchangeNode exchangeNode = null;
    if (numNodes != 1) {
      // create coordinator plan fragment (single ExchangeNode, possibly
      // followed by a merge aggregation step and a top-n node)
      coordPlan = exchangeNode = new ExchangeNode(slavePlan.getTupleIds());
      coordPlan.id = exchNodeId = getNextNodeId();
      coordPlan.rowTupleIds = slavePlan.rowTupleIds;

      if (aggInfo != null) {
        coordPlan = currentPlanRoot =
          createMergeAggNode(analyzer, coordPlan, aggInfo);
        coordPlan.id = getNextNodeId();
      }
    } else {
      currentPlanRoot = slavePlan;
    }

    if (selectStmt.getHavingPred() != null) {
      Preconditions.checkState(currentPlanRoot instanceof AggregationNode);
      // only enforce having predicate after the final aggregation step
      // TODO: substitute having pred
      currentPlanRoot.setConjuncts(selectStmt.getHavingPred().getConjuncts());
    }

    // top-n is always applied at the very end
    // (if we wanted to apply top-n to the slave plan fragments, they would need to be
    // fragmented by the grouping exprs of the GROUP BY, which would only be possible
    // if we grouped by the table's partitioning exprs, and even then do we want
    // to use HDFS file splits for plan fragmentation)
    SortInfo sortInfo = selectStmt.getSortInfo();
    if (sortInfo != null) {
      Preconditions.checkState(selectStmt.getLimit() != -1);
      if (coordPlan != null) {
        coordPlan = new SortNode(coordPlan, sortInfo, true);
        currentPlanRoot = coordPlan;
      } else {
        slavePlan = new SortNode(slavePlan, sortInfo, true);
        currentPlanRoot = slavePlan;
      }
      currentPlanRoot.id = getNextNodeId();
    }

    slavePlan.finalize(analyzer);
    markRefdSlots(slavePlan, selectStmt, analyzer);
    if (coordPlan != null) {
      coordPlan.setLimit(selectStmt.getLimit());
      coordPlan.finalize(analyzer);
      markRefdSlots(coordPlan, selectStmt, analyzer);
      if (coordPlan instanceof ExchangeNode) {
        // if all we're doing is merging results from the slaves, we can
        // also set the limit in the slaves
        slavePlan.setLimit(selectStmt.getLimit());
      }
    } else {
      slavePlan.setLimit(selectStmt.getLimit());
    }
    // don't compute mem layout before marking slots that aren't being referenced
    analyzer.getDescTbl().computeMemLayout();

    // TODO: determine if slavePlan produces more slots than are being
    // ref'd by coordPlan; if so, insert MaterializationNode that trims the
    // output
    // TODO: substitute select list exprs against output of currentPlanRoot
    // probably best to add PlanNode.substMap

    // partition fragment by partitioning leftmost scan;
    // do this now, so we set the correct number of senders for the exchange node
    ScanNode leftmostScan = getLeftmostScan(slavePlan);
    Preconditions.checkState(leftmostScan != null);
    List<TScanRange> scanRanges = Lists.newArrayList();
    List<String> hosts = Lists.newArrayList();
    int numPartitions;
    if (numNodes > 1) {
      // if we asked for a specific number of nodes, partition into numNodes - 1
      // fragments
      numPartitions = numNodes - 1;
    } else if (numNodes == Constants.NUM_NODES_ALL
        || numNodes == Constants.NUM_NODES_ALL_RACKS) {
      numPartitions = numNodes;
    } else {
      numPartitions = 1;
    }
    leftmostScan.getScanParams(numPartitions, scanRanges, hosts);
    if (scanRanges.isEmpty() && hosts.isEmpty()) {
      // if we're scanning an empty table we still need a single
      // host to execute the scan
      hosts.add("localhost");
    }
    if (exchangeNode != null) {
      exchangeNode.setNumSenders(hosts.size());
    }

    TQueryExecRequest request = new TQueryExecRequest();
    TPlanExecRequest fragmentRequest = new TPlanExecRequest(
        new TUniqueId(), Expr.treesToThrift(selectStmt.getSelectListExprs()));
    if (coordPlan != null) {
      // coordinator fragment comes first
      planFragments.add(coordPlan);
      fragmentRequest.setPlanFragment(coordPlan.treeToThrift());
      fragmentRequest.setDescTbl(analyzer.getDescTbl().toThrift());
      request.addToFragmentRequests(fragmentRequest);

      // add one empty exec param (coord fragment doesn't scan any tables
      // and doesn't send the output anywhere)
      request.addToNodeRequestParams(Lists.newArrayList(new TPlanExecParams()));

      // Coordinator performs insert into a table.
      if (analysisResult.isInsertStmt()) {
        // Data sink to a table. For insert statements.
        DataSink dataSink = analysisResult.getInsertStmt().createDataSink();
        fragmentRequest.setDataSink(dataSink.toThrift());
        dataSinks.add(dataSink);
      } else {
        // Coordinator does not feed into any sink.
        dataSinks.add(null);
      }

      fragmentRequest = new TPlanExecRequest();
    }

    // Set data sink that slavePlan writes to.
    DataSink dataSink = null;
    if (coordPlan == null) {
      if (analysisResult.isInsertStmt()) {
        // Single node insert execution.
        dataSink = analysisResult.getInsertStmt().createDataSink();
      }
    } else {
      // Stream data sink that writes to an exchange node.
      dataSink = new DataStreamSink(exchNodeId);
    }
    if (dataSink != null) {
      fragmentRequest.setDataSink(dataSink.toThrift());
    }
    dataSinks.add(dataSink);

    planFragments.add(slavePlan);
    fragmentRequest.setPlanFragment(slavePlan.treeToThrift());
    fragmentRequest.setDescTbl(analyzer.getDescTbl().toThrift());
    request.addToFragmentRequests(fragmentRequest);

    // one TPlanExecParams per fragment/scan range;
    // we need to add an "empty" range for empty tables (in which case
    // scanRanges will be empty)
    List<TPlanExecParams> fragmentParamsList = Lists.newArrayList();
    Iterator<TScanRange> scanRange = scanRanges.iterator();
    do {
      TPlanExecParams fragmentParams = new TPlanExecParams();
      if (scanRange.hasNext()) {
        fragmentParams.addToScanRanges(scanRange.next());
      }
      THostPort hostPort = new THostPort();
      hostPort.host = "localhost";
      hostPort.port = 0;  // set elsewhere
      fragmentParams.setDestinations(Lists.newArrayList(hostPort));
      fragmentParamsList.add(fragmentParams);
    } while (scanRange.hasNext());

    // add scan ranges for the non-partitioning scans to each of
    // fragmentRequests[1]'s parameters
    List<ScanNode> scanNodes = Lists.newArrayList();
    slavePlan.collectSubclasses(ScanNode.class, scanNodes);
    for (ScanNode scan: scanNodes) {
      if (scan == leftmostScan) {
        continue;
      }
      scanRanges = Lists.newArrayList();
      scan.getScanParams(1, scanRanges, null);
      Preconditions.checkState(scanRanges.size() <= 1);
      if (!scanRanges.isEmpty()) {
        for (TPlanExecParams fragmentParams: fragmentParamsList) {
          fragmentParams.addToScanRanges(scanRanges.get(0));
        }
      }
    }

    request.addToExecNodes(hosts);
    request.addToNodeRequestParams(fragmentParamsList);

    return request;
  }
}
