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
import com.cloudera.impala.analysis.BaseTableRef;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.DescriptorTable;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.InlineViewRef;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SortInfo;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.analysis.UnionStmt;
import com.cloudera.impala.catalog.HdfsRCFileTable;
import com.cloudera.impala.catalog.HdfsSeqFileTable;
import com.cloudera.impala.catalog.HdfsTextTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.common.Reference;
import com.cloudera.impala.thrift.Constants;
import com.cloudera.impala.thrift.THBaseKeyRange;
import com.cloudera.impala.thrift.THdfsFileSplit;
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

  // Control how much info explain plan outputs
  private PlanNode.ExplainPlanLevel explainPlanLevel = PlanNode.ExplainPlanLevel.NORMAL;

  private int getNextNodeId() {
    return nextNodeId++;
  }

  public Planner() {
  }

  /**
   * Sets how much details the explain plan the planner will generate.
   * @param level
   */
  public void setExplainPlanDetailLevel(PlanNode.ExplainPlanLevel level) {
    explainPlanLevel = level;
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
   * Create a tree of nodes for the inline view ref
   * @param analyzer
   * @param inlineViewRef the inline view ref
   * @return a tree of nodes for the inline view
   */
  private PlanNode createInlineViewPlan(Analyzer analyzer, InlineViewRef inlineViewRef)
      throws NotImplementedException, InternalException {
    // Get the list of fully bound predicates
    List<Predicate> boundConjuncts = analyzer.getConjuncts(inlineViewRef.getIdList());

    // TODO (alan): this is incorrect for left outer join. Predicate should not be
    // evaluated after the join.

    if (inlineViewRef.getViewStmt() instanceof UnionStmt) {
      throw new NotImplementedException("Planning of UNION not implemented yet.");
    }
    SelectStmt selectStmt = (SelectStmt) inlineViewRef.getViewStmt();

    // If the view select statement does not compute aggregates, predicates are
    // registered with the inline view's analyzer for predicate pushdown.
    if (selectStmt.getAggInfo() == null) {
      for (Predicate boundedConjunct: boundConjuncts) {
        inlineViewRef.getAnalyzer().registerConjuncts(boundedConjunct);
      }
      analyzer.markConjunctsAssigned(boundConjuncts);
    }

    // Create a tree of plan node for the inline view, using inline view's analyzer.
    PlanNode result = createSelectPlan(selectStmt, inlineViewRef.getAnalyzer());

    // If the view select statement has aggregates, predicates aren't pushed into the
    // inline view. The predicates have to be evaluated at the root of the plan node
    // for the inline view (which should be an aggregate node).
    if (selectStmt.getAggInfo() != null) {
      result.getConjuncts().addAll(boundConjuncts);
      analyzer.markConjunctsAssigned(boundConjuncts);
    }

    return result;
  }

  /**
   * Create node for scanning all data files of a particular table.
   * @param analyzer
   * @param tblRef
   * @return a scan node
   * @throws NotImplementedException
   */
  private PlanNode createScanNode(Analyzer analyzer, TableRef tblRef) {
    ScanNode scanNode = null;
    if (tblRef.getTable() instanceof HdfsTextTable) {
      // Hive Text table
      scanNode = new HdfsTextScanNode(
          getNextNodeId(), tblRef.getDesc(), (HdfsTextTable) tblRef.getTable());
    } else if (tblRef.getTable() instanceof HdfsRCFileTable) {
      // Hive RCFile table
      scanNode = new HdfsRCFileScanNode(
          getNextNodeId(), tblRef.getDesc(), (HdfsRCFileTable) tblRef.getTable());
    } else if (tblRef.getTable() instanceof HdfsSeqFileTable) {
      // Hive Sequence table
      scanNode = new HdfsSeqFileScanNode(
          getNextNodeId(), tblRef.getDesc(), (HdfsSeqFileTable) tblRef.getTable());
    } else {
      // HBase table
      scanNode = new HBaseScanNode(getNextNodeId(), tblRef.getDesc());
    }

    // TODO (alan): this is incorrect for left outer joins. Predicate should not be
    // evaluated after the join.

    List<Predicate> conjuncts = analyzer.getConjuncts(tblRef.getId().asList());
    analyzer.markConjunctsAssigned(conjuncts);
    ArrayList<ValueRange> keyRanges = Lists.newArrayList();
    boolean addedRange = false;  // added non-null range
    // determine scan predicates for clustering cols
    for (int i = 0; i < tblRef.getTable().getNumClusteringCols(); ++i) {
      SlotDescriptor slotDesc =
          analyzer.getColumnSlot(tblRef.getDesc(),
                                 tblRef.getTable().getColumns().get(i));
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
      List<Pair<Expr, Expr>> joinConjuncts,
      List<Predicate> joinPredicates) {
    joinConjuncts.clear();
    joinPredicates.clear();
    TupleId rhsId = rhs.getId();
    List<TupleId> rhsIds = rhs.getIdList();
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
      if (p.getChild(0).isBound(rhsIds)) {
        rhsExpr = p.getChild(0);
      } else {
        Preconditions.checkState(p.getChild(1).isBound(rhsIds));
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
      throws NotImplementedException, InternalException {
    // the rows coming from the build node only need to have space for the tuple
    // materialized by that node
    PlanNode inner = createTableRefNode(analyzer, innerRef);
    inner.rowTupleIds = Lists.newArrayList(innerRef.getIdList());

    List<Pair<Expr, Expr>> eqJoinConjuncts = Lists.newArrayList();
    List<Predicate> eqJoinPredicates = Lists.newArrayList();
    getHashLookupJoinConjuncts(
        analyzer, outer.getTupleIds(), innerRef, eqJoinConjuncts, eqJoinPredicates);
    if (eqJoinPredicates.isEmpty()) {
      throw new NotImplementedException(
          "Join requires at least one equality predicate between the two tables.");
    }
    HashJoinNode result =
        new HashJoinNode(getNextNodeId(), outer, inner, innerRef.getJoinOp(),
                         eqJoinConjuncts, innerRef.getOtherJoinConjuncts());
    analyzer.markConjunctsAssigned(eqJoinPredicates);

    // The remaining conjuncts that are bound by result.getTupleIds()
    // need to be evaluated explicitly by the hash join.
    ArrayList<Predicate> conjuncts =
      new ArrayList<Predicate>(analyzer.getConjuncts(result.getTupleIds()));
    result.setConjuncts(conjuncts);
    analyzer.markConjunctsAssigned(conjuncts);
    return result;
  }

  /**
   * Mark slots that are being referenced by any conjuncts, order-by exprs, or select list
   * exprs as materialized. All aggregate slots are materialized.
   */
  private void markRefdSlots(PlanNode root, SelectStmt selectStmt, Analyzer analyzer) {
    Preconditions.checkArgument(root != null);
    List<SlotId> refdIdList = Lists.newArrayList();
    root.getMaterializedIds(refdIdList);

    Expr.getIds(selectStmt.getResultExprs(), null, refdIdList);

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
   * Create a tree of PlanNodes for the given tblRef, which can be a BaseTableRef or a
   * InlineViewRef
   * @param analyzer
   * @param tblRef
   */
  private PlanNode createTableRefNode(Analyzer analyzer, TableRef tblRef)
      throws NotImplementedException, InternalException {
    if (tblRef instanceof BaseTableRef) {
      return createScanNode(analyzer, tblRef);
    }
    if (tblRef instanceof InlineViewRef) {
      return createInlineViewPlan(analyzer, (InlineViewRef)tblRef);
    }
    throw new NotImplementedException("unknown Table Ref Node");
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
      rowTuples.addAll(tblRef.getIdList());
    }

    // create left-deep sequence of binary hash joins; assign node ids as we go along
    TableRef tblRef = selectStmt.getTableRefs().get(0);
    PlanNode root = createTableRefNode(analyzer, tblRef);
    root.rowTupleIds = rowTuples;
    for (int i = 1; i < selectStmt.getTableRefs().size(); ++i) {
      TableRef innerRef = selectStmt.getTableRefs().get(i);
      root = createHashJoinNode(analyzer, root, innerRef);
      root.rowTupleIds = rowTuples;
      // Have the build side of a join copy data to a compact representation
      // in the tuple buffer.
      root.getChildren().get(1).setCompactData(true);
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
   * Create tree of PlanNodes that implements the Select/Project/Join/Group by/Having
   * of the selectStmt query block.
   *
   * @param selectStmt
   * @param analyzer
   * @return return a tree of PlanNodes for the selectStmt
   */
  private PlanNode createSelectPlan(SelectStmt selectStmt, Analyzer analyzer)
      throws NotImplementedException, InternalException {
    PlanNode result = createSpjPlan(selectStmt, analyzer);
    // add aggregation, if required
    AggregateInfo aggInfo = selectStmt.getAggInfo();
    if (aggInfo != null) {
      result = new AggregationNode(getNextNodeId(), result, aggInfo);
    }
    // add having clause, if required
    if (selectStmt.getHavingPred() != null) {
      Preconditions.checkState(result instanceof AggregationNode);
      result.setConjuncts(selectStmt.getHavingPred().getConjuncts());
      analyzer.markConjunctsAssigned(selectStmt.getHavingPred().getConjuncts());
    }

    // add order by and limit
    SortInfo sortInfo = selectStmt.getSortInfo();
    if (sortInfo != null) {
      Preconditions.checkState(selectStmt.getLimit() != -1);
      result = new SortNode(getNextNodeId(), result, sortInfo, true);
      result.setLimit(selectStmt.getLimit());
    }

    // All the conjuncts in the inline view analyzer should be assigned
    Preconditions.checkState(!analyzer.hasUnassignedConjuncts());

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
   * Return the execution parameter explain string for the given plan fragment index
   * @param request
   * @param planFragIdx
   * @return
   */
  private String getExecParamExplainString(TQueryExecRequest request, int planFragIdx) {
    StringBuilder execParamExplain = new StringBuilder();
    String prefix = "  ";

    List<TPlanExecParams> execHostsParams =
        request.getNodeRequestParams().get(planFragIdx);
    for (int nodeIdx = 0; nodeIdx < execHostsParams.size(); nodeIdx++) {
      // If the host has no parameter set, don't print anything
      TPlanExecParams hostExecParams = execHostsParams.get(nodeIdx);
      if (hostExecParams == null || !hostExecParams.isSetScanRanges()) {
        continue;
      }

      if (planFragIdx == 0) {
        // plan fragment 0 is the coordinator
        execParamExplain.append(prefix + "  HOST: coordinator\n");
      } else {
        String hostnode = request.getExecNodes().get(planFragIdx - 1).get(nodeIdx);
        execParamExplain.append(prefix + "  HOST: " + hostnode + "\n");
      }

      for (TScanRange scanRange: hostExecParams.getScanRanges()) {
        int nodeId = scanRange.getNodeId();
        if (scanRange.isSetHbaseKeyRanges()) {
          // HBase scan range is printed as "startKey:stopKey"
          execParamExplain.append(
              prefix + "    HBASE KEY RANGES NODE ID: " + nodeId + "\n");
          for (THBaseKeyRange kr: scanRange.getHbaseKeyRanges()) {
            execParamExplain.append(prefix + "      ");
            if (kr.isSetStartKey()) {
              execParamExplain.append(
                  HBaseScanNode.printKey(kr.getStartKey().getBytes()));
            } else {
              execParamExplain.append("<unbounded>");
            }
            execParamExplain.append(":");
            if (kr.isSetStopKey()) {
              execParamExplain.append(HBaseScanNode.printKey(kr.getStopKey().getBytes()));
            } else {
              execParamExplain.append("<unbounded>");
            }
            execParamExplain.append("\n");
          }
        } else if (scanRange.isSetHdfsFileSplits()) {
          // HDFS splits is printed as "<path> <offset>:<length>"
          execParamExplain.append(prefix + "    HDFS SPLITS NODE ID: " + nodeId + "\n");
          for (THdfsFileSplit fs: scanRange.getHdfsFileSplits() ) {
            execParamExplain.append(prefix + "      ");
            execParamExplain.append(fs.getPath() + " ");
            execParamExplain.append(fs.getOffset() + ":");
            execParamExplain.append(fs.getLength() + "\n");
          }
        }
      }
    }
    if (execParamExplain.length() > 0) {
      execParamExplain.insert(0, "\n" + prefix + "EXEC PARAMS\n");
    }

    return execParamExplain.toString();
  }

  /**
   * Build an explain plan string for plan fragments and execution parameters.
   * @param explainString output parameter that contains the explain plan string
   * @param planFragments
   * @param dataSinks
   * @param request
   */
  private void buildExplainString(
      StringBuilder explainStr, List<PlanNode> planFragments,
      List<DataSink> dataSinks, TQueryExecRequest request) {
    Preconditions.checkState(planFragments.size() == dataSinks.size());

    for (int planFragIdx = 0; planFragIdx < planFragments.size(); ++planFragIdx) {
      // An extra line after each plan fragment
      if (planFragIdx != 0) {
        explainStr.append("\n");
      }

      explainStr.append("Plan Fragment " + planFragIdx + "\n");
      DataSink dataSink = dataSinks.get(planFragIdx);
      PlanNode fragment = planFragments.get(planFragIdx);
      String expString;
      // Coordinator (can only be the first) fragment might not have an associated sink.
      if (dataSink == null) {
        Preconditions.checkState(planFragIdx == 0);
        expString = fragment.getExplainString("  ", explainPlanLevel);
      } else {
        expString = dataSink.getExplainString("  ") +
            fragment.getExplainString("  ", explainPlanLevel);
      }
      explainStr.append(expString);

      // Execution parameters of the current plan fragment
      String execParamExplain = getExecParamExplainString(request, planFragIdx);
      explainStr.append(execParamExplain);
    }

  }

  /**
   * Add aggregation, HAVING predicate and sort node for single-node execution.
   */
  private PlanNode createSingleNodePlan(
      Analyzer analyzer, PlanNode spjPlan, SelectStmt selectStmt) {
    // add aggregation, if required, but without having predicate
    PlanNode root = spjPlan;
    if (selectStmt.getAggInfo() != null) {
      root = new AggregationNode(getNextNodeId(), root, selectStmt.getAggInfo());

      // if we're computing DISTINCT agg fns, the analyzer already created the
      // merge agginfo
      if (selectStmt.getMergeAggInfo() != null) {
        root = new AggregationNode(getNextNodeId(), root, selectStmt.getMergeAggInfo());
      }
    }

    if (selectStmt.getHavingPred() != null) {
      Preconditions.checkState(root instanceof AggregationNode);
      // only enforce having predicate after the final aggregation step
      // TODO: substitute having pred
      root.setConjuncts(selectStmt.getHavingPred().getConjuncts());
      analyzer.markConjunctsAssigned(selectStmt.getHavingPred().getConjuncts());
    }

    SortInfo sortInfo = selectStmt.getSortInfo();
    if (sortInfo != null) {
      Preconditions.checkState(selectStmt.getLimit() != -1);
      root = new SortNode(getNextNodeId(), root, sortInfo, true);
    }

    return root;
  }

  /**
   * Add aggregation, HAVING predicate and sort node for multi-node execution.
   */
  private void createMultiNodePlans(
      Analyzer analyzer, PlanNode spjPlan, SelectStmt selectStmt,
      int numNodes, Reference<PlanNode> coordRef, Reference<PlanNode> slaveRef)
      throws InternalException {
    // plan fragment executed by the coordinator, which does merging and
    // post-aggregation, if applicable
    PlanNode coord = null;

    // plan fragment executed by slave that feeds into coordinator;
    // does everything aside from application of Having predicate and final
    // sorting/top-n step
    PlanNode slave = spjPlan;

    // add aggregation to slave, if required, but without having predicate
    AggregateInfo aggInfo = selectStmt.getAggInfo();
    if (aggInfo != null) {
      slave = new AggregationNode(getNextNodeId(), slave, aggInfo);
    }

    // create coordinator plan fragment (single ExchangeNode, possibly
    // followed by a merge aggregation step and a top-n node)
    coord = new ExchangeNode(getNextNodeId(), slave.getTupleIds());
    coord.rowTupleIds = slave.rowTupleIds;
    coord.nullableTupleIds = slave.nullableTupleIds;

    if (aggInfo != null) {
      if (selectStmt.getMergeAggInfo() != null) {
        // if we're computing DISTINCT agg fns, the analyzer already created the
        // merge agginfo
        coord = new AggregationNode(getNextNodeId(), coord, selectStmt.getMergeAggInfo());
      } else {
        AggregateInfo mergeAggInfo =
            AggregateInfo.createMergeAggInfo(aggInfo, analyzer);
        coord = new AggregationNode(getNextNodeId(), coord, mergeAggInfo);
      }
    }

    if (selectStmt.getHavingPred() != null) {
      Preconditions.checkState(coord instanceof AggregationNode);
      // only enforce having predicate after the final aggregation step
      // TODO: substitute having pred
      coord.setConjuncts(selectStmt.getHavingPred().getConjuncts());
      analyzer.markConjunctsAssigned(selectStmt.getHavingPred().getConjuncts());
    }

    // top-n is always applied at the very end
    // (if we wanted to apply top-n to the slave plan fragments, they would need to be
    // fragmented by the grouping exprs of the GROUP BY, which would only be possible
    // if we grouped by the table's partitioning exprs, and even then do we want
    // to use HDFS file splits for plan fragmentation)
    SortInfo sortInfo = selectStmt.getSortInfo();
    if (sortInfo != null) {
      Preconditions.checkState(selectStmt.getLimit() != -1);
      coord = new SortNode(getNextNodeId(), coord, sortInfo, true);
    }

    coordRef.setRef(coord);
    slaveRef.setRef(slave);
  }

  /**
   * Given an analysisResult, creates a sequence of plan fragments that implement the query.
   *
   * @param analysisResult
   *          result of query analysis
   * @param numNodes
   *          number of nodes on which to execute fragments; same semantics as
   *          TQueryRequest.numNodes;
   *          allowed values:
   *          1: single-node execution
   *          NUM_NODES_ALL: executes on all nodes that contain relevant data
   *          NUM_NODES_ALL_RACKS: executes on one node per rack that holds relevant data
   *          > 1: executes on at most that many nodes at any point in time (ie, there
   *          can be more nodes than numNodes with plan fragments for this query, but
   *          at most numNodes would be active at any point in time)
   * @param explainString output parameter of the explain plan string, if not null
   * @return query exec request containing plan fragments and all execution parameters
   */
  public TQueryExecRequest createPlanFragments(
      AnalysisContext.AnalysisResult analysisResult, int numNodes,
      StringBuilder explainString)
      throws NotImplementedException, InternalException {
    // Set selectStmt from analyzed SELECT or INSERT query.
    SelectStmt selectStmt = null;
    if (analysisResult.isQueryStmt() &&
        analysisResult.getQueryStmt() instanceof UnionStmt) {
      throw new NotImplementedException("Planning of UNION not implemented.");
    }
    if (analysisResult.isInsertStmt()) {
      QueryStmt queryStmt = analysisResult.getInsertStmt().getQueryStmt();
      if (queryStmt instanceof UnionStmt) {
        throw new NotImplementedException("Planning of UNION not implemented.");
      }
      selectStmt = (SelectStmt) queryStmt;
    } else {
      Preconditions.checkState(analysisResult.isQueryStmt());
      Preconditions.checkState(analysisResult.getQueryStmt() instanceof SelectStmt);
      selectStmt = (SelectStmt) analysisResult.getQueryStmt();
    }
    Analyzer analyzer = analysisResult.getAnalyzer();
    PlanNode spjPlan = createSpjPlan(selectStmt, analyzer);

    TQueryExecRequest request = new TQueryExecRequest();
    if (spjPlan == null) {
      // SELECT without FROM clause
      TPlanExecRequest fragmentRequest = new TPlanExecRequest(
          new TUniqueId(), Expr.treesToThrift(selectStmt.getResultExprs()));
      request.addToFragmentRequests(fragmentRequest);
      return request;
    }

    // add aggregation/sort/etc. nodes;
    // root: the node producing the final output (ie, coord plan for distrib. execution)
    PlanNode root = null;
    // slave: only set for distrib. execution; plan feeding into coord
    PlanNode slave = null;
    if (numNodes == 1) {
      root = createSingleNodePlan(analyzer, spjPlan, selectStmt);
    } else {
      Reference<PlanNode> rootRef = new Reference<PlanNode>();
      Reference<PlanNode> slaveRef = new Reference<PlanNode>();
      createMultiNodePlans(analyzer, spjPlan, selectStmt, numNodes, rootRef, slaveRef);
      root = rootRef.getRef();
      slave = slaveRef.getRef();
      slave.finalize(analyzer);
      markRefdSlots(slave, selectStmt, analyzer);
      if (root instanceof ExchangeNode) {
        // if all we're doing is merging results from the slaves, we can
        // also set the limit in the slaves
        slave.setLimit(selectStmt.getLimit());
      }
    }

    root.setLimit(selectStmt.getLimit());
    root.finalize(analyzer);
    markRefdSlots(root, selectStmt, analyzer);
    // don't compute mem layout before marking slots that aren't being referenced
    analyzer.getDescTbl().computeMemLayout();

    // TODO: determine if slavePlan produces more slots than are being
    // ref'd by coordPlan; if so, insert MaterializationNode that trims the
    // output
    // TODO: substitute select list exprs against output of currentPlanRoot
    // probably best to add PlanNode.substMap
    // create plan fragments

    // create scan ranges and determine hosts; do this before serializing the
    // plan trees, otherwise we won't pick up on numSenders for exchange nodes
    ArrayList<ScanNode> scans = Lists.newArrayList();  // leftmost scan is first in list
    List<TScanRange> scanRanges = Lists.newArrayList();
    List<String> hosts = Lists.newArrayList();
    if (numNodes == 1) {
      createPartitionParams(root, 1, scanRanges, hosts);
      root.collectSubclasses(ScanNode.class, scans);
    } else {
      createPartitionParams(slave, numNodes, scanRanges, hosts);
      slave.collectSubclasses(ScanNode.class, scans);
      ExchangeNode exchangeNode = root.findFirstOf(ExchangeNode.class);
      exchangeNode.setNumSenders(hosts.size());
    }

    // collect data sinks for explain string; dataSinks.size() == # of plan fragments
    List<DataSink> dataSinks = Lists.newArrayList();
    // create TPlanExecRequests and set up data sinks
    if (numNodes == 1) {
      TPlanExecRequest planRequest =
          createPlanExecRequest(root, analyzer.getDescTbl(), request);
      planRequest.setOutputExprs(
          Expr.treesToThrift(selectStmt.getResultExprs()));
    } else {
      // coordinator fragment comes first
      TPlanExecRequest coordRequest =
          createPlanExecRequest(root, analyzer.getDescTbl(), request);
      coordRequest.setOutputExprs(
          Expr.treesToThrift(selectStmt.getResultExprs()));
      // create TPlanExecRequest for slave plan
      createPlanExecRequest(slave, analyzer.getDescTbl(), request);

      // Slaves write to stream data sink for an exchange node.
      ExchangeNode exchNode = root.findFirstOf(ExchangeNode.class);
      DataSink dataSink = new DataStreamSink(exchNode.getId());
      request.fragmentRequests.get(1).setDataSink(dataSink.toThrift());
      dataSinks.add(dataSink);
    }

    // create table data sink for insert stmt
    if (analysisResult.isInsertStmt()) {
      DataSink dataSink = analysisResult.getInsertStmt().createDataSink();
      request.fragmentRequests.get(0).setDataSink(dataSink.toThrift());
      // this is the fragment producing the output; always add in first position
      dataSinks.add(0, dataSink);
    } else {
      // record the fact that coord doesn't have a sink
      dataSinks.add(0, null);
    }

    // set request.execNodes and request.nodeRequestParams
    if (numNodes != 1) {
      // add one empty exec param (coord fragment doesn't scan any tables
      // and doesn't send the output anywhere)
      request.addToNodeRequestParams(Lists.newArrayList(new TPlanExecParams()));
    }
    createExecParams(request, scans, scanRanges, hosts);

    // Build the explain plan string, if requested
    if (explainString != null) {
      List<PlanNode> planFragments = Lists.newArrayList();
      planFragments.add(root);
      if (slave != null) {
        planFragments.add(slave);
      }
      buildExplainString(explainString, planFragments, dataSinks, request);
    }

    // All the conjuncts in the analyzer should be assigned
    Preconditions.checkState(!analyzer.hasUnassignedConjuncts());

    return request;
  }

  /**
   * Compute partitioning parameters (scan ranges and hosts) for leftmost scan of plan.
   */
  private void createPartitionParams(PlanNode plan, int numNodes,
      List<TScanRange> scanRanges, List<String> hosts) {
    ScanNode leftmostScan = getLeftmostScan(plan);
    Preconditions.checkState(leftmostScan != null);
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
  }

  /**
   * Create TPlanExecRequest for root and add it to queryRequest.
   */
  private TPlanExecRequest createPlanExecRequest(PlanNode root,
      DescriptorTable descTbl, TQueryExecRequest queryRequest) {
    TPlanExecRequest planRequest = new TPlanExecRequest();
    planRequest.setPlanFragment(root.treeToThrift());
    planRequest.setDescTbl(descTbl.toThrift());
    queryRequest.addToFragmentRequests(planRequest);
    return planRequest;
  }

  /**
   * Set request.execNodes and request.nodeRequestParams based on
   * scanRanges and hosts.
   */
  private void createExecParams(
      TQueryExecRequest request, ArrayList<ScanNode> scans,
      List<TScanRange> scanRanges, List<String> hosts) {
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
    for (int i = 1; i < scans.size(); ++i) {
      ScanNode scan = scans.get(i);
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
  }

}
