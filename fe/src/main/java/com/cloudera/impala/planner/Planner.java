// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BaseTableRef;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.InlineViewRef;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SortInfo;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.analysis.UnionStmt;
import com.cloudera.impala.analysis.UnionStmt.Qualifier;
import com.cloudera.impala.analysis.UnionStmt.UnionOperand;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPartitionType;
import com.cloudera.impala.thrift.TQueryOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * The planner is responsible for turning parse trees into plan fragments that
 * can be shipped off to backends for execution.
 *
 */
public class Planner {
  private final static Logger LOG = LoggerFactory.getLogger(Planner.class);

  // For generating a string of the current time.
  private final SimpleDateFormat formatter =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

  private final IdGenerator<PlanNodeId> nodeIdGenerator = new IdGenerator<PlanNodeId>();
  private final IdGenerator<PlanFragmentId> fragmentIdGenerator =
      new IdGenerator<PlanFragmentId>();

  /**
   * Create plan fragments for an analyzed statement, given a set of execution options.
   * The fragments are returned in a list such that element i of that list can
   * only consume output of the following fragments j > i.
   */
  public ArrayList<PlanFragment> createPlanFragments(
      AnalysisContext.AnalysisResult analysisResult, TQueryOptions queryOptions)
      throws NotImplementedException, InternalException {
    // Set queryStmt from analyzed SELECT or INSERT query.
    QueryStmt queryStmt = null;
    if (analysisResult.isInsertStmt()) {
      queryStmt = analysisResult.getInsertStmt().getQueryStmt();
    } else {
      queryStmt = analysisResult.getQueryStmt();
    }
    Analyzer analyzer = analysisResult.getAnalyzer();

    PlanNode singleNodePlan = createSingleNodePlan(queryStmt, analyzer);
    //LOG.info("single-node plan:"
        //+ singleNodePlan.getExplainString("", TExplainLevel.VERBOSE));
    ArrayList<PlanFragment> fragments = Lists.newArrayList();
    if (queryOptions.num_nodes == 1 || singleNodePlan == null) {
      // single-node execution; we're almost done
      fragments.add(new PlanFragment(singleNodePlan, DataPartition.UNPARTITIONED));
    } else {
      // leave the root fragment partitioned if we end up writing to a table;
      // otherwise merge everything into a single coordinator fragment, so we can
      // pass it back to the client
      createPlanFragments(
          singleNodePlan, analysisResult.isInsertStmt(), queryOptions.partition_agg,
          fragments);
    }

    PlanFragment rootFragment = fragments.get(fragments.size() - 1);
    if (analysisResult.isInsertStmt()) {
      // set up table sink for root fragment
      rootFragment.setSink(analysisResult.getInsertStmt().createDataSink());
    }
    // set output exprs before calling finalize()
    rootFragment.setOutputExprs(queryStmt.getResultExprs());

    for (PlanFragment fragment: fragments) {
      fragment.finalize(analyzer, !queryOptions.allow_unsupported_formats);
    }
    // compute mem layout after finalize()
    analyzer.getDescTbl().computeMemLayout();

    Collections.reverse(fragments);
    return fragments;
  }

  /**
   * Return combined explain string for all plan fragments.
   */
  public String getExplainString(
      ArrayList<PlanFragment> fragments, TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < fragments.size(); ++i) {
      PlanFragment fragment = fragments.get(i);
      if (i > 0) {
        // a blank line between plan fragments
        str.append("\n");
      }
      str.append("Plan Fragment " + i + "\n");
      str.append(fragment.getExplainString(explainLevel));
    }
    return str.toString();
  }

  /**
   * Return plan fragment that produces result of 'root'; recursively creates
   * all input fragments to the returned fragment.
   * If a new fragment is created, it is appended to 'fragments', so that
   * each fragment is preceded by those from which it consumes the output.
   * If 'isPartitioned' is false, the returned fragment is unpartitioned;
   * otherwise it may be partitioned, depending on whether its inputs are
   * partitioned; the partitioning function is derived from the inputs.
   * If partitionAgg is true, AggregationNodes that do grouping are partitioned
   * on their grouping exprs and placed in a separate fragment.
   */
  private PlanFragment createPlanFragments(
      PlanNode root, boolean isPartitioned, boolean partitionAgg,
      ArrayList<PlanFragment> fragments)
      throws InternalException, NotImplementedException {
    ArrayList<PlanFragment> childFragments = Lists.newArrayList();
    for (PlanNode child: root.getChildren()) {
      // allow child fragments to be partitioned; merge later if needed
      childFragments.add(createPlanFragments(child, true, partitionAgg, fragments));
    }

    PlanFragment result = null;
    if (root instanceof ScanNode) {
      result = createScanFragment(root);
      fragments.add(result);
    } else if (root instanceof HashJoinNode) {
      Preconditions.checkState(childFragments.size() == 2);
      result = createHashJoinFragment(
          (HashJoinNode) root, childFragments.get(1), childFragments.get(0), fragments);
    } else if (root instanceof MergeNode) {
      result = createMergeNodeFragment((MergeNode) root, childFragments);
    } else if (root instanceof AggregationNode) {
      result = createAggregationFragment(
          (AggregationNode) root, childFragments.get(0), partitionAgg, fragments);
    } else if (root instanceof SortNode) {
      result = createTopnFragment((SortNode) root, childFragments.get(0), fragments);
    } else {
      throw new InternalException(
          "Cannot create plan fragment for this node type: " + root.getExplainString());
    }
    // move 'result' to end, it depends on all of its children
    fragments.remove(result);
    fragments.add(result);

    if (!isPartitioned && result.isPartitioned()) {
      result = createMergeFragment(result);
      fragments.add(result);
    }

    return result;
  }

  /**
   * Return unpartitioned fragment that merges the input fragment's output.
   * Requires that input fragment be partitioned.
   */
  private PlanFragment createMergeFragment(PlanFragment inputFragment) {
    Preconditions.checkState(inputFragment.isPartitioned());
    // top-n should not exist as the output of a partitioned fragment
    Preconditions.checkState(!(inputFragment.getPlanRoot() instanceof SortNode));

    PlanNode mergePlan = new ExchangeNode(
        new PlanNodeId(nodeIdGenerator), inputFragment.getPlanRoot(), false);
    PlanNodeId exchId = mergePlan.getId();
    if (inputFragment.getPlanRoot() instanceof AggregationNode) {
      // insert merge aggregation
      AggregationNode aggNode = (AggregationNode) inputFragment.getPlanRoot();
      mergePlan =
          new AggregationNode(new PlanNodeId(nodeIdGenerator), mergePlan,
                              aggNode.getAggInfo().getMergeAggInfo());
      // HAVING predicates can only be evaluated after the merge agg step
      aggNode.transferConjuncts(mergePlan);
    }

    PlanFragment fragment =
        new PlanFragment(mergePlan, DataPartition.UNPARTITIONED);
    inputFragment.setDestination(fragment, exchId);
    return fragment;
  }

  /**
   * Create new randomly-partitioned fragment containing a single scan node.
   * TODO: take bucketing into account to produce a naturally hash-partitioned
   * fragment
   * TODO: hbase scans are range-partitioned on the row key
   */
  private PlanFragment createScanFragment(PlanNode node) {
    return new PlanFragment(node, DataPartition.RANDOM);
  }

  /**
   * Doesn't create a new fragment, but modifies leftChildFragment to execute
   * a hash join:
   * - the output of the right child fragment is broadcast to the left child fragment
   * - if the output of the right child fragment is partitioned and an aggregation
   *   result (either from TopN- or AggregationNode), creates a merge fragment
   *   prior to broadcasting; this is to avoid recomputation of the merge step
   *   at every receiving backend
   */
  private PlanFragment createHashJoinFragment(
      HashJoinNode node, PlanFragment rightChildFragment,
      PlanFragment leftChildFragment, ArrayList<PlanFragment> fragments) {
    PlanNode rightChildRoot = rightChildFragment.getPlanRoot();
    if (rightChildFragment.isPartitioned()
        && (rightChildRoot instanceof AggregationNode
            || rightChildRoot instanceof SortNode)) {
      rightChildFragment = createMergeFragment(rightChildFragment);
      fragments.add(rightChildFragment);
    }
    connectChildFragment(node, 1, leftChildFragment, rightChildFragment);
    leftChildFragment.setPlanRoot(node);
    return leftChildFragment;
  }

  /**
   * Creates an unpartitioned fragment that merges the outputs of all of its children
   * (with a single ExchangeNode), corresponding to the 'mergeNode' of the
   * non-distributed plan.
   * Each of the child fragments receives a MergeNode as a new plan root (with
   * the child fragment's plan tree as its only input), so that each child
   * fragment's output is mapped onto the MergeNode's result tuple id.
   * Throws NotImplementedException if mergeNode contains constant result
   * exprs (which need to be placed in the parent fragment).
   * TODO: handle MergeNodes with constant select list exprs.
   * TODO: if this is implementing a UNION DISTINCT, the parent of the mergeNode
   * is a duplicate-removing AggregationNode, which might make sense to apply
   * to the children as well, in order to reduce the amount of data that needs
   * to be sent to the parent; augment the planner to decide whether that would
   * reduce the runtime.
   * TODO: since the fragment that does the merge is unpartitioned, it can absorb
   * any child fragments that are also unpartitioned
   */
  private PlanFragment createMergeNodeFragment(MergeNode mergeNode,
      ArrayList<PlanFragment> childFragments)
      throws NotImplementedException {
    Preconditions.checkState(mergeNode.getChildren().size() == childFragments.size());
    if (!mergeNode.getConstExprLists().isEmpty()) {
      throw new NotImplementedException(
          "Distributed UNION with constant SELECT clauses not implemented.");
    }

    // create an ExchangeNode to perform the merge operation of mergeNode;
    // the ExchangeNode retains the generic PlanNode parameters of mergeNode
    ExchangeNode exchNode =
        new ExchangeNode(new PlanNodeId(nodeIdGenerator), mergeNode, true);
    PlanFragment parentFragment =
        new PlanFragment(exchNode, DataPartition.UNPARTITIONED);

    for (int i = 0; i < childFragments.size(); ++i) {
      PlanFragment childFragment = childFragments.get(i);
      MergeNode childMergeNode =
          new MergeNode(new PlanNodeId(nodeIdGenerator), mergeNode, i,
                        childFragment.getPlanRoot());
      childFragment.setPlanRoot(childMergeNode);
      childFragment.setDestination(parentFragment, exchNode.getId());
    }
    return parentFragment;
  }

  /**
   * Replace node's child at index childIdx with an ExchangeNode that receives its
   * input from childFragment.
   */
  private void connectChildFragment(PlanNode node, int childIdx,
      PlanFragment parentFragment, PlanFragment childFragment) {
    PlanNode child = node.getChild(childIdx);
    PlanNode exchangeNode =
        new ExchangeNode(new PlanNodeId(nodeIdGenerator), child, false);
    node.setChild(childIdx, exchangeNode);
    childFragment.setDestination(parentFragment, exchangeNode.getId());
  }

  /**
   * Create a new fragment containing a single ExchangeNode that consumes the output
   * of childFragment, and set the destination of childFragment to the new parent.
   */
  private PlanFragment createParentFragment(
      PlanFragment childFragment, DataPartition partition) {
    PlanNode exchangeNode = new ExchangeNode(
        new PlanNodeId(nodeIdGenerator), childFragment.getPlanRoot(), false);
    PlanFragment parentFragment = new PlanFragment(exchangeNode, partition);
    childFragment.setDestination(parentFragment, exchangeNode.getId());
    return parentFragment;
  }

  /**
   * Returns a fragment that materializes the aggregation result of 'node'.
   * If partitionAgg is true, the result fragment will be partitioned on the
   * grouping exprs of 'node'; if partitionAgg is false, the result fragment will be
   * unpartitioned.
   * If 'node' is phase 1 of a 2-phase DISTINCT aggregation, this will simply
   * add 'node' to the child fragment and return the child fragment; the new
   * fragment will be created by the subsequent call of createAggregationFragment()
   * for the phase 2 AggregationNode.
   */
  private PlanFragment createAggregationFragment(AggregationNode node,
      PlanFragment childFragment, boolean partitionAgg,
      ArrayList<PlanFragment> fragments) {
    if (!childFragment.isPartitioned()) {
      // nothing to distribute; do full aggregation directly within childFragment
      childFragment.addPlanRoot(node);
      return childFragment;
    }

    if (node.getAggInfo().isDistinctAgg()) {
      // 'node' is phase 1 of a DISTINCT aggregation; the actual agg fragment
      // will get created in the next createAggregationFragment() call
      // for the parent AggregationNode
      childFragment.addPlanRoot(node);
      return childFragment;
    }

    // if we're not doing a grouping aggregation, there's no point in partitioning it
    ArrayList<Expr> groupingExprs = node.getAggInfo().getGroupingExprs();
    if (groupingExprs.isEmpty()) {
      partitionAgg = false;
    }

    DataPartition partition = null;
    if (partitionAgg) {
      partition = new DataPartition(TPartitionType.HASH_PARTITIONED, groupingExprs);
    } else {
      partition = DataPartition.UNPARTITIONED;
    }

    // is 'node' the 2nd phase of a DISTINCT aggregation?
    boolean is2ndPhaseDistinctAgg =
        node.getChild(0) instanceof AggregationNode
          && ((AggregationNode)(node.getChild(0))).getAggInfo().isDistinctAgg();

    if (is2ndPhaseDistinctAgg) {
      Preconditions.checkState(node.getChild(0) == childFragment.getPlanRoot());
      // place a merge aggregation step for the 1st phase in a new fragment
      PlanFragment aggFragment = createParentFragment(childFragment, partition);
      AggregateInfo mergeAggInfo =
          ((AggregationNode)(node.getChild(0))).getAggInfo().getMergeAggInfo();
      AggregationNode mergeAggNode =
          new AggregationNode(
            new PlanNodeId(nodeIdGenerator), node.getChild(0), mergeAggInfo);
      aggFragment.addPlanRoot(mergeAggNode);
      // the 2nd-phase aggregation consumes the output of the merge agg;
      // if there is a limit, it had already been placed with the 2nd aggregation
      // step (which is where it should be)
      aggFragment.addPlanRoot(node);

      // TODO: transfer having predicates
      return aggFragment;
    } else {
      // place the original aggregation in the child fragment
      childFragment.addPlanRoot(node);
      // if there is a limit, we need to transfer it from the pre-aggregation
      // node in the child fragment to the merge aggregation node in the parent
      long limit = node.getLimit();
      node.unsetLimit();
      // place a merge aggregation step in a new fragment
      PlanFragment aggFragment = createParentFragment(childFragment, partition);
      AggregationNode mergeAggNode =
          new AggregationNode(
            new PlanNodeId(nodeIdGenerator), aggFragment.getPlanRoot(),
            node.getAggInfo().getMergeAggInfo());
      mergeAggNode.setLimit(limit);
      aggFragment.addPlanRoot(mergeAggNode);

      // HAVING predicates can only be evaluated after the merge agg step
      node.transferConjuncts(mergeAggNode);

      return aggFragment;
    }
  }

  /**
   * Returns a fragment that outputs the result of 'node'.
   * - if the child fragment is unpartitioned, adds the top-n computation to the child
   *   fragment
   * - otherwise it creates a new unpartitioned fragment that merges
   *   the output of the child and does the top-n computation
   *
   * TODO: recognize whether the child fragment's partition is compatible with the
   * required partition for a distributed top-n computation; doing a distributed
   * top-n computation doesn't save cycles, but the pre-aggregation step reduces the
   * output.
   */
  private PlanFragment createTopnFragment(SortNode node,
      PlanFragment childFragment, ArrayList<PlanFragment> fragments) {
    if (!childFragment.isPartitioned()) {
      childFragment.addPlanRoot(node);
      return childFragment;
    }

    // we're doing top-n in a single unpartitioned new fragment
    // that merges the output of childFragment
    PlanFragment result = createMergeFragment(childFragment);
    PlanNode exchNode = result.getPlanRoot().findFirstOf(ExchangeNode.class);
    Preconditions.checkState(exchNode != null);
    result.addPlanRoot(node);
    childFragment.setDestination(result, exchNode.getId());
    childFragment.setOutputPartition(DataPartition.UNPARTITIONED);
    return result;
  }

  /**
   * Create plan tree for single-node execution.
   */
  private PlanNode createSingleNodePlan(QueryStmt stmt, Analyzer analyzer)
      throws NotImplementedException, InternalException {
    if (stmt instanceof SelectStmt) {
      return createSelectPlan((SelectStmt) stmt, analyzer);
    } else {
      Preconditions.checkState(stmt instanceof UnionStmt);
      return createUnionPlan((UnionStmt) stmt, analyzer);
    }
  }

  /**
   * Create tree of PlanNodes that implements the Select/Project/Join/Group by/Having
   * of the selectStmt query block.
   * @throws NotImplementedException if selectStmt contains Order By clause w/o Limit
   */
  private PlanNode createSelectPlan(SelectStmt selectStmt, Analyzer analyzer)
      throws NotImplementedException, InternalException {
    if (selectStmt.getTableRefs().isEmpty()) {
      // no from clause -> nothing to plan
      return null;
    }
    // collect ids of tuples materialized by the subtree that includes all joins
    // and scans
    ArrayList<TupleId> rowTuples = Lists.newArrayList();
    for (TableRef tblRef: selectStmt.getTableRefs()) {
      rowTuples.addAll(tblRef.getMaterializedTupleIds());
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
      // TODO: only use topN if the memory footprint is expected to be low;
      // how to account for strings?
      throw new NotImplementedException(
          "ORDER BY without LIMIT currently not supported");
    }

    // add aggregation, if required
    AggregateInfo aggInfo = selectStmt.getAggInfo();
    if (aggInfo != null) {
      root = new AggregationNode(new PlanNodeId(nodeIdGenerator), root, aggInfo);
      // if we're computing DISTINCT agg fns, the analyzer already created the
      // 2nd phase agginfo
      if (aggInfo.isDistinctAgg()) {
        root = new AggregationNode(
            new PlanNodeId(nodeIdGenerator), root,
            aggInfo.getSecondPhaseDistinctAggInfo());
      }
    }
    // add Having clause, if required
    if (selectStmt.getHavingPred() != null) {
      Preconditions.checkState(root instanceof AggregationNode);
      root.setConjuncts(selectStmt.getHavingPred().getConjuncts());
      analyzer.markConjunctsAssigned(selectStmt.getHavingPred().getConjuncts());
    }

    // add order by and limit
    SortInfo sortInfo = selectStmt.getSortInfo();
    if (sortInfo != null) {
      Preconditions.checkState(selectStmt.getLimit() != -1);
      root = new SortNode(new PlanNodeId(nodeIdGenerator), root, sortInfo, true);
    }
    root.setLimit(selectStmt.getLimit());

    // All the conjuncts in the inline view analyzer should be assigned
    Preconditions.checkState(!analyzer.hasUnassignedConjuncts());

    return root;
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
    List<Predicate> boundConjuncts =
        analyzer.getBoundConjuncts(inlineViewRef.getMaterializedTupleIds());

    // TODO (alan): this is incorrect for left outer join. Predicate should not be
    // evaluated after the join.

    if (inlineViewRef.getViewStmt() instanceof UnionStmt) {
      // Register predicates with the inline view's analyzer
      // such that the topmost merge node evaluates them.
      inlineViewRef.getAnalyzer().registerConjuncts(boundConjuncts);
      analyzer.markConjunctsAssigned(boundConjuncts);
      return createUnionPlan((UnionStmt) inlineViewRef.getViewStmt(),
          inlineViewRef.getAnalyzer());
    }

    Preconditions.checkState(inlineViewRef.getViewStmt() instanceof SelectStmt);
    SelectStmt selectStmt = (SelectStmt) inlineViewRef.getViewStmt();

    // If the view select statement does not compute aggregates, predicates are
    // registered with the inline view's analyzer for predicate pushdown.
    if (selectStmt.getAggInfo() == null) {
      inlineViewRef.getAnalyzer().registerConjuncts(boundConjuncts);
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
   */
  private PlanNode createScanNode(Analyzer analyzer, TableRef tblRef) {
    ScanNode scanNode = null;

    if (tblRef.getTable() instanceof HdfsTable) {
      scanNode = new HdfsScanNode(new PlanNodeId(nodeIdGenerator), tblRef.getDesc(),
          (HdfsTable)tblRef.getTable());
    } else {
      // HBase table
      scanNode = new HBaseScanNode(new PlanNodeId(nodeIdGenerator), tblRef.getDesc());
    }

    // TODO (alan): this is incorrect for left outer joins. Predicate should not be
    // evaluated after the join.

    List<Predicate> conjuncts = analyzer.getBoundConjuncts(tblRef.getId().asList());
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
    List<TupleId> rhsIds = rhs.getMaterializedTupleIds();
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
    inner.rowTupleIds = Lists.newArrayList(innerRef.getMaterializedTupleIds());

    List<Pair<Expr, Expr>> eqJoinConjuncts = Lists.newArrayList();
    List<Predicate> eqJoinPredicates = Lists.newArrayList();
    getHashLookupJoinConjuncts(
        analyzer, outer.getTupleIds(), innerRef, eqJoinConjuncts, eqJoinPredicates);
    if (eqJoinPredicates.isEmpty()) {
      throw new NotImplementedException(
          "Join requires at least one equality predicate between the two tables.");
    }
    HashJoinNode result =
        new HashJoinNode(
            new PlanNodeId(nodeIdGenerator), outer, inner, innerRef.getJoinOp(),
            eqJoinConjuncts, innerRef.getOtherJoinConjuncts());
    analyzer.markConjunctsAssigned(eqJoinPredicates);

    // The remaining conjuncts that are bound by result.getTupleIds()
    // need to be evaluated explicitly by the hash join.
    ArrayList<Predicate> conjuncts =
      new ArrayList<Predicate>(analyzer.getBoundConjuncts(result.getTupleIds()));
    result.setConjuncts(conjuncts);
    analyzer.markConjunctsAssigned(conjuncts);
    return result;
  }

  /**
   * Create a tree of PlanNodes for the given tblRef, which can be a BaseTableRef or a
   * InlineViewRef
   */
  private PlanNode createTableRefNode(Analyzer analyzer, TableRef tblRef)
      throws NotImplementedException, InternalException {
    if (tblRef instanceof BaseTableRef) {
      return createScanNode(analyzer, tblRef);
    }
    if (tblRef instanceof InlineViewRef) {
      return createInlineViewPlan(analyzer, (InlineViewRef) tblRef);
    }
    throw new InternalException("unknown TableRef node");
  }

  /**
   * Creates the plan for a union stmt in three phases:
   * 1. If present, absorbs all DISTINCT-qualified operands into a single merge node,
   *    and adds an aggregation node on top to remove duplicates.
   * 2. If present, absorbs all ALL-qualified operands into a single merge node,
   *    also adding the subplan generated in 1 (if applicable).
   * 3. Set conjuncts if necessary, and add order by and limit.
   * The absorption of operands applies unnesting rules.
   */
  private PlanNode createUnionPlan(UnionStmt unionStmt, Analyzer analyzer)
      throws NotImplementedException, InternalException {
    List<UnionOperand> operands = unionStmt.getUnionOperands();
    Preconditions.checkState(operands.size() > 1);
    TupleDescriptor tupleDesc =
        analyzer.getDescTbl().getTupleDesc(unionStmt.getTupleId());

    MergeNode mergeNode = new MergeNode(new PlanNodeId(nodeIdGenerator), tupleDesc);
    PlanNode result = mergeNode;
    absorbUnionOperand(operands.get(0), mergeNode, operands.get(1).getQualifier());

    // Put DISTINCT operands into a single mergeNode.
    // Later, we'll put an agg node on top for duplicate removal.
    boolean hasDistinct = false;
    int opIx = 1;
    while (opIx < operands.size()) {
      UnionOperand operand = operands.get(opIx);
      if (operand.getQualifier() != Qualifier.DISTINCT) {
        break;
      }
      hasDistinct = true;
      absorbUnionOperand(operand, mergeNode, Qualifier.DISTINCT);
      ++opIx;
    }

    // If we generated a merge node for DISTINCT-qualified operands,
    // add an agg node on top to remove duplicates.
    AggregateInfo aggInfo = null;
    if (hasDistinct) {
      ArrayList<Expr> groupingExprs = Expr.cloneList(unionStmt.getResultExprs(), null);
      // Aggregate produces exactly the same tuple as the original union stmt.
      try {
        aggInfo =
            AggregateInfo.create(groupingExprs, null,
              analyzer.getDescTbl().getTupleDesc(unionStmt.getTupleId()), analyzer);
      } catch (AnalysisException e) {
        // this should never happen
        throw new InternalException("error creating agg info in createUnionPlan()");
      }
      // aggInfo.aggTupleSMap is empty, which happens to be correct in this case,
      // because this aggregation is only removing duplicates
      result = new AggregationNode(new PlanNodeId(nodeIdGenerator), mergeNode, aggInfo);
      // If there are more operands, then add the distinct subplan as a child
      // of a new merge node which also merges the remaining ALL-qualified operands.
      if (opIx < operands.size()) {
        mergeNode = new MergeNode(new PlanNodeId(nodeIdGenerator), tupleDesc);
        mergeNode.addChild(result, unionStmt.getResultExprs());
        result = mergeNode;
      }
    }

    // Put all ALL-qualified operands into a single mergeNode.
    // During analysis we propagated DISTINCT to the left. Therefore,
    // we should only encounter ALL qualifiers at this point.
    while (opIx < operands.size()) {
      UnionOperand operand = operands.get(opIx);
      Preconditions.checkState(operand.getQualifier() == Qualifier.ALL);
      absorbUnionOperand(operand, mergeNode, Qualifier.ALL);
      ++opIx;
    }

    // A MergeNode may have predicates if a union is used inside an inline view,
    // and the enclosing select stmt has predicates on its columns.
    List<Predicate> conjuncts =
        analyzer.getBoundConjuncts(unionStmt.getTupleId().asList());
    // If the topmost node is an agg node, then set the conjuncts on its first child
    // (which must be a MergeNode), to evaluate the conjuncts as early as possible.
    if (!conjuncts.isEmpty() && result instanceof AggregationNode) {
      Preconditions.checkState(result.getChild(0) instanceof MergeNode);
      result.getChild(0).setConjuncts(conjuncts);
    } else {
      result.setConjuncts(conjuncts);
    }

    // Add order by and limit if present.
    SortInfo sortInfo = unionStmt.getSortInfo();
    if (sortInfo != null) {
      if (unionStmt.getLimit() == -1) {
        throw new NotImplementedException(
            "ORDER BY without LIMIT currently not supported");
      }
      result = new SortNode(new PlanNodeId(nodeIdGenerator), result, sortInfo, true);
    }
    result.setLimit(unionStmt.getLimit());

    return result;
  }

  /**
   * Absorbs the given operand into the topMergeNode, as follows:
   * 1. Operand's query stmt is a select stmt: Generate its plan
   *    and add it into topMergeNode.
   * 2. Operand's query stmt is a union stmt:
   *    Apply unnesting rules, i.e., check if the union stmt's operands
   *    can be directly added into the topMergeNode
   *    If unnesting is possible then absorb the union stmt's operands into topMergeNode,
   *    otherwise generate the union stmt's subplan and add it into the topMergeNode.
   * topQualifier refers to the qualifier of original operand which
   * was passed to absordUnionOperand() (i.e., at the root of the recursion)
   */
  private void absorbUnionOperand(UnionOperand operand, MergeNode topMergeNode,
      Qualifier topQualifier) throws NotImplementedException, InternalException {
    QueryStmt queryStmt = operand.getQueryStmt();
    Analyzer analyzer = operand.getAnalyzer();
    if (queryStmt instanceof SelectStmt) {
      SelectStmt selectStmt = (SelectStmt) queryStmt;
      PlanNode selectPlan = createSelectPlan(selectStmt, analyzer);
      if (selectPlan == null) {
        // Select with no FROM clause.
        topMergeNode.addConstExprList(selectStmt.getResultExprs());
      } else {
        topMergeNode.addChild(selectPlan, selectStmt.getResultExprs());
      }
      return;
    }

    Preconditions.checkState(queryStmt instanceof UnionStmt);
    UnionStmt unionStmt = (UnionStmt) queryStmt;
    List<UnionOperand> unionOperands = unionStmt.getUnionOperands();
    // We cannot recursively absorb this union stmt's operands if either:
    // 1. The union stmt has a limit.
    // 2. Or the top qualifier is ALL and the first operand qualifier is not ALL.
    // Note that the first qualifier is ALL iff all operand qualifiers are ALL,
    // because DISTINCT is propagated to the left during analysis.
    if (unionStmt.hasLimitClause() || (topQualifier == Qualifier.ALL &&
        unionOperands.get(1).getQualifier() != Qualifier.ALL)) {
      PlanNode node = createUnionPlan(unionStmt, analyzer);

      // If node is a MergeNode then it means it's operands are mixed ALL/DISTINCT.
      // We cannot directly absorb it's operands, but we can safely add
      // the MergeNode's children to topMergeNode if the UnionStmt has no limit.
      if (node instanceof MergeNode && !unionStmt.hasLimitClause()) {
        MergeNode mergeNode = (MergeNode) node;
        topMergeNode.getChildren().addAll(mergeNode.getChildren());
        topMergeNode.getResultExprLists().addAll(mergeNode.getResultExprLists());
        topMergeNode.getConstExprLists().addAll(mergeNode.getConstExprLists());
      } else {
        topMergeNode.addChild(node, unionStmt.getResultExprs());
      }
    } else {
      for (UnionOperand nestedOperand : unionStmt.getUnionOperands()) {
        absorbUnionOperand(nestedOperand, topMergeNode, topQualifier);
      }
    }
  }

}
