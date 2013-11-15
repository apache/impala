// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AggregateInfo;
import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BaseTableRef;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.EquivalenceClassId;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.InlineViewRef;
import com.cloudera.impala.analysis.InsertStmt;
import com.cloudera.impala.analysis.JoinOperator;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.SortInfo;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.analysis.UnionStmt;
import com.cloudera.impala.analysis.UnionStmt.Qualifier;
import com.cloudera.impala.analysis.UnionStmt.UnionOperand;
import com.cloudera.impala.analysis.ValuesStmt;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.common.RuntimeEnv;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPartitionType;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TQueryOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/**
 * The planner is responsible for turning parse trees into plan fragments that
 * can be shipped off to backends for execution.
 *
 */
public class Planner {
  private final static Logger LOG = LoggerFactory.getLogger(Planner.class);

  // Estimate of the overhead imposed by storing data in a hash tbl;
  // used for determining whether a broadcast join is feasible.
  public final static double HASH_TBL_SPACE_OVERHEAD = 1.1;

  private final IdGenerator<PlanNodeId> nodeIdGenerator = new IdGenerator<PlanNodeId>();

  /**
   * Create plan fragments for an analyzed statement, given a set of execution options.
   * The fragments are returned in a list such that element i of that list can
   * only consume output of the following fragments j > i.
   */
  public ArrayList<PlanFragment> createPlanFragments(
      AnalysisContext.AnalysisResult analysisResult, TQueryOptions queryOptions)
      throws NotImplementedException, InternalException, AuthorizationException {
    // Set queryStmt from analyzed SELECT or INSERT query.
    QueryStmt queryStmt = null;
    if (analysisResult.isInsertStmt() ||
        analysisResult.isCreateTableAsSelectStmt()) {
      queryStmt = analysisResult.getInsertStmt().getQueryStmt();
    } else {
      queryStmt = analysisResult.getQueryStmt();
    }
    Analyzer analyzer = analysisResult.getAnalyzer();

    LOG.debug("desctbl: " + analyzer.getDescTbl().debugString());
    analyzer.computeEquivClasses();
    LOG.debug("create single-node plan");
    PlanNode singleNodePlan =
        createQueryPlan(queryStmt, analyzer, queryOptions.getDefault_order_by_limit());
    Preconditions.checkNotNull(singleNodePlan);
    // compute referenced slots before calling computeMemLayout()
    markRefdSlots(analyzer, singleNodePlan, queryStmt.getResultExprs());
    // compute mem layout *before* finalize(); finalize() may reference
    // TupleDescriptor.avgSerializedSize
    analyzer.getDescTbl().computeMemLayout();
    singleNodePlan.finalize(analyzer);

    ArrayList<PlanFragment> fragments = Lists.newArrayList();
    if (queryOptions.num_nodes == 1) {
      // single-node execution; we're almost done
      singleNodePlan = addUnassignedConjuncts(analyzer, singleNodePlan);
      fragments.add(new PlanFragment(singleNodePlan, DataPartition.UNPARTITIONED));
    } else {
      // For inserts, unless there is a limit clause, leave the root fragment
      // partitioned, otherwise merge everything into a single coordinator fragment,
      // so we can pass it back to the client.
      boolean isPartitioned = false;
      if (analysisResult.isInsertStmt() && !queryStmt.hasLimitClause()) {
        isPartitioned = true;
      }
      LOG.debug("create plan fragments");
      long perNodeMemLimit = queryOptions.mem_limit;
      LOG.debug("memlimit=" + Long.toString(perNodeMemLimit));
      createPlanFragments(
          singleNodePlan, analyzer, isPartitioned, perNodeMemLimit, fragments);
    }

    PlanFragment rootFragment = fragments.get(fragments.size() - 1);
    if (analysisResult.isInsertStmt() ||
        analysisResult.isCreateTableAsSelectStmt()) {
      InsertStmt insertStmt = analysisResult.getInsertStmt();
      if (queryOptions.num_nodes != 1) {
        // repartition on partition keys
        rootFragment = repartitionForInsert(
            rootFragment, insertStmt.getPartitionKeyExprs(), analyzer, fragments);
      }

      // set up table sink for root fragment
      rootFragment.setSink(insertStmt.createDataSink());
    }
    rootFragment.setOutputExprs(queryStmt.getResultExprs());

    LOG.debug("finalize plan fragments");
    for (PlanFragment fragment: fragments) {
      fragment.finalize(analyzer, !queryOptions.allow_unsupported_formats);
    }

    Collections.reverse(fragments);
    return fragments;
  }

  /**
   * Mark slots that are being referenced by the plan tree itself or by the
   * outputExprs exprs as materialized. If the latter is null, mark all slots in
   * planRoot's tupleIds() as being referenced. All aggregate slots are materialized.
   *
   * TODO: instead of materializing everything produced by the plan root, derived
   * referenced slots from destination fragment and add a materialization node
   * if not all output is needed by destination fragment
   * TODO 2: should the materialization decision be cost-based?
   */
  private void markRefdSlots(
      Analyzer analyzer, PlanNode planRoot, ArrayList<Expr> outputExprs) {
    if (planRoot == null) return;
    List<SlotId> refdIdList = Lists.newArrayList();
    planRoot.getMaterializedIds(analyzer, refdIdList);

    if (outputExprs != null) {
      Expr.getIds(outputExprs, null, refdIdList);
    }

    HashSet<SlotId> refdIds = Sets.newHashSet(refdIdList);
    for (TupleDescriptor tupleDesc: analyzer.getDescTbl().getTupleDescs()) {
      for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
        if (refdIds.contains(slotDesc.getId())) {
          slotDesc.setIsMaterialized(true);
        }
      }
    }

    if (outputExprs == null) {
      // mark all slots in planRoot.getTupleIds() as materialized
      ArrayList<TupleId> tids = planRoot.getTupleIds();
      for (TupleId tid: tids) {
        TupleDescriptor tupleDesc = analyzer.getDescTbl().getTupleDesc(tid);
        for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
          slotDesc.setIsMaterialized(true);
        }
      }
    }
  }

  /**
   * Return combined explain string for all plan fragments.
   * Includes the estimated resource requirements from the request if set.
   */
  public String getExplainString(ArrayList<PlanFragment> fragments,
      TQueryExecRequest request, TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
    if (request.isSetPer_host_mem_req() && request.isSetPer_host_vcores()
        && explainLevel == TExplainLevel.VERBOSE) {
      str.append(
          String.format("Estimated Per-Host Requirements: Memory=%s VCores=%s\n\n",
          PrintUtils.printBytes(request.getPer_host_mem_req()),
          request.per_host_vcores));
    }
    for (int i = 0; i < fragments.size(); ++i) {
      PlanFragment fragment = fragments.get(i);
      if (i > 0) {
        // a blank line between plan fragments
        str.append("\n");
      }
      str.append("PLAN FRAGMENT " + i + "\n");
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
   * partitioned; the partition function is derived from the inputs.
   */
  private PlanFragment createPlanFragments(
      PlanNode root, Analyzer analyzer, boolean isPartitioned,
      long perNodeMemLimit, ArrayList<PlanFragment> fragments)
      throws InternalException, NotImplementedException {
    ArrayList<PlanFragment> childFragments = Lists.newArrayList();
    for (PlanNode child: root.getChildren()) {
      // allow child fragments to be partitioned, unless they contain a limit clause
      // (the result set with the limit constraint needs to be computed centrally);
      // merge later if needed
      boolean childIsPartitioned = !child.hasLimit();
      childFragments.add(
          createPlanFragments(
            child, analyzer, childIsPartitioned, perNodeMemLimit, fragments));
    }

    PlanFragment result = null;
    if (root instanceof ScanNode) {
      result = createScanFragment(root);
      fragments.add(result);
    } else if (root instanceof HashJoinNode) {
      Preconditions.checkState(childFragments.size() == 2);
      result = createHashJoinFragment(
          (HashJoinNode) root, childFragments.get(1), childFragments.get(0),
          perNodeMemLimit, fragments);
    } else if (root instanceof SelectNode) {
      result = createSelectNodeFragment((SelectNode) root, childFragments, analyzer);
    } else if (root instanceof MergeNode) {
      result = createMergeNodeFragment((MergeNode) root, childFragments, fragments,
          analyzer);
    } else if (root instanceof AggregationNode) {
      result = createAggregationFragment(
          (AggregationNode) root, childFragments.get(0), fragments, analyzer);
    } else if (root instanceof SortNode) {
      result =
          createTopnFragment((SortNode) root, childFragments.get(0), fragments, analyzer);
    } else {
      throw new InternalException(
          "Cannot create plan fragment for this node type: " + root.getExplainString());
    }
    // move 'result' to end, it depends on all of its children
    fragments.remove(result);
    fragments.add(result);

    if (!isPartitioned && result.isPartitioned()) {
      result = createMergeFragment(result, analyzer);
      fragments.add(result);
    }

    return result;
  }

  /**
   * Returns the product of the distinct value estimates of the individual exprs
   * or -1 if any of them doesn't have a distinct value estimate.
   */
  private long getNumDistinctValues(List<Expr> exprs) {
    long result = 1;
    for (Expr expr: exprs) {
      result *= expr.getNumDistinctValues();
      if (result < 0) return -1;
    }
    return result;
  }

  /**
   * Returns plan fragment that partitions the output of 'inputFragment' on
   * partitionExprs, unless the expected number of partitions is less than the number
   * of nodes on which inputFragment runs.
   * If it ends up creating a new fragment, appends that to 'fragments'.
   */
  private PlanFragment repartitionForInsert(
      PlanFragment inputFragment, List<Expr> partitionExprs, Analyzer analyzer,
      ArrayList<PlanFragment> fragments) {
    if (partitionExprs.isEmpty()) return inputFragment;

    // we ignore constants for the sake of partitioning
    List<Expr> nonConstPartitionExprs = Lists.newArrayList(partitionExprs);
    Expr.removeConstants(nonConstPartitionExprs);
    DataPartition inputPartition = inputFragment.getDataPartition();

    // do nothing if the input fragment is already partitioned on partitionExprs
    if (Expr.equalLists(inputPartition.getPartitionExprs(), nonConstPartitionExprs)) {
      return inputFragment;
    }

    // if the existing partition exprs are a subset of the table partition exprs, check
    // if it is distributed across all nodes; if so, don't repartition
    if (Expr.isSubset(inputPartition.getPartitionExprs(), nonConstPartitionExprs)) {
      long numPartitions = getNumDistinctValues(inputPartition.getPartitionExprs());
      if (numPartitions >= inputFragment.getNumNodes()) return inputFragment;
    }

    // don't repartition if the resulting number of partitions is too low to get good
    // parallelism
    long numPartitions = getNumDistinctValues(nonConstPartitionExprs);

    // don't repartition if we know we have fewer partitions than nodes
    // (ie, default to repartitioning if col stats are missing)
    // TODO: we want to repartition if the resulting files would otherwise
    // be very small (less than some reasonable multiple of the recommended block size);
    // in order to do that, we need to come up with an estimate of the avg row size
    // in the particular file format of the output table/partition.
    // We should always know on how many nodes our input is running.
    Preconditions.checkState(inputFragment.getNumNodes() != -1);
    if (numPartitions > 0 && numPartitions <= inputFragment.getNumNodes()) {
      return inputFragment;
    }

    ExchangeNode exchNode = new ExchangeNode(new PlanNodeId(nodeIdGenerator));
    exchNode.addChild(inputFragment.getPlanRoot(), false);
    exchNode.computeStats(analyzer);
    Preconditions.checkState(exchNode.hasValidStats());
    DataPartition partition =
        new DataPartition(TPartitionType.HASH_PARTITIONED, nonConstPartitionExprs);
    PlanFragment fragment = new PlanFragment(exchNode, partition);
    inputFragment.setDestination(fragment, exchNode.getId());
    inputFragment.setOutputPartition(partition);
    fragments.add(fragment);
    return fragment;
  }

  /**
   * Return unpartitioned fragment that merges the input fragment's output via
   * an ExchangeNode.
   * Requires that input fragment be partitioned.
   */
  private PlanFragment createMergeFragment(
      PlanFragment inputFragment, Analyzer analyzer) {
    Preconditions.checkState(inputFragment.isPartitioned());
    ExchangeNode mergePlan = new ExchangeNode(new PlanNodeId(nodeIdGenerator));
    mergePlan.addChild(inputFragment.getPlanRoot(), false);
    mergePlan.computeStats(analyzer);
    Preconditions.checkState(mergePlan.hasValidStats());
    PlanNodeId exchId = mergePlan.getId();
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
   * Creates either a broadcast join or a repartitioning join, depending on the
   * expected cost.
   * If any of the inputs to the cost computation is unknown, it assumes the cost
   * will be 0. Costs being equal, it'll favor partitioned over broadcast joins.
   * If perNodeMemLimit > 0 and the size of the hash table for a broadcast join is
   * expected to exceed that mem limit, switches to partitioned join instead.
   * TODO: revisit the choice of broadcast as the default
   * TODO: don't create a broadcast join if we already anticipate that this will
   * exceed the query's memory budget.
   */
  private PlanFragment createHashJoinFragment(
      HashJoinNode node, PlanFragment rightChildFragment,
      PlanFragment leftChildFragment, long perNodeMemLimit,
      ArrayList<PlanFragment> fragments) {
    // broadcast: send the rightChildFragment's output to each node executing
    // the leftChildFragment; the cost across all nodes is proportional to the
    // total amount of data sent
    PlanNode rhsTree = rightChildFragment.getPlanRoot();
    long rhsDataSize = 0;
    long broadcastCost = Long.MAX_VALUE;
    if (rhsTree.getCardinality() != -1 && leftChildFragment.getNumNodes() != -1) {
      rhsDataSize = Math.round(
          (double) rhsTree.getCardinality() * rhsTree.getAvgRowSize());
      broadcastCost = rhsDataSize * leftChildFragment.getNumNodes();
    }
    LOG.debug("broadcast: cost=" + Long.toString(broadcastCost));
    LOG.debug("card=" + Long.toString(rhsTree.getCardinality()) + " row_size="
        + Float.toString(rhsTree.getAvgRowSize()) + " #nodes="
        + Integer.toString(leftChildFragment.getNumNodes()));

    // repartition: both left- and rightChildFragment are partitioned on the
    // join exprs
    // TODO: take existing partition of input fragments into account to avoid
    // unnecessary repartitioning
    PlanNode lhsTree = leftChildFragment.getPlanRoot();
    long partitionCost = Long.MAX_VALUE;
    if (lhsTree.getCardinality() != -1 && rhsTree.getCardinality() != -1) {
      partitionCost = Math.round(
          (double) lhsTree.getCardinality() * lhsTree.getAvgRowSize()
          + (double) rhsTree.getCardinality() * rhsTree.getAvgRowSize());
    }
    LOG.debug("partition: cost=" + Long.toString(partitionCost));
    LOG.debug("lhs card=" + Long.toString(lhsTree.getCardinality()) + " row_size="
        + Float.toString(lhsTree.getAvgRowSize()));
    LOG.debug("rhs card=" + Long.toString(rhsTree.getCardinality()) + " row_size="
        + Float.toString(rhsTree.getAvgRowSize()));
    LOG.debug(rhsTree.getExplainString());

    boolean doBroadcast;
    // we do a broadcast join if
    // - we're explicitly told to do so
    // - or if it's cheaper and we weren't explicitly told to do a partitioned join
    // - and we're not doing a full or right outer join (those require the left-hand
    //   side to be partitioned for correctness)
    // - and the expected size of the hash tbl doesn't exceed perNodeMemLimit
    // we do a "<=" comparison of the costs so that we default to broadcast joins if
    // we're unable to estimate the cost
    if (node.getJoinOp() != JoinOperator.RIGHT_OUTER_JOIN
        && node.getJoinOp() != JoinOperator.FULL_OUTER_JOIN
        && (perNodeMemLimit == 0
            || Math.round((double) rhsDataSize * HASH_TBL_SPACE_OVERHEAD)
                <= perNodeMemLimit)
        && (node.getInnerRef().isBroadcastJoin()
            || (!node.getInnerRef().isPartitionJoin()
                && broadcastCost <= partitionCost))) {
      doBroadcast = true;
    } else {
      doBroadcast = false;
    }

    // The rhs tree is going to send data through an exchange node which effectively
    // compacts the data. No reason to do it again at the rhs root node.
    rhsTree.setCompactData(false);
    if (doBroadcast) {
      node.setDistributionMode(HashJoinNode.DistributionMode.BROADCAST);
      // Doesn't create a new fragment, but modifies leftChildFragment to execute
      // the join; the build input is provided by an ExchangeNode, which is the
      // destination of the rightChildFragment's output
      node.setChild(0, leftChildFragment.getPlanRoot());
      connectChildFragment(node, 1, leftChildFragment, rightChildFragment);
      leftChildFragment.setPlanRoot(node);
      return leftChildFragment;
    } else {
      node.setDistributionMode(HashJoinNode.DistributionMode.PARTITIONED);
      // Create a new parent fragment containing a HashJoin node with two
      // ExchangeNodes as inputs; the latter are the destinations of the
      // left- and rightChildFragments, which now partition their output
      // on their respective join exprs.
      // The new fragment is hash-partitioned on the lhs input join exprs.
      // TODO: create equivalence classes based on equality predicates

      // first, extract join exprs
      List<Pair<Expr, Expr>> eqJoinConjuncts = node.getEqJoinConjuncts();
      List<Expr> lhsJoinExprs = Lists.newArrayList();
      List<Expr> rhsJoinExprs = Lists.newArrayList();
      for (Pair<Expr, Expr> pair: eqJoinConjuncts) {
        // no remapping necessary
        lhsJoinExprs.add(pair.first.clone(null));
        rhsJoinExprs.add(pair.second.clone(null));
      }

      // create the parent fragment containing the HashJoin node
      DataPartition lhsJoinPartition =
          new DataPartition(TPartitionType.HASH_PARTITIONED,
                            Expr.cloneList(lhsJoinExprs, null));
      ExchangeNode lhsExchange = new ExchangeNode(new PlanNodeId(nodeIdGenerator));
      lhsExchange.addChild(leftChildFragment.getPlanRoot(), false);
      lhsExchange.computeStats(null);
      DataPartition rhsJoinPartition =
          new DataPartition(TPartitionType.HASH_PARTITIONED, rhsJoinExprs);
      ExchangeNode rhsExchange = new ExchangeNode(new PlanNodeId(nodeIdGenerator));
      rhsExchange.addChild(rightChildFragment.getPlanRoot(), false);
      rhsExchange.computeStats(null);
      node.setChild(0, lhsExchange);
      node.setChild(1, rhsExchange);
      PlanFragment joinFragment = new PlanFragment(node, lhsJoinPartition);

      // connect the child fragments
      leftChildFragment.setDestination(joinFragment, lhsExchange.getId());
      leftChildFragment.setOutputPartition(lhsJoinPartition);
      rightChildFragment.setDestination(joinFragment, rhsExchange.getId());
      rightChildFragment.setOutputPartition(rhsJoinPartition);

      return joinFragment;
    }
  }

  /**
   * Creates an unpartitioned fragment that merges the outputs of all of its children
   * (with a single ExchangeNode), corresponding to the 'mergeNode' of the
   * non-distributed plan.
   * Each of the child fragments receives a MergeNode as a new plan root (with
   * the child fragment's plan tree as its only input), so that each child
   * fragment's output is mapped onto the MergeNode's result tuple id.
   * TODO: if this is implementing a UNION DISTINCT, the parent of the mergeNode
   * is a duplicate-removing AggregationNode, which might make sense to apply
   * to the children as well, in order to reduce the amount of data that needs
   * to be sent to the parent; augment the planner to decide whether that would
   * reduce the runtime.
   * TODO: since the fragment that does the merge is unpartitioned, it can absorb
   * all child fragments that are also unpartitioned
   */
  private PlanFragment createMergeNodeFragment(MergeNode mergeNode,
      ArrayList<PlanFragment> childFragments, ArrayList<PlanFragment> fragments,
      Analyzer analyzer) {
    Preconditions.checkState(mergeNode.getChildren().size() == childFragments.size());

    // If the mergeNode only has constant exprs, return it in an unpartitioned fragment.
    if (mergeNode.getChildren().isEmpty()) {
      Preconditions.checkState(!mergeNode.getConstExprLists().isEmpty());
      return new PlanFragment(mergeNode, DataPartition.UNPARTITIONED);
    }

    // create an ExchangeNode to perform the merge operation of mergeNode;
    // the ExchangeNode retains the generic PlanNode parameters of mergeNode
    ExchangeNode exchNode = new ExchangeNode(new PlanNodeId(nodeIdGenerator));
    exchNode.addChild(mergeNode, true);
    PlanFragment parentFragment =
        new PlanFragment(exchNode, DataPartition.UNPARTITIONED);

    // we don't expect to be parallelizing a MergeNode that was inserted solely
    // to evaluate conjuncts (ie, that doesn't explicitly materialize its output)
    Preconditions.checkState(mergeNode.getTupleIds().size() == 1);

    for (int i = 0; i < childFragments.size(); ++i) {
      PlanFragment childFragment = childFragments.get(i);
      // create a clone of mergeNode; we want to keep the limit and conjuncts
      MergeNode childMergeNode =
          new MergeNode(new PlanNodeId(nodeIdGenerator), mergeNode);
      List<Expr> resultExprs =
          Expr.cloneList(mergeNode.getResultExprLists().get(i), null);
      childMergeNode.addChild(childFragment.getPlanRoot(), resultExprs);
      childFragment.setPlanRoot(childMergeNode);
      childFragment.setDestination(parentFragment, exchNode.getId());
      exchNode.addChild(childMergeNode, true);
    }

    // Add an unpartitioned child fragment with a MergeNode for the constant exprs.
    if (!mergeNode.getConstExprLists().isEmpty()) {
      MergeNode childMergeNode = new MergeNode(new PlanNodeId(nodeIdGenerator),
          mergeNode);
      childMergeNode.getConstExprLists().addAll(mergeNode.getConstExprLists());
      // Clear original constant exprs to make sure nobody else picks them up.
      mergeNode.getConstExprLists().clear();
      PlanFragment childFragment =
          new PlanFragment(childMergeNode, DataPartition.UNPARTITIONED);
      childFragment.setPlanRoot(childMergeNode);
      childFragment.setDestination(parentFragment, exchNode.getId());
      childFragments.add(childFragment);
      fragments.add(childFragment);
      exchNode.addChild(childMergeNode, true);
    }
    exchNode.computeStats(analyzer);

    return parentFragment;
  }

  /**
   * Adds the SelectNode as the new plan root to the child fragment and returns
   * the child fragment.
   */
  private PlanFragment createSelectNodeFragment(SelectNode selectNode,
      ArrayList<PlanFragment> childFragments, Analyzer analyzer) {
    Preconditions.checkState(selectNode.getChildren().size() == childFragments.size());
    PlanFragment childFragment = childFragments.get(0);
    // set the child explicitly, an ExchangeNode might have been inserted
    // (whereas selectNode.child[0] would point to the original child)
    selectNode.setChild(0, childFragment.getPlanRoot());
    childFragment.setPlanRoot(selectNode);
    return childFragment;
  }

  /**
   * Replace node's child at index childIdx with an ExchangeNode that receives its
   * input from childFragment.
   */
  private void connectChildFragment(PlanNode node, int childIdx,
      PlanFragment parentFragment, PlanFragment childFragment) {
    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId(nodeIdGenerator));
    exchangeNode.addChild(childFragment.getPlanRoot(), false);
    exchangeNode.computeStats(null);
    node.setChild(childIdx, exchangeNode);
    childFragment.setDestination(parentFragment, exchangeNode.getId());
  }

  /**
   * Create a new fragment containing a single ExchangeNode that consumes the output
   * of childFragment, set the destination of childFragment to the new parent
   * and the output partition of childFragment to that of the new parent.
   */
  private PlanFragment createParentAggFragment(
      PlanFragment childFragment, DataPartition parentPartition) {
    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId(nodeIdGenerator));
    exchangeNode.addChild(childFragment.getPlanRoot(), false);
    exchangeNode.computeStats(null);
    PlanFragment parentFragment = new PlanFragment(exchangeNode, parentPartition);
    exchangeNode.addChild(childFragment.getPlanRoot(), false);
    childFragment.setDestination(parentFragment, exchangeNode.getId());
    childFragment.setOutputPartition(parentPartition);
    return parentFragment;
  }

  /**
   * Returns a fragment that materializes the aggregation result of 'node'.
   * If the input fragment is partitioned, the result fragment will be partitioned on
   * the grouping exprs of 'node'.
   * If 'node' is phase 1 of a 2-phase DISTINCT aggregation, this will simply
   * add 'node' to the child fragment and return the child fragment; the new
   * fragment will be created by the subsequent call of createAggregationFragment()
   * for the phase 2 AggregationNode.
   */
  private PlanFragment createAggregationFragment(AggregationNode node,
      PlanFragment childFragment, ArrayList<PlanFragment> fragments, Analyzer analyzer) {
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

    ArrayList<Expr> groupingExprs = node.getAggInfo().getGroupingExprs();
    boolean hasGrouping = !groupingExprs.isEmpty();
    // 2nd phase of DISTINCT aggregation
    boolean isDistinct =
        node.getChild(0) instanceof AggregationNode
          && ((AggregationNode)(node.getChild(0))).getAggInfo().isDistinctAgg();

    if (!isDistinct) {
      // the original aggregation goes into the child fragment,
      // merge aggregation into a parent fragment
      childFragment.addPlanRoot(node);
      // if there is a limit, we need to transfer it from the pre-aggregation
      // node in the child fragment to the merge aggregation node in the parent
      long limit = node.getLimit();
      node.unsetLimit();
      node.unsetNeedsFinalize();

      DataPartition parentPartition = null;
      if (hasGrouping) {
        // the parent fragment is partitioned on the grouping exprs;
        // substitute grouping exprs to reference the *output* of the agg, not the input
        // TODO: add infrastructure so that all PlanNodes have smaps to make this
        // process of turning exprs into executable exprs less ad-hoc; might even want to
        // introduce another mechanism that simply records a mapping of slots
        List<Expr> partitionExprs =
            Expr.cloneList(groupingExprs, node.getAggInfo().getSMap());
        parentPartition =
            new DataPartition(TPartitionType.HASH_PARTITIONED, partitionExprs);
      } else {
        // the parent fragment is unpartitioned
        parentPartition = DataPartition.UNPARTITIONED;
      }

      // place a merge aggregation step in a new fragment
      PlanFragment mergeFragment =
          createParentAggFragment(childFragment, parentPartition);
      AggregationNode mergeAggNode =
          new AggregationNode(
            new PlanNodeId(nodeIdGenerator), mergeFragment.getPlanRoot(),
            node.getAggInfo().getMergeAggInfo());
      mergeAggNode.setLimit(limit);

      // HAVING predicates can only be evaluated after the merge agg step
      node.transferConjuncts(mergeAggNode);
      // Recompute stats after transferring the conjuncts (order is important).
      node.computeStats(analyzer);
      mergeFragment.getPlanRoot().computeStats(analyzer);
      mergeAggNode.computeStats(analyzer);
      // Set new plan root after updating stats.
      mergeFragment.addPlanRoot(mergeAggNode);

      return mergeFragment;
    }

    Preconditions.checkState(isDistinct);
    // The first-phase aggregation node is already in the child fragment.
    Preconditions.checkState(node.getChild(0) == childFragment.getPlanRoot());

    DataPartition mergePartition = null;
    if (hasGrouping) {
      // We need to do
      // - child fragment:
      //   * phase-1 aggregation
      // - merge fragment, hash-partitioned on grouping exprs:
      //   * merge agg of phase 1
      //   * phase 2 agg
      List<Expr> partitionExprs =
          Expr.cloneList(groupingExprs, node.getAggInfo().getSMap());
      mergePartition =
          new DataPartition(TPartitionType.HASH_PARTITIONED, partitionExprs);
    } else {
      // We need to do
      // - child fragment:
      //   * phase-1 aggregation
      // - merge fragment 1, hash-partitioned on distinct exprs:
      //   * merge agg of phase 1
      //   * phase 2 agg
      // - merge fragment 2, unpartitioned:
      //   * merge agg of phase 2
      List<Expr> distinctExprs =
          ((AggregationNode)(node.getChild(0))).getAggInfo().getGroupingExprs();
      List<Expr> partitionExprs =
          Expr.cloneList(
            distinctExprs, ((AggregationNode)(node.getChild(0))).getAggInfo().getSMap());
      mergePartition =
          new DataPartition(TPartitionType.HASH_PARTITIONED, partitionExprs);
    }

    // place a merge aggregation step for the 1st phase in a new fragment
    PlanFragment mergeFragment = createParentAggFragment(childFragment, mergePartition);
    AggregateInfo mergeAggInfo =
        ((AggregationNode)(node.getChild(0))).getAggInfo().getMergeAggInfo();
    AggregationNode mergeAggNode =
        new AggregationNode(
          new PlanNodeId(nodeIdGenerator), node.getChild(0), mergeAggInfo);
    mergeAggNode.unsetNeedsFinalize();
    mergeFragment.addPlanRoot(mergeAggNode);
    mergeAggNode.computeStats(analyzer);
    // the 2nd-phase aggregation consumes the output of the merge agg;
    // if there is a limit, it had already been placed with the 2nd aggregation
    // step (which is where it should be)
    mergeFragment.addPlanRoot(node);

    if (!hasGrouping) {
      // place the merge aggregation of the 2nd phase in an unpartitioned fragment;
      // add preceding merge fragment at end
      fragments.add(mergeFragment);

      node.unsetNeedsFinalize();
      mergeFragment =
          createParentAggFragment(mergeFragment, DataPartition.UNPARTITIONED);
      mergeAggInfo = node.getAggInfo().getMergeAggInfo();
      mergeAggNode =
          new AggregationNode(
            new PlanNodeId(nodeIdGenerator), node.getChild(0), mergeAggInfo);
      mergeAggNode.computeStats(analyzer);
      mergeFragment.addPlanRoot(mergeAggNode);
    }

    // TODO: transfer having predicates? (aren't they already in the 2nd-phase
    // agg node?)
    return mergeFragment;
  }

  /**
   * Returns a fragment that outputs the result of 'node'.
   * - adds the top-n computation to the child fragment
   * - if the child fragment is partitioned creates a new unpartitioned fragment that
   *   merges the output of the child and does another top-n computation
   */
  private PlanFragment createTopnFragment(SortNode node,
      PlanFragment childFragment, ArrayList<PlanFragment> fragments, Analyzer analyzer) {
    node.setChild(0, childFragment.getPlanRoot());
    childFragment.addPlanRoot(node);
    if (!childFragment.isPartitioned()) {
      return childFragment;
    }

    // we're doing top-n in a single unpartitioned new fragment
    // that merges the output of childFragment
    PlanFragment mergeFragment = createMergeFragment(childFragment, analyzer);
    // insert sort node that repeats the child's sort
    SortNode childSortNode = (SortNode) childFragment.getPlanRoot();
    LOG.debug("childsortnode limit: " + Long.toString(childSortNode.getLimit()) +
        " offset: " + Long.toString(childSortNode.getOffset()));
    Preconditions.checkState(childSortNode.hasLimit());
    PlanNode exchNode = mergeFragment.getPlanRoot();
    // the merging exchange node must not apply the limit (that's done by the
    // merging top-n)
    exchNode.unsetLimit();
    PlanNode mergeNode =
        new SortNode(new PlanNodeId(nodeIdGenerator), childSortNode, exchNode);

    // If there is an offset, it must be applied at the top-n. Child nodes do not apply
    // the offset, and instead must keep at least (limit+offset) rows so that the top-n
    // node does not miss any rows that should be in the top-n.
    long offset = childSortNode.getOffset();
    if (offset != 0) {
      long sortLimit = childSortNode.getLimit();
      childSortNode.unsetLimit();
      childSortNode.setLimit(sortLimit + offset);
      childSortNode.setOffset(0);
    }
    mergeNode.computeStats(analyzer);
    Preconditions.checkState(mergeNode.hasValidStats());
    mergeFragment.setPlanRoot(mergeNode);

    return mergeFragment;
  }

  /**
   * Create plan tree for single-node execution.
   */
  private PlanNode createQueryPlan(
      QueryStmt stmt, Analyzer analyzer, long defaultOrderByLimit)
      throws NotImplementedException, InternalException, AuthorizationException {
    if (stmt instanceof SelectStmt) {
      return createSelectPlan((SelectStmt) stmt, analyzer, defaultOrderByLimit);
    } else {
      Preconditions.checkState(stmt instanceof UnionStmt);
      return createUnionPlan((UnionStmt) stmt, analyzer);
    }
  }

  /**
   * If there are unassigned conjuncts, returns a SelectNode on top of root
   * that evaluate those conjuncts; otherwise returns root unchanged.
   */
  private PlanNode addUnassignedConjuncts(Analyzer analyzer, PlanNode root) {
    Preconditions.checkNotNull(root);
    List<Expr> conjuncts = analyzer.getUnassignedConjuncts(root.getTupleIds());
    if (conjuncts.isEmpty()) return root;
    // evaluate conjuncts in SelectNode
    SelectNode selectNode = new SelectNode(new PlanNodeId(nodeIdGenerator), root);
    selectNode.computeStats(analyzer);
    Preconditions.checkState(selectNode.hasValidStats());
    selectNode.getConjuncts().addAll(conjuncts);
    analyzer.markConjunctsAssigned(conjuncts);
    return selectNode;
  }

  /**
   * Create tree of PlanNodes that implements the Select/Project/Join/Group by/Having
   * of the selectStmt query block.
   * @throws NotImplementedException if selectStmt contains Order By clause w/o Limit
   *   and the query options don't contain a default limit
   */
  private PlanNode createSelectPlan(
      SelectStmt selectStmt, Analyzer analyzer, long defaultOrderByLimit)
      throws NotImplementedException, InternalException, AuthorizationException {
    // no from clause -> materialize the select's exprs with a MergeNode
    if (selectStmt.getTableRefs().isEmpty()) {
      return createConstantSelectPlan(selectStmt, analyzer);
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
    for (int i = 1; i < selectStmt.getTableRefs().size(); ++i) {
      TableRef outerRef = selectStmt.getTableRefs().get(i - 1);
      TableRef innerRef = selectStmt.getTableRefs().get(i);
      root = createHashJoinNode(analyzer, root, outerRef, innerRef);
      // Have the build side of a join copy data to a compact representation
      // in the tuple buffer.
      root.getChildren().get(1).setCompactData(true);
      assignConjuncts(root, analyzer);
    }

    if (selectStmt.getSortInfo() != null
        && selectStmt.getLimit() == -1 && defaultOrderByLimit == -1) {
      // TODO: only use topN if the memory footprint is expected to be low;
      // how to account for strings?
      throw new NotImplementedException(
          "ORDER BY without LIMIT currently not supported");
    }

    if (root != null) {
      // add unassigned conjuncts before aggregation
      // (scenario: agg input comes from an inline view which wasn't able to
      // evaluate all Where clause conjuncts from this scope)
      root = addUnassignedConjuncts(analyzer, root);
    }

    // add aggregation, if required
    AggregateInfo aggInfo = selectStmt.getAggInfo();
    if (aggInfo != null) {
      root = new AggregationNode(new PlanNodeId(nodeIdGenerator), root, aggInfo);
      root.computeStats(analyzer);
      Preconditions.checkState(root.hasValidStats());
      // if we're computing DISTINCT agg fns, the analyzer already created the
      // 2nd phase agginfo
      if (aggInfo.isDistinctAgg()) {
        ((AggregationNode)root).unsetNeedsFinalize();
        root = new AggregationNode(
            new PlanNodeId(nodeIdGenerator), root,
            aggInfo.getSecondPhaseDistinctAggInfo());
        root.computeStats(analyzer);
        Preconditions.checkState(root.hasValidStats());
      }
      // add Having clause
      assignConjuncts(root, analyzer);
    }

    // add order by and limit
    SortInfo sortInfo = selectStmt.getSortInfo();
    if (sortInfo != null) {
      Preconditions.checkState(selectStmt.getLimit() != -1 || defaultOrderByLimit != -1);
      boolean isDefaultLimit = (selectStmt.getLimit() == -1);
      root = new SortNode(new PlanNodeId(nodeIdGenerator), root, sortInfo, true,
          isDefaultLimit, selectStmt.getOffset());
      root.computeStats(analyzer);
      Preconditions.checkState(root.hasValidStats());
      // Don't assign conjuncts here. If this is the tree for an inline view, and
      // it contains a limit clause, we need to evaluate the conjuncts inherited
      // from the enclosing select block *after* the limit.
      // TODO: have HashJoinNode evaluate those conjuncts after receiving rows
      // from the build tree
      root.setLimit(
          selectStmt.getLimit() != -1 ? selectStmt.getLimit() : defaultOrderByLimit);
    } else {
      root.setLimit(selectStmt.getLimit());
    }

    // All the conjuncts should be assigned at this point.
    Preconditions.checkState(!analyzer.hasUnassignedConjuncts());

    return root;
  }

  /**
  * Returns a MergeNode that materializes the exprs of the constant selectStmt.
  * Replaces the resultExprs of the selectStmt with SlotRefs into the materialized tuple.
  */
 private PlanNode createConstantSelectPlan(SelectStmt selectStmt, Analyzer analyzer) {
   Preconditions.checkState(selectStmt.getTableRefs().isEmpty());
   ArrayList<Expr> resultExprs = selectStmt.getResultExprs();
   ArrayList<String> colLabels = selectStmt.getColLabels();
   // Create tuple descriptor for materialized tuple.
   TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor();
   tupleDesc.setIsMaterialized(true);
   MergeNode mergeNode =
       new MergeNode(new PlanNodeId(nodeIdGenerator), tupleDesc.getId());
   // Analysis guarantees that selects without a FROM clause only have constant exprs.
   mergeNode.addConstExprList(Lists.newArrayList(resultExprs));

   // Replace the select stmt's resultExprs with SlotRefs into tupleDesc.
   for (int i = 0; i < resultExprs.size(); ++i) {
     SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(tupleDesc);
     slotDesc.setLabel(colLabels.get(i));
     slotDesc.setType(resultExprs.get(i).getType());
     slotDesc.setStats(ColumnStats.fromExpr(resultExprs.get(i)));
     slotDesc.setIsMaterialized(true);
     SlotRef slotRef = new SlotRef(slotDesc);
     resultExprs.set(i, slotRef);
   }
   return mergeNode;
 }

  /**
   * Returns true if predicate can be correctly evaluated by a tree materializing
   * 'tupleIds', otherwise false:
   * - the predicate needs to be bound by the materialized tuple ids
   * - a Where clause predicate can only be correctly evaluated if for all outer-joined
   *   referenced tids the last join to outer-join this tid has been materialized
   */
  private boolean canEvalPredicate(
      List<TupleId> tupleIds, Expr e, Analyzer analyzer) {
    if (!e.isBound(tupleIds)) return false;
    if (!analyzer.isWhereClauseConjunct(e)) return true;
    ArrayList<TupleId> tids = Lists.newArrayList();
    e.getIds(tids, null);
    for (TupleId tid: tids) {
      TableRef rhsRef = analyzer.getLastOjClause(tid);
      if (rhsRef == null) {
        // this is not outer-joined; ignore
        continue;
      }
      if (!tupleIds.containsAll(rhsRef.getMaterializedTupleIds())
          || !tupleIds.containsAll(rhsRef.getLeftTblRef().getMaterializedTupleIds())) {
        // the last join to outer-join this tid hasn't been materialized yet
        return false;
      }
    }
    return true;
  }


  /**
   * Assign all unassigned conjuncts to node which can be correctly evaluated
   * by node. Ignores OJ conjuncts.
   */
  private void assignConjuncts(PlanNode node, Analyzer analyzer) {
    List<Expr> conjuncts = getUnassignedConjuncts(node, analyzer);
    node.addConjuncts(conjuncts);
    analyzer.markConjunctsAssigned(conjuncts);
  }

  /**
   * Return all unassigned conjuncts which can be correctly evaluated by node.
   * Ignores OJ conjuncts.
   */
  private List<Expr> getUnassignedConjuncts(PlanNode node, Analyzer analyzer) {
    List<Expr> conjuncts = Lists.newArrayList();
    for (Expr e: analyzer.getUnassignedConjuncts(node.getTupleIds())) {
      if (canEvalPredicate(node.getTupleIds(), e, analyzer)) {
        conjuncts.add(e);
      }
    }
    return conjuncts;
  }

  /**
   * Packages all conjuncts that are fully bound by 'd' into a SingleColumnFilter and
   * removes them from 'conjuncts'.
   * Returns constructed SingleColumnFilter or null if no suitable conjuncts were found.
   */
  private SingleColumnFilter createKeyFilter(
      SlotDescriptor d, List<Expr> conjuncts) {
    SingleColumnFilter filter = null;
    ListIterator<Expr> i = conjuncts.listIterator();
    while (i.hasNext()) {
      Expr e = i.next();
      if (e.isBound(d.getId())){
        if (filter == null) {
          filter = new SingleColumnFilter(d);
        }
        filter.addConjunct(e);
        i.remove();
      }
    }
    return filter;
  }

  /**
   * Transform '=', '<[=]' and '>[=]' comparisons for given slot into
   * ValueRange. Also removes those predicates which were used for the construction
   * of ValueRange from 'conjuncts'. Only looks at comparisons w/ constants
   * (ie, the bounds of the result can be evaluated with Expr::GetValue(NULL)).
   * If there are multiple competing comparison predicates that could be used
   * to construct a ValueRange, only the first one from each category is chosen.
   */
  private ValueRange createScanRange(SlotDescriptor d, List<Expr> conjuncts) {
    ListIterator<Expr> i = conjuncts.listIterator();
    ValueRange result = null;
    while (i.hasNext()) {
      Expr e = i.next();
      if (!(e instanceof BinaryPredicate)) continue;
      BinaryPredicate comp = (BinaryPredicate) e;
      if (comp.getOp() == BinaryPredicate.Operator.NE) continue;
      Expr slotBinding = comp.getSlotBinding(d.getId());
      if (slotBinding == null || !slotBinding.isConstant()) continue;

      if (comp.getOp() == BinaryPredicate.Operator.EQ) {
        i.remove();
        return ValueRange.createEqRange(slotBinding);
      }

      if (result == null) result = new ValueRange();

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
   * Returns plan tree for an inline view ref.
   */
  private PlanNode createInlineViewPlan(Analyzer analyzer, InlineViewRef inlineViewRef)
      throws NotImplementedException, InternalException, AuthorizationException {
    // If the subquery doesn't contain a limit clause, determine which conjuncts can be
    // evaluated inside the subquery tree;
    // if it does contain a limit clause, it's not correct to have the view plan
    // evaluate predicates from the enclosing scope.
    List<Expr> conjuncts = Lists.newArrayList();
    if (!inlineViewRef.getViewStmt().hasLimitClause()) {
      for (Expr e:
          analyzer.getUnassignedConjuncts(inlineViewRef.getMaterializedTupleIds())) {
        if (canEvalPredicate(inlineViewRef.getMaterializedTupleIds(), e, analyzer)) {
          conjuncts.add(e);
        }
      }
      inlineViewRef.getAnalyzer().registerConjuncts(conjuncts);
      analyzer.markConjunctsAssigned(conjuncts);
    }

    // Turn a constant select into a MergeNode that materializes the exprs.
    QueryStmt viewStmt = inlineViewRef.getViewStmt();
    if (viewStmt instanceof SelectStmt) {
      SelectStmt selectStmt = (SelectStmt) viewStmt;
      if (selectStmt.getTableRefs().isEmpty()) {
        // Analysis should have generated a tuple id into which to materialize the exprs.
        Preconditions.checkState(inlineViewRef.getMaterializedTupleIds().size() == 1);
        MergeNode mergeNode = new MergeNode(new PlanNodeId(nodeIdGenerator),
            inlineViewRef.getMaterializedTupleIds().get(0));
        mergeNode.getConstExprLists().add(selectStmt.getResultExprs());
        mergeNode.getConjuncts().addAll(conjuncts);
        return mergeNode;
      }
    }

    return createQueryPlan(inlineViewRef.getViewStmt(), inlineViewRef.getAnalyzer(), -1);
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

    List<Expr> conjuncts = getUnassignedConjuncts(scanNode, analyzer);
    // mark conjuncts assigned here; they will either end up inside a
    // SingleColumnFilter/ValueRange or will be evaluated directly by the node
    analyzer.markConjunctsAssigned(conjuncts);
    List<SingleColumnFilter> keyFilters = Lists.newArrayList();
    List<ValueRange> keyRanges = Lists.newArrayList();
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
        keyFilters.add(null);
        keyRanges.add(null);
      } else {
        // create SingleColumnFilter/ValueRange from conjuncts for slot; also removes
        // conjuncts that were used as input for filter
        if (scanNode instanceof HdfsScanNode) {
          keyFilters.add(createKeyFilter(slotDesc, conjuncts));
        } else {
          keyRanges.add(createScanRange(slotDesc, conjuncts));
        }
      }
    }

    if (scanNode instanceof HdfsScanNode) {
      ((HdfsScanNode)scanNode).setKeyFilters(keyFilters);
    } else {
      ((HBaseScanNode)scanNode).setKeyRanges(keyRanges);
    }
    scanNode.addConjuncts(conjuncts);

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
   * If no conjuncts are found, constructs them based on equivalence classes, where
   * possible. In that case, they are still returned through joinConjuncts, but
   * joinPredicates would be empty.
   */
  private void getHashLookupJoinConjuncts(
      Analyzer analyzer,
      List<TupleId> lhsIds, TableRef rhs,
      List<Pair<Expr, Expr>> joinConjuncts,
      List<Expr> joinPredicates) throws AuthorizationException {
    joinConjuncts.clear();
    joinPredicates.clear();
    TupleId rhsId = rhs.getId();
    List<TupleId> rhsIds = rhs.getMaterializedTupleIds();
    List<Expr> candidates;
    if (rhs.getJoinOp().isOuterJoin()) {
      // TODO: create test for this
      Preconditions.checkState(rhs.getOnClause() != null);
      candidates = analyzer.getEqJoinConjuncts(rhsId, rhs);
    } else {
      candidates = analyzer.getEqJoinConjuncts(rhsId, null);
    }
    if (candidates == null) {
      return;
    }

    // equivalence classes of eq predicates in joinPredicates
    Set<EquivalenceClassId> joinEquivClasses = Sets.newHashSet();

    for (Expr e: candidates) {
      // Ignore predicate if one of its children is a constant.
      if (e.getChild(0).isConstant() || e.getChild(1).isConstant()) continue;

      Expr rhsExpr = null;
      if (e.getChild(0).isBound(rhsIds)) {
        rhsExpr = e.getChild(0);
      } else {
        Preconditions.checkState(e.getChild(1).isBound(rhsIds));
        rhsExpr = e.getChild(1);
      }

      Expr lhsExpr = null;
      if (e.getChild(1).isBound(lhsIds)) {
        lhsExpr = e.getChild(1);
      } else if (e.getChild(0).isBound(lhsIds)) {
        lhsExpr = e.getChild(0);
      } else {
        // not an equi-join condition between lhsIds and rhsId
        continue;
      }

      // ignore predicates that express an equivalence relationship if that
      // relationship is already captured via another predicate; we still
      // return those predicates in joinPredicates so they get marked as assigned
      Pair<SlotId, SlotId> joinSlots = ((Predicate) e).getEqSlots();
      if (joinSlots != null) {
        EquivalenceClassId id1 = analyzer.getEquivClassId(joinSlots.first);
        EquivalenceClassId id2 = analyzer.getEquivClassId(joinSlots.second);
        // both slots need not be in the same equiv class, due to outer joins
        // null check: we don't have equiv classes for anything in subqueries
        if (id1 != null && id2 != null && id1.equals(id2)
            && joinEquivClasses.contains(id1)) {
          // record this so it gets marked as assigned later
          joinPredicates.add(e);
          continue;
        }
        joinEquivClasses.add(id1);
      }

      // e is a non-redundant join predicate
      Preconditions.checkState(lhsExpr != rhsExpr);
      joinPredicates.add(e);
      Pair<Expr, Expr> entry = Pair.create(lhsExpr, rhsExpr);
      joinConjuncts.add(entry);
    }
    if (!joinPredicates.isEmpty()) return;
    Preconditions.checkState(joinConjuncts.isEmpty());

    // construct joinConjunct entries derived from equivalence class membership
    List<SlotId> lhsSlotIds = Lists.newArrayList();
    for (SlotDescriptor slotDesc: rhs.getDesc().getSlots()) {
      analyzer.getEquivSlots(slotDesc.getId(), lhsIds, lhsSlotIds);
      if (!lhsSlotIds.isEmpty()) {
        SlotId lhsSlotId = lhsSlotIds.get(0);
        // construct a BinaryPredicates in order to get correct casting;
        // we only do this for one of the equivalent slots, all the other implied
        // equalities are redundant
        BinaryPredicate pred = new BinaryPredicate(BinaryPredicate.Operator.EQ,
            new SlotRef(analyzer.getDescTbl().getSlotDesc(lhsSlotId)),
            new SlotRef(analyzer.getDescTbl().getSlotDesc(slotDesc.getId())));
        // analyze() creates casts, if needed
        try {
          pred.analyze(analyzer);
        } catch(AnalysisException e) {
          throw new IllegalStateException(
              "constructed predicate failed analysis: " + pred.toSql());
        }
        joinConjuncts.add(new Pair<Expr, Expr>(pred.getChild(0), pred.getChild(1)));
      }
    }
  }

  /**
   * Create HashJoinNode to join outer with inner.
   */
  private PlanNode createHashJoinNode(
      Analyzer analyzer, PlanNode outer, TableRef outerRef, TableRef innerRef)
      throws NotImplementedException, InternalException, AuthorizationException {
    // the rows coming from the build node only need to have space for the tuple
    // materialized by that node
    PlanNode inner = createTableRefNode(analyzer, innerRef);

    List<Pair<Expr, Expr>> eqJoinConjuncts = Lists.newArrayList();
    List<Expr> eqJoinPredicates = Lists.newArrayList();
    getHashLookupJoinConjuncts(
        analyzer, outer.getTupleIds(), innerRef, eqJoinConjuncts, eqJoinPredicates);
    if (eqJoinConjuncts.isEmpty()) {
      throw new NotImplementedException(
          String.format("Join between '%s' and '%s' requires at least one " +
                        "conjunctive equality predicate between the two tables",
                        outerRef.getAliasAsName(), innerRef.getAliasAsName()));
    }
    analyzer.markConjunctsAssigned(eqJoinPredicates);

    List<Expr> ojConjuncts = Lists.newArrayList();
    if (innerRef.getJoinOp().isOuterJoin()) {
      // Also assign conjuncts from On clause. All remaining unassigned conjuncts
      // that can be evaluated by this join are assigned in createSelectPlan().
      ojConjuncts = analyzer.getUnassignedOjConjuncts(innerRef);
      analyzer.markConjunctsAssigned(ojConjuncts);
    }

    HashJoinNode result =
        new HashJoinNode(
            new PlanNodeId(nodeIdGenerator), outer, inner, innerRef,
            eqJoinConjuncts, ojConjuncts);
    return result;
  }

  /**
   * Create a tree of PlanNodes for the given tblRef, which can be a BaseTableRef or a
   * InlineViewRef
   */
  private PlanNode createTableRefNode(Analyzer analyzer, TableRef tblRef)
      throws NotImplementedException, InternalException, AuthorizationException {
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
      throws NotImplementedException, InternalException, AuthorizationException {
    List<UnionOperand> operands = unionStmt.getUnionOperands();
    Preconditions.checkState(operands.size() > 0);
    MergeNode mergeNode =
        new MergeNode(new PlanNodeId(nodeIdGenerator), unionStmt.getTupleId());
    PlanNode result = mergeNode;

    // Only a ValuesStmt can have a single union operand.
    if (operands.size() == 1) {
      Preconditions.checkState(unionStmt instanceof ValuesStmt);
      SelectStmt selectStmt = (SelectStmt) operands.get(0).getQueryStmt();
      mergeNode.addConstExprList(selectStmt.getResultExprs());
      addConjunctsToUnionPlan(result, unionStmt, analyzer);
      result = addSortAndLimitToUnionPlan(result, unionStmt);
      return result;
    }

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
        mergeNode =
            new MergeNode(new PlanNodeId(nodeIdGenerator), unionStmt.getTupleId());
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

    // Assign conjuncts, add sort and limit.
    addConjunctsToUnionPlan(result, unionStmt, analyzer);
    result = addSortAndLimitToUnionPlan(result, unionStmt);

    return result;
  }

  private void addConjunctsToUnionPlan(PlanNode result, UnionStmt unionStmt,
      Analyzer analyzer) {
    // A MergeNode may have predicates if a union is used inside an inline view,
    // and the enclosing select stmt has predicates on its columns.
    List<Expr> conjuncts =
        analyzer.getUnassignedConjuncts(unionStmt.getTupleId().asList());
    // If the topmost node is an agg node, then set the conjuncts on its first child
    // (which must be a MergeNode), to evaluate the conjuncts as early as possible.
    if (!conjuncts.isEmpty() && result instanceof AggregationNode) {
      Preconditions.checkState(result.getChild(0) instanceof MergeNode);
      result.getChild(0).addConjuncts(conjuncts);
    } else {
      result.addConjuncts(conjuncts);
    }
    analyzer.markConjunctsAssigned(conjuncts);
  }

  private PlanNode addSortAndLimitToUnionPlan(PlanNode result, UnionStmt unionStmt)
      throws NotImplementedException {
    // Add order by and limit if present.
    SortInfo sortInfo = unionStmt.getSortInfo();
    if (sortInfo != null) {
      if (unionStmt.getLimit() == -1) {
        throw new NotImplementedException(
            "ORDER BY without LIMIT currently not supported");
      }
      result = new SortNode(new PlanNodeId(nodeIdGenerator), result, sortInfo, true,
          false, unionStmt.getOffset());
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
      Qualifier topQualifier) throws NotImplementedException, InternalException,
      AuthorizationException {
    QueryStmt queryStmt = operand.getQueryStmt();
    Analyzer analyzer = operand.getAnalyzer();
    if (queryStmt instanceof SelectStmt) {
      SelectStmt selectStmt = (SelectStmt) queryStmt;
      if (selectStmt.getTableRefs().isEmpty()) {
        // Select with no FROM clause.
        topMergeNode.addConstExprList(selectStmt.getResultExprs());
      } else {
        PlanNode selectPlan = createSelectPlan(selectStmt, analyzer, -1);
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
        unionOperands.size() > 1 &&
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

  /**
   * Represents a set of PlanNodes and DataSinks that execute and consume resources
   * concurrently. PlanNodes and DataSinks in such a pipelined plan node set may belong
   * to different plan fragments because data is streamed across fragments.
   *
   * For example, a series of left-deep joins consists of two plan node sets. The first
   * set contains all build-side nodes. The second set contains the leftmost
   * scan. Both sets contain all join nodes because they execute and consume
   * resources during the build and probe phases. Similarly, all nodes below a 'blocking'
   * node (e.g, an AggregationNode) are placed into a differnet plan node set than the
   * nodes above it, but the blocking node itself belongs to both sets.
   */
  private class PipelinedPlanNodeSet {
    // Minimum per-host resource requirements to ensure that no plan node set can have
    // estimates of zero, even if the contained PlanNodes have estimates of zero.
    public static final long MIN_PER_HOST_MEM = 10 * 1024 * 1024;
    public static final int MIN_PER_HOST_VCORES = 1;

    // List of plan nodes that execute and consume resources concurrently.
    private final ArrayList<PlanNode> planNodes = Lists.newArrayList();

    // DataSinks that execute and consume resources concurrently.
    // Primarily used for estimating the cost of insert queries.
    private final List<DataSink> dataSinks = Lists.newArrayList();

    // Estimated per-host memory and CPU requirements.
    // Valid after computeResourceEstimates().
    private long perHostMem = MIN_PER_HOST_MEM;
    private int perHostVcores = MIN_PER_HOST_VCORES;

    public void add(PlanNode node) {
      Preconditions.checkNotNull(node.getFragment());
      planNodes.add(node);
    }

    public void addSink(DataSink sink) {
      Preconditions.checkNotNull(sink);
      dataSinks.add(sink);
    }

    /**
     * Computes the estimated per-host memory and CPU requirements of this plan node set.
     * Optionally excludes unpartitioned fragments from the estimation.
     * Returns true if at least one plan node was included in the estimation.
     * Otherwise returns false indicating the estimates are invalid.
     */
    public boolean computeResourceEstimates(boolean excludeUnpartitionedFragments,
        TQueryOptions queryOptions) {
      Set<PlanFragment> uniqueFragments = Sets.newHashSet();

      // Distinguish the per-host memory estimates for scan nodes and non-scan nodes to
      // get a tighter estimate on the amount of memory required by multiple concurrent
      // scans. The memory required by all concurrent scans of the same type (Hdfs/Hbase)
      // cannot exceed the per-host upper memory bound for that scan type. Intuitively,
      // the amount of I/O buffers is limited by the disk bandwidth.
      long perHostHbaseScanMem = 0L;
      long perHostHdfsScanMem = 0L;
      long perHostNonScanMem = 0L;
      for (int i = 0; i < planNodes.size(); ++i) {
        PlanNode node = planNodes.get(i);
        PlanFragment fragment = node.getFragment();
        if (!fragment.isPartitioned() && excludeUnpartitionedFragments) continue;
        node.computeCosts(queryOptions);
        uniqueFragments.add(fragment);
        if (node.getPerHostMemCost() < 0) {
          LOG.warn(String.format("Invalid per-host memory requirement %s of node %s.\n" +
              "PlanNode stats are: numNodes=%s ", node.getPerHostMemCost(),
              node.getClass().getSimpleName(), node.getNumNodes()));
        }
        if (node instanceof HBaseScanNode) {
          perHostHbaseScanMem += node.getPerHostMemCost();
        } else if (node instanceof HdfsScanNode) {
          perHostHdfsScanMem += node.getPerHostMemCost();
        } else {
          perHostNonScanMem += node.getPerHostMemCost();
        }
      }
      // The memory required by concurrent scans cannot exceed the upper memory bound
      // for that scan type.
      // TODO: In the future, we may want to restrict scanner concurrency based on a
      // memory limit. This estimation will need to accoung for that as well.
      perHostHbaseScanMem =
          Math.min(perHostHbaseScanMem, HBaseScanNode.getPerHostMemUpperBound());
      perHostHdfsScanMem =
          Math.min(perHostHdfsScanMem, HdfsScanNode.getPerHostMemUpperBound());

      long perHostDataSinkMem = 0L;
      for (int i = 0; i < dataSinks.size(); ++i) {
        DataSink sink = dataSinks.get(i);
        PlanFragment fragment = sink.getFragment();
        if (!fragment.isPartitioned() && excludeUnpartitionedFragments) continue;
        // Sanity check that this plan-node set has at least one PlanNode of fragment.
        Preconditions.checkState(uniqueFragments.contains(fragment));
        sink.computeCosts();
        if (sink.getPerHostMemCost() < 0) {
          LOG.warn(String.format("Invalid per-host memory requirement %s of sink %s.\n" +
              sink.getPerHostMemCost(), sink.getClass().getSimpleName()));
        }
        perHostDataSinkMem += sink.getPerHostMemCost();
      }

      // Combine the memory estimates of all sinks, scans nodes and non-scan nodes.
      long perHostMem = perHostHdfsScanMem + perHostHbaseScanMem + perHostNonScanMem +
          perHostDataSinkMem;

      // The backend needs at least one thread per fragment.
      int perHostVcores = uniqueFragments.size();

      // This plan node set might only have unpartitioned fragments.
      // Only set estimates if they are valid.
      if (perHostMem > 0 && perHostVcores > 0) {
        this.perHostMem = perHostMem;
        this.perHostVcores = perHostVcores;
        return true;
      }
      return false;
    }

    public long getPerHostMem() { return perHostMem; }
    public int getPerHostVcores() { return perHostVcores; }
  }

  /**
   * Estimates the per-host memory and CPU requirements for the given plan fragments,
   * and sets the results in request.
   * Optionally excludes the requirements for unpartitioned fragments.
   * TODO: The LOG.warn() messages should eventually become Preconditions checks
   * once resource estimation is more robust.
   */
  public void computeResourceReqs(List<PlanFragment> fragments,
      boolean excludeUnpartitionedFragments, TQueryOptions queryOptions,
      TQueryExecRequest request) {
    Preconditions.checkState(!fragments.isEmpty());
    Preconditions.checkNotNull(request);

    // Maps from an ExchangeNode's PlanNodeId to the fragments feeding it.
    Map<PlanNodeId, List<PlanFragment>> exchangeSources = Maps.newHashMap();
    for (PlanFragment fragment: fragments) {
      List<PlanFragment> srcFragments = exchangeSources.get(fragment.getDestNodeId());
      if (srcFragments == null) {
        srcFragments = Lists.newArrayList();
        exchangeSources.put(fragment.getDestNodeId(), srcFragments);
      }
      srcFragments.add(fragment);
    }

    // Compute pipelined plan node sets.
    ArrayList<PipelinedPlanNodeSet> planNodeSets =
        Lists.newArrayList(new PipelinedPlanNodeSet());
    computePlanNodeSets(fragments.get(0).getPlanRoot(),
        exchangeSources, planNodeSets.get(0), null, planNodeSets);

    // Compute the max of the per-host mem and vcores requirement.
    // Note that the max mem and vcores may come from different plan node sets.
    long maxPerHostMem = Long.MIN_VALUE;
    int maxPerHostVcores = Integer.MIN_VALUE;
    for (PipelinedPlanNodeSet planNodeSet: planNodeSets) {
      if (!planNodeSet.computeResourceEstimates(
          excludeUnpartitionedFragments, queryOptions)) {
        continue;
      }
      long perHostMem = planNodeSet.getPerHostMem();
      int perHostVcores = planNodeSet.getPerHostVcores();
      if (perHostMem > maxPerHostMem) maxPerHostMem = perHostMem;
      if (perHostVcores > maxPerHostVcores) maxPerHostVcores = perHostVcores;
    }

    // Do not ask for more cores than are in the RuntimeEnv.
    maxPerHostVcores = Math.min(maxPerHostVcores, RuntimeEnv.INSTANCE.getNumCores());

    // Legitimately set costs to zero if there are only unpartitioned fragments
    // and excludeUnpartitionedFragments is true.
    if (maxPerHostMem == Long.MIN_VALUE || maxPerHostVcores == Integer.MIN_VALUE) {
      boolean allUnpartitioned = true;
      for (PlanFragment fragment: fragments) {
        if (fragment.isPartitioned()) {
          allUnpartitioned = false;
          break;
        }
      }
      if (allUnpartitioned && excludeUnpartitionedFragments) {
        maxPerHostMem = 0;
        maxPerHostVcores = 0;
      }
    }

    if (maxPerHostMem < 0 || maxPerHostMem == Long.MIN_VALUE) {
      LOG.warn("Invalid per-host memory requirement: " + maxPerHostMem);
    }
    if (maxPerHostVcores < 0 || maxPerHostVcores == Integer.MIN_VALUE) {
      LOG.warn("Invalid per-host virtual cores requirement: " + maxPerHostVcores);
    }
    request.setPer_host_mem_req(maxPerHostMem);
    request.setPer_host_vcores((short) maxPerHostVcores);

    LOG.debug("Estimated per-host peak memory requirement: " + maxPerHostMem);
    LOG.debug("Estimated per-host virtual cores requirement: " + maxPerHostVcores);
  }

  /**
   * Populates 'planNodeSets' by recursively traversing the plan tree rooted at 'node'
   * belonging to 'fragment'. The traversal spans fragments by resolving exchange nodes
   * to their feeding fragment via exchangeSources.
   *
   * The plan node sets are computed top-down. As a result, the plan node sets are added
   * in reverse order of their runtime execution.
   *
   * Nodes are generally added to lhsSet. Joins are treated specially in that their
   * left child is added to lhsSet and their right child to rhsSet to make sure
   * that concurrent join builds end up in the same plan node set.
   */
  private void computePlanNodeSets(PlanNode node,
      Map<PlanNodeId, List<PlanFragment>> exchangeSources, PipelinedPlanNodeSet lhsSet,
      PipelinedPlanNodeSet rhsSet, ArrayList<PipelinedPlanNodeSet> planNodeSets) {
    lhsSet.add(node);
    if (node == node.getFragment().getPlanRoot() && node.getFragment().hasSink()) {
      lhsSet.addSink(node.getFragment().getSink());
    }

    if (node instanceof HashJoinNode) {
      // Create a new set for the right-hand sides of joins if necessary.
      if (rhsSet == null) {
        rhsSet = new PipelinedPlanNodeSet();
        planNodeSets.add(rhsSet);
      }
      // The join node itself is added to the lhsSet (above) and the rhsSet.
      rhsSet.add(node);
      computePlanNodeSets(node.getChild(1), exchangeSources, rhsSet, null,
          planNodeSets);
      computePlanNodeSets(node.getChild(0), exchangeSources, lhsSet, rhsSet,
          planNodeSets);
      return;
    }

    if (node instanceof ExchangeNode) {
      // Recurse into the plan roots of the fragments feeding this exchange.
      // Assume that all feeding fragments execute concurrently.
      List<PlanFragment> srcFragments = exchangeSources.get(node.getId());
      Preconditions.checkNotNull(srcFragments);
      for (PlanFragment srcFragment: srcFragments) {
        computePlanNodeSets(srcFragment.getPlanRoot(), exchangeSources, lhsSet, null,
            planNodeSets);
      }
      return;
    }

    if (node.isBlockingNode()) {
      // We add blocking nodes to two plan node sets because they require resources while
      // consuming their input (execution of the preceding set) and while they
      // emit their output (execution of the following set).
      lhsSet = new PipelinedPlanNodeSet();
      lhsSet.add(node);
      planNodeSets.add(lhsSet);
      // Join builds under this blocking node belong in a new rhsSet.
      rhsSet = null;
    }

    // Assume that non-join, non-blocking nodes with multiple children (e.g., MergeNode)
    // consume their inputs in an arbitrary order (i.e., all child subtrees execute
    // concurrently).
    for (PlanNode child: node.getChildren()) {
      computePlanNodeSets(child, exchangeSources, lhsSet, rhsSet, planNodeSets);
    }
  }
}
