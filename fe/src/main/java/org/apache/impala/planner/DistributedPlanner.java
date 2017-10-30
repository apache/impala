// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InsertStmt;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.planner.JoinNode.DistributionMode;
import org.apache.impala.planner.RuntimeFilterGenerator.RuntimeFilter;
import org.apache.impala.util.KuduUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * The distributed planner is responsible for creating an executable, distributed plan
 * from a single-node plan that can be sent to the backend.
 */
public class DistributedPlanner {
  private final static Logger LOG = LoggerFactory.getLogger(DistributedPlanner.class);

  private final PlannerContext ctx_;

  public DistributedPlanner(PlannerContext ctx) {
    ctx_ = ctx;
  }

  /**
   * Create plan fragments for a single-node plan considering a set of execution options.
   * The fragments are returned in a list such that element i of that list can
   * only consume output of the following fragments j > i.
   *
   * TODO: take data partition of the plan fragments into account; in particular,
   * coordinate between hash partitioning for aggregation and hash partitioning
   * for analytic computation more generally than what createQueryPlan() does
   * right now (the coordination only happens if the same select block does both
   * the aggregation and analytic computation).
   */
  public ArrayList<PlanFragment> createPlanFragments(
      PlanNode singleNodePlan) throws ImpalaException {
    Preconditions.checkState(!ctx_.isSingleNodeExec());
    AnalysisContext.AnalysisResult analysisResult = ctx_.getAnalysisResult();
    QueryStmt queryStmt = ctx_.getQueryStmt();
    ArrayList<PlanFragment> fragments = Lists.newArrayList();
    // For inserts or CTAS, unless there is a limit, leave the root fragment
    // partitioned, otherwise merge everything into a single coordinator fragment,
    // so we can pass it back to the client.
    boolean isPartitioned = false;
    if ((analysisResult.isInsertStmt() || analysisResult.isCreateTableAsSelectStmt()
        || analysisResult.isUpdateStmt() || analysisResult.isDeleteStmt())
        && !singleNodePlan.hasLimit()) {
      Preconditions.checkState(!queryStmt.hasOffset());
      isPartitioned = true;
    }
    createPlanFragments(singleNodePlan, isPartitioned, fragments);
    return fragments;
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
      PlanNode root, boolean isPartitioned, ArrayList<PlanFragment> fragments)
      throws ImpalaException {
    ArrayList<PlanFragment> childFragments = Lists.newArrayList();
    for (PlanNode child: root.getChildren()) {
      // allow child fragments to be partitioned, unless they contain a limit clause
      // (the result set with the limit constraint needs to be computed centrally);
      // merge later if needed
      boolean childIsPartitioned = !child.hasLimit();
      // Do not fragment the subplan of a SubplanNode since it is executed locally.
      if (root instanceof SubplanNode && child == root.getChild(1)) continue;
      childFragments.add(createPlanFragments(child, childIsPartitioned, fragments));
    }

    PlanFragment result = null;
    if (root instanceof ScanNode) {
      result = createScanFragment(root);
      fragments.add(result);
    } else if (root instanceof HashJoinNode) {
      Preconditions.checkState(childFragments.size() == 2);
      result = createHashJoinFragment((HashJoinNode) root,
          childFragments.get(1), childFragments.get(0), fragments);
    } else if (root instanceof NestedLoopJoinNode) {
      Preconditions.checkState(childFragments.size() == 2);
      result = createNestedLoopJoinFragment((NestedLoopJoinNode) root,
          childFragments.get(1), childFragments.get(0), fragments);
    } else if (root instanceof SubplanNode) {
      Preconditions.checkState(childFragments.size() == 1);
      result = createSubplanNodeFragment((SubplanNode) root, childFragments.get(0));
    } else if (root instanceof SelectNode) {
      result = createSelectNodeFragment((SelectNode) root, childFragments);
    } else if (root instanceof UnionNode) {
      result = createUnionNodeFragment((UnionNode) root, childFragments, fragments);
    } else if (root instanceof AggregationNode) {
      result = createAggregationFragment(
          (AggregationNode) root, childFragments.get(0), fragments);
    } else if (root instanceof SortNode) {
      if (((SortNode) root).isAnalyticSort()) {
        // don't parallelize this like a regular SortNode
        result = createAnalyticFragment(
            root, childFragments.get(0), fragments);
      } else {
        result = createOrderByFragment(
            (SortNode) root, childFragments.get(0), fragments);
      }
    } else if (root instanceof AnalyticEvalNode) {
      result = createAnalyticFragment(root, childFragments.get(0), fragments);
    } else if (root instanceof EmptySetNode) {
      result = new PlanFragment(
          ctx_.getNextFragmentId(), root, DataPartition.UNPARTITIONED);
    } else {
      throw new InternalException("Cannot create plan fragment for this node type: "
          + root.getExplainString(ctx_.getQueryOptions()));
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
   * Decides whether to repartition the output of 'inputFragment' before feeding its
   * data into the table sink of the given 'insertStmt'. The decision obeys the
   * shuffle/noshuffle plan hints if present. Otherwise, returns a plan fragment that
   * partitions the output of 'inputFragment' on the partition exprs of 'insertStmt',
   * unless the expected number of partitions is less than the number of nodes on which
   * inputFragment runs, or the target table is unpartitioned.
   * For inserts into unpartitioned tables or inserts with only constant partition exprs,
   * the shuffle hint leads to a plan that merges all rows at the coordinator where
   * the table sink is executed.
   * If this functions ends up creating a new fragment, appends that to 'fragments'.
   */
  public PlanFragment createInsertFragment(
      PlanFragment inputFragment, InsertStmt insertStmt, Analyzer analyzer,
      ArrayList<PlanFragment> fragments)
      throws ImpalaException {
    if (insertStmt.hasNoShuffleHint()) return inputFragment;

    List<Expr> partitionExprs = Lists.newArrayList(insertStmt.getPartitionKeyExprs());
    // Ignore constants for the sake of partitioning.
    Expr.removeConstants(partitionExprs);

    // Do nothing if the input fragment is already appropriately partitioned. TODO: handle
    // Kudu tables here (IMPALA-5254).
    DataPartition inputPartition = inputFragment.getDataPartition();
    if (!partitionExprs.isEmpty()
        && analyzer.equivSets(inputPartition.getPartitionExprs(), partitionExprs)
        && !(insertStmt.getTargetTable() instanceof KuduTable)) {
      return inputFragment;
    }

    // Make a cost-based decision only if no user hint was supplied and this is not a Kudu
    // table. TODO: consider making a cost based decision for Kudu tables.
    if (!insertStmt.hasShuffleHint()
        && !(insertStmt.getTargetTable() instanceof KuduTable)) {
      // If the existing partition exprs are a subset of the table partition exprs, check
      // if it is distributed across all nodes. If so, don't repartition.
      if (Expr.isSubset(inputPartition.getPartitionExprs(), partitionExprs)) {
        long numPartitions = getNumDistinctValues(inputPartition.getPartitionExprs());
        if (numPartitions >= inputFragment.getNumNodes()) return inputFragment;
      }

      // Don't repartition if we know we have fewer partitions than nodes
      // (ie, default to repartitioning if col stats are missing).
      // TODO: We want to repartition if the resulting files would otherwise
      // be very small (less than some reasonable multiple of the recommended block size).
      // In order to do that, we need to come up with an estimate of the avg row size
      // in the particular file format of the output table/partition.
      // We should always know on how many nodes our input is running.
      long numPartitions = getNumDistinctValues(partitionExprs);
      Preconditions.checkState(inputFragment.getNumNodes() != -1);
      if (numPartitions > 0 && numPartitions <= inputFragment.getNumNodes()) {
        return inputFragment;
      }
    }

    ExchangeNode exchNode =
        new ExchangeNode(ctx_.getNextNodeId(), inputFragment.getPlanRoot());
    exchNode.init(analyzer);
    Preconditions.checkState(exchNode.hasValidStats());
    DataPartition partition;
    if (partitionExprs.isEmpty()) {
      partition = DataPartition.UNPARTITIONED;
    } else if (insertStmt.getTargetTable() instanceof KuduTable) {
      partition = DataPartition.kuduPartitioned(
          KuduUtil.createPartitionExpr(insertStmt, ctx_.getRootAnalyzer()));
    } else {
      partition = DataPartition.hashPartitioned(partitionExprs);
    }
    PlanFragment fragment =
        new PlanFragment(ctx_.getNextFragmentId(), exchNode, partition);
    inputFragment.setDestination(exchNode);
    inputFragment.setOutputPartition(partition);
    fragments.add(fragment);
    return fragment;
  }

  /**
   * Return unpartitioned fragment that merges the input fragment's output via
   * an ExchangeNode.
   * Requires that input fragment be partitioned.
   */
  private PlanFragment createMergeFragment(PlanFragment inputFragment)
      throws ImpalaException {
    Preconditions.checkState(inputFragment.isPartitioned());
    ExchangeNode mergePlan =
        new ExchangeNode(ctx_.getNextNodeId(), inputFragment.getPlanRoot());
    mergePlan.init(ctx_.getRootAnalyzer());
    Preconditions.checkState(mergePlan.hasValidStats());
    PlanFragment fragment = new PlanFragment(ctx_.getNextFragmentId(), mergePlan,
        DataPartition.UNPARTITIONED);
    inputFragment.setDestination(mergePlan);
    return fragment;
  }

  /**
   * Create new randomly-partitioned fragment containing a single scan node.
   * TODO: take bucketing into account to produce a naturally hash-partitioned
   * fragment
   * TODO: hbase scans are range-partitioned on the row key
   */
  private PlanFragment createScanFragment(PlanNode node) {
    return new PlanFragment(ctx_.getNextFragmentId(), node, DataPartition.RANDOM);
  }

  /**
   * Adds the SubplanNode as the new plan root to the child fragment and returns
   * the child fragment.
   */
  private PlanFragment createSubplanNodeFragment(SubplanNode node,
      PlanFragment childFragment) {
    node.setChild(0, childFragment.getPlanRoot());
    childFragment.setPlanRoot(node);
    return childFragment;
  }

  /**
   * Modifies the leftChildFragment to execute a cross join. The right child input is
   * provided by an ExchangeNode, which is the destination of the rightChildFragment's
   * output.
   */
  private PlanFragment createNestedLoopJoinFragment(NestedLoopJoinNode node,
      PlanFragment rightChildFragment, PlanFragment leftChildFragment,
      ArrayList<PlanFragment> fragments) throws ImpalaException {
    node.setDistributionMode(DistributionMode.BROADCAST);
    node.setChild(0, leftChildFragment.getPlanRoot());
    connectChildFragment(node, 1, leftChildFragment, rightChildFragment);
    leftChildFragment.setPlanRoot(node);
    return leftChildFragment;
  }

  /**
   * Helper function to produce a partitioning hash-join fragment
   */
  private PlanFragment createPartitionedHashJoinFragment(HashJoinNode node,
      Analyzer analyzer, boolean lhsHasCompatPartition, boolean rhsHasCompatPartition,
      PlanFragment leftChildFragment, PlanFragment rightChildFragment,
      List<Expr> lhsJoinExprs, List<Expr> rhsJoinExprs,
      ArrayList<PlanFragment> fragments) throws ImpalaException {
    Preconditions.checkState(node.getDistributionMode() == DistributionMode.PARTITIONED);
    // The lhs and rhs input fragments are already partitioned on the join exprs.
    // Combine the lhs/rhs input fragments into leftChildFragment by placing the join
    // node into leftChildFragment and setting its lhs/rhs children to the plan root of
    // the lhs/rhs child fragment, respectively. No new child fragments or exchanges
    // are created, and the rhs fragment is removed.
    // TODO: Relax the isCompatPartition() check below. The check is conservative and
    // may reject partitions that could be made physically compatible. Fix this by
    // removing equivalent duplicates from partition exprs and impose a canonical order
    // on partition exprs (both using the canonical equivalence class representatives).
    if (lhsHasCompatPartition
        && rhsHasCompatPartition
        && isCompatPartition(
            leftChildFragment.getDataPartition(),
            rightChildFragment.getDataPartition(),
            lhsJoinExprs, rhsJoinExprs, analyzer)) {
      node.setChild(0, leftChildFragment.getPlanRoot());
      node.setChild(1, rightChildFragment.getPlanRoot());
      // fix up PlanNode.fragment_ for the migrated PlanNode tree of the rhs child
      leftChildFragment.setFragmentInPlanTree(node.getChild(1));
      // Relocate input fragments of rightChildFragment to leftChildFragment.
      for (PlanFragment rhsInput: rightChildFragment.getChildren()) {
        leftChildFragment.getChildren().add(rhsInput);
      }
      // Remove right fragment because its plan tree has been merged into leftFragment.
      fragments.remove(rightChildFragment);
      leftChildFragment.setPlanRoot(node);
      return leftChildFragment;
    }

    // The lhs input fragment is already partitioned on the join exprs.
    // Make the HashJoin the new root of leftChildFragment and set the join's
    // first child to the lhs plan root. The second child of the join is an
    // ExchangeNode that is fed by the rhsInputFragment whose sink repartitions
    // its data by the rhs join exprs.
    DataPartition rhsJoinPartition = null;
    if (lhsHasCompatPartition) {
      rhsJoinPartition = getCompatPartition(lhsJoinExprs,
          leftChildFragment.getDataPartition(), rhsJoinExprs, analyzer);
      if (rhsJoinPartition != null) {
        node.setChild(0, leftChildFragment.getPlanRoot());
        connectChildFragment(node, 1, leftChildFragment, rightChildFragment);
        rightChildFragment.setOutputPartition(rhsJoinPartition);
        leftChildFragment.setPlanRoot(node);
        return leftChildFragment;
      }
    }

    // Same as above but with rhs and lhs reversed.
    DataPartition lhsJoinPartition = null;
    if (rhsHasCompatPartition) {
      lhsJoinPartition = getCompatPartition(rhsJoinExprs,
          rightChildFragment.getDataPartition(), lhsJoinExprs, analyzer);
      if (lhsJoinPartition != null) {
        node.setChild(1, rightChildFragment.getPlanRoot());
        connectChildFragment(node, 0, rightChildFragment, leftChildFragment);
        leftChildFragment.setOutputPartition(lhsJoinPartition);
        rightChildFragment.setPlanRoot(node);
        return rightChildFragment;
      }
    }

    Preconditions.checkState(lhsJoinPartition == null);
    Preconditions.checkState(rhsJoinPartition == null);
    lhsJoinPartition = DataPartition.hashPartitioned(Expr.cloneList(lhsJoinExprs));
    rhsJoinPartition = DataPartition.hashPartitioned(Expr.cloneList(rhsJoinExprs));

    // Neither lhs nor rhs are already partitioned on the join exprs.
    // Create a new parent fragment containing a HashJoin node with two
    // ExchangeNodes as inputs; the latter are the destinations of the
    // left- and rightChildFragments, which now partition their output
    // on their respective join exprs.
    // The new fragment is hash-partitioned on the lhs input join exprs.
    ExchangeNode lhsExchange =
        new ExchangeNode(ctx_.getNextNodeId(), leftChildFragment.getPlanRoot());
    lhsExchange.computeStats(null);
    node.setChild(0, lhsExchange);
    ExchangeNode rhsExchange =
        new ExchangeNode(ctx_.getNextNodeId(), rightChildFragment.getPlanRoot());
    rhsExchange.computeStats(null);
    node.setChild(1, rhsExchange);

    // Connect the child fragments in a new fragment, and set the data partition
    // of the new fragment and its child fragments.
    PlanFragment joinFragment =
        new PlanFragment(ctx_.getNextFragmentId(), node, lhsJoinPartition);
    leftChildFragment.setDestination(lhsExchange);
    leftChildFragment.setOutputPartition(lhsJoinPartition);
    rightChildFragment.setDestination(rhsExchange);
    rightChildFragment.setOutputPartition(rhsJoinPartition);
    return joinFragment;
  }

  /**
   * Creates either a broadcast join or a repartitioning join depending on the expected
   * cost and various constraints. See computeDistributionMode() for more details.
   * TODO: don't create a broadcast join if we already anticipate that this will
   * exceed the query's memory budget.
   */
  private PlanFragment createHashJoinFragment(
      HashJoinNode node, PlanFragment rightChildFragment,
      PlanFragment leftChildFragment, ArrayList<PlanFragment> fragments)
      throws ImpalaException {
    // For both join types, the total cost is calculated as the amount of data
    // sent over the network, plus the amount of data inserted into the hash table.
    // broadcast: send the rightChildFragment's output to each node executing
    // the leftChildFragment, and build a hash table with it on each node.
    Analyzer analyzer = ctx_.getRootAnalyzer();
    PlanNode rhsTree = rightChildFragment.getPlanRoot();
    long rhsDataSize = -1;
    long broadcastCost = -1;
    if (rhsTree.getCardinality() != -1) {
      rhsDataSize = Math.round(
          rhsTree.getCardinality() * ExchangeNode.getAvgSerializedRowSize(rhsTree));
      if (leftChildFragment.getNumNodes() != -1) {
        broadcastCost = 2 * rhsDataSize * leftChildFragment.getNumNodes();
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("broadcast: cost=" + Long.toString(broadcastCost));
      LOG.trace("card=" + Long.toString(rhsTree.getCardinality()) + " row_size="
          + Float.toString(rhsTree.getAvgRowSize()) + " #nodes="
          + Integer.toString(leftChildFragment.getNumNodes()));
    }

    // repartition: both left- and rightChildFragment are partitioned on the
    // join exprs, and a hash table is built with the rightChildFragment's output.
    PlanNode lhsTree = leftChildFragment.getPlanRoot();
    List<Expr> lhsJoinExprs = Lists.newArrayList();
    List<Expr> rhsJoinExprs = Lists.newArrayList();
    for (Expr joinConjunct: node.getEqJoinConjuncts()) {
      // no remapping necessary
      lhsJoinExprs.add(joinConjunct.getChild(0).clone());
      rhsJoinExprs.add(joinConjunct.getChild(1).clone());
    }
    boolean lhsHasCompatPartition = false;
    boolean rhsHasCompatPartition = false;
    long partitionCost = -1;
    if (lhsTree.getCardinality() != -1 && rhsTree.getCardinality() != -1) {
      lhsHasCompatPartition = analyzer.equivSets(lhsJoinExprs,
          leftChildFragment.getDataPartition().getPartitionExprs());
      rhsHasCompatPartition = analyzer.equivSets(rhsJoinExprs,
          rightChildFragment.getDataPartition().getPartitionExprs());

      Preconditions.checkState(rhsDataSize != -1);
      double lhsNetworkCost = (lhsHasCompatPartition) ? 0.0 :
        Math.round(
            lhsTree.getCardinality() * ExchangeNode.getAvgSerializedRowSize(lhsTree));
      double rhsNetworkCost = (rhsHasCompatPartition) ? 0.0 : rhsDataSize;
      partitionCost = Math.round(lhsNetworkCost + rhsNetworkCost + rhsDataSize);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("partition: cost=" + Long.toString(partitionCost));
      LOG.trace("lhs card=" + Long.toString(lhsTree.getCardinality()) + " row_size="
          + Float.toString(lhsTree.getAvgRowSize()));
      LOG.trace("rhs card=" + Long.toString(rhsTree.getCardinality()) + " row_size="
          + Float.toString(rhsTree.getAvgRowSize()));
      LOG.trace(rhsTree.getExplainString(ctx_.getQueryOptions()));
    }

    DistributionMode distrMode = computeJoinDistributionMode(
        node, broadcastCost, partitionCost, rhsDataSize);
    node.setDistributionMode(distrMode);

    PlanFragment hjFragment = null;
    if (distrMode == DistributionMode.BROADCAST) {
      // Doesn't create a new fragment, but modifies leftChildFragment to execute
      // the join; the build input is provided by an ExchangeNode, which is the
      // destination of the rightChildFragment's output
      node.setChild(0, leftChildFragment.getPlanRoot());
      connectChildFragment(node, 1, leftChildFragment, rightChildFragment);
      leftChildFragment.setPlanRoot(node);
      hjFragment = leftChildFragment;
    } else {
      hjFragment = createPartitionedHashJoinFragment(node, analyzer,
          lhsHasCompatPartition, rhsHasCompatPartition, leftChildFragment,
          rightChildFragment, lhsJoinExprs, rhsJoinExprs, fragments);
    }
    return hjFragment;
 }

 /**
  * Determines and returns the distribution mode for the given join based on the expected
  * costs and the right-hand size data size. Considers the following:
  * - Some join types require a specific distribution strategy to run correctly.
  * - Checks for join hints.
  * - Uses the default join strategy (query option) when the costs are unknown or tied.
  * - Returns broadcast if it is cheaper than partitioned and the expected hash table
  *   size is within the query mem limit.
  * - Otherwise, returns partitioned.
  * For 'broadcastCost', 'partitionCost', and 'rhsDataSize' a value of -1 indicates
  * unknown, e.g., due to missing stats.
  */
 private DistributionMode computeJoinDistributionMode(JoinNode node,
     long broadcastCost, long partitionCost, long rhsDataSize) {
   // Check join types that require a specific distribution strategy to run correctly.
   JoinOperator op = node.getJoinOp();
   if (op == JoinOperator.RIGHT_OUTER_JOIN || op == JoinOperator.RIGHT_SEMI_JOIN
       || op == JoinOperator.RIGHT_ANTI_JOIN || op == JoinOperator.FULL_OUTER_JOIN) {
     return DistributionMode.PARTITIONED;
   }
   if (op == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) return DistributionMode.BROADCAST;

   // Check join hints.
   if (node.getDistributionModeHint() != DistributionMode.NONE) {
     return node.getDistributionModeHint();
   }

   // Use the default mode when the costs are unknown or tied.
   if (broadcastCost == -1 || partitionCost == -1 || broadcastCost == partitionCost) {
     return DistributionMode.fromThrift(
         ctx_.getQueryOptions().getDefault_join_distribution_mode());
   }

   // Decide the distribution mode based on the estimated costs and the mem limit.
   long htSize = Math.round(rhsDataSize * PlannerContext.HASH_TBL_SPACE_OVERHEAD);
   long memLimit = ctx_.getQueryOptions().mem_limit;
   if (broadcastCost <= partitionCost && (memLimit == 0 || htSize <= memLimit)) {
     return DistributionMode.BROADCAST;
   }
   // Partitioned was cheaper or the broadcast HT would not fit within the mem limit.
   return DistributionMode.PARTITIONED;
 }

  /**
   * Returns true if the lhs and rhs partitions are physically compatible for executing
   * a partitioned join with the given lhs/rhs join exprs. Physical compatibility means
   * that lhs/rhs exchange nodes hashing on exactly those partition expressions are
   * guaranteed to send two rows with identical partition-expr values to the same node.
   * The requirements for physical compatibility are:
   * 1. Number of exprs must be the same
   * 2. The lhs partition exprs are identical to the lhs join exprs and the rhs partition
   *    exprs are identical to the rhs join exprs
   * 3. Or for each expr in the lhs partition, there must be an equivalent expr in the
   *    rhs partition at the same ordinal position within the expr list
   * (4. The expr types must be identical, but that is enforced later in PlanFragment)
   * Conditions 2 and 3 are similar but not the same due to outer joins, e.g., for full
   * outer joins condition 3 can never be met, but condition 2 can.
   * TODO: Move parts of this function into DataPartition as appropriate.
   */
  private boolean isCompatPartition(DataPartition lhsPartition,
      DataPartition rhsPartition, List<Expr> lhsJoinExprs, List<Expr> rhsJoinExprs,
      Analyzer analyzer) {
    List<Expr> lhsPartExprs = lhsPartition.getPartitionExprs();
    List<Expr> rhsPartExprs = rhsPartition.getPartitionExprs();
    // 1. Sizes must be equal.
    if (lhsPartExprs.size() != rhsPartExprs.size()) return false;
    // 2. Lhs/rhs join exprs are identical to lhs/rhs partition exprs.
    Preconditions.checkState(lhsJoinExprs.size() == rhsJoinExprs.size());
    if (lhsJoinExprs.size() == lhsPartExprs.size()) {
      if (lhsJoinExprs.equals(lhsPartExprs) && rhsJoinExprs.equals(rhsPartExprs)) {
        return true;
      }
    }
    // 3. Each lhs part expr must have an equivalent expr at the same position
    // in the rhs part exprs.
    for (int i = 0; i < lhsPartExprs.size(); ++i) {
      if (!analyzer.equivExprs(lhsPartExprs.get(i), rhsPartExprs.get(i))) return false;
    }
    return true;
  }

  /**
   * Returns a new data partition that is suitable for creating an exchange node to feed
   * a partitioned hash join. The hash join is assumed to be placed in a fragment with an
   * existing data partition that is compatible with either the lhs or rhs join exprs
   * (srcPartition belongs to the fragment and srcJoinExprs are the compatible exprs).
   * The returned partition uses the given joinExprs which are assumed to be the lhs or
   * rhs join exprs, whichever srcJoinExprs are not.
   * The returned data partition has two important properties to ensure correctness:
   * 1. It has exactly the same number of hash exprs as the srcPartition (IMPALA-1307),
   *    possibly by removing redundant exprs from joinExprs or adding some joinExprs
   *    multiple times to match the srcPartition
   * 2. The hash exprs are ordered based on their corresponding 'matches' in
   *    the existing srcPartition (IMPALA-1324).
   * Returns null if no compatible data partition could be constructed.
   * TODO: Move parts of this function into DataPartition as appropriate.
   * TODO: Make comment less operational and more semantic.
   */
  private DataPartition getCompatPartition(List<Expr> srcJoinExprs,
      DataPartition srcPartition, List<Expr> joinExprs, Analyzer analyzer) {
    Preconditions.checkState(srcPartition.isHashPartitioned());
    List<Expr> srcPartExprs = srcPartition.getPartitionExprs();
    List<Expr> resultPartExprs = Lists.newArrayList();
    for (int i = 0; i < srcPartExprs.size(); ++i) {
      for (int j = 0; j < srcJoinExprs.size(); ++j) {
        if (analyzer.equivExprs(srcPartExprs.get(i), srcJoinExprs.get(j))) {
          resultPartExprs.add(joinExprs.get(j).clone());
          break;
        }
      }
    }
    if (resultPartExprs.size() != srcPartExprs.size()) return null;
    return DataPartition.hashPartitioned(resultPartExprs);
  }

  /**
   * Returns a new fragment with a UnionNode as its root. The data partition of the
   * returned fragment and how the data of the child fragments is consumed depends on the
   * data partitions of the child fragments:
   * - All child fragments are unpartitioned or partitioned: The returned fragment has an
   *   UNPARTITIONED or RANDOM data partition, respectively. The UnionNode absorbs the
   *   plan trees of all child fragments.
   * - Mixed partitioned/unpartitioned child fragments: The returned fragment is
   *   RANDOM partitioned. The plan trees of all partitioned child fragments are absorbed
   *   into the UnionNode. All unpartitioned child fragments are connected to the
   *   UnionNode via a RANDOM exchange, and remain unchanged otherwise.
   */
  private PlanFragment createUnionNodeFragment(UnionNode unionNode,
      ArrayList<PlanFragment> childFragments, ArrayList<PlanFragment> fragments)
      throws ImpalaException {
    Preconditions.checkState(unionNode.getChildren().size() == childFragments.size());

    // A UnionNode could have no children or constant selects if all of its operands
    // were dropped because of constant predicates that evaluated to false.
    if (unionNode.getChildren().isEmpty()) {
      return new PlanFragment(
          ctx_.getNextFragmentId(), unionNode, DataPartition.UNPARTITIONED);
    }

    Preconditions.checkState(!childFragments.isEmpty());
    int numUnpartitionedChildFragments = 0;
    for (int i = 0; i < childFragments.size(); ++i) {
      if (!childFragments.get(i).isPartitioned()) ++numUnpartitionedChildFragments;
    }

    // remove all children to avoid them being tagged with the wrong
    // fragment (in the PlanFragment c'tor; we haven't created ExchangeNodes yet)
    unionNode.clearChildren();

    // If all child fragments are unpartitioned, return a single unpartitioned fragment
    // with a UnionNode that merges all child fragments.
    if (numUnpartitionedChildFragments == childFragments.size()) {
      PlanFragment unionFragment = new PlanFragment(ctx_.getNextFragmentId(),
          unionNode, DataPartition.UNPARTITIONED);
      // Absorb the plan trees of all childFragments into unionNode
      // and fix up the fragment tree in the process.
      for (int i = 0; i < childFragments.size(); ++i) {
        unionNode.addChild(childFragments.get(i).getPlanRoot());
        unionFragment.setFragmentInPlanTree(unionNode.getChild(i));
        unionFragment.addChildren(childFragments.get(i).getChildren());
      }
      unionNode.init(ctx_.getRootAnalyzer());
      // All child fragments have been absorbed into unionFragment.
      fragments.removeAll(childFragments);
      return unionFragment;
    }

    // There is at least one partitioned child fragment.
    PlanFragment unionFragment = new PlanFragment(
        ctx_.getNextFragmentId(), unionNode, DataPartition.RANDOM);
    for (int i = 0; i < childFragments.size(); ++i) {
      PlanFragment childFragment = childFragments.get(i);
      if (childFragment.isPartitioned()) {
        // absorb the plan trees of all partitioned child fragments into unionNode
        unionNode.addChild(childFragment.getPlanRoot());
        unionFragment.setFragmentInPlanTree(unionNode.getChild(i));
        unionFragment.addChildren(childFragment.getChildren());
        fragments.remove(childFragment);
      } else {
        // dummy entry for subsequent addition of the ExchangeNode
        unionNode.addChild(null);
        // Connect the unpartitioned child fragments to unionNode via a random exchange.
        connectChildFragment(unionNode, i, unionFragment, childFragment);
        childFragment.setOutputPartition(DataPartition.RANDOM);
      }
    }
    unionNode.init(ctx_.getRootAnalyzer());
    return unionFragment;
  }

  /**
   * Adds the SelectNode as the new plan root to the child fragment and returns
   * the child fragment.
   */
  private PlanFragment createSelectNodeFragment(SelectNode selectNode,
      ArrayList<PlanFragment> childFragments) {
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
   * input from childFragment. ParentFragment contains node and the new ExchangeNode.
   */
  private void connectChildFragment(PlanNode node, int childIdx,
      PlanFragment parentFragment, PlanFragment childFragment) throws ImpalaException {
    ExchangeNode exchangeNode =
        new ExchangeNode(ctx_.getNextNodeId(), childFragment.getPlanRoot());
    exchangeNode.init(ctx_.getRootAnalyzer());
    exchangeNode.setFragment(parentFragment);
    node.setChild(childIdx, exchangeNode);
    childFragment.setDestination(exchangeNode);
  }

  /**
   * Create a new fragment containing a single ExchangeNode that consumes the output
   * of childFragment, set the destination of childFragment to the new parent
   * and the output partition of childFragment to that of the new parent.
   * TODO: the output partition of a child isn't necessarily the same as the data
   * partition of the receiving parent (if there is more materialization happening
   * in the parent, such as during distinct aggregation). Do we care about the data
   * partition of the parent being applicable to the *output* of the parent (it's
   * correct for the input).
   */
  private PlanFragment createParentFragment(
      PlanFragment childFragment, DataPartition parentPartition)
      throws ImpalaException {
    ExchangeNode exchangeNode =
        new ExchangeNode(ctx_.getNextNodeId(), childFragment.getPlanRoot());
    exchangeNode.init(ctx_.getRootAnalyzer());
    PlanFragment parentFragment = new PlanFragment(ctx_.getNextFragmentId(),
        exchangeNode, parentPartition);
    childFragment.setDestination(exchangeNode);
    childFragment.setOutputPartition(parentPartition);
    return parentFragment;
  }

  /**
   * Returns a fragment that materializes the aggregation result of 'node'.
   * If the child fragment is partitioned, the result fragment will be partitioned on
   * the grouping exprs of 'node'.
   * If 'node' is phase 1 of a 2-phase DISTINCT aggregation, this will simply
   * add 'node' to the child fragment and return the child fragment; the new
   * fragment will be created by the subsequent call of createAggregationFragment()
   * for the phase 2 AggregationNode.
   */
  private PlanFragment createAggregationFragment(AggregationNode node,
      PlanFragment childFragment, ArrayList<PlanFragment> fragments)
      throws ImpalaException {
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

    // Check if 'node' is phase 2 of a DISTINCT aggregation.
    boolean isDistinct = node.getChild(0) instanceof AggregationNode
          && ((AggregationNode)(node.getChild(0))).getAggInfo().isDistinctAgg();
    if (isDistinct) {
      return createPhase2DistinctAggregationFragment(node, childFragment, fragments);
    } else {
      return createMergeAggregationFragment(node, childFragment);
    }
  }

  /**
   * Returns a fragment that materializes the final result of an aggregation where
   * 'childFragment' is a partitioned fragment and 'node' is not part of a distinct
   * aggregation.
   */
  private PlanFragment createMergeAggregationFragment(
      AggregationNode node, PlanFragment childFragment)
      throws ImpalaException {
    Preconditions.checkArgument(childFragment.isPartitioned());
    ArrayList<Expr> groupingExprs = node.getAggInfo().getGroupingExprs();
    boolean hasGrouping = !groupingExprs.isEmpty();

    DataPartition parentPartition = null;
    if (hasGrouping) {
      List<Expr> partitionExprs = node.getAggInfo().getPartitionExprs();
      if (partitionExprs == null) partitionExprs = groupingExprs;
      boolean childHasCompatPartition = ctx_.getRootAnalyzer().equivSets(partitionExprs,
            childFragment.getDataPartition().getPartitionExprs());
      if (childHasCompatPartition && !childFragment.refsNullableTupleId(partitionExprs)) {
        // The data is already partitioned on the required expressions. We can do the
        // aggregation in the child fragment without an extra merge step.
        // An exchange+merge step is required if the grouping exprs reference a tuple
        // that is made nullable in 'childFragment' to bring NULLs from outer-join
        // non-matches together.
        childFragment.addPlanRoot(node);
        return childFragment;
      }
      // the parent fragment is partitioned on the grouping exprs;
      // substitute grouping exprs to reference the *output* of the agg, not the input
      partitionExprs = Expr.substituteList(partitionExprs,
          node.getAggInfo().getIntermediateSmap(), ctx_.getRootAnalyzer(), false);
      parentPartition = DataPartition.hashPartitioned(partitionExprs);
    } else {
      // the parent fragment is unpartitioned
      parentPartition = DataPartition.UNPARTITIONED;
    }

    // the original aggregation materializes the intermediate agg tuple and goes
    // into the child fragment; merge aggregation materializes the output agg tuple
    // and goes into a parent fragment
    childFragment.addPlanRoot(node);
    node.setIntermediateTuple();
    node.setIsPreagg(ctx_);

    // if there is a limit, we need to transfer it from the pre-aggregation
    // node in the child fragment to the merge aggregation node in the parent
    long limit = node.getLimit();
    node.unsetLimit();
    node.unsetNeedsFinalize();

    // place a merge aggregation step in a new fragment
    PlanFragment mergeFragment = createParentFragment(childFragment, parentPartition);
    AggregationNode mergeAggNode = new AggregationNode(ctx_.getNextNodeId(),
        mergeFragment.getPlanRoot(), node.getAggInfo().getMergeAggInfo());
    mergeAggNode.init(ctx_.getRootAnalyzer());
    mergeAggNode.setLimit(limit);
    // Merge of non-grouping agg only processes one tuple per Impala daemon - codegen
    // will cost more than benefit.
    if (!hasGrouping) {
      mergeFragment.getPlanRoot().setDisableCodegen(true);
      mergeAggNode.setDisableCodegen(true);
    }

    // HAVING predicates can only be evaluated after the merge agg step
    node.transferConjuncts(mergeAggNode);
    // Recompute stats after transferring the conjuncts_ (order is important).
    node.computeStats(ctx_.getRootAnalyzer());
    mergeFragment.getPlanRoot().computeStats(ctx_.getRootAnalyzer());
    mergeAggNode.computeStats(ctx_.getRootAnalyzer());
    // Set new plan root after updating stats.
    mergeFragment.addPlanRoot(mergeAggNode);

    return mergeFragment;
  }

  /**
   * Returns a fragment that materialises the final result of a distinct aggregation
   * where 'childFragment' is a partitioned fragment with the phase-1 aggregation
   * as its root.
   */
  private PlanFragment createPhase2DistinctAggregationFragment(
      AggregationNode phase2AggNode, PlanFragment childFragment,
      ArrayList<PlanFragment> fragments) throws ImpalaException {
    // The phase-1 aggregation node is already in the child fragment.
    Preconditions.checkState(phase2AggNode.getChild(0) == childFragment.getPlanRoot());

    AggregateInfo phase1AggInfo = ((AggregationNode) phase2AggNode.getChild(0))
        .getAggInfo();
    // We need to do
    // - child fragment:
    //   * phase-1 aggregation
    // - first merge fragment, hash-partitioned on grouping and distinct exprs:
    //   * merge agg of phase-1
    //   * phase-2 agg
    // - second merge fragment, partitioned on grouping exprs or unpartitioned
    //   without grouping exprs
    //   * merge agg of phase-2
    // With grouping, the output partition exprs of the child are the (input) grouping
    // exprs of the parent. The grouping exprs reference the output tuple of phase-1
    // but the partitioning happens on the intermediate tuple of the phase-1.
    ArrayList<Expr> partitionExprs = Expr.substituteList(
        phase1AggInfo.getGroupingExprs(), phase1AggInfo.getIntermediateSmap(),
        ctx_.getRootAnalyzer(), false);

    PlanFragment firstMergeFragment;
    boolean childHasCompatPartition = ctx_.getRootAnalyzer().equivSets(partitionExprs,
        childFragment.getDataPartition().getPartitionExprs());
    if (childHasCompatPartition) {
      // The data is already partitioned on the required expressions, we can skip the
      // phase-1 merge step.
      childFragment.addPlanRoot(phase2AggNode);
      firstMergeFragment = childFragment;
    } else {
      DataPartition mergePartition = DataPartition.hashPartitioned(partitionExprs);
      // Convert the existing node to a preaggregation.
      AggregationNode preaggNode = (AggregationNode)phase2AggNode.getChild(0);
      preaggNode.setIsPreagg(ctx_);

      // place phase-1 merge aggregation step in a new fragment
      firstMergeFragment = createParentFragment(childFragment, mergePartition);
      AggregateInfo phase1MergeAggInfo = phase1AggInfo.getMergeAggInfo();
      AggregationNode phase1MergeAggNode =
          new AggregationNode(ctx_.getNextNodeId(), preaggNode, phase1MergeAggInfo);
      phase1MergeAggNode.init(ctx_.getRootAnalyzer());
      phase1MergeAggNode.unsetNeedsFinalize();
      phase1MergeAggNode.setIntermediateTuple();
      firstMergeFragment.addPlanRoot(phase1MergeAggNode);

      // the phase-2 aggregation consumes the output of the phase-1 merge agg;
      // if there is a limit, it had already been placed with the phase-2 aggregation
      // step (which is where it should be)
      firstMergeFragment.addPlanRoot(phase2AggNode);
      fragments.add(firstMergeFragment);
    }
    phase2AggNode.unsetNeedsFinalize();
    phase2AggNode.setIntermediateTuple();
    // Limit should be applied at the final merge aggregation node
    long limit = phase2AggNode.getLimit();
    phase2AggNode.unsetLimit();

    DataPartition mergePartition;
    if (phase2AggNode.getAggInfo().getGroupingExprs().isEmpty()) {
      mergePartition = DataPartition.UNPARTITIONED;
    } else {
      phase2AggNode.setIsPreagg(ctx_);
      mergePartition = DataPartition.hashPartitioned(
          phase2AggNode.getAggInfo().getMergeAggInfo().getGroupingExprs());
    }
    PlanFragment secondMergeFragment =
        createParentFragment(firstMergeFragment, mergePartition);
    AggregationNode phase2MergeAggNode = new AggregationNode(ctx_.getNextNodeId(),
        phase2AggNode, phase2AggNode.getAggInfo().getMergeAggInfo());
    phase2MergeAggNode.init(ctx_.getRootAnalyzer());
    phase2MergeAggNode.setLimit(limit);
    // Transfer having predicates to final merge agg node
    phase2AggNode.transferConjuncts(phase2MergeAggNode);
    secondMergeFragment.addPlanRoot(phase2MergeAggNode);
    return secondMergeFragment;
  }

  /**
   * Returns a fragment that produces the output of either an AnalyticEvalNode
   * or of the SortNode that provides the input to an AnalyticEvalNode.
   * ('node' can be either an AnalyticEvalNode or a SortNode).
   * The returned fragment is either partitioned on the Partition By exprs or
   * unpartitioned in the absence of such exprs.
   */
  private PlanFragment createAnalyticFragment(PlanNode node,
      PlanFragment childFragment, ArrayList<PlanFragment> fragments)
      throws ImpalaException {
    Preconditions.checkState(
        node instanceof SortNode || node instanceof AnalyticEvalNode);
    if (node instanceof AnalyticEvalNode) {
      AnalyticEvalNode analyticNode = (AnalyticEvalNode) node;
      if (analyticNode.getPartitionExprs().isEmpty()
          && analyticNode.getOrderByElements().isEmpty()) {
        // no Partition-By/Order-By exprs: compute analytic exprs in single
        // unpartitioned fragment
        PlanFragment fragment = childFragment;
        if (childFragment.isPartitioned()) {
          fragment = createParentFragment(childFragment, DataPartition.UNPARTITIONED);
        }
        fragment.addPlanRoot(analyticNode);
        return fragment;
      } else {
        childFragment.addPlanRoot(analyticNode);
        return childFragment;
      }
    }

    SortNode sortNode = (SortNode) node;
    Preconditions.checkState(sortNode.isAnalyticSort());
    PlanFragment analyticFragment = childFragment;
    if (sortNode.getInputPartition() != null) {
      sortNode.getInputPartition().substitute(
          childFragment.getPlanRoot().getOutputSmap(), ctx_.getRootAnalyzer());
      // Make sure the childFragment's output is partitioned as required by the sortNode.
      // Even if the fragment and the sort partition exprs are equal, an exchange is
      // required if the sort partition exprs reference a tuple that is made nullable in
      // 'childFragment' to bring NULLs from outer-join non-matches together.
      DataPartition sortPartition = sortNode.getInputPartition();
      if (!childFragment.getDataPartition().equals(sortPartition)
          || childFragment.refsNullableTupleId(sortPartition.getPartitionExprs())) {
        analyticFragment = createParentFragment(childFragment, sortPartition);
      }
    }
    analyticFragment.addPlanRoot(sortNode);
    return analyticFragment;
  }

  /**
   * Returns a new unpartitioned fragment that materializes the result of the given
   * SortNode. If the child fragment is partitioned, returns a new fragment with a
   * sort-merging exchange that merges the results of the partitioned sorts.
   * The offset and limit are adjusted in the child and parent plan nodes to produce
   * the correct result.
   */
  private PlanFragment createOrderByFragment(SortNode node,
      PlanFragment childFragment, ArrayList<PlanFragment> fragments)
      throws ImpalaException {
    node.setChild(0, childFragment.getPlanRoot());
    childFragment.addPlanRoot(node);
    if (!childFragment.isPartitioned()) return childFragment;

    // Remember original offset and limit.
    boolean hasLimit = node.hasLimit();
    long limit = node.getLimit();
    long offset = node.getOffset();

    // Create a new fragment for a sort-merging exchange.
    PlanFragment mergeFragment =
        createParentFragment(childFragment, DataPartition.UNPARTITIONED);
    ExchangeNode exchNode = (ExchangeNode) mergeFragment.getPlanRoot();

    // Set limit, offset and merge parameters in the exchange node.
    exchNode.unsetLimit();
    if (hasLimit) exchNode.setLimit(limit);
    exchNode.setMergeInfo(node.getSortInfo(), offset);

    // Child nodes should not process the offset. If there is a limit,
    // the child nodes need only return (offset + limit) rows.
    SortNode childSortNode = (SortNode) childFragment.getPlanRoot();
    Preconditions.checkState(node == childSortNode);
    if (hasLimit) {
      childSortNode.unsetLimit();
      childSortNode.setLimit(limit + offset);
    }
    childSortNode.setOffset(0);
    childSortNode.computeStats(ctx_.getRootAnalyzer());
    exchNode.computeStats(ctx_.getRootAnalyzer());

    return mergeFragment;
  }
}
