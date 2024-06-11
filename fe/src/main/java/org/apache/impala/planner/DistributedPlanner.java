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

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.DmlStatementBase;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InsertStmt;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.MultiAggregateInfo.AggPhase;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.planner.JoinNode.DistributionMode;
import org.apache.impala.thrift.TPartitionType;
import org.apache.impala.thrift.TVirtualColumnType;
import org.apache.impala.util.KuduUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;

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
  public List<PlanFragment> createPlanFragments(
      PlanNode singleNodePlan) throws ImpalaException {
    Preconditions.checkState(!ctx_.isSingleNodeExec());
    QueryStmt queryStmt = ctx_.getQueryStmt();
    List<PlanFragment> fragments = new ArrayList<>();
    // For DML statements, unless there is a limit, leave the root fragment
    // partitioned, otherwise merge everything into a single coordinator fragment,
    // so we can pass it back to the client.
    boolean isPartitioned = false;
    if (ctx_.hasTableSink() && !singleNodePlan.hasLimit()) {
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
  public PlanFragment createPlanFragments(
      PlanNode root, boolean isPartitioned, List<PlanFragment> fragments)
      throws ImpalaException {
    List<PlanFragment> childFragments = new ArrayList<>();
    for (PlanNode child: root.getChildren()) {
      // allow child fragments to be partitioned, unless they contain a limit clause
      // (the result set with the limit constraint needs to be computed centrally);
      // merge later if needed
      boolean childIsPartitioned = child.allowPartitioned();

      // Do not fragment the subplan of a SubplanNode since it is executed locally.
      if (root instanceof SubplanNode && child == root.getChild(1)) continue;
      childFragments.add(createPlanFragments(child, childIsPartitioned, fragments));
    }

    PlanFragment result = null;
    if (root instanceof ScanNode) {
      if (root instanceof IcebergMetadataScanNode) {
        result = createIcebergMetadataScanFragment(root);
        fragments.add(result);
      } else {
        result = createScanFragment(root);
        fragments.add(result);
      }
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
    } else if (root instanceof CardinalityCheckNode) {
      result = createCardinalityCheckNodeFragment((CardinalityCheckNode) root, childFragments);
    } else if (root instanceof IcebergDeleteNode) {
      Preconditions.checkState(childFragments.size() == 2);
      result = createIcebergDeleteFragment((IcebergDeleteNode) root,
          childFragments.get(0), childFragments.get(1));
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
    Preconditions.checkNotNull(exprs);
    return exprs.isEmpty() ? 1 : Expr.getNumDistinctValues(exprs);
  }

  public PlanFragment createInsertFragment(
    PlanFragment inputFragment, DmlStatementBase dmlStmt, Analyzer analyzer,
      List<PlanFragment> fragments)
      throws ImpalaException {
    return createDmlFragment(inputFragment, dmlStmt, analyzer, fragments);
  }

  /**
   * Decides whether to repartition the output of 'inputFragment' before feeding
   * its data into the table sink of the given 'insertStmt'. The decision obeys
   * the shuffle/noshuffle plan hints if present unless MAX_FS_WRITERS
   * or COMPUTE_PROCESSING_COST query option is used where the noshuffle hint is
   * ignored. The decision is based on a number of factors including, whether the target
   * table is partitioned or unpartitioned, the input fragment and the target table's
   * partition expressions, expected number of output partitions, num of nodes on which
   * the input partition will run, whether MAX_FS_WRITERS or COMPUTE_PROCESSING_COST
   * query option is used. If this functions ends up creating a new fragment, appends
   * that to 'fragments'.
   */
  public PlanFragment createDmlFragment(
      PlanFragment inputFragment, DmlStatementBase dmlStmt, Analyzer analyzer,
      List<PlanFragment> fragments)
      throws ImpalaException {
    boolean isComputeCost = analyzer.getQueryOptions().isCompute_processing_cost();
    boolean enforce_hdfs_writer_limit = dmlStmt.getTargetTable() instanceof FeFsTable
        && (analyzer.getQueryOptions().getMax_fs_writers() > 0 || isComputeCost);

    if (dmlStmt.hasNoShuffleHint() && !enforce_hdfs_writer_limit) return inputFragment;

    List<Expr> partitionExprs = Lists.newArrayList(dmlStmt.getPartitionKeyExprs());
    // Ignore constants for the sake of partitioning.
    Expr.removeConstants(partitionExprs);

    // Do nothing if the input fragment is already appropriately partitioned. TODO: handle
    // Kudu tables here (IMPALA-5254).
    DataPartition inputPartition = inputFragment.getDataPartition();
    if (!partitionExprs.isEmpty()
        && analyzer.setsHaveValueTransfer(inputPartition.getPartitionExprs(),
            partitionExprs, true)
        && !(dmlStmt.getTargetTable() instanceof FeKuduTable)
        && !enforce_hdfs_writer_limit) {
      return inputFragment;
    }

    int maxHdfsWriters = analyzer.getQueryOptions().getMax_fs_writers();
    // We also consider fragments containing union nodes along with scan fragments
    // (leaf fragments) since they are either a part of those scan fragments or are
    // co-located with them to maintain parallelism.
    List<HdfsScanNode> hdfsScanNodes = Lists.newArrayList();
    inputFragment.collectPlanNodes(
        Predicates.instanceOf(HdfsScanNode.class), hdfsScanNodes);
    List<UnionNode> unionNodes = Lists.newArrayList();
    inputFragment.collectPlanNodes(Predicates.instanceOf(UnionNode.class), unionNodes);
    boolean hasHdfsScanORUnion = !hdfsScanNodes.isEmpty() || !unionNodes.isEmpty();

    int expectedNumInputInstance = inputFragment.getNumInstances();
    if (enforce_hdfs_writer_limit && isComputeCost) {
      // Default to minParallelism * numNodes if cardinality or average row size is
      // unknown.
      int costBasedMaxWriter = IntMath.saturatedMultiply(
          inputFragment.getNumNodes(), analyzer.getMinParallelismPerNode());

      PlanNode root = inputFragment.getPlanRoot();
      if (root.getCardinality() > -1 && root.getAvgRowSize() > -1) {
        // Both cardinality and avg row size is known.
        // Estimate such that each writer will work on at least MIN_WRITE_BYTES of rows.
        // However, if this is a partitioned insert, the output volume will be divided
        // into several partitions. In that case, consider totalNumPartitions so that:
        // total num writers is close to totalNumPartitions.
        int totalNumPartitions = (int) Math.min(
            Integer.MAX_VALUE, Math.max(1, getNumDistinctValues(partitionExprs)));
        int minNumWriter = Math.min(totalNumPartitions, inputFragment.getNumNodes());
        int maxNumWriter = Math.min(totalNumPartitions,
            IntMath.saturatedMultiply(
                inputFragment.getNumNodes(), analyzer.getMaxParallelismPerNode()));
        costBasedMaxWriter = (int) Math.round(
            Math.ceil((root.getAvgRowSize() / HdfsTableSink.MIN_WRITE_BYTES)
                * root.getCardinality()));
        costBasedMaxWriter =
            Math.min(maxNumWriter, Math.max(minNumWriter, costBasedMaxWriter));
      }

      if (maxHdfsWriters > 0) {
        // Pick min between MAX_FS_WRITER option and costBasedMaxWriter.
        maxHdfsWriters = Math.min(maxHdfsWriters, costBasedMaxWriter);
      } else {
        // User does not set MAX_FS_WRITER option.
        maxHdfsWriters = costBasedMaxWriter;
      }
      Preconditions.checkState(maxHdfsWriters > 0);
      dmlStmt.setMaxTableSinks(maxHdfsWriters);
      // At this point, parallelism of writer fragment is fixed and will not be adjusted
      // by costing phase.

      if (!hdfsScanNodes.isEmpty() && fragments.size() == 1) {
        // If input fragment have HdfsScanNode and input fragment is the only fragment in
        // the plan, check for opportunity to collocate scan nodes and table sink.
        // Since the actual costing phase only happens later after distributed plan
        // created, this code redundantly compute the scan cost ahead of costing phase
        // to help estimate the scan parallelism.
        int maxScanThread = 1;
        for (HdfsScanNode scanNode : hdfsScanNodes) {
          ProcessingCost scanCost =
              scanNode.computeScanProcessingCost(analyzer.getQueryOptions());
          maxScanThread = Math.max(
              maxScanThread, scanCost.getNumInstanceMax(inputFragment.getNumNodes()));
        }
        // Override expectedNumInputInstance so that collocation may happen
        // (case 3 in branch below).
        expectedNumInputInstance = maxScanThread;
      }
    }

    // Make a cost-based decision only if no user hint was supplied.
    if (!dmlStmt.hasShuffleHint()) {
      if (dmlStmt.getTargetTable() instanceof FeKuduTable) {
        // If the table is unpartitioned or all of the partition exprs are constants,
        // don't insert the exchange.
        // TODO: make a more sophisticated decision here for partitioned tables and when
        // we have info about tablet locations.
        if (partitionExprs.isEmpty()) return inputFragment;
      } else if (!enforce_hdfs_writer_limit || !hasHdfsScanORUnion
          || (expectedNumInputInstance <= maxHdfsWriters)) {
        // Only consider skipping the addition of an exchange node if
        // 1. The hdfs writer limit does not apply
        // 2. Writer limit applies and there are no hdfs scan or union nodes. In this
        //    case we will restrict the number of instances of this internal fragment.
        // 3. Writer limit applies and there is a scan node or union node, but its num
        //    of instances are already under the writer limit.
        // Basically covering all cases where we don't mind restricting the parallelism
        // of their instances.
        Preconditions.checkState(
            expectedNumInputInstance <= inputFragment.getNumInstances());
        int input_instances = expectedNumInputInstance;
        if (enforce_hdfs_writer_limit && !hasHdfsScanORUnion) {
          // For an internal fragment we enforce an upper limit based on the
          // resulting maxHdfsWriters.
          Preconditions.checkState(maxHdfsWriters > 0);
          input_instances = Math.min(input_instances, maxHdfsWriters);
        }
        // If the existing partition exprs are a subset of the table partition exprs,
        // check if it is distributed across all nodes. If so, don't repartition.
        if (Expr.isSubset(inputPartition.getPartitionExprs(), partitionExprs)) {
          long numPartitions = getNumDistinctValues(inputPartition.getPartitionExprs());
          if (numPartitions >= input_instances) {
            return inputFragment;
          }
        }

        // Don't repartition if we know we have fewer partitions than nodes
        // (ie, default to repartitioning if col stats are missing).
        // TODO: We want to repartition if the resulting files would otherwise
        // be very small (less than some reasonable multiple of the recommended block
        // size). In order to do that, we need to come up with an estimate of the avg row
        // size in the particular file format of the output table/partition.
        // We should always know on how many nodes our input is running.
        long numPartitions = getNumDistinctValues(partitionExprs);
        Preconditions.checkState(expectedNumInputInstance != -1);
        if (numPartitions > 0 && numPartitions <= input_instances) {
          return inputFragment;
        }
      }
    }

    ExchangeNode exchNode =
        new ExchangeNode(ctx_.getNextNodeId(), inputFragment.getPlanRoot());
    exchNode.init(analyzer);
    Preconditions.checkState(exchNode.hasValidStats());
    DataPartition partition;
    if (partitionExprs.isEmpty()) {
      if (enforce_hdfs_writer_limit
          && inputFragment.getDataPartition().getType() == TPartitionType.RANDOM) {
        // This ensures the parallelism of the writers is maintained while maintaining
        // legacy behavior(when not using MAX_FS_WRITER query option).
        partition = DataPartition.RANDOM;
      } else {
        partition = DataPartition.UNPARTITIONED;
      }
    } else if (dmlStmt instanceof InsertStmt &&
               dmlStmt.getTargetTable() instanceof FeKuduTable) {
      partition = DataPartition.kuduPartitioned(
          KuduUtil.createPartitionExpr((InsertStmt)dmlStmt, ctx_.getRootAnalyzer()));
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
   * Create an Iceberg Metadata scan fragment. This fragment is marked as a coordinator
   * only fragment.
   */
  private PlanFragment createIcebergMetadataScanFragment(PlanNode node) {
    return new PlanFragment(ctx_.getNextFragmentId(), node, DataPartition.UNPARTITIONED,
        /* coordinatorOnly */ true);
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
      List<PlanFragment> fragments) throws ImpalaException {
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
      List<PlanFragment> fragments) throws ImpalaException {
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
    lhsExchange.computeStats(ctx_.getRootAnalyzer());
    node.setChild(0, lhsExchange);
    ExchangeNode rhsExchange =
        new ExchangeNode(ctx_.getNextNodeId(), rightChildFragment.getPlanRoot());
    rhsExchange.computeStats(ctx_.getRootAnalyzer());
    node.setChild(1, rhsExchange);

    // Connect the child fragments in a new fragment, and set the data partition
    // of the new fragment and its child fragments.
    DataPartition outputPartition;
    switch(node.getJoinOp()) {
      // For full outer joins the null values of the lhs/rhs join exprs are not
      // partitioned so random partition is the best we have now.
      case FULL_OUTER_JOIN:
        outputPartition = DataPartition.RANDOM;
        break;
      // For right anti and semi joins the lhs join slots does not appear in the output.
      case RIGHT_ANTI_JOIN:
      case RIGHT_SEMI_JOIN:
      // For right outer joins the null values of the lhs join expr are not partitioned.
      case RIGHT_OUTER_JOIN:
        outputPartition = rhsJoinPartition;
        break;
      // Otherwise we're good to use the lhs partition.
      default:
        outputPartition = lhsJoinPartition;
    }
    PlanFragment joinFragment =
        new PlanFragment(ctx_.getNextFragmentId(), node, outputPartition);
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
      PlanFragment leftChildFragment, List<PlanFragment> fragments)
      throws ImpalaException {
    // For both join types, the total cost is calculated as the amount of data
    // sent over the network, plus the amount of data inserted into the hash table.
    // broadcast: send the rightChildFragment's output to each node executing
    // the leftChildFragment, and build a hash table with it on each node.
    Analyzer analyzer = ctx_.getRootAnalyzer();
    PlanNode rhsTree = rightChildFragment.getPlanRoot();
    long rhsDataSize = -1;
    long broadcastCost = -1;
    int mt_dop = ctx_.getQueryOptions().mt_dop;
    int leftChildNodes = leftChildFragment.getNumNodes();
    if (rhsTree.getCardinality() != -1) {
      rhsDataSize = Math.round(
          rhsTree.getCardinality() * ExchangeNode.getAvgSerializedRowSize(rhsTree));
      if (leftChildNodes != -1) {
        // RHS data must be broadcast once to each node.
        // TODO: IMPALA-9176: this is inaccurate for NAAJ until IMPALA-9176 is fixed
        // because it must be broadcast once per instance.
        long dataPayload = rhsDataSize * leftChildNodes;
        long hashTblBuildCost = dataPayload;
        if (mt_dop > 1 && ctx_.getQueryOptions().use_dop_for_costing) {
          // In the broadcast join a single thread per node is building the hash
          // table of size N compared to the partition case where m threads are
          // building hash tables of size N/m each (assuming uniform distribution).
          // Hence, the build side is faster in the latter case. For relative costing,
          // we multiply the hash table build cost by C sqrt(m) where m = plan node's
          // parallelism and C = a coefficient that controls the function's rate of
          // growth (a tunable parameter). We use the sqrt to model a non-linear
          // function since the slowdown with broadcast is not exactly linear.
          // TODO: more analysis is needed to establish an accurate correlation.
          // TODO: revisit this calculation if COMPUTE_PROCESSING_COST=true.
          //   Num instances might change during Planner.computeProcessingCost(),
          //   later after parallel plan created.
          PlanNode leftPlanRoot = leftChildFragment.getPlanRoot();
          int actual_dop = leftPlanRoot.getNumInstances()/leftPlanRoot.getNumNodes();
          hashTblBuildCost *= (long) (ctx_.getQueryOptions().broadcast_to_partition_factor
              * Math.max(1.0, Math.sqrt(actual_dop)));
        }
        broadcastCost = dataPayload + hashTblBuildCost;
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("broadcast: cost=" + Long.toString(broadcastCost));
      LOG.trace("card=" + Long.toString(rhsTree.getCardinality()) + " row_size="
          + Float.toString(rhsTree.getAvgRowSize()) + " #nodes="
          + Integer.toString(leftChildNodes));
    }

    // repartition: both left- and rightChildFragment are partitioned on the
    // join exprs, and a hash table is built with the rightChildFragment's output.
    PlanNode lhsTree = leftChildFragment.getPlanRoot();
    List<Expr> lhsJoinExprs = new ArrayList<>();
    List<Expr> rhsJoinExprs = new ArrayList<>();
    for (Expr joinConjunct: node.getEqJoinConjuncts()) {
      // no remapping necessary
      lhsJoinExprs.add(joinConjunct.getChild(0).clone());
      rhsJoinExprs.add(joinConjunct.getChild(1).clone());
    }
    boolean lhsHasCompatPartition = false;
    boolean rhsHasCompatPartition = false;
    long partitionCost = -1;
    if (lhsTree.getCardinality() != -1 && rhsTree.getCardinality() != -1) {
      lhsHasCompatPartition = analyzer.setsHaveValueTransfer(
          leftChildFragment.getDataPartition().getPartitionExprs(), lhsJoinExprs,false);
      rhsHasCompatPartition = analyzer.setsHaveValueTransfer(
          rightChildFragment.getDataPartition().getPartitionExprs(), rhsJoinExprs, false);

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
   * Creates a fragment for an IcebergDeleteNode with DIRECTED distribution mode.
   * Similarly to a BROADCAST join, the left child of the join is in the same fragment
   * with the join itself, while the right child is in a separate fragment.
   */
  private PlanFragment createIcebergDeleteFragment(IcebergDeleteNode node,
      PlanFragment leftChildFragment, PlanFragment rightChildFragment)
          throws ImpalaException {
    Preconditions.checkState(node.getEqJoinConjuncts().size() == 2);
    BinaryPredicate filePathEq = node.getEqJoinConjuncts().get(1);

    // Verify that the partitioning is based on file path
    Preconditions.checkState(
        ((SlotRef) filePathEq.getChild(0)).getDesc().getVirtualColumnType()
        == TVirtualColumnType.INPUT_FILE_NAME);

    node.setDistributionMode(DistributionMode.DIRECTED);

    // Doesn't create a new fragment, but modifies leftChildFragment to execute
    // the join; the build input is provided by an ExchangeNode, which is the
    // destination of the rightChildFragment's output
    node.setChild(0, leftChildFragment.getPlanRoot());
    rightChildFragment.setOutputPartition(DataPartition.DIRECTED);
    connectChildFragment(node, 1, leftChildFragment, rightChildFragment);
    leftChildFragment.setPlanRoot(node);
    return leftChildFragment;
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

   // Decide the distribution mode based on the estimated costs, the mem limit and
   // the broadcast bytes limit. The last value is a safety check to ensure we
   // don't broadcast very large inputs (for example in case the broadcast cost was
   // not computed correctly and the query mem limit has not been set or set too high)
   long htSize = Math.round(rhsDataSize * PlannerContext.HASH_TBL_SPACE_OVERHEAD);
   long memLimit = ctx_.getQueryOptions().mem_limit;
   long broadcast_bytes_limit = ctx_.getQueryOptions().getBroadcast_bytes_limit();

   if (broadcastCost <= partitionCost && (memLimit == 0 || htSize <= memLimit) &&
           (broadcast_bytes_limit == 0 || htSize <= broadcast_bytes_limit)) {
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
      if (!analyzer.exprsHaveValueTransfer(lhsPartExprs.get(i), rhsPartExprs.get(i),
          true)) {
        return false;
      }
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
    List<Expr> resultPartExprs = new ArrayList<>();
    for (Expr srcPartExpr : srcPartExprs) {
      for (int j = 0; j < srcJoinExprs.size(); ++j) {
        if (analyzer.exprsHaveValueTransfer(srcPartExpr, srcJoinExprs.get(j), false)) {
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
      List<PlanFragment> childFragments, List<PlanFragment> fragments)
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
      List<PlanFragment> childFragments) {
    Preconditions.checkState(selectNode.getChildren().size() == childFragments.size());
    PlanFragment childFragment = childFragments.get(0);
    // set the child explicitly, an ExchangeNode might have been inserted
    // (whereas selectNode.child[0] would point to the original child)
    selectNode.setChild(0, childFragment.getPlanRoot());
    childFragment.setPlanRoot(selectNode);
    return childFragment;
  }

  /**
   * Adds the CardinalityCheckNode as the new plan root to the child fragment and returns
   * the child fragment.
   */
  private PlanFragment createCardinalityCheckNodeFragment(
      CardinalityCheckNode cardinalityCheckNode,
      List<PlanFragment> childFragments) throws ImpalaException {
    PlanFragment childFragment = childFragments.get(0);
    // The cardinality check must execute on a single node.
    if (childFragment.getOutputPartition().isPartitioned()) {
      childFragment = createMergeFragment(childFragment);
    }
    // Set the child explicitly, an ExchangeNode might have been inserted
    // (whereas cardinalityCheckNode.child[0] would point to the original child)
    cardinalityCheckNode.setChild(0, childFragment.getPlanRoot());
    childFragment.setPlanRoot(cardinalityCheckNode);
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

  private PlanFragment createParentFragment(
      PlanFragment childFragment, DataPartition parentPartition) throws ImpalaException {
    return createParentFragment(childFragment, parentPartition, false);
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
      PlanFragment childFragment, DataPartition parentPartition, boolean unsetLimit)
      throws ImpalaException {
    ExchangeNode exchangeNode =
        new ExchangeNode(ctx_.getNextNodeId(), childFragment.getPlanRoot());
    exchangeNode.init(ctx_.getRootAnalyzer());
    if (unsetLimit) exchangeNode.unsetLimit();
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
   * If 'node' is phase 1 of a 2-phase DISTINCT aggregation or the 'transpose' phase of a
   * multiple-distinct aggregation, this will simply add 'node' to the child fragment and
   * return the child fragment; the new fragment will be created by the call of
   * createAggregationFragment() for the phase 2 AggregationNode.
   */
  private PlanFragment createAggregationFragment(AggregationNode node,
      PlanFragment childFragment, List<PlanFragment> fragments)
      throws ImpalaException {
    if (!childFragment.isPartitioned() || node.getAggPhase() == AggPhase.TRANSPOSE) {
      // nothing to distribute; do full aggregation directly within childFragment
      childFragment.addPlanRoot(node);
      return childFragment;
    }

    if (node.isDistinctAgg()) {
      // 'node' is phase 1 of a DISTINCT aggregation; the actual agg fragment
      // will get created in the next createAggregationFragment() call
      // for the parent AggregationNode
      childFragment.addPlanRoot(node);
      return childFragment;
    }

    // Check if 'node' is phase 2 of a DISTINCT aggregation.
    boolean isDistinct = node.getChild(0) instanceof AggregationNode
        && ((AggregationNode) (node.getChild(0))).isDistinctAgg();
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
      AggregationNode node, PlanFragment childFragment) throws ImpalaException {
    Preconditions.checkArgument(childFragment.isPartitioned());
    List<Expr> partitionExprs = node.getMergePartitionExprs(ctx_.getRootAnalyzer());
    boolean hasGrouping = !partitionExprs.isEmpty();
    DataPartition parentPartition = null;
    if (hasGrouping) {
      boolean childHasCompatPartition = node.isSingleClassAgg()
          && ctx_.getRootAnalyzer().setsHaveValueTransfer(partitionExprs,
                 childFragment.getDataPartition().getPartitionExprs(), true);
      if (childHasCompatPartition) {
        // The data is already partitioned on the required expressions. We can do the
        // aggregation in the child fragment without an extra merge step.
        // An exchange+merge step is required if the grouping exprs reference a tuple
        // that is made nullable in 'childFragment' to bring NULLs from outer-join
        // non-matches together.
        childFragment.addPlanRoot(node);
        return childFragment;
      }
      parentPartition = DataPartition.hashPartitioned(partitionExprs);
    } else {
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
    if (node.getMultiAggInfo().hasAggregateExprs() || !node.getConjuncts().isEmpty()) {
      node.unsetLimit();
    }
    node.unsetNeedsFinalize();

    // place a merge aggregation step in a new fragment
    PlanFragment mergeFragment = createParentFragment(childFragment, parentPartition);
    AggregationNode mergeAggNode = new AggregationNode(ctx_.getNextNodeId(),
        mergeFragment.getPlanRoot(), node.getMultiAggInfo(), AggPhase.FIRST_MERGE);
    mergeAggNode.init(ctx_.getRootAnalyzer());
    mergeAggNode.setLimit(limit);
    // Carry the IsNonCorrelatedSclarSubquery_ flag to the merge node. This flag is
    // applicable regardless of the partition scheme for the children since it is a
    // logical property.
    mergeAggNode.setIsNonCorrelatedScalarSubquery(node.isNonCorrelatedScalarSubquery());
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
      List<PlanFragment> fragments) throws ImpalaException {
    // The phase-1 aggregation node is already in the child fragment.
    Preconditions.checkState(phase2AggNode.getChild(0) == childFragment.getPlanRoot());
    // When a query has both grouping and distinct exprs, Impala can optionally include
    // the distinct exprs in the hash exchange of the first aggregation phase to spread
    // the data among more nodes. However, this plan requires another hash exchange on
    // the grouping exprs in the second phase which is not required when omitting the
    // distinct exprs in the first phase. Shuffling by both is better if the grouping
    // exprs have low NDVs.
    boolean shuffleDistinctExprs = ctx_.getQueryOptions().shuffle_distinct_exprs;
    boolean hasGrouping = phase2AggNode.hasGrouping();

    AggregationNode phase1AggNode = ((AggregationNode) phase2AggNode.getChild(0));
    // With grouping, the output partition exprs of the child are the (input) grouping
    // exprs of the parent. The grouping exprs reference the output tuple of phase-1
    // but the partitioning happens on the intermediate tuple of the phase-1.
    List<Expr> phase1PartitionExprs =
        phase1AggNode.getMergePartitionExprs(ctx_.getRootAnalyzer());

    PlanFragment firstMergeFragment;
    boolean childHasCompatPartition = phase1AggNode.isSingleClassAgg()
        && ctx_.getRootAnalyzer().setsHaveValueTransfer(phase1PartitionExprs,
               childFragment.getDataPartition().getPartitionExprs(), true);
    if (childHasCompatPartition) {
      // The data is already partitioned on the required expressions, we can skip the
      // phase-1 merge step.
      childFragment.addPlanRoot(phase2AggNode);
      firstMergeFragment = childFragment;
    } else {
      phase1AggNode.setIntermediateTuple();
      phase1AggNode.setIsPreagg(ctx_);

      DataPartition phase1MergePartition =
          DataPartition.hashPartitioned(phase1PartitionExprs);

      // place phase-1 merge aggregation step in a new fragment
      firstMergeFragment = createParentFragment(childFragment, phase1MergePartition);
      AggregationNode phase1MergeAggNode = new AggregationNode(ctx_.getNextNodeId(),
          phase1AggNode, phase1AggNode.getMultiAggInfo(), AggPhase.FIRST_MERGE);
      phase1MergeAggNode.init(ctx_.getRootAnalyzer());
      phase1MergeAggNode.unsetNeedsFinalize();
      phase1MergeAggNode.setIntermediateTuple();
      firstMergeFragment.addPlanRoot(phase1MergeAggNode);

      // the phase-2 aggregation consumes the output of the phase-1 merge agg;
      // if there is a limit, it had already been placed with the phase-2 aggregation
      // step (which is where it should be)
      firstMergeFragment.addPlanRoot(phase2AggNode);
      if (shuffleDistinctExprs || !hasGrouping) fragments.add(firstMergeFragment);
    }
    if (!shuffleDistinctExprs && hasGrouping) return firstMergeFragment;

    phase2AggNode.unsetNeedsFinalize();
    phase2AggNode.setIntermediateTuple();
    // Limit should be applied at the final merge aggregation node
    long limit = phase2AggNode.getLimit();
    phase2AggNode.unsetLimit();

    DataPartition phase2MergePartition;
    List<Expr> phase2PartitionExprs =
        phase2AggNode.getMergePartitionExprs(ctx_.getRootAnalyzer());
    if (phase2PartitionExprs.isEmpty()) {
      phase2MergePartition = DataPartition.UNPARTITIONED;
    } else {
      phase2AggNode.setIsPreagg(ctx_);
      phase2MergePartition = DataPartition.hashPartitioned(phase2PartitionExprs);
    }
    PlanFragment secondMergeFragment =
        createParentFragment(firstMergeFragment, phase2MergePartition);

    AggregationNode phase2MergeAggNode = new AggregationNode(ctx_.getNextNodeId(),
        phase2AggNode, phase2AggNode.getMultiAggInfo(), AggPhase.SECOND_MERGE);
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
      PlanFragment childFragment, List<PlanFragment> fragments)
      throws ImpalaException {
    Preconditions.checkState(
        node instanceof SortNode || node instanceof AnalyticEvalNode);
    if (node instanceof AnalyticEvalNode) {
      AnalyticEvalNode analyticNode = (AnalyticEvalNode) node;
      if (analyticNode.requiresUnpartitionedEval()) {
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

    boolean addedLowerTopN = false;
    SortNode lowerTopN = null;
    AnalyticEvalNode analyticNode = sortNode.getAnalyticEvalNode();
    if (sortNode.getInputPartition() != null) {
      sortNode.getInputPartition().substitute(
          childFragment.getPlanRoot().getOutputSmap(), ctx_.getRootAnalyzer());
      // Make sure the childFragment's output is partitioned as required by the sortNode.
      DataPartition sortPartition = sortNode.getInputPartition();
      boolean hasNullableTupleIds = childFragment.getPlanRoot().
          getNullableTupleIds().size() > 0;
      boolean hasCompatiblePartition = false;
      if (hasNullableTupleIds) {
        // If the input stream has nullable tuple ids (produced from the nullable
        // side of an outer join), do an exact equality comparison of the child's
        // data partition with the required partition) since a hash exchange is
        // required to co-locate all the null values produced from the outer join
        // (these tuples may not be originally null but became null after OJ).
        hasCompatiblePartition = childFragment.getDataPartition().equals(sortPartition);
      } else {
        // Otherwise, a mutual value transfer is sufficient for compatible partitions.
        // E.g if analytic fragment's required partition key is t2.a2 and the child is
        // an inner join with t1.a1 = t2.a2, either a1 or a2 are sufficient to satisfy
        // the required partitioning.
        hasCompatiblePartition = ctx_.getRootAnalyzer().setsHaveValueTransfer(
            childFragment.getDataPartition().getPartitionExprs(),
            sortPartition.getPartitionExprs(), true);
      }
      if (!hasCompatiblePartition) {
        if (sortNode.isTypeTopN() || sortNode.isPartitionedTopN()) {
          lowerTopN = sortNode;
          childFragment.addPlanRoot(lowerTopN);
          // Update partitioning exprs to reference sort tuple.
          sortPartition.substitute(
                  sortNode.getSortInfo().getOutputSmap(), ctx_.getRootAnalyzer());
          addedLowerTopN = true;
          // When creating the analytic fragment, pass in a flag to unset the limit
          // on the partition exchange. This ensures that the exchange does not
          // prematurely stop sending rows in case there's a downstream operator
          // that has a LIMIT - for instance a Sort with LIMIT after the
          // Analytic operator.
          analyticFragment = createParentFragment(childFragment, sortPartition, true);
        } else {
          analyticFragment = createParentFragment(childFragment, sortPartition);
        }
      }
    }
    if (addedLowerTopN) {
      // Create the upper TopN node
      SortNode upperTopN;
      if (lowerTopN.isTypeTopN()) {
        upperTopN = SortNode.createTopNSortNode(ctx_.getQueryOptions(),
              ctx_.getNextNodeId(), childFragment.getPlanRoot(),
              lowerTopN.getSortInfo(), sortNode.getOffset(), lowerTopN.getSortLimit(),
              lowerTopN.isIncludeTies());
      } else {
        upperTopN = SortNode.createPartitionedTopNSortNode(
                ctx_.getNextNodeId(), childFragment.getPlanRoot(),
                lowerTopN.getSortInfo(), lowerTopN.getNumPartitionExprs(),
                lowerTopN.getPerPartitionLimit(), lowerTopN.isIncludeTies());
      }
      upperTopN.setIsAnalyticSort(true);
      upperTopN.init(ctx_.getRootAnalyzer());
      // connect this to the analytic eval node
      analyticNode.setChild(0, upperTopN);
      analyticFragment.addPlanRoot(upperTopN);
    } else {
      analyticFragment.addPlanRoot(sortNode);
    }
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
      PlanFragment childFragment, List<PlanFragment> fragments)
      throws ImpalaException {
    node.setChild(0, childFragment.getPlanRoot());
    childFragment.addPlanRoot(node);
    if (!childFragment.isPartitioned()) return childFragment;

    // Create a new fragment for a sort-merging exchange.
    PlanFragment mergeFragment =
        createParentFragment(childFragment, DataPartition.UNPARTITIONED);
    ExchangeNode exchNode = (ExchangeNode) mergeFragment.getPlanRoot();
    SortNode childSortNode = (SortNode) childFragment.getPlanRoot();
    if (node.isIncludeTies()) {
      Preconditions.checkState(node.getOffset() == 0,
              "Tie handling with offset not supported");
      if (node.isPartitionedTopN()) {
        // Partitioned TopN that returns ties needs special handling because ties are not
        // handled correctly by the ExchangeNode limit. We need to generate a partitioned
        // top-n on top of the exchange to correctly merge the input.
        SortNode parentSortNode = SortNode.createPartitionedTopNSortNode(
                ctx_.getNextNodeId(), exchNode, childSortNode.getSortInfo(),
                childSortNode.getNumPartitionExprs(),
                childSortNode.getPerPartitionLimit(), childSortNode.isIncludeTies());
        parentSortNode.init(ctx_.getRootAnalyzer());
        mergeFragment.addPlanRoot(parentSortNode);
      } else {
        Preconditions.checkState(node.isTypeTopN(), "only top-n handles ties");
        //TopN that returns ties needs special handling because ties are not handled
        // correctly by the ExchangeNode limit. We need to generate a top-n on top
        // of the exchange to correctly merge the input.
        SortNode parentSortNode = SortNode.createTopNSortNode(
                ctx_.getQueryOptions(), ctx_.getNextNodeId(), exchNode,
                childSortNode.getSortInfo(), 0, node.getSortLimit(),
                childSortNode.isIncludeTies());
        parentSortNode.init(ctx_.getRootAnalyzer());
        mergeFragment.addPlanRoot(parentSortNode);
      }
    } else {
      // Remember original offset and limit.
      boolean hasLimit = node.hasLimit();
      long limit = node.getLimit();
      long offset = node.getOffset();

      // Set limit, offset and merge parameters in the exchange node.
      exchNode.unsetLimit();
      if (hasLimit) exchNode.setLimit(limit);
      exchNode.setMergeInfo(node.getSortInfo(), offset);

      // Child nodes should not process the offset. If there is a limit,
      // the child nodes need only return (offset + limit) rows.
      Preconditions.checkState(node == childSortNode);
      if (hasLimit) {
        childSortNode.unsetLimit();
        childSortNode.setLimit(PlanNode.checkedAdd(limit, offset));
      }
      childSortNode.setOffset(0);
    }


    childSortNode.computeStats(ctx_.getRootAnalyzer());
    exchNode.computeStats(ctx_.getRootAnalyzer());
    return mergeFragment;
  }
}
