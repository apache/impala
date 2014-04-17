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
import java.util.Comparator;
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
import com.cloudera.impala.analysis.UnionStmt.UnionOperand;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.common.RuntimeEnv;
import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPartitionType;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TQueryOptions;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Joiner;
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

  private final IdGenerator<PlanNodeId> nodeIdGenerator_ = PlanNodeId.createGenerator();
  private final IdGenerator<PlanFragmentId> fragmentIdGenerator_ =
      PlanFragmentId.createGenerator();

  /**
   * Create plan fragments for an analyzed statement, given a set of execution options.
   * The fragments are returned in a list such that element i of that list can
   * only consume output of the following fragments j > i.
   */
  public ArrayList<PlanFragment> createPlanFragments(
      AnalysisContext.AnalysisResult analysisResult, TQueryOptions queryOptions)
      throws ImpalaException {
    // Set queryStmt from analyzed SELECT or INSERT query.
    QueryStmt queryStmt = null;
    if (analysisResult.isInsertStmt() ||
        analysisResult.isCreateTableAsSelectStmt()) {
      queryStmt = analysisResult.getInsertStmt().getQueryStmt();
    } else {
      queryStmt = analysisResult.getQueryStmt();
    }
    Analyzer analyzer = analysisResult.getAnalyzer();
    analyzer.computeEquivClasses();

    // Mark slots referenced by output exprs as materialized, prior to generating the
    // plan tree.
    // We need to mark the result exprs of the topmost select block as materialized, so
    // that PlanNode.init() can compute the final mem layout of materialized tuples
    // (the byte size of tuples is needed for cost computations).
    // TODO: instead of materializing everything produced by the plan root, derive
    // referenced slots from destination fragment and add a materialization node
    // if not all output is needed by destination fragment
    // TODO 2: should the materialization decision be cost-based?
    if (queryStmt.getBaseTblResultExprs() != null) {
      analyzer.materializeSlots(queryStmt.getBaseTblResultExprs());
    }

    PlanNode singleNodePlan =
        createQueryPlan(queryStmt, analyzer, queryOptions.getDefault_order_by_limit());
    Preconditions.checkNotNull(singleNodePlan);
    LOG.trace("desctbl: " + analyzer.getDescTbl().debugString());

    ArrayList<PlanFragment> fragments = Lists.newArrayList();
    if (queryOptions.num_nodes == 1) {
      // single-node execution; we're almost done
      singleNodePlan =
          addUnassignedConjuncts(analyzer, singleNodePlan.getTupleIds(), singleNodePlan);
      fragments.add(new PlanFragment(
          fragmentIdGenerator_.getNextId(), singleNodePlan, DataPartition.UNPARTITIONED));
    } else {
      // For inserts or CTAS, unless there is a limit clause, leave the root fragment
      // partitioned, otherwise merge everything into a single coordinator fragment,
      // so we can pass it back to the client.
      boolean isPartitioned = false;
      if ((analysisResult.isInsertStmt() || analysisResult.isCreateTableAsSelectStmt())
          && !queryStmt.hasLimitClause()) {
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
        rootFragment = createInsertFragment(
            rootFragment, insertStmt, analyzer, fragments);
      }

      // set up table sink for root fragment
      rootFragment.setSink(insertStmt.createDataSink());
    }

    if (analysisResult.isInsertStmt()) {
      rootFragment.setOutputExprs(analysisResult.getInsertStmt().getResultExprs());
    } else {
      rootFragment.setOutputExprs(queryStmt.getBaseTblResultExprs());
    }

    LOG.debug("finalize plan fragments");
    for (PlanFragment fragment: fragments) {
      fragment.finalize(analyzer, !queryOptions.allow_unsupported_formats);
    }

    Collections.reverse(fragments);
    return fragments;
  }

  /**
   * Return combined explain string for all plan fragments.
   * Includes the estimated resource requirements from the request if set.
   */
  public String getExplainString(ArrayList<PlanFragment> fragments,
      TQueryExecRequest request, TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
    boolean hasHeader = false;
    if (request.isSetPer_host_mem_req() && request.isSetPer_host_vcores()) {
      str.append(
          String.format("Estimated Per-Host Requirements: Memory=%s VCores=%s\n",
          PrintUtils.printBytes(request.getPer_host_mem_req()),
          request.per_host_vcores));
      hasHeader = true;
    }
    // Append warning about tables missing stats.
    if (request.query_ctxt.isSetTables_missing_stats() &&
        !request.query_ctxt.getTables_missing_stats().isEmpty()) {
      List<String> tableNames = Lists.newArrayList();
      for (TTableName tableName: request.query_ctxt.getTables_missing_stats()) {
        tableNames.add(tableName.db_name + "." + tableName.table_name);
      }
      str.append("WARNING: The following tables are missing relevant table " +
          "and/or column statistics.\n" + Joiner.on(", ").join(tableNames) + "\n");
      hasHeader = true;
    }
    if (hasHeader) str.append("\n");

    if (explainLevel.ordinal() < TExplainLevel.VERBOSE.ordinal()) {
      // Print the non-fragmented parallel plan.
      str.append(fragments.get(0).getExplainString(explainLevel));
    } else {
      // Print the fragmented parallel plan.
      for (int i = 0; i < fragments.size(); ++i) {
        PlanFragment fragment = fragments.get(i);
        str.append(fragment.getExplainString(explainLevel));
        if (explainLevel == TExplainLevel.VERBOSE && i + 1 != fragments.size()) {
          str.append("\n");
        }
      }
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
      throws InternalException, NotImplementedException, AuthorizationException {
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
          perNodeMemLimit, fragments, analyzer);
    } else if (root instanceof CrossJoinNode) {
      Preconditions.checkState(childFragments.size() == 2);
      result = createCrossJoinFragment(
          (CrossJoinNode) root, childFragments.get(1), childFragments.get(0),
          perNodeMemLimit, fragments, analyzer);
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
   * Makes a cost-based decision on whether to repartition the output of 'inputFragment'
   * before feeding its data into the table sink of the given 'insertStmt'. Considers
   * user-supplied plan hints to determine whether to repartition or not.
   * Returns a plan fragment that partitions the output of 'inputFragment' on the
   * partition exprs of 'insertStmt', unless the expected number of partitions is less
   * than the number of nodes on which inputFragment runs.
   * If it ends up creating a new fragment, appends that to 'fragments'.
   */
  private PlanFragment createInsertFragment(
      PlanFragment inputFragment, InsertStmt insertStmt, Analyzer analyzer,
      ArrayList<PlanFragment> fragments)
      throws AuthorizationException, InternalException {
    List<Expr> partitionExprs = insertStmt.getPartitionKeyExprs();
    Boolean partitionHint = insertStmt.isRepartition();
    if (partitionExprs.isEmpty()) return inputFragment;
    if (partitionHint != null && !partitionHint) return inputFragment;

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
    if (partitionHint == null && numPartitions > 0 &&
        numPartitions <= inputFragment.getNumNodes()) {
      return inputFragment;
    }

    Preconditions.checkState(partitionHint == null || partitionHint);
    ExchangeNode exchNode = new ExchangeNode(nodeIdGenerator_.getNextId());
    exchNode.addChild(inputFragment.getPlanRoot(), false);
    exchNode.init(analyzer);
    Preconditions.checkState(exchNode.hasValidStats());
    DataPartition partition =
        new DataPartition(TPartitionType.HASH_PARTITIONED, nonConstPartitionExprs);
    PlanFragment fragment =
        new PlanFragment(fragmentIdGenerator_.getNextId(), exchNode, partition);
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
  private PlanFragment createMergeFragment(
      PlanFragment inputFragment, Analyzer analyzer)
      throws InternalException, AuthorizationException {
    Preconditions.checkState(inputFragment.isPartitioned());
    ExchangeNode mergePlan = new ExchangeNode(nodeIdGenerator_.getNextId());
    mergePlan.addChild(inputFragment.getPlanRoot(), false);
    mergePlan.init(analyzer);
    Preconditions.checkState(mergePlan.hasValidStats());
    PlanFragment fragment = new PlanFragment(fragmentIdGenerator_.getNextId(), mergePlan,
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
    return new PlanFragment(
        fragmentIdGenerator_.getNextId(), node, DataPartition.RANDOM);
  }

  /**
   * Modifies the leftChildFragment to execute a cross join. The right child input is
   * provided by an ExchangeNode, which is the destination of the rightChildFragment's
   * output.
   */
  private PlanFragment createCrossJoinFragment(CrossJoinNode node,
      PlanFragment rightChildFragment, PlanFragment leftChildFragment,
      long perNodeMemLimit, ArrayList<PlanFragment> fragments,
      Analyzer analyzer) throws AuthorizationException, InternalException {
    // The rhs tree is going to send data through an exchange node which effectively
    // compacts the data. No reason to do it again at the rhs root node.
    rightChildFragment.getPlanRoot().setCompactData(false);
    node.setChild(0, leftChildFragment.getPlanRoot());
    connectChildFragment(analyzer, node, 1, leftChildFragment, rightChildFragment);
    leftChildFragment.setPlanRoot(node);
    return leftChildFragment;
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
      ArrayList<PlanFragment> fragments, Analyzer analyzer)
      throws InternalException, AuthorizationException {
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
    PlanNode lhsTree = leftChildFragment.getPlanRoot();
    long partitionCost = Long.MAX_VALUE;
    List<Expr> lhsJoinExprs = Lists.newArrayList();
    List<Expr> rhsJoinExprs = Lists.newArrayList();
    for (Pair<Expr, Expr> pair: node.getEqJoinConjuncts()) {
      // no remapping necessary
      lhsJoinExprs.add(pair.first.clone(null));
      rhsJoinExprs.add(pair.second.clone(null));
    }
    boolean lhsHasCompatPartition = false;
    boolean rhsHasCompatPartition = false;
    if (lhsTree.getCardinality() != -1 && rhsTree.getCardinality() != -1) {
      lhsHasCompatPartition = analyzer.isEquivSlots(lhsJoinExprs,
          leftChildFragment.getDataPartition().getPartitionExprs());
      rhsHasCompatPartition = analyzer.isEquivSlots(rhsJoinExprs,
          rightChildFragment.getDataPartition().getPartitionExprs());

      double lhsCost = (lhsHasCompatPartition) ? 0.0 :
        Math.round((double) lhsTree.getCardinality() * lhsTree.getAvgRowSize());
      double rhsCost = (rhsHasCompatPartition) ? 0.0 :
        Math.round((double) rhsTree.getCardinality() * rhsTree.getAvgRowSize());
      partitionCost = Math.round(lhsCost + rhsCost);
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
            || (!node.getInnerRef().isPartitionedJoin()
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
      connectChildFragment(analyzer, node, 1, leftChildFragment, rightChildFragment);
      leftChildFragment.setPlanRoot(node);
      return leftChildFragment;
    } else {
      node.setDistributionMode(HashJoinNode.DistributionMode.PARTITIONED);

      // The lhs and rhs input fragments are already partitioned on the join exprs.
      // Combine the lhs/rhs input fragments into leftChildFragment by placing the join
      // node into leftChildFragment and setting its lhs/rhs children to the plan root of
      // the lhs/rhs child fragment, respectively. No new child fragments or exchanges
      // are created, and the rhs fragment is removed.
      if (lhsHasCompatPartition && rhsHasCompatPartition) {
        node.setChild(0, leftChildFragment.getPlanRoot());
        node.setChild(1, rightChildFragment.getPlanRoot());
        // Redirect fragments sending to rightFragment to leftFragment.
        for (PlanFragment fragment: fragments) {
          if (fragment.getDestFragment() == rightChildFragment) {
            fragment.setDestination(fragment.getDestNode());
          }
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
      DataPartition rhsJoinPartition = new DataPartition(TPartitionType.HASH_PARTITIONED,
          Expr.cloneList(rhsJoinExprs, null));
      if (lhsHasCompatPartition) {
        node.setChild(0, leftChildFragment.getPlanRoot());
        connectChildFragment(analyzer, node, 1, leftChildFragment, rightChildFragment);
        rightChildFragment.setOutputPartition(rhsJoinPartition);
        leftChildFragment.setPlanRoot(node);
        return leftChildFragment;
      }

      // Same as above but with rhs and lhs reversed.
      DataPartition lhsJoinPartition = new DataPartition(TPartitionType.HASH_PARTITIONED,
          Expr.cloneList(lhsJoinExprs, null));
      if (rhsHasCompatPartition) {
        node.setChild(1, rightChildFragment.getPlanRoot());
        connectChildFragment(analyzer, node, 0, rightChildFragment, leftChildFragment);
        leftChildFragment.setOutputPartition(lhsJoinPartition);
        rightChildFragment.setPlanRoot(node);
        return rightChildFragment;
      }

      // Neither lhs nor rhs are already partitioned on the join exprs.
      // Create a new parent fragment containing a HashJoin node with two
      // ExchangeNodes as inputs; the latter are the destinations of the
      // left- and rightChildFragments, which now partition their output
      // on their respective join exprs.
      // The new fragment is hash-partitioned on the lhs input join exprs.
      ExchangeNode lhsExchange = new ExchangeNode(nodeIdGenerator_.getNextId());
      lhsExchange.addChild(leftChildFragment.getPlanRoot(), false);
      lhsExchange.computeStats(null);
      node.setChild(0, lhsExchange);
      ExchangeNode rhsExchange = new ExchangeNode(nodeIdGenerator_.getNextId());
      rhsExchange.addChild(rightChildFragment.getPlanRoot(), false);
      rhsExchange.computeStats(null);
      node.setChild(1, rhsExchange);

      // Connect the child fragments in a new fragment, and set the data partition
      // of the new fragment and its child fragments.
      PlanFragment joinFragment =
          new PlanFragment(fragmentIdGenerator_.getNextId(), node, lhsJoinPartition);
      leftChildFragment.setDestination(lhsExchange);
      leftChildFragment.setOutputPartition(lhsJoinPartition);
      rightChildFragment.setDestination(rhsExchange);
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
   * fragment's output is mapped onto the MergeNode's result tuple id_.
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
      Analyzer analyzer) throws InternalException, AuthorizationException {
    Preconditions.checkState(mergeNode.getChildren().size() == childFragments.size());

    // A MergeNode could have no children or constant selects if all of its operands
    // were dropped because of constant predicates that evaluated to false.
    if (mergeNode.getChildren().isEmpty()) {
      return new PlanFragment(
          fragmentIdGenerator_.getNextId(), mergeNode, DataPartition.UNPARTITIONED);
    }

    // create an ExchangeNode to perform the merge operation of mergeNode;
    // the ExchangeNode retains the generic PlanNode parameters of mergeNode
    ExchangeNode exchNode = new ExchangeNode(nodeIdGenerator_.getNextId());
    PlanFragment parentFragment = new PlanFragment(fragmentIdGenerator_.getNextId(),
        exchNode, DataPartition.UNPARTITIONED);
    exchNode.setFragment(parentFragment);

    // we don't expect to be parallelizing a MergeNode that was inserted solely
    // to evaluate conjuncts_ (ie, that doesn't explicitly materialize its output)
    Preconditions.checkState(mergeNode.getTupleIds().size() == 1);

    for (int i = 0; i < childFragments.size(); ++i) {
      PlanFragment childFragment = childFragments.get(i);
      MergeNode childMergeNode =
          new MergeNode(
              nodeIdGenerator_.getNextId(), mergeNode, i, childFragment.getPlanRoot());
      childMergeNode.init(analyzer);
      childFragment.setPlanRoot(childMergeNode);
      childFragment.setDestination(exchNode);
      exchNode.addChild(childMergeNode, true);
    }

    // Add an unpartitioned child fragment with a MergeNode for the constant exprs.
    if (!mergeNode.getConstExprLists().isEmpty()) {
      MergeNode childMergeNode = MergeNode.createConstIntermediateMerge(
          nodeIdGenerator_.getNextId(), mergeNode);
      childMergeNode.init(analyzer);
      // Clear original constant exprs to make sure nobody else picks them up.
      mergeNode.getConstExprLists().clear();
      PlanFragment childFragment = new PlanFragment(fragmentIdGenerator_.getNextId(),
          childMergeNode, DataPartition.UNPARTITIONED);
      childFragment.setPlanRoot(childMergeNode);
      childFragment.setDestination(exchNode);
      childFragments.add(childFragment);
      fragments.add(childFragment);
      exchNode.addChild(childMergeNode, true);
    }
    exchNode.init(analyzer);

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
  private void connectChildFragment(Analyzer analyzer, PlanNode node, int childIdx,
      PlanFragment parentFragment, PlanFragment childFragment)
      throws InternalException, AuthorizationException {
    ExchangeNode exchangeNode = new ExchangeNode(nodeIdGenerator_.getNextId());
    exchangeNode.addChild(childFragment.getPlanRoot(), false);
    exchangeNode.init(analyzer);
    node.setChild(childIdx, exchangeNode);
    childFragment.setDestination(exchangeNode);
  }

  /**
   * Returns a fragment containing a single ExchangeNode that consumes the output
   * of all childFragments. Sets the destination of childFragments to the new parent
   * and the output partition of the childFragments to that of the new parent.
   * Creates the ExchangeNode inside parentFragment if one was provided (not null),
   * otherwise creates a new parent fragment.
   * TODO: the output partition of a child isn't necessarily the same as the data
   * partition of the receiving parent (if there is more materialization happening
   * in the parent, such as during distinct aggregation). Do we care about the data
   * partition of the parent being applicable to the *output* of the parent (it's
   * correct for the input).
   */
  private PlanFragment createParentFragment(
      Analyzer analyzer, List<PlanFragment> childFragments,
      DataPartition parentPartition, PlanFragment parentFragment)
      throws InternalException, AuthorizationException {
    ExchangeNode exchangeNode = new ExchangeNode(nodeIdGenerator_.getNextId());
    for (PlanFragment childFragment: childFragments) {
      exchangeNode.addChild(childFragment.getPlanRoot(), false);
      childFragment.setDestination(exchangeNode);
      childFragment.setOutputPartition(parentPartition);
    }
    if (parentFragment == null) {
      parentFragment = new PlanFragment(fragmentIdGenerator_.getNextId(),
          exchangeNode, parentPartition);
    } else {
      parentFragment.setPlanRoot(exchangeNode);
      parentFragment.setDataPartition(parentPartition);
    }
    exchangeNode.init(analyzer);
    return parentFragment;
  }

  /**
   * Populates childFragments. If fragment is unpartitioned and its root is an
   * ExchangeNode without a limit, then childFragments contains all the child
   * fragments of the exchange and this function returns true. Otherwise
   * childFragments simply contains the given fragment and this function
   * returns false.
   * Used for distributing operations (e.g., agg, topn) over union branches.
   */
  private boolean getUnionInputs(
      PlanFragment fragment, List<PlanFragment> childFragments) {
    PlanNode root = fragment.getPlanRoot();
    if (fragment.isPartitioned() || !(root instanceof ExchangeNode) || root.hasLimit()) {
      childFragments.add(fragment);
      return false;
    }
    ExchangeNode childPlanRoot = (ExchangeNode) fragment.getPlanRoot();
    for (PlanNode p: childPlanRoot.getChildren()) {
      childFragments.add(p.getFragment());
    }
    return true;
  }

  /**
   * Makes 'root' the new plan root for all fragments. Uses the original root
   * for the first fragment and clones root for the remaining fragments.
   */
  private void addPlanRoots(List<PlanFragment> fragments, PlanNode root,
      Analyzer analyzer) {
    for (int i = 0; i < fragments.size(); ++i) {
      PlanNode newRoot = root;
      if (i != 0) newRoot = root.clone(nodeIdGenerator_.getNextId());
      newRoot.getChildren().clear();
      newRoot.addChild(fragments.get(i).getPlanRoot());
      newRoot.unsetLimit();
      newRoot.computeStats(analyzer);
      fragments.get(i).addPlanRoot(newRoot);
    }
  }

  /**
   * Returns a fragment that materializes the aggregation result of 'node'.
   * If the child fragment is partitioned, the result fragment will be partitioned on
   * the grouping exprs of 'node'.
   * To handle distributed aggregation over unions, we extract the children of
   * childFragment if it contains an unpartitioned ExchangeNode, and use those as the
   * child fragments on which to perform local aggregation.
   * If 'node' is phase 1 of a 2-phase DISTINCT aggregation, this will simply
   * add 'node' to the child fragment(s) and return the childFragment; the new
   * fragment will be created by the subsequent call of createAggregationFragment()
   * for the phase 2 AggregationNode.
   */
  private PlanFragment createAggregationFragment(AggregationNode node,
      PlanFragment childFragment, ArrayList<PlanFragment> fragments, Analyzer analyzer)
      throws InternalException, AuthorizationException {
    List<PlanFragment> childFragments = Lists.newArrayList();
    PlanFragment parentAggFragment = null;
    if (getUnionInputs(childFragment, childFragments)) {
      // Use childFragment for merge aggregation if we unwrapped an exchange.
      parentAggFragment = childFragment;
    }
    if (parentAggFragment == null && !childFragment.isPartitioned()) {
        // nothing to distribute; do full aggregation directly within childFragment
        childFragment.addPlanRoot(node);
        return childFragment;
    }

    if (node.getAggInfo().isDistinctAgg()) {
      // 'node' is phase 1 of a DISTINCT aggregation; the actual agg fragment
      // will get created in the next createAggregationFragment() call
      // for the parent AggregationNode
      addPlanRoots(childFragments, node, analyzer);
      return childFragment;
    }

    ArrayList<Expr> groupingExprs = node.getAggInfo().getGroupingExprs();
    boolean hasGrouping = !groupingExprs.isEmpty();
    // 2nd phase of DISTINCT aggregation
    boolean isDistinct =
        node.getChild(0) instanceof AggregationNode
          && ((AggregationNode)(node.getChild(0))).getAggInfo().isDistinctAgg();

    if (!isDistinct) {
      // if there is a limit, we need to transfer it from the pre-aggregation
      // node to the merge aggregation node in the parent; remember the limit here
      // because addPlanRoots() will unset the limit of the pre-aggregation
      long limit = node.getLimit();
      // the original aggregation goes into all child fragments,
      // merge aggregation into a single parent fragment
      addPlanRoots(childFragments, node, analyzer);
      for (PlanFragment f: childFragments) {
        Preconditions.checkState(f.getPlanRoot() instanceof AggregationNode);
        AggregationNode childPlanFoot = (AggregationNode) f.getPlanRoot();
        childPlanFoot.unsetNeedsFinalize();
      }

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

      // place a merge aggregation step in a new fragment, or an existing
      // parentAggFragment if we are aggregating over a union
      PlanFragment mergeFragment = createParentFragment(analyzer, childFragments,
          parentPartition, parentAggFragment);
      AggregationNode mergeAggNode =
          new AggregationNode(
            nodeIdGenerator_.getNextId(), mergeFragment.getPlanRoot(),
            node.getAggInfo().getMergeAggInfo());
      mergeAggNode.init(analyzer);
      mergeAggNode.setLimit(limit);

      // HAVING predicates can only be evaluated after the merge agg step
      node.transferConjuncts(mergeAggNode);
      // Recompute stats after transferring the conjuncts_ (order is important).
      node.computeStats(analyzer);
      mergeFragment.getPlanRoot().computeStats(analyzer);
      mergeAggNode.computeStats(analyzer);
      // Set new plan root after updating stats.
      mergeFragment.addPlanRoot(mergeAggNode);

      return mergeFragment;
    }

    Preconditions.checkState(isDistinct);
    // The first-phase aggregation node is already in the child fragments.
    Preconditions.checkState(node.getChild(0) == childFragments.get(0).getPlanRoot());

    DataPartition mergePartition = null;
    if (hasGrouping) {
      // We need to do
      // - child fragment:
      //   * phase-1 aggregation
      // - merge fragment, hash-partitioned on grouping exprs:
      //   * merge agg of phase 1
      //   * phase 2 agg
      // The output partition exprs of the child are the (input) grouping exprs
      // of the parent.
      List<Expr> partitionExprs = Expr.cloneList(groupingExprs, null);
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

    // place a merge aggregation step for the 1st phase in parentAggFragment or a new
    // fragment, depending on whether we unwrapped an exchange
    PlanFragment mergeFragment = createParentFragment(analyzer, childFragments,
        mergePartition, parentAggFragment);
    AggregateInfo mergeAggInfo =
        ((AggregationNode)(node.getChild(0))).getAggInfo().getMergeAggInfo();
    AggregationNode mergeAggNode =
        new AggregationNode(
            nodeIdGenerator_.getNextId(), node.getChild(0), mergeAggInfo);
    mergeAggNode.init(analyzer);
    mergeAggNode.unsetNeedsFinalize();
    mergeFragment.addPlanRoot(mergeAggNode);
    // the 2nd-phase aggregation consumes the output of the merge agg;
    // if there is a limit, it had already been placed with the 2nd aggregation
    // step (which is where it should be)
    mergeFragment.addPlanRoot(node);

    if (!hasGrouping) {
      // place the merge aggregation of the 2nd phase in an unpartitioned fragment;
      // add preceding merge fragment at end; remove mergeFragment because it may have
      // already been added if we are aggregating over a union
      fragments.remove(mergeFragment);
      fragments.add(mergeFragment);

      node.unsetNeedsFinalize();
      mergeFragment = createParentFragment(analyzer,
          Lists.newArrayList(mergeFragment), DataPartition.UNPARTITIONED, null);
      mergeAggInfo = node.getAggInfo().getMergeAggInfo();
      mergeAggNode =
          new AggregationNode(
            nodeIdGenerator_.getNextId(), node.getChild(0), mergeAggInfo);
      mergeAggNode.init(analyzer);
      // Transfer having predicates. If hasGrouping == true, the predicates should
      // instead be evaluated by the 2nd phase agg (the predicates are already there).
      node.transferConjuncts(mergeAggNode);
      mergeFragment.addPlanRoot(mergeAggNode);
    }
    return mergeFragment;
  }

  /**
   * Returns a fragment that outputs the result of 'node'.
   * - adds the top-n computation to the child fragment(s)
   * - if the child fragment is partitioned creates a new unpartitioned fragment that
   *   merges the output of the child and does another top-n computation
   * - for a top-n over a union, the child fragment is unpartitioned and only contains an
   *   ExchangeNoode; adds local top-n nodes to the children of the exchange, and then
   *   merges the output in childFragment with another top-n
   */
  private PlanFragment createTopnFragment(SortNode node,
      PlanFragment childFragment, ArrayList<PlanFragment> fragments, Analyzer analyzer)
      throws InternalException, AuthorizationException {
    List<PlanFragment> childFragments = Lists.newArrayList();
    PlanFragment mergeFragment = null;
    if (getUnionInputs(childFragment, childFragments)) {
      mergeFragment = childFragment;
    }
    if (mergeFragment == null && !childFragment.isPartitioned()) {
      childFragment.addPlanRoot(node);
      return childFragment;
    }
    // Remember original offset and limit.
    long limit = node.getLimit();
    long offset = node.getOffset();
    addPlanRoots(childFragments, node, analyzer);

    // we're doing top-n in a single unpartitioned new fragment
    // that merges the output of childFragment
    mergeFragment = createParentFragment(analyzer, childFragments,
        DataPartition.UNPARTITIONED, mergeFragment);
    // insert sort node that repeats the childrens' sort
    PlanNode exchNode = mergeFragment.getPlanRoot();
    SortNode mergeNode = (SortNode) node.clone(nodeIdGenerator_.getNextId());
    mergeNode.addChild(exchNode);
    // the merging exchange node must not apply the limit (that's done by the
    // merging top-n)
    exchNode.unsetLimit();

    // If there is an offset_, it must be applied at the top-n. Child nodes do not apply
    // the offset_, and instead must keep at least (limit+offset_) rows so that the top-n
    // node does not miss any rows that should be in the top-n.
    for (PlanFragment f: childFragments) {
      SortNode childSortNode = (SortNode) f.getPlanRoot();
      childSortNode.unsetLimit();
      childSortNode.setLimit(limit + ((offset > 0) ? offset : 0));
      childSortNode.setOffset(0);
      childSortNode.init(analyzer);
    }
    // Set limit and offset in merge node.
    mergeNode.setLimit(limit);
    mergeNode.setOffset(offset);
    mergeNode.init(analyzer);
    Preconditions.checkState(mergeNode.hasValidStats());
    mergeFragment.setPlanRoot(mergeNode);
    exchNode.computeStats(analyzer);

    return mergeFragment;
  }

  /**
   * Create plan tree for single-node execution.
   */
  private PlanNode createQueryPlan(
      QueryStmt stmt, Analyzer analyzer, long defaultOrderByLimit)
      throws ImpalaException {
    if (stmt instanceof SelectStmt) {
      return createSelectPlan((SelectStmt) stmt, analyzer, defaultOrderByLimit);
    } else {
      Preconditions.checkState(stmt instanceof UnionStmt);
      return createUnionPlan(analyzer, (UnionStmt) stmt);
    }
  }

  /**
   * If there are unassigned conjuncts_ that are bound by tupleIds_, returns a SelectNode
   * on top of root that evaluate those conjuncts_; otherwise returns root unchanged.
   */
  private PlanNode addUnassignedConjuncts(
      Analyzer analyzer, List<TupleId> tupleIds, PlanNode root)
      throws InternalException, AuthorizationException {
    Preconditions.checkNotNull(root);
    // TODO: standardize on logical tuple ids?
    List<Expr> conjuncts = analyzer.getUnassignedConjuncts(root);
    //List<Expr> conjuncts_ = analyzer.getUnassignedConjuncts(tupleIds_);
    if (conjuncts.isEmpty()) return root;
    // evaluate conjuncts_ in SelectNode
    SelectNode selectNode = new SelectNode(nodeIdGenerator_.getNextId(), root);
    // init() picks up the unassigned conjuncts_
    selectNode.init(analyzer);
    Preconditions.checkState(selectNode.hasValidStats());
    return selectNode;
  }

  /**
   * Return the cheapest plan that materializes the joins of all TblRefs in
   * refPlans; for this plan:
   * - the plan is executable, ie, all non-cross joins have equi-join predicates
   * - the leftmost scan is over the largest of the inputs for which we can still
   *   construct an executable plan
   * - all rhs's are in decreasing order of selectiveness (percentage of rows they
   *   eliminate)
   * Returns null if we can't create an executable plan.
   */
  private PlanNode createCheapestJoinPlan(
      Analyzer analyzer, List<Pair<TableRef, PlanNode>> refPlans)
      throws ImpalaException {
    LOG.trace("createCheapestJoinPlan");

    // collect eligible candidates for the leftmost input; list contains
    // (plan, materialized size)
    ArrayList<Pair<TableRef, Long>> candidates = Lists.newArrayList();
    for (Pair<TableRef, PlanNode> entry: refPlans) {
      TableRef ref = entry.first;
      JoinOperator joinOp = ref.getJoinOp();
      if (joinOp.isOuterJoin() || joinOp.isSemiJoin() || joinOp.isCrossJoin()) {
        // this cannot appear as the leftmost input
        // TODO: make this less restrictive by considering plans with inverted Outer
        // Join directions
        continue;
      }

      PlanNode plan = entry.second;
      if (plan.getCardinality() == -1) {
        // use 0 for the size to avoid it becoming the leftmost input
        // TODO: Consider raw size of scanned partitions in the absence of stats.
        candidates.add(new Pair(ref, new Long(0)));
        LOG.trace("candidate " + ref.getAlias() + ": 0");
        continue;
      }
      Preconditions.checkNotNull(ref.getDesc());
      long materializedSize =
          (long) Math.ceil(plan.getAvgRowSize() * (double) plan.getCardinality());
      candidates.add(new Pair(ref, new Long(materializedSize)));
      LOG.trace("candidate " + ref.getAlias() + Long.toString(materializedSize));
    }
    if (candidates.isEmpty()) return null;

    // order candidates by descending materialized size; we want to minimize the memory
    // consumption of the materialized hash tables required for the join sequence
    Collections.sort(candidates,
        new Comparator<Pair<TableRef, Long>>() {
          public int compare(Pair<TableRef, Long> a, Pair<TableRef, Long> b) {
            long diff = b.second - a.second;
            return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
          }
        });
    for (Pair<TableRef, Long> candidate: candidates) {
      PlanNode result = createJoinPlan(analyzer, candidate.first, refPlans);
      if (result != null) return result;
    }
    return null;
  }

  /**
   * Returns a plan with leftmostRef's plan as its leftmost input; the joins
   * are in decreasing order of selectiveness (percentage of rows they eliminate).
   */
  private PlanNode createJoinPlan(
      Analyzer analyzer, TableRef leftmostRef, List<Pair<TableRef, PlanNode>> refPlans)
      throws ImpalaException {
    LOG.trace("createJoinPlan: " + leftmostRef.getAlias());
    // the refs that have yet to be joined
    List<Pair<TableRef, PlanNode>> remainingRefs = Lists.newArrayList();
    PlanNode root = null;  // root of accumulated join plan
    for (Pair<TableRef, PlanNode> entry: refPlans) {
      if (entry.first == leftmostRef) {
        root = entry.second;
      } else {
        remainingRefs.add(entry);
      }
    }
    Preconditions.checkNotNull(root);
    // refs that have been joined. The union of joinedRefs and the refs in remainingRefs
    // are the set of all table refs.
    Set<TableRef> joinedRefs = Sets.newHashSet();
    joinedRefs.add(leftmostRef);

    long numOps = 0;
    int i = 0;
    while (!remainingRefs.isEmpty()) {
      // we minimize the resulting cardinality at each step in the join chain,
      // which minimizes the total number of hash table lookups
      PlanNode newRoot = null;
      Pair<TableRef, PlanNode> minEntry = null;
      for (Pair<TableRef, PlanNode> entry: remainingRefs) {
        TableRef ref = entry.first;
        LOG.trace(Integer.toString(i) + " considering ref " + ref.getAlias());

        // Determine whether we can or must consider this join at this point in the plan.
        // Place outer/semi joins at a fixed position in the plan tree (IMPALA-860), s.t.
        // all the tables appearing to the left/right of an outer/semi join in the
        // original query still remain to the left/right after join ordering. This
        // prevents join ordering across outer/semi joins which is generally incorrect.
        // The checks below relies on remainingRefs being in the order as they originally
        // appeared in the query.
        boolean fixedJoinPos = false;
        JoinOperator joinOp = ref.getJoinOp();
        if (joinOp.isOuterJoin() || joinOp.isSemiJoin()) {
          List<TupleId> currentTids = Lists.newArrayList(root.getTblRefIds());
          currentTids.add(ref.getId());
          // TODO: make this less restrictive by considering plans with inverted Outer
          // Join directions
          // Place outer/semi joins at a fixed position in the plan tree. We know that
          // the join resulting from 'ref' must become the new root if the current root
          // materializes exactly those tuple ids corresponding to TableRefs appearing
          // to the left of 'ref' in the original query.
          List<TupleId> tableRefTupleIds = ref.getAllTupleIds();
          if (currentTids.containsAll(tableRefTupleIds)
              && tableRefTupleIds.containsAll(currentTids)) {
            fixedJoinPos = true;
          } else {
            // Do not consider the remaining table refs to prevent incorrect re-ordering
            // of tables across outer/semi joins.
            break;
          }
        } else if (ref.getJoinOp().isCrossJoin()) {
          if (!joinedRefs.contains(ref.getLeftTblRef())) continue;
        }

        PlanNode rhsPlan = entry.second;
        analyzer.setAssignedConjuncts(root.getAssignedConjuncts());
        PlanNode candidate = createJoinNode(analyzer, root, rhsPlan, ref, false);
        if (candidate == null) continue;
        LOG.trace("cardinality=" + Long.toString(candidate.getCardinality()));

        // Use 'candidate' as the new root; don't consider any other table refs at this
        // position in the plan.
        if (fixedJoinPos) {
          newRoot = candidate;
          minEntry = entry;
          break;
        }

        if (newRoot == null || candidate.getCardinality() < newRoot.getCardinality()) {
          newRoot = candidate;
          minEntry = entry;
        }
      }
      if (newRoot == null) return null;

      // we need to insert every rhs row into the hash table and then look up
      // every lhs row
      long lhsCardinality = root.getCardinality();
      long rhsCardinality = minEntry.second.getCardinality();
      numOps += lhsCardinality + rhsCardinality;
      LOG.debug(Integer.toString(i) + " chose " + minEntry.first.getAlias()
          + " #lhs=" + Long.toString(lhsCardinality)
          + " #rhs=" + Long.toString(rhsCardinality)
          + " #ops=" + Long.toString(numOps));
      remainingRefs.remove(minEntry);
      joinedRefs.add(minEntry.first);
      root = newRoot;
      // assign id_ after running through the possible choices in order to end up
      // with a dense sequence of node ids
      root.setId(nodeIdGenerator_.getNextId());
      analyzer.setAssignedConjuncts(root.getAssignedConjuncts());
      // build side copies data to a compact representation in the tuple buffer.
      root.getChildren().get(1).setCompactData(true);
      ++i;
    }

    return root;
  }


  /**
   * Return a plan with joins in the order of refPlans (= FROM clause order).
   */
  private PlanNode createFromClauseJoinPlan(
      Analyzer analyzer, List<Pair<TableRef, PlanNode>> refPlans)
      throws ImpalaException {
    // create left-deep sequence of binary hash joins; assign node ids as we go along
    Preconditions.checkState(!refPlans.isEmpty());
    PlanNode root = refPlans.get(0).second;
    for (int i = 1; i < refPlans.size(); ++i) {
      TableRef innerRef = refPlans.get(i).first;
      PlanNode innerPlan = refPlans.get(i).second;
      root = createJoinNode(analyzer, root, innerPlan, innerRef, true);
      root.setId(nodeIdGenerator_.getNextId());
      // build side copies data to a compact representation in the tuple buffer.
      root.getChildren().get(1).setCompactData(true);
    }
    return root;
  }

  /**
   * Create tree of PlanNodes that implements the Select/Project/Join/Group by/Having
   * of the selectStmt query block.
   * @throws NotImplementedException if selectStmt contains Order By clause w/o Limit
   *   and the query options don't contain a default limit
   */
  private PlanNode createSelectPlan(
      SelectStmt selectStmt, Analyzer analyzer, long defaultOrderByLimit)
      throws ImpalaException {
    // no from clause -> materialize the select's exprs with a MergeNode
    if (selectStmt.getTableRefs().isEmpty()) {
      return createConstantSelectPlan(selectStmt, analyzer);
    }

    // collect output tuples of subtrees
    ArrayList<TupleId> rowTuples = Lists.newArrayList();
    for (TableRef tblRef: selectStmt.getTableRefs()) {
      rowTuples.addAll(tblRef.getMaterializedTupleIds());
    }

    // Slot materialization:
    // We need to mark all slots as materialized that are needed during the execution
    // of selectStmt, and we need to do that prior to creating plans for the TableRefs
    // (because createTableRefNode() might end up calling computeMemLayout() on one or more
    // TupleDescriptors, at which point all referenced slots need to be marked).
    //
    // For non-join predicates, slots are marked as follows:
    // - for base table scan predicates, this is done directly by ScanNode.init(), which
    //   can do a better job because it doesn't need to materialize slots that are only
    //   referenced for partition pruning, for instance
    // - for inline views, non-join predicates are pushed down, at which point the process
    //   repeats itself.
    selectStmt.materializeRequiredSlots(analyzer);

    // create plans for our table refs; use a list here instead of a map to
    // maintain a deterministic order of traversing the TableRefs during join
    // plan generation (helps with tests)
    List<Pair<TableRef, PlanNode>> refPlans = Lists.newArrayList();
    for (TableRef ref: selectStmt.getTableRefs()) {
      PlanNode plan = createTableRefNode(analyzer, ref);
      Preconditions.checkState(plan != null);
      refPlans.add(new Pair(ref, plan));
    }
    // save state of conjunct assignment; needed for join plan generation
    for (Pair<TableRef, PlanNode> entry: refPlans) {
      entry.second.setAssignedConjuncts(analyzer.getAssignedConjuncts());
    }

    PlanNode root = null;
    if (!selectStmt.getSelectList().isStraightJoin()) {
      root = createCheapestJoinPlan(analyzer, refPlans);
    }
    if (selectStmt.getSelectList().isStraightJoin() || root == null) {
      // we didn't have enough stats to do a cost-based join plan, or the STRAIGHT_JOIN
      // keyword was in the select list: use the FROM clause order instead
      root = createFromClauseJoinPlan(analyzer, refPlans);
      Preconditions.checkNotNull(root);
    }

    if (selectStmt.getSortInfo() != null
        && selectStmt.getLimit() == -1 && defaultOrderByLimit == -1) {
      // TODO: only use topN if the memory footprint is expected to be low;
      // how to account for strings?
      throw new NotImplementedException(
          "ORDER BY without LIMIT currently not supported");
    }

    if (root != null) {
      // add unassigned conjuncts_ before aggregation
      // (scenario: agg input comes from an inline view which wasn't able to
      // evaluate all Where clause conjuncts_ from this scope)
      root = addUnassignedConjuncts(analyzer, root.getTupleIds(), root);
    }

    // add aggregation, if required
    AggregateInfo aggInfo = selectStmt.getAggInfo();
    if (aggInfo != null) {
      root = new AggregationNode(nodeIdGenerator_.getNextId(), root, aggInfo);
      root.init(analyzer);
      Preconditions.checkState(root.hasValidStats());
      // if we're computing DISTINCT agg fns, the analyzer already created the
      // 2nd phase agginfo
      if (aggInfo.isDistinctAgg()) {
        ((AggregationNode)root).unsetNeedsFinalize();
        root = new AggregationNode(
            nodeIdGenerator_.getNextId(), root,
            aggInfo.getSecondPhaseDistinctAggInfo());
        root.init(analyzer);
        Preconditions.checkState(root.hasValidStats());
      }
      // add Having clause
      root.assignConjuncts(analyzer);
    }

    root = addOrderByLimit(
        analyzer, root, selectStmt.getSortInfo(), selectStmt.getLimit(),
        defaultOrderByLimit, selectStmt.getOffset());

    // All the conjuncts_ should be assigned at this point.
    // TODO: Re-enable this check here and/or elswehere.
    //Preconditions.checkState(!analyzer.hasUnassignedConjuncts());

    return root;
  }

  /**
   * Returns a SortNode with 'root' as its input if sortInfo != null, otherwise
   * just sets the limit.
   */
  private PlanNode addOrderByLimit(Analyzer analyzer, PlanNode root,
      SortInfo sortInfo, long limit, long defaultOrderByLimit, long offset)
      throws InternalException, AuthorizationException {
    if (sortInfo != null) {
      Preconditions.checkState(limit != -1 || defaultOrderByLimit != -1);
      boolean isDefaultLimit = (limit == -1);
      root = new SortNode(nodeIdGenerator_.getNextId(), root, sortInfo, true,
          isDefaultLimit, offset);
      Preconditions.checkState(root.hasValidStats());
      root.setLimit(limit != -1 ? limit : defaultOrderByLimit);
      root.init(analyzer);
    } else {
      root.setLimit(limit);
      root.computeStats(analyzer);
    }
    return root;
  }

  /**
  * Returns a MergeNode that materializes the exprs of the constant selectStmt.
  * Replaces the resultExprs of the selectStmt with SlotRefs into the materialized tuple.
  */
  private PlanNode createConstantSelectPlan(SelectStmt selectStmt, Analyzer analyzer)
      throws InternalException {
    Preconditions.checkState(selectStmt.getTableRefs().isEmpty());
    ArrayList<Expr> resultExprs = selectStmt.getBaseTblResultExprs();
    ArrayList<String> colLabels = selectStmt.getColLabels();
    // Create tuple descriptor for materialized tuple.
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor();
    tupleDesc.setIsMaterialized(true);
    MergeNode mergeNode = new MergeNode(nodeIdGenerator_.getNextId(), tupleDesc.getId());
    // Analysis guarantees that selects without a FROM clause only have constant exprs.
    mergeNode.addConstExprList(Lists.newArrayList(resultExprs));

    // Replace the select stmt's resultExprs with SlotRefs into tupleDesc.
    for (int i = 0; i < resultExprs.size(); ++i) {
      SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
      slotDesc.setLabel(colLabels.get(i));
      slotDesc.setType(resultExprs.get(i).getType());
      slotDesc.setStats(ColumnStats.fromExpr(resultExprs.get(i)));
      slotDesc.setIsMaterialized(true);
      SlotRef slotRef = new SlotRef(slotDesc);
      resultExprs.set(i, slotRef);
    }
    tupleDesc.computeMemLayout();
    // MergeNode.init() needs tupleDesc to have been initialized
    mergeNode.init(analyzer);
    return mergeNode;
  }

  /**
   * Transform '=', '<[=]' and '>[=]' comparisons for given slot into
   * ValueRange. Also removes those predicates which were used for the construction
   * of ValueRange from 'conjuncts_'. Only looks at comparisons w/ string constants
   * (ie, the bounds of the result can be evaluated with Expr::GetValue(NULL)).
   * HBase row key filtering works only if the row key is mapped to a string column and
   * the expression is a string constant expression.
   * If there are multiple competing comparison predicates that could be used
   * to construct a ValueRange, only the first one from each category is chosen.
   */
  private ValueRange createHBaseValueRange(SlotDescriptor d, List<Expr> conjuncts) {
    ListIterator<Expr> i = conjuncts.listIterator();
    ValueRange result = null;
    while (i.hasNext()) {
      Expr e = i.next();
      if (!(e instanceof BinaryPredicate)) continue;
      BinaryPredicate comp = (BinaryPredicate) e;
      if (comp.getOp() == BinaryPredicate.Operator.NE) continue;
      Expr slotBinding = comp.getSlotBinding(d.getId());
      if (slotBinding == null || !slotBinding.isConstant() ||
          !slotBinding.getType().equals(ColumnType.STRING)) {
        continue;
      }

      if (comp.getOp() == BinaryPredicate.Operator.EQ) {
        i.remove();
        return ValueRange.createEqRange(slotBinding);
      }

      if (result == null) result = new ValueRange();

      // TODO: do we need copies here?
      if (comp.getOp() == BinaryPredicate.Operator.GT
          || comp.getOp() == BinaryPredicate.Operator.GE) {
        if (result.getLowerBound() == null) {
          result.setLowerBound(slotBinding);
          result.setLowerBoundInclusive(comp.getOp() == BinaryPredicate.Operator.GE);
          i.remove();
        }
      } else {
        if (result.getUpperBound() == null) {
          result.setUpperBound(slotBinding);
          result.setUpperBoundInclusive(comp.getOp() == BinaryPredicate.Operator.LE);
          i.remove();
        }
      }
    }
    return result;
  }

  /**
   * Returns plan tree for an inline view ref:
   * - predicates from the enclosing scope that can be evaluated directly within
   *   the inline-view plan are pushed down
   * - predicates that cannot be evaluated directly within the inline-view plan
   *   but only apply to the inline view are evaluated in a SelectNode placed
   *   on top of the inline view plan
   * - all slots that are referenced by predicates from the enclosing scope that cannot
   *   be pushed down are marked as materialized (so that when computeMemLayout() is
   *   called on the base table descriptors materialized by the inline view it has a
   *   complete picture)
   */
  private PlanNode createInlineViewPlan(Analyzer analyzer, InlineViewRef inlineViewRef)
      throws ImpalaException {
    // If possible, "push down" view predicates; this is needed in order to ensure
    // that predicates such as "x + y = 10" are evaluated in the view's plan tree
    // rather than a SelectNode grafted on top of that plan tree.
    // This doesn't prevent predicate propagation, because predicates like
    // "x = 10" that get pushed down are still connected to equivalent slots
    // via the equality predicates created for the view's select list.
    // Include outer join conjuncts here as well because predicates from the
    // On-clause of an outer join may be pushed into the inline view as well.
    List<Expr> unassigned =
        analyzer.getUnassignedConjuncts(inlineViewRef.getId().asList(), true);
    if (!inlineViewRef.getViewStmt().hasLimitClause()) {
      // check if we can evaluate them
      List<Expr> preds = Lists.newArrayList();
      for (Expr e: unassigned) {
        if (analyzer.canEvalPredicate(inlineViewRef.getId().asList(), e)) preds.add(e);
      }
      unassigned.removeAll(preds);
      // create new predicates against the inline view's unresolved result exprs, not
      // the resolved result exprs, in order to avoid skipping scopes (and ignoring
      // limit clauses on the way)
      List<Expr> viewPredicates = Expr.cloneList(preds, inlineViewRef.getSmap());

      // "migrate" conjuncts_ by marking them as assigned and re-registering them with
      // new ids
      analyzer.markConjunctsAssigned(viewPredicates);
      analyzer.registerConjuncts(viewPredicates);
    }

    // mark (fully resolve) slots referenced by remaining unassigned conjuncts_ as
    // materialized
    analyzer.materializeSlots(Expr.cloneList(unassigned, inlineViewRef.getBaseTblSmap()));

    // Turn a constant select into a MergeNode that materializes the exprs.
    // TODO: unify this with createConstantSelectPlan(), this is basically the
    // same thing
    QueryStmt viewStmt = inlineViewRef.getViewStmt();
    if (viewStmt instanceof SelectStmt) {
      SelectStmt selectStmt = (SelectStmt) viewStmt;
      if (selectStmt.getTableRefs().isEmpty()) {
        // Analysis should have generated a tuple id_ into which to materialize the exprs.
        Preconditions.checkState(inlineViewRef.getMaterializedTupleIds().size() == 1);
        // we need to materialize all slots of our inline view tuple
        analyzer.getTupleDesc(inlineViewRef.getId()).materializeSlots();
        MergeNode mergeNode = new MergeNode(nodeIdGenerator_.getNextId(),
            inlineViewRef.getMaterializedTupleIds().get(0));
        mergeNode.setTblRefIds(Lists.newArrayList(inlineViewRef.getId()));
        mergeNode.getConstExprLists().add(selectStmt.getBaseTblResultExprs());
        mergeNode.init(analyzer);
        return mergeNode;
      }
    }

    PlanNode rootNode =
        createQueryPlan(inlineViewRef.getViewStmt(), inlineViewRef.getAnalyzer(), -1);
    // TODO: we should compute the "physical layout" of the view's descriptor, so that
    // the avg row size is availble during optimization; however, that means we need to
    // select references to its resultExprs from the enclosing scope(s)
    rootNode.setTblRefIds(Lists.newArrayList(inlineViewRef.getId()));
    // set smap *before* creating a SelectNode in order to allow proper resolution
    rootNode.setBaseTblSmap(inlineViewRef.getBaseTblSmap());
    // if the view has a limit clause we may have conjuncts_ from the enclosing scope
    // left
    rootNode = addUnassignedConjuncts(
        analyzer, inlineViewRef.getDesc().getId().asList(), rootNode);
    return rootNode;
  }

  /**
   * Create node for scanning all data files of a particular table.
   */
  private PlanNode createScanNode(Analyzer analyzer, TableRef tblRef)
      throws InternalException, AuthorizationException {
    ScanNode scanNode = null;
    if (tblRef.getTable() instanceof HdfsTable) {
      scanNode = new HdfsScanNode(nodeIdGenerator_.getNextId(), tblRef.getDesc(),
          (HdfsTable)tblRef.getTable());
      scanNode.init(analyzer);
      return scanNode;
    } else {
      // HBase table
      scanNode = new HBaseScanNode(nodeIdGenerator_.getNextId(), tblRef.getDesc());
    }
    // TODO: move this to HBaseScanNode.init();
    Preconditions.checkState(scanNode instanceof HBaseScanNode);

    List<Expr> conjuncts = analyzer.getUnassignedConjuncts(scanNode);
    // mark conjuncts_ assigned here; they will either end up inside a
    // ValueRange or will be evaluated directly by the node
    analyzer.markConjunctsAssigned(conjuncts);
    List<ValueRange> keyRanges = Lists.newArrayList();
    // determine scan predicates for clustering cols
    for (int i = 0; i < tblRef.getTable().getNumClusteringCols(); ++i) {
      SlotDescriptor slotDesc = analyzer.getColumnSlot(
          tblRef.getDesc(), tblRef.getTable().getColumns().get(i));
      if (slotDesc == null || !slotDesc.getType().isStringType()) {
        // the hbase row key is mapped to a non-string type
        // (since it's stored in ascii it will be lexicographically ordered,
        // and non-string comparisons won't work)
        keyRanges.add(null);
      } else {
        // create ValueRange from conjuncts_ for slot; also removes conjuncts_ that were
        // used as input for filter
        keyRanges.add(createHBaseValueRange(slotDesc, conjuncts));
      }
    }

    ((HBaseScanNode)scanNode).setKeyRanges(keyRanges);
    scanNode.addConjuncts(conjuncts);
    scanNode.init(analyzer);

    return scanNode;
  }

  /**
   * Return join conjuncts_ that can be used for hash table lookups.
   * - for inner joins, those are equi-join predicates in which one side is fully bound
   *   by lhsIds and the other by rhs' id_;
   * - for outer joins: same type of conjuncts_ as inner joins, but only from the JOIN
   *   clause
   * Returns the conjuncts_ in 'joinConjuncts' (in which "<lhs> = <rhs>" is returned
   * as Pair(<lhs>, <rhs>)) and also in their original form in 'joinPredicates'.
   * If no conjuncts_ are found, constructs them based on equivalence classes, where
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
    List<TupleId> rhsIds = Lists.newArrayList(rhsId);
    List<Expr> candidates;
    if (rhs.getJoinOp().isOuterJoin()) {
      // TODO: create test for this
      Preconditions.checkState(rhs.getOnClause() != null);
      candidates = analyzer.getEqJoinConjuncts(rhsId, rhs);
    } else {
      candidates = analyzer.getEqJoinConjuncts(rhsId, null);
    }
    if (candidates == null) return;

    // equivalence classes of eq predicates in joinPredicates
    Set<EquivalenceClassId> joinEquivClasses = Sets.newHashSet();

    for (Expr e: candidates) {
      // Ignore predicate if one of its children is a constant.
      if (e.getChild(0).isConstant() || e.getChild(1).isConstant()) continue;

      Expr rhsExpr = null;
      if (e.getChild(0).isBoundByTupleIds(rhsIds)) {
        rhsExpr = e.getChild(0);
      } else {
        Preconditions.checkState(e.getChild(1).isBoundByTupleIds(rhsIds));
        rhsExpr = e.getChild(1);
      }

      Expr lhsExpr = null;
      if (e.getChild(1).isBoundByTupleIds(lhsIds)) {
        lhsExpr = e.getChild(1);
      } else if (e.getChild(0).isBoundByTupleIds(lhsIds)) {
        lhsExpr = e.getChild(0);
      } else {
        // not an equi-join condition between lhsIds and rhsId
        continue;
      }

      // Ignore predicates that express a redundant equivalence relationship. We assume
      // that for each equivalence class, the equivalences between slots from only the
      // lhs or rhs are already enforced by predicates in the lhs/rhs plan trees,
      // respectively (see Analyzer.enforceSlotEquivalences()). Therefore, it is
      // sufficient to establish equivalence between the lhs and rhs slots by assigning
      // a single join predicate per equivalence class, i.e., any join predicates beyond
      // that are redundant. We still return those predicates in joinPredicates so they
      // get marked as assigned.
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
        // construct a BinaryPredicates in order to get correct casting;
        // we only do this for one of the equivalent slots, all the other implied
        // equalities are redundant
        Expr pred = analyzer.createEqPredicate(lhsSlotIds.get(0), slotDesc.getId());
        joinConjuncts.add(new Pair<Expr, Expr>(pred.getChild(0), pred.getChild(1)));
      }
    }
  }

  /**
   * Create a node to join outer with inner.
   */
  private PlanNode createJoinNode(
      Analyzer analyzer, PlanNode outer, PlanNode inner, TableRef innerRef,
      boolean throwOnError) throws ImpalaException {
    if (innerRef.getJoinOp() == JoinOperator.CROSS_JOIN) {
      // TODO If there are eq join predicates then we should construct a hash join
      CrossJoinNode result = new CrossJoinNode(outer, inner, innerRef);
      result.init(analyzer);
      result.getChildren().get(1).setCompactData(true);
      return result;
    }

    List<Pair<Expr, Expr>> eqJoinConjuncts = Lists.newArrayList();
    List<Expr> eqJoinPredicates = Lists.newArrayList();
    // get eq join predicates for the TableRefs' ids (not the PlanNodes' ids, which
    // are materialized)
    getHashLookupJoinConjuncts(
        analyzer, outer.getTblRefIds(), innerRef, eqJoinConjuncts, eqJoinPredicates);
    if (eqJoinConjuncts.isEmpty()) {
      if (!throwOnError) return null;
      throw new NotImplementedException(
          String.format(
            "Join with '%s' requires at least one conjunctive equality predicate. To " +
            "perform a Cartesian product between two tables, use a CROSS JOIN.",
            innerRef.getAliasAsName()));
    }
    analyzer.markConjunctsAssigned(eqJoinPredicates);

    List<Expr> ojConjuncts = Lists.newArrayList();
    if (innerRef.getJoinOp().isOuterJoin()) {
      // Also assign conjuncts_ from On clause. All remaining unassigned conjuncts_
      // that can be evaluated by this join are assigned in createSelectPlan().
      ojConjuncts = analyzer.getUnassignedOjConjuncts(innerRef);
      analyzer.markConjunctsAssigned(ojConjuncts);
    }

    HashJoinNode result =
        new HashJoinNode(outer, inner, innerRef, eqJoinConjuncts, ojConjuncts);
    result.init(analyzer);

    // build side of join copies data to a compact representation in the tuple buffer
    result.getChildren().get(1).setCompactData(true);
    return result;
  }

  /**
   * Create a tree of PlanNodes for the given tblRef, which can be a BaseTableRef or a
   * InlineViewRef
   */
  private PlanNode createTableRefNode(Analyzer analyzer, TableRef tblRef)
      throws ImpalaException {
    if (tblRef instanceof BaseTableRef) {
      return createScanNode(analyzer, tblRef);
    }
    if (tblRef instanceof InlineViewRef) {
      return createInlineViewPlan(analyzer, (InlineViewRef) tblRef);
    }
    throw new InternalException("unknown TableRef node");
  }

  /**
   * Create a plan tree corresponding to 'unionOperands' for the given unionStmt.
   * The individual operands' plan trees are attached to a single MergeNode.
   */
  private MergeNode createUnionMergePlan(
      Analyzer analyzer, UnionStmt unionStmt, List<UnionOperand> unionOperands)
      throws ImpalaException {
    MergeNode mergeNode =
        new MergeNode(nodeIdGenerator_.getNextId(), unionStmt.getTupleId());
    for (UnionOperand op: unionOperands) {
      QueryStmt queryStmt = op.getQueryStmt();
      if (op.isDropped()) continue;
      if (queryStmt instanceof SelectStmt) {
        SelectStmt selectStmt = (SelectStmt) queryStmt;
        if (selectStmt.getTableRefs().isEmpty()) {
          mergeNode.addConstExprList(selectStmt.getBaseTblResultExprs());
          continue;
        }
      }
      PlanNode opPlan = createQueryPlan(queryStmt, analyzer, -1);
      mergeNode.addChild(opPlan, op.getQueryStmt().getBaseTblResultExprs());
    }
    mergeNode.init(analyzer);
    return mergeNode;
  }

  /**
   * Returns plan tree for unionStmt:
   * - distinctOperands' plan trees are collected in a single MergeNode
   *   and duplicates removed via distinct aggregation
   * - the output of that plus the allOperands' plan trees are collected in
   *   another MergeNode which materializes the result of unionStmt
   */
  private PlanNode createUnionPlan(Analyzer analyzer, UnionStmt unionStmt)
      throws ImpalaException {
    // Turn unassigned predicates for unionStmt's tupleId_ into predicates for
    // the individual operands.
    // Do this prior to creating the operands' plan trees so they get a chance to
    // pick up propagated predicates.
    // Drop operands that have constant conjuncts evaluating to false, and drop
    // constant conjuncts evaluating to true.
    List<Expr> conjuncts =
        analyzer.getUnassignedConjuncts(unionStmt.getTupleId().asList(), false);
    for (UnionOperand op: unionStmt.getOperands()) {
      List<Expr> opConjuncts = Expr.cloneList(conjuncts, op.getSmap());
      List<Expr> nonConstOpConjuncts = Lists.newArrayList();
      for (int i = 0; i < opConjuncts.size(); ++i) {
        // analyze after expr substitution to insert casts, etc.
        Expr opConjunct = opConjuncts.get(i);
        opConjunct.reanalyze(analyzer);
        if (!opConjunct.isConstant()) {
          nonConstOpConjuncts.add(opConjunct);
          continue;
        }
        // Evaluate constant conjunct and drop operand if it evals to false.
        if (!FeSupport.EvalPredicate(opConjunct, analyzer.getQueryContext())) {
          op.drop();
          break;
        }
      }
      if (!op.isDropped()) analyzer.registerConjuncts(nonConstOpConjuncts);
    }
    analyzer.markConjunctsAssigned(conjuncts);

    // mark slots after predicate propagation but prior to plan tree generation
    unionStmt.materializeRequiredSlots(analyzer);

    PlanNode result = null;
    // create DISTINCT tree
    if (unionStmt.hasDistinctOps()) {
      result = createUnionMergePlan(
          analyzer, unionStmt, unionStmt.getDistinctOperands());
      result = new AggregationNode(
          nodeIdGenerator_.getNextId(), result, unionStmt.getDistinctAggInfo());
      result.init(analyzer);
    }
    // create ALL tree
    if (unionStmt.hasAllOps()) {
      MergeNode allMerge =
          createUnionMergePlan(analyzer, unionStmt, unionStmt.getAllOperands());
      // for unionStmt, baseTblResultExprs = resultExprs
      if (result != null) allMerge.addChild(result, unionStmt.getResultExprs());
      result = allMerge;
    }

    result = addOrderByLimit(analyzer, result, unionStmt.getSortInfo(),
        unionStmt.getLimit(), -1, unionStmt.getOffset());
    return result;
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
              "PlanNode stats are: numNodes_=%s ", node.getPerHostMemCost(),
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
    // TODO: This mapping is not necessary anymore. Remove it and clean up.
    Map<PlanNodeId, List<PlanFragment>> exchangeSources = Maps.newHashMap();
    for (PlanFragment fragment: fragments) {
      if (fragment.getDestNode() == null) continue;
      List<PlanFragment> srcFragments =
          exchangeSources.get(fragment.getDestNode().getId());
      if (srcFragments == null) {
        srcFragments = Lists.newArrayList();
        exchangeSources.put(fragment.getDestNode().getId(), srcFragments);
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
