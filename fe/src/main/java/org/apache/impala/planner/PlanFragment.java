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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.TreeNode;
import org.apache.impala.planner.JoinNode.DistributionMode;
import org.apache.impala.planner.PlanNode.ExecPhaseResourceProfiles;
import org.apache.impala.planner.RuntimeFilterGenerator.RuntimeFilter;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPartitionType;
import org.apache.impala.thrift.TPlanFragment;
import org.apache.impala.thrift.TPlanFragmentTree;
import org.apache.impala.thrift.TQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;

import javax.annotation.Nullable;

/**
 * PlanFragments form a tree structure via their ExchangeNodes. A tree of fragments
 * connected in that way forms a plan. The output of a plan is produced by the root
 * fragment and is either the result of the query or an intermediate result
 * needed by a different plan (such as a hash table).
 *
 * Plans are grouped into cohorts based on the consumer of their output: all
 * plans that materialize intermediate results for a particular consumer plan
 * are grouped into a single cohort.
 *
 * A PlanFragment encapsulates the specific tree of execution nodes (PlanNodes) that
 * are used to produce the output of the plan fragment, as well as output exprs,
 * destination node, etc. If there are no output exprs, the full row that is
 * is produced by the plan root is marked as materialized.
 *
 * PlanNode trees are connected across fragments where the parent fragment consumes the
 * output of the child fragment. In this case the PlanNode and DataSink of the two
 * fragments must match, e.g. ExchangeNode and DataStreamSink or a JoinNode and a
 * JoinBuildSink.
 *
 * A plan fragment can have one or many instances, each of which in turn is executed by
 * an individual node and the output sent to a specific instance of the destination
 * fragment (or, in the case of the root fragment, is materialized in some form).
 *
 * A hash-partitioned plan fragment is the result of one or more hash-partitioning data
 * streams being received by plan nodes in this fragment. In the future, a fragment's
 * data partition could also be hash partitioned based on a scan node that is reading
 * from a physically hash-partitioned table.
 *
 * The sequence of calls is:
 * - c'tor
 * - assemble with getters, etc. setSink() must be called so that the fragment has a sink.
 * - finalizeExchanges()
 * - computeResourceProfile()
 * - toThrift()
 */
public class PlanFragment extends TreeNode<PlanFragment> {
  private final static Logger LOG = LoggerFactory.getLogger(PlanFragment.class);
  private final PlanFragmentId fragmentId_;
  private PlanId planId_;
  private CohortId cohortId_;

  private final boolean coordinatorOnly_;

  // root of plan tree executed by this fragment
  private PlanNode planRoot_;

  // exchange node or join node to which this fragment sends its output
  private PlanNode destNode_;

  // created in finalize() or set in setSink()
  private DataSink sink_;

  // specification of the partition of the input of this fragment;
  // an UNPARTITIONED fragment is executed on only a single node
  // TODO: improve this comment, "input" is a bit misleading
  private DataPartition dataPartition_;

  // specification of how the output of this fragment is partitioned (i.e., how
  // it's sent to its destination);
  // if the output is UNPARTITIONED, it is being broadcast
  private DataPartition outputPartition_;

  // Resource requirements and estimates for an instance of this plan fragment.
  // Initialized with a dummy value. Gets set correctly in
  // computeResourceProfile().
  private ResourceProfile perInstanceResourceProfile_ = ResourceProfile.invalid();

  // Resource requirements and estimates for per-host resources that are consumed
  // on a backend running an instance of this plan fragment. Initialized with a
  // dummy value. Gets set correctly in computeResourceProfile().
  private ResourceProfile perBackendResourceProfile_ = ResourceProfile.invalid();

  // The total of initial memory reservations (in bytes) that will be claimed over the
  // lifetime of a fragment executing on a backend. Computed in computeResourceProfile().
  // Split between the per-instance amounts and reservations shared across all instance
  // on a backend.
  private long perInstanceInitialMemReservationTotalClaims_ = -1;
  private long perBackendInitialMemReservationTotalClaims_ = -1;

  // The total memory (in bytes) required for the runtime filters produced by the
  // plan nodes in this fragment. Each instance of the fragment will produce a separate
  // copy of the filter, so requires its own memory.
  private long producedRuntimeFiltersMemReservationBytes_ = 0;

  // The total memory (in bytes) required for global runtime filters consumed by the
  // plan nodes in this fragment. Memory for locally produced filters is accounted
  // for in producedRuntimeFiltersMemReservationBytes_.
  // A single instance of the filter is shared between all instances of a fragment
  // on a backend.
  private long consumedGlobalRuntimeFiltersMemReservationBytes_ = 0;

  // The root of segment costs tree of this fragment.
  // Individual element of the tree describe processing cost of subset of plan nodes
  // that is divided by blocking PlanNode/DataSink boundary. Together, they describe total
  // processing cost of this fragment. Set in computeCostingSegment().
  private CostingSegment rootSegment_;

  // Maximum allowed parallelism of this fragment before caping it against
  // MAX_FRAGMENT_INSTANCES_PER_NODE query option and Analyzer.getAvailableCoresPerNode().
  // It is displayed in explain string of query profile to assist with query tuning.
  // Set in adjustToMaxParallelism().
  private int maxParallelism_ = -1;

  // An adjusted number of instance based on ProcessingCost calculation.
  // A positive value implies that the instance count has been adjusted, either through
  // traverseEffectiveParallelism() or by fragment member (PlanNode or DataSink) calling
  // setFixedInstanceCount(). Internally, this must be set through
  // setAdjustedInstanceCount().
  private int adjustedInstanceCount_ = -1;

  // Mark if this fragment has a fixed instance count dictated by any of its PlanNode or
  // DataSink member.
  private boolean isFixedParallelism_ = false;

  // The original instance count before ProcessingCost based adjustment.
  // Set in setEffectiveNumInstance() and only set if instance count differ between
  // the original plan vs the ProcessingCost based plan.
  private int originalInstanceCount_ = -1;

  // Information about any cpu comparison that was made (if any) at this fragment.
  // Set in maxCore().
  private int thisTreeCpuCore_ = -1;
  private int subtreeCpuCore_ = -1;

  // Determine whether this fragment is the dominant one in the plan tree based on
  // calculation initiated by Planner.computeBlockingAwareCores().
  // A fragment is dominant if it contribute towards the final CoreCount.
  private boolean isDominantFragment_ = false;

  public long getProducedRuntimeFiltersMemReservationBytes() {
    return producedRuntimeFiltersMemReservationBytes_;
  }

  /**
   * C'tor for fragment with specific partition; the output is by default broadcast.
   */
  public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition,
      boolean coordinatorOnly) {
    fragmentId_ = id;
    planRoot_ = root;
    dataPartition_ = partition;
    outputPartition_ = DataPartition.UNPARTITIONED;
    setFragmentInPlanTree(planRoot_);
    coordinatorOnly_ = coordinatorOnly;

    // Coordinator-only fragments must be unpartitined as there is only one instance of
    // them.
    Preconditions.checkState(!coordinatorOnly ||
        dataPartition_.equals(DataPartition.UNPARTITIONED));
  }

  public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition) {
    this(id, root, partition, false);
  }

  /**
   * Assigns 'this' as fragment of all PlanNodes in the plan tree rooted at node.
   * Does not traverse the children of ExchangeNodes because those must belong to a
   * different fragment.
   */
  public void setFragmentInPlanTree(PlanNode node) {
    if (node == null) return;
    node.setFragment(this);
    if (node instanceof ExchangeNode) return;
    for (PlanNode child : node.getChildren()) setFragmentInPlanTree(child);
  }

  /**
   * Collect and return all PlanNodes that belong to the exec tree of this fragment.
   */
  public List<PlanNode> collectPlanNodes() {
    List<PlanNode> nodes = new ArrayList<>();
    collectPlanNodesHelper(planRoot_, Predicates.alwaysTrue(), nodes);
    return nodes;
  }

  /**
   * Collect PlanNodes that belong to the exec tree of this fragment and for which
   * 'predicate' is true. Collected nodes are added to 'node'. Nodes are cast to
   * T.
   */
  public <T extends PlanNode> void collectPlanNodes(
      Predicate<? super PlanNode> predicate, List<T> nodes) {
    collectPlanNodesHelper(planRoot_, predicate, nodes);
  }

  @SuppressWarnings("unchecked")
  private  <T extends PlanNode> void collectPlanNodesHelper(
      PlanNode root, Predicate<? super PlanNode> predicate, List<T> nodes) {
    if (root == null) return;
    if (predicate.apply(root)) nodes.add((T)root);
    for (PlanNode child: root.getChildren()) {
      if (child.getFragment() == this) {
        collectPlanNodesHelper(child, predicate, nodes);
      }
    }
  }

  /**
   * Compute processing cost of PlanNodes and DataSink of this fragment, and aggregate
   * them into {@link CostingSegment} rooted at {@link #rootSegment_}.
   * <p>For example, given the following fragment plan:
   * <pre>
   * F03:PLAN FRAGMENT [HASH(i_class)] hosts=3 instances=3
   * segment-costs=[34550429, 2159270, 23752870, 1]
   * 08:TOP-N [LIMIT=100]
   * |  cost=900
   * |
   * 07:ANALYTIC
   * |  cost=23751970
   * |
   * 06:SORT
   * |  cost=2159270
   * |
   * 12:AGGREGATE [FINALIZE]
   * |  cost=34548320
   * |
   * 11:EXCHANGE [HASH(i_class)]
   *    cost=2109
   * </pre>
   * The post-order traversal of {@link #rootSegment_} tree show processing cost detail of
   * {@code [(2109+34548320), 2159270, (23751970+900), 1]}.
   * The DataSink with cost 1 is a separate segment since the last PlanNode (TOP-N) is a
   * blocking node.
   *
   * @param queryOptions A query options for this query.
   */
  public void computeCostingSegment(TQueryOptions queryOptions) {
    List<PlanNode> planNodes = collectPlanNodes();
    // Iterate node list in reverse from leaf to root.
    for (int i = planNodes.size() - 1; i >= 0; i--) {
      PlanNode node = planNodes.get(i);
      node.computeProcessingCost(queryOptions);
      node.computeRowConsumptionAndProductionToCost();
      if (LOG.isTraceEnabled()) {
        LOG.trace("ProcessingCost Node " + node.getProcessingCost().debugString());
      }
    }
    sink_.computeProcessingCost(queryOptions);
    sink_.computeRowConsumptionAndProductionToCost();
    if (LOG.isTraceEnabled()) {
      LOG.trace("ProcessingCost Sink " + sink_.getProcessingCost().debugString());
    }

    CostingSegment topSegment = collectCostingSegmentHelper(planRoot_);

    if (isBlockingNode(planRoot_)) {
      rootSegment_ = new CostingSegment(sink_);
      rootSegment_.addChild(topSegment);
    } else {
      topSegment.setSink(sink_);
      rootSegment_ = topSegment;
    }
  }

  private CostingSegment collectCostingSegmentHelper(PlanNode root) {
    Preconditions.checkNotNull(root);

    List<CostingSegment> blockingChildSegments = Lists.newArrayList();
    List<CostingSegment> nonBlockingChildSegments = Lists.newArrayList();
    for (PlanNode child : root.getChildren()) {
      if (child.getFragment() != this) continue;
      CostingSegment childCostingSegment = collectCostingSegmentHelper(child);

      if (isBlockingNode(child)) {
        blockingChildSegments.add(childCostingSegment);
      } else {
        nonBlockingChildSegments.add(childCostingSegment);
      }
    }

    CostingSegment thisSegment;
    if (nonBlockingChildSegments.isEmpty()) {
      // No child or all children are blocking nodes.
      thisSegment = new CostingSegment(root);
    } else {
      thisSegment = CostingSegment.mergeCostingSegment(nonBlockingChildSegments);
      thisSegment.appendNode(root);
    }

    if (!blockingChildSegments.isEmpty()) thisSegment.addChildren(blockingChildSegments);
    return thisSegment;
  }

  /**
   * Do any final work to set up the ExchangeNodes and DataStreamSinks for this fragment.
   * If this fragment has partitioned joins, ensures that the corresponding partition
   * exprs of all hash-partitioning senders are cast to appropriate types.
   * Otherwise, the hashes generated for identical partition values may differ
   * among senders if the partition-expr types are not identical.
   */
  public void finalizeExchanges(Analyzer analyzer) throws InternalException {
    if (destNode_ != null && destNode_ instanceof ExchangeNode) {
      Preconditions.checkState(sink_ == null);
      // we're streaming to an exchange node
      DataStreamSink streamSink =
          new DataStreamSink((ExchangeNode)destNode_, outputPartition_);
      streamSink.setFragment(this);
      sink_ = streamSink;
    }

    // Must be called regardless of this fragment's data partition. This fragment might
    // be RANDOM partitioned due to a union. The union could still have partitioned joins
    // in its child subtrees for which casts on the exchange senders are needed.
    castPartitionedJoinExchanges(planRoot_, analyzer);
  }

  /**
   * Recursively traverses the plan tree rooted at 'node' and casts the partition exprs
   * of all senders feeding into a series of partitioned joins to compatible types.
   */
  private void castPartitionedJoinExchanges(PlanNode node, Analyzer analyzer) {
    if (node instanceof HashJoinNode
        && ((JoinNode) node).getDistributionMode() == DistributionMode.PARTITIONED) {
      // Contains all exchange nodes in this fragment below the current join node.
      List<ExchangeNode> exchNodes = new ArrayList<>();
      node.collect(ExchangeNode.class, exchNodes);

      // Contains partition-expr lists of all hash-partitioning sender fragments.
      List<List<Expr>> senderPartitionExprs = new ArrayList<>();
      for (ExchangeNode exchNode: exchNodes) {
        Preconditions.checkState(!exchNode.getChildren().isEmpty());
        PlanFragment senderFragment = exchNode.getChild(0).getFragment();
        Preconditions.checkNotNull(senderFragment);
        if (!senderFragment.getOutputPartition().isHashPartitioned()) continue;
        List<Expr> partExprs = senderFragment.getOutputPartition().getPartitionExprs();
        senderPartitionExprs.add(partExprs);
      }

      // Cast partition exprs of all hash-partitioning senders to their compatible types.
      try {
        analyzer.castToSetOpCompatibleTypes(senderPartitionExprs, false);
      } catch (AnalysisException e) {
        // Should never happen. Analysis should have ensured type compatibility already.
        throw new IllegalStateException(e);
      }
    } else {
      // Recursively traverse plan nodes in this fragment.
      for (PlanNode child: node.getChildren()) {
        if (child.getFragment() == this) castPartitionedJoinExchanges(child, analyzer);
      }
    }
  }

  public void computePipelineMembership() {
    planRoot_.computePipelineMembership();
  }

  /**
   * Compute the peak resource profile for an instance of this fragment. Must
   * be called after all the plan nodes and sinks are added to the fragment and resource
   * profiles of all children fragments are computed. Also accounts for the memory used by
   * runtime filters that are stored at the fragment level.
   */
  public void computeResourceProfile(Analyzer analyzer) {
    Preconditions.checkState(sink_ != null);
    // Compute resource profiles for all plan nodes and sinks in the fragment.
    sink_.computeResourceProfile(analyzer.getQueryOptions());
    computeRuntimeFilterResources(analyzer);

    perBackendInitialMemReservationTotalClaims_ =
        consumedGlobalRuntimeFiltersMemReservationBytes_;
    perInstanceInitialMemReservationTotalClaims_ =
        sink_.getResourceProfile().getMinMemReservationBytes()
        + producedRuntimeFiltersMemReservationBytes_;
    for (PlanNode node: collectPlanNodes()) {
      perInstanceInitialMemReservationTotalClaims_ +=
          node.getNodeResourceProfile().getMinMemReservationBytes();
    }

    ExecPhaseResourceProfiles planTreeProfile =
        planRoot_.computeTreeResourceProfiles(analyzer.getQueryOptions());
    // The sink is opened after the plan tree.
    ResourceProfile fInstancePostOpenProfile =
        planTreeProfile.postOpenProfile.sum(sink_.getResourceProfile());
    // One thread is required to execute the plan tree.
    perInstanceResourceProfile_ =
        new ResourceProfileBuilder()
            .setMemEstimateBytes(producedRuntimeFiltersMemReservationBytes_)
            .setMinMemReservationBytes(producedRuntimeFiltersMemReservationBytes_)
            .setThreadReservation(1)
            .build()
            .sum(planTreeProfile.duringOpenProfile.max(fInstancePostOpenProfile));
    perBackendResourceProfile_ =
        new ResourceProfileBuilder()
            .setMemEstimateBytes(consumedGlobalRuntimeFiltersMemReservationBytes_)
            .setMinMemReservationBytes(consumedGlobalRuntimeFiltersMemReservationBytes_)
            .setThreadReservation(0)
            .build();
    validateResourceProfiles();
  }

  /**
   * Validates that the resource profiles for this PlanFragment are complete and valid,
   * i.e. that computeResourceProfile() was called and that it filled out the profiles
   * correctly. Raises an exception if an invariant is violated. */
  private void validateResourceProfiles() {
    Preconditions.checkState(perInstanceResourceProfile_.isValid());
    Preconditions.checkState(perBackendResourceProfile_.isValid());
    Preconditions.checkArgument(perInstanceInitialMemReservationTotalClaims_ > -1);
    Preconditions.checkArgument(perBackendInitialMemReservationTotalClaims_ > -1);
    Preconditions.checkArgument(producedRuntimeFiltersMemReservationBytes_ > -1);
    Preconditions.checkArgument(consumedGlobalRuntimeFiltersMemReservationBytes_ > -1);
  }

  /**
   * Helper for computeResourceProfile(). Populates
   * producedRuntimeFiltersMemReservationBytes_ and
   * consumedGlobalRuntimeFiltersMemReservationBytes_.
   */
  private void computeRuntimeFilterResources(Analyzer analyzer) {
    Map<RuntimeFilterId, RuntimeFilter> consumedFilters = new HashMap<>();
    Map<RuntimeFilterId, RuntimeFilter> producedFilters = new HashMap<>();
    // Visit all sinks and nodes to identify filters produced or consumed by fragment.
    sink_.computeResourceProfile(analyzer.getQueryOptions());
    Preconditions.checkState(
        sink_.getRuntimeFilters().isEmpty() || sink_ instanceof JoinBuildSink);
    for (RuntimeFilter filter : sink_.getRuntimeFilters()) {
      // Join build sinks are always runtime filter producers, not consumers.
      producedFilters.put(filter.getFilterId(), filter);
    }
    for (PlanNode node : collectPlanNodes()) {
      node.computeNodeResourceProfile(analyzer.getQueryOptions());
      boolean isFilterProducer = node instanceof JoinNode;
      for (RuntimeFilter filter : node.getRuntimeFilters()) {
        if (isFilterProducer) {
          producedFilters.put(filter.getFilterId(), filter);
        } else {
          consumedFilters.put(filter.getFilterId(), filter);
        }
      }
    }
    for (RuntimeFilter f : producedFilters.values()) {
      producedRuntimeFiltersMemReservationBytes_ += f.getFilterSize();
      if (analyzer.getQueryOptions().max_num_filters_aggregated_per_host > 1
          && !f.isBroadcast()) {
        // If distributed aggregation is enabled, any backend can be selected as
        // an intermediate aggregator. Add the same amount of memory per backend to
        // anticipate for it.
        consumedGlobalRuntimeFiltersMemReservationBytes_ += f.getFilterSize();
      }
    }
    for (RuntimeFilter f : consumedFilters.values()) {
      if (!producedFilters.containsKey(f.getFilterId())) {
        consumedGlobalRuntimeFiltersMemReservationBytes_ += f.getFilterSize();
      }
    }
  }

  public boolean coordinatorOnly() {
    return coordinatorOnly_;
  }

  public ResourceProfile getPerInstanceResourceProfile() {
    return perInstanceResourceProfile_;
  }
  public ResourceProfile getPerBackendResourceProfile() {
    return perBackendResourceProfile_;
  }

  /*
   * Return the resource profile for all instances on a single backend.
   */
  public ResourceProfile getTotalPerBackendResourceProfile(TQueryOptions queryOptions) {
    return perInstanceResourceProfile_.multiply(getNumInstancesPerHost(queryOptions))
        .sum(perBackendResourceProfile_);
  }

  /**
   * Return the number of nodes on which the plan fragment will execute.
   * invalid: -1
   */
  public int getNumNodes() {
    if (dataPartition_ == DataPartition.UNPARTITIONED) {
      return 1;
    } else if (sink_ instanceof JoinBuildSink) {
      // One instance is scheduled per node, for all instances of the fragment containing
      // the destination join node. ParallelPlanner sets the destination fragment when
      // adding the JoinBuildSink.
      return ((JoinBuildSink)sink_).getNumNodes();
    } else if (sink_ instanceof HdfsTableSink) {
      return ((HdfsTableSink)sink_).getNumNodes();
    } else {
      return planRoot_.getNumNodes();
    }
  }

  /**
   * Return an estimate of the number of instances of this fragment per host that it
   * executes on.
   */
  public int getNumInstancesPerHost(TQueryOptions queryOptions) {
    // Assume that instances are evenly divided across hosts.
    int numNodes = getNumNodes();
    int numInstances = getNumInstances();
    if (numNodes == -1 || numInstances == -1) {
      Preconditions.checkState(!queryOptions.isCompute_processing_cost());
      // Fall back to assuming that all maxDop instances will be generated.
      return Math.max(1, queryOptions.getMt_dop());
    }
    return (int) Math.ceil((double)numInstances / (double)numNodes);
  }

  /**
   * Return the total number of instances of this fragment across all hosts.
   * invalid: -1
   */
  public int getNumInstances() {
    if (dataPartition_ == DataPartition.UNPARTITIONED) {
      return 1;
    } else if (sink_ instanceof JoinBuildSink) {
      // One instance is scheduled per instance of the fragment containing the destination
      // join. ParallelPlanner sets the destination fragment when adding the
      // JoinBuildSink.
      return ((JoinBuildSink)sink_).getNumInstances();
    } else if (sink_ instanceof HdfsTableSink) {
      return ((HdfsTableSink)sink_).getNumInstances();
    } else {
      if (originalInstanceCount_ > -1) {
        int adjustedCount = getAdjustedInstanceCount();
        Preconditions.checkState(planRoot_.getNumInstances() == adjustedCount,
            "Instance count of " + getId() + " with plan root "
                + planRoot_.getDisplayLabel() + " (" + planRoot_.getNumInstances()
                + ") does not follow cost based adjustment (" + adjustedCount + ")!");
      }
      return planRoot_.getNumInstances();
    }
  }

  /**
   * Return early estimate of total number of instances of this fragment across all hosts.
   * This is intended for use by CPC code for estimating instance counts prior to any
   * call to setEffectiveNumInstance(). Currently we simply return the estimate from
   * getNumInstances() but we should try to improve this in the future.
   * TODO: Come up with a better prediction here of the instance count we're likely to
   * end up with after subsequent ProcessingCost adjustments.
   */
  public int getNumInstancesForCosting() {
    Preconditions.checkState(originalInstanceCount_ < 0);
    return getNumInstances();
  }

  /**
   * Estimates the number of distinct values of exprs per fragment instance based on the
   * data partition of this fragment, the number of nodes, and the degree of parallelism.
   * By default, it estimates the number of distinct values by using simple multiplication
   * of each expr.getNumDistinctValues(), and divide it by numInstances if a partition
   * column exist in an expr. However, if useMaxNdv is true, this method will return the
   * minimum between the default estimation vs the maximum expr.getNumDistinctValues().
   * Returns -1 for an invalid estimate, e.g., because getNumDistinctValues() failed on
   * one of the exprs.
   */
  public long getPerInstanceNdv(List<Expr> exprs, boolean useMaxNdv) {
    Preconditions.checkNotNull(dataPartition_);
    long result = 1;
    int numInstances = getNumInstances();
    Preconditions.checkState(numInstances >= 0);
    // The number of nodes is zero for empty tables.
    if (numInstances == 0) return 0;
    long maxNdv = 1;
    boolean partition = false;
    for (Expr expr: exprs) {
      long numDistinct = expr.getNumDistinctValues();
      if (numDistinct == -1) {
        result = -1;
        break;
      }
      if (dataPartition_.getPartitionExprs().contains(expr)) {
        partition = true;
      }
      result = PlanNode.checkedMultiply(result, numDistinct);
      maxNdv = Math.max(maxNdv, numDistinct);
    }
    if (partition) {
      result = (long)Math.max((double) result / (double) numInstances, 1L);
    }
    if (useMaxNdv && result > maxNdv) result = maxNdv;
    return result;
  }

  /**
   * Estimate the number of distinct values of exprs per fragment instance accounting
   * for the likelihood of duplicate keys across fragment instances (see IMPALA-2945).
   * We currently only use this estimate for CPU costing when CPC is enabled but we
   * should use this approach more broadly for resource estimates because the approach
   * in getPerInstanceNdv() ignores dups and can underestimate badly especially for low
   * NDV.
   */
  public long getPerInstanceNdvForCpuCosting(long inputCardinality, List<Expr> exprs) {
    Preconditions.checkState(inputCardinality > -1);

    long result = 1;
    int numInstances = getNumInstancesForCosting();
    Preconditions.checkState(numInstances >= 0);
    // TODO: Should we use AggregationNode.DEFAULT_SKEW_FACTOR when calculating
    // per instance input cardinality here?
    long perInstanceInputCardinality =
        (long) Math.ceil(((double) inputCardinality / numInstances));
    for (Expr expr : exprs) {
      long exprGlobalNdv = expr.getNumDistinctValues();
      if (exprGlobalNdv == -1) {
        // A worst-case cardinality is better than an unknown cardinality.
        exprGlobalNdv = inputCardinality;
      }
      // Estimate the per instance NDV for this expr accounting for the probability of
      // duplicates
      double probGivenValueIncludedInFragment = 1.0
          - Math.pow(((double) (exprGlobalNdv - 1) / exprGlobalNdv),
                perInstanceInputCardinality);
      long exprPerInstanceNdv = (long) (probGivenValueIncludedInFragment * exprGlobalNdv);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Expr per instance NDV: " + exprPerInstanceNdv
            + ", expr: " + expr.debugString() + ", Input Cardinality: " + inputCardinality
            + ", perInstanceInputCardinality: " + perInstanceInputCardinality);
      }
      // Include in combined NDV
      result = PlanNode.checkedMultiply(result, exprPerInstanceNdv);
    }

    if (result > inputCardinality) result = inputCardinality;
    return result;
  }

  public TPlanFragment toThrift() {
    validateResourceProfiles();
    TPlanFragment result = new TPlanFragment();
    result.setDisplay_name(fragmentId_.toString());
    if (planRoot_ != null) result.setPlan(planRoot_.treeToThrift());
    if (sink_ != null) result.setOutput_sink(sink_.toThrift());
    result.setPartition(dataPartition_.toThrift());

    result.setInstance_initial_mem_reservation_total_claims(
        perInstanceInitialMemReservationTotalClaims_);
    result.setBackend_initial_mem_reservation_total_claims(
        perBackendInitialMemReservationTotalClaims_);
    result.setProduced_runtime_filters_reservation_bytes(
        producedRuntimeFiltersMemReservationBytes_);
    result.setConsumed_runtime_filters_reservation_bytes(
        consumedGlobalRuntimeFiltersMemReservationBytes_);
    result.setInstance_min_mem_reservation_bytes(
        perInstanceResourceProfile_.getMinMemReservationBytes());
    result.setBackend_min_mem_reservation_bytes(
        perBackendResourceProfile_.getMinMemReservationBytes());
    result.setThread_reservation(perInstanceResourceProfile_.getThreadReservation());
    if (hasAdjustedInstanceCount()) {
      result.setEffective_instance_count(adjustedInstanceCount_);
    }
    result.setIs_coordinator_only(coordinatorOnly_);
    result.setIs_dominant(isDominantFragment_);
    return result;
  }

  public TPlanFragmentTree treeToThrift() {
    TPlanFragmentTree result = new TPlanFragmentTree();
    treeToThriftHelper(result);
    return result;
  }

  private void treeToThriftHelper(TPlanFragmentTree plan) {
    plan.addToFragments(toThrift());
    for (PlanFragment child: children_) {
      child.treeToThriftHelper(plan);
    }
  }

  public String getExplainString(TQueryOptions queryOptions, TExplainLevel detailLevel) {
    return getExplainString("", "", queryOptions, detailLevel);
  }

  /**
   * The root of the output tree will be prefixed by rootPrefix and the remaining plan
   * output will be prefixed by prefix.
   */
  protected final String getExplainString(String rootPrefix, String prefix,
      TQueryOptions queryOptions, TExplainLevel detailLevel) {
    StringBuilder str = new StringBuilder();
    Preconditions.checkState(dataPartition_ != null);
    String detailPrefix = prefix + "|  ";  // sink detail
    if (detailLevel == TExplainLevel.VERBOSE) {
      // we're printing a new tree, start over with the indentation
      prefix = "  ";
      rootPrefix = "  ";
      detailPrefix = prefix + "|  ";
      str.append(getFragmentHeaderString("", "", queryOptions, detailLevel));
      if (sink_ != null && sink_ instanceof DataStreamSink) {
        str.append(
            sink_.getExplainString(rootPrefix, detailPrefix, queryOptions, detailLevel));
      }
    } else if (detailLevel == TExplainLevel.EXTENDED) {
      // Print a fragment prefix displaying the # nodes and # instances
      str.append(
          getFragmentHeaderString(rootPrefix, detailPrefix, queryOptions, detailLevel));
      rootPrefix = prefix;
    }

    String planRootPrefix = rootPrefix;
    // Always print sinks other than DataStreamSinks.
    if (sink_ != null && !(sink_ instanceof DataStreamSink)) {
      str.append(
          sink_.getExplainString(rootPrefix, detailPrefix, queryOptions, detailLevel));
      if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
        str.append(prefix + "|\n");
      }
      // we already used the root prefix for the sink
      planRootPrefix = prefix;
    }
    if (planRoot_ != null) {
      str.append(
          planRoot_.getExplainString(planRootPrefix, prefix, queryOptions, detailLevel));
    }
    return str.toString();
  }

  /**
   * Get a header string for a fragment in an explain plan.
   */
  public String getFragmentHeaderString(String firstLinePrefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel) {
    boolean isComputeCost = queryOptions.isCompute_processing_cost();
    boolean useMTFragment = Planner.useMTFragment(queryOptions);
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("%s%s:PLAN FRAGMENT [%s]", firstLinePrefix,
        fragmentId_.toString(), dataPartition_.getExplainString()));
    builder.append(PrintUtils.printNumHosts(" ", getNumNodes()));
    builder.append(PrintUtils.printNumInstances(" ", getNumInstances()));
    if (isComputeCost && originalInstanceCount_ != getNumInstances()) {
      builder.append(" (adjusted from " + originalInstanceCount_ + ")");
    }
    builder.append("\n");
    String perHostPrefix =
        !useMTFragment ? "Per-Host Resources: " : "Per-Host Shared Resources: ";
    String perHostExplainString = null;
    String perInstanceExplainString = null;
    if (!useMTFragment) {
      // There is no point separating out per-host and per-instance resources when there
      // is only a single instance per host so combine them together.
      ResourceProfile perHostProfile = getTotalPerBackendResourceProfile(queryOptions);
      StringBuilder perHostBuilder = new StringBuilder(perHostProfile.getExplainString());
      long totalRuntimeFilterReservation = producedRuntimeFiltersMemReservationBytes_
          + consumedGlobalRuntimeFiltersMemReservationBytes_;
      if (perHostProfile.isValid() && totalRuntimeFilterReservation > 0) {
        perHostBuilder.append(" runtime-filters-memory=");
        perHostBuilder.append(PrintUtils.printBytes(totalRuntimeFilterReservation));
      }
      perHostExplainString = perHostBuilder.toString();
    } else {
      if (perBackendResourceProfile_.isValid()
          && perBackendResourceProfile_.isNonZero()) {
        StringBuilder perHostBuilder =
            new StringBuilder(perBackendResourceProfile_.getExplainString());
        if (consumedGlobalRuntimeFiltersMemReservationBytes_ > 0) {
          perHostBuilder.append(" runtime-filters-memory=");
          perHostBuilder.append(
              PrintUtils.printBytes(consumedGlobalRuntimeFiltersMemReservationBytes_));
        }
        perHostExplainString = perHostBuilder.toString();
      }
      if (perInstanceResourceProfile_.isValid()) {
        StringBuilder perInstanceBuilder =
            new StringBuilder(perInstanceResourceProfile_.getExplainString());
        if (producedRuntimeFiltersMemReservationBytes_ > 0) {
          perInstanceBuilder.append(" runtime-filters-memory=");
          perInstanceBuilder.append(
              PrintUtils.printBytes(producedRuntimeFiltersMemReservationBytes_));
        }
        perInstanceExplainString = perInstanceBuilder.toString();
      }
    }

    if (perHostExplainString != null) {
      builder.append(detailPrefix);
      builder.append(perHostPrefix);
      builder.append(perHostExplainString);
      builder.append("\n");
    }
    if (perInstanceExplainString != null) {
      builder.append(detailPrefix);
      builder.append("Per-Instance Resources: ");
      builder.append(perInstanceExplainString);
      builder.append("\n");
    }
    if (isComputeCost && rootSegment_ != null
        && explainLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      // Print processing cost.
      builder.append(detailPrefix);
      builder.append("max-parallelism=");
      builder.append(maxParallelism_);
      builder.append(" segment-costs=");
      builder.append(costingSegmentSummary());
      if (thisTreeCpuCore_ > 0 && subtreeCpuCore_ > 0) {
        builder.append(" cpu-comparison-result=");
        builder.append(Math.max(thisTreeCpuCore_, subtreeCpuCore_));
        builder.append(" [max(");
        builder.append(thisTreeCpuCore_);
        builder.append(" (self) vs ");
        builder.append(subtreeCpuCore_);
        builder.append(" (sum children))]");
      }
      builder.append("\n");
      if (explainLevel.ordinal() >= TExplainLevel.VERBOSE.ordinal()) {
        builder.append(explainProcessingCosts(detailPrefix, false));
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  /** Returns true if this fragment is partitioned. */
  public boolean isPartitioned() {
    return (dataPartition_.getType() != TPartitionType.UNPARTITIONED);
  }

  public PlanFragmentId getId() { return fragmentId_; }
  public PlanId getPlanId() { return planId_; }
  public void setPlanId(PlanId id) { planId_ = id; }
  public CohortId getCohortId() { return cohortId_; }
  public void setCohortId(CohortId id) { cohortId_ = id; }
  public PlanFragment getDestFragment() {
    if (destNode_ == null) return null;
    return destNode_.getFragment();
  }
  public PlanNode getDestNode() { return destNode_; }
  public DataPartition getDataPartition() { return dataPartition_; }
  public void setDataPartition(DataPartition dataPartition) {
    this.dataPartition_ = dataPartition;
  }
  public DataPartition getOutputPartition() { return outputPartition_; }
  public void setOutputPartition(DataPartition outputPartition) {
    this.outputPartition_ = outputPartition;
  }
  public PlanNode getPlanRoot() { return planRoot_; }
  public void setPlanRoot(PlanNode root) {
    planRoot_ = root;
    setFragmentInPlanTree(planRoot_);
  }
  protected void markDominant() { isDominantFragment_ = true; }

  /**
   * Set the destination node of this fragment's sink, i.e. an ExchangeNode or a JoinNode.
   */
  public void setDestination(PlanNode destNode) {
    destNode_ = destNode;
    PlanFragment dest = getDestFragment();
    Preconditions.checkNotNull(dest);
    dest.addChild(this);
  }

  public boolean hasSink() { return sink_ != null; }
  public DataSink getSink() { return sink_; }
  public void setSink(DataSink sink) {
    Preconditions.checkState(this.sink_ == null);
    Preconditions.checkNotNull(sink);
    sink.setFragment(this);
    this.sink_ = sink;
  }

  /**
   * Adds a node as the new root to the plan tree. Connects the existing
   * root as the child of newRoot.
   */
  public void addPlanRoot(PlanNode newRoot) {
    Preconditions.checkState(newRoot.getChildren().size() == 1);
    newRoot.setChild(0, planRoot_);
    planRoot_ = newRoot;
    planRoot_.setFragment(this);
  }

  /**
   * Return all fragments in the current plan (i.e. ancestors of this root reachable
   * via exchanges but not via join builds).
   * Only valid to call once all fragments have sinks created.
   */
  public List<PlanFragment> getFragmentsInPlanPreorder() {
    List<PlanFragment> result = new ArrayList<>();
    getFragmentsInPlanPreorderAux(result);
    return result;
  }

  /**
   * Helper for getFragmentsInPlanPreorder().
   */
  protected void getFragmentsInPlanPreorderAux(List<PlanFragment> result) {
    result.add(this);
    for (PlanFragment child: children_) {
      if (child.getSink() instanceof DataStreamSink) {
        child.getFragmentsInPlanPreorderAux(result);
      }
    }
  }

  /**
   * Verify that the tree of PlanFragments and their contained tree of
   * PlanNodes is constructed correctly.
   */
  public void verifyTree() {
    // PlanNode.fragment_ is set correctly
    List<PlanNode> nodes = collectPlanNodes();
    List<PlanNode> exchNodes = new ArrayList<>();
    for (PlanNode node: nodes) {
      if (node instanceof ExchangeNode) exchNodes.add(node);
      Preconditions.checkState(node.getFragment() == this);
    }

    // all ExchangeNodes have registered input fragments
    Preconditions.checkState(exchNodes.size() == getChildren().size());
    List<PlanFragment> childFragments = new ArrayList<>();
    for (PlanNode exchNode: exchNodes) {
      PlanFragment childFragment = exchNode.getChild(0).getFragment();
      Preconditions.checkState(!childFragments.contains(childFragment));
      childFragments.add(childFragment);
      Preconditions.checkState(childFragment.getDestNode() == exchNode);
    }
    // all registered children are accounted for
    Preconditions.checkState(getChildren().containsAll(childFragments));

    for (PlanFragment child: getChildren()) child.verifyTree();
  }

  /// Returns a seed value to use when hashing tuples for nodes within this fragment.
  /// Also see RuntimeState::fragment_hash_seed().
  public int getHashSeed() {
    // IMPALA-219: we should use different seeds for different fragment.
    // We add one to prevent having a hash seed of 0.
    return planRoot_.getId().asInt() + 1;
  }

  /**
   * Get maximum allowed parallelism based on minimum processing load per fragment.
   * <p>This is controlled by {@code min_processing_per_thread} flag. Only valid after
   * {@link #computeCostingSegment(TQueryOptions)} has been called.
   *
   * @return maximum allowed parallelism based on minimum processing load per fragment.
   */
  protected int getCostBasedMaxParallelism() {
    ProcessingCost maxCostingSegment = ProcessingCost.zero();
    List<CostingSegment> allSegments = rootSegment_.getNodesPostOrder();
    for (CostingSegment costingSegment : allSegments) {
      maxCostingSegment =
          ProcessingCost.maxCost(maxCostingSegment, costingSegment.getProcessingCost());
    }

    return maxCostingSegment.getNumInstanceMax(getNumNodes());
  }

  protected int getCappedCostBasedMaxParallelism(int minCap) {
    int costBasedMaxParallelism = Math.max(minCap, getCostBasedMaxParallelism());
    Preconditions.checkState(costBasedMaxParallelism > 0);
    return costBasedMaxParallelism;
  }

  /**
   * A fragment is either distributed evenly to all nodes or only at subset of nodes
   * (ie., there is only 1 fragment instances with PlanRootSink and MAX_FS_WRITER
   * fragment instances with HdfsTableSink). Increment by num nodes for the former case
   * and 1 for the latter case.
   */
  protected int getNodeStepCountForParallelismTraversal() {
    return getNumInstances() % getNumNodes() == 0 ? getNumNodes() : 1;
  }

  protected boolean hasBlockingNode() {
    if (sink_ instanceof JoinBuildSink) return true;
    for (PlanNode p : collectPlanNodes()) {
      if (isBlockingNode(p)) return true;
    }
    return false;
  }

  protected boolean hasAdjustedInstanceCount() { return adjustedInstanceCount_ > 0; }

  protected void setFixedInstanceCount(int count) {
    isFixedParallelism_ = true;
    setAdjustedInstanceCount(count);
  }

  private void setAdjustedInstanceCount(int count) {
    Preconditions.checkState(count > 0,
        getId() + " adjusted instance count (" + count + ") is not positive number.");
    boolean isFirstAdjustment = adjustedInstanceCount_ <= 0;
    adjustedInstanceCount_ = count;
    if (rootSegment_ != null) {
      List<CostingSegment> costingSegments = rootSegment_.getNodesPostOrder();
      for (CostingSegment costingSegment : costingSegments) {
        // Reset for each segment cost since it might be overriden during
        // tryLowerParallelism().
        costingSegment.getProcessingCost().setNumInstanceExpected(
            this::getAdjustedInstanceCount);
      }
    }

    if (isFirstAdjustment) {
      // Set num instance expected for ProcessingCost attached to PlanNodes and DataSink.
      for (PlanNode node : collectPlanNodes()) {
        node.getProcessingCost().setNumInstanceExpected(this::getAdjustedInstanceCount);
      }
      sink_.getProcessingCost().setNumInstanceExpected(this::getAdjustedInstanceCount);
    }
  }

  /**
   * Only valid to call after calling {@link #traverseEffectiveParallelism(int, int,
   * PlanFragment)}.
   */
  protected int getAdjustedInstanceCount() {
    Preconditions.checkState(adjustedInstanceCount_ > -1);
    return adjustedInstanceCount_;
  }

  /**
   * Only valid to call after calling {@link #traverseEffectiveParallelism(int, int,
   * PlanFragment)}.
   */
  protected int getMaxParallelism() {
    Preconditions.checkState(maxParallelism_ > -1);
    return maxParallelism_;
  }

  protected ProcessingCost getLastCostingSegment() {
    return rootSegment_.getProcessingCost();
  }

  private List<Long> costingSegmentSummary() {
    return rootSegment_.getNodesPostOrder()
        .stream()
        .map(s -> ((CostingSegment) s).getProcessingCost().getTotalCost())
        .collect(Collectors.toList());
  }

  private String explainProcessingCosts(String linePrefix, boolean fullExplain) {
    return rootSegment_.getNodesPreOrder()
        .stream()
        .map(s
            -> ((CostingSegment) s)
                   .getProcessingCost()
                   .getExplainString(linePrefix, fullExplain))
        .collect(Collectors.joining("\n"));
  }

  private String debugProcessingCosts() { return explainProcessingCosts("", true); }

  /**
   * Validates that properties related to processing cost of this fragment are complete
   * and valid.
   */
  private void validateProcessingCosts() {
    Preconditions.checkState(hasAdjustedInstanceCount());
    Preconditions.checkNotNull(rootSegment_);
    List<CostingSegment> costingSegments = rootSegment_.getNodesPreOrder();
    for (CostingSegment costingSegment : costingSegments) {
      ProcessingCost cost = costingSegment.getProcessingCost();
      Preconditions.checkState(cost.isValid(), "Segment cost is invalid! %s", cost);
      Preconditions.checkState(
          cost.getNumInstancesExpected() == getAdjustedInstanceCount());
    }
  }

  /**
   * Traverse down the query tree starting from this fragment and calculate the effective
   * parallelism of each PlanFragments.
   *
   * @param minThreadPerNode Minimum thread per fragment per node based on
   *                         {@code PROCESSING_COST_MIN_THREADS} query option.
   * @param maxThreadPerNode Maximum thread per fragment per node based on
   *                         {@code max(PROCESSING_COST_MIN_THREADS,
   *                         TExecutorGroupSet.num_cores_per_executor)}.
   * @param parentFragment parent fragment of this fragment.
   */
  protected void traverseEffectiveParallelism(final int minThreadPerNode,
      final int maxThreadPerNode, final @Nullable PlanFragment parentFragment,
      TQueryOptions queryOptions) {
    Preconditions.checkNotNull(
        rootSegment_, "ProcessingCost Fragment %s has not been computed!", getId());
    int nodeStepCount = getNodeStepCountForParallelismTraversal();

    // step 1: Set initial parallelism to the maximum possible.
    //   Subsequent steps after this will not exceed maximum parallelism sets here.
    boolean canTryLower = adjustToMaxParallelism(
        minThreadPerNode, maxThreadPerNode, parentFragment, nodeStepCount, queryOptions);

    if (canTryLower) {
      // step 2: Try lower parallelism by comparing output ProcessingCost of the input
      //   child fragment against this fragment's segment costs.
      Preconditions.checkState(getChildCount() > 0);
      Preconditions.checkState(getChild(0).getSink() instanceof DataStreamSink);

      // Check if this fragment parallelism can be lowered.
      int maxParallelism = getAdjustedInstanceCount();
      int minParallelism = IntMath.saturatedMultiply(minThreadPerNode, getNumNodes());
      int effectiveParallelism = rootSegment_.tryAdjustParallelism(
          nodeStepCount, minParallelism, maxParallelism);
      setAdjustedInstanceCount(effectiveParallelism);
      if (LOG.isTraceEnabled() && effectiveParallelism != maxParallelism) {
        logCountAdjustmentTrace(maxParallelism, effectiveParallelism,
            "Lower parallelism based on load and produce-consume rate ratio.");
      }
    }
    validateProcessingCosts();

    // step 3: Compute the parallelism of join build fragment.
    //   Child parallelism may be enforced to follow this fragment's parallelism.
    // TODO: This code assume that probe side of the join always have higher per-instance
    //   cost than the build side. If this assumption is false and the child is a
    //   non-shared join build fragment, then this fragment should increase its
    //   parallelism to match the child fragment parallelism.
    for (PlanFragment child : getChildren()) {
      if (child.getSink() instanceof JoinBuildSink) {
        child.traverseEffectiveParallelism(
            minThreadPerNode, maxThreadPerNode, this, queryOptions);
      }
    }
  }

  protected boolean isPartitionedJoinBuildFragment() {
    return (sink_ instanceof JoinBuildSink) && !((JoinBuildSink) sink_).isShared();
  }

  protected int getMaxParallelismForUnionFragment(
      UnionNode node, boolean findUnboundedCount, @Nullable TQueryOptions queryOptions) {
    Preconditions.checkState(this == node.getFragment());
    // We set parallelism of union fragment as a max between its input fragments and
    // its collocated ScanNode's expected parallelism
    // (see Scheduler::CreateCollocatedAndScanInstances()).
    // We skip any join builder child fragment here because their parallelism
    // is not adjusted yet.
    int nodeStepCount = getNodeStepCountForParallelismTraversal();
    int costBasedMaxParallelism = getCappedCostBasedMaxParallelism(nodeStepCount);
    int maxParallelism = 1;
    for (PlanFragment child : getChildren()) {
      if (child.getSink() instanceof JoinBuildSink) continue;
      Preconditions.checkState(child.hasAdjustedInstanceCount());
      maxParallelism = Math.max(maxParallelism,
          (findUnboundedCount ? child.getMaxParallelism() :
                                child.getAdjustedInstanceCount()));
    }

    List<ScanNode> scanNodes = Lists.newArrayList();
    collectPlanNodes(Predicates.instanceOf(ScanNode.class), scanNodes);
    int maxScannerThreads = 0;
    if (!scanNodes.isEmpty()) {
      // The existence of scan node may justify an increase of parallelism for this
      // union fargment, but it should be capped at costBasedMaxParallelism.
      maxScannerThreads = ScanNode.MIN_NUM_SCAN_THREADS;
      for (ScanNode scanNode : scanNodes) {
        // if findUnboundedCount is True, use estScanRangeAfterRuntimeFilter()
        // because computeMaxScannerThreadsForCPC() is capped by
        //   maxThreadsGlobal = getNumNodes() * maxThreadsPerNode.
        // while estScanRangeAfterRuntimeFilter() is not.
        int thisScannerThreads = ScanNode.MIN_NUM_SCAN_THREADS;
        if (findUnboundedCount) {
          thisScannerThreads = scanNode.estScanRangeAfterRuntimeFilter();
        } else {
          Preconditions.checkNotNull(queryOptions);
          thisScannerThreads = scanNode.computeMaxScannerThreadsForCPC(queryOptions);
        }
        maxScannerThreads = Math.max(maxScannerThreads, thisScannerThreads);
      }
      maxParallelism =
          Math.max(maxParallelism, Math.min(costBasedMaxParallelism, maxScannerThreads));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Set maxParallelism to: " + maxParallelism + ", costBasedMaxParallelism: "
          + costBasedMaxParallelism + ", maxScannerThreads: " + maxScannerThreads
          + ", findUnboundedCount: " + findUnboundedCount);
    }
    return maxParallelism;
  }

  /**
   * Adjust parallelism of this fragment to the maximum allowed.
   * This method initialize maxParallelism_ and adjustedInstanceCount_.
   *
   * @param minThreadPerNode Minimum thread per fragment per node based on
   *                         {@code PROCESSING_COST_MIN_THREADS} query option.
   * @param maxThreadPerNode Maximum thread per fragment per node based on
   *                         {@code max(PROCESSING_COST_MIN_THREADS,
   *                         TExecutorGroupSet.num_cores_per_executor)}.
   * @param parentFragment Parent fragment of this fragment.
   * @param nodeStepCount The step count used to increase this fragment's parallelism.
   *                      Usually equal to number of nodes or just 1.
   * @return True if it is possible to lower this fragment's parallelism through
   * ProcessingCost comparison. False if the parallelism should not be changed anymore.
   */
  private boolean adjustToMaxParallelism(final int minThreadPerNode,
      final int maxThreadPerNode, final @Nullable PlanFragment parentFragment,
      final int nodeStepCount, TQueryOptions queryOptions) {
    int maxThreadAllowed = IntMath.saturatedMultiply(maxThreadPerNode, getNumNodes());
    boolean canTryLower = true;

    // Compute selectedParallelism as the maximum allowed parallelism.
    int selectedParallelism = getNumInstances();
    if (isFixedParallelism_) {
      selectedParallelism = getAdjustedInstanceCount();
      maxParallelism_ = selectedParallelism;
      canTryLower = false;
    } else if (isPartitionedJoinBuildFragment()) {
      // This is a non-shared (PARTITIONED) join build fragment.
      // Parallelism of this fragment is equal to its parent parallelism.
      Preconditions.checkNotNull(parentFragment);
      final int parentParallelism = parentFragment.getAdjustedInstanceCount();
      if (LOG.isTraceEnabled() && selectedParallelism != parentParallelism) {
        logCountAdjustmentTrace(selectedParallelism, parentParallelism,
            "Partitioned join build fragment follow parent's parallelism.");
      }
      selectedParallelism = parentParallelism;
      maxParallelism_ = parentFragment.getMaxParallelism();
      canTryLower = false; // no need to compute effective parallelism anymore.
    } else {
      UnionNode unionNode = getUnionNode();

      if (unionNode != null) {
        // Special handling for Union fragment.
        maxParallelism_ =
            getMaxParallelismForUnionFragment(unionNode, false, queryOptions);

        if (maxParallelism_ > maxThreadAllowed) {
          selectedParallelism = maxThreadAllowed;
          if (LOG.isTraceEnabled()) {
            logCountAdjustmentTrace(
                getNumInstances(), selectedParallelism, "Follow maxThreadPerNode.");
          }
        } else {
          selectedParallelism = maxParallelism_;
          if (LOG.isTraceEnabled()) {
            logCountAdjustmentTrace(getNumInstances(), selectedParallelism,
                "Follow minimum work per thread or max child count.");
          }
        }
        canTryLower = false;
      } else {
        // This is an interior fragment or fragment with single scan node.
        // We calculate maxParallelism_, minParallelism, and selectedParallelism across
        // all executors in the selected executor group.
        int maxScannerThreads = Integer.MAX_VALUE;
        int costBasedMaxParallelism = getCappedCostBasedMaxParallelism(nodeStepCount);

        // track maximum parallelism unbounded by minThreadPerNode and maxThreadPerNode.
        maxParallelism_ = costBasedMaxParallelism;
        // track maximum parallelism bounded by minThreadPerNode and maxThreadPerNode.
        int boundedParallelism = costBasedMaxParallelism;

        // Bound maxParallelism_ by ScanNode's effective scan range count if
        // this fragment has ScanNode.
        List<ScanNode> scanNodes = Lists.newArrayList();
        collectPlanNodes(Predicates.instanceOf(ScanNode.class), scanNodes);
        if (!scanNodes.isEmpty()) {
          Preconditions.checkState(scanNodes.size() == 1);
          maxScannerThreads =
              scanNodes.get(0).computeMaxScannerThreadsForCPC(queryOptions);
          maxParallelism_ = Math.max(ScanNode.MIN_NUM_SCAN_THREADS,
              Math.min(
                  maxParallelism_, scanNodes.get(0).estScanRangeAfterRuntimeFilter()));
          boundedParallelism = Math.max(ScanNode.MIN_NUM_SCAN_THREADS,
              Math.min(boundedParallelism, maxScannerThreads));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Set maxParallelism to: " + maxParallelism_
                + ", costBasedMaxParallelism: " + costBasedMaxParallelism
                + ", maxScannerThreads: " + maxScannerThreads);
          }

          // Prevent caller from lowering parallelism if fragment has ScanNode
          // because there is no child fragment to compare with.
          canTryLower = false;
        }

        int minParallelism = Math.min(
            maxThreadAllowed, IntMath.saturatedMultiply(minThreadPerNode, getNumNodes()));
        LOG.info("maxParallelism_=" + maxParallelism_ + " boundedParallelism="
            + boundedParallelism + " minParallelism=" + minParallelism);

        if (boundedParallelism > maxThreadAllowed) {
          selectedParallelism = maxThreadAllowed;
          if (LOG.isTraceEnabled()) {
            logCountAdjustmentTrace(
                getNumInstances(), selectedParallelism, "Follow maxThreadPerNode.");
          }
        } else {
          if (boundedParallelism < minParallelism && minParallelism < maxScannerThreads) {
            boundedParallelism = minParallelism;
            canTryLower = false;
            if (LOG.isTraceEnabled()) {
              logCountAdjustmentTrace(
                  getNumInstances(), boundedParallelism, "Follow minThreadPerNode.");
            }
          } else if (LOG.isTraceEnabled()) {
            logCountAdjustmentTrace(
                getNumInstances(), boundedParallelism, "Follow minimum work per thread.");
          }
          selectedParallelism = boundedParallelism;
        }
      }
    }

    // Validate that selectedParallelism does not exceed maxThreadAllowed.
    // selectedParallelism can be lower than minThreadPerNode, ie., in the case of plan
    // root sink (only 1 per query) or scan with very few scan ranges, so this does not
    // validate against minThreadPerNode.
    Preconditions.checkState(selectedParallelism <= maxThreadAllowed);

    // Initialize this fragment's parallelism to the selectedParallelism.
    setAdjustedInstanceCount(selectedParallelism);
    return canTryLower && selectedParallelism > 1;
  }

  /**
   * Get the top-most UnionNode of this fragment.
   * If there is none, return null.
   */
  protected @Nullable UnionNode getUnionNode() {
    List<PlanNode> nodes = Lists.newArrayList();
    collectPlanNodes(Predicates.instanceOf(UnionNode.class), nodes);
    return nodes.isEmpty() ? null : ((UnionNode) nodes.get(0));
  }

  /**
   * Compute {@link CoreCount} of this fragment and populate it into 'fragmentCoreState'.
   * @param fragmentCoreState A map holding per-fragment core state.
   * @param findUnboundedCount if True, return the unbounded core count.
   *                           Otherwise, return the bounded one.
   * All successor of this fragment must already have its CoreCount registered into this
   * map.
   */
  protected void computeBlockingAwareCores(
      Map<PlanFragmentId, Pair<CoreCount, List<CoreCount>>> fragmentCoreState,
      boolean findUnboundedCount) {
    Preconditions.checkNotNull(
        rootSegment_, "ProcessingCost Fragment %s has not been computed!", getId());
    ImmutableList.Builder<CoreCount> subtreeCoreBuilder =
        new ImmutableList.Builder<CoreCount>();
    CoreCount coreReq = rootSegment_.traverseBlockingAwareCores(
        fragmentCoreState, subtreeCoreBuilder, findUnboundedCount);
    fragmentCoreState.put(getId(), Pair.create(coreReq, subtreeCoreBuilder.build()));
  }

  protected CoreCount maxCore(
      CoreCount thisTreeCpuCore, CoreCount subtreeCpuCore, boolean findUnboundedCount) {
    if (!findUnboundedCount) {
      // Only initialize thisTreeCpuCore_ and subtreeCpuCore_ when doing bounded counting.
      // This will be displayed in query profile to show how the maximum selection goes,
      // such as:
      //   cpu-comparison-result=220 [max(120 (self) vs 220 (sum children))]
      // "self" is 'thisTreeCpuCore_' while "sum children" is 'subtreeCpuCore_'.
      thisTreeCpuCore_ = thisTreeCpuCore.total();
      subtreeCpuCore_ = subtreeCpuCore.total();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("At {}, do {} comparison of {} ({}) vs {} ({})", getId(), thisTreeCpuCore,
          (findUnboundedCount ? "unbounded" : "bounded"), thisTreeCpuCore.total(),
          subtreeCpuCore, subtreeCpuCore.total());
    }
    return CoreCount.max(thisTreeCpuCore, subtreeCpuCore);
  }

  /**
   * Override parallelism of this fragment with adjusted parallelism from CPU costing
   * algorithm.
   * <p>Only valid after {@link #traverseEffectiveParallelism(int, int, int)}
   * called.
   */
  protected void setEffectiveNumInstance() {
    validateProcessingCosts();
    if (originalInstanceCount_ <= 0) {
      originalInstanceCount_ = getNumInstances();
    }

    int adjustedCount = getAdjustedInstanceCount();
    if (LOG.isTraceEnabled() && originalInstanceCount_ != adjustedCount) {
      logCountAdjustmentTrace(
          originalInstanceCount_, adjustedCount, "Finalize effective parallelism.");
    }

    for (PlanNode node : collectPlanNodes()) { node.numInstances_ = adjustedCount; }

    if (LOG.isTraceEnabled()) {
      LOG.trace("ProcessingCost Fragment {}:\n{}", getId(), debugProcessingCosts());
    }
  }

  private void logCountAdjustmentTrace(int oldCount, int newCount, String reason) {
    LOG.trace("{} adjust instance count from {} to {}. {}", getId(), oldCount, newCount,
        reason);
  }

  private static boolean isBlockingNode(PlanNode node) {
    // Preaggregation node can behave like final aggregation node when it does not
    // passedthrough any row. From CPU costing perspective, treat both final aggregation
    // and preaggregation as a blocking node. Otherwise, follow PlanNode.isBlockingNode().
    return node.isBlockingNode() || node instanceof AggregationNode;
  }
}
