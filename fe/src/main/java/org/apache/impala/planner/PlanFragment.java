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
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

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
  private final PlanFragmentId fragmentId_;
  private PlanId planId_;
  private CohortId cohortId_;

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

  public long getProducedRuntimeFiltersMemReservationBytes() {
    return producedRuntimeFiltersMemReservationBytes_;
  }

  /**
   * C'tor for fragment with specific partition; the output is by default broadcast.
   */
  public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition) {
    fragmentId_ = id;
    planRoot_ = root;
    dataPartition_ = partition;
    outputPartition_ = DataPartition.UNPARTITIONED;
    setFragmentInPlanTree(planRoot_);
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
        analyzer.castToSetOpCompatibleTypes(senderPartitionExprs);
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
    }
    for (RuntimeFilter f : consumedFilters.values()) {
      if (!producedFilters.containsKey(f.getFilterId())) {
        consumedGlobalRuntimeFiltersMemReservationBytes_ += f.getFilterSize();
      }
    }
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
  public ResourceProfile getTotalPerBackendResourceProfile(int mtDop) {
    return perInstanceResourceProfile_.multiply(getNumInstancesPerHost(mtDop))
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
  public int getNumInstancesPerHost(int mt_dop) {
    // Assume that instances are evenly divided across hosts.
    int numNodes = getNumNodes();
    int numInstances = getNumInstances();
    // Fall back to assuming that all mt_dop instances will be generated.
    if (numNodes == -1 || numInstances == -1) return Math.max(1, mt_dop);
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
      return planRoot_.getNumInstances();
    }
  }

  /**
    * data partition of this fragment, the number of nodes, and the degree of parallelism.
    * Returns -1 for an invalid estimate, e.g., because getNumDistinctValues() failed on
    * one of the exprs.
    */
  public long getPerInstanceNdv(int mt_dop, List<Expr> exprs) {
    Preconditions.checkNotNull(dataPartition_);
    long result = 1;
    int numInstances = getNumInstances();
    Preconditions.checkState(numInstances >= 0);
    // The number of nodes is zero for empty tables.
    if (numInstances == 0) return 0;
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
    }
    if (partition) {
      result = (long)Math.max((double) result / (double) numInstances, 1L);
    }
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
      str.append(getFragmentHeaderString("", "", queryOptions.getMt_dop()));
      if (sink_ != null && sink_ instanceof DataStreamSink) {
        str.append(
            sink_.getExplainString(rootPrefix, detailPrefix, queryOptions, detailLevel));
      }
    } else if (detailLevel == TExplainLevel.EXTENDED) {
      // Print a fragment prefix displaying the # nodes and # instances
      str.append(
          getFragmentHeaderString(rootPrefix, detailPrefix, queryOptions.getMt_dop()));
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
      int mt_dop) {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("%s%s:PLAN FRAGMENT [%s]", firstLinePrefix,
        fragmentId_.toString(), dataPartition_.getExplainString()));
    builder.append(PrintUtils.printNumHosts(" ", getNumNodes()));
    builder.append(PrintUtils.printNumInstances(" ", getNumInstances()));
    builder.append("\n");
    String perHostPrefix = mt_dop == 0 ?
        "Per-Host Resources: " : "Per-Host Shared Resources: ";
    String perHostExplainString = null;
    String perInstanceExplainString = null;
    if (mt_dop == 0) {
      // There is no point separating out per-host and per-instance resources when there
      // is only a single instance per host so combine them together.
      ResourceProfile perHostProfile = getTotalPerBackendResourceProfile(mt_dop);
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
}
