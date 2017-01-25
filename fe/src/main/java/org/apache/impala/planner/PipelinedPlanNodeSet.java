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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.thrift.TQueryOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Represents a set of PlanNodes and DataSinks that execute and consume resources
 * concurrently. PlanNodes and DataSinks in such a pipelined plan node set may belong
 * to different plan fragments because data is streamed across fragments.
 *
 * For example, a series of left-deep joins consists of two plan node sets. The first
 * set contains all build-side nodes. The second set contains the leftmost
 * scan. Both sets contain all join nodes because they execute and consume
 * resources during the build and probe phases. Similarly, all nodes below a 'blocking'
 * node (e.g, an AggregationNode) are placed into a different plan node set than the
 * nodes above it, but the blocking node itself belongs to both sets.
 */
public class PipelinedPlanNodeSet {
  private final static Logger LOG = LoggerFactory.getLogger(PipelinedPlanNodeSet.class);

  // Minimum per-host resource requirements to ensure that no plan node set can have
  // estimates of zero, even if the contained PlanNodes have estimates of zero.
  public static final long MIN_PER_HOST_MEM_ESTIMATE_BYTES = 10 * 1024 * 1024;

  // List of plan nodes that execute and consume resources concurrently.
  private final ArrayList<PlanNode> planNodes_ = Lists.newArrayList();

  // DataSinks that execute and consume resources concurrently.
  // Primarily used for estimating the cost of insert queries.
  private final List<DataSink> dataSinks_ = Lists.newArrayList();

  private void addNode(PlanNode node) {
    Preconditions.checkNotNull(node.getFragment());
    planNodes_.add(node);
  }

  private void addSink(DataSink sink) {
    Preconditions.checkNotNull(sink);
    dataSinks_.add(sink);
  }

  /**
   * Computes the per-host resource profile of this plan node set.
   *
   * If there are no nodes included in the estimate, the returned estimate will not be
   * valid.
   */
  public ResourceProfile computePerHostResources(TQueryOptions queryOptions) {
    Set<PlanFragment> uniqueFragments = Sets.newHashSet();

    // Distinguish the per-host memory estimates for scan nodes and non-scan nodes to
    // get a tighter estimate on the amount of memory required by multiple concurrent
    // scans. The memory required by all concurrent scans of the same type (Hdfs/Hbase)
    // cannot exceed the per-host upper memory bound for that scan type. Intuitively,
    // the amount of I/O buffers is limited by the disk bandwidth.
    long hbaseScanMemEstimate = 0L;
    long hdfsScanMemEstimate = 0L;
    long nonScanMemEstimate = 0L;
    long minReservationBytes = 0L;
    int numNodesIncluded = 0;

    for (PlanNode node : planNodes_) {
      PlanFragment fragment = node.getFragment();
      // Multiple instances of a partitioned fragment may execute per host
      int instancesPerHost = fragment.getNumInstancesPerHost(queryOptions.getMt_dop());

      ResourceProfile nodeProfile = node.getResourceProfile();
      Preconditions.checkState(nodeProfile.getMemEstimateBytes() >= 0);
      long memEstimate = instancesPerHost * nodeProfile.getMemEstimateBytes();
      ++numNodesIncluded;
      uniqueFragments.add(fragment);
      if (node instanceof HBaseScanNode) {
        hbaseScanMemEstimate += memEstimate;
      } else if (node instanceof HdfsScanNode) {
        hdfsScanMemEstimate += memEstimate;
      } else {
        nonScanMemEstimate += memEstimate;
      }
      Preconditions.checkState(nodeProfile.getMinReservationBytes() >= 0);
      minReservationBytes += instancesPerHost * nodeProfile.getMinReservationBytes();
    }

    if (queryOptions.getMt_dop() == 0) {
      // The thread tokens for the non-MT path impose a limit on the memory that can
      // be consumed by concurrent scans.
      hbaseScanMemEstimate =
          Math.min(hbaseScanMemEstimate, HBaseScanNode.getPerHostMemUpperBound());
      hdfsScanMemEstimate =
          Math.min(hdfsScanMemEstimate, HdfsScanNode.getPerHostMemUpperBound());
    }

    long dataSinkMemEstimate = 0L;
    for (DataSink sink: dataSinks_) {
      PlanFragment fragment = sink.getFragment();
      // Sanity check that this plan-node set has at least one PlanNode of fragment.
      Preconditions.checkState(uniqueFragments.contains(fragment));
      int instancesPerHost = fragment.getNumInstancesPerHost(queryOptions.getMt_dop());

      ResourceProfile sinkProfile = sink.getResourceProfile();
      Preconditions.checkState(sinkProfile.getMemEstimateBytes() >= 0);
      dataSinkMemEstimate += instancesPerHost * sinkProfile.getMemEstimateBytes();
      Preconditions.checkState(sinkProfile.getMinReservationBytes() >= 0);
      minReservationBytes += instancesPerHost * sinkProfile.getMinReservationBytes();
    }

    // Combine the memory estimates of all sinks, scans nodes and non-scan nodes.
    long perHostMemEstimate =
        Math.max(MIN_PER_HOST_MEM_ESTIMATE_BYTES, hdfsScanMemEstimate
                + hbaseScanMemEstimate + nonScanMemEstimate + dataSinkMemEstimate);
    // This plan node set might only have unpartitioned fragments and be invalid.
    return numNodesIncluded > 0 ?
        new ResourceProfile(perHostMemEstimate, minReservationBytes) :
          ResourceProfile.invalid();
  }

  /**
   * Computes and returns the pipelined plan node sets of the given plan.
   */
  public static ArrayList<PipelinedPlanNodeSet> computePlanNodeSets(PlanNode root) {
    ArrayList<PipelinedPlanNodeSet> planNodeSets =
        Lists.newArrayList(new PipelinedPlanNodeSet());
    computePlanNodeSets(root, planNodeSets.get(0), null, planNodeSets);
    return planNodeSets;
  }

  /**
   * Populates 'planNodeSets' by recursively traversing the plan tree rooted at 'node'
   * The plan node sets are computed top-down. As a result, the plan node sets are added
   * in reverse order of their runtime execution.
   *
   * Nodes are generally added to lhsSet. Joins are treated specially in that their
   * left child is added to lhsSet and their right child to rhsSet to make sure
   * that concurrent join builds end up in the same plan node set.
   */
  private static void computePlanNodeSets(PlanNode node, PipelinedPlanNodeSet lhsSet,
      PipelinedPlanNodeSet rhsSet, ArrayList<PipelinedPlanNodeSet> planNodeSets) {
    lhsSet.addNode(node);
    if (node == node.getFragment().getPlanRoot() && node.getFragment().hasSink()) {
      lhsSet.addSink(node.getFragment().getSink());
    }

    if (node instanceof JoinNode && ((JoinNode)node).isBlockingJoinNode()) {
      // Create a new set for the right-hand sides of joins if necessary.
      if (rhsSet == null) {
        rhsSet = new PipelinedPlanNodeSet();
        planNodeSets.add(rhsSet);
      }
      // The join node itself is added to the lhsSet (above) and the rhsSet.
      rhsSet.addNode(node);
      computePlanNodeSets(node.getChild(1), rhsSet, null, planNodeSets);
      computePlanNodeSets(node.getChild(0), lhsSet, rhsSet, planNodeSets);
      return;
    }

    if (node.isBlockingNode()) {
      // We add blocking nodes to two plan node sets because they require resources while
      // consuming their input (execution of the preceding set) and while they
      // emit their output (execution of the following set).
      // TODO: IMPALA-4862: this logic does not accurately reflect the behaviour of
      // concurrent join builds in the backend
      lhsSet = new PipelinedPlanNodeSet();
      lhsSet.addNode(node);
      planNodeSets.add(lhsSet);
      // Join builds under this blocking node belong in a new rhsSet.
      rhsSet = null;
    }

    // Assume that non-join, non-blocking nodes with multiple children
    // (e.g., ExchangeNodes) consume their inputs in an arbitrary order,
    // i.e., all child subtrees execute concurrently.
    // TODO: IMPALA-4862: can overestimate resource consumption of UnionNodes - the
    // execution of union branches is serialised within a fragment (but not across
    // fragment boundaries).
    for (PlanNode child: node.getChildren()) {
      computePlanNodeSets(child, lhsSet, rhsSet, planNodeSets);
    }
  }
}
