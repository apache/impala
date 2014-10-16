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
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.thrift.TQueryOptions;
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
 * node (e.g, an AggregationNode) are placed into a differnet plan node set than the
 * nodes above it, but the blocking node itself belongs to both sets.
 */
public class PipelinedPlanNodeSet {
  private final static Logger LOG = LoggerFactory.getLogger(PipelinedPlanNodeSet.class);

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
        LOG.warn(String.format("Invalid per-host memory requirement %s of sink %s.\n",
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
    if (perHostMem >= 0 && perHostVcores >= 0) {
      this.perHostMem = perHostMem;
      this.perHostVcores = perHostVcores;
      return true;
    }
    return false;
  }

  public long getPerHostMem() { return perHostMem; }
  public int getPerHostVcores() { return perHostVcores; }

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
      computePlanNodeSets(node.getChild(1), rhsSet, null, planNodeSets);
      computePlanNodeSets(node.getChild(0), lhsSet, rhsSet, planNodeSets);
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

    // Assume that non-join, non-blocking nodes with multiple children
    // (e.g., ExchangeNodes) consume their inputs in an arbitrary order,
    // i.e., all child subtrees execute concurrently.
    // TODO: This is not true for UnionNodes anymore. Fix the estimates accordingly.
    for (PlanNode child: node.getChildren()) {
      computePlanNodeSets(child, lhsSet, rhsSet, planNodeSets);
    }
  }
}
