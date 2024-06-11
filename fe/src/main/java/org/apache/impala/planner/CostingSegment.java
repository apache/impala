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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.impala.common.Id;
import org.apache.impala.common.Pair;
import org.apache.impala.common.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A grouping of adjacent PlanNode and DataSink of a fragment for CPU costing purpose.
 * <p>
 * Each fragment segment is a subtree of PlanNodes/DataSink in the fragment with a
 * DataSink or blocking PlanNode as root. All other nodes in the segment are non-blocking.
 * In most cases, every segment is a blocking segment, that is the root is either
 * a blocking operator or blocking DataSink (ie., JoinBuildSink). A segment is not a
 * blocking segment if its root is a non-blocking DataSink (ie., DataStreamSink).
 * <p>
 * PlanNodes or DataSink that belong to the same segment will have their ProcessingCost
 * summed. Analyses done through this class might recurse around the CostingSegment tree,
 * but should not traverse into different fragment.
 */
public class CostingSegment extends TreeNode<CostingSegment> {
  private final static Logger LOG = LoggerFactory.getLogger(CostingSegment.class);

  // List of PlanNode belong to this segment.
  private List<PlanNode> nodes_ = Lists.newArrayList();

  // The ProcessingCost of this fragment segment, which is the sum of the processing cost
  // of all nodes in nodes_ and sink_ (if set).
  // Initialized at constructor.
  private ProcessingCost cost_;

  // DataSink associated with this segment.
  // Must not be null for output segment.
  private DataSink sink_ = null;

  public CostingSegment(DataSink sink) {
    Preconditions.checkArgument(sink.getProcessingCost().isValid());
    cost_ = sink.getProcessingCost();
    sink_ = sink;
  }

  public CostingSegment(PlanNode node) {
    Preconditions.checkArgument(node.getProcessingCost().isValid());
    cost_ = node.getProcessingCost();
    nodes_.add(node);
  }

  private CostingSegment() {
    cost_ = ProcessingCost.zero();
  }

  public ProcessingCost getProcessingCost() { return cost_; }
  public boolean isOutputSegment() { return sink_ != null; }

  private Id getRootId() {
    if (isOutputSegment()) {
      return sink_.getFragment().getId();
    } else {
      Preconditions.checkState(!nodes_.isEmpty());
      return nodes_.get(nodes_.size() - 1).getId();
    }
  }

  private CoreCount createCoreCount(boolean isUnbounded) {
    PlanFragment fragment;
    PlanNode topNode = null;
    if (isOutputSegment()) {
      fragment = sink_.getFragment();
    } else {
      Preconditions.checkState(!nodes_.isEmpty());
      topNode = nodes_.get(nodes_.size() - 1);
      fragment = topNode.getFragment();
    }

    int maxParallelism = cost_.getNumInstancesExpected();
    if (isUnbounded) {
      maxParallelism = fragment.getMaxParallelism();
      UnionNode unionNode = fragment.getUnionNode();
      if (unionNode != null) {
        // Union fragment need recalculation because its max parallelism is bounded by
        // its children's adjusted count.
        maxParallelism =
            fragment.getMaxParallelismForUnionFragment(unionNode, true, null);
      }
    }

    if (topNode == null) {
      return new CoreCount(sink_.getFragment(), maxParallelism);
    } else {
      Preconditions.checkNotNull(topNode);
      return new CoreCount(topNode, maxParallelism);
    }
  }

  private void appendCost(ProcessingCost additionalCost) {
    Preconditions.checkArgument(
        additionalCost.isValid(), "additionalCost is invalid! %s", additionalCost);
    ProcessingCost newTotalCost = ProcessingCost.sumCost(additionalCost, cost_);
    newTotalCost.setNumRowToConsume(cost_.getNumRowToConsume());
    newTotalCost.setNumRowToProduce(additionalCost.getNumRowToConsume());
    cost_ = newTotalCost;
  }

  protected void setSink(DataSink sink) {
    appendCost(sink.getProcessingCost());
    sink_ = sink;
  }

  protected void appendNode(PlanNode node) {
    appendCost(node.getProcessingCost());
    nodes_.add(node);
  }

  /**
   * Gather {@link CoreCount} rooted from this segment and populate
   * subtreeCoreBuilder with {@link CoreCount} of child-blocking-subtree.
   * @param fragmentCoreState A map holding per-fragment core state.
   * @param subtreeCoreBuilder An ImmutableList builder to populate.
   * @param findUnboundedCount if True, return the unbounded core count.
   *                           Otherwise, return the bounded one.
   * @return A {@link CoreCount} value of segment tree
   * rooted at this segment.
   */
  protected CoreCount traverseBlockingAwareCores(
      Map<PlanFragmentId, Pair<CoreCount, List<CoreCount>>> fragmentCoreState,
      ImmutableList.Builder<CoreCount> subtreeCoreBuilder, boolean findUnboundedCount) {
    CoreCount segmentCore = createCoreCount(findUnboundedCount);
    // If not in input segment, gather cost of children first.
    for (CostingSegment childSegment : getChildren()) {
      CoreCount childSegmentCores = childSegment.traverseBlockingAwareCores(
          fragmentCoreState, subtreeCoreBuilder, findUnboundedCount);
      if (childSegmentCores.total() > 0) {
        segmentCore = CoreCount.max(segmentCore, childSegmentCores);
      }
    }

    // Look up child fragment that is connected through this segment.
    for (PlanNode node : nodes_) {
      for (int i = 0; i < node.getChildCount(); i++) {
        PlanFragment childFragment = node.getChild(i).getFragment();
        if (childFragment == node.getFragment()) continue;

        Pair<CoreCount, List<CoreCount>> childCores =
            fragmentCoreState.get(childFragment.getId());
        Preconditions.checkNotNull(childCores);

        if (childFragment.hasBlockingNode()) {
          CoreCount childCoreCount = childFragment.maxCore(
              childCores.first, CoreCount.sum(childCores.second), findUnboundedCount);
          subtreeCoreBuilder.add(childCoreCount);
        } else {
          Preconditions.checkState(node instanceof ExchangeNode);
          Preconditions.checkState(i == 0);
          segmentCore = CoreCount.sum(segmentCore, childCores.first);
          subtreeCoreBuilder.addAll(childCores.second);
        }
      }
    }

    return segmentCore;
  }

  /**
   * Try to come up with lower parallelism for this segment by comparing the output
   * ProcessingCost of child segment or input fragment.
   * <p>
   * This segment cost is the Consumer cost, while the ProcessingCost of child segment
   * or input fragment is the Producer cost. It compares between per-row production cost
   * of Producer versus the per-row consumption cost of Consumer. The expected parallelism
   * of Consumer (this segment) then is adjusted to get closer to the produce-consume
   * ratio when compared to the Producer.
   *
   * @param nodeStepCount The step count used to increase this fragment's parallelism.
   *                      Usually equal to number of nodes or just 1.
   * @param minParallelism The minimum parallelism of this segment.
   * @param maxParallelism The maximum parallelism this segment is allowed to adjust to.
   */
  protected int tryAdjustParallelism(
      int nodeStepCount, int minParallelism, int maxParallelism) {
    // TODO: The ratio based adjustment can be further improved by considering the plan
    //   nodes too. For example, one does not need to make DoP of the parent of a scale
    //   aggregate fragment the same as the aggregate fragment, say the aggregate fragment
    //   produces one row and uses 10 instances.
    int newParallelism = minParallelism;
    int originalParallelism = cost_.getNumInstancesExpected();
    ProcessingCost producerCost = ProcessingCost.zero();

    if (getChildCount() > 0) {
      for (CostingSegment childSegment : getChildren()) {
        newParallelism = Math.max(newParallelism,
            childSegment.tryAdjustParallelism(
                nodeStepCount, minParallelism, maxParallelism));
      }
      producerCost = mergeCostingSegment(getChildren()).getProcessingCost();
    }

    // If this segment has UnionNode, it may have ExchangeNode belonging to this segment.
    List<ProcessingCost> childOutputCosts =
        nodes_.stream()
            .filter(Predicates.instanceOf(ExchangeNode.class))
            .map(p -> p.getChild(0).getFragment().getLastCostingSegment())
            .collect(Collectors.toList());

    if (!childOutputCosts.isEmpty()) {
      if (producerCost.getTotalCost() > 0) childOutputCosts.add(producerCost);
      producerCost = ProcessingCost.fullMergeCosts(childOutputCosts);
    }

    ProcessingCost.tryAdjustConsumerParallelism(
        nodeStepCount, minParallelism, maxParallelism, producerCost, cost_);
    newParallelism = Math.max(newParallelism, cost_.getNumInstancesExpected());
    Preconditions.checkState(newParallelism <= maxParallelism,
        getRootId() + " originalParallelism=" + originalParallelism + ". newParallelism="
            + newParallelism + " > maxParallelism=" + maxParallelism);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Adjust ProcessingCost on {}. originalParallelism={} minParallelism={} "
              + "maxParallelism={} newParallelism={} consumerCost={} "
              + "consumerInstCount={} producerCost={} producerInstCount={}",
          getRootId(), originalParallelism, minParallelism, maxParallelism,
          newParallelism, cost_.getTotalCost(), cost_.getNumInstancesExpected(),
          producerCost.getTotalCost(), producerCost.getNumInstancesExpected());
    }

    return newParallelism;
  }

  /**
   * Merge given list of CostingSegment into a new combined CostingSegment.
   * <p>
   * The resulting CostingSegment will contain all nodes, children segments, and sum of
   * ProcessingCost from all given CostingSegment.
   *
   * @param costingSegments List of CostingSegment to merge. Must not be empty and must
   *                        not contain output segment (segment with DataSink set).
   * @return A combined CostingSegment.
   */
  protected static CostingSegment mergeCostingSegment(
      List<CostingSegment> costingSegments) {
    Preconditions.checkNotNull(costingSegments);
    Preconditions.checkArgument(!costingSegments.isEmpty());

    if (costingSegments.size() == 1) return costingSegments.get(0);
    CostingSegment mergedCost = new CostingSegment();
    List<ProcessingCost> allCosts = Lists.newArrayList();
    for (CostingSegment costingSegment : costingSegments) {
      Preconditions.checkArgument(!costingSegment.isOutputSegment());
      mergedCost.nodes_.addAll(costingSegment.nodes_);
      mergedCost.addChildren(costingSegment.getChildren());
      allCosts.add(costingSegment.getProcessingCost());
    }
    mergedCost.cost_ = ProcessingCost.fullMergeCosts(allCosts);
    return mergedCost;
  }
}
