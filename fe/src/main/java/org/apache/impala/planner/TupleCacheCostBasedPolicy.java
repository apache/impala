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

import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.thrift.TQueryOptions;

/**
 * The cost-based tuple cache placement policy uses the processing cost information
 * to try to pick the best locations. It has three parts:
 * 1. Threshold - Locations must meet a certain cost threshold to be considered. The
 *                threshold is currently based on the ratio of the regular processing
 *                cost versus the cost of reading from the cache. This ratio must exceed
 *                the tuple_cache_required_cost_improvement_factor to be considered.
 *                For example, if the tuple_cache_required_cost_improvement_factor is
 *                3.0, then the cost of reading from the cache must be 1/3rd the cost of
 *                starting from scratch.
 * 2. Ranking   - The locations that meet the threshold are ranked based on the
 *                greatest cost improvement per byte.
 * 3. Budget    - Locations are picked by the ranking order until the budget is reached.
 *                Currently, the budget is based on the number of bytes per executor
 *                set by the tuple_cache_budget_bytes_per_executor.
 *
 * The algorithm is fairly flexible, so the specific threshold, ranking, and budget
 * could be modified over time.
 */
public class TupleCacheCostBasedPolicy implements TupleCachePlacementPolicy {
  private final static Logger LOG =
      LoggerFactory.getLogger(TupleCacheCostBasedPolicy.class);

  private class CostReductionPerByteComparator implements Comparator<PlanNode> {
    @Override
    public int compare(PlanNode n1, PlanNode n2) {
      Double n1_cost_density = computeCostReductionPerByte(n1);
      Double n2_cost_density = computeCostReductionPerByte(n2);
      // To order things such that the highest cost density comes first, we need to flip
      // the sign on the comparison.
      int result = -n1_cost_density.compareTo(n2_cost_density);
      if (result != 0) return result;
      // Two locations can have the same cost, so this uses the plan node id to break
      // ties to make it consistent.
      return n1.getId().asInt() - n2.getId().asInt();
    }
  }

  private final Comparator<PlanNode> rankingComparator_;
  private final TQueryOptions queryOptions_;

  public TupleCacheCostBasedPolicy(TQueryOptions queryOptions) {
    rankingComparator_ = new CostReductionPerByteComparator();
    queryOptions_ = queryOptions;
  }

  private boolean meetsRequiredCostReductionFactor(PlanNode node) {
    long cumulativeProcessingCost =
        node.getTupleCacheInfo().getCumulativeProcessingCost();
    // To avoid division by zero and exotic floating point behavior, require the cache
    // read processing cost to be 1 or above.
    long cacheReadProcessingCost =
        Math.max(node.getTupleCacheInfo().getReadProcessingCost(), 1);
    double costReductionFactor =
        (double) cumulativeProcessingCost / cacheReadProcessingCost;
    double requiredCostReductionFactor =
        queryOptions_.tuple_cache_required_cost_reduction_factor;
    if (costReductionFactor < requiredCostReductionFactor) {
      LOG.trace(String.format("%s eliminated (cost reduction factor %f < threshold %f)",
        node.getDisplayLabel(), costReductionFactor, requiredCostReductionFactor));
      return false;
    }
    return true;
  }

  private boolean meetsCostThresholds(PlanNode node) {
    // Filter out locations without statistics
    if (node.getTupleCacheInfo().getEstimatedSerializedSize() < 0) {
      LOG.trace(node.getDisplayLabel() + " eliminated due to missing statistics");
      return false;
    }
    // Filter out locations that exceed the budget. They can never be picked.
    long budget = queryOptions_.tuple_cache_budget_bytes_per_executor;
    long bytesPerExecutor = node.getTupleCacheInfo().getEstimatedSerializedSizePerNode();
    if (bytesPerExecutor > budget) {
      LOG.trace(String.format("%s eliminated (bytes per executor %d > budget %d)",
          node.getDisplayLabel(), bytesPerExecutor, budget));
      return false;
    }
    return meetsRequiredCostReductionFactor(node);
  }

  private Double computeCostReductionPerByte(PlanNode node) {
    long cumulativeProcessingCost =
        node.getTupleCacheInfo().getCumulativeProcessingCost();
    long cacheReadProcessingCost = node.getTupleCacheInfo().getReadProcessingCost();
    long estimatedSerializedSize = node.getTupleCacheInfo().getEstimatedSerializedSize();
    long costReduction = cumulativeProcessingCost - cacheReadProcessingCost;
    // The estimated serialized size can be zero when the cardinality is zero. To keep
    // this from being infinite, increment the serialized size by one.
    return Double.valueOf((double) costReduction / (estimatedSerializedSize + 1));
  }

  public Set<PlanNode> getFinalCachingLocations(Set<PlanNode> eligibleLocations) {
    Preconditions.checkState(eligibleLocations.size() > 0);
    PriorityQueue<PlanNode> sortedLocations =
        new PriorityQueue<PlanNode>(eligibleLocations.size(), rankingComparator_);
    for (PlanNode node : eligibleLocations) {
      if (meetsCostThresholds(node)) {
        sortedLocations.add(node);
      }
    }
    Set<PlanNode> finalLocations = new HashSet<PlanNode>();
    // We pick the best locations (by the sorting order) until we reach the budget. This
    // uses the bytes per executor as the units for the budget.
    long remainingBytesPerExecutorBudget =
        queryOptions_.tuple_cache_budget_bytes_per_executor;
    // This continues past a location that would exceed the budget. That allows
    // smaller locations later in the list to have a chance to fit in the
    // remaining budget. This also means that one large entry early in the list
    // won't block any other locations from being considered.
    while (sortedLocations.size() > 0) {
      PlanNode node = sortedLocations.poll();
      long curBytesPerExecutor =
        node.getTupleCacheInfo().getEstimatedSerializedSizePerNode();
      if (curBytesPerExecutor > remainingBytesPerExecutorBudget) {
        LOG.trace(String.format(
            "Skipped %s (bytes per executor: %d, remaining budget: %d)",
            node.getDisplayLabel(), curBytesPerExecutor,
            remainingBytesPerExecutorBudget));
        continue;
      }
      LOG.trace(String.format("Picked %s (bytes per executor: %d, remaining budget: %d)",
          node.getDisplayLabel(), curBytesPerExecutor, remainingBytesPerExecutorBudget));
      finalLocations.add(node);
      remainingBytesPerExecutorBudget -= curBytesPerExecutor;
    }
    return finalLocations;
  }
}
