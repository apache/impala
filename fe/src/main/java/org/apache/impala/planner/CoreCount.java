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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.impala.common.Id;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A container class that represent CPU cores, computed from the CPU cost, of certain
 * subtree of a query or the query itself.
 */
public class CoreCount {
  // List of Id (either PlanFragmentId or PlanNodeId) that describe the origin of counts_
  // element.
  private final ImmutableList<Id> ids_;

  // List of CPU core count contributing to this CoreCount.
  private final ImmutableList<Integer> counts_;

  // Set of unique fragment that contributes toward this CoreCount.
  private final ImmutableSet<PlanFragmentId> uniqueFragmentIds_;

  // True if this CoreCount include a plan root sink.
  private final boolean hasPlanRootSink_;

  // Sum of all elements in count_.
  private final int total_;

  public CoreCount(PlanFragment fragment, int count) {
    Preconditions.checkArgument(count >= 0, "Core count must be a non-negative number");
    ids_ = ImmutableList.of(fragment.getId());
    counts_ = ImmutableList.of(count);
    uniqueFragmentIds_ = ImmutableSet.of(fragment.getId());
    hasPlanRootSink_ = (fragment.getSink() instanceof PlanRootSink);
    total_ = counts_.stream().mapToInt(v -> v).sum();
  }

  public CoreCount(PlanNode node, int count) {
    Preconditions.checkArgument(count >= 0, "Core count must be a non-negative number");
    ids_ = ImmutableList.of(node.getId());
    counts_ = ImmutableList.of(count);
    PlanFragment fragment = node.getFragment();
    uniqueFragmentIds_ = ImmutableSet.of(fragment.getId());
    hasPlanRootSink_ = false;
    total_ = counts_.stream().mapToInt(v -> v).sum();
  }

  private CoreCount(ImmutableList<Id> ids, ImmutableList<Integer> counts,
      ImmutableSet<PlanFragmentId> uniqueFragments, boolean hasPlanRootSink) {
    Preconditions.checkArgument(
        ids.size() == counts.size(), "ids and counts must have same size!");
    ids_ = ids;
    counts_ = counts;
    uniqueFragmentIds_ = uniqueFragments;
    hasPlanRootSink_ = hasPlanRootSink;
    total_ = counts_.stream().mapToInt(v -> v).sum();
  }

  public int total() { return total_; }
  public boolean hasCoordinator() { return hasPlanRootSink_; }

  /**
   * If this CoreCount has coordinator fragment in it, return total() - 1.
   * Otherwise, return the same value as total().
   */
  public int totalWithoutCoordinator() { return total_ - (hasPlanRootSink_ ? 1 : 0); }

  /**
   * Return a set of PlanFragmentId that contribute toward this CoreCount.
   */
  public ImmutableSet<PlanFragmentId> getUniqueFragmentIds() {
    return uniqueFragmentIds_;
  }

  @Override
  public String toString() {
    if (ids_.isEmpty()) {
      return "<empty>";
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append("{total=");
      sb.append(total());
      sb.append(" trace=");
      sb.append(IntStream.range(0, ids_.size())
                    .mapToObj(i
                        -> ((ids_.get(i) instanceof PlanNodeId) ? "N" : "") + ids_.get(i)
                            + ":" + counts_.get(i))
                    .collect(Collectors.joining("+")));
      sb.append("}");
      return sb.toString();
    }
  }

  protected static CoreCount sum(List<CoreCount> cores) {
    ImmutableList.Builder<Id> idBuilder = new ImmutableList.Builder<Id>();
    ImmutableList.Builder<Integer> countBuilder = new ImmutableList.Builder<Integer>();
    ImmutableSet.Builder<PlanFragmentId> fragmentIdBuilder =
        new ImmutableSet.Builder<PlanFragmentId>();
    boolean hasPlanRootSink = false;
    for (CoreCount coreRequirement : cores) {
      idBuilder.addAll(coreRequirement.ids_);
      countBuilder.addAll(coreRequirement.counts_);
      fragmentIdBuilder.addAll(coreRequirement.uniqueFragmentIds_);
      hasPlanRootSink |= coreRequirement.hasPlanRootSink_;
    }
    return new CoreCount(idBuilder.build(), countBuilder.build(),
        fragmentIdBuilder.build(), hasPlanRootSink);
  }

  protected static CoreCount sum(CoreCount core1, CoreCount core2) {
    return sum(ImmutableList.of(core1, core2));
  }

  protected static CoreCount max(CoreCount core1, CoreCount core2) {
    if (core1.totalWithoutCoordinator() < core2.totalWithoutCoordinator()) {
      return core2;
    } else {
      return core1;
    }
  }
}
