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

import org.apache.impala.common.Id;

import java.util.Comparator;
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

  // Sum of all elements in count_.
  // Cached after the first call of total().
  private int total_ = -1;

  public CoreCount(Id id, int count) {
    Preconditions.checkArgument(count >= 0, "Core count must be a non-negative number");
    ids_ = ImmutableList.of(id);
    counts_ = ImmutableList.of(count);
  }

  private CoreCount(ImmutableList<Id> ids, ImmutableList<Integer> counts) {
    Preconditions.checkArgument(
        ids.size() == counts.size(), "ids and counts must have same size!");
    ids_ = ids;
    counts_ = counts;
  }

  public int total() {
    if (total_ < 0) {
      total_ = counts_.stream().mapToInt(v -> v).sum();
    }
    return total_;
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
    for (CoreCount coreRequirement : cores) {
      idBuilder.addAll(coreRequirement.ids_);
      countBuilder.addAll(coreRequirement.counts_);
    }
    return new CoreCount(idBuilder.build(), countBuilder.build());
  }

  protected static CoreCount sum(CoreCount core1, CoreCount core2) {
    ImmutableList.Builder<Id> idBuilder = new ImmutableList.Builder<Id>();
    ImmutableList.Builder<Integer> countBuilder = new ImmutableList.Builder<Integer>();

    idBuilder.addAll(core1.ids_);
    idBuilder.addAll(core2.ids_);
    countBuilder.addAll(core1.counts_);
    countBuilder.addAll(core2.counts_);

    return new CoreCount(idBuilder.build(), countBuilder.build());
  }

  protected static CoreCount max(CoreCount core1, CoreCount core2) {
    return (core1.total() < core2.total()) ? core2 : core1;
  }
}
