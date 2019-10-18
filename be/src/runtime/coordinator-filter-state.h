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


#include <memory>
#include <utility>
#include <vector>
#include <boost/unordered_set.hpp>

#include "runtime/coordinator.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"

namespace impala {

class MemTracker;

/// Represents a runtime filter target.
struct Coordinator::FilterTarget {
  TPlanNodeId node_id;
  bool is_local;
  bool is_bound_by_partition_columns;
  int fragment_idx;

  FilterTarget(const TRuntimeFilterTargetDesc& desc, int f_idx)
    : node_id(desc.node_id),
      is_local(desc.is_local_target),
      is_bound_by_partition_columns(desc.is_bound_by_partition_columns),
      fragment_idx(f_idx) {}
};

/// State of runtime filters that are received for aggregation. A runtime filter will
/// contain a bloom or min-max filter.
///
/// A broadcast join filter is published as soon as the first update is received for it
/// and subsequent updates are ignored (as they will be the same).
/// Updates for a partitioned join filter are aggregated and then published once
/// 'pending_count' reaches 0 and if the filter was not disabled before that.
///
///
/// A filter is disabled if an always_true filter update is received, an OOM is hit,
/// filter aggregation is complete or if the query is complete.
/// Once a filter is disabled, subsequent updates for that filter are ignored.
class Coordinator::FilterState {
 public:
  FilterState(const TRuntimeFilterDesc& desc, const TPlanNodeId& src)
    : desc_(desc), src_(src) {
    // bloom_filter_ is a disjunction so the unit value is always_false.
    bloom_filter_.always_false = true;
    min_max_filter_.always_false = true;
  }

  TBloomFilter& bloom_filter() { return bloom_filter_; }
  TMinMaxFilter& min_max_filter() { return min_max_filter_; }
  std::vector<FilterTarget>* targets() { return &targets_; }
  const std::vector<FilterTarget>& targets() const { return targets_; }
  int64_t first_arrival_time() const { return first_arrival_time_; }
  int64_t completion_time() const { return completion_time_; }
  const TPlanNodeId& src() const { return src_; }
  const TRuntimeFilterDesc& desc() const { return desc_; }
  bool is_bloom_filter() const { return desc_.type == TRuntimeFilterType::BLOOM; }
  bool is_min_max_filter() const { return desc_.type == TRuntimeFilterType::MIN_MAX; }
  int pending_count() const { return pending_count_; }
  void set_pending_count(int pending_count) { pending_count_ = pending_count; }
  int num_producers() const { return num_producers_; }
  void set_num_producers(int num_producers) { num_producers_ = num_producers; }
  bool disabled() const {
    if (is_bloom_filter()) {
      return bloom_filter_.always_true;
    } else {
      DCHECK(is_min_max_filter());
      return min_max_filter_.always_true;
    }
  }

  /// Aggregates partitioned join filters and updates memory consumption.
  /// Disables filter if always_true filter is received or OOM is hit.
  void ApplyUpdate(const TUpdateFilterParams& params, Coordinator* coord);

  /// Disables a filter. A disabled filter consumes no memory.
  void Disable(MemTracker* tracker);

 private:
  /// Contains the specification of the runtime filter.
  TRuntimeFilterDesc desc_;

  TPlanNodeId src_;
  std::vector<FilterTarget> targets_;

  /// Number of remaining backends to hear from before filter is complete.
  int pending_count_ = 0;

  /// Number of fragment instances producing this filter. The full information about the
  /// producer instances is tracked in 'finstance_filters_produced'.
  int num_producers_ = 0;

  /// Filters aggregated from all source plan nodes, to be broadcast to all
  /// destination plan fragment instances. Only set for partitioned joins (broadcast joins
  /// need no aggregation).
  /// In order to avoid memory spikes, an incoming filter is moved (vs. copied) to the
  /// output structure in the case of a broadcast join. Similarly, for partitioned joins,
  /// the filter is moved from the following member to the output structure.
  TBloomFilter bloom_filter_;
  TMinMaxFilter min_max_filter_;

  /// Time at which first local filter arrived.
  int64_t first_arrival_time_ = 0L;

  /// Time at which all local filters arrived.
  int64_t completion_time_ = 0L;

  /// TODO: Add a per-object lock so that we can avoid holding the global routing table
  /// lock for every filter update.
};

/// Struct to contain all of the data structures for filter routing. Coordinator
/// has access to all the internals of this structure and must protect invariants.
struct Coordinator::FilterRoutingTable {
  int64_t num_filters() const { return id_to_filter.size(); }

  /// Maps the filter ID to the state of that filter.
  boost::unordered_map<int32_t, FilterState> id_to_filter;

  // List of runtime filters that this fragment instance is the source for.
  // The key of the map is the instance index returned by GetInstanceIdx().
  // The value is source plan node id and the filter ID.
  boost::unordered_map<int, std::vector<TRuntimeFilterSource>> finstance_filters_produced;

  /// Synchronizes updates to the state of this routing table.
  SpinLock update_lock;

  /// Protects this routing table.
  /// Usage pattern:
  /// 1. To update the routing table: Acquire shared access on 'lock' and
  ///    upgrade to exclusive access by subsequently acquiring 'update_lock'.
  /// 2. To read the routing table: if 'is_complete' is true and no threads
  ///    will be destroying the table concurrently, it is safe to read the
  ///    routing table without acquiring a lock. Otherwise, acquire shared
  ///    access on 'lock'
  /// 3. To initialize/destroy the routing table: Directly acquire exclusive
  ///    access on 'lock'.
  boost::shared_mutex lock;

  /// Set to true when all calls to UpdateFilterRoutingTable() have finished, and it's
  /// safe to concurrently read from this routing table.
  bool is_complete = false;
};
}
