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

#include <condition_variable>
#include <memory>
#include <utility>
#include <vector>
#include <boost/thread/shared_mutex.hpp>
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
///
/// This class is not thread safe. Callers must always take 'lock()' themselves when
/// calling any FilterState functions if thread safety is needed.
class Coordinator::FilterState {
 public:
  FilterState(const TRuntimeFilterDesc& desc)
    : desc_(desc) {
    // bloom_filter_ is a disjunction so the unit value is always_false.
    bloom_filter_.set_always_false(true);
    min_max_filter_.set_always_false(true);
  }

  BloomFilterPB& bloom_filter() { return bloom_filter_; }
  std::string& bloom_filter_directory() { return bloom_filter_directory_; }
  MinMaxFilterPB& min_max_filter() { return min_max_filter_; }
  InListFilterPB& in_list_filter() { return in_list_filter_; }
  std::vector<FilterTarget>* targets() { return &targets_; }
  const std::vector<FilterTarget>& targets() const { return targets_; }
  int64_t first_arrival_time() const { return first_arrival_time_; }
  bool has_first_arrival_time() const { return first_arrival_time_ > -1; }
  int64_t completion_time() const { return completion_time_; }
  bool has_completion_time() const { return completion_time_ > -1; }
  const TRuntimeFilterDesc& desc() const { return desc_; }
  bool is_bloom_filter() const { return desc_.type == TRuntimeFilterType::BLOOM; }
  bool is_min_max_filter() const { return desc_.type == TRuntimeFilterType::MIN_MAX; }
  bool is_in_list_filter() const { return desc_.type == TRuntimeFilterType::IN_LIST; }
  int pending_count() const { return pending_count_; }
  void set_pending_count(int pending_count) { pending_count_ = pending_count; }
  int num_producers() const { return num_producers_; }
  void set_num_producers(int num_producers) { num_producers_ = num_producers; }
  bool disabled() const {
    if (is_bloom_filter()) {
      return bloom_filter_.always_true();
    } else if (is_min_max_filter()) {
      return min_max_filter_.always_true();
    } else {
      DCHECK(is_in_list_filter());
      return in_list_filter_.always_true();
    }
  }
  bool enabled() const { return !disabled(); }
  int num_inflight_rpcs() const { return num_inflight_publish_filter_rpcs_; }
  SpinLock& lock() { return lock_; }
  std::condition_variable_any& get_publish_filter_done_cv() {
    return publish_filter_done_cv_;
  }
  bool received_all_updates() const { return all_updates_received_; }

  /// Aggregates partitioned join filters and updates memory consumption.
  /// Disables filter if always_true filter is received or OOM is hit.
  void ApplyUpdate(const UpdateFilterParamsPB& params, Coordinator* coord,
      kudu::rpc::RpcContext* context);

  /// Disables the filter and releases the consumed memory if the filter is a Bloom
  /// filter.
  void DisableAndRelease(MemTracker* tracker, const bool all_updates_received);
  /// Disables the filter but does not release the consumed memory.
  void Disable(const bool all_updates_received);
  /// Release consumed memory of this filter. Caller must hold `lock_` and make sure
  /// filter already disabled.
  void Release(MemTracker* tracker);

  void IncrementNumInflightRpcs(int i) {
    num_inflight_publish_filter_rpcs_ += i;
    DCHECK_GE(num_inflight_publish_filter_rpcs_, 0);
  }

  /// Waits until any inflight PublishFilter rpcs have completed.
  void WaitForPublishFilter();

  bool AlwaysTrueFilterReceived() const { return always_true_filter_received_; }
  bool AlwaysFalseFlippedToFalse() const { return always_false_flipped_to_false_; }

  // Display a FilterState object without creating an MinMaxFilter first.
  std::string DebugString() const;

 private:
  /// Contains the specification of the runtime filter.
  TRuntimeFilterDesc desc_;

  std::vector<FilterTarget> targets_;

  /// Number of remaining backends to hear from before filter is complete.
  int pending_count_ = 0;

  /// Number of fragment instances producing this filter. The full information about the
  /// producer instances is tracked in 'finstance_filters_produced'.
  int num_producers_ = 0;

  /// Filters aggregated from all source plan nodes, to be broadcast to all
  /// destination plan fragment instances. Only set for partitioned joins (broadcast
  /// joins need no aggregation).
  /// In order to avoid memory spikes, an incoming filter is moved (vs. copied) to the
  /// output structure in the case of a broadcast join. Similarly, for partitioned joins,
  /// the filter is moved from the following member to the output structure.
  BloomFilterPB bloom_filter_;
  /// When the filter is a Bloom filter, we use this string to store the contents of the
  /// aggregated Bloom filter.
  std::string bloom_filter_directory_;
  MinMaxFilterPB min_max_filter_;
  InListFilterPB in_list_filter_;

  /// Time at which first local filter arrived.
  int64_t first_arrival_time_ = -1L;

  /// Time at which all local filters arrived.
  int64_t completion_time_ = -1L;

  /// Per-object lock so that we can avoid holding the global routing table
  /// lock for every filter update.
  SpinLock lock_;

  /// Keeps track of the number of inflight PublishFilter rpcs.
  int num_inflight_publish_filter_rpcs_ = 0;

  /// Signaled when 'num_inflight_rpcs' reaches 0.
  std::condition_variable_any publish_filter_done_cv_;

  /// True value means coordinator has heard back from all pending backends.
  bool all_updates_received_ = false;

  /// True value means an alwaysTrue filter is received. Set in
  /// FilterState::ApplyUpdate().
  bool always_true_filter_received_ = false;

  /// True value means the always false flag in aggregated filter is flipped from
  /// 'true' to 'false' by the coordinator. Set in FilterState::Disable().
  bool always_false_flipped_to_false_ = false;
};

/// Struct to contain all of the data structures for filter routing. Coordinator
/// has access to all the internals of this structure and must protect invariants.
struct Coordinator::FilterRoutingTable {
  int64_t num_filters() const { return id_to_filter.size(); }

  /// Get the existing FilterState for 'filter' or create it if not present.
  FilterState* GetOrCreateFilterState(const TRuntimeFilterDesc& filter) {
    auto i = id_to_filter.find(filter.filter_id);
    if (i == id_to_filter.end()) {
      i = id_to_filter.emplace(std::piecewise_construct,
                           std::forward_as_tuple(filter.filter_id),
                           std::forward_as_tuple(filter)).first;
    }
    return &(i->second);
  }

  /// Maps the filter ID to the state of that filter.
  boost::unordered_map<int32_t, FilterState> id_to_filter;

  // List of runtime filters that this fragment instance is the source for.
  // The key of the map is the instance index returned by GetInstanceIdx().
  // The value is source plan node id and the filter ID.
  boost::unordered_map<int, std::vector<TRuntimeFilterSource>> finstance_filters_produced;

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
