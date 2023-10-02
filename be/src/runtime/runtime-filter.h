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

#pragma once

#include <mutex>

#include "gen-cpp/ExternalDataSource_types.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-filter-bank.h"
#include "util/bloom-filter.h"
#include "util/in-list-filter.h"
#include "util/condition-variable.h"
#include "util/time.h"

namespace impala {

class BloomFilter;
class RuntimeFilterTest;

/// RuntimeFilters represent set-membership predicates that are computed during query
/// execution (rather than during planning). They can then be sent to other operators to
/// reduce their output. For example, a RuntimeFilter might compute a predicate
/// corresponding to set membership, where the members of that set can only be computed at
/// runtime (for example, the distinct values of the build side of a hash table). Other
/// plan nodes can use that predicate by testing for membership of that set to filter rows
/// early on in the plan tree (e.g. the scan that feeds the probe side of that join node
/// could eliminate rows from consideration for join matching).
///
/// A RuntimeFilter may compute its set-membership predicate as a bloom filters or a
/// min-max filter, depending on its filter description.
class RuntimeFilter {
 public:
  RuntimeFilter(const TRuntimeFilterDesc& filter, int64_t filter_size)
      : bloom_filter_(nullptr), min_max_filter_(nullptr), in_list_filter_(nullptr),
        filter_desc_(filter), registration_time_(MonotonicMillis()), arrival_time_(0L),
        filter_size_(filter_size) {
    DCHECK(filter_desc_.type == TRuntimeFilterType::MIN_MAX || filter_size_ > 0);
  }

  /// Returns true if SetFilter() has been called.
  bool HasFilter() const { return has_filter_.Load(); }

  const TRuntimeFilterDesc& filter_desc() const { return filter_desc_; }
  const std::string& krpc_hostname_to_report() const {
    return intermediate_krpc_hostname_;
  }
  const NetworkAddressPB& krpc_backend_to_report() const {
    return intermediate_krpc_backend_;
  }
  bool is_intermediate_aggregator() const { return is_intermediate_aggregator_; }
  int32_t id() const { return filter_desc().filter_id; }
  int64_t filter_size() const { return filter_size_; }
  ColumnType type() const {
    return ColumnType::FromThrift(filter_desc().src_expr.nodes[0].type);
  }
  bool is_bloom_filter() const { return filter_desc().type == TRuntimeFilterType::BLOOM; }
  bool is_min_max_filter() const {
    return filter_desc().type == TRuntimeFilterType::MIN_MAX;
  }
  bool is_in_list_filter() const {
    return filter_desc().type == TRuntimeFilterType::IN_LIST;
  }

  extdatasource::TComparisonOp::type getCompareOp() const {
    return filter_desc().compareOp;
  }

  BloomFilter* get_bloom_filter() const { return bloom_filter_.Load(); }
  MinMaxFilter* get_min_max() const { return min_max_filter_.Load(); }
  InListFilter* get_in_list_filter() const { return in_list_filter_.Load(); }

  /// Sets the internal filter to 'bloom_filter', 'min_max_filter' or 'in_list_filter'
  /// depending on the type of this RuntimeFilter. Can only legally be called
  /// once per filter. Does not acquire the memory associated with 'bloom_filter'.
  void SetFilter(BloomFilter* bloom_filter, MinMaxFilter* min_max_filter,
      InListFilter* in_list_filter);

  /// Set the internal bloom or min-max filter to the equivalent filter from 'other'.
  /// The parameters of 'other' must be compatible and the filters must have the same
  /// ID. Can only legally be called once per filter. Does not acquire the memory from
  /// the other filter.
  void SetFilter(RuntimeFilter* other);

  /// Merge 'bloom_filter' or 'min_max_filter' into this filter. The caller must provide
  /// the appropriate kind of filter for this RuntimeFilter instance.
  /// Not thread-safe.
  void Or(RuntimeFilter* other);

  /// Signal that no filter should be arriving, waking up any threads blocked in
  /// WaitForArrival().
  void Cancel();

  /// Returns false iff 'bloom_filter_' has been set via SetBloomFilter() and hash[val] is
  /// not in that 'bloom_filter_'. Otherwise returns true. Is safe to call concurrently
  /// with SetBloomFilter(). 'val' is a value derived from evaluating a tuple row against
  /// the expression of the owning filter context. 'col_type' is the value's type.
  /// Inlined in IR so that the constant 'col_type' can be propagated.
  bool IR_ALWAYS_INLINE Eval(void* val, const ColumnType& col_type) const noexcept;

  /// Returns the amount of time in milliseconds elapsed between the registration of the
  /// filter and its arrival. If the filter has not yet arrived, it returns the time
  /// elapsed since registration.
  int32_t arrival_delay_ms() const {
    if (arrival_time_.Load() == 0L) return TimeSinceRegistrationMs();
    return arrival_time_.Load() - registration_time_;
  }

  /// Return the amount of time since 'registration_time_'.
  int32_t TimeSinceRegistrationMs() const {
    return MonotonicMillis() - registration_time_;
  }

  /// Periodically (every 20ms) checks to see if the global filter has arrived. Waits for
  /// a maximum of timeout_ms before returning. Returns true if the filter has arrived,
  /// false otherwise.
  bool WaitForArrival(int32_t timeout_ms) const;

  /// Returns true if the filter returns true/false for all elements, i.e. Eval(v) returns
  /// true/false for all v.
  inline bool AlwaysTrue() const;
  inline bool AlwaysFalse() const;

  bool IsBoundByPartitionColumn(int plan_id) const {
    int target_ndx = filter_desc().planid_to_target_ndx.at(plan_id);
    return filter_desc().targets[target_ndx].is_bound_by_partition_columns;
  }

  bool IsColumnInDataFile(int plan_id) const {
    int target_ndx = filter_desc().planid_to_target_ndx.at(plan_id);
    return filter_desc().targets[target_ndx].is_column_in_data_file;
  }

  /// Set intermediate aggregation info for this runtime filter.
  void SetIntermediateAggregation(bool is_intermediate_aggregator,
      std::string intermediate_krpc_hostname, NetworkAddressPB intermediate_krpc_backend);

  /// Return true if runtime filter update from this fragment instance should report
  /// filter update to intermediate aggregator rather than coordinator.
  /// Otherwise, return false, which means filter update report should go to coordinator.
  bool IsReportToSubAggregator() const {
    return !intermediate_krpc_hostname_.empty() && !is_intermediate_aggregator_;
  }

  /// Return true if this runtime filter is scheduled with subaggregation strategy.
  /// Otherwise, return false (all filter updates aggregation happen in coordinator).
  bool RequireSubAggregation() const { return !intermediate_krpc_hostname_.empty(); }

  /// Frequency with which to check for filter arrival in WaitForArrival()
  static const int SLEEP_PERIOD_MS;

  /// Class name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 private:
  friend class RuntimeFilterTest;

  /// Membership bloom_filter. May be NULL even after arrival_time_ is set, meaning that
  /// it does not filter any rows, either because it was not created
  /// (filter_desc_.bloom_filter is false), there was not enough memory, or the false
  /// positive rate was determined to be too high.
  AtomicPtr<BloomFilter> bloom_filter_;

  /// May be NULL even after arrival_time_ is set if filter_desc_.min_max_filter is false.
  AtomicPtr<MinMaxFilter> min_max_filter_;

  /// May be NULL even after arrival_time_ is set if filter_desc_.in_list_filter is false.
  AtomicPtr<InListFilter> in_list_filter_;

  /// Reference to the filter's thrift descriptor in the thrift Plan tree.
  const TRuntimeFilterDesc& filter_desc_;

  /// Time in ms (from MonotonicMillis()), that the filter was registered.
  const int64_t registration_time_;

  /// Time, in ms (from MonotonicMillis()), that the global filter arrived, or the
  /// filter was cancelled. Set in SetFilter() or Cancel().
  AtomicInt64 arrival_time_;

  /// Only set after arrival_time_, if SetFilter() was called.
  AtomicBool has_filter_{false};

  /// The size of the Bloom filter, in bytes.
  const int64_t filter_size_;

  /// Lock to protect 'arrival_cv_'
  mutable std::mutex arrival_mutex_;

  /// Signalled when a filter arrives or the filter is cancelled. Paired with
  /// 'arrival_mutex_'
  mutable ConditionVariable arrival_cv_;

  /// Injection delay for WaitForArrival. Used in testing only.
  /// See IMPALA-9612.
  int64_t injection_delay_ = 0;

  bool is_intermediate_aggregator_ = false;

  std::string intermediate_krpc_hostname_;

  NetworkAddressPB intermediate_krpc_backend_;
};
}
