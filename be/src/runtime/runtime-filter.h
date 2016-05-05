// Copyright 2016 Cloudera Inc.
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


#ifndef IMPALA_RUNTIME_RUNTIME_FILTER_H
#define IMPALA_RUNTIME_RUNTIME_FILTER_H

#include <boost/unordered_map.hpp>

#include "common/object-pool.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "runtime/types.h"
#include "util/runtime-profile.h"
#include "util/spinlock.h"

namespace impala {

class BloomFilter;
class RuntimeFilter;
class RuntimeState;

/// RuntimeFilters are produced and consumed by plan nodes at run time to propagate
/// predicates across the plan tree dynamically. Each fragment instance manages its
/// filters with a RuntimeFilterBank which provides low-synchronization access to filter
/// objects and data structures.
///
/// A RuntimeFilterBank manages both production and consumption of filters. In the case
/// where a given filter is both consumed and produced by the same fragment, the
/// RuntimeFilterBank treats each filter independently.
///
/// All filters must be registered with the filter bank via RegisterFilter(). Local plan
/// fragments update the bloom filters by calling UpdateFilterFromLocal()
/// (UpdateFilterFromLocal() may only be called once per filter ID per filter bank). The
/// bloom_filter that is passed into UpdateFilterFromLocal() must have been allocated by
/// AllocateScratchBloomFilter() (or be NULL); this allows RuntimeFilterBank to manage all
/// memory associated with filters.
///
/// Filters are aggregated at the coordinator, and then made available to consumers after
/// PublishGlobalFilter() has been called.
///
/// After PublishGlobalFilter() has been called (and again, it may only be called once per
/// filter_id), the RuntimeFilter object associated with filter_id will have a valid
/// bloom_filter, and may be used for filter evaluation. This operation occurs without
/// synchronisation, and neither the thread that calls PublishGlobalFilter() nor the
/// thread that may call RuntimeFilter::Eval() need to coordinate in any way.
class RuntimeFilterBank {
 public:
  RuntimeFilterBank(const TQueryCtx& query_ctx, RuntimeState* state);

  /// Registers a filter that will either be produced (is_producer == false) or consumed
  /// (is_producer == true) by fragments that share this RuntimeState. The filter
  /// bloom_filter itself is unallocated until the first call to PublishGlobalFilter().
  RuntimeFilter* RegisterFilter(const TRuntimeFilterDesc& filter_desc, bool is_producer);

  /// Updates a filter's bloom_filter with 'bloom_filter' which has been produced by some
  /// operator in the local fragment instance. 'bloom_filter' may be NULL, representing a
  /// full filter that contains all elements.
  void UpdateFilterFromLocal(int32_t filter_id, BloomFilter* bloom_filter);

  /// Makes a bloom_filter (aggregated globally from all producer fragments) available for
  /// consumption by operators that wish to use it for filtering.
  void PublishGlobalFilter(int32_t filter_id, const TBloomFilter& thrift_filter);

  /// Returns true if, according to the observed NDV in 'observed_ndv', a filter of size
  /// 'filter_size' would have an expected false-positive rate which would exceed
  /// FLAGS_max_filter_error_rate.
  bool FpRateTooHigh(int64_t filter_size, int64_t observed_ndv);

  /// Returns a RuntimeFilter with the given filter id. This is safe to call after all
  /// calls to RegisterFilter() have finished, and not before. Filters may be cached by
  /// clients and subsequently accessed without synchronization. Concurrent calls to
  /// PublishGlobalFilter() will update a filter's bloom filter atomically, without the
  /// need for client synchronization.
  inline const RuntimeFilter* GetRuntimeFilter(int32_t filter_id);

  /// Returns a bloom_filter that can be used by an operator to produce a local filter,
  /// which may then be used in UpdateFilterFromLocal(). The memory returned is owned by
  /// the RuntimeFilterBank (which may transfer it to a RuntimeFilter subsequently), and
  /// should not be deleted by the caller. The filter identified by 'filter_id' must have
  /// been previously registered as a 'producer' by RegisterFilter().
  ///
  /// If there is not enough memory, or if Close() has been called first, returns NULL.
  BloomFilter* AllocateScratchBloomFilter(int32_t filter_id);

  /// Default hash seed to use when computing hashed values to insert into filters.
  static const int32_t DefaultHashSeed() { return 1234; }

  /// Releases all memory allocated for BloomFilters.
  void Close();

  static const int64_t MIN_BLOOM_FILTER_SIZE = 4 * 1024;           // 4KB
  static const int64_t MAX_BLOOM_FILTER_SIZE = 16 * 1024 * 1024;   // 16MB

 private:
  /// Returns the the space (in bytes) required for a filter to achieve the configured
  /// maximum false-positive rate based on the expected NDV. If 'ndv' is -1 (i.e. no
  /// estimate is known), the default filter size is returned.
  int64_t GetFilterSizeForNdv(int64_t ndv);

  const TQueryCtx query_ctx_;

  /// Lock protecting produced_filters_ and consumed_filters_.
  boost::mutex runtime_filter_lock_;

  /// Map from filter id to a RuntimeFilter.
  typedef boost::unordered_map<int32_t, RuntimeFilter*> RuntimeFilterMap;

  /// All filters expected to be produced by the local plan fragment instance.
  RuntimeFilterMap produced_filters_;

  /// All filters expected to be consumed by the local plan fragment instance.
  RuntimeFilterMap consumed_filters_;

  /// Fragment instance's runtime state.
  RuntimeState* state_;

  /// Object pool to track allocated Bloom filters.
  ObjectPool obj_pool_;

  /// True iff Close() has been called. Used to prevent races between
  /// AllocateScratchBloomFilter() and Close().
  bool closed_;

  /// Total amount of memory allocated to Bloom Filters
  RuntimeProfile::Counter* memory_allocated_;

  /// Precomputed default BloomFilter size.
  int64_t default_filter_size_;

  /// Maximum filter size, in bytes, rounded up to a power of two.
  int64_t max_filter_size_;

  /// Minimum filter size, in bytes, rounded up to a power of two.
  int64_t min_filter_size_;
};

/// RuntimeFilters represent set-membership predicates (implemented with bloom filters)
/// that are computed during query execution (rather than during planning). They can then
/// be sent to other operators to reduce their output. For example, a RuntimeFilter might
/// compute a predicate corresponding to set membership, where the members of that set can
/// only be computed at runtime (for example, the distinct values of the build side of a
/// hash table). Other plan nodes can use that predicate by testing for membership of that
/// set to filter rows early on in the plan tree (e.g. the scan that feeds the probe side
/// of that join node could eliminate rows from consideration for join matching).
class RuntimeFilter {
 public:
  RuntimeFilter(const TRuntimeFilterDesc& filter, int64_t filter_size)
      : bloom_filter_(NULL), filter_desc_(filter), arrival_time_(0L),
        filter_size_(filter_size) {
    DCHECK_GT(filter_size_, 0);
    registration_time_ = MonotonicMillis();
  }

  /// Returns true if SetBloomFilter() has been called.
  bool HasBloomFilter() const { return arrival_time_ != 0; }

  const TRuntimeFilterDesc& filter_desc() const { return filter_desc_; }
  const int32_t id() const { return filter_desc().filter_id; }
  int64_t filter_size() const { return filter_size_; }

  /// Sets the internal filter bloom_filter to 'bloom_filter'. Can only legally be called
  /// once per filter. Does not acquire the memory associated with 'bloom_filter'.
  inline void SetBloomFilter(BloomFilter* bloom_filter);

  /// Returns false iff the bloom_filter filter has been set via SetBloomFilter() and
  /// hash[val] is not in that bloom_filter. Otherwise returns true. Is safe to call
  /// concurrently with SetBloomFilter().
  ///
  /// Templatized in preparation for templatized hashes.
  template<typename T>
  inline bool Eval(T* val, const ColumnType& col_type) const;

  /// Returns the amount of time waited since registration for the filter to
  /// arrive. Returns 0 if filter has not yet arrived.
  int32_t arrival_delay() const {
    if (arrival_time_ == 0L) return 0L;
    return arrival_time_ - registration_time_;
  }

  /// Periodically (every 20ms) checks to see if the global filter has arrived. Waits for
  /// a maximum of timeout_ms before returning. Returns true if the filter has arrived,
  /// false otherwise.
  bool WaitForArrival(int32_t timeout_ms) const;

  /// Returns true if the filter returns true for all elements, i.e. Eval(v) returns true
  /// for all v.
  inline bool AlwaysTrue() const;

  /// Frequency with which to check for filter arrival in WaitForArrival()
  static const int SLEEP_PERIOD_MS;

 private:
  /// Membership bloom_filter. May be NULL even after arrival_time_ is set. This is a
  /// compact way of representing a full Bloom filter that contains every element.
  BloomFilter* bloom_filter_;

  /// Descriptor of the filter.
  TRuntimeFilterDesc filter_desc_;

  /// Time, in ms, that the filter was registered.
  int64_t registration_time_;

  /// Time, in ms, that the global fiter arrived. Set in SetBloomFilter().
  int64_t arrival_time_;

  /// The size of the Bloom filter, in bytes.
  int64_t filter_size_;
};

}

#endif
