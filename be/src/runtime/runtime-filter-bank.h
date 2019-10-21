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

#ifndef IMPALA_RUNTIME_RUNTIME_FILTER_BANK_H
#define IMPALA_RUNTIME_RUNTIME_FILTER_BANK_H

#include "codegen/impala-ir.h"
#include "common/object-pool.h"
#include "gen-cpp/data_stream_service.pb.h"
#include "gutil/port.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/mem-pool.h"
#include "runtime/types.h"
#include "util/runtime-profile.h"
#include "util/spinlock.h"

#include <boost/unordered_map.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/lock_guard.hpp>

#include <condition_variable>

namespace kudu {
namespace rpc {
class RpcContext;
class RpcController;
} // namespace rpc
} // namespace kudu

namespace impala {

class BloomFilter;
class MemTracker;
class MinMaxFilter;
class RuntimeFilter;
class QueryState;
class TBloomFilter;
class TRuntimeFilterDesc;
class TQueryCtx;

/// RuntimeFilters are produced and consumed by plan nodes at run time to propagate
/// predicates across the plan tree dynamically. Each query backend manages its
/// filters with a RuntimeFilterBank which provides low-synchronization access to filter
/// objects and data structures.
///
/// A RuntimeFilterBank manages both production and consumption of filters. In the case
/// where a given filter is both consumed and produced by the same backend, the
/// RuntimeFilterBank treats each filter independently.
///
/// All filters must be registered with the filter bank via RegisterFilter(). Local plan
/// fragments update the filters by calling UpdateFilterFromLocal(), with either a bloom
/// filter or a min-max filter, depending on the filter's type. The 'bloom_filter' or
/// 'min_max_filter' that is passed into UpdateFilterFromLocal() must have been allocated
/// by AllocateScratch*Filter(); this allows RuntimeFilterBank to manage all memory
/// associated with filters.
///
/// Filters are aggregated, first locally in this RuntimeFilterBank, if there are multiple
/// producers, and then made available to consumers after PublishGlobalFilter() has been
/// called. The expected number of filters to be produced locally must be specified ahead
/// of time so that RuntimeFilterBank knows when the filter is complete.
///
/// After PublishGlobalFilter() has been called (at most once per filter_id), the
/// RuntimeFilter object associated with filter_id will have a valid bloom_filter or
/// min_max_filter, and may be used for filter evaluation. This operation occurs
/// without synchronisation, and neither the thread that calls PublishGlobalFilter()
/// nor the thread that may call RuntimeFilter::Eval() need to coordinate in any way.
class RuntimeFilterBank {
 public:
  /// 'produced_filter_counts': contains an entry for every filter produced or consumed
  /// on this backend, along with the number of producers (0 if there are only consumers
  /// on this backend).
  RuntimeFilterBank(QueryState* query_state,
      const boost::unordered_map<int32_t, int>& produced_filter_counts,
      long total_filter_mem_required);

  // Define destructor in runtime-filter-bank.cc so that we can compile with only a
  // forward declaration of RuntimeFilter.
  ~RuntimeFilterBank();

  /// Initialize 'buffer_pool_client_' and claim the initial reservation. The client is
  /// automatically cleaned up in Close(). Should not be called if the client is already
  /// open.
  ///
  /// Must return the initial reservation to QueryState::initial_reservations(), which is
  /// done automatically in Close() as long as the initial reservation is not released
  /// before Close().
  Status ClaimBufferReservation() WARN_UNUSED_RESULT;

  /// Registers a filter that will either be produced (is_producer == false) or consumed
  /// (is_producer == true) by fragments that share this QueryState. The filter
  /// bloom_filter itself is unallocated until the first call to PublishGlobalFilter().
  RuntimeFilter* RegisterFilter(const TRuntimeFilterDesc& filter_desc, bool is_producer);

  /// Updates a filter's 'bloom_filter' or 'min_max_filter' which has been produced by
  /// some operator in a local fragment instance. At most one of 'bloom_filter' and
  /// 'min_max_filter' may be non-NULL, depending on the filter's type. They may both be
  /// NULL, representing a filter that allows all rows to pass.
  void UpdateFilterFromLocal(
      int32_t filter_id, BloomFilter* bloom_filter, MinMaxFilter* min_max_filter);

  /// Makes a bloom_filter (aggregated globally from all producer fragments) available for
  /// consumption by operators that wish to use it for filtering.
  void PublishGlobalFilter(
      const PublishFilterParamsPB& params, kudu::rpc::RpcContext* context);

  /// Returns true if, according to the observed NDV in 'observed_ndv', a filter of size
  /// 'filter_size' would have an expected false-positive rate which would exceed
  /// FLAGS_max_filter_error_rate.
  bool FpRateTooHigh(int64_t filter_size, int64_t observed_ndv);

  /// Returns a bloom_filter that can be used by an operator to produce a local filter,
  /// which may then be used in UpdateFilterFromLocal(). The memory returned is owned by
  /// the RuntimeFilterBank and should not be deleted by the caller. The filter identified
  /// by 'filter_id' must have been previously registered as a 'producer' by
  /// RegisterFilter().
  ///
  /// If memory allocation for the filter fails, or if Close() has been called first,
  /// returns NULL.
  BloomFilter* AllocateScratchBloomFilter(int32_t filter_id);

  /// Returns a new MinMaxFilter. Handles memory the same as AllocateScratchBloomFilter().
  MinMaxFilter* AllocateScratchMinMaxFilter(int32_t filter_id, ColumnType type);

  /// Default hash seed to use when computing hashed values to insert into filters.
  static int32_t IR_ALWAYS_INLINE DefaultHashSeed() { return 1234; }

  /// Called to signal that the query is being cancelled. Wakes up any threads blocked
  /// waiting for filters to allow them to finish.
  void Cancel();

  /// Releases all memory allocated for BloomFilters.
  void Close();

  static const int64_t MIN_BLOOM_FILTER_SIZE = 4 * 1024;           // 4KB
  static const int64_t MAX_BLOOM_FILTER_SIZE = 512 * 1024 * 1024; // 512MB

 private:
  struct PerFilterState;

  static boost::unordered_map<int32_t, std::unique_ptr<PerFilterState>> BuildFilterMap(
      const boost::unordered_map<int32_t, int>& produced_filter_counts);

  /// Acquire locks for all filters, returning them to the caller.
  std::vector<boost::unique_lock<SpinLock>> LockAllFilters();

  /// Implementation of Cancel(). All filter locks must be held by caller.
  void CancelLocked();

  /// Data tracked for each produced filter in the filter bank.
  struct ProducedFilter {
    ProducedFilter(int pending_producers);

    /// The initial filter returned from RegisterFilter() with metadata about the filter.
    /// Initialised when RegisterFilter(is_producer=true) is called for this filter id.
    /// Not modified by producers.
    RuntimeFilter* result_filter = nullptr;

    // The expected number of instances of the filter yet to arrive, i.e. additional
    // UpdateFilterFromLocal() calls expected.
    int pending_producers;

    // A temporary filter that needs to be merged into the final filter. See
    // UpdateFilterFromLocal() for details on the algorithm for merging.
    // Only used for partitioned join filters.
    std::unique_ptr<RuntimeFilter> pending_merge_filter;
  };

  /// All state tracked for a particular filter in this filter bank. PerFilterStates are
  /// all created when the filter bank is initialized. Each filter state can be locked
  /// separately to help with scalability. Aligned so that each lock is on a separate
  /// cache line.
  struct PerFilterState {
    PerFilterState(int pending_producers);
    /// Lock protecting the structures in this PerFilterState. If multiple locks are
    /// acquired, they must be acquired in the 'filters_' map iteration order.
    SpinLock lock;

    /// State of a filter that will be produced by this filter bank.
    ProducedFilter produced_filter;

    /// The filter that is returned to consumers that call RegisterFilter(producer=false).
    /// Initialised when RegisterFilter(producer=false) is called for this filter id.
    RuntimeFilter* consumed_filter = nullptr;

    /// Contains references to all the bloom filters generated. Used in Close() to safely
    /// release all memory allocated for BloomFilters.
    vector<BloomFilter*> bloom_filters;

    /// Contains references to all the min-max filters generated. Used in Close() to
    /// safely release all memory allocated for MinMaxFilters.
    vector<MinMaxFilter*> min_max_filters;
  } CACHELINE_ALIGNED;

  /// Object pool for objects that will be freed in Close(), e.g. allocated filters.
  ObjectPool obj_pool_;

  /// Lock protecting 'num_inflight_rpcs_' and it should not be taken at the same
  /// time as runtime_filter_lock_.
  SpinLock num_inflight_rpcs_lock_;
  /// Use 'num_inflight_rpcs_' to keep track of the number of current in-flight
  /// KRPC calls to prevent the memory pointed to by a BloomFilter* being
  /// deallocated in RuntimeFilterBank::Close() before all KRPC calls have
  /// been completed.
  int32_t num_inflight_rpcs_ = 0;
  std::condition_variable_any krpcs_done_cv_;

  /// All filters produced or consumed in this bank. Not modified after construction.
  /// PerFilterState objects live in obj_pool_.
  const boost::unordered_map<int32_t, std::unique_ptr<PerFilterState>> filters_;

  /// Query state for this backend.
  QueryState* const query_state_;

  /// MemTracker to track bloom filter memory. Owned by query_state_->obj_pool() so that
  /// it will have same lifetime as rest of MemTracker tree.
  MemTracker* const filter_mem_tracker_;

  /// True iff Cancel() or Close() has been called. Writer must hold the
  /// 'PerFilterState::lock' of all filters in 'filters_'. Reader must hold at least one
  /// filter lock.
  bool cancelled_ = false;

  /// True iff Close() has been called. Used to prevent races between
  /// AllocateScratch*Filter() and Close(). Writer must hold the 'PerFilterState::lock' of
  /// all filters in 'filters_'. Reader must hold at least one filter lock.
  bool closed_ = false;

  /// Total amount of memory allocated to Bloom Filters
  RuntimeProfile::Counter* const bloom_memory_allocated_;

  /// Total amount of memory required by the bloom filters as calculated by the planner.
  const int64_t total_bloom_filter_mem_required_;

  /// Buffer pool client for the filter bank. Initialized with the required reservation
  /// in ClaimBufferReservation(). Reservations are returned to the initial reservations
  /// pool in Close().
  /// Safe to access from multiple threads concurrently because we only use thread-safe
  /// methods.
  BufferPool::ClientHandle buffer_pool_client_;

  /// This is the callback for the asynchronous rpc UpdateFilterAsync() in
  /// UpdateFilterFromLocal().
  void UpdateFilterCompleteCb(
      const kudu::rpc::RpcController* rpc_controller, const UpdateFilterResultPB* res);
};

}

#endif
