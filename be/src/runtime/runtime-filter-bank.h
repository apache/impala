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

#include <condition_variable>
#include <mutex>

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
class InListFilter;
class RuntimeFilter;
class QueryState;
class TBloomFilter;
class TNetworkAddress;
class TRuntimeFilterDesc;
class TQueryCtx;

/// Metadata about each filter required to initialize the RuntimeFilterBank for a query
/// running on a backend.
struct FilterRegistration {
  FilterRegistration(const TRuntimeFilterDesc& desc) : desc(desc) {}

  const TRuntimeFilterDesc& desc;

  // Whether or not there is a consumer of the filter on this backend.
  bool has_consumer = false;

  // The number of producers of this filter executing on the backend.
  int num_producers = 0;

  // True means this filter is planned for intermediate aggregation.
  bool need_subaggregation = false;

  // True if this Impala backend is the intermediate aggregator for this filter.
  bool is_intermediate_aggregator = false;

  // Num hosts that will send filter update to the intermediate aggregator backend
  // (including the aggregator backend itself) if 'need_subaggregation' is True.
  int num_reporting_hosts = 0;

  // ip and port of the aggregator backend.
  TNetworkAddress krpc_backend_to_report;

  // Hostname of the aggregator backend.
  std::string krpc_hostname_to_report;
};

/// RuntimeFilters are produced and consumed by plan nodes at run time to propagate
/// predicates across the plan tree dynamically. Each query backend manages its
/// filters with a RuntimeFilterBank which provides low-synchronization access to filter
/// objects and data structures.
///
/// All producers and consumers of filters must register via RegisterProducer() and
/// RegisterConsumer(). Local plan fragments update the filters by calling
/// UpdateFilterFromLocal(), with either a bloom filter, a min-max filter, or an in-list
/// filter, depending on the filter's type. The 'bloom_filter', 'min_max_filter' or
/// 'in_list_filter' that is passed into UpdateFilterFromLocal() must have been allocated
/// by AllocateScratch*Filter(); this allows RuntimeFilterBank to manage all memory
/// associated with filters.
///
/// Filters are aggregated, first locally in this RuntimeFilterBank, if there are multiple
/// producers, and then made available to consumers after PublishGlobalFilter() has been
/// called. The expected number of filters to be produced locally must be specified ahead
/// of time so that RuntimeFilterBank knows when the filter is complete.
///
/// If distributed runtime filter aggregation is enabled
/// (MAX_NUM_FILTERS_AGGREGATED_PER_HOST>1), few number of backend executors will be
/// selected as intermediate filter aggregator to help coordinator. Besides doing
/// local aggregation, each intermediate aggregator will also listen and aggregate
/// filter updates from at most MAX_NUM_FILTERS_AGGREGATED_PER_HOST-1 other executors.
/// Intermediate aggregator then sends the aggregated filter update to coordinator for
/// final aggregation and publishing.
///
/// After PublishGlobalFilter() has been called (at most once per filter_id), the
/// RuntimeFilter object associated with filter_id will have a valid bloom_filter,
/// min_max_filter or in_list_filter, and may be used for filter evaluation. This
/// operation occurs without synchronisation, and neither the thread that calls
/// PublishGlobalFilter() nor the thread that may call RuntimeFilter::Eval() need to
/// coordinate in any way.
class RuntimeFilterBank {
 public:
  /// 'filters': contains an entry for every filter produced or consumed on this backend.
  RuntimeFilterBank(QueryState* query_state,
      const boost::unordered_map<int32_t, FilterRegistration>& filters,
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

  /// Registers a producer of a filter. The filter must have been registered when
  /// constructing the RuntimeFilterBank. The storage for the filter is not allocated;
  /// the caller must call AllocateScratch*Filter() to allocate the actual filter.
  RuntimeFilter* RegisterProducer(const TRuntimeFilterDesc& filter_desc);

  /// Registers a consumer of a filter. The filter must have been registered when
  /// constructing the RuntimeFilterBank. The consumer can use the returned RuntimeFilter
  /// to check for the filter's arrival.
  RuntimeFilter* RegisterConsumer(const TRuntimeFilterDesc& filter_desc);

  /// Updates a filter's 'bloom_filter', 'min_max_filter' or 'in_list_filter' which has
  /// been produced by some operator in a local fragment instance. At most one of
  /// 'bloom_filter', 'min_max_filter' and 'in_list_filter' may be non-NULL, depending on
  /// the filter's type. They may both be NULL, representing a filter that allows all rows
  /// to pass.
  void UpdateFilterFromLocal(int32_t filter_id, BloomFilter* bloom_filter,
      MinMaxFilter* min_max_filter, InListFilter* in_list_filter);

  /// Update filter received from remote hosts.
  void UpdateFilterFromRemote(
      const UpdateFilterParamsPB& params, kudu::rpc::RpcContext* context);

  /// Makes a filter (aggregated globally from all producer fragments) available for
  /// consumption by operators that wish to use it for filtering.
  void PublishGlobalFilter(
      const PublishFilterParamsPB& params, kudu::rpc::RpcContext* context);

  /// Returns a bloom_filter that can be used by an operator to produce a local filter,
  /// which may then be used in UpdateFilterFromLocal(). The memory returned is owned by
  /// the RuntimeFilterBank and should not be deleted by the caller. The filter identified
  /// by 'filter_id' must have been previously registered by RegisterProducer().
  ///
  /// If memory allocation for the filter fails, or if Close() has been called first,
  /// returns NULL.
  BloomFilter* AllocateScratchBloomFilter(int32_t filter_id);

  /// Returns a new MinMaxFilter. Handles memory the same as AllocateScratchBloomFilter().
  MinMaxFilter* AllocateScratchMinMaxFilter(int32_t filter_id, ColumnType type);

  /// Returns a new InListFilter. Handles memory the same as AllocateScratchBloomFilter().
  InListFilter* AllocateScratchInListFilter(int32_t filter_id, ColumnType type);

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
      const boost::unordered_map<int32_t, FilterRegistration>& filters,
      ObjectPool* obj_pool);

  /// Acquire locks for all filters, returning them to the caller.
  std::vector<std::unique_lock<SpinLock>> LockAllFilters();

  /// For each filter, acquire its lock and check whether aggregation has completed or
  /// not. If there is a runtime filter has not completed aggregation, unlock and wait
  /// for RUNTIME_FILTER_WAIT_TIME_MS for aggregation to happen for that incomplete
  /// filter. If aggregation still does not happen, send the incomplete filter to
  /// Coordinator.
  void SendIncompleteFilters();

  /// Implementation of Cancel(). All filter locks must be held by caller.
  void CancelLocked();

  /// Wait for in-flight RPC to complete.
  void WaitForInFlightRpc();

  /// Data tracked for each produced filter in the filter bank.
  struct ProducedFilter {
    ProducedFilter(
        int pending_producers, int pending_remotes, RuntimeFilter* result_filter);

    /// The initial filter returned from RegisterProducer() metadata about the filter.
    /// Not modified by producers. Owned by 'obj_pool_'.
    RuntimeFilter* const result_filter;

    /// The expected number of instances of the filter yet to arrive, i.e. additional
    /// UpdateFilterFromLocal() calls expected.
    int pending_producers;

    /// The expected number of remote host to send their filter update to this backend.
    int pending_remotes;

    /// Total number of all producers for this ProducedFilter.
    int total_producers;

    /// Pointer to runtime filter that holds the merge result of all remote updates.
    std::unique_ptr<RuntimeFilter> pending_remote_filter;

    /// A temporary filter that needs to be merged into the final filter. See
    /// UpdateFilterFromLocal() for details on the algorithm for merging.
    /// Only used for partitioned join filters.
    std::unique_ptr<RuntimeFilter> pending_merge_filter;

    /// Return number of remaining filter producers, both remote and local.
    inline int AllRemainingProducers() { return pending_remotes + pending_producers; }

    /// Return true if no filter update has been received.
    inline bool IsEmpty() {
      return pending_remotes + pending_producers == total_producers;
    }

    /// Return true if all filter updates have been received.
    inline bool IsComplete() { return pending_remotes + pending_producers <= 0; }

    /// Return number of filter updates that have been received.
    inline int ReceivedUpdate() {
      return total_producers - (pending_remotes + pending_producers);
    }
  };

  /// All state tracked for a particular filter in this filter bank. PerFilterStates are
  /// all created when the filter bank is initialized. Each filter state can be locked
  /// separately to help with scalability. Aligned so that each lock is on a separate
  /// cache line.
  struct PerFilterState {
    /// pending_producers: the number of producers that will call UpdateFilterFromLocal().
    /// result_filter: the initial filter that will be returned to producers. Non-NULL if
    ///   there are any producers. Must be owned by 'obj_pool_'.
    /// consumed_filter: the filter that will be returned to consumers. Non-NULL if there
    ///   are any consumers. Must be owned by 'obj_pool_'.
    PerFilterState(int pending_producers, int pending_remotes,
        RuntimeFilter* result_filter, RuntimeFilter* consumed_filter);

    /// Lock protecting the structures in this PerFilterState. If multiple locks are
    /// acquired, they must be acquired in the 'filters_' map iteration order.
    SpinLock lock;

    /// State of a filter that will be produced by this filter bank.
    ProducedFilter produced_filter;

    /// The filter that is returned to consumers that call RegisterConsumer().
    /// Initialised in the constructor if there are consumer filters on this backend.
    ///
    /// For broadcast joins, SetFilter() must be called while holding 'lock' and after
    /// checking HasFilter() to avoid SetFilter() being called multiple times for
    /// broadcast join filters.
    RuntimeFilter* const consumed_filter;

    /// Contains references to all the bloom filters generated. Used in Close() to safely
    /// release all memory allocated for BloomFilters.
    vector<BloomFilter*> bloom_filters;

    /// Contains references to all the min-max filters generated. Used in Close() to
    /// safely release all memory allocated for MinMaxFilters.
    vector<MinMaxFilter*> min_max_filters;

    /// Contains references to all the in-list filters generated. Used in Close() to
    /// safely release all memory allocated for InListFilters.
    vector<InListFilter*> in_list_filters;
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
  /// filter lock to read/write.
  bool cancelled_ = false;

  /// True iff Close() has been called. Used to prevent races between
  /// AllocateScratch*Filter() and Close(). Writer must hold the 'PerFilterState::lock' of
  /// all filters in 'filters_'. Reader must hold at least one filter lock.
  bool closed_ = false;

  /// Total amount of memory allocated to Bloom Filters
  RuntimeProfile::Counter* const bloom_memory_allocated_;

  /// Total number of items of all in-list filters.
  RuntimeProfile::Counter* const total_in_list_filter_items_;

  /// Total amount of memory required by the bloom filters as calculated by the planner.
  const int64_t total_bloom_filter_mem_required_;

  /// Buffer pool client for the filter bank. Initialized with the required reservation
  /// in ClaimBufferReservation(). Reservations are returned to the initial reservations
  /// pool in Close().
  /// Safe to access from multiple threads concurrently because we only use thread-safe
  /// methods.
  BufferPool::ClientHandle buffer_pool_client_;

  /// Combine both 'pending_merge_filter' and 'pending_remote_filter' into
  /// 'result_filter'. 'pending_merge_filter' and 'pending_remote_filter' will be
  /// discarded after this function call. Only valid to call if
  /// produced_filter.result_filter->is_intermediate_aggregator() == true.
  void CombinePeerAndLocalUpdates(
      std::unique_lock<SpinLock>* lock, ProducedFilter& produced_filter);

  /// Distribute 'complete_filter' to local and/or remote target.
  /// Caller must hold 'lock' (which is over 'PerFilterState.lock').
  void DistributeCompleteFilter(std::unique_lock<SpinLock>* lock, PerFilterState* fs,
      RuntimeFilter* complete_filter);

  /// This is the callback for the asynchronous rpc UpdateFilterAsync() in
  /// UpdateFilterFromLocal().
  void UpdateFilterCompleteCb(const kudu::rpc::RpcController* rpc_controller,
      const UpdateFilterResultPB* res, bool is_remote_update);

  /// A locked implementation of AllocateScratchBloomFilter().
  BloomFilter* AllocateScratchBloomFilterLocked(
      std::unique_lock<SpinLock>* lock, PerFilterState* fs);

  /// Disable a bloom filter by replacing it with an ALWAYS_TRUE_FILTER.
  /// Return a pointer to the new runtime filter.
  RuntimeFilter* DisableBloomFilter(std::unique_ptr<RuntimeFilter>& bloom_filter);

  int32_t GetRuntimeFilterWaitTime() const;
};

}

#endif
