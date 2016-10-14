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
#include "runtime/types.h"
#include "util/runtime-profile.h"

#include <boost/scoped_ptr.hpp>
#include <boost/thread/lock_guard.hpp>
#include <boost/unordered_map.hpp>

namespace impala {

class BloomFilter;
class MemTracker;
class RuntimeFilter;
class RuntimeState;
class TBloomFilter;
class TRuntimeFilterDesc;
class TQueryCtx;

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
/// AllocateScratchBloomFilter(); this allows RuntimeFilterBank to manage all memory
/// associated with filters.
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
  static int32_t IR_ALWAYS_INLINE DefaultHashSeed() { return 1234; }

  /// Releases all memory allocated for BloomFilters.
  void Close();

  static const int64_t MIN_BLOOM_FILTER_SIZE = 4 * 1024;           // 4KB
  static const int64_t MAX_BLOOM_FILTER_SIZE = 512 * 1024 * 1024;  // 512MB

 private:
  /// Returns the the space (in bytes) required for a filter to achieve the configured
  /// maximum false-positive rate based on the expected NDV. If 'ndv' is -1 (i.e. no
  /// estimate is known), the default filter size is returned.
  int64_t GetFilterSizeForNdv(int64_t ndv);

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

  /// MemTracker to track Bloom filter memory.
  boost::scoped_ptr<MemTracker> filter_mem_tracker_;

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

}

#endif
