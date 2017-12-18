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


#ifndef IMPALA_RUNTIME_RUNTIME_FILTER_H
#define IMPALA_RUNTIME_RUNTIME_FILTER_H

#include "runtime/raw-value.h"
#include "runtime/runtime-filter-bank.h"
#include "util/bloom-filter.h"
#include "util/spinlock.h"
#include "util/time.h"

namespace impala {

class BloomFilter;

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
      : bloom_filter_(nullptr), min_max_filter_(nullptr), filter_desc_(filter),
        registration_time_(MonotonicMillis()), arrival_time_(0L),
        filter_size_(filter_size) {
    DCHECK(filter_desc_.type == TRuntimeFilterType::MIN_MAX || filter_size_ > 0);
  }

  /// Returns true if SetFilter() has been called.
  bool HasFilter() const { return arrival_time_.Load() != 0; }

  const TRuntimeFilterDesc& filter_desc() const { return filter_desc_; }
  int32_t id() const { return filter_desc().filter_id; }
  int64_t filter_size() const { return filter_size_; }
  ColumnType type() const {
    return ColumnType::FromThrift(filter_desc().src_expr.nodes[0].type);
  }
  bool is_bloom_filter() const { return filter_desc().type == TRuntimeFilterType::BLOOM; }
  bool is_min_max_filter() const {
    return filter_desc().type == TRuntimeFilterType::MIN_MAX;
  }

  MinMaxFilter* get_min_max() const { return min_max_filter_.Load(); }

  /// Sets the internal filter bloom_filter to 'bloom_filter'. Can only legally be called
  /// once per filter. Does not acquire the memory associated with 'bloom_filter'.
  inline void SetFilter(BloomFilter* bloom_filter, MinMaxFilter* min_max_filter);

  /// Returns false iff 'bloom_filter_' has been set via SetBloomFilter() and hash[val] is
  /// not in that 'bloom_filter_'. Otherwise returns true. Is safe to call concurrently
  /// with SetBloomFilter(). 'val' is a value derived from evaluating a tuple row against
  /// the expression of the owning filter context. 'col_type' is the value's type.
  /// Inlined in IR so that the constant 'col_type' can be propagated.
  bool IR_ALWAYS_INLINE Eval(void* val, const ColumnType& col_type) const noexcept;

  /// Returns the amount of time waited since registration for the filter to
  /// arrive. Returns 0 if filter has not yet arrived.
  int32_t arrival_delay() const {
    if (arrival_time_.Load() == 0L) return 0L;
    return arrival_time_.Load() - registration_time_;
  }

  /// Periodically (every 20ms) checks to see if the global filter has arrived. Waits for
  /// a maximum of timeout_ms before returning. Returns true if the filter has arrived,
  /// false otherwise.
  bool WaitForArrival(int32_t timeout_ms) const;

  /// Returns true if the filter returns true/false for all elements, i.e. Eval(v) returns
  /// true/false for all v.
  inline bool AlwaysTrue() const;
  inline bool AlwaysFalse() const;

  /// Frequency with which to check for filter arrival in WaitForArrival()
  static const int SLEEP_PERIOD_MS;

  /// Class name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 private:
  /// Membership bloom_filter. May be NULL even after arrival_time_ is set, meaning that
  /// it does not filter any rows, either because it was not created
  /// (filter_desc_.bloom_filter is false), there was not enough memory, or the false
  /// positive rate was determined to be too high.
  AtomicPtr<BloomFilter> bloom_filter_;

  /// May be NULL even after arrival_time_ is set if filter_desc_.min_max_filter is false.
  AtomicPtr<MinMaxFilter> min_max_filter_;

  /// Reference to the filter's thrift descriptor in the thrift Plan tree.
  const TRuntimeFilterDesc& filter_desc_;

  /// Time, in ms, that the filter was registered.
  const int64_t registration_time_;

  /// Time, in ms, that the global filter arrived. Set in SetFilter().
  AtomicInt64 arrival_time_;

  /// The size of the Bloom filter, in bytes.
  const int64_t filter_size_;
};

}

#endif
