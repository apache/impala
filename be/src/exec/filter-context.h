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


#ifndef IMPALA_EXEC_FILTER_CONTEXT_H
#define IMPALA_EXEC_FILTER_CONTEXT_H

#include <boost/unordered_map.hpp>
#include <gutil/strings/substitute.h>

#include "exprs/expr-context.h"
#include "util/runtime-profile.h"

namespace impala {

class BloomFilter;
class RuntimeFilter;

/// Container struct for per-filter statistics, with statistics for each granularity of
/// set of rows to which a Runtimefilter might be applied. Common groupings are "Rows",
/// "Files" and "Splits".
/// This class must be (and is) thread-safe to copy after all RegisterCounterGroup()
/// calls are finished.
class FilterStats {
 public:
  /// Constructs a new FilterStats object with a profile that is a child of
  /// 'profile'. 'is_partition_filter' determines whether partition-level counters are
  /// registered.
  FilterStats(RuntimeProfile* runtime_profile, bool is_partition_filter);

  static const std::string ROW_GROUPS_KEY;
  static const std::string FILES_KEY;
  static const std::string SPLITS_KEY;
  static const std::string ROWS_KEY;

  struct CounterGroup {
    /// Total that could have been filtered.
    RuntimeProfile::Counter* total;

    /// Total that filter was applied to.
    RuntimeProfile::Counter* processed;

    /// Total the filter rejected.
    RuntimeProfile::Counter* rejected;
  };

  /// Increment the counters for CounterGroup with key 'key'.
  /// Thread safe as long as there are no concurrent calls to RegisterCounterGroup().
  void IncrCounters(const std::string& key, int32_t total, int32_t processed,
      int32_t rejected) const;

  /// Adds a new counter group with key 'key'. Not thread safe.
  void RegisterCounterGroup(const std::string& key);

 private:
  /// Map from some key to statistics for that key.
  typedef boost::unordered_map<std::string, CounterGroup> CountersMap;
  CountersMap counters;

  /// Runtime profile to which counters are added. Owned by runtime state's object pool.
  RuntimeProfile* profile;
};

/// FilterContext contains all metadata for a single runtime filter, and allows the filter
/// to be applied in the context of a single thread.
struct FilterContext {
  /// Expression which produces a value to test against the runtime filter.
  ExprContext* expr;

  /// Cache of filter from runtime filter bank.
  const RuntimeFilter* filter;

  /// Statistics for this filter, owned by object pool.
  FilterStats* stats;

  /// Working copy of local bloom filter
  BloomFilter* local_bloom_filter;

  /// Clones this FilterContext for use in a multi-threaded context (i.e. by scanner
  /// threads).
  Status CloneFrom(const FilterContext& from, RuntimeState* state);

  FilterContext()
      : expr(NULL), filter(NULL), local_bloom_filter(NULL) { }
};

}

#endif
