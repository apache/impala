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


#ifndef IMPALA_EXEC_FILTER_CONTEXT_H
#define IMPALA_EXEC_FILTER_CONTEXT_H

#include <boost/unordered_map.hpp>
#include "runtime/runtime-filter.h"
#include "util/runtime-profile.h"

namespace llvm {
class Function;
}

namespace impala {

class BloomFilter;
class LlvmCodeGen;
class MinMaxFilter;
class RuntimeState;
class ScalarExpr;
class ScalarExprEvaluator;
class TupleRow;

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
  FilterStats(RuntimeProfile* runtime_profile);

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
  /// Evaluator for 'expr'. This field is referenced in generated code so if the order
  /// of it changes inside this struct, please update CodegenEval().
  ScalarExprEvaluator* expr_eval = nullptr;

  /// Cache of filter from runtime filter bank.
  /// The field is referenced in generated code so if the order of it changes
  /// inside this struct, please update CodegenEval().
  const RuntimeFilter* filter = nullptr;

  /// Statistics for this filter, owned by object pool.
  FilterStats* stats = nullptr;

  /// Working copy of local bloom filter
  BloomFilter* local_bloom_filter = nullptr;

  /// Working copy of local min-max filter
  MinMaxFilter* local_min_max_filter = nullptr;

  /// Working copy of local in-list filter
  InListFilter* local_in_list_filter = nullptr;

  /// Struct name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

  /// Clones this FilterContext for use in a multi-threaded context (i.e. by scanner
  /// threads).
  Status CloneFrom(const FilterContext& from, ObjectPool* pool, RuntimeState* state,
      MemPool* expr_perm_pool, MemPool* expr_results_pool);

  /// Evaluates 'row' with 'expr_eval' with the resulting value being checked
  /// against runtime filter 'filter' for matches. Returns true if 'row' finds
  /// a match in 'filter'. Returns false otherwise.
  bool Eval(TupleRow* row) const noexcept;

  /// Evaluates 'row' with 'expr_eval' and inserts the value into 'local_bloom_filter'
  /// or 'local_min_max_filter' as appropriate.
  void Insert(TupleRow* row) const noexcept;

  /// Implements different flavors of insertion based on filter type and comparison
  /// op in filter desc.
  ///  1). When the op is EQ, regardless of filter type, call this->Insert(TupleRow* row);
  ///  2). When the op is LE, LT, GE or GT and the filter type is min/max, call
  //       MinMaxFilter::InsertFor<op>(TupleRow* row);
  ///  3). DCHECK(false) otherwise.
  void InsertPerCompareOp(TupleRow* row) const noexcept;

  /// Materialize filter values by copying any values stored by filters into memory owned
  /// by the filter. Filters may assume that the memory for Insert()-ed values stays valid
  /// until this is called.
  void MaterializeValues() const;

  /// Codegen Eval() by codegen'ing the expression 'filter_expr' and replacing the type
  /// argument to RuntimeFilter::Eval() with a constant. On success, 'fn' is set to
  /// the generated function. On failure, an error status is returned.
  static Status CodegenEval(LlvmCodeGen* codegen, ScalarExpr* filter_expr,
      llvm::Function** fn) WARN_UNUSED_RESULT;

  /// Codegen Insert() by codegen'ing the expression 'filter_expr', replacing the type
  /// argument to RawValue::GetHashValue() with a constant, and calling into the correct
  /// version of BloomFilter::Insert() or MinMaxFilter::Insert(), depending on the filter
  /// desc and if 'local_bloom_filter' or 'local_min_max_filter' are null.
  /// For bloom filters, it also selects the correct Insert() based on the presence of
  /// AVX, and for min-max filters it selects the correct Insert() based on type.
  /// On success, 'fn' is set to the generated function. On failure, an error status is
  /// returned.
  static Status CodegenInsert(LlvmCodeGen* codegen, ScalarExpr* filter_expr,
      const TRuntimeFilterDesc& filter_desc, llvm::Function** fn) WARN_UNUSED_RESULT;

  // Returns if there is any always_false filter in ctxs. If there is, the counter stats
  // is updated.
  static bool CheckForAlwaysFalse(const std::string& stats_name,
      const std::vector<FilterContext>& ctxs);

  /// Returns true if 'filter' is a min-max filter and whose range overlaps enough
  /// with the range defined by the column low and high values in 'desc'. Return false
  /// otherwise. The degree of the overlap is determined by the overlap ratio and the
  /// 'threshold' (query option 'minmax_filter_threshold') that is used as a lower bound
  /// for the ratio.
  static bool ShouldRejectFilterBasedOnColumnStats(const TRuntimeFilterTargetDesc& desc,
      MinMaxFilter* minmax_filter, float threshold);
};

}

#endif
