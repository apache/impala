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
#include "exprs/scalar-expr-evaluator.h"
#include "util/runtime-profile.h"

namespace impala {

class BloomFilter;
class LlvmCodeGen;
class RuntimeFilter;
class ScalarExpr;
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

  /// Evaluates 'row' with 'expr_eval' and hashes the resulting value.
  /// The hash value is then used for setting some bits in 'local_bloom_filter'.
  void Insert(TupleRow* row) const noexcept;

  /// Codegen Eval() by codegen'ing the expression 'filter_expr' and replacing the type
  /// argument to RuntimeFilter::Eval() with a constant. On success, 'fn' is set to
  /// the generated function. On failure, an error status is returned.
  static Status CodegenEval(LlvmCodeGen* codegen, ScalarExpr* filter_expr,
      llvm::Function** fn) WARN_UNUSED_RESULT;

  /// Codegen Insert() by codegen'ing the expression 'filter_expr', replacing the type
  /// argument to RawValue::GetHashValue() with a constant, and calling into the correct
  /// version of BloomFilter::Insert(), depending on the presence of AVX.  On success,
  /// 'fn' is set to the generated function. On failure, an error status is returned.
  static Status CodegenInsert(LlvmCodeGen* codegen, ScalarExpr* filter_expr,
      llvm::Function** fn) WARN_UNUSED_RESULT;

  // Returns if there is any always_false filter in ctxs. If there is, the counter stats
  // is updated.
  static bool CheckForAlwaysFalse(const std::string& stats_name,
      const std::vector<FilterContext>& ctxs);
};

}

#endif
