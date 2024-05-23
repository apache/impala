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

#include <cstdint>
#include <memory>
#include <vector>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>

#include "codegen/impala-ir.h"
#include "common/compiler-util.h"
#include "common/logging.h"
#include "exprs/scalar-expr.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/suballocator.h"
#include "runtime/tuple-row.h"
#include "util/bitmap.h"
#include "util/hash-util.h"
#include "util/runtime-profile.h"
#include "util/tagged-ptr.h"

namespace llvm {
  class Function;
}

namespace impala {

class CodegenAnyVal;
class LlvmCodeGen;
class MemTracker;
class RowDescriptor;
class RuntimeState;
class ScalarExpr;
class ScalarExprEvaluator;
class Tuple;
class TupleRow;
class HashTable;
struct HashTableStatsProfile;
struct ScalarExprsResultsRowLayout;

/// Linear or quadratic probing hash table implementation tailored to the usage pattern
/// for partitioned hash aggregation and hash joins. The hash table stores TupleRows and
/// allows for different exprs for insertions and finds. This is the pattern we use for
/// joins and aggregation where the input/build tuple row descriptor is different from the
/// find/probe descriptor. The implementation is designed to allow codegen for some paths.
//
/// In addition to the hash table there is also an accompanying hash table context that is
/// used for insertions and probes. For example, the hash table context stores evaluated
/// expr results for the current row being processed when possible into a contiguous
/// memory buffer. This allows for efficient hash computation.
//
/// The hash table does not support removes. The table is optimized for the partition hash
/// aggregation and hash joins and is not intended to be a generic hash table
/// implementation.
///
/// Operations that mutate the hash table are not thread-safe. Read-only access to the
/// hash table, is, however, thread safe: multiple threads, each with their own
/// HashTableCtx, can safely look up rows in the hash table with FindProbeRow(), etc so
/// long as no thread is mutating the hash table. Thread-safe methods are explicitly
/// documented.
///
/// The data (rows) are stored in a BufferedTupleStream. The basic data structure of this
/// hash table is a vector of buckets. The buckets (indexed by the mod of the hash)
/// contain a pointer to either the slot in the tuple-stream or in case of duplicate
/// values, to the head of a linked list of nodes that in turn contain a pointer to
/// tuple-stream slots. When inserting an entry we start at the bucket at position
/// (hash % size) and search for either a bucket with the same hash or for an empty
/// bucket. If a bucket with the same hash is found, we then compare for row equality and
/// either insert a duplicate node if the equality is true, or continue the search if the
/// row equality is false. Similarly, when probing we start from the bucket at position
/// (hash % size) and search for an entry with the same hash or for an empty bucket.
/// In the former case, we then check for row equality and continue the search if the row
/// equality is false. In the latter case, the probe is not successful. When growing the
/// hash table, the number of buckets is doubled. We trigger a resize when the fill
/// factor is approx 75%. Due to the doubling nature of the buckets, we require that the
/// number of buckets is a power of 2. This allows us to perform a modulo of the hash
/// using a bitmask.
///
/// We choose to use linear or quadratic probing because they exhibit good (predictable)
/// cache behavior.
///
/// The first NUM_SMALL_BLOCKS of nodes_ are made of blocks less than the IO size (of 8MB)
/// to reduce the memory footprint of small queries.
///
/// TODO: Compare linear and quadratic probing and remove the loser.
/// TODO: We currently use 32-bit hashes. There is room in the bucket structure for at
/// least 48-bits. We should exploit this space.
/// TODO: Consider capping the probes with a threshold value. If an insert reaches
/// that threshold it is inserted to another linked list of overflow entries.
/// TODO: Smarter resizes, and perhaps avoid using powers of 2 as the hash table size.
/// TODO: this is not a fancy hash table in terms of memory access patterns
/// (cuckoo-hashing or something that spills to disk). We will likely want to invest
/// more time into this.
/// TODO: hash-join and aggregation have very different access patterns.  Joins insert all
/// the rows and then calls scan to find them.  Aggregation interleaves FindProbeRow() and
/// Inserts().  We may want to optimize joins more heavily for Inserts() (in particular
/// growing).
/// TODO: Batched interface for inserts and finds.
/// TODO: as an optimization, compute variable-length data size for the agg node.

/// Collection of variables required to create instances of HashTableCtx and to codegen
/// hash table methods.
struct HashTableConfig {
  HashTableConfig() = delete;
  HashTableConfig(const std::vector<ScalarExpr*>& build_exprs,
      const std::vector<ScalarExpr*>& probe_exprs, const bool stores_nulls,
      const std::vector<bool>& finds_nulls);

  /// The exprs used to evaluate rows for inserting rows into hash table.
  /// Also used when matching hash table entries against probe rows. Not Owned.
  const std::vector<ScalarExpr*>& build_exprs;

  /// The exprs used to evaluate rows for look-up in the hash table. Not Owned.
  const std::vector<ScalarExpr*>& probe_exprs;

  /// If false, TupleRows with nulls are ignored during Insert
  const bool stores_nulls;

  /// if finds_nulls[i] is false, FindProbeRow() return BUCKET_NOT_FOUND for TupleRows
  /// with nulls in position i even if stores_nulls is true.
  const std::vector<bool> finds_nulls;

  /// finds_some_nulls_ is just the logical OR of finds_nulls_.
  const bool finds_some_nulls;

  /// The memory efficient layout for storing the results of evaluating build expressions.
  const ScalarExprsResultsRowLayout build_exprs_results_row_layout;
};

/// Control block for a hash table. This class contains the logic as well as the variables
/// needed by a thread to operate on a hash table.
class HashTableCtx {
 public:
  /// Create a hash table context with the specified parameters, invoke Init() to
  /// initialize the new hash table context and return it in 'ht_ctx'. Expression
  /// evaluators for the build and probe expressions will also be allocated.
  /// Please see the comments of HashTableCtx constructor and Init() for details
  /// of other parameters.
  static Status Create(ObjectPool* pool, RuntimeState* state,
      const std::vector<ScalarExpr*>& build_exprs,
      const std::vector<ScalarExpr*>& probe_exprs, bool stores_nulls,
      const std::vector<bool>& finds_nulls, int32_t initial_seed, int max_levels,
      int num_build_tuples, MemPool* expr_perm_pool, MemPool* build_expr_results_pool,
      MemPool* probe_expr_results_pool, boost::scoped_ptr<HashTableCtx>* ht_ctx);

  static Status Create(ObjectPool* pool, RuntimeState* state,
      const HashTableConfig& config, int32_t initial_seed, int max_levels,
      int num_build_tuples, MemPool* expr_perm_pool, MemPool* build_expr_results_pool,
      MemPool* probe_expr_results_pool, boost::scoped_ptr<HashTableCtx>* ht_ctx);

  /// Initialize the build and probe expression evaluators.
  Status Open(RuntimeState* state);

  /// Call to cleanup any resources allocated by the expression evaluators.
  void Close(RuntimeState* state);

  /// Update and print some statistics that can be used for performance debugging.
  std::string PrintStats() const;

  /// Add operations stats of this hash table context to the counters in profile.
  /// This method should only be called once for each context and be called during
  /// closing the owner object of the context. Not all the counters are added with the
  /// method, only counters for Probes, travels and collisions are affected.
  void StatsCountersAdd(HashTableStatsProfile* profile);

  void set_level(int level);

  int ALWAYS_INLINE level() const { return level_; }

  uint32_t ALWAYS_INLINE seed(int level) { return seeds_.at(level); }

  TupleRow* ALWAYS_INLINE scratch_row() const { return scratch_row_; }

  /// Returns the results of the expression at 'expr_idx' evaluated at the current row.
  /// This value is invalid if the expr evaluated to NULL.
  /// TODO: this is an awkward abstraction but aggregation node can take advantage of
  /// it and save some expr evaluation calls.
  void* ALWAYS_INLINE ExprValue(int expr_idx) const {
    return expr_values_cache_.ExprValuePtr(
        expr_values_cache_.cur_expr_values(), expr_idx);
  }

  /// Returns if the expression at 'expr_idx' is evaluated to NULL for the current row.
  bool ALWAYS_INLINE ExprValueNull(int expr_idx) const {
    return static_cast<bool>(*(expr_values_cache_.cur_expr_values_null() + expr_idx));
  }

  /// Evaluate and hash the build/probe row, saving the evaluation to the current row of
  /// the ExprValuesCache in this hash table context: the results are saved in
  /// 'cur_expr_values_', the nullness of expressions values in 'cur_expr_values_null_',
  /// and the hashed expression values in 'cur_expr_values_hash_'. Returns false if this
  /// row should be rejected  (doesn't need to be processed further) because it contains
  /// NULL. These need to be inlined in the IR module so we can find and replace the
  /// calls to EvalBuildRow()/EvalProbeRow().
  bool IR_ALWAYS_INLINE EvalAndHashBuild(const TupleRow* row);
  bool IR_ALWAYS_INLINE EvalAndHashProbe(const TupleRow* row);

  /// Codegen for evaluating a tuple row. Codegen'd function matches the signature
  /// for EvalBuildRow and EvalTupleRow.
  /// If build_row is true, the codegen uses the build_exprs, otherwise the probe_exprs.
  static Status CodegenEvalRow(LlvmCodeGen* codegen, bool build_row,
      const HashTableConfig& config, llvm::Function** fn);

  /// Codegen for evaluating a TupleRow and comparing equality. Function signature
  /// matches HashTable::Equals(). 'inclusive_equality' is true if the generated
  /// equality function should treat all NULLs as equal and all NaNs as equal.
  /// See the template parameter to HashTable::Equals().
  static Status CodegenEquals(LlvmCodeGen* codegen, bool inclusive_equality,
      const HashTableConfig& config, llvm::Function** fn);

  /// Codegen for hashing expr values. Function prototype matches HashRow identically.
  /// Unlike HashRow(), the returned function only uses a single hash function, rather
  /// than switching based on level_. If 'use_murmur' is true, murmur hash is used,
  /// otherwise CRC is used if the hardware supports it (see hash-util.h).
  static Status CodegenHashRow(LlvmCodeGen* codegen, bool use_murmur,
      const HashTableConfig& config, llvm::Function** fn);

  /// Struct that returns the number of constants replaced by ReplaceConstants().
  struct HashTableReplacedConstants {
    int stores_nulls;
    int finds_some_nulls;
    int stores_tuples;
    int stores_duplicates;
    int quadratic_probing;
  };

  /// Replace hash table parameters with constants in 'fn'. Updates 'replacement_counts'
  /// with the number of replacements made. 'num_build_tuples' and 'stores_duplicates'
  /// correspond to HashTable parameters with the same name.
  static Status ReplaceHashTableConstants(LlvmCodeGen* codegen,
      const HashTableConfig& config, bool stores_duplicates, int num_build_tuples,
      llvm::Function* fn, HashTableReplacedConstants* replacement_counts);

  static const char* LLVM_CLASS_NAME;

  /// To enable prefetching, the hash table building and probing are pipelined by the
  /// exec nodes. A set of rows in a row batch will be evaluated and hashed first and
  /// the corresponding hash table buckets are prefetched before they are probed against
  /// the hash table. ExprValuesCache is a container for caching the results of
  /// expressions evaluations for the rows in a prefetch set to avoid re-evaluating the
  /// rows again during probing. Expressions evaluation can be very expensive.
  ///
  /// The expression evaluation results are cached in the following data structures:
  ///
  /// - 'expr_values_array_' is an array caching the results of the rows
  /// evaluated against either the build or probe expressions. 'cur_expr_values_'
  /// is a pointer into this array.
  /// - 'expr_values_null_array_' is an array caching the nullness of each evaluated
  /// expression in each row. 'cur_expr_values_null_' is a pointer into this array.
  /// - 'expr_values_hash_array_' is an array of cached hash values of the rows.
  /// 'cur_expr_values_hash_' is a pointer into this array.
  /// - 'null_bitmap_' is a bitmap which indicates rows evaluated to NULL.
  ///
  /// ExprValuesCache provides an iterator like interface for performing a write pass
  /// followed by a read pass. We refrain from providing an interface for random accesses
  /// as there isn't a use case for it now and we want to avoid expensive multiplication
  /// as the buffer size of each row is not necessarily power of two:
  /// - Reset(), ResetForRead(): reset the iterators before writing / reading cached
  /// values.
  /// - NextRow(): moves the iterators to point to the next row of cached values.
  /// - AtEnd(): returns true if all cached rows have been read. Valid in read mode only.
  ///
  /// Various metadata information such as layout of results buffer is also stored in
  /// this class. Note that the result buffer doesn't store variable length data. It only
  /// contains pointers to the variable length data (e.g. if an expression value is a
  /// StringValue).
  ///
  class ExprValuesCache {
   public:
    ExprValuesCache();

    /// Allocates memory and initializes various data structures. Return error status
    /// if memory allocation leads to the memory limits of the exec node to be exceeded.
    /// 'tracker' is the memory tracker of the exec node which owns this HashTableCtx.
    Status Init(RuntimeState* state, MemTracker* tracker,
        const ScalarExprsResultsRowLayout& exprs_results_row_layout);

    /// Frees up various resources and updates memory tracker with proper accounting.
    /// 'tracker' should be the same memory tracker which was passed in for Init().
    void Close(MemTracker* tracker);

    /// Resets the cache states (iterators, end pointers etc) before writing.
    void Reset() noexcept;

    /// Resets the iterators to the start before reading. Will record the current position
    /// of the iterators in end pointer before resetting so AtEnd() can determine if all
    /// cached values have been read.
    void ResetForRead();

    /// Advances the iterators to the next row by moving to the next entries in the
    /// arrays of cached values.
    void ALWAYS_INLINE NextRow();

    /// Compute the total memory usage of this ExprValuesCache.
    static int MemUsage(int capacity, int results_buffer_size, int num_build_exprs);

    /// Returns the maximum number rows of expression values states which can be cached.
    int ALWAYS_INLINE capacity() const { return capacity_; }

    /// Returns the total size in bytes of a row of evaluated expressions' values.
    int ALWAYS_INLINE expr_values_bytes_per_row() const {
      return expr_values_bytes_per_row_;
    }

    /// Returns the offset into the result buffer of the first variable length
    /// data results.
    int ALWAYS_INLINE var_result_offset() const { return var_result_offset_; }

    /// Returns true if the current read pass is complete, meaning all cached values
    /// have been read.
    bool ALWAYS_INLINE AtEnd() const {
      return cur_expr_values_hash_ == cur_expr_values_hash_end_;
    }

    /// Returns true if the current row is null but nulls are not considered in the
    /// current phase (build or probe).
    bool ALWAYS_INLINE IsRowNull() const { return null_bitmap_.Get(CurIdx()); }

    /// Record in a bitmap that the current row is null but nulls are not considered in
    /// the current phase (build or probe).
    void ALWAYS_INLINE SetRowNull() { null_bitmap_.Set(CurIdx(), true); }

    /// Returns the hash values of the current row.
    uint32_t ALWAYS_INLINE CurExprValuesHash() const { return *cur_expr_values_hash_; }

    /// Sets the hash values for the current row.
    void ALWAYS_INLINE SetCurExprValuesHash(uint32_t hash) {
      *cur_expr_values_hash_ = hash;
    }

    /// Returns a pointer to the expression value at 'expr_idx' in 'expr_values'.
    uint8_t* ExprValuePtr(uint8_t* expr_values, int expr_idx) const;
    const uint8_t* ExprValuePtr(const uint8_t* expr_values, int expr_idx) const;

    /// Returns the current row's expression buffer. The expression values in the buffer
    /// are accessed using ExprValuePtr().
    uint8_t* ALWAYS_INLINE cur_expr_values() const { return cur_expr_values_; }

    /// Returns null indicator bytes for the current row, one per expression. Non-zero
    /// bytes mean NULL, zero bytes mean non-NULL. Indexed by the expression index.
    /// These are uint8_t instead of bool to simplify codegen with IRBuilder.
    /// TODO: is there actually a valid reason why this is necessary for codegen?
    uint8_t* ALWAYS_INLINE cur_expr_values_null() const { return cur_expr_values_null_; }

    /// Returns the offset into the results buffer of the expression value at 'expr_idx'.
    int ALWAYS_INLINE expr_values_offsets(int expr_idx) const {
      return expr_values_offsets_[expr_idx];
    }

   private:
    friend class HashTableCtx;

    /// Resets the iterators to the beginning of the cache values' arrays.
    void ResetIterators();

    /// Returns the offset in number of rows into the cached values' buffer.
    int ALWAYS_INLINE CurIdx() const {
      return cur_expr_values_hash_ - expr_values_hash_array_.get();
    }

    /// Max amount of memory in bytes for caching evaluated expression values.
    static const int MAX_EXPR_VALUES_CACHE_BYTES = 256 << 10;

    /// Maximum number of rows of expressions evaluation states which this
    /// ExprValuesCache can cache.
    int capacity_;

    /// Byte size of a row of evaluated expression values. Never changes once set,
    /// can be used for constant substitution during codegen.
    int expr_values_bytes_per_row_;

    /// Number of build/probe expressions.
    int num_exprs_;

    /// Pointer into 'expr_values_array_' for the current row's expression values.
    uint8_t* cur_expr_values_;

    /// Pointer into 'expr_values_null_array_' for the current row's nullness of each
    /// expression value.
    uint8_t* cur_expr_values_null_;

    /// Pointer into 'expr_hash_value_array_' for the hash value of current row's
    /// expression values.
    uint32_t* cur_expr_values_hash_;

    /// Pointer to the buffer one beyond the end of the last entry of cached expressions'
    /// hash values.
    uint32_t* cur_expr_values_hash_end_;

    /// Array for caching up to 'capacity_' number of rows worth of evaluated expression
    /// values. Each row consumes 'expr_values_bytes_per_row_' number of bytes.
    boost::scoped_array<uint8_t> expr_values_array_;

    /// Array for caching up to 'capacity_' number of rows worth of null booleans.
    /// Each row contains 'num_exprs_' booleans to indicate nullness of expression values.
    /// Used when the hash table supports NULL. Use 'uint8_t' to guarantee each entry is 1
    /// byte as sizeof(bool) is implementation dependent. The IR depends on this
    /// assumption.
    boost::scoped_array<uint8_t> expr_values_null_array_;

    /// Array for caching up to 'capacity_' number of rows worth of hashed values.
    boost::scoped_array<uint32_t> expr_values_hash_array_;

    /// One bit for each row. A bit is set if that row is not hashed as it's evaluated
    /// to NULL but the hash table doesn't support NULL. Such rows may still be included
    /// in outputs for certain join types (e.g. left anti joins).
    Bitmap null_bitmap_;

    /// Maps from expression index to the byte offset into a row of expression values.
    /// One entry per build/probe expression.
    std::vector<int> expr_values_offsets_;

    /// Byte offset into 'cur_expr_values_' that begins the variable length results for
    /// a row. If -1, there are no variable length slots. Never changes once set, can be
    /// constant substituted with codegen.
    int var_result_offset_;
  };

  ExprValuesCache* ALWAYS_INLINE expr_values_cache() { return &expr_values_cache_; }

 private:
  friend class HashTable;
  friend class HashTableTest_HashEmpty_Test;

  /// Construct a hash table context.
  ///  - build_exprs are the exprs that should be used to evaluate rows during Insert().
  ///  - probe_exprs are used during FindProbeRow()
  ///  - stores_nulls: if false, TupleRows with nulls are ignored during Insert
  ///  - finds_nulls: if finds_nulls[i] is false, FindProbeRow() returns BUCKET_NOT_FOUND
  ///        for TupleRows with nulls in position i even if stores_nulls is true.
  ///  - initial_seed: initial seed value to use when computing hashes for rows with
  ///        level 0. Other levels have their seeds derived from this seed.
  ///  - max_levels: the max lhashevels we will hash with.
  ///  - expr_perm_pool: the MemPool from which the expression evaluators make permanent
  ///        allocations that live until Close(). Owned by the exec node which owns this
  ///        hash table context. Memory usage of the expression value cache is charged
  ///        against this MemPool's tracker.
  ///  - build_expr_results_pool: the MemPool from which the expression evaluators make
  ///        allocations to hold expression results. Cached build expression values may
  ///        reference memory in this pool. Owned by the exec node which owns this hash
  ///        table context.
  ///  - probe_expr_results_pool: the MemPool from which the expression evaluators make
  ///        allocations to hold expression results. Cached probe expression values may
  ///        reference memory in this pool. Owned by the exec node which owns this hash
  ///        table context.
  ///
  /// TODO: stores_nulls is too coarse: for a hash table in which some columns are joined
  ///       with '<=>' and others with '=', stores_nulls could distinguish between columns
  ///       in which nulls are stored and columns in which they are not, which could save
  ///       space by not storing some rows we know will never match.
  /// TODO: remove this constructor once all client classes switch to using
  ///       HashTableConfig to create instances of this class.
  HashTableCtx(const std::vector<ScalarExpr*>& build_exprs,
      const std::vector<ScalarExpr*>& probe_exprs, bool stores_nulls,
      const std::vector<bool>& finds_nulls, int32_t initial_seed, int max_levels,
      MemPool* expr_perm_pool, MemPool* build_expr_results_pool,
      MemPool* probe_expr_results_pool);

  HashTableCtx(const HashTableConfig& config, int32_t initial_seed, int max_levels,
      MemPool* expr_perm_pool, MemPool* build_expr_results_pool,
      MemPool* probe_expr_results_pool);

  /// Allocate various buffers for storing expression evaluation results, hash values,
  /// null bits etc. Also allocate evaluators for the build and probe expressions and
  /// store them in 'pool'. Returns error if allocation causes query memory limit to
  /// be exceeded or the evaluators fail to initialize. 'num_build_tuples' is the number
  /// of tuples of a row in the build side, used for computing the size of a scratch row.
  Status Init(ObjectPool* pool, RuntimeState* state, int num_build_tuples);

  /// Compute the hash of the values in 'expr_values' with nullness 'expr_values_null'.
  /// This will be replaced by codegen.  We don't want this inlined for replacing
  /// with codegen'd functions so the function name does not change.
  uint32_t IR_NO_INLINE HashRow(
      const uint8_t* expr_values, const uint8_t* expr_values_null) const noexcept;

  /// Wrapper function for calling correct HashUtil function in non-codegen'd case.
  uint32_t Hash(const void* input, int len, uint32_t hash) const;

  /// Evaluate 'row' over build exprs, storing values into 'expr_values' and nullness
  /// into 'expr_values_null'. This will be replaced by codegen. We do not want this
  /// function inlined when cross compiled because we need to be able to differentiate
  /// between EvalBuildRow and EvalProbeRow by name and the build/probe exprs are baked
  /// into the codegen'd function.
  bool IR_NO_INLINE EvalBuildRow(
      const TupleRow* row, uint8_t* expr_values, uint8_t* expr_values_null) noexcept {
    return EvalRow(row, build_expr_evals_, expr_values, expr_values_null);
  }

  /// Evaluate 'row' over probe exprs, storing the values into 'expr_values' and nullness
  /// into 'expr_values_null'. This will be replaced by codegen.
  bool IR_NO_INLINE EvalProbeRow(
      const TupleRow* row, uint8_t* expr_values, uint8_t* expr_values_null) noexcept {
    return EvalRow(row, probe_expr_evals_, expr_values, expr_values_null);
  }

  /// Compute the hash of the values in 'expr_values' with nullness 'expr_values_null'
  /// for a row with variable length fields (e.g. strings).
  uint32_t HashVariableLenRow(
      const uint8_t* expr_values, const uint8_t* expr_values_null) const;

  /// Evaluate the exprs over row, storing the values into 'expr_values' and nullness into
  /// 'expr_values_null'. Returns whether any expr evaluated to NULL. This will be
  /// replaced by codegen.
  bool EvalRow(const TupleRow* row, const std::vector<ScalarExprEvaluator*>& evaluators,
      uint8_t* expr_values, uint8_t* expr_values_null) noexcept;

  /// Returns true if the values of build_exprs evaluated over 'build_row' equal the
  /// values in 'expr_values' with nullness 'expr_values_null'. INCLUSIVE_EQUALITY
  /// means "NULL==NULL" and "NaN==NaN". This will be replaced by codegen.
  template <bool INCLUSIVE_EQUALITY>
  bool IR_NO_INLINE Equals(const TupleRow* build_row, const uint8_t* expr_values,
      const uint8_t* expr_values_null) const noexcept;

  /// Helper function that calls Equals() with the current row. Always inlined so that
  /// it does not appear in cross-compiled IR.
  template <bool INCLUSIVE_EQUALITY>
  bool ALWAYS_INLINE Equals(const TupleRow* build_row) const {
    return Equals<INCLUSIVE_EQUALITY>(build_row, expr_values_cache_.cur_expr_values(),
        expr_values_cache_.cur_expr_values_null());
  }

  /// Cross-compiled function to access member variables used in CodegenHashRow().
  uint32_t IR_ALWAYS_INLINE GetHashSeed() const;

  /// Functions to be replaced by codegen to specialize the hash table.
  bool IR_NO_INLINE stores_nulls() const { return stores_nulls_; }
  bool IR_NO_INLINE finds_some_nulls() const { return finds_some_nulls_; }

  /// Cross-compiled function to access the build/probe expression evaluators.
  ScalarExprEvaluator* const* IR_ALWAYS_INLINE build_expr_evals() const;
  ScalarExprEvaluator* const* IR_ALWAYS_INLINE probe_expr_evals() const;

  /// The exprs used to evaluate rows for inserting rows into hash table.
  /// Also used when matching hash table entries against probe rows.
  const std::vector<ScalarExpr*>& build_exprs_;
  const ScalarExprsResultsRowLayout build_exprs_results_row_layout_;
  std::vector<ScalarExprEvaluator*> build_expr_evals_;

  /// The exprs used to evaluate rows for look-up in the hash table.
  const std::vector<ScalarExpr*>& probe_exprs_;
  std::vector<ScalarExprEvaluator*> probe_expr_evals_;

  /// Constants on how the hash table should behave. Joins and aggs have slightly
  /// different behavior.
  const bool stores_nulls_;
  const std::vector<bool> finds_nulls_;

  /// finds_some_nulls_ is just the logical OR of finds_nulls_.
  const bool finds_some_nulls_;

  /// The current level this context is working on. Each level needs to use a
  /// different seed.
  int level_;

  /// The seeds to use for hashing. Indexed by the level.
  std::vector<uint32_t> seeds_;

  /// The ExprValuesCache for caching expression evaluation results, null bytes and hash
  /// values for rows. Used to store results of batch evaluations of rows.
  ExprValuesCache expr_values_cache_;

  /// Scratch buffer to generate rows on the fly.
  TupleRow* scratch_row_;

  /// MemPool for 'build_expr_evals_' and 'probe_expr_evals_' to allocate expr-managed
  /// memory from. Not owned.
  MemPool* expr_perm_pool_;

  /// MemPools for allocations by 'build_expr_evals_' and 'probe_expr_evals_' that hold
  /// results of expr evaluation. Not owned. The owner of these pools is responsible for
  /// clearing them when results from the respective expr evaluators are no longer needed.
  MemPool* build_expr_results_pool_;
  MemPool* probe_expr_results_pool_;

  /// The stats below can be used for debugging perf.
  /// Number of Probe() calls that probe the hash table.
  int64_t num_probes_ = 0;

  /// Total distance traveled for each probe. That is the sum of the diff between the end
  /// position of a probe (find/insert) and its start position
  /// '(hash & (num_buckets_ - 1))'.
  int64_t travel_length_ = 0;

  /// The number of cases where we had to compare buckets with the same hash value, but
  /// the row equality failed.
  int64_t num_hash_collisions_ = 0;
};

/// HashTableStatsProfile encapsulates hash tables stats. It tracks the stats of all the
/// hash tables created by a node. It should be created, stored by the node, and be
/// released when the node is released.
struct HashTableStatsProfile {
  /// Profile object for HashTable Stats
  RuntimeProfile* hashtable_profile = nullptr;

  /// Number of hash collisions - unequal rows that have identical hash values
  RuntimeProfile::Counter* num_hash_collisions_ = nullptr;

  /// Number of hash table probes.
  RuntimeProfile::Counter* num_hash_probes_ = nullptr;

  /// Total distance traveled for each hash table probe.
  RuntimeProfile::Counter* num_hash_travels_ = nullptr;

  /// Number of hash table resized
  RuntimeProfile::Counter* num_hash_resizes_ = nullptr;

  /// Total number of hash buckets across all partitions.
  RuntimeProfile::Counter* num_hash_buckets_ = nullptr;

};

/// The hash table consists of a contiguous array of buckets that contain a pointer to the
/// data, the hash value and three flags: whether this bucket is filled, whether this
/// entry has been matched (used in right and full joins) and whether this entry has
/// duplicates. If there are duplicates, then the data is pointing to the head of a
/// linked list of duplicate nodes that point to the actual data. Note that the duplicate
/// nodes do not contain the hash value, because all the linked nodes have the same hash
/// value, the one in the bucket. The data is either a tuple stream index or a Tuple*.
/// This array of buckets is sparse, we are shooting for up to 3/4 fill factor (75%). The
/// data allocated by the hash table comes from the BufferPool.
class HashTable {
 private:
  /// Rows are represented as pointers into the BufferedTupleStream data with one
  /// of two formats, depending on the number of tuples in the row.
  union HtData {
    // For rows with multiple tuples per row, a pointer to the flattened TupleRow.
    BufferedTupleStream::FlatRowPtr flat_row;
    // For rows with one tuple per row, a pointer to the Tuple itself.
    Tuple* tuple;
  };

  struct DuplicateNode; // Forward Declaration
  class TaggedDuplicateNode : public TaggedPtr<DuplicateNode, false> {
   public:
    ALWAYS_INLINE bool IsMatched() { return IsTagBitSet<0>(); }
    ALWAYS_INLINE void SetMatched() { SetTagBit<0>(); }
    ALWAYS_INLINE void SetNode(DuplicateNode* node) { SetPtr(node); }
    // Set Node and UnsetMatched
    ALWAYS_INLINE void SetNodeUnMatched(DuplicateNode* node) {
      SetData(reinterpret_cast<uintptr_t>(node));
      DCHECK(!IsMatched());
    }
  };
  /// struct DuplicateNode is referenced by SIZE_OF_DUPLICATENODE of
  /// planner/PlannerContext.java. If struct DuplicateNode is modified, please modify
  /// SIZE_OF_DUPLICATENODE synchronously.
  /// Linked list of entries used for duplicates.
  struct DuplicateNode {
    HtData htdata;
    ALWAYS_INLINE DuplicateNode* Next() { return tdn.GetPtr(); }
    ALWAYS_INLINE void SetNext(DuplicateNode* node) { tdn.SetNode(node); }
    ALWAYS_INLINE bool IsMatched() { return tdn.IsMatched(); }
    ALWAYS_INLINE void SetMatched() { tdn.SetMatched(); }
    ALWAYS_INLINE void SetNextUnMatched(DuplicateNode* node) {
      tdn.SetNodeUnMatched(node);
    }

   private:
    /// Chain to next duplicate node, NULL when end of list.
    /// 'bool matched' is folded into this next pointer. Tag bit 0 represents it.
    TaggedDuplicateNode tdn;
  };

  union BucketData {
    HtData htdata;
    DuplicateNode* duplicates;
  };

  /// 'TaggedPtr' for 'BucketData'.
  /// This doesn't own BucketData* so Allocation and Deallocation is not its
  /// responsibility.
  /// Following fields are also folded in the TagggedPtr below:
  /// 1. bool matched: (Tag bit 0) Used for full outer and right {outer, anti, semi}
  ///    joins. Indicates whether the row in the bucket has been matched.
  ///    From an abstraction point of view, this is an awkward place to store this
  ///    information but it is efficient. This space is otherwise unused.
  /// 2. bool hasDuplicates: (Tag bit 1) Used in case of duplicates. If true, then
  ///    the bucketData union should be used as 'duplicates'.
  ///
  /// 'TAGGED': Methods to fetch or set data might have template parameter TAGGED.
  /// It can be set to 'false' only when tag fields above are not set. This avoids
  /// extra bit operations.
  class TaggedBucketData : public TaggedPtr<uint8, false> {
   public:
    TaggedBucketData() = default;
    ALWAYS_INLINE bool IsFilled() { return GetData() != 0; }
    ALWAYS_INLINE bool IsMatched() { return IsTagBitSet<0>(); }
    ALWAYS_INLINE bool HasDuplicates() { return IsTagBitSet<1>(); }
    ALWAYS_INLINE void SetMatched() { SetTagBit<0>(); }
    ALWAYS_INLINE void SetHasDuplicates() { SetTagBit<1>(); }
    /// Set 'data' as BucketData. 'TAGGED' is described in class description.
    template <class T, const bool TAGGED>
    ALWAYS_INLINE void SetBucketData(T* data) {
      (TAGGED) ? SetPtr(reinterpret_cast<uint8*>(data)) :
                 SetData(reinterpret_cast<uintptr_t>(data));
    }
    /// Get tuple pointer stored. 'TAGGED' is described in class description.
    template <bool TAGGED>
    ALWAYS_INLINE Tuple* GetTuple() {
      return (TAGGED) ? reinterpret_cast<Tuple*>(GetPtr()) :
                        reinterpret_cast<Tuple*>(GetData());
    }
    ALWAYS_INLINE DuplicateNode* GetDuplicate() {
      return reinterpret_cast<DuplicateNode*>(GetPtr());
    }
    ALWAYS_INLINE void PrepareBucketForInsert() { SetData(0); }
    TaggedBucketData& operator=(const TaggedBucketData& bd) = default;
  };

  /// struct Bucket is referenced by SIZE_OF_BUCKET of planner/PlannerContext.java.
  /// If struct Bucket is modified, please modify SIZE_OF_BUCKET synchronously.
  /// TaggedPtr is used to store BucketData. 2 booleans are folded into
  /// TaggedPtr. Check comments for TaggedBucketData for details on booleans
  /// stored.
  struct Bucket {
    /// Return the BucketData.
    /// 'TAGGED' is described in the comments for 'TaggedBucketData'.
    template <bool TAGGED = true>
    ALWAYS_INLINE BucketData GetBucketData() {
      BucketData bucket_data;
      bucket_data.htdata.tuple = bd.GetTuple<TAGGED>();
      return bucket_data;
    }
    /// Get Tuple pointer stored in bucket.
    /// 'TAGGED' is described in the comments for 'TaggedBucketData'.
    template <bool TAGGED = true>
    ALWAYS_INLINE Tuple* GetTuple() {
      return bd.GetTuple<TAGGED>();
    }
    /// Get Duplicate Node
    ALWAYS_INLINE DuplicateNode* GetDuplicate() { return bd.GetDuplicate(); }
    /// Whether this bucket contains a vaild entry, or it is empty.
    ALWAYS_INLINE bool IsFilled() { return bd.IsFilled(); }
    /// Indicates whether the row in the bucket has been matched.
    /// For more details read the comment for TaggedBucketData.
    ALWAYS_INLINE bool IsMatched() { return bd.IsMatched(); }
    /// Indicates if bucket has duplicates instead of data for bucket.
    ALWAYS_INLINE bool HasDuplicates() { return bd.HasDuplicates(); }

    /// Set/Unset methods corresponding to above.
    ALWAYS_INLINE void SetMatched() { bd.SetMatched(); }
    ALWAYS_INLINE void SetHasDuplicates() { bd.SetHasDuplicates(); }
    /// Set DuplicateNode pointer as data.
    /// 'TAGGED' is described in the comments for 'TaggedBucketData'.
    template <bool TAGGED = true>
    ALWAYS_INLINE void SetDuplicate(DuplicateNode* node) {
      bd.SetBucketData<DuplicateNode, TAGGED>(node);
    }
    /// Set Tuple pointer as data.
    /// 'TAGGED' is described in the comments for 'TaggedBucketData'.
    template <bool TAGGED = true>
    ALWAYS_INLINE void SetTuple(Tuple* tuple) {
      bd.SetBucketData<Tuple, TAGGED>(tuple);
    }
    /// Set FlatRowPtr as data.
    /// 'TAGGED' is described in the comments for 'TaggedBucketData'.
    template <bool TAGGED = true>
    ALWAYS_INLINE void SetFlatRow(BufferedTupleStream::FlatRowPtr flat_row) {
      bd.SetBucketData<uint8_t, TAGGED>(flat_row);
    }
    ALWAYS_INLINE void PrepareBucketForInsert() { bd.PrepareBucketForInsert(); }

   private:
    // This should not be exposed outside as implementation details
    // can change.
    TaggedBucketData bd;
  };

  static_assert(BitUtil::IsPowerOf2(sizeof(Bucket) && sizeof(Bucket) == 8),
      "We assume that Hash-table bucket directories are a power-of-two (8 bytes "
      "currently) sizes because allocating only bucket directories with power-of-two "
      "byte sizes avoids internal fragmentation in the simple buddy allocator. Assert "
      "checks for fixed size to avoid accidental changes.");

 public:
  class Iterator;

  /// Returns a newly allocated HashTable. The probing algorithm is set by the
  /// FLAG_enable_quadratic_probing.
  ///  - allocator: allocator to allocate bucket directory and data pages from.
  ///  - stores_duplicates: true if rows with duplicate keys may be inserted into the
  ///    hash table.
  ///  - num_build_tuples: number of Tuples in the build tuple row.
  ///  - tuple_stream: the tuple stream which contains the tuple rows index by the
  ///    hash table. Can be NULL if the rows contain only a single tuple, in which
  ///    case the 'tuple_stream' is unused.
  ///  - max_num_buckets: the maximum number of buckets that can be stored. If we
  ///    try to grow the number of buckets to a larger number, the inserts will fail.
  ///    -1, if it unlimited.
  ///  - initial_num_buckets: number of buckets that the hash table should be initialized
  ///    with.
  static HashTable* Create(Suballocator* allocator, bool stores_duplicates,
      int num_build_tuples, BufferedTupleStream* tuple_stream, int64_t max_num_buckets,
      int64_t initial_num_buckets);

  /// Allocates the initial bucket structure. Returns a non-OK status if an error is
  /// encountered. If an OK status is returned , 'got_memory' is set to indicate whether
  /// enough memory for the initial buckets was allocated from the Suballocator.
  Status Init(bool* got_memory) WARN_UNUSED_RESULT;

  /// Create the counters for HashTable stats and put them into the child profile
  /// "Hash Table".
  /// Returns a HashTableStatsProfile object.
  static std::unique_ptr<HashTableStatsProfile> AddHashTableCounters(
      RuntimeProfile* parent_profile);

  /// Call to cleanup any resources. Must be called once.
  void Close();

  /// Add operations stats of this hash table to the counters in profile.
  /// This method should only be called once for each HashTable and be called during
  /// closing the owner object of the HashTable. Not all the counters are added with the
  /// method, only counters for resizes are affected.
  void StatsCountersAdd(HashTableStatsProfile* profile);

  /// Inserts the row to the hash table. The caller is responsible for ensuring that the
  /// table has free buckets. Returns true if the insertion was successful. Always
  /// returns true if the table has free buckets and the key is not a duplicate. If the
  /// key was a duplicate and memory could not be allocated for the new duplicate node,
  /// returns false. If an error is encountered while creating a duplicate node, returns
  /// false and sets 'status' to the error.
  ///
  /// 'flat_row' is a pointer to the flattened row in 'tuple_stream_' If the row contains
  /// only one tuple, a pointer to that tuple is stored. Otherwise the 'flat_row' pointer
  /// is stored. The 'row' is not copied by the hash table and the caller must guarantee
  /// it stays in memory. This will not grow the hash table.
  bool IR_ALWAYS_INLINE Insert(HashTableCtx* __restrict__ ht_ctx,
      BufferedTupleStream::FlatRowPtr flat_row, TupleRow* row,
      Status* status) WARN_UNUSED_RESULT;

  /// Prefetch the hash table bucket which the given hash value 'hash' maps to.
  /// Thread-safe for read-only hash tables.
  template <const bool READ>
  void IR_ALWAYS_INLINE PrefetchBucket(uint32_t hash);

  /// Returns an iterator to the bucket that matches the probe expression results that
  /// are cached at the current position of the ExprValuesCache in 'ht_ctx'. Assumes that
  /// the ExprValuesCache was filled using EvalAndHashProbe(). Returns HashTable::End()
  /// if no match is found. The iterator can be iterated until HashTable::End() to find
  /// all the matching rows. Advancing the returned iterator will go to the next matching
  /// row. The matching rows do not need to be evaluated since all the nodes of a bucket
  /// are duplicates. One scan can be in progress for each 'ht_ctx'. Used in the probe
  /// phase of hash joins.
  /// Thread-safe for read-only hash tables.
  Iterator IR_ALWAYS_INLINE FindProbeRow(HashTableCtx* __restrict__ ht_ctx);

  /// Enum for the type of Bucket
  enum BucketType : bool {
    MATCH_SET = true, // matched flag is not set for the bucket
    MATCH_UNSET = false // matched flag is not set for the bucket
  };

  /// If a match is found in the table, return an iterator as in FindProbeRow(). If a
  /// match was not present, return an iterator pointing to the empty bucket where the key
  /// should be inserted. Returns End() if the table is full. The caller can set the data
  /// in the bucket using a Set*() method on the iterator.
  /// 'MATCH' is set true if Buckets of HashTable can have matched flag set.
  /// Thread-safe for read-only hash tables.
  template <BucketType TYPE = MATCH_SET>
  Iterator IR_ALWAYS_INLINE FindBuildRowBucket(
      HashTableCtx* __restrict__ ht_ctx, bool* found);

  /// Find slot for 'hash' from 0 to 'num_buckets'.
  int64_t getBucketId(uint32_t hash, int64_t num_buckets);

  /// Returns number of elements inserted in the hash table
  /// Thread-safe for read-only hash tables.
  int64_t size() const {
    return num_filled_buckets_ - num_buckets_with_duplicates_ + num_duplicate_nodes_;
  }

  /// Returns the number of empty buckets.
  /// Thread-safe for read-only hash tables.
  int64_t EmptyBuckets() const { return num_buckets_ - num_filled_buckets_; }

  /// Returns the number of buckets
  /// Thread-safe for read-only hash tables.
  int64_t num_buckets() const { return num_buckets_; }

  /// Returns the load factor (the number of non-empty buckets)
  /// Thread-safe for read-only hash tables.
  double load_factor() const {
    return static_cast<double>(num_filled_buckets_) / num_buckets_;
  }

  /// Return an estimate of the number of bytes needed to build the hash table
  /// structure for 'num_rows'. To do that, it estimates the number of buckets,
  /// rounded up to a power of two, and also assumes that there are no duplicates.
  static int64_t EstimateNumBuckets(int64_t num_rows) {
    /// Assume max 66% fill factor and no duplicates.
    return BitUtil::RoundUpToPowerOfTwo(3 * num_rows / 2);
  }
  static int64_t EstimateSize(int64_t num_rows) {
    int64_t num_buckets = EstimateNumBuckets(num_rows);
    return num_buckets * sizeof(Bucket);
  }

  /// Return the size of a hash table bucket in bytes.
  static const int64_t BUCKET_SIZE = sizeof(Bucket);

  /// Returns the memory occupied by the hash table, takes into account the number of
  /// duplicates.
  /// Thread-safe for read-only hash tables.
  int64_t CurrentMemSize() const;

  /// Returns the number of inserts that can be performed before resizing the table.
  int64_t NumInsertsBeforeResize() const;

  /// Calculates the fill factor if 'buckets_to_fill' additional buckets were to be
  /// filled and resizes the hash table so that the projected fill factor is below the
  /// max fill factor.
  /// If 'got_memory' is true, then it is guaranteed at least 'rows_to_add' rows can be
  /// inserted without need to resize. If there is not enough memory available to
  /// resize the hash table, Status::OK() is returned and 'got_memory' is false. If a
  /// another error occurs, an error status may be returned.
  Status CheckAndResize(uint64_t buckets_to_fill, HashTableCtx* __restrict__ ht_ctx,
      bool* got_memory) WARN_UNUSED_RESULT;

  /// Returns the number of bytes allocated to the hash table from the block manager.
  int64_t ByteSize() const {
    return num_buckets_ * sizeof(Bucket) + total_data_page_size_;
  }

  /// Returns an iterator at the beginning of the hash table.  Advancing this iterator
  /// will traverse all elements.
  /// Thread-safe for read-only hash tables.
  Iterator Begin(const HashTableCtx* ht_ctx);

  /// Return an iterator pointing to the first element (Bucket or DuplicateNode, if the
  /// bucket has duplicates) in the hash table that does not have its matched flag set.
  /// Used in right joins and full-outer joins.
  /// Thread-safe for read-only hash tables.
  Iterator FirstUnmatched(HashTableCtx* ctx);

  /// Return true if there was a least one match.
  /// Thread-safe for read-only hash tables.
  bool HasMatches() const { return has_matches_; }

  /// Return end marker.
  /// Thread-safe for read-only hash tables.
  Iterator End() { return Iterator(); }

  /// Dump out the entire hash table to string.  If 'skip_empty', empty buckets are
  /// skipped.  If 'show_match', it also prints the matched flag of each node. If
  /// 'build_desc' is non-null, the build rows will be printed. Otherwise, only the
  /// the addresses of the build rows will be printed.
  /// Thread-safe for read-only hash tables.
  std::string DebugString(bool skip_empty, bool show_match,
      const RowDescriptor* build_desc);

  /// Print the content of a bucket or node.
  void DebugStringTuple(std::stringstream& ss, HtData& htdata, const RowDescriptor* desc);

  /// Update and print some statistics that can be used for performance debugging.
  /// Thread-safe for read-only hash tables.
  std::string PrintStats() const;

  /// stl-like iterator interface.
  class Iterator {
   private:
    /// Bucket index value when probe is not successful.
    static const int64_t BUCKET_NOT_FOUND = -1;

   public:
    IR_ALWAYS_INLINE Iterator() :
        table_(NULL),
        scratch_row_(NULL),
        bucket_idx_(BUCKET_NOT_FOUND),
        node_(NULL) { }

    /// Iterates to the next element. It should be called only if !AtEnd().
    /// Thread-safe for read-only hash tables.
    void IR_ALWAYS_INLINE Next();

    /// Iterates to the next duplicate node. If the bucket does not have duplicates or
    /// when it reaches the last duplicate node, then it moves the Iterator to AtEnd().
    /// Used when we want to iterate over all the duplicate nodes bypassing the Next()
    /// interface (e.g. in semi/outer joins without other_join_conjuncts, in order to
    /// iterate over all nodes of an unmatched bucket).
    /// Thread-safe for read-only hash tables.
    void IR_ALWAYS_INLINE NextDuplicate();

    /// Iterates to the next element that does not have its matched flag set. Used in
    /// right-outer and full-outer joins.
    /// Thread-safe for read-only hash tables.
    void IR_ALWAYS_INLINE NextUnmatched();

    /// Return the current row or tuple. Callers must check the iterator is not AtEnd()
    /// before calling them.  The returned row is owned by the iterator and valid until
    /// the next call to GetRow(). It is safe to advance the iterator.
    /// 'MATCH' is true when current node has 'IsMatched()' flag true.
    /// Thread-safe for read-only hash tables.
    TupleRow* IR_ALWAYS_INLINE GetRow() const;
    template <BucketType TYPE = MATCH_SET>
    Tuple* IR_ALWAYS_INLINE GetTuple() const;

    /// Set the current tuple for an empty bucket. Designed to be used with the iterator
    /// returned from FindBuildRowBucket() in the case when the value is not found.  It is
    /// not valid to call this function if the bucket already has an entry.
    /// Not thread-safe.
    void SetTuple(Tuple* tuple, uint32_t hash);

    /// Sets as matched the Bucket or DuplicateNode currently pointed by the iterator,
    /// depending on whether the bucket has duplicates or not. The iterator cannot be
    /// AtEnd().
    /// Not thread-safe.
    void SetMatched();

    /// Returns the 'matched' flag of the current Bucket or DuplicateNode, depending on
    /// whether the bucket has duplicates or not. It should be called only if !AtEnd().
    /// Thread-safe for read-only hash tables.
    bool IsMatched() const;

    /// Resets everything but the pointer to the hash table.
    /// Not thread-safe.
    void SetAtEnd();

    /// Returns true if this iterator is at the end, i.e. GetRow() cannot be called.
    /// Thread-safe for read-only hash tables.
    bool ALWAYS_INLINE AtEnd() const { return bucket_idx_ == BUCKET_NOT_FOUND; }

    /// Prefetch the hash table bucket which the iterator is pointing to now.
    /// Thread-safe for read-only hash tables.
    template<const bool READ>
    void IR_ALWAYS_INLINE PrefetchBucket();

   private:
    friend class HashTable;

    ALWAYS_INLINE
    Iterator(HashTable* table, TupleRow* row, int bucket_idx, DuplicateNode* node)
      : table_(table),
        scratch_row_(row),
        bucket_idx_(bucket_idx),
        node_(node) {
    }

    HashTable* table_;

    /// Scratch buffer to hold generated rows. Not owned.
    TupleRow* scratch_row_;

    /// Current bucket idx.
    int64_t bucket_idx_;

    /// Pointer to the current duplicate node.
    DuplicateNode* node_;
  };

 private:
  friend class Iterator;
  friend class HashTableTest;

  /// Hash table constructor. Private because Create() should be used, instead
  /// of calling this constructor directly.
  ///  - quadratic_probing: set to true when the probing algorithm is quadratic, as
  ///    opposed to linear.
  HashTable(bool quadratic_probing, Suballocator* allocator, bool stores_duplicates,
      int num_build_tuples, BufferedTupleStream* tuple_stream, int64_t max_num_buckets,
      int64_t initial_num_buckets);

  /// Performs the probing operation according to the probing algorithm (linear or
  /// quadratic. Returns one of the following:
  /// (a) the index of the bucket that contains the entry matching 'hash' and, if
  ///     COMPARE_ROW is true, also equals the last row evaluated in 'ht_ctx'.
  ///     If COMPARE_ROW is false, returns the index of the first bucket with
  ///     matching hash.
  /// (b) the index of the first empty bucket according to the probing algorithm (linear
  ///     or quadratic), if the entry is not in the hash table or 'ht_ctx' is NULL.
  /// (c) Iterator::BUCKET_NOT_FOUND if the probe was not successful, i.e. the maximum
  ///     distance was traveled without finding either an empty or a matching bucket.
  /// Using the returned index value, the caller can create an iterator that can be
  /// iterated until End() to find all the matching rows.
  ///
  /// EvalAndHashBuild() or EvalAndHashProbe() must have been called before calling
  /// this function. The values of the expression values cache in 'ht_ctx' will be
  /// used to probe the hash table.
  ///
  /// 'INCLUSIVE_EQUALITY' is true if NULLs and NaNs should always be
  /// considered equal when comparing two rows.
  ///
  /// 'MATCH' is false if none of 'buckets' has matched (IsMatched) flag set. For
  /// instance in the case of Grouping Aggregate it would be false.
  ///
  /// 'hash' is the hash computed by EvalAndHashBuild() or EvalAndHashProbe().
  /// 'found' indicates that a bucket that contains an equal row is found.
  ///
  /// There are wrappers of this function that perform the Find and Insert logic.
  template <bool INCLUSIVE_EQUALITY, bool COMPARE_ROW, BucketType TYPE = MATCH_SET>
  int64_t IR_ALWAYS_INLINE Probe(Bucket* buckets, uint32_t* hash_array,
      int64_t num_buckets, HashTableCtx* __restrict__ ht_ctx, uint32_t hash, bool* found,
      BucketData* bd);

  /// Performs the insert logic. Returns the Bucket* of the bucket where the data
  /// should be inserted either in the bucket itself or in it's DuplicateNode.
  /// Returns NULL if the insert was not successful and either sets 'status' to OK
  /// if it failed because not enough reservation was available or the error if an
  /// error was encountered.
  Bucket* IR_ALWAYS_INLINE InsertInternal(
      HashTableCtx* __restrict__ ht_ctx, Status* status);

  /// Updates 'bucket_idx' to the index of the next non-empty bucket. If the bucket has
  /// duplicates, 'node' will be pointing to the head of the linked list of duplicates.
  /// Otherwise, 'node' should not be used. If there are no more buckets, sets
  /// 'bucket_idx' to BUCKET_NOT_FOUND.
  void NextFilledBucket(int64_t* bucket_idx, DuplicateNode** node);

  /// Resize the hash table to 'num_buckets'. 'got_memory' is false on OOM.
  Status ResizeBuckets(
      int64_t num_buckets, HashTableCtx* __restrict__ ht_ctx, bool* got_memory);

  /// Appends the DuplicateNode pointed by next_node_ to 'bucket' and moves the next_node_
  /// pointer to the next DuplicateNode in the page, updating the remaining node counter.
  DuplicateNode* IR_ALWAYS_INLINE AppendNextNode(Bucket* bucket);

  /// Creates a new DuplicateNode for a entry and chains it to the bucket with index
  /// 'bucket_idx'. The duplicate nodes of a bucket are chained as a linked list.
  /// This places the new duplicate node at the beginning of the list. If this is the
  /// first duplicate entry inserted in this bucket, then the entry already contained by
  /// the bucket is converted to a DuplicateNode. That is, the contents of 'data' of the
  /// bucket are copied to a DuplicateNode and 'data' is updated to pointing to a
  /// DuplicateNode.
  /// Returns NULL and sets 'status' to OK if the node array could not grow, i.e. there
  /// was not enough memory to allocate a new DuplicateNode. Returns NULL and sets
  /// 'status' to an error if another error was encountered.
  DuplicateNode* IR_ALWAYS_INLINE InsertDuplicateNode(
      int64_t bucket_idx, Status* status, BucketData* bucket_data);

  /// Resets the contents of the empty bucket with index 'bucket_idx', in preparation for
  /// an insert. Sets all the fields of the bucket other than 'data'.
  void IR_ALWAYS_INLINE PrepareBucketForInsert(int64_t bucket_idx, uint32_t hash);

  /// Return the TupleRow pointed by 'htdata'.
  TupleRow* GetRow(HtData& htdata, TupleRow* row) const;

  /// Returns the TupleRow of the pointed 'bucket'. In case of duplicates, it
  /// returns the content of the first chained duplicate node of the bucket.
  /// It also fills 'bd' with the BucketData of 'bucket'.
  /// 'MATCH' is set true if 'bucket' can have matched flag set i.e.,
  /// 'IsMatched()' returns true.
  /// They can either have duplicates or matched buckets.
  template <BucketType TYPE = MATCH_SET>
  TupleRow* GetRow(Bucket* bucket, TupleRow* row, BucketData* bd) const;

  /// Grow the node array. Returns true and sets 'status' to OK on success. Returns false
  /// and set 'status' to OK if we can't get sufficient reservation to allocate the next
  /// data page. Returns false and sets 'status' if another error is encountered.
  bool GrowNodeArray(Status* status);

  /// Reset HashTable's internal state to Default State
  void ResetState();

  /// Functions to be replaced by codegen to specialize the hash table.
  bool IR_NO_INLINE stores_tuples() const { return stores_tuples_; }
  bool IR_NO_INLINE stores_duplicates() const { return stores_duplicates_; }
  bool IR_NO_INLINE quadratic_probing() const { return quadratic_probing_; }

  /// Load factor that will trigger growing the hash table on insert.  This is
  /// defined as the number of non-empty buckets / total_buckets
  static constexpr double MAX_FILL_FACTOR = 0.75;

  /// The size in bytes of each page of duplicate nodes. Should be large enough to fit
  /// enough DuplicateNodes to amortise the overhead of allocating each page and low
  /// enough to not waste excessive memory to internal fragmentation.
  static constexpr int64_t DATA_PAGE_SIZE = 64L * 1024;

  RuntimeState* state_;

  /// Suballocator to allocate data pages and hash table buckets with.
  Suballocator* const allocator_;

  /// Stream contains the rows referenced by the hash table. Can be NULL if the
  /// row only contains a single tuple, in which case the TupleRow indirection
  /// is removed by the hash table.
  BufferedTupleStream* const tuple_stream_;

  /// Constants on how the hash table should behave.

  /// True if the HtData uses the Tuple* representation, or false if it uses FlatRowPtr.
  const bool stores_tuples_;

  /// True if duplicates may be inserted into hash table.
  const bool stores_duplicates_;

  /// Quadratic probing enabled (as opposed to linear).
  const bool quadratic_probing_;

  /// Data pages for all nodes. Allocated from suballocator to reduce memory
  /// consumption of small tables.
  std::vector<std::unique_ptr<Suballocation>> data_pages_;

  /// Byte size of all buffers in data_pages_.
  int64_t total_data_page_size_ = 0;

  /// Next duplicate node to insert. Vaild when node_remaining_current_page_ > 0.
  DuplicateNode* next_node_ = nullptr;

  /// Number of nodes left in the current page.
  int node_remaining_current_page_ = 0;

  /// Number of duplicate nodes.
  int64_t num_duplicate_nodes_ = 0;

  const int64_t max_num_buckets_ = 0;

  /// Allocation containing all buckets.
  std::unique_ptr<Suballocation> bucket_allocation_;

  /// Pointer to the 'buckets_' array from 'bucket_allocation_'.
  Bucket* buckets_ = nullptr;

  /// Allocation containing the cached hash value for every bucket.
  std::unique_ptr<Suballocation> hash_allocation_;

  /// Cache of the hash for data. It is an array of hash values where ith value
  /// corresponds to hash value of ith bucket in 'buckets_' array.
  /// This is not part of struct 'Bucket' to make sure 'sizeof(Bucket)' is power of 2.
  uint32_t* hash_array_;

  /// Total number of buckets (filled and empty).
  int64_t num_buckets_;

  /// Number of non-empty buckets.  Used to determine when to resize.
  int64_t num_filled_buckets_ = 0;

  /// Number of (non-empty) buckets with duplicates. These buckets do not point to slots
  /// in the tuple stream, rather than to a linked list of Nodes.
  int64_t num_buckets_with_duplicates_ = 0;

  /// Number of build tuples, used for constructing temp row* for probes.
  const int num_build_tuples_;

  /// Flag used to check that we don't lose stored matches when spilling hash tables
  /// (IMPALA-1488).
  bool has_matches_ = false;

  /// How many times this table has resized so far.
  int64_t num_resizes_ = 0;
};

} // namespace impala
