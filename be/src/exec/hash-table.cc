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

#include "exec/hash-table.inline.h"

#include <functional>
#include <numeric>
#include <gutil/strings/substitute.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exprs/slot-ref.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"

#include "common/names.h"

using namespace impala;
using strings::Substitute;

DEFINE_bool(enable_quadratic_probing, true, "Enable quadratic probing hash table");

const char* HashTableCtx::LLVM_CLASS_NAME = "class.impala::HashTableCtx";

// Random primes to multiply the seed with.
static uint32_t SEED_PRIMES[] = {
  1, // First seed must be 1, level 0 is used by other operators in the fragment.
  1431655781,
  1183186591,
  622729787,
  472882027,
  338294347,
  275604541,
  41161739,
  29999999,
  27475109,
  611603,
  16313357,
  11380003,
  21261403,
  33393119,
  101,
  71043403
};

// Put a non-zero constant in the result location for NULL.
// We don't want(NULL, 1) to hash to the same as (0, 1).
// This needs to be as big as the biggest primitive type since the bytes
// get copied directly.
// TODO find a better approach, since primitives like CHAR(N) can be up
// to 255 bytes
static int64_t NULL_VALUE[] = {
  HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
  HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
  HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
  HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
  HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
  HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
  HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
  HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED
};

static_assert(sizeof(NULL_VALUE) >= ColumnType::MAX_CHAR_LENGTH,
    "NULL_VALUE must be at least as large as the largest possible slot");

HashTableCtx::HashTableCtx(const std::vector<ScalarExpr*>& build_exprs,
    const std::vector<ScalarExpr*>& probe_exprs, bool stores_nulls,
    const std::vector<bool>& finds_nulls, int32_t initial_seed,
    int max_levels, MemPool* expr_perm_pool, MemPool* build_expr_results_pool,
    MemPool* probe_expr_results_pool)
    : build_exprs_(build_exprs),
      probe_exprs_(probe_exprs),
      stores_nulls_(stores_nulls),
      finds_nulls_(finds_nulls),
      finds_some_nulls_(std::accumulate(
          finds_nulls_.begin(), finds_nulls_.end(), false, std::logical_or<bool>())),
      level_(0),
      scratch_row_(NULL),
      expr_perm_pool_(expr_perm_pool),
      build_expr_results_pool_(build_expr_results_pool),
      probe_expr_results_pool_(probe_expr_results_pool) {
  DCHECK(!finds_some_nulls_ || stores_nulls_);
  // Compute the layout and buffer size to store the evaluated expr results
  DCHECK_EQ(build_exprs_.size(), probe_exprs_.size());
  DCHECK_EQ(build_exprs_.size(), finds_nulls_.size());
  DCHECK(!build_exprs_.empty());

  // Populate the seeds to use for all the levels. TODO: revisit how we generate these.
  DCHECK_GE(max_levels, 0);
  DCHECK_LT(max_levels, sizeof(SEED_PRIMES) / sizeof(SEED_PRIMES[0]));
  DCHECK_NE(initial_seed, 0);
  seeds_.resize(max_levels + 1);
  seeds_[0] = initial_seed;
  for (int i = 1; i <= max_levels; ++i) {
    seeds_[i] = seeds_[i - 1] * SEED_PRIMES[i];
  }
}

Status HashTableCtx::Init(ObjectPool* pool, RuntimeState* state, int num_build_tuples) {
  int scratch_row_size = sizeof(Tuple*) * num_build_tuples;
  scratch_row_ = reinterpret_cast<TupleRow*>(malloc(scratch_row_size));
  if (UNLIKELY(scratch_row_ == NULL)) {
    return Status(Substitute("Failed to allocate $0 bytes for scratch row of "
        "HashTableCtx.", scratch_row_size));
  }
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(build_exprs_, state, pool, expr_perm_pool_,
      build_expr_results_pool_, &build_expr_evals_));
  DCHECK_EQ(build_exprs_.size(), build_expr_evals_.size());
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(probe_exprs_, state, pool, expr_perm_pool_,
      probe_expr_results_pool_, &probe_expr_evals_));
  DCHECK_EQ(probe_exprs_.size(), probe_expr_evals_.size());
  return expr_values_cache_.Init(state, expr_perm_pool_->mem_tracker(), build_exprs_);
}

Status HashTableCtx::Create(ObjectPool* pool, RuntimeState* state,
    const std::vector<ScalarExpr*>& build_exprs,
    const std::vector<ScalarExpr*>& probe_exprs, bool stores_nulls,
    const std::vector<bool>& finds_nulls, int32_t initial_seed, int max_levels,
    int num_build_tuples, MemPool* expr_perm_pool, MemPool* build_expr_results_pool,
    MemPool* probe_expr_results_pool, scoped_ptr<HashTableCtx>* ht_ctx) {
  ht_ctx->reset(new HashTableCtx(build_exprs, probe_exprs, stores_nulls,
      finds_nulls, initial_seed, max_levels, expr_perm_pool,
      build_expr_results_pool, probe_expr_results_pool));
  return (*ht_ctx)->Init(pool, state, num_build_tuples);
}

Status HashTableCtx::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(build_expr_evals_, state));
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(probe_expr_evals_, state));
  return Status::OK();
}

void HashTableCtx::Close(RuntimeState* state) {
  free(scratch_row_);
  scratch_row_ = NULL;
  expr_values_cache_.Close(expr_perm_pool_->mem_tracker());
  ScalarExprEvaluator::Close(build_expr_evals_, state);
  ScalarExprEvaluator::Close(probe_expr_evals_, state);
}

uint32_t HashTableCtx::Hash(const void* input, int len, uint32_t hash) const {
  /// Use CRC hash at first level for better performance. Switch to murmur hash at
  /// subsequent levels since CRC doesn't randomize well with different seed inputs.
  if (level_ == 0) return HashUtil::Hash(input, len, hash);
  return HashUtil::MurmurHash2_64(input, len, hash);
}

uint32_t HashTableCtx::HashRow(
    const uint8_t* expr_values, const uint8_t* expr_values_null) const noexcept {
  DCHECK_LT(level_, seeds_.size());
  if (expr_values_cache_.var_result_offset() == -1) {
    /// This handles NULLs implicitly since a constant seed value was put
    /// into results buffer for nulls.
    return Hash(
        expr_values, expr_values_cache_.expr_values_bytes_per_row(), seeds_[level_]);
  } else {
    return HashTableCtx::HashVariableLenRow(expr_values, expr_values_null);
  }
}

bool HashTableCtx::EvalRow(const TupleRow* row,
    const vector<ScalarExprEvaluator*>& evals,
    uint8_t* expr_values, uint8_t* expr_values_null) noexcept {
  bool has_null = false;
  for (int i = 0; i < evals.size(); ++i) {
    void* loc = expr_values_cache_.ExprValuePtr(expr_values, i);
    void* val = evals[i]->GetValue(row);
    if (val == NULL) {
      // If the table doesn't store nulls, no reason to keep evaluating
      if (!stores_nulls_) return true;
      expr_values_null[i] = true;
      val = reinterpret_cast<void*>(&NULL_VALUE);
      has_null = true;
    } else {
      expr_values_null[i] = false;
    }
    DCHECK_LE(build_exprs_[i]->type().GetSlotSize(), sizeof(NULL_VALUE));
    RawValue::Write(val, loc, build_exprs_[i]->type(), NULL);
  }
  return has_null;
}

uint32_t HashTableCtx::HashVariableLenRow(const uint8_t* expr_values,
    const uint8_t* expr_values_null) const {
  uint32_t hash = seeds_[level_];
  int var_result_offset = expr_values_cache_.var_result_offset();
  // Hash the non-var length portions (if there are any)
  if (var_result_offset != 0) {
    hash = Hash(expr_values, var_result_offset, hash);
  }

  for (int i = 0; i < build_exprs_.size(); ++i) {
    // non-string and null slots are already part of 'expr_values'.
    if (build_exprs_[i]->type().type != TYPE_STRING &&
        build_exprs_[i]->type().type != TYPE_VARCHAR) {
      continue;
    }
    const void* loc = expr_values_cache_.ExprValuePtr(expr_values, i);
    if (expr_values_null[i]) {
      // Hash the null random seed values at 'loc'
      hash = Hash(loc, sizeof(StringValue), hash);
    } else {
      // Hash the string
      // TODO: when using CRC hash on empty string, this only swaps bytes.
      const StringValue* str = reinterpret_cast<const StringValue*>(loc);
      hash = Hash(str->ptr, str->len, hash);
    }
  }
  return hash;
}

template <bool FORCE_NULL_EQUALITY>
bool HashTableCtx::Equals(const TupleRow* build_row, const uint8_t* expr_values,
    const uint8_t* expr_values_null) const noexcept {
  for (int i = 0; i < build_expr_evals_.size(); ++i) {
    void* val = build_expr_evals_[i]->GetValue(build_row);
    if (val == NULL) {
      if (!(FORCE_NULL_EQUALITY || finds_nulls_[i])) return false;
      if (!expr_values_null[i]) return false;
      continue;
    } else {
      if (expr_values_null[i]) return false;
    }

    const void* loc = expr_values_cache_.ExprValuePtr(expr_values, i);
    DCHECK(build_exprs_[i] == &build_expr_evals_[i]->root());
    if (!RawValue::Eq(loc, val, build_exprs_[i]->type())) return false;
  }
  return true;
}
template bool HashTableCtx::Equals<true>(const TupleRow* build_row,
    const uint8_t* expr_values, const uint8_t* expr_values_null) const;
template bool HashTableCtx::Equals<false>(const TupleRow* build_row,
    const uint8_t* expr_values, const uint8_t* expr_values_null) const;

HashTableCtx::ExprValuesCache::ExprValuesCache()
  : capacity_(0),
    cur_expr_values_(NULL),
    cur_expr_values_null_(NULL),
    cur_expr_values_hash_(NULL),
    cur_expr_values_hash_end_(NULL),
    expr_values_array_(NULL),
    expr_values_null_array_(NULL),
    expr_values_hash_array_(NULL),
    null_bitmap_(0) {}

Status HashTableCtx::ExprValuesCache::Init(RuntimeState* state,
    MemTracker* tracker, const std::vector<ScalarExpr*>& build_exprs) {
  // Initialize the number of expressions.
  num_exprs_ = build_exprs.size();
  // Compute the layout of evaluated values of a row.
  expr_values_bytes_per_row_ = ScalarExpr::ComputeResultsLayout(build_exprs,
      &expr_values_offsets_, &var_result_offset_);
  if (expr_values_bytes_per_row_ == 0) {
    DCHECK_EQ(num_exprs_, 0);
    return Status::OK();
  }
  DCHECK_GT(expr_values_bytes_per_row_, 0);
  // Compute the maximum number of cached rows which can fit in the memory budget.
  // TODO: Find the optimal prefetch batch size. This may be something
  // processor dependent so we may need calibration at Impala startup time.
  capacity_ = std::max(1, std::min(state->batch_size(),
      MAX_EXPR_VALUES_ARRAY_SIZE / expr_values_bytes_per_row_));

  int mem_usage = MemUsage(capacity_, expr_values_bytes_per_row_, num_exprs_);
  if (UNLIKELY(!tracker->TryConsume(mem_usage))) {
    capacity_ = 0;
    string details = Substitute("HashTableCtx::ExprValuesCache failed to allocate $0 bytes.",
        mem_usage);
    return tracker->MemLimitExceeded(state, details, mem_usage);
  }

  int expr_values_size = expr_values_bytes_per_row_ * capacity_;
  expr_values_array_.reset(new uint8_t[expr_values_size]);
  cur_expr_values_ = expr_values_array_.get();
  memset(cur_expr_values_, 0, expr_values_size);

  int expr_values_null_size = num_exprs_ * capacity_;
  expr_values_null_array_.reset(new uint8_t[expr_values_null_size]);
  cur_expr_values_null_ = expr_values_null_array_.get();
  memset(cur_expr_values_null_, 0, expr_values_null_size);

  expr_values_hash_array_.reset(new uint32_t[capacity_]);
  cur_expr_values_hash_ = expr_values_hash_array_.get();
  cur_expr_values_hash_end_ = cur_expr_values_hash_;
  memset(cur_expr_values_hash_, 0, sizeof(uint32) * capacity_);

  null_bitmap_.Reset(capacity_);
  return Status::OK();
}

void HashTableCtx::ExprValuesCache::Close(MemTracker* tracker) {
  if (capacity_ == 0) return;
  cur_expr_values_ = NULL;
  cur_expr_values_null_ = NULL;
  cur_expr_values_hash_ = NULL;
  cur_expr_values_hash_end_ = NULL;
  expr_values_array_.reset();
  expr_values_null_array_.reset();
  expr_values_hash_array_.reset();
  null_bitmap_.Reset(0);
  int mem_usage = MemUsage(capacity_, expr_values_bytes_per_row_, num_exprs_);
  tracker->Release(mem_usage);
}

int HashTableCtx::ExprValuesCache::MemUsage(int capacity,
    int expr_values_bytes_per_row, int num_exprs) {
  return expr_values_bytes_per_row * capacity + // expr_values_array_
      num_exprs * capacity +                    // expr_values_null_array_
      sizeof(uint32) * capacity +               // expr_values_hash_array_
      Bitmap::MemUsage(capacity);               // null_bitmap_
}

uint8_t* HashTableCtx::ExprValuesCache::ExprValuePtr(
    uint8_t* expr_values, int expr_idx) const {
  return expr_values + expr_values_offsets_[expr_idx];
}

const uint8_t* HashTableCtx::ExprValuesCache::ExprValuePtr(
    const uint8_t* expr_values, int expr_idx) const {
  return expr_values + expr_values_offsets_[expr_idx];
}

void HashTableCtx::ExprValuesCache::ResetIterators() {
  cur_expr_values_ = expr_values_array_.get();
  cur_expr_values_null_ = expr_values_null_array_.get();
  cur_expr_values_hash_ = expr_values_hash_array_.get();
}

void HashTableCtx::ExprValuesCache::Reset() noexcept {
  ResetIterators();
  // Set the end pointer after resetting the other pointers so they point to
  // the same location.
  cur_expr_values_hash_end_ = cur_expr_values_hash_;
  null_bitmap_.SetAllBits(false);
}

void HashTableCtx::ExprValuesCache::ResetForRead() {
  // Record the end of hash values iterator to be used in AtEnd().
  // Do it before resetting the pointers.
  cur_expr_values_hash_end_ = cur_expr_values_hash_;
  ResetIterators();
}

constexpr double HashTable::MAX_FILL_FACTOR;
constexpr int64_t HashTable::DATA_PAGE_SIZE;

HashTable* HashTable::Create(Suballocator* allocator, bool stores_duplicates,
    int num_build_tuples, BufferedTupleStream* tuple_stream, int64_t max_num_buckets,
    int64_t initial_num_buckets) {
  return new HashTable(FLAGS_enable_quadratic_probing, allocator, stores_duplicates,
      num_build_tuples, tuple_stream, max_num_buckets, initial_num_buckets);
}

HashTable::HashTable(bool quadratic_probing, Suballocator* allocator,
    bool stores_duplicates, int num_build_tuples, BufferedTupleStream* stream,
    int64_t max_num_buckets, int64_t num_buckets)
  : allocator_(allocator),
    tuple_stream_(stream),
    stores_tuples_(num_build_tuples == 1),
    stores_duplicates_(stores_duplicates),
    quadratic_probing_(quadratic_probing),
    total_data_page_size_(0),
    next_node_(NULL),
    node_remaining_current_page_(0),
    num_duplicate_nodes_(0),
    max_num_buckets_(max_num_buckets),
    buckets_(NULL),
    num_buckets_(num_buckets),
    num_filled_buckets_(0),
    num_buckets_with_duplicates_(0),
    num_build_tuples_(num_build_tuples),
    has_matches_(false),
    num_probes_(0), num_failed_probes_(0), travel_length_(0), num_hash_collisions_(0),
    num_resizes_(0) {
  DCHECK_EQ((num_buckets & (num_buckets - 1)), 0) << "num_buckets must be a power of 2";
  DCHECK_GT(num_buckets, 0) << "num_buckets must be larger than 0";
  DCHECK(stores_tuples_ || stream != NULL);
}

Status HashTable::Init(bool* got_memory) {
  int64_t buckets_byte_size = num_buckets_ * sizeof(Bucket);
  RETURN_IF_ERROR(allocator_->Allocate(buckets_byte_size, &bucket_allocation_));
  if (bucket_allocation_ == nullptr) {
    num_buckets_ = 0;
    *got_memory = false;
    return Status::OK();
  }
  buckets_ = reinterpret_cast<Bucket*>(bucket_allocation_->data());
  memset(buckets_, 0, buckets_byte_size);
  *got_memory = true;
  return Status::OK();
}

void HashTable::Close() {
  // Print statistics only for the large or heavily used hash tables.
  // TODO: Tweak these numbers/conditions, or print them always?
  const int64_t LARGE_HT = 128 * 1024;
  const int64_t HEAVILY_USED = 1024 * 1024;
  // TODO: These statistics should go to the runtime profile as well.
  if ((num_buckets_ > LARGE_HT) || (num_probes_ > HEAVILY_USED)) VLOG(2) << PrintStats();
  for (auto& data_page : data_pages_) allocator_->Free(move(data_page));
  data_pages_.clear();
  if (ImpaladMetrics::HASH_TABLE_TOTAL_BYTES != NULL) {
    ImpaladMetrics::HASH_TABLE_TOTAL_BYTES->Increment(-total_data_page_size_);
  }
  if (bucket_allocation_ != nullptr) allocator_->Free(move(bucket_allocation_));
}

Status HashTable::CheckAndResize(
    uint64_t buckets_to_fill, const HashTableCtx* ht_ctx, bool* got_memory) {
  uint64_t shift = 0;
  while (num_filled_buckets_ + buckets_to_fill >
         (num_buckets_ << shift) * MAX_FILL_FACTOR) {
    ++shift;
  }
  if (shift > 0) return ResizeBuckets(num_buckets_ << shift, ht_ctx, got_memory);
  *got_memory = true;
  return Status::OK();
}

Status HashTable::ResizeBuckets(
    int64_t num_buckets, const HashTableCtx* ht_ctx, bool* got_memory) {
  DCHECK_EQ((num_buckets & (num_buckets - 1)), 0)
      << "num_buckets=" << num_buckets << " must be a power of 2";
  DCHECK_GT(num_buckets, num_filled_buckets_)
    << "Cannot shrink the hash table to smaller number of buckets than the number of "
    << "filled buckets.";
  VLOG(2) << "Resizing hash table from " << num_buckets_ << " to " << num_buckets
          << " buckets.";
  if (max_num_buckets_ != -1 && num_buckets > max_num_buckets_) {
    *got_memory = false;
    return Status::OK();
  }
  ++num_resizes_;

  // All memory that can grow proportional to the input should come from the block mgrs
  // mem tracker.
  // Note that while we copying over the contents of the old hash table, we need to have
  // allocated both the old and the new hash table. Once we finish, we return the memory
  // of the old hash table.
  // int64_t old_size = num_buckets_ * sizeof(Bucket);
  int64_t new_size = num_buckets * sizeof(Bucket);

  unique_ptr<Suballocation> new_allocation;
  RETURN_IF_ERROR(allocator_->Allocate(new_size, &new_allocation));
  if (new_allocation == NULL) {
    *got_memory = false;
    return Status::OK();
  }
  Bucket* new_buckets = reinterpret_cast<Bucket*>(new_allocation->data());
  memset(new_buckets, 0, new_size);

  // Walk the old table and copy all the filled buckets to the new (resized) table.
  // We do not have to do anything with the duplicate nodes. This operation is expected
  // to succeed.
  for (HashTable::Iterator iter = Begin(ht_ctx); !iter.AtEnd();
       NextFilledBucket(&iter.bucket_idx_, &iter.node_)) {
    Bucket* bucket_to_copy = &buckets_[iter.bucket_idx_];
    bool found = false;
    int64_t bucket_idx =
        Probe<true>(new_buckets, num_buckets, NULL, bucket_to_copy->hash, &found);
    DCHECK(!found);
    DCHECK_NE(bucket_idx, Iterator::BUCKET_NOT_FOUND) << " Probe failed even though "
        " there are free buckets. " << num_buckets << " " << num_filled_buckets_;
    Bucket* dst_bucket = &new_buckets[bucket_idx];
    *dst_bucket = *bucket_to_copy;
  }

  num_buckets_ = num_buckets;
  allocator_->Free(move(bucket_allocation_));
  bucket_allocation_ = move(new_allocation);
  buckets_ = reinterpret_cast<Bucket*>(bucket_allocation_->data());
  *got_memory = true;
  return Status::OK();
}

bool HashTable::GrowNodeArray(Status* status) {
  unique_ptr<Suballocation> allocation;
  *status = allocator_->Allocate(DATA_PAGE_SIZE, &allocation);
  if (!status->ok() || allocation == nullptr) return false;
  next_node_ = reinterpret_cast<DuplicateNode*>(allocation->data());
  data_pages_.push_back(move(allocation));
  ImpaladMetrics::HASH_TABLE_TOTAL_BYTES->Increment(DATA_PAGE_SIZE);
  node_remaining_current_page_ = DATA_PAGE_SIZE / sizeof(DuplicateNode);
  total_data_page_size_ += DATA_PAGE_SIZE;
  return true;
}

void HashTable::DebugStringTuple(stringstream& ss, HtData& htdata,
    const RowDescriptor* desc) {
  if (stores_tuples_) {
    ss << "(" << htdata.tuple << ")";
  } else {
    ss << "(" << htdata.flat_row << ")";
  }
  if (desc != NULL) {
    Tuple* row[num_build_tuples_];
    ss << " " << PrintRow(GetRow(htdata, reinterpret_cast<TupleRow*>(row)), *desc);
  }
}

string HashTable::DebugString(bool skip_empty, bool show_match,
    const RowDescriptor* desc) {
  stringstream ss;
  ss << endl;
  for (int i = 0; i < num_buckets_; ++i) {
    if (skip_empty && !buckets_[i].filled) continue;
    ss << i << ": ";
    if (show_match) {
      if (buckets_[i].matched) {
        ss << " [M]";
      } else {
        ss << " [U]";
      }
    }
    if (buckets_[i].hasDuplicates) {
      DuplicateNode* node = buckets_[i].bucketData.duplicates;
      bool first = true;
      ss << " [D] ";
      while (node != NULL) {
        if (!first) ss << ",";
        DebugStringTuple(ss, node->htdata, desc);
        node = node->next;
        first = false;
      }
    } else {
      ss << " [B] ";
      if (buckets_[i].filled) {
        DebugStringTuple(ss, buckets_[i].bucketData.htdata, desc);
      } else {
        ss << " - ";
      }
    }
    ss << endl;
  }
  return ss.str();
}

string HashTable::PrintStats() const {
  double curr_fill_factor = (double)num_filled_buckets_/(double)num_buckets_;
  double avg_travel = (double)travel_length_/(double)num_probes_;
  double avg_collisions = (double)num_hash_collisions_/(double)num_filled_buckets_;
  stringstream ss;
  ss << "Buckets: " << num_buckets_ << " " << num_filled_buckets_ << " "
     << curr_fill_factor << endl;
  ss << "Duplicates: " << num_buckets_with_duplicates_ << " buckets "
     << num_duplicate_nodes_ << " nodes" << endl;
  ss << "Probes: " << num_probes_ << endl;
  ss << "FailedProbes: " << num_failed_probes_ << endl;
  ss << "Travel: " << travel_length_ << " " << avg_travel << endl;
  ss << "HashCollisions: " << num_hash_collisions_ << " " << avg_collisions << endl;
  ss << "Resizes: " << num_resizes_ << endl;
  return ss.str();
}

// Helper function to store a value into the results buffer if the expr
// evaluated to NULL.  We don't want (NULL, 1) to hash to the same as (0,1) so
// we'll pick a more random value.
static void CodegenAssignNullValue(LlvmCodeGen* codegen, LlvmBuilder* builder,
    llvm::Value* dst, const ColumnType& type) {
  uint64_t fnv_seed = HashUtil::FNV_SEED;

  if (type.type == TYPE_STRING || type.type == TYPE_VARCHAR) {
    llvm::Value* dst_ptr = builder->CreateStructGEP(NULL, dst, 0, "string_ptr");
    llvm::Value* dst_len = builder->CreateStructGEP(NULL, dst, 1, "string_len");
    llvm::Value* null_len = codegen->GetI32Constant(fnv_seed);
    llvm::Value* null_ptr = builder->CreateIntToPtr(null_len, codegen->ptr_type());
    builder->CreateStore(null_ptr, dst_ptr);
    builder->CreateStore(null_len, dst_len);
  } else {
    llvm::Value* null_value = NULL;
    int byte_size = type.GetByteSize();
    // Get a type specific representation of fnv_seed
    switch (type.type) {
      case TYPE_BOOLEAN:
        // In results, booleans are stored as 1 byte
        dst = builder->CreateBitCast(dst, codegen->ptr_type());
        null_value = codegen->GetI8Constant(fnv_seed);
        break;
      case TYPE_TIMESTAMP: {
        // Cast 'dst' to 'i128*'
        DCHECK_EQ(byte_size, 16);
        llvm::PointerType* fnv_seed_ptr_type =
            codegen->GetPtrType(llvm::Type::getIntNTy(codegen->context(), byte_size * 8));
        dst = builder->CreateBitCast(dst, fnv_seed_ptr_type);
        null_value = codegen->GetIntConstant(byte_size, fnv_seed, fnv_seed);
        break;
      }
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
      case TYPE_DECIMAL:
        null_value = codegen->GetIntConstant(byte_size, fnv_seed, fnv_seed);
        break;
      case TYPE_FLOAT: {
        // Don't care about the value, just the bit pattern
        float fnv_seed_float = *reinterpret_cast<float*>(&fnv_seed);
        null_value =
            llvm::ConstantFP::get(codegen->context(), llvm::APFloat(fnv_seed_float));
        break;
      }
      case TYPE_DOUBLE: {
        // Don't care about the value, just the bit pattern
        double fnv_seed_double = *reinterpret_cast<double*>(&fnv_seed);
        null_value =
            llvm::ConstantFP::get(codegen->context(), llvm::APFloat(fnv_seed_double));
        break;
      }
      default:
        DCHECK(false);
    }
    builder->CreateStore(null_value, dst);
  }
}

// Codegen for evaluating a tuple row over either build_expr_evals_ or
// probe_expr_evals_. For a group by with (big int, string) the IR looks like:
//
// define i1 @EvalProbeRow(%"class.impala::HashTableCtx"* %this_ptr,
//    %"class.impala::TupleRow"* %row, i8* %expr_values, i8* %expr_values_null) #34 {
// entry:
//   %loc_addr = getelementptr i8, i8* %expr_values, i32 0
//   %loc = bitcast i8* %loc_addr to i64*
//   %result = call { i8, i64 } @GetSlotRef.2(%"class.impala::ExprContext"*
//        inttoptr (i64 197737664 to %"class.impala::ExprContext"*),
//        %"class.impala::TupleRow"* %row)
//   %0 = extractvalue { i8, i64 } %result, 0
//   %is_null = trunc i8 %0 to i1
//   %1 = zext i1 %is_null to i8
//   %null_byte_loc = getelementptr i8, i8* %expr_values_null, i32 0
//   store i8 %1, i8* %null_byte_loc
//   br i1 %is_null, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   store i64 2166136261, i64* %loc
//   br label %continue
//
// not_null:                                         ; preds = %entry
//   %val = extractvalue { i8, i64 } %result, 1
//   store i64 %val, i64* %loc
//   br label %continue
//
// continue:                                         ; preds = %not_null, %null
//   %is_null_phi = phi i1 [ true, %null ], [ false, %not_null ]
//   %has_null = or i1 false, %is_null_phi
//   %loc_addr1 = getelementptr i8, i8* %expr_values, i32 8
//   %loc2 = bitcast i8* %loc_addr1 to %"struct.impala::StringValue"*
//   %result6 = call { i64, i8* } @GetSlotRef.3(%"class.impala::ExprContext"*
//      inttoptr (i64 197738048 to %"class.impala::ExprContext"*),
//      %"class.impala::TupleRow"* %row)
//   %2 = extractvalue { i64, i8* } %result6, 0
//   %is_null7 = trunc i64 %2 to i1
//   %3 = zext i1 %is_null7 to i8
//   %null_byte_loc8 = getelementptr i8, i8* %expr_values_null, i32 1
//   store i8 %3, i8* %null_byte_loc8
//   br i1 %is_null7, label %null3, label %not_null4
//
// null3:                                            ; preds = %continue
//   %string_ptr = getelementptr inbounds %"struct.impala::StringValue",
//        %"struct.impala::StringValue"* %loc2, i32 0, i32 0
//   %string_len = getelementptr inbounds %"struct.impala::StringValue",
//        %"struct.impala::StringValue"* %loc2, i32 0, i32 1
//   store i8* inttoptr (i32 -2128831035 to i8*), i8** %string_ptr
//   store i32 -2128831035, i32* %string_len
//   br label %continue5
//
// not_null4:                                        ; preds = %continue
//   %4 = extractvalue { i64, i8* } %result6, 0
//   %5 = ashr i64 %4, 32
//   %6 = trunc i64 %5 to i32
//   %7 = insertvalue %"struct.impala::StringValue" zeroinitializer, i32 %6, 1
//   %result9 = extractvalue { i64, i8* } %result6, 1
//   %8 = insertvalue %"struct.impala::StringValue" %7, i8* %result9, 0
//   store %"struct.impala::StringValue" %8, %"struct.impala::StringValue"* %loc2
//   br label %continue5
//
// continue5:                                        ; preds = %not_null4, %null3
//   %is_null_phi10 = phi i1 [ true, %null3 ], [ false, %not_null4 ]
//   %has_null11 = or i1 %has_null, %is_null_phi10
//   ret i1 %has_null11
// }
//
// For each expr, we create 3 code blocks.  The null, not null and continue blocks.
// Both the null and not null branch into the continue block.  The continue block
// becomes the start of the next block for codegen (either the next expr or just the
// end of the function).
Status HashTableCtx::CodegenEvalRow(
    LlvmCodeGen* codegen, bool build, llvm::Function** fn) {
  const vector<ScalarExpr*>& exprs = build ? build_exprs_ : probe_exprs_;
  for (int i = 0; i < exprs.size(); ++i) {
    // Disable codegen for CHAR
    if (exprs[i]->type().type == TYPE_CHAR) {
      return Status("HashTableCtx::CodegenEvalRow(): CHAR NYI");
    }
  }

  // Get types to generate function prototype
  llvm::PointerType* this_ptr_type = codegen->GetStructPtrType<HashTableCtx>();
  llvm::PointerType* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();
  LlvmCodeGen::FnPrototype prototype(codegen, build ? "EvalBuildRow" : "EvalProbeRow",
      codegen->bool_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("expr_values", codegen->ptr_type()));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("expr_values_null", codegen->ptr_type()));

  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  llvm::Value* args[4];
  *fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* this_ptr = args[0];
  llvm::Value* row = args[1];
  llvm::Value* expr_values = args[2];
  llvm::Value* expr_values_null = args[3];
  llvm::Value* has_null = codegen->false_value();

  // evaluator_vector = build_expr_evals_.data() / probe_expr_evals_.data()
  llvm::Value* eval_vector = codegen->CodegenCallFunction(&builder,
      build ? IRFunction::HASH_TABLE_GET_BUILD_EXPR_EVALUATORS :
              IRFunction::HASH_TABLE_GET_PROBE_EXPR_EVALUATORS,
      this_ptr, "eval_vector");

  for (int i = 0; i < exprs.size(); ++i) {
    // TODO: refactor this to somewhere else?  This is not hash table specific except for
    // the null handling bit and would be used for anyone that needs to materialize a
    // vector of exprs
    // Convert result buffer to llvm ptr type
    int offset = expr_values_cache_.expr_values_offsets(i);
    llvm::Value* loc = builder.CreateInBoundsGEP(
        NULL, expr_values, codegen->GetI32Constant(offset), "loc_addr");
    llvm::Value* llvm_loc = builder.CreatePointerCast(loc,
        codegen->GetSlotPtrType(exprs[i]->type()), "loc");

    llvm::BasicBlock* null_block = llvm::BasicBlock::Create(context, "null", *fn);
    llvm::BasicBlock* not_null_block = llvm::BasicBlock::Create(context, "not_null", *fn);
    llvm::BasicBlock* continue_block = llvm::BasicBlock::Create(context, "continue", *fn);

    // Call expr
    llvm::Function* expr_fn;
    Status status = exprs[i]->GetCodegendComputeFn(codegen, &expr_fn);
    if (!status.ok()) {
      (*fn)->eraseFromParent(); // deletes function
      *fn = NULL;
      return Status(Substitute(
          "Problem with HashTableCtx::CodegenEvalRow(): $0", status.GetDetail()));
    }

    // Avoid bloating function by inlining too many exprs into it.
    if (i >= LlvmCodeGen::CODEGEN_INLINE_EXPRS_THRESHOLD) {
      codegen->SetNoInline(expr_fn);
    }

    llvm::Value* eval_arg = codegen->CodegenArrayAt(&builder, eval_vector, i, "eval");
    CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(
        codegen, &builder, exprs[i]->type(), expr_fn, {eval_arg, row}, "result");
    llvm::Value* is_null = result.GetIsNull();

    // Set null-byte result
    llvm::Value* null_byte = builder.CreateZExt(is_null, codegen->i8_type());
    llvm::Value* llvm_null_byte_loc = builder.CreateInBoundsGEP(
        NULL, expr_values_null, codegen->GetI32Constant(i), "null_byte_loc");
    builder.CreateStore(null_byte, llvm_null_byte_loc);
    builder.CreateCondBr(is_null, null_block, not_null_block);

    // Null block
    builder.SetInsertPoint(null_block);
    if (!stores_nulls_) {
      // hash table doesn't store nulls, no reason to keep evaluating exprs
      builder.CreateRet(codegen->true_value());
    } else {
      CodegenAssignNullValue(codegen, &builder, llvm_loc, exprs[i]->type());
      builder.CreateBr(continue_block);
    }

    // Not null block
    builder.SetInsertPoint(not_null_block);
    result.StoreToNativePtr(llvm_loc);
    builder.CreateBr(continue_block);

    // Continue block
    builder.SetInsertPoint(continue_block);
    if (stores_nulls_) {
      // Update has_null
      llvm::PHINode* is_null_phi =
          builder.CreatePHI(codegen->bool_type(), 2, "is_null_phi");
      is_null_phi->addIncoming(codegen->true_value(), null_block);
      is_null_phi->addIncoming(codegen->false_value(), not_null_block);
      has_null = builder.CreateOr(has_null, is_null_phi, "has_null");
    }
  }
  builder.CreateRet(has_null);

  // Avoid inlining a large EvalRow() function into caller.
  if (exprs.size() > LlvmCodeGen::CODEGEN_INLINE_EXPR_BATCH_THRESHOLD) {
    codegen->SetNoInline(*fn);
  }

  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status("Codegen'd HashTableCtx::EvalRow() function failed verification, "
                  "see log");
  }
  return Status::OK();
}

// Codegen for hashing the current row.  In the case with both string and non-string data
// (group by int_col, string_col), the IR looks like:
//
// define i32 @HashRow(%"class.impala::HashTableCtx"* %this_ptr, i8* %expr_values,
//    i8* %expr_values_null) #34 {
// entry:
//   %seed = call i32 @_ZNK6impala12HashTableCtx11GetHashSeedEv(
//        %"class.impala::HashTableCtx"* %this_ptr)
//   %hash = call i32 @CrcHash8(i8* %expr_values, i32 8, i32 %seed)
//   %loc_addr = getelementptr i8, i8* %expr_values, i32 8
//   %null_byte_loc = getelementptr i8, i8* %expr_values_null, i32 1
//   %null_byte = load i8, i8* %null_byte_loc
//   %is_null = icmp ne i8 %null_byte, 0
//   br i1 %is_null, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   %str_null = call i32 @CrcHash16(i8* %loc_addr, i32 16, i32 %hash)
//   br label %continue
//
// not_null:                                         ; preds = %entry
//   %str_val = bitcast i8* %loc_addr to %"struct.impala::StringValue"*
//   %0 = getelementptr inbounds %"struct.impala::StringValue",
//        %"struct.impala::StringValue"* %str_val, i32 0, i32 0
//   %1 = getelementptr inbounds %"struct.impala::StringValue",
//        %"struct.impala::StringValue"* %str_val, i32 0, i32 1
//   %ptr = load i8*, i8** %0
//   %len = load i32, i32* %1
//   %string_hash = call i32 @IrCrcHash(i8* %ptr, i32 %len, i32 %hash)
//   br label %continue
//
// continue:                                         ; preds = %not_null, %null
//   %hash_phi = phi i32 [ %string_hash, %not_null ], [ %str_null, %null ]
//   ret i32 %hash_phi
// }
Status HashTableCtx::CodegenHashRow(
    LlvmCodeGen* codegen, bool use_murmur, llvm::Function** fn) {
  for (int i = 0; i < build_exprs_.size(); ++i) {
    // Disable codegen for CHAR
    if (build_exprs_[i]->type().type == TYPE_CHAR) {
      return Status("HashTableCtx::CodegenHashRow(): CHAR NYI");
    }
  }

  // Get types to generate function prototype
  llvm::PointerType* this_ptr_type = codegen->GetStructPtrType<HashTableCtx>();

  LlvmCodeGen::FnPrototype prototype(
      codegen, (use_murmur ? "MurmurHashRow" : "HashRow"), codegen->i32_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("expr_values", codegen->ptr_type()));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("expr_values_null", codegen->ptr_type()));

  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  llvm::Value* args[3];
  *fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* this_arg = args[0];
  llvm::Value* expr_values = args[1];
  llvm::Value* expr_values_null = args[2];

  // Call GetHashSeed() to get seeds_[level_]
  llvm::Value* seed = codegen->CodegenCallFunction(
      &builder, IRFunction::HASH_TABLE_GET_HASH_SEED, this_arg, "seed");

  llvm::Value* hash_result = seed;
  const int var_result_offset = expr_values_cache_.var_result_offset();
  const int expr_values_bytes_per_row = expr_values_cache_.expr_values_bytes_per_row();
  if (var_result_offset == -1) {
    // No variable length slots, just hash what is in 'expr_expr_values_cache_'
    if (expr_values_bytes_per_row > 0) {
      llvm::Function* hash_fn = use_murmur ?
          codegen->GetMurmurHashFunction(expr_values_bytes_per_row) :
          codegen->GetHashFunction(expr_values_bytes_per_row);
      llvm::Value* len = codegen->GetI32Constant(expr_values_bytes_per_row);
      hash_result = builder.CreateCall(
          hash_fn, llvm::ArrayRef<llvm::Value*>({expr_values, len, hash_result}), "hash");
    }
  } else {
    if (var_result_offset > 0) {
      llvm::Function* hash_fn = use_murmur ?
          codegen->GetMurmurHashFunction(var_result_offset) :
          codegen->GetHashFunction(var_result_offset);
      llvm::Value* len = codegen->GetI32Constant(var_result_offset);
      hash_result = builder.CreateCall(
          hash_fn, llvm::ArrayRef<llvm::Value*>({expr_values, len, hash_result}), "hash");
    }

    // Hash string slots
    for (int i = 0; i < build_exprs_.size(); ++i) {
      if (build_exprs_[i]->type().type != TYPE_STRING &&
          build_exprs_[i]->type().type != TYPE_VARCHAR) {
        continue;
      }

      llvm::BasicBlock* null_block = NULL;
      llvm::BasicBlock* not_null_block = NULL;
      llvm::BasicBlock* continue_block = NULL;
      llvm::Value* str_null_result = NULL;

      int offset = expr_values_cache_.expr_values_offsets(i);
      llvm::Value* llvm_loc = builder.CreateInBoundsGEP(
          NULL, expr_values, codegen->GetI32Constant(offset), "loc_addr");

      // If the hash table stores nulls, we need to check if the stringval
      // evaluated to NULL
      if (stores_nulls_) {
        null_block = llvm::BasicBlock::Create(context, "null", *fn);
        not_null_block = llvm::BasicBlock::Create(context, "not_null", *fn);
        continue_block = llvm::BasicBlock::Create(context, "continue", *fn);

        llvm::Value* llvm_null_byte_loc = builder.CreateInBoundsGEP(NULL,
            expr_values_null, codegen->GetI32Constant(i), "null_byte_loc");
        llvm::Value* null_byte = builder.CreateLoad(llvm_null_byte_loc, "null_byte");
        llvm::Value* is_null = builder.CreateICmpNE(
            null_byte, codegen->GetI8Constant(0), "is_null");
        builder.CreateCondBr(is_null, null_block, not_null_block);

        // For null, we just want to call the hash function on the portion of
        // the data
        builder.SetInsertPoint(null_block);
        llvm::Function* null_hash_fn = use_murmur ?
            codegen->GetMurmurHashFunction(sizeof(StringValue)) :
            codegen->GetHashFunction(sizeof(StringValue));
        llvm::Value* len = codegen->GetI32Constant(sizeof(StringValue));
        str_null_result = builder.CreateCall(null_hash_fn,
            llvm::ArrayRef<llvm::Value*>({llvm_loc, len, hash_result}), "str_null");
        builder.CreateBr(continue_block);

        builder.SetInsertPoint(not_null_block);
      }

      // Convert expr_values_buffer_ loc to llvm value
      llvm::Value* str_val = builder.CreatePointerCast(
          llvm_loc, codegen->GetSlotPtrType(TYPE_STRING), "str_val");

      llvm::Value* ptr = builder.CreateStructGEP(NULL, str_val, 0);
      llvm::Value* len = builder.CreateStructGEP(NULL, str_val, 1);
      ptr = builder.CreateLoad(ptr, "ptr");
      len = builder.CreateLoad(len, "len");

      // Call hash(ptr, len, hash_result);
      llvm::Function* general_hash_fn =
          use_murmur ? codegen->GetMurmurHashFunction() : codegen->GetHashFunction();
      llvm::Value* string_hash_result = builder.CreateCall(general_hash_fn,
          llvm::ArrayRef<llvm::Value*>({ptr, len, hash_result}), "string_hash");

      if (stores_nulls_) {
        builder.CreateBr(continue_block);
        builder.SetInsertPoint(continue_block);
        // Use phi node to reconcile that we could have come from the string-null
        // path and string not null paths.
        llvm::PHINode* phi_node =
            builder.CreatePHI(codegen->i32_type(), 2, "hash_phi");
        phi_node->addIncoming(string_hash_result, not_null_block);
        phi_node->addIncoming(str_null_result, null_block);
        hash_result = phi_node;
      } else {
        hash_result = string_hash_result;
      }
    }
  }

  builder.CreateRet(hash_result);

  // Avoid inlining into caller if there are many exprs.
  if (build_exprs_.size() > LlvmCodeGen::CODEGEN_INLINE_EXPR_BATCH_THRESHOLD) {
    codegen->SetNoInline(*fn);
  }
  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status(
        "Codegen'd HashTableCtx::HashRow() function failed verification, see log");
  }
  return Status::OK();
}

// Codegen for HashTableCtx::Equals.  For a group by with (bigint, string),
// the IR looks like:
//
// define i1 @Equals(%"class.impala::HashTableCtx"* %this_ptr, %"class.impala::TupleRow"*
// %row,
//      i8* %expr_values, i8* %expr_values_null) #34 {
// entry:
//   %0 = alloca { i64, i8* }
//   %result = call { i8, i64 } @GetSlotRef.2(%"class.impala::ExprContext"*
//        inttoptr (i64 139107136 to %"class.impala::ExprContext"*),
//        %"class.impala::TupleRow"* %row)
//   %1 = extractvalue { i8, i64 } %result, 0
//   %is_null = trunc i8 %1 to i1
//   %null_byte_loc = getelementptr i8, i8* %expr_values_null, i32 0
//   %2 = load i8, i8* %null_byte_loc
//   %3 = icmp ne i8 %2, 0
//   %loc = getelementptr i8, i8* %expr_values, i32 0
//   %row_val = bitcast i8* %loc to i64*
//   br i1 %is_null, label %null, label %not_null
//
// false_block:                                      ; preds = %cmp9, %not_null2, %null1,
//                                                             %cmp, %not_null, %null
//   ret i1 false
//
// null:                                             ; preds = %entry
//   br i1 %3, label %continue, label %false_block
//
// not_null:                                         ; preds = %entry
//   br i1 %3, label %false_block, label %cmp
//
// continue:                                         ; preds = %cmp, %null
//   %result4 = call { i64, i8* } @GetSlotRef.3(%"class.impala::ExprContext"*
//        inttoptr (i64 139107328 to %"class.impala::ExprContext"*),
//        %"class.impala::TupleRow"* %row)
//   %4 = extractvalue { i64, i8* } %result4, 0
//   %is_null5 = trunc i64 %4 to i1
//   %null_byte_loc6 = getelementptr i8, i8* %expr_values_null, i32 1
//   %5 = load i8, i8* %null_byte_loc6
//   %6 = icmp ne i8 %5, 0
//   %loc7 = getelementptr i8, i8* %expr_values, i32 8
//   %row_val8 = bitcast i8* %loc7 to %"struct.impala::StringValue"*
//   br i1 %is_null5, label %null1, label %not_null2
//
// cmp:                                              ; preds = %not_null
//   %7 = load i64, i64* %row_val
//   %val = extractvalue { i8, i64 } %result, 1
//   %cmp_raw = icmp eq i64 %val, %7
//   br i1 %cmp_raw, label %continue, label %false_block
//
// null1:                                            ; preds = %continue
//   br i1 %6, label %continue3, label %false_block
//
// not_null2:                                        ; preds = %continue
//   br i1 %6, label %false_block, label %cmp9
//
// continue3:                                        ; preds = %cmp9, %null1
//   ret i1 true
//
// cmp9:                                             ; preds = %not_null2
//   store { i64, i8* } %result4, { i64, i8* }* %0
//   %8 = bitcast { i64, i8* }* %0 to %"struct.impala_udf::StringVal"*
//   %cmp_raw10 = call i1
//        @_Z13StringValueEqRKN10impala_udf9StringValERKN6impala11StringValueE(
//        %"struct.impala_udf::StringVal"* %8, %"struct.impala::StringValue"* %row_val8)
//   br i1 %cmp_raw10, label %continue3, label %false_block
// }
Status HashTableCtx::CodegenEquals(
    LlvmCodeGen* codegen, bool force_null_equality, llvm::Function** fn) {
  for (int i = 0; i < build_exprs_.size(); ++i) {
    // Disable codegen for CHAR
    if (build_exprs_[i]->type().type == TYPE_CHAR) {
      return Status("HashTableCtx::CodegenEquals(): CHAR NYI");
    }
  }

  // Get types to generate function prototype
  llvm::PointerType* this_ptr_type = codegen->GetStructPtrType<HashTableCtx>();
  llvm::PointerType* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();

  LlvmCodeGen::FnPrototype prototype(codegen, "Equals", codegen->bool_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("expr_values", codegen->ptr_type()));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("expr_values_null", codegen->ptr_type()));

  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  llvm::Value* args[4];
  *fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* this_ptr = args[0];
  llvm::Value* row = args[1];
  llvm::Value* expr_values = args[2];
  llvm::Value* expr_values_null = args[3];

  // eval_vector = build_expr_evals_.data()
  llvm::Value* eval_vector = codegen->CodegenCallFunction(&builder,
      IRFunction::HASH_TABLE_GET_BUILD_EXPR_EVALUATORS, this_ptr, "eval_vector");

  llvm::BasicBlock* false_block = llvm::BasicBlock::Create(context, "false_block", *fn);
  for (int i = 0; i < build_exprs_.size(); ++i) {
    llvm::BasicBlock* null_block = llvm::BasicBlock::Create(context, "null", *fn);
    llvm::BasicBlock* not_null_block = llvm::BasicBlock::Create(context, "not_null", *fn);
    llvm::BasicBlock* continue_block = llvm::BasicBlock::Create(context, "continue", *fn);

    // call GetValue on build_exprs[i]
    llvm::Function* expr_fn;
    Status status = build_exprs_[i]->GetCodegendComputeFn(codegen, &expr_fn);
    if (!status.ok()) {
      (*fn)->eraseFromParent(); // deletes function
      *fn = NULL;
      return Status(
          Substitute("Problem with HashTableCtx::CodegenEquals: $0", status.GetDetail()));
    }
    if (build_exprs_.size() > LlvmCodeGen::CODEGEN_INLINE_EXPRS_THRESHOLD) {
      // Avoid bloating function by inlining too many exprs into it.
      codegen->SetNoInline(expr_fn);
    }

    // Load ScalarExprEvaluator*: eval = eval_vector[i];
    llvm::Value* eval_arg = codegen->CodegenArrayAt(&builder, eval_vector, i, "eval");
    // Evaluate the expression.
    CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        build_exprs_[i]->type(), expr_fn, {eval_arg, row}, "result");
    llvm::Value* is_null = result.GetIsNull();

    // Determine if row is null (i.e. expr_values_null[i] == true). In
    // the case where the hash table does not store nulls, this is always false.
    llvm::Value* row_is_null = codegen->false_value();

    // We consider null values equal if we are comparing build rows or if the join
    // predicate is <=>
    if (force_null_equality || finds_nulls_[i]) {
      llvm::Value* llvm_null_byte_loc = builder.CreateInBoundsGEP(
          NULL, expr_values_null, codegen->GetI32Constant(i), "null_byte_loc");
      llvm::Value* null_byte = builder.CreateLoad(llvm_null_byte_loc);
      row_is_null =
          builder.CreateICmpNE(null_byte, codegen->GetI8Constant(0));
    }

    // Get llvm value for row_val from 'expr_values'
    int offset = expr_values_cache_.expr_values_offsets(i);
    llvm::Value* loc = builder.CreateInBoundsGEP(
        NULL, expr_values, codegen->GetI32Constant(offset), "loc");
    llvm::Value* row_val = builder.CreatePointerCast(
        loc, codegen->GetSlotPtrType(build_exprs_[i]->type()), "row_val");

    // Branch for GetValue() returning NULL
    builder.CreateCondBr(is_null, null_block, not_null_block);

    // Null block
    builder.SetInsertPoint(null_block);
    builder.CreateCondBr(row_is_null, continue_block, false_block);

    // Not-null block
    builder.SetInsertPoint(not_null_block);
    if (stores_nulls_) {
      llvm::BasicBlock* cmp_block = llvm::BasicBlock::Create(context, "cmp", *fn);
      // First need to compare that row expr[i] is not null
      builder.CreateCondBr(row_is_null, false_block, cmp_block);
      builder.SetInsertPoint(cmp_block);
    }
    // Check result == row_val
    llvm::Value* is_equal = result.EqToNativePtr(row_val);
    builder.CreateCondBr(is_equal, continue_block, false_block);

    builder.SetInsertPoint(continue_block);
  }
  builder.CreateRet(codegen->true_value());

  builder.SetInsertPoint(false_block);
  builder.CreateRet(codegen->false_value());

  // Avoid inlining into caller if it is large.
  if (build_exprs_.size() > LlvmCodeGen::CODEGEN_INLINE_EXPR_BATCH_THRESHOLD) {
    codegen->SetNoInline(*fn);
  }
  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status("Codegen'd HashTableCtx::Equals() function failed verification, "
                  "see log");
  }
  return Status::OK();
}

Status HashTableCtx::ReplaceHashTableConstants(LlvmCodeGen* codegen,
    bool stores_duplicates, int num_build_tuples, llvm::Function* fn,
    HashTableReplacedConstants* replacement_counts) {
  replacement_counts->stores_nulls = codegen->ReplaceCallSitesWithBoolConst(
      fn, stores_nulls(), "stores_nulls");
  replacement_counts->finds_some_nulls = codegen->ReplaceCallSitesWithBoolConst(
      fn, finds_some_nulls(), "finds_some_nulls");
  replacement_counts->stores_tuples = codegen->ReplaceCallSitesWithBoolConst(
      fn, num_build_tuples == 1, "stores_tuples");
  replacement_counts->stores_duplicates = codegen->ReplaceCallSitesWithBoolConst(
      fn, stores_duplicates, "stores_duplicates");
  replacement_counts->quadratic_probing = codegen->ReplaceCallSitesWithBoolConst(
      fn, FLAGS_enable_quadratic_probing, "quadratic_probing");
  return Status::OK();
}
