// Copyright 2012 Cloudera Inc.
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

#include "exec/hash-table.inline.h"

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "exprs/slot-ref.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"

using namespace impala;
using namespace llvm;
using namespace std;

const char* HashTableCtx::LLVM_CLASS_NAME = "class.impala::HashTableCtx";

HashTableCtx::HashTableCtx(const std::vector<ExprContext*>& build_expr_ctxs,
    const std::vector<ExprContext*>& probe_expr_ctxs, bool stores_nulls, bool finds_nulls,
    int32_t initial_seed, int max_levels)
    : build_expr_ctxs_(build_expr_ctxs),
      probe_expr_ctxs_(probe_expr_ctxs),
      stores_nulls_(stores_nulls),
      finds_nulls_(finds_nulls),
      level_(0) {
  // Compute the layout and buffer size to store the evaluated expr results
  DCHECK_EQ(build_expr_ctxs_.size(), probe_expr_ctxs_.size());
  results_buffer_size_ = Expr::ComputeResultsLayout(build_expr_ctxs_,
      &expr_values_buffer_offsets_, &var_result_begin_);
  expr_values_buffer_ = new uint8_t[results_buffer_size_];
  memset(expr_values_buffer_, 0, sizeof(uint8_t) * results_buffer_size_);
  expr_value_null_bits_ = new uint8_t[build_expr_ctxs.size()];

  // Populate the seeds to use for all the levels. TODO: throw some primes in here.
  DCHECK_GE(max_levels, 0);
  seeds_.resize(max_levels + 1);
  seeds_[0] = initial_seed;
  for (int i = 1; i <= max_levels; ++i) {
    seeds_[i] = initial_seed + (i << 16);
  }
}

void HashTableCtx::Close() {
  // TODO: use tr1::array?
  DCHECK_NOTNULL(expr_values_buffer_);
  delete[] expr_values_buffer_;
  expr_values_buffer_ = NULL;
  DCHECK_NOTNULL(expr_value_null_bits_);
  delete[] expr_value_null_bits_;
  expr_value_null_bits_ = NULL;
}

bool HashTableCtx::EvalRow(TupleRow* row, const vector<ExprContext*>& ctxs) {
  // Put a non-zero constant in the result location for NULL.
  // We don't want(NULL, 1) to hash to the same as (0, 1).
  // This needs to be as big as the biggest primitive type since the bytes
  // get copied directly.
  int64_t null_value[] = { HashUtil::FNV_SEED, HashUtil::FNV_SEED };

  bool has_null = false;
  for (int i = 0; i < ctxs.size(); ++i) {
    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    void* val = ctxs[i]->GetValue(row);
    if (val == NULL) {
      // If the table doesn't store nulls, no reason to keep evaluating
      if (!stores_nulls_) return true;

      expr_value_null_bits_[i] = true;
      val = reinterpret_cast<void*>(&null_value);
      has_null = true;
    } else {
      expr_value_null_bits_[i] = false;
    }
    RawValue::Write(val, loc, build_expr_ctxs_[i]->root()->type(), NULL);
  }
  return has_null;
}

// Codegen for evaluating a tuple row over either build_expr_ctxs_ or probe_expr_ctxs_.
Function* HashTableCtx::CodegenEvalRow(RuntimeState* state, bool build) {
  // TODO: Codegen -- disabled for now.
  return NULL;
}


uint32_t HashTableCtx::HashVariableLenRow() {
  uint32_t hash = seeds_[level_];
  // Hash the non-var length portions (if there are any)
  if (var_result_begin_ != 0) {
    hash = HashUtil::Hash(expr_values_buffer_, var_result_begin_, hash);
  }

  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    // non-string and null slots are already part of expr_values_buffer
    if (build_expr_ctxs_[i]->root()->type().type != TYPE_STRING &&
        build_expr_ctxs_[i]->root()->type().type != TYPE_VARCHAR) continue;

    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    if (expr_value_null_bits_[i]) {
      // Hash the null random seed values at 'loc'
      hash = HashUtil::Hash(loc, sizeof(StringValue), hash);
    } else {
      // Hash the string
      StringValue* str = reinterpret_cast<StringValue*>(loc);
      hash = HashUtil::Hash(str->ptr, str->len, hash);
    }
  }
  return hash;
}

// Codegen for hashing the current row.
Function* HashTableCtx::CodegenHashCurrentRow(RuntimeState* state) {
  // TODO: Codegen -- disabled for now.
  return NULL;
}

bool HashTableCtx::Equals(TupleRow* build_row) {
  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    void* val = build_expr_ctxs_[i]->GetValue(build_row);
    if (val == NULL) {
      if (!stores_nulls_) return false;
      if (!expr_value_null_bits_[i]) return false;
      continue;
    } else {
      if (expr_value_null_bits_[i]) return false;
    }

    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    if (!RawValue::Eq(loc, val, build_expr_ctxs_[i]->root()->type())) {
      return false;
    }
  }
  return true;
}

// Codegen for HashTableCtx::Equals.
Function* HashTableCtx::CodegenEquals(RuntimeState* state) {
  // TODO: Codegen
  return NULL;
}

const float HashTable::MAX_BUCKET_OCCUPANCY_FRACTION = 0.75f;
static const int PAGE_SIZE = 8 * 1024 * 1024;

HashTable::HashTable(RuntimeState* state, int num_build_tuples,
    MemTracker* mem_tracker, bool stores_tuples, int64_t num_buckets)
  : state_(state),
    num_build_tuples_(num_build_tuples),
    stores_tuples_(stores_tuples),
    num_filled_buckets_(0),
    num_nodes_(0),
    mem_pool_(new MemPool(mem_tracker)),
    num_data_pages_(0),
    next_node_(NULL),
    node_remaining_current_page_(0),
    mem_tracker_(mem_tracker),
    mem_limit_exceeded_(false) {
  DCHECK(mem_tracker != NULL);
  DCHECK_EQ((num_buckets & (num_buckets-1)), 0) << "num_buckets must be a power of 2";
  DCHECK_GT(num_buckets, 0) << "num_buckets must be larger than 0";
  buckets_.resize(num_buckets);
  num_buckets_ = num_buckets;
  num_buckets_till_resize_ = MAX_BUCKET_OCCUPANCY_FRACTION * num_buckets_;
  mem_tracker_->Consume(buckets_.capacity() * sizeof(Bucket));

  GrowNodeArray();
}

void HashTable::Close() {
  mem_pool_->FreeAll();
  if (ImpaladMetrics::HASH_TABLE_TOTAL_BYTES != NULL) {
    ImpaladMetrics::HASH_TABLE_TOTAL_BYTES->Increment(-num_data_pages_ * PAGE_SIZE);
  }
  mem_tracker_->Release(buckets_.capacity() * sizeof(Bucket));
  buckets_.clear();
}

void HashTable::AddBitmapFilters(HashTableCtx* ht_ctx) {
  DCHECK_NOTNULL(ht_ctx);
  DCHECK_EQ(ht_ctx->build_expr_ctxs_.size(), ht_ctx->probe_expr_ctxs_.size());
  vector<pair<SlotId, Bitmap*> > bitmaps;
  bitmaps.resize(ht_ctx->probe_expr_ctxs_.size());
  for (int i = 0; i < ht_ctx->build_expr_ctxs_.size(); ++i) {
    if (ht_ctx->probe_expr_ctxs_[i]->root()->is_slotref()) {
      bitmaps[i].first =
          reinterpret_cast<SlotRef*>(ht_ctx->probe_expr_ctxs_[i]->root())->slot_id();
      bitmaps[i].second = new Bitmap(state_->slot_filter_bitmap_size());
    } else {
      bitmaps[i].second = NULL;
    }
  }
  // For the bitmap filters, always use the initial seed. The other parts of the plan
  // tree (e.g. the scan node) relies on this.
  uint32_t seed = ht_ctx->seeds_[0];

  // Walk the build table and generate a bitmap for each probe side slot.
  HashTable::Iterator iter = Begin();
  while (iter != End()) {
    TupleRow* row = iter.GetRow();
    for (int i = 0; i < ht_ctx->build_expr_ctxs_.size(); ++i) {
      if (bitmaps[i].second == NULL) continue;
      void* e = ht_ctx->build_expr_ctxs_[i]->GetValue(row);
      uint32_t h =
          RawValue::GetHashValue(e, ht_ctx->build_expr_ctxs_[i]->root()->type(), seed);
      bitmaps[i].second->Set<true>(h, true);
    }
    iter.Next<false>(ht_ctx);
  }

  // Add all the bitmaps to the runtime state.
  for (int i = 0; i < bitmaps.size(); ++i) {
    if (bitmaps[i].second == NULL) continue;
    state_->AddBitmapFilter(bitmaps[i].first, bitmaps[i].second);
    VLOG(2) << "Bitmap filter added on slot: " << bitmaps[i].first;
    delete bitmaps[i].second;
  }
}

void HashTable::ResizeBuckets(int64_t num_buckets) {
  DCHECK_EQ((num_buckets & (num_buckets-1)), 0)
      << "num_buckets=" << num_buckets << " must be a power of 2";

  int64_t old_num_buckets = num_buckets_;
  // This can be a rather large allocation so check the limit before (to prevent
  // us from going over the limits too much).
  int64_t delta_size = (num_buckets - old_num_buckets) * sizeof(Bucket);
  if (!mem_tracker_->TryConsume(delta_size)) {
    MemLimitExceeded(delta_size);
    return;
  }
  buckets_.resize(num_buckets);

  // If we're doubling the number of buckets, all nodes in a particular bucket
  // either remain there, or move down to an analogous bucket in the other half.
  // In order to efficiently check which of the two buckets a node belongs in, the number
  // of buckets must be a power of 2.
  bool doubled_buckets = (num_buckets == old_num_buckets * 2);
  for (int i = 0; i < num_buckets_; ++i) {
    Bucket* bucket = &buckets_[i];
    Bucket* sister_bucket = &buckets_[i + old_num_buckets];
    Node* last_node = NULL;
    Node* node = bucket->node;

    while (node != NULL) {
      Node* next = node->next;
      uint32_t hash = node->hash;

      bool node_must_move;
      Bucket* move_to;
      if (doubled_buckets) {
        node_must_move = ((hash & old_num_buckets) != 0);
        move_to = sister_bucket;
      } else {
        int64_t bucket_idx = hash & (num_buckets - 1);
        node_must_move = (bucket_idx != i);
        move_to = &buckets_[bucket_idx];
      }

      if (node_must_move) {
        MoveNode(bucket, move_to, node, last_node);
      } else {
        last_node = node;
      }

      node = next;
    }
  }

  num_buckets_ = num_buckets;
  num_buckets_till_resize_ = MAX_BUCKET_OCCUPANCY_FRACTION * num_buckets_;
}

void HashTable::GrowNodeArray() {
  node_remaining_current_page_ = PAGE_SIZE / sizeof(Node);
  next_node_ = reinterpret_cast<Node*>(mem_pool_->Allocate(PAGE_SIZE));
  ++num_data_pages_;
  if (ImpaladMetrics::HASH_TABLE_TOTAL_BYTES != NULL) {
    ImpaladMetrics::HASH_TABLE_TOTAL_BYTES->Increment(PAGE_SIZE);
  }
  if (mem_tracker_->LimitExceeded()) MemLimitExceeded(PAGE_SIZE);
}

void HashTable::MemLimitExceeded(int64_t allocation_size) {
  mem_limit_exceeded_ = true;
  if (state_ != NULL) state_->SetMemLimitExceeded(mem_tracker_, allocation_size);
}

string HashTable::DebugString(bool skip_empty, bool show_match,
    const RowDescriptor* desc) {
  stringstream ss;
  ss << endl;
  for (int i = 0; i < buckets_.size(); ++i) {
    Node* node = buckets_[i].node;
    bool first = true;
    if (skip_empty && node == NULL) continue;
    ss << i << ": ";
    while (node != NULL) {
      if (!first) ss << ",";
      ss << node << "(" << node->data << ")";
      if (desc != NULL) ss << " " << PrintRow(GetRow(node), *desc);
      if (show_match) {
        if (node->matched) {
          ss << " [M]";
        } else {
          ss << " [U]";
        }
      }
      node = node->next;
      first = false;
    }
    ss << endl;
  }
  return ss.str();
}
