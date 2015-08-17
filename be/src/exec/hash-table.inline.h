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


#ifndef IMPALA_EXEC_HASH_TABLE_INLINE_H
#define IMPALA_EXEC_HASH_TABLE_INLINE_H

#include "exec/hash-table.h"

#include "exprs/expr.h"
#include "exprs/expr-context.h"

namespace impala {

inline bool HashTableCtx::EvalAndHashBuild(TupleRow* row, uint32_t* hash) {
  bool has_null = EvalBuildRow(row);
  if (!stores_nulls_ && has_null) return false;
  *hash = HashCurrentRow();
  return true;
}

inline bool HashTableCtx::EvalAndHashProbe(TupleRow* row, uint32_t* hash) {
  bool has_null = EvalProbeRow(row);
  if (has_null && !(stores_nulls_ && finds_some_nulls_)) return false;
  *hash = HashCurrentRow();
  return true;
}

template <bool FORCE_NULL_EQUALITY>
inline int64_t HashTable::Probe(Bucket* buckets, int64_t num_buckets,
    HashTableCtx* ht_ctx, uint32_t hash, bool* found) {
  DCHECK(buckets != NULL);
  DCHECK_GT(num_buckets, 0);
  *found = false;
  int64_t bucket_idx = hash & (num_buckets - 1);

  // In case of linear probing it counts the total number of steps for statistics and
  // for knowing when to exit the loop (e.g. by capping the total travel length). In case
  // of quadratic probing it is also used for calculating the length of the next jump.
  int64_t step = 0;
  do {
    Bucket* bucket = &buckets[bucket_idx];
    if (!bucket->filled) return bucket_idx;
    if (hash == bucket->hash) {
      if (ht_ctx != NULL &&
          ht_ctx->Equals<FORCE_NULL_EQUALITY>(GetRow(bucket, ht_ctx->row_))) {
        *found = true;
        return bucket_idx;
      }
      // Row equality failed, or not performed. This is a hash collision. Continue
      // searching.
      ++num_hash_collisions_;
    }
    // Move to the next bucket.
    ++step;
    ++travel_length_;
    if (quadratic_probing_) {
      // The i-th probe location is idx = (hash + (step * (step + 1)) / 2) mod num_buckets.
      // This gives num_buckets unique idxs (between 0 and N-1) when num_buckets is a power
      // of 2.
      bucket_idx = (bucket_idx + step) & (num_buckets - 1);
    } else {
      bucket_idx = (bucket_idx + 1) & (num_buckets - 1);
    }
  } while (LIKELY(step < num_buckets));
  DCHECK_EQ(num_filled_buckets_, num_buckets) << "Probing of a non-full table "
      << "failed: " << quadratic_probing_ << " " << hash;
  return Iterator::BUCKET_NOT_FOUND;
}

inline HashTable::HtData* HashTable::InsertInternal(HashTableCtx* ht_ctx,
    uint32_t hash) {
  ++num_probes_;
  bool found = false;
  int64_t bucket_idx = Probe<true>(buckets_, num_buckets_, ht_ctx, hash, &found);
  DCHECK_NE(bucket_idx, Iterator::BUCKET_NOT_FOUND);
  if (found) {
    // We need to insert a duplicate node, note that this may fail to allocate memory.
    DuplicateNode* new_node = InsertDuplicateNode(bucket_idx);
    if (UNLIKELY(new_node == NULL)) return NULL;
    return &new_node->htdata;
  } else {
    PrepareBucketForInsert(bucket_idx, hash);
    return &buckets_[bucket_idx].bucketData.htdata;
  }
}

inline bool HashTable::Insert(HashTableCtx* ht_ctx,
    const BufferedTupleStream::RowIdx& idx, TupleRow* row, uint32_t hash) {
  if (stores_tuples_) return Insert(ht_ctx, row->GetTuple(0), hash);
  HtData* htdata = InsertInternal(ht_ctx, hash);
  // If successful insert, update the contents of the newly inserted entry with 'idx'.
  if (LIKELY(htdata != NULL)) {
    htdata->idx = idx;
    return true;
  }
  return false;
}

inline bool HashTable::Insert(HashTableCtx* ht_ctx, Tuple* tuple, uint32_t hash) {
  DCHECK(stores_tuples_);
  HtData* htdata = InsertInternal(ht_ctx, hash);
  // If successful insert, update the contents of the newly inserted entry with 'tuple'.
  if (LIKELY(htdata != NULL)) {
    htdata->tuple = tuple;
    return true;
  }
  return false;
}

inline HashTable::Iterator HashTable::FindProbeRow(HashTableCtx* ht_ctx, uint32_t hash) {
  ++num_probes_;
  bool found = false;
  int64_t bucket_idx = Probe<false>(buckets_, num_buckets_, ht_ctx, hash, &found);
  if (found) {
    return Iterator(this, ht_ctx->row(), bucket_idx,
        buckets_[bucket_idx].bucketData.duplicates);
  }
  return End();
}

inline HashTable::Iterator HashTable::FindBuildRowBucket(
    HashTableCtx* ht_ctx, uint32_t hash, bool* found) {
  ++num_probes_;
  int64_t bucket_idx = Probe<true>(buckets_, num_buckets_, ht_ctx, hash, found);
  DuplicateNode* duplicates = LIKELY(bucket_idx != Iterator::BUCKET_NOT_FOUND) ?
      buckets_[bucket_idx].bucketData.duplicates : NULL;
  return Iterator(this, ht_ctx->row(), bucket_idx, duplicates);
}

inline HashTable::Iterator HashTable::Begin(HashTableCtx* ctx) {
  int64_t bucket_idx = Iterator::BUCKET_NOT_FOUND;
  DuplicateNode* node = NULL;
  NextFilledBucket(&bucket_idx, &node);
  return Iterator(this, ctx->row(), bucket_idx, node);
}

inline HashTable::Iterator HashTable::FirstUnmatched(HashTableCtx* ctx) {
  int64_t bucket_idx = Iterator::BUCKET_NOT_FOUND;
  DuplicateNode* node = NULL;
  NextFilledBucket(&bucket_idx, &node);
  Iterator it(this, ctx->row(), bucket_idx, node);
  // Check whether the bucket, or its first duplicate node, is matched. If it is not
  // matched, then return. Otherwise, move to the first unmatched entry (node or bucket).
  Bucket* bucket = &buckets_[bucket_idx];
  if ((!bucket->hasDuplicates && bucket->matched) ||
      (bucket->hasDuplicates && node->matched)) {
    it.NextUnmatched();
  }
  return it;
}

inline void HashTable::NextFilledBucket(int64_t* bucket_idx, DuplicateNode** node) {
  ++*bucket_idx;
  for (; *bucket_idx < num_buckets_; ++*bucket_idx) {
    if (buckets_[*bucket_idx].filled) {
      *node = buckets_[*bucket_idx].bucketData.duplicates;
      return;
    }
  }
  // Reached the end of the hash table.
  *bucket_idx = Iterator::BUCKET_NOT_FOUND;
  *node = NULL;
}

inline void HashTable::PrepareBucketForInsert(int64_t bucket_idx, uint32_t hash) {
  DCHECK_GE(bucket_idx, 0);
  DCHECK_LT(bucket_idx, num_buckets_);
  Bucket* bucket = &buckets_[bucket_idx];
  DCHECK(!bucket->filled);
  ++num_filled_buckets_;
  bucket->filled = true;
  bucket->matched = false;
  bucket->hasDuplicates = false;
  bucket->hash = hash;
}

inline HashTable::DuplicateNode* HashTable::AppendNextNode(Bucket* bucket) {
  DCHECK_GT(node_remaining_current_page_, 0);
  bucket->bucketData.duplicates = next_node_;
  ++num_duplicate_nodes_;
  --node_remaining_current_page_;
  return next_node_++;
}

inline HashTable::DuplicateNode* HashTable::InsertDuplicateNode(int64_t bucket_idx) {
  DCHECK_GE(bucket_idx, 0);
  DCHECK_LT(bucket_idx, num_buckets_);
  Bucket* bucket = &buckets_[bucket_idx];
  DCHECK(bucket->filled);
  // Allocate one duplicate node for the new data and one for the preexisting data,
  // if needed.
  while (node_remaining_current_page_ < 1 + !bucket->hasDuplicates) {
    if (UNLIKELY(!GrowNodeArray())) return NULL;
  }
  if (!bucket->hasDuplicates) {
    // This is the first duplicate in this bucket. It means that we need to convert
    // the current entry in the bucket to a node and link it from the bucket.
    next_node_->htdata.idx = bucket->bucketData.htdata.idx;
    DCHECK(!bucket->matched);
    next_node_->matched = false;
    next_node_->next = NULL;
    AppendNextNode(bucket);
    bucket->hasDuplicates = true;
    ++num_buckets_with_duplicates_;
  }
  // Link a new node.
  next_node_->next = bucket->bucketData.duplicates;
  next_node_->matched = false;
  return AppendNextNode(bucket);
}

inline TupleRow* HashTable::GetRow(HtData& htdata, TupleRow* row) const {
  if (stores_tuples_) {
    return reinterpret_cast<TupleRow*>(&htdata.tuple);
  } else {
    tuple_stream_->GetTupleRow(htdata.idx, row);
    return row;
  }
}

inline TupleRow* HashTable::GetRow(Bucket* bucket, TupleRow* row) const {
  DCHECK(bucket != NULL);
  if (UNLIKELY(bucket->hasDuplicates)) {
    DuplicateNode* duplicate = bucket->bucketData.duplicates;
    DCHECK(duplicate != NULL);
    return GetRow(duplicate->htdata, row);
  } else {
    return GetRow(bucket->bucketData.htdata, row);
  }
}

inline TupleRow* HashTable::Iterator::GetRow() const {
  DCHECK(!AtEnd());
  DCHECK(table_ != NULL);
  DCHECK(row_ != NULL);
  Bucket* bucket = &table_->buckets_[bucket_idx_];
  if (UNLIKELY(bucket->hasDuplicates)) {
    DCHECK(node_ != NULL);
    return table_->GetRow(node_->htdata, row_);
  } else {
    return table_->GetRow(bucket->bucketData.htdata, row_);
  }
}

inline Tuple* HashTable::Iterator::GetTuple() const {
  DCHECK(!AtEnd());
  DCHECK(table_->stores_tuples_);
  Bucket* bucket = &table_->buckets_[bucket_idx_];
  // TODO: To avoid the hasDuplicates check, store the HtData* in the Iterator.
  if (UNLIKELY(bucket->hasDuplicates)) {
    DCHECK(node_ != NULL);
    return node_->htdata.tuple;
  } else {
    return bucket->bucketData.htdata.tuple;
  }
}

inline void HashTable::Iterator::SetTuple(Tuple* tuple, uint32_t hash) {
  DCHECK(!AtEnd());
  DCHECK(table_->stores_tuples_);
  table_->PrepareBucketForInsert(bucket_idx_, hash);
  table_->buckets_[bucket_idx_].bucketData.htdata.tuple = tuple;
}

inline void HashTable::Iterator::SetMatched() {
  DCHECK(!AtEnd());
  Bucket* bucket = &table_->buckets_[bucket_idx_];
  if (bucket->hasDuplicates) {
    node_->matched = true;
  } else {
    bucket->matched = true;
  }
  // Used for disabling spilling of hash tables in right and full-outer joins with
  // matches. See IMPALA-1488.
  table_->has_matches_ = true;
}

inline bool HashTable::Iterator::IsMatched() const {
  DCHECK(!AtEnd());
  Bucket* bucket = &table_->buckets_[bucket_idx_];
  if (bucket->hasDuplicates) {
    return node_->matched;
  }
  return bucket->matched;
}

inline void HashTable::Iterator::SetAtEnd() {
  bucket_idx_ = BUCKET_NOT_FOUND;
  node_ = NULL;
}

inline void HashTable::Iterator::Next() {
  DCHECK(!AtEnd());
  if (table_->buckets_[bucket_idx_].hasDuplicates && node_->next != NULL) {
    node_ = node_->next;
  } else {
    table_->NextFilledBucket(&bucket_idx_, &node_);
  }
}

inline void HashTable::Iterator::NextDuplicate() {
  DCHECK(!AtEnd());
  if (table_->buckets_[bucket_idx_].hasDuplicates && node_->next != NULL) {
    node_ = node_->next;
  } else {
    bucket_idx_ = BUCKET_NOT_FOUND;
    node_ = NULL;
  }
}

inline void HashTable::Iterator::NextUnmatched() {
  DCHECK(!AtEnd());
  Bucket* bucket = &table_->buckets_[bucket_idx_];
  // Check if there is any remaining unmatched duplicate node in the current bucket.
  if (bucket->hasDuplicates) {
    while (node_->next != NULL) {
      node_ = node_->next;
      if (!node_->matched) return;
    }
  }
  // Move to the next filled bucket and return if this bucket is not matched or
  // iterate to the first not matched duplicate node.
  table_->NextFilledBucket(&bucket_idx_, &node_);
  while (bucket_idx_ != Iterator::BUCKET_NOT_FOUND) {
    bucket = &table_->buckets_[bucket_idx_];
    if (!bucket->hasDuplicates) {
      if (!bucket->matched) return;
    } else {
      while (node_->matched && node_->next != NULL) {
        node_ = node_->next;
      }
      if (!node_->matched) return;
    }
    table_->NextFilledBucket(&bucket_idx_, &node_);
  }
}

inline void HashTableCtx::set_level(int level) {
  DCHECK_GE(level, 0);
  DCHECK_LT(level, seeds_.size());
  level_ = level;
}

inline int64_t HashTable::CurrentMemSize() const {
  return num_buckets_ * sizeof(Bucket) + num_duplicate_nodes_ * sizeof(DuplicateNode);
}

inline int64_t HashTable::NumInsertsBeforeResize() const {
  return max<int64_t>(0,
      static_cast<int64_t>(num_buckets_ * MAX_FILL_FACTOR) - num_filled_buckets_);
}

}

#endif
