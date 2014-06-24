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

namespace impala {

inline HashTable::Iterator HashTable::Find(TupleRow* probe_row) {
  uint32_t hash;
  if (!EvalAndHashProbe(probe_row, &hash)) return End();
  int64_t bucket_idx = hash & (num_buckets_ - 1);

  Bucket* bucket = &buckets_[bucket_idx];
  Node* node = bucket->node_;
  while (node != NULL) {
    if (node->hash_ == hash && Equals(GetRow(node))) {
      return Iterator(this, bucket_idx, node, hash);
    }
    node = node->next_;
  }

  return End();
}

inline HashTable::Iterator HashTable::Begin() {
  int64_t bucket_idx = -1;
  Bucket* bucket = NextBucket(&bucket_idx);
  if (bucket != NULL) return Iterator(this, bucket_idx, bucket->node_, 0);
  return End();
}

inline HashTable::Bucket* HashTable::NextBucket(int64_t* bucket_idx) {
  ++*bucket_idx;
  for (; *bucket_idx < num_buckets_; ++*bucket_idx) {
    if (buckets_[*bucket_idx].node_ != NULL) return &buckets_[*bucket_idx];
  }
  *bucket_idx = -1;
  return NULL;
}

inline void HashTable::InsertImpl(void* data) {
  if (UNLIKELY(mem_limit_exceeded_)) return;
  if (UNLIKELY(num_filled_buckets_ > num_buckets_till_resize_)) {
    // TODO: next prime instead of double?
    ResizeBuckets(num_buckets_ * 2);
    if (UNLIKELY(mem_limit_exceeded_)) return;
  }

  uint32_t hash = HashCurrentRow();
  int64_t bucket_idx = hash & (num_buckets_ - 1);
  if (node_remaining_current_page_ == 0) {
    GrowNodeArray();
    if (UNLIKELY(mem_limit_exceeded_)) return;
  }
  next_node_->hash_ = hash;
  next_node_->data_ = data;
  next_node_->matched_ = false;
  AddToBucket(&buckets_[bucket_idx], next_node_);
  DCHECK_GT(node_remaining_current_page_, 0);
  --node_remaining_current_page_;
  ++next_node_;
  ++num_nodes_;
}

inline void HashTable::AddToBucket(Bucket* bucket, Node* node) {
  if (bucket->node_ == NULL) ++num_filled_buckets_;
  node->next_ = bucket->node_;
  bucket->node_ = node;
}

inline bool HashTable::EvalAndHashBuild(TupleRow* row, uint32_t* hash) {
  bool has_null = EvalBuildRow(row);
  if (!stores_nulls_ && has_null) return false;
  *hash = HashCurrentRow();
  return true;
}

inline bool HashTable::EvalAndHashProbe(TupleRow* row, uint32_t* hash) {
  bool has_null = EvalProbeRow(row);
  if ((!stores_nulls_ || !finds_nulls_) && has_null) return false;
  *hash = HashCurrentRow();
  return true;
}

inline void HashTable::MoveNode(Bucket* from_bucket, Bucket* to_bucket,
    Node* node, Node* previous_node) {
  if (previous_node != NULL) {
    previous_node->next_ = node->next_;
  } else {
    // Update bucket directly
    from_bucket->node_ = node->next_;
    if (node->next_ == NULL) --num_filled_buckets_;
  }

  AddToBucket(to_bucket, node);
}

template<bool check_match>
inline void HashTable::Iterator::Next() {
  if (bucket_idx_ == -1) return;

  // TODO: this should prefetch the next tuplerow
  // Iterator is not from a full table scan, evaluate equality now.  Only the current
  // bucket needs to be scanned. 'expr_values_buffer_' contains the results
  // for the current probe row.
  if (check_match) {
    // TODO: this should prefetch the next node
    Node* node = node_->next_;
    while (node != NULL) {
      if (node->hash_ == scan_hash_ && table_->Equals(table_->GetRow(node))) {
        node_ = node;
        return;
      }
      node = node->next_;
    }
    *this = table_->End();
  } else {
    // Move onto the next chained node
    if (node_->next_ != NULL) {
      node_ = node_->next_;
      return;
    }

    // Move onto the next bucket
    Bucket* bucket = table_->NextBucket(&bucket_idx_);
    if (bucket == NULL) {
      bucket_idx_ = -1;
      node_ = NULL;
    } else {
      node_ = bucket->node_;
    }
  }
}

}

#endif
