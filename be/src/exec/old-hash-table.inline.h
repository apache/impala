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


#ifndef IMPALA_EXEC_OLD_HASH_TABLE_INLINE_H
#define IMPALA_EXEC_OLD_HASH_TABLE_INLINE_H

#include "exec/old-hash-table.h"

namespace impala {

inline OldHashTable::Iterator OldHashTable::Find(TupleRow* probe_row) {
  uint32_t hash;
  if (!EvalAndHashProbe(probe_row, &hash)) return End();
  int64_t bucket_idx = hash & (num_buckets_ - 1);
  Bucket* bucket = &buckets_[bucket_idx];
  Node* node = bucket->node;
  while (node != NULL) {
    if (node->hash == hash && Equals(GetRow(node))) {
      return Iterator(this, bucket_idx, node, hash);
    }
    node = node->next;
  }
  return End();
}

inline OldHashTable::Iterator OldHashTable::Begin() {
  int64_t bucket_idx = -1;
  Bucket* bucket = NextBucket(&bucket_idx);
  if (bucket != NULL) return Iterator(this, bucket_idx, bucket->node, 0);
  return End();
}

inline OldHashTable::Iterator OldHashTable::FirstUnmatched() {
  int64_t bucket_idx = -1;
  Bucket* bucket = NextBucket(&bucket_idx);
  while (bucket != NULL) {
    Node* node = bucket->node;
    while (node != NULL && node->matched) {
      node = node->next;
    }
    if (node == NULL) {
      bucket = NextBucket(&bucket_idx);
    } else {
      DCHECK(!node->matched);
      return Iterator(this, bucket_idx, node, 0);
    }
  }
  return End();
}

inline OldHashTable::Bucket* OldHashTable::NextBucket(int64_t* bucket_idx) {
  ++*bucket_idx;
  for (; *bucket_idx < num_buckets_; ++*bucket_idx) {
    if (buckets_[*bucket_idx].node != NULL) return &buckets_[*bucket_idx];
  }
  *bucket_idx = -1;
  return NULL;
}

inline bool OldHashTable::InsertImpl(void* data) {
  uint32_t hash = HashCurrentRow();
  int64_t bucket_idx = hash & (num_buckets_ - 1);
  if (node_remaining_current_page_ == 0) {
    GrowNodeArray();
    if (UNLIKELY(mem_limit_exceeded_)) return false;
  }
  next_node_->hash = hash;
  next_node_->data = data;
  next_node_->matched = false;
  AddToBucket(&buckets_[bucket_idx], next_node_);
  DCHECK_GT(node_remaining_current_page_, 0);
  --node_remaining_current_page_;
  ++next_node_;
  ++num_nodes_;
  return true;
}

inline void OldHashTable::AddToBucket(Bucket* bucket, Node* node) {
  num_filled_buckets_ += (bucket->node == NULL);
  node->next = bucket->node;
  bucket->node = node;
}

inline bool OldHashTable::EvalAndHashBuild(TupleRow* row, uint32_t* hash) {
  bool has_null = EvalBuildRow(row);
  if (!stores_nulls_ && has_null) return false;
  *hash = HashCurrentRow();
  return true;
}

inline bool OldHashTable::EvalAndHashProbe(TupleRow* row, uint32_t* hash) {
  bool has_null = EvalProbeRow(row);
  if (has_null && !(stores_nulls_ && finds_some_nulls_)) return false;
  *hash = HashCurrentRow();
  return true;
}

inline void OldHashTable::MoveNode(Bucket* from_bucket, Bucket* to_bucket,
    Node* node, Node* previous_node) {
  if (previous_node != NULL) {
    previous_node->next = node->next;
  } else {
    // Update bucket directly
    from_bucket->node = node->next;
    num_filled_buckets_ -= (node->next == NULL);
  }
  AddToBucket(to_bucket, node);
}

template<bool check_match>
inline void OldHashTable::Iterator::Next() {
  if (bucket_idx_ == -1) return;

  // TODO: this should prefetch the next tuplerow
  // Iterator is not from a full table scan, evaluate equality now.  Only the current
  // bucket needs to be scanned. 'expr_values_buffer_' contains the results
  // for the current probe row.
  if (check_match) {
    // TODO: this should prefetch the next node
    Node* node = node_->next;
    while (node != NULL) {
      if (node->hash == scan_hash_ && table_->Equals(table_->GetRow(node))) {
        node_ = node;
        return;
      }
      node = node->next;
    }
    *this = table_->End();
  } else {
    // Move onto the next chained node
    if (node_->next != NULL) {
      node_ = node_->next;
      return;
    }

    // Move onto the next bucket
    Bucket* bucket = table_->NextBucket(&bucket_idx_);
    if (bucket == NULL) {
      bucket_idx_ = -1;
      node_ = NULL;
    } else {
      node_ = bucket->node;
    }
  }
}

inline bool OldHashTable::Iterator::NextUnmatched() {
  if (bucket_idx_ == -1) return false;
  while (true) {
    while (node_->next != NULL && node_->next->matched) {
      node_ = node_->next;
    }
    if (node_->next == NULL) {
      // Move onto the next bucket.
      Bucket* bucket = table_->NextBucket(&bucket_idx_);
      if (bucket == NULL) {
        bucket_idx_ = -1;
        node_ = NULL;
        return false;
      } else {
        node_ = bucket->node;
        if (node_ != NULL && !node_->matched) return true;
      }
    } else {
      DCHECK(!node_->next->matched);
      node_ = node_->next;
      return true;
    }
  }
}

}

#endif
