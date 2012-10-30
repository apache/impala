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
  bool has_nulls = EvalProbeRow(probe_row);
  if (!stores_nulls_ && has_nulls) return End();
  uint32_t hash = HashCurrentRow();
  int64_t bucket_idx = hash % num_buckets_;

  Bucket* bucket = &buckets_[bucket_idx];
  int64_t node_idx = bucket->node_idx_;
  while (node_idx != -1) {
    Node* node = GetNode(node_idx);
    if (node->hash_ == hash && Equals(node->data())) {
      return Iterator(this, bucket_idx, node_idx, hash);
    }
    node_idx = node->next_idx_;
  }

  return End();
}
  
inline HashTable::Iterator HashTable::Begin() {
  int64_t bucket_idx = -1;
  Bucket* bucket = NextBucket(&bucket_idx);
  if (bucket != NULL) {
    return Iterator(this, bucket_idx, bucket->node_idx_, 0);
  }
  return End();
}

inline HashTable::Bucket* HashTable::NextBucket(int64_t* bucket_idx) {
  ++*bucket_idx;
  for (; *bucket_idx < num_buckets_; ++*bucket_idx) {
    if (buckets_[*bucket_idx].node_idx_ != -1) return &buckets_[*bucket_idx];
  }
  *bucket_idx = -1;
  return NULL;
}

inline void HashTable::InsertImpl(TupleRow* row) {
  bool has_null = EvalBuildRow(row);
  if (!stores_nulls_ && has_null) return;

  uint32_t hash = HashCurrentRow();
  int64_t bucket_idx = hash % num_buckets_;
  if (num_nodes_ == nodes_capacity_) GrowNodeArray();
  Node* node = GetNode(num_nodes_);
  TupleRow* data = node->data();
  node->hash_ = hash;
  memcpy(data, row, sizeof(Tuple*) * num_build_tuples_);
  AddToBucket(&buckets_[bucket_idx], num_nodes_, node);
  ++num_nodes_;
}

inline void HashTable::AddToBucket(Bucket* bucket, int64_t node_idx, Node* node) {
  if (bucket->node_idx_ == -1) ++num_filled_buckets_;
  node->next_idx_ = bucket->node_idx_;
  bucket->node_idx_ = node_idx;
}

template<bool check_match>
inline void HashTable::Iterator::Next() {
  if (bucket_idx_ == -1) return;

  // TODO: this should prefetch the next tuplerow
  Node* node = table_->GetNode(node_idx_);
  // Iterator is not from a full table scan, evaluate equality now.  Only the current
  // bucket needs to be scanned. 'expr_values_buffer_' contains the results
  // for the current probe row.
  if (check_match) {
    // TODO: this should prefetch the next node
    int64_t next_idx = node->next_idx_;
    while (next_idx != -1) {
      node = table_->GetNode(next_idx);
      if (node->hash_ == scan_hash_ && table_->Equals(node->data())) {
        node_idx_ = next_idx;
        return;
      } 
      next_idx = node->next_idx_;
    }
    *this = table_->End();
  } else {
    // Move onto the next chained node
    if (node->next_idx_ != -1) {
      node_idx_ = node->next_idx_;
      return;
    }

    // Move onto the next bucket
    Bucket* bucket = table_->NextBucket(&bucket_idx_);
    if (bucket == NULL) {
      bucket_idx_ = -1;
      node_idx_ = -1;
    } else {
      node_idx_ = bucket->node_idx_;
    }
  }
}

}

#endif
