// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HASH_TABLE_INLINE_H
#define IMPALA_EXEC_HASH_TABLE_INLINE_H

#include "exec/hash-table.h"

namespace impala {

inline HashTable::Iterator HashTable::Find(TupleRow* probe_row) {
  bool has_nulls = EvalProbeRow(probe_row);
  if (!stores_nulls_ && has_nulls) return End();
  uint32_t hash = HashCurrentRow();
  int bucket_idx = hash % buckets_.size();

  Bucket* bucket = &buckets_[bucket_idx];
  int node_idx = bucket->node_idx_;
  while (node_idx != -1) {
    Node* node = GetNode(node_idx);
    if (node->hash_ == hash && Equals(node->data())) {
      return Iterator(this, false, bucket_idx, node_idx, hash);
    }
    node_idx = node->next_idx_;
  }

  return End();
}
  
inline HashTable::Iterator HashTable::Begin() {
  int bucket_idx = -1;
  Bucket* bucket = NextBucket(&bucket_idx);
  if (bucket != NULL) {
    return Iterator(this, true, bucket_idx, bucket->node_idx_, 0);
  }
  return End();
}

inline HashTable::Bucket* HashTable::NextBucket(int* bucket_idx) {
  ++*bucket_idx;
  for (; *bucket_idx < buckets_.size(); ++*bucket_idx) {
    if (buckets_[*bucket_idx].node_idx_ != -1) return &buckets_[*bucket_idx];
  }
  *bucket_idx = -1;
  return NULL;
}

inline void HashTable::InsertImpl(TupleRow* row) {
  bool has_null = EvalBuildRow(row);
  if (!stores_nulls_ && has_null) return;

  uint32_t hash = HashCurrentRow();
  int bucket_idx = hash % buckets_.size();
  if (num_nodes_ == nodes_capacity_) GrowNodeArray();
  Node* node = GetNode(num_nodes_);
  TupleRow* data = node->data();
  node->next_idx_ = -1;
  node->hash_ = hash;
  memcpy(data, row, sizeof(Tuple*) * num_build_tuples_);
  AddToBucket(&buckets_[bucket_idx], num_nodes_);
  ++num_nodes_;
}

inline void HashTable::AddToBucket(Bucket* bucket, int node_idx) {
  if (bucket->node_idx_ == -1) ++num_filled_buckets_;
  Node* node = GetNode(node_idx);
  node->next_idx_ = bucket->node_idx_;
  bucket->node_idx_ = node_idx;
}

inline void HashTable::Iterator::Next() {
  if (bucket_idx_ == -1) return;

  // TODO: this should prefetch the next tuplerow
  Node* node = table_->GetNode(node_idx_);
  // Iterator is not from a full table scan, evaluate equality now.  Only the current
  // bucket needs to be scanned. 'expr_values_buffer_' contains the results
  // for the current probe row.
  if (!is_scan_all_iterator_) {
    // TODO: this should prefetch the next node
    int next_idx = node->next_idx_;
    while (next_idx != -1) {
      Node* node = table_->GetNode(next_idx);
      if (node->hash_ == scan_hash_ && table_->Equals(node->data())) {
        node_idx_ = next_idx;
        return;
      } 
      next_idx = node->next_idx_;
    }
    *this = table_->End();
    return;
  }

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

#endif
