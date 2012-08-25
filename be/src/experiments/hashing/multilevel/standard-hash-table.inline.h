#ifndef IMPALA_EXPERIMENTS_HASHING_STANDARD_HASH_TABLE_INLINE_H
#define IMPALA_EXPERIMENTS_HASHING_STANDARD_HASH_TABLE_INLINE_H

#include "standard-hash-table.h"
#include "hashing-util.h"

namespace impala {

inline BuildTuple* StandardHashTable::Find(const ProbeTuple* probe) {
  uint32_t bucket = probe->hash % BUCKETS;
  for (int i = buckets_[bucket].node_idx_; i != NULL_CONTENT; i = nodes_[i].next_idx_) {
    if (probe->id == nodes_[i].tuple.id) {
      // found
      return &nodes_[i].tuple;
    }
  }
  return NULL;
}

inline void StandardHashTable::Insert(const BuildTuple* row) {
  DCHECK(!Full()); // caller is responsible for ensuring this passes.
  uint32_t hash = hash_id(row->id);
  int bucket_idx = hash % BUCKETS;
  Node* node = &nodes_[num_nodes_];
  node->next_idx_ = buckets_[bucket_idx].node_idx_;
  node->tuple.hash = hash;
  node->tuple.id = row->id;
  node->tuple.count = row->count;
  buckets_[bucket_idx].node_idx_ = num_nodes_;
  ++num_nodes_;
}

inline StandardHashTable::Iterator StandardHashTable::Begin() {
  if (num_nodes_ > 0) {
    return Iterator(this);
  } else {
    // If table is empty, give them an empty iterator.
    return End();
  }
}

inline void StandardHashTable::Iterator::Next() {
  if (++node_idx_ >= table_->num_nodes_) {
    // done
    node_idx_ = -1;
  }
}

}

#endif
