#ifndef IMPALA_EXPERIMENTS_HASHING_CACHE_HASH_TABLE_INLINE_H
#define IMPALA_EXPERIMENTS_HASHING_CACHE_HASH_TABLE_INLINE_H

#include "hashing-util.h"

namespace impala {

inline BuildTuple* CacheHashTable::Find(const ProbeTuple* probe) {
  uint32_t bucket_idx = hash_id(probe->id) % BUCKETS;
  Bucket& bucket = buckets_[bucket_idx];
  int length = bucket.count;
  if (length == 0) return NULL;
  idx_t content_idx = bucket.content;
  ContentBlob* content = &content_[content_idx];
  ContentBlobDescriptor* descriptor = &content_descriptors_[content_idx];
  // TODO lots of branching here
  while (length > 0) {
    DCHECK_NE(content_idx, NULL_CONTENT);
    idx_t start = 0;
    if (length == bucket.count) {
      // on the first block, need to skip any empty slots.
      // The number of items in the first blob is bucket.count % CONTENT_SIZE,
      // unless that is 0, in which case the first blob is full, not empty.
      start = (CONTENT_SIZE - (bucket.count % CONTENT_SIZE)) % CONTENT_SIZE;
    }
    for (int i = start; i < CONTENT_SIZE; ++i) {
      BuildTuple* build = &content->build_tuples[i];
      if (build->id == probe->id) {
        // found
        return build;
      }
    }
    length -= CONTENT_SIZE - start;
    content_idx = descriptor->next;
    content = &content_[content_idx];
    descriptor = &content_descriptors_[content_idx];
  }
  return NULL;
}

inline void CacheHashTable::Insert(const BuildTuple* row) {
  // insert new
  uint32_t hash = hash_id(row->id);
  Bucket& bucket = buckets_[hash % BUCKETS];
  AllocateOneMoreSlot(&bucket);
  BuildTuple* tuple = InsertionPoint(&bucket);
  tuple->count = row->count;
  tuple->id = row->id;
  tuple->hash = hash;
  ++bucket.count;
}

inline BuildTuple* CacheHashTable::InsertionPoint(Bucket* bucket) {
  // Empty slots are in the very front of the chunk list, so we need the index of the
  // last one.
  idx_t tuple_idx = CONTENT_SIZE - (bucket->count % CONTENT_SIZE) - 1;
  return &content_[bucket->content].build_tuples[tuple_idx];
}

inline CacheHashTable::idx_t CacheHashTable::AllocateContent() {
  CHECK_LT(num_content_allocated_, CONTENT_BLOBS);
  return num_content_allocated_++;
}

inline void CacheHashTable::AllocateOneMoreSlot(Bucket* bucket) {
  count_t count = bucket->count;
  if (count % CONTENT_SIZE == 0) {
    // currently full, need new ContentBlob
    idx_t idx = AllocateContent();
    // Insert to front of bucket's linked list
    content_descriptors_[idx].next = bucket->content;
    bucket->content = idx;
  }
}

}

#endif
