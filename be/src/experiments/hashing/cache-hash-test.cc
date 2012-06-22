#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include <glog/logging.h>

#include "cache-hash-table.h"
#include "runtime/mem-pool.h"
#include "util/hash-util.h"
#include "util/runtime-profile.h"

using namespace impala;

// Very basic hash aggregation prototype and test
// TODO: Implement simple version of the currently-used hash table for comparison
// TODO: Generalize beyond hash aggregation, beyond hashing one the one column, etc. 


BuildTuple* CacheHashTable::Find(const ProbeTuple* probe) {
  uint32_t bucket = hash_id(probe->id) % num_buckets_;
  for (int i = 0; i < buckets_[bucket].count; ++i) {
    if (probe->id == buckets_[bucket].build_tuples[i].id) {
      // found
      return &buckets_[bucket].build_tuples[i];
    }
  }
  return NULL;
}

inline void CacheHashTable::Insert(const BuildTuple* row) {
  // insert new
  uint32_t hash = hash_id(row->id);
  Bucket& bucket = buckets_[hash % num_buckets_];
  DCHECK_LT(bucket.count, BUCKET_SIZE);
  BuildTuple* tuple = &bucket.build_tuples[bucket.count];
  tuple->count = row->count;
  tuple->id = row->id;
  tuple->hash = hash;
  ++bucket.count;
}


// Update ht, which is doing a COUNT(*) GROUP BY id,
// by having it process the new tuple probe.
inline void Process(CacheHashTable* ht, const ProbeTuple* probe) {
  BuildTuple *existing = ht->Find(probe);
  if (existing != NULL) {
    ++existing->count;
  } else {
    BuildTuple build;
    build.id = probe->id;
    build.count = 1;
    ht->Insert(&build);
  }
}

// Generate n random tuples, with ids in the range [0, max_id).
ProbeTuple* GenTuples(int n, int max_id) {
  ProbeTuple* tuples
    = static_cast<ProbeTuple*>(malloc(n * sizeof(ProbeTuple)));

  for (int i = 0; i < n; ++i) {
    tuples[i].id = rand() % max_id;
  }
  return tuples;
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  CacheHashTable ht;

  const int NUM_TUPLES = 100000000; //10^8
  ProbeTuple* input = GenTuples(NUM_TUPLES, CacheHashTable::MaxBuildTuples() / 2);
  for (int i = 0; i < NUM_TUPLES; ++i) {
    Process(&ht, &input[i]);
  }

  BuildTuple* first = ht.Find(&input[0]);
  return 0;
}
