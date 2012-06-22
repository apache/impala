#ifndef IMPALA_EXPERIMENTS_HASHING_CACHE_HASH_TABLE_H
#define IMPALA_EXPERIMENTS_HASHING_CACHE_HASH_TABLE_H


#include "util/hash-util.h"

namespace impala {


// aggregating tuple stored in the hashtable
struct BuildTuple {
  uint32_t hash; // hash of the column we're grouping on (id)
  int32_t id; // id column, what we're aggregating on
  int32_t count; // current value of aggregate function (COUNT(*))
} __attribute__((__packed__));

// tuple from the db
struct ProbeTuple {
  int32_t id;
} __attribute__((__packed__));



// L1-sized hash table that hopes to use cache well
class CacheHashTable {
 public:
  CacheHashTable() {
    num_buckets_ = BUCKETS;
    buckets_ = static_cast<Bucket*>(malloc(BUCKETS * sizeof(Bucket)));
    // TODO memset instead?
    for (int i = 0; i < num_buckets_; ++i) {
      buckets_[i].count = 0;
    }
  }

  // Lookup probe in the hashtable. If there is an aggregation tuple that matches probe
  // on the aggregation columns (currently just id), then this returns a pointer to that
  // build_tuple in the hash table.
  // Else, returns NULL
  BuildTuple* Find(const ProbeTuple* probe);

  // Inserts a new build_tuple row into the hash table. There must not already be an
  // existing entry that matches row on the aggregation columns (currently just id).
  void Insert(const BuildTuple* row);

  // Returns the maximum number of build tuples that can fit in the hash table
  // In practice, we must not attempt to store this much unless the entries hash
  // perfectly.
  static int MaxBuildTuples() { return MAX_BUILD_TUPLES; }

 private:
  static const int L1_SIZE = 32 * 1024; // 32K cache
  static const int TABLE_SIZE = 30 * 1024; // 30K table
  static const int LINE_SIZE = 64; // 64B lines
  static const int LINES_PER_BUCKET = 3;
  static const int BYTES_PER_BUCKET = LINE_SIZE * LINES_PER_BUCKET;


  // build_tuples per bucket
  static const int BUCKET_SIZE = (BYTES_PER_BUCKET - sizeof(int32_t)) / sizeof(BuildTuple);


  struct Bucket {
    int32_t count; // how many build_tuples are currently in the bucket
    BuildTuple build_tuples[BUCKET_SIZE];
    uint8_t padding[8]; // pad to cache line
  } __attribute__((__packed__));

  static const int BUCKETS = TABLE_SIZE / sizeof(Bucket);
  static const int MAX_BUILD_TUPLES = BUCKETS * BUCKET_SIZE;


  // for now, always just hash on id
  uint32_t hash_id(int32_t id) {
    return HashUtil::Hash(&id, sizeof(id), 0);
  }

  Bucket* buckets_;
  int32_t num_buckets_;
};

}

#endif
