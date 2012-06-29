#ifndef IMPALA_EXPERIMENTS_HASHING_STANDARD_HASH_TABLE_H
#define IMPALA_EXPERIMENTS_HASHING_STANDARD_HASH_TABLE_H

#include <glog/logging.h>

#include "util/hash-util.h"
#include "cache-hash-test.h"


namespace impala {


// Hash table implemented like the one in exec/hash-table.*
// One big array holds the entries.
// Buckets are singly-linked-lists.
// For now, this can't grow since we're comparing to CacheHashTable.
class StandardHashTable {
 public:

  // Create a hash table with room for storage_size BuildTuples
  StandardHashTable();

  // implement HashTableInterface
  inline BuildTuple* Find(const ProbeTuple* probe);
  inline void Insert(const BuildTuple* row);

  // EXPERIMENTING: Print the distribution of bucket sizes
  void BucketSizeDistribution();

 private:
  static const int BUCKETS = 1700;
  static const int NODES = 1500;

  typedef uint16_t idx_t;
  static const idx_t NULL_CONTENT = UINT16_MAX;

  // Bucket in the hashtable
  struct Bucket {
    // Index into the node vector of the first node in the bucket.
    idx_t node_idx_;

    Bucket() {
      node_idx_ = NULL_CONTENT;
    }
  };

  // Node in a bucket.
  struct Node {
    idx_t next_idx_; // Index of the next node in this bucket.
    BuildTuple tuple; // The content

    Node() {
      next_idx_ = NULL_CONTENT;
    }
  };


  Node nodes_[NODES];
  Bucket buckets_[BUCKETS];
  int num_nodes_;
};

}

#endif
