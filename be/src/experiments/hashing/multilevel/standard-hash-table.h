#ifndef IMPALA_EXPERIMENTS_HASHING_STANDARD_HASH_TABLE_H
#define IMPALA_EXPERIMENTS_HASHING_STANDARD_HASH_TABLE_H

#include <glog/logging.h>

#include "util/hash-util.h"
#include "tuple-types.h"


namespace impala {


// Hash table implemented like the one in exec/hash-table.*
// One big array holds the entries.
// Buckets are singly-linked-lists.
// For now, this can't grow since we're comparing to CacheHashTable.
class StandardHashTable {
 public:
  class Iterator;

  StandardHashTable();

  // Lookup probe in the hashtable. If there is an aggregation tuple that matches probe
  // on the aggregation columns (currently just id), then this returns a pointer to that
  // build_tuple in the hash table.
  // Else, returns NULL
  inline BuildTuple* Find(const ProbeTuple* probe);

  // Inserts a new BuildTuple row into the hash table. There must not already be an
  // existing entry that matches row on the aggregation columns (currently just id).
  // The table must have capacity. (Full() returns false.)
  inline void Insert(const BuildTuple* row);

  // Return beginning of hash table.  Advancing this iterator will traverse all
  // elements.
  inline Iterator Begin();

  // Returns end marker
  inline Iterator End() {
    return Iterator();
  }

  // Returns true if capacity for Tuples has been used up (ie. Insert will fail).
  // else false.
  inline bool Full() {
    return num_nodes_ == NODES;
  }




  // EXPERIMENTING: Print the distribution of bucket sizes
  void BucketSizeDistribution();


  // stl-like iterator interface.
  class Iterator {
   public:
    Iterator() : table_(NULL), node_idx_(-1) {
    }

    // Iterates to the next element.  In the case where the iterator was
    // from a Find, this will lazily evaluate that bucket, only returning
    // TupleRows that match the current scan row.
    void Next();

    // Returns the current row or NULL if at end.
    inline BuildTuple* GetRow() {
      if (node_idx_ == -1) return NULL;
      return &table_->nodes_[node_idx_].tuple;
    }

    // Returns if the iterator is at the end
    inline bool HasNext() {
      return node_idx_ != -1;
    }

    inline BuildTuple* operator*() {
      return GetRow();
    }

    inline Iterator& operator++() {
      Next();
      return *this;
    }

    inline bool operator==(const Iterator& rhs) {
      return node_idx_ == rhs.node_idx_;
    }

    inline bool operator!=(const Iterator& rhs) {
      return node_idx_ != rhs.node_idx_;
    }

   private:
    friend class StandardHashTable;

    Iterator(StandardHashTable* table) : table_(table), node_idx_(0) {
    }

    StandardHashTable* table_;
    // Current node idx
    int node_idx_;
  };


 private:
  friend class Iterator;
  friend class GrowingTest;

  static const int BUCKETS = 1700 * 3/4;
  static const int NODES = 1500 * 3/4;

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
