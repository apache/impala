#ifndef IMPALA_EXPERIMENTS_HASHING_NAIVE_HASHMAP_H
#define IMPALA_EXPERIMENTS_HASHING_NAIVE_HASHMAP_H

#include <glog/logging.h>

#include "tuple-types.h"

namespace impala {

class NaiveHashmap {

 public:
  NaiveHashmap() {
    num_buckets_ = 1500 * 3/4;
    allocated_nodes_ = 1700 * 3/4;
    num_nodes_ = 0;
    nodes_ = new Node[allocated_nodes_];
    buckets_ = new Bucket[num_buckets_];
  }

  ~NaiveHashmap() {
    delete[] nodes_;
    delete[] buckets_;
  }

  inline BuildTuple* Find(const ProbeTuple* probe) {
    uint32_t bucket = probe->hash % num_buckets_;
    for (int i = buckets_[bucket].node_idx_; i != NULL_CONTENT; i = nodes_[i].next_idx_) {
      if (probe->id == nodes_[i].tuple.id) {
        // found
        return &nodes_[i].tuple;
      }
    }
    return NULL;
  }

  inline void Insert(const BuildTuple* row) {
    if (allocated_nodes_ == num_nodes_) {
      Node* old_nodes = nodes_;
      allocated_nodes_ *= 2;
      num_buckets_ *= 2;
      nodes_ = new Node[allocated_nodes_];
      buckets_ = new Bucket[num_buckets_];
      for (int i = 0; i < num_nodes_; ++i) {
        int bucket_idx = old_nodes[i].tuple.hash % num_buckets_;
        nodes_[i] = old_nodes[i];
        nodes_[i].next_idx_ = buckets_[bucket_idx].node_idx_;
        buckets_[bucket_idx].node_idx_ = i;
      }
    }
    int bucket_idx = row->hash % num_buckets_;
    Node* node = &nodes_[num_nodes_];
    node->next_idx_ = buckets_[bucket_idx].node_idx_;
    node->tuple = *row;
    buckets_[bucket_idx].node_idx_ = num_nodes_;
    ++num_nodes_;
  }

  void Aggregate(ProbeTuple* tuples, int n) {
    for (int i = 0; i < n; ++i) {
      ProbeTuple* probe = &tuples[i];
      probe->hash = hash_id(probe->id);
      BuildTuple* existing = Find(probe);
      if (existing != NULL) {
        ++existing->count;
      } else {
        BuildTuple build;
        build.id = probe->id;
        build.count = 1;
        build.hash = probe->hash;
        Insert(&build);
      }

    }
  }



 private:
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

  int num_buckets_;
  int num_nodes_;
  int allocated_nodes_;
  Node* nodes_;
  Bucket* buckets_;
};

}

#endif
