// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HASH_TABLE_H
#define IMPALA_EXEC_HASH_TABLE_H

#include <vector>
#include <boost/cstdint.hpp>
#include "common/logging.h"
#include "util/hash-util.h"

namespace impala {

class Expr;
class RowDescriptor;
class Tuple;
class TupleRow;

// Hash table implementation designed for hash aggregation and hash joins.  This is not
// templatized and is tailored to the usage pattern for aggregation and joins.  The
// hash table store TupleRows and allows for different exprs for insertions and finds.
// This is the pattern we use for joins and aggregation where the input/build tuple
// row descriptor is different from the find/probe descriptor.
// The table is optimized for the query engine's use case as much as possible and is not
// intended to be a generic hash table implementation.  The API loosely mimics the 
// std::hashset API.
// 
// The hash table stores evaluated expr results for the current row being processed
// when possible into a contiguous memory buffer. This allows for very efficient 
// computation for hashing.  The implementation is also designed to allow codegen 
// for some paths.
//
// The hash table does not support removes. The hash table is not thread safe.
//
// The implementation is based on the boost multiset.  The hashtable is implemented by
// two data structures: a vector of buckets and a vector of nodes.  Inserted values
// are stored as nodes (in the order they are inserted).  The buckets (indexed by the
// mod of the hash) contain pointers to the node vector.  Nodes that fall in the same
// bucket are linked together (the bucket pointer gets you the head of that linked list).
// For growing the hash table, new buckets are allocated but the node vector is modified
// in place.
//
// TODO: this is not a fancy hash table in terms of memory access patterns (cuckoo-hashing
// or something that spills to disk). We will likely want to invest more time into this.
// TODO: hash-join and aggregation have very different access patterns.  Joins insert
// all the rows and then calls scan to find them.  Aggregation interleaves Find() and 
// Inserts().  We can want to optimize joins more heavily for Inserts() (in particular
// growing).
class HashTable {
 public:
  class Iterator;

  // Create a hash table.  
  //  - build_exprs are the exprs that should be used to evaluate rows during Insert().  
  //  - probe_exprs are used during Find()
  //  - num_build_tuples: number of Tuples in the build tuple row
  //  - stores_nulls: if false, TupleRows with nulls are ignored during Insert
  //  - num_buckets: number of buckets that the hash table should be initialized to
  HashTable(const std::vector<Expr*>& build_exprs, const std::vector<Expr*>& probe_exprs,
      int num_build_tuples, bool stores_nulls, int64_t num_buckets = 1024);

  ~HashTable() {
    // TODO: use tr1::array?
    delete[] expr_values_buffer_;
    delete[] expr_value_null_bits_;
    free(nodes_);
  }

  // Insert row into the hash table.  Row will be evaluated over build_exprs_
  // This will grow the hash table if necessary
  void Insert(TupleRow* row) {
    if (num_filled_buckets_ > buckets_.size() * MAX_BUCKET_OCCUPANCY_FRACTION) {
      // TODO: next prime instead of double?
      ResizeBuckets(num_buckets() * 2);
    }
    InsertImpl(row);
  }
  
  // Returns the start iterator for all rows that match 'probe_row'.  'probe_row' is
  // evaluated with probe_exprs_.  The iterator can be iterated until HashTable::End() 
  // to find all the matching rows.
  // Only one scan be in progress at any time (i.e. it is not legal to call
  // Find(), begin iterating through all the matches, call another Find(),
  // and continuing iterator from the first scan iterator).  
  // Advancing the returned iterator will go to the next matching row.  The matching 
  // rows are evaluated lazily (i.e. computed as the Iterator is moved).   
  // Returns HashTable::End() if there is no match.
  Iterator Find(TupleRow* probe_row);
  
  // Returns number of elements in the hash table
  int64_t size() { return num_nodes_; }

  // Returns the number of buckets
  int64_t num_buckets() { return buckets_.size(); }

  // Returns the load factor (the number of non-empty buckets)
  float load_factor() { 
    return num_filled_buckets_ / static_cast<float>(buckets_.size()); 
  }
  
  // Returns the number of bytes allocated to the hash table
  int64_t byte_size() const { 
    return node_byte_size() * nodes_capacity_ + sizeof(Bucket) * buckets_.size();
  }

  // Returns the results of the exprs at 'expr_idx' evaluated over the last row
  // processed by the HashTable.
  // This value is invalid if the expr evaluated to NULL.
  // TODO: this is an awkward abstraction but aggregation node can take advantage of
  // it and save some expr evaluation calls.
  void* last_expr_value(int expr_idx) const { 
    return expr_values_buffer_ + expr_values_buffer_offsets_[expr_idx]; 
  }
  
  // Returns if the expr at 'expr_idx' evaluated to NULL for the last row.
  bool last_expr_value_null(int expr_idx) const { 
    return expr_value_null_bits_[expr_idx]; 
  }

  // Return beginning of hash table.  Advancing this iterator will traverse all
  // elements.
  Iterator Begin();

  // Returns end marker
  Iterator End() {
    return Iterator();
  }

  // Dump out the entire hash table to string.  If skip_empty, empty buckets are
  // skipped.  If build_desc is non-null, the build rows will be output.  Otherwise
  // just the build row addresses.
  std::string DebugString(bool skip_empty, const RowDescriptor* build_desc);

  // stl-like iterator interface.
  class Iterator {
   public:
    Iterator() : table_(NULL), bucket_idx_(-1), node_idx_(-1), 
        is_scan_all_iterator_(false) {
    }

    // Iterates to the next element.  In the case where the iterator was
    // from a Find, this will lazily evaluate that bucket, only returning
    // TupleRows that match the current scan row.
    void Next();

    // Returns the current row or NULL if at end.
    TupleRow* GetRow() {
      if (node_idx_ == -1) return NULL;
      return table_->GetNode(node_idx_)->data();
    }

    // Returns if the iterator is at the end
    bool HasNext() {
      return node_idx_ != -1;
    }

    TupleRow* operator*() {
      return GetRow();
    }

    Iterator& operator++() {
      Next();
      return *this;
    }

    bool operator==(const Iterator& rhs) {
      return bucket_idx_ == rhs.bucket_idx_ && node_idx_ == rhs.node_idx_;
    }

    bool operator!=(const Iterator& rhs) {
      return bucket_idx_ != rhs.bucket_idx_ || node_idx_ != rhs.node_idx_;
    }

   private:
    friend class HashTable;

    Iterator(HashTable* table, bool is_scan_all, int bucket_idx, 
        int node, uint32_t hash) :
      table_(table),
      bucket_idx_(bucket_idx),
      node_idx_(node),
      is_scan_all_iterator_(is_scan_all),
      scan_hash_(hash) {
    }

    HashTable* table_;
    // Current bucket idx
    int bucket_idx_;        
    // Current node idx (within current bucket)
    int node_idx_;          
    // whether or not this iterator is from a Begin() call
    bool is_scan_all_iterator_; 
    // cached hash value for the row passed to Find()()
    uint32_t scan_hash_;    
  };

 private:
  friend class Iterator;
  friend class HashTableTest;

  // Header portion of a Node.  The node data (TupleRow) is right after the 
  // node memory to maximize cache hits.
  struct Node {
    int next_idx_;    // chain to next node for collisions
    uint32_t hash_;   // Cache of the hash for data_

    TupleRow* data() {
      uint8_t* mem = reinterpret_cast<uint8_t*>(this);
      DCHECK_EQ(reinterpret_cast<uint64_t>(mem) % 8, 0);
      return reinterpret_cast<TupleRow*>(mem + sizeof(Node));
    }
  };

  struct Bucket {
    int node_idx_;

    Bucket() {
      node_idx_ = -1;
    }
  };

  // Returns the next non-empty bucket and updates idx to be the index of that bucket.
  // If there are no more buckets, returns NULL and sets idx to -1
  Bucket* NextBucket(int* bucket_idx);

  // Returns node at idx.  Tracking structures do not use pointers since they will
  // change as the HashTable grows.
  Node* GetNode(int idx) {
    DCHECK_NE(idx, -1);
    return reinterpret_cast<Node*>(nodes_ + node_byte_size() * idx);
  }
  
  // Resize the hash table to 'num_buckets'
  void ResizeBuckets(int64_t num_buckets);

  // Insert row into the hash table
  void InsertImpl(TupleRow* row);

  // Chains the node at 'node_idx' to 'bucket'.  Nodes in a bucket are chained
  // as a linked list; this places the new node at the beginning of the list.
  void AddToBucket(Bucket* bucket, int node_idx);

  // Evaluate the exprs over row and cache the results in 'expr_values_buffer_'.
  // Returns whether any expr evaluated to NULL
  // This will be replaced by codegen
  bool EvalRow(TupleRow* row, const std::vector<Expr*>& exprs);

  // Evaluate 'row' over build_exprs_ caching the results in 'expr_values_buffer_'
  // This will be replaced by codegen.
  bool EvalBuildRow(TupleRow* row) {
    return EvalRow(row, build_exprs_);
  }

  // Evaluate 'row' over probe_exprs_ caching the results in 'expr_values_buffer_'
  // This will be replaced by codegen.
  bool EvalProbeRow(TupleRow* row) {
    return EvalRow(row, probe_exprs_);
  }

  // Compute the hash of the values in expr_values_buffer_.
  // This will be replaced by codegen.
  uint32_t HashCurrentRow() {
    if (var_result_begin_ == -1) {
      // This handles NULLs implicitly since a constant seed value was put
      // into results buffer for nulls.
      return HashUtil::Hash(expr_values_buffer_, results_buffer_size_, 0);
    } else {
      return HashVariableLenRow();
    }
  }

  // Compute the hash of the values in expr_values_buffer_ for rows with variable length
  // fields (e.g. strings)
  uint32_t HashVariableLenRow();

  // Returns true if the values of build_exprs evaluated over 'build_row' equal 
  // the values cached in expr_values_buffer_
  // This will be replaced by codegen.
  bool Equals(TupleRow* build_row);

  // The byte size of a Node.  This includes the fixed sized header (sizeof(Node))
  // as well as the Tuple*'s that immediately follow
  uint32_t node_byte_size() const {
    return sizeof(Node) + sizeof(Tuple*) * num_build_tuples_;
  }

  // Grow the node array.
  void GrowNodeArray();

  // Load factor that will trigger growing the hash table on insert.  This is 
  // defined as the number of non-empty buckets / total_buckets
  static const float MAX_BUCKET_OCCUPANCY_FRACTION;

  const std::vector<Expr*>& build_exprs_;
  const std::vector<Expr*>& probe_exprs_;

  // Number of Tuple* in the build tuple row
  int num_build_tuples_;
  bool stores_nulls_;

  // Number of non-empty buckets.  Used to determine when to grow and rehash
  int num_filled_buckets_;
  // Memory to store node data.  This is not allocated from a pool to take advantage
  // of realloc.
  // TODO: integrate with mem pools
  uint8_t* nodes_;
  // number of nodes stored (i.e. size of hash table)
  int64_t num_nodes_;
  // max number of nodes that can be stored in 'nodes_' before realloc
  int64_t nodes_capacity_;
  std::vector<Bucket> buckets_;

  // Cache of exprs values for the current row being evaluated.  This can either
  // be a build row (during Insert()) or probe row (during Find()).
  std::vector<int> expr_values_buffer_offsets_;
  int results_buffer_size_;
  int var_result_begin_;
  uint8_t* expr_values_buffer_;
  bool* expr_value_null_bits_;
};

}

#endif
