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


#ifndef IMPALA_EXEC_OLD_HASH_TABLE_H
#define IMPALA_EXEC_OLD_HASH_TABLE_H

#include <vector>
#include <boost/cstdint.hpp>
#include "codegen/impala-ir.h"
#include "common/logging.h"
#include "runtime/mem-pool.h"
#include "util/bitmap.h"
#include "util/hash-util.h"
#include "util/runtime-profile.h"

namespace llvm {
  class Function;
}

namespace impala {

class Expr;
class ExprContext;
class LlvmCodeGen;
class MemTracker;
class RowDescriptor;
class RuntimeState;
class Tuple;
class TupleRow;

/// TODO: Temporarily moving the old HashTable implementation to make it work with the
/// non-partitioned versions of HJ and AGG. It should be removed once we remove those
/// non-partitioned versions.

/// Hash table implementation designed for hash aggregation and hash joins.  This is not
/// templatized and is tailored to the usage pattern for aggregation and joins.  The
/// hash table store TupleRows and allows for different exprs for insertions and finds.
/// This is the pattern we use for joins and aggregation where the input/build tuple
/// row descriptor is different from the find/probe descriptor.
/// The table is optimized for the query engine's use case as much as possible and is not
/// intended to be a generic hash table implementation.  The API loosely mimics the
/// std::hashset API.
//
/// The hash table stores evaluated expr results for the current row being processed
/// when possible into a contiguous memory buffer. This allows for very efficient
/// computation for hashing.  The implementation is also designed to allow codegen
/// for some paths.
//
/// The hash table does not support removes. The hash table is not thread safe.
//
/// The implementation is based on the boost multiset.  The hashtable is implemented by
/// two data structures: a vector of buckets and a vector of nodes.  Inserted values
/// are stored as nodes (in the order they are inserted).  The buckets (indexed by the
/// mod of the hash) contain pointers to the node vector.  Nodes that fall in the same
/// bucket are linked together (the bucket pointer gets you the head of that linked list).
/// When growing the hash table, the number of buckets is doubled, and nodes from a
/// particular bucket either stay in place or move to an analogous bucket in the second
/// half of buckets. This behavior allows us to avoid moving about half the nodes each
/// time, and maintains good cache properties by only accessing 2 buckets at a time.
/// The node vector is modified in place.
/// Due to the doubling nature of the buckets, we require that the number of buckets is a
/// power of 2. This allows us to determine if a node needs to move by simply checking a
/// single bit, and further allows us to initially hash nodes using a bitmask.
//
/// TODO: this is not a fancy hash table in terms of memory access patterns (cuckoo-hashing
/// or something that spills to disk). We will likely want to invest more time into this.
/// TODO: hash-join and aggregation have very different access patterns.  Joins insert
/// all the rows and then calls scan to find them.  Aggregation interleaves Find() and
/// Inserts().  We can want to optimize joins more heavily for Inserts() (in particular
/// growing).
/// TODO: batched interface for inserts and finds.
class OldHashTable {
 private:
  struct Node;

 public:
  class Iterator;

  /// Create a hash table.
  ///  - build_exprs are the exprs that should be used to evaluate rows during Insert().
  ///  - probe_exprs are used during Find()
  ///  - num_build_tuples: number of Tuples in the build tuple row
  ///  - stores_nulls: if false, TupleRows with nulls are ignored during Insert
  ///  - finds_nulls: if finds_nulls[i] is false, Find() returns End() for TupleRows with
  ///      nulls in position i even if stores_nulls is true.
  ///  - num_buckets: number of buckets that the hash table should be initialized to
  ///  - mem_tracker: if non-empty, all memory allocations for nodes and for buckets are
  ///    tracked; the tracker must be valid until the d'tor is called
  ///  - initial_seed: Initial seed value to use when computing hashes for rows
  ///  - stores_tuples: If true, the hash table stores tuples, otherwise it stores tuple
  ///    rows.
  /// TODO: stores_nulls is too coarse: for a hash table in which some columns are joined
  ///       with '<=>' and others with '=', stores_nulls could distinguish between columns
  ///       in which nulls are stored and columns in which they are not, which could save
  ///       space by not storing some rows we know will never match.
  OldHashTable(RuntimeState* state, const std::vector<ExprContext*>& build_expr_ctxs,
      const std::vector<ExprContext*>& probe_expr_ctxs, int num_build_tuples,
      bool stores_nulls, const std::vector<bool>& finds_nulls, int32_t initial_seed,
      MemTracker* mem_tracker, bool stores_tuples = false, int64_t num_buckets = 1024);

  /// Call to cleanup any resources. Must be called once.
  void Close();

  /// Insert row into the hash table.  Row will be evaluated over build exprs.
  /// This will grow the hash table if necessary.
  /// If the hash table has or needs to go over the mem limit, the Insert will
  /// be ignored. The caller is assumed to periodically (e.g. per row batch) check
  /// the limits to identify this case.
  /// The 'row' is not copied by the hash table and the caller must guarantee it
  /// stays in memory.
  /// Returns false if there was not enough memory to insert the row.
  bool IR_ALWAYS_INLINE Insert(TupleRow* row) {
    if (UNLIKELY(mem_limit_exceeded_)) return false;
    bool has_null = EvalBuildRow(row);
    if (!stores_nulls_ && has_null) return true;

    if (UNLIKELY(num_filled_buckets_ > num_buckets_till_resize_)) {
      /// TODO: next prime instead of double?
      ResizeBuckets(num_buckets_ * 2);
      if (UNLIKELY(mem_limit_exceeded_)) return false;
    }
    return InsertImpl(row);
  }

  bool IR_ALWAYS_INLINE Insert(Tuple* tuple) {
    if (UNLIKELY(mem_limit_exceeded_)) return false;
    bool has_null = EvalBuildRow(reinterpret_cast<TupleRow*>(&tuple));
    if (!stores_nulls_ && has_null) return true;

    if (UNLIKELY(num_filled_buckets_ > num_buckets_till_resize_)) {
      /// TODO: next prime instead of double?
      ResizeBuckets(num_buckets_ * 2);
      if (UNLIKELY(mem_limit_exceeded_)) return false;
    }
    return InsertImpl(tuple);
  }

  /// Evaluate and hash the build/probe row, returning in *hash. Returns false if this
  /// row should be rejected (doesn't need to be processed further) because it
  /// contains NULL.
  /// These need to be inlined in the IR module so we can find and replace the calls to
  /// EvalBuildRow()/EvalProbeRow().
  bool IR_ALWAYS_INLINE EvalAndHashBuild(TupleRow* row, uint32_t* hash);
  bool IR_ALWAYS_INLINE EvalAndHashProbe(TupleRow* row, uint32_t* hash);

  /// Returns the start iterator for all rows that match 'probe_row'.  'probe_row' is
  /// evaluated with probe exprs.  The iterator can be iterated until OldHashTable::End()
  /// to find all the matching rows.
  /// Only one scan can be in progress at any time (i.e. it is not legal to call
  /// Find(), begin iterating through all the matches, call another Find(),
  /// and continuing iterator from the first scan iterator).
  /// Advancing the returned iterator will go to the next matching row.  The matching
  /// rows are evaluated lazily (i.e. computed as the Iterator is moved).
  /// Returns OldHashTable::End() if there is no match.
  Iterator IR_ALWAYS_INLINE Find(TupleRow* probe_row);

  /// Returns number of elements in the hash table
  int64_t size() const { return num_nodes_; }

  /// Returns the number of buckets
  int64_t num_buckets() const { return buckets_.size(); }

  /// Returns the load factor (the number of non-empty buckets)
  float load_factor() const {
    return num_filled_buckets_ / static_cast<float>(buckets_.size());
  }

  /// Returns an estimate of the number of bytes needed to build the hash table
  /// structure for 'num_rows'.
  static int64_t EstimateSize(int64_t num_rows) {
    // Assume 50% fill factor.
    int64_t num_buckets = num_rows * 2;
    return num_buckets * sizeof(Bucket) + num_rows * sizeof(Node);
  }

  /// Returns the number of bytes allocated to the hash table
  int64_t byte_size() const {
    int64_t nodes_mem = (num_nodes_ + node_remaining_current_page_) * sizeof(Node);
    return nodes_mem + sizeof(Bucket) * buckets_.capacity();
  }

  bool mem_limit_exceeded() const { return mem_limit_exceeded_; }

  /// Returns the results of the exprs at 'expr_idx' evaluated over the last row
  /// processed by the OldHashTable.
  /// This value is invalid if the expr evaluated to NULL.
  /// TODO: this is an awkward abstraction but aggregation node can take advantage of
  /// it and save some expr evaluation calls.
  void* last_expr_value(int expr_idx) const {
    return expr_values_buffer_ + expr_values_buffer_offsets_[expr_idx];
  }

  /// Returns if the expr at 'expr_idx' evaluated to NULL for the last row.
  bool last_expr_value_null(int expr_idx) const {
    return expr_value_null_bits_[expr_idx];
  }

  /// Can be called after all insert calls to add bitmap filters for the probe
  /// side values.
  /// For each probe_expr_ that is a slot ref, generate a bitmap filter on that slot.
  /// These filters are added to the runtime state.
  /// The bitmap filter is similar to a Bloom filter in that has no false negatives
  /// but will have false positives.
  void AddBitmapFilters();

  /// Returns an iterator at the beginning of the hash table.  Advancing this iterator
  /// will traverse all elements.
  Iterator Begin();

  /// Return an iterator pointing to the first element in the hash table that does not
  /// have its matched flag set. Used in right-outer and full-outer joins.
  Iterator FirstUnmatched();

  /// Returns end marker
  Iterator End() { return Iterator(); }

  /// Codegen for evaluating a tuple row.  Codegen'd function matches the signature
  /// for EvalBuildRow and EvalTupleRow.
  /// if build_row is true, the codegen uses the build_exprs, otherwise the probe_exprs
  llvm::Function* CodegenEvalTupleRow(RuntimeState* state, bool build_row);

  /// Codegen for hashing the expr values in 'expr_values_buffer_'.  Function
  /// prototype matches HashCurrentRow identically.
  llvm::Function* CodegenHashCurrentRow(RuntimeState* state);

  /// Codegen for evaluating a TupleRow and comparing equality against
  /// 'expr_values_buffer_'.  Function signature matches OldHashTable::Equals()
  llvm::Function* CodegenEquals(RuntimeState* state);

  static const char* LLVM_CLASS_NAME;

  /// Dump out the entire hash table to string.  If skip_empty, empty buckets are
  /// skipped.  If show_match, it also prints the matched flag of each node. If build_desc
  /// is non-null, the build rows will be output.  Otherwise just the build row addresses.
  std::string DebugString(bool skip_empty, bool show_match,
      const RowDescriptor* build_desc);

  /// stl-like iterator interface.
  class Iterator {
   public:
    Iterator() : table_(NULL), bucket_idx_(-1), node_(NULL) {
    }

    /// Iterates to the next element.  In the case where the iterator was
    /// from a Find, this will lazily evaluate that bucket, only returning
    /// TupleRows that match the current scan row. No-op if the iterator is at the end.
    template<bool check_match>
    void IR_ALWAYS_INLINE Next();

    /// Iterates to the next element that does not have its matched flag set. Returns false
    /// if it reaches the end of the table without finding an unmatched element. Used in
    /// right-outer and full-outer joins.
    bool NextUnmatched();

    /// Returns the current row. Callers must check the iterator is not AtEnd() before
    /// calling GetRow().
    TupleRow* GetRow() {
      DCHECK(!AtEnd());
      DCHECK(!table_->stores_tuples_);
      return reinterpret_cast<TupleRow*>(node_->data);
    }

    Tuple* GetTuple() {
      DCHECK(!AtEnd());
      DCHECK(table_->stores_tuples_);
      return reinterpret_cast<Tuple*>(node_->data);
    }

    void set_matched(bool v) {
      DCHECK(!AtEnd());
      node_->matched = v;
    }

    bool matched() const {
      DCHECK(!AtEnd());
      return node_->matched;
    }

    void reset() {
      bucket_idx_ = -1;
      node_ = NULL;
    }

    /// Returns true if this iterator is at the end, i.e. GetRow() cannot be called.
    bool AtEnd() const { return node_ == NULL; }
    bool operator!=(const Iterator& rhs) { return !(*this == rhs); }

    bool operator==(const Iterator& rhs) {
      return bucket_idx_ == rhs.bucket_idx_ && node_ == rhs.node_;
    }

   private:
    friend class OldHashTable;

    Iterator(OldHashTable* table, int bucket_idx, Node* node, uint32_t hash) :
      table_(table),
      bucket_idx_(bucket_idx),
      node_(node),
      scan_hash_(hash) {
    }

    OldHashTable* table_;

    /// Current bucket idx
    int64_t bucket_idx_;

    /// Current node idx (within current bucket)
    Node* node_;

    /// Cached hash value for the row passed to Find()
    uint32_t scan_hash_;
  };

 private:
  friend class Iterator;
  friend class OldHashTableTest;

  /// TODO: bit pack this struct. The default alignment makes this struct have a lot
  /// of wasted bytes.
  struct Node {
    /// Only used for full/right outer hash join to indicate if this row has been
    /// matched.
    /// From an abstraction point of view, this is an awkward place to store this
    /// information but it is very efficient here. This space is otherwise unused
    /// (and we can bitpack this more in the future).
    bool matched;

    uint32_t hash;   // Cache of the hash for data_
    Node* next;      // Chain to next node for collisions
    void* data;      // Either the Tuple* or TupleRow*
  };

  struct Bucket {
    Node* node;
    Bucket() : node(NULL) { }
  };

  /// Returns the next non-empty bucket and updates idx to be the index of that bucket.
  /// If there are no more buckets, returns NULL and sets idx to -1
  Bucket* NextBucket(int64_t* bucket_idx);

  /// Resize the hash table to 'num_buckets'
  void ResizeBuckets(int64_t num_buckets);

  /// Insert row into the hash table
  bool IR_ALWAYS_INLINE InsertImpl(void* data);

  /// Chains the node at 'node_idx' to 'bucket'.  Nodes in a bucket are chained
  /// as a linked list; this places the new node at the beginning of the list.
  void AddToBucket(Bucket* bucket, Node* node);

  /// Moves a node from one bucket to another. 'previous_node' refers to the
  /// node (if any) that's chained before this node in from_bucket's linked list.
  void MoveNode(Bucket* from_bucket, Bucket* to_bucket, Node* node,
                Node* previous_node);

  /// Evaluate the exprs over row and cache the results in 'expr_values_buffer_'.
  /// Returns whether any expr evaluated to NULL
  /// This will be replaced by codegen
  bool EvalRow(TupleRow* row, const std::vector<ExprContext*>& ctxs);

  /// Evaluate 'row' over build exprs caching the results in 'expr_values_buffer_' This
  /// will be replaced by codegen.  We do not want this function inlined when cross
  /// compiled because we need to be able to differentiate between EvalBuildRow and
  /// EvalProbeRow by name and the build/probe exprs are baked into the codegen'd function.
  bool IR_NO_INLINE EvalBuildRow(TupleRow* row) {
    return EvalRow(row, build_expr_ctxs_);
  }

  /// Evaluate 'row' over probe exprs caching the results in 'expr_values_buffer_'
  /// This will be replaced by codegen.
  bool IR_NO_INLINE EvalProbeRow(TupleRow* row) {
    return EvalRow(row, probe_expr_ctxs_);
  }

  /// Compute the hash of the values in expr_values_buffer_.
  /// This will be replaced by codegen.  We don't want this inlined for replacing
  /// with codegen'd functions so the function name does not change.
  uint32_t IR_NO_INLINE HashCurrentRow() {
    if (var_result_begin_ == -1) {
      /// This handles NULLs implicitly since a constant seed value was put
      /// into results buffer for nulls.
      return HashUtil::Hash(expr_values_buffer_, results_buffer_size_, initial_seed_);
    } else {
      return HashVariableLenRow();
    }
  }

  TupleRow* GetRow(Node* node) const {
    if (stores_tuples_) {
      return reinterpret_cast<TupleRow*>(&node->data);
    } else {
      return reinterpret_cast<TupleRow*>(node->data);
    }
  }

  /// Compute the hash of the values in expr_values_buffer_ for rows with variable length
  /// fields (e.g. strings)
  uint32_t HashVariableLenRow();

  /// Returns true if the values of build_exprs evaluated over 'build_row' equal
  /// the values cached in expr_values_buffer_
  /// This will be replaced by codegen.
  bool Equals(TupleRow* build_row);

  /// Grow the node array.
  void GrowNodeArray();

  /// Sets mem_limit_exceeded_ to true and MEM_LIMIT_EXCEEDED for the query.
  /// allocation_size is the attempted size of the allocation that would have
  /// brought us over the mem limit.
  void MemLimitExceeded(int64_t allocation_size);

  /// Load factor that will trigger growing the hash table on insert.  This is
  /// defined as the number of non-empty buckets / total_buckets
  static const float MAX_BUCKET_OCCUPANCY_FRACTION;

  RuntimeState* state_;

  const std::vector<ExprContext*>& build_expr_ctxs_;
  const std::vector<ExprContext*>& probe_expr_ctxs_;

  /// Number of Tuple* in the build tuple row
  const int num_build_tuples_;

  /// Constants on how the hash table should behave. Joins and aggs have slightly
  /// different behavior.
  /// TODO: these constants are an ideal candidate to be removed with codegen.
  const bool stores_nulls_;
  const std::vector<bool> finds_nulls_;

  /// finds_some_nulls_ is just the logical OR of finds_nulls_.
  const bool finds_some_nulls_;

  const bool stores_tuples_;

  const int32_t initial_seed_;

  /// Number of non-empty buckets.  Used to determine when to grow and rehash
  int64_t num_filled_buckets_;

  /// number of nodes stored (i.e. size of hash table)
  int64_t num_nodes_;

  /// MemPool used to allocate data pages.
  boost::scoped_ptr<MemPool> mem_pool_;

  /// Number of data pages for nodes.
  int num_data_pages_;

  /// Next node to insert.
  Node* next_node_;

  /// Number of nodes left in the current page.
  int node_remaining_current_page_;

  MemTracker* mem_tracker_;

  /// Set to true if the hash table exceeds the memory limit. If this is set,
  /// subsequent calls to Insert() will be ignored.
  bool mem_limit_exceeded_;

  std::vector<Bucket> buckets_;

  /// equal to buckets_.size() but more efficient than the size function
  int64_t num_buckets_;

  /// The number of filled buckets to trigger a resize.  This is cached for efficiency
  int64_t num_buckets_till_resize_;

  /// Cache of exprs values for the current row being evaluated.  This can either
  /// be a build row (during Insert()) or probe row (during Find()).
  std::vector<int> expr_values_buffer_offsets_;

  /// byte offset into expr_values_buffer_ that begins the variable length results
  int var_result_begin_;

  /// byte size of 'expr_values_buffer_'
  int results_buffer_size_;

  /// buffer to store evaluated expr results.  This address must not change once
  /// allocated since the address is baked into the codegen
  uint8_t* expr_values_buffer_;

  /// Use bytes instead of bools to be compatible with llvm.  This address must
  /// not change once allocated.
  uint8_t* expr_value_null_bits_;
};

}

#endif
