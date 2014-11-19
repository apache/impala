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


#ifndef IMPALA_EXEC_HASH_TABLE_H
#define IMPALA_EXEC_HASH_TABLE_H

#include <vector>
#include <boost/cstdint.hpp>
#include <boost/scoped_ptr.hpp>
#include "codegen/impala-ir.h"
#include "common/logging.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/mem-tracker.h"
#include "runtime/tuple-row.h"
#include "util/bitmap.h"
#include "util/hash-util.h"

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
class HashTable;

// Hash table implementation designed for partitioned hash aggregation and hash joins.
// This is not templatized and is tailored to the usage pattern for aggregation and joins.
// The hash table store TupleRows and allows for different exprs for insertions and finds.
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
// When growing the hash table, the number of buckets is doubled, and nodes from a
// particular bucket either stay in place or move to an analogous bucket in the second
// half of buckets. This behavior allows us to avoid moving about half the nodes each
// time, and maintains good cache properties by only accessing 2 buckets at a time.
// The node vector is modified in place.
// Due to the doubling nature of the buckets, we require that the number of buckets is a
// power of 2. This allows us to determine if a node needs to move by simply checking a
// single bit, and further allows us to initially hash nodes using a bitmask.
//
// TODO: this is not a fancy hash table in terms of memory access patterns (cuckoo-hashing
// or something that spills to disk). We will likely want to invest more time into this.
// TODO: hash-join and aggregation have very different access patterns.  Joins insert
// all the rows and then calls scan to find them.  Aggregation interleaves Find() and
// Inserts().  We can want to optimize joins more heavily for Inserts() (in particular
// growing).
// TODO: batched interface for inserts and finds.
// TODO: do we need to check mem limit exceeded so often. Check once per batch?

// Control block for a hash table.  This class contains the logic as well as the variables
// needed by a thread to operate on a hash table.
class HashTableCtx {
 public:
  // Create a hash table context.
  //  - build_exprs are the exprs that should be used to evaluate rows during Insert().
  //  - probe_exprs are used during Find()
  //  - stores_nulls: if false, TupleRows with nulls are ignored during Insert
  //  - finds_nulls: if false, Find() returns End() for TupleRows with nulls
  //      even if stores_nulls is true
  //  - initial_seed: Initial seed value to use when computing hashes for rows with
  //    level 0. Other levels have their seeds derived from this seed.
  //  - The max levels we will hash with.
  HashTableCtx(const std::vector<ExprContext*>& build_expr_ctxs,
      const std::vector<ExprContext*>& probe_expr_ctxs, bool stores_nulls,
      bool finds_nulls, int32_t initial_seed, int max_levels,
      int num_build_tuples);

  // Call to cleanup any resources.
  void Close();

  void set_level(int level);
  int level() const { return level_; }

  // Returns the results of the exprs at 'expr_idx' evaluated over the last row
  // processed.
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

  // Evaluate and hash the build/probe row, returning in *hash. Returns false if this
  // row should be rejected (doesn't need to be processed further) because it
  // contains NULL.
  // These need to be inlined in the IR module so we can find and replace the calls to
  // EvalBuildRow()/EvalProbeRow().
  bool IR_ALWAYS_INLINE EvalAndHashBuild(TupleRow* row, uint32_t* hash);
  bool IR_ALWAYS_INLINE EvalAndHashProbe(TupleRow* row, uint32_t* hash);

  // Codegen for evaluating a tuple row.  Codegen'd function matches the signature
  // for EvalBuildRow and EvalTupleRow.
  // If build_row is true, the codegen uses the build_exprs, otherwise the probe_exprs.
  llvm::Function* CodegenEvalRow(RuntimeState* state, bool build_row);

  // Codegen for evaluating a TupleRow and comparing equality against
  // 'expr_values_buffer_'.  Function signature matches HashTable::Equals().
  llvm::Function* CodegenEquals(RuntimeState* state);

  // Codegen for hashing the expr values in 'expr_values_buffer_'. Function prototype
  // matches HashCurrentRow identically. Unlike HashCurrentRow(), the returned function
  // only uses a single hash function, rather than switching based on level_.
  // If 'use_murmur' is true, murmur hash is used, otherwise CRC is used if the hardware
  // supports it (see hash-util.h).
  llvm::Function* CodegenHashCurrentRow(RuntimeState* state, bool use_murmur);

  static const char* LLVM_CLASS_NAME;

 private:
  friend class HashTable;

  // Compute the hash of the values in expr_values_buffer_.
  // This will be replaced by codegen.  We don't want this inlined for replacing
  // with codegen'd functions so the function name does not change.
  uint32_t IR_NO_INLINE HashCurrentRow() {
    DCHECK_LT(level_, seeds_.size());
    uint32_t seed = seeds_[level_];
    if (var_result_begin_ == -1) {
      // This handles NULLs implicitly since a constant seed value was put
      // into results buffer for nulls.
      // TODO: figure out which hash function to use. We need to generate uncorrelated
      // hashes by changing just the seed. CRC does not have this property and FNV is
      // okay. We should switch to something else.
      return Hash(expr_values_buffer_, results_buffer_size_, seed);
    } else {
      return HashTableCtx::HashVariableLenRow();
    }
  }

  // Wrapper function for calling correct HashUtil function in non-codegen'd case.
  uint32_t inline Hash(const void* input, int len, int32_t hash) {
    // Use CRC hash at first level for better performance. Switch to murmur hash at
    // subsequent levels since CRC doesn't randomize well with different seed inputs.
    if (level_ == 0) return HashUtil::Hash(input, len, hash);
    return HashUtil::MurmurHash2_64(input, len, hash);
  }

  // Evaluate 'row' over build exprs caching the results in 'expr_values_buffer_' This
  // will be replaced by codegen.  We do not want this function inlined when cross
  // compiled because we need to be able to differentiate between EvalBuildRow and
  // EvalProbeRow by name and the build/probe exprs are baked into the codegen'd function.
  bool IR_NO_INLINE EvalBuildRow(TupleRow* row) {
    return EvalRow(row, build_expr_ctxs_);
  }

  // Evaluate 'row' over probe exprs caching the results in 'expr_values_buffer_'
  // This will be replaced by codegen.
  bool IR_NO_INLINE EvalProbeRow(TupleRow* row) {
    return EvalRow(row, probe_expr_ctxs_);
  }

  // Compute the hash of the values in expr_values_buffer_ for rows with variable length
  // fields (e.g. strings)
  uint32_t HashVariableLenRow();

  // Evaluate the exprs over row and cache the results in 'expr_val_buf'.
  // Returns whether any expr evaluated to NULL.
  // This will be replaced by codegen.
  bool EvalRow(TupleRow* row, const std::vector<ExprContext*>& ctxs);

  // Returns true if the values of build_exprs evaluated over 'build_row' equal
  // the values cached in expr_values_buffer_.
  // This will be replaced by codegen.
  bool Equals(TupleRow* build_row);

  const std::vector<ExprContext*>& build_expr_ctxs_;
  const std::vector<ExprContext*>& probe_expr_ctxs_;

  // Constants on how the hash table should behave. Joins and aggs have slightly
  // different behavior.
  // TODO: these constants are an ideal candidate to be removed with codegen.
  // TODO: ..or with template-ization
  const bool stores_nulls_;
  const bool finds_nulls_;

  // The current level this context is working on. Each level needs to use a
  // different seed.
  int level_;

  // The seeds to use for hashing. Indexed by the level.
  std::vector<uint32_t> seeds_;

  // Cache of exprs values for the current row being evaluated.  This can either
  // be a build row (during Insert()) or probe row (during Find()).
  std::vector<int> expr_values_buffer_offsets_;

  // byte offset into expr_values_buffer_ that begins the variable length results
  int var_result_begin_;

  // byte size of 'expr_values_buffer_'
  int results_buffer_size_;

  // buffer to store evaluated expr results.  This address must not change once
  // allocated since the address is baked into the codegen
  uint8_t* expr_values_buffer_;

  // Use bytes instead of bools to be compatible with llvm.  This address must
  // not change once allocated.
  uint8_t* expr_value_null_bits_;

  // Scratch buffer to generate rows on the fly.
  TupleRow* row_;

  // Cross-compiled functions to access member variables used in CodegenHashCurrentRow().
  uint32_t GetHashSeed() const;
};

// The hash table data structure. Consists of a vector of buckets (of linked nodes).
// The data allocated by the hash table comes from the BufferedBlockMgr.
class HashTable {
 private:
  struct Node;

 public:
  class Iterator;

  // Create a hash table.
  //  - client: block mgr client to allocate data pages from.
  //  - num_build_tuples: number of Tuples in the build tuple row.
  //  - tuple_stream: the tuple stream which contains the tuple rows index by the
  //    hash table. Can be NULL if the rows contain only a single tuple, in which
  //    the 'tuple_stream' is unused.
  //  - use_initial_small_pages: if true the first fixed N data_pages_ will be smaller
  //    than the io buffer size.
  //  - max_num_buckets: the maximum number of buckets that can be stored. If we
  //    try to grow the number of buckets to a larger number, the inserts will fail.
  //    -1, if it unlimited.
  //  - initial_num_buckets: number of buckets that the hash table
  //    should be initialized with.
  HashTable(RuntimeState* state, BufferedBlockMgr::Client* client,
      int num_build_tuples, BufferedTupleStream* tuple_stream,
      bool use_initial_small_pages, int64_t max_num_buckets,
      int64_t initial_num_buckets = 1024);

  // Ctor used only for testing. Memory is allocated from the pool instead of the
  // block mgr.
  HashTable(MemPool* pool, int num_buckets = 1024);

  // Allocates the initial bucket structure. Returns false if OOM.
  bool Init();

  // Call to cleanup any resources. Must be called once.
  void Close();

  // Insert row into the hash table. EvalAndHashBuild()/EvalAndHashProbe() must
  // be called on 'row' before inserting.
  // This will grow the hash table if necessary. If there is not enough memory,
  // the insert fails and this function returns false.
  // The 'row' is not copied by the hash table and the caller must guarantee it
  // stays in memory.
  // 'idx' is the index into tuple_stream_ for this row. If the row contains more
  // than one tuples, the 'idx' is stored instead of the 'row'.
  // Returns false if there was not enough memory to insert the row.
  bool IR_ALWAYS_INLINE Insert(HashTableCtx* ht_ctx,
      const BufferedTupleStream::RowIdx& idx, TupleRow* row, uint32_t hash);

  bool IR_ALWAYS_INLINE Insert(HashTableCtx* ht_ctx, Tuple* tuple, uint32_t hash);

  // Returns the start iterator for all rows that match the last row evaluated in
  // 'ht_cxt'. EvalAndHashBuild/EvalAndHashProbe must have been called before calling
  // this. Hash must be the hash returned by EvalAndHashBuild/Probe.
  // The iterator can be iterated until HashTable::End() to find all the matching rows.
  // Only one scan can be in progress at any time (i.e. it is not legal to call
  // Find(), begin iterating through all the matches, call another Find(),
  // and continuing iterator from the first scan iterator).
  // Advancing the returned iterator will go to the next matching row.  The matching
  // rows are evaluated lazily (i.e. computed as the Iterator is moved).
  // Returns HashTable::End() if there is no match.
  Iterator IR_ALWAYS_INLINE Find(HashTableCtx* ht_ctx, uint32_t hash);

  // Returns number of elements in the hash table
  int64_t size() const { return num_nodes_; }

  // Returns the number of buckets
  int64_t num_buckets() const { return num_buckets_; }

  // Returns the load factor (the number of non-empty buckets)
  float load_factor() const {
    return num_filled_buckets_ / static_cast<float>(num_buckets_);
  }

  // Returns an estimate of the number of bytes needed to build the hash table
  // structure for 'num_rows'.
  static int64_t EstimateSize(int64_t num_rows) {
    return EstimatedNumBuckets(num_rows) * sizeof(Bucket) + num_rows * sizeof(Node);
  }

  // Returns the estimated number of buckets (rounded up to a power of two) to
  // store num_rows.
  static int64_t EstimatedNumBuckets(int64_t num_rows) {
    // Assume 50% fill factor.
    int64_t num_buckets = num_rows * 2;
    return BitUtil::NextPowerOfTwo(num_buckets);
  }

  // Returns the number of bytes allocated to the hash table
  int64_t byte_size() const { return total_data_page_size_; }

  // Can be called after all insert calls to update the bitmap filters for the probe
  // side values. The bitmap filters are similar to Bloom filters in that they have no
  // false negatives but they will have false positives.
  // These filters are not added to the runtime state.
  void UpdateProbeFilters(HashTableCtx* ht_ctx,
      std::vector<std::pair<SlotId, Bitmap*> >& bitmaps);

  // Returns an iterator at the beginning of the hash table.  Advancing this iterator
  // will traverse all elements.
  Iterator Begin(HashTableCtx* ht_ctx);

  // Return an iterator pointing to the first element in the hash table that does not
  // have its matched flag set. Used in right-outer and full-outer joins.
  Iterator FirstUnmatched(HashTableCtx* ctx);

  // Returns true if there was a least one match.
  bool HasMatches() const { return has_matches_; }

  // Returns end marker
  Iterator End() { return Iterator(); }

  // Dump out the entire hash table to string.  If skip_empty, empty buckets are
  // skipped.  If show_match, it also prints the matched flag of each node. If build_desc
  // is non-null, the build rows will be output.  Otherwise just the build row addresses.
  std::string DebugString(bool skip_empty, bool show_match,
      const RowDescriptor* build_desc);

  // stl-like iterator interface.
  class Iterator {
   public:
    Iterator() : table_(NULL), bucket_idx_(-1), node_(NULL) {
    }

    // Iterates to the next element.  In the case where the iterator was
    // from a Find, this will lazily evaluate that bucket, only returning
    // TupleRows that match the current scan row. No-op if the iterator is at the end.
    template<bool check_match>
    void IR_ALWAYS_INLINE Next(HashTableCtx* ht_ctx);

    // Iterates to the next element that does not have its matched flag set. Returns false
    // if it reaches the end of the table without finding an unmatched element. Used in
    // right-outer and full-outer joins.
    bool NextUnmatched();

    // Returns the current row. Callers must check the iterator is not AtEnd() before
    // calling GetRow(). The returned row is owned by the iterator and valid until
    // the next call to GetRow(). It is safe to advance the iterator.
    TupleRow* GetRow() {
      DCHECK(!AtEnd());
      return table_->GetRow(node_, ctx_->row_);
    }

    Tuple* GetTuple() {
      DCHECK(!AtEnd());
      DCHECK(table_->stores_tuples_);
      return reinterpret_cast<Tuple*>(node_->tuple);
    }

    // Sets as matched the node currently pointed by the iterator. The iterator
    // cannot be AtEnd().
    void set_matched() {
      DCHECK(!AtEnd());
      node_->matched = true;
      table_->has_matches_ = true;
    }

    bool matched() const {
      DCHECK(!AtEnd());
      return node_->matched;
    }

    void reset() {
      bucket_idx_ = -1;
      node_ = NULL;
    }

    // Returns true if this iterator is at the end, i.e. GetRow() cannot be called.
    bool AtEnd() const { return node_ == NULL; }
    bool operator!=(const Iterator& rhs) { return !(*this == rhs); }

    bool operator==(const Iterator& rhs) {
      return bucket_idx_ == rhs.bucket_idx_ && node_ == rhs.node_;
    }

   private:
    friend class HashTable;

    Iterator(HashTable* table, HashTableCtx* ctx,
        int bucket_idx, Node* node, uint32_t hash)
      : table_(table),
        ctx_(ctx),
        bucket_idx_(bucket_idx),
        node_(node),
        scan_hash_(hash) {
    }

    HashTable* table_;
    HashTableCtx* ctx_;

    // Current bucket idx
    int64_t bucket_idx_;

    // Current node idx (within current bucket)
    Node* node_;

    // Cached hash value for the row passed to Find()
    uint32_t scan_hash_;
  };

 private:
  friend class Iterator;
  friend class HashTableTest;

  // TODO: bit pack this struct. The default alignment makes this struct have a lot
  // of wasted bytes.
  struct Node {
    // Only used for full/right outer hash join to indicate if this row has been
    // matched.
    // From an abstraction point of view, this is an awkward place to store this
    // information but it is very efficient here. This space is otherwise unused
    // (and we can bitpack this more in the future).
    bool matched;

    // TODO: Do we even have to cache the hash value?
    uint32_t hash;   // Cache of the hash for data_
    Node* next;      // Chain to next node for collisions
    union {
      BufferedTupleStream::RowIdx idx;
      Tuple* tuple;
    };
  };

  struct Bucket {
    Node* node;
    Bucket() : node(NULL) { }
  };

  // Returns the next non-empty bucket and updates idx to be the index of that bucket.
  // If there are no more buckets, returns NULL and sets idx to -1
  Bucket* NextBucket(int64_t* bucket_idx);

  // Resize the hash table to 'num_buckets'. Returns false on OOM.
  bool ResizeBuckets(int64_t num_buckets);

  // Insert row into the hash table. Returns the node that was inserted. Returns NULL
  // if there was not enough memory.
  Node* IR_ALWAYS_INLINE InsertImpl(HashTableCtx* ht_ctx, uint32_t hash);

  // Chains the node at 'node_idx' to 'bucket'.  Nodes in a bucket are chained
  // as a linked list; this places the new node at the beginning of the list.
  void AddToBucket(Bucket* bucket, Node* node);

  // Moves a node from one bucket to another. 'previous_node' refers to the
  // node (if any) that's chained before this node in from_bucket's linked list.
  void MoveNode(Bucket* from_bucket, Bucket* to_bucket, Node* node,
     Node* previous_node);

  TupleRow* GetRow(Node* node, TupleRow* row) const {
    if (stores_tuples_) {
      return reinterpret_cast<TupleRow*>(&node->tuple);
    } else {
      tuple_stream_->GetTupleRow(node->idx, row);
      return row;
    }
  }

  // Grow the node array. Returns false on OOM.
  bool GrowNodeArray();

  // Load factor that will trigger growing the hash table on insert.  This is
  // defined as the number of non-empty buckets / total_buckets
  static const float MAX_BUCKET_OCCUPANCY_FRACTION;

  RuntimeState* state_;

  // Client to allocate data pages with.
  BufferedBlockMgr::Client* block_mgr_client_;

  // Stream contains the rows referenced by the hash table. Can be NULL if the
  // row only contains a single tuple, in which case the TupleRow indirection
  // is removed by the hash table.
  BufferedTupleStream* tuple_stream_;

  // Only used for tests to allocate data pages instead of the block mgr.
  MemPool* data_page_pool_;

  // Number of Tuple* in the build tuple row
  const int num_build_tuples_;

  // Constants on how the hash table should behave. Joins and aggs have slightly
  // different behavior.
  // TODO: these constants are an ideal candidate to be removed with codegen.
  // TODO: ..or with template-ization
  const bool stores_tuples_;

  // If true use small pages for the first few allocated data pages.
  const bool use_initial_small_pages_;

  const int64_t max_num_buckets_;

  // Number of non-empty buckets.  Used to determine when to grow and rehash
  int64_t num_filled_buckets_;

  // number of nodes stored (i.e. size of hash table)
  int64_t num_nodes_;

  // Data pages for all nodes. These are always pinned.
  std::vector<BufferedBlockMgr::Block*> data_pages_;

  // Byte size of all buffers in data_pages_.
  int64_t total_data_page_size_;

  // Next node to insert.
  Node* next_node_;

  // Number of nodes left in the current page.
  int node_remaining_current_page_;

  // Array of all buckets. Owned by this node. Using c-style array to control
  // control memory footprint.
  Bucket* buckets_;

  // Number of buckets.
  int64_t num_buckets_;

  // The number of filled buckets to trigger a resize.  This is cached for efficiency
  int64_t num_buckets_till_resize_;

  // Flag used to disable spilling hash tables that already had matches in case of
  // right joins (IMPALA-1488).
  // TODO: Not fail when spilling hash tables with matches in right joins
  bool has_matches_;
};

}

#endif
