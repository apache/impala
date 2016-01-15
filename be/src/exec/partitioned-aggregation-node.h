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


#ifndef IMPALA_EXEC_PARTITIONED_AGGREGATION_NODE_H
#define IMPALA_EXEC_PARTITIONED_AGGREGATION_NODE_H

#include <functional>
#include <boost/scoped_ptr.hpp>

#include "exec/exec-node.h"
#include "exec/hash-table.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/descriptors.h"  // for TupleId
#include "runtime/mem-pool.h"
#include "runtime/string-value.h"

namespace llvm {
  class Function;
}

namespace impala {

class AggFnEvaluator;
class LlvmCodeGen;
class RowBatch;
class RuntimeState;
struct StringValue;
class Tuple;
class TupleDescriptor;
class SlotDescriptor;

/// Node for doing partitioned hash aggregation.
/// This node consumes the input (which can be from the child(0) or a spilled partition).
///  1. Each row is hashed and we pick a dst partition (hash_partitions_).
///  2. If the dst partition is not spilled, we probe into the partitions hash table
///  to aggregate/insert the row.
///  3. If the partition is already spilled, the input row is spilled.
///  4. When all the input is consumed, we walk hash_partitions_, put the spilled ones
///  into spilled_partitions_ and the non-spilled ones into aggregated_partitions_.
///  aggregated_partitions_ contain partitions that are fully processed and the result
///  can just be returned. Partitions in spilled_partitions_ need to be repartitioned
///  and we just repeat these steps.
//
/// Each partition contains these structures:
/// 1) Hash Table for aggregated rows. This contains just the hash table directory
///    structure but not the rows themselves. This is NULL for spilled partitions when
///    we stop maintaining the hash table.
/// 2) MemPool for var-len result data for rows in the hash table. If the aggregate
///    function returns a string, we cannot append it to the tuple stream as that
///    structure is immutable. Instead, when we need to spill, we sweep and copy the
///    rows into a tuple stream.
/// 3) Aggregated tuple stream for rows that are/were in the hash table. This stream
///    contains rows that are aggregated. When the partition is not spilled, this stream
///    is pinned and contains the memory referenced by the hash table.
///    In the case where the aggregate function does not return a string (meaning the
///    size of all the slots is known when the row is constructed), this stream contains
///    all the memory for the result rows and the MemPool (2) is not used.
/// 4) Unaggregated tuple stream. Stream to spill unaggregated rows.
///    Rows in this stream always have child(0)'s layout.
///
/// Buffering: Each stream and hash table needs to maintain at least one buffer for
/// some duration of the processing. To minimize the memory requirements of small queries
/// (i.e. memory usage is less than one IO-buffer per partition), the streams and hash
/// tables of each partition start using small (less than IO-sized) buffers, regardless
/// of the level.
///
/// TODO: Buffer rows before probing into the hash table?
/// TODO: After spilling, we can still maintain a very small hash table just to remove
/// some number of rows (from likely going to disk).
/// TODO: Consider allowing to spill the hash table structure in addition to the rows.
/// TODO: Do we want to insert a buffer before probing into the partition's hash table?
/// TODO: Use a prefetch/batched probe interface.
/// TODO: Return rows from the aggregated_row_stream rather than the HT.
/// TODO: Think about spilling heuristic.
/// TODO: When processing a spilled partition, we have a lot more information and can
/// size the partitions/hash tables better.
/// TODO: Start with unpartitioned (single partition) and switch to partitioning and
/// spilling only if the size gets large, say larger than the LLC.
/// TODO: Simplify or cleanup the various uses of agg_fn_ctx, agg_fn_ctx_, and ctx.
/// There are so many contexts in use that a plain "ctx" variable should never be used.
/// Likewise, it's easy to mixup the agg fn ctxs, there should be a way to simplify this.
class PartitionedAggregationNode : public ExecNode {
 public:
  PartitionedAggregationNode(ObjectPool* pool,
      const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

  static const char* LLVM_CLASS_NAME;

 protected:
  /// Frees local allocations from aggregate_evaluators_ and agg_fn_ctxs
  virtual Status QueryMaintenance(RuntimeState* state);

  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  struct Partition;

  /// Number of initial partitions to create. Must be a power of 2.
  static const int PARTITION_FANOUT = 16;

  /// Needs to be the log(PARTITION_FANOUT).
  /// We use the upper bits to pick the partition and lower bits in the HT.
  /// TODO: different hash functions here too? We don't need that many bits to pick
  /// the partition so this might be okay.
  static const int NUM_PARTITIONING_BITS = 4;

  /// Maximum number of times we will repartition. The maximum build table we can process
  /// (if we have enough scratch disk space) in case there is no skew is:
  ///  MEM_LIMIT * (PARTITION_FANOUT ^ MAX_PARTITION_DEPTH).
  /// In the case where there is skew, repartitioning is unlikely to help (assuming a
  /// reasonable hash function).
  /// Note that we need to have at least as many SEED_PRIMES in HashTableCtx.
  /// TODO: we can revisit and try harder to explicitly detect skew.
  static const int MAX_PARTITION_DEPTH = 16;

  /// Codegen doesn't allow for automatic Status variables because then exception
  /// handling code is needed to destruct the Status, and our function call substitution
  /// doesn't know how to deal with the LLVM IR 'invoke' instruction. Workaround that by
  /// placing the Status here so exceptions won't need to destruct it.
  /// TODO: fix IMPALA-1948 and remove this.
  Status process_batch_status_;

  /// Tuple into which Update()/Merge()/Serialize() results are stored.
  TupleId intermediate_tuple_id_;
  TupleDescriptor* intermediate_tuple_desc_;

  /// Row with the intermediate tuple as its only tuple.
  boost::scoped_ptr<RowDescriptor> intermediate_row_desc_;

  /// Tuple into which Finalize() results are stored. Possibly the same as
  /// the intermediate tuple.
  TupleId output_tuple_id_;
  TupleDescriptor* output_tuple_desc_;

  /// Certain aggregates require a finalize step, which is the final step of the
  /// aggregate after consuming all input rows. The finalize step converts the aggregate
  /// value into its final form. This is true if this node contains aggregate that
  /// requires a finalize step.
  const bool needs_finalize_;

  /// Contains any evaluators that require the serialize step.
  bool needs_serialize_;

  std::vector<AggFnEvaluator*> aggregate_evaluators_;

  /// FunctionContext for each aggregate function and backing MemPool. String data
  /// returned by the aggregate functions is allocated via these contexts.
  /// These contexts are only passed to the evaluators in the non-partitioned
  /// (non-grouping) case. Otherwise they are only used to clone FunctionContexts for the
  /// partitions.
  /// TODO: we really need to plumb through CHAR(N) for intermediate types.
  std::vector<impala_udf::FunctionContext*> agg_fn_ctxs_;
  boost::scoped_ptr<MemPool> agg_fn_pool_;

  /// Exprs used to evaluate input rows
  std::vector<ExprContext*> probe_expr_ctxs_;

  /// Exprs used to insert constructed aggregation tuple into the hash table.
  /// All the exprs are simply SlotRefs for the intermediate tuple.
  std::vector<ExprContext*> build_expr_ctxs_;

  /// True if the resulting tuple contains var-len agg/grouping values. This
  /// means we need to do more work when allocating and spilling these rows.
  bool contains_var_len_grouping_exprs_;

  RuntimeState* state_;
  BufferedBlockMgr::Client* block_mgr_client_;

  /// MemPool used to allocate memory for when we don't have grouping and don't initialize
  /// the partitioning structures, or during Close() when creating new output tuples.
  /// For non-grouping aggregations, the ownership of the pool's memory is transferred
  /// to the output batch on eos. The pool should not be Reset() to allow amortizing
  /// memory allocation over a series of Reset()/Open()/GetNext()* calls.
  boost::scoped_ptr<MemPool> mem_pool_;

  /// The current partition and iterator to the next row in its hash table that we need
  /// to return in GetNext()
  Partition* output_partition_;
  HashTable::Iterator output_iterator_;

  typedef Status (*ProcessRowBatchFn)(
      PartitionedAggregationNode*, RowBatch*, HashTableCtx*);
  /// Jitted ProcessRowBatch function pointer.  Null if codegen is disabled.
  ProcessRowBatchFn process_row_batch_fn_;

  /// Time spent processing the child rows
  RuntimeProfile::Counter* build_timer_;

  /// Total time spent resizing hash tables.
  RuntimeProfile::Counter* ht_resize_timer_;

  /// Time spent returning the aggregated rows
  RuntimeProfile::Counter* get_results_timer_;

  /// Total number of hash buckets across all partitions.
  RuntimeProfile::Counter* num_hash_buckets_;

  /// Total number of partitions created.
  RuntimeProfile::Counter* partitions_created_;

  /// Level of max partition (i.e. number of repartitioning steps).
  RuntimeProfile::HighWaterMarkCounter* max_partition_level_;

  /// Number of rows that have been repartitioned.
  RuntimeProfile::Counter* num_row_repartitioned_;

  /// Number of partitions that have been repartitioned.
  RuntimeProfile::Counter* num_repartitions_;

  /// Number of partitions that have been spilled.
  RuntimeProfile::Counter* num_spilled_partitions_;

  /// The largest fraction after repartitioning. This is expected to be
  /// 1 / PARTITION_FANOUT. A value much larger indicates skew.
  RuntimeProfile::HighWaterMarkCounter* largest_partition_percent_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Result of aggregation w/o GROUP BY.
  /// Note: can be NULL even if there is no grouping if the result tuple is 0 width
  /// e.g. select 1 from table group by col.
  Tuple* singleton_output_tuple_;
  bool singleton_output_tuple_returned_;

  /// Used for hash-related functionality, such as evaluating rows and calculating hashes.
  /// TODO: If we want to multi-thread then this context should be thread-local and not
  /// associated with the node.
  boost::scoped_ptr<HashTableCtx> ht_ctx_;

  /// Object pool that holds the Partition objects in hash_partitions_.
  boost::scoped_ptr<ObjectPool> partition_pool_;

  /// Current partitions we are partitioning into.
  std::vector<Partition*> hash_partitions_;

  /// All partitions that have been spilled and need further processing.
  std::list<Partition*> spilled_partitions_;

  /// All partitions that are aggregated and can just return the results in GetNext().
  /// After consuming all the input, hash_partitions_ is split into spilled_partitions_
  /// and aggregated_partitions_, depending on if it was spilled or not.
  std::list<Partition*> aggregated_partitions_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// The hash table and streams (aggregated and unaggregated) for an individual
  /// partition. The streams of each partition always (i.e. regardless of level)
  /// initially use small buffers.
  struct Partition {
    Partition(PartitionedAggregationNode* parent, int level)
      : parent(parent), is_closed(false), level(level) {}

    ~Partition();

    /// Initializes aggregated_row_stream and unaggregated_row_stream, reserving
    /// one buffer for each. The buffers backing these streams are reserved, so this
    /// function will not fail with a continuable OOM. If we fail to init these buffers,
    /// the mem limit is too low to run this algorithm.
    Status InitStreams();

    /// Initializes the hash table. Returns false on OOM.
    bool InitHashTable();

    /// Called in case we need to serialize aggregated rows. This step effectively does
    /// a merge aggregation in this node.
    Status CleanUp();

    /// Closes this partition. If finalize_rows is true, this iterates over all rows
    /// in aggregated_row_stream and finalizes them (this is only used in the cancellation
    /// path).
    void Close(bool finalize_rows);

    /// Spills this partition, unpinning streams and cleaning up hash tables as necessary.
    Status Spill();

    bool is_spilled() const { return hash_tbl.get() == NULL; }

    PartitionedAggregationNode* parent;

    /// If true, this partition is closed and there is nothing left to do.
    bool is_closed;

    /// How many times rows in this partition have been repartitioned. Partitions created
    /// from the node's children's input is level 0, 1 after the first repartitionining,
    /// etc.
    const int level;

    /// Hash table for this partition.
    /// Can be NULL if this partition is no longer maintaining a hash table (i.e.
    /// is spilled).
    boost::scoped_ptr<HashTable> hash_tbl;

    /// Clone of parent's agg_fn_ctxs_ and backing MemPool.
    std::vector<impala_udf::FunctionContext*> agg_fn_ctxs;
    boost::scoped_ptr<MemPool> agg_fn_pool;

    /// Tuple stream used to store aggregated rows. When the partition is not spilled,
    /// (meaning the hash table is maintained), this stream is pinned and contains the
    /// memory referenced by the hash table. When it is spilled, aggregate rows are
    /// just appended to this stream.
    boost::scoped_ptr<BufferedTupleStream> aggregated_row_stream;

    /// Unaggregated rows that are spilled.
    boost::scoped_ptr<BufferedTupleStream> unaggregated_row_stream;
  };

  /// Stream used to store serialized spilled rows. Only used if needs_serialize_
  /// is set. This stream is never pinned and only used in Partition::Spill as a
  /// a temporary buffer.
  boost::scoped_ptr<BufferedTupleStream> serialize_stream_;

  /// Allocates a new aggregation intermediate tuple.
  /// Initialized to grouping values computed over 'current_row_' using 'agg_fn_ctxs'.
  /// Aggregation expr slots are set to their initial values.
  /// Pool/Stream specify where the memory (tuple and var len slots) should be allocated
  /// from. Only one can be set.
  /// Returns NULL if there was not enough memory to allocate the tuple or an error
  /// occurred. When returning NULL, sets *status. If 'stream' is set and its small
  /// buffers get full, it will attempt to switch to IO-buffers.
  Tuple* ConstructIntermediateTuple(
      const std::vector<impala_udf::FunctionContext*>& agg_fn_ctxs,
      MemPool* pool, BufferedTupleStream* stream, Status* status);

  /// Updates the given aggregation intermediate tuple with aggregation values computed
  /// over 'row' using 'agg_fn_ctxs'. Whether the agg fn evaluator calls Update() or
  /// Merge() is controlled by the evaluator itself, unless enforced explicitly by passing
  /// in is_merge == true.  The override is needed to merge spilled and non-spilled rows
  /// belonging to the same partition independent of whether the agg fn evaluators have
  /// is_merge() == true.
  /// This function is replaced by codegen (which is why we don't use a vector argument
  /// for agg_fn_ctxs).
  void UpdateTuple(impala_udf::FunctionContext** agg_fn_ctxs, Tuple* tuple, TupleRow* row,
                   bool is_merge = false);

  /// Called on the intermediate tuple of each group after all input rows have been
  /// consumed and aggregated. Computes the final aggregate values to be returned in
  /// GetNext() using the agg fn evaluators' Serialize() or Finalize().
  /// For the Finalize() case if the output tuple is different from the intermediate
  /// tuple, then a new tuple is allocated from 'pool' to hold the final result.
  /// Grouping values are copied into the output tuple and the the output tuple holding
  /// the finalized/serialized aggregate values is returned.
  /// TODO: Coordinate the allocation of new tuples with the release of memory
  /// so as not to make memory consumption blow up.
  Tuple* GetOutputTuple(const std::vector<impala_udf::FunctionContext*>& agg_fn_ctxs,
                        Tuple* tuple, MemPool* pool);

  /// Do the aggregation for all tuple rows in the batch when there is no grouping.
  /// The HashTableCtx argument is unused, but included so the signature matches that of
  /// ProcessBatch() for codegen. This function is replaced by codegen.
  Status ProcessBatchNoGrouping(RowBatch* batch, HashTableCtx* ht_ctx = NULL);

  /// Processes a batch of rows. This is the core function of the algorithm. We partition
  /// the rows into hash_partitions_, spilling as necessary.
  /// If AGGREGATED_ROWS is true, it means that the rows in the batch are already
  /// pre-aggregated.
  //
  /// This function is replaced by codegen. It's inlined into ProcessBatch_true/false in
  /// the IR module. We pass in ht_ctx_.get() as an argument for performance.
  template<bool AGGREGATED_ROWS>
  Status IR_ALWAYS_INLINE ProcessBatch(RowBatch* batch, HashTableCtx* ht_ctx);

  /// This function processes each individual row in ProcessBatch(). Must be inlined
  /// into ProcessBatch for codegen to substitute function calls with codegen'd versions.
  template<bool AGGREGATED_ROWS>
  Status IR_ALWAYS_INLINE ProcessRow(TupleRow* row, HashTableCtx* ht_ctx);

  /// Create a new intermediate tuple in partition, initialized with row. ht_ctx is
  /// the context for the partition's hash table and hash is the precomputed hash of
  /// the row. The row can be an unaggregated or aggregated row depending on
  /// AGGREGATED_ROWS. Spills partitions if necessary to append the new intermediate
  /// tuple to the partition's stream. Must be inlined into ProcessBatch for codegen to
  /// substitute function calls with codegen'd versions. insert_it is an iterator for
  /// insertion returned from HashTable::FindOrInsert().
  template<bool AGGREGATED_ROWS>
  Status IR_ALWAYS_INLINE AddIntermediateTuple(Partition* partition,
      HashTableCtx* ht_ctx, TupleRow* row, uint32_t hash, HashTable::Iterator insert_it);

  /// Append a row to a spilled partition. May spill partitions if needed to switch to
  /// I/O buffers. Selects the correct stream according to the argument. Inlined into
  /// ProcessBatch().
  template<bool AGGREGATED_ROWS>
  Status IR_ALWAYS_INLINE AppendSpilledRow(Partition* partition, TupleRow* row);

  /// Append a row to a stream of a spilled partition. May spill partitions if needed
  /// to append the row.
  Status AppendSpilledRow(BufferedTupleStream* stream, TupleRow* row);

  /// Reads all the rows from input_stream and process them by calling ProcessBatch().
  template<bool AGGREGATED_ROWS>
  Status ProcessStream(BufferedTupleStream* input_stream);

  /// Initializes hash_partitions_. 'level' is the level for the partitions to create.
  /// Also sets ht_ctx_'s level to 'level'.
  Status CreateHashPartitions(int level);

  /// Ensure that hash tables for all in-memory partitions are large enough to fit
  /// num_rows additional rows.
  Status CheckAndResizeHashPartitions(int num_rows, HashTableCtx* ht_ctx);

  /// Iterates over all the partitions in hash_partitions_ and returns the number of rows
  /// of the largest spilled partition (in terms of number of aggregated and unaggregated
  /// rows).
  int64_t LargestSpilledPartition() const;

  /// Prepares the next partition to return results from. On return, this function
  /// initializes output_iterator_ and output_partition_. This either removes
  /// a partition from aggregated_partitions_ (and is done) or removes the next
  /// partition from aggregated_partitions_ and repartitions it.
  Status NextPartition();

  /// Picks a partition from hash_partitions_ to spill.
  Status SpillPartition();

  /// Moves the partitions in hash_partitions_ to aggregated_partitions_ or
  /// spilled_partitions_. Partitions moved to spilled_partitions_ are unpinned.
  /// input_rows is the number of input rows that have been repartitioned.
  /// Used for diagnostics.
  Status MoveHashPartitions(int64_t input_rows);

  /// Calls Close() on every Partition in 'aggregated_partitions_',
  /// 'spilled_partitions_', and 'hash_partitions_' and then resets the lists,
  /// the vector and the partition pool.
  void ClosePartitions();

  /// Calls finalizes on all tuples starting at 'it'.
  void CleanupHashTbl(const std::vector<impala_udf::FunctionContext*>& agg_fn_ctxs,
      HashTable::Iterator it);

  /// Codegen UpdateSlot(). Returns NULL if codegen is unsuccessful.
  /// Assumes is_merge = false;
  llvm::Function* CodegenUpdateSlot(AggFnEvaluator* evaluator, SlotDescriptor* slot_desc);

  /// Codegen UpdateTuple(). Returns NULL if codegen is unsuccessful.
  llvm::Function* CodegenUpdateTuple();

  /// Codegen the process row batch loop.  The loop has already been compiled to
  /// IR and loaded into the codegen object.  UpdateAggTuple has also been
  /// codegen'd to IR.  This function will modify the loop subsituting the statically
  /// compiled functions with codegen'd ones.
  /// Assumes AGGREGATED_ROWS = false.
  llvm::Function* CodegenProcessBatch();

  /// Functions to instantiate templated versions of ProcessBatch().
  /// The xcompiled versions of these functions are used in CodegenProcessBatch().
  /// TODO: is there a better way to do this?
  Status ProcessBatch_false(RowBatch* batch, HashTableCtx* ht_ctx);
  Status ProcessBatch_true(RowBatch* batch, HashTableCtx* ht_ctx);

  /// We need two buffers per partition, one for the aggregated stream and one
  /// for the unaggregated stream. We need an additional buffer to read the stream
  /// we are currently repartitioning.
  /// If we need to serialize, we need an additional buffer while spilling a partition
  /// as the partitions aggregate stream needs to be serialized and rewritten.
  int MinRequiredBuffers() const {
    return 2 * PARTITION_FANOUT + 1 + (needs_serialize_ ? 1 : 0);
  }
};

}

#endif
