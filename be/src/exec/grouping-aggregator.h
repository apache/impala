// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef IMPALA_EXEC_GROUPING_AGGREGATOR_H
#define IMPALA_EXEC_GROUPING_AGGREGATOR_H

#include <deque>
#include <memory>
#include <vector>

#include "codegen/codegen-fn-ptr.h"
#include "exec/aggregator.h"
#include "exec/hash-table.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/bufferpool/suballocator.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/reservation-manager.h"

namespace impala {

class AggFnEvaluator;
class GroupingAggregator;
class PlanNode;
class LlvmCodeGen;
class QueryState;
class RowBatch;
class RuntimeState;
struct ScalarExprsResultsRowLayout;
class TAggregator;
class Tuple;

/// Aggregator for doing grouping aggregations. Input is passed to the aggregator through
/// AddBatch(), or AddBatchStreaming() if this is a pre-agg. Then:
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
/// Buffering: Each stream and hash table needs to maintain at least one buffer when
/// it is being read or written. The streams for a given agg use a uniform buffer size,
/// except when processing rows larger than that buffer size. In that case, the agg uses
/// BufferedTupleStream's variable buffer size support to handle larger rows up to the
/// maximum row size. Only two max-sized buffers are needed for the agg to spill: one
/// to hold rows being read from a spilled input stream and another for a temporary write
/// buffer when adding a row to an output stream.
///
/// Two-phase aggregation: we support two-phase distributed aggregations, where
/// pre-aggregrations attempt to reduce the size of data before shuffling data across the
/// network to be merged by the merge aggregation node. This aggregator supports a
/// streaming mode for pre-aggregations where it maintains a hash table of aggregated
/// rows, but can pass through unaggregated rows (after transforming them into the
/// same tuple format as aggregated rows) when a heuristic determines that it is better
/// to send rows across the network instead of consuming additional memory and CPU
/// resources to expand its hash table. The planner decides whether a given
/// pre-aggregation should use the streaming preaggregation algorithm or the same
/// blocking aggregation algorithm as used in merge aggregations.
/// TODO: make this less of a heuristic by factoring in the cost of the exchange vs the
/// cost of the pre-aggregation.
///
/// Handling memory pressure: the node uses two different strategies for responding to
/// memory pressure, depending on whether it is a streaming pre-aggregation or not. If
/// the node is a streaming preaggregation, it stops growing its hash table further by
/// converting unaggregated rows into the aggregated tuple format and passing them
/// through. If the node is not a streaming pre-aggregation, it responds to memory
/// pressure by spilling partitions to disk.
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
/// TODO: support an Init() method with an initial value in the UDAF interface.

class GroupingAggregatorConfig : public AggregatorConfig {
 public:
  GroupingAggregatorConfig(
      const TAggregator& taggregator, FragmentState* state, PlanNode* pnode, int agg_idx);
  Status Init(
      const TAggregator& taggregator, FragmentState* state, PlanNode* pnode) override;
  void Close() override;
  void Codegen(FragmentState* state) override;
  ~GroupingAggregatorConfig() override {}

  /// Row with the intermediate tuple as its only tuple.
  /// Construct a new row desc for preparing the build exprs because neither the child's
  /// nor this node's output row desc may contain the intermediate tuple, e.g.,
  /// in a single-node plan with an intermediate tuple different from the output tuple.
  /// Lives in the query state's obj_pool.
  RowDescriptor intermediate_row_desc_;

  /// True if this is first phase of a two-phase distributed aggregation for which we
  /// are doing a streaming preaggregation.
  const bool is_streaming_preagg_;

  /// Resource information sent from the frontend.
  const TBackendResourceProfile resource_profile_;

  /// True if any of the evaluators require the serialize step.
  bool needs_serialize_ = false;

  /// Exprs used to insert constructed aggregation tuple into the hash table.
  /// All the exprs are simply SlotRefs for the intermediate tuple.
  std::vector<ScalarExpr*> build_exprs_;

  /// Exprs used to evaluate input rows
  std::vector<ScalarExpr*> grouping_exprs_;

  /// Indices of grouping exprs with var-len string types in grouping_exprs_.
  /// We need to do more work for var-len expressions when allocating and spilling rows.
  /// All var-len grouping exprs have type string.
  std::vector<int> string_grouping_exprs_;

  /// Used for codegening hash table specific methods and to create the corresponding
  /// instance of HashTableCtx.
  const HashTableConfig* hash_table_config_;

  typedef Status (*AddBatchImplFn)(
      GroupingAggregator*, RowBatch*, TPrefetchMode::type, HashTableCtx*, bool);
  /// Jitted AddBatchImpl function pointer. Null if codegen is disabled.
  CodegenFnPtr<AddBatchImplFn> add_batch_impl_fn_;

  typedef Status (*AddBatchStreamingImplFn)(GroupingAggregator*, int, bool,
      TPrefetchMode::type, RowBatch*, RowBatch*, HashTableCtx*, int[]);
  /// Jitted AddBatchStreamingImpl function pointer. Null if codegen is disabled.
  CodegenFnPtr<AddBatchStreamingImplFn> add_batch_streaming_impl_fn_;

  int GetNumGroupingExprs() const override { return grouping_exprs_.size(); }

 private:
  /// Codegen the non-streaming add row batch loop. The loop has already been compiled to
  /// IR and loaded into the codegen object. UpdateAggTuple has also been codegen'd to IR.
  /// This function will modify the loop subsituting the statically compiled functions
  /// with codegen'd ones. 'add_batch_impl_fn_' will be updated with the codegened
  /// function.
  /// Assumes AGGREGATED_ROWS = false.
  Status CodegenAddBatchImpl(
      LlvmCodeGen* codegen, TPrefetchMode::type prefetch_mode) WARN_UNUSED_RESULT;

  /// Codegen the materialization loop for streaming preaggregations.
  /// 'add_batch_streaming_impl_fn_' will be updated with the codegened function.
  Status CodegenAddBatchStreamingImpl(
      LlvmCodeGen* codegen, TPrefetchMode::type prefetch_mode) WARN_UNUSED_RESULT;
};

class GroupingAggregator : public Aggregator {
 public:
  GroupingAggregator(ExecNode* exec_node, ObjectPool* pool,
      const GroupingAggregatorConfig& config, int64_t estimated_input_cardinality);

  virtual Status Prepare(RuntimeState* state) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch) override;
  virtual void Close(RuntimeState* state) override;

  virtual Status AddBatch(RuntimeState* state, RowBatch* batch) override;
  virtual Status AddBatchStreaming(RuntimeState* state, RowBatch* out_batch,
      RowBatch* child_batch, bool* eos) override;
  virtual Status InputDone() override;

  virtual int GetNumGroupingExprs() override { return grouping_exprs_.size(); }

  virtual void SetDebugOptions(const TDebugOptions& debug_options) override;

  virtual std::string DebugString(int indentation_level = 0) const override;
  virtual void DebugString(int indentation_level, std::stringstream* out) const override;

  virtual int64_t GetNumKeys() const override;

 private:
  struct Partition;

  /// Reference to the hash table config which is a part of the GroupingAggregatorConfig
  /// that was used to create this object. Its used to create an instance of the
  /// HashTableCtx in Prepare(). Not Owned.
  const HashTableConfig& hash_table_config_;

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

  /// Default initial number of buckets in a hash table.
  /// TODO: rethink this ?
  static const int64_t PAGG_DEFAULT_HASH_TABLE_SZ = 1024;

  /// Codegen doesn't allow for automatic Status variables because then exception
  /// handling code is needed to destruct the Status, and our function call substitution
  /// doesn't know how to deal with the LLVM IR 'invoke' instruction. Workaround that by
  /// placing the Status here so exceptions won't need to destruct it.
  /// TODO: fix IMPALA-1948 and remove this.
  Status add_batch_status_;

  /// Row with the intermediate tuple as its only tuple.
  /// Construct a new row desc for preparing the build exprs because neither the child's
  /// nor this node's output row desc may contain the intermediate tuple, e.g.,
  /// in a single-node plan with an intermediate tuple different from the output tuple.
  /// Lives in the query state's obj_pool.
  const RowDescriptor& intermediate_row_desc_;

  /// True if this is first phase of a two-phase distributed aggregation for which we
  /// are doing a streaming preaggregation.
  const bool is_streaming_preagg_;

  /// True if any of the evaluators require the serialize step.
  bool needs_serialize_ = false;

  /// Exprs used to evaluate input rows
  const std::vector<ScalarExpr*>& grouping_exprs_;

  /// Exprs used to insert constructed aggregation tuple into the hash table.
  /// All the exprs are simply SlotRefs for the intermediate tuple.
  const std::vector<ScalarExpr*>& build_exprs_;

  /// Indices of grouping exprs with var-len string types in grouping_exprs_.
  /// We need to do more work for var-len expressions when allocating and spilling rows.
  /// All var-len grouping exprs have type string.
  std::vector<int> string_grouping_exprs_;

  RuntimeState* state_;

  /// Allocator for hash table memory.
  std::unique_ptr<Suballocator> ht_allocator_;
  /// Saved reservation for writing a large page to a spilled stream or writing the last
  /// large row to a pinned partition when building/repartitioning a spilled partition.
  /// ('max_page_len' - 'default_page_len') reservation is saved when claiming the initial
  /// min reservation.
  BufferPool::SubReservation large_write_page_reservation_;
  /// Saved reservation for reading a large page from a spilled stream.
  /// ('max_page_len' - 'default_page_len') reservation is saved when claiming the initial
  /// min reservation.
  BufferPool::SubReservation large_read_page_reservation_;

  /// MemPool used to allocate memory during Close() when creating new output tuples. The
  /// pool should not be Reset() to allow amortizing memory allocation over a series of
  /// Reset()/Open()/GetNext()* calls.
  std::unique_ptr<MemPool> tuple_pool_;

  /// The current partition and iterator to the next row in its hash table that we need
  /// to return in GetNext(). If 'output_iterator_' is not AtEnd() then
  /// 'output_partition_' is not nullptr.
  Partition* output_partition_ = nullptr;
  HashTable::Iterator output_iterator_;

  /// Resource information sent from the frontend.
  const TBackendResourceProfile resource_profile_;

  std::unique_ptr<ReservationTracker> reservation_tracker_;
  ReservationManager reservation_manager_;
  BufferPool::ClientHandle* buffer_pool_client();

  /// The number of rows that have been passed to AddBatch() or AddBatchStreaming().
  int64_t num_input_rows_ = 0;

  /// True if this aggregator is being executed in a subplan.
  const bool is_in_subplan_;

  int64_t limit_; // -1: no limit
  bool ReachedLimit() { return limit_ != -1 && num_rows_returned_ >= limit_; }

  /// Jitted AddBatchImpl function pointer. Null if codegen is disabled.
  const CodegenFnPtr<GroupingAggregatorConfig::AddBatchImplFn>& add_batch_impl_fn_;

  /// Jitted AddBatchStreamingImpl function pointer. Null if codegen is disabled.
  const CodegenFnPtr<GroupingAggregatorConfig::AddBatchStreamingImplFn>&
      add_batch_streaming_impl_fn_;

  /// Total time spent resizing hash tables.
  RuntimeProfile::Counter* ht_resize_timer_ = nullptr;

  /// Time spent returning the aggregated rows
  RuntimeProfile::Counter* get_results_timer_ = nullptr;

  /// Counters and profile objects for HashTable stats
  std::unique_ptr<HashTableStatsProfile> ht_stats_profile_;

  /// Total number of partitions created.
  RuntimeProfile::Counter* partitions_created_ = nullptr;

  /// Level of max partition (i.e. number of repartitioning steps).
  RuntimeProfile::HighWaterMarkCounter* max_partition_level_ = nullptr;

  /// Number of rows that have been repartitioned.
  RuntimeProfile::Counter* num_row_repartitioned_ = nullptr;

  /// Number of partitions that have been repartitioned.
  RuntimeProfile::Counter* num_repartitions_ = nullptr;

  /// Number of partitions that have been spilled.
  RuntimeProfile::Counter* num_spilled_partitions_ = nullptr;

  /// The largest fraction after repartitioning. This is expected to be
  /// 1 / PARTITION_FANOUT. A value much larger indicates skew.
  RuntimeProfile::HighWaterMarkCounter* largest_partition_percent_ = nullptr;

  /// Time spent in streaming preagg algorithm.
  RuntimeProfile::Counter* streaming_timer_ = nullptr;

  /// The number of rows passed through without aggregation.
  RuntimeProfile::Counter* num_passthrough_rows_ = nullptr;

  /// The estimated reduction of the preaggregation.
  RuntimeProfile::Counter* preagg_estimated_reduction_ = nullptr;

  /// Expose the minimum reduction factor to continue growing the hash tables.
  RuntimeProfile::Counter* preagg_streaming_ht_min_reduction_ = nullptr;

  /// The estimated number of input rows from the planner.
  int64_t estimated_input_cardinality_;

  TDebugOptions debug_options_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// If true, no more rows to output from partitions.
  bool partition_eos_ = false;

  /// When streaming rows through unaggregated, if the out batch reaches capacity before
  /// the input batch is fully processed, 'streaming_idx_' indicates the position within
  /// the input batch to resume at in the next call to AddBatchStreaming(). This is used
  /// in the case where there are multiple aggregators, as the out batch passed to
  /// AddBatchStreaming() may already have rows passed through by another aggregator.
  int32_t streaming_idx_ = 0;

  /// Used for hash-related functionality, such as evaluating rows and calculating hashes.
  /// It also owns the evaluators for the grouping and build expressions used during hash
  /// table insertion and probing.
  boost::scoped_ptr<HashTableCtx> ht_ctx_;

  /// Object pool that holds the Partition objects in hash_partitions_.
  std::unique_ptr<ObjectPool> partition_pool_;

  /// Current partitions we are partitioning into. IMPALA-5788: For the case where we
  /// rebuild a spilled partition that fits in memory, all pointers in this vector will
  /// point to a single in-memory partition.
  std::vector<Partition*> hash_partitions_;

  /// Cache for hash tables in 'hash_partitions_'. IMPALA-5788: For the case where we
  /// rebuild a spilled partition that fits in memory, all pointers in this array will
  /// point to the hash table that is a part of a single in-memory partition.
  HashTable* hash_tbls_[PARTITION_FANOUT];

  /// All partitions that have been spilled and need further processing.
  std::deque<Partition*> spilled_partitions_;

  /// All partitions that are aggregated and can just return the results in GetNext().
  /// After consuming all the input, hash_partitions_ is split into spilled_partitions_
  /// and aggregated_partitions_, depending on if it was spilled or not.
  std::deque<Partition*> aggregated_partitions_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// The hash table and streams (aggregated and unaggregated) for an individual
  /// partition. The streams of each partition always (i.e. regardless of level)
  /// initially use small buffers. Streaming pre-aggregations do not spill and do not
  /// require an unaggregated stream.
  struct Partition {
    Partition(GroupingAggregator* parent, int level, int idx)
      : parent(parent), is_closed(false), level(level), idx(idx) {}

    ~Partition();

    /// Initializes aggregated_row_stream and unaggregated_row_stream (if a spilling
    /// aggregation), allocating one buffer for each. Spilling merge aggregations must
    /// have enough reservation for the initial buffer for the stream, so this should
    /// not fail due to OOM. Preaggregations do not reserve any buffers: if does not
    /// have enough reservation for the initial buffer, the aggregated row stream is not
    /// created and an OK status is returned.
    Status InitStreams() WARN_UNUSED_RESULT;

    /// Initializes the hash table. 'aggregated_row_stream' must be non-NULL.
    /// Sets 'got_memory' to true if the hash table was initialised or false on OOM.
    /// After returning, 'hash_tbl' will be non-null iff the returned status is OK.
    Status InitHashTable(bool* got_memory) WARN_UNUSED_RESULT;

    /// Called in case we need to serialize aggregated rows. This step effectively does
    /// a merge aggregation in this aggregator.
    Status SerializeStreamForSpilling() WARN_UNUSED_RESULT;

    /// Closes this partition. If finalize_rows is true, this iterates over all rows
    /// in aggregated_row_stream and finalizes them (this is only used in the cancellation
    /// path).
    void Close(bool finalize_rows);

    /// Spill this partition. 'more_aggregate_rows' = true means that more aggregate rows
    /// may be appended to the the partition before appending unaggregated rows. On
    /// success, one of the streams is left with a write iterator: the aggregated stream
    /// if 'more_aggregate_rows' is true or the unaggregated stream otherwise.
    Status Spill(bool more_aggregate_rows) WARN_UNUSED_RESULT;

    bool is_spilled() const { return hash_tbl.get() == nullptr; }

    std::string DebugString() const;

    GroupingAggregator* parent;

    /// If true, this partition is closed and there is nothing left to do.
    bool is_closed;

    /// How many times rows in this partition have been repartitioned. Partitions created
    /// from the aggregator's input is level 0, 1 after the first repartitionining, etc.
    const int level;

    /// The index of this partition within 'hash_partitions_' at its level.
    const int idx;

    /// Hash table for this partition.
    /// Can be NULL if this partition is no longer maintaining a hash table (i.e.
    /// is spilled or we are passing through all rows for this partition).
    std::unique_ptr<HashTable> hash_tbl;

    /// Clone of parent's agg_fn_evals_. Permanent allocations come from
    /// 'agg_fn_perm_pool' and result allocations come from 'expr_results_pool_'.
    std::vector<AggFnEvaluator*> agg_fn_evals;

    /// Pool for permanent allocations for this partition's 'agg_fn_evals'. Freed at the
    /// same times as 'agg_fn_evals' are closed: either when the partition is closed or
    /// when it is spilled.
    std::unique_ptr<MemPool> agg_fn_perm_pool;

    /// Tuple stream used to store aggregated rows. When the partition is not spilled,
    /// (meaning the hash table is maintained), this stream is pinned and contains the
    /// memory referenced by the hash table. When it is spilled, this consumes reservation
    /// for a write buffer only during repartitioning of aggregated rows.
    ///
    /// For streaming preaggs, this may be NULL if sufficient memory is not available.
    /// In that case hash_tbl is also NULL and all rows for the partition will be passed
    /// through.
    std::unique_ptr<BufferedTupleStream> aggregated_row_stream;

    /// Unaggregated rows that are spilled. Always NULL for streaming pre-aggregations.
    /// Always unpinned. Has a write buffer allocated when the partition is spilled and
    /// unaggregated rows are being processed.
    std::unique_ptr<BufferedTupleStream> unaggregated_row_stream;
  };

  /// Stream used to store serialized spilled rows. Only used if needs_serialize_
  /// is set. This stream is never pinned and only used in Partition::Spill as a
  /// a temporary buffer.
  std::unique_ptr<BufferedTupleStream> serialize_stream_;

  /// Accessor for 'hash_tbls_' that verifies consistency with the partitions.
  HashTable* ALWAYS_INLINE GetHashTable(int partition_idx) {
    HashTable* ht = hash_tbls_[partition_idx];
    DCHECK_EQ(ht, hash_partitions_[partition_idx]->hash_tbl.get());
    return ht;
  }

  /// Copies grouping values stored in 'ht_ctx_' that were computed over 'current_row_'
  /// using 'grouping_expr_evals_'. Aggregation expr slots are set to their initial
  /// values. Returns NULL if there was not enough memory to allocate the tuple or errors
  /// occurred. In which case, 'status' is set. Allocates tuple and var-len data for
  /// grouping exprs from stream. Var-len data for aggregate exprs is allocated from the
  /// FunctionContexts, so is stored outside the stream. If stream's small buffers get
  /// full, it will attempt to switch to IO-buffers.
  Tuple* ConstructIntermediateTuple(const std::vector<AggFnEvaluator*>& agg_fn_evals,
      BufferedTupleStream* stream, Status* status) noexcept;

  /// Constructs intermediate tuple, allocating memory from pool instead of the stream.
  /// Returns NULL and sets status if there is not enough memory to allocate the tuple.
  Tuple* ConstructIntermediateTuple(const std::vector<AggFnEvaluator*>& agg_fn_evals,
      MemPool* pool, Status* status) noexcept;

  /// Returns the number of bytes of variable-length data for the grouping values stored
  /// in 'ht_ctx_'.
  int GroupingExprsVarlenSize();

  /// Initializes intermediate tuple by copying grouping values stored in 'ht_ctx_' that
  /// that were computed over 'current_row_' using 'grouping_expr_evals_'. Writes the
  /// var-len data into buffer. 'buffer' points to the start of a buffer of at least the
  /// size of the variable-length data: 'varlen_size'.
  void CopyGroupingValues(Tuple* intermediate_tuple, uint8_t* buffer, int varlen_size);

  /// Processes a batch of rows. This is the core function of the algorithm. We partition
  /// the rows into hash_partitions_, spilling as necessary.
  /// If AGGREGATED_ROWS is true, it means that the rows in the batch are already
  /// pre-aggregated.
  /// 'prefetch_mode' specifies the prefetching mode in use. If it's not PREFETCH_NONE,
  ///     hash table buckets will be prefetched based on the hash values computed. Note
  ///     that 'prefetch_mode' will be substituted with constants during codegen time.
  /// 'has_more_rows' is used in building/repartitioning a spilled partition, to indicate
  ///     whether there are more rows after the given 'batch' in the input. We may restore
  ///     the 'large_write_page_reservation_' when adding the last row.
  ///
  /// This function is replaced by codegen. We pass in ht_ctx_.get() as an argument for
  /// performance.
  template <bool AGGREGATED_ROWS>
  Status IR_ALWAYS_INLINE AddBatchImpl(RowBatch* batch, TPrefetchMode::type prefetch_mode,
      HashTableCtx* ht_ctx, bool has_more_rows) WARN_UNUSED_RESULT;

  /// Evaluates the rows in 'batch' starting at 'start_row_idx' and stores the results in
  /// the expression values cache in 'ht_ctx'. The number of rows evaluated depends on
  /// the capacity of the cache. 'prefetch_mode' specifies the prefetching mode in use.
  /// If it's not PREFETCH_NONE, hash table buckets for the computed hashes will be
  /// prefetched. Note that codegen replaces 'prefetch_mode' with a constant.
  template <bool AGGREGATED_ROWS>
  void EvalAndHashPrefetchGroup(RowBatch* batch, int start_row_idx,
      TPrefetchMode::type prefetch_mode, HashTableCtx* ht_ctx);

  /// This function processes each individual row in AddBatchImpl(). Must be inlined into
  /// AddBatchImpl for codegen to substitute function calls with codegen'd versions.
  /// May spill partitions if not enough memory is available.
  template <bool AGGREGATED_ROWS>
  Status IR_ALWAYS_INLINE ProcessRow(
      TupleRow* row, HashTableCtx* ht_ctx, bool has_more_rows) WARN_UNUSED_RESULT;

  /// Create a new intermediate tuple in partition, initialized with row. ht_ctx is
  /// the context for the partition's hash table and hash is the precomputed hash of
  /// the row. The row can be an unaggregated or aggregated row depending on
  /// AGGREGATED_ROWS. Spills partitions if necessary to append the new intermediate
  /// tuple to the partition's stream. Must be inlined into AddBatchImpl for codegen
  /// to substitute function calls with codegen'd versions.  insert_it is an iterator
  /// for insertion returned from HashTable::FindBuildRowBucket().
  template <bool AGGREGATED_ROWS>
  Status IR_ALWAYS_INLINE AddIntermediateTuple(Partition* partition, TupleRow* row,
      uint32_t hash, HashTable::Iterator insert_it, bool has_more_rows)
      WARN_UNUSED_RESULT;

  /// Append a row to a spilled partition. The row may be aggregated or unaggregated
  /// according to AGGREGATED_ROWS. May spill partitions if needed to append the row
  /// buffers.
  template <bool AGGREGATED_ROWS>
  Status IR_ALWAYS_INLINE AppendSpilledRow(
      Partition* partition, TupleRow* row) WARN_UNUSED_RESULT;

  /// Reads all the rows from input_stream and process them by calling AddBatchImpl().
  template <bool AGGREGATED_ROWS>
  Status ProcessStream(BufferedTupleStream* input_stream, bool has_more_streams)
      WARN_UNUSED_RESULT;

  /// Get rows for the next rowbatch from the next partition. Sets 'partition_eos_' to
  /// true if all rows from all partitions have been returned or the limit is reached.
  Status GetRowsFromPartition(
      RuntimeState* state, RowBatch* row_batch) WARN_UNUSED_RESULT;

  /// Return true if we should keep expanding hash tables in the preagg. If false,
  /// the preagg should pass through any rows it can't fit in its tables.
  bool ShouldExpandPreaggHashTables() const;

  /// Streaming processing of in_batch from child. Rows from child are either aggregated
  /// into the hash table or added to 'out_batch' in the intermediate tuple format.
  /// 'in_batch' is processed entirely, and 'out_batch' must have enough capacity to
  /// store all of the rows in 'in_batch'.
  /// 'agg_idx' and 'needs_serialize' are arguments so that codegen can replace them with
  ///     constants, rather than using the member variables of the same names.
  /// 'prefetch_mode' specifies the prefetching mode in use. If it's not PREFETCH_NONE,
  ///     hash table buckets will be prefetched based on the hash values computed. Note
  ///     that 'prefetch_mode' will be substituted with constants during codegen time.
  /// 'remaining_capacity' is an array with PARTITION_FANOUT entries with the number of
  ///     additional rows that can be added to the hash table per partition. It is updated
  ///     by AddBatchStreamingImpl() when it inserts new rows.
  /// 'ht_ctx' is passed in as a way to avoid aliasing of 'this' confusing the optimiser.
  Status AddBatchStreamingImpl(int agg_idx, bool needs_serialize,
      TPrefetchMode::type prefetch_mode, RowBatch* in_batch, RowBatch* out_batch,
      HashTableCtx* ht_ctx, int remaining_capacity[PARTITION_FANOUT]) WARN_UNUSED_RESULT;

  /// Tries to add intermediate to the hash table 'hash_tbl' of 'partition' for streaming
  /// aggregation. The input row must have been evaluated with 'ht_ctx', with 'hash' set
  /// to the corresponding hash. If the tuple already exists in the hash table, update
  /// the tuple and return true. Otherwise try to create a new entry in the hash table,
  /// returning true if successful or false if the table is full. 'remaining_capacity'
  /// keeps track of how many more entries can be added to the hash table so we can avoid
  /// retrying inserts. It is decremented if an insert succeeds and set to zero if an
  /// insert fails. If an error occurs, returns false and sets 'status'.
  bool IR_ALWAYS_INLINE TryAddToHashTable(HashTableCtx* ht_ctx, Partition* partition,
      HashTable* hash_tbl, TupleRow* in_row, uint32_t hash, int* remaining_capacity,
      Status* status) WARN_UNUSED_RESULT;

  /// Initializes hash_partitions_. 'level' is the level for the partitions to create.
  /// If 'single_partition_idx' is provided, it must be a number in range
  /// [0, PARTITION_FANOUT), and only that partition is created - all others point to it.
  /// Also sets ht_ctx_'s level to 'level'.
  Status CreateHashPartitions(
      int level, int single_partition_idx = -1) WARN_UNUSED_RESULT;

  /// Ensure that hash tables for all in-memory partitions are large enough to fit
  /// 'num_rows' additional hash table entries. If there is not enough memory to
  /// resize the hash tables, may spill partitions. 'aggregated_rows' is true if
  /// we're currently partitioning aggregated rows.
  Status CheckAndResizeHashPartitions(
      bool aggregated_rows, int num_rows, HashTableCtx* ht_ctx) WARN_UNUSED_RESULT;

  /// Prepares the next partition to return results from. On return, this function
  /// initializes output_iterator_ and output_partition_. This either removes
  /// a partition from aggregated_partitions_ (and is done) or removes the next
  /// partition from aggregated_partitions_ and repartitions it.
  Status NextPartition() WARN_UNUSED_RESULT;

  /// Tries to build the first partition in 'spilled_partitions_'.
  /// If successful, set *built_partition to the partition. The caller owns the partition
  /// and is responsible for closing it. If unsuccessful because the partition could not
  /// fit in memory, set *built_partition to NULL and append the spilled partition to the
  /// head of 'spilled_partitions_' so it can be processed by
  /// RepartitionSpilledPartition().
  Status BuildSpilledPartition(Partition** built_partition) WARN_UNUSED_RESULT;

  /// Repartitions the first partition in 'spilled_partitions_' into PARTITION_FANOUT
  /// output partitions. On success, each output partition is either:
  /// * closed, if no rows were added to the partition.
  /// * in 'spilled_partitions_', if the partition spilled.
  /// * in 'aggregated_partitions_', if the output partition was not spilled.
  Status RepartitionSpilledPartition() WARN_UNUSED_RESULT;

  /// Picks a partition from 'hash_partitions_' to spill. 'more_aggregate_rows' is passed
  /// to Partition::Spill() when spilling the partition. See the Partition::Spill()
  /// comment for further explanation.
  Status SpillPartition(bool more_aggregate_rows) WARN_UNUSED_RESULT;

  /// Moves the partitions in hash_partitions_ to aggregated_partitions_ or
  /// spilled_partitions_. Partitions moved to spilled_partitions_ are unpinned.
  /// input_rows is the number of input rows that have been repartitioned.
  /// Used for diagnostics.
  Status MoveHashPartitions(int64_t input_rows) WARN_UNUSED_RESULT;

  /// Adds a partition to the front of 'spilled_partitions_' for later processing.
  /// 'spilled_partitions_' uses LIFO so more finely partitioned partitions are processed
  /// first). This allows us to delete pages earlier and bottom out the recursion
  /// earlier and also improves time locality of access to spilled data on disk.
  Status PushSpilledPartition(Partition* partition) WARN_UNUSED_RESULT;

  /// Calls Close() on 'output_partition_' and every Partition in
  /// 'aggregated_partitions_', 'spilled_partitions_', and 'hash_partitions_' and then
  /// resets the lists, the vector, the partition pool, and 'output_iterator_'.
  void ClosePartitions();

  /// Calls finalizes on all tuples starting at 'it'.
  void CleanupHashTbl(
      const std::vector<AggFnEvaluator*>& agg_fn_evals, HashTable::Iterator it);

  /// Compute minimum buffer reservation for grouping aggregations.
  /// We need one buffer per partition, which is used either as the write buffer for the
  /// aggregated stream or the unaggregated stream. We need an additional buffer to read
  /// the stream we are currently repartitioning. The read buffer needs to be a max-sized
  /// buffer to hold a max-sized row and we need one max-sized write buffer that is used
  /// temporarily to append a row to any stream.
  ///
  /// If we need to serialize, we need an additional buffer while spilling a partition
  /// as the partitions aggregate stream needs to be serialized and rewritten.
  /// We do not spill streaming preaggregations, so we do not need to reserve any buffers.
  int64_t MinReservation() const;

  /// Try to save the extra reservation ('max_row_buffer_size' - 'spillable_buffer_size')
  /// for a large write page to 'large_write_page_reservation_'. Do nothing if there are
  /// not enough unused reservation. Return true if succeeds.
  bool TrySaveLargeWritePageReservation();

  /// Similar to above but for the large read page.
  bool TrySaveLargeReadPageReservation();

  /// Same as TrySaveLargeWritePageReservation() but make sure it succeeds.
  void SaveLargeWritePageReservation();

  /// Same as TrySaveLargeReadPageReservation() but make sure it succeeds.
  void SaveLargeReadPageReservation();

  /// Restore the extra reservation we saved in 'large_write_page_reservation_' for a
  /// large write page. 'large_write_page_reservation_' must not be used.
  void RestoreLargeWritePageReservation();

  /// Similar to above but for the large read page.
  void RestoreLargeReadPageReservation();

  /// A wrapper of 'stream->AddRow()' to add 'row' to a spilled 'stream'. When it fails to
  /// add a large row due to run out of unused reservation and fails to increase the
  /// reservation, retry it after restoring the large write page reservation when we don't
  /// need to save this reservation for spilling partitions. If succeeds, returns true and
  /// save back the large write page reservation. Otherwise, returns false with a non-ok
  /// status.
  bool AddRowToSpilledStream(BufferedTupleStream* stream, TupleRow* __restrict__ row,
      Status* status);

  /// Gets current number of pinned partitions.
  int GetNumPinnedPartitions();
};
} // namespace impala

#endif // IMPALA_EXEC_GROUPING_AGGREGATOR_H
