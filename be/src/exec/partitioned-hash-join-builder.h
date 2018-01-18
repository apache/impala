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

#ifndef IMPALA_EXEC_PARTITIONED_HASH_JOIN_BUILDER_H
#define IMPALA_EXEC_PARTITIONED_HASH_JOIN_BUILDER_H

#include <boost/scoped_ptr.hpp>
#include <memory>

#include "common/object-pool.h"
#include "common/status.h"
#include "exec/data-sink.h"
#include "exec/filter-context.h"
#include "exec/hash-table.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/suballocator.h"

#include "gen-cpp/PlanNodes_types.h"

namespace impala {

class RowDescriptor;
class RuntimeState;
class ScalarExpr;
class ScalarExprEvaluator;

/// The build side for the PartitionedHashJoinNode. Build-side rows are hash-partitioned
/// into PARTITION_FANOUT partitions, with partitions spilled if the full build side
/// does not fit in memory. Spilled partitions can be repartitioned with a different
/// hash function per level of repartitioning.
///
/// The builder owns the hash tables and build row streams. The builder first does the
/// level 0 partitioning of build rows. After FlushFinal() the builder has produced some
/// in-memory partitions and some spilled partitions. The in-memory partitions have hash
/// tables and the spilled partitions have a probe-side stream prepared with one write
/// buffer, which is sufficient to spill the partition's probe rows to disk without
/// allocating additional buffers.
///
/// After this initial partitioning, the join node probes the in-memory hash partitions.
/// The join node then drives processing of any spilled partitions, calling
/// Partition::BuildHashTable() to build hash tables for a spilled partition or calling
/// RepartitionBuildInput() to repartition a level n partition into multiple level n + 1
/// partitions.
///
/// Both the PartitionedHashJoinNode and the builder share a BufferPool client
/// and the corresponding reservations. Different stages of the spilling algorithm
/// require different mixes of build and probe buffers and hash tables, so we can
/// share the reservation to minimize the combined memory requirement. Initial probe-side
/// buffers are allocated in the builder then handed off to the probe side to implement
/// this reservation sharing.
///
/// TODO: after we have reliable reservations (IMPALA-3200), we can simplify the handoff
///   to the probe side by using reservations instead of preparing the streams.
///
/// The full hash join algorithm is documented in PartitionedHashJoinNode.
class PhjBuilder : public DataSink {
 public:
  class Partition;

  PhjBuilder(int join_node_id, TJoinOp::type join_op, const RowDescriptor* probe_row_desc,
      const RowDescriptor* build_row_desc, RuntimeState* state,
      BufferPool::ClientHandle* buffer_pool_client, int64_t spillable_buffer_size,
      int64_t max_row_buffer_size);

  Status InitExprsAndFilters(RuntimeState* state,
      const std::vector<TEqJoinCondition>& eq_join_conjuncts,
      const std::vector<TRuntimeFilterDesc>& filters) WARN_UNUSED_RESULT;

  /// Implementations of DataSink interface methods.
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status Send(RuntimeState* state, RowBatch* batch) override;
  virtual Status FlushFinal(RuntimeState* state) override;
  virtual void Close(RuntimeState* state) override;

  /// Does all codegen for the builder (if codegen is enabled).
  /// Updates the the builder's runtime profile with info about whether any errors
  /// occured during codegen.
  void Codegen(LlvmCodeGen* codegen);

  /////////////////////////////////////////
  // The following functions are used only by PartitionedHashJoinNode.
  /////////////////////////////////////////

  /// Reset the builder to the same state as it was in after calling Open().
  void Reset();

  /// Transfer ownership of the probe streams to the caller. One stream was allocated per
  /// spilled partition in FlushFinal(). The probe streams are empty but prepared for
  /// writing with a write buffer allocated.
  std::vector<std::unique_ptr<BufferedTupleStream>> TransferProbeStreams();

  /// Clears the current list of hash partitions. Called after probing of the partitions
  /// is done. The partitions are not closed or destroyed, since they may be spilled or
  /// may contain unmatched build rows for certain join modes (e.g. right outer join).
  void ClearHashPartitions() { hash_partitions_.clear(); }

  /// Close the null aware partition (if there is one) and set it to NULL.
  void CloseNullAwarePartition(RowBatch* out_batch) {
    if (null_aware_partition_ != NULL) {
      null_aware_partition_->Close(out_batch);
      null_aware_partition_ = NULL;
    }
  }

  /// Creates new hash partitions and repartitions 'input_partition'. The previous
  /// hash partitions must have been cleared with ClearHashPartitions().
  /// 'level' is the level new partitions should be created with. This functions prepares
  /// 'input_probe_rows' for reading in "delete_on_read" mode, so that the probe phase
  /// has enough buffers preallocated to execute successfully.
  Status RepartitionBuildInput(Partition* input_partition, int level,
      BufferedTupleStream* input_probe_rows) WARN_UNUSED_RESULT;

  /// Returns the largest build row count out of the current hash partitions.
  int64_t LargestPartitionRows() const;

  /// True if the hash table may contain rows with one or more NULL join keys. This
  /// depends on the join type and the equijoin conjuncts.
  bool HashTableStoresNulls() const;

  /// Accessor functions, mainly required to expose state to PartitionedHashJoinNode.
  inline bool non_empty_build() const { return non_empty_build_; }
  inline const std::vector<bool>& is_not_distinct_from() const {
    return is_not_distinct_from_;
  }
  inline int num_hash_partitions() const { return hash_partitions_.size(); }
  inline Partition* hash_partition(int partition_idx) const {
    DCHECK_GE(partition_idx, 0);
    DCHECK_LT(partition_idx, hash_partitions_.size());
    return hash_partitions_[partition_idx];
  }
  inline Partition* null_aware_partition() const { return null_aware_partition_; }

  std::string DebugString() const;

  /// Number of initial partitions to create. Must be a power of two.
  static const int PARTITION_FANOUT = 16;

  /// Needs to be log2(PARTITION_FANOUT).
  static const int NUM_PARTITIONING_BITS = 4;

  /// Maximum number of times we will repartition. The maximum build table we
  /// can process is:
  /// MEM_LIMIT * (PARTITION_FANOUT ^ MAX_PARTITION_DEPTH). With a (low) 1GB
  /// limit and 64 fanout, we can support 256TB build tables in the case where
  /// there is no skew.
  /// In the case where there is skew, repartitioning is unlikely to help (assuming a
  /// reasonable hash function).
  /// Note that we need to have at least as many SEED_PRIMES in HashTableCtx.
  /// TODO: we can revisit and try harder to explicitly detect skew.
  static const int MAX_PARTITION_DEPTH = 16;

  /// A partition containing a subset of build rows.
  ///
  /// A partition may contain two data structures: the build rows and optionally a hash
  /// table built over the build rows.  Building the hash table requires all build rows to
  /// be pinned in memory. The build rows are kept in memory if all partitions fit in
  /// memory, but can be unpinned and spilled to disk to free up memory. Reading or
  /// writing the unpinned rows requires a single read or write buffer. If the unpinned
  /// rows are not being read or written, they can be completely unpinned, requiring no
  /// buffers.
  ///
  /// The build input is first partitioned by hash function level 0 into the level 0
  /// partitions. Then, if the join spills and the size of a level n partition is too
  /// large to fit in memory, the partition's rows can be repartitioned with the level
  /// n + 1 hash function into the level n + 1 partitions.
  class Partition {
   public:
    Partition(RuntimeState* state, PhjBuilder* parent, int level);
    ~Partition();

    /// Close the partition and attach resources to 'batch' if non-NULL or free the
    /// resources if 'batch' is NULL. Idempotent.
    void Close(RowBatch* batch);

    /// Returns the estimated byte size of the in-memory data structures for this
    /// partition. This includes all build rows and the hash table.
    int64_t EstimatedInMemSize() const;

    /// Pins the build tuples for this partition and constructs the hash table from it.
    /// Build rows cannot be added after calling this. If the build rows could not be
    /// pinned or the hash table could not be built due to memory pressure, sets *built
    /// to false and returns OK. Returns an error status if any other error is
    /// encountered.
    Status BuildHashTable(bool* built) WARN_UNUSED_RESULT;

    /// Spills this partition, the partition's stream is unpinned with 'mode' and
    /// its hash table is destroyed if it was built. Calling with 'mode' UNPIN_ALL
    /// unpins all pages and frees all buffers associated with the partition so that
    /// the partition does not use any reservation. Calling with 'mode'
    /// UNPIN_ALL_EXCEPT_CURRENT may leave the read or write pages of the unpinned stream
    /// pinned and therefore using reservation. If the partition was previously
    /// spilled with mode UNPIN_ALL_EXCEPT_CURRENT, then calling Spill() again with
    /// UNPIN_ALL may release more reservation by unpinning the read or write page
    /// in the stream.
    Status Spill(BufferedTupleStream::UnpinMode mode) WARN_UNUSED_RESULT;

    std::string DebugString();

    bool ALWAYS_INLINE IsClosed() const { return build_rows_ == NULL; }
    BufferedTupleStream* ALWAYS_INLINE build_rows() { return build_rows_.get(); }
    HashTable* ALWAYS_INLINE hash_tbl() const { return hash_tbl_.get(); }
    bool ALWAYS_INLINE is_spilled() const { return is_spilled_; }
    int ALWAYS_INLINE level() const { return level_; }
    /// Return true if the partition can be spilled - is not closed and is not spilled.
    bool CanSpill() const { return !IsClosed() && !is_spilled(); }

   private:
    /// Inserts each row in 'batch' into 'hash_tbl_' using 'ctx'. 'flat_rows' is an array
    /// containing the rows in the hash table's tuple stream.
    /// 'prefetch_mode' is the prefetching mode in use. If it's not PREFETCH_NONE, hash
    /// table buckets which the rows hashes to will be prefetched. This parameter is
    /// replaced with a constant during codegen time. This function may be replaced with
    /// a codegen'd version. Returns true if all rows in 'batch' are successfully
    /// inserted and false otherwise. If inserting failed, 'status' indicates why it
    /// failed: if 'status' is ok, inserting failed because not enough reservation
    /// was available and if 'status' is an error, inserting failed because of that error.
    bool InsertBatch(TPrefetchMode::type prefetch_mode, HashTableCtx* ctx,
        RowBatch* batch, const std::vector<BufferedTupleStream::FlatRowPtr>& flat_rows,
        Status* status);

    const PhjBuilder* parent_;

    /// True if this partition is spilled.
    bool is_spilled_;

    /// How many times rows in this partition have been repartitioned. Partitions created
    /// from the node's children's input is level 0, 1 after the first repartitioning,
    /// etc.
    const int level_;

    /// The hash table for this partition.
    boost::scoped_ptr<HashTable> hash_tbl_;

    /// Stream of build tuples in this partition. Initially owned by this object but
    /// transferred to the parent exec node (via the row batch) when the partition
    /// is closed. If NULL, ownership has been transferred and the partition is closed.
    std::unique_ptr<BufferedTupleStream> build_rows_;
  };

  /// Computes the minimum reservation required to execute the spilling partitioned
  /// hash algorithm successfully for any input size (assuming enough disk space is
  /// available for spilled rows). The buffers are used for buffering both build and
  /// probe rows at different times, so the total requirement is the peak sum of build
  /// and probe buffers required.
  /// We need one output buffer per partition to partition the build or probe side. We
  /// need one additional buffer for the input while repartitioning the build or probe.
  /// For NAAJ, we need 3 additional buffers for 'null_aware_partition_',
  /// 'null_aware_probe_partition_' and 'null_probe_rows_'.
  int64_t MinReservation() const {
    // Must be kept in sync with HashJoinNode.computeNodeResourceProfile() in fe.
    int num_reserved_buffers = PARTITION_FANOUT + 1;
    if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) num_reserved_buffers += 3;
    // Two of the buffers must fit the maximum row.
    return spillable_buffer_size_ * (num_reserved_buffers - 2) + max_row_buffer_size_ * 2;
  }

  /// Class name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 private:
  /// Create and initialize a set of hash partitions for partitioning level 'level'.
  /// The previous hash partitions must have been cleared with ClearHashPartitions().
  /// After calling this, batches are added to the new partitions by calling Send().
  Status CreateHashPartitions(int level) WARN_UNUSED_RESULT;

  /// Create a new partition in 'all_partitions_' and prepare it for writing.
  Status CreateAndPreparePartition(int level, Partition** partition) WARN_UNUSED_RESULT;

  /// Reads the rows in build_batch and partitions them into hash_partitions_. If
  /// 'build_filters' is true, runtime filters are populated. 'is_null_aware' is
  /// set to true if the join type is a null aware join.
  Status ProcessBuildBatch(
      RowBatch* build_batch, HashTableCtx* ctx, bool build_filters,
      bool is_null_aware) WARN_UNUSED_RESULT;

  /// Append 'row' to 'stream'. In the common case, appending the row to the stream
  /// immediately succeeds. Otherwise this function falls back to the slower path of
  /// AppendRowStreamFull(), which may spill partitions to free memory. Returns false
  /// and sets 'status' if it was unable to append the row, even after spilling
  /// partitions. This odd return convention is used to avoid emitting unnecessary code
  /// for ~Status in perf-critical code.
  bool AppendRow(
      BufferedTupleStream* stream, TupleRow* row, Status* status) WARN_UNUSED_RESULT;

  /// Slow path for AppendRow() above. It is called when the stream has failed to append
  /// the row. We need to find more memory by either switching to IO-buffers, in case the
  /// stream still uses small buffers, or spilling a partition. Returns false and sets
  /// 'status' if it was unable to append the row, even after spilling partitions.
  bool AppendRowStreamFull(BufferedTupleStream* stream, TupleRow* row,
      Status* status) noexcept WARN_UNUSED_RESULT;

  /// Frees memory by spilling one of the hash partitions. The 'mode' argument is passed
  /// to the Spill() call for the selected partition. The current policy is to spill the
  /// null-aware partition first (if a NAAJ), then the largest partition. Returns non-ok
  /// status if we couldn't spill a partition. If 'spilled_partition' is non-NULL, set
  /// to the partition that was the one spilled.
  Status SpillPartition(BufferedTupleStream::UnpinMode mode,
      Partition** spilled_partition = nullptr) WARN_UNUSED_RESULT;

  /// Tries to build hash tables for all unspilled hash partitions. Called after
  /// FlushFinal() when all build rows have been partitioned and added to the appropriate
  /// streams. If the hash table could not be built for a partition, the partition is
  /// spilled (with all build blocks unpinned) and the probe stream is prepared for
  /// writing (i.e. has an initial probe buffer allocated).
  ///
  /// When this function returns successfully, each partition is in one of these states:
  /// 1. closed. No probe partition is created and the build partition is closed.
  /// 2. in-memory. The build rows are pinned and has a hash table built. No probe
  ///     partition is created.
  /// 3. spilled. The build rows are fully unpinned and the probe stream is prepared.
  Status BuildHashTablesAndPrepareProbeStreams() WARN_UNUSED_RESULT;

  /// Ensures that 'spilled_partition_probe_streams_' has a stream per spilled partition
  /// in 'hash_partitions_'. May spill additional partitions until it can create enough
  /// probe streams with write buffers. Returns an error if an error is encountered or
  /// if it runs out of partitions to spill.
  Status InitSpilledPartitionProbeStreams() WARN_UNUSED_RESULT;

  /// Calls Close() on every Partition, deletes them, and cleans up any pointers that
  /// may reference them. Also cleans up 'spilled_partition_probe_streams_'.
  void CloseAndDeletePartitions();

  /// For each filter in filters_, allocate a bloom_filter from the fragment-local
  /// RuntimeFilterBank and store it in runtime_filters_ to populate during the build
  /// phase.
  void AllocateRuntimeFilters();

  /// Iterates over the runtime filters in filters_ and inserts each row into each filter.
  /// This is replaced at runtime with code generated by CodegenInsertRuntimeFilters().
  void InsertRuntimeFilters(TupleRow* build_row) noexcept;

  /// Publish the runtime filters to the fragment-local RuntimeFilterBank.
  /// 'num_build_rows' is used to determine whether the computed filters have an
  /// unacceptably high false-positive rate.
  void PublishRuntimeFilters(int64_t num_build_rows);

  /// Codegen processing build batches. Identical signature to ProcessBuildBatch().
  /// Returns non-OK status if codegen was not possible.
  Status CodegenProcessBuildBatch(LlvmCodeGen* codegen, llvm::Function* hash_fn,
      llvm::Function* murmur_hash_fn, llvm::Function* eval_row_fn,
      llvm::Function* insert_filters_fn) WARN_UNUSED_RESULT;

  /// Codegen inserting batches into a partition's hash table. Identical signature to
  /// Partition::InsertBatch(). Returns non-OK if codegen was not possible.
  Status CodegenInsertBatch(LlvmCodeGen* codegen, llvm::Function* hash_fn,
      llvm::Function* murmur_hash_fn, llvm::Function* eval_row_fn,
      TPrefetchMode::type prefetch_mode) WARN_UNUSED_RESULT;

  /// Codegen inserting rows into runtime filters. Identical signature to
  /// InsertRuntimeFilters(). Returns non-OK if codegen was not possible.
  Status CodegenInsertRuntimeFilters(
      LlvmCodeGen* codegen, const vector<ScalarExpr*>& filter_exprs, llvm::Function** fn);

  RuntimeState* const runtime_state_;

  // The ID of the plan join node this is associated with.
  // TODO: we may want to replace this with a sink ID once we progress further with
  // multithreading.
  const int join_node_id_;

  /// The join operation this is building for.
  const TJoinOp::type join_op_;

  /// Descriptor for the probe rows, needed to initialize probe streams.
  const RowDescriptor* probe_row_desc_;

  /// Pool for objects with same lifetime as builder.
  ObjectPool obj_pool_;

  /// Client to the buffer pool, used to allocate build partition buffers and hash tables.
  /// When probing, the spilling algorithm keeps some build partitions in memory while
  /// using memory for probe buffers for spilled partitions. To support dynamically
  /// dividing memory between build and probe, this client is shared between the builder
  /// and the PartitionedHashJoinNode.
  /// TODO: this approach to sharing will not work for spilling broadcast joins with a
  /// 1:N relationship from builders to join nodes.
  BufferPool::ClientHandle* buffer_pool_client_;

  /// The default and max buffer sizes to use in the build and probe streams.
  const int64_t spillable_buffer_size_;
  const int64_t max_row_buffer_size_;

  /// Allocator for hash table memory.
  boost::scoped_ptr<Suballocator> ht_allocator_;

  /// If true, the build side has at least one row.
  bool non_empty_build_;

  /// Expressions over input rows for hash table build.
  std::vector<ScalarExpr*> build_exprs_;

  /// is_not_distinct_from_[i] is true if and only if the ith equi-join predicate is IS
  /// NOT DISTINCT FROM, rather than equality.
  std::vector<bool> is_not_distinct_from_;

  /// Expressions for evaluating input rows for insertion into runtime filters.
  std::vector<ScalarExpr*> filter_exprs_;

  /// List of filters to build. One-to-one correspondence with exprs in 'filter_exprs_'.
  std::vector<FilterContext> filter_ctxs_;

  /// Used for hash-related functionality, such as evaluating rows and calculating hashes.
  /// The level is set to the same level as 'hash_partitions_'.
  boost::scoped_ptr<HashTableCtx> ht_ctx_;

  /// Total number of partitions created.
  RuntimeProfile::Counter* partitions_created_;

  /// The largest fraction (of build side) after repartitioning. This is expected to be
  /// 1 / PARTITION_FANOUT. A value much larger indicates skew.
  RuntimeProfile::HighWaterMarkCounter* largest_partition_percent_;

  /// Level of max partition (i.e. number of repartitioning steps).
  RuntimeProfile::HighWaterMarkCounter* max_partition_level_;

  /// Number of build rows that have been partitioned.
  RuntimeProfile::Counter* num_build_rows_partitioned_;

  /// Number of hash collisions - unequal rows that have identical hash values
  RuntimeProfile::Counter* num_hash_collisions_;

  /// Total number of hash buckets across all partitions.
  RuntimeProfile::Counter* num_hash_buckets_;

  /// Number of partitions that have been spilled.
  RuntimeProfile::Counter* num_spilled_partitions_;

  /// Number of partitions that have been repartitioned.
  RuntimeProfile::Counter* num_repartitions_;

  /// Time spent partitioning build rows.
  RuntimeProfile::Counter* partition_build_rows_timer_;

  /// Time spent building hash tables.
  RuntimeProfile::Counter* build_hash_table_timer_;

  /// Time spent repartitioning and building hash tables of any resulting partitions
  /// that were not spilled.
  RuntimeProfile::Counter* repartition_timer_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Vector that owns all of the Partition objects.
  std::vector<std::unique_ptr<Partition>> all_partitions_;

  /// The current set of partitions that are being built or probed. This vector is
  /// initialized before partitioning or re-partitioning the build input
  /// and cleared after we've finished probing the partitions.
  /// This is not used when processing a single spilled partition.
  std::vector<Partition*> hash_partitions_;

  /// Partition used for null-aware joins. This partition is always processed at the end
  /// after all build and probe rows are processed. In this partition's 'build_rows_', we
  /// store all the rows for which 'build_expr_evals_' evaluated over the row returns
  /// NULL (i.e. it has a NULL on the eq join slot).
  /// NULL if the join is not null aware or we are done processing this partition.
  /// This partitions starts off in memory but can be spilled.
  Partition* null_aware_partition_;

  /// Populated during the hash table building phase if any partitions spilled.
  /// One probe stream per spilled partition is prepared for writing so that the
  /// initial write buffer is allocated.
  ///
  /// These streams are handed off to PartitionedHashJoinNode for use in buffering
  /// spilled probe rows. The allocation is done in the builder so that it can divide
  /// memory between the in-memory build partitions and write buffers based on the size
  /// of the partitions and available memory. E.g. if all the partitions fit in memory, no
  /// write buffers need to be allocated, but if some partitions are spilled, more build
  /// partitions may be spilled to free up memory for write buffers.
  ///
  /// Because of this, at the end of the build phase, we always have sufficient memory
  /// to execute the probe phase of the algorithm without spilling more partitions.
  std::vector<std::unique_ptr<BufferedTupleStream>> spilled_partition_probe_streams_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// For the below codegen'd functions, xxx_fn_level0_ uses CRC hashing when available
  /// and is used when the partition level is 0, otherwise xxx_fn_ uses murmur hash and is
  /// used for subsequent levels.
  typedef Status (*ProcessBuildBatchFn)(
      PhjBuilder*, RowBatch*, HashTableCtx*, bool build_filters, bool is_null_aware);
  /// Jitted ProcessBuildBatch function pointers.  NULL if codegen is disabled.
  ProcessBuildBatchFn process_build_batch_fn_;
  ProcessBuildBatchFn process_build_batch_fn_level0_;

  typedef bool (*InsertBatchFn)(Partition*, TPrefetchMode::type, HashTableCtx*, RowBatch*,
      const std::vector<BufferedTupleStream::FlatRowPtr>&, Status*);
  /// Jitted Partition::InsertBatch() function pointers. NULL if codegen is disabled.
  InsertBatchFn insert_batch_fn_;
  InsertBatchFn insert_batch_fn_level0_;
};
}

#endif
