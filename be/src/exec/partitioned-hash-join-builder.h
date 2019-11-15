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
#include <list>
#include <vector>

#include "common/object-pool.h"
#include "common/status.h"
#include "exec/data-sink.h"
#include "exec/filter-context.h"
#include "exec/hash-table.h"
#include "exec/join-op.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/suballocator.h"

namespace impala {

class RowDescriptor;
class RuntimeState;
class ScalarExpr;
class ScalarExprEvaluator;

/// See partitioned-hash-join-node.h for explanation of the top-level algorithm and how
/// these states fit in it.
enum class HashJoinState {
  /// Partitioning the build (right) child's input into the builder's hash partitions.
  PARTITIONING_BUILD,

  /// Processing the probe (left) child's input, probing hash tables and
  /// spilling probe rows into 'probe_hash_partitions_' if necessary.
  PARTITIONING_PROBE,

  /// Processing the spilled probe rows of a single spilled partition
  /// ('input_partition_') that fits in memory.
  PROBING_SPILLED_PARTITION,

  /// Repartitioning the build rows of a single spilled partition ('input_partition_')
  /// into the builder's hash partitions.
  /// Corresponds to PARTITIONING_BUILD but reading from a spilled partition.
  REPARTITIONING_BUILD,

  /// Probing the repartitioned hash partitions of a single spilled partition
  /// ('input_partition_') with the probe rows of that partition.
  /// Corresponds to PARTITIONING_PROBE but reading from a spilled partition.
  REPARTITIONING_PROBE,
};

/// The build side for the PartitionedHashJoinNode. Build-side rows are hash-partitioned
/// into PARTITION_FANOUT partitions, with partitions spilled if the full build side
/// does not fit in memory. Spilled partitions can be repartitioned with a different
/// hash function per level of repartitioning.
///
/// The builder owns the hash tables and build row streams. The builder first does the
/// level 0 partitioning of build rows. After FlushFinal() the builder has produced some
/// in-memory partitions and some spilled partitions. The in-memory partitions have hash
/// tables and the spilled partitions have memory reserved for a probe-side stream with
/// one write buffer, which is sufficient to spill the partition's probe rows to disk
/// without allocating additional buffers.
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
/// share the reservation to minimize the combined memory requirement. Memory for
/// probe-side buffers is reserved in the builder then handed off to the probe side
/// to implement this reservation sharing.
///
/// The full hash join algorithm is documented in PartitionedHashJoinNode.
class PhjBuilder : public DataSink {
 public:
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

  class Partition;

  PhjBuilder(int join_node_id, const std::string& join_node_label, TJoinOp::type join_op,
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
  virtual void Codegen(LlvmCodeGen* codegen) override;

  /////////////////////////////////////////
  // The following functions are used only by PartitionedHashJoinNode.
  /////////////////////////////////////////

  /// Reset the builder to the same state as it was in after calling Open().
  void Reset(RowBatch* row_batch);

  /// Represents a set of hash partitions to be handed off to the probe side.
  struct HashPartitions {
    HashPartitions() { Reset(); }
    HashPartitions(
        int level, const std::vector<Partition*>* hash_partitions, bool non_empty_build)
      : level(level),
        hash_partitions(hash_partitions),
        non_empty_build(non_empty_build) {}

    void Reset() {
      level = -1;
      hash_partitions = nullptr;
      non_empty_build = false;
    }

    // The partitioning level of this set of partitions. -1 indicates that this is
    // invalid.
    int level;

    // The current set of hash partitions. Always contains PARTITION_FANOUT partitions.
    // The partitions may be in-memory, spilled, or closed. Valid until
    // DoneProbingHashPartitions() is called.
    const std::vector<Partition*>* hash_partitions;

    // True iff the build side had at least one row in a partition.
    bool non_empty_build;
  };

  /// Get hash partitions and reservation for the initial partitionining of the probe
  /// side. Only valid to call once when in state PARTITIONING_PROBE.
  /// When this function returns successfully, 'probe_client' will have enough
  /// reservation for a write buffer for each spilled partition.
  /// Return the current set of hash partitions.
  /// TODO: IMPALA-9156: this will be a synchronization point for shared join build.
  HashPartitions BeginInitialProbe(BufferPool::ClientHandle* probe_client);

  /// Prepare to process the probe side of 'partition', either by building a hash
  /// table over 'partition', or if does not fit in memory, by repartitioning into
  /// PARTITION_FANOUT new partitions.
  ///
  /// When this function returns successfully, 'probe_client' will have enough
  /// reservation for a read buffer for the input probe stream and, if repartitioning,
  /// a write buffer for each spilled partition.
  ///
  /// If repartitioning, creates new hash partitions and repartitions 'partition' into
  /// PARTITION_FANOUT new partitions with level input_partition->level() + 1. The
  /// previous hash partitions must have been cleared with DoneProbingHashPartitions().
  /// The new hash partitions are returned in 'new_partitions'.
  /// TODO: IMPALA-9156: this will be a synchronization point for shared join build.
  Status BeginSpilledProbe(bool empty_probe, Partition* partition,
      BufferPool::ClientHandle* probe_client, bool* repartitioned, int* level,
      HashPartitions* new_partitions);

  /// Called after probing of the hash partitions returned by BeginInitialProbe() or
  /// BeginSpilledProbe() (when *repartitioning as true) is complete,
  /// i.e. all of the corresponding probe rows have been processed by
  /// PartitionedHashJoinNode. Appends in-memory partitions that may contain build
  /// rows to output to 'output_partitions' for build modes like right outer join
  /// that output unmatched rows. Close other in-memory partitions, attaching any
  /// tuple data to 'batch' if 'batch' is non-NULL. Closes spilled partitions if
  /// 'retain_spilled_partition' is false for that partition index.
  /// TODO: IMPALA-9156: this will be a synchronization point for shared join build.
  void DoneProbingHashPartitions(const bool retain_spilled_partition[PARTITION_FANOUT],
      std::list<Partition*>* output_partitions, RowBatch* batch);

  /// Called after probing of a single spilled partition returned by
  /// BeginSpilledProbe() when *repartitioning is false.
  ///
  /// If the join op requires outputting unmatched build rows and the partition
  /// may have build rows to return, it is appended to 'output_partitions'. Partitions
  /// returned via 'output_partitions' are ready for the caller to read from - either
  /// they are in-memory with a hash table built or have build_rows() prepared for
  /// reading.
  ///
  /// If no build rows need to be returned, closes the build partition and attaches any
  /// tuple data to 'batch' if 'batch' is non-NULL.
  /// TODO: IMPALA-9156: this will be a synchronization point for shared join build.
  void DoneProbingSinglePartition(
      Partition* partition, std::list<Partition*>* output_partitions, RowBatch* batch);

  /// Close the null aware partition (if there is one) and set it to NULL.
  /// TODO: IMPALA-9176: improve the encapsulation of the null-aware partition.
  void CloseNullAwarePartition() {
    if (null_aware_partition_ != nullptr) {
      // We don't need to pass in a batch because the anti-join only returns tuple data
      // from the probe side - i.e. the RowDescriptor for PartitionedHashJoinNode does
      // not include the build tuple.
      null_aware_partition_->Close(nullptr);
      null_aware_partition_ = nullptr;
    }
  }

  /// True if the hash table may contain rows with one or more NULL join keys. This
  /// depends on the join type and the equijoin conjuncts.
  /// Valid to call after InitExprsAndFilters(). Thread-safe.
  bool HashTableStoresNulls() const;

  void AddHashTableStatsToProfile(RuntimeProfile* profile);

  /// TODO: IMPALA-9156: document thread safety for accessing this from
  /// multiple PartitionedHashJoinNodes.
  HashJoinState state() const { return state_; }

  /// Valid to call after InitExprsAndFilters(). Thread-safe.
  inline const std::vector<bool>& is_not_distinct_from() const {
    return is_not_distinct_from_;
  }

  /// Accessor to allow PartitionedHashJoinNode to access null_aware_partition_.
  /// TODO: IMPALA-9176: improve the encapsulation of the null-aware partition.
  inline Partition* null_aware_partition() const { return null_aware_partition_; }

  std::string DebugString() const;

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

    bool ALWAYS_INLINE IsClosed() const { return build_rows_ == nullptr; }
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
  /// Updates 'state_' to 'next_state', logging the transition.
  void UpdateState(HashJoinState next_state);

  /// Returns the current 'state_' as a string.
  std::string PrintState() const;

  /// Create and initialize a set of hash partitions for partitioning level 'level'.
  /// The previous hash partitions must have been cleared with DoneProbing().
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
  /// spilled (with all build blocks unpinned) and memory reservation is set aside
  /// for a write buffer for the output probe streams, and, if this input is a spilled
  /// partitioned, a read buffer for the input probe stream.
  ///
  /// When this function returns successfully, each partition is in one of these states:
  /// 1. closed. No probe partition is created and the build partition is closed. No
  ///       probe stream memory is reserved for this partition.
  /// 2. in-memory. The build rows are pinned and has a hash table built. No
  ///       probe stream memory is reserved for this partition.
  /// 3. spilled. The build rows are fully unpinned and the probe stream is prepared.
  ///       Memory for a probe stream write buffer is reserved for this partition.
  Status BuildHashTablesAndReserveProbeBuffers() WARN_UNUSED_RESULT;

  /// Ensures that 'probe_stream_reservation_' has enough reservation for a stream per
  /// spilled partition in 'hash_partitions_', plus for the input stream if the input
  /// is a spilled partition (indicated by input_is_spilled). May spill additional
  /// partitions until it can free enough reservation. Returns an error if an error
  /// is encountered or if it runs out of partitions to spill.
  Status ReserveProbeBuffers(bool input_is_spilled) WARN_UNUSED_RESULT;

  /// Returns the number of partitions in 'partitions' that are spilled.
  static int GetNumSpilledPartitions(const std::vector<Partition*>& partitions);

  /// Transfer reservation for probe streams to 'probe_client'. Memory for one stream was
  /// reserved per spilled partition in FlushFinal(), plus the input stream if the input
  /// partition was spilled.
  void TransferProbeStreamReservation(BufferPool::ClientHandle* probe_client);

  /// Creates new hash partitions and repartitions 'input_partition' into PARTITION_FANOUT
  /// new partitions with level input_partition->level() + 1. The previous hash partitions
  /// must have been cleared with ClearHashPartitions(). This function reserves enough
  /// memory for a read buffer for the input probe stream and a write buffer for each
  /// spilled partition after repartitioning.
  Status RepartitionBuildInput(Partition* input_partition) WARN_UNUSED_RESULT;

  /// Returns the largest build row count out of the current hash partitions.
  int64_t LargestPartitionRows() const;

  /// Calls Close() on every Partition, deletes them, and cleans up any pointers that
  /// may reference them. If 'row_batch' if not NULL, transfers the ownership of all
  /// row-backing resources to it.
  void CloseAndDeletePartitions(RowBatch* row_batch);

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

  /// The ID of the plan join node this is associated with.
  /// TODO: we may want to replace this with a sink ID once we progress further with
  /// multithreading.
  const int join_node_id_;

  /// The label of the plan join node this is associated with.
  const std::string join_node_label_;

  /// The join operation this is building for.
  const TJoinOp::type join_op_;

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

  /// The default and max buffer sizes to use in the build streams.
  const int64_t spillable_buffer_size_;
  const int64_t max_row_buffer_size_;

  /// Allocator for hash table memory.
  boost::scoped_ptr<Suballocator> ht_allocator_;

  /// Expressions over input rows for hash table build.
  std::vector<ScalarExpr*> build_exprs_;

  /// is_not_distinct_from_[i] is true if and only if the ith equi-join predicate is IS
  /// NOT DISTINCT FROM, rather than equality.
  /// Set in InitExprsAndFilters() and constant thereafter.
  std::vector<bool> is_not_distinct_from_;

  /// Expressions for evaluating input rows for insertion into runtime filters.
  /// Only includes exprs for filters produced by this builder.
  std::vector<ScalarExpr*> filter_exprs_;

  /// List of filters to build. One-to-one correspondence with exprs in 'filter_exprs_'.
  std::vector<FilterContext> filter_ctxs_;

  /// Used for hash-related functionality, such as evaluating rows and calculating hashes.
  /// The level is set to the same level as 'hash_partitions_'.
  boost::scoped_ptr<HashTableCtx> ht_ctx_;

  /// Counters and profile objects for HashTable stats
  std::unique_ptr<HashTableStatsProfile> ht_stats_profile_;

  /// Total number of partitions created.
  RuntimeProfile::Counter* partitions_created_ = nullptr;

  /// The largest fraction (of build side) after repartitioning. This is expected to be
  /// 1 / PARTITION_FANOUT. A value much larger indicates skew.
  RuntimeProfile::HighWaterMarkCounter* largest_partition_percent_ = nullptr;

  /// Level of max partition (i.e. number of repartitioning steps).
  RuntimeProfile::HighWaterMarkCounter* max_partition_level_ = nullptr;

  /// Number of build rows that have been partitioned.
  RuntimeProfile::Counter* num_build_rows_partitioned_ = nullptr;

  /// Number of partitions that have been spilled.
  RuntimeProfile::Counter* num_spilled_partitions_ = nullptr;

  /// Number of partitions that have been repartitioned.
  RuntimeProfile::Counter* num_repartitions_ = nullptr;

  /// Time spent partitioning build rows.
  RuntimeProfile::Counter* partition_build_rows_timer_ = nullptr;

  /// Time spent building hash tables.
  RuntimeProfile::Counter* build_hash_table_timer_ = nullptr;

  /// Number of partitions which had zero probe rows and we therefore didn't build the
  /// hash table.
  RuntimeProfile::Counter* num_hash_table_builds_skipped_ = nullptr;

  /// Time spent repartitioning and building hash tables of any resulting partitions
  /// that were not spilled.
  RuntimeProfile::Counter* repartition_timer_ = nullptr;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// State of the partitioned hash join algorithm. See HashJoinState for more
  /// information.
  HashJoinState state_ = HashJoinState::PARTITIONING_BUILD;

  /// If true, the build side has at least one row.
  /// Set in FlushFinal() and not modified until Reset().
  bool non_empty_build_ = false;

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
  /// Always NULL once we are done processing the level 0 partitions.
  /// This partition starts off in memory but can be spilled.
  Partition* null_aware_partition_ = nullptr;

  /// Populated during the hash table building phase if any partitions spilled.
  /// Reservation for one probe stream write buffer per spilled partition is
  /// saved to be handed off to PartitionedHashJoinNode for use in buffering
  /// spilled probe rows.
  ///
  /// The allocation is done in the builder so that it can divide memory between the
  /// in-memory build partitions and write buffers based on the size of the partitions
  /// and available memory. E.g. if all the partitions fit in memory, no write buffers
  /// need to be allocated, but if some partitions are spilled, more build partitions
  /// may be spilled to free up memory for write buffers.
  ///
  /// Because of this, at the end of the build phase, we always have sufficient memory
  /// to execute the probe phase of the algorithm without spilling more partitions.
  ///
  /// Initialized in Open() and closed in Closed().
  BufferPool::SubReservation probe_stream_reservation_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// For the below codegen'd functions, xxx_fn_level0_ uses CRC hashing when available
  /// and is used when the partition level is 0, otherwise xxx_fn_ uses murmur hash and is
  /// used for subsequent levels.
  typedef Status (*ProcessBuildBatchFn)(
      PhjBuilder*, RowBatch*, HashTableCtx*, bool build_filters, bool is_null_aware);
  /// Jitted ProcessBuildBatch function pointers.  NULL if codegen is disabled.
  ProcessBuildBatchFn process_build_batch_fn_ = nullptr;
  ProcessBuildBatchFn process_build_batch_fn_level0_ = nullptr;

  typedef bool (*InsertBatchFn)(Partition*, TPrefetchMode::type, HashTableCtx*, RowBatch*,
      const std::vector<BufferedTupleStream::FlatRowPtr>&, Status*);
  /// Jitted Partition::InsertBatch() function pointers. NULL if codegen is disabled.
  InsertBatchFn insert_batch_fn_ = nullptr;
  InsertBatchFn insert_batch_fn_level0_ = nullptr;
};
}

#endif
