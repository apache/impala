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

#include <deque>
#include <memory>
#include <utility>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/atomic.h"
#include "codegen/codegen-fn-ptr.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exec/filter-context.h"
#include "exec/hash-table.h"
#include "exec/join-builder.h"
#include "exec/join-op.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/suballocator.h"
#include "runtime/reservation-manager.h"

namespace impala {

class CyclicBarrier;
class PhjBuilder;
class PhjBuilderPartition;
class RowDescriptor;
class RuntimeState;
class ScalarExpr;
class ScalarExprEvaluator;

/// Method signature of the codegened version of InsertBatch().
typedef bool (*InsertBatchFn)(PhjBuilderPartition*, TPrefetchMode::type, HashTableCtx*,
    RowBatch*, const std::vector<BufferedTupleStream::FlatRowPtr>&, Status*);

/// Partitioned Hash Join Builder Config class. This has a few extra methods to be used
/// directly by the PartitionedHashJoinPlanNode. Since it is expected to only be created
/// and used by PartitionedHashJoinPlanNode only, the DataSinkConfig::Init() and
/// DataSinkConfig::CreateSink() are not implemented for it.
class PhjBuilderConfig : public JoinBuilderConfig {
 public:
  DataSink* CreateSink(RuntimeState* state) const override;

  /// Creates a PhjBuilder for embedded use within a PartitionedHashJoinNode.
  PhjBuilder* CreateSink(BufferPool::ClientHandle* buffer_pool_client,
      int64_t spillable_buffer_size, int64_t max_row_buffer_size,
      RuntimeState* state) const;

  /// Creates a PhjBuilderConfig for embedded use within a PartitionedHashJoinNode.
  /// Creates the object in the state's object pool. To be used only by
  /// PartitionedHashJoinPlanNode.
  static Status CreateConfig(FragmentState* state, int join_node_id,
      TJoinOp::type join_op, const RowDescriptor* build_row_desc,
      const std::vector<TEqJoinCondition>& eq_join_conjuncts,
      const std::vector<TRuntimeFilterDesc>& filters, uint32_t hash_seed,
      PhjBuilderConfig** sink);

  void Close() override;
  void Codegen(FragmentState* state) override;

  ~PhjBuilderConfig() override {}

  /// Expressions over input rows for hash table build.
  std::vector<ScalarExpr*> build_exprs_;

  /// is_not_distinct_from_[i] is true if and only if the ith equi-join predicate is IS
  /// NOT DISTINCT FROM, rather than equality. This is the case when IS NOT DISTINCT FROM
  /// is explicitly used as a join predicate or when joining Iceberg equality delete
  /// files to data files.
  /// Set in InitExprsAndFilters() and constant thereafter.
  std::vector<bool> is_not_distinct_from_;

  /// Expressions for evaluating input rows for insertion into runtime filters.
  /// Only includes exprs for filters produced by this builder.
  std::vector<ScalarExpr*> filter_exprs_;

  /// The runtime filter descriptors of filters produced by this builder.
  vector<TRuntimeFilterDesc> filter_descs_;

  /// Seed used for hashing rows. Must match seed used in the PartitionedHashJoinNode.
  uint32_t hash_seed_;

  /// Resource information sent from the frontend. Non-null if this is a separate join
  /// build.
  const TBackendResourceProfile* resource_profile_ = nullptr;

  /// Used for codegening hash table specific methods and to create the corresponding
  /// instance of HashTableCtx.
  const HashTableConfig* hash_table_config_;

  /// For the below codegen'd functions, xxx_fn_level0_ uses CRC hashing when available
  /// and is used when the partition level is 0, otherwise xxx_fn_ uses murmur hash and is
  /// used for subsequent levels.
  typedef Status (*ProcessBuildBatchFn)(
      PhjBuilder*, RowBatch*, HashTableCtx*, bool build_filters, bool is_null_aware);
  /// Jitted ProcessBuildBatch function pointers. NULL if codegen is disabled.
  CodegenFnPtr<ProcessBuildBatchFn> process_build_batch_fn_;
  CodegenFnPtr<ProcessBuildBatchFn> process_build_batch_fn_level0_;

  /// Jitted Partition::InsertBatch() function pointers. NULL if codegen is disabled.

  /// Method signature of the codegened version of Partition::InsertBatch().
  CodegenFnPtr<InsertBatchFn> insert_batch_fn_;
  CodegenFnPtr<InsertBatchFn> insert_batch_fn_level0_;

 protected:
  /// Initialization for separate sink.
  Status Init(const TDataSink& tsink, const RowDescriptor* input_row_desc,
      FragmentState* state) override;

 private:
  /// Helper method used by CreateConfig() to initialize embedded builder.
  /// 'tsink' does not need to be initialized by the caller - all values to be used are
  /// passed in as arguments and this function fills in required fields in 'tsink'.
  Status Init(FragmentState* state, int join_node_id, TJoinOp::type join_op,
      const RowDescriptor* build_row_desc,
      const std::vector<TEqJoinCondition>& eq_join_conjuncts,
      const std::vector<TRuntimeFilterDesc>& filters, uint32_t hash_seed,
      TDataSink* tsink);

  /// Initializes the build and filter expressions, creates a copy of the filter
  /// descriptors that will be generated by this sink and initializes the hash table
  /// config object.
  Status InitExprsAndFilters(FragmentState* state,
      const std::vector<TEqJoinCondition>& eq_join_conjuncts,
      const std::vector<TRuntimeFilterDesc>& filters);

  /// Codegen processing build batches. Identical signature to ProcessBuildBatch().
  /// Returns non-OK status if codegen was not possible.
  Status CodegenProcessBuildBatch(LlvmCodeGen* codegen, llvm::Function* hash_fn,
      llvm::Function* murmur_hash_fn, llvm::Function* eval_row_fn,
      llvm::Function* insert_filters_fn);

  /// Codegen inserting batches into a partition's hash table. Identical signature to
  /// Partition::InsertBatch(). Returns non-OK if codegen was not possible.
  Status CodegenInsertBatch(LlvmCodeGen* codegen, llvm::Function* hash_fn,
      llvm::Function* murmur_hash_fn, llvm::Function* eval_row_fn,
      TPrefetchMode::type prefetch_mode);

  /// Codegen inserting rows into runtime filters. Identical signature to
  /// InsertRuntimeFilters(). Returns non-OK if codegen was not possible.
  Status CodegenInsertRuntimeFilters(LlvmCodeGen* codegen,
      const std::vector<ScalarExpr*>& filter_exprs, llvm::Function** fn);
};

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
class PhjBuilderPartition {
 public:
  PhjBuilderPartition(RuntimeState* state, PhjBuilder* parent, int level);
  ~PhjBuilderPartition();

  using PartitionId = int;

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
  PartitionId id() const { return id_; }
  bool ALWAYS_INLINE is_spilled() const { return is_spilled_; }
  int ALWAYS_INLINE level() const { return level_; }
  /// Return true if the partition can be spilled - is not closed and is not spilled.
  bool CanSpill() const { return !IsClosed() && !is_spilled(); }
  int64_t num_spilled_probe_rows() const { return num_spilled_probe_rows_.Load(); }

  /// Increment the number of spilled probe rows. Thread-safe.
  void IncrementNumSpilledProbeRows(int64_t count) {
    num_spilled_probe_rows_.Add(count);
  }

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

  /// Id for this partition that is unique within the builder.
  const PartitionId id_;

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

  /// The number of spilled probe rows associated with this partition. Updated in
  /// DoneProbingHashPartitions().
  AtomicInt64 num_spilled_probe_rows_{0};
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
/// After this initial partitioning, the join node probes the in-memory hash partitions,
/// after which it calls DoneProbingHashPartitions(). Then the spilling algorithm can
/// commence, with the join node calling BeginSpilledProbe() and DoneProbing*() methods
/// until all spilled partitions are processed.
///
/// Both the PartitionedHashJoinNode and the builder share memory reservation. Different
/// stages of the spilling algorithm require different mixes of build and probe buffers
/// and hash tables, so we can share the reservation to minimize the combined memory
/// requirement. Memory for probe-side buffers is reserved in the builder then handed
/// off to the probe side to implement this reservation sharing. When the builder is
/// integrated into the join node, this is implemented with a shared BufferPool client.
/// When the build is separate, reservation is transferred between the builder's and the
/// join node's clients as needed. The probe client is passed into various methods as
/// 'probe_client'. If the join is integrated, 'probe_client' must be the same client
/// as was passed into the constructor. If the join is separate, 'probe_client' must
/// be a different client.
///
/// The full hash join algorithm is documented in PartitionedHashJoinNode.
///
/// Shared Build
/// ------------
/// A separate builder can be shared between multiple PartitionedHashJoinNodes. The
/// spilling hash join algorithm mutates the state of the builder between phases, so
/// requires synchronization between the probe threads executing PartitionedHashJoinNode
/// that are reading that state.
///
/// The algorithm (specifically the HashJoinState state machine) is executed in lockstep
/// across all probe threads with each probe thread working on the same set of partitions
/// at the same time. A CyclicBarrier, 'probe_barrier_', is used for synchronization.
/// At each state transition where the builder state needs to be mutated, all probe
/// threads must arrive at the barrier before proceeding. The state transition is executed
/// serially by a single thread before all threads proceed. All probe threads go through
/// the same state transitions in lockstep, even if they have no work to do. E.g. if a
/// probe thread has zero rows remaining in its spilled partitions, it still needs to
/// wait for the other probe threads.
///
/// Not all join ops can be used with a shared build. For example, RIGHT_OUTER_JOIN is
/// not supported currently, in part because it mutates the hash table during probing to
/// track matches, but also because hash table matches would need to be broadcast across
/// all instances within the query, not just the backend.
class PhjBuilder : public JoinBuilder {
 public:
  friend class PhjBuilderPartition;

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

  using PartitionId = int;

  // Constructor for separate join build.
  PhjBuilder(
      TDataSinkId sink_id, const PhjBuilderConfig& sink_config, RuntimeState* state);
  // Constructor for join builder embedded in a PartitionedHashJoinNode. Shares
  // 'buffer_pool_client' with the parent node and inherits buffer sizes from
  // the parent node.
  PhjBuilder(const PhjBuilderConfig& sink_config,
      BufferPool::ClientHandle* buffer_pool_client, int64_t spillable_buffer_size,
      int64_t max_row_buffer_size, RuntimeState* state);
  ~PhjBuilder();

  /// Implementations of DataSink interface methods.
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status Send(RuntimeState* state, RowBatch* batch) override;
  virtual Status FlushFinal(RuntimeState* state) override;
  virtual void Close(RuntimeState* state) override;

  /////////////////////////////////////////
  // The following functions are used only by PartitionedHashJoinNode.
  /////////////////////////////////////////

  /// Reset the builder to the same state as it was in after calling Open().
  /// Not valid to call on a separate join build.
  void Reset(RowBatch* row_batch);

  /// Represents a set of hash partitions to be handed off to the probe side.
  struct HashPartitions {
    HashPartitions() { Reset(); }
    HashPartitions(int level,
        const std::vector<std::unique_ptr<PhjBuilderPartition>>* hash_partitions,
        bool non_empty_build)
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
    const std::vector<std::unique_ptr<PhjBuilderPartition>>* hash_partitions;

    // True iff the build side had at least one row in a partition.
    bool non_empty_build;
  };

  /// Get hash partitions and reservation for the initial partitioning of the probe
  /// side. Only valid to call once per PartitionedHashJoinNode when in state
  /// PARTITIONING_PROBE (i.e. once if the build is not shared).
  /// When this function returns successfully, 'probe_client' will have enough
  /// reservation for a write buffer for each spilled partition.
  /// Return the current set of hash partitions in 'partitions'.
  Status BeginInitialProbe(
      BufferPool::ClientHandle* probe_client, HashPartitions* partitions);

  /// Pick a spilled partition to process (returned in *input_partition) and
  /// prepare to probe it. Builds a hash table over *input_partition
  /// if it fits in memory. Otherwise repartition it into PARTITION_FANOUT
  /// new partitions.
  ///
  /// When this function returns successfully, 'probe_client' will have enough
  /// reservation for a read buffer for the input probe stream and, if repartitioning,
  /// a write buffer for each spilled partition.
  ///
  /// If repartitioning, creates new hash partitions and repartitions 'partition' into
  /// PARTITION_FANOUT new partitions with level input_partition->level() + 1. The
  /// previous hash partitions must have been cleared with DoneProbingHashPartitions().
  /// The new hash partitions are returned in 'new_partitions'.
  ///
  /// This is a synchronization point for shared join build. The time elapsed during the
  /// serial execution phase is attributed to the builder. All probe threads must call
  /// this function before continuing the next phase of the hash join algorithm.
  Status BeginSpilledProbe(BufferPool::ClientHandle* probe_client,
      RuntimeProfile* probe_profile, bool* repartitioned,
      PhjBuilderPartition** input_partition, HashPartitions* new_partitions);

  /// Called after probing of the hash partitions returned by BeginInitialProbe() or
  /// BeginSpilledProbe() (when *repartitioned is true) is complete, i.e. all of the
  /// corresponding probe rows have been processed by PartitionedHashJoinNode. The number
  /// of spilled probe rows per partition must be passed in via 'num_spilled_probe_rows'
  /// so that the builder can determine whether a spilled partition needs to be retained.
  /// Appends in-memory partitions that may contain build rows to output to
  /// 'output_partitions' for build modes like right outer join that output unmatched
  /// rows. Close other in-memory partitions, attaching any tuple data to 'batch' if
  /// 'batch' is non-NULL. Closes spilled partitions if no more processing is needed.
  ///
  /// The reservation that was transferred to 'probe_client' in Begin*Probe() is
  /// transferred back to the builder.
  ///
  /// Returns an error if an error was encountered or if the query was cancelled.
  ///
  /// This is a synchronization point for shared join build. The time elapsed during the
  /// serial execution phase is attributed to the builder. All probe threads must call
  /// this function before continuing the next phase of the hash join algorithm.
  Status DoneProbingHashPartitions(const int64_t num_spilled_probe_rows[PARTITION_FANOUT],
      BufferPool::ClientHandle* probe_client, RuntimeProfile* probe_profile,
      std::deque<std::unique_ptr<PhjBuilderPartition>>* output_partitions,
      RowBatch* batch);

  /// Called after probing of a single spilled partition returned by
  /// BeginSpilledProbe() when *repartitioned is false.
  ///
  /// If the join op requires outputting unmatched build rows and the partition
  /// may have build rows to return, it is appended to 'output_partitions'. Partitions
  /// returned via 'output_partitions' are ready for the caller to read from - either
  /// they are in-memory with a hash table built or have build_rows() prepared for
  /// reading.
  ///
  /// If no build rows need to be returned, closes the build partition and attaches any
  /// tuple data to 'batch' if 'batch' is non-NULL.
  ///
  /// The reservation that was transferred to 'probe_client' in Begin*Probe() is
  /// transferred back to the builder.
  ///
  /// Returns an error if an error was encountered or if the query was cancelled.
  ///
  /// This is a synchronization point for shared join build. The time elapsed during the
  /// serial execution phase is attributed to the builder. All probe threads must call
  /// this function before continuing the next phase of the hash join algorithm.
  Status DoneProbingSinglePartition(BufferPool::ClientHandle* probe_client,
      RuntimeProfile* probe_profile,
      std::deque<std::unique_ptr<PhjBuilderPartition>>* output_partitions,
      RowBatch* batch);

  /// Called to begin probing of the null-aware partition, after all other partitions
  /// have been fully processed. This should only be called if there are build rows in the
  /// null-aware partition. This pins the null-aware build rows in memory and allows all
  /// probe threads to access those rows in a read-only manner.
  ///
  /// Returns an error if an error was encountered or if the query was cancelled.
  ///
  /// This is a synchronization point for shared join build. All probe threads must
  /// call this function before continuing the next phase of the hash join algorithm.
  Status BeginNullAwareProbe();

  /// Called after probing of the null-aware build partition is complete.
  ///
  /// This is a synchronization point for shared join build. All probe threads must
  /// call this function before continuing the next phase of the hash join algorithm.
  Status DoneProbingNullAwarePartition();

  /// True if the hash table may contain rows with one or more NULL join keys. This
  /// depends on the join type, passed in via 'join_op' and the 'is_not_distinct_from'
  /// flags of the equijoin conjuncts, which are passed in via 'is_not_distinct_from'.
  static bool HashTableStoresNulls(
      TJoinOp::type join_op, const std::vector<bool>& is_not_distinct_from);

  /// Returns 'bytes' of reservation to the builder from 'probe_client'.
  /// Called by the probe side to return surplus reservation. This is usually handled by
  /// the above methods, but if an error occured during execution, the probe may still
  /// have some surplus reservation.
  /// Must only be called if this is a separate build.
  void ReturnReservation(BufferPool::ClientHandle* probe_client, int64_t bytes);

  /// Safe to call from PartitionedHashJoinNode threads during the probe phase.
  HashJoinState state() const { return state_; }

  /// Accessor to allow PartitionedHashJoinNode to access 'null_aware_partition_'.
  /// Generally the PartitionedHashJoinNode should only access this partition in
  /// a read-only manner.
  inline PhjBuilderPartition* null_aware_partition() const {
    return null_aware_partition_.get();
  }

  /// Thread-safe.
  HashTableStatsProfile* ht_stats_profile() const { return ht_stats_profile_.get(); }

  std::string DebugString() const;

  /// Unregisters one probe thread from the barrier
  void UnregisterThreadFromBarrier() const;

  /// Computes the minimum reservation required to execute the spilling partitioned
  /// hash algorithm successfully for any input size (assuming enough disk space is
  /// available for spilled rows). This includes buffers used by the build side,
  /// the probe side, and buffers that are shared between build and probe.
  /// We need one output buffer per partition to partition the build or probe side. We
  /// need one additional buffer for the input while repartitioning the build or probe.
  /// For NAAJ, we need an additional buffer for 'null_aware_partition_' on the build
  /// side and two additional buffers for 'null_aware_probe_partition_' and
  /// 'null_probe_rows_' on the probe side.
  /// Returns a pair with the probe and build reservation requirements.
  std::pair<int64_t, int64_t> MinReservation() const {
    // Must be kept in sync with HashJoinNode.computeNodeResourceProfile() in fe.
    int num_reserved_build_buffers = PARTITION_FANOUT + 1;
    int64_t probe_reservation = 0;
    if (join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
      num_reserved_build_buffers += 1;
      // One of the NAAJ buffers needs to fit the max row, since we write/read
      // one stream at a time. If the build is integrated, we already have a max-sized
      // buffer accounted for in the build reservation.
      probe_reservation = is_separate_build_ ?
        max_row_buffer_size_ + spillable_buffer_size_ : spillable_buffer_size_ * 2;
    }
    // Two of the build buffers must fit the maximum row for use as read and write
    // buffers while repartitioning a stream.
    return {probe_reservation,
        spillable_buffer_size_ * (num_reserved_build_buffers - 2)
            + max_row_buffer_size_ * 2};
  }

  /// Class name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 private:
  /// Updates 'state_' to 'next_state', logging the transition.
  void UpdateState(HashJoinState next_state);

  /// Returns the string represenvation of 'state'.
  static std::string PrintState(HashJoinState state);

  /// Create and initialize a set of hash partitions for partitioning level 'level'.
  /// The previous hash partitions must have been cleared with DoneProbing().
  /// After calling this, batches are added to the new partitions by calling Send().
  Status CreateHashPartitions(int level) WARN_UNUSED_RESULT;

  /// Create a new partition and prepare it for writing. Returns an error if initializing
  /// the partition or allocating the write buffer fails.
  Status CreateAndPreparePartition(int level,
      std::unique_ptr<PhjBuilderPartition>* partition);

  /// Reads the rows in build_batch and partitions them into hash_partitions_. If
  /// 'build_filters' is true, runtime filters are populated. 'is_null_aware' is
  /// set to true if the join type is a null aware join.
  Status ProcessBuildBatch(
      RowBatch* build_batch, HashTableCtx* ctx, bool build_filters, bool is_null_aware);

  /// Helper method for Send() that that does the actual work apart from updating the
  /// counters. Also used by RepartitionBuildInput().
  Status AddBatch(RowBatch* build_batch);

  /// Helper method for FlushFinal() that does the actual work. Also used by
  /// RepartitionBuildInput().
  Status FinalizeBuild(RuntimeState* state);

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
      PhjBuilderPartition** spilled_partition = nullptr) WARN_UNUSED_RESULT;

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
  ///
  /// 'next_state' is the state that this will transition into after building the hash
  /// tables, either PARTITIONING_PROBE or REPARTITIONING_PROBE.
  Status BuildHashTablesAndReserveProbeBuffers(HashJoinState next_state);

  /// Ensures that 'probe_stream_reservation_' has enough reservation for a stream per
  /// spilled partition in 'hash_partitions_', plus for the input stream if the input
  /// is a spilled partition (determined by 'next_state' - either PARTITIONING_PROBE or
  /// REPARTITIONING_PROBE). If num_probe_threads_ is > 1, reserves this amount for each
  /// probe thread. May spill additional partitions until it can free enough
  /// reservation. Returns an error if an error is encountered or if it runs out of
  /// partitions to spill.
  Status ReserveProbeBuffers(HashJoinState next_state);

  /// Returns the number of partitions in 'partitions' that are spilled.
  static int GetNumSpilledPartitions(
      const std::vector<std::unique_ptr<PhjBuilderPartition>>& partitions);

  /// Transfer reservation for probe streams to 'probe_client'. Memory for one stream was
  /// reserved per spilled partition in FlushFinal(), plus the input stream if the input
  /// partition was spilled.
  /// This is safe to call from multiple probe threads concurrently.
  Status TransferProbeStreamReservation(BufferPool::ClientHandle* probe_client);

  /// Calculates the amount of memory per probe thread/join node instance to be
  /// transferred for probe streams when probing in the given 'state'. Depends on
  /// 'hash_partitions_', 'spilled_partitions_' and 'spillable_buffer_size_'.
  int64_t CalcProbeStreamReservation(HashJoinState state) const;

  /// The serial part of BeginSpilledProbe() that is executed by a single thread.
  Status BeginSpilledProbeSerial();

  /// Creates new hash partitions and repartitions 'input_partition' into PARTITION_FANOUT
  /// new partitions with level input_partition->level() + 1. The previous hash partitions
  /// must have been cleared with ClearHashPartitions(). This function reserves enough
  /// memory for a read buffer for the input probe stream and a write buffer for each
  /// spilled partition after repartitioning.
  Status RepartitionBuildInput(PhjBuilderPartition* input_partition) WARN_UNUSED_RESULT;

  /// Returns the largest build row count out of the current hash partitions.
  int64_t LargestPartitionRows() const;

  /// Helper for DoneProbingHashPartitions() that processes and cleans up the hash
  /// partitions.
  void CleanUpHashPartitions(
      std::deque<std::unique_ptr<PhjBuilderPartition>>* output_partitions,
      RowBatch* batch);

  /// Helper for DoneProbingSinglePartition() that processes and cleans up the current
  /// spilled partition.
  void CleanUpSinglePartition(
      std::deque<std::unique_ptr<PhjBuilderPartition>>* output_partitions,
      RowBatch* batch);

  /// The serial part of BeginNullAwareProbe() that is executed by a single thread.
  Status BeginNullAwareProbeSerial();

  /// Close the null aware partition (if there is one) and set it to NULL.
  void CloseNullAwarePartition();

  /// Calls Close() on every Partition, deletes them, and cleans up any pointers that
  /// may reference them. If 'row_batch' if not NULL, transfers the ownership of all
  /// row-backing resources to it.
  void CloseAndDeletePartitions(RowBatch* row_batch);

  /// For each filter in filters_, allocate a runtime_filter from the fragment-local
  /// RuntimeFilterBank and store it in runtime_filters_ to populate during the build
  /// phase.
  void AllocateRuntimeFilters();

  /// Iterates over the runtime filters and inserts each row into each filter.
  /// This is replaced at runtime with code generated by CodegenInsertRuntimeFilters().
  void InsertRuntimeFilters(FilterContext filter_ctxs[], TupleRow* build_row) noexcept;

  /// Publish the runtime filters to the fragment-local RuntimeFilterBank.
  /// 'num_build_rows' is used to determine whether the computed filters have an
  /// unacceptably high false-positive rate.
  void PublishRuntimeFilters(int64_t num_build_rows);

  // Determine the usefulness of min/max filters in the context of column min/max stats.
  // Set AlwaysTrue to true for each not useful. Called at the end of AddBatch().
  void DetermineUsefulnessForMinmaxFilters();

  RuntimeState* const runtime_state_;

  /// Seed used for hashing rows. Must match seed used in the PartitionedHashJoinNode.
  const uint32_t hash_seed_;

  /// Pool for objects with same lifetime as builder.
  ObjectPool obj_pool_;

  /// Resource information sent from the frontend. Non-null if this is a separate join
  /// build.
  const TBackendResourceProfile* const resource_profile_;

  /// Wraps the buffer pool client. Only used if this is a separate build sink. The node's
  /// minimum reservation is claimed in Open(). After this, the client must hold onto
  /// at least the minimum reservation so that it can be returned to the initial
  /// reservations pool in Close().
  ReservationManager reservation_manager_;

  /// Client to the buffer pool, used to allocate build partition buffers and hash tables.
  /// When probing, the spilling algorithm keeps some build partitions in memory while
  /// using memory for probe buffers for spilled partitions.
  /// Memory is shared between build and probe in different ways, depending on whether
  /// this is a separate join build (i.e. 'is_separate_build_' is true). If a separate
  /// build, this builder has its own buffer pool client, and transfer reservation to
  /// the probe client when needed. If the builder is embedded in the join node, this
  /// is just a pointer to the join node's client so no transfer is required.
  BufferPool::ClientHandle* buffer_pool_client_;

  /// The default and max buffer sizes to use in the build streams.
  const int64_t spillable_buffer_size_;
  const int64_t max_row_buffer_size_;

  /// Allocator for hash table memory.
  boost::scoped_ptr<Suballocator> ht_allocator_;

  /// Expressions over input rows for hash table build.
  const std::vector<ScalarExpr*>& build_exprs_;

  /// is_not_distinct_from_[i] is true if and only if the ith equi-join predicate is IS
  /// NOT DISTINCT FROM, rather than equality.
  std::vector<bool> is_not_distinct_from_;

  /// Expressions for evaluating input rows for insertion into runtime filters.
  /// Only includes exprs for filters produced by this builder.
  const std::vector<ScalarExpr*>& filter_exprs_;

  /// List of filters to build. One-to-one correspondence with exprs in 'filter_exprs_'.
  std::vector<FilterContext> filter_ctxs_;

  /// Separately cached list of min/max filter contexts populated during
  /// PhjBuilder::AllocateRuntimeFilters() to speed up
  /// PhjBuilder::DetermineUsefulnessForMinmaxFilters() where only minmax filters are
  /// relevant. Contexts in the vector are removed if they are determined to host
  /// min/max filters that are overlapping with column stats too much.
  std::vector<FilterContext*> minmax_filter_ctxs_;

  /// Reference to the hash table config which is a part of the PhjBuilderConfig that was
  /// used to create this object. Its used to create an instance of the HashTableCtx in
  /// Prepare(). Not Owned.
  const HashTableConfig& hash_table_config_;

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

  // Barrier used to synchronize the probe-side threads at synchronization points in the
  // partitioned hash join algorithm. Used only when 'num_probe_threads_' > 1.
  std::unique_ptr<CyclicBarrier> probe_barrier_;

  /// Cached copy of the min/max filter threshold value to avoid repeated fetch of
  /// the value from the plan. It remains a constant through out the execution of
  /// the query.
  float minmax_filter_threshold_ = 0.0;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// State of the partitioned hash join algorithm. See HashJoinState for more
  /// information.
  HashJoinState state_ = HashJoinState::PARTITIONING_BUILD;

  /// If true, the build side has at least one row.
  /// Set in FlushFinal() and not modified until Reset().
  bool non_empty_build_ = false;

  /// Id to assign to the next partition created.
  PartitionId next_partition_id_ = 0;

  /// The current set of partitions that are being built or probed. This vector is
  /// initialized before partitioning or re-partitioning the build input
  /// and cleared after we've finished probing the partitions.
  /// This is not used when processing a single spilled partition.
  std::vector<std::unique_ptr<PhjBuilderPartition>> hash_partitions_;

  /// Spilled partitions that need further processing. Populated in
  /// DoneProbingHashPartitions() with the spilled hash partitions.
  ///
  /// This is used as a stack to do a depth-first walk of spilled partitions (i.e. more
  /// finely partitioned partitions are processed first). This allows us to delete spilled
  /// data and bottom out the recursion earlier.
  ///
  /// spilled_partitions_.back() is the spilled partition being processed, if one is
  /// currently being processed (i.e. between BeginSpilledProbe() and the corresponding
  /// DoneProbing*() call).
  std::vector<std::unique_ptr<PhjBuilderPartition>> spilled_partitions_;

  /// Partition used for null-aware joins. This partition is always processed at the end
  /// after all build and probe rows are processed. In this partition's 'build_rows_', we
  /// store all the rows for which 'build_expr_evals_' evaluated over the row returns
  /// NULL (i.e. it has a NULL on the eq join slot).
  /// NULL if the join is not null aware or we are done processing this partition.
  /// This partition starts off in memory but can be spilled.
  std::unique_ptr<PhjBuilderPartition> null_aware_partition_;

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

  /// Jitted ProcessBuildBatch function pointers. NULL if codegen is disabled.
  const CodegenFnPtr<PhjBuilderConfig::ProcessBuildBatchFn>& process_build_batch_fn_;
  const CodegenFnPtr<PhjBuilderConfig::ProcessBuildBatchFn>&
      process_build_batch_fn_level0_;

  /// Jitted Partition::InsertBatch() function pointers. NULL if codegen is disabled.
  const CodegenFnPtr<InsertBatchFn>& insert_batch_fn_;
  const CodegenFnPtr<InsertBatchFn>& insert_batch_fn_level0_;
};
} // namespace impala
#endif
