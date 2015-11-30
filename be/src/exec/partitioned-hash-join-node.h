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


#ifndef IMPALA_EXEC_PARTITIONED_HASH_JOIN_NODE_H
#define IMPALA_EXEC_PARTITIONED_HASH_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>
#include <boost/unordered_set.hpp>
#include <boost/thread.hpp>
#include <string>

#include "exec/blocking-join-node.h"
#include "exec/exec-node.h"
#include "exec/filter-context.h"
#include "exec/hash-table.h"
#include "runtime/buffered-block-mgr.h"

#include "gen-cpp/PlanNodes_types.h"  // for TJoinOp

namespace impala {

class BloomFilter;
class BufferedBlockMgr;
class MemPool;
class RowBatch;
class RuntimeFilter;
class TupleRow;
class BufferedTupleStream;

/// Operator to perform partitioned hash join, spilling to disk as necessary.
/// A spilled partition is one that is not fully pinned.
/// The operator runs in these distinct phases:
///  1. Consume all build input and partition them. No hash tables are maintained.
///  2. Construct hash tables from as many partitions as possible.
///  3. Consume all the probe rows. Rows belonging to partitions that are spilled
///     must be spilled as well.
///  4. Iterate over the spilled partitions, construct the hash table from the spilled
///     build rows and process the spilled probe rows. If the partition is still too
///     big, repeat steps 1-4, using this spilled partitions build and probe rows as
///     input.
//
/// TODO: don't copy tuple rows so often.
/// TODO: we need multiple hash functions. Each repartition needs new hash functions
/// or new bits. Multiplicative hashing?
/// TODO: think about details about multithreading. Multiple partitions in parallel?
/// Multiple threads against a single partition? How to build hash tables in parallel?
/// TODO: BuildHashTables() should start with the partitions that are already pinned.
class PartitionedHashJoinNode : public BlockingJoinNode {
 public:
  PartitionedHashJoinNode(ObjectPool* pool, const TPlanNode& tnode,
      const DescriptorTbl& descs);
  virtual ~PartitionedHashJoinNode();

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

 protected:
  virtual void AddToDebugString(int indentation_level, std::stringstream* out) const;
  virtual Status InitGetNext(TupleRow* first_probe_row);
  virtual Status ConstructBuildSide(RuntimeState* state);

 private:
  class Partition;

  /// Implementation details:
  /// Logically, the algorithm runs in three modes.
  ///   1. [PARTITIONING_BUILD or REPARTITIONING] Read the build side rows and partition
  ///      them into hash_partitions_. This is a fixed fan out of the input. The input
  ///      can either come from child(1) OR from the build tuple stream of partition
  ///      that needs to be repartitioned.
  ///   2. [PROCESSING_PROBE or REPARTITIONING] Read the probe side rows, partition them
  ///      and either perform the join or spill them into hash_partitions_. If the
  ///      partition has the hash table in memory, we perform the join, otherwise we
  ///      spill the probe row. Similar to step one, the rows can come from child(0) or
  ///      a spilled partition.
  ///   3. [PROBING_SPILLED_PARTITION] Read and construct a single spilled partition.
  ///      In this case we are walking a spilled partition and the hash table fits in
  ///      memory. Neither the build nor probe side need to be partitioned and we just
  ///      perform the join.
  ///
  /// States:
  /// The transition goes from PARTITIONING_BUILD -> PROCESSING_PROBE ->
  ///    PROBING_SPILLED_PARTITION/REPARTITIONING.
  /// The last two steps will switch back and forth as many times as we need to
  /// repartition.
  enum HashJoinState {
    /// Partitioning the build (right) child's input. Corresponds to mode 1 above but
    /// only when consuming from child(1).
    PARTITIONING_BUILD,

    /// Processing the probe (left) child's input. Corresponds to mode 2 above but
    /// only when consuming from child(0).
    PROCESSING_PROBE,

    /// Probing a spilled partition. The hash table for this partition fits in memory.
    /// Corresponds to mode 3.
    PROBING_SPILLED_PARTITION,

    /// Repartitioning a single spilled partition (input_partition_) into
    /// hash_partitions_.
    /// Corresponds to mode 1 & 2 but reading from a spilled partition.
    REPARTITIONING,
  };

  /// Number of initial partitions to create. Must be a power of two.
  /// TODO: this is set to a lower than actual value for testing.
  static const int PARTITION_FANOUT = 16;

  /// Needs to be the log(PARTITION_FANOUT)
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

  /// Append the row to stream. In the common case, the row is just in memory and the
  /// append succeeds. If the append fails, we fallback to the slower path of
  /// AppendRowStreamFull().
  /// Returns true if the row was added and false otherwise. If false is returned,
  /// *status contains the error (doesn't return status because this is very perf
  /// sensitive).
  bool AppendRow(BufferedTupleStream* stream, TupleRow* row, Status* status);

  /// Slow path for AppendRow() above. It is called when the stream has failed to append
  /// the row. We need to find more memory by either switching to IO-buffers, in case the
  /// stream still uses small buffers, or spilling a partition.
  bool AppendRowStreamFull(BufferedTupleStream* stream, TupleRow* row, Status* status);

  /// Called when we need to free up memory by spilling a partition.
  /// This function walks hash_partitions_ and picks one to spill.
  /// *spilled_partition is the partition that was spilled.
  /// Returns non-ok status if we couldn't spill a partition.
  Status SpillPartition(Partition** spilled_partition);

  /// Partitions the entire build input (either from child(1) or input_partition_) into
  /// hash_partitions_. When this call returns, hash_partitions_ is ready to consume
  /// the probe input.
  /// 'level' is the level new partitions (in hash_partitions_) should be created with.
  Status ProcessBuildInput(RuntimeState* state, int level);

  /// Reads the rows in build_batch and partitions them in hash_partitions_.
  Status ProcessBuildBatch(RowBatch* build_batch);

  /// Call at the end of partitioning the build rows (which could be from the build child
  /// or from repartitioning an existing partition). After this function returns, all
  /// partitions in hash_partitions_ are ready to accept probe rows. This function
  /// constructs hash tables for as many partitions as fit in memory (which can be none).
  /// For the remaining partitions, this function initializes the probe spilling
  /// structures.
  Status BuildHashTables(RuntimeState* state);

  /// Process probe rows from probe_batch_. Returns either if out_batch is full or
  /// probe_batch_ is entirely consumed.
  /// For RIGHT_ANTI_JOIN, all this function does is to mark whether each build row
  /// had a match.
  /// Returns the number of rows added to out_batch; -1 on error (and *status will be
  /// set).
  template<int const JoinOp>
  int ProcessProbeBatch(RowBatch* out_batch, HashTableCtx* ht_ctx, Status* status);

  /// Wrapper that calls the templated version of ProcessProbeBatch() based on 'join_op'.
  int ProcessProbeBatch(const TJoinOp::type join_op, RowBatch* out_batch,
                        HashTableCtx* ht_ctx, Status* status);

  /// Sweep the hash_tbl_ of the partition that is at the front of
  /// output_build_partitions_, using hash_tbl_iterator_ and output any unmatched build
  /// rows. If reaches the end of the hash table it closes that partition, removes it from
  /// output_build_partitions_ and moves hash_tbl_iterator_ to the beginning of the
  /// new partition at the front of output_build_partitions_.
  void OutputUnmatchedBuild(RowBatch* out_batch);

  /// Initializes null_aware_partition_ and nulls_build_batch_ to output rows.
  Status PrepareNullAwarePartition();

  /// Continues processing from null_aware_partition_. Called after we have finished
  /// processing all build and probe input (including repartitioning them).
  Status OutputNullAwareProbeRows(RuntimeState* state, RowBatch* out_batch);

  /// Evaluates all other_join_conjuncts against null_probe_rows_ with all the
  /// rows in build. This updates matched_null_probe_, short-circuiting if one of the
  /// conjuncts pass (i.e. there is a match).
  /// This is used for NAAJ, when there are NULL probe rows.
  Status EvaluateNullProbe(BufferedTupleStream* build);

  /// Prepares to output NULLs on the probe side for NAAJ. Before calling this,
  /// matched_null_probe_ should have been fully evaluated.
  Status PrepareNullAwareNullProbe();

  /// Outputs NULLs on the probe side, returning rows where matched_null_probe_[i] is
  /// false. Used for NAAJ.
  Status OutputNullAwareNullProbe(RuntimeState* state, RowBatch* out_batch);

  /// Call at the end of consuming the probe rows. Walks hash_partitions_ and
  ///  - If this partition had a hash table, close it. This partition is fully processed
  ///    on both the build and probe sides. The streams are transferred to batch.
  ///    In the case of right-outer and full-outer joins, instead of closing this
  ///    partition we put it on a list of partitions for which we need to flush their
  ///    unmatched rows.
  ///  - If this partition did not have a hash table, meaning both sides were spilled,
  ///    move the partition to spilled_partitions_.
  Status CleanUpHashPartitions(RowBatch* batch);

  /// Get the next row batch from the probe (left) side (child(0)). If we are done
  /// consuming the input, sets probe_batch_pos_ to -1, otherwise, sets it to 0.
  Status NextProbeRowBatch(RuntimeState*, RowBatch* out_batch);

  /// Get the next probe row batch from input_partition_. If we are done consuming the
  /// input, sets probe_batch_pos_ to -1, otherwise, sets it to 0.
  Status NextSpilledProbeRowBatch(RuntimeState*, RowBatch* out_batch);

  /// Moves onto the next spilled partition and initializes input_partition_. This
  /// function processes the entire build side of input_partition_ and when this function
  /// returns, we are ready to consume the probe side of input_partition_.
  /// If the build side's hash table fits in memory, we will construct input_partition_'s
  /// hash table. If it does not, meaning we need to repartition, this function will
  /// initialize hash_partitions_.
  Status PrepareNextPartition(RuntimeState*);

  /// Iterates over all the partitions in hash_partitions_ and returns the number of rows
  /// of the largest partition (in terms of number of build and probe rows).
  int64_t LargestSpilledPartition() const;

  /// Calls Close() on every Partition in 'hash_partitions_',
  /// 'spilled_partitions_', and 'output_build_partitions_' and then resets the lists,
  /// the vector and the partition pool.
  void ClosePartitions();

  /// Prepares for probing the next batch.
  void ResetForProbe();

  /// For each filter in filters_, allocate a bloom_filter from the fragment-local
  /// RuntimeFilterBank and store it in runtime_filters_ to populate during the build
  /// phase. Returns false if filter construction is disabled.
  bool AllocateRuntimeFilters(RuntimeState* state);

  /// Publish the runtime filters to the fragment-local RuntimeFilterBank.
  bool PublishRuntimeFilters(RuntimeState* state);

  /// Codegen function to create output row. Assumes that the probe row is non-NULL.
  Status CodegenCreateOutputRow(LlvmCodeGen* codegen, llvm::Function** fn);

  /// Codegen processing build batches.  Identical signature to ProcessBuildBatch.
  /// Returns non-OK status if codegen was not possible.
  Status CodegenProcessBuildBatch(
      RuntimeState* state, llvm::Function* hash_fn, llvm::Function* murmur_hash_fn);

  /// Codegen processing probe batches.  Identical signature to ProcessProbeBatch.
  /// Returns non-OK if codegen was not possible.
  Status CodegenProcessProbeBatch(
      RuntimeState* state, llvm::Function* hash_fn, llvm::Function* murmur_hash_fn);

  /// Returns the current state of the partition as a string.
  std::string PrintState() const;

  /// Updates state_ to 's', logging the transition.
  void UpdateState(HashJoinState s);

  std::string NodeDebugString() const;

  /// We need two output buffers per partition (one for build and one for probe) and
  /// and two additional buffers for the input (while repartitioning; for the build and
  /// probe sides).
  /// For NAAJ, we need 3 additional buffers to maintain the null_aware_partition_.
  int MinRequiredBuffers() const {
    int num_reserved_buffers = PARTITION_FANOUT * 2 + 2;
    num_reserved_buffers += join_op_ == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ? 3 : 0;
    return num_reserved_buffers;
  }

  RuntimeState* runtime_state_;

  /// Our equi-join predicates "<lhs> = <rhs>" are separated into
  /// build_expr_ctxs_ (over child(1)) and probe_expr_ctxs_ (over child(0))
  std::vector<ExprContext*> probe_expr_ctxs_;
  std::vector<ExprContext*> build_expr_ctxs_;

  /// List of filters to build during build phase.
  std::vector<FilterContext> filters_;

  /// is_not_distinct_from_[i] is true if and only if the ith equi-join predicate is IS
  /// NOT DISTINCT FROM, rather than equality.
  std::vector<bool> is_not_distinct_from_;

  /// Non-equi-join conjuncts from the ON clause.
  std::vector<ExprContext*> other_join_conjunct_ctxs_;

  /// Codegen doesn't allow for automatic Status variables because then exception
  /// handling code is needed to destruct the Status, and our function call substitution
  /// doesn't know how to deal with the LLVM IR 'invoke' instruction. Workaround that by
  /// placing the build-side status here so exceptions won't need to destruct it.
  /// This status should used directly only by ProcesssBuildBatch().
  /// TODO: fix IMPALA-1948 and remove this.
  Status buildStatus_;

  /// Client to the buffered block mgr.
  BufferedBlockMgr::Client* block_mgr_client_;

  /// Used for hash-related functionality, such as evaluating rows and calculating hashes.
  /// TODO: If we want to multi-thread then this context should be thread-local and not
  /// associated with the node.
  boost::scoped_ptr<HashTableCtx> ht_ctx_;

  /// The iterator that corresponds to the look up of current_probe_row_.
  HashTable::Iterator hash_tbl_iterator_;

  /// Total time spent partitioning build.
  RuntimeProfile::Counter* partition_build_timer_;

  /// Total number of hash buckets across all partitions.
  RuntimeProfile::Counter* num_hash_buckets_;

  /// Total number of partitions created.
  RuntimeProfile::Counter* partitions_created_;

  /// Level of max partition (i.e. number of repartitioning steps).
  RuntimeProfile::HighWaterMarkCounter* max_partition_level_;

  /// Number of build/probe rows that have been partitioned.
  RuntimeProfile::Counter* num_build_rows_partitioned_;
  RuntimeProfile::Counter* num_probe_rows_partitioned_;

  /// Number of partitions that have been repartitioned.
  RuntimeProfile::Counter* num_repartitions_;

  /// Number of partitions that have been spilled.
  RuntimeProfile::Counter* num_spilled_partitions_;

  /// The largest fraction (of build side) after repartitioning. This is expected to be
  /// 1 / PARTITION_FANOUT. A value much larger indicates skew.
  RuntimeProfile::HighWaterMarkCounter* largest_partition_percent_;

  /// Number of hash collisions - unequal rows that have identical hash values
  RuntimeProfile::Counter* num_hash_collisions_;

  /// Time spent evaluating other_join_conjuncts for NAAJ.
  RuntimeProfile::Counter* null_aware_eval_timer_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// State of the partitioned hash join algorithm. Used just for debugging.
  HashJoinState state_;

  /// Object pool that holds the Partition objects in hash_partitions_.
  boost::scoped_ptr<ObjectPool> partition_pool_;

  /// The current set of partitions that are being built. This is only used in
  /// mode 1 and 2 when we need to partition the build and probe inputs.
  /// This is not used when processing a single partition.
  /// After CleanUpHashPartitions() is called this vector should be empty.
  std::vector<Partition*> hash_partitions_;

  /// The list of partitions that have been spilled on both sides and still need more
  /// processing. These partitions could need repartitioning, in which case more
  /// partitions will be added to this list after repartitioning.
  /// This list is populated at CleanUpHashPartitions().
  std::list<Partition*> spilled_partitions_;

  /// Cache of the per partition hash table to speed up ProcessProbeBatch.
  /// In the case where we need to partition the probe:
  ///  hash_tbls_[i] = hash_partitions_[i]->hash_tbl();
  /// In the case where we don't need to partition the probe:
  ///  hash_tbls_[i] = input_partition_->hash_tbl();
  HashTable* hash_tbls_[PARTITION_FANOUT];

  /// The current input partition to be processed (not in spilled_partitions_).
  /// This partition can either serve as the source for a repartitioning step, or
  /// if the hash table fits in memory, the source of the probe rows.
  Partition* input_partition_;

  /// In the case of right-outer and full-outer joins, this is the list of the partitions
  /// for which we need to output their unmatched build rows.
  /// This list is populated at CleanUpHashPartitions().
  std::list<Partition*> output_build_partitions_;

  /// Partition used if null_aware_ is set. This partition is always processed at the end
  /// after all build and probe rows are processed. Rows are added to this partition along
  /// the way.
  /// In this partition's build_rows_, we store all the rows for which build_expr_ctxs_
  /// evaluated over the row returns NULL (i.e. it has a NULL on the eq join slot).
  /// In this partition's probe_rows, we store all probe rows that did not have a match
  /// in the hash table.
  /// At the very end, we then iterate over all the probe rows. For each probe row, we
  /// return the rows that did not match any of the build rows.
  /// NULL if we this join is not null aware or we are done processing this partition.
  Partition* null_aware_partition_;

  /// Used while processing null_aware_partition_. It contains all the build tuple rows
  /// with a NULL when evaluating the hash table expr.
  boost::scoped_ptr<RowBatch> nulls_build_batch_;

  /// If true, the build side has at least one row.
  bool non_empty_build_;

  /// For NAAJ, this stream contains all probe rows that had NULL on the hash table
  /// conjuncts.
  BufferedTupleStream* null_probe_rows_;

  /// For each row in null_probe_rows_, true if this row has matched any build row
  /// (i.e. the resulting joined row passes other_join_conjuncts).
  /// TODO: remove this. We need to be able to put these bits inside the tuple itself.
  std::vector<bool> matched_null_probe_;

  /// The current index into null_probe_rows_/matched_null_probe_ that we are
  /// outputting.
  int64_t null_probe_output_idx_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  class Partition {
   public:
    Partition(RuntimeState* state, PartitionedHashJoinNode* parent, int level);
    ~Partition();

    BufferedTupleStream* build_rows() { return build_rows_; }
    BufferedTupleStream* probe_rows() { return probe_rows_; }
    HashTable* hash_tbl() const { return hash_tbl_.get(); }

    bool is_closed() const { return is_closed_; }
    bool is_spilled() const { return is_spilled_; }

    /// Must be called once per partition to release any resources. This should be called
    /// as soon as possible to release memory.
    /// If batch is non-null, the build and probe streams are attached to the batch,
    /// transferring ownership to them.
    void Close(RowBatch* batch);

    /// Returns the estimated size of the in memory size for the build side of this
    /// partition. This includes the entire build side and the hash table.
    int64_t EstimatedInMemSize() const;

    /// Returns the actual size of the in memory build side. Only valid to call on
    /// partitions after BuildPartition()
    int64_t InMemSize() const;

    /// Pins the build tuples for this partition and constructs the hash_tbl_ from it.
    /// Build rows cannot be added after calling this.
    /// If the partition could not be built due to memory pressure, *built is set to false
    /// and the caller is responsible for spilling this partition.
    /// If 'BUILD_RUNTIME_FILTERS' is set, populates runtime filters.
    template<bool const BUILD_RUNTIME_FILTERS>
    Status BuildHashTableInternal(RuntimeState* state, bool* built);

    /// Wrapper for the template-based BuildHashTable() based on 'add_runtime_filters'.
    Status BuildHashTable(RuntimeState* state, bool* built,
        const bool add_runtime_filters);

    /// Spills this partition, cleaning up and unpinning blocks.
    /// If 'unpin_all_build' is true, the build stream is completely unpinned, otherwise,
    /// it is unpinned with one buffer remaining.
    Status Spill(bool unpin_all_build);

   private:
    friend class PartitionedHashJoinNode;

    PartitionedHashJoinNode* parent_;

    /// This partition is completely processed and nothing needs to be done for it again.
    /// All resources associated with this partition are returned.
    bool is_closed_;

    /// True if this partition is spilled.
    bool is_spilled_;

    /// How many times rows in this partition have been repartitioned. Partitions created
    /// from the node's children's input is level 0, 1 after the first repartitionining,
    /// etc.
    int level_;

    /// The hash table for this partition.
    boost::scoped_ptr<HashTable> hash_tbl_;

    /// Stream of build/probe tuples in this partition. Allocated from the runtime state's
    /// object pool. Initially owned by this object (meaning it has to call Close() on it)
    /// but transferred to the parent exec node (via the row batch) when the partition
    /// is complete.
    /// If NULL, ownership has been transfered.
    BufferedTupleStream* build_rows_;
    BufferedTupleStream* probe_rows_;
  };

  /// llvm function and signature for codegening build batch.
  typedef Status (*ProcessBuildBatchFn)(PartitionedHashJoinNode*, RowBatch*);
  /// Jitted ProcessBuildBatch function pointers.  NULL if codegen is disabled.
  /// process_build_batch_fn_level0_ uses CRC hashing when available and is used when the
  /// partition level is 0, otherwise process_build_batch_fn_ uses murmur hash and is used
  /// for subsequent levels.
  ProcessBuildBatchFn process_build_batch_fn_;
  ProcessBuildBatchFn process_build_batch_fn_level0_;

  /// llvm function and signature for codegening probe batch.
  typedef int (*ProcessProbeBatchFn)(
      PartitionedHashJoinNode*, RowBatch*, HashTableCtx*, Status*);
  /// Jitted ProcessProbeBatch function pointer.  NULL if codegen is disabled.
  /// process_probe_batch_fn_level0_ uses CRC hashing when available and is used when the
  /// partition level is 0, otherwise process_probe_batch_fn_ uses murmur hash and is used
  /// for subsequent levels.
  ProcessProbeBatchFn process_probe_batch_fn_;
  ProcessProbeBatchFn process_probe_batch_fn_level0_;
};

}

#endif
