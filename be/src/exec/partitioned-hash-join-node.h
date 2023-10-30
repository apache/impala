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

#ifndef IMPALA_EXEC_PARTITIONED_HASH_JOIN_NODE_H
#define IMPALA_EXEC_PARTITIONED_HASH_JOIN_NODE_H

#include <list>
#include <memory>
#include <string>
#include <boost/scoped_ptr.hpp>

#include "exec/blocking-join-node.h"
#include "exec/exec-node.h"
#include "exec/partitioned-hash-join-builder.h"

#include "gen-cpp/Types_types.h"

namespace impala {

class BloomFilter;
class MemPool;
class PartitionedHashJoinNode;
class RowBatch;
class RuntimeFilter;
class TupleRow;

class PartitionedHashJoinPlanNode : public BlockingJoinPlanNode {
 public:
  virtual Status Init(const TPlanNode& tnode, FragmentState* state) override;
  virtual void Close() override;
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  virtual void Codegen(FragmentState* state) override;

  ~PartitionedHashJoinPlanNode(){}

  /// Our equi-join predicates "<lhs> = <rhs>" are separated into
  /// build_exprs_ (over child(1)) and probe_exprs_ (over child(0))
  std::vector<ScalarExpr*> build_exprs_;
  std::vector<ScalarExpr*> probe_exprs_;

  /// is_not_distinct_from_[i] is true if and only if the ith equi-join predicate is IS
  /// NOT DISTINCT FROM, rather than equality. This is the case when IS NOT DISTINCT FROM
  /// is explicitly used as a join predicate or when joining Iceberg equality delete
  /// files to data files.
  std::vector<bool> is_not_distinct_from_;

  /// Non-equi-join conjuncts from the ON clause.
  std::vector<ScalarExpr*> other_join_conjuncts_;

  /// Data sink config object for creating a phj builder that will be eventually used by
  /// the exec node.
  PhjBuilderConfig* phj_builder_config_;

  /// Seed used for hashing rows.
  uint32_t hash_seed_;

  /// Used for codegening hash table specific methods and to create the corresponding
  /// instance of HashTableCtx.
  const HashTableConfig* hash_table_config_;

  /// For the below codegen'd functions, xxx_fn_level0_ uses CRC hashing when available
  /// and is used when the partition level is 0, otherwise xxx_fn_ uses murmur hash and is
  /// used for subsequent levels.
  typedef int (*ProcessProbeBatchFn)(
      PartitionedHashJoinNode*, TPrefetchMode::type, RowBatch*, HashTableCtx*, Status*);
  /// Jitted PartitionedHashJoinNode::ProcessProbeBatch function pointers.  NULL if
  /// codegen is disabled.
  CodegenFnPtr<ProcessProbeBatchFn> process_probe_batch_fn_;
  CodegenFnPtr<ProcessProbeBatchFn> process_probe_batch_fn_level0_;

 private:
  /// Codegen function to create output row. Assumes that the probe row is non-NULL.
  Status CodegenCreateOutputRow(LlvmCodeGen* codegen, llvm::Function** fn);

  /// Codegen processing probe batches.  Identical signature to
  /// PartitionedHashJoinNode::ProcessProbeBatch(). Returns non-OK if codegen was not
  /// possible.
  Status CodegenProcessProbeBatch(
      LlvmCodeGen* codegen, TPrefetchMode::type prefetch_mode);
};

/// Operator to perform partitioned hash join, spilling to disk as necessary. This
/// operator implements multiple join modes with the same code algorithm.
///
/// The high-level algorithm is as follows:
///  1. Consume all build input and partition it. No hash tables are maintained.
///  2. Construct hash tables for as many unspilled partitions as possible.
///  3. Consume the probe input. Each probe row is hashed to find the corresponding build
///     partition. If the build partition is in-memory (i.e. not spilled), then the
///     partition's hash table is probed and any matching rows can be outputted. If the
///     build partition is spilled, the probe row must also be spilled for later
///     processing.
///  4. Any spilled partitions are processed. If the build rows and hash table for a
///     spilled partition fit in memory, the spilled partition is brought into memory
///     and its spilled probe rows are processed. Otherwise the spilled partition must be
///     repartitioned into smaller partitions. Repartitioning repeats steps 1-3 above,
///     except with the partition's spilled build and probe rows as input.
///
/// IMPLEMENTATION DETAILS:
/// -----------------------
/// The partitioned hash join algorithm is implemented with the PartitionedHashJoinNode
/// and PhjBuilder classes. Each join node has a builder (see PhjBuilder) that
/// partitions, stores and builds hash tables over the build rows.
///
/// The above algorithm is implemented as a state machine with the following phases:
///
///   1. [PARTITIONING_BUILD or REPARTITIONING_BUILD] Read build rows from the right
///      input plan tree OR from the spilled build rows of a partition and partition them
///      into the builder's hash partitions. If there is sufficient memory, all build
///      partitions are kept in memory. Otherwise, build partitions are spilled as needed
///      to free up memory. Finally, build a hash table for each in-memory partition and
///      create a probe partition with a write buffer for each spilled partition.
///
///      After the phase, the algorithm advances from PARTITIONING_BUILD to
///      PARTITIONING_PROBE or from REPARTITIONING_BUILD to REPARTITIONING_PROBE.
///
///   2. [PARTITIONING_PROBE or REPARTITIONING_PROBE] Read the probe rows from child(0) or
///      a the spilled probe rows of a partition and partition them. If a probe row's
///      partition is in memory, probe the partition's hash table, otherwise spill the
///      probe row. Finally, output unmatched build rows for join modes that require it.
///
///      After the phase, the algorithm terminates if no spilled partitions remain or
///      continues to process one of the remaining spilled partitions by advancing to
///      either PROBING_SPILLED_PARTITION or REPARTITIONING_BUILD, depending on whether
///      the spilled partition's hash table fits in memory or not.
///
///      This phase has sub-states (see ProbeState) that are used in GetNext() to drive
///      progress.
///
///   3. [PROBING_SPILLED_PARTITION] Read the probe rows from a spilled partition that
///      was brought back into memory and probe the partition's hash table. Finally,
///      output unmatched build rows for join modes that require it.
///
///      After the phase, the algorithm terminates if no spilled partitions remain or
///      continues to process one of the remaining spilled partitions by advancing to
///      either PROBING_SPILLED_PARTITION or REPARTITIONING_BUILD, depending on whether
///      the spilled partition's hash table fits in memory or not.
///
///      This phase has sub-states (see ProbeState) that are used in GetNext() to drive
///      progress.
///
///
/// When the PhjBuilder is shared by multiple PartitionedHashJoinNodes, HashJoinState of
/// the builder will drive the hash join algorithm across all the PartitionedHashJoinNode
/// implementations sharing the builder. Each PartitionedHashJoinNode implementation will
/// independently execute its ProbeState state machine, synchronizing via the builder for
/// transitions of the HashJoinState state machine.
///
/// Null aware anti-join (NAAJ) extends the above algorithm by accumulating rows with
/// NULLs into several different streams, which are processed in a separate step to
/// produce additional output rows. The NAAJ algorithm is documented in more detail in
/// header comments for the null aware functions and data structures.

class PartitionedHashJoinNode : public BlockingJoinNode {
 public:
  PartitionedHashJoinNode(RuntimeState* state, const PartitionedHashJoinPlanNode& pnode,
      const DescriptorTbl& descs);
  virtual ~PartitionedHashJoinNode();

  virtual Status Prepare(RuntimeState* state) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch) override;
  virtual void Close(RuntimeState* state) override;

 protected:
  virtual void AddToDebugString(
      int indentation_level, std::stringstream* out) const override;

  // Safe to close the build side early because we rematerialize the build rows always.
  virtual bool CanCloseBuildEarly() const override { return true; }
  virtual Status AcquireResourcesForBuild(RuntimeState* state) override;

 private:
  class ProbePartition;

  // This enum drives a different state machine within the PARTITIONING_PROBE,
  // PROBING_SPILLED_PARTITION and REPARTITIONING_PROBE states.
  // This drives the state machine in GetNext() that processes probe batches and generates
  // output rows. This state machine executes within a HashJoinState state, starting with
  // PROBING_IN_BATCH and ending with PROBE_COMPLETE.
  //
  // The state transition diagram is below. The state machine handles iterating through
  // probe batches (PROBING_IN_BATCH <-> PROBING_END_BATCH), with each input probe batch
  // producing a variable number of output rows. Once the last probe batch is processed,
  // additional rows may need to be emitted from the build side per-partition, e.g.
  // for right outer join (OUTPUTTING_UNMATCHED). Then PROBE_COMPLETE is entered,
  // indicating that probing is complete. Then the top-level state machine (described by
  // HashJoinState) takes over and either the next spilled partition is processed, final
  // null-aware anti join processing is done, or eos can be returned from GetNext().
  //
  // start                     if hash tables
  //     +------------------+  store matches  +----------------------+
  //---->+ PROBING_IN_BATCH |  +------------->+ OUTPUTTING_UNMATCHED |
  //     +-----+-----+------+  |              +------+---------------+
  //           ^     |         |                     |
  //           |     |         |                     |
  //           |     v         |                     v
  //     +-----+-----+-------+ | otherwise  +--------+-------+
  // +-->+ PROBING_END_BATCH +-+----------->+ PROBE_COMPLETE |
  // |   +-------------------+              +--+------+------+
  // |                                       | |    | if NAAJ
  // +---------------------------------------+ |    | and no spilled
  //      if spilled partitions left           |    | partitions left
  //                                           |    |
  //                           if not NAAJ     |    |
  //                           and no spilled  |    +---------------+
  //                +-------+  partitions left |    | if null-aware | otherwise
  //                |  EOS  +<-----------------+    | partition     |
  //                +---+---+                       v has rows      |
  //                    ^              +------------+----------+    |
  //                    |              | OUTPUTTING_NULL_AWARE |    |
  //                    |              +------------+----------+    |
  //                    |                           |               |
  //                    |                           v               |
  //                    |              +------------+----------+    |
  //                    +--------------+ OUTPUTTING_NULL_PROBE +<---+
  //                                   +-----------------------+
  enum class ProbeState {
    // Processing probe batches and more rows in the current probe batch must be
    // processed.
    PROBING_IN_BATCH,
    // Processing probe batches and no more rows in the current probe batch to process.
    PROBING_END_BATCH,
    // All probe batches have been processed, unmatched build rows need to be outputted
    // from 'output_build_partitions_'.
    // This state is only used if NeedToProcessUnmatchedBuildRows(join_op_) is true.
    OUTPUTTING_UNMATCHED,
    // All input probe rows from the child ExecNode or the current spilled partition have
    // been processed, and all unmatched rows from the build have been output.
    PROBE_COMPLETE,
    // All input has been processed. We need to process builder->null_aware_partition()
    // and output any rows from it.
    // This state is only used if join_op_ is NULL_AWARE_ANTI_JOIN.
    OUTPUTTING_NULL_AWARE,
    // All input has been processed. We need to process null_probe_rows_ and output any
    // rows from it.
    // This state is only used if join_op_ is NULL_AWARE_ANTI_JOIN.
    OUTPUTTING_NULL_PROBE,
    // All output rows have been produced - GetNext() should return eos.
    EOS,
  };

  /// Constants from PhjBuilder, added to this node for convenience.
  static const int PARTITION_FANOUT = PhjBuilder::PARTITION_FANOUT;
  static const int NUM_PARTITIONING_BITS = PhjBuilder::NUM_PARTITIONING_BITS;
  static const int MAX_PARTITION_DEPTH = PhjBuilder::MAX_PARTITION_DEPTH;

  /// Initialize 'probe_hash_partitions_' and 'hash_tbls_' before probing. One probe
  /// partition is created per spilled build partition, and 'hash_tbls_' is initialized
  /// with pointers to the hash tables of in-memory partitions and NULL pointers for
  /// spilled or closed partitions. The builder's hash partitions must be initialized
  /// initialized and present in 'build_hash_partitions_', i.e. the state must be
  /// PARTITIONING_PROBE or REPARTITIONING_PROBE.
  ///
  /// If we are probing a spilled partition (i.e. the state is REPARTITIONING_PROBE), this
  /// also prepares 'input_partition_' for reading.
  ///
  /// Called after the builder has partitioned the build rows and built hash tables,
  /// either in the initial build step, or after repartitioning a spilled partition.
  /// After this function returns, all partitions are ready to process probe rows.
  Status PrepareForPartitionedProbe();

  /// Initialize 'hash_tbls_' and 'input_partition_' so that we can read probe rows
  /// from 'input_partition_' and probe 'hash_tbls_'.
  Status PrepareForUnpartitionedProbe();

  // Initialize 'probe_hash_partitions_'. Each spilled build partition gets a
  // corresponding probe partition. Closed or in-memory build partitions do
  // not get a probe partition. If an error is encountered, 'probe_hash_partitions_'
  // may be left with some partitions and will be cleaned up by Close().
  Status CreateProbeHashPartitions(bool* have_spilled_hash_partitions);

  /// Append the probe row 'row' to 'stream'. The stream must be unpinned and must have
  /// a write buffer allocated, so this will succeed unless an error is encountered.
  /// Returns false and sets 'status' to an error if an error is encountered. This odd
  /// return convention is used to avoid emitting unnecessary code for ~Status in perf-
  /// critical code.
  bool AppendSpilledProbeRow(
      BufferedTupleStream* stream, TupleRow* row, Status* status) WARN_UNUSED_RESULT;

  /// Append the probe row 'row' to 'stream'. The stream may be pinned or unpinned and
  /// and must have a write buffer allocated. Unpins the stream if needed to append the
  /// row, so this will succeed unless an error is encountered. Returns false and sets
  /// 'status' to an error if an error is encountered. This odd return convention is
  /// used to avoid emitting unnecessary code for ~Status in perf-critical code.
  bool AppendProbeRow(
      BufferedTupleStream* stream, TupleRow* row, Status* status) WARN_UNUSED_RESULT;

  /// Slow path for AppendProbeRow() where appending fails initially.
  bool AppendProbeRowSlow(BufferedTupleStream* stream, TupleRow* row, Status* status);

  /// Probes the hash table for rows matching the current probe row and appends
  /// all the matching build rows (with probe row) to output batch. Returns true
  /// if probing is done for the current probe row and should continue to next row.
  ///
  /// 'out_batch_iterator' is the iterator for the output batch.
  /// 'remaining_capacity' tracks the number of additional rows that can be added to
  /// the output batch. It's updated as rows are added to the output batch.
  /// Using a separate variable is probably faster than calling
  /// 'out_batch_iterator->parent()->AtCapacity()' as it avoids unnecessary memory load.
  bool inline ProcessProbeRowInnerJoin(
      ScalarExprEvaluator* const* other_join_conjunct_evals, int num_other_join_conjuncts,
      ScalarExprEvaluator* const* conjunct_evals, int num_conjuncts,
      RowBatch::Iterator* out_batch_iterator, int* remaining_capacity) WARN_UNUSED_RESULT;

  /// Probes and updates the hash table for the current probe row for either
  /// RIGHT_SEMI_JOIN or RIGHT_ANTI_JOIN. For RIGHT_SEMI_JOIN, all matching build
  /// rows will be appended to the output batch; For RIGHT_ANTI_JOIN, update the
  /// hash table only if matches are found. The actual output happens in
  /// OutputUnmatchedBuild(). Returns true if probing is done for the current
  /// probe row and should continue to next row.
  ///
  /// 'out_batch_iterator' is the iterator for the output batch.
  /// 'remaining_capacity' tracks the number of additional rows that can be added to
  /// the output batch. It's updated as rows are added to the output batch.
  /// Using a separate variable is probably faster than calling
  /// 'out_batch_iterator->parent()->AtCapacity()' as it avoids unnecessary memory load.
  template <int const JoinOp>
  bool inline ProcessProbeRowRightSemiJoins(
      ScalarExprEvaluator* const* other_join_conjunct_evals, int num_other_join_conjuncts,
      ScalarExprEvaluator* const* conjunct_evals, int num_conjuncts,
      RowBatch::Iterator* out_batch_iterator, int* remaining_capacity) WARN_UNUSED_RESULT;

  /// Probes the hash table for the current probe row for LEFT_SEMI_JOIN,
  /// LEFT_ANTI_JOIN or NULL_AWARE_LEFT_ANTI_JOIN. The probe row will be appended
  /// to output batch if there is a match (for LEFT_SEMI_JOIN) or if there is no
  /// match (for LEFT_ANTI_JOIN). Returns true if probing is done for the current
  /// probe row and should continue to next row.
  ///
  /// 'out_batch_iterator' is the iterator for the output batch.
  /// 'remaining_capacity' tracks the number of additional rows that can be added to
  /// the output batch. It's updated as rows are added to the output batch.
  /// Using a separate variable is probably faster than calling
  /// 'out_batch_iterator->parent()->AtCapacity()' as it avoids unnecessary memory load.
  template <int const JoinOp>
  bool inline ProcessProbeRowLeftSemiJoins(
      ScalarExprEvaluator* const* other_join_conjunct_evals, int num_other_join_conjuncts,
      ScalarExprEvaluator* const* conjunct_evals, int num_conjuncts,
      RowBatch::Iterator* out_batch_iterator, int* remaining_capacity,
      Status* status) WARN_UNUSED_RESULT;

  /// Probes the hash table for the current probe row for LEFT_OUTER_JOIN,
  /// RIGHT_OUTER_JOIN or FULL_OUTER_JOIN. The matching build and/or probe row
  /// will be appended to output batch. For RIGHT/FULL_OUTER_JOIN, some of the outputs
  /// are added in OutputUnmatchedBuild(). Returns true if probing is done for the
  /// current probe row and should continue to next row.
  ///
  /// 'out_batch_iterator' is the iterator for the output batch.
  /// 'remaining_capacity' tracks the number of additional rows that can be added to
  /// the output batch. It's updated as rows are added to the output batch.
  /// Using a separate variable is probably faster than calling
  /// 'out_batch_iterator->parent()->AtCapacity()' as it avoids unnecessary memory load.
  /// 'status' may be updated if appending to null aware BTS fails.
  template <int const JoinOp>
  bool inline ProcessProbeRowOuterJoins(
      ScalarExprEvaluator* const* other_join_conjunct_evals, int num_other_join_conjuncts,
      ScalarExprEvaluator* const* conjunct_evals, int num_conjuncts,
      RowBatch::Iterator* out_batch_iterator, int* remaining_capacity) WARN_UNUSED_RESULT;

  /// Probes 'current_probe_row_' against the the hash tables and append outputs
  /// to output batch. Wrapper around the join-type specific probe row functions
  /// declared above.
  template <int const JoinOp>
  bool inline ProcessProbeRow(ScalarExprEvaluator* const* other_join_conjunct_evals,
      int num_other_join_conjuncts, ScalarExprEvaluator* const* conjunct_evals,
      int num_conjuncts, RowBatch::Iterator* out_batch_iterator, int* remaining_capacity,
      Status* status) WARN_UNUSED_RESULT;

  /// Evaluates some number of rows in 'probe_batch_' against the probe expressions
  /// and hashes the results to 32-bit hash values. The evaluation results and the hash
  /// values are stored in the expression values cache in 'ht_ctx'. The number of rows
  /// processed depends on the capacity available in 'ht_ctx->expr_values_cache_'.
  /// 'prefetch_mode' specifies the prefetching mode in use. If it's not PREFETCH_NONE,
  /// hash table buckets will be prefetched based on the hash values computed. Note
  /// that 'prefetch_mode' will be substituted with constants during codegen time.
  void EvalAndHashProbePrefetchGroup(TPrefetchMode::type prefetch_mode,
      HashTableCtx* ctx);

  /// Find the next probe row. Returns true if a probe row is found. In which case,
  /// 'current_probe_row_' and 'hash_tbl_iterator_' have been set up to point to the
  /// next probe row and its corresponding partition. 'status' may be updated if
  /// append to the spilled partitions' BTS or null probe rows' BTS fail.
  template <int const JoinOp>
  bool inline NextProbeRow(HashTableCtx* ht_ctx, RowBatch::Iterator* probe_batch_iterator,
      int* remaining_capacity, Status* status) WARN_UNUSED_RESULT;

  /// Process probe rows from probe_batch_. Returns either if out_batch is full or
  /// probe_batch_ is entirely consumed.
  /// For RIGHT_ANTI_JOIN, all this function does is to mark whether each build row
  /// had a match.
  /// Returns the number of rows added to out_batch; -1 on error (and *status will
  /// be set). This function doesn't commit rows to the output batch so it's the caller's
  /// responsibility to do so.
  template<int const JoinOp>
  int ProcessProbeBatch(TPrefetchMode::type, RowBatch* out_batch, HashTableCtx* ht_ctx,
      Status* status);

  /// Wrapper that calls either the interpreted or codegen'd version of
  /// ProcessProbeBatch() and commits the rows to 'out_batch' on success.
  Status ProcessProbeBatch(RowBatch* out_batch);

  /// Wrapper that calls the templated version of ProcessProbeBatch() based on 'join_op'.
  int ProcessProbeBatch(const TJoinOp::type join_op, TPrefetchMode::type,
      RowBatch* out_batch, HashTableCtx* ht_ctx, Status* status);

  /// Used when NeedToProcessUnmatchedBuildRows() is true. Writes all unmatched rows from
  /// 'output_build_partitions_' to 'out_batch', up to 'out_batch' capacity.
  Status OutputUnmatchedBuild(RowBatch* out_batch) WARN_UNUSED_RESULT;

  /// Called by OutputUnmatchedBuild() when there isn't a hash table built, which happens
  /// when a spilled partition had 0 probe rows. In this case, all of the build rows are
  /// unmatched and we can iterate over the entire build side of the partition, which will
  /// be the only partition in 'output_build_partitions_'. If it reaches the end of the
  /// partition, it closes that partition and removes it from 'output_build_partitions_'.
  Status OutputAllBuild(RowBatch* out_batch) WARN_UNUSED_RESULT;

  /// Called by OutputUnmatchedBuild when there is a hash table built. Sweeps the
  /// 'hash_tbl_' of the partition that is at the front of 'output_build_partitions_',
  /// using 'hash_tbl_iterator_' and outputs any unmatched build rows. If it reaches the
  /// end of the hash table it closes that partition, removes it from
  /// 'output_build_partitions_' and moves 'hash_tbl_iterator_' to the beginning of the
  /// new partition at the front of 'output_build_partitions_'.
  void OutputUnmatchedBuildFromHashTable(RowBatch* out_batch);

  /// Writes 'build_row' to 'out_batch' at the position of 'out_batch_iterator' in a
  /// 'join_op_' specific way.
  void OutputBuildRow(
      RowBatch* out_batch, TupleRow* build_row, RowBatch::Iterator* out_batch_iterator);

  /// Initializes 'null_aware_probe_partition_' and prepares its probe stream for writing.
  Status InitNullAwareProbePartition() WARN_UNUSED_RESULT;

  /// Initializes 'null_probe_rows_' and prepares that stream for writing.
  Status InitNullProbeRows() WARN_UNUSED_RESULT;

  /// Prepare to output rows from the null-aware partition.
  /// *has_null_aware_rows is set to true if the null-aware partition has rows that need
  /// to be processed by calling OutputNullAwareProbeRows(), false otherwise. In both
  /// cases, null probe rows need to be processed with OutputNullAwareNullProbe().
  Status BeginNullAwareProbe(bool* has_null_aware_rows) WARN_UNUSED_RESULT;

  /// Output rows from builder_->null_aware_partition(). Called when 'probe_state_'
  /// is OUTPUTTING_NULL_AWARE - after all input is processed, including spilled
  /// partitions. Sets *done = true if there are no more rows to output from this
  /// function, false otherwise.
  Status OutputNullAwareProbeRows(
      RuntimeState* state, RowBatch* out_batch, bool* done) WARN_UNUSED_RESULT;

  /// Evaluates 'other_join_conjuncts' for all pairs of 'null_probe_rows_' and the build
  /// rows provided by the caller until a match is found for that null probe row. This
  /// updates matched_null_probe_, short-circuiting if one of the conjuncts pass (i.e.
  /// there is a match). 'build' must be pinned.
  /// This is used for NAAJ, when there are NULL probe rows.
  Status EvaluateNullProbe(
      RuntimeState* state, BufferedTupleStream* build) WARN_UNUSED_RESULT;

  /// Prepares to output NULLs on the probe side for NAAJ, when transitioning
  /// 'probe_state_' to OUTPUTTING_NULL_PROBE. Before calling this, 'matched_null_probe_'
  /// must be fully evaluated.
  Status PrepareNullAwareNullProbe() WARN_UNUSED_RESULT;

  /// Outputs NULLs on the probe side, returning rows where matched_null_probe_[i] is
  /// false. Called repeatedly after PrepareNullAwareNullProbe(), when 'probe_state_'
  /// is OUTPUTTING_NULL_PROBE for NAAJ. Sets *done = true if there are no more rows to
  /// output from this function, false otherwise.
  Status OutputNullAwareNullProbe(
      RuntimeState* state, RowBatch* out_batch, bool* done) WARN_UNUSED_RESULT;

  /// Call at the end of consuming the probe rows, when 'probe_state_' is
  /// PROBING_END_BATCH, before transitioning to PROBE_COMPLETE or OUTPUTTING_UNMATCHED.
  /// Cleans up the build and probe hash partitions, if needed, and:
  ///  - If the build partition had a hash table, close it. The build and probe
  ///    partitions are fully processed. The streams are transferred to 'batch'.
  ///    In the case of right-outer and full-outer joins, instead of closing this
  ///    partition we put it on a list of partitions for which we need to flush their
  ///    unmatched rows.
  ///  - If the build partition did not have a hash table, meaning both build and probe
  ///    rows were spilled, move the partition to 'spilled_partitions_'.
  /// Also cleans up 'input_partition_' (if processing a spilled partition).
  Status DoneProbing(RuntimeState* state, RowBatch* batch) WARN_UNUSED_RESULT;

  /// Get the next row batch from the probe (left) side (child(0)), if we are still
  /// doing the first pass over the input (i.e. state_ is PARTITIONING_PROBE) or
  /// from the spilled 'input_partition_' if state_ is REPARTITIONING_PROBE.
  //. If we are done consuming the input, sets 'probe_batch_pos_' to -1, otherwise,
  /// sets it to 0.  'probe_state_' must be PROBING_END_BATCH. *eos is true iff
  /// 'out_batch' contains the last rows from the child or spilled partition.
  Status NextProbeRowBatch(
      RuntimeState* state, RowBatch* out_batch, bool* eos) WARN_UNUSED_RESULT;

  /// Get the next row batch from the probe (left) side (child(0)). If we are done
  /// consuming the input, sets 'probe_batch_pos_' to -1, otherwise, sets it to 0.
  /// 'probe_state_' must be PROBING_END_BATCH. *eos is true iff 'out_batch'
  /// contains the last rows from the child.
  Status NextProbeRowBatchFromChild(RuntimeState* state, RowBatch* out_batch, bool* eos);

  /// Get the next probe row batch from 'input_partition_'. If we are done consuming the
  /// input, sets 'probe_batch_pos_' to -1, otherwise, sets it to 0.
  /// 'probe_state_' must be PROBING_END_BATCH.. *eos is true iff 'out_batch'
  /// contains the last rows from 'input_partition_'.
  Status NextSpilledProbeRowBatch(RuntimeState* state, RowBatch* out_batch, bool* eos);

  /// Called when 'probe_state_' is PROBE_COMPLETE to start processing the next spilled
  /// partition. This function sets 'input_partition_' to the chosen partition, then
  /// delegates to 'builder_' to bring all or part of the spilled build side into
  /// memory' and sets up this node to probe the partition.
  ///
  /// If the build side's hash table fits in memory and there are probe rows, then there
  /// will be a single in-memory partition. If it does not fit, meaning we need to
  /// repartition, this function will repartition the build rows into PARTITION_FANOUT
  /// hash partitions and prepare for repartitioning the partition's probe
  /// rows. If there are no probe rows, we just prepare the build side to be read by
  /// OutputUnmatchedBuild().
  ///
  /// When this function returns function returns, we are ready to start reading probe
  /// rows from 'input_partition_'.
  Status BeginSpilledProbe() WARN_UNUSED_RESULT;

  /// Calls Close() on every probe partition, destroys the partitions and cleans up any
  /// references to the partitions. Also closes and destroys 'null_probe_rows_'. If
  /// 'row_batch' is not NULL, transfers ownership of all row-backing resources to it.
  void CloseAndDeletePartitions(RowBatch* row_batch);

  /// Prepares for probing the next batch. Called after populating 'probe_batch_'
  /// with rows and entering 'probe_state_' PROBING_IN_BATCH.
  void ResetForProbe();

  uint32_t hash_seed() const {
    return static_cast<const PartitionedHashJoinPlanNode&>(plan_node_).hash_seed_;
  }

  std::string NodeDebugString() const;

  RuntimeState* runtime_state_;

  /// Our equi-join predicates "<lhs> = <rhs>" are separated into
  /// build_exprs_ (over the build input row) and probe_exprs_ (over child(0))
  const std::vector<ScalarExpr*>& build_exprs_;
  const std::vector<ScalarExpr*>& probe_exprs_;

  /// Non-equi-join conjuncts from the ON clause.
  const std::vector<ScalarExpr*>& other_join_conjuncts_;
  std::vector<ScalarExprEvaluator*> other_join_conjunct_evals_;

  /// Reference to the hash table config which is a part of the
  /// PartitionedHashJoinPlanNode that was used to create this object. Its used to create
  /// an instance of the HashTableCtx in Prepare(). Not Owned.
  const HashTableConfig& hash_table_config_;

  /// Used for hash-related functionality, such as evaluating rows and calculating hashes.
  /// This owns the evaluators for the build and probe expressions used during insertion
  /// and probing of the hash tables.
  boost::scoped_ptr<HashTableCtx> ht_ctx_;

  /// MemPool that stores allocations that hold results from evaluation of probe
  /// exprs by 'ht_ctx_'. Cached probe expression values may reference memory in this
  /// pool.
  boost::scoped_ptr<MemPool> probe_expr_results_pool_;

  /// The iterator that corresponds to the look up of current_probe_row_.
  HashTable::Iterator hash_tbl_iterator_;

  /// Number of probe rows that have been partitioned.
  RuntimeProfile::Counter* num_probe_rows_partitioned_ = nullptr;

  /// Time spent evaluating other_join_conjuncts for NAAJ.
  RuntimeProfile::Counter* null_aware_eval_timer_ = nullptr;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// State of the probing algorithm. Used to drive the state machine in GetNext().
  ProbeState probe_state_ = ProbeState::PROBE_COMPLETE;

  /// Additional state for flushing build-side data. Needed for
  /// separate build.
  /// TODO: IMPALA-9411: this could be removed if we attached the buffers.
  bool flushed_unattachable_build_buffers_ = false;

  /// The build-side rows of the join. Initialized in Prepare() if the build is embedded
  /// in the join, otherwise looked up in Open() if it's a separate build. Owned by an
  /// object pool with query lifetime in either case.
  PhjBuilder* builder_ = nullptr;

  /// Last set of hash partitions obtained from builder_. Only valid when the
  /// builder's state is PARTITIONING_PROBE or REPARTITIONING_PROBE.
  PhjBuilder::HashPartitions build_hash_partitions_;

  /// Cache of the per partition hash table to speed up ProcessProbeBatch().
  /// In the case where we need to partition the probe:
  ///  hash_tbls_[i] = (*build_hash_partitions_.hash_partitions)[i]->hash_tbl();
  /// In the case where we don't need to partition the probe:
  ///  hash_tbls_[i] = input_partition_->hash_tbl();
  HashTable* hash_tbls_[PARTITION_FANOUT];

  /// Probe partitions, with indices corresponding to the build partitions in
  /// build_hash_partitions_. This is non-empty only in the PARTITIONING_PROBE or
  /// REPARTITIONING_PROBE states, in which case it has NULL entries for in-memory
  /// build partitions and non-NULL entries for spilled build partitions (so that we
  /// have somewhere to spill the probe rows for the spilled partition).
  std::vector<std::unique_ptr<ProbePartition>> probe_hash_partitions_;

  /// Probe partitions that have been spilled and still need more processing. Each of
  /// these has a corresponding build partition in 'builder_' with the same PartitionId.
  /// For shared broadcast join builds, the set of keys in this map will be the same
  /// across all of the instances of the join builder.
  /// This list is populated at DoneProbing().
  std::unordered_map<PhjBuilder::PartitionId, std::unique_ptr<ProbePartition>>
      spilled_partitions_;

  /// The current spilled probe partition being processed as input to repartitioning,
  /// or the source of the probe rows if the hash table fits in memory.
  std::unique_ptr<ProbePartition> input_partition_;

  /// In the case of right-outer and full-outer joins, this is the list of the partitions
  /// for which we need to output their unmatched build rows. This list is populated at
  /// DoneProbing(). If this is non-empty, probe_state_ must be OUTPUTTING_UNMATCHED.
  std::deque<std::unique_ptr<PhjBuilderPartition>> output_build_partitions_;

  /// Partition used if 'null_aware_' is set. During probing, rows from the probe
  /// side that did not have a match in the hash table are appended to this partition.
  /// At the very end, we then iterate over the partition's probe rows. For each probe
  /// row, we return the rows that did not match any of the partition's build rows. This
  /// is NULL if this join is not null aware or we are done processing this partition.
  /// The probe stream starts off in memory but is unpinned if there is memory pressure,
  /// specifically if any partitions spilled or appending to the pinned stream failed.
  boost::scoped_ptr<ProbePartition> null_aware_probe_partition_;

  /// For NAAJ, this stream contains all probe rows that had NULL on the hash table
  /// conjuncts. Must be unique_ptr so we can release it and transfer to output batches.
  /// The stream starts off in memory but is unpinned if there is memory pressure,
  /// specifically if any partitions spilled or appending to the pinned stream failed.
  /// Populated during the first pass over the probe input (i.e. while state_ is
  /// PARTITIONING_PROBE) and then output at the end after all input data is processed.
  std::unique_ptr<BufferedTupleStream> null_probe_rows_;

  /// For each row in null_probe_rows_, true if this row has matched any build row
  /// (i.e. the resulting joined row passes other_join_conjuncts). Populated
  /// during the first pass over the probe input (i.e. while state_ is PARTITIONING_PROBE)
  /// and then evaluated for each build partition.
  /// TODO: ideally we would store the bits inside the tuple data of 'null_probe_rows_'
  /// instead of in this untracked auxiliary memory.
  std::vector<bool> matched_null_probe_;

  /// The current index into null_probe_rows_/matched_null_probe_ that we are
  /// outputting. -1 means invalid. Only has a valid index when probe_state_ is
  /// OUTPUTTING_NULL_PROBE.
  int64_t null_probe_output_idx_ = -1;

  /// Used by OutputAllBuild() to iterate over the entire build side tuple stream of the
  /// current partition. Only used when probe_state_ is OUTPUTTING_UNMATCHED.
  std::unique_ptr<RowBatch> output_unmatched_batch_;

  /// Stores an iterator into 'output_unmatched_batch_' to start from on the next call to
  /// OutputAllBuild(), or NULL if there are no partitions without hash tables needing to
  /// be processed by OutputUnmatchedBuild(). Only used when probe_state_ is
  /// OUTPUTTING_UNMATCHED.
  std::unique_ptr<RowBatch::Iterator> output_unmatched_batch_iter_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// The probe-side partition corresponding to a build partition. The probe partition
  /// is created when a build partition is spilled so that probe rows can be spilled to
  /// disk for later processing.
  class ProbePartition {
   public:
    /// Create a new probe partition for the same hash partition as 'build_partition'.
    ProbePartition(RuntimeState* state, PartitionedHashJoinNode* parent,
        PhjBuilderPartition* build_partition);
    ~ProbePartition();

    /// Prepare to write the probe rows. Allocates the first write block. This stream
    /// is unpinned so writes should not fail with out of memory if this succeeds.
    /// Returns an error if the first write block cannot be acquired.
    Status PrepareForWrite(PartitionedHashJoinNode* parent, bool pinned);

    /// Prepare to read the probe rows. Allocates the first read block, so reads will
    /// not fail with out of memory if this succeeds. Returns an error if the first read
    /// block cannot be acquired. "delete_on_read" mode is used, so the blocks backing
    /// the buffered tuple stream will be destroyed after reading.
    Status PrepareForRead() WARN_UNUSED_RESULT;

    /// Close the partition and attach resources to 'batch' if non-NULL or free the
    /// resources if 'batch' is NULL. Idempotent.
    void Close(RowBatch* batch);

    BufferedTupleStream* ALWAYS_INLINE probe_rows() { return probe_rows_.get(); }
    PhjBuilderPartition* build_partition() { return build_partition_; }

    inline bool IsClosed() const { return probe_rows_ == NULL; }

   private:
    /// The corresponding build partition. Not NULL. Owned by PhjBuilder.
    PhjBuilderPartition* build_partition_;

    /// Stream of probe tuples in this partition. Initially owned by this object but
    /// transferred to the parent exec node (via the row batch) when the partition
    /// is complete. If NULL, ownership was transferred and the partition is closed.
    std::unique_ptr<BufferedTupleStream> probe_rows_;
  };

  /// See PartitionedHashJoinPlanNode::ProcessProbeBatchFn for more details.
  const CodegenFnPtr<PartitionedHashJoinPlanNode::ProcessProbeBatchFn>&
      process_probe_batch_fn_;
  const CodegenFnPtr<PartitionedHashJoinPlanNode::ProcessProbeBatchFn>&
      process_probe_batch_fn_level0_;
};

}

#endif
