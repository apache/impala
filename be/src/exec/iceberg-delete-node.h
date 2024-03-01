
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

#pragma once

#include "exec/blocking-join-node.h"
#include "exec/iceberg-delete-builder.h"
#include "runtime/row-batch.h"

namespace impala {

class ExecNode;
class FragmentState;
class RowBatch;
class TupleRow;

class IcebergDeletePlanNode : public BlockingJoinPlanNode {
 public:
  Status Init(const TPlanNode& tnode, FragmentState* state) override;
  void Close() override;
  Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;

  ~IcebergDeletePlanNode() {}

  /// Data sink config object for creating a id builder that will be eventually used by
  /// the exec node.
  IcebergDeleteBuilderConfig* id_builder_config_;
};

/// Operator to perform iceberg delete.
///
/// The high-level algorithm is as follows:
///  1. Consume all build input.
///  2. Construct hash table.
///  3. Consume the probe input.
///
/// IMPLEMENTATION DETAILS:
/// -----------------------
/// The iceberg delete algorithm is implemented with the IcebergDeleteNode
/// and IcebergDeleteBuilder classes. Each delete node has a builder (see
/// IcebergDeleteBuilder) that stores and builds hash tables over the build
/// rows.
///
/// The above algorithm has the following phases:
///
///   1. Read build rows from the right input plan tree. Everything is kept in memory.
///
///   2. Read the probe rows from child(0) and filter them based on the data in the
///      hash table
///
///      This phase has sub-states (see ProbeState) that are used in GetNext() to drive
///      progress.
///
class IcebergDeleteNode : public BlockingJoinNode {
 public:
  IcebergDeleteNode(RuntimeState* state, const IcebergDeletePlanNode& pnode,
      const DescriptorTbl& descs);
  ~IcebergDeleteNode();

  Status Prepare(RuntimeState* state) override;
  Status Open(RuntimeState* state) override;
  Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  Status Reset(RuntimeState* state, RowBatch* row_batch) override;
  void Close(RuntimeState* state) override;

 protected:
  // Safe to close the build side early because we rematerialize the build rows always.
  bool CanCloseBuildEarly() const override { return true; }
  Status AcquireResourcesForBuild(RuntimeState* state) override;

 private:
  // This enum drives the state machine in GetNext() that processes probe batches and
  // generates output rows.
  //
  // The state transition diagram is below. The state machine handles iterating through
  // probe batches (PROBING_IN_BATCH <-> PROBING_END_BATCH), with each input probe batch
  // producing a variable number of output rows. When the processing is done EOS is
  // entered.
  //
  // start
  //     +------------------+
  //---->+ PROBING_IN_BATCH |
  //     +-----+-----+------+
  //           ^     |
  //           |     |
  //           |     v
  //     +-----+-----+-------+              +----------------+
  //     + PROBING_END_BATCH +------------->+       EOS      |
  //     +-------------------+              +----------------+
  //
  enum class ProbeState {
    // Processing probe batches and more rows in the current probe batch must be
    // processed.
    PROBING_IN_BATCH,
    // Processing probe batches and no more rows in the current probe batch to process.
    PROBING_END_BATCH,
    // All output rows have been produced - GetNext() should return eos.
    EOS,
  };

  /// Probes 'current_probe_row_' against the hash tables and append outputs
  /// to output batch.
  bool inline ProcessProbeRow(RowBatch::Iterator* out_batch_iterator,
      int* remaining_capacity) WARN_UNUSED_RESULT;

  /// Append outputs to output batch.
  bool inline ProcessProbeRowNoCheck(RowBatch::Iterator* out_batch_iterator,
      int* remaining_capacity) WARN_UNUSED_RESULT;

  /// Process probe rows from probe_batch_. Returns either if out_batch is full or
  /// probe_batch_ is entirely consumed.
  /// Returns the number of rows added to out_batch; -1 on error (and *status will
  /// be set). This function doesn't commit rows to the output batch so it's the caller's
  /// responsibility to do so.
  int ProcessProbeBatch(TPrefetchMode::type, RowBatch* out_batch);

  /// Wrapper that ProcessProbeBatch() and commits the rows to 'out_batch' on success.
  Status ProcessProbeBatch(RowBatch* out_batch);

  /// Call at the end of consuming the probe rows, when 'probe_state_' is
  /// PROBING_END_BATCH, before transitioning to PROBE_EOS.
  Status DoneProbing(RuntimeState* state, RowBatch* batch) WARN_UNUSED_RESULT;

  /// Get the next row batch from the probe (left) side (child(0)).
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

  /// Prepares for probing the next batch. Called after populating 'probe_batch_'
  /// with rows and entering 'probe_state_' PROBING_IN_BATCH.
  inline void ResetForProbe() {
    current_probe_row_ = NULL;
    probe_batch_pos_ = 0;
    matched_probe_ = true;
  }

  std::string NodeDebugString() const;

  RuntimeState* runtime_state_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// State of the probing algorithm. Used to drive the state machine in GetNext().
  ProbeState probe_state_ = ProbeState::EOS;

  /// The build-side rows of the join. Initialized in Prepare() if the build is embedded
  /// in the join, otherwise looked up in Open() if it's a separate build. Owned by an
  /// object pool with query lifetime in either case.
  IcebergDeleteBuilder* builder_ = nullptr;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  int file_path_offset_;
  int pos_offset_;

  class IcebergDeleteState {
   public:
    void Init(IcebergDeleteBuilder* builder);

    // Recalculated the next delete row id if:
    //  1. data file path changed
    //  2. probe position is bigger than the next delete row id (there was a gap)
    //     in the probe side
    void Update(impala::StringValue* file_path, int64_t* probe_pos);

    // Checks if the current probe row is deleted.
    bool IsDeleted() const;

    // Progresses the delete row id, or sets to invalid if we reached to the end
    // of the delete vector.
    void Delete();

    // Returns true, if we can pass through the rest of the row batch
    bool NeedCheck() const;

    // Clears the state after the row batch is processed
    void Clear();

    void Reset();

   private:
    void UpdateImpl();
    static constexpr int64_t INVALID_ROW_ID = -1;

    // Using pointers and index instead of iterators to have nicer default state
    // when we switch rowbatch
    const impala::StringValue* current_file_path_;
    const impala::StringValue* previous_file_path_;
    IcebergDeleteBuilder::DeleteRowVector* current_delete_row_;
    int64_t current_deleted_pos_row_id_;
    int64_t current_probe_pos_;

    IcebergDeleteBuilder* builder_ = nullptr;
  };

  IcebergDeleteState iceberg_delete_state_;
};

} // namespace impala
