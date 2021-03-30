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

#ifndef IMPALA_EXEC_NESTED_LOOP_JOIN_BUILDER_H
#define IMPALA_EXEC_NESTED_LOOP_JOIN_BUILDER_H

#include "common/atomic.h"
#include "exec/blocking-join-node.h"
#include "exec/join-builder.h"
#include "exec/row-batch-cache.h"
#include "exec/row-batch-list.h"
#include "runtime/descriptors.h"

namespace impala {

class NljBuilderConfig : public JoinBuilderConfig {
 public:
  DataSink* CreateSink(RuntimeState* state) const override;

  ~NljBuilderConfig() override {}

 protected:
  Status Init(const TDataSink& tsink, const RowDescriptor* input_row_desc,
      FragmentState* state) override;
};

/// Builder for the NestedLoopJoinNode that accumulates the build-side rows for the join.
/// Implements the JoinBuilder and DataSink interfaces but also exposes some methods for
/// direct use by NestedLoopJoinNode.
///
/// The builder will operate in one of two modes depending on the memory ownership of
/// row batches pulled from the child node on the build side. If the row batches own all
/// tuple memory, the non-copying mode is used and row batches are simply accumulated in
/// the builder. If the batches reference tuple data they do not own, the copying mode
/// is used and all data is deep copied into memory owned by the builder. We always
/// use the copying mode for separate build sinks so that the source fragment plan tree
/// can be safely torn down.
class NljBuilder : public JoinBuilder {
 public:
  /// To be used by the NestedLoopJoinNode to create an instance of this sink.
  static NljBuilder* CreateEmbeddedBuilder(
      const RowDescriptor* row_desc, RuntimeState* state, int join_node_id);
  // Factory method for separate builder.
  static NljBuilder* CreateSeparateBuilder(
      TDataSinkId sink_id, const NljBuilderConfig& sink_config, RuntimeState* state);

  ~NljBuilder();

  /// Implementations of DataSink interface methods.
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status Send(RuntimeState* state, RowBatch* batch) override;
  virtual Status FlushFinal(RuntimeState* state) override;
  void Close(RuntimeState* state) override;

  /// Reset the builder to the same state as it was in after calling Open().
  /// Not valid to call on a separate join build.
  void Reset();

  /// Returns the next build batch that should be filled and passed to AddBuildBatch().
  /// Exposed so that NestedLoopJoinNode can bypass the DataSink interface for efficiency.
  inline RowBatch* GetNextEmptyBatch() {
    return build_batch_cache_.GetNextBatch(mem_tracker_.get());
  }

  /// Add a batch to the build side. Does not copy the data, so either resources must
  /// be owned by the batch (or a later batch), or DeepCopyBuildBatches() must be called
  /// before the referenced resources are released.
  /// Exposed so that NestedLoopJoinNode can bypass the DataSink interface for efficiency.
  inline void AddBuildBatch(RowBatch* batch) { input_build_batches_.AddRowBatch(batch); }

  /// Return a pointer to the final list of build batches.
  /// Only valid to call after FlushFinal() has been called. The returned build batches
  /// are not mutated and valid to use until Close() is called on the builder.
  RowBatchList* GetFinalBuildBatches() {
    if (copied_build_batches_.total_num_rows() > 0) {
      DCHECK_EQ(input_build_batches_.total_num_rows(), 0);
      return &copied_build_batches_;
    } else {
      return &input_build_batches_;
    }
  }

  inline RowBatchList* input_build_batches() { return &input_build_batches_; }
  inline RowBatchList* copied_build_batches() { return &copied_build_batches_; }

 private:
  // Constructor for builder embedded in NestedLoopJoinNode.
  NljBuilder(const NljBuilderConfig& sink_config, RuntimeState* state);
  // Constructor for separate builder.
  NljBuilder(
      TDataSinkId sink_id, const NljBuilderConfig& sink_config, RuntimeState* state);

  /// Deep copy all build batches in 'input_build_batches_' to 'copied_build_batches_'.
  /// Resets all the source batches and clears 'input_build_batches_'.
  /// If the memory limit is exceeded while copying batches, returns a
  /// MEM_LIMIT_EXCEEDED status, sets the query status to MEM_LIMIT_EXCEEDED and leave
  /// the row batches to be cleaned up later when the node is closed.
  Status DeepCopyBuildBatches(RuntimeState* state);

  /// Creates and caches RowBatches for the build side. The RowBatch objects are owned
  /// by this cache. The cache helps to avoid creating new RowBatches after a Reset().
  RowBatchCache build_batch_cache_;

  /// List of the input build batches we obtained from the child, which may reference
  /// memory that is owned by the child node.
  RowBatchList input_build_batches_;

  /// List of build batches that were deep copied from 'input_build_batches_' and are
  /// backed by each row batch's pool.
  RowBatchList copied_build_batches_;
};

}

#endif
