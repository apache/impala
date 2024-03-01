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

#include <map>
#include <vector>

#include "common/object-pool.h"
#include "common/status.h"
#include "exec/join-builder.h"

namespace impala {

class IcebergDeleteBuilder;
class RowDescriptor;
class RuntimeState;

/// Iceberg Delete Builder Config class. This has a few extra methods to be used
/// directly by the IcebergDeletePlanNode.
class IcebergDeleteBuilderConfig : public JoinBuilderConfig {
 public:
  DataSink* CreateSink(RuntimeState* state) const override;

  /// Creates an IcebergDeleteBuilder for embedded use within an IcebergDeleteNode.
  IcebergDeleteBuilder* CreateSink(BufferPool::ClientHandle* buffer_pool_client,
      int64_t spillable_buffer_size, int64_t max_row_buffer_size,
      RuntimeState* state) const;

  /// Creates an IcebergDeleteBuilderConfig for embedded use within an
  /// IcebergDeleteNode. Creates the object in the state's object pool. To be
  /// used only by IcebergDeletePlanNode.
  static Status CreateConfig(FragmentState* state, int join_node_id,
      TJoinOp::type join_op, const RowDescriptor* build_row_desc,
      IcebergDeleteBuilderConfig** sink);

  void Close() override;

  ~IcebergDeleteBuilderConfig() override {}

  const RowDescriptor* build_row_desc_;

 protected:
  /// Initialization for separate sink.
  Status Init(const TDataSink& tsink, const RowDescriptor* input_row_desc,
      FragmentState* state) override;

 private:
  /// Helper method used by CreateConfig() to initialize embedded builder.
  /// 'tsink' does not need to be initialized by the caller - all values to be used are
  /// passed in as arguments and this function fills in required fields in 'tsink'.
  Status Init(FragmentState* state, int join_node_id, TJoinOp::type join_op,
      const RowDescriptor* build_row_desc, TDataSink* tsink);
};

/// The build side for the IcebergDeleteNode. Processed the scanned data from delete
/// files, and stores them in unordered_map<file_path, ordered vector of row ids> to allow
/// fast probing.
///
/// Unlike the PartitionedHashJoin, there is only one mode:
///
///   Directed: each fragment will only receive delete records that apply to data files
///   processed by this executor
///
/// Shared Build
/// ------------
/// A separate builder can be shared between multiple IcebergDeleteNodes.
class IcebergDeleteBuilder : public JoinBuilder {
 public:
  // Constructor for separate join build.
  IcebergDeleteBuilder(TDataSinkId sink_id, const IcebergDeleteBuilderConfig& sink_config,
      RuntimeState* state);
  // Constructor for join builder embedded in a IcebergDeleteNode. Shares
  // 'buffer_pool_client' with the parent node and inherits buffer sizes from
  // the parent node.
  IcebergDeleteBuilder(const IcebergDeleteBuilderConfig& sink_config,
      BufferPool::ClientHandle* buffer_pool_client, int64_t spillable_buffer_size,
      int64_t max_row_buffer_size, RuntimeState* state);
  ~IcebergDeleteBuilder();

  // Checks distribution mode and collects the processed data files' file path in case
  // of broadcast mode.
  Status CalculateDataFiles();

  /// Implementations of DataSink interface methods.
  Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;
  Status Open(RuntimeState* state) override;
  Status Send(RuntimeState* state, RowBatch* batch) override;
  Status FlushFinal(RuntimeState* state) override;
  void Close(RuntimeState* state) override;

  /// Reset the builder the same state as it was in after calling Open().
  /// Not valid to call on a separate join build.
  void Reset(RowBatch* row_batch);

  std::string DebugString() const;

  struct StringValueHashWrapper {
    size_t operator()(const impala::StringValue& str) const {
      return impala::hash_value(str);
    }
  };

  using DeleteRowVector = std::vector<int64_t>;
  using DeleteRowHashTable =
      std::unordered_map<impala::StringValue, DeleteRowVector, StringValueHashWrapper>;

  DeleteRowHashTable& deleted_rows() { return deleted_rows_; }

 private:
  /// Reads the rows in build_batch and collects them into delete_hash_.
  Status ProcessBuildBatch(RuntimeState* state, RowBatch* build_batch);

  /// Helper method for Send() that does the actual work apart from updating the
  /// counters.
  Status AddBatch(RuntimeState* state, RowBatch* build_batch);

  /// Helper method for FlushFinal() that does the actual work.
  Status FinalizeBuild(RuntimeState* state);

  RuntimeState* const runtime_state_;

  /// Pool for objects with same lifetime as builder.
  ObjectPool obj_pool_;

  // Runtime profile for this node. Owned by the QueryState's ObjectPool.
  RuntimeProfile* const runtime_profile_;

  // Measuring the time took to sort row ids
  RuntimeProfile::Counter* position_sort_timer_;

  // Specification of iceberg delete files allows to optimize for data extraction
  const RowDescriptor* build_row_desc_;
  int file_path_offset_;
  int pos_offset_;

  // Use the length of a cache line as initial capacity
  static constexpr size_t INITIAL_DELETE_VECTOR_CAPACITY = 8;

  // Stores {file_path: ordered row ids vector}
  DeleteRowHashTable deleted_rows_;
};
} // namespace impala
