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

#include "exec/data-sink.h"

namespace impala {

class RowDescriptor;
class RuntimeState;
class ScalarExpr;
class ScalarExprEvaluator;
class TableSinkBaseConfig;
class TupleRow;

/// Configuration for creating Iceberg merge sink objects. A merge sink has two
/// child table sinks, this class holds the configurations for both sinks.
class IcebergMergeSinkConfig : public DataSinkConfig {
 public:
  DataSink* CreateSink(RuntimeState* state) const override;

  IcebergMergeSinkConfig() = default;
  ~IcebergMergeSinkConfig() override = default;
  IcebergMergeSinkConfig(const IcebergMergeSinkConfig& other) = delete;
  IcebergMergeSinkConfig(IcebergMergeSinkConfig&& other) = delete;
  auto operator=(const IcebergMergeSinkConfig& other) = delete;
  auto operator=(IcebergMergeSinkConfig&& other) = delete;

  const TableSinkBaseConfig* delete_sink_config() const { return delete_sink_config_; }
  const TableSinkBaseConfig* insert_sink_config() const { return insert_sink_config_; }

  ScalarExpr* merge_action() const { return merge_action_; }

 protected:
  Status Init(const TDataSink& tsink, const RowDescriptor* input_row_desc,
      FragmentState* state) override;

 private:
  TableSinkBaseConfig* delete_sink_config_{};
  TableSinkBaseConfig* insert_sink_config_{};
  ScalarExpr* merge_action_{};
};

/// IcebergMergeSink has two child table sink objects. It sends the received row
/// to one of the sink objects based on the content of the merge action tuple.
class IcebergMergeSink : public DataSink {
 public:
  IcebergMergeSink(TDataSinkId sink_id, const IcebergMergeSinkConfig& sink_config,
      const TDataSink& dsink, RuntimeState* state);
  Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;
  Status Open(RuntimeState* state) override;
  /// Sends the incoming rows to their respective sinks based on the content of the
  /// merge action tuple. The merge action tuple contains a TINYINT that hold
  /// a TIcebergMergeSinkAction value.
  Status Send(RuntimeState* state, RowBatch* batch) override;
  Status FlushFinal(RuntimeState* state) override;
  void Close(RuntimeState* state) override;
  void AddRow(RowBatch& output_batch, TupleRow* row);

 private:
  TableSinkBase* insert_sink_;
  TableSinkBase* delete_sink_;
  ScalarExpr* merge_action_{};
  ScalarExprEvaluator* merge_action_evaluator_{};
};

} // namespace impala
