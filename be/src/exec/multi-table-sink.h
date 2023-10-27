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

#include <vector>

#include "exec/data-sink.h"
#include "exec/table-sink-base.h"

namespace impala {

class RowDescriptor;
class RuntimeState;

/// Configuration for creating multi table sink objects. A multi table sink has multiple
/// child table sinks, so this holds the configurations of all the sinks.
class MultiTableSinkConfig : public DataSinkConfig {
 public:
  /// Creates a new MultiTableSink object.
  DataSink* CreateSink(RuntimeState* state) const override;

  /// Returns the table sink configs of the child sinks.
  const std::vector<TableSinkBaseConfig*> table_sink_configs() const {
    return table_sink_configs_;
  }

  ~MultiTableSinkConfig() override {}

 protected:
  Status Init(const TDataSink& tsink, const RowDescriptor* input_row_desc,
      FragmentState* state) override;
 private:
   std::vector<TableSinkBaseConfig*> table_sink_configs_;
};

/// MultiTableSink has multiple child table sink objects. It sends the received row
/// batches to all of its children.
class MultiTableSink : public DataSink {
 public:
  MultiTableSink(TDataSinkId sink_id, const MultiTableSinkConfig& sink_config,
      const TDataSink& dsink, RuntimeState* state);

  //////////////////////////////////////
  /// BEGIN: Following methods just delegate calls to the child sinks in 'table_sinks_'.
  Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;

  Status Open(RuntimeState* state) override;

  Status Send(RuntimeState* state, RowBatch* batch) override;

  Status FlushFinal(RuntimeState* state) override;

  void Close(RuntimeState* state) override;
  /// END: Methods above just delegate calls to the child sinks in 'table_sinks_'.
  //////////////////////////////////////
 private:
  std::vector<TableSinkBase*> table_sinks_;
};

}
