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

#include "common/object-pool.h"
#include "exec/hdfs-table-sink.h"
#include "exec/multi-table-sink.h"
#include "runtime/fragment-state.h"
#include "runtime/runtime-state.h"

namespace impala {

Status MultiTableSinkConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, FragmentState* state) {
  RETURN_IF_ERROR(DataSinkConfig::Init(tsink, input_row_desc, state));
  for (const TDataSink& child_sink : tsink.child_data_sinks) {
    // We only allow table sinks for now.
    DCHECK(child_sink.__isset.table_sink);
    DataSinkConfig* data_sink_config;
    RETURN_IF_ERROR(DataSinkConfig::CreateConfig(child_sink, input_row_desc,
        state, &data_sink_config));
    DCHECK(data_sink_config != nullptr);
    table_sink_configs_.push_back(static_cast<TableSinkBaseConfig*>(data_sink_config));
  }
  return Status::OK();
}

DataSink* MultiTableSinkConfig::CreateSink(RuntimeState* state) const {
  TDataSinkId sink_id = state->fragment().idx;
  return state->obj_pool()->Add(
    new MultiTableSink(sink_id, *this, *tsink_, state));
}

MultiTableSink::MultiTableSink(TDataSinkId sink_id,
    const MultiTableSinkConfig& sink_config, const TDataSink& dsink,
    RuntimeState* state) : DataSink(sink_id, sink_config, "MultiTableSink", state) {
  for (TableSinkBaseConfig* tbl_sink_config : sink_config.table_sink_configs()) {
    TableSinkBase* tsink_base =
        DCHECK_NOTNULL(dynamic_cast<TableSinkBase*>(tbl_sink_config->CreateSink(state)));
    table_sinks_.push_back(tsink_base);
    profile()->AddChild(tsink_base->profile());
  }
}

Status MultiTableSink::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  for (TableSinkBase* tsink : table_sinks_) {
    RETURN_IF_ERROR(tsink->Prepare(state, parent_mem_tracker));
  }
  return Status::OK();
}

Status MultiTableSink::Open(RuntimeState* state) {
  RETURN_IF_ERROR(DataSink::Open(state));
  for (TableSinkBase* tsink : table_sinks_) {
    RETURN_IF_ERROR(tsink->Open(state));
  }
  return Status::OK();
}

Status MultiTableSink::Send(RuntimeState* state, RowBatch* batch) {
  for (TableSinkBase* tsink : table_sinks_) {
    RETURN_IF_ERROR(tsink->Send(state, batch));
  }
  return Status::OK();
}

Status MultiTableSink::FlushFinal(RuntimeState* state) {
  DCHECK(!closed_);
  for (TableSinkBase* tsink : table_sinks_) {
    RETURN_IF_ERROR(tsink->FlushFinal(state));
  }
  return Status::OK();
}

void MultiTableSink::Close(RuntimeState* state) {
  for (TableSinkBase* tsink : table_sinks_) {
    tsink->Close(state);
  }
  DataSink::Close(state);
  DCHECK(closed_);
}

}
