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

#include "exec/iceberg-merge-sink.h"

#include "exec/data-sink.h"
#include "exec/table-sink-base.h"
#include "exprs/expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "gen-cpp/DataSinks_types.h"
#include "runtime/descriptors.h"
#include "runtime/fragment-state.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"

namespace impala {

Status IcebergMergeSinkConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, FragmentState* state) {
  RETURN_IF_ERROR(DataSinkConfig::Init(tsink, input_row_desc, state));
  DCHECK(tsink.child_data_sinks.size() == 2);
  DCHECK(tsink.child_data_sinks[0].__isset.table_sink);
  DCHECK(tsink.child_data_sinks[1].__isset.table_sink);
  DCHECK(tsink.child_data_sinks[0].table_sink.action == TSinkAction::INSERT);
  DCHECK(tsink.child_data_sinks[1].table_sink.action == TSinkAction::DELETE);

  const TDataSink& insert_sink = tsink.child_data_sinks[0];
  RETURN_IF_ERROR(DataSinkConfig::CreateConfig(insert_sink, input_row_desc, state,
      reinterpret_cast<DataSinkConfig**>(&insert_sink_config_)));
  DCHECK(insert_sink_config_ != nullptr);

  const TDataSink& delete_sink = tsink.child_data_sinks[1];
  RETURN_IF_ERROR(DataSinkConfig::CreateConfig(delete_sink, input_row_desc, state,
      reinterpret_cast<DataSinkConfig**>(&delete_sink_config_)));
  DCHECK(delete_sink_config_ != nullptr);

  DCHECK(tsink.__isset.output_exprs);
  DCHECK(tsink.output_exprs.size() == 1);
  RETURN_IF_ERROR(
      ScalarExpr::Create(tsink.output_exprs[0], *input_row_desc, state, &merge_action_));

  return Status::OK();
}

DataSink* IcebergMergeSinkConfig::CreateSink(RuntimeState* state) const {
  TDataSinkId sink_id = state->fragment().idx;
  return state->obj_pool()->Add(new IcebergMergeSink(sink_id, *this, *tsink_, state));
}

IcebergMergeSink::IcebergMergeSink(TDataSinkId sink_id,
    const IcebergMergeSinkConfig& sink_config, const TDataSink& dsink,
    RuntimeState* state)
  : DataSink(sink_id, sink_config, "IcebergMergeSink", state),
    insert_sink_(nullptr),
    delete_sink_(nullptr),
    merge_action_(sink_config.merge_action()) {
  auto insert_sink_config = sink_config.insert_sink_config();
  insert_sink_ =
      DCHECK_NOTNULL(dynamic_cast<TableSinkBase*>(insert_sink_config->CreateSink(state)));
  auto delete_sink_config = sink_config.delete_sink_config();
  delete_sink_ =
      DCHECK_NOTNULL(dynamic_cast<TableSinkBase*>(delete_sink_config->CreateSink(state)));
  profile()->AddChild(insert_sink_->profile());
  profile()->AddChild(delete_sink_->profile());
}

Status IcebergMergeSink::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  merge_action_evaluator_ = output_expr_evals_[0];
  RETURN_IF_ERROR(insert_sink_->Prepare(state, parent_mem_tracker));
  RETURN_IF_ERROR(delete_sink_->Prepare(state, parent_mem_tracker));
  return Status::OK();
}

Status IcebergMergeSink::Open(RuntimeState* state) {
  RETURN_IF_ERROR(DataSink::Open(state));
  RETURN_IF_ERROR(insert_sink_->Open(state));
  RETURN_IF_ERROR(delete_sink_->Open(state));
  return Status::OK();
}

Status IcebergMergeSink::Send(RuntimeState* state, RowBatch* batch) {
  RowBatch delete_rows(this->row_desc_, batch->capacity(), mem_tracker());
  RowBatch insert_rows(this->row_desc_, batch->capacity(), mem_tracker());
  FOREACH_ROW(batch, 0, iter) {
    auto row = iter.Get();
    // Reading the merge action for the row
    auto merge_action = merge_action_evaluator_->GetTinyIntVal(row).val;
    if (merge_action & TIcebergMergeSinkAction::DELETE) {
      AddRow(delete_rows, row);
    }
    if (merge_action & TIcebergMergeSinkAction::DATA) {
      AddRow(insert_rows, row);
    }
  }
  RETURN_IF_ERROR(insert_sink_->Send(state, &insert_rows));
  RETURN_IF_ERROR(delete_sink_->Send(state, &delete_rows));
  return Status::OK();
}

void IcebergMergeSink::AddRow(RowBatch& output_batch, TupleRow* row) {
  auto index = output_batch.AddRow();
  auto output_row = output_batch.GetRow(index);
  output_batch.CopyRow(row, output_row);
  output_batch.CommitLastRow();
}

Status IcebergMergeSink::FlushFinal(RuntimeState* state) {
  DCHECK(!closed_);
  RETURN_IF_ERROR(insert_sink_->FlushFinal(state));
  RETURN_IF_ERROR(delete_sink_->FlushFinal(state));
  return Status::OK();
}

void IcebergMergeSink::Close(RuntimeState* state) {
  insert_sink_->Close(state);
  delete_sink_->Close(state);
  DataSink::Close(state);
  DCHECK(closed_);
}

} // namespace impala