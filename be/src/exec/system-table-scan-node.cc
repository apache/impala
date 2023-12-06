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

#include "system-table-scan-node.h"

#include "exec/exec-node.inline.h"
#include "exec/system-table-scanner.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"

#include "common/names.h"

namespace impala {

Status SystemTableScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(
      plan_node().tnode_->system_table_scan_node.tuple_id);
  DCHECK(tuple_desc_ != nullptr);
  return Status::OK();
}

Status SystemTableScanNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  RETURN_IF_ERROR(SystemTableScanner::CreateScanner(state, runtime_profile(),
      plan_node().tnode_->system_table_scan_node.table_name, &scanner_));
  RETURN_IF_ERROR(scanner_->Open());
  return Status::OK();
}

Status SystemTableScanNode::MaterializeNextTuple(MemPool* tuple_pool, Tuple* tuple) {
  tuple->Init(tuple_desc_->byte_size());
  RETURN_IF_ERROR(scanner_->MaterializeNextTuple(tuple_pool, tuple, tuple_desc_));
  return Status::OK();
}

Status SystemTableScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  int64_t tuple_buffer_size;
  uint8_t* tuple_mem;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buffer_size, &tuple_mem));

  SCOPED_TIMER(materialize_tuple_timer());

  // copy rows until we hit the limit/capacity or until we exhaust input batch
  while (!ReachedLimit() && !row_batch->AtCapacity() && !scanner_->eos()) {
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);
    RETURN_IF_ERROR(MaterializeNextTuple(row_batch->tuple_data_pool(), tuple));
    TupleRow* tuple_row = row_batch->GetRow(row_batch->AddRow());
    tuple_row->SetTuple(0, tuple);

    if (ExecNode::EvalConjuncts(
            conjunct_evals_.data(), conjunct_evals_.size(), tuple_row)) {
      row_batch->CommitLastRow();
      tuple_mem += tuple_desc_->byte_size();
      IncrementNumRowsReturned(1);
    }
  }
  COUNTER_SET(rows_returned_counter_, rows_returned());
  *eos = ReachedLimit() || scanner_->eos();
  return Status::OK();
}

Status SystemTableScanNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  DCHECK(false) << "NYI";
  return Status("NYI");
}

void SystemTableScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  scanner_.reset();
  ScanNode::Close(state);
}

} // namespace impala
