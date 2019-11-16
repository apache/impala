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

#include "exec/cardinality-check-node.h"
#include "exec/exec-node-util.h"
#include "gen-cpp/PlanNodes_types.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

Status CardinalityCheckPlanNode::CreateExecNode(
    RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new CardinalityCheckNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

CardinalityCheckNode::CardinalityCheckNode(
    ObjectPool* pool, const CardinalityCheckPlanNode& pnode, const DescriptorTbl& descs)
    : ExecNode(pool, pnode, descs),
      display_statement_(pnode.tnode_->cardinality_check_node.display_statement) {
}

Status CardinalityCheckNode::Prepare(RuntimeState* state) {
  DCHECK(conjuncts_.empty());
  DCHECK_EQ(limit_, 1);

  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  return Status::OK();
}

Status CardinalityCheckNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  row_batch_.reset(new RowBatch(row_desc(), 1, mem_tracker()));

  // Read rows from the child, raise error if there are more rows than one
  RowBatch child_batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
  bool child_eos = false;
  int rows_collected = 0;
  do {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    RETURN_IF_ERROR(child(0)->GetNext(state, &child_batch, &child_eos));

    rows_collected += child_batch.num_rows();
    if (rows_collected > 1) {
      return Status(Substitute("Subquery must not return more than one row: $0",
          display_statement_));
    }
    if (child_batch.num_rows() != 0) child_batch.DeepCopyTo(row_batch_.get());
    child_batch.Reset();
  } while (!child_eos);

  DCHECK(rows_collected == 0 || rows_collected == 1);

  // If we are inside a subplan we can expect a call to Open()/GetNext()
  // on the child again.
  if (!IsInSubplan()) child(0)->Close(state);
  return Status::OK();
}

Status CardinalityCheckNode::GetNext(
    RuntimeState* state, RowBatch* output_row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  DCHECK_LE(row_batch_->num_rows(), 1);

  if (row_batch_->num_rows() == 1) {
    TupleRow* src_row = row_batch_->GetRow(0);
    TupleRow* dst_row = output_row_batch->GetRow(output_row_batch->AddRow());
    output_row_batch->CopyRow(src_row, dst_row);
    output_row_batch->CommitLastRow();
    row_batch_->TransferResourceOwnership(output_row_batch);
    SetNumRowsReturned(1);
    COUNTER_SET(rows_returned_counter_, rows_returned());
  }
  *eos = true;
  row_batch_->Reset();
  return Status::OK();
}

Status CardinalityCheckNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  row_batch_->Reset();
  return ExecNode::Reset(state, row_batch);
}

void CardinalityCheckNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  // Need to call destructor to release resources before calling ExecNode::Close().
  row_batch_.reset();
  ExecNode::Close(state);
}

}
