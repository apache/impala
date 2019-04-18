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

#include "exec/sort-node.h"

#include "exec/exec-node-util.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/sorted-run-merger.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

SortNode::SortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    offset_(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
    sorter_(NULL),
    num_rows_skipped_(0) {
}

SortNode::~SortNode() {
}

Status SortNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  const TSortInfo& tsort_info = tnode.sort_node.sort_info;
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));
  RETURN_IF_ERROR(ScalarExpr::Create(tsort_info.ordering_exprs, row_descriptor_,
      state, &ordering_exprs_));
  DCHECK(tsort_info.__isset.sort_tuple_slot_exprs);
  RETURN_IF_ERROR(ScalarExpr::Create(tsort_info.sort_tuple_slot_exprs,
      *child(0)->row_desc(), state, &sort_tuple_exprs_));
  is_asc_order_ = tnode.sort_node.sort_info.is_asc_order;
  nulls_first_ = tnode.sort_node.sort_info.nulls_first;
  runtime_profile()->AddInfoString("SortType", "Total");
  return Status::OK();
}

Status SortNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  sorter_.reset(new Sorter(ordering_exprs_, is_asc_order_, nulls_first_,
      sort_tuple_exprs_, &row_descriptor_, mem_tracker(), buffer_pool_client(),
      resource_profile_.spillable_buffer_size, runtime_profile(), state, id(), true));
  RETURN_IF_ERROR(sorter_->Prepare(pool_));
  DCHECK_GE(resource_profile_.min_reservation, sorter_->ComputeMinReservation());
  state->CheckAndAddCodegenDisabledMessage(runtime_profile());
  return Status::OK();
}

void SortNode::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  ExecNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  Status codegen_status = sorter_->Codegen(state);
  runtime_profile()->AddCodegenMsg(codegen_status.ok(), codegen_status);
}

Status SortNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  // Claim reservation after the child has been opened to reduce the peak reservation
  // requirement.
  if (!buffer_pool_client()->is_registered()) {
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }
  RETURN_IF_ERROR(sorter_->Open());
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  // The child has been opened and the sorter created. Sort the input.
  // The final merge is done on-demand as rows are requested in GetNext().
  RETURN_IF_ERROR(SortInput(state));
  return Status::OK();
}

Status SortNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  } else {
    *eos = false;
  }

  if (returned_buffer_) {
    // If the Sorter returned a buffer on the last call to GetNext(), we might have an
    // opportunity to release memory. Release reservation, unless it might be needed
    // for the next subplan iteration or merging spilled runs.
    returned_buffer_ = false;
    if (!IsInSubplan() && !sorter_->HasSpilledRuns()) {
      DCHECK(!buffer_pool_client()->has_unpinned_pages());
      Status status = ReleaseUnusedReservation();
      DCHECK(status.ok()) << "Should not fail - no runs were spilled so no pages are "
                          << "unpinned. " << status.GetDetail();
    }
  }

  DCHECK_EQ(row_batch->num_rows(), 0);
  RETURN_IF_ERROR(sorter_->GetNext(row_batch, eos));
  while ((num_rows_skipped_ < offset_)) {
    num_rows_skipped_ += row_batch->num_rows();
    // Throw away rows in the output batch until the offset is skipped.
    int rows_to_keep = num_rows_skipped_ - offset_;
    if (rows_to_keep > 0) {
      row_batch->CopyRows(0, row_batch->num_rows() - rows_to_keep, rows_to_keep);
      row_batch->set_num_rows(rows_to_keep);
    } else {
      row_batch->set_num_rows(0);
    }
    if (rows_to_keep > 0 || *eos) break;
    RETURN_IF_ERROR(sorter_->GetNext(row_batch, eos));
  }

  returned_buffer_ = row_batch->num_buffers() > 0;
  CheckLimitAndTruncateRowBatchIfNeeded(row_batch, eos);

  COUNTER_SET(rows_returned_counter_, rows_returned());

  return Status::OK();
}

Status SortNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  num_rows_skipped_ = 0;
  if (sorter_.get() != NULL) sorter_->Reset();
  return ExecNode::Reset(state, row_batch);
}

void SortNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (sorter_ != nullptr) sorter_->Close(state);
  sorter_.reset();
  ScalarExpr::Close(ordering_exprs_);
  ScalarExpr::Close(sort_tuple_exprs_);
  ExecNode::Close(state);
}

void SortNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "SortNode(" << ScalarExpr::DebugString(ordering_exprs_);
  for (int i = 0; i < is_asc_order_.size(); ++i) {
    *out << (i > 0 ? " " : "")
         << (is_asc_order_[i] ? "asc" : "desc")
         << " nulls " << (nulls_first_[i] ? "first" : "last");
  }
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

Status SortNode::SortInput(RuntimeState* state) {
  RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
  bool eos;
  do {
    RETURN_IF_ERROR(child(0)->GetNext(state, &batch, &eos));
    RETURN_IF_ERROR(sorter_->AddBatch(&batch));
    batch.Reset();
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
  } while(!eos);

  // Unless we are inside a subplan expecting to call Open()/GetNext() on the child
  // again, the child can be closed at this point to release resources.
  if (!IsInSubplan()) child(0)->Close(state);

  RETURN_IF_ERROR(sorter_->InputDone());
  return Status::OK();
}

}
