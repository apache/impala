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

#include "exec/partial-sort-node.h"

#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/sorted-run-merger.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

PartialSortNode::PartialSortNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    sorter_(nullptr),
    input_batch_index_(0),
    input_eos_(false),
    sorter_eos_(true) {}

PartialSortNode::~PartialSortNode() {
  DCHECK(input_batch_.get() == nullptr);
}

Status PartialSortNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  DCHECK(!tnode.sort_node.__isset.offset || tnode.sort_node.offset == 0);
  DCHECK(limit_ == -1);
  const TSortInfo& tsort_info = tnode.sort_node.sort_info;
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));
  RETURN_IF_ERROR(ScalarExpr::Create(
      tsort_info.ordering_exprs, row_descriptor_, state, &ordering_exprs_));
  DCHECK(tsort_info.__isset.sort_tuple_slot_exprs);
  RETURN_IF_ERROR(ScalarExpr::Create(tsort_info.sort_tuple_slot_exprs,
      *child(0)->row_desc(), state, &sort_tuple_exprs_));
  is_asc_order_ = tnode.sort_node.sort_info.is_asc_order;
  nulls_first_ = tnode.sort_node.sort_info.nulls_first;
  runtime_profile()->AddInfoString("SortType", "Partial");
  return Status::OK();
}

Status PartialSortNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  sorter_.reset(new Sorter(ordering_exprs_, is_asc_order_, nulls_first_,
      sort_tuple_exprs_, &row_descriptor_, mem_tracker(), &buffer_pool_client_,
      resource_profile_.spillable_buffer_size, runtime_profile(), state, id(), false));
  RETURN_IF_ERROR(sorter_->Prepare(pool_));
  DCHECK_GE(resource_profile_.min_reservation, sorter_->ComputeMinReservation());
  AddCodegenDisabledMessage(state);
  input_batch_.reset(
      new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
  return Status::OK();
}

void PartialSortNode::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  ExecNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  Status codegen_status = sorter_->Codegen(state);
  runtime_profile()->AddCodegenMsg(codegen_status.ok(), codegen_status);
}

Status PartialSortNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  if (!buffer_pool_client_.is_registered()) {
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }
  return Status::OK();
}

Status PartialSortNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  DCHECK_EQ(row_batch->num_rows(), 0);
  if (!sorter_eos_) {
    // There were rows in the current run that didn't fit in the last output batch.
    RETURN_IF_ERROR(sorter_->GetNext(row_batch, &sorter_eos_));
    if (sorter_eos_) {
      sorter_->Reset();
      *eos = input_eos_;
    }
    num_rows_returned_ += row_batch->num_rows();
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
    return Status::OK();
  }

  if (input_eos_) {
    *eos = true;
    return Status::OK();
  }

  DCHECK(sorter_eos_);
  RETURN_IF_ERROR(sorter_->Open());
  do {
    if (input_batch_index_ == input_batch_->num_rows()) {
      input_batch_->Reset();
      input_batch_index_ = 0;
      RETURN_IF_ERROR(child(0)->GetNext(state, input_batch_.get(), &input_eos_));
    }

    int num_processed;
    RETURN_IF_ERROR(
        sorter_->AddBatchNoSpill(input_batch_.get(), input_batch_index_, &num_processed));
    input_batch_index_ += num_processed;
    DCHECK(input_batch_index_ <= input_batch_->num_rows());
    RETURN_IF_ERROR(QueryMaintenance(state));
  } while (input_batch_index_ == input_batch_->num_rows() && !input_eos_);

  RETURN_IF_ERROR(sorter_->InputDone());
  RETURN_IF_ERROR(sorter_->GetNext(row_batch, &sorter_eos_));
  if (sorter_eos_) {
    sorter_->Reset();
    *eos = input_eos_;
  }

  num_rows_returned_ += row_batch->num_rows();
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

Status PartialSortNode::Reset(RuntimeState* state) {
  DCHECK(false) << "PartialSortNode cannot be part of a subplan.";
  return ExecNode::Reset(state);
}

void PartialSortNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  input_batch_.reset();
  child(0)->Close(state);
  if (sorter_ != nullptr) sorter_->Close(state);
  sorter_.reset();
  ScalarExpr::Close(ordering_exprs_);
  ScalarExpr::Close(sort_tuple_exprs_);
  ExecNode::Close(state);
}

void PartialSortNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "PartialSortNode(" << ScalarExpr::DebugString(ordering_exprs_);
  for (int i = 0; i < is_asc_order_.size(); ++i) {
    *out << (i > 0 ? " " : "") << (is_asc_order_[i] ? "asc" : "desc") << " nulls "
         << (nulls_first_[i] ? "first" : "last");
  }
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
}
