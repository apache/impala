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

#include "exec/exec-node-util.h"
#include "runtime/fragment-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/sorted-run-merger.h"
#include "runtime/sorter-internal.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

Status PartialSortPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  DCHECK(!tnode.sort_node.__isset.offset || tnode.sort_node.offset == 0);
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  const TSortInfo& tsort_info = tnode.sort_node.sort_info;
  RETURN_IF_ERROR(ScalarExpr::Create(
      tsort_info.ordering_exprs, *row_descriptor_, state, &ordering_exprs_));
  DCHECK(tsort_info.__isset.sort_tuple_slot_exprs);
  RETURN_IF_ERROR(ScalarExpr::Create(tsort_info.sort_tuple_slot_exprs,
      *children_[0]->row_descriptor_, state, &sort_tuple_slot_exprs_));
  row_comparator_config_ =
      state->obj_pool()->Add(new TupleRowComparatorConfig(tsort_info, ordering_exprs_));
  state->CheckAndAddCodegenDisabledMessage(codegen_status_msgs_);
  return Status::OK();
}

void PartialSortPlanNode::Close() {
  ScalarExpr::Close(ordering_exprs_);
  ScalarExpr::Close(sort_tuple_slot_exprs_);
  PlanNode::Close();
}

Status PartialSortPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new PartialSortNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

PartialSortNode::PartialSortNode(
    ObjectPool* pool, const PartialSortPlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs),
    sort_tuple_exprs_(pnode.sort_tuple_slot_exprs_),
    tuple_row_comparator_config_(*pnode.row_comparator_config_),
    sorter_(nullptr),
    input_batch_index_(0),
    input_eos_(false),
    sorter_eos_(true) {
  runtime_profile()->AddInfoString("SortType", "Partial");
  child_get_next_timer_ = ADD_SUMMARY_STATS_TIMER(runtime_profile(), "ChildGetNextTime");
}

PartialSortNode::~PartialSortNode() {
  DCHECK(input_batch_.get() == nullptr);
}

Status PartialSortNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  const PartialSortPlanNode& pnode = static_cast<const PartialSortPlanNode&>(plan_node_);
  sorter_.reset(
      new Sorter(tuple_row_comparator_config_, sort_tuple_exprs_, &row_descriptor_,
          mem_tracker(), buffer_pool_client(), resource_profile_.spillable_buffer_size,
          runtime_profile(), state, label(), false, pnode.codegend_sort_helper_fn_));
  RETURN_IF_ERROR(sorter_->Prepare(pool_));
  DCHECK_GE(resource_profile_.min_reservation, sorter_->ComputeMinReservation());
  input_batch_.reset(
      new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
  return Status::OK();
}

void PartialSortPlanNode::Codegen(FragmentState* state) {
  DCHECK(state->ShouldCodegen());
  PlanNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  llvm::Function* compare_fn = nullptr;
  AddCodegenStatus(row_comparator_config_->Codegen(state, &compare_fn));
  AddCodegenStatus(
      Sorter::TupleSorter::Codegen(state, compare_fn,
          row_descriptor_->tuple_descriptors()[0]->byte_size(),
          &codegend_sort_helper_fn_));
}

Status PartialSortNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  if (!buffer_pool_client()->is_registered()) {
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }
  return Status::OK();
}

Status PartialSortNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
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
    IncrementNumRowsReturned(row_batch->num_rows());
    COUNTER_SET(rows_returned_counter_, rows_returned());
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
      MonotonicStopWatch timer;
      timer.Start();
      Status status = child(0)->GetNext(state, input_batch_.get(), &input_eos_);
      timer.Stop();
      RETURN_IF_ERROR(status);
      child_get_next_timer_->UpdateCounter(timer.ElapsedTime());
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

  IncrementNumRowsReturned(row_batch->num_rows());
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

Status PartialSortNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  DCHECK(false) << "PartialSortNode cannot be part of a subplan.";
  return Status("Cannot reset partial sort");
}

void PartialSortNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  input_batch_.reset();
  child(0)->Close(state);
  if (sorter_ != nullptr) sorter_->Close(state);
  sorter_.reset();
  ExecNode::Close(state);
}

void PartialSortNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  const PartialSortPlanNode& pnode = static_cast<const PartialSortPlanNode&>(plan_node_);
  const TSortInfo& tsort_info = pnode.tnode_->sort_node.sort_info;
  *out << "PartialSortNode(" << ScalarExpr::DebugString(pnode.ordering_exprs_);
  for (int i = 0; i < tsort_info.is_asc_order.size(); ++i) {
    *out << (i > 0 ? " " : "") << (tsort_info.is_asc_order[i] ? "asc" : "desc")
         << " nulls " << (tsort_info.nulls_first[i] ? "first" : "last");
  }
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
}
