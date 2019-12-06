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

#include "exec/select-node.h"

#include "codegen/llvm-codegen.h"
#include "exec/exec-node-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "gen-cpp/PlanNodes_types.h"
#include "runtime/fragment-state.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

Status SelectPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new SelectNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

SelectNode::SelectNode(
    ObjectPool* pool, const SelectPlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs),
    child_row_batch_(NULL),
    child_row_idx_(0),
    child_eos_(false),
    codegend_copy_rows_fn_(pnode.codegend_copy_rows_fn_) {}

Status SelectNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  return Status::OK();
}

void SelectPlanNode::Codegen(FragmentState* state) {
  DCHECK(state->ShouldCodegen());
  PlanNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  AddCodegenStatus(CodegenCopyRows(state));
}

Status SelectPlanNode::CodegenCopyRows(FragmentState* state) {
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != nullptr);
  llvm::Function* copy_rows_fn =
      codegen->GetFunction(IRFunction::SELECT_NODE_COPY_ROWS, true);
  DCHECK(copy_rows_fn != nullptr);

  llvm::Function* eval_conjuncts_fn;
  RETURN_IF_ERROR(
      ExecNode::CodegenEvalConjuncts(codegen, conjuncts_, &eval_conjuncts_fn));

  int replaced = codegen->ReplaceCallSites(copy_rows_fn, eval_conjuncts_fn,
      "EvalConjuncts");
  DCHECK_REPLACE_COUNT(replaced, 1);
  copy_rows_fn = codegen->FinalizeFunction(copy_rows_fn);
  if (copy_rows_fn == nullptr) return Status("Failed to finalize CopyRows().");
  codegen->AddFunctionToJit(copy_rows_fn, &codegend_copy_rows_fn_);
  return Status::OK();
}

Status SelectNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  child_row_batch_.reset(
      new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
  return Status::OK();
}

Status SelectNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  // start (or continue) consuming row batches from child
  do {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    if (child_row_batch_->num_rows() == 0) {
      // Fetch rows from child if either child row batch has been
      // consumed completely or it is empty.
      RETURN_IF_ERROR(child(0)->GetNext(state, child_row_batch_.get(), &child_eos_));
    }

    SelectPlanNode::CopyRowsFn copy_rows_fn = codegend_copy_rows_fn_.load();
    if (copy_rows_fn != nullptr) {
      copy_rows_fn(this, row_batch);
    } else {
      CopyRows(row_batch);
    }
    COUNTER_SET(rows_returned_counter_, rows_returned());
    *eos = ReachedLimit()
        || (child_row_idx_ == child_row_batch_->num_rows() && child_eos_);
    if (*eos || child_row_idx_ == child_row_batch_->num_rows()) {
      child_row_idx_ = 0;
      child_row_batch_->TransferResourceOwnership(row_batch);
      child_row_batch_->Reset();
    }
  } while (!*eos && !row_batch->AtCapacity());
  return Status::OK();
}

Status SelectNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  child_row_batch_->TransferResourceOwnership(row_batch);
  child_row_idx_ = 0;
  child_eos_ = false;
  return ExecNode::Reset(state, row_batch);
}

void SelectNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  child_row_batch_.reset();
  ExecNode::Close(state);
}

}
