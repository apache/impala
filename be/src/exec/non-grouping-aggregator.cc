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

#include "exec/non-grouping-aggregator.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "exec/exec-node.h"
#include "exec/exec-node.inline.h"
#include "exprs/agg-fn-evaluator.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/fragment-state.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

NonGroupingAggregatorConfig::NonGroupingAggregatorConfig(
    const TAggregator& taggregator, FragmentState* state, PlanNode* pnode, int agg_idx)
  : AggregatorConfig(taggregator, state, pnode, agg_idx) {}

void NonGroupingAggregatorConfig::Codegen(FragmentState* state) {
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != nullptr);
  TPrefetchMode::type prefetch_mode = state->query_options().prefetch_mode;
  Status status = CodegenAddBatchImpl(codegen, prefetch_mode);
  codegen_status_msg_ = FragmentState::GenerateCodegenMsg(status.ok(), status);
}

NonGroupingAggregator::NonGroupingAggregator(
    ExecNode* exec_node, ObjectPool* pool, const NonGroupingAggregatorConfig& config)
  : Aggregator(
        exec_node, pool, config, Substitute("NonGroupingAggregator $0", config.agg_idx_)),
    add_batch_impl_fn_(config.add_batch_impl_fn_) {}

Status NonGroupingAggregator::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(Aggregator::Prepare(state));
  singleton_tuple_pool_.reset(new MemPool(mem_tracker_.get()));
  return Status::OK();
}

Status NonGroupingAggregator::Open(RuntimeState* state) {
  RETURN_IF_ERROR(Aggregator::Open(state));

  // Create the single output tuple for this non-grouping agg. This must happen after
  // opening the aggregate evaluators.
  singleton_output_tuple_ =
      ConstructSingletonOutputTuple(agg_fn_evals_, singleton_tuple_pool_.get());
  // Check for failures during AggFnEvaluator::Init().
  RETURN_IF_ERROR(state->GetQueryStatus());
  singleton_output_tuple_returned_ = false;

  return Status::OK();
}

Status NonGroupingAggregator::GetNext(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(QueryMaintenance(state));
  // There was no grouping, so evaluate the conjuncts and return the single result row.
  // We allow calling GetNext() after eos, so don't return this row again.
  if (!singleton_output_tuple_returned_) GetSingletonOutput(row_batch);
  singleton_output_tuple_returned_ = true;
  *eos = true;
  return Status::OK();
}

void NonGroupingAggregator::GetSingletonOutput(RowBatch* row_batch) {
  int row_idx = row_batch->AddRow();
  TupleRow* row = row_batch->GetRow(row_idx);
  row_batch->ClearRow(row);
  // The output row batch may reference memory allocated by Serialize() or Finalize(),
  // allocating that memory directly from the row batch's pool means we can safely return
  // the batch.
  vector<ScopedResultsPool> allocate_from_batch_pool =
      ScopedResultsPool::Create(agg_fn_evals_, row_batch->tuple_data_pool());
  Tuple* output_tuple = GetOutputTuple(
      agg_fn_evals_, singleton_output_tuple_, row_batch->tuple_data_pool());
  row->SetTuple(agg_idx_, output_tuple);
  if (ExecNode::EvalConjuncts(conjunct_evals_.data(), conjunct_evals_.size(), row)) {
    row_batch->CommitLastRow();
    ++num_rows_returned_;
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  }
  // Keep the current chunk to amortize the memory allocation over a series
  // of Reset()/Open()/GetNext()* calls.
  row_batch->tuple_data_pool()->AcquireData(singleton_tuple_pool_.get(), true);
  // This node no longer owns the memory for singleton_output_tuple_.
  singleton_output_tuple_ = nullptr;
}

void NonGroupingAggregator::Close(RuntimeState* state) {
  if (!singleton_output_tuple_returned_) {
    GetOutputTuple(agg_fn_evals_, singleton_output_tuple_, singleton_tuple_pool_.get());
  }

  if (singleton_tuple_pool_.get() != nullptr) singleton_tuple_pool_->FreeAll();
  // Must be called after singleton_tuple_pool_ is freed, so that mem_tracker_ can be
  // closed.
  Aggregator::Close(state);
}

Status NonGroupingAggregator::AddBatch(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(build_timer_);
  RETURN_IF_ERROR(QueryMaintenance(state));

  NonGroupingAggregatorConfig::AddBatchImplFn add_batch_impl_fn
      = add_batch_impl_fn_.load();
  if (add_batch_impl_fn != nullptr) {
    RETURN_IF_ERROR(add_batch_impl_fn(this, batch));
  } else {
    RETURN_IF_ERROR(AddBatchImpl(batch));
  }

  return Status::OK();
}

Status NonGroupingAggregator::AddBatchStreaming(
    RuntimeState* state, RowBatch* out_batch, RowBatch* child_batch, bool* eos) {
  *eos = true;
  return AddBatch(state, child_batch);
}

Tuple* NonGroupingAggregator::ConstructSingletonOutputTuple(
    const vector<AggFnEvaluator*>& agg_fn_evals, MemPool* pool) {
  Tuple* output_tuple = Tuple::Create(intermediate_tuple_desc_->byte_size(), pool);
  InitAggSlots(agg_fn_evals, output_tuple);
  return output_tuple;
}

string NonGroupingAggregator::DebugString(int indentation_level) const {
  stringstream ss;
  DebugString(indentation_level, &ss);
  return ss.str();
}

void NonGroupingAggregator::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "NonGroupingAggregator("
       << "intermediate_tuple_id=" << intermediate_tuple_id_
       << " output_tuple_id=" << output_tuple_id_ << " needs_finalize=" << needs_finalize_
       << " agg_exprs=" << AggFn::DebugString(agg_fns_);
  *out << ")";
}

Status NonGroupingAggregatorConfig::CodegenAddBatchImpl(
    LlvmCodeGen* codegen, TPrefetchMode::type prefetch_mode) {
  llvm::Function* update_tuple_fn;
  RETURN_IF_ERROR(CodegenUpdateTuple(codegen, &update_tuple_fn));

  // Get the cross compiled update row batch function
  IRFunction::Type ir_fn = IRFunction::NON_GROUPING_AGG_ADD_BATCH_IMPL;
  llvm::Function* add_batch_impl_fn = codegen->GetFunction(ir_fn, true);
  DCHECK(add_batch_impl_fn != nullptr);

  int replaced;
  replaced = codegen->ReplaceCallSites(add_batch_impl_fn, update_tuple_fn, "UpdateTuple");
  DCHECK_GE(replaced, 1);
  add_batch_impl_fn = codegen->FinalizeFunction(add_batch_impl_fn);
  if (add_batch_impl_fn == nullptr) {
    return Status("NonGroupingAggregator::CodegenAddBatchImpl(): codegen'd "
                  "AddBatchImpl() function failed verification, see log");
  }

  codegen->AddFunctionToJit(add_batch_impl_fn, &add_batch_impl_fn_);
  return Status::OK();
}
} // namespace impala
