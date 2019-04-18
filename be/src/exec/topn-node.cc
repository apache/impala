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

#include "exec/topn-node.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "exec/exec-node-util.h"
#include "exprs/scalar-expr.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using std::priority_queue;
using namespace impala;

TopNNode::TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    offset_(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
    output_tuple_desc_(row_descriptor_.tuple_descriptors()[0]),
    tuple_row_less_than_(NULL),
    tmp_tuple_(NULL),
    tuple_pool_(NULL),
    codegend_insert_batch_fn_(NULL),
    rows_to_reclaim_(0),
    tuple_pool_reclaim_counter_(NULL),
    num_rows_skipped_(0) {
}

Status TopNNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  const TSortInfo& tsort_info = tnode.sort_node.sort_info;
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));
  RETURN_IF_ERROR(ScalarExpr::Create(tsort_info.ordering_exprs, row_descriptor_,
      state, &ordering_exprs_));
  DCHECK(tsort_info.__isset.sort_tuple_slot_exprs);
  RETURN_IF_ERROR(ScalarExpr::Create(tsort_info.sort_tuple_slot_exprs,
      *child(0)->row_desc(), state, &output_tuple_exprs_));
  is_asc_order_ = tnode.sort_node.sort_info.is_asc_order;
  nulls_first_ = tnode.sort_node.sort_info.nulls_first;
  DCHECK_EQ(conjuncts_.size(), 0)
      << "TopNNode should never have predicates to evaluate.";
  runtime_profile()->AddInfoString("SortType", "TopN");
  return Status::OK();
}

Status TopNNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  DCHECK(output_tuple_desc_ != nullptr);
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  tuple_pool_.reset(new MemPool(mem_tracker()));
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(output_tuple_exprs_, state, pool_,
      expr_perm_pool(), expr_results_pool(), &output_tuple_expr_evals_));
  tuple_row_less_than_.reset(
      new TupleRowComparator(ordering_exprs_, is_asc_order_, nulls_first_));
  output_tuple_desc_ = row_descriptor_.tuple_descriptors()[0];
  insert_batch_timer_ = ADD_TIMER(runtime_profile(), "InsertBatchTime");
  state->CheckAndAddCodegenDisabledMessage(runtime_profile());
  tuple_pool_reclaim_counter_ = ADD_COUNTER(runtime_profile(), "TuplePoolReclamations",
      TUnit::UNIT);
  return Status::OK();
}

void TopNNode::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  ExecNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;

  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != NULL);

  // TODO: inline tuple_row_less_than_->Compare()
  Status codegen_status = tuple_row_less_than_->Codegen(state);
  if (codegen_status.ok()) {
    llvm::Function* insert_batch_fn =
        codegen->GetFunction(IRFunction::TOPN_NODE_INSERT_BATCH, true);
    DCHECK(insert_batch_fn != NULL);

    // Generate two MaterializeExprs() functions, one using tuple_pool_ and
    // one with no pool.
    DCHECK(output_tuple_desc_ != NULL);
    llvm::Function* materialize_exprs_tuple_pool_fn;
    llvm::Function* materialize_exprs_no_pool_fn;

    codegen_status = Tuple::CodegenMaterializeExprs(codegen, false,
        *output_tuple_desc_, output_tuple_exprs_,
        true, &materialize_exprs_tuple_pool_fn);

    if (codegen_status.ok()) {
      codegen_status = Tuple::CodegenMaterializeExprs(codegen, false,
          *output_tuple_desc_, output_tuple_exprs_,
          false, &materialize_exprs_no_pool_fn);

      if (codegen_status.ok()) {
        int replaced = codegen->ReplaceCallSites(insert_batch_fn,
            materialize_exprs_tuple_pool_fn, Tuple::MATERIALIZE_EXPRS_SYMBOL);
        DCHECK_REPLACE_COUNT(replaced, 1) << LlvmCodeGen::Print(insert_batch_fn);

        replaced = codegen->ReplaceCallSites(insert_batch_fn,
            materialize_exprs_no_pool_fn, Tuple::MATERIALIZE_EXPRS_NULL_POOL_SYMBOL);
        DCHECK_REPLACE_COUNT(replaced, 1) << LlvmCodeGen::Print(insert_batch_fn);

        insert_batch_fn = codegen->FinalizeFunction(insert_batch_fn);
        DCHECK(insert_batch_fn != NULL);
        codegen->AddFunctionToJit(insert_batch_fn,
            reinterpret_cast<void**>(&codegend_insert_batch_fn_));
      }
    }
  }
  runtime_profile()->AddCodegenMsg(codegen_status.ok(), codegen_status);
}

Status TopNNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(
      tuple_row_less_than_->Open(pool_, state, expr_perm_pool(), expr_results_pool()));
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(output_tuple_expr_evals_, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  // Allocate memory for a temporary tuple.
  tmp_tuple_ = reinterpret_cast<Tuple*>(
      tuple_pool_->Allocate(output_tuple_desc_->byte_size()));

  RETURN_IF_ERROR(child(0)->Open(state));

  // Limit of 0, no need to fetch anything from children.
  if (limit_ != 0) {
    RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
    bool eos;
    do {
      batch.Reset();
      RETURN_IF_ERROR(child(0)->GetNext(state, &batch, &eos));
      {
        SCOPED_TIMER(insert_batch_timer_);
        if (codegend_insert_batch_fn_ != NULL) {
          codegend_insert_batch_fn_(this, &batch);
        } else {
          InsertBatch(&batch);
        }
        if (rows_to_reclaim_ > 2 * (limit_ + offset_)) {
          RETURN_IF_ERROR(ReclaimTuplePool(state));
          COUNTER_ADD(tuple_pool_reclaim_counter_, 1);
        }
      }
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
    } while (!eos);
  }
  DCHECK_LE(priority_queue_.size(), limit_ + offset_);
  PrepareForOutput();

  // Unless we are inside a subplan expecting to call Open()/GetNext() on the child
  // again, the child can be closed at this point.
  if (!IsInSubplan()) child(0)->Close(state);
  return Status::OK();
}

Status TopNNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  while (!row_batch->AtCapacity() && (get_next_iter_ != sorted_top_n_.end())) {
    if (num_rows_skipped_ < offset_) {
      ++get_next_iter_;
      ++num_rows_skipped_;
      continue;
    }
    int row_idx = row_batch->AddRow();
    TupleRow* dst_row = row_batch->GetRow(row_idx);
    Tuple* src_tuple = *get_next_iter_;
    TupleRow* src_row = reinterpret_cast<TupleRow*>(&src_tuple);
    row_batch->CopyRow(src_row, dst_row);
    ++get_next_iter_;
    row_batch->CommitLastRow();
    IncrementNumRowsReturned(1);
    COUNTER_SET(rows_returned_counter_, rows_returned());
  }
  *eos = get_next_iter_ == sorted_top_n_.end();

  // Transfer ownership of tuple data to output batch.
  // TODO: To improve performance for small inputs when this node is run multiple times
  // inside a subplan, we might choose to only selectively transfer, e.g., when the
  // block(s) in the pool are all full or when the pool has reached a certain size.
  if (*eos) row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), false);
  return Status::OK();
}

Status TopNNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  priority_queue_.clear();
  num_rows_skipped_ = 0;
  // Transfer ownership of tuple data to output batch.
  row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), false);
  // We deliberately do not free the tuple_pool_ here to allow selective transferring
  // of resources in the future.
  return ExecNode::Reset(state, row_batch);
}

void TopNNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (tuple_pool_.get() != nullptr) tuple_pool_->FreeAll();
  if (tuple_row_less_than_.get() != nullptr) tuple_row_less_than_->Close(state);
  ScalarExprEvaluator::Close(output_tuple_expr_evals_, state);
  ScalarExpr::Close(ordering_exprs_);
  ScalarExpr::Close(output_tuple_exprs_);
  ExecNode::Close(state);
}

// Reverse the order of the tuples in the priority queue
void TopNNode::PrepareForOutput() {
  sorted_top_n_.resize(priority_queue_.size());
  int64_t index = sorted_top_n_.size() - 1;

  while (priority_queue_.size() > 0) {
    Tuple* tuple = priority_queue_.front();
    PopHeap(&priority_queue_,
        ComparatorWrapper<TupleRowComparator>(*tuple_row_less_than_));
    sorted_top_n_[index] = tuple;
    --index;
  }

  get_next_iter_ = sorted_top_n_.begin();
}

Status TopNNode::ReclaimTuplePool(RuntimeState* state) {
  unique_ptr<MemPool> temp_pool(new MemPool(mem_tracker()));

  for (int i = 0; i < priority_queue_.size(); i++) {
    Tuple* insert_tuple = reinterpret_cast<Tuple*>(temp_pool->TryAllocate(
        output_tuple_desc_->byte_size()));
    if (UNLIKELY(insert_tuple == nullptr)) {
      return temp_pool->mem_tracker()->MemLimitExceeded(state,
          "Failed to allocate memory in TopNNode::ReclaimTuplePool.",
          output_tuple_desc_->byte_size());
    }
    priority_queue_[i]->DeepCopy(insert_tuple, *output_tuple_desc_, temp_pool.get());
    priority_queue_[i] = insert_tuple;
  }

  rows_to_reclaim_ = 0;
  tmp_tuple_ = reinterpret_cast<Tuple*>(temp_pool->TryAllocate(
      output_tuple_desc_->byte_size()));
  if (UNLIKELY(tmp_tuple_ == nullptr)) {
    return temp_pool->mem_tracker()->MemLimitExceeded(state,
        "Failed to allocate memory in TopNNode::ReclaimTuplePool.",
        output_tuple_desc_->byte_size());
  }
  tuple_pool_->FreeAll();
  tuple_pool_.reset(temp_pool.release());
  return Status::OK();
}

void TopNNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "TopNNode("
       << ScalarExpr::DebugString(ordering_exprs_);
  for (int i = 0; i < is_asc_order_.size(); ++i) {
    *out << (i > 0 ? " " : "")
         << (is_asc_order_[i] ? "asc" : "desc")
         << " nulls " << (nulls_first_[i] ? "first" : "last");
 }

  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
