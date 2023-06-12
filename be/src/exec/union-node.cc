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

#include "exec/union-node.h"

#include "codegen/llvm-codegen.h"
#include "exec/exec-node-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "gen-cpp/PlanNodes_types.h"
#include "runtime/fragment-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace impala;

Status UnionPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  DCHECK(tnode_->__isset.union_node);
  DCHECK_EQ(conjuncts_.size(), 0);
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tnode_->union_node.tuple_id);
  DCHECK(tuple_desc_ != nullptr);
  first_materialized_child_idx_ = tnode_->union_node.first_materialized_child_idx;
  DCHECK_GT(first_materialized_child_idx_, -1);
  const int64_t num_nonconst_scalar_expr_to_be_codegened =
      state->NumScalarExprNeedsCodegen();
  // Create const_exprs_lists_ from thrift exprs.
  const vector<vector<TExpr>>& const_texpr_lists = tnode_->union_node.const_expr_lists;
  for (const vector<TExpr>& texprs : const_texpr_lists) {
    vector<ScalarExpr*> const_exprs;
    RETURN_IF_ERROR(ScalarExpr::Create(texprs, *row_descriptor_, state, &const_exprs));
    DCHECK_EQ(const_exprs.size(), tuple_desc_->slots().size());
    const_exprs_lists_.push_back(const_exprs);
  }
  num_const_scalar_expr_to_be_codegened_ =
      state->NumScalarExprNeedsCodegen() - num_nonconst_scalar_expr_to_be_codegened;
  // Create child_exprs_lists_ from thrift exprs.
  const vector<vector<TExpr>>& thrift_result_exprs = tnode_->union_node.result_expr_lists;
  for (int i = 0; i < thrift_result_exprs.size(); ++i) {
    const vector<TExpr>& texprs = thrift_result_exprs[i];
    vector<ScalarExpr*> child_exprs;
    RETURN_IF_ERROR(
        ScalarExpr::Create(texprs, *children_[i]->row_descriptor_, state, &child_exprs));
    child_exprs_lists_.push_back(child_exprs);
    DCHECK_EQ(child_exprs.size(), tuple_desc_->slots().size());
  }
  codegend_union_materialize_batch_fns_ =
      std::vector<CodegenFnPtr<UnionMaterializeBatchFn>>(child_exprs_lists_.size());
  return Status::OK();
}

void UnionPlanNode::Close() {
  for (const vector<ScalarExpr*>& const_exprs : const_exprs_lists_) {
    ScalarExpr::Close(const_exprs);
  }
  for (const vector<ScalarExpr*>& child_exprs : child_exprs_lists_) {
    ScalarExpr::Close(child_exprs);
  }
  PlanNode::Close();
}

Status UnionPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new UnionNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

UnionNode::UnionNode(
    ObjectPool* pool, const UnionPlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs),
    tuple_desc_(pnode.tuple_desc_),
    first_materialized_child_idx_(pnode.first_materialized_child_idx_),
    num_const_scalar_expr_to_be_codegened_(pnode.num_const_scalar_expr_to_be_codegened_),
    is_codegen_status_added_(pnode.is_codegen_status_added_),
    const_exprs_lists_(pnode.const_exprs_lists_),
    child_exprs_lists_(pnode.child_exprs_lists_),
    codegend_union_materialize_batch_fns_(pnode.codegend_union_materialize_batch_fns_),
    child_idx_(0),
    child_batch_(nullptr),
    child_row_idx_(0),
    child_eos_(false),
    const_exprs_lists_idx_(0),
    to_close_child_idx_(-1) {}

Status UnionNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  DCHECK(tuple_desc_ != nullptr);

  // Prepare const expr lists.
  for (const vector<ScalarExpr*>& const_exprs : const_exprs_lists_) {
    vector<ScalarExprEvaluator*> const_expr_evals;
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(const_exprs, state, pool_,
        expr_perm_pool(), expr_results_pool(), &const_expr_evals));
    const_expr_evals_lists_.push_back(const_expr_evals);
  }

  // Prepare result expr lists.
  for (const vector<ScalarExpr*>& child_exprs : child_exprs_lists_) {
    vector<ScalarExprEvaluator*> child_expr_evals;
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(child_exprs, state, pool_,
        expr_perm_pool(), expr_results_pool(), &child_expr_evals));
    child_expr_evals_lists_.push_back(child_expr_evals);
  }
  return Status::OK();
}

void UnionPlanNode::Codegen(FragmentState* state){
  DCHECK(state->ShouldCodegen());
  PlanNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != nullptr);
  std::stringstream codegen_message;
  Status codegen_status;
  for (int i = 0; i < child_exprs_lists_.size(); ++i) {
    if (IsChildPassthrough(i)) continue;

    llvm::Function* tuple_materialize_exprs_fn;
    codegen_status = Tuple::CodegenMaterializeExprs(codegen, false, *tuple_desc_,
        child_exprs_lists_[i], true, &tuple_materialize_exprs_fn);
    if (!codegen_status.ok()) {
      // Codegen may fail in some corner cases. If this happens, abort codegen for this
      // and the remaining children.
      codegen_message << "Codegen failed for child: " << children_[i]->tnode_->node_id;
      break;
    }

    // Get a copy of the function. This function will be modified and added to the
    // vector of functions.
    llvm::Function* union_materialize_batch_fn =
        codegen->GetFunction(IRFunction::UNION_MATERIALIZE_BATCH, true);
    DCHECK(union_materialize_batch_fn != nullptr);

    int replaced = codegen->ReplaceCallSites(union_materialize_batch_fn,
        tuple_materialize_exprs_fn, Tuple::MATERIALIZE_EXPRS_SYMBOL);
    DCHECK_REPLACE_COUNT(replaced, 1) << LlvmCodeGen::Print(union_materialize_batch_fn);

    union_materialize_batch_fn = codegen->FinalizeFunction(
        union_materialize_batch_fn);
    DCHECK(union_materialize_batch_fn != nullptr);

    // Add the function to Jit and to the vector of codegened functions.
    codegen->AddFunctionToJit(union_materialize_batch_fn,
        &(codegend_union_materialize_batch_fns_.data()[i]));
  }
  AddCodegenStatus(codegen_status, codegen_message.str());
  is_codegen_status_added_ = true;
}

Status UnionNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  // Open const expr lists.
  for (const vector<ScalarExprEvaluator*>& evals : const_expr_evals_lists_) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Open(evals, state));
  }
  // Open result expr lists.
  for (const vector<ScalarExprEvaluator*>& evals : child_expr_evals_lists_) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Open(evals, state));
  }

  // Ensures that rows are available for clients to fetch after this Open() has
  // succeeded.
  if (!children_.empty()) RETURN_IF_ERROR(child(child_idx_)->Open(state));
  return Status::OK();
}

Status UnionNode::GetNextPassThrough(RuntimeState* state, RowBatch* row_batch) {
  DCHECK(!ReachedLimit());
  DCHECK(!IsInSubplan());
  DCHECK_LT(child_idx_, children_.size());
  DCHECK(IsChildPassthrough(child_idx_));
  DCHECK(child(child_idx_)->row_desc()->LayoutEquals(*row_batch->row_desc()));
  if (child_eos_) RETURN_IF_ERROR(child(child_idx_)->Open(state));
  DCHECK_EQ(row_batch->num_rows(), 0);
  RETURN_IF_ERROR(child(child_idx_)->GetNext(state, row_batch, &child_eos_));
  if (child_eos_) {
    // Even though the child is at eos, it's not OK to Close() it here. Once we close
    // the child, the row batches that it produced are invalid. Marking the batch as
    // needing a deep copy let's us safely close the child in the next GetNext() call.
    // TODO: Remove this as part of IMPALA-4179.
    row_batch->MarkNeedsDeepCopy();
    to_close_child_idx_ = child_idx_;
    ++child_idx_;
  }
  return Status::OK();
}

Status UnionNode::GetNextMaterialized(RuntimeState* state, RowBatch* row_batch) {
  // Fetch from children, evaluate corresponding exprs and materialize.
  DCHECK(!ReachedLimit());
  DCHECK_LT(child_idx_, children_.size());
  int64_t tuple_buf_size;
  uint8_t* tuple_buf;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buf_size, &tuple_buf));
  memset(tuple_buf, 0, tuple_buf_size);

  while (HasMoreMaterialized() && !row_batch->AtCapacity()) {
    // The loop runs until we are either done iterating over the children that require
    // materialization, or the row batch is at capacity.
    DCHECK(!IsChildPassthrough(child_idx_));
    // Child row batch was either never set or we're moving on to a different child.
    if (child_batch_.get() == nullptr) {
      DCHECK_LT(child_idx_, children_.size());
      child_batch_.reset(new RowBatch(
          child(child_idx_)->row_desc(), state->batch_size(), mem_tracker()));
      child_row_idx_ = 0;
      // Open the current child unless it's the first child, which was already opened in
      // UnionNode::Open().
      if (child_eos_) RETURN_IF_ERROR(child(child_idx_)->Open(state));
      // The first batch from each child is always fetched here.
      RETURN_IF_ERROR(child(child_idx_)->GetNext(
          state, child_batch_.get(), &child_eos_));
    }

    while (!row_batch->AtCapacity()) {
      DCHECK(child_batch_.get() != nullptr);
      DCHECK_LE(child_row_idx_, child_batch_->num_rows());
      if (child_row_idx_ == child_batch_->num_rows()) {
        // Move on to the next child if it is at eos.
        if (child_eos_) break;
        // Fetch more rows from the child.
        child_batch_->Reset();
        child_row_idx_ = 0;
        // All batches except the first batch from each child are fetched here.
        RETURN_IF_ERROR(child(child_idx_)->GetNext(
            state, child_batch_.get(), &child_eos_));
        // If we fetched an empty batch, go back to the beginning of this while loop, and
        // try again.
        if (child_batch_->num_rows() == 0) continue;
      }
      DCHECK_EQ(codegend_union_materialize_batch_fns_.size(), children_.size());
      UnionPlanNode::UnionMaterializeBatchFn fn =
          codegend_union_materialize_batch_fns_[child_idx_].load();
      if (fn == nullptr) {
        MaterializeBatch(row_batch, &tuple_buf);
      } else {
        fn(this, row_batch, &tuple_buf);
      }
    }
    // It shouldn't be the case that we reached the limit because we shouldn't have
    // incremented 'num_rows_returned_' yet.
    DCHECK(!ReachedLimit());

    if (child_eos_ && child_row_idx_ == child_batch_->num_rows()) {
      // Unless we are inside a subplan expecting to call Open()/GetNext() on the child
      // again, the child can be closed at this point.
      child_batch_.reset();
      if (!IsInSubplan()) child(child_idx_)->Close(state);
      ++child_idx_;
    } else {
      // If we haven't finished consuming rows from the current child, we must have ended
      // up here because the row batch is at capacity.
      DCHECK(row_batch->AtCapacity());
    }
  }

  DCHECK_LE(child_idx_, children_.size());
  return Status::OK();
}

Status UnionNode::GetNextConst(RuntimeState* state, RowBatch* row_batch) {
  DCHECK(state->instance_ctx().per_fragment_instance_idx == 0 || IsInSubplan());
  DCHECK_LT(const_exprs_lists_idx_, const_expr_evals_lists_.size());
  // Create new tuple buffer for row_batch.
  int64_t tuple_buf_size;
  uint8_t* tuple_buf;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buf_size, &tuple_buf));
  memset(tuple_buf, 0, tuple_buf_size);

  while (const_exprs_lists_idx_ < const_exprs_lists_.size() && !row_batch->AtCapacity()) {
    MaterializeExprs(
        const_expr_evals_lists_[const_exprs_lists_idx_], nullptr, tuple_buf, row_batch);
    tuple_buf += tuple_desc_->byte_size();
    ++const_exprs_lists_idx_;
  }

  return Status::OK();
}

Status UnionNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (to_close_child_idx_ != -1) {
    // The previous child needs to be closed if passthrough was enabled for it. In the non
    // passthrough case, the child was already closed in the previous call to GetNext().
    DCHECK(IsChildPassthrough(to_close_child_idx_));
    DCHECK(!IsInSubplan());
    child(to_close_child_idx_)->Close(state);
    to_close_child_idx_ = -1;
  }

  // Save the number of rows in case GetNext() is called with a non-empty batch, which can
  // happen in a subplan.
  int num_rows_before = row_batch->num_rows();

  if (HasMorePassthrough()) {
    RETURN_IF_ERROR(GetNextPassThrough(state, row_batch));
  } else if (HasMoreMaterialized()) {
    RETURN_IF_ERROR(GetNextMaterialized(state, row_batch));
  } else if (HasMoreConst(state)) {
    RETURN_IF_ERROR(GetNextConst(state, row_batch));
  }

  int num_rows_added = row_batch->num_rows() - num_rows_before;
  DCHECK_GE(num_rows_added, 0);
  if (limit_ != -1 && rows_returned() + num_rows_added > limit_) {
    // Truncate the row batch if we went over the limit.
    num_rows_added = limit_ - rows_returned();
    row_batch->set_num_rows(num_rows_before + num_rows_added);
    DCHECK_GE(num_rows_added, 0);
  }
  IncrementNumRowsReturned(num_rows_added);

  *eos = ReachedLimit() ||
      (!HasMorePassthrough() && !HasMoreMaterialized() && !HasMoreConst(state));

  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

Status UnionNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  child_idx_ = 0;
  child_batch_.reset();
  child_row_idx_ = 0;
  child_eos_ = false;
  const_exprs_lists_idx_ = 0;
  // Since passthrough is disabled in subplans, verify that there is no passthrough child
  // that needs to be closed.
  DCHECK_EQ(to_close_child_idx_, -1);
  return ExecNode::Reset(state, row_batch);
}

void UnionNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  child_batch_.reset();
  for (const vector<ScalarExprEvaluator*>& evals : const_expr_evals_lists_) {
    ScalarExprEvaluator::Close(evals, state);
  }
  for (const vector<ScalarExprEvaluator*>& evals : child_expr_evals_lists_) {
    ScalarExprEvaluator::Close(evals, state);
  }
  ExecNode::Close(state);
  if (is_codegen_status_added_ && num_const_scalar_expr_to_be_codegened_ == 0
      && !const_exprs_lists_.empty()) {
    runtime_profile_->AppendExecOption("Codegen Disabled for const scalar expressions");
  }
}
