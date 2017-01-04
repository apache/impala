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
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "util/runtime-profile-counters.h"
#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

namespace impala {

UnionNode::UnionNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs),
      tuple_id_(tnode.union_node.tuple_id),
      tuple_desc_(nullptr),
      first_materialized_child_idx_(tnode.union_node.first_materialized_child_idx),
      child_idx_(0),
      child_batch_(nullptr),
      child_row_idx_(0),
      child_eos_(false),
      const_expr_list_idx_(0),
      to_close_child_idx_(-1) { }

Status UnionNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));
  DCHECK(tnode.__isset.union_node);
  DCHECK_EQ(conjunct_ctxs_.size(), 0);
  // Create const_expr_ctx_lists_ from thrift exprs.
  const vector<vector<TExpr>>& const_texpr_lists = tnode.union_node.const_expr_lists;
  for (const vector<TExpr>& texprs : const_texpr_lists) {
    vector<ExprContext*> ctxs;
    RETURN_IF_ERROR(Expr::CreateExprTrees(pool_, texprs, &ctxs));
    const_expr_lists_.push_back(ctxs);
  }
  // Create result_expr_ctx_lists_ from thrift exprs.
  const vector<vector<TExpr>>& result_texpr_lists = tnode.union_node.result_expr_lists;
  for (const vector<TExpr>& texprs : result_texpr_lists) {
    vector<ExprContext*> ctxs;
    RETURN_IF_ERROR(Expr::CreateExprTrees(pool_, texprs, &ctxs));
    child_expr_lists_.push_back(ctxs);
  }
  return Status::OK();
}

Status UnionNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != nullptr);

  // Prepare const expr lists.
  for (const vector<ExprContext*>& exprs : const_expr_lists_) {
    RETURN_IF_ERROR(Expr::Prepare(exprs, state, row_desc(), expr_mem_tracker()));
    AddExprCtxsToFree(exprs);
    DCHECK_EQ(exprs.size(), tuple_desc_->slots().size());
  }

  // Prepare result expr lists.
  for (int i = 0; i < child_expr_lists_.size(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare(
        child_expr_lists_[i], state, child(i)->row_desc(), expr_mem_tracker()));
    AddExprCtxsToFree(child_expr_lists_[i]);
    DCHECK_EQ(child_expr_lists_[i].size(), tuple_desc_->slots().size());
  }
  return Status::OK();
}

Status UnionNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  // Open const expr lists.
  for (const vector<ExprContext*>& exprs : const_expr_lists_) {
    RETURN_IF_ERROR(Expr::Open(exprs, state));
  }
  // Open result expr lists.
  for (const vector<ExprContext*>& exprs : child_expr_lists_) {
    RETURN_IF_ERROR(Expr::Open(exprs, state));
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
  DCHECK(child(child_idx_)->row_desc().LayoutEquals(row_batch->row_desc()));
  if (child_eos_) RETURN_IF_ERROR(child(child_idx_)->Open(state));
  DCHECK_EQ(row_batch->num_rows(), 0);
  RETURN_IF_ERROR(child(child_idx_)->GetNext(state, row_batch, &child_eos_));
  if (limit_ != -1 && num_rows_returned_ + row_batch->num_rows() > limit_) {
    row_batch->set_num_rows(limit_ - num_rows_returned_);
  }
  num_rows_returned_ += row_batch->num_rows();
  DCHECK(limit_ == -1 || num_rows_returned_ <= limit_);
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
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
    // There are only 2 ways of getting out of this loop:
    // 1. The loop ends normally when we are either done iterating over the children that
    //    need materialization or the row batch is at capacity.
    // 2. We return from the function from inside the loop if limit is reached.
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
      // This loop fetches row batches from a single child and materializes each output
      // row, until one of these conditions:
      // 1. The loop ends normally if the row batch is at capacity.
      // 2. We break out of the loop if all the rows were consumed from the current child
      //    and we are moving on to the next child.
      // 3. We return from the function from inside the loop if the limit is reached.
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
      DCHECK_LT(child_row_idx_, child_batch_->num_rows());
      TupleRow* child_row = child_batch_->GetRow(child_row_idx_);
      MaterializeExprs(child_expr_lists_[child_idx_], child_row, tuple_buf, row_batch);
      tuple_buf += tuple_desc_->byte_size();
      ++child_row_idx_;
      if (ReachedLimit()) {
        COUNTER_SET(rows_returned_counter_, num_rows_returned_);
        // It's OK to close the child here even if we are inside a subplan.
        child_batch_.reset();
        child(child_idx_)->Close(state);
        return Status::OK();
      }
    }

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
  DCHECK_EQ(state->instance_ctx().per_fragment_instance_idx, 0);
  DCHECK_LT(const_expr_list_idx_, const_expr_lists_.size());
  // Create new tuple buffer for row_batch.
  int64_t tuple_buf_size;
  uint8_t* tuple_buf;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buf_size, &tuple_buf));
  memset(tuple_buf, 0, tuple_buf_size);
  while (const_expr_list_idx_ < const_expr_lists_.size() &&
      !row_batch->AtCapacity() && !ReachedLimit()) {
    MaterializeExprs(
        const_expr_lists_[const_expr_list_idx_], nullptr, tuple_buf, row_batch);
    tuple_buf += tuple_desc_->byte_size();
    ++const_expr_list_idx_;
  }

  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

Status UnionNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (to_close_child_idx_ != -1) {
    // The previous child needs to be closed if passthrough was enabled for it. In the non
    // passthrough case, the child was already closed in the previous call to GetNext().
    DCHECK(IsChildPassthrough(to_close_child_idx_));
    child(to_close_child_idx_)->Close(state);
    to_close_child_idx_ = -1;
  }

  if (HasMorePassthrough()) {
    RETURN_IF_ERROR(GetNextPassThrough(state, row_batch));
  } else if (HasMoreMaterialized()) {
    RETURN_IF_ERROR(GetNextMaterialized(state, row_batch));
  } else if (HasMoreConst(state)) {
    RETURN_IF_ERROR(GetNextConst(state, row_batch));
  }

  *eos = ReachedLimit() ||
      (!HasMorePassthrough() && !HasMoreMaterialized() && !HasMoreConst(state));

  return Status::OK();
}

void UnionNode::MaterializeExprs(const vector<ExprContext*>& exprs,
    TupleRow* row, uint8_t* tuple_buf, RowBatch* dst_batch) {
  DCHECK(!dst_batch->AtCapacity());
  Tuple* dst_tuple = reinterpret_cast<Tuple*>(tuple_buf);
  TupleRow* dst_row = dst_batch->GetRow(dst_batch->AddRow());
  dst_tuple->MaterializeExprs<false, false>(row, *tuple_desc_,
      exprs, dst_batch->tuple_data_pool());
  dst_row->SetTuple(0, dst_tuple);
  dst_batch->CommitLastRow();
  ++num_rows_returned_;
}

Status UnionNode::Reset(RuntimeState* state) {
  child_idx_ = 0;
  child_batch_.reset();
  child_row_idx_ = 0;
  child_eos_ = false;
  const_expr_list_idx_ = 0;
  // Since passthrough is disabled in subplans, verify that there is no passthrough
  // child that needs to be closed.
  DCHECK_EQ(to_close_child_idx_, -1);
  return ExecNode::Reset(state);
}

void UnionNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  child_batch_.reset();
  for (const vector<ExprContext*>& exprs : const_expr_lists_) {
    Expr::Close(exprs, state);
  }
  for (const vector<ExprContext*>& exprs : child_expr_lists_) {
    Expr::Close(exprs, state);
  }
  ExecNode::Close(state);
}

}
