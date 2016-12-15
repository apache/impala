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
      tuple_desc_(NULL),
      child_idx_(0),
      child_batch_(NULL),
      child_row_idx_(0),
      child_eos_(false),
      const_expr_list_idx_(0) {
}

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
  DCHECK(tuple_desc_ != NULL);

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

  // Open and fetch from the first child if there is one. Ensures that rows are
  // available for clients to fetch after this Open() has succeeded.
  if (!children_.empty()) RETURN_IF_ERROR(OpenCurrentChild(state));

  return Status::OK();
}

Status UnionNode::OpenCurrentChild(RuntimeState* state) {
  DCHECK_LT(child_idx_, children_.size());
  child_batch_.reset(new RowBatch(
      child(child_idx_)->row_desc(), state->batch_size(), mem_tracker()));
  // Open child and fetch the first row batch.
  RETURN_IF_ERROR(child(child_idx_)->Open(state));
  RETURN_IF_ERROR(child(child_idx_)->GetNext(state, child_batch_.get(),
      &child_eos_));
  child_row_idx_ = 0;
  return Status::OK();
}

Status UnionNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  // Create new tuple buffer for row_batch.
  int64_t tuple_buf_size;
  uint8_t* tuple_buf;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buf_size, &tuple_buf));
  memset(tuple_buf, 0, tuple_buf_size);

  // Fetch from children, evaluate corresponding exprs and materialize.
  while (child_idx_ < children_.size()) {
    // Row batch was either never set or we're moving on to a different child.
    if (child_batch_.get() == NULL) RETURN_IF_ERROR(OpenCurrentChild(state));

    // Start or continue consuming row batches from current child.
    while (true) {
      RETURN_IF_ERROR(QueryMaintenance(state));

      // Start or continue processing current child batch.
      while (child_row_idx_ < child_batch_->num_rows()) {
        TupleRow* child_row = child_batch_->GetRow(child_row_idx_);
        MaterializeExprs(child_expr_lists_[child_idx_], child_row, tuple_buf, row_batch);
        tuple_buf += tuple_desc_->byte_size();
        ++child_row_idx_;
        *eos = ReachedLimit();
        if (*eos || row_batch->AtCapacity()) {
          COUNTER_SET(rows_returned_counter_, num_rows_returned_);
          return Status::OK();
        }
      }

      // Fetch new batch if one is available, otherwise move on to next child.
      if (child_eos_) break;
      child_batch_->Reset();
      RETURN_IF_ERROR(child(child_idx_)->GetNext(state, child_batch_.get(),
          &child_eos_));
      child_row_idx_ = 0;
    }

    // Close current child and move on to next one. It is OK to close the child as
    // long as all RowBatches have already been consumed so that we are sure to have
    // transfered all resources. It is not OK to close the child above in the case when
    // ReachedLimit() is true as we may end up releasing resources that are referenced
    // by the output row_batch.
    child_batch_.reset();

    // Unless we are inside a subplan expecting to call Open()/GetNext() on the child
    // again, the child can be closed at this point.
    if (!IsInSubplan()) child(child_idx_)->Close(state);
    ++child_idx_;
  }

  // Only evaluate the const expr lists by the first fragment instance.
  if (state->instance_ctx().per_fragment_instance_idx == 0) {
    // Evaluate and materialize the const expr lists exactly once.
    while (const_expr_list_idx_ < const_expr_lists_.size()) {
      MaterializeExprs(
          const_expr_lists_[const_expr_list_idx_], NULL, tuple_buf, row_batch);
      tuple_buf += tuple_desc_->byte_size();
      ++const_expr_list_idx_;
      *eos = ReachedLimit();
      if (*eos || row_batch->AtCapacity()) {
        COUNTER_SET(rows_returned_counter_, num_rows_returned_);
        return Status::OK();
      }
    }
  }

  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  *eos = true;
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
  child_row_idx_ = 0;
  const_expr_list_idx_ = 0;
  child_idx_ = 0;
  child_batch_.reset();
  child_eos_ = false;
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
