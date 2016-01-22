// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/union-node.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/raw-value.h"
#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

namespace impala {

UnionNode::UnionNode(ObjectPool* pool, const TPlanNode& tnode,
                     const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs),
      tuple_id_(tnode.union_node.tuple_id),
      tuple_desc_(NULL),
      const_result_expr_idx_(0),
      child_idx_(0),
      child_row_batch_(NULL),
      child_row_idx_(0),
      child_eos_(false) {
}

Status UnionNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  DCHECK(tnode.__isset.union_node);
  // Create const_expr_ctx_lists_ from thrift exprs.
  const vector<vector<TExpr> >& const_texpr_lists = tnode.union_node.const_expr_lists;
  for (int i = 0; i < const_texpr_lists.size(); ++i) {
    vector<ExprContext*> ctxs;
    RETURN_IF_ERROR(Expr::CreateExprTrees(pool_, const_texpr_lists[i], &ctxs));
    const_result_expr_ctx_lists_.push_back(ctxs);
  }
  // Create result_expr_ctx_lists_ from thrift exprs.
  const vector<vector<TExpr> >& result_texpr_lists = tnode.union_node.result_expr_lists;
  for (int i = 0; i < result_texpr_lists.size(); ++i) {
    vector<ExprContext*> ctxs;
    RETURN_IF_ERROR(Expr::CreateExprTrees(pool_, result_texpr_lists[i], &ctxs));
    result_expr_ctx_lists_.push_back(ctxs);
  }
  return Status::OK();
}

Status UnionNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);

  // prepare materialized_slots_
  for (int i = 0; i < tuple_desc_->slots().size(); ++i) {
    SlotDescriptor* desc = tuple_desc_->slots()[i];
    if (desc->is_materialized()) materialized_slots_.push_back(desc);
  }

  // Prepare const expr lists.
  for (int i = 0; i < const_result_expr_ctx_lists_.size(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare(
        const_result_expr_ctx_lists_[i], state, row_desc(), expr_mem_tracker()));
    AddExprCtxsToFree(const_result_expr_ctx_lists_[i]);
    DCHECK_EQ(const_result_expr_ctx_lists_[i].size(), materialized_slots_.size());
  }

  // Prepare result expr lists.
  for (int i = 0; i < result_expr_ctx_lists_.size(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare(
        result_expr_ctx_lists_[i], state, child(i)->row_desc(), expr_mem_tracker()));
    AddExprCtxsToFree(result_expr_ctx_lists_[i]);
    DCHECK_EQ(result_expr_ctx_lists_[i].size(), materialized_slots_.size());
  }
  return Status::OK();
}

Status UnionNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  // Open const expr lists.
  for (int i = 0; i < const_result_expr_ctx_lists_.size(); ++i) {
    RETURN_IF_ERROR(Expr::Open(const_result_expr_ctx_lists_[i], state));
  }
  // Open result expr lists.
  for (int i = 0; i < result_expr_ctx_lists_.size(); ++i) {
    RETURN_IF_ERROR(Expr::Open(result_expr_ctx_lists_[i], state));
  }

  // Open and fetch from the first child if there is one. Ensures that rows are
  // available for clients to fetch after this Open() has succeeded.
  if (!children_.empty()) RETURN_IF_ERROR(OpenCurrentChild(state));

  return Status::OK();
}

Status UnionNode::OpenCurrentChild(RuntimeState* state) {
  DCHECK_LT(child_idx_, children_.size());
  child_row_batch_.reset(new RowBatch(
      child(child_idx_)->row_desc(), state->batch_size(), mem_tracker()));
  // Open child and fetch the first row batch.
  RETURN_IF_ERROR(child(child_idx_)->Open(state));
  RETURN_IF_ERROR(child(child_idx_)->GetNext(state, child_row_batch_.get(),
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
  int tuple_buffer_size = row_batch->MaxTupleBufferSize();
  Tuple* tuple = Tuple::Create(tuple_buffer_size, row_batch->tuple_data_pool());

  // Fetch from children, evaluate corresponding exprs and materialize.
  while (child_idx_ < children_.size()) {
    // Row batch was either never set or we're moving on to a different child.
    if (child_row_batch_.get() == NULL) RETURN_IF_ERROR(OpenCurrentChild(state));

    // Start (or continue) consuming row batches from current child.
    while (true) {
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));

      // Continue materializing exprs on child_row_batch_ into row batch.
      RETURN_IF_ERROR(EvalAndMaterializeExprs(result_expr_ctx_lists_[child_idx_], false,
          &tuple, row_batch));
      if (row_batch->AtCapacity() || ReachedLimit()) {
        *eos = ReachedLimit();
        return Status::OK();
      }

      // Fetch new batch if one is available, otherwise move on to next child.
      if (child_eos_) break;
      child_row_batch_->Reset();
      RETURN_IF_ERROR(child(child_idx_)->GetNext(state, child_row_batch_.get(),
          &child_eos_));
      child_row_idx_ = 0;
    }

    // Close current child and move on to next one. It is OK to close the child as
    // long as all RowBatches have already been consumed so that we are sure to have
    // transfered all resources. It is not OK to close the child above in the case when
    // ReachedLimit() is true as we may end up releasing resources that are referenced
    // by the output row_batch.
    child_row_batch_.reset();

    // Unless we are inside a subplan expecting to call Open()/GetNext() on the child
    // again, the child can be closed at this point.
    if (!IsInSubplan()) child(child_idx_)->Close(state);
    ++child_idx_;
  }

  // Evaluate and materialize the const expr lists exactly once.
  while (const_result_expr_idx_ < const_result_expr_ctx_lists_.size()) {
    // Only evaluate the const expr lists by the first fragment instance.
    if (state->fragment_ctx().fragment_instance_idx == 0) {
      // Materialize expr results into row_batch.
      RETURN_IF_ERROR(EvalAndMaterializeExprs(
          const_result_expr_ctx_lists_[const_result_expr_idx_], true, &tuple,
          row_batch));
    }
    ++const_result_expr_idx_;
    *eos = ReachedLimit();
    if (*eos || row_batch->AtCapacity()) return Status::OK();
  }

  *eos = true;
  return Status::OK();
}

Status UnionNode::Reset(RuntimeState* state) {
  child_row_idx_ = 0;
  const_result_expr_idx_ = 0;
  child_idx_ = 0;
  child_row_batch_.reset();
  child_eos_ = false;
  return ExecNode::Reset(state);
}

void UnionNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  child_row_batch_.reset();
  for (int i = 0; i < const_result_expr_ctx_lists_.size(); ++i) {
    Expr::Close(const_result_expr_ctx_lists_[i], state);
  }
  for (int i = 0; i < result_expr_ctx_lists_.size(); ++i) {
    Expr::Close(result_expr_ctx_lists_[i], state);
  }
  ExecNode::Close(state);
}

Status UnionNode::EvalAndMaterializeExprs(const vector<ExprContext*>& ctxs,
    bool const_exprs, Tuple** tuple, RowBatch* row_batch) {
  // Make sure there are rows left in the batch.
  if (!const_exprs && child_row_idx_ >= child_row_batch_->num_rows()) {
    return Status::OK();
  }
  // Execute the body at least once.
  bool done = true;
  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  int num_conjunct_ctxs = conjunct_ctxs_.size();

  do {
    TupleRow* child_row = NULL;
    if (!const_exprs) {
      DCHECK(child_row_batch_ != NULL);
      // Non-const expr list. Fetch next row from batch.
      child_row = child_row_batch_->GetRow(child_row_idx_);
      ++child_row_idx_;
      done = child_row_idx_ >= child_row_batch_->num_rows();
    }

    // Add a new row to the batch.
    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    row->SetTuple(0, *tuple);

    // Materialize expr results into tuple.
    DCHECK_EQ(ctxs.size(), materialized_slots_.size());
    for (int i = 0; i < ctxs.size(); ++i) {
      // our exprs correspond to materialized slots
      SlotDescriptor* slot_desc = materialized_slots_[i];
      const void* value = ctxs[i]->GetValue(child_row);
      RETURN_IF_ERROR(ctxs[i]->root()->GetFnContextError(ctxs[i]));
      RawValue::Write(value, *tuple, slot_desc, row_batch->tuple_data_pool());
    }

    if (EvalConjuncts(conjunct_ctxs, num_conjunct_ctxs, row)) {
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);
      char* new_tuple = reinterpret_cast<char*>(*tuple);
      new_tuple += tuple_desc_->byte_size();
      *tuple = reinterpret_cast<Tuple*>(new_tuple);
    } else {
      // Make sure to reset null indicators since we're overwriting
      // the tuple assembled for the previous row.
      (*tuple)->Init(tuple_desc_->byte_size());
    }
    if (row_batch->AtCapacity() || ReachedLimit()) return Status::OK();
  } while (!done);

  return Status::OK();
}

}
