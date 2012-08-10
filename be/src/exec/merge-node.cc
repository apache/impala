// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/merge-node.h"
#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/raw-value.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace std;

namespace impala {

MergeNode::MergeNode(ObjectPool* pool, const TPlanNode& tnode,
                     const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs),
      const_result_expr_idx_(0),
      child_idx_(INVALID_CHILD_IDX),
      child_row_batch_(NULL),
      child_eos_(false),
      child_row_idx_(0) {
  DCHECK_EQ(1, tnode.row_tuples.size());
  tuple_id_ = tnode.row_tuples[0];
  // TODO: log errors in runtime state
  Status status = Init(pool, tnode);
  DCHECK(status.ok())
      << "MergeNode c'tor: Init() failed:\n"
      << status.GetErrorMsg();
}

Status MergeNode::Init(ObjectPool* pool, const TPlanNode& tnode) {
  DCHECK(tnode.__isset.merge_node);
  // Create const_expr_lists_ from thrift exprs.
  const vector<vector<TExpr> >& const_texpr_lists = tnode.merge_node.const_expr_lists;
  for (int i = 0; i < const_texpr_lists.size(); ++i) {
    vector<Expr*> exprs;
    RETURN_IF_ERROR(Expr::CreateExprTrees(pool, const_texpr_lists[i], &exprs));
    const_result_expr_lists_.push_back(exprs);
  }
  // Create result_expr_lists_ from thrift exprs.
  const vector<vector<TExpr> >& result_texpr_lists = tnode.merge_node.result_expr_lists;
  for (int i = 0; i < result_texpr_lists.size(); ++i) {
    vector<Expr*> exprs;
    RETURN_IF_ERROR(Expr::CreateExprTrees(pool, result_texpr_lists[i], &exprs));
    result_expr_lists_.push_back(exprs);
  }
  return Status::OK;
}

Status MergeNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);
  // Prepare const expr lists.
  for (int i = 0; i < const_result_expr_lists_.size(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare(const_result_expr_lists_[i], state, row_desc()));
    DCHECK_EQ(const_result_expr_lists_[i].size(), tuple_desc_->slots().size());
  }
  // Prepare result expr lists.
  for (int i = 0; i < result_expr_lists_.size(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare(result_expr_lists_[i], state, child(i)->row_desc()));
    DCHECK_EQ(result_expr_lists_[i].size(), tuple_desc_->slots().size());
  }
  return Status::OK;
}

Status MergeNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_CANCELLED(state);
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  // Create new tuple buffer for row_batch.
  int tuple_buffer_size = row_batch->capacity() * tuple_desc_->byte_size();
  void* tuple_buffer = row_batch->tuple_data_pool()->Allocate(tuple_buffer_size);
  bzero(tuple_buffer, tuple_buffer_size);
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);

  // Evaluate and materialize the const expr lists exactly once.
  while (const_result_expr_idx_ < const_result_expr_lists_.size()) {
    // Materialize expr results into row_batch.
    EvalAndMaterializeExprs(const_result_expr_lists_[const_result_expr_idx_], true,
        &tuple, row_batch);
    ++const_result_expr_idx_;
    *eos = ReachedLimit();
    if (*eos || row_batch->IsFull()) return Status::OK;
  }
  if (child_idx_ == INVALID_CHILD_IDX) {
    child_idx_ = 0;
  }

  // Fetch from children, evaluate corresponding exprs and materialize.
  while (child_idx_ < children_.size()) {
    // Row batch was either never set or we're moving on to a different child.
    if (child_row_batch_.get() == NULL) {
      RETURN_IF_CANCELLED(state);
      child_row_batch_.reset(
          new RowBatch(child(child_idx_)->row_desc(), state->batch_size()));
      // Open child and fetch the first row batch.
      RETURN_IF_ERROR(child(child_idx_)->Open(state));
      RETURN_IF_ERROR(child(child_idx_)->GetNext(state, child_row_batch_.get(),
          &child_eos_));
      child_row_idx_ = 0;
    }

    // Start (or continue) consuming row batches from current child.
    while (true) {
      // Continue materializing exprs on child_row_batch_ into row batch.
      if (EvalAndMaterializeExprs(result_expr_lists_[child_idx_], false, &tuple,
          row_batch)) {
        *eos = ReachedLimit();
        if (*eos) {
          child_idx_ = INVALID_CHILD_IDX;
        }
        return Status::OK;
      }

      // Fetch new batch if one is available, otherwise move on to next child.
      if (child_eos_) break;
      RETURN_IF_CANCELLED(state);
      child_row_batch_->Reset();
      RETURN_IF_ERROR(child(child_idx_)->GetNext(state, child_row_batch_.get(),
          &child_eos_));
      child_row_idx_ = 0;
    }

    // Close current child and move on to next one.
    ++child_idx_;
    child_row_batch_.reset(NULL);
  }

  child_idx_ = INVALID_CHILD_IDX;
  *eos = true;
  return Status::OK;
}

Status MergeNode::Close(RuntimeState* state) {
  // don't call ExecNode::Close(), it always closes all children
  child_row_batch_.reset(NULL);
  COUNTER_UPDATE(rows_returned_counter_, num_rows_returned_);
  return ExecNode::Close(state);
}

bool MergeNode::EvalAndMaterializeExprs(const vector<Expr*>& exprs,
    bool const_exprs, Tuple** tuple, RowBatch* row_batch) {
  // Make sure there are rows left in the batch.
  if (!const_exprs && child_row_idx_ >= child_row_batch_->num_rows()) {
    return false;
  }
  // Execute the body at least once.
  bool done = true;
  Expr* const* conjuncts = &conjuncts_[0];
  int num_conjuncts = conjuncts_.size();

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
    DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
    TupleRow* row = row_batch->GetRow(row_idx);
    row->SetTuple(0, *tuple);

    // Materialize expr results into tuple.
    for (int i = 0; i < exprs.size(); ++i) {
      SlotDescriptor* slot_desc = tuple_desc_->slots()[i];
      RawValue::Write(exprs[i]->GetValue(child_row), *tuple, slot_desc,
          row_batch->tuple_data_pool());
    }

    if (EvalConjuncts(conjuncts, num_conjuncts, row)) {
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      char* new_tuple = reinterpret_cast<char*>(*tuple);
      new_tuple += tuple_desc_->byte_size();
      *tuple = reinterpret_cast<Tuple*>(new_tuple);
    } else {
      // Make sure to reset null indicators since we're overwriting
      // the tuple assembled for the previous row.
      (*tuple)->Init(tuple_desc_->byte_size());
    }

    if (row_batch->IsFull() || ReachedLimit()) {
      return true;
    }
  } while (!done);

  return false;
}

}
