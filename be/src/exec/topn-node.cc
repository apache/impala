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

#include "exec/topn-node.h"

#include <sstream>

#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using std::priority_queue;
using namespace impala;

TopNNode::TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    offset_(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
    materialized_tuple_desc_(NULL),
    tuple_row_less_than_(NULL),
    tmp_tuple_(NULL),
    tuple_pool_(NULL),
    num_rows_skipped_(0),
    priority_queue_(NULL) {
}

Status TopNNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  RETURN_IF_ERROR(sort_exec_exprs_.Init(tnode.sort_node.sort_info, pool_));
  is_asc_order_ = tnode.sort_node.sort_info.is_asc_order;
  nulls_first_ = tnode.sort_node.sort_info.nulls_first;

  DCHECK_EQ(conjunct_ctxs_.size(), 0)
      << "TopNNode should never have predicates to evaluate.";

  return Status::OK();
}

Status TopNNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  tuple_pool_.reset(new MemPool(mem_tracker()));
  RETURN_IF_ERROR(sort_exec_exprs_.Prepare(
      state, child(0)->row_desc(), row_descriptor_, expr_mem_tracker()));
  AddExprCtxsToFree(sort_exec_exprs_);
  materialized_tuple_desc_ = row_descriptor_.tuple_descriptors()[0];
  return Status::OK();
}

Status TopNNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  RETURN_IF_ERROR(sort_exec_exprs_.Open(state));

  // These objects must be created after opening the sort_exec_exprs_. Avoid creating
  // them after every Reset()/Open().
  if (tuple_row_less_than_.get() == NULL) {
    DCHECK(priority_queue_.get() == NULL);
    tuple_row_less_than_.reset(new TupleRowComparator(
        sort_exec_exprs_.lhs_ordering_expr_ctxs(),
        sort_exec_exprs_.rhs_ordering_expr_ctxs(),
        is_asc_order_, nulls_first_));
    priority_queue_.reset(
        new priority_queue<Tuple*, vector<Tuple*>, TupleRowComparator>(
            *tuple_row_less_than_));
  }
  // Allocate memory for a temporary tuple.
  tmp_tuple_ = reinterpret_cast<Tuple*>(
      tuple_pool_->Allocate(materialized_tuple_desc_->byte_size()));

  RETURN_IF_ERROR(child(0)->Open(state));

  // Limit of 0, no need to fetch anything from children.
  if (limit_ != 0) {
    RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
    bool eos;
    do {
      batch.Reset();
      RETURN_IF_ERROR(child(0)->GetNext(state, &batch, &eos));
      for (int i = 0; i < batch.num_rows(); ++i) {
        InsertTupleRow(batch.GetRow(i));
      }
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
    } while (!eos);
  }
  DCHECK_LE(priority_queue_->size(), limit_ + offset_);
  PrepareForOutput();

  // Unless we are inside a subplan expecting to call Open()/GetNext() on the child
  // again, the child can be closed at this point.
  if (!IsInSubplan()) child(0)->Close(state);
  return Status::OK();
}

Status TopNNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
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
    ++num_rows_returned_;
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  }
  *eos = get_next_iter_ == sorted_top_n_.end();

  // Transfer ownership of tuple data to output batch.
  // TODO: To improve performance for small inputs when this node is run multiple times
  // inside a subplan, we might choose to only selectively transfer, e.g., when the
  // block(s) in the pool are all full or when the pool has reached a certain size.
  if (*eos) row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), false);
  return Status::OK();
}

Status TopNNode::Reset(RuntimeState* state) {
  while(!priority_queue_->empty()) priority_queue_->pop();
  num_rows_skipped_ = 0;
  // We deliberately do not free the tuple_pool_ here to allow selective transferring
  // of resources in the future.
  return ExecNode::Reset(state);
}

void TopNNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (tuple_pool_.get() != NULL) tuple_pool_->FreeAll();
  sort_exec_exprs_.Close(state);
  ExecNode::Close(state);
}

// Insert if either not at the limit or it's a new TopN tuple_row
void TopNNode::InsertTupleRow(TupleRow* input_row) {
  Tuple* insert_tuple = NULL;

  if (priority_queue_->size() < limit_ + offset_) {
    insert_tuple = reinterpret_cast<Tuple*>(
        tuple_pool_->Allocate(materialized_tuple_desc_->byte_size()));
    insert_tuple->MaterializeExprs<false>(input_row, *materialized_tuple_desc_,
        sort_exec_exprs_.sort_tuple_slot_expr_ctxs(), tuple_pool_.get());
  } else {
    DCHECK(!priority_queue_->empty());
    Tuple* top_tuple = priority_queue_->top();
    tmp_tuple_->MaterializeExprs<false>(input_row, *materialized_tuple_desc_,
            sort_exec_exprs_.sort_tuple_slot_expr_ctxs(), NULL);
    if ((*tuple_row_less_than_)(tmp_tuple_, top_tuple)) {
      // TODO: DeepCopy() will allocate new buffers for the string data. This needs
      // to be fixed to use a freelist
      tmp_tuple_->DeepCopy(top_tuple, *materialized_tuple_desc_, tuple_pool_.get());
      insert_tuple = top_tuple;
      priority_queue_->pop();
    }
  }

  if (insert_tuple != NULL) priority_queue_->push(insert_tuple);
}

// Reverse the order of the tuples in the priority queue
void TopNNode::PrepareForOutput() {
  sorted_top_n_.resize(priority_queue_->size());
  int index = sorted_top_n_.size() - 1;

  while (priority_queue_->size() > 0) {
    Tuple* tuple = priority_queue_->top();
    priority_queue_->pop();
    sorted_top_n_[index] = tuple;
    --index;
  }

  get_next_iter_ = sorted_top_n_.begin();
}

void TopNNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "TopNNode("
      << Expr::DebugString(sort_exec_exprs_.lhs_ordering_expr_ctxs());
  for (int i = 0; i < is_asc_order_.size(); ++i) {
    *out << (i > 0 ? " " : "")
         << (is_asc_order_[i] ? "asc" : "desc")
         << " nulls " << (nulls_first_[i] ? "first" : "last");
 }

  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
