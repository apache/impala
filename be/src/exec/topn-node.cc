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

using namespace impala;
using namespace std;

TopNNode::TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs) 
  : ExecNode(pool, tnode, descs),
    tuple_row_less_than_(this),
    priority_queue_(tuple_row_less_than_) {
  // TODO: log errors in runtime state
  Status status = Init(pool, tnode);
  DCHECK(status.ok()) << "TopNNode c'tor:Init failed: \n" << status.GetErrorMsg();
}

Status TopNNode::Init(ObjectPool* pool, const TPlanNode& tnode) {
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool, tnode.sort_node.ordering_exprs, &lhs_ordering_exprs_));
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool, tnode.sort_node.ordering_exprs, &rhs_ordering_exprs_));
  is_asc_order_.insert(
      is_asc_order_.begin(), tnode.sort_node.is_asc_order.begin(),
      tnode.sort_node.is_asc_order.end());
  DCHECK_EQ(conjuncts_.size(), 0) << "TopNNode should never have predicates to evaluate.";
  abort_on_default_limit_exceeded_ = tnode.sort_node.is_default_limit;
  return Status::OK;
}

// The stl::priority_queue is a MAX heap.
bool TopNNode::TupleRowLessThan::operator()(TupleRow* const& lhs, TupleRow* const& rhs)
    const {
  DCHECK(node_ != NULL);

  vector<Expr*>::const_iterator lhs_expr_iter = node_->lhs_ordering_exprs_.begin();
  vector<Expr*>::const_iterator rhs_expr_iter = node_->rhs_ordering_exprs_.begin();
  vector<bool>::const_iterator is_asc_iter = node_->is_asc_order_.begin();

  for (;lhs_expr_iter != node_->lhs_ordering_exprs_.end(); 
      ++lhs_expr_iter,++rhs_expr_iter,++is_asc_iter) {
    Expr* lhs_expr = *lhs_expr_iter;
    Expr* rhs_expr = *rhs_expr_iter;
    void *lhs_value = lhs_expr->GetValue(lhs);
    void *rhs_value = rhs_expr->GetValue(rhs);
    bool less_than = *is_asc_iter;

    // NULL's always go at the end regardless of asc/desc
    if (lhs_value == NULL && rhs_value == NULL) continue;
    if (lhs_value == NULL && rhs_value != NULL) return false;
    if (lhs_value != NULL && rhs_value == NULL) return true;

    int result = RawValue::Compare(lhs_value, rhs_value, lhs_expr->type());
    if (!less_than) result = -result;
    if (result > 0) return false;
    if (result < 0) return true;
    // Otherwise, try the next Expr
  }
  return true;
}

Status TopNNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  tuple_pool_.reset(new MemPool(state->mem_limits()));
  tuple_descs_ = child(0)->row_desc().tuple_descriptors();
  Expr::Prepare(lhs_ordering_exprs_, state, child(0)->row_desc());
  Expr::Prepare(rhs_ordering_exprs_, state, child(0)->row_desc());
  abort_on_default_limit_exceeded_ = abort_on_default_limit_exceeded_ &&
      state->abort_on_default_limit_exceeded();
  return Status::OK;
}

Status TopNNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::OPEN, state));
  RETURN_IF_CANCELLED(state);
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(child(0)->Open(state));

  // Limit of 0, no need to fetch anything from children.
  if (limit_ != 0) {
    RowBatch batch(child(0)->row_desc(), state->batch_size(), *state->mem_limits());
    bool eos;
    do {
      RETURN_IF_CANCELLED(state);
      batch.Reset();
      RETURN_IF_ERROR(child(0)->GetNext(state, &batch, &eos));
      if (abort_on_default_limit_exceeded_ && child(0)->rows_returned() > limit_) {
        return Status("DEFAULT_ORDER_BY_LIMIT has been exceeded.");
      }
      for (int i = 0; i < batch.num_rows(); ++i) {
        InsertTupleRow(batch.GetRow(i));
      }
      RETURN_IF_LIMIT_EXCEEDED(state);
    } while (!eos);
  }
  DCHECK_LE(priority_queue_.size(), limit_);
  PrepareForOutput();
  return Status::OK;
}

Status TopNNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  while (!row_batch->IsFull() && (get_next_iter_ != sorted_top_n_.end())) {
    int row_idx = row_batch->AddRow();
    TupleRow* dst_row = row_batch->GetRow(row_idx);
    TupleRow* src_row = *get_next_iter_;
    row_batch->CopyRow(src_row, dst_row);
    ++get_next_iter_;
    row_batch->CommitLastRow();
    ++num_rows_returned_;
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  }
  *eos = get_next_iter_ == sorted_top_n_.end();
  return Status::OK; 
}

Status TopNNode::Close(RuntimeState* state) {
  if (memory_used_counter() != NULL) {
    COUNTER_UPDATE(memory_used_counter(), tuple_pool_->peak_allocated_bytes());
  }
  return ExecNode::Close(state);
}

// Insert if either not at the limit or it's a new TopN tuple_row
void TopNNode::InsertTupleRow(TupleRow* input_row) {
  TupleRow* insert_tuple_row = NULL;
  
  if (priority_queue_.size() < limit_) {
    insert_tuple_row = input_row->DeepCopy(tuple_descs_, tuple_pool_.get());
  } else {
    DCHECK(!priority_queue_.empty());
    TupleRow* top_tuple_row = priority_queue_.top();
    if (tuple_row_less_than_(input_row, top_tuple_row)) {
      // TODO: DeepCopy will allocate new buffers for the string data.  This needs
      // to be fixed to use a freelist
      input_row->DeepCopy(top_tuple_row, tuple_descs_, tuple_pool_.get(), true);
      insert_tuple_row = top_tuple_row;
      priority_queue_.pop();
    }
  }

  if (insert_tuple_row != NULL) {
    priority_queue_.push(insert_tuple_row);
  }
}

// Reverse the order of the tuples in the priority queue
void TopNNode::PrepareForOutput() {
  sorted_top_n_.resize(priority_queue_.size());
  int index = sorted_top_n_.size() - 1;

  while (priority_queue_.size() > 0) {
    TupleRow* tuple_row = priority_queue_.top();
    priority_queue_.pop();
    sorted_top_n_[index] = tuple_row;
    --index;
  }

  get_next_iter_ = sorted_top_n_.begin();
}

void TopNNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "TopNNode("
       << " ordering_exprs=" << Expr::DebugString(lhs_ordering_exprs_)
       << " sort_order=[";
  for (int i = 0; i < is_asc_order_.size(); ++i) {
    *out << (i > 0 ? " " : "") << (is_asc_order_[i] ? "asc" : "desc");
  }
  *out << "]";
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
