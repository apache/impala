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
    offset_(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
    num_rows_skipped_(0) {
}

Status TopNNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.sort_node.ordering_exprs, &lhs_ordering_exprs_));
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.sort_node.ordering_exprs, &rhs_ordering_exprs_));
  is_asc_order_.insert(
      is_asc_order_.begin(), tnode.sort_node.is_asc_order.begin(),
      tnode.sort_node.is_asc_order.end());
  if (tnode.sort_node.__isset.nulls_first) {
    nulls_first_.insert(
        nulls_first_.begin(), tnode.sort_node.nulls_first.begin(),
        tnode.sort_node.nulls_first.end());
  } else {
    nulls_first_.assign(is_asc_order_.size(), false);
  }

  DCHECK_EQ(conjuncts_.size(), 0) << "TopNNode should never have predicates to evaluate.";
  abort_on_default_limit_exceeded_ = tnode.sort_node.is_default_limit;

  tuple_row_less_than_.reset(new TupleRowComparator(
      lhs_ordering_exprs_, rhs_ordering_exprs_, is_asc_order_, nulls_first_));
  priority_queue_.reset(
      new priority_queue<TupleRow*, vector<TupleRow*>, TupleRowComparator>(
          *tuple_row_less_than_));
  return Status::OK;
}

Status TopNNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  tuple_pool_.reset(new MemPool(mem_tracker()));
  tuple_descs_ = child(0)->row_desc().tuple_descriptors();
  RETURN_IF_ERROR(Expr::Prepare(lhs_ordering_exprs_, state, child(0)->row_desc()));
  RETURN_IF_ERROR(Expr::Prepare(rhs_ordering_exprs_, state, child(0)->row_desc()));
  abort_on_default_limit_exceeded_ = abort_on_default_limit_exceeded_ &&
      state->abort_on_default_limit_exceeded();
  return Status::OK;
}

Status TopNNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(Expr::Open(lhs_ordering_exprs_, state));
  RETURN_IF_ERROR(Expr::Open(rhs_ordering_exprs_, state));
  RETURN_IF_ERROR(child(0)->Open(state));

  // Limit of 0, no need to fetch anything from children.
  if (limit_ != 0) {
    RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
    bool eos;
    do {
      batch.Reset();
      RETURN_IF_ERROR(child(0)->GetNext(state, &batch, &eos));
      if (abort_on_default_limit_exceeded_ && child(0)->rows_returned() > limit_) {
        DCHECK(offset_ == 0); // Offset should be 0 when the default limit is set.
        return Status("DEFAULT_ORDER_BY_LIMIT has been exceeded.");
      }
      for (int i = 0; i < batch.num_rows(); ++i) {
        InsertTupleRow(batch.GetRow(i));
      }
      RETURN_IF_ERROR(state->CheckQueryState());
    } while (!eos);
  }
  DCHECK_LE(priority_queue_->size(), limit_ + offset_);
  PrepareForOutput();
  child(0)->Close(state);
  return Status::OK;
}

Status TopNNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  while (!row_batch->AtCapacity() && (get_next_iter_ != sorted_top_n_.end())) {
    if (num_rows_skipped_ < offset_) {
      ++get_next_iter_;
      ++num_rows_skipped_;
      continue;
    }
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

void TopNNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (tuple_pool_.get() != NULL) tuple_pool_->FreeAll();
  Expr::Close(lhs_ordering_exprs_, state);
  Expr::Close(rhs_ordering_exprs_, state);
  ExecNode::Close(state);
}

// Insert if either not at the limit or it's a new TopN tuple_row
void TopNNode::InsertTupleRow(TupleRow* input_row) {
  TupleRow* insert_tuple_row = NULL;

  if (priority_queue_->size() < limit_ + offset_) {
    insert_tuple_row = input_row->DeepCopy(tuple_descs_, tuple_pool_.get());
  } else {
    DCHECK(!priority_queue_->empty());
    TupleRow* top_tuple_row = priority_queue_->top();
    if ((*tuple_row_less_than_)(input_row, top_tuple_row)) {
      // TODO: DeepCopy will allocate new buffers for the string data.  This needs
      // to be fixed to use a freelist
      input_row->DeepCopy(top_tuple_row, tuple_descs_, tuple_pool_.get(), true);
      insert_tuple_row = top_tuple_row;
      priority_queue_->pop();
    }
  }

  if (insert_tuple_row != NULL) {
    priority_queue_->push(insert_tuple_row);
  }
}

// Reverse the order of the tuples in the priority queue
void TopNNode::PrepareForOutput() {
  sorted_top_n_.resize(priority_queue_->size());
  int index = sorted_top_n_.size() - 1;

  while (priority_queue_->size() > 0) {
    TupleRow* tuple_row = priority_queue_->top();
    priority_queue_->pop();
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
