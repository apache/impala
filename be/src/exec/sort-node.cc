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

#include "exec/sort-node.h"
#include "exec/sort-exec-exprs.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/sorted-run-merger.h"

#include "common/names.h"

namespace impala {

SortNode::SortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    offset_(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
    sorter_(NULL),
    num_rows_skipped_(0) {
}

SortNode::~SortNode() {
}

Status SortNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  RETURN_IF_ERROR(sort_exec_exprs_.Init(tnode.sort_node.sort_info, pool_));
  is_asc_order_ = tnode.sort_node.sort_info.is_asc_order;
  nulls_first_ = tnode.sort_node.sort_info.nulls_first;
  return Status::OK();
}

Status SortNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  RETURN_IF_ERROR(sort_exec_exprs_.Prepare(
      state, child(0)->row_desc(), row_descriptor_, expr_mem_tracker()));
  AddExprCtxsToFree(sort_exec_exprs_);
  return Status::OK();
}

Status SortNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(sort_exec_exprs_.Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  RETURN_IF_ERROR(child(0)->Open(state));

  // These objects must be created after opening the sort_exec_exprs_. Avoid creating
  // them after every Reset()/Open().
  if (sorter_.get() == NULL) {
    TupleRowComparator less_than(
        sort_exec_exprs_.lhs_ordering_expr_ctxs(),
        sort_exec_exprs_.rhs_ordering_expr_ctxs(),
        is_asc_order_, nulls_first_);
    // Create and initialize the external sort impl object
    sorter_.reset(new Sorter(
        less_than, sort_exec_exprs_.sort_tuple_slot_expr_ctxs(),
        &row_descriptor_, mem_tracker(), runtime_profile(), state));
    RETURN_IF_ERROR(sorter_->Init());
  }

  // The child has been opened and the sorter created. Sort the input.
  // The final merge is done on-demand as rows are requested in GetNext().
  RETURN_IF_ERROR(SortInput(state));

  // Unless we are inside a subplan expecting to call Open()/GetNext() on the child
  // again, the child can be closed at this point.
  if (!IsInSubplan()) child(0)->Close(state);
  return Status::OK();
}

Status SortNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  } else {
    *eos = false;
  }

  DCHECK_EQ(row_batch->num_rows(), 0);
  RETURN_IF_ERROR(sorter_->GetNext(row_batch, eos));
  while ((num_rows_skipped_ < offset_)) {
    num_rows_skipped_ += row_batch->num_rows();
    // Throw away rows in the output batch until the offset is skipped.
    int rows_to_keep = num_rows_skipped_ - offset_;
    if (rows_to_keep > 0) {
      row_batch->CopyRows(0, row_batch->num_rows() - rows_to_keep, rows_to_keep);
      row_batch->set_num_rows(rows_to_keep);
    } else {
      row_batch->set_num_rows(0);
    }
    if (rows_to_keep > 0 || *eos) break;
    RETURN_IF_ERROR(sorter_->GetNext(row_batch, eos));
  }

  num_rows_returned_ += row_batch->num_rows();
  if (ReachedLimit()) {
    row_batch->set_num_rows(row_batch->num_rows() - (num_rows_returned_ - limit_));
    *eos = true;
  }

  COUNTER_SET(rows_returned_counter_, num_rows_returned_);

  return Status::OK();
}

Status SortNode::Reset(RuntimeState* state) {
  num_rows_skipped_ = 0;
  if (sorter_.get() != NULL) sorter_->Reset();
  return ExecNode::Reset(state);
}

void SortNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  sort_exec_exprs_.Close(state);
  sorter_.reset();
  ExecNode::Close(state);
}

void SortNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "SortNode("
       << Expr::DebugString(sort_exec_exprs_.lhs_ordering_expr_ctxs());
  for (int i = 0; i < is_asc_order_.size(); ++i) {
    *out << (i > 0 ? " " : "")
         << (is_asc_order_[i] ? "asc" : "desc")
         << " nulls " << (nulls_first_[i] ? "first" : "last");
  }
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

Status SortNode::SortInput(RuntimeState* state) {
  RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
  bool eos;
  do {
    batch.Reset();
    RETURN_IF_ERROR(child(0)->GetNext(state, &batch, &eos));
    RETURN_IF_ERROR(sorter_->AddBatch(&batch));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
  } while(!eos);
  RETURN_IF_ERROR(sorter_->InputDone());
  return Status::OK();
}

}
