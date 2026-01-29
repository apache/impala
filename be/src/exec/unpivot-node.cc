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

#include "exec/unpivot-node.h"

#include "common/status.h"
#include "exec/exec-node.inline.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "runtime/fragment-state.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"

using std::make_unique;
using std::vector;

namespace impala {

Status UnpivotPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  DCHECK(tnode.__isset.unpivot_node);
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  DCHECK(tnode.row_tuples.size() == 1);
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tnode.row_tuples[0]);
  RETURN_IF_ERROR(ScalarExpr::Create(tnode.unpivot_node.source_exprs,
      *(children_[0]->row_descriptor_), state, &source_exprs_));
  DCHECK_EQ(tuple_desc_->slots().size(), source_exprs_.size());
  num_unpivot_columns_ = tnode.unpivot_node.num_unpivot_columns;
  if (tnode.unpivot_node.__isset.data_slot_id) {
    data_slot_id_ = tnode.unpivot_node.data_slot_id;
    DCHECK(tnode.unpivot_node.__isset.data_exprs);
    RETURN_IF_ERROR(ScalarExpr::Create(tnode.unpivot_node.data_exprs,
        *(children_[0]->row_descriptor_), state, &data_exprs_));
  }
  if (tnode.unpivot_node.__isset.header_slot_id) {
    header_slot_id_ = tnode.unpivot_node.header_slot_id;
    DCHECK(tnode.unpivot_node.__isset.header_exprs);
    RETURN_IF_ERROR(ScalarExpr::Create(tnode.unpivot_node.header_exprs,
        *(children_[0]->row_descriptor_), state, &header_exprs_));
  }
  return Status::OK();
}

void UnpivotPlanNode::Close() {
  for (auto expr : source_exprs_) {
    expr->Close();
  }
  for (auto expr : data_exprs_) {
    expr->Close();
  }
  for (auto expr : header_exprs_) {
    expr->Close();
  }
  PlanNode::Close();
}

Status UnpivotPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new UnpivotNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

UnpivotNode::UnpivotNode(
    ObjectPool* pool, const UnpivotPlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs) {}

Status UnpivotNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  const auto& plan_node = static_cast<const UnpivotPlanNode&>(plan_node_);
  auto init_evals = [&](const auto& exprs, auto& evals) -> Status {
    for (const auto expr : exprs) {
      ScalarExprEvaluator* eval = nullptr;
      RETURN_IF_ERROR(ScalarExprEvaluator::Create(
          *expr, state, pool_, expr_perm_pool(), expr_results_pool(), &eval));
      evals.push_back(eval);
    }
    return Status::OK();
  };
  RETURN_IF_ERROR(init_evals(plan_node.source_exprs_, source_evals_));
  RETURN_IF_ERROR(init_evals(plan_node.data_exprs_, data_evals_));
  RETURN_IF_ERROR(init_evals(plan_node.header_exprs_, header_evals_));
  return Status::OK();
}

Status UnpivotNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  for (const auto eval : source_evals_) {
    RETURN_IF_ERROR(eval->Open(state));
  }
  for (const auto eval : data_evals_) {
    RETURN_IF_ERROR(eval->Open(state));
  }
  for (const auto eval : header_evals_) {
    RETURN_IF_ERROR(eval->Open(state));
  }
  return Status::OK();
}

void UnpivotNode::MaterializeOutputTuple(Tuple* output_tuple, RowBatch* row_batch) {
  const auto& plan_node = static_cast<const UnpivotPlanNode&>(plan_node_);
  const auto num_slots = plan_node.tuple_desc_->slots().size();
  auto child_row = child_row_batch_->GetRow(child_row_idx_);
  for (int slot_idx = 0; slot_idx < num_slots; ++slot_idx) {
    auto output_slot = plan_node.tuple_desc_->slots()[slot_idx];
    void* src = nullptr;
    if (output_slot->id() == plan_node.data_slot_id_) {
      src = data_evals_[unpivot_slot_idx_]->GetValue(child_row);
    } else if (output_slot->id() == plan_node.header_slot_id_) {
      src = header_evals_[unpivot_slot_idx_]->GetValue(child_row);
    } else {
      src = source_evals_[slot_idx]->GetValue(child_row);
    }
    RawValue::Write(src, output_tuple, output_slot, row_batch->tuple_data_pool());
  }
}

// Like a UnionNode, an UnpivotNode materializes tuples by evaluating expressions.
// Unlike a UnionNode, for each input tuple, an UnpivotNode might produce multiple
// output tuples.
Status UnpivotNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  const auto& plan_node = static_cast<const UnpivotPlanNode&>(plan_node_);
  int64_t tuple_buf_size = -1;
  uint8_t* tuple_buf = nullptr;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buf_size, &tuple_buf));
  memset(tuple_buf, 0, tuple_buf_size);
  auto output_tuple = tuple_buf;
  DCHECK_GT(plan_node.num_unpivot_columns_, 0);
  // Materializes one row in each iteration if *eos is false.
  do {
    if (unpivot_slot_idx_ == plan_node.num_unpivot_columns_) {
      ++child_row_idx_;
      unpivot_slot_idx_ = 0;
    }
    if (child_row_batch_ == nullptr || child_row_idx_ == child_row_batch_->num_rows()) {
      if (child_row_batch_ == nullptr) {
        child_row_batch_ = make_unique<RowBatch>(
            child(0)->row_desc(), state->batch_size(), mem_tracker());
      } else {
        child_row_batch_->Reset();
      }
      if (!child_eos_) {
        RETURN_IF_ERROR(child(0)->GetNext(state, child_row_batch_.get(), &child_eos_));
      }
      child_row_idx_ = 0;
    }
    *eos = ReachedLimit()
        || (child_eos_ && (child_row_idx_ == child_row_batch_->num_rows()));
    if (*eos) {
      return Status::OK();
    }
    if (child_row_batch_->num_rows() == 0) {
      continue;
    }
    MaterializeOutputTuple(reinterpret_cast<Tuple*>(output_tuple), row_batch);
    ++unpivot_slot_idx_;
    auto output_row = row_batch->GetRow(row_batch->AddRow());
    output_row->SetTuple(0, reinterpret_cast<Tuple*>(output_tuple));
    if (EvalConjuncts(conjunct_evals_.data(), conjunct_evals_.size(), output_row)) {
      row_batch->CommitLastRow();
      IncrementNumRowsReturned(1);
      output_tuple += plan_node.tuple_desc_->byte_size();
    }
  } while (!*eos && !row_batch->AtCapacity());
  return Status::OK();
}

Status UnpivotNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  if (child_row_batch_ != nullptr) {
    child_row_batch_->Reset();
  }
  child_row_idx_ = 0;
  unpivot_slot_idx_ = 0;
  child_eos_ = false;
  return ExecNode::Reset(state, row_batch);
}

void UnpivotNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  child_row_batch_.reset();
  for (const auto& eval : source_evals_) {
    eval->Close(state);
  }
  for (const auto& eval : data_evals_) {
    eval->Close(state);
  }
  for (const auto& eval : header_evals_) {
    eval->Close(state);
  }
  ExecNode::Close(state);
}

} // namespace impala
