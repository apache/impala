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

#include "exec/iceberg-merge-node.h"

#include <vector>

#include "common/status.h"
#include "exec/exec-node-util.h"
#include "exec/exec-node.inline.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/fragment-state.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

namespace impala {

Status IcebergMergePlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  ObjectPool* pool = state->obj_pool();

  for (auto& tmerge_case : tnode.merge_node.cases) {
    auto* merge_case_plan = pool->Add(new IcebergMergeCasePlan());
    RETURN_IF_ERROR(merge_case_plan->Init(tmerge_case, state, row_descriptor_));
    merge_case_plans_.push_back(merge_case_plan);
  }
  RETURN_IF_ERROR(ScalarExpr::Create(
      tnode.merge_node.row_present, *row_descriptor_, state, pool, &row_present_));

  RETURN_IF_ERROR(ScalarExpr::Create(tnode.merge_node.position_meta_exprs,
      *row_descriptor_, state, pool, &position_meta_exprs_));

  RETURN_IF_ERROR(ScalarExpr::Create(tnode.merge_node.partition_meta_exprs,
      *row_descriptor_, state, pool, &partition_meta_exprs_));

  merge_action_tuple_id_ = tnode.merge_node.merge_action_tuple_id;
  target_tuple_id_ = tnode.merge_node.target_tuple_id;

  return Status::OK();
}

Status IcebergMergeCasePlan::Init(const TIcebergMergeCase& tmerge_case,
    FragmentState* state, const RowDescriptor* row_desc) {
  ObjectPool* pool = state->obj_pool();
  RETURN_IF_ERROR(ScalarExpr::Create(
      tmerge_case.output_expressions, *row_desc, state, pool, &output_exprs_));
  RETURN_IF_ERROR(ScalarExpr::Create(
      tmerge_case.filter_conjuncts, *row_desc, state, pool, &filter_conjuncts_));
  case_type_ = tmerge_case.type;
  match_type_ = tmerge_case.match_type;
  return Status::OK();
}

Status IcebergMergePlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new IcebergMergeNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

IcebergMergeNode::IcebergMergeNode(
    ObjectPool* pool, const IcebergMergePlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs),
    child_row_batch_(nullptr),
    child_row_idx_(0),
    child_eos_(false),
    row_present_(pnode.row_present_),
    position_meta_exprs_(pnode.position_meta_exprs_),
    partition_meta_exprs_(pnode.partition_meta_exprs_) {
  DCHECK(pnode.merge_action_tuple_id_ != -1);
  DCHECK(pnode.target_tuple_id_ != -1);

  merge_action_tuple_idx_ = row_descriptor_.GetTupleIdx(pnode.merge_action_tuple_id_);
  target_tuple_idx_ = row_descriptor_.GetTupleIdx(pnode.target_tuple_id_);

  for (auto* merge_case_plan : pnode.merge_case_plans_) {
    auto merge_case = pool->Add(new IcebergMergeCase(merge_case_plan));
    all_cases_.push_back(merge_case);
    switch (merge_case->match_type_) {
      case TMergeMatchType::MATCHED:
        matched_cases_.push_back(merge_case);
        break;
      case TMergeMatchType::NOT_MATCHED_BY_TARGET:
        not_matched_by_target_cases_.push_back(merge_case);
        break;
      case TMergeMatchType::NOT_MATCHED_BY_SOURCE:
        not_matched_by_source_cases_.push_back(merge_case);
        break;
    }
  }
}

Status IcebergMergeNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  RETURN_IF_ERROR(ScalarExprEvaluator::Create(*row_present_, state, state->obj_pool(),
      expr_perm_pool_.get(), expr_results_pool_.get(), &row_present_evaluator_));

  RETURN_IF_ERROR(
      ScalarExprEvaluator::Create(position_meta_exprs_, state, state->obj_pool(),
          expr_perm_pool_.get(), expr_results_pool_.get(), &position_meta_evaluators_));

  RETURN_IF_ERROR(
      ScalarExprEvaluator::Create(partition_meta_exprs_, state, state->obj_pool(),
          expr_perm_pool_.get(), expr_results_pool_.get(), &partition_meta_evaluators_));

  for (auto* merge_case : all_cases_) {
    RETURN_IF_ERROR(merge_case->Prepare(state, *this));
  }

  return Status::OK();
}

Status IcebergMergeNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  child_row_batch_.reset(
      new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));

  RETURN_IF_ERROR(row_present_evaluator_->Open(state));
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(position_meta_evaluators_, state));
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(partition_meta_evaluators_, state));

  for (auto* merge_case : all_cases_) {
    RETURN_IF_ERROR(merge_case->Open(state));
  }

  return Status::OK();
}

Status IcebergMergeNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  do {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    if (child_row_batch_->num_rows() == 0) {
      RETURN_IF_ERROR(child(0)->GetNext(state, child_row_batch_.get(), &child_eos_));
    }
    RETURN_IF_ERROR(EvaluateCases(row_batch));
    COUNTER_SET(rows_returned_counter_, rows_returned());
    *eos =
        ReachedLimit() || (child_row_idx_ == child_row_batch_->num_rows() && child_eos_);
    if (*eos || child_row_idx_ == child_row_batch_->num_rows()) {
      child_row_idx_ = 0;
      child_row_batch_->Reset();
    }
  } while (!*eos && !row_batch->AtCapacity());
  return Status::OK();
}

Status IcebergMergeNode::EvaluateCases(RowBatch* output_batch) {
  FOREACH_ROW(child_row_batch_.get(), child_row_idx_, iter) {
    ++child_row_idx_;
    auto row = iter.Get();

    if (IsDuplicateRow(row)) {
      return Status(
          "Duplicate row found: one target table row matched more than one source row");
    }
    previous_row_target_tuple_ = row->GetTuple(target_tuple_idx_);

    auto row_present = row_present_evaluator_->GetTinyIntVal(row).val;
    IcebergMergeCase* selected_case = nullptr;


    std::vector<IcebergMergeCase*>* cases = nullptr;
    switch (row_present) {
      case TIcebergMergeRowPresent::BOTH: {
        cases = &matched_cases_;
        break;
      }
      case TIcebergMergeRowPresent::SOURCE: {
        cases = &not_matched_by_target_cases_;
        break;
      }
      case TIcebergMergeRowPresent::TARGET: {
        cases = &not_matched_by_source_cases_;
        break;
      }
      default:
        return Status("Invalid row presence value in MERGE statement's result set.");
    }
    for (auto* merge_case : *cases) {
      if (CheckCase(merge_case, row)) {
        selected_case = merge_case;
        break;
      }
    }

    if (!selected_case) continue;
    // Add a new row to output_batch
    AddRow(output_batch, selected_case, row);
    if (ReachedLimit() || output_batch->AtCapacity()) return Status::OK();
  }
  return Status::OK();
}

bool IcebergMergeNode::CheckCase(const IcebergMergeCase* merge_case, TupleRow* row) {
  return EvalConjuncts(
      merge_case->filter_evaluators_.data(), merge_case->filter_evaluators_.size(), row);
}

void IcebergMergeNode::AddRow(
    RowBatch* output_batch, IcebergMergeCase* merge_case, TupleRow* row) {
  TupleRow* dst_row = output_batch->GetRow(output_batch->AddRow());

  auto* target_tuple =
      Tuple::Create(row_descriptor_.tuple_descriptors()[target_tuple_idx_]->byte_size(),
          output_batch->tuple_data_pool());
  auto* merge_action_tuple = Tuple::Create(
      row_descriptor_.tuple_descriptors()[merge_action_tuple_idx_]->byte_size(),
      output_batch->tuple_data_pool());

  dst_row->SetTuple(target_tuple_idx_, target_tuple);
  dst_row->SetTuple(merge_action_tuple_idx_, merge_action_tuple);

  for (int i = 0; i < row_descriptor_.tuple_descriptors().size(); i++) {
    if (i != target_tuple_idx_ && i != merge_action_tuple_idx_) {
      dst_row->SetTuple(i, nullptr);
    }
  }

  target_tuple->MaterializeExprs<false, false>(row,
      *row_descriptor_.tuple_descriptors()[target_tuple_idx_],
      merge_case->combined_evaluators_, output_batch->tuple_data_pool());

  TIcebergMergeSinkAction::type action = merge_case->SinkAction();

  RawValue::WriteNonNullPrimitive(
      &action, merge_action_tuple, merge_action_tuple_type_, nullptr);

  output_batch->CommitLastRow();
  IncrementNumRowsReturned(1);
}

bool IcebergMergeNode::IsDuplicateRow(TupleRow* actual_row) {
  if (previous_row_target_tuple_ == nullptr) { return false; }
  auto actual_row_target_tuple = actual_row->GetTuple(target_tuple_idx_);
  return previous_row_target_tuple_ == actual_row_target_tuple;
}

Status IcebergMergeNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  child_row_batch_->TransferResourceOwnership(row_batch);
  child_row_idx_ = 0;
  child_eos_ = false;
  previous_row_target_tuple_ = nullptr;
  return ExecNode::Reset(state, row_batch);
}

void IcebergMergeNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  child_row_batch_.reset();
  for (auto merge_case : all_cases_) {
    merge_case->Close(state);
  }
  row_present_evaluator_->Close(state);
  ScalarExprEvaluator::Close(position_meta_evaluators_, state);
  ScalarExprEvaluator::Close(partition_meta_evaluators_, state);
  ExecNode::Close(state);
}

const std::vector<ScalarExprEvaluator*>& IcebergMergeNode::PositionMetaEvals() {
  return position_meta_evaluators_;
}

const std::vector<ScalarExprEvaluator*>& IcebergMergeNode::PartitionMetaEvals() {
  return partition_meta_evaluators_;
}

IcebergMergeCase::IcebergMergeCase(const IcebergMergeCasePlan* merge_case_plan)
  : filter_conjuncts_(merge_case_plan->filter_conjuncts_),
    output_exprs_(merge_case_plan->output_exprs_),
    case_type_(merge_case_plan->case_type_),
    match_type_(merge_case_plan->match_type_) {}

Status IcebergMergeCase::Prepare(RuntimeState* state, IcebergMergeNode& parent) {
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(output_exprs_, state, state->obj_pool(),
      parent.expr_perm_pool(), parent.expr_results_pool(), &output_evaluators_));
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(filter_conjuncts_, state, state->obj_pool(),
      parent.expr_perm_pool(), parent.expr_results_pool(), &filter_evaluators_));
  /// Combining output expression, Iceberg position related columns, and
  /// Iceberg partition related columns into one vector. The order of the evaluators
  /// matches the order of expressions in the merge query.
  combined_evaluators_.insert(
      combined_evaluators_.end(), output_evaluators_.begin(), output_evaluators_.end());
  combined_evaluators_.insert(combined_evaluators_.end(),
      parent.PositionMetaEvals().begin(), parent.PositionMetaEvals().end());
  combined_evaluators_.insert(combined_evaluators_.end(),
      parent.PartitionMetaEvals().begin(), parent.PartitionMetaEvals().end());
  return Status::OK();
}

Status IcebergMergeCase::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(filter_evaluators_, state));
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(output_evaluators_, state));
  return Status::OK();
}

void IcebergMergeCase::Close(RuntimeState* state) {
  ScalarExprEvaluator::Close(filter_evaluators_, state);
  ScalarExprEvaluator::Close(output_evaluators_, state);
}

} // namespace impala
