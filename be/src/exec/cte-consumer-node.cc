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

#include "codegen/llvm-codegen.h"
#include "exec/cte-consumer-node.h"
#include "exec/exec-node-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/local-exchanger.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-filter-bank.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-filter.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "util/debug-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"
#include "util/runtime-profile.h"

#include "common/names.h"

DECLARE_double(min_filter_reject_ratio);

namespace impala {

constexpr int BATCHES_PER_FILTER_SELECTIVITY_CHECK = 16;

Status CTEConsumerPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  DCHECK(tnode_->__isset.cte_consumer);
  const TCTEConsumer& cte_node = tnode_->cte_consumer;
  DCHECK_EQ(row_descriptor_->tuple_descriptors().size(), 1);
  tuple_desc_ = row_descriptor_->tuple_descriptors()[0];

  DCHECK_EQ(cte_node.input_row_tuples.size(), cte_node.nullable_tuples.size());
  RowDescriptor input_row_desc(
      state->desc_tbl(), cte_node.input_row_tuples, cte_node.nullable_tuples);

  RETURN_IF_ERROR(
      ScalarExpr::Create(cte_node.result_exprs, input_row_desc, state, &input_exprs_));
  DCHECK_EQ(input_exprs_.size(), tuple_desc_->slots().size());

  // Set up runtime filter expressions, one per filter assigned to this node.
  for (const TRuntimeFilterDesc& filter_desc : tnode.runtime_filters) {
    auto it = filter_desc.planid_to_target_ndx.find(tnode.node_id);
    DCHECK(it != filter_desc.planid_to_target_ndx.end());
    const TRuntimeFilterTargetDesc& target = filter_desc.targets[it->second];
    ScalarExpr* filter_expr;
    RETURN_IF_ERROR(
        ScalarExpr::Create(target.target_expr, *row_descriptor_, state, &filter_expr));
    runtime_filter_exprs_.push_back(filter_expr);
  }

  is_passthrough_ = row_descriptor_->LayoutEquals(input_row_desc);
  return Status::OK();
}

void CTEConsumerPlanNode::Close() {
  ScalarExpr::Close(input_exprs_);
  ScalarExpr::Close(runtime_filter_exprs_);
  PlanNode::Close();
}

Status CTEConsumerPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new CTEConsumerNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

void CTEConsumerPlanNode::Codegen(FragmentState* state) {
  DCHECK(state->ShouldCodegen());
  PlanNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  if (is_passthrough_) return;
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != nullptr);
  std::stringstream codegen_message;

  llvm::Function* tuple_materialize_exprs_fn;
  Status codegen_status = Tuple::CodegenMaterializeExprs(codegen, false, *tuple_desc_,
      input_exprs_, true, &tuple_materialize_exprs_fn);
  if (!codegen_status.ok()) {
    // Codegen may fail in some corner cases. If this happens, abort codegen.
    AddCodegenStatus(codegen_status, "Codegen failed");
    return;
  }

  // Get a copy of the function. This function will be modified and added to the
  // vector of functions.
  llvm::Function* materialize_batch_fn =
      codegen->GetFunction(IRFunction::CTE_MATERIALIZE_BATCH, true);
  DCHECK(materialize_batch_fn != nullptr);

  int replaced = codegen->ReplaceCallSites(materialize_batch_fn,
      tuple_materialize_exprs_fn, Tuple::MATERIALIZE_EXPRS_SYMBOL);
  DCHECK_REPLACE_COUNT(replaced, 1) << LlvmCodeGen::Print(materialize_batch_fn);

  materialize_batch_fn = codegen->FinalizeFunction(materialize_batch_fn);
  DCHECK(materialize_batch_fn != nullptr);

  // Add the function to Jit and to the vector of codegened functions.
  codegen->AddFunctionToJit(materialize_batch_fn, &codegend_materialize_batch_fn_);
}

CTEConsumerNode::CTEConsumerNode(
    ObjectPool* pool, const CTEConsumerPlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs),
    tuple_desc_(pnode.tuple_desc_),
    input_exprs_(pnode.input_exprs_),
    codegend_materialize_batch_fn_(pnode.codegend_materialize_batch_fn_),
    is_passthrough_(pnode.is_passthrough_) { }

Status CTEConsumerNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  if (!is_passthrough_) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(input_exprs_, state, pool_,
        expr_perm_pool(), expr_results_pool(), &input_expr_evals_));
  }

  const CTEConsumerPlanNode& pnode =
      static_cast<const CTEConsumerPlanNode&>(plan_node());
  const std::vector<ScalarExpr*>& filter_exprs = pnode.runtime_filter_exprs_;
  DCHECK_EQ(filter_exprs.size(), plan_node().tnode_->runtime_filters.size());
  for (int i = 0; i < filter_exprs.size(); ++i) {
    const TRuntimeFilterDesc& filter_desc =
        plan_node().tnode_->runtime_filters[i];
    filter_ctxs_.emplace_back();
    FilterContext& filter_ctx = filter_ctxs_.back();
    filter_ctx.filter = state->filter_bank()->RegisterConsumer(filter_desc);
    string filter_profile_title =
        Substitute("$0$1 ($2)", RuntimeProfile::PREFIX_FILTER, filter_desc.filter_id,
            PrettyPrinter::Print(filter_ctx.filter->filter_size(), TUnit::BYTES));
    RuntimeProfile* filter_profile =
        RuntimeProfile::Create(state->obj_pool(), filter_profile_title, false);
    runtime_profile_->AddChild(filter_profile);
    filter_ctx.stats = state->obj_pool()->Add(new FilterStats(filter_profile));
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(*filter_exprs[i], state, pool_,
        expr_perm_pool(), expr_results_pool(), &filter_ctx.expr_eval));
  }
  filter_stats_.resize(filter_ctxs_.size());

  // Must match CTEProducerPlanNode::GetCTEName. CTEProducerPlanNode::Init registers CTEs
  // before fragment instances begin execution, so the mapping must exist here.
  if (auto it = state->instance_ctx().cte_consumer_to_producer_idx.find(id());
      it != state->instance_ctx().cte_consumer_to_producer_idx.end()) {
    name_ = plan_node().tnode_->cte_consumer.name + "_" + std::to_string(it->second);
  } else {
    // No CTE producer scheduled on this node for this consumer. Already logged in
    // Scheduler::ComputeFragmentExecParams.
    name_ = plan_node().tnode_->cte_consumer.name;
  }
  return Status::OK();
}

Status CTEConsumerNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile()->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  if (!buffer_pool_client()->is_registered()) {
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }

  if (!is_passthrough_) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Open(input_expr_evals_, state));
  }

  for (FilterContext& ctx : filter_ctxs_) {
    RETURN_IF_ERROR(ctx.expr_eval->Open(state));
  }

  DCHECK_EQ(nullptr, exchanger_);
  VLOG_QUERY << "Finding CTE exchange " << name_ << " in instance "
      << state->instance_ctx().per_fragment_instance_idx << " of " << label();
  exchanger_ = state->query_state()->GetExchanger(name_);
  if (exchanger_ != nullptr) {
    consumer_index_ = exchanger_->Open();
  } else {
    VLOG_QUERY << "No CTE exchanger present for CTE consumer: " << name_;
  }

  return Status::OK();
}

Status CTEConsumerNode::GetNext(
    RuntimeState* state, RowBatch* output_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile()->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (exchanger_ == nullptr) {
    *eos = true;
    return Status::OK();
  }

  // Fetch rows from LocalExchanger
  RowBatch* input_batch = exchanger_->Pull(consumer_index_, eos);
  if (input_batch == nullptr) {
    // Pull only returns nullptr when eos_ is set and no batches remain.
    DCHECK(*eos);
    return Status::OK();
  }

  VLOG_PROGRESS << "Pulled " << input_batch->num_rows() << " rows from CTE exchange "
      << name_ << " in instance " << state->instance_ctx().per_fragment_instance_idx
      << " of " << label();
  if (is_passthrough_) {
    int rows_to_copy = input_batch->num_rows();
    if (rows_to_copy > 0) {
      DCHECK_LE(rows_to_copy, output_batch->capacity() - output_batch->num_rows())
          << "Output batch capacity: " << output_batch->capacity()
          << ", current rows: " << output_batch->num_rows()
          << ", input batch rows: " << input_batch->num_rows();
      int dst_offset = output_batch->AddRows(rows_to_copy);
      output_batch->CopyRows(input_batch, rows_to_copy, 0, dst_offset);
      output_batch->CommitRows(rows_to_copy);
      // Ensure blocking operators make a deep copy of data if they need to retain it.
      // Heap memory is re-used from input_batch; if this is the last reference to the
      // Cell in LocalExchanger, it will be freed on the next GetNext() call.
      output_batch->MarkNeedsDeepCopy();
    }
  } else {
    // Copy input_batch to output_batch while translating slots.
    int64_t tuple_buf_size;
    uint8_t* tuple_buf;
    RETURN_IF_ERROR(output_batch->ResizeAndAllocateTupleBuffer(
        state, &tuple_buf_size, &tuple_buf));
    memset(tuple_buf, 0, tuple_buf_size);
    CTEConsumerPlanNode::MaterializeBatchFn fn = codegend_materialize_batch_fn_.load();
    if (fn == nullptr) {
      MaterializeBatch(input_batch, output_batch, &tuple_buf);
    } else {
      fn(this, input_batch, output_batch, &tuple_buf);
    }
  }

  if (!filter_ctxs_.empty()) {
    if (!filters_waited_) {
      filters_waited_ = true;
      WaitForRuntimeFilters(state, filter_ctxs_);
    }
    FilterRowBatch(output_batch);
  }

  CheckLimitAndTruncateRowBatchIfNeeded(output_batch, eos);
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

Status CTEConsumerNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  // Reset() is not supported.
  const char* msg = "Internal error: CTE consumer nodes should not appear in subplans.";
  DCHECK(false) << msg;
  return Status(msg);
}

void CTEConsumerNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  FinalizeFilters(state);
  if (exchanger_) {
    VLOG_QUERY << "Closing consumer of CTE exchange " << name_ << " in instance "
        << state->instance_ctx().per_fragment_instance_idx << " of " << label();
    exchanger_->CloseConsumer(consumer_index_);
  }
  if (!is_passthrough_) {
    ScalarExprEvaluator::Close(input_expr_evals_, state);
  }
  ExecNode::Close(state);
}

void CTEConsumerNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ') << "CTEConsumerNode(" << name_;
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

bool CTEConsumerNode::EvalRuntimeFilters(
    TupleRow* row, vector<LocalFilterContext>& local_filter_ctxs) noexcept {
  for (LocalFilterContext& local_filter_ctx : local_filter_ctxs) {
    LocalFilterStats& stats = local_filter_ctx.stats;
    ++stats.total_possible;
    if (!stats.enabled_for_row || !local_filter_ctx.filter_ctx.filter->HasFilter()) {
      continue;
    }
    ++stats.considered;
    if (!local_filter_ctx.filter_ctx.Eval(row)) {
      ++stats.rejected;
      return false;
    }
  }
  return true;
}

void CTEConsumerNode::FilterRowBatch(RowBatch* batch) noexcept {
  int num_rows = batch->num_rows();
  vector<LocalFilterContext> local_filter_ctxs;
  local_filter_ctxs.reserve(filter_ctxs_.size());
  for (int i = 0; i < filter_ctxs_.size(); ++i) {
    local_filter_ctxs.emplace_back(filter_ctxs_[i], filter_stats_[i]);
  }
  int out_idx = 0;
  for (int i = 0; i < num_rows; ++i) {
    TupleRow* row = batch->GetRow(i);
    if (EvalRuntimeFilters(row, local_filter_ctxs)) {
      if (out_idx != i) batch->CopyRows(out_idx, i, 1);
      ++out_idx;
    }
  }
  batch->set_num_rows(out_idx);

  ++row_batches_since_filter_check_;
  if (row_batches_since_filter_check_ == BATCHES_PER_FILTER_SELECTIVITY_CHECK) {
    CheckFiltersEffectiveness();
    row_batches_since_filter_check_ = 0;
  }
}

void CTEConsumerNode::CheckFiltersEffectiveness() noexcept {
  for (int i = 0; i < filter_stats_.size(); ++i) {
    LocalFilterStats& stats = filter_stats_[i];
    const RuntimeFilter* filter = filter_ctxs_[i].filter;
    double reject_ratio = stats.rejected / static_cast<double>(stats.considered);
    if (filter->AlwaysTrue() || reject_ratio < FLAGS_min_filter_reject_ratio) {
      stats.enabled_for_row = false;
    }
  }
}

void CTEConsumerNode::FinalizeFilters(RuntimeState* state) noexcept {
  for (int i = 0; i < filter_stats_.size(); ++i) {
    const LocalFilterStats& stats = filter_stats_[i];
    filter_ctxs_[i].stats->IncrCounters(FilterStats::ROWS_KEY,
        stats.total_possible, stats.considered, stats.rejected);
  }
  for (const FilterContext& ctx : filter_ctxs_) {
    if (ctx.stats != nullptr) {
      if (ctx.stats->HasRejectedRows()) {
        effective_filter_ids_.push_back(ctx.filter->id());
      }
      ctx.expr_eval->Close(state);
    }
  }
}
}
