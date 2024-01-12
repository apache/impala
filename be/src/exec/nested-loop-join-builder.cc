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

#include "exec/nested-loop-join-builder.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"

#include <utility>

#include "runtime/fragment-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "service/hs2-util.h"
#include "util/min-max-filter.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace impala;

DataSink* NljBuilderConfig::CreateSink(RuntimeState* state) const {
  // We have one fragment per sink, so we can use the fragment index as the sink ID.
  TDataSinkId sink_id = state->fragment().idx;
  return NljBuilder::CreateSeparateBuilder(sink_id, *this, state);
}

Status NljBuilderConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, FragmentState* state) {
  RETURN_IF_ERROR(JoinBuilderConfig::Init(tsink, input_row_desc, state));
  const TJoinBuildSink& join_build_sink = tsink.join_build_sink;
  const vector<TRuntimeFilterDesc>& filter_descs = join_build_sink.runtime_filters;
  RETURN_IF_ERROR(InitExprsAndFilters(filter_descs, state));
  return Status::OK();
}

Status NljBuilderConfig::DoInitExprsAndFilters(
    const vector<TRuntimeFilterDesc>& filter_descs,
    const vector<TRuntimeFilterSource>& filters_produced, FragmentState* state) {
  // Skip over filters that are not produced by the instances of the builder, i.e.
  // broadcast filters where this instance was not selected as a filter producer.
  for (const TRuntimeFilterDesc& filter_desc : filter_descs) {
    DCHECK(state->query_options().runtime_filter_mode == TRuntimeFilterMode::GLOBAL
        || filter_desc.is_broadcast_join || state->query_options().num_nodes == 1);
    DCHECK(!state->query_options().disable_row_runtime_filtering
        || filter_desc.applied_on_partition_columns);
    auto it = std::find_if(filters_produced.begin(), filters_produced.end(),
        [this, &filter_desc](const TRuntimeFilterSource f) {
          return f.src_node_id == join_node_id_ && f.filter_id == filter_desc.filter_id;
        });
    if (it == filters_produced.end()) continue;
    filter_descs_.push_back(filter_desc);

    ScalarExpr* filter_expr = nullptr;
    RETURN_IF_ERROR(
        ScalarExpr::Create(filter_desc.src_expr, *input_row_desc_, state, &filter_expr));
    filter_exprs_.push_back(filter_expr);
  }
  return Status::OK();
}

Status NljBuilderConfig::InitExprsAndFilters(
    const vector<TRuntimeFilterDesc>& filter_descs, FragmentState* state) {
  const std::vector<const TPlanFragmentInstanceCtx*>& instance_ctxs =
      state->instance_ctxs();
  // Skip over filters that are not produced by the instances of the builder, i.e.
  // broadcast filters where this instance was not selected as a filter producer.
  // We can pick any instance since the filters produced should be the same for all
  // instances.
  if (instance_ctxs.size() > 0) {
    const TPlanFragmentInstanceCtx& instance_ctx = *instance_ctxs[0];
    const vector<TRuntimeFilterSource>& filters_produced = instance_ctx.filters_produced;
    return DoInitExprsAndFilters(filter_descs, filters_produced, state);
  }
  return Status::OK();
}

Status NljBuilderConfig::InitExprsAndFilters(
    const vector<TRuntimeFilterDesc>& filter_descs, RuntimeState* state) {
  QueryState* queryState = state->query_state();
  FragmentState* fragmentState = queryState->findFragmentState(state->fragment().idx);
  DCHECK(fragmentState != nullptr);
  // Skip over filters that are not produced by the instances of the builder, i.e.
  // broadcast filters where this instance was not selected as a filter producer.
  const TPlanFragmentInstanceCtx& instance_ctx = state->instance_ctx();
  // We can pick any instance since the filters produced should be the same for all
  // instances.
  const vector<TRuntimeFilterSource>& filters_produced = instance_ctx.filters_produced;

  return DoInitExprsAndFilters(filter_descs, filters_produced, fragmentState);
}

void NljBuilderConfig::Close() {
  ScalarExpr::Close(filter_exprs_);
  DataSinkConfig::Close();
}

Status NljBuilder::CreateEmbeddedBuilder(const RowDescriptor* row_desc,
    RuntimeState* state, int join_node_id,
    const std::vector<TRuntimeFilterDesc>& filters, NljBuilder** nlj_builder) {
  ObjectPool* pool = state->obj_pool();
  NljBuilderConfig* sink_config = pool->Add(new NljBuilderConfig());
  sink_config->join_node_id_ = join_node_id;
  sink_config->tsink_ = pool->Add(new TDataSink());
  sink_config->input_row_desc_ = row_desc;
  RETURN_IF_ERROR(sink_config->InitExprsAndFilters(filters, state));
  *nlj_builder = pool->Add(new NljBuilder(*sink_config, state));
  return Status::OK();
}

NljBuilder* NljBuilder::CreateSeparateBuilder(
    TDataSinkId sink_id, const NljBuilderConfig& sink_config, RuntimeState* state) {
  return state->obj_pool()->Add(new NljBuilder(sink_id, sink_config, state));
}

void NljBuilder::InitFilterContexts(
    const NljBuilderConfig& sink_config, RuntimeState* state) {
  for (const TRuntimeFilterDesc& filter_desc : sink_config.filter_descs_) {
    filter_ctxs_.emplace_back();
    filter_ctxs_.back().filter = state->filter_bank()->RegisterProducer(filter_desc);
  }
}

NljBuilder::NljBuilder(
    TDataSinkId sink_id, const NljBuilderConfig& sink_config, RuntimeState* state)
  : JoinBuilder(sink_id, sink_config,
      ConstructBuilderName("Nested Loop", sink_config.join_node_id_), state),
    build_batch_cache_(row_desc_, state->batch_size()),
    filter_exprs_(sink_config.filter_exprs_),
    minmax_filter_threshold_(0.0),
    runtime_state_(state) {
  InitFilterContexts(sink_config, state);
}

NljBuilder::NljBuilder(const NljBuilderConfig& sink_config, RuntimeState* state)
  : JoinBuilder(-1, sink_config,
      ConstructBuilderName("Nested Loop", sink_config.join_node_id_), state),
    build_batch_cache_(row_desc_, state->batch_size()),
    filter_exprs_(sink_config.filter_exprs_),
    minmax_filter_threshold_(0.0),
    runtime_state_(state) {
  InitFilterContexts(sink_config, state);
}

NljBuilder::~NljBuilder() {}

Status NljBuilder::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  num_build_rows_ = ADD_COUNTER(profile(), "BuildRows", TUnit::UNIT);

  DCHECK_EQ(filter_exprs_.size(), filter_ctxs_.size());
  for (int i = 0; i < filter_exprs_.size(); ++i) {
    RETURN_IF_ERROR(
        ScalarExprEvaluator::Create(*filter_exprs_[i], state, state->obj_pool(),
            expr_perm_pool_.get(), expr_results_pool_.get(), &filter_ctxs_[i].expr_eval));
  }
  return Status::OK();
}

Status NljBuilder::Open(RuntimeState* state) {
  RETURN_IF_ERROR(DataSink::Open(state));

  for (const FilterContext& ctx : filter_ctxs_) {
    RETURN_IF_ERROR(ctx.expr_eval->Open(state));
  }

  AllocateRuntimeFilters();
  return Status::OK();
}

Status NljBuilder::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  int num_input_rows = batch->num_rows();
  // Swap the contents of the batch into a batch owned by the builder.
  RowBatch* build_batch = GetNextEmptyBatch();
  build_batch->AcquireState(batch);

  AddBuildBatch(build_batch);
  if (is_separate_build_
      || build_batch->flush_mode() == RowBatch::FlushMode::FLUSH_RESOURCES
      || build_batch->num_buffers() > 0) {
    // This batch and earlier batches may refer to resources passed from the child
    // that aren't owned by the row batch itself. Deep copying ensures that the row
    // batches are backed by memory owned by this node that is safe to hold on to.
    //
    // Acquiring ownership of attached Blocks or Buffers does not correctly update the
    // accounting, so also copy data in that cases to avoid stealing reservation
    // from whoever created the Block/Buffer. TODO: remove workaround when IMPALA-4179
    // is fixed.
    RETURN_IF_ERROR(DeepCopyBuildBatches(state));
  }
  COUNTER_ADD(num_build_rows_, num_input_rows);
  return Status::OK();
}

Status NljBuilder::FlushFinal(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  if (copied_build_batches_.total_num_rows() > 0) {
    // To simplify things, we only want to process one list, so we need to copy
    // the remaining input batches.
    RETURN_IF_ERROR(DeepCopyBuildBatches(state));
  }

  DCHECK(copied_build_batches_.total_num_rows() == 0 ||
      input_build_batches_.total_num_rows() == 0);

  PublishRuntimeFilters(
      copied_build_batches_.total_num_rows() + input_build_batches_.total_num_rows());

  if (is_separate_build_) HandoffToProbesAndWait(state);

  return Status::OK();
}

void NljBuilder::Reset() {
  DCHECK(!is_separate_build_);
  build_batch_cache_.Reset();
  input_build_batches_.Reset();
  copied_build_batches_.Reset();
}

void NljBuilder::Close(RuntimeState* state) {
  if (closed_) return;
  build_batch_cache_.Clear();
  input_build_batches_.Reset();
  copied_build_batches_.Reset();

  for (const FilterContext& ctx : filter_ctxs_) {
    if (ctx.expr_eval != nullptr) ctx.expr_eval->Close(state);
  }

  DataSink::Close(state);
  closed_ = true;
}

Status NljBuilder::DeepCopyBuildBatches(RuntimeState* state) {
  for (RowBatchList::BatchIterator it = input_build_batches_.BatchesBegin();
    it != input_build_batches_.BatchesEnd(); ++it) {
    RowBatch* input_batch = *it;
    // TODO: it would be more efficient to do the deep copy within the same batch, rather
    // than to a new batch.
    RowBatch* copied_batch = build_batch_cache_.GetNextBatch(mem_tracker());
    input_batch->DeepCopyTo(copied_batch);
    copied_build_batches_.AddRowBatch(copied_batch);
    // Reset input batches as we go to free up memory if possible.
    input_batch->Reset();

    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->CheckQueryState());
  }
  input_build_batches_.Reset();
  return Status::OK();
}

void NljBuilder::AllocateRuntimeFilters() {
  for (int i = 0; i < filter_ctxs_.size(); ++i) {
    DCHECK(filter_ctxs_[i].filter->is_min_max_filter());
    filter_ctxs_[i].local_min_max_filter =
        runtime_state_->filter_bank()->AllocateScratchMinMaxFilter(
            filter_ctxs_[i].filter->id(), filter_ctxs_[i].expr_eval->root().type());
  }
  minmax_filter_threshold_ =
      (float)(runtime_state_->query_options().minmax_filter_threshold);
}

void NljBuilder::InsertRuntimeFilters(
    FilterContext filter_ctxs[], TupleRow* build_row) noexcept {
  // For the only interpreted path we can directly use the filter_ctxs_ member variable.
  DCHECK_EQ(filter_ctxs_.data(), filter_ctxs);
  for (const FilterContext& ctx : filter_ctxs_) ctx.InsertPerCompareOp(build_row);
}

void NljBuilder::PublishRuntimeFilters(int64_t num_build_rows) {
  JoinBuilder::PublishRuntimeFilters(
      filter_ctxs_, runtime_state_, minmax_filter_threshold_, num_build_rows);
}
