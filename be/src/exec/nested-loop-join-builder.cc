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

#include <utility>

#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace impala;

DataSink* NljBuilderConfig::CreateSink(const TPlanFragmentCtx& fragment_ctx,
    const TPlanFragmentInstanceCtx& fragment_instance_ctx,
    RuntimeState* state) const {
  // We have one fragment per sink, so we can use the fragment index as the sink ID.
  TDataSinkId sink_id = fragment_ctx.fragment.idx;
  return NljBuilder::CreateSeparateBuilder(sink_id, *this, state);
}

Status NljBuilderConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, RuntimeState* state) {
  RETURN_IF_ERROR(JoinBuilderConfig::Init(tsink, input_row_desc, state));
  return Status::OK();
}

NljBuilder* NljBuilder::CreateEmbeddedBuilder(
    const RowDescriptor* row_desc, RuntimeState* state, int join_node_id) {
  ObjectPool* pool = state->obj_pool();
  NljBuilderConfig* sink_config = pool->Add(new NljBuilderConfig());
  sink_config->join_node_id_ = join_node_id;
  sink_config->tsink_ = pool->Add(new TDataSink());
  sink_config->input_row_desc_ = row_desc;
  return pool->Add(new NljBuilder(*sink_config, state));
}

NljBuilder::NljBuilder(
    TDataSinkId sink_id, const NljBuilderConfig& sink_config, RuntimeState* state)
  : JoinBuilder(sink_id, sink_config, "Nested Loop Join Builder", state),
    build_batch_cache_(row_desc_, state->batch_size()) {}


NljBuilder::NljBuilder(const NljBuilderConfig& sink_config, RuntimeState* state)
  : JoinBuilder(-1, sink_config, "Nested Loop Join Builder", state),
    build_batch_cache_(row_desc_, state->batch_size()) {}

NljBuilder* NljBuilder::CreateSeparateBuilder(
    TDataSinkId sink_id, const NljBuilderConfig& sink_config, RuntimeState* state) {
  return state->obj_pool()->Add(new NljBuilder(sink_id, sink_config, state));
}

NljBuilder::~NljBuilder() {}

Status NljBuilder::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  return Status::OK();
}

Status NljBuilder::Open(RuntimeState* state) {
  return Status::OK();
}

Status NljBuilder::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
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
