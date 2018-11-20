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

#include "common/names.h"

using namespace impala;

NljBuilder::NljBuilder(const RowDescriptor* row_desc, RuntimeState* state)
  : DataSink(-1, row_desc, "Nested Loop Join Builder", state),
    build_batch_cache_(row_desc, state->batch_size()) {}

Status NljBuilder::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  return Status::OK();
}

Status NljBuilder::Open(RuntimeState* state) {
  return Status::OK();
}

Status NljBuilder::Send(RuntimeState* state, RowBatch* batch) {
  // Swap the contents of the batch into a batch owned by the builder.
  RowBatch* build_batch = GetNextEmptyBatch();
  build_batch->AcquireState(batch);

  AddBuildBatch(build_batch);
  if (build_batch->needs_deep_copy() || build_batch->num_buffers() > 0) {
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
  if (copied_build_batches_.total_num_rows() > 0) {
    // To simplify things, we only want to process one list, so we need to copy
    // the remaining input batches.
    RETURN_IF_ERROR(DeepCopyBuildBatches(state));
  }

  DCHECK(copied_build_batches_.total_num_rows() == 0 ||
      input_build_batches_.total_num_rows() == 0);
  return Status::OK();
}

void NljBuilder::Reset() {
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
