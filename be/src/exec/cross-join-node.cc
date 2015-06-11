// Copyright 2013 Cloudera Inc.
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

#include "exec/cross-join-node.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using namespace impala;
using namespace llvm;

CrossJoinNode::CrossJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode("CrossJoinNode", TJoinOp::CROSS_JOIN, pool, tnode, descs) {
}

Status CrossJoinNode::Prepare(RuntimeState* state) {
  DCHECK(join_op_ == TJoinOp::CROSS_JOIN);
  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));
  return Status::OK();
}

Status CrossJoinNode::Reset(RuntimeState* state) {
  build_batch_pool_.Clear();
  build_batches_.Reset();
  return BlockingJoinNode::Reset(state);
}

void CrossJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  build_batch_pool_.Clear();
  BlockingJoinNode::Close(state);
}

Status CrossJoinNode::ConstructBuildSide(RuntimeState* state) {
  // Do a full scan of child(1) and store all build row batches.
  RETURN_IF_ERROR(child(1)->Open(state));
  while (true) {
    RowBatch* batch = build_batch_pool_.Add(
        new RowBatch(child(1)->row_desc(), state->batch_size(), mem_tracker()));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    bool eos;
    RETURN_IF_ERROR(child(1)->GetNext(state, batch, &eos));
    DCHECK_EQ(batch->num_io_buffers(), 0) << "Build batch should be compact.";
    SCOPED_TIMER(build_timer_);
    build_batches_.AddRowBatch(batch);
    VLOG_ROW << BuildListDebugString();
    COUNTER_SET(build_row_counter_,
        static_cast<int64_t>(build_batches_.total_num_rows()));
    if (eos) break;
  }
  return Status::OK();
}

Status CrossJoinNode::InitGetNext(TupleRow* first_left_row) {
  current_build_row_ = build_batches_.Iterator();
  return Status::OK();
}

Status CrossJoinNode::GetNext(RuntimeState* state, RowBatch* output_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  if (ReachedLimit() || eos_) {
    *eos = true;
    probe_batch_->TransferResourceOwnership(output_batch);
    build_batches_.TransferResourceOwnership(output_batch);
    return Status::OK();
  }
  *eos = false;

  ScopedTimer<MonotonicStopWatch> timer(probe_timer_);
  while (!eos_) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));

    // Compute max rows that should be added to output_batch
    int64_t max_added_rows = output_batch->capacity() - output_batch->num_rows();
    if (limit() != -1) max_added_rows = min(max_added_rows, limit() - rows_returned());

    // Continue processing this row batch
    num_rows_returned_ +=
        ProcessLeftChildBatch(output_batch, probe_batch_.get(), max_added_rows);
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);

    if (ReachedLimit() || output_batch->AtCapacity()) {
      *eos = ReachedLimit();
      break;
    }

    // Check to see if we're done processing the current left child batch
    if (current_build_row_.AtEnd() && probe_batch_pos_ == probe_batch_->num_rows()) {
      probe_batch_->TransferResourceOwnership(output_batch);
      probe_batch_pos_ = 0;
      if (output_batch->AtCapacity()) break;
      if (probe_side_eos_) {
        *eos = eos_ = true;
        break;
      } else {
        timer.Stop();
        RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &probe_side_eos_));
        timer.Start();
        COUNTER_ADD(probe_row_counter_, probe_batch_->num_rows());
      }
    }
  }

  if (*eos) {
    probe_batch_->TransferResourceOwnership(output_batch);
    build_batches_.TransferResourceOwnership(output_batch);
  }
  return Status::OK();
}

string CrossJoinNode::BuildListDebugString() {
  stringstream out;
  out << "BuildList(";
  out << build_batches_.DebugString(child(1)->row_desc());
  out << ")";
  return out.str();
}

// TODO: this can be replaced with a codegen'd function
int CrossJoinNode::ProcessLeftChildBatch(RowBatch* output_batch, RowBatch* batch,
    int max_added_rows) {
  int row_idx = output_batch->AddRows(max_added_rows);
  DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
  uint8_t* output_row_mem = reinterpret_cast<uint8_t*>(output_batch->GetRow(row_idx));
  TupleRow* output_row = reinterpret_cast<TupleRow*>(output_row_mem);

  int rows_returned = 0;
  ExprContext* const* ctxs = &conjunct_ctxs_[0];

  while (true) {
    while (!current_build_row_.AtEnd()) {
      CreateOutputRow(output_row, current_probe_row_, current_build_row_.GetRow());
      current_build_row_.Next();

      if (!EvalConjuncts(ctxs, conjunct_ctxs_.size(), output_row)) continue;
      ++rows_returned;
      // Filled up out batch or hit limit
      if (UNLIKELY(rows_returned == max_added_rows)) goto end;
      // Advance to next out row
      output_row_mem += output_batch->row_byte_size();
      output_row = reinterpret_cast<TupleRow*>(output_row_mem);
    }

    DCHECK(current_build_row_.AtEnd());
    // Advance to the next row in the left child batch
    if (UNLIKELY(probe_batch_pos_ == batch->num_rows())) goto end;
    current_probe_row_ = batch->GetRow(probe_batch_pos_++);
    current_build_row_ = build_batches_.Iterator();
  }

end:
  output_batch->CommitRows(rows_returned);
  return rows_returned;
}
