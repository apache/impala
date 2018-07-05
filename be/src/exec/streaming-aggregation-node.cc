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

#include "exec/streaming-aggregation-node.h"

#include <sstream>

#include "gutil/strings/substitute.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

namespace impala {

StreamingAggregationNode::StreamingAggregationNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs), child_eos_(false) {
  DCHECK(conjunct_evals_.empty()) << "Preaggs have no conjuncts";
  DCHECK(!tnode.agg_node.grouping_exprs.empty()) << "Streaming preaggs do grouping";
  DCHECK(limit_ == -1) << "Preaggs have no limits";
}

Status StreamingAggregationNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));
  aggregator_.reset(new GroupingAggregator(this, pool_, tnode, state->desc_tbl()));
  RETURN_IF_ERROR(aggregator_->Init(tnode, state));
  runtime_profile_->AddChild(aggregator_->runtime_profile());
  return Status::OK();
}

Status StreamingAggregationNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  aggregator_->SetDebugOptions(debug_options_);
  RETURN_IF_ERROR(aggregator_->Prepare(state));
  state->CheckAndAddCodegenDisabledMessage(runtime_profile());
  return Status::OK();
}

void StreamingAggregationNode::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  ExecNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;

  aggregator_->Codegen(state);
}

Status StreamingAggregationNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  // Open the child before consuming resources in this node.
  RETURN_IF_ERROR(child(0)->Open(state));
  RETURN_IF_ERROR(ExecNode::Open(state));

  RETURN_IF_ERROR(aggregator_->Open(state));

  // Streaming preaggregations do all processing in GetNext().
  return Status::OK();
}

Status StreamingAggregationNode::GetNext(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }

  bool aggregator_eos = false;
  if (!child_eos_) {
    // For streaming preaggregations, we process rows from the child as we go.
    RETURN_IF_ERROR(GetRowsStreaming(state, row_batch));
  } else {
    RETURN_IF_ERROR(aggregator_->GetNext(state, row_batch, &aggregator_eos));
  }

  *eos = aggregator_eos && child_eos_;
  num_rows_returned_ += row_batch->num_rows();
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

Status StreamingAggregationNode::GetRowsStreaming(
    RuntimeState* state, RowBatch* out_batch) {
  DCHECK(!child_eos_);

  if (child_batch_ == nullptr) {
    child_batch_.reset(
        new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
  }

  do {
    DCHECK_EQ(out_batch->num_rows(), 0);
    RETURN_IF_CANCELLED(state);

    RETURN_IF_ERROR(child(0)->GetNext(state, child_batch_.get(), &child_eos_));

    RETURN_IF_ERROR(aggregator_->AddBatchStreaming(state, out_batch, child_batch_.get()));
    child_batch_->Reset(); // All rows from child_batch_ were processed.
  } while (out_batch->num_rows() == 0 && !child_eos_);

  if (child_eos_) {
    child(0)->Close(state);
    child_batch_.reset();
    RETURN_IF_ERROR(aggregator_->InputDone());
  }

  return Status::OK();
}

Status StreamingAggregationNode::Reset(RuntimeState* state) {
  DCHECK(false) << "Cannot reset preaggregation";
  return Status("Cannot reset preaggregation");
}

void StreamingAggregationNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  // All expr mem allocations should happen in the Aggregator.
  DCHECK(expr_results_pool() == nullptr
      || expr_results_pool()->total_allocated_bytes() == 0);
  aggregator_->Close(state);
  child_batch_.reset();
  ExecNode::Close(state);
}

void StreamingAggregationNode::DebugString(
    int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "StreamingAggregationNode("
       << "aggregator=" << aggregator_->DebugString();
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
} // namespace impala
