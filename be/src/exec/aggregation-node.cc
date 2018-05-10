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

#include "exec/aggregation-node.h"

#include <sstream>

#include "exec/grouping-aggregator.h"
#include "exec/non-grouping-aggregator.h"
#include "gutil/strings/substitute.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

namespace impala {

AggregationNode::AggregationNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs) {}

Status AggregationNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));
  if (tnode.agg_node.grouping_exprs.empty()) {
    aggregator_.reset(new NonGroupingAggregator(this, pool_, tnode, state->desc_tbl()));
  } else {
    aggregator_.reset(new GroupingAggregator(this, pool_, tnode, state->desc_tbl()));
  }
  RETURN_IF_ERROR(aggregator_->Init(tnode, state));
  runtime_profile_->AddChild(aggregator_->runtime_profile());
  return Status::OK();
}

Status AggregationNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  aggregator_->SetDebugOptions(debug_options_);
  RETURN_IF_ERROR(aggregator_->Prepare(state));
  state->CheckAndAddCodegenDisabledMessage(runtime_profile());
  return Status::OK();
}

void AggregationNode::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  ExecNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  aggregator_->Codegen(state);
}

Status AggregationNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  // Open the child before consuming resources in this node.
  RETURN_IF_ERROR(child(0)->Open(state));
  RETURN_IF_ERROR(ExecNode::Open(state));

  RETURN_IF_ERROR(aggregator_->Open(state));

  RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
  // Read all the rows from the child and process them.
  bool eos = false;
  do {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    RETURN_IF_ERROR(children_[0]->GetNext(state, &batch, &eos));
    RETURN_IF_ERROR(aggregator_->AddBatch(state, &batch));
    batch.Reset();
  } while (!eos);

  // The child can be closed at this point in most cases because we have consumed all of
  // the input from the child and transfered ownership of the resources we need. The
  // exception is if we are inside a subplan expecting to call Open()/GetNext() on the
  // child again,
  if (!IsInSubplan()) child(0)->Close(state);

  RETURN_IF_ERROR(aggregator_->InputDone());
  return Status::OK();
}

Status AggregationNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }

  RETURN_IF_ERROR(aggregator_->GetNext(state, row_batch, eos));
  num_rows_returned_ += row_batch->num_rows();
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

Status AggregationNode::Reset(RuntimeState* state) {
  RETURN_IF_ERROR(aggregator_->Reset(state));
  return ExecNode::Reset(state);
}

void AggregationNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  aggregator_->Close(state);
  ExecNode::Close(state);
}

void AggregationNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "AggregationNode("
       << "aggregator=" << aggregator_->DebugString();
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
} // namespace impala
