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

#include "exec/exec-node-util.h"
#include "gutil/strings/substitute.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

namespace impala {

AggregationNode::AggregationNode(
    ObjectPool* pool, const AggregationPlanNode& pnode, const DescriptorTbl& descs)
  : AggregationNodeBase(pool, pnode, descs) {
  for (auto& t_agg : pnode.tnode_->agg_node.aggregators) {
    DCHECK(!t_agg.use_streaming_preaggregation);
  }
}

Status AggregationNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  // Open the child before consuming resources in this node.
  RETURN_IF_ERROR(child(0)->Open(state));
  RETURN_IF_ERROR(ExecNode::Open(state));
  for (auto& agg : aggs_) RETURN_IF_ERROR(agg->Open(state));
  RowBatch child_batch(child(0)->row_desc(), state->batch_size(), mem_tracker());

  int num_aggs = aggs_.size();
  // Create mini batches.
  vector<unique_ptr<RowBatch>> mini_batches;
  if (!replicate_input_ && num_aggs > 1) {
    for (int i = 0; i < num_aggs; ++i) {
      mini_batches.push_back(make_unique<RowBatch>(
          child(0)->row_desc(), state->batch_size(), mem_tracker()));
    }
  }

  // Read all the rows from the child and process them.
  bool eos = false;
  do {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(children_[0]->GetNext(state, &child_batch, &eos));

    if (num_aggs == 1) {
      RETURN_IF_ERROR(aggs_[0]->AddBatch(state, &child_batch));
      child_batch.Reset();
      if (fast_limit_check_) {
        DCHECK(limit() > -1);
        if (aggs_[0]->GetNumKeys() >= limit()) {
          eos = true;
          runtime_profile_->AddInfoString("FastLimitCheckExceededRows",
              SimpleItoa(aggs_[0]->GetNumKeys() - limit()));
          VLOG_QUERY << Substitute("the number of rows ($0) returned from the "
              "aggregation node has exceeded the limit of $1", aggs_[0]->GetNumKeys(),
              limit());
          break;
        }
      }
      continue;
    }

    if (replicate_input_) {
      for (auto& agg : aggs_) RETURN_IF_ERROR(agg->AddBatch(state, &child_batch));
      child_batch.Reset();
      continue;
    }

    // Separate input batch into mini batches destined for the different aggs.
    int num_tuples = child(0)->row_desc()->tuple_descriptors().size();
    DCHECK_EQ(num_aggs, num_tuples);
    int num_rows = child_batch.num_rows();
    if (num_rows > 0) {
      RETURN_IF_ERROR(SplitMiniBatches(&child_batch, &mini_batches));

      for (int i = 0; i < num_tuples; ++i) {
        RowBatch* mini_batch = mini_batches[i].get();
        if (mini_batch->num_rows() > 0) {
          RETURN_IF_ERROR(aggs_[i]->AddBatch(state, mini_batch));
          mini_batch->Reset();
        }
      }
    }
    child_batch.Reset();
  } while (!eos);

  // The child can be closed at this point in most cases because we have consumed all of
  // the input from the child and transfered ownership of the resources we need. The
  // exception is if we are inside a subplan expecting to call Open()/GetNext() on the
  // child again,
  if (!IsInSubplan()) child(0)->Close(state);

  for (auto& agg : aggs_) RETURN_IF_ERROR(agg->InputDone());
  return Status::OK();
}

Status AggregationNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);

  if (curr_output_agg_idx_ >= aggs_.size() || ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }

  // With multiple Aggregators, each will only set a single tuple per row. We rely on the
  // other tuples to be null to detect which Aggregator set which row.
  if (aggs_.size() > 1) row_batch->ClearTuplePointers();

  bool pagg_eos = false;
  RETURN_IF_ERROR(aggs_[curr_output_agg_idx_]->GetNext(state, row_batch, &pagg_eos));
  if (pagg_eos) ++curr_output_agg_idx_;

  *eos = ReachedLimit() || (pagg_eos && curr_output_agg_idx_ >= aggs_.size());
  IncrementNumRowsReturned(row_batch->num_rows());
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

void AggregationNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  // All expr mem allocations should happen in the Aggregator.
  DCHECK(expr_results_pool() == nullptr
      || expr_results_pool()->total_allocated_bytes() == 0);
  for (auto& agg : aggs_) agg->Close(state);
  ExecNode::Close(state);
}

void AggregationNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "AggregationNode(";
  for (auto& agg : aggs_) agg->DebugString(indentation_level, out);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
} // namespace impala
