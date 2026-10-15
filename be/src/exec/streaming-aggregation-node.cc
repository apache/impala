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

#include "exec/exec-node-util.h"
#include "gutil/strings/substitute.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

namespace impala {

StreamingAggregationNode::StreamingAggregationNode(
    ObjectPool* pool, const AggregationPlanNode& pnode, const DescriptorTbl& descs)
  : AggregationNodeBase(pool, pnode, descs) {
  DCHECK(pnode.tnode_->conjuncts.empty()) << "Preaggs have no conjuncts";
  DCHECK(limit_ == -1 || (limit_ != -1 && fast_limit_check_))
      << "Preaggs have no limits";
  for (auto& t_agg : pnode.tnode_->agg_node.aggregators) {
    DCHECK(t_agg.use_streaming_preaggregation);
  }
  if (chain_aggregators_) {
    DCHECK_GT(aggs_.size(), 1);
    DCHECK(!fast_limit_check_);
    runtime_profile_->AppendExecOption("Chained Aggregators");
  }
}

Status StreamingAggregationNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  // Open the child before consuming resources in this node.
  RETURN_IF_ERROR(child(0)->Open(state));
  RETURN_IF_ERROR(ExecNode::Open(state));

  for (auto& agg : aggs_) RETURN_IF_ERROR(agg->Open(state));

  // Streaming preaggregations do all processing in GetNext().
  return Status::OK();
}

Status StreamingAggregationNode::GetNext(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);

  if (!fast_limit_check_ && ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }

  if (chain_aggregators_) {
    RETURN_IF_ERROR(GetNextChained(state, row_batch, eos));
    IncrementNumRowsReturned(row_batch->num_rows());
    COUNTER_SET(rows_returned_counter_, rows_returned());
    return Status::OK();
  }

  // With multiple Aggregators, each will only set a single tuple per row. We rely on the
  // other tuples to be null to detect which Aggregator set which row.
  if (aggs_.size() > 1) row_batch->ClearTuplePointers();

  if (!child_eos_ || !child_batch_processed_) {
    // For streaming preaggregations, we process rows from the child as we go.
    RETURN_IF_ERROR(GetRowsStreaming(state, row_batch));
    *eos = false;
  } else {
    bool aggregator_eos = false;
    RETURN_IF_ERROR(
        aggs_[curr_output_agg_idx_]->GetNext(state, row_batch, &aggregator_eos));
    if (aggregator_eos) ++curr_output_agg_idx_;
    *eos = curr_output_agg_idx_ >= aggs_.size();
  }

  IncrementNumRowsReturned(row_batch->num_rows());
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

Status StreamingAggregationNode::GetRowsStreaming(
    RuntimeState* state, RowBatch* out_batch) {
  if (child_batch_ == nullptr) {
    child_batch_.reset(
        new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
  }

  int num_aggs = aggs_.size();
  // Create mini batches.
  vector<unique_ptr<RowBatch>> mini_batches;
  if (!replicate_input_ && num_aggs > 1) {
    for (int i = 0; i < num_aggs; ++i) {
      mini_batches.push_back(make_unique<RowBatch>(
          child(0)->row_desc(), state->batch_size(), mem_tracker()));
    }
  }

  do {
    DCHECK_EQ(out_batch->num_rows(), 0);
    RETURN_IF_CANCELLED(state);

    if (child_batch_processed_) {
      DCHECK_EQ(child_batch_->num_rows(), 0);
      RETURN_IF_ERROR(child(0)->GetNext(state, child_batch_.get(), &child_eos_));
      child_batch_processed_ = false;
    }

    if (num_aggs == 1) {
      RETURN_IF_ERROR(aggs_[0]->AddBatchStreaming(
          state, out_batch, child_batch_.get(), &child_batch_processed_));
      // We're not guaranteed to be able to stream the entirety of 'child_batch_' into
      // 'out_batch' as AddBatchStreaming() will attach all var-len data to 'out_batch'
      // and 'child_batch_' may have been referencing data that wasn't attached to it.
      if (child_batch_processed_) {
        child_batch_->Reset();
      }
      if (fast_limit_check_) {
        DCHECK(limit() > -1);
        if (aggs_[0]->GetNumKeys() >= limit()) {
          child_eos_ = true;
          child_batch_processed_ = true;
          child_batch_->Reset();
          runtime_profile_->AddInfoString("FastLimitCheckExceededRows",
              SimpleItoa(aggs_[0]->GetNumKeys() - limit()));
          VLOG_QUERY << Substitute("the number of rows ($0) returned from the streaming "
              "aggregation node has exceeded the limit of $1",aggs_[0]->GetNumKeys(),
              limit());
          break;
        }
      }
      continue;
    }

    if (replicate_input_) {
      bool eos = false;
      while (replicate_agg_idx_ < num_aggs) {
        RETURN_IF_ERROR(aggs_[replicate_agg_idx_]->AddBatchStreaming(
            state, out_batch, child_batch_.get(), &eos));
        if (eos) ++replicate_agg_idx_;
        if (out_batch->AtCapacity()) break;
        // If out_batch isn't full, we must have processed the entire input.
        DCHECK(eos);
      }
      if (replicate_agg_idx_ == num_aggs) {
        replicate_agg_idx_ = 0;
        child_batch_processed_ = true;
        child_batch_->Reset();
      }
      continue;
    }

    // Separate input batch into mini batches destined for the different aggs.
    int num_tuples = child(0)->row_desc()->tuple_descriptors().size();
    DCHECK_EQ(num_aggs, num_tuples);
    int num_rows = child_batch_->num_rows();
    DCHECK_LE(num_rows, out_batch->capacity());
    if (num_rows > 0) {
      RETURN_IF_ERROR(SplitMiniBatches(child_batch_.get(), &mini_batches));

      for (int i = 0; i < num_tuples; ++i) {
        RowBatch* mini_batch = mini_batches[i].get();
        if (mini_batch->num_rows() > 0) {
          bool eos;
          RETURN_IF_ERROR(
              aggs_[i]->AddBatchStreaming(state, out_batch, mini_batch, &eos));
          // child_batch_'s size is <= out_batch's capacity, so even under high memory
          // pressure where all rows are streamed though, we will be able to process
          // the entire input. Note that unlike the single agg case above, this works
          // because any node that hits this path must have been preceded by an
          // aggregation node in 'replicate_input_' mode, and so all memory pointed to by
          // 'child_batch_' will be attached to 'child_batch_'.
          DCHECK(eos);
          mini_batch->Reset();
        }
      }
    }
    child_batch_processed_ = true;
    child_batch_->Reset();
  } while (out_batch->num_rows() == 0 && !child_eos_);

  if (child_eos_ && child_batch_processed_) {
    child(0)->Close(state);
    child_batch_.reset();
    for (auto& agg : aggs_) RETURN_IF_ERROR(agg->InputDone());
  }

  return Status::OK();
}

Status StreamingAggregationNode::GetNextChained(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  DCHECK_EQ(row_batch->num_rows(), 0);
  int num_aggs = aggs_.size();
  while (chained_output_batches_.empty() && curr_output_agg_idx_ < num_aggs) {
    RETURN_IF_CANCELLED(state);
    if (!child_eos_) {
      if (child_batch_ == nullptr) {
        child_batch_.reset(
            new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
      }
      RETURN_IF_ERROR(child(0)->GetNext(state, child_batch_.get(), &child_eos_));
      if (child_batch_->num_rows() > 0) {
        RETURN_IF_ERROR(
            AddBatchToChain(state, 0, child_batch_.get(), row_batch->capacity()));
      }
      // The output rows don't reference 'child_batch_'.
      child_batch_->Reset();
      if (child_eos_) {
        child(0)->Close(state);
        child_batch_.reset();
        RETURN_IF_ERROR(aggs_[0]->InputDone());
      }
    } else {
      // Output the hash tables of the current Aggregator and feed them to the next one.
      unique_ptr<RowBatch> batch = GetFreeBatch(row_batch->capacity());
      bool agg_eos = false;
      RETURN_IF_ERROR(aggs_[curr_output_agg_idx_]->GetNext(state, batch.get(), &agg_eos));
      int next_agg_idx = curr_output_agg_idx_ + 1;
      if (batch->num_rows() > 0 && next_agg_idx < num_aggs) {
        RETURN_IF_ERROR(
            AddBatchToChain(state, next_agg_idx, batch.get(), row_batch->capacity()));
      }
      // Keep empty batches too, as resources may be attached to them.
      chained_output_batches_.push_back(move(batch));
      if (agg_eos) {
        curr_output_agg_idx_ = next_agg_idx;
        if (next_agg_idx < num_aggs) RETURN_IF_ERROR(aggs_[next_agg_idx]->InputDone());
      }
    }
  }

  if (!chained_output_batches_.empty()) {
    unique_ptr<RowBatch> batch = move(chained_output_batches_.front());
    chained_output_batches_.pop_front();
    int num_rows = batch->num_rows();
    if (num_rows > 0) {
      DCHECK_LE(num_rows, row_batch->capacity());
      row_batch->CopyRows(batch.get(), num_rows, 0, 0);
      row_batch->CommitRows(num_rows);
    }
    // Also resets 'batch'.
    batch->TransferResourceOwnership(row_batch);
    free_batches_.push_back(move(batch));
  }
  *eos = chained_output_batches_.empty() && curr_output_agg_idx_ >= num_aggs;
  return Status::OK();
}

Status StreamingAggregationNode::AddBatchToChain(
    RuntimeState* state, int agg_idx, RowBatch* batch, int capacity) {
  DCHECK_GT(batch->num_rows(), 0);
  bool eos = false;
  while (!eos) {
    unique_ptr<RowBatch> out_batch = GetFreeBatch(capacity);
    RETURN_IF_ERROR(
        aggs_[agg_idx]->AddBatchStreaming(state, out_batch.get(), batch, &eos));
    if (out_batch->num_rows() > 0) {
      if (agg_idx + 1 < aggs_.size()) {
        RETURN_IF_ERROR(AddBatchToChain(state, agg_idx + 1, out_batch.get(), capacity));
      }
      chained_output_batches_.push_back(move(out_batch));
    } else {
      // Only a full 'out_batch' stops AddBatchStreaming() early. Avoid an endless loop.
      DCHECK(eos);
      if (!eos) return Status("Aggregator made no progress on its input batch");
      out_batch->Reset();
      free_batches_.push_back(move(out_batch));
    }
  }
  return Status::OK();
}

unique_ptr<RowBatch> StreamingAggregationNode::GetFreeBatch(int capacity) {
  unique_ptr<RowBatch> batch;
  if (!free_batches_.empty()) {
    batch = move(free_batches_.back());
    free_batches_.pop_back();
  }
  if (batch == nullptr || batch->capacity() != capacity) {
    batch.reset(new RowBatch(row_desc(), capacity, mem_tracker()));
  }
  // Each Aggregator only sets its own tuple.
  batch->ClearTuplePointers();
  return batch;
}

Status StreamingAggregationNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  DCHECK(false) << "Cannot reset preaggregation";
  return Status("Cannot reset preaggregation");
}

void StreamingAggregationNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  // All expr mem allocations should happen in the Aggregator.
  DCHECK(expr_results_pool() == nullptr
      || expr_results_pool()->total_allocated_bytes() == 0);
  for (auto& agg : aggs_) agg->Close(state);
  child_batch_.reset();
  chained_output_batches_.clear();
  free_batches_.clear();
  ExecNode::Close(state);
}

void StreamingAggregationNode::DebugString(
    int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "StreamingAggregationNode(";
  for (auto& agg : aggs_) agg->DebugString(indentation_level, out);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
} // namespace impala
