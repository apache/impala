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

#include "exec/aggregation-node-base.h"

#include "exec/aggregation-node.h"
#include "exec/grouping-aggregator.h"
#include "exec/non-grouping-aggregator.h"
#include "exec/streaming-aggregation-node.h"
#include "runtime/fragment-state.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

Status AggregationPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  int num_ags = tnode_->agg_node.aggregators.size();
  for (int i = 0; i < num_ags; ++i) {
    const TAggregator& agg = tnode_->agg_node.aggregators[i];
    AggregatorConfig* node = nullptr;
    if (agg.grouping_exprs.empty()) {
      node = state->obj_pool()->Add(new NonGroupingAggregatorConfig(agg, state, this, i));
    } else {
      node = state->obj_pool()->Add(new GroupingAggregatorConfig(agg, state, this, i));
    }
    DCHECK(node != nullptr);
    aggs_.push_back(node);
    RETURN_IF_ERROR(aggs_[i]->Init(agg, state, this));
  }
  DCHECK(aggs_.size() > 0);
  state->CheckAndAddCodegenDisabledMessage(codegen_status_msgs_);
  return Status::OK();
}

void AggregationPlanNode::Close() {
  for (AggregatorConfig* config : aggs_) config->Close();
  PlanNode::Close();
}

Status AggregationPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  if (tnode_->agg_node.aggregators[0].use_streaming_preaggregation) {
    *node = pool->Add(new StreamingAggregationNode(pool, *this, state->desc_tbl()));
  } else {
    *node = pool->Add(new AggregationNode(pool, *this, state->desc_tbl()));
  }
  return Status::OK();
}

AggregationNodeBase::AggregationNodeBase(
    ObjectPool* pool, const AggregationPlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs),
    replicate_input_(pnode.tnode_->agg_node.replicate_input) {
  // Create the Aggregator nodes from their configs.
  int num_aggs = pnode.aggs_.size();
  for (int i = 0; i < num_aggs; ++i) {
    const AggregatorConfig* agg = pnode.aggs_[i];
    unique_ptr<Aggregator> node;
    if (agg->GetNumGroupingExprs() == 0) {
      const NonGroupingAggregatorConfig* non_grouping_config =
          static_cast<const NonGroupingAggregatorConfig*>(agg);
      node.reset(new NonGroupingAggregator(this, pool_, *non_grouping_config));
    } else {
      const GroupingAggregatorConfig* grouping_config =
          static_cast<const GroupingAggregatorConfig*>(agg);
      DCHECK(grouping_config != nullptr);
      node.reset(new GroupingAggregator(this, pool_, *grouping_config,
          pnode.tnode_->agg_node.estimated_input_cardinality));
    }
    aggs_.push_back(std::move(node));
    runtime_profile_->AddChild(aggs_[i]->runtime_profile());
  }
  fast_limit_check_ = pnode.tnode_->agg_node.fast_limit_check;
}

Status AggregationNodeBase::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  for (auto& agg : aggs_) {
    agg->SetDebugOptions(debug_options_);
    RETURN_IF_ERROR(agg->Prepare(state));
  }
  return Status::OK();
}

void AggregationPlanNode::Codegen(FragmentState* state) {
  DCHECK(state->ShouldCodegen());
  PlanNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  for (auto& agg : aggs_) {
    agg->Codegen(state);
  }
}

Status AggregationNodeBase::Reset(RuntimeState* state, RowBatch* row_batch) {
  for (auto& agg : aggs_) RETURN_IF_ERROR(agg->Reset(state, row_batch));
  curr_output_agg_idx_ = 0;
  return ExecNode::Reset(state, row_batch);
}

Status AggregationNodeBase::SplitMiniBatches(
    RowBatch* batch, vector<unique_ptr<RowBatch>>* mini_batches) {
  int num_tuples = child(0)->row_desc()->tuple_descriptors().size();
  int num_rows = batch->num_rows();
  int last_agg_idx = 0;
  for (int i = 0; i < num_rows; ++i) {
    TupleRow* src_row = batch->GetRow(i);
    int dst_agg_idx = -1;
    // Optimization that bets that the index of non-null agg will be the same from one
    // tuple to the next.
    if (src_row->GetTuple(last_agg_idx) != nullptr) {
      dst_agg_idx = last_agg_idx;
    } else {
      for (int j = 0; j < num_tuples; ++j) {
        if (src_row->GetTuple(j) != nullptr) {
          dst_agg_idx = j;
          last_agg_idx = j;
          break;
        }
      }
    }

    DCHECK_GE(dst_agg_idx, 0);
    DCHECK_LT(dst_agg_idx, num_tuples);

    RowBatch* mini_batch = (*mini_batches)[dst_agg_idx].get();
    TupleRow* dst_row = mini_batch->GetRow(mini_batch->AddRow());
    batch->CopyRow(src_row, dst_row);
    mini_batch->CommitLastRow();
  }
  return Status::OK();
}

} // namespace impala
