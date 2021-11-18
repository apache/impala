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

#include "exec/subplan-node.h"

#include "exec/exec-node-util.h"
#include "exec/singular-row-src-node.h"
#include "exec/subplan-node.h"
#include "exec/unnest-node.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

namespace impala {

Status SubplanPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  DCHECK_EQ(children_.size(), 2);
  RETURN_IF_ERROR(SetContainingSubplan(state, this, children_[1]));
  return Status::OK();
}

Status SubplanPlanNode::SetContainingSubplan(
    FragmentState* state, SubplanPlanNode* ancestor, PlanNode* node) {
  node->containing_subplan_ = ancestor;
  if (node->tnode_->node_type == TPlanNodeType::SUBPLAN_NODE) {
    // Only traverse the first child and not the second one, because the Subplan
    // parent of nodes inside it should be 'node' and not 'ancestor'.
    RETURN_IF_ERROR(SetContainingSubplan(state, ancestor, node->children_[0]));
  } else {
    if (node->tnode_->node_type == TPlanNodeType::UNNEST_NODE) {
      UnnestPlanNode* unnest_node = reinterpret_cast<UnnestPlanNode*>(node);
      RETURN_IF_ERROR(unnest_node->InitCollExprs(state));
    }
    int num_children = node->children_.size();
    for (int i = 0; i < num_children; ++i) {
      RETURN_IF_ERROR(SetContainingSubplan(state, ancestor, node->children_[i]));
    }
  }
  return Status::OK();
}

Status SubplanPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new SubplanNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

SubplanNode::SubplanNode(
    ObjectPool* pool, const SubplanPlanNode& pnode, const DescriptorTbl& descs)
    : ExecNode(pool, pnode, descs),
      input_batch_(NULL),
      input_eos_(false),
      input_row_idx_(0),
      current_input_row_(NULL),
      subplan_is_open_(false),
      subplan_eos_(false) {
}

void SubplanNode::SetContainingSubplan(SubplanNode* ancestor, ExecNode* node) {
  node->set_containing_subplan(ancestor);
  if (node->type() == TPlanNodeType::SUBPLAN_NODE) {
    // Only traverse the first child and not the second one, because the Subplan
    // parent of nodes inside it should be 'node' and not 'ancestor'.
    SetContainingSubplan(ancestor, node->child(0));
  } else {
    int num_children = node->num_children();
    for (int i = 0; i < num_children; ++i) {
      SetContainingSubplan(ancestor, node->child(i));
    }
  }
}

Status SubplanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  SetContainingSubplan(this, child(1));
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  input_batch_.reset(
      new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
  return Status::OK();
}

Status SubplanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  return Status::OK();
}

Status SubplanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  *eos = false;

  while (true) {
    if (subplan_is_open_) {
      if (subplan_eos_) {
        // Reset the subplan before opening it again. 'row_batch' is passed in to allow
        // any remaining resources to be transferred to it.
        RETURN_IF_ERROR(child(1)->Reset(state, row_batch));
        subplan_is_open_ = false;
      } else {
        // Continue fetching rows from the open subplan into the output row_batch.
        DCHECK(!row_batch->AtCapacity());
        RETURN_IF_ERROR(child(1)->GetNext(state, row_batch, &subplan_eos_));
        // Apply limit and check whether the output batch is at capacity.
        if (limit_ != -1 && rows_returned() + row_batch->num_rows() >= limit_) {
          row_batch->set_num_rows(limit_ - rows_returned());
          IncrementNumRowsReturned(row_batch->num_rows());
          *eos = true;
          break;
        }
        if (row_batch->AtCapacity()) {
          IncrementNumRowsReturned(row_batch->num_rows());
          return Status::OK();
        }
        // Check subplan_eos_ and repeat fetching until the output batch is at capacity
        // or we have reached our limit.
        continue;
      }
    }

    if (input_row_idx_ >= input_batch_->num_rows()) {
      input_batch_->TransferResourceOwnership(row_batch);
      if (input_eos_) {
        *eos = true;
        break;
      }
      // Could be at capacity after resources have been transferred to it.
      if (row_batch->AtCapacity()) return Status::OK();
      // Continue fetching input rows.
      input_batch_->Reset();
      RETURN_IF_ERROR(child(0)->GetNext(state, input_batch_.get(), &input_eos_));
      input_row_idx_ = 0;
      if (input_batch_->num_rows() == 0) continue;
    }

    // Advance the current input row to be picked up by dependent nodes,
    // and Open() the subplan.
    current_input_row_ = input_batch_->GetRow(input_row_idx_);
    ++input_row_idx_;
    RETURN_IF_ERROR(child(1)->Open(state));
    subplan_is_open_ = true;
    subplan_eos_ = false;
  }

  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

Status SubplanNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  input_batch_->TransferResourceOwnership(row_batch);
  input_eos_ = false;
  input_row_idx_ = 0;
  subplan_eos_ = false;
  SetNumRowsReturned(0);
  RETURN_IF_ERROR(child(0)->Reset(state, row_batch));
  // If child(1) is not open it means that we have just Reset() it and returned from
  // GetNext() without opening it again. It is not safe to call Reset() on the same
  // exec node twice in a row.
  if (subplan_is_open_) RETURN_IF_ERROR(child(1)->Reset(state, row_batch));
  return Status::OK();
}

void SubplanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  input_batch_.reset();
  ExecNode::Close(state);
}

}
