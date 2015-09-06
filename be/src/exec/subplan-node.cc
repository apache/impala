// Copyright 2015 Cloudera Inc.
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

#include "exec/subplan-node.h"

#include <algorithm>
#include "exec/singular-row-src-node.h"
#include "exec/subplan-node.h"
#include "exec/unnest-node.h"
#include "exprs/slot-ref.h"
#include "exprs/expr-context.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"

using std::find;

namespace impala {

SubplanNode::SubplanNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs),
      input_batch_(NULL),
      input_eos_(false),
      input_row_idx_(0),
      current_input_row_(NULL),
      subplan_is_open_(false),
      subplan_eos_(false) {
}

Status SubplanNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  DCHECK_EQ(children_.size(), 2);
  return Status::OK();
}

void SubplanNode::SetContainingSubplan(SubplanNode* ancestor, ExecNode* node,
    RuntimeState* state) {
  node->set_containing_subplan(ancestor);
  if (node->type() == TPlanNodeType::UNNEST_NODE) {
    // Gather the collection-typed slots that are unnested in this subplan.
    UnnestNode* unnestNode = static_cast<UnnestNode*>(node);
    DCHECK(unnestNode->array_expr_ctx_->root()->is_slotref());
    const SlotRef* slot_ref = static_cast<SlotRef*>(unnestNode->array_expr_ctx_->root());
    const SlotDescriptor* array_slot_desc =
        state->desc_tbl().GetSlotDescriptor(slot_ref->slot_id());
    DCHECK(array_slot_desc != NULL);
    // Make sure that a collection-typed slot is only referenced once.
    DCHECK(find(unnested_array_slots_.begin(), unnested_array_slots_.end(),
        array_slot_desc) == unnested_array_slots_.end());
    unnested_array_slots_.push_back(array_slot_desc);
    // Find and cache the corresponding tuple index.
    const RowDescriptor& input_row_desc = children_[0]->row_desc();
    int tuple_idx = input_row_desc.GetTupleIdx(array_slot_desc->parent()->id());
    unnested_array_tuple_idxs_.push_back(tuple_idx);
  }
  if (node->type() == TPlanNodeType::SUBPLAN_NODE) {
    // Only traverse the first child and not the second one, because the Subplan
    // parent of nodes inside it should be 'node' and not 'ancestor'.
    SetContainingSubplan(ancestor, node->child(0), state);
  } else {
    int num_children = node->num_children();
    for (int i = 0; i < num_children; ++i) {
      SetContainingSubplan(ancestor, node->child(i), state);
    }
  }
}

Status SubplanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  // Set the containing subplan before calling ExecNode::Prepare() because
  // the Prepare() of our children rely on it.
  DCHECK(unnested_array_slots_.empty());
  SetContainingSubplan(this, child(1), state);
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  input_batch_.reset(
      new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
  return Status::OK();
}

Status SubplanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  return Status::OK();
}

Status SubplanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  while (true) {
    if (subplan_is_open_) {
      if (subplan_eos_) {
        // Poor man's projection of the unnested slots.
        DCHECK_EQ(unnested_array_slots_.size(), unnested_array_tuple_idxs_.size());
        DCHECK(current_input_row_ != NULL);
        for (int i = 0; i < unnested_array_slots_.size(); ++i) {
          Tuple* tuple = current_input_row_->GetTuple(unnested_array_tuple_idxs_[i]);
          if (tuple == NULL) continue;
          tuple->SetNull(unnested_array_slots_[i]->null_indicator_offset());
        }
        // Reset the subplan before opening it again. At this point, all resources from
        // the subplan are assumed to have been transferred to the output row_batch.
        RETURN_IF_ERROR(child(1)->Reset(state));
        subplan_is_open_ = false;
      } else {
        // Continue fetching rows from the open subplan into the output row_batch.
        DCHECK(!row_batch->AtCapacity());
        RETURN_IF_ERROR(child(1)->GetNext(state, row_batch, &subplan_eos_));
        // Apply limit and check whether the output batch is at capacity.
        if (limit_ != -1 && num_rows_returned_ + row_batch->num_rows() >= limit_) {
          row_batch->set_num_rows(limit_ - num_rows_returned_);
          num_rows_returned_ += row_batch->num_rows();
          *eos = true;
          break;
        }
        if (row_batch->AtCapacity()) {
          num_rows_returned_ += row_batch->num_rows();
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

  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

Status SubplanNode::Reset(RuntimeState* state) {
  input_eos_ = false;
  input_row_idx_ = 0;
  subplan_eos_ = false;
  return ExecNode::Reset(state);
}

void SubplanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  input_batch_.reset();
  ExecNode::Close(state);
}

}
