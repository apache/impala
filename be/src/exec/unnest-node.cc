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

#include "exec/unnest-node.h"

#include <algorithm>

#include "common/status.h"
#include "exec/exec-node.inline.h"
#include "exec/exec-node-util.h"
#include "exec/subplan-node.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/slot-ref.h"
#include "runtime/fragment-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/runtime-profile-counters.h"

namespace impala {

const CollectionValue UnnestNode::EMPTY_COLLECTION_VALUE;

Status UnnestPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  DCHECK(tnode.__isset.unnest_node);
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  return Status::OK();
}

void UnnestPlanNode::Close() {
  for (auto coll_expr : collection_exprs_) {
    if (coll_expr != nullptr) coll_expr->Close();
  }
  PlanNode::Close();
}

Status UnnestPlanNode::InitCollExprs(FragmentState* state) {
  DCHECK(containing_subplan_ != nullptr)
      << "set_containing_subplan() must have been called";
  const RowDescriptor& row_desc = *containing_subplan_->children_[0]->row_descriptor_;
  RETURN_IF_ERROR(ScalarExpr::Create(
      tnode_->unnest_node.collection_exprs, row_desc, state, &collection_exprs_));
  DCHECK_GT(collection_exprs_.size(), 0);

  for (ScalarExpr* coll_expr : collection_exprs_) {
    DCHECK(coll_expr->IsSlotRef());
    const SlotRef* slot_ref = static_cast<SlotRef*>(coll_expr);
    SlotDescriptor* slot_desc = state->desc_tbl().GetSlotDescriptor(slot_ref->slot_id());
    DCHECK(slot_desc != nullptr);
    coll_slot_descs_.push_back(slot_desc);

    // If the collection is in a struct we don't use the itemTupleDesc of the struct but
    // the tuple in which the top level struct is placed.
    const TupleDescriptor* parent_tuple = slot_desc->parent();
    const TupleDescriptor* master_tuple = parent_tuple->getMasterTuple();
    const TupleDescriptor* top_level_tuple = master_tuple == nullptr ?
        parent_tuple : master_tuple;
    coll_tuple_idxs_.push_back(row_desc.GetTupleIdx(top_level_tuple->id()));
  }
  return Status::OK();
}

Status UnnestPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new UnnestNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

UnnestNode::UnnestNode(
    ObjectPool* pool, const UnnestPlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs),
    coll_slot_descs_(&(pnode.coll_slot_descs_)),
    input_coll_tuple_idxs_(&(pnode.coll_tuple_idxs_)),
    item_idx_(0),
    longest_collection_size_(0),
    num_collections_(0),
    total_collection_size_(0),
    max_collection_size_(-1),
    min_collection_size_(-1),
    avg_collection_size_counter_(nullptr),
    max_collection_size_counter_(nullptr),
    min_collection_size_counter_(nullptr),
    num_collections_counter_(nullptr) {
  DCHECK_GT(coll_slot_descs_->size(), 0);
  DCHECK_EQ(coll_slot_descs_->size(), input_coll_tuple_idxs_->size());
  coll_values_.resize(coll_slot_descs_->size());
  for (const SlotDescriptor* slot_desc : *coll_slot_descs_) {
    output_coll_tuple_idxs_.push_back(GetCollTupleIdx(slot_desc));
  }
}

Status UnnestNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  avg_collection_size_counter_ =
      ADD_COUNTER(runtime_profile_, "AvgCollectionSize", TUnit::DOUBLE_VALUE);
  max_collection_size_counter_ =
      ADD_COUNTER(runtime_profile_, "MaxCollectionSize", TUnit::UNIT);
  min_collection_size_counter_ =
      ADD_COUNTER(runtime_profile_, "MinCollectionSize", TUnit::UNIT);
  num_collections_counter_ =
      ADD_COUNTER(runtime_profile_, "NumCollections", TUnit::UNIT);

  DCHECK_EQ(coll_values_.size(), row_desc()->tuple_descriptors().size());
  item_byte_sizes_.resize(row_desc()->tuple_descriptors().size());
  for (int i = 0; i < row_desc()->tuple_descriptors().size(); ++i) {
    const TupleDescriptor* item_tuple_desc = row_desc()->tuple_descriptors()[i];
    DCHECK(item_tuple_desc != nullptr);
    item_byte_sizes_[i] = item_tuple_desc->byte_size();
  }

  return Status::OK();
}

Status UnnestNode::Open(RuntimeState* state) {
  DCHECK(IsInSubplan());
  // Omit ScopedOpenEventAdder since this is always in a subplan.
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));

  DCHECK(containing_subplan_->current_row() != nullptr);
  longest_collection_size_ = 0;
  for (int i = 0; i < coll_values_.size(); ++i) {
    Tuple* tuple =
        containing_subplan_->current_input_row_->GetTuple((*input_coll_tuple_idxs_)[i]);
    if (tuple != nullptr) {
      SlotDescriptor* coll_slot_desc = (*coll_slot_descs_)[i];
      coll_values_[i] = reinterpret_cast<const CollectionValue*>(
          tuple->GetSlot(coll_slot_desc->tuple_offset()));
      // Projection: Set the slot containing the collection value to nullptr.
      tuple->SetNull(coll_slot_desc->null_indicator_offset());

      // Update stats. Only take into account non-empty collections.
      int num_tuples = coll_values_[i]->num_tuples;
      if (num_tuples > 0) {
        longest_collection_size_ = std::max(longest_collection_size_,
            (int64_t)num_tuples);
        total_collection_size_ += num_tuples;
        ++num_collections_;
        max_collection_size_ = std::max(max_collection_size_, (int64_t)num_tuples);
        if (min_collection_size_ == -1 || num_tuples < min_collection_size_) {
          min_collection_size_ = num_tuples;
        }
      }
    } else {
      coll_values_[i] = &EMPTY_COLLECTION_VALUE;
      DCHECK_EQ(coll_values_[i]->num_tuples, 0);
    }
  }

  COUNTER_SET(num_collections_counter_, num_collections_);
  COUNTER_SET(avg_collection_size_counter_,
      static_cast<double>(total_collection_size_) / num_collections_);
  COUNTER_SET(max_collection_size_counter_, max_collection_size_);
  COUNTER_SET(min_collection_size_counter_, min_collection_size_);
  return Status::OK();
}

Status UnnestNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  // Avoid expensive query maintenance overhead for small collections.
  if (item_idx_ > 0) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
  }
  *eos = false;

  // Populate the output row_batch with tuples from the collections.
  while (item_idx_ < longest_collection_size_) {
    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    for (int i = 0; i < coll_values_.size(); ++i) {
      const CollectionValue* coll_value = coll_values_[i];
      DCHECK(coll_value != nullptr);
      DCHECK_GE(coll_value->num_tuples, 0);
      Tuple* input_tuple;
      if (coll_value->num_tuples <= item_idx_) {
        input_tuple = CreateNullTuple(i, row_batch);
      } else {
        input_tuple =
            reinterpret_cast<Tuple*>(coll_value->ptr + item_idx_ * item_byte_sizes_[i]);
      }
      row->SetTuple(output_coll_tuple_idxs_[i], input_tuple);
    }
    ++item_idx_;
    DCHECK_EQ(conjuncts_.size(), conjunct_evals_.size());
    if (EvalConjuncts(conjunct_evals_.data(), conjuncts_.size(), row)) {
      row_batch->CommitLastRow();
      // The limit is handled outside of this loop.
      if (row_batch->AtCapacity()) break;
    }
  }

  // Checking the limit here is simpler/cheaper than doing it in the loop above.
  const bool reached_limit = CheckLimitAndTruncateRowBatchIfNeeded(row_batch, eos);
  if (!reached_limit && item_idx_ == longest_collection_size_) *eos = true;
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

int UnnestNode::GetCollTupleIdx(const SlotDescriptor* slot_desc) const {
  DCHECK(slot_desc != nullptr);
  const TupleDescriptor* coll_tuple = slot_desc->children_tuple_descriptor();
  DCHECK(coll_tuple != nullptr);
  return row_descriptor_.GetTupleIdx(coll_tuple->id());
}

Tuple* UnnestNode::CreateNullTuple(int coll_idx, RowBatch* row_batch) const {
  const TupleDescriptor* coll_tuple =
      (*coll_slot_descs_)[coll_idx]->children_tuple_descriptor();
  DCHECK(coll_tuple != nullptr);
  if (coll_tuple->slots().size() == 0) return nullptr;
  DCHECK_EQ(coll_tuple->slots().size(), 1);
  const SlotDescriptor* coll_item_slot = coll_tuple->slots()[0];
  DCHECK(coll_item_slot != nullptr);
  Tuple* tuple = Tuple::Create(item_byte_sizes_[coll_idx], row_batch->tuple_data_pool());
  if (tuple == nullptr) return nullptr;
  tuple->SetNull(coll_item_slot->null_indicator_offset());
  return tuple;
}

Status UnnestNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  item_idx_ = 0;
  return ExecNode::Reset(state, row_batch);
}

void UnnestNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  ExecNode::Close(state);
}

}
