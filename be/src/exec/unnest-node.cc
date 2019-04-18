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

#include "common/status.h"
#include "exec/exec-node-util.h"
#include "exec/subplan-node.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/slot-ref.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/runtime-profile-counters.h"

namespace impala {

const CollectionValue UnnestNode::EMPTY_COLLECTION_VALUE;

UnnestNode::UnnestNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    item_byte_size_(0),
    thrift_coll_expr_(tnode.unnest_node.collection_expr),
    coll_expr_(nullptr),
    coll_expr_eval_(nullptr),
    coll_slot_desc_(nullptr),
    coll_tuple_idx_(-1),
    coll_value_(nullptr),
    item_idx_(0),
    num_collections_(0),
    total_collection_size_(0),
    max_collection_size_(-1),
    min_collection_size_(-1),
    avg_collection_size_counter_(nullptr),
    max_collection_size_counter_(nullptr),
    min_collection_size_counter_(nullptr),
    num_collections_counter_(nullptr) {
}

Status UnnestNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  DCHECK(tnode.__isset.unnest_node);
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));
  return Status::OK();
}

Status UnnestNode::InitCollExpr(RuntimeState* state) {
  DCHECK(containing_subplan_ != nullptr)
      << "set_containing_subplan() must have been called";
  const RowDescriptor& row_desc = *containing_subplan_->child(0)->row_desc();
  RETURN_IF_ERROR(ScalarExpr::Create(thrift_coll_expr_, row_desc, state, &coll_expr_));
  DCHECK(coll_expr_->IsSlotRef());
  return Status::OK();
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

  DCHECK_EQ(1, row_desc()->tuple_descriptors().size());
  const TupleDescriptor* item_tuple_desc = row_desc()->tuple_descriptors()[0];
  DCHECK(item_tuple_desc != nullptr);
  item_byte_size_ = item_tuple_desc->byte_size();

  RETURN_IF_ERROR(ScalarExprEvaluator::Create(*coll_expr_, state, pool_,
      expr_perm_pool(), expr_results_pool(), &coll_expr_eval_));

  // Set the coll_slot_desc_ and the corresponding tuple index used for manually
  // evaluating the collection SlotRef and for projection.
  DCHECK(coll_expr_->IsSlotRef());
  const SlotRef* slot_ref = static_cast<SlotRef*>(coll_expr_);
  coll_slot_desc_ = state->desc_tbl().GetSlotDescriptor(slot_ref->slot_id());
  DCHECK(coll_slot_desc_ != nullptr);
  const RowDescriptor* row_desc = containing_subplan_->child(0)->row_desc();
  coll_tuple_idx_ = row_desc->GetTupleIdx(coll_slot_desc_->parent()->id());

  return Status::OK();
}

Status UnnestNode::Open(RuntimeState* state) {
  DCHECK(IsInSubplan());
  // Omit ScopedOpenEventAdder since this is always in a subplan.
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(coll_expr_eval_->Open(state));

  DCHECK(containing_subplan_->current_row() != nullptr);
  Tuple* tuple = containing_subplan_->current_input_row_->GetTuple(coll_tuple_idx_);
  if (tuple != nullptr) {
    // Retrieve the collection value to be unnested directly from the tuple. We purposely
    // ignore the null bit of the slot because we may have set it in a previous Open() of
    // this same unnest node for projection.
    coll_value_ = reinterpret_cast<const CollectionValue*>(
        tuple->GetSlot(coll_slot_desc_->tuple_offset()));
    // Projection: Set the slot containing the collection value to nullptr.
    tuple->SetNull(coll_slot_desc_->null_indicator_offset());
  } else {
    coll_value_ = &EMPTY_COLLECTION_VALUE;
    DCHECK_EQ(coll_value_->num_tuples, 0);
  }

  ++num_collections_;
  COUNTER_SET(num_collections_counter_, num_collections_);
  total_collection_size_ += coll_value_->num_tuples;
  COUNTER_SET(avg_collection_size_counter_,
      static_cast<double>(total_collection_size_) / num_collections_);
  if (max_collection_size_ == -1 || coll_value_->num_tuples > max_collection_size_) {
    max_collection_size_ = coll_value_->num_tuples;
    COUNTER_SET(max_collection_size_counter_, max_collection_size_);
  }
  if (min_collection_size_ == -1 || coll_value_->num_tuples < min_collection_size_) {
    min_collection_size_ = coll_value_->num_tuples;
    COUNTER_SET(min_collection_size_counter_, min_collection_size_);
  }
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

  // Populate the output row_batch with tuples from the collection.
  DCHECK(coll_value_ != nullptr);
  DCHECK_GE(coll_value_->num_tuples, 0);
  while (item_idx_ < coll_value_->num_tuples) {
    Tuple* item =
        reinterpret_cast<Tuple*>(coll_value_->ptr + item_idx_ * item_byte_size_);
    ++item_idx_;
    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    row->SetTuple(0, item);
    // TODO: Ideally these should be evaluated by the parent scan node.
    DCHECK_EQ(conjuncts_.size(), conjunct_evals_.size());
    if (EvalConjuncts(conjunct_evals_.data(), conjuncts_.size(), row)) {
      row_batch->CommitLastRow();
      // The limit is handled outside of this loop.
      if (row_batch->AtCapacity()) break;
    }
  }

  // Checking the limit here is simpler/cheaper than doing it in the loop above.
  const bool reached_limit = CheckLimitAndTruncateRowBatchIfNeeded(row_batch, eos);
  if (!reached_limit && item_idx_ == coll_value_->num_tuples) {
    *eos = true;
  }
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

Status UnnestNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  item_idx_ = 0;
  return ExecNode::Reset(state, row_batch);
}

void UnnestNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (coll_expr_eval_ != nullptr) coll_expr_eval_->Close(state);
  if (coll_expr_ != nullptr) coll_expr_->Close();
  ExecNode::Close(state);
}

}
