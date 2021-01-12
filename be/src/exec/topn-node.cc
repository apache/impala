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

#include "exec/topn-node.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "exec/exec-node-util.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/descriptors.h"
#include "runtime/fragment-state.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"
#include "util/tuple-row-compare.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using namespace impala;

Status TopNPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  const TSortInfo& tsort_info = tnode.sort_node.sort_info;
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  RETURN_IF_ERROR(ScalarExpr::Create(
      tsort_info.ordering_exprs, *row_descriptor_, state, &ordering_exprs_));
  DCHECK(tsort_info.__isset.sort_tuple_slot_exprs);
  output_tuple_desc_ = row_descriptor_->tuple_descriptors()[0];
  RETURN_IF_ERROR(ScalarExpr::Create(tsort_info.sort_tuple_slot_exprs,
      *children_[0]->row_descriptor_, state, &output_tuple_exprs_));
  row_comparator_config_ =
      state->obj_pool()->Add(new TupleRowComparatorConfig(tsort_info, ordering_exprs_));
  DCHECK_EQ(conjuncts_.size(), 0) << "TopNNode should never have predicates to evaluate.";
  state->CheckAndAddCodegenDisabledMessage(codegen_status_msgs_);
  return Status::OK();
}

void TopNPlanNode::Close() {
  ScalarExpr::Close(ordering_exprs_);
  ScalarExpr::Close(output_tuple_exprs_);
  PlanNode::Close();
}

Status TopNPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new TopNNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

TopNNode::TopNNode(
    ObjectPool* pool, const TopNPlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs),
    offset_(pnode.offset()),
    output_tuple_exprs_(pnode.output_tuple_exprs_),
    output_tuple_desc_(pnode.output_tuple_desc_),
    tuple_row_less_than_(new TupleRowLexicalComparator(*pnode.row_comparator_config_)),
    tuple_pool_(nullptr),
    codegend_insert_batch_fn_(pnode.codegend_insert_batch_fn_),
    rows_to_reclaim_(0),
    num_rows_skipped_(0) {
  runtime_profile()->AddInfoString("SortType", "TopN");
}

Status TopNNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  DCHECK(output_tuple_desc_ != nullptr);
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  tuple_pool_.reset(new MemPool(mem_tracker()));
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(output_tuple_exprs_, state, pool_,
      expr_perm_pool(), expr_results_pool(), &output_tuple_expr_evals_));
  insert_batch_timer_ = ADD_TIMER(runtime_profile(), "InsertBatchTime");
  tuple_pool_reclaim_counter_ = ADD_COUNTER(runtime_profile(), "TuplePoolReclamations",
      TUnit::UNIT);
  return Status::OK();
}

void TopNPlanNode::Codegen(FragmentState* state) {
  DCHECK(state->ShouldCodegen());
  PlanNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != NULL);

  llvm::Function* compare_fn = nullptr;
  Status codegen_status = row_comparator_config_->Codegen(state, &compare_fn);
  if (codegen_status.ok()) {
    llvm::Function* insert_batch_fn =
        codegen->GetFunction(IRFunction::TOPN_NODE_INSERT_BATCH, true);
    DCHECK(insert_batch_fn != NULL);

    // Generate two MaterializeExprs() functions, one with no pool that
    // does a shallow copy and one with 'tuple_pool_' that does a deep copy of the data.
    DCHECK(output_tuple_desc_ != NULL);
    llvm::Function* materialize_exprs_tuple_pool_fn = nullptr;
    llvm::Function* materialize_exprs_no_pool_fn = nullptr;

    codegen_status = Tuple::CodegenMaterializeExprs(codegen, false,
        *output_tuple_desc_, output_tuple_exprs_,
        true, &materialize_exprs_tuple_pool_fn);

    if (codegen_status.ok()) {
      codegen_status = Tuple::CodegenMaterializeExprs(codegen, false,
          *output_tuple_desc_, output_tuple_exprs_,
          false, &materialize_exprs_no_pool_fn);
    }

    if (codegen_status.ok()) {
      int replaced;
      replaced = codegen->ReplaceCallSites(insert_batch_fn,
          materialize_exprs_tuple_pool_fn, Tuple::MATERIALIZE_EXPRS_SYMBOL);
      DCHECK_REPLACE_COUNT(replaced, 1) << LlvmCodeGen::Print(insert_batch_fn);

      replaced = codegen->ReplaceCallSites(insert_batch_fn,
          materialize_exprs_no_pool_fn, Tuple::MATERIALIZE_EXPRS_NULL_POOL_SYMBOL);
      DCHECK_REPLACE_COUNT(replaced, 1) << LlvmCodeGen::Print(insert_batch_fn);

      // The total number of calls to tuple_row_less_than_->Compare() is 3 in
      // PriorityQueue (called from 2 places), 1 in TopNNode::Heap::InsertTupleRow()
      // and 3 in TopNNode::Heap::InsertTupleWithTieHandling
      // Each tuple_row_less_than_->Less(Tuple*, Tuple*) indirectly calls Compare() once.
      replaced = codegen->ReplaceCallSites(insert_batch_fn,
          compare_fn, TupleRowComparator::COMPARE_SYMBOL);
      DCHECK_REPLACE_COUNT(replaced, 10) << LlvmCodeGen::Print(insert_batch_fn);

      replaced = codegen->ReplaceCallSitesWithValue(insert_batch_fn,
          codegen->GetI64Constant(heap_capacity()), "heap_capacity");
      DCHECK_REPLACE_COUNT(replaced, 2)
          << LlvmCodeGen::Print(insert_batch_fn);

      replaced = codegen->ReplaceCallSitesWithBoolConst(
          insert_batch_fn, include_ties(), "include_ties");
      DCHECK_REPLACE_COUNT(replaced, 1)
          << LlvmCodeGen::Print(insert_batch_fn);

      int tuple_byte_size = output_tuple_desc_->byte_size();
      replaced = codegen->ReplaceCallSitesWithValue(insert_batch_fn,
          codegen->GetI32Constant(tuple_byte_size), "tuple_byte_size");
      DCHECK_REPLACE_COUNT(replaced, 3)
          << LlvmCodeGen::Print(insert_batch_fn);

      insert_batch_fn = codegen->FinalizeFunction(insert_batch_fn);
      DCHECK(insert_batch_fn != NULL);
      codegen->AddFunctionToJit(insert_batch_fn, &codegend_insert_batch_fn_);
    }
  }
  AddCodegenStatus(codegen_status);
}

Status TopNNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(
      tuple_row_less_than_->Open(pool_, state, expr_perm_pool(), expr_results_pool()));
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(output_tuple_expr_evals_, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  const TopNPlanNode& pnode = static_cast<const TopNPlanNode&>(plan_node_);
  heap_.reset(
      new Heap(*tuple_row_less_than_, pnode.heap_capacity(), pnode.include_ties()));

  // Allocate memory for a temporary tuple.
  tmp_tuple_ = reinterpret_cast<Tuple*>(
      tuple_pool_->Allocate(output_tuple_desc_->byte_size()));

  RETURN_IF_ERROR(child(0)->Open(state));

  // Limit of 0, no need to fetch anything from children.
  if (pnode.heap_capacity() != 0) {
    RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
    bool eos;
    do {
      batch.Reset();
      RETURN_IF_ERROR(child(0)->GetNext(state, &batch, &eos));
      {
        SCOPED_TIMER(insert_batch_timer_);
        TopNPlanNode::InsertBatchFn insert_batch_fn = codegend_insert_batch_fn_.load();
        if (insert_batch_fn != nullptr) {
          insert_batch_fn(this, &batch);
        } else {
          InsertBatch(&batch);
        }
        DCHECK(heap_->DCheckConsistency());
        if (rows_to_reclaim_ > 2 * pnode.heap_capacity()) {
          RETURN_IF_ERROR(ReclaimTuplePool(state));
          COUNTER_ADD(tuple_pool_reclaim_counter_, 1);
        }
      }
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
    } while (!eos);
  }
  PrepareForOutput();

  // Unless we are inside a subplan expecting to call Open()/GetNext() on the child
  // again, the child can be closed at this point.
  if (!IsInSubplan()) child(0)->Close(state);
  return Status::OK();
}

Status TopNNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  while (!row_batch->AtCapacity() && (get_next_iter_ != sorted_top_n_.end())) {
    if (num_rows_skipped_ < offset_) {
      ++get_next_iter_;
      ++num_rows_skipped_;
      continue;
    }
    int row_idx = row_batch->AddRow();
    TupleRow* dst_row = row_batch->GetRow(row_idx);
    Tuple* src_tuple = *get_next_iter_;
    TupleRow* src_row = reinterpret_cast<TupleRow*>(&src_tuple);
    row_batch->CopyRow(src_row, dst_row);
    ++get_next_iter_;
    row_batch->CommitLastRow();
    IncrementNumRowsReturned(1);
    COUNTER_SET(rows_returned_counter_, rows_returned());
  }
  *eos = get_next_iter_ == sorted_top_n_.end();

  // Transfer ownership of tuple data to output batch.
  // TODO: To improve performance for small inputs when this node is run multiple times
  // inside a subplan, we might choose to only selectively transfer, e.g., when the
  // block(s) in the pool are all full or when the pool has reached a certain size.
  if (*eos) row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), false);
  return Status::OK();
}

Status TopNNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  heap_->Reset();
  tmp_tuple_ = nullptr;
  num_rows_skipped_ = 0;
  // Transfer ownership of tuple data to output batch.
  row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), false);
  // We deliberately do not free the tuple_pool_ here to allow selective transferring
  // of resources in the future.
  return ExecNode::Reset(state, row_batch);
}

void TopNNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (heap_ != nullptr) heap_->Close();
  if (tuple_pool_.get() != nullptr) tuple_pool_->FreeAll();
  if (tuple_row_less_than_.get() != nullptr) tuple_row_less_than_->Close(state);
  ScalarExprEvaluator::Close(output_tuple_expr_evals_, state);
  ExecNode::Close(state);
}

void TopNNode::PrepareForOutput() {
  DCHECK(heap_->DCheckConsistency());
  heap_->PrepareForOutput(*this, &sorted_top_n_);
  get_next_iter_ = sorted_top_n_.begin();
}

void TopNNode::Heap::PrepareForOutput(
    const TopNNode& RESTRICT node, vector<Tuple*>* sorted_top_n) RESTRICT {
  // Reverse the order of the tuples in the priority queue
  sorted_top_n->resize(num_tuples());
  int64_t index = sorted_top_n->size() - 1;
  /// Any ties with the min will be the last elements in 'sorted_top_n'.
  while (!overflowed_ties_.empty()) {
    (*sorted_top_n)[index] = overflowed_ties_.back();
    overflowed_ties_.pop_back();
    --index;
  }
  while (!priority_queue_.Empty()) {
    (*sorted_top_n)[index] = priority_queue_.Pop();
    --index;
  }
}

bool TopNNode::Heap::DCheckConsistency() {
  DCHECK_LE(num_tuples(), capacity_ + overflowed_ties_.size())
      << num_tuples() << " > " << capacity_ << " + " << overflowed_ties_.size();
  if (!overflowed_ties_.empty()) {
    DCHECK(include_ties_);
    DCHECK_EQ(capacity_, priority_queue_.Size())
        << "Ties should only be present if heap is at capacity";
  }
  return true;
}

Status TopNNode::ReclaimTuplePool(RuntimeState* state) {
  unique_ptr<MemPool> temp_pool(new MemPool(mem_tracker()));
  RETURN_IF_ERROR(heap_->RematerializeTuples(this, state, temp_pool.get()));
  DCHECK(heap_->DCheckConsistency());

  rows_to_reclaim_ = 0;
  tmp_tuple_ = reinterpret_cast<Tuple*>(temp_pool->TryAllocate(
      output_tuple_desc_->byte_size()));
  if (UNLIKELY(tmp_tuple_ == nullptr)) {
    return temp_pool->mem_tracker()->MemLimitExceeded(state,
        "Failed to allocate memory in TopNNode::ReclaimTuplePool.",
        output_tuple_desc_->byte_size());
  }
  tuple_pool_->FreeAll();
  tuple_pool_ = move(temp_pool);
  return Status::OK();
}

void TopNNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  const TopNPlanNode& pnode = static_cast<const TopNPlanNode&>(plan_node_);
  const TSortInfo& tsort_info = pnode.tnode_->sort_node.sort_info;

  *out << "TopNNode(" << ScalarExpr::DebugString(pnode.ordering_exprs_);
  for (int i = 0; i < tsort_info.is_asc_order.size(); ++i) {
    *out << (i > 0 ? " " : "") << (tsort_info.is_asc_order[i] ? "asc" : "desc")
         << " nulls " << (tsort_info.nulls_first[i] ? "first" : "last");
  }

  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

TopNNode::Heap::Heap(const TupleRowComparator& c, int64_t capacity, bool include_ties) :
    capacity_(capacity), include_ties_(include_ties), priority_queue_(c) {}

void TopNNode::Heap::Reset() {
  priority_queue_.Clear();
  overflowed_ties_.clear();
}

void TopNNode::Heap::Close() {
  priority_queue_.Clear();
  overflowed_ties_.clear();
}

template <class T>
Status TopNNode::Heap::RematerializeTuplesHelper(TopNNode* node,
    RuntimeState* state, MemPool* new_pool, T begin_it, T end_it) {
  const TupleDescriptor& tuple_desc = *node->output_tuple_desc_;
  int tuple_size = tuple_desc.byte_size();
  for (T it = begin_it; it != end_it; ++it) {
    Tuple* insert_tuple = reinterpret_cast<Tuple*>(new_pool->TryAllocate(tuple_size));
    if (UNLIKELY(insert_tuple == nullptr)) {
      return new_pool->mem_tracker()->MemLimitExceeded(state,
          "Failed to allocate memory in TopNNode::ReclaimTuplePool.", tuple_size);
    }
    (*it)->DeepCopy(insert_tuple, tuple_desc, new_pool);
    *it = insert_tuple;
  }
  return Status::OK();
}

Status TopNNode::Heap::RematerializeTuples(TopNNode* node,
    RuntimeState* state, MemPool* new_pool) {
  RETURN_IF_ERROR(RematerializeTuplesHelper(
        node, state, new_pool, priority_queue_.Begin(), priority_queue_.End()));
  RETURN_IF_ERROR(RematerializeTuplesHelper(
        node, state, new_pool, overflowed_ties_.begin(), overflowed_ties_.end()));
  return Status::OK();
}

template class impala::PriorityQueue<Tuple*, TupleRowComparator>;
template class impala::PriorityQueueIterator<Tuple*, TupleRowComparator>;
