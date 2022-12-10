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

#include <algorithm>
#include <sstream>

#include "codegen/llvm-codegen.h"
#include "exec/exec-node-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "exprs/slot-ref.h"
#include "runtime/descriptors.h"
#include "runtime/fragment-state.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/sorter.h"
#include "runtime/sorter-internal.h" // For TupleSorter
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"
#include "util/tuple-row-compare.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using namespace impala;

// Soft limit on the number of partitions in an instance of a partitioned top-n
// node to avoid potential scalability problems with a large number of heaps in
// the std::map.
DEFINE_int32(partitioned_topn_in_mem_partitions_limit, 1000, "(Experimental) Soft limit "
    "on the number of in-memory partitions in an instance of the partitioned top-n "
    "operator.");

// Soft limit on the aggregate size of heaps. If heaps exceed this, we will evict some
// from memory.
DEFINE_int64(partitioned_topn_soft_limit_bytes, 64L * 1024L * 1024L, "(Experimental) "
    "Soft limit on the number of in-memory partitions in an instance of the "
    "partitioned top-n operator.");

Status TopNPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  const TSortInfo& tsort_info = tnode.sort_node.sort_info;
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  RETURN_IF_ERROR(ScalarExpr::Create(
      tsort_info.ordering_exprs, *row_descriptor_, state, &ordering_exprs_));
  DCHECK(tsort_info.__isset.sort_tuple_slot_exprs);
  output_tuple_desc_ = row_descriptor_->tuple_descriptors()[0];
  RETURN_IF_ERROR(ScalarExpr::Create(tsort_info.sort_tuple_slot_exprs,
      *children_[0]->row_descriptor_, state, &output_tuple_exprs_));
  ordering_comparator_config_ =
      state->obj_pool()->Add(new TupleRowComparatorConfig(tsort_info, ordering_exprs_));
  if (is_partitioned()) {
    DCHECK(tnode.sort_node.__isset.partition_exprs);
    RETURN_IF_ERROR(ScalarExpr::Create(
        tnode.sort_node.partition_exprs, *row_descriptor_, state, &partition_exprs_));

    // We need a TSortInfo for internal use in the sorted map. Initialize with
    // arbitrary parameters.
    TSortInfo* tpartition_sort_info = state->obj_pool()->Add(new TSortInfo);
    tpartition_sort_info->sorting_order = TSortingOrder::LEXICAL;
    tpartition_sort_info->is_asc_order.resize(partition_exprs_.size(), true);
    tpartition_sort_info->nulls_first.resize(partition_exprs_.size(), false);
    partition_comparator_config_ = state->obj_pool()->Add(
        new TupleRowComparatorConfig(*tpartition_sort_info, partition_exprs_));

    DCHECK(tnode.sort_node.__isset.intra_partition_sort_info);
    const TSortInfo& intra_part_sort_info = tnode.sort_node.intra_partition_sort_info;
    // Set up the intra-partition comparator.
    RETURN_IF_ERROR(ScalarExpr::Create(intra_part_sort_info.ordering_exprs,
        *row_descriptor_, state, &intra_partition_ordering_exprs_));
    intra_partition_comparator_config_ =
        state->obj_pool()->Add(new TupleRowComparatorConfig(
            intra_part_sort_info, intra_partition_ordering_exprs_));

    // Construct SlotRefs that simply copy the output tuple to itself.
    for (const SlotDescriptor* slot_desc : output_tuple_desc_->slots()) {
      SlotRef* slot_ref = state->obj_pool()->Add(SlotRef::TypeSafeCreate(slot_desc));
      noop_tuple_exprs_.push_back(slot_ref);
      RETURN_IF_ERROR(slot_ref->Init(*row_descriptor_, true, state));
    }
  }
  DCHECK_EQ(conjuncts_.size(), 0) << "TopNNode should never have predicates to evaluate.";
  state->CheckAndAddCodegenDisabledMessage(codegen_status_msgs_);
  return Status::OK();
}

void TopNPlanNode::Close() {
  ScalarExpr::Close(ordering_exprs_);
  ScalarExpr::Close(partition_exprs_);
  ScalarExpr::Close(intra_partition_ordering_exprs_);
  ScalarExpr::Close(output_tuple_exprs_);
  ScalarExpr::Close(noop_tuple_exprs_);
  PlanNode::Close();
}

Status TopNPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new TopNNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

/// In the TopNNode constructor if 'pnode.partition_comparator_config_' is NULL, we use
/// this dummy comparator to avoid 'partition_cmp_' becoming a null pointer. This is
/// needed because 'partition_cmp_' is wrapped in a
/// 'ComparatorWrapper<TupleRowComparator>' that takes a reference to the underlying
/// comparator, so we cannot pass in a null pointer or dereference it.
class DummyTupleRowComparator: public TupleRowComparator {
 public:
  DummyTupleRowComparator()
   : TupleRowComparator(dummy_scalar_exprs_, dummy_codegend_compare_fn_) {
  }
 private:
  static const std::vector<ScalarExpr*> dummy_scalar_exprs_;
  static const CodegenFnPtr<TupleRowComparatorConfig::CompareFn>
      dummy_codegend_compare_fn_;

  int CompareInterpreted(const TupleRow* lhs, const TupleRow* rhs) const override {
    // This function should never be called as this is a dummy comparator.
    DCHECK(false);
    return std::less<const TupleRow*>{}(lhs, rhs);
  }
};

/// Initialise vector length to 0 so no buffer needs to be allocated.
const std::vector<ScalarExpr*> DummyTupleRowComparator::dummy_scalar_exprs_{0};
const CodegenFnPtr<TupleRowComparatorConfig::CompareFn>
DummyTupleRowComparator::dummy_codegend_compare_fn_{};

TopNNode::TopNNode(
    ObjectPool* pool, const TopNPlanNode& pnode, const DescriptorTbl& descs)
  : ExecNode(pool, pnode, descs),
    offset_(pnode.offset()),
    output_tuple_exprs_(pnode.output_tuple_exprs_),
    output_tuple_desc_(pnode.output_tuple_desc_),
    order_cmp_(new TupleRowLexicalComparator(*pnode.ordering_comparator_config_)),
    partition_cmp_(pnode.partition_comparator_config_ == nullptr ?
            static_cast<TupleRowComparator*>(new DummyTupleRowComparator()) :
            new TupleRowLexicalComparator(*pnode.partition_comparator_config_)),
    intra_partition_order_cmp_(pnode.intra_partition_comparator_config_ == nullptr ?
            nullptr :
            new TupleRowLexicalComparator(*pnode.intra_partition_comparator_config_)),
    tuple_pool_(nullptr),
    codegend_insert_batch_fn_(pnode.codegend_insert_batch_fn_),
    partition_heaps_(ComparatorWrapper<TupleRowComparator>(*partition_cmp_)) {
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
  if (is_partitioned()) {
    num_partitions_counter_ = ADD_COUNTER(runtime_profile(), "NumPartitions",
        TUnit::UNIT);
    in_mem_heap_created_counter_ = ADD_COUNTER(runtime_profile(), "InMemoryHeapsCreated",
        TUnit::UNIT);
    in_mem_heap_evicted_counter_ = ADD_COUNTER(runtime_profile(), "InMemoryHeapsEvicted",
        TUnit::UNIT);
    in_mem_heap_rows_filtered_counter_ = ADD_COUNTER(runtime_profile(),
        "InMemoryHeapsRowsFiltered", TUnit::UNIT);
  }

  // Set up heaps and sorters for the partitioned and non-partitioned cases.
  const TopNPlanNode& pnode = static_cast<const TopNPlanNode&>(plan_node_);
  if (is_partitioned()) {
    DCHECK_GT(per_partition_limit(), 0)
        << "Planner should not generate partitioned top-n with 0 limit";
    // Partitioned Top-N needs the external sorter.
    sorter_.reset(new Sorter(*pnode.ordering_comparator_config_, pnode.noop_tuple_exprs_,
        &row_descriptor_, mem_tracker(), buffer_pool_client(),
        resource_profile_.spillable_buffer_size, runtime_profile(), state, label(),
        true, pnode.codegend_sort_helper_fn_));
    RETURN_IF_ERROR(sorter_->Prepare(pool_));
    DCHECK_GE(resource_profile_.min_reservation, sorter_->ComputeMinReservation());
  } else {
    heap_.reset(new Heap(*order_cmp_, pnode.heap_capacity(), pnode.include_ties()));
  }
  return Status::OK();
}

void TopNPlanNode::Codegen(FragmentState* state) {
  DCHECK(state->ShouldCodegen());
  PlanNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != NULL);

  llvm::Function* compare_fn = nullptr;
  llvm::Function* intra_partition_compare_fn = nullptr;
  Status codegen_status = ordering_comparator_config_->Codegen(state, &compare_fn);
  if (codegen_status.ok() && is_partitioned()) {
    codegen_status =
        Sorter::TupleSorter::Codegen(state, compare_fn, output_tuple_desc_->byte_size(),
            &codegend_sort_helper_fn_);
  }
  if (codegen_status.ok() && is_partitioned()) {
    // TODO: IMPALA-10228: replace comparisons in std::map.
    codegen_status = partition_comparator_config_->Codegen(state);
  }
  if (codegen_status.ok() && is_partitioned()) {
    codegen_status =
        intra_partition_comparator_config_->Codegen(state, &intra_partition_compare_fn);
  }
  if (codegen_status.ok()) {
    llvm::Function* insert_batch_fn = codegen->GetFunction(is_partitioned() ?
            IRFunction::TOPN_NODE_INSERT_BATCH_PARTITIONED :
            IRFunction::TOPN_NODE_INSERT_BATCH_UNPARTITIONED, true);
    DCHECK(insert_batch_fn != NULL);

    // Generate two MaterializeExprs() functions, one with no pool that
    // does a shallow copy (used in partitioned and unpartitioned modes) and
    // one with 'tuple_pool_' that does a deep copy of the data.
    DCHECK(output_tuple_desc_ != NULL);
    llvm::Function* materialize_exprs_tuple_pool_fn = nullptr;
    llvm::Function* materialize_exprs_no_pool_fn = nullptr;

    if (!is_partitioned()) {
      codegen_status = Tuple::CodegenMaterializeExprs(codegen, false,
          *output_tuple_desc_, output_tuple_exprs_,
          true, &materialize_exprs_tuple_pool_fn);
    }

    if (codegen_status.ok()) {
      codegen_status = Tuple::CodegenMaterializeExprs(codegen, false,
          *output_tuple_desc_, output_tuple_exprs_,
          false, &materialize_exprs_no_pool_fn);
    }

    if (codegen_status.ok()) {
      int replaced;
      if (!is_partitioned()) {
        replaced = codegen->ReplaceCallSites(insert_batch_fn,
            materialize_exprs_tuple_pool_fn, Tuple::MATERIALIZE_EXPRS_SYMBOL);
        DCHECK_REPLACE_COUNT(replaced, 1) << LlvmCodeGen::Print(insert_batch_fn);
      }

      replaced = codegen->ReplaceCallSites(insert_batch_fn,
          materialize_exprs_no_pool_fn, Tuple::MATERIALIZE_EXPRS_NULL_POOL_SYMBOL);
      DCHECK_REPLACE_COUNT(replaced, 1) << LlvmCodeGen::Print(insert_batch_fn);

      if (is_partitioned()) {
        // The total number of calls to tuple_row_less_than_->Compare() is 3 in
        // PriorityQueue (called from 2 places), 1 in
        // TopNNode::Heap::InsertMaterializedTuple() and 3 in
        // TopNNode::Heap::InsertTupleWithTieHandling()
        // Each Less(Tuple*, Tuple*) indirectly calls Compare() once.
        replaced = codegen->ReplaceCallSites(insert_batch_fn,
            intra_partition_compare_fn, TupleRowComparator::COMPARE_SYMBOL);
        DCHECK_REPLACE_COUNT(replaced, 10) << LlvmCodeGen::Print(insert_batch_fn);
      } else {
        // The total number of calls to tuple_row_less_than_->Compare() is 3 in
        // PriorityQueue (called from 2 places), 1 in TopNNode::Heap::InsertTupleRow()
        // and 3 in TopNNode::Heap::InsertTupleWithTieHandling
        // Each Less(Tuple*, Tuple*) indirectly calls Compare() once.
        replaced = codegen->ReplaceCallSites(insert_batch_fn,
            compare_fn, TupleRowComparator::COMPARE_SYMBOL);
        DCHECK_REPLACE_COUNT(replaced, 10) << LlvmCodeGen::Print(insert_batch_fn);
      }

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
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  RETURN_IF_ERROR(child(0)->Open(state));

  RETURN_IF_ERROR(
      order_cmp_->Open(pool_, state, expr_perm_pool(), expr_results_pool()));
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(output_tuple_expr_evals_, state));
  if (is_partitioned()) {
    // Set up state required by partitioned top-N implementation. Claim reservation
    // after the child has been opened to reduce the peak reservation requirement.
    if (!buffer_pool_client()->is_registered()) {
      RETURN_IF_ERROR(ClaimBufferReservation(state));
    }
    RETURN_IF_ERROR(
        partition_cmp_->Open(pool_, state, expr_perm_pool(), expr_results_pool()));
    RETURN_IF_ERROR(intra_partition_order_cmp_->Open(
          pool_, state, expr_perm_pool(), expr_results_pool()));
    RETURN_IF_ERROR(sorter_->Open());
  }

  // Allocate memory for a temporary tuple.
  tmp_tuple_ = reinterpret_cast<Tuple*>(
      tuple_pool_->Allocate(output_tuple_desc_->byte_size()));

  // Limit of 0, no need to fetch anything from children.
  const TopNPlanNode& pnode = static_cast<const TopNPlanNode&>(plan_node_);
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
          insert_batch_fn(this, state, &batch);
        } else if (is_partitioned()) {
          InsertBatchPartitioned(state, &batch);
        } else {
          InsertBatchUnpartitioned(state, &batch);
        }
        DCHECK(is_partitioned() || heap_->DCheckConsistency());
        if (is_partitioned()) {
          if (partition_heaps_.size() > FLAGS_partitioned_topn_in_mem_partitions_limit ||
            tuple_pool_->total_reserved_bytes() >
              FLAGS_partitioned_topn_soft_limit_bytes) {
            RETURN_IF_ERROR(EvictPartitions(state, /*evict_final=*/false));
          }
        } else if (rows_to_reclaim_ > 2 * unpartitioned_capacity()) {
          RETURN_IF_ERROR(ReclaimTuplePool(state));
        }
      }
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
    } while (!eos);
  }
  RETURN_IF_ERROR(PrepareForOutput(state));

  // Unless we are inside a subplan expecting to call Open()/GetNext() on the child
  // again, the child can be closed at this point.
  if (!IsInSubplan()) child(0)->Close(state);
  return Status::OK();
}

Status TopNNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  return is_partitioned() ? GetNextPartitioned(state, row_batch, eos)
                          : GetNextUnpartitioned(state, row_batch, eos);
}

Status TopNNode::GetNextUnpartitioned(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  DCHECK(!is_partitioned());
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
  }
  *eos = get_next_iter_ == sorted_top_n_.end();

  // Transfer ownership of tuple data to output batch.
  // TODO: To improve performance for small inputs when this node is run multiple times
  // inside a subplan, we might choose to only selectively transfer, e.g., when the
  // block(s) in the pool are all full or when the pool has reached a certain size.
  if (*eos) row_batch->tuple_data_pool()->AcquireData(tuple_pool_.get(), false);
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

Status TopNNode::GetNextPartitioned(
    RuntimeState* state, RowBatch* batch, bool* eos) {
  DCHECK(is_partitioned());
  *eos = false;
  while (!batch->AtCapacity()) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    if (sort_out_batch_pos_ >= sort_out_batch_->num_rows()) {
      // Output rows will reference tuples from sorter output batches - make sure memory
      // is transferred correctly.
      sort_out_batch_->TransferResourceOwnership(batch);
      sort_out_batch_->Reset();
      sort_out_batch_pos_ = 0;
      if (batch->AtCapacity()) break;
      bool sorter_eos = false;
      RETURN_IF_ERROR(sorter_->GetNext(sort_out_batch_.get(), &sorter_eos));
      if (sorter_eos && sort_out_batch_->num_rows() == 0) {
        sort_out_batch_->TransferResourceOwnership(batch);
        *eos = true;
        break;
      }
    }
    // Copy rows within the partition limits from 'sort_out_batch_' to 'batch'.
    // NOTE: this loop could be codegen'd, but is unlikely to be the bottleneck for
    // most partitioned top-N queries.
    while (sort_out_batch_pos_ < sort_out_batch_->num_rows()) {
      TupleRow* curr_row = sort_out_batch_->GetRow(sort_out_batch_pos_);
      ++sort_out_batch_pos_;
      // If 'num_rows_returned_from_partition_' > 0, then 'prev_row' is the previous row
      // returned from the current partition.
      TupleRow* prev_row = reinterpret_cast<TupleRow*>(&tmp_tuple_);
      bool add_row = false;
      if (num_rows_returned_from_partition_ > 0
          && partition_cmp_->Compare(curr_row, prev_row) == 0) {
        // Return rows up to the limit plus any ties that match the last returned row.
        if (num_rows_returned_from_partition_ < per_partition_limit()
            || (include_ties() &&
                intra_partition_order_cmp_->Compare(curr_row, prev_row) == 0)) {
          add_row = true;
          ++num_rows_returned_from_partition_;
        }
      } else {
        // New partition.
        DCHECK_GT(per_partition_limit(), 0);
        COUNTER_ADD(num_partitions_counter_, 1);
        add_row = true;
        num_rows_returned_from_partition_ = 1;
      }
      if (add_row) {
        Tuple* out_tuple = curr_row->GetTuple(0);
        tmp_tuple_ = out_tuple;
        TupleRow* out_row = batch->GetRow(batch->AddRow());
        out_row->SetTuple(0, out_tuple);
        batch->CommitLastRow();
        IncrementNumRowsReturned(1);
        if (batch->AtCapacity()) break;
      }
    }
  }
  DCHECK(*eos || batch->AtCapacity());
  if (num_rows_returned_from_partition_ == 0) {
    // tmp_tuple_ references a previous partition, if anything. Make it clear that it's
    // invalid.
    tmp_tuple_ = nullptr;
  } else if (num_rows_returned_from_partition_ > 0) {
    // 'tmp_tuple_' is part of the current partition. Deep copy so that it doesn't
    // reference memory that is attached to the output row batch.
    Tuple* prev_tmp_tuple = tmp_tuple_;
    unique_ptr<MemPool> temp_pool(new MemPool(mem_tracker()));
    RETURN_IF_ERROR(InitTmpTuple(state, temp_pool.get()));
    prev_tmp_tuple->DeepCopy(tmp_tuple_, *output_tuple_desc_, temp_pool.get());
    tuple_pool_->FreeAll();
    tuple_pool_ = move(temp_pool);
  }
  COUNTER_SET(rows_returned_counter_, rows_returned());
  return Status::OK();
}

Status TopNNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  if (is_partitioned()) {
    partition_heaps_.clear();
    sorter_->Reset();
    sort_out_batch_.reset();
    sort_out_batch_pos_ = 0;
    num_rows_returned_from_partition_ = 0;
  } else {
    heap_->Reset();
  }
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
  for (auto& entry : partition_heaps_) {
    DCHECK(entry.second != nullptr);
    if (entry.second != nullptr) entry.second->Close();
  }
  if (tuple_pool_.get() != nullptr) tuple_pool_->FreeAll();
  if (order_cmp_.get() != nullptr) order_cmp_->Close(state);
  if (partition_cmp_.get() != nullptr) partition_cmp_->Close(state);
  if (intra_partition_order_cmp_.get() != nullptr) {
    intra_partition_order_cmp_->Close(state);
  }
  if (sorter_ != nullptr) sorter_->Close(state);
  sort_out_batch_.reset();
  ScalarExprEvaluator::Close(output_tuple_expr_evals_, state);
  ExecNode::Close(state);
}

Status TopNNode::EvictPartitions(RuntimeState* state, bool evict_final) {
  DCHECK(is_partitioned());
  vector<unique_ptr<Heap>> heaps_to_evict;
  if (evict_final) {
    // Move all the partitions to 'sorter_' in preparation for the final sort. Partitions
    // are evicted in the order of the partition key to reduce the amount of shuffling
    // that the final sort will do to rearrange partitions.
    for (auto& entry : partition_heaps_) {
      heaps_to_evict.push_back(move(entry.second));
    }
    partition_heaps_.clear();
  } else {
    heaps_to_evict = SelectPartitionsToEvict();
  }
  // Only count heap eviction if they are as a result of memory pressure.
  if (!evict_final) COUNTER_ADD(in_mem_heap_evicted_counter_, heaps_to_evict.size());

  RowBatch batch(row_desc(), state->batch_size(), mem_tracker());
  for (auto& heap : heaps_to_evict) {
    DCHECK(heap->DCheckConsistency());
    // Extract partition entries from the heap in sorted order to reduce amount of sorting
    // required in final sort. This sorting is not required for correctness since
    // 'sorter_' will do a full sort later.
    heap->PrepareForOutput(*this, &sorted_top_n_);
    for (int64_t i = 0; i < sorted_top_n_.size(); ++i) {
      TupleRow* row = batch.GetRow(batch.AddRow());
      row->SetTuple(0, sorted_top_n_[i]);
      batch.CommitLastRow();
      if (batch.AtCapacity() || i == sorted_top_n_.size() - 1) {
        RETURN_IF_ERROR(sorter_->AddBatch(&batch));
        batch.Reset();
      }
    }
    sorted_top_n_.clear();
  }
  heaps_to_evict.clear();

  // ReclaimTuplePool() can now reclaim memory that is not used by in-memory partitions.
  RETURN_IF_ERROR(ReclaimTuplePool(state));
  return Status::OK();
}

vector<unique_ptr<TopNNode::Heap>> TopNNode::SelectPartitionsToEvict() {
  // Evict a subset of heaps to free enough memory to continue.
  // The goal of this approach is to try to maximize rows filtered out, while only
  // adding O(1) amortized cost per input row. Rematerializing all the heaps (required
  // to free memory) is O(m) work, where m is the total number of tuples in the heaps.
  // If we clear out O(m) tuples, that means we will have to process at least O(m) input
  // rows before another eviction, so the amortized overhead of eviction per input row
  // is O(m) / O(m) = O(1).
  //
  // We evict heaps starting with the heaps that were least effective at filtering
  // input. We evict 25% of heap tuples so that we achieve O(1) amortized time but
  // retain effectively filtering heaps as much as possible. We break ties, which
  // are most likely heaps that have not filtered input since the last eviction,
  // based on whether they are growing and likely to start filtering in the near
  // future.
  // TODO: it's possible that we could free up memory without evicting any heaps
  // just by reclaiming unreferenced variable-length data. We do not do that yet
  // because we don't know if it will reclaim enough memory. Evicting some heaps
  // is guaranteed to be effective.
  vector<PartitionHeapMap::iterator> sorted_heaps;
  int64_t total_tuples = 0;
  sorted_heaps.reserve(partition_heaps_.size());
  for (auto it = partition_heaps_.begin(); it != partition_heaps_.end(); ++it) {
    total_tuples += it->second->num_tuples();
    sorted_heaps.push_back(it);
  }
  sort(sorted_heaps.begin(), sorted_heaps.end(),
      [](const PartitionHeapMap::iterator& left,
          const PartitionHeapMap::iterator& right) {
        int64_t left_discarded = left->second->num_tuples_discarded();
        int64_t right_discarded = right->second->num_tuples_discarded();
        if (left_discarded != right_discarded) {
          return left_discarded < right_discarded;
        }
        return left->second->num_tuples_added_since_eviction() <
            right->second->num_tuples_added_since_eviction();
      });

  vector<unique_ptr<Heap>> result;
  int64_t num_tuples_evicted = 0;
  for (auto it : sorted_heaps) {
    if (num_tuples_evicted < total_tuples / 4) {
      result.push_back(move(it->second));
      partition_heaps_.erase(it);
      num_tuples_evicted += result.back()->num_tuples();
    } else {
      // Reset counters on surviving heaps so that statistics are accurate about
      // recent filtering.
      it->second->ResetStats(*this);
    }
  }
  return result;
}

Status TopNNode::PrepareForOutput(RuntimeState* state) {
  if (is_partitioned()) {
    // Dump all rows into the sorter and sort by partition, so that we can iterate
    // through the rows and build heaps partition-by-partition.
    RETURN_IF_ERROR(EvictPartitions(state, /*evict_final=*/true));
    DCHECK(partition_heaps_.empty());
    RETURN_IF_ERROR(sorter_->InputDone());
    sort_out_batch_.reset(
        new RowBatch(row_desc(), state->batch_size(), mem_tracker()));
  } else {
    DCHECK(heap_->DCheckConsistency());
    heap_->PrepareForOutput(*this, &sorted_top_n_);
    get_next_iter_ = sorted_top_n_.begin();
  }
  return Status::OK();
}

void TopNNode::Heap::PrepareForOutput(
    const TopNNode& RESTRICT node, vector<Tuple*>* sorted_top_n) RESTRICT {
  ResetStats(node); // Ensure all counters are updated.
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

void TopNNode::Heap::ResetStats(const TopNNode& RESTRICT node) {
  RuntimeProfile::Counter* counter = node.in_mem_heap_rows_filtered_counter_;
  if (counter != nullptr) COUNTER_ADD(counter, num_tuples_discarded_);
  num_tuples_discarded_ = 0;
  num_tuples_at_last_eviction_ = num_tuples();
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
  COUNTER_ADD(tuple_pool_reclaim_counter_, 1);
  unique_ptr<MemPool> temp_pool(new MemPool(mem_tracker()));

  if (is_partitioned()) {
    vector<unique_ptr<Heap>> rematerialized_heaps;
    for (auto& entry : partition_heaps_) {
      RETURN_IF_ERROR(entry.second->RematerializeTuples(this, state, temp_pool.get()));
      DCHECK(entry.second->DCheckConsistency());
    }
    // The second loop is needed for IMPALA-11631. We only move heaps from partition_heap_
    // to rematerialized_heaps once all have been rematerialized. Otherwise, in case of
    // an error, we may call Close() on a nullptr or leak the memory by not explicitly
    // calling Close() on the heap pointer. Maybe better to add Close() in the Heap
    // destructor later.
    for (auto& entry : partition_heaps_) {
      // The key references memory in 'tuple_pool_'. Replace it with a rematerialized
      // tuple.
      rematerialized_heaps.push_back(move(entry.second));
    }
    partition_heaps_.clear();
    for (auto& heap_ptr : rematerialized_heaps) {
      const Tuple* key_tuple = heap_ptr->top();
      partition_heaps_.emplace(key_tuple, move(heap_ptr));
    }
  } else {
    RETURN_IF_ERROR(heap_->RematerializeTuples(this, state, temp_pool.get()));
    DCHECK(heap_->DCheckConsistency());
  }
  rows_to_reclaim_ = 0;
  RETURN_IF_ERROR(InitTmpTuple(state, temp_pool.get()));
  tuple_pool_->FreeAll();
  tuple_pool_ = move(temp_pool);
  return Status::OK();
}

Status TopNNode::InitTmpTuple(RuntimeState* state, MemPool* pool) {
  tmp_tuple_ = reinterpret_cast<Tuple*>(pool->TryAllocate(
      output_tuple_desc_->byte_size()));
  if (UNLIKELY(tmp_tuple_ == nullptr)) {
    return pool->mem_tracker()->MemLimitExceeded(state,
        "Failed to allocate memory in TopNNode::ReclaimTuplePool.",
        output_tuple_desc_->byte_size());
  }
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
