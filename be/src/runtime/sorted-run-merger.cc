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

#include "runtime/sorted-run-merger.h"
#include "exprs/scalar-expr.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "runtime/sorter.h"
#include "runtime/tuple-row.h"
#include "util/runtime-profile-counters.h"
#include "util/tuple-row-compare.h"
#include "runtime/fragment-state.h"
#include "codegen/llvm-codegen.h"

#include "common/names.h"

namespace impala {

SortedRunMerger::SortedRunWrapper::SortedRunWrapper(SortedRunMerger* parent,
    const RunBatchSupplierFn& sorted_run)
  : sorted_run_(sorted_run),
    input_row_batch_(nullptr),
    input_row_batch_index_(-1),
    parent_(parent) {
}

Status SortedRunMerger::SortedRunWrapper::Init(bool* eos) {
  *eos = false;
  RETURN_IF_ERROR(sorted_run_(&input_row_batch_));
  if (input_row_batch_ == nullptr) {
    *eos = true;
    return Status::OK();
  }
  RETURN_IF_ERROR(Advance(nullptr, eos));
  return Status::OK();
}

Status SortedRunMerger::SortedRunWrapper::Advance(RowBatch* transfer_batch, bool* eos) {
  DCHECK(input_row_batch_ != nullptr);
  ++input_row_batch_index_;
  if (input_row_batch_index_ < input_row_batch_->num_rows()) {
    *eos = false;
    return Status::OK();
  }

  // Iterate until we hit eos or get a non-empty batch.
  do {
    // Make sure to transfer resources from every batch received from 'sorted_run_'.
    if (transfer_batch != nullptr) {
      DCHECK_ENUM_EQ(
          RowBatch::FlushMode::NO_FLUSH_RESOURCES, input_row_batch_->flush_mode())
          << "Run batch suppliers that flush resources must use a deep-copying merger";
      input_row_batch_->TransferResourceOwnership(transfer_batch);
    }

    {
      ScopedTimer<MonotonicStopWatch> timer(parent_->get_next_batch_timer_);
      RETURN_IF_ERROR(sorted_run_(&input_row_batch_));
    }
  } while (input_row_batch_ != nullptr && input_row_batch_->num_rows() == 0);

  *eos = input_row_batch_ == nullptr;
  input_row_batch_index_ = 0;
  return Status::OK();
}

void SortedRunMerger::Heapify(int parent_index) {
  const HeapifyHelperFn heapify_helper_fn = codegend_heapify_helper_fn_.load();

  if (heapify_helper_fn != nullptr) {
    heapify_helper_fn(this, parent_index);
  } else {
    HeapifyHelper(parent_index);
  }
}

SortedRunMerger::SortedRunMerger(const TupleRowComparator& comparator,
    const RowDescriptor* row_desc, RuntimeProfile* profile, bool deep_copy_input,
    const CodegenFnPtr<HeapifyHelperFn>& codegend_heapify_helper_fn)
  : comparator_(comparator),
    input_row_desc_(row_desc),
    deep_copy_input_(deep_copy_input),
    codegend_heapify_helper_fn_(codegend_heapify_helper_fn) {
  get_next_timer_ = ADD_TIMER(profile, "MergeGetNext");
  get_next_batch_timer_ = ADD_TIMER(profile, "MergeGetNextBatch");
}

Status SortedRunMerger::Prepare(const vector<RunBatchSupplierFn>& input_runs) {
  DCHECK_EQ(min_heap_.size(), 0);
  min_heap_.reserve(input_runs.size());
  for (const RunBatchSupplierFn& input_run: input_runs) {
    SortedRunWrapper* new_elem = pool_.Add(new SortedRunWrapper(this, input_run));
    DCHECK(new_elem != nullptr);
    bool empty;
    RETURN_IF_ERROR(new_elem->Init(&empty));
    if (!empty) min_heap_.push_back(new_elem);
  }

  // Construct the min heap from the sorted runs.
  const int last_parent = (min_heap_.size() / 2) - 1;
  for (int i = last_parent; i >= 0; --i) {
    Heapify(i);
  }
  return Status::OK();
}

Status SortedRunMerger::GetNext(RowBatch* output_batch, bool* eos) {
  ScopedTimer<MonotonicStopWatch> timer(get_next_timer_);

  while (!output_batch->AtCapacity() && !min_heap_.empty()) {
    SortedRunWrapper* min = min_heap_[0];
    int output_row_index = output_batch->AddRow();
    TupleRow* output_row = output_batch->GetRow(output_row_index);
    if (deep_copy_input_) {
      min->current_row()->DeepCopy(output_row, input_row_desc_->tuple_descriptors(),
          output_batch->tuple_data_pool(), false);
    } else {
      // Simply copy tuple pointers if deep_copy is false.
      memcpy(output_row, min->current_row(),
          input_row_desc_->tuple_descriptors().size() * sizeof(Tuple*));
    }

    output_batch->CommitLastRow();
    RETURN_IF_ERROR(AdvanceMinRow(output_batch));
  }
  *eos = min_heap_.empty();
  return Status::OK();
}

Status SortedRunMerger::AdvanceMinRow(RowBatch* transfer_batch) {
  SortedRunWrapper* min = min_heap_[0];
  bool min_run_complete;
  // Advance to the next element in min. output_batch is supplied to transfer
  // resource ownership if the input batch in min is exhausted.
  RETURN_IF_ERROR(min->Advance(deep_copy_input_ ? nullptr : transfer_batch,
      &min_run_complete));
  if (min_run_complete) {
    // Remove the element from the heap.
    iter_swap(min_heap_.begin(), min_heap_.end() - 1);
    min_heap_.pop_back();
  }
  if (!min_heap_.empty()) {
    Heapify(0);
  }
  return Status::OK();
}

const char* SortedRunMerger::LLVM_CLASS_NAME = "class.impala::SortedRunMerger";

Status SortedRunMerger::Codegen(FragmentState* state, llvm::Function* compare_fn,
      CodegenFnPtr<HeapifyHelperFn>* codegend_fn) {
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != nullptr);

  llvm::Function* fn = codegen->GetFunction(
      IRFunction::SORTED_RUN_MERGER_HEAPIFY_HELPER, true);
  DCHECK(fn != nullptr);

  int replaced =
      codegen->ReplaceCallSites(fn, compare_fn, TupleRowComparator::COMPARE_SYMBOL);
  DCHECK_REPLACE_COUNT(replaced, 2) << LlvmCodeGen::Print(fn);

  fn = codegen->FinalizeFunction(fn);
  if (fn == nullptr) {
    return Status("SortedRunMerger::Codegen(): failed to finalize function");
  }
  codegen->AddFunctionToJit(fn, codegend_fn);

  return Status::OK();
}

}
