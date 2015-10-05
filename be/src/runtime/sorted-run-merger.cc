// Copyright 2012 Cloudera Inc.
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
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "runtime/sorter.h"
#include "runtime/tuple-row.h"
#include "util/runtime-profile.h"

#include "common/names.h"

namespace impala {

/// BatchedRowSupplier returns individual rows in a batch obtained from a sorted input
/// run (a RunBatchSupplier). Used as the heap element in the min heap maintained by the
/// merger.
/// Next() advances the row supplier to the next row in the input batch and retrieves
/// the next batch from the input if the current input batch is exhausted. Transfers
/// ownership from the current input batch to an output batch if requested.
class SortedRunMerger::BatchedRowSupplier {
 public:
  /// Construct an instance from a sorted input run.
  BatchedRowSupplier(SortedRunMerger* parent, const RunBatchSupplier& sorted_run)
    : sorted_run_(sorted_run),
      input_row_batch_(NULL),
      input_row_batch_index_(-1),
      parent_(parent) {
  }

  /// Retrieves the first batch of sorted rows from the run.
  Status Init(bool* done) {
    *done = false;
    RETURN_IF_ERROR(sorted_run_(&input_row_batch_));
    if (input_row_batch_ == NULL) {
      *done = true;
      return Status::OK();
    }
    RETURN_IF_ERROR(Next(NULL, done));
    return Status::OK();
  }

  /// Increment the current row index. If the current input batch is exhausted fetch the
  /// next one from the sorted run. Transfer ownership to transfer_batch if not NULL.
  Status Next(RowBatch* transfer_batch, bool* done) {
    DCHECK(input_row_batch_ != NULL);
    ++input_row_batch_index_;
    if (input_row_batch_index_ < input_row_batch_->num_rows()) {
      *done = false;
    } else {
      ScopedTimer<MonotonicStopWatch> timer(parent_->get_next_batch_timer_);
      if (transfer_batch != NULL) {
        input_row_batch_->TransferResourceOwnership(transfer_batch);
      }

      RETURN_IF_ERROR(sorted_run_(&input_row_batch_));
      DCHECK(input_row_batch_ == NULL || input_row_batch_->num_rows() > 0);
      *done = input_row_batch_ == NULL;
      input_row_batch_index_ = 0;
    }
    return Status::OK();
  }

  TupleRow* current_row() const {
    return input_row_batch_->GetRow(input_row_batch_index_);
  }

 private:
  friend class SortedRunMerger;

  /// The run from which this object supplies rows.
  RunBatchSupplier sorted_run_;

  /// The current input batch being processed.
  RowBatch* input_row_batch_;

  /// Index into input_row_batch_ of the current row being processed.
  int input_row_batch_index_;

  /// The parent merger instance.
  SortedRunMerger* parent_;
};

void SortedRunMerger::Heapify(int parent_index) {
  int left_index = 2 * parent_index + 1;
  int right_index = left_index + 1;
  if (left_index >= min_heap_.size()) return;
  int least_child;
  // Find the least child of parent.
  if (right_index >= min_heap_.size() ||
      compare_less_than_(min_heap_[left_index]->current_row(),
          min_heap_[right_index]->current_row())) {
    least_child = left_index;
  } else {
    least_child = right_index;
  }

  // If the parent is out of place, swap it with the least child and invoke
  // Heapify recursively.
  if (compare_less_than_(min_heap_[least_child]->current_row(),
      min_heap_[parent_index]->current_row())) {
    iter_swap(min_heap_.begin() + least_child, min_heap_.begin() + parent_index);
    Heapify(least_child);
  }
}

SortedRunMerger::SortedRunMerger(const TupleRowComparator& compare_less_than,
    RowDescriptor* row_desc, RuntimeProfile* profile, bool deep_copy_input)
  : compare_less_than_(compare_less_than),
    input_row_desc_(row_desc),
    deep_copy_input_(deep_copy_input) {
  get_next_timer_ = ADD_TIMER(profile, "MergeGetNext");
  get_next_batch_timer_ = ADD_TIMER(profile, "MergeGetNextBatch");
}

Status SortedRunMerger::Prepare(const vector<RunBatchSupplier>& input_runs) {
  DCHECK_EQ(min_heap_.size(), 0);
  min_heap_.reserve(input_runs.size());
  BOOST_FOREACH(const RunBatchSupplier& input_run, input_runs) {
    BatchedRowSupplier* new_elem = pool_.Add(new BatchedRowSupplier(this, input_run));
    DCHECK(new_elem != NULL);
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
  if (min_heap_.empty()) {
    *eos = true;
    return Status::OK();
  }

  while (!output_batch->AtCapacity()) {
    BatchedRowSupplier* min = min_heap_[0];
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

    bool min_run_complete = false;
    // Advance to the next element in min. output_batch is supplied to transfer
    // resource ownership if the input batch in min is exhausted.
    RETURN_IF_ERROR(min->Next(deep_copy_input_ ? NULL : output_batch,
        &min_run_complete));
    if (min_run_complete) {
      // Remove the element from the heap.
      iter_swap(min_heap_.begin(), min_heap_.end() - 1);
      min_heap_.pop_back();
      if (min_heap_.empty()) break;
    }

    Heapify(0);
  }

  *eos = min_heap_.empty();
  return Status::OK();
}

}
