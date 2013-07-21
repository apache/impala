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

#include "sorted-merger.h"

#include "exprs/expr.h"
#include "sort-util.h"
#include "util/tuple-row-compare.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"

using namespace boost;
using namespace std;

namespace impala {

SortedMerger::SortedMerger(
    const RowDescriptor& row_desc,
    const std::vector<Expr*>& sort_exprs_lhs,
    const std::vector<Expr*>& sort_exprs_rhs,
    const std::vector<bool>& is_asc,
    bool nulls_first, bool remove_dups, uint64_t mem_limit)
    : row_desc_(row_desc),
      remove_dups_(remove_dups),
      merge_heap_(TupleRowComparator(sort_exprs_lhs, sort_exprs_rhs, is_asc, nulls_first)),
      last_output_row_(NULL),
      row_equality_checker_(row_desc) {
  DCHECK_EQ(sort_exprs_lhs.size(), sort_exprs_rhs.size());
  DCHECK_EQ(sort_exprs_lhs.size(), is_asc.size());

  mem_tracker_.reset(new MemTracker(mem_limit));
  tuple_pool_.reset(new MemPool(mem_tracker_.get()));
  last_output_row_pool_.reset(new MemPool(mem_tracker_.get()));
}

// Represents an incoming sorted run of TupleRows, comprised of a set of RowBatches.
// The SortedRun owns the RowBatch used to load from the RowBatchSuppliers.
class SortedMerger::SortedRun {
 public:
  SortedRun(RowBatchSupplier* batch_supplier, MergeHeap* merge_heap,
      const RowDescriptor& row_desc, MemTracker* mem_tracker)
      : batch_supplier_(batch_supplier), merge_heap_(merge_heap),
        index_(0), end_of_batches_(false) {
    batch_.reset(new RowBatch(row_desc, ROW_BATCH_SIZE, mem_tracker));
  }

  Status Next() {
    if (index_ >= batch_->num_rows() && !end_of_batches_) {
      batch_->Reset();
      RETURN_IF_ERROR(batch_supplier_->GetNext(batch_.get(), &end_of_batches_));
      index_ = 0;
    }

    // If the run is empty, just don't add to the heap.
    if (index_ >= batch_->num_rows()) return Status::OK;

    TupleRow* cur_row = batch_->GetRow(index_);
    merge_heap_->Push(this, cur_row);
    ++index_;
    return Status::OK;
  }

  RowBatchSupplier* batch_supplier() const { return batch_supplier_; }

 private:
  static const int ROW_BATCH_SIZE = 1024;

  // Source of ordered RowBatches.
  RowBatchSupplier* batch_supplier_;

  // Used to load all batches from the batch_supplier_.
  scoped_ptr<RowBatch> batch_;

  // Heap to put our TupleRows.
  // The heap is owned by the SortedMerger.
  MergeHeap* merge_heap_;

  // Index into the current batch.
  int index_;

  // True if the batch supplier has no more batches.
  bool end_of_batches_;
};

// Destructor must be defined after definition of SortedRun to avoid warnings.
SortedMerger::~SortedMerger() {
  for (int i = 0; i < input_runs_.size(); ++i) {
    delete input_runs_[i]->batch_supplier();
    delete input_runs_[i];
  }
}

void SortedMerger::AddRun(RowBatchSupplier* batch_supplier) {
  SortedRun* supplier =
      new SortedRun(batch_supplier, &merge_heap_, row_desc_, mem_tracker_.get());
  supplier->Next();
  input_runs_.push_back(supplier);
}

Status SortedMerger::GetNext(RowBatch* batch, bool* eos) {
  while (!batch->IsFull() && !merge_heap_.empty()) {
    MergeTuple min = merge_heap_.Pop();

    // Possibly skip this row if remove_dups is on and it's a duplicate
    // NB: If remove_dups_ is false, last_output_row_ is always NULL.
    if (last_output_row_ == NULL || !row_equality_checker_(last_output_row_, min.row_)) {
      int row_idx = batch->AddRow();
      TupleRow* output_row = batch->GetRow(row_idx);
      min.row_->DeepCopy(output_row, row_desc_.tuple_descriptors(),
          batch->tuple_data_pool(), false);
      batch->CommitLastRow();

      if (remove_dups_) last_output_row_ = output_row;
    }

    RETURN_IF_ERROR(min.run_->Next());
  }

  // Make sure we hold onto the last tuple row for the next batch by copying
  // it into our internal pool.
  if (remove_dups_ && last_output_row_ != NULL) {
    last_output_row_pool_->Clear();
    last_output_row_ = last_output_row_->DeepCopy(row_desc_.tuple_descriptors(),
        last_output_row_pool_.get());
  }

  *eos = merge_heap_.empty();

  return Status::OK;
}

}
