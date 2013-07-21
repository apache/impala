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


#ifndef IMPALA_SORTING_SORTED_MERGER_H_
#define IMPALA_SORTING_SORTED_MERGER_H_

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "row-batch-supplier.h"
#include "util/tuple-row-compare.h"
#include "common/object-pool.h"
#include "runtime/mem-pool.h"
#include "runtime/descriptors.h"
#include "util/runtime-profile.h"

namespace impala {

class Expr;
class RowBatch;
class Tuple;
class TupleRow;

// Constructs a specialized merger intended to merge a set of already-sorted runs
// into a single sorted stream. The input and output rows share the same row_desc.
// Row comparison is done on the exprs in sort_exprs_lhs/_rhs. 'is_asc' determines
// if each expr comparison is done in ascending or descending sort order.
// If 'nulls_first' is true, NULL values precede all other values, otherwise
// they follow all other values.
// If 'remove_dups' is true, duplicates are removed during merging.
// The merger will try to use no more than 'mem_limit' total memory.
// TODO: Utilize normalized keys if we have them.
class SortedMerger {
 public:
  SortedMerger(
      const RowDescriptor& row_desc,
      const std::vector<Expr*>& sort_exprs_lhs,
      const std::vector<Expr*>& sort_exprs_rhs,
      const std::vector<bool>& is_asc,
      bool nulls_first,
      bool remove_dups,
      uint64_t mem_limit);
  ~SortedMerger();

  // Adds a new sorted run to be merged, supplied as a set of ordered RowBatches.
  // Must not be called after GetNext().
  // The merger takes ownership of the batch supplier, and will free it when done.
  // This function is not thread-safe.
  void AddRun(RowBatchSupplier* batch_supplier);

  // Remove next batch of output tuples/rows from merger’s stream of sorted tuples.
  Status GetNext(RowBatch* batch, bool* eos);

 private:
  class SortedRun;

  // Pairs a TupleRow with the SortedRun it came from.
  struct MergeTuple {
    SortedRun* run_;
    TupleRow* row_;
    MergeTuple(SortedRun* run, TupleRow* row) : run_(run), row_(row) { }
  };

  // Implements a priority queue of MergeTuples.
  // The heap is backed by a vector. We insert a dummy element to make the vector
  // 1-indexed. This means that the children of node i are 2i and 2i+1.
  // Similarly, the parent of node i is i/2.
  // TODO: Utilize in top-n node.
  class MergeHeap {
   public:
    MergeHeap(const TupleRowComparator& key_comp) : comp_(key_comp) {
      // Add dummy element to make the vector 1-indexed.
      heap_.push_back(MergeTuple(NULL, NULL));
    }

    // Push a new TupleRow from a SortedRun onto the heap. The row is not copied.
    void Push(SortedRun* run, TupleRow* row) {
      heap_.push_back(MergeTuple(run, row));
      HeapBubbleUp(heap_.size() - 1);
    }

    // Removes the least Tuple from the heap.
    MergeTuple Pop() {
      MergeTuple min = heap_[1];
      iter_swap(heap_.begin() + 1, heap_.end() - 1);
      heap_.pop_back();
      HeapBubbleDown(1);
      return min;
    }

    // Returns the least Tuple in the heap without removing it.
    MergeTuple Peek() {
      return heap_[1];
    }

    bool empty() const { return heap_.size() == 1; }

   private:
    // Recursively bubbles an out-of-place element up the heap.
    void HeapBubbleUp(int i) {
      if (i == 1) return;

      int parent = i / 2;
      if (comp_(heap_[i].row_, heap_[parent].row_)) {
        iter_swap(heap_.begin() + i, heap_.begin() + parent);
        HeapBubbleUp(parent);
      }
    }

    // Recursively bubbles an out-of-place element down the heap.
    void HeapBubbleDown(int i) {
      int child0 = 2 * i;
      int child1 = 2 * i + 1;
      int least_child;

      if (child0 >= heap_.size()) return;

      if (child1 >= heap_.size() || comp_(heap_[child0].row_, heap_[child1].row_)) {
        least_child = child0;
      } else {
        least_child = child1;
      }

      if (comp_(heap_[least_child].row_, heap_[i].row_)) {
        iter_swap(heap_.begin() + i, heap_.begin() + least_child);
        HeapBubbleDown(least_child);
      }
    }

    // Underlying vector backing the heap.
    std::vector<MergeTuple> heap_;

    // Comparator for tuple rows.
    TupleRowComparator comp_;
  };

  RowDescriptor row_desc_;
  const bool remove_dups_;
  boost::scoped_ptr<MemTracker> mem_tracker_;

  // The set of incoming SortedRuns that we're merging.
  // This will be a input_runs_.size()-way merge.
  std::vector<SortedRun*> input_runs_;

  // The heap in which we store the least tuple remaining from each SortedRun.
  MergeHeap merge_heap_;

  // Holds sorting and output tuples
  boost::scoped_ptr<MemPool> tuple_pool_;

  // The last TupleRow returned to the user, used for duplicate elimination.
  TupleRow* last_output_row_;

  // Pool in which the last_output_row_ is stored to preserve it across GetNext() calls.
  boost::scoped_ptr<MemPool> last_output_row_pool_;

  // Tests for equality of tuple rows.
  RowEqualityChecker row_equality_checker_;
};

}

#endif
