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


#ifndef IMPALA_RUNTIME_SORTED_RUN_MERGER_H_
#define IMPALA_RUNTIME_SORTED_RUN_MERGER_H_

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "common/object-pool.h"
#include "util/tuple-row-compare.h"

namespace impala {

class RowBatch;
class RowDescriptor;
class RuntimeProfile;

/// SortedRunMerger is used to merge multiple sorted runs of tuples. A run is a sorted
/// sequence of row batches, which are fetched from a RunBatchSupplier function object.
/// Merging is implemented using a binary min-heap that maintains the run with the next
/// tuple in sorted order at the top of the heap.
///
/// Merged batches of rows are retrieved from SortedRunMerger via calls to GetNext().
/// The merger is constructed with a boolean flag deep_copy_input.
/// If true, sorted output rows are deep copied into the data pool of the output batch.
/// If false, GetNext() only copies tuple pointers (TupleRows) into the output batch,
/// and transfers resource ownership from the input batches to the output batch when
/// an input batch is processed.
class SortedRunMerger {
 public:
  /// Function that returns the next batch of rows from an input sorted run. The batch
  /// is owned by the supplier (i.e. not SortedRunMerger). eos is indicated by an NULL
  /// batch being returned.
  typedef boost::function<Status (RowBatch**)> RunBatchSupplier;

  SortedRunMerger(const TupleRowComparator& compare_less_than, RowDescriptor* row_desc,
      RuntimeProfile* profile, bool deep_copy_input);

  /// Prepare this merger to merge and return rows from the sorted runs in 'input_runs'.
  /// Retrieves the first batch from each run and sets up the binary heap implementing
  /// the priority queue.
  Status Prepare(const std::vector<RunBatchSupplier>& input_runs);

  /// Return the next batch of sorted rows from this merger.
  Status GetNext(RowBatch* output_batch, bool* eos);

  /// Called to finalize a merge when deep_copy is false. Transfers resources from
  /// all input batches to the specified output batch.
  void TransferAllResources(RowBatch* transfer_resource_batch);

 private:
  class BatchedRowSupplier;

  /// Assuming the element at parent_index is the only out of place element in the heap,
  /// restore the heap property (i.e. swap elements so parent <= children).
  void Heapify(int parent_index);

  /// The binary min-heap used to merge rows from the sorted input runs. Since the heap is
  /// stored in a 0-indexed array, the 0-th element is the minimum element in the heap,
  /// and the children of the element at index i are 2*i+1 and 2*i+2. The heap property is
  /// that row of the parent element is <= the rows of the child elements according to the
  /// comparator compare_less_than_.
  /// The BatchedRowSupplier objects used in the min_heap_ are owned by this
  /// SortedRunMerger instance.
  std::vector<BatchedRowSupplier*> min_heap_;

  /// Row comparator. Returns true if lhs < rhs.
  TupleRowComparator compare_less_than_;

  /// Descriptor for the rows provided by the input runs. Owned by the exec-node through
  /// which this merger was created.
  RowDescriptor* input_row_desc_;

  /// True if rows must be deep copied into the output batch.
  bool deep_copy_input_;

  /// Pool of BatchedRowSupplier instances.
  ObjectPool pool_;

  /// Times calls to GetNext().
  RuntimeProfile::Counter* get_next_timer_;

  /// Times calls to get the next batch of rows from the input run.
  RuntimeProfile::Counter* get_next_batch_timer_;
};

}

#endif
