// Copyright 2013 Cloudera Inc.
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

#ifndef IMPALA_RUNTIME_SORTER_H_
#define IMPALA_RUNTIME_SORTER_H_

#include <deque>

#include "runtime/buffered-block-mgr.h"
#include "util/tuple-row-compare.h"

namespace impala {

class SortedRunMerger;
class RuntimeProfile;
class RowBatch;

/// Sorter contains the external sort implementation. Its purpose is to sort arbitrarily
/// large input data sets with a fixed memory budget by spilling data to disk if
/// necessary. BufferedBlockMgr is used to allocate and manage blocks of data to be
/// sorted.
//
/// The client API for Sorter is as follows:
/// AddBatch() is used to add input rows to be sorted. Multiple tuples in an input row are
/// materialized into a row with a single tuple (the sort tuple) using the materialization
/// exprs in sort_tuple_slot_expr_ctxs_. The sort tuples are sorted according to the sort
/// parameters and output by the sorter.
/// AddBatch() can be called multiple times.
//
/// InputDone() is called to indicate the end of input. If multiple sorted runs were
/// created, it triggers intermediate merge steps (if necessary) and creates the final
/// merger that returns results via GetNext().
//
/// GetNext() is used to retrieve sorted rows. It can be called multiple times.
/// AddBatch(), InputDone() and GetNext() must be called in that order.
//
/// Batches of input rows are collected into a sequence of pinned BufferedBlockMgr blocks
/// called a run. The maximum size of a run is determined by the maximum available buffers
/// in the block manager. After the run is full, it is sorted in memory, unpinned and the
/// next run is collected.  The variable-length column data (e.g. string slots) in the
/// materialized sort tuples are stored in separate sequence of blocks from the tuples
/// themselves.
/// When the blocks containing tuples in a run are unpinned, the var-len slot pointers are
/// converted to offsets from the start of the first var-len data block. When a block is
/// read back, these offsets are converted back to pointers.
/// The in-memory sorter sorts the fixed-length tuples in-place. The output rows have the
/// same schema as the materialized sort tuples.
//
/// After the input is consumed, the sorter is left with one or more sorted runs. The
/// client calls GetNext(output_batch) to retrieve batches of sorted rows. If there are
/// multiple runs, the runs are merged using SortedRunMerger to produce a stream of sorted
/// tuples. At least one block per run (two if there are var-length slots) must be pinned
/// in memory during a merge, so multiple merges may be necessary if the number of runs is
/// too large. During a merge, rows from multiple sorted input runs are compared and copied
/// into a single larger run. One input batch is created to hold tuple rows for each
/// input run, and one batch is created to hold deep copied rows (i.e. ptrs + data) from
/// the output of the merge.
//
/// If there is a single sorted run (i.e. no merge required), only tuple rows are
/// copied into the output batch supplied by GetNext, and the data itself is left in
/// pinned blocks held by the sorter.
//
/// Note that Init() must be called right after the constructor.
//
/// During a merge, one row batch is created for each input run, and one batch is created
/// for the output of the merge (if is not the final merge). It is assumed that the memory
/// for these batches have already been accounted for in the memory budget for the sort.
/// That is, the memory for these batches does not come out of the block buffer manager.
//
/// TODO: Not necessary to actually copy var-len data - instead take ownership of the
/// var-length data in the input batch. Copying can be deferred until a run is unpinned.
/// TODO: When the first run is constructed, create a sequence of pointers to materialized
/// tuples. If the input fits in memory, the pointers can be sorted instead of sorting the
/// tuples in place.
class Sorter {
 public:
  /// sort_tuple_slot_exprs are the slot exprs used to materialize the tuple to be sorted.
  /// compare_less_than is a comparator for the sort tuples (returns true if lhs < rhs).
  /// merge_batch_size_ is the size of the batches created to provide rows to the merger
  /// and retrieve rows from an intermediate merger.
  Sorter(const TupleRowComparator& compare_less_than,
      const std::vector<ExprContext*>& sort_tuple_slot_expr_ctxs,
      RowDescriptor* output_row_desc, MemTracker* mem_tracker,
      RuntimeProfile* profile, RuntimeState* state);

  ~Sorter();

  /// Initialization code, including registration to the block_mgr and the initialization
  /// of the unsorted_run_, both of these may fail.
  Status Init();

  /// Adds a batch of input rows to the current unsorted run.
  Status AddBatch(RowBatch* batch);

  /// Called to indicate there is no more input. Triggers the creation of merger(s) if
  /// necessary.
  Status InputDone();

  /// Get the next batch of sorted output rows from the sorter.
  Status GetNext(RowBatch* batch, bool* eos);

  /// Resets all internal state like ExecNode::Reset().
  /// Init() must have been called, AddBatch()/GetNext()/InputDone()
  /// may or may not have been called.
  Status Reset();

  /// Estimate the memory overhead in bytes for an intermediate merge, based on the
  /// maximum number of memory buffers available for the sort, the row descriptor for
  /// the sorted tuples and the batch size used (in rows).
  /// This is a pessimistic estimate of the memory needed by the sorter in addition to the
  /// memory used by the block buffer manager. The memory overhead is 0 if the input fits
  /// in memory. Merges incur additional memory overhead because row batches are created
  /// to hold tuple rows from the input runs, and the merger itself deep-copies
  /// sort-merged rows into its output batch.
  static uint64_t EstimateMergeMem(uint64_t available_blocks, RowDescriptor* row_desc,
      int merge_batch_size);

 private:
  class Run;
  class TupleSorter;

  /// Create a SortedRunMerger from the first 'num_runs' sorted runs in sorted_runs_ and
  /// assign it to merger_. The runs to be merged are removed from sorted_runs_.
  /// The Sorter sets the deep_copy_input flag to true for the merger, since the blocks
  /// containing input run data will be unpinned as input runs are read.
  Status CreateMerger(int num_runs);

  /// Repeatedly replaces multiple smaller runs in sorted_runs_ with a single larger
  /// merged run until the number of remaining runs is small enough for a single merge.
  /// At least 1 (2 if var-len slots) block from each sorted run must be pinned for
  /// a merge. If the number of sorted runs is too large, merge sets of smaller runs
  /// into large runs until a final merge can be performed. An intermediate row batch
  /// containing deep copied rows is used for the output of each intermediate merge.
  Status MergeIntermediateRuns();

  /// Sorts unsorted_run_ and appends it to the list of sorted runs. Deletes any empty
  /// blocks at the end of the run. Updates the sort bytes counter if necessary.
  Status SortRun();

  /// Runtime state instance used to check for cancellation. Not owned.
  RuntimeState* const state_;

  /// In memory sorter and less-than comparator.
  TupleRowComparator compare_less_than_;
  boost::scoped_ptr<TupleSorter> in_mem_tuple_sorter_;

  /// Block manager object used to allocate, pin and release runs. Not owned by Sorter.
  BufferedBlockMgr* block_mgr_;

  /// Handle to block mgr to make allocations from.
  BufferedBlockMgr::Client* block_mgr_client_;

  /// True if the tuples to be sorted have var-length slots.
  bool has_var_len_slots_;

  /// Expressions used to materialize the sort tuple. Contains one expr per slot in the
  /// tuple.
  std::vector<ExprContext*> sort_tuple_slot_expr_ctxs_;

  /// Mem tracker for batches created during merge. Not owned by Sorter.
  MemTracker* mem_tracker_;

  /// Descriptor for the sort tuple. Input rows are materialized into 1 tuple before
  /// sorting. Not owned by the Sorter.
  RowDescriptor* output_row_desc_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// The current unsorted run that is being collected. Is sorted and added to
  /// sorted_runs_ after it is full (i.e. number of blocks allocated == max available
  /// buffers) or after the input is complete. Owned and placed in obj_pool_.
  /// When it is added to sorted_runs_, it is set to NULL.
  Run* unsorted_run_;

  /// List of sorted runs that have been produced but not merged. unsorted_run_ is added
  /// to this list after an in-memory sort. Sorted runs produced by intermediate merges
  /// are also added to this list. Runs are added to the object pool.
  std::deque<Run*> sorted_runs_;

  /// Merger object (intermediate or final) currently used to produce sorted runs.
  /// Only one merge is performed at a time. Will never be used if the input fits in
  /// memory.
  boost::scoped_ptr<SortedRunMerger> merger_;

  /// Runs that are currently processed by the merge_.
  /// These runs can be deleted when we are done with the current merge.
  std::deque<Run*> merging_runs_;

  /// Pool of owned Run objects. Maintains Runs objects across non-freeing Reset() calls.
  ObjectPool obj_pool_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// Runtime profile and counters for this sorter instance.
  RuntimeProfile* profile_;
  RuntimeProfile::Counter* initial_runs_counter_;
  RuntimeProfile::Counter* num_merges_counter_;
  RuntimeProfile::Counter* in_mem_sort_timer_;
  RuntimeProfile::Counter* sorted_data_size_;
};

} // namespace impala

#endif
