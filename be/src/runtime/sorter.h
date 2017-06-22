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
/// exprs in sort_tuple_exprs_. The sort tuples are sorted according to the sort
/// parameters and output by the sorter. AddBatch() can be called multiple times.
//
/// Callers that don't want to spill can use AddBatchNoSpill() instead, which only adds
/// rows up to the memory limit and then returns the number of rows that were added.
/// For this use case, 'enable_spill' should be set to false so that the sorter can reduce
/// the number of buffers requested from the block mgr since there won't be merges.
//
/// InputDone() is called to indicate the end of input. If multiple sorted runs were
/// created, it triggers intermediate merge steps (if necessary) and creates the final
/// merger that returns results via GetNext().
//
/// GetNext() is used to retrieve sorted rows. It can be called multiple times.
/// AddBatch()/AddBatchNoSpill(), InputDone() and GetNext() must be called in that order.
//
/// Batches of input rows are collected into a sequence of pinned BufferedBlockMgr blocks
/// called a run. The maximum size of a run is determined by the number of blocks that
/// can be pinned by the Sorter. After the run is full, it is sorted in memory, unpinned
/// and the next run is constructed. The variable-length column data (e.g. string slots)
/// in the materialized sort tuples are stored in a separate sequence of blocks from the
/// tuples themselves.  When the blocks containing tuples in a run are unpinned, the
/// var-len slot pointers are converted to offsets from the start of the first var-len
/// data block. When a block is read back, these offsets are converted back to pointers.
/// The in-memory sorter sorts the fixed-length tuples in-place. The output rows have the
/// same schema as the materialized sort tuples.
//
/// After the input is consumed, the sorter is left with one or more sorted runs. If
/// there are multiple runs, the runs are merged using SortedRunMerger. At least one
/// block per run (two if there are var-length slots) must be pinned in memory during
/// a merge, so multiple merges may be necessary if the number of runs is too large.
/// First a series of intermediate merges are performed, until the number of runs is
/// small enough to do a single final merge that returns batches of sorted rows to the
/// caller of GetNext().
///
/// If there is a single sorted run (i.e. no merge required), only tuple rows are
/// copied into the output batch supplied by GetNext(), and the data itself is left in
/// pinned blocks held by the sorter.
///
/// When merges are performed, one input batch is created to hold tuple rows for each
/// input run, and one batch is created to hold deep copied rows (i.e. ptrs + data) from
/// the output of the merge.
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
  /// 'sort_tuple_exprs' are the slot exprs used to materialize the tuples to be
  /// sorted. 'compare_less_than' is a comparator for the sort tuples (returns true if
  /// lhs < rhs). 'merge_batch_size_' is the size of the batches created to provide rows
  /// to the merger and retrieve rows from an intermediate merger. 'enable_spilling'
  /// should be set to false to reduce the number of requested buffers if the caller will
  /// use AddBatchNoSpill().
  Sorter(const TupleRowComparator& compare_less_than,
      const std::vector<ScalarExpr*>& sort_tuple_exprs, RowDescriptor* output_row_desc,
      MemTracker* mem_tracker, RuntimeProfile* profile, RuntimeState* state,
      bool enable_spilling = true);

  ~Sorter();

  /// Initial set-up of the sorter for execution. Registers with the block mgr.
  /// The evaluators for 'sort_tuple_exprs_' will be created and stored in 'obj_pool'.
  /// All allocation from the evaluators will be from 'expr_mem_pool'.
  Status Prepare(ObjectPool* obj_pool, MemPool* expr_mem_pool) WARN_UNUSED_RESULT;

  /// Opens the sorter for adding rows and initializes the evaluators for materializing
  /// the tuples. Must be called after Prepare() or Reset() and before calling AddBatch().
  Status Open() WARN_UNUSED_RESULT;

  /// Adds the entire batch of input rows to the sorter. If the current unsorted run fills
  /// up, it is sorted and a new unsorted run is created. Cannot be called if
  /// 'enable_spill' is false.
  Status AddBatch(RowBatch* batch) WARN_UNUSED_RESULT;

  /// Adds input rows to the current unsorted run, starting from 'start_index' up to the
  /// memory limit. Returns the number of rows added in 'num_processed'.
  Status AddBatchNoSpill(
      RowBatch* batch, int start_index, int* num_processed) WARN_UNUSED_RESULT;

  /// Called to indicate there is no more input. Triggers the creation of merger(s) if
  /// necessary.
  Status InputDone() WARN_UNUSED_RESULT;

  /// Get the next batch of sorted output rows from the sorter.
  Status GetNext(RowBatch* batch, bool* eos) WARN_UNUSED_RESULT;

  /// Free any local allocations made when materializing and sorting the tuples.
  void FreeLocalAllocations();

  /// Resets all internal state like ExecNode::Reset().
  /// Init() must have been called, AddBatch()/GetNext()/InputDone()
  /// may or may not have been called.
  void Reset();

  /// Close the Sorter and free resources.
  void Close(RuntimeState* state);

 private:
  class Run;
  class TupleIterator;
  class TupleSorter;

  /// Create a SortedRunMerger from sorted runs in 'sorted_runs_' and assign it to
  /// 'merger_'. Attempts to set up merger with 'max_num_runs' runs but may set it
  /// up with fewer if it cannot pin the initial blocks of all of the runs. Fails
  /// if it cannot merge at least two runs. The runs to be merged are removed from
  /// 'sorted_runs_'.  The Sorter sets the 'deep_copy_input' flag to true for the
  /// merger, since the blocks containing input run data will be deleted as input
  /// runs are read.
  Status CreateMerger(int max_num_runs) WARN_UNUSED_RESULT;

  /// Repeatedly replaces multiple smaller runs in sorted_runs_ with a single larger
  /// merged run until there are few enough runs to be merged with a single merger.
  /// Returns when 'merger_' is set up to merge the final runs.
  /// At least 1 (2 if var-len slots) block from each sorted run must be pinned for
  /// a merge. If the number of sorted runs is too large, merge sets of smaller runs
  /// into large runs until a final merge can be performed. An intermediate row batch
  /// containing deep copied rows is used for the output of each intermediate merge.
  Status MergeIntermediateRuns() WARN_UNUSED_RESULT;

  /// Execute a single step of the intermediate merge, pulling rows from 'merger_'
  /// and adding them to 'merged_run'.
  Status ExecuteIntermediateMerge(Sorter::Run* merged_run) WARN_UNUSED_RESULT;

  /// Called once there no more rows to be added to 'unsorted_run_'. Sorts
  /// 'unsorted_run_' and appends it to the list of sorted runs.
  Status SortCurrentInputRun() WARN_UNUSED_RESULT;

  /// Helper that cleans up all runs in the sorter.
  void CleanupAllRuns();

  /// Runtime state instance used to check for cancellation. Not owned.
  RuntimeState* const state_;

  /// In memory sorter and less-than comparator.
  const TupleRowComparator& compare_less_than_;
  boost::scoped_ptr<TupleSorter> in_mem_tuple_sorter_;

  /// Block manager object used to allocate, pin and release runs. Not owned by Sorter.
  BufferedBlockMgr* block_mgr_;

  /// Handle to block mgr to make allocations from.
  BufferedBlockMgr::Client* block_mgr_client_;

  /// True if the tuples to be sorted have var-length slots.
  bool has_var_len_slots_;

  /// Expressions used to materialize the sort tuple. One expr per slot in the tuple.
  const std::vector<ScalarExpr*>& sort_tuple_exprs_;
  std::vector<ScalarExprEvaluator*> sort_tuple_expr_evals_;

  /// Mem tracker for batches created during merge. Not owned by Sorter.
  MemTracker* mem_tracker_;

  /// Descriptor for the sort tuple. Input rows are materialized into 1 tuple before
  /// sorting. Not owned by the Sorter.
  RowDescriptor* output_row_desc_;

  /// True if this sorter can spill. Used to determine the number of buffers to reserve.
  bool enable_spilling_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// The current unsorted run that is being collected. Is sorted and added to
  /// sorted_runs_ after it is full (i.e. number of blocks allocated == max available
  /// buffers) or after the input is complete. Owned and placed in obj_pool_.
  /// When it is added to sorted_runs_, it is set to NULL.
  Run* unsorted_run_;

  /// List of sorted runs that have been produced but not merged. unsorted_run_ is added
  /// to this list after an in-memory sort. Sorted runs produced by intermediate merges
  /// are also added to this list during the merge. Runs are added to the object pool.
  std::deque<Run*> sorted_runs_;

  /// Merger object (intermediate or final) currently used to produce sorted runs.
  /// Only one merge is performed at a time. Will never be used if the input fits in
  /// memory.
  boost::scoped_ptr<SortedRunMerger> merger_;

  /// Runs that are currently processed by the merge_.
  /// These runs can be deleted when we are done with the current merge.
  std::deque<Run*> merging_runs_;

  /// Output run for the merge. Stored in Sorter() so that it can be cleaned up
  /// in Sorter::Close() in case of errors.
  Run* merge_output_run_;

  /// Pool of owned Run objects. Maintains Runs objects across non-freeing Reset() calls.
  ObjectPool obj_pool_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// Runtime profile and counters for this sorter instance.
  RuntimeProfile* profile_;

  /// Number of initial runs created.
  RuntimeProfile::Counter* initial_runs_counter_;

  /// Number of runs that were unpinned and may have spilled to disk, including initial
  /// and intermediate runs.
  RuntimeProfile::Counter* spilled_runs_counter_;

  /// Number of merges of sorted runs.
  RuntimeProfile::Counter* num_merges_counter_;

  /// Time spent sorting initial runs in memory.
  RuntimeProfile::Counter* in_mem_sort_timer_;

  /// Total size of the initial runs in bytes.
  RuntimeProfile::Counter* sorted_data_size_;

  /// Min, max, and avg size of runs in number of tuples.
  RuntimeProfile::SummaryStatsCounter* run_sizes_;
};

} // namespace impala

#endif
