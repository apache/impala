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

#include "runtime/bufferpool/buffer-pool.h"
#include "util/runtime-profile.h"
#include "util/tuple-row-compare.h"
#include "runtime/sorted-run-merger.h"

namespace impala {

class SortedRunMerger;
class RowBatch;

/// Sorter contains the external sort implementation. Its purpose is to sort arbitrarily
/// large input data sets with a fixed memory budget by spilling data to disk if
/// necessary.
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
/// Batches of input rows are collected into a sequence of pinned BufferPool pages
/// called a run. The maximum size of a run is determined by the number of pages that
/// can be pinned by the Sorter. After the run is full, it is sorted in memory, unpinned
/// and the next run is constructed. The variable-length column data (e.g. string slots)
/// in the materialized sort tuples are stored in a separate sequence of pages from the
/// tuples themselves.  When the pages containing tuples in a run are unpinned, the
/// var-len slot pointers are converted to offsets from the start of the first var-len
/// data page. When a page is read back, these offsets are converted back to pointers.
/// The in-memory sorter sorts the fixed-length tuples in-place. The output rows have the
/// same schema as the materialized sort tuples.
//
/// After the input is consumed, the sorter is left with one or more sorted runs. If
/// there are multiple runs, the runs are merged using SortedRunMerger. At least one
/// page per run (two if there are var-length slots) must be pinned in memory during
/// a merge, so multiple merges may be necessary if the number of runs is too large.
/// First a series of intermediate merges are performed, until the number of runs is
/// small enough to do a single final merge that returns batches of sorted rows to the
/// caller of GetNext().
///
/// If there is a single sorted run (i.e. no merge required), only tuple rows are
/// copied into the output batch supplied by GetNext(), and the data itself is left in
/// pinned pages held by the sorter.
///
/// When merges are performed, one input batch is created to hold tuple rows for each
/// input run, and one batch is created to hold deep copied rows (i.e. ptrs + data) from
/// the output of the merge.
//
/// Note that Prepare() must be called right after the constructor.
//
/// During a merge, one row batch is created for each input run, and one batch is created
/// for the output of the merge (if is not the final merge). It is assumed that the memory
/// for these batches have already been accounted for in the memory budget for the sort.
/// That is, the memory for these batches does not come out of the buffer pool.
//
/// TODO: Not necessary to actually copy var-len data - instead take ownership of the
/// var-length data in the input batch. Copying can be deferred until a run is unpinned.
/// TODO: When the first run is constructed, create a sequence of pointers to materialized
/// tuples. If the input fits in memory, the pointers can be sorted instead of sorting the
/// tuples in place.
class Sorter {
 public:

  class TupleSorter;
  class TupleIterator;
  typedef Status (*SortHelperFn)(TupleSorter*, TupleIterator, TupleIterator);

  /// 'sort_tuple_exprs' are the slot exprs used to materialize the tuples to be
  /// sorted.
  /// 'tuple_row_comparator_config' is used to create the comparator for the sort tuples.
  /// 'node_label' is the label of the exec node using the sorter for error reporting.
  /// 'enable_spilling' should be set to false to reduce the number of requested buffers
  /// if the caller will use AddBatchNoSpill().
  /// 'codegend_sort_helper_fn' is a reference to the codegen version of
  /// the Sorter::TupleSorter::SortHelper() method.
  /// 'estimated_input_size' is the total rows in bytes that are estimated to get added
  /// into this sorter. This is used to decide if sorter needs to proactively spill for
  /// the first run. -1 value means estimate is unavailable.
  ///
  /// The Sorter assumes that it has exclusive use of the client's
  /// reservations for sorting, and may increase the size of the client's reservation.
  /// The caller is responsible for ensuring that the minimum reservation (returned from
  /// ComputeMinReservation()) is available.
  Sorter(const TupleRowComparatorConfig& tuple_row_comparator_config,
      const std::vector<ScalarExpr*>& sort_tuple_exprs, RowDescriptor* output_row_desc,
      MemTracker* mem_tracker, BufferPool::ClientHandle* client, int64_t page_len,
      RuntimeProfile* profile, RuntimeState* state, const std::string& node_label,
      bool enable_spilling,
      const CodegenFnPtr<SortHelperFn>& codegend_sort_helper_fn,
      const CodegenFnPtr<SortedRunMerger::HeapifyHelperFn>& codegend_heapify_helper_fn =
            default_heapify_helper_fn_,
      int64_t estimated_input_size = -1);
  ~Sorter();

  /// Initial set-up of the sorter for execution.
  /// The evaluators for 'sort_tuple_exprs_' will be created and stored in 'obj_pool'.
  Status Prepare(ObjectPool* obj_pool) WARN_UNUSED_RESULT;

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

  /// Resets all internal state like ExecNode::Reset().
  /// Prepare() must have been called, AddBatch()/GetNext()/InputDone()
  /// may or may not have been called.
  void Reset();

  /// Close the Sorter and free resources.
  void Close(RuntimeState* state);

  /// Compute the minimum amount of buffer memory in bytes required to execute a
  /// sort with the current sorter. Must be kept in sync with
  /// SortNode.computeNodeResourceProfile() in fe.
  int64_t ComputeMinReservation() const;

  /// Return true if the sorter has any spilled runs.
  bool HasSpilledRuns() const;

  /// The logic in AddBatchNoSpill() that handles the different cases when
  /// AddBatchInternal() returns without processing the entire batch.
  /// In this case we need to initialize a new minirun for the incoming rows.
  /// Tries initializing a new minirun. If the initialization was successful,
  /// allocation_failed is set to false, otherwise (e.g. a memory limit is hit) true.
  Status InitializeNewMinirun(bool* allocation_failed) WARN_UNUSED_RESULT;

 private:
  class Page;
  class Run;

  /// Minimum value for sort_run_bytes_limit query option.
  static const int64_t MIN_SORT_RUN_BYTES_LIMIT = 32 << 20; // 32 MB

  /// Merges multiple smaller runs in sorted_inmem_runs_ into a single larger merged
  /// run that can be spilled page by page during the process, until all pages from
  /// sorted_inmem_runs are consumed and freed.
  /// Always performs one-level merge and spills the output run to disc. Therefore there
  /// is no need to deep-copy the input, since the pages containing var-len data
  /// remain in the memory until the end of the in-memory merge.
  /// TODO: delete pages that are consumed during merge asap.
  Status MergeInMemoryRuns() WARN_UNUSED_RESULT;

  /// Create a SortedRunMerger from sorted runs in 'sorted_runs_' and assign it to
  /// 'merger_'. 'num_runs' indicates how many runs should be covered by the current
  /// merging attempt. Returns error if memory allocation fails in Run::PrepareRead().
  /// The runs to be merged are removed from 'sorted_runs_'.
  /// If 'external' is set to true, it performs an external merge, and the
  /// Sorter sets the 'deep_copy_input' flag to true for the merger, since the pages
  /// containing input run data will be deleted as input runs are read.
  /// If 'external' is false, it creates an in-memory merger for the in-memory miniruns.
  /// In this case, 'deep_copy_input' is set to false.
  Status CreateMerger(int num_runs, bool external) WARN_UNUSED_RESULT;

  /// Repeatedly replaces multiple smaller runs in sorted_runs_ with a single larger
  /// merged run until there are few enough runs to be merged with a single merger.
  /// Returns when 'merger_' is set up to merge the final runs. If the number of sorted
  /// runs is too large, merge sets of smaller runs into large runs until a final merge
  /// can be performed. An intermediate row batch containing deep copied rows is used for
  /// the output of each intermediate merge.
  Status MergeIntermediateRuns() WARN_UNUSED_RESULT;

  /// Execute a single step of the intermediate merge, pulling rows from 'merger_'
  /// and adding them to 'merged_run'.
  Status ExecuteIntermediateMerge(Sorter::Run* merged_run) WARN_UNUSED_RESULT;

  /// Handles cases in AddBatch where a memory limit is reached and spilling must start.
  /// Used only in case of multiple in-memory runs set by query option MAX_SORT_RUN_SIZE.
  inline Status MergeAndSpill() WARN_UNUSED_RESULT;

  /// Called once there no more rows to be added to 'unsorted_run_'. Sorts
  /// 'unsorted_run_' and appends it to the list of sorted runs.
  Status SortCurrentInputRun() WARN_UNUSED_RESULT;

  /// Helper that cleans up all runs in the sorter.
  void CleanupAllRuns();

  /// Based on the amount of unused buffers the Sorter has through the BufferPool this
  /// function calculates the maximum number of runs that can be taken care of during the
  /// next merge intermediate merge. Takes into account that a separate run is needed for
  /// the output.
  int MaxRunsInNextMerge() const;

  /// Calculates the number of runs the 'merger_' should grab for merging in the current
  /// round of merging. Returns at most MaxRunsInNextMerge(), so the Sorter will have
  /// enough reservation to merge this number of runs.
  int GetNumOfRunsForMerge() const;

  /// If the number of available buffers is not enough to grab all the runs to merge in
  /// one round then this functions starts allocating additional free buffers one by one
  /// until it reaches the maximum limit the Sorter can have or until we have enough free
  /// buffers for all the runs. This is possible if other operators have released memory
  /// since the Sorter has started working on it's initial runs.
  void TryToIncreaseMemAllocationForMerge();

  /// Given 'estimated_input_size', compute whether we most likely going to spill or not.
  void ComputeSpillEstimate(int64_t estimated_input_size);

  /// Check if we should enable enforce_sort_run_bytes_limit_ flag.
  void CheckSortRunBytesLimitEnforcement();

  /// Get value of sort_run_bytes_limit query option. If user specifies value between
  /// [1,MIN_SORT_RUN_BYTES_LIMIT], it will be silently overridden by
  /// MIN_SORT_RUN_BYTES_LIMIT. Otherwise, return sort_run_bytes_limit from state_.
  int64_t GetSortRunBytesLimit() const;

  /// Check if it is necessary to do an intermediate run and spill the sorted run.
  /// There are two cases where it is necessary to do an intermediate run.
  /// 1) Number of rows added to a run is less than total number of rows in RowBatch,
  ///    indicating that mem_limit has been reached.
  /// 2) enforce_sort_run_bytes_limit_ is true and current used memory is greater than or
  ///    equal to specified sort_run_bytes_limit.
  bool MustSortAndSpill(const int rows_added, const int batch_num_rows);

  /// Try to lower current memory reservation up to sort_run_bytes_limit.
  void TryLowerMemUpToSortRunBytesLimit();

  /// Label of the ExecNode that owns the sorter, used for error reporting.
  const std::string node_label_;

  /// Runtime state instance used to check for cancellation. Not owned.
  RuntimeState* const state_;

  /// MemPool for allocating data structures used by expression evaluators in the sorter.
  MemPool expr_perm_pool_;

  /// MemPool for allocations that hold results of expression evaluation in the sorter.
  /// Cleared periodically during sorting to prevent memory accumulating.
  MemPool expr_results_pool_;

  /// In memory sorter and less-than comparator.
  boost::scoped_ptr<TupleRowComparator> compare_less_than_;
  boost::scoped_ptr<TupleSorter> in_mem_tuple_sorter_;

  /// A reference to the codegened version of TupleSorter::SortHelper() that is stored
  /// inside SortPlanNode, PartialSortPlanNode and TopNPlanNode.
  const CodegenFnPtr<SortHelperFn>& codegend_sort_helper_fn_;

  /// A reference to the codegened version of SortedRunMerger::HeapifyHelper() that is
  /// stored inside SortPlanNode and ExchangePlanNode.
  const CodegenFnPtr<SortedRunMerger::HeapifyHelperFn>& codegend_heapify_helper_fn_;

  /// A default codegened function pointer storing nullptr, which is used when the
  /// merger is not needed. Used as a default value in the constructor, when the
  /// CodegenFnPtr is not provided.
  static const CodegenFnPtr<SortedRunMerger::HeapifyHelperFn> default_heapify_helper_fn_;

  /// Client used to allocate pages from the buffer pool. Not owned.
  BufferPool::ClientHandle* const buffer_pool_client_;

  /// The length of page to use.
  const int64_t page_len_;

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
  /// sorted_runs_ after it is full (i.e. number of pages allocated == max available
  /// buffers) or after the input is complete. Owned and placed in obj_pool_.
  /// When it is added to sorted_runs_, it is set to NULL.
  Run* unsorted_run_;

  /// List of quicksorted miniruns before merging in memory.
  std::deque<Run*> sorted_inmem_runs_;

  /// List of sorted runs that have been produced but not merged. 'unsorted_run_' is
  /// added to this list after an in-memory sort. Sorted runs produced by intermediate
  /// merges are also added to this list during the merge. Runs are added to the
  /// object pool.
  std::deque<Run*> sorted_runs_;

  /// Merger object (intermediate or final) currently used to produce sorted runs.
  /// Only one merge is performed at a time. Will never be used if the input fits in
  /// memory.
  boost::scoped_ptr<SortedRunMerger> merger_;

  /// Spilled runs that are currently processed by the merge_.
  /// These runs can be deleted when we are done with the current merge.
  std::deque<Run*> merging_runs_;

  /// Output run for the merge. Stored in Sorter() so that it can be cleaned up
  /// in Sorter::Close() in case of errors.
  Run* merge_output_run_;

  /// Pool of owned Run objects. Maintains Runs objects across non-freeing Reset() calls.
  ObjectPool run_pool_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// Runtime profile and counters for this sorter instance.
  RuntimeProfile* profile_;

  /// Pool of objects (e.g. exprs) that are not freed during Reset() calls.
  ObjectPool obj_pool_;

  /// Number of initial runs created.
  RuntimeProfile::Counter* initial_runs_counter_;

  /// Number of runs that were unpinned and may have spilled to disk, including initial
  /// and intermediate runs.
  RuntimeProfile::Counter* spilled_runs_counter_;

  /// Number of merges of sorted runs.
  RuntimeProfile::Counter* num_merges_counter_;

  /// Time spent sorting initial runs in memory.
  RuntimeProfile::Counter* in_mem_sort_timer_;

  /// Time spent merging initial miniruns in memory.
  RuntimeProfile::Counter* in_mem_merge_timer_;

  /// Total size of the initial runs in bytes.
  RuntimeProfile::Counter* sorted_data_size_;

  /// Min, max, and avg size of runs in number of tuples.
  RuntimeProfile::SummaryStatsCounter* run_sizes_;

  /// Flag to enforce sort_run_bytes_limit.
  bool enforce_sort_run_bytes_limit_ = false;

  /// Maximum number of fixed-length + variable-length pages in an in-memory run set by
  /// Query Option MAX_SORT_RUN_SIZE.
  /// The default value is 0 which means that only 1 in-memory run will be created, and
  /// its size will be determined by other limits eg. memory or sort_run_bytes_limit.
  int inmem_run_max_pages_ = 0;
};

} // namespace impala

#endif
