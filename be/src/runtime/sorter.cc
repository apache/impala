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

#include "runtime/sorter.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <gutil/strings/substitute.h>

#include "runtime/buffered-block-mgr.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/sorted-run-merger.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using boost::uniform_int;
using boost::mt19937_64;
using namespace strings;

namespace impala {

// Number of pinned blocks required for a merge with fixed-length data only.
const int MIN_BUFFERS_PER_MERGE = 3;

// Maximum number of buffers to use in each merge to prevent sorter trying to grab
// all the memory when spilling. Given 8mb buffers, this limits the sorter to using
// 1GB of buffers when merging.
// TODO: this is an arbitrary limit. Once we have reliable reservations (IMPALA-3200)
// we should base this on the number of reservations.
const int MAX_BUFFERS_PER_MERGE = 128;

const string MEM_ALLOC_FAILED_ERROR_MSG = "Failed to allocate block for $0-length "
    "data needed for sorting. Reducing query concurrency or increasing the "
    "memory limit may help this query to complete successfully.";

const string MERGE_FAILED_ERROR_MSG = "Failed to allocate block to merge spilled runs "
    "during sorting. Only $0 runs could be merged, but must be able to merge at least 2 "
    "to make progress. Reducing query concurrency or increasing the memory limit may "
    "help this query to complete successfully.";

/// Delete all non-null blocks in blocks and clear vector.
static void DeleteAndClearBlocks(vector<BufferedBlockMgr::Block*>* blocks) {
  for (BufferedBlockMgr::Block* block: *blocks) {
    if (block != NULL) block->Delete();
  }
  blocks->clear();
}

static int NumNonNullBlocks(const vector<BufferedBlockMgr::Block*>& blocks) {
  int count = 0;
  for (BufferedBlockMgr::Block* block: blocks) {
    if (block != NULL) ++count;
  }
  return count;
}

/// A run is a sequence of tuples. The run can be sorted or unsorted (in which case the
/// Sorter will sort it). A run comprises a sequence of fixed-length blocks containing the
/// tuples themselves (i.e. fixed-len slots that may contain ptrs to var-length data), and
/// an optional sequence of var-length blocks containing the var-length data.
///
/// Runs are either "initial runs" constructed from the sorter's input by evaluating
/// the expressions in 'sort_tuple_slot_expr_ctxs_' or "intermediate runs" constructed
/// by merging already-sorted runs. Initial runs are sorted in-place in memory. Once
/// sorted, runs can be spilled to disk to free up memory. Sorted runs are merged by
/// SortedRunMerger, either to produce the final sorted output or to produce another
/// sorted run.
///
/// The expected calling sequence of functions is as follows:
/// * Init() to initialize the run and allocate initial blocks.
/// * Add*Batch() to add batches of tuples to the run.
/// * FinalizeInput() to signal that no more batches will be added.
/// * If the run is unsorted, it must be sorted. After that set_sorted() must be called.
/// * Once sorted, the run is ready to read in sorted order for merging or final output.
/// * PrepareRead() to allocate resources for reading the run.
/// * GetNext() (if there was a single run) or GetNextBatch() (when merging multiple runs)
///   to read from the run.
/// * Once reading is done, DeleteAllBlocks() should be called to free resources.
class Sorter::Run {
 public:
  Run(Sorter* parent, TupleDescriptor* sort_tuple_desc, bool initial_run);

  ~Run() {
    DCHECK(fixed_len_blocks_.empty());
    DCHECK(var_len_blocks_.empty());
    DCHECK(var_len_copy_block_ == NULL);
  }

  /// Initialize the run for input rows by allocating the minimum number of required
  /// blocks - one block for fixed-len data added to fixed_len_blocks_, one for the
  /// initially unsorted var-len data added to var_len_blocks_, and one to copy sorted
  /// var-len data into var_len_copy_block_.
  Status Init();

  /// Add the rows from 'batch' starting at 'start_index' to the current run. Returns the
  /// number of rows actually added in 'num_processed'. If the run is full (no more blocks
  /// can be allocated), 'num_processed' may be less than the number of remaining rows in
  /// the batch. AddInputBatch() materializes the input rows using the expressions in
  /// sorter_->sort_tuple_slot_expr_ctxs_, while AddIntermediateBatch() just copies rows.
  Status AddInputBatch(RowBatch* batch, int start_index, int* num_processed) {
    DCHECK(initial_run_);
    if (has_var_len_slots_) {
      return AddBatchInternal<true, true>(batch, start_index, num_processed);
    } else {
      return AddBatchInternal<false, true>(batch, start_index, num_processed);
    }
  }
  Status AddIntermediateBatch(RowBatch* batch, int start_index, int* num_processed) {
    DCHECK(!initial_run_);
    if (has_var_len_slots_) {
      return AddBatchInternal<true, false>(batch, start_index, num_processed);
    } else {
      return AddBatchInternal<false, false>(batch, start_index, num_processed);
    }
  }

  /// Called after the final call to Add*Batch() to do any bookkeeping necessary to
  /// finalize the run. Must be called before sorting or merging the run.
  Status FinalizeInput();

  /// Unpins all the blocks in a sorted run. Var-length column data is copied into new
  /// blocks in sorted order. Pointers in the original tuples are converted to offsets
  /// from the beginning of the sequence of var-len data blocks. Returns an error and
  /// may leave some blocks pinned if an error is encountered in the block mgr.
  Status UnpinAllBlocks();

  /// Deletes all blocks.
  void DeleteAllBlocks();

  /// Prepare to read a sorted run. Pins the first block(s) in the run if the run was
  /// previously unpinned. If the run was unpinned, try to pin the initial fixed and
  /// var len blocks in the run. If it couldn't pin them, set pinned_all_blocks to false.
  /// In that case, none or one of the initial blocks may be pinned and it is valid to
  /// call PrepareRead() again to retry pinning the remainder. pinned_all_blocks is
  /// always set to true if the run is pinned.
  Status PrepareRead(bool* pinned_all_blocks);

  /// Interface for merger - get the next batch of rows from this run. This run still
  /// owns the returned batch. Calls GetNext(RowBatch*, bool*).
  Status GetNextBatch(RowBatch** sorted_batch);

  /// Fill output_batch with rows from this run. If CONVERT_OFFSET_TO_PTR is true, offsets
  /// in var-length slots are converted back to pointers. Only row pointers are copied
  /// into output_batch. eos is set to true after all rows from the run are returned.
  /// If eos is true, the returned output_batch has zero rows and has no attached blocks.
  /// If this run was unpinned, one block (two if there are var-len slots) is pinned while
  /// rows are filled into output_batch. The block is unpinned before the next block is
  /// pinned, so at most one (two if there are var-len slots) block(s) will be pinned at
  /// once. If the run was pinned, the blocks are not unpinned and each block is attached
  /// to 'output_batch' once all rows referencing data in the block have been returned,
  /// either in the current batch or previous batches. In both pinned and unpinned cases,
  /// all rows in output_batch will reference at most one fixed-len and one var-len block.
  template <bool CONVERT_OFFSET_TO_PTR>
  Status GetNext(RowBatch* output_batch, bool* eos);

  /// Delete all blocks in 'runs' and clear 'runs'.
  static void CleanupRuns(deque<Run*>* runs) {
    for (Run* run: *runs) {
      run->DeleteAllBlocks();
    }
    runs->clear();
  }

  /// Return total amount of fixed and var len data in run, not including blocks that
  /// were already transferred.
  int64_t TotalBytes() const;

  inline bool is_pinned() const { return is_pinned_; }
  inline bool is_finalized() const { return is_finalized_; }
  inline bool is_sorted() const { return is_sorted_; }
  inline void set_sorted() { is_sorted_ = true; }
  inline int64_t num_tuples() const { return num_tuples_; }

 private:
  /// TupleIterator needs access to internals to iterate over tuples.
  friend class TupleIterator;

  /// Templatized implementation of Add*Batch() functions.
  /// INITIAL_RUN and HAS_VAR_LEN_SLOTS are template arguments for performance and must
  /// match 'initial_run_' and 'has_var_len_slots_'.
  template <bool HAS_VAR_LEN_SLOTS, bool INITIAL_RUN>
  Status AddBatchInternal(RowBatch* batch, int start_index, int* num_processed);

  /// Finalize the list of blocks: delete empty final blocks and unpin the previous block
  /// if the run is unpinned.
  Status FinalizeBlocks(vector<BufferedBlockMgr::Block*>* blocks);

  /// Collect the non-null var-len (e.g. STRING) slots from 'src' in 'var_len_values' and
  /// return the total length of all var-len values in 'total_var_len'.
  void CollectNonNullVarSlots(Tuple* src, vector<StringValue*>* var_len_values,
      int* total_var_len);

  enum AddBlockMode { KEEP_PREV_PINNED, UNPIN_PREV };

  /// Try to extend the current run by a block. If 'mode' is KEEP_PREV_PINNED, try to
  /// allocate a new block, which may fail to extend the run due to lack of memory. If
  /// mode is 'UNPIN_PREV', unpin the previous block in block_sequence before allocating
  /// and adding a new block - this never fails due to lack of memory.
  ///
  /// Returns an error status only if the block manager returns an error. If no error is
  /// encountered, sets 'added' to indicate whether the run was extended and returns
  /// Status::OK(). The new block is appended to 'block_sequence'.
  Status TryAddBlock(AddBlockMode mode, vector<BufferedBlockMgr::Block*>* block_sequence,
      bool* added);

  /// Advance to the next read block. If the run is pinned, has no effect. If the run
  /// is unpinned, atomically pin the block at 'block_index' + 1 in 'blocks' and delete
  /// the block at 'block_index'.
  Status PinNextReadBlock(vector<BufferedBlockMgr::Block*>* blocks, int block_index);

  /// Copy the StringValues in 'var_values' to 'dest' in order and update the StringValue
  /// ptrs in 'dest' to point to the copied data.
  void CopyVarLenData(const vector<StringValue*>& var_values, uint8_t* dest);

  /// Copy the StringValues in 'var_values' to 'dest' in order. Update the StringValue
  /// ptrs in 'dest' to contain a packed offset for the copied data comprising
  /// block_index and the offset relative to block_start.
  void CopyVarLenDataConvertOffset(const vector<StringValue*>& var_values,
      int block_index, const uint8_t* block_start, uint8_t* dest);

  /// Convert encoded offsets to valid pointers in tuple with layout 'sort_tuple_desc_'.
  /// 'tuple' is modified in-place. Returns true if the pointers refer to the block at
  /// 'var_len_blocks_index_' and were successfully converted or false if the var len
  /// data is in the next block, in which case 'tuple' is unmodified.
  bool ConvertOffsetsToPtrs(Tuple* tuple);

  /// Returns true if we have var-len blocks in the run.
  inline bool HasVarLenBlocks() const {
    // Shouldn't have any blocks unless there are slots.
    DCHECK(var_len_blocks_.empty() || has_var_len_slots_);
    return !var_len_blocks_.empty();
  }

  /// Parent sorter object.
  const Sorter* sorter_;

  /// Materialized sort tuple. Input rows are materialized into 1 tuple (with descriptor
  /// sort_tuple_desc_) before sorting.
  const TupleDescriptor* sort_tuple_desc_;

  /// The size in bytes of the sort tuple.
  const int sort_tuple_size_;

  /// Number of tuples per block in a run. This gets multiplied with
  /// TupleIterator::block_index_ in various places and to make sure we don't overflow the
  /// result of that operation we make this int64_t here.
  const int64_t block_capacity_;

  const bool has_var_len_slots_;

  /// True if this is an initial run. False implies this is an sorted intermediate run
  /// resulting from merging other runs.
  const bool initial_run_;

  /// True if all blocks in the run are pinned. Initial runs start off pinned and
  /// can be unpinned. Intermediate runs are always unpinned.
  bool is_pinned_;

  /// True after FinalizeInput() is called. No more tuples can be added after the
  /// run is finalized.
  bool is_finalized_;

  /// True if the tuples in the run are currently in sorted order.
  /// Always true for intermediate runs.
  bool is_sorted_;

  /// Sequence of blocks in this run containing the fixed-length portion of the sort
  /// tuples comprising this run. The data pointed to by the var-len slots are in
  /// var_len_blocks_. A run can have zero blocks if no rows are appended.
  /// If the run is sorted, the tuples in fixed_len_blocks_ will be in sorted order.
  /// fixed_len_blocks_[i] is NULL iff it has been transferred or deleted.
  vector<BufferedBlockMgr::Block*> fixed_len_blocks_;

  /// Sequence of blocks in this run containing the var-length data corresponding to the
  /// var-length columns from fixed_len_blocks_. In intermediate runs, the var-len data is
  /// always stored in the same order as the fixed-length tuples. In initial runs, the
  /// var-len data is initially in unsorted order, but is reshuffled into sorted order in
  /// UnpinAllBlocks(). A run can have no var len blocks if there are no var len slots or
  /// if all the var len data is empty or NULL.
  /// var_len_blocks_[i] is NULL iff it has been transferred or deleted.
  vector<BufferedBlockMgr::Block*> var_len_blocks_;

  /// For initial unsorted runs, an extra pinned block is needed to reorder var-len data
  /// into fixed order in UnpinAllBlocks(). 'var_len_copy_block_' stores this extra
  /// block. Deleted in UnpinAllBlocks().
  /// TODO: in case of in-memory runs, this could be deleted earlier to free up memory.
  BufferedBlockMgr::Block* var_len_copy_block_;

  /// Number of tuples added so far to this run.
  int64_t num_tuples_;

  /// Number of tuples returned via GetNext(), maintained for debug purposes.
  int64_t num_tuples_returned_;

  /// Used to implement GetNextBatch() interface required for the merger.
  scoped_ptr<RowBatch> buffered_batch_;

  /// Members used when a run is read in GetNext().
  /// The index into 'fixed_' and 'var_len_blocks_' of the blocks being read in GetNext().
  int fixed_len_blocks_index_;
  int var_len_blocks_index_;

  /// If true, the last call to GetNext() reached the end of the previous fixed or
  /// var-len block. The next call to GetNext() must increment 'fixed_len_blocks_index_'
  /// or 'var_len_blocks_index_'. It must also pin the next block if the run is unpinned.
  bool end_of_fixed_len_block_;
  bool end_of_var_len_block_;

  /// Offset into the current fixed length data block being processed.
  int fixed_len_block_offset_;
};

/// Helper class used to iterate over tuples in a run during sorting.
class Sorter::TupleIterator {
 public:
  /// Creates an iterator pointing at the tuple with the given 'index' in the 'run'.
  /// The index can be in the range [0, run->num_tuples()]. If it is equal to
  /// run->num_tuples(), the iterator points to one past the end of the run, so
  /// invoking Prev() will cause the iterator to point at the last tuple in the run.
  /// 'run' must be finalized.
  TupleIterator(Sorter::Run* run, int64_t index);

  /// Default constructor used for local variable. Produces invalid iterator that must
  /// be assigned before use.
  TupleIterator() : index_(-1), tuple_(NULL), buffer_start_index_(-1),
      buffer_end_index_(-1), block_index_(-1) { }

  /// Create an iterator pointing to the first tuple in the run.
  static inline TupleIterator Begin(Sorter::Run* run) { return TupleIterator(run, 0); }

  /// Create an iterator pointing one past the end of the run.
  static inline TupleIterator End(Sorter::Run* run) {
    return TupleIterator(run, run->num_tuples());
  }

  /// Increments 'index_' and sets 'tuple_' to point to the next tuple in the run.
  /// Increments 'block_index_' and advances to the next block if the next tuple is in
  /// the next block. Can be advanced one past the last tuple in the run, but is not
  /// valid to dereference 'tuple_' in that case. 'run' and 'tuple_size' are passed as
  /// arguments to avoid redundantly storing the same values in multiple iterators in
  /// perf-critical algorithms.
  inline void Next(Sorter::Run* run, int tuple_size);

  /// The reverse of Next(). Can advance one before the first tuple in the run, but it is
  /// invalid to dereference 'tuple_' in that case.
  inline void Prev(Sorter::Run* run, int tuple_size);

  inline int64_t index() const { return index_; }
  inline Tuple* tuple() const { return reinterpret_cast<Tuple*>(tuple_); }
  /// Returns current tuple in TupleRow format. The caller should not modify the row.
  inline const TupleRow* row() const {
    return reinterpret_cast<const TupleRow*>(&tuple_);
  }

 private:
  // Move to the next block in the run (or do nothing if at end of run).
  // This is the slow path for Next();
  void NextBlock(Sorter::Run* run, int tuple_size);

  // Move to the previous block in the run (or do nothing if at beginning of run).
  // This is the slow path for Prev();
  void PrevBlock(Sorter::Run* run, int tuple_size);

  /// Index of the current tuple in the run.
  /// Can be -1 or run->num_rows() if Next() or Prev() moves iterator outside of run.
  int64_t index_;

  /// Pointer to the current tuple.
  /// Will be an invalid pointer outside of current buffer if Next() or Prev() moves
  /// iterator outside of run.
  uint8_t* tuple_;

  /// Indices of start and end tuples of block at block_index_. I.e. the current block
  /// has tuples with indices in range [buffer_start_index_, buffer_end_index).
  int64_t buffer_start_index_;
  int64_t buffer_end_index_;

  /// Index into fixed_len_blocks_ of the block containing the current tuple.
  /// If index_ is negative or past end of run, will point to the first or last block
  /// in run respectively.
  int block_index_;
};

/// Sorts a sequence of tuples from a run in place using a provided tuple comparator.
/// Quick sort is used for sequences of tuples larger that 16 elements, and insertion sort
/// is used for smaller sequences. The TupleSorter is initialized with a RuntimeState
/// instance to check for cancellation during an in-memory sort.
class Sorter::TupleSorter {
 public:
  TupleSorter(const TupleRowComparator& comparator, int64_t block_size,
      int tuple_size, RuntimeState* state);

  ~TupleSorter();

  /// Performs a quicksort for tuples in 'run' followed by an insertion sort to
  /// finish smaller blocks. Only valid to call if this is an initial run that has not
  /// yet been sorted. Returns an error status if any error is encountered or if the
  /// query is cancelled.
  Status Sort(Run* run);

 private:
  static const int INSERTION_THRESHOLD = 16;

  /// Size of the tuples in memory.
  const int tuple_size_;

  /// Tuple comparator with method Less() that returns true if lhs < rhs.
  const TupleRowComparator& comparator_;

  /// Number of times comparator_.Less() can be invoked again before
  /// comparator_.FreeLocalAllocations() needs to be called.
  int num_comparisons_till_free_;

  /// Runtime state instance to check for cancellation. Not owned.
  RuntimeState* const state_;

  /// The run to be sorted.
  Run* run_;

  /// Temporarily allocated space to copy and swap tuples (Both are used in Partition()).
  /// Owned by this TupleSorter instance.
  uint8_t* temp_tuple_buffer_;
  uint8_t* swap_buffer_;

  /// Random number generator used to randomly choose pivots. We need a RNG that
  /// can generate 64-bit ints. Quality of randomness doesn't need to be especially
  /// high: Mersenne Twister should be more than adequate.
  mt19937_64 rng_;

  /// Wrapper around comparator_.Less(). Also call comparator_.FreeLocalAllocations()
  /// on every 'state_->batch_size()' invocations of comparator_.Less(). Returns true
  /// if 'lhs' is less than 'rhs'.
  bool Less(const TupleRow* lhs, const TupleRow* rhs);

  /// Perform an insertion sort for rows in the range [begin, end) in a run.
  /// Only valid to call for ranges of size at least 1.
  Status InsertionSort(const TupleIterator& begin, const TupleIterator& end);

  /// Partitions the sequence of tuples in the range [begin, end) in a run into two
  /// groups around the pivot tuple - i.e. tuples in first group are <= the pivot, and
  /// tuples in the second group are >= pivot. Tuples are swapped in place to create the
  /// groups and the index to the first element in the second group is returned in 'cut'.
  /// Return an error status if any error is encountered or if the query is cancelled.
  Status Partition(TupleIterator begin, TupleIterator end, const Tuple* pivot,
      TupleIterator* cut);

  /// Performs a quicksort of rows in the range [begin, end) followed by insertion sort
  /// for smaller groups of elements. Return an error status for any errors or if the
  /// query is cancelled.
  Status SortHelper(TupleIterator begin, TupleIterator end);

  /// Select a pivot to partition [begin, end).
  Tuple* SelectPivot(TupleIterator begin, TupleIterator end);

  /// Return median of three tuples according to the sort comparator.
  Tuple* MedianOfThree(Tuple* t1, Tuple* t2, Tuple* t3);

  /// Swaps tuples pointed to by left and right using 'swap_tuple'.
  static void Swap(Tuple* left, Tuple* right, Tuple* swap_tuple, int tuple_size);
};

// Sorter::Run methods
Sorter::Run::Run(Sorter* parent, TupleDescriptor* sort_tuple_desc,
    bool initial_run)
  : sorter_(parent),
    sort_tuple_desc_(sort_tuple_desc),
    sort_tuple_size_(sort_tuple_desc->byte_size()),
    block_capacity_(parent->block_mgr_->max_block_size() / sort_tuple_size_),
    has_var_len_slots_(sort_tuple_desc->HasVarlenSlots()),
    initial_run_(initial_run),
    is_pinned_(initial_run),
    is_finalized_(false),
    is_sorted_(!initial_run),
    var_len_copy_block_(NULL),
    num_tuples_(0) { }

Status Sorter::Run::Init() {
  BufferedBlockMgr::Block* block = NULL;
  RETURN_IF_ERROR(
      sorter_->block_mgr_->GetNewBlock(sorter_->block_mgr_client_, NULL, &block));
  if (block == NULL) {
    return sorter_->mem_tracker_->MemLimitExceeded(
        sorter_->state_, Substitute(MEM_ALLOC_FAILED_ERROR_MSG, "fixed"));
  }
  fixed_len_blocks_.push_back(block);
  if (has_var_len_slots_) {
    RETURN_IF_ERROR(
        sorter_->block_mgr_->GetNewBlock(sorter_->block_mgr_client_, NULL, &block));
    if (block == NULL) {
      return sorter_->mem_tracker_->MemLimitExceeded(
          sorter_->state_, Substitute(MEM_ALLOC_FAILED_ERROR_MSG, "variable"));
    }
    var_len_blocks_.push_back(block);
    if (initial_run_) {
      // Need additional var len block to reorder var len data in UnpinAllBlocks().
      RETURN_IF_ERROR(sorter_->block_mgr_->GetNewBlock(
          sorter_->block_mgr_client_, NULL, &var_len_copy_block_));
      if (var_len_copy_block_ == NULL) {
        return sorter_->mem_tracker_->MemLimitExceeded(
            sorter_->state_, Substitute(MEM_ALLOC_FAILED_ERROR_MSG, "variable"));
      }
    }
  }
  if (initial_run_) {
    sorter_->initial_runs_counter_->Add(1);
  } else {
    sorter_->spilled_runs_counter_->Add(1);
  }
  return Status::OK();
}

template <bool HAS_VAR_LEN_SLOTS, bool INITIAL_RUN>
Status Sorter::Run::AddBatchInternal(RowBatch* batch, int start_index, int* num_processed) {
  DCHECK(!is_finalized_);
  DCHECK(!fixed_len_blocks_.empty());
  DCHECK_EQ(HAS_VAR_LEN_SLOTS, has_var_len_slots_);
  DCHECK_EQ(INITIAL_RUN, initial_run_);

  *num_processed = 0;
  BufferedBlockMgr::Block* cur_fixed_len_block = fixed_len_blocks_.back();

  if (!INITIAL_RUN) {
    // For intermediate merges, the input row is the sort tuple.
    DCHECK_EQ(batch->row_desc().tuple_descriptors().size(), 1);
    DCHECK_EQ(batch->row_desc().tuple_descriptors()[0], sort_tuple_desc_);
  }

  /// Keep initial unsorted runs pinned in memory so we can sort them.
  const AddBlockMode add_mode = INITIAL_RUN ? KEEP_PREV_PINNED : UNPIN_PREV;

  // Input rows are copied/materialized into tuples allocated in fixed_len_blocks_.
  // The variable length column data are copied into blocks stored in var_len_blocks_.
  // Input row processing is split into two loops.
  // The inner loop processes as many input rows as will fit in cur_fixed_len_block.
  // The outer loop allocates a new block for fixed-len data if the input batch is
  // not exhausted.

  // cur_input_index is the index into the input 'batch' of the current input row being
  // processed.
  int cur_input_index = start_index;
  vector<StringValue*> string_values;
  string_values.reserve(sort_tuple_desc_->string_slots().size());
  while (cur_input_index < batch->num_rows()) {
    // tuples_remaining is the number of tuples to copy/materialize into
    // cur_fixed_len_block.
    int tuples_remaining = cur_fixed_len_block->BytesRemaining() / sort_tuple_size_;
    tuples_remaining = min(batch->num_rows() - cur_input_index, tuples_remaining);

    for (int i = 0; i < tuples_remaining; ++i) {
      int total_var_len = 0;
      TupleRow* input_row = batch->GetRow(cur_input_index);
      Tuple* new_tuple = cur_fixed_len_block->Allocate<Tuple>(sort_tuple_size_);
      if (INITIAL_RUN) {
        new_tuple->MaterializeExprs<HAS_VAR_LEN_SLOTS, true>(input_row, *sort_tuple_desc_,
            sorter_->sort_tuple_slot_expr_ctxs_, NULL, &string_values, &total_var_len);
        if (total_var_len > sorter_->block_mgr_->max_block_size()) {
          return Status(ErrorMsg(TErrorCode::INTERNAL_ERROR, Substitute(
              "Variable length data in a single tuple larger than block size $0 > $1",
              total_var_len, sorter_->block_mgr_->max_block_size())));
        }
      } else {
        memcpy(new_tuple, input_row->GetTuple(0), sort_tuple_size_);
        if (HAS_VAR_LEN_SLOTS) {
          CollectNonNullVarSlots(new_tuple, &string_values, &total_var_len);
        }
      }

      if (HAS_VAR_LEN_SLOTS) {
        DCHECK_GT(var_len_blocks_.size(), 0);
        BufferedBlockMgr::Block* cur_var_len_block = var_len_blocks_.back();
        if (cur_var_len_block->BytesRemaining() < total_var_len) {
          bool added;
          RETURN_IF_ERROR(TryAddBlock(add_mode, &var_len_blocks_, &added));
          if (added) {
            cur_var_len_block = var_len_blocks_.back();
          } else {
            // There was not enough space in the last var-len block for this tuple, and
            // the run could not be extended. Return the fixed-len allocation and exit.
            cur_fixed_len_block->ReturnAllocation(sort_tuple_size_);
            return Status::OK();
          }
        }

        // Sorting of tuples containing array values is not implemented. The planner
        // combined with projection should guarantee that none are in each tuple.
        for (const SlotDescriptor* coll_slot: sort_tuple_desc_->collection_slots()) {
          DCHECK(new_tuple->IsNull(coll_slot->null_indicator_offset()));
        }

        uint8_t* var_data_ptr = cur_var_len_block->Allocate<uint8_t>(total_var_len);
        if (INITIAL_RUN) {
          CopyVarLenData(string_values, var_data_ptr);
        } else {
          DCHECK_EQ(var_len_blocks_.back(), cur_var_len_block);
          CopyVarLenDataConvertOffset(string_values, var_len_blocks_.size() - 1,
              reinterpret_cast<uint8_t*>(cur_var_len_block->buffer()), var_data_ptr);
        }
      }
      ++num_tuples_;
      ++*num_processed;
      ++cur_input_index;
    }

    // If there are still rows left to process, get a new block for the fixed-length
    // tuples. If the run is already too long, return.
    if (cur_input_index < batch->num_rows()) {
      bool added;
      RETURN_IF_ERROR(TryAddBlock(add_mode, &fixed_len_blocks_, &added));
      if (!added) return Status::OK();
      cur_fixed_len_block = fixed_len_blocks_.back();
    }
  }
  return Status::OK();
}

Status Sorter::Run::FinalizeInput() {
  DCHECK(!is_finalized_);

  RETURN_IF_ERROR(FinalizeBlocks(&fixed_len_blocks_));
  if (has_var_len_slots_) {
    RETURN_IF_ERROR(FinalizeBlocks(&var_len_blocks_));
  }
  is_finalized_ = true;
  return Status::OK();
}

Status Sorter::Run::FinalizeBlocks(vector<BufferedBlockMgr::Block*>* blocks) {
  DCHECK_GT(blocks->size(), 0);
  BufferedBlockMgr::Block* last_block = blocks->back();
  if (last_block->valid_data_len() > 0) {
    DCHECK_EQ(initial_run_, is_pinned_);
    if (!is_pinned_) {
      // Unpin the last block of this unpinned run. We've finished writing the run so
      // all blocks in the run can now be unpinned.
      RETURN_IF_ERROR(last_block->Unpin());
    }
  } else {
    last_block->Delete();
    blocks->pop_back();
  }
  return Status::OK();
}

void Sorter::Run::DeleteAllBlocks() {
  DeleteAndClearBlocks(&fixed_len_blocks_);
  DeleteAndClearBlocks(&var_len_blocks_);
  if (var_len_copy_block_ != NULL) var_len_copy_block_->Delete();
  var_len_copy_block_ = NULL;
}

Status Sorter::Run::UnpinAllBlocks() {
  DCHECK(is_sorted_);
  DCHECK(initial_run_);
  DCHECK(is_pinned_);
  DCHECK(is_finalized_);
  // A list of var len blocks to replace 'var_len_blocks_'. Note that after we are done
  // we may have a different number of blocks, because internal fragmentation may leave
  // slightly different amounts of wasted space at the end of each block.
  // We need to be careful to clean up these blocks if we run into an error in this method.
  vector<BufferedBlockMgr::Block*> sorted_var_len_blocks;
  sorted_var_len_blocks.reserve(var_len_blocks_.size());

  vector<StringValue*> string_values;
  int total_var_len;
  string_values.reserve(sort_tuple_desc_->string_slots().size());
  BufferedBlockMgr::Block* cur_sorted_var_len_block = NULL;
  if (HasVarLenBlocks()) {
    DCHECK(var_len_copy_block_ != NULL);
    sorted_var_len_blocks.push_back(var_len_copy_block_);
    cur_sorted_var_len_block = var_len_copy_block_;
    // Set var_len_copy_block_ to NULL since it was moved to var_len_blocks_.
    var_len_copy_block_ = NULL;
  } else if (has_var_len_slots_) {
    // If we don't have any var-len blocks, clean up the copy block.
    DCHECK(var_len_copy_block_ != NULL);
    var_len_copy_block_->Delete();
    var_len_copy_block_ = NULL;
  } else {
    DCHECK(var_len_copy_block_ == NULL);
  }

  Status status;
  for (int i = 0; i < fixed_len_blocks_.size(); ++i) {
    BufferedBlockMgr::Block* cur_fixed_block = fixed_len_blocks_[i];
    // Skip converting the pointers if no var-len slots, or if all the values are null
    // or zero-length. This will possibly leave zero-length pointers pointing to
    // arbitrary memory, but zero-length data cannot be dereferenced anyway.
    if (HasVarLenBlocks()) {
      for (int block_offset = 0; block_offset < cur_fixed_block->valid_data_len();
          block_offset += sort_tuple_size_) {
        Tuple* cur_tuple =
            reinterpret_cast<Tuple*>(cur_fixed_block->buffer() + block_offset);
        CollectNonNullVarSlots(cur_tuple, &string_values, &total_var_len);
        DCHECK(cur_sorted_var_len_block != NULL);
        if (cur_sorted_var_len_block->BytesRemaining() < total_var_len) {
          bool added;
          status = TryAddBlock(UNPIN_PREV, &sorted_var_len_blocks, &added);
          if (!status.ok()) goto cleanup_blocks;
          DCHECK(added) << "TryAddBlock() with UNPIN_PREV should not fail to add";
          cur_sorted_var_len_block = sorted_var_len_blocks.back();
        }
        uint8_t* var_data_ptr =
            cur_sorted_var_len_block->Allocate<uint8_t>(total_var_len);
        DCHECK_EQ(sorted_var_len_blocks.back(), cur_sorted_var_len_block);
        CopyVarLenDataConvertOffset(string_values, sorted_var_len_blocks.size() - 1,
            reinterpret_cast<uint8_t*>(cur_sorted_var_len_block->buffer()), var_data_ptr);
      }
    }
    status = cur_fixed_block->Unpin();
    if (!status.ok()) goto cleanup_blocks;
  }

  if (HasVarLenBlocks()) {
    DCHECK_GT(sorted_var_len_blocks.back()->valid_data_len(), 0);
    status = sorted_var_len_blocks.back()->Unpin();
    if (!status.ok()) goto cleanup_blocks;
  }

  // Clear var_len_blocks_ and replace with it with the contents of sorted_var_len_blocks
  DeleteAndClearBlocks(&var_len_blocks_);
  sorted_var_len_blocks.swap(var_len_blocks_);
  is_pinned_ = false;
  sorter_->spilled_runs_counter_->Add(1);
  return Status::OK();

cleanup_blocks:
  DeleteAndClearBlocks(&sorted_var_len_blocks);
  return status;
}

Status Sorter::Run::PrepareRead(bool* pinned_all_blocks) {
  DCHECK(is_finalized_);
  DCHECK(is_sorted_);

  fixed_len_blocks_index_ = 0;
  fixed_len_block_offset_ = 0;
  var_len_blocks_index_ = 0;
  end_of_fixed_len_block_ = end_of_var_len_block_ = fixed_len_blocks_.empty();
  num_tuples_returned_ = 0;

  buffered_batch_.reset(new RowBatch(*sorter_->output_row_desc_,
      sorter_->state_->batch_size(), sorter_->mem_tracker_));

  // If the run is pinned, all blocks are already pinned, so we're ready to read.
  if (is_pinned_) {
    *pinned_all_blocks = true;
    return Status::OK();
  }

  // Attempt to pin the first fixed and var-length blocks. In either case, pinning may
  // fail if the number of reserved blocks is oversubscribed, see IMPALA-1590.
  if (fixed_len_blocks_.size() > 0) {
    bool pinned;
    RETURN_IF_ERROR(fixed_len_blocks_[0]->Pin(&pinned));
    if (!pinned) {
      *pinned_all_blocks = false;
      return Status::OK();
    }
  }

  if (HasVarLenBlocks()) {
    bool pinned;
    RETURN_IF_ERROR(var_len_blocks_[0]->Pin(&pinned));
    if (!pinned) {
      *pinned_all_blocks = false;
      return Status::OK();
    }
  }

  *pinned_all_blocks = true;
  return Status::OK();
}

Status Sorter::Run::GetNextBatch(RowBatch** output_batch) {
  DCHECK(buffered_batch_ != NULL);
  buffered_batch_->Reset();
  // Fill more rows into buffered_batch_.
  bool eos;
  if (HasVarLenBlocks() && !is_pinned_) {
    RETURN_IF_ERROR(GetNext<true>(buffered_batch_.get(), &eos));
  } else {
    RETURN_IF_ERROR(GetNext<false>(buffered_batch_.get(), &eos));
  }

  if (eos) {
    // Setting output_batch to NULL signals eos to the caller, so GetNext() is not
    // allowed to attach resources to the batch on eos.
    DCHECK_EQ(buffered_batch_->num_rows(), 0);
    DCHECK_EQ(buffered_batch_->num_blocks(), 0);
    *output_batch = NULL;
    return Status::OK();
  }
  *output_batch = buffered_batch_.get();
  return Status::OK();
}

template <bool CONVERT_OFFSET_TO_PTR>
Status Sorter::Run::GetNext(RowBatch* output_batch, bool* eos) {
  // Var-len offsets are converted only when reading var-len data from unpinned runs.
  // We shouldn't convert var len offsets if there are no blocks, since in that case
  // they must all be null or zero-length strings, which don't point into a valid block.
  DCHECK_EQ(CONVERT_OFFSET_TO_PTR, HasVarLenBlocks() && !is_pinned_);

  if (end_of_fixed_len_block_ &&
      fixed_len_blocks_index_ >= static_cast<int>(fixed_len_blocks_.size()) - 1) {
    if (is_pinned_) {
      // All blocks were previously attached to output batches. GetNextBatch() assumes
      // that we don't attach resources to the batch on eos.
      DCHECK_EQ(NumNonNullBlocks(fixed_len_blocks_), 0);
      DCHECK_EQ(NumNonNullBlocks(var_len_blocks_), 0);

      // Flush resources in case we are in a subplan and need to allocate more blocks
      // when the node is reopened.
      output_batch->MarkFlushResources();
    } else {
      // We held onto the last fixed or var len blocks without transferring them to the
      // caller. We signalled MarkNeedsDeepCopy() to the caller, so we can safely delete
      // them now to free memory.
      if (!fixed_len_blocks_.empty()) DCHECK_EQ(NumNonNullBlocks(fixed_len_blocks_), 1);
      if (!var_len_blocks_.empty()) DCHECK_EQ(NumNonNullBlocks(var_len_blocks_), 1);
    }
    DeleteAllBlocks();
    *eos = true;
    DCHECK_EQ(num_tuples_returned_, num_tuples_);
    return Status::OK();
  }

  // Advance the fixed or var len block if we reached the end in the previous call to
  // GetNext().
  if (end_of_fixed_len_block_) {
    RETURN_IF_ERROR(PinNextReadBlock(&fixed_len_blocks_, fixed_len_blocks_index_));
    ++fixed_len_blocks_index_;
    fixed_len_block_offset_ = 0;
    end_of_fixed_len_block_ = false;
  }
  if (end_of_var_len_block_) {
    RETURN_IF_ERROR(PinNextReadBlock(&var_len_blocks_, var_len_blocks_index_));
    ++var_len_blocks_index_;
    end_of_var_len_block_ = false;
  }

  // Fills rows into the output batch until a block boundary is reached.
  BufferedBlockMgr::Block* fixed_len_block = fixed_len_blocks_[fixed_len_blocks_index_];
  DCHECK(fixed_len_block != NULL);
  while (!output_batch->AtCapacity() &&
      fixed_len_block_offset_ < fixed_len_block->valid_data_len()) {
    DCHECK(fixed_len_block != NULL);
    Tuple* input_tuple = reinterpret_cast<Tuple*>(
        fixed_len_block->buffer() + fixed_len_block_offset_);

    if (CONVERT_OFFSET_TO_PTR && !ConvertOffsetsToPtrs(input_tuple)) {
      DCHECK(!is_pinned_);
      // The var-len data is in the next block. We are done with the current block, so
      // return rows we've accumulated so far and advance to the next block in the next
      // GetNext() call. This is needed for the unpinned case where we need to exchange
      // this block for the next in the next GetNext() call. So therefore we must hold
      // onto the current var-len block and signal to the caller that the block is going
      // to be deleted.
      output_batch->MarkNeedsDeepCopy();
      end_of_var_len_block_ = true;
      break;
    }
    output_batch->GetRow(output_batch->AddRow())->SetTuple(0, input_tuple);
    output_batch->CommitLastRow();
    fixed_len_block_offset_ += sort_tuple_size_;
    ++num_tuples_returned_;
  }

  if (fixed_len_block_offset_ >= fixed_len_block->valid_data_len()) {
    // Reached the block boundary, need to move to the next block.
    if (is_pinned_) {
      // Attach block to batch. Caller can delete the block when it wants to.
      output_batch->AddBlock(fixed_len_blocks_[fixed_len_blocks_index_],
          RowBatch::FlushMode::NO_FLUSH_RESOURCES);
      fixed_len_blocks_[fixed_len_blocks_index_] = NULL;

      // Attach the var-len blocks at eos once no more rows will reference the blocks.
      if (fixed_len_blocks_index_ == fixed_len_blocks_.size() - 1) {
        for (BufferedBlockMgr::Block* var_len_block: var_len_blocks_) {
          DCHECK(var_len_block != NULL);
          output_batch->AddBlock(var_len_block, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
        }
        var_len_blocks_.clear();
      }
    } else {
      // To iterate over unpinned runs, we need to exchange this block for the next
      // in the next GetNext() call, so we need to hold onto the block and signal to
      // the caller that the block is going to be deleted.
      output_batch->MarkNeedsDeepCopy();
    }
    end_of_fixed_len_block_ = true;
  }
  *eos = false;
  return Status::OK();
}

Status Sorter::Run::PinNextReadBlock(vector<BufferedBlockMgr::Block*>* blocks,
    int block_index) {
  DCHECK_GE(block_index, 0);
  DCHECK_LT(block_index, blocks->size() - 1);
  BufferedBlockMgr::Block* curr_block = (*blocks)[block_index];
  BufferedBlockMgr::Block* next_block = (*blocks)[block_index + 1];
  DCHECK_EQ(is_pinned_, next_block->is_pinned());
  if (is_pinned_) {
    // The current block was attached to a batch and 'next_block' is already pinned.
    DCHECK(curr_block == NULL);
    return Status::OK();
  }
  bool pinned;
  // Atomically delete the previous block and pin this one. Should not fail due to lack
  // of memory. Pin() deletes the block even in error cases, so we need to remove it from
  // the vector first to avoid an inconsistent state.
  (*blocks)[block_index] = NULL;
  RETURN_IF_ERROR(next_block->Pin(&pinned, curr_block, false));
  DCHECK(pinned) << "Atomic delete and pin should not fail without error.";
  return Status::OK();
}

void Sorter::Run::CollectNonNullVarSlots(Tuple* src,
    vector<StringValue*>* string_values, int* total_var_len) {
  string_values->clear();
  *total_var_len = 0;
  for (const SlotDescriptor* string_slot: sort_tuple_desc_->string_slots()) {
    if (!src->IsNull(string_slot->null_indicator_offset())) {
      StringValue* string_val =
          reinterpret_cast<StringValue*>(src->GetSlot(string_slot->tuple_offset()));
      string_values->push_back(string_val);
      *total_var_len += string_val->len;
    }
  }
}

Status Sorter::Run::TryAddBlock(AddBlockMode mode,
    vector<BufferedBlockMgr::Block*>* block_sequence, bool* added) {
  DCHECK(!block_sequence->empty());
  BufferedBlockMgr::Block* prev_block;
  if (mode == KEEP_PREV_PINNED) {
    prev_block = NULL;
  } else {
    DCHECK(mode == UNPIN_PREV);
    // Swap the prev block with the next, to guarantee success.
    prev_block = block_sequence->back();
  }

  BufferedBlockMgr::Block* new_block;
  RETURN_IF_ERROR(sorter_->block_mgr_->GetNewBlock(
      sorter_->block_mgr_client_, prev_block, &new_block));
  if (new_block != NULL) {
    *added = true;
    block_sequence->push_back(new_block);
  } else {
    DCHECK_EQ(mode, KEEP_PREV_PINNED);
    *added = false;
  }
  return Status::OK();
}

void Sorter::Run::CopyVarLenData(const vector<StringValue*>& string_values,
    uint8_t* dest) {
  for (StringValue* string_val: string_values) {
    memcpy(dest, string_val->ptr, string_val->len);
    string_val->ptr = reinterpret_cast<char*>(dest);
    dest += string_val->len;
  }
}

void Sorter::Run::CopyVarLenDataConvertOffset(const vector<StringValue*>& string_values,
    int block_index, const uint8_t* block_start, uint8_t* dest) {
  DCHECK_GE(block_index, 0);
  DCHECK_GE(dest - block_start, 0);

  for (StringValue* string_val: string_values) {
    memcpy(dest, string_val->ptr, string_val->len);
    DCHECK_LE(dest - block_start, sorter_->block_mgr_->max_block_size());
    DCHECK_LE(dest - block_start, INT_MAX);
    int block_offset = dest - block_start;
    uint64_t packed_offset =
        (static_cast<uint64_t>(block_index) << 32) | block_offset;
    string_val->ptr = reinterpret_cast<char*>(packed_offset);
    dest += string_val->len;
  }
}

bool Sorter::Run::ConvertOffsetsToPtrs(Tuple* tuple) {
  // We need to be careful to handle the case where var_len_blocks_ is empty,
  // e.g. if all strings are NULL.
  uint8_t* block_start = var_len_blocks_.empty() ? NULL :
      var_len_blocks_[var_len_blocks_index_]->buffer();

  const vector<SlotDescriptor*>& string_slots = sort_tuple_desc_->string_slots();
  for (int i = 0; i < string_slots.size(); ++i) {
    SlotDescriptor* slot_desc = string_slots[i];
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;

    DCHECK(slot_desc->type().IsVarLenStringType());
    StringValue* value = reinterpret_cast<StringValue*>(
        tuple->GetSlot(slot_desc->tuple_offset()));
    // packed_offset includes the block index in the upper 32 bits and the block
    // offset in the lower 32 bits. See CopyVarLenDataConvertOffset().
    uint64_t packed_offset = reinterpret_cast<uint64_t>(value->ptr);
    int block_index = packed_offset >> 32;
    int block_offset = packed_offset & 0xFFFFFFFF;

    if (block_index > var_len_blocks_index_) {
      // We've reached the block boundary for the current var-len block.
      // This tuple will be returned in the next call to GetNext().
      DCHECK_GE(block_index, 0);
      DCHECK_LE(block_index, var_len_blocks_.size());
      DCHECK_EQ(block_index, var_len_blocks_index_ + 1);
      DCHECK_EQ(block_offset, 0); // The data is the first thing in the next block.
      DCHECK_EQ(i, 0); // Var len data for tuple shouldn't be split across blocks.
      return false;
    }

    DCHECK_EQ(block_index, var_len_blocks_index_);
    if (var_len_blocks_.empty()) {
      DCHECK_EQ(value->len, 0);
    } else {
      DCHECK_LE(block_offset + value->len, var_len_blocks_[block_index]->valid_data_len());
    }
    // Calculate the address implied by the offset and assign it. May be NULL for
    // zero-length strings if there are no blocks in the run since block_start is NULL.
    DCHECK(block_start != NULL || block_offset == 0);
    value->ptr = reinterpret_cast<char*>(block_start + block_offset);
  }
  return true;
}

int64_t Sorter::Run::TotalBytes() const {
  int64_t total_bytes = 0;
  for (BufferedBlockMgr::Block* block: fixed_len_blocks_) {
    if (block != NULL) total_bytes += block->valid_data_len();
  }

  for (BufferedBlockMgr::Block* block: var_len_blocks_) {
    if (block != NULL) total_bytes += block->valid_data_len();
  }
  return total_bytes;
}

Sorter::TupleIterator::TupleIterator(Sorter::Run* run, int64_t index)
    : index_(index), tuple_(NULL) {
  DCHECK(run->is_finalized_);
  DCHECK_GE(index, 0);
  DCHECK_LE(index, run->num_tuples());
  // If the run is empty, only index_ and tuple_ are initialized.
  if (run->num_tuples() == 0) {
    DCHECK_EQ(index, 0);
    return;
  }

  const int tuple_size = run->sort_tuple_size_;
  int block_offset;
  if (UNLIKELY(index == run->num_tuples())) {
    // If the iterator is initialized past the end, set up buffer_start_index_,
    // 'buffer_end_index_' and 'block_index_' for the last block, then set 'tuple' to
    // 'tuple_size' bytes past the last tuple, so everything is correct when Prev() is
    // invoked.
    block_index_ = run->fixed_len_blocks_.size() - 1;
    block_offset = ((index - 1) % run->block_capacity_) * tuple_size + tuple_size;
  } else {
    block_index_ = index / run->block_capacity_;
    block_offset = (index % run->block_capacity_) * tuple_size;
  }
  buffer_start_index_ = block_index_ * run->block_capacity_;
  buffer_end_index_ = buffer_start_index_ + run->block_capacity_;
  tuple_ = run->fixed_len_blocks_[block_index_]->buffer() + block_offset;
}

void Sorter::TupleIterator::Next(Sorter::Run* run, int tuple_size) {
  DCHECK_LT(index_, run->num_tuples()) << "Can only advance one past end of run";
  tuple_ += tuple_size;
  ++index_;
  if (UNLIKELY(index_ >= buffer_end_index_)) NextBlock(run, tuple_size);
}

void Sorter::TupleIterator::NextBlock(Sorter::Run* run, int tuple_size) {
  // When moving after the last tuple, stay at the last block.
  if (index_ >= run->num_tuples()) return;
  ++block_index_;
  DCHECK_LT(block_index_, run->fixed_len_blocks_.size());
  buffer_start_index_ = block_index_ * run->block_capacity_;
  DCHECK_EQ(index_, buffer_start_index_);
  buffer_end_index_ = buffer_start_index_ + run->block_capacity_;
  tuple_ = run->fixed_len_blocks_[block_index_]->buffer();
}

void Sorter::TupleIterator::Prev(Sorter::Run* run, int tuple_size) {
  DCHECK_GE(index_, 0) << "Can only advance one before start of run";
  tuple_ -= tuple_size;
  --index_;
  if (UNLIKELY(index_ < buffer_start_index_)) PrevBlock(run, tuple_size);
}

void Sorter::TupleIterator::PrevBlock(Sorter::Run* run, int tuple_size) {
  // When moving before the first tuple, stay at the first block.
  if (index_ < 0) return;
  --block_index_;
  DCHECK_GE(block_index_, 0);
  buffer_start_index_ = block_index_ * run->block_capacity_;
  buffer_end_index_ = buffer_start_index_ + run->block_capacity_;
  DCHECK_EQ(index_, buffer_end_index_ - 1);
  int last_tuple_block_offset = run->sort_tuple_size_ * (run->block_capacity_ - 1);
  tuple_ = run->fixed_len_blocks_[block_index_]->buffer() + last_tuple_block_offset;
}

Sorter::TupleSorter::TupleSorter(const TupleRowComparator& comp, int64_t block_size,
    int tuple_size, RuntimeState* state)
  : tuple_size_(tuple_size),
    comparator_(comp),
    num_comparisons_till_free_(state->batch_size()),
    state_(state) {
  temp_tuple_buffer_ = new uint8_t[tuple_size];
  swap_buffer_ = new uint8_t[tuple_size];
}

Sorter::TupleSorter::~TupleSorter() {
  delete[] temp_tuple_buffer_;
  delete[] swap_buffer_;
}

bool Sorter::TupleSorter::Less(const TupleRow* lhs, const TupleRow* rhs) {
  --num_comparisons_till_free_;
  DCHECK_GE(num_comparisons_till_free_, 0);
  if (UNLIKELY(num_comparisons_till_free_ == 0)) {
    comparator_.FreeLocalAllocations();
    num_comparisons_till_free_ = state_->batch_size();
  }
  return comparator_.Less(lhs, rhs);
}

Status Sorter::TupleSorter::Sort(Run* run) {
  DCHECK(run->is_finalized());
  DCHECK(!run->is_sorted());
  run_ = run;
  RETURN_IF_ERROR(SortHelper(TupleIterator::Begin(run_), TupleIterator::End(run_)));
  run_->set_sorted();
  return Status::OK();
}

// Sort the sequence of tuples from [begin, last).
// Begin with a sorted sequence of size 1 [begin, begin+1).
// During each pass of the outermost loop, add the next tuple (at position 'i') to
// the sorted sequence by comparing it to each element of the sorted sequence
// (reverse order) to find its correct place in the sorted sequence, copying tuples
// along the way.
Status Sorter::TupleSorter::InsertionSort(const TupleIterator& begin,
    const TupleIterator& end) {
  DCHECK_LT(begin.index(), end.index());

  // Hoist member variable lookups out of loop to avoid extra loads inside loop.
  Run* run = run_;
  int tuple_size = tuple_size_;
  uint8_t* temp_tuple_buffer = temp_tuple_buffer_;

  TupleIterator insert_iter = begin;
  insert_iter.Next(run, tuple_size);
  for (; insert_iter.index() < end.index(); insert_iter.Next(run, tuple_size)) {
    // insert_iter points to the tuple after the currently sorted sequence that must
    // be inserted into the sorted sequence. Copy to temp_tuple_buffer_ since it may be
    // overwritten by the one at position 'insert_iter - 1'
    memcpy(temp_tuple_buffer, insert_iter.tuple(), tuple_size);

    // 'iter' points to the tuple that temp_tuple_buffer will be compared to.
    // 'copy_to' is the where iter should be copied to if it is >= temp_tuple_buffer.
    // copy_to always to the next row after 'iter'
    TupleIterator iter = insert_iter;
    iter.Prev(run, tuple_size);
    Tuple* copy_to = insert_iter.tuple();
    while (Less(reinterpret_cast<TupleRow*>(&temp_tuple_buffer), iter.row())) {
      memcpy(copy_to, iter.tuple(), tuple_size);
      copy_to = iter.tuple();
      // Break if 'iter' has reached the first row, meaning that the temp row
      // will be inserted in position 'begin'
      if (iter.index() <= begin.index()) break;
      iter.Prev(run, tuple_size);
    }

    memcpy(copy_to, temp_tuple_buffer, tuple_size);
  }
  RETURN_IF_CANCELLED(state_);
  RETURN_IF_ERROR(state_->GetQueryStatus());
  return Status::OK();
}

Status Sorter::TupleSorter::Partition(TupleIterator begin,
    TupleIterator end, const Tuple* pivot, TupleIterator* cut) {
  // Hoist member variable lookups out of loop to avoid extra loads inside loop.
  Run* run = run_;
  int tuple_size = tuple_size_;
  Tuple* temp_tuple = reinterpret_cast<Tuple*>(temp_tuple_buffer_);
  Tuple* swap_tuple = reinterpret_cast<Tuple*>(swap_buffer_);

  // Copy pivot into temp_tuple since it points to a tuple within [begin, end).
  DCHECK(temp_tuple != NULL);
  DCHECK(pivot != NULL);
  memcpy(temp_tuple, pivot, tuple_size);

  TupleIterator left = begin;
  TupleIterator right = end;
  right.Prev(run, tuple_size); // Set 'right' to the last tuple in range.
  while (true) {
    // Search for the first and last out-of-place elements, and swap them.
    while (Less(left.row(), reinterpret_cast<TupleRow*>(&temp_tuple))) {
      left.Next(run, tuple_size);
    }
    while (Less(reinterpret_cast<TupleRow*>(&temp_tuple), right.row())) {
      right.Prev(run, tuple_size);
    }

    if (left.index() >= right.index()) break;
    // Swap first and last tuples.
    Swap(left.tuple(), right.tuple(), swap_tuple, tuple_size);

    left.Next(run, tuple_size);
    right.Prev(run, tuple_size);

    RETURN_IF_CANCELLED(state_);
    RETURN_IF_ERROR(state_->GetQueryStatus());
  }

  *cut = left;
  return Status::OK();
}

Status Sorter::TupleSorter::SortHelper(TupleIterator begin, TupleIterator end) {
  // Use insertion sort for smaller sequences.
  while (end.index() - begin.index() > INSERTION_THRESHOLD) {
    // Select a pivot and call Partition() to split the tuples in [begin, end) into two
    // groups (<= pivot and >= pivot) in-place. 'cut' is the index of the first tuple in
    // the second group.
    Tuple* pivot = SelectPivot(begin, end);
    TupleIterator cut;
    RETURN_IF_ERROR(Partition(begin, end, pivot, &cut));

    // Recurse on the smaller partition. This limits stack size to log(n) stack frames.
    if (cut.index() - begin.index() < end.index() - cut.index()) {
      // Left partition is smaller.
      RETURN_IF_ERROR(SortHelper(begin, cut));
      begin = cut;
    } else {
      // Right partition is equal or smaller.
      RETURN_IF_ERROR(SortHelper(cut, end));
      end = cut;
    }
  }

  if (begin.index() < end.index()) RETURN_IF_ERROR(InsertionSort(begin, end));
  return Status::OK();
}

Tuple* Sorter::TupleSorter::SelectPivot(TupleIterator begin, TupleIterator end) {
  // Select the median of three random tuples. The random selection avoids pathological
  // behaviour associated with techniques that pick a fixed element (e.g. picking
  // first/last/middle element) and taking the median tends to help us select better
  // pivots that more evenly split the input range. This method makes selection of
  // bad pivots very infrequent.
  //
  // To illustrate, if we define a bad pivot as one in the lower or upper 10% of values,
  // then the median of three is a bad pivot only if all three randomly-selected values
  // are in the lower or upper 10%. The probability of that is 0.2 * 0.2 * 0.2 = 0.008:
  // less than 1%. Since selection is random each time, the chance of repeatedly picking
  // bad pivots decreases exponentialy and becomes negligibly small after a few
  // iterations.
  Tuple* tuples[3];
  for (int i = 0; i < 3; ++i) {
    int64_t index = uniform_int<int64_t>(begin.index(), end.index() - 1)(rng_);
    TupleIterator iter(run_, index);
    DCHECK(iter.tuple() != NULL);
    tuples[i] = iter.tuple();
  }

  return MedianOfThree(tuples[0], tuples[1], tuples[2]);
}

Tuple* Sorter::TupleSorter::MedianOfThree(Tuple* t1, Tuple* t2, Tuple* t3) {
  TupleRow* tr1 = reinterpret_cast<TupleRow*>(&t1);
  TupleRow* tr2 = reinterpret_cast<TupleRow*>(&t2);
  TupleRow* tr3 = reinterpret_cast<TupleRow*>(&t3);

  bool t1_lt_t2 = Less(tr1, tr2);
  bool t2_lt_t3 = Less(tr2, tr3);
  bool t1_lt_t3 = Less(tr1, tr3);

  if (t1_lt_t2) {
    // t1 < t2
    if (t2_lt_t3) {
      // t1 < t2 < t3
      return t2;
    } else if (t1_lt_t3) {
      // t1 < t3 <= t2
      return t3;
    } else {
      // t3 <= t1 < t2
      return t1;
    }
  } else {
    // t2 <= t1
    if (t1_lt_t3) {
      // t2 <= t1 < t3
      return t1;
    } else if (t2_lt_t3) {
      // t2 < t3 <= t1
      return t3;
    } else {
      // t3 <= t2 <= t1
      return t2;
    }
  }
}

inline void Sorter::TupleSorter::Swap(Tuple* left, Tuple* right, Tuple* swap_tuple,
    int tuple_size) {
  memcpy(swap_tuple, left, tuple_size);
  memcpy(left, right, tuple_size);
  memcpy(right, swap_tuple, tuple_size);
}

Sorter::Sorter(const TupleRowComparator& compare_less_than,
    const vector<ExprContext*>& slot_materialize_expr_ctxs,
    RowDescriptor* output_row_desc, MemTracker* mem_tracker,
    RuntimeProfile* profile, RuntimeState* state)
  : state_(state),
    compare_less_than_(compare_less_than),
    in_mem_tuple_sorter_(NULL),
    block_mgr_(state->block_mgr()),
    block_mgr_client_(NULL),
    has_var_len_slots_(false),
    sort_tuple_slot_expr_ctxs_(slot_materialize_expr_ctxs),
    mem_tracker_(mem_tracker),
    output_row_desc_(output_row_desc),
    unsorted_run_(NULL),
    merge_output_run_(NULL),
    profile_(profile),
    initial_runs_counter_(NULL),
    num_merges_counter_(NULL),
    in_mem_sort_timer_(NULL),
    sorted_data_size_(NULL) {
}

Sorter::~Sorter() {
  DCHECK(sorted_runs_.empty());
  DCHECK(merging_runs_.empty());
  DCHECK(unsorted_run_ == NULL);
  DCHECK(merge_output_run_ == NULL);
}

Status Sorter::Init() {
  DCHECK(unsorted_run_ == NULL) << "Already initialized";
  TupleDescriptor* sort_tuple_desc = output_row_desc_->tuple_descriptors()[0];
  has_var_len_slots_ = sort_tuple_desc->HasVarlenSlots();
  in_mem_tuple_sorter_.reset(new TupleSorter(compare_less_than_,
      block_mgr_->max_block_size(), sort_tuple_desc->byte_size(), state_));
  unsorted_run_ = obj_pool_.Add(new Run(this, sort_tuple_desc, true));

  initial_runs_counter_ = ADD_COUNTER(profile_, "InitialRunsCreated", TUnit::UNIT);
  spilled_runs_counter_ = ADD_COUNTER(profile_, "SpilledRuns", TUnit::UNIT);
  num_merges_counter_ = ADD_COUNTER(profile_, "TotalMergesPerformed", TUnit::UNIT);
  in_mem_sort_timer_ = ADD_TIMER(profile_, "InMemorySortTime");
  sorted_data_size_ = ADD_COUNTER(profile_, "SortDataSize", TUnit::BYTES);

  // Must be kept in sync with SortNode.computeResourceProfile() in fe.
  int min_buffers_required = MIN_BUFFERS_PER_MERGE;
  // Fixed and var-length blocks are separate, so we need MIN_BUFFERS_PER_MERGE
  // blocks for both if there is var-length data.
  if (has_var_len_slots_) min_buffers_required *= 2;

  RETURN_IF_ERROR(block_mgr_->RegisterClient(Substitute("Sorter ptr=$0", this),
      min_buffers_required, false, mem_tracker_, state_, &block_mgr_client_));

  DCHECK(unsorted_run_ != NULL);
  RETURN_IF_ERROR(unsorted_run_->Init());
  return Status::OK();
}

Status Sorter::AddBatch(RowBatch* batch) {
  DCHECK(unsorted_run_ != NULL);
  DCHECK(batch != NULL);
  int num_processed = 0;
  int cur_batch_index = 0;
  while (cur_batch_index < batch->num_rows()) {
    RETURN_IF_ERROR(unsorted_run_->AddInputBatch(batch, cur_batch_index, &num_processed));

    cur_batch_index += num_processed;
    if (cur_batch_index < batch->num_rows()) {
      // The current run is full. Sort it and begin the next one.
      RETURN_IF_ERROR(SortCurrentInputRun());
      RETURN_IF_ERROR(sorted_runs_.back()->UnpinAllBlocks());
      unsorted_run_ = obj_pool_.Add(
          new Run(this, output_row_desc_->tuple_descriptors()[0], true));
      RETURN_IF_ERROR(unsorted_run_->Init());
    }
  }
  return Status::OK();
}

Status Sorter::InputDone() {
  // Sort the tuples in the last run.
  RETURN_IF_ERROR(SortCurrentInputRun());

  if (sorted_runs_.size() == 1) {
    // The entire input fit in one run. Read sorted rows in GetNext() directly from the
    // in-memory sorted run.
    DCHECK(sorted_runs_.back()->is_pinned());
    bool success;
    RETURN_IF_ERROR(sorted_runs_.back()->PrepareRead(&success));
    DCHECK(success) << "Should always be able to prepare pinned run for read.";
    return Status::OK();
  }

  // Unpin the final run to free up memory for the merge.
  // TODO: we could keep it in memory in some circumstances as an optimisation, once
  // we have a buffer pool with more reliable reservations (IMPALA-3200).
  RETURN_IF_ERROR(sorted_runs_.back()->UnpinAllBlocks());

  // Merge intermediate runs until we have a final merge set-up.
  // TODO: Attempt to allocate more memory before doing intermediate merges. This may
  // be possible if other operators have relinquished memory after the sort has built
  // its runs. This depends on more reliable reservations (IMPALA-3200)
  return MergeIntermediateRuns();
}

Status Sorter::GetNext(RowBatch* output_batch, bool* eos) {
  if (sorted_runs_.size() == 1) {
    DCHECK(sorted_runs_.back()->is_pinned());
    return sorted_runs_.back()->GetNext<false>(output_batch, eos);
  } else {
    return merger_->GetNext(output_batch, eos);
  }
}

Status Sorter::Reset() {
  DCHECK(unsorted_run_ == NULL) << "Cannot Reset() before calling InputDone()";
  merger_.reset();
  // Free resources from the current runs.
  CleanupAllRuns();
  obj_pool_.Clear();
  unsorted_run_ = obj_pool_.Add(
      new Run(this, output_row_desc_->tuple_descriptors()[0], true));
  RETURN_IF_ERROR(unsorted_run_->Init());
  return Status::OK();
}

void Sorter::Close() {
  CleanupAllRuns();
  block_mgr_->ClearReservations(block_mgr_client_);
  obj_pool_.Clear();
}

void Sorter::CleanupAllRuns() {
  Run::CleanupRuns(&sorted_runs_);
  Run::CleanupRuns(&merging_runs_);
  if (unsorted_run_ != NULL) unsorted_run_->DeleteAllBlocks();
  unsorted_run_ = NULL;
  if (merge_output_run_ != NULL) merge_output_run_->DeleteAllBlocks();
  merge_output_run_ = NULL;
}

Status Sorter::SortCurrentInputRun() {
  RETURN_IF_ERROR(unsorted_run_->FinalizeInput());

  {
    SCOPED_TIMER(in_mem_sort_timer_);
    RETURN_IF_ERROR(in_mem_tuple_sorter_->Sort(unsorted_run_));
  }
  sorted_runs_.push_back(unsorted_run_);
  sorted_data_size_->Add(unsorted_run_->TotalBytes());
  unsorted_run_ = NULL;

  RETURN_IF_CANCELLED(state_);
  return Status::OK();
}

Status Sorter::MergeIntermediateRuns() {
  DCHECK_GE(sorted_runs_.size(), 2);
  int pinned_blocks_per_run = has_var_len_slots_ ? 2 : 1;
  int max_runs_per_final_merge = MAX_BUFFERS_PER_MERGE / pinned_blocks_per_run;

  // During an intermediate merge, the one or two blocks from the output sorted run
  // that are being written must be pinned.
  int max_runs_per_intermediate_merge = max_runs_per_final_merge - 1;
  DCHECK_GT(max_runs_per_intermediate_merge, 1);

  while (true) {
    // An intermediate merge adds one merge to unmerged_sorted_runs_.
    // TODO: once we have reliable reservations (IMPALA-3200), we should calculate this
    // based on the available reservations.
    int num_runs_to_merge =
        min<int>(max_runs_per_intermediate_merge, sorted_runs_.size());

    DCHECK(merge_output_run_ == NULL) << "Should have finished previous merge.";
    // Create the merged run in case we need to do intermediate merges. We need the
    // output run and at least two input runs in memory to make progress on the
    // intermediate merges.
    // TODO: this isn't optimal: we could defer creating the merged run if we have
    // reliable reservations (IMPALA-3200).
    merge_output_run_ = obj_pool_.Add(
        new Run(this, output_row_desc_->tuple_descriptors()[0], false));
    RETURN_IF_ERROR(merge_output_run_->Init());
    RETURN_IF_ERROR(CreateMerger(num_runs_to_merge));

    // If CreateMerger() consumed all the sorted runs, we have set up the final merge.
    if (sorted_runs_.empty()) {
      // Don't need intermediate run for final merge.
      if (merge_output_run_ != NULL) {
        merge_output_run_->DeleteAllBlocks();
        merge_output_run_ = NULL;
      }
      return Status::OK();
    }
    RETURN_IF_ERROR(ExecuteIntermediateMerge(merge_output_run_));
    sorted_runs_.push_back(merge_output_run_);
    merge_output_run_ = NULL;
  }
  return Status::OK();
}

Status Sorter::CreateMerger(int max_num_runs) {
  DCHECK_GE(max_num_runs, 2);
  DCHECK_GE(sorted_runs_.size(), 2);
  // Clean up the runs from the previous merge.
  Run::CleanupRuns(&merging_runs_);

  // TODO: 'deep_copy_input' is set to true, which forces the merger to copy all rows
  // from the runs being merged. This is unnecessary overhead that is not required if we
  // correctly transfer resources.
  merger_.reset(
      new SortedRunMerger(compare_less_than_, output_row_desc_, profile_, true));

  vector<function<Status (RowBatch**)>> merge_runs;
  merge_runs.reserve(max_num_runs);
  for (int i = 0; i < max_num_runs; ++i) {
    Run* run = sorted_runs_.front();
    bool success;
    RETURN_IF_ERROR(run->PrepareRead(&success));
    if (!success) {
      // If we can merge at least two runs, we can continue, otherwise we have a problem
      // because we can't make progress on the merging.
      // TODO: IMPALA-3200: we should not need this logic once we have reliable
      // reservations (IMPALA-3200).
      if (merging_runs_.size() < 2) {
        return mem_tracker_->MemLimitExceeded(
            state_, Substitute(MERGE_FAILED_ERROR_MSG, merging_runs_.size()));
      }
      // Merge the runs that we were able to prepare.
      break;
    }
    // Run::GetNextBatch() is used by the merger to retrieve a batch of rows to merge
    // from this run.
    merge_runs.push_back(bind<Status>(mem_fn(&Run::GetNextBatch), run, _1));
    sorted_runs_.pop_front();
    merging_runs_.push_back(run);
  }
  RETURN_IF_ERROR(merger_->Prepare(merge_runs));

  num_merges_counter_->Add(1);
  return Status::OK();
}


Status Sorter::ExecuteIntermediateMerge(Sorter::Run* merged_run) {
  RowBatch intermediate_merge_batch(*output_row_desc_, state_->batch_size(),
      mem_tracker_);
  bool eos = false;
  while (!eos) {
    // Copy rows into the new run until done.
    int num_copied;
    RETURN_IF_CANCELLED(state_);
    RETURN_IF_ERROR(merger_->GetNext(&intermediate_merge_batch, &eos));
    RETURN_IF_ERROR(
        merged_run->AddIntermediateBatch(&intermediate_merge_batch, 0, &num_copied));

    DCHECK_EQ(num_copied, intermediate_merge_batch.num_rows());
    intermediate_merge_batch.Reset();
  }

  RETURN_IF_ERROR(merged_run->FinalizeInput());
  return Status::OK();
}

} // namespace impala
