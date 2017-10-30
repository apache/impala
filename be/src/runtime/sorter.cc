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

#include <limits>

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <gutil/strings/substitute.h>

#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/sorted-run-merger.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using boost::uniform_int;
using boost::mt19937_64;
using namespace strings;

namespace impala {

// Number of pinned pages required for a merge with fixed-length data only.
const int MIN_BUFFERS_PER_MERGE = 3;

// Maximum number of buffers to use in each merge to prevent sorter trying to grab
// all the memory when spilling. Given 8mb buffers, this limits the sorter to using
// 1GB of buffers when merging.
// TODO: this is an arbitrary limit. Once we have reliable reservations (IMPALA-3200)
// we should base this on the number of reservations.
const int MAX_BUFFERS_PER_MERGE = 128;

const string MERGE_FAILED_ERROR_MSG = "Failed to allocate page to merge spilled runs "
    "during sorting. Only $0 runs could be merged, but must be able to merge at least 2 "
    "to make progress. Reducing query concurrency or increasing the memory limit may "
    "help this query to complete successfully.";

/// Wrapper around BufferPool::PageHandle that tracks additional info about the page.
/// The Page can be in four states:
/// * Closed: The page starts in this state before Init() is called. Calling
///   ExtractBuffer() or Close() puts the page back in this state. No other operations
///   are valid on a closed page.
/// * In memory: the page is pinned and the buffer is in memory. data() is valid. The
///   page is in this state after Init(). If the page is pinned but not in memory, it
///   can be brought into this state by calling WaitForBuffer().
/// * Unpinned: the page was unpinned by calling Unpin(). It is invalid to access the
///   page's buffer.
/// * Pinned but not in memory: Pin() was called on the unpinned page, but
///   WaitForBuffer() has not been called. It is invalid to access the page's buffer.
class Sorter::Page {
 public:
  Page() { Reset(); }

  /// Create a new page of length 'sorter->page_len_' bytes using
  /// 'sorter->buffer_pool_client_'. Caller must ensure the client has enough
  /// reservation for the page.
  Status Init(Sorter* sorter) WARN_UNUSED_RESULT {
    const BufferPool::BufferHandle* page_buffer;
    RETURN_IF_ERROR(pool()->CreatePage(sorter->buffer_pool_client_, sorter->page_len_,
        &handle_, &page_buffer));
    data_ = page_buffer->data();
    return Status::OK();
  }

  /// Extract the buffer from the page. The page must be in memory. When this function
  /// returns the page is closed.
  BufferPool::BufferHandle ExtractBuffer(BufferPool::ClientHandle* client) {
    DCHECK(data_ != nullptr) << "Page must be in memory";
    BufferPool::BufferHandle buffer;
    Status status = pool()->ExtractBuffer(client, &handle_, &buffer);
    DCHECK(status.ok()) << "Page was in memory, ExtractBuffer() shouldn't fail";
    Reset();
    return buffer;
  }

  /// Allocate 'len' bytes in the current page. The page must be in memory, and the
  /// amount to allocate cannot exceed BytesRemaining().
  uint8_t* AllocateBytes(int64_t len) {
    DCHECK_GE(len, 0);
    DCHECK_LE(len, BytesRemaining());
    DCHECK(data_ != nullptr);
    uint8_t* result = data_ + valid_data_len_;
    valid_data_len_ += len;
    return result;
  }

  /// Free the last 'len' bytes allocated from AllocateBytes(). The page must be in
  /// memory.
  void FreeBytes(int64_t len) {
    DCHECK_GE(len, 0);
    DCHECK_LE(len, valid_data_len_);
    DCHECK(data_ != nullptr);
    valid_data_len_ -= len;
  }

  /// Return number of bytes remaining in page.
  int64_t BytesRemaining() { return len() - valid_data_len_; }

  /// Brings a pinned page into memory, if not already in memory, and sets 'data_' to
  /// point to the page's buffer.
  Status WaitForBuffer() WARN_UNUSED_RESULT {
    DCHECK(handle_.is_pinned());
    if (data_ != nullptr) return Status::OK();
    const BufferPool::BufferHandle* page_buffer;
    RETURN_IF_ERROR(handle_.GetBuffer(&page_buffer));
    data_ = page_buffer->data();
    return Status::OK();
  }

  /// Helper to pin the page. Caller must ensure the client has enough reservation
  /// remaining to pin the page. Only valid to call on an unpinned page.
  Status Pin(BufferPool::ClientHandle* client) WARN_UNUSED_RESULT {
    DCHECK(!handle_.is_pinned());
    return pool()->Pin(client, &handle_);
  }

  /// Helper to unpin the page.
  void Unpin(BufferPool::ClientHandle* client) {
    pool()->Unpin(client, &handle_);
    data_ = nullptr;
  }

  /// Destroy the page with 'client'.
  void Close(BufferPool::ClientHandle* client) {
    pool()->DestroyPage(client, &handle_);
    Reset();
  }

  int64_t valid_data_len() const { return valid_data_len_; }
  /// Returns a pointer to the start of the page's buffer. Only valid to call if the
  /// page is in memory.
  uint8_t* data() const {
    DCHECK(data_ != nullptr);
    return data_;
  }
  int64_t len() const { return handle_.len(); }
  bool is_open() const { return handle_.is_open(); }
  bool is_pinned() const { return handle_.is_pinned(); }
  std::string DebugString() const { return handle_.DebugString(); }

 private:
  /// Reset the page to an unitialized state. 'handle_' must already be closed.
  void Reset() {
    DCHECK(!handle_.is_open());
    valid_data_len_ = 0;
    data_ = nullptr;
  }

  /// Helper to get the singleton buffer pool.
  static BufferPool* pool() { return ExecEnv::GetInstance()->buffer_pool(); }

  BufferPool::PageHandle handle_;

  /// Length of valid data written to the page.
  int64_t valid_data_len_;

  /// Cached pointer to the buffer in 'handle_'. NULL if the page is unpinned. May be NULL
  /// or not NULL if the page is pinned. Can be populated by calling WaitForBuffer() on a
  /// pinned page.
  uint8_t* data_;
};

/// A run is a sequence of tuples. The run can be sorted or unsorted (in which case the
/// Sorter will sort it). A run comprises a sequence of fixed-length pages containing the
/// tuples themselves (i.e. fixed-len slots that may contain ptrs to var-length data), and
/// an optional sequence of var-length pages containing the var-length data.
///
/// Runs are either "initial runs" constructed from the sorter's input by evaluating
/// the expressions in 'sort_tuple_exprs_' or "intermediate runs" constructed
/// by merging already-sorted runs. Initial runs are sorted in-place in memory. Once
/// sorted, runs can be spilled to disk to free up memory. Sorted runs are merged by
/// SortedRunMerger, either to produce the final sorted output or to produce another
/// sorted run.
///
/// The expected calling sequence of functions is as follows:
/// * Init() to initialize the run and allocate initial pages.
/// * Add*Batch() to add batches of tuples to the run.
/// * FinalizeInput() to signal that no more batches will be added.
/// * If the run is unsorted, it must be sorted. After that set_sorted() must be called.
/// * Once sorted, the run is ready to read in sorted order for merging or final output.
/// * PrepareRead() to allocate resources for reading the run.
/// * GetNext() (if there was a single run) or GetNextBatch() (when merging multiple runs)
///   to read from the run.
/// * Once reading is done, CloseAllPages() should be called to free resources.
class Sorter::Run {
 public:
  Run(Sorter* parent, TupleDescriptor* sort_tuple_desc, bool initial_run);

  ~Run() {
    DCHECK(fixed_len_pages_.empty());
    DCHECK(var_len_pages_.empty());
    DCHECK(!var_len_copy_page_.is_open());
  }

  /// Initialize the run for input rows by allocating the minimum number of required
  /// pages - one page for fixed-len data added to fixed_len_pages_, one for the
  /// initially unsorted var-len data added to var_len_pages_, and one to copy sorted
  /// var-len data into var_len_copy_page_.
  Status Init() WARN_UNUSED_RESULT;

  /// Add the rows from 'batch' starting at 'start_index' to the current run. Returns the
  /// number of rows actually added in 'num_processed'. If the run is full (no more pages
  /// can be allocated), 'num_processed' may be less than the number of remaining rows in
  /// the batch. AddInputBatch() materializes the input rows using the expressions in
  /// sorter_->sort_tuple_expr_evals_, while AddIntermediateBatch() just copies rows.
  Status AddInputBatch(
      RowBatch* batch, int start_index, int* num_processed) WARN_UNUSED_RESULT {
    DCHECK(initial_run_);
    if (has_var_len_slots_) {
      return AddBatchInternal<true, true>(batch, start_index, num_processed);
    } else {
      return AddBatchInternal<false, true>(batch, start_index, num_processed);
    }
  }

  Status AddIntermediateBatch(
      RowBatch* batch, int start_index, int* num_processed) WARN_UNUSED_RESULT {
    DCHECK(!initial_run_);
    if (has_var_len_slots_) {
      return AddBatchInternal<true, false>(batch, start_index, num_processed);
    } else {
      return AddBatchInternal<false, false>(batch, start_index, num_processed);
    }
  }

  /// Called after the final call to Add*Batch() to do any bookkeeping necessary to
  /// finalize the run. Must be called before sorting or merging the run.
  Status FinalizeInput() WARN_UNUSED_RESULT;

  /// Unpins all the pages in a sorted run. Var-length column data is copied into new
  /// pages in sorted order. Pointers in the original tuples are converted to offsets
  /// from the beginning of the sequence of var-len data pages. Returns an error and
  /// may leave some pages pinned if an error is encountered.
  Status UnpinAllPages() WARN_UNUSED_RESULT;

  /// Closes all pages and clears vectors of pages.
  void CloseAllPages();

  /// Prepare to read a sorted run. Pins the first page(s) in the run if the run was
  /// previously unpinned. If the run was unpinned, try to pin the initial fixed and
  /// var len pages in the run. If it couldn't pin them, set pinned to false.
  /// In that case, none of the initial pages will be pinned and it is valid to
  /// call PrepareRead() again to retry pinning. pinned is always set to
  /// true if the run was pinned.
  Status PrepareRead(bool* pinned) WARN_UNUSED_RESULT;

  /// Interface for merger - get the next batch of rows from this run. This run still
  /// owns the returned batch. Calls GetNext(RowBatch*, bool*).
  Status GetNextBatch(RowBatch** sorted_batch) WARN_UNUSED_RESULT;

  /// Fill output_batch with rows from this run. If CONVERT_OFFSET_TO_PTR is true, offsets
  /// in var-length slots are converted back to pointers. Only row pointers are copied
  /// into output_batch. eos is set to true after all rows from the run are returned.
  /// If eos is true, the returned output_batch has zero rows and has no attached pages.
  /// If this run was unpinned, one page (two if there are var-len slots) is pinned while
  /// rows are filled into output_batch. The page is unpinned before the next page is
  /// pinned, so at most one (two if there are var-len slots) page(s) will be pinned at
  /// once. If the run was pinned, the pages are not unpinned and each page is attached
  /// to 'output_batch' once all rows referencing data in the page have been returned,
  /// either in the current batch or previous batches. In both pinned and unpinned cases,
  /// all rows in output_batch will reference at most one fixed-len and one var-len page.
  template <bool CONVERT_OFFSET_TO_PTR>
  Status GetNext(RowBatch* output_batch, bool* eos) WARN_UNUSED_RESULT;

  /// Delete all pages in 'runs' and clear 'runs'.
  static void CleanupRuns(deque<Run*>* runs) {
    for (Run* run : *runs) {
      run->CloseAllPages();
    }
    runs->clear();
  }

  /// Return total amount of fixed and var len data in run, not including pages that
  /// were already transferred or closed.
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
  Status AddBatchInternal(
      RowBatch* batch, int start_index, int* num_processed) WARN_UNUSED_RESULT;

  /// Finalize the list of pages: delete empty final pages and unpin the previous page
  /// if the run is unpinned.
  Status FinalizePages(vector<Page>* pages) WARN_UNUSED_RESULT;

  /// Collect the non-null var-len (e.g. STRING) slots from 'src' in 'var_len_values' and
  /// return the total length of all var-len values in 'total_var_len'.
  void CollectNonNullVarSlots(
      Tuple* src, vector<StringValue*>* var_len_values, int* total_var_len);

  enum AddPageMode { KEEP_PREV_PINNED, UNPIN_PREV };

  /// Try to extend the current run by a page. If 'mode' is KEEP_PREV_PINNED, try to
  /// allocate a new page, which may fail to extend the run due to lack of memory. If
  /// mode is 'UNPIN_PREV', unpin the previous page in page_sequence before allocating
  /// and adding a new page - this never fails due to lack of memory.
  ///
  /// Returns an error status only if the buffer pool returns an error. If no error is
  /// encountered, sets 'added' to indicate whether the run was extended and returns
  /// Status::OK(). The new page is appended to 'page_sequence'.
  Status TryAddPage(
      AddPageMode mode, vector<Page>* page_sequence, bool* added) WARN_UNUSED_RESULT;

  /// Adds a new page to 'page_sequence' by a page. Caller must ensure enough
  /// reservation is available to create the page.
  ///
  /// Returns an error status only if the buffer pool returns an error. If an error
  /// is returned 'page_sequence' is left unmodified.
  Status AddPage(vector<Page>* page_sequence) WARN_UNUSED_RESULT;

  /// Advance to the next read page. If the run is pinned, has no effect. If the run
  /// is unpinned, atomically pin the page at 'page_index' + 1 in 'pages' and delete
  /// the page at 'page_index'.
  Status PinNextReadPage(vector<Page>* pages, int page_index) WARN_UNUSED_RESULT;

  /// Copy the StringValues in 'var_values' to 'dest' in order and update the StringValue
  /// ptrs in 'dest' to point to the copied data.
  void CopyVarLenData(const vector<StringValue*>& var_values, uint8_t* dest);

  /// Copy the StringValues in 'var_values' to 'dest' in order. Update the StringValue
  /// ptrs in 'dest' to contain a packed offset for the copied data comprising
  /// page_index and the offset relative to page_start.
  void CopyVarLenDataConvertOffset(const vector<StringValue*>& var_values, int page_index,
      const uint8_t* page_start, uint8_t* dest);

  /// Convert encoded offsets to valid pointers in tuple with layout 'sort_tuple_desc_'.
  /// 'tuple' is modified in-place. Returns true if the pointers refer to the page at
  /// 'var_len_pages_index_' and were successfully converted or false if the var len
  /// data is in the next page, in which case 'tuple' is unmodified.
  bool ConvertOffsetsToPtrs(Tuple* tuple);

  /// Returns true if we have var-len pages in the run.
  inline bool HasVarLenPages() const {
    // Shouldn't have any pages unless there are slots.
    DCHECK(var_len_pages_.empty() || has_var_len_slots_);
    return !var_len_pages_.empty();
  }

  static int NumOpenPages(const vector<Page>& pages) {
    int count = 0;
    for (const Page& page : pages) {
      if (page.is_open()) ++count;
    }
    return count;
  }

  /// Close all open pages and clear vector.
  void DeleteAndClearPages(vector<Page>* pages) {
    for (Page& page : *pages) {
      if (page.is_open()) page.Close(sorter_->buffer_pool_client_);
    }
    pages->clear();
  }

  /// Parent sorter object.
  Sorter* const sorter_;

  /// Materialized sort tuple. Input rows are materialized into 1 tuple (with descriptor
  /// sort_tuple_desc_) before sorting.
  const TupleDescriptor* sort_tuple_desc_;

  /// The size in bytes of the sort tuple.
  const int sort_tuple_size_;

  /// Number of tuples per page in a run. This gets multiplied with
  /// TupleIterator::page_index_ in various places and to make sure we don't overflow the
  /// result of that operation we make this int64_t here.
  const int64_t page_capacity_;

  const bool has_var_len_slots_;

  /// True if this is an initial run. False implies this is an sorted intermediate run
  /// resulting from merging other runs.
  const bool initial_run_;

  /// True if all pages in the run are pinned. Initial runs start off pinned and
  /// can be unpinned. Intermediate runs are always unpinned.
  bool is_pinned_;

  /// True after FinalizeInput() is called. No more tuples can be added after the
  /// run is finalized.
  bool is_finalized_;

  /// True if the tuples in the run are currently in sorted order.
  /// Always true for intermediate runs.
  bool is_sorted_;

  /// Sequence of pages in this run containing the fixed-length portion of the sort
  /// tuples comprising this run. The data pointed to by the var-len slots are in
  /// var_len_pages_. A run can have zero pages if no rows are appended.
  /// If the run is sorted, the tuples in fixed_len_pages_ will be in sorted order.
  /// fixed_len_pages_[i] is closed iff it has been transferred or deleted.
  vector<Page> fixed_len_pages_;

  /// Sequence of pages in this run containing the var-length data corresponding to the
  /// var-length columns from fixed_len_pages_. In intermediate runs, the var-len data is
  /// always stored in the same order as the fixed-length tuples. In initial runs, the
  /// var-len data is initially in unsorted order, but is reshuffled into sorted order in
  /// UnpinAllPages(). A run can have no var len pages if there are no var len slots or
  /// if all the var len data is empty or NULL.
  /// var_len_pages_[i] is closed iff it has been transferred or deleted.
  vector<Page> var_len_pages_;

  /// For initial unsorted runs, an extra pinned page is needed to reorder var-len data
  /// into fixed order in UnpinAllPages(). 'var_len_copy_page_' stores this extra
  /// page. Deleted in UnpinAllPages().
  /// TODO: in case of in-memory runs, this could be deleted earlier to free up memory.
  Page var_len_copy_page_;

  /// Number of tuples added so far to this run.
  int64_t num_tuples_;

  /// Number of tuples returned via GetNext(), maintained for debug purposes.
  int64_t num_tuples_returned_;

  /// Used to implement GetNextBatch() interface required for the merger.
  scoped_ptr<RowBatch> buffered_batch_;

  /// Members used when a run is read in GetNext().
  /// The index into 'fixed_' and 'var_len_pages_' of the pages being read in GetNext().
  int fixed_len_pages_index_;
  int var_len_pages_index_;

  /// If true, the last call to GetNext() reached the end of the previous fixed or
  /// var-len page. The next call to GetNext() must increment 'fixed_len_pages_index_'
  /// or 'var_len_pages_index_'. It must also pin the next page if the run is unpinned.
  bool end_of_fixed_len_page_;
  bool end_of_var_len_page_;

  /// Offset into the current fixed length data page being processed.
  int fixed_len_page_offset_;
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
      buffer_end_index_(-1), page_index_(-1) { }

  /// Create an iterator pointing to the first tuple in the run.
  static inline TupleIterator Begin(Sorter::Run* run) { return TupleIterator(run, 0); }

  /// Create an iterator pointing one past the end of the run.
  static inline TupleIterator End(Sorter::Run* run) {
    return TupleIterator(run, run->num_tuples());
  }

  /// Increments 'index_' and sets 'tuple_' to point to the next tuple in the run.
  /// Increments 'page_index_' and advances to the next page if the next tuple is in
  /// the next page. Can be advanced one past the last tuple in the run, but is not
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
  // Move to the next page in the run (or do nothing if at end of run).
  // This is the slow path for Next();
  void NextPage(Sorter::Run* run, int tuple_size);

  // Move to the previous page in the run (or do nothing if at beginning of run).
  // This is the slow path for Prev();
  void PrevPage(Sorter::Run* run, int tuple_size);

  /// Index of the current tuple in the run.
  /// Can be -1 or run->num_rows() if Next() or Prev() moves iterator outside of run.
  int64_t index_;

  /// Pointer to the current tuple.
  /// Will be an invalid pointer outside of current buffer if Next() or Prev() moves
  /// iterator outside of run.
  uint8_t* tuple_;

  /// Indices of start and end tuples of page at page_index_. I.e. the current page
  /// has tuples with indices in range [buffer_start_index_, buffer_end_index).
  int64_t buffer_start_index_;
  int64_t buffer_end_index_;

  /// Index into fixed_len_pages_ of the page containing the current tuple.
  /// If index_ is negative or past end of run, will point to the first or last page
  /// in run respectively.
  int page_index_;
};

/// Sorts a sequence of tuples from a run in place using a provided tuple comparator.
/// Quick sort is used for sequences of tuples larger that 16 elements, and insertion sort
/// is used for smaller sequences. The TupleSorter is initialized with a RuntimeState
/// instance to check for cancellation during an in-memory sort.
class Sorter::TupleSorter {
 public:
  TupleSorter(Sorter* parent, const TupleRowComparator& comparator, int64_t page_size,
      int tuple_size, RuntimeState* state);

  ~TupleSorter();

  /// Performs a quicksort for tuples in 'run' followed by an insertion sort to
  /// finish smaller ranges. Only valid to call if this is an initial run that has not
  /// yet been sorted. Returns an error status if any error is encountered or if the
  /// query is cancelled.
  Status Sort(Run* run) WARN_UNUSED_RESULT;

 private:
  static const int INSERTION_THRESHOLD = 16;

  Sorter* const parent_;

  /// Size of the tuples in memory.
  const int tuple_size_;

  /// Tuple comparator with method Less() that returns true if lhs < rhs.
  const TupleRowComparator& comparator_;

  /// Number of times comparator_.Less() can be invoked again before
  /// comparator_. expr_results_pool_.Clear() needs to be called.
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

  /// Wrapper around comparator_.Less(). Also call expr_results_pool_.Clear()
  /// on every 'state_->batch_size()' invocations of comparator_.Less(). Returns true
  /// if 'lhs' is less than 'rhs'.
  bool Less(const TupleRow* lhs, const TupleRow* rhs);

  /// Perform an insertion sort for rows in the range [begin, end) in a run.
  /// Only valid to call for ranges of size at least 1.
  Status InsertionSort(
      const TupleIterator& begin, const TupleIterator& end) WARN_UNUSED_RESULT;

  /// Partitions the sequence of tuples in the range [begin, end) in a run into two
  /// groups around the pivot tuple - i.e. tuples in first group are <= the pivot, and
  /// tuples in the second group are >= pivot. Tuples are swapped in place to create the
  /// groups and the index to the first element in the second group is returned in 'cut'.
  /// Return an error status if any error is encountered or if the query is cancelled.
  Status Partition(TupleIterator begin, TupleIterator end, const Tuple* pivot,
      TupleIterator* cut) WARN_UNUSED_RESULT;

  /// Performs a quicksort of rows in the range [begin, end) followed by insertion sort
  /// for smaller groups of elements. Return an error status for any errors or if the
  /// query is cancelled.
  Status SortHelper(TupleIterator begin, TupleIterator end) WARN_UNUSED_RESULT;

  /// Select a pivot to partition [begin, end).
  Tuple* SelectPivot(TupleIterator begin, TupleIterator end);

  /// Return median of three tuples according to the sort comparator.
  Tuple* MedianOfThree(Tuple* t1, Tuple* t2, Tuple* t3);

  /// Swaps tuples pointed to by left and right using 'swap_tuple'.
  static void Swap(Tuple* left, Tuple* right, Tuple* swap_tuple, int tuple_size);
};

// Sorter::Run methods
Sorter::Run::Run(Sorter* parent, TupleDescriptor* sort_tuple_desc, bool initial_run)
  : sorter_(parent),
    sort_tuple_desc_(sort_tuple_desc),
    sort_tuple_size_(sort_tuple_desc->byte_size()),
    page_capacity_(parent->page_len_ / sort_tuple_size_),
    has_var_len_slots_(sort_tuple_desc->HasVarlenSlots()),
    initial_run_(initial_run),
    is_pinned_(initial_run),
    is_finalized_(false),
    is_sorted_(!initial_run),
    num_tuples_(0) {}

Status Sorter::Run::Init() {
  int num_to_create = 1 + has_var_len_slots_ + (has_var_len_slots_ && initial_run_);
  int64_t required_mem = num_to_create * sorter_->page_len_;
  if (!sorter_->buffer_pool_client_->IncreaseReservationToFit(required_mem)) {
    return Status(Substitute(
        "Unexpected error trying to reserve $0 bytes for a sorted run: $2",
        required_mem, sorter_->buffer_pool_client_->DebugString()));
  }

  RETURN_IF_ERROR(AddPage(&fixed_len_pages_));
  if (has_var_len_slots_) {
    RETURN_IF_ERROR(AddPage(&var_len_pages_));
    if (initial_run_) {
      // Need additional var len page to reorder var len data in UnpinAllPages().
      RETURN_IF_ERROR(var_len_copy_page_.Init(sorter_));
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
Status Sorter::Run::AddBatchInternal(
    RowBatch* batch, int start_index, int* num_processed) {
  DCHECK(!is_finalized_);
  DCHECK(!fixed_len_pages_.empty());
  DCHECK_EQ(HAS_VAR_LEN_SLOTS, has_var_len_slots_);
  DCHECK_EQ(INITIAL_RUN, initial_run_);

  *num_processed = 0;
  Page* cur_fixed_len_page = &fixed_len_pages_.back();

  if (!INITIAL_RUN) {
    // For intermediate merges, the input row is the sort tuple.
    DCHECK_EQ(batch->row_desc()->tuple_descriptors().size(), 1);
    DCHECK_EQ(batch->row_desc()->tuple_descriptors()[0], sort_tuple_desc_);
  }

  /// Keep initial unsorted runs pinned in memory so we can sort them.
  const AddPageMode add_mode = INITIAL_RUN ? KEEP_PREV_PINNED : UNPIN_PREV;

  // Input rows are copied/materialized into tuples allocated in fixed_len_pages_.
  // The variable length column data are copied into pages stored in var_len_pages_.
  // Input row processing is split into two loops.
  // The inner loop processes as many input rows as will fit in cur_fixed_len_page.
  // The outer loop allocates a new page for fixed-len data if the input batch is
  // not exhausted.

  // cur_input_index is the index into the input 'batch' of the current input row being
  // processed.
  int cur_input_index = start_index;
  vector<StringValue*> string_values;
  string_values.reserve(sort_tuple_desc_->string_slots().size());
  while (cur_input_index < batch->num_rows()) {
    // tuples_remaining is the number of tuples to copy/materialize into
    // cur_fixed_len_page.
    int tuples_remaining = cur_fixed_len_page->BytesRemaining() / sort_tuple_size_;
    tuples_remaining = min(batch->num_rows() - cur_input_index, tuples_remaining);

    for (int i = 0; i < tuples_remaining; ++i) {
      int total_var_len = 0;
      TupleRow* input_row = batch->GetRow(cur_input_index);
      Tuple* new_tuple =
          reinterpret_cast<Tuple*>(cur_fixed_len_page->AllocateBytes(sort_tuple_size_));
      if (INITIAL_RUN) {
        new_tuple->MaterializeExprs<HAS_VAR_LEN_SLOTS, true>(input_row,
            *sort_tuple_desc_, sorter_->sort_tuple_expr_evals_, NULL,
            &string_values, &total_var_len);
        if (total_var_len > sorter_->page_len_) {
          int64_t max_row_size = sorter_->state_->query_options().max_row_size;
          return Status(TErrorCode::MAX_ROW_SIZE,
              PrettyPrinter::Print(total_var_len, TUnit::BYTES), sorter_->node_id_,
              PrettyPrinter::Print(max_row_size, TUnit::BYTES));
        }
      } else {
        memcpy(new_tuple, input_row->GetTuple(0), sort_tuple_size_);
        if (HAS_VAR_LEN_SLOTS) {
          CollectNonNullVarSlots(new_tuple, &string_values, &total_var_len);
        }
      }

      if (HAS_VAR_LEN_SLOTS) {
        DCHECK_GT(var_len_pages_.size(), 0);
        Page* cur_var_len_page = &var_len_pages_.back();
        if (cur_var_len_page->BytesRemaining() < total_var_len) {
          bool added;
          RETURN_IF_ERROR(TryAddPage(add_mode, &var_len_pages_, &added));
          if (added) {
            cur_var_len_page = &var_len_pages_.back();
          } else {
            // There was not enough space in the last var-len page for this tuple, and
            // the run could not be extended. Return the fixed-len allocation and exit.
            cur_fixed_len_page->FreeBytes(sort_tuple_size_);
            return Status::OK();
          }
        }

        // Sorting of tuples containing array values is not implemented. The planner
        // combined with projection should guarantee that none are in each tuple.
        for (const SlotDescriptor* coll_slot: sort_tuple_desc_->collection_slots()) {
          DCHECK(new_tuple->IsNull(coll_slot->null_indicator_offset()));
        }

        uint8_t* var_data_ptr = cur_var_len_page->AllocateBytes(total_var_len);
        if (INITIAL_RUN) {
          CopyVarLenData(string_values, var_data_ptr);
        } else {
          DCHECK_EQ(&var_len_pages_.back(), cur_var_len_page);
          CopyVarLenDataConvertOffset(string_values, var_len_pages_.size() - 1,
              cur_var_len_page->data(), var_data_ptr);
        }
      }
      ++num_tuples_;
      ++*num_processed;
      ++cur_input_index;
    }

    // If there are still rows left to process, get a new page for the fixed-length
    // tuples. If the run is already too long, return.
    if (cur_input_index < batch->num_rows()) {
      bool added;
      RETURN_IF_ERROR(TryAddPage(add_mode, &fixed_len_pages_, &added));
      if (!added) return Status::OK();
      cur_fixed_len_page = &fixed_len_pages_.back();
    }
  }
  return Status::OK();
}

Status Sorter::Run::FinalizeInput() {
  DCHECK(!is_finalized_);

  RETURN_IF_ERROR(FinalizePages(&fixed_len_pages_));
  if (has_var_len_slots_) {
    RETURN_IF_ERROR(FinalizePages(&var_len_pages_));
  }
  is_finalized_ = true;
  return Status::OK();
}

Status Sorter::Run::FinalizePages(vector<Page>* pages) {
  DCHECK_GT(pages->size(), 0);
  Page* last_page = &pages->back();
  if (last_page->valid_data_len() > 0) {
    DCHECK_EQ(initial_run_, is_pinned_);
    if (!is_pinned_) {
      // Unpin the last page of this unpinned run. We've finished writing the run so
      // all pages in the run can now be unpinned.
      last_page->Unpin(sorter_->buffer_pool_client_);
    }
  } else {
    last_page->Close(sorter_->buffer_pool_client_);
    pages->pop_back();
  }
  return Status::OK();
}

void Sorter::Run::CloseAllPages() {
  DeleteAndClearPages(&fixed_len_pages_);
  DeleteAndClearPages(&var_len_pages_);
  if (var_len_copy_page_.is_open()) {
    var_len_copy_page_.Close(sorter_->buffer_pool_client_);
  }
}

Status Sorter::Run::UnpinAllPages() {
  DCHECK(is_sorted_);
  DCHECK(initial_run_);
  DCHECK(is_pinned_);
  DCHECK(is_finalized_);
  // A list of var len pages to replace 'var_len_pages_'. Note that after we are done
  // we may have a different number of pages, because internal fragmentation may leave
  // slightly different amounts of wasted space at the end of each page.
  // We need to be careful to clean up these pages if we run into an error in this method.
  vector<Page> sorted_var_len_pages;
  sorted_var_len_pages.reserve(var_len_pages_.size());

  vector<StringValue*> string_values;
  int total_var_len;
  string_values.reserve(sort_tuple_desc_->string_slots().size());
  Page* cur_sorted_var_len_page = NULL;
  if (HasVarLenPages()) {
    DCHECK(var_len_copy_page_.is_open());
    sorted_var_len_pages.push_back(move(var_len_copy_page_));
    cur_sorted_var_len_page = &sorted_var_len_pages.back();
  } else if (has_var_len_slots_) {
    // If we don't have any var-len pages, clean up the copy page.
    DCHECK(var_len_copy_page_.is_open());
    var_len_copy_page_.Close(sorter_->buffer_pool_client_);
  } else {
    DCHECK(!var_len_copy_page_.is_open());
  }

  Status status;
  for (int i = 0; i < fixed_len_pages_.size(); ++i) {
    Page* cur_fixed_page = &fixed_len_pages_[i];
    // Skip converting the pointers if no var-len slots, or if all the values are null
    // or zero-length. This will possibly leave zero-length pointers pointing to
    // arbitrary memory, but zero-length data cannot be dereferenced anyway.
    if (HasVarLenPages()) {
      for (int page_offset = 0; page_offset < cur_fixed_page->valid_data_len();
           page_offset += sort_tuple_size_) {
        Tuple* cur_tuple = reinterpret_cast<Tuple*>(cur_fixed_page->data() + page_offset);
        CollectNonNullVarSlots(cur_tuple, &string_values, &total_var_len);
        DCHECK(cur_sorted_var_len_page->is_open());
        if (cur_sorted_var_len_page->BytesRemaining() < total_var_len) {
          bool added;
          status = TryAddPage(UNPIN_PREV, &sorted_var_len_pages, &added);
          if (!status.ok()) goto cleanup_pages;
          DCHECK(added) << "TryAddPage() with UNPIN_PREV should not fail to add";
          cur_sorted_var_len_page = &sorted_var_len_pages.back();
        }
        uint8_t* var_data_ptr = cur_sorted_var_len_page->AllocateBytes(total_var_len);
        DCHECK_EQ(&sorted_var_len_pages.back(), cur_sorted_var_len_page);
        CopyVarLenDataConvertOffset(string_values, sorted_var_len_pages.size() - 1,
            cur_sorted_var_len_page->data(), var_data_ptr);
      }
    }
    cur_fixed_page->Unpin(sorter_->buffer_pool_client_);
  }

  if (HasVarLenPages()) {
    DCHECK_GT(sorted_var_len_pages.back().valid_data_len(), 0);
    sorted_var_len_pages.back().Unpin(sorter_->buffer_pool_client_);
  }

  // Clear var_len_pages_ and replace with it with the contents of sorted_var_len_pages
  DeleteAndClearPages(&var_len_pages_);
  sorted_var_len_pages.swap(var_len_pages_);
  is_pinned_ = false;
  sorter_->spilled_runs_counter_->Add(1);
  return Status::OK();

cleanup_pages:
  DeleteAndClearPages(&sorted_var_len_pages);
  return status;
}

Status Sorter::Run::PrepareRead(bool* pinned) {
  DCHECK(is_finalized_);
  DCHECK(is_sorted_);

  fixed_len_pages_index_ = 0;
  fixed_len_page_offset_ = 0;
  var_len_pages_index_ = 0;
  end_of_fixed_len_page_ = end_of_var_len_page_ = fixed_len_pages_.empty();
  num_tuples_returned_ = 0;

  buffered_batch_.reset(new RowBatch(
      sorter_->output_row_desc_, sorter_->state_->batch_size(), sorter_->mem_tracker_));

  // If the run is pinned, all pages are already pinned, so we're ready to read.
  if (is_pinned_) {
    *pinned = true;
    return Status::OK();
  }

  int num_to_pin = (fixed_len_pages_.size() > 0 ? 1 : 0) + (HasVarLenPages() ? 1 : 0);
  int64_t required_mem = num_to_pin * sorter_->page_len_;
  if (!sorter_->buffer_pool_client_->IncreaseReservationToFit(required_mem)) {
    *pinned = false;
    return Status::OK();
  }

  // Attempt to pin the first fixed and var-length pages.
  if (fixed_len_pages_.size() > 0) {
    RETURN_IF_ERROR(fixed_len_pages_[0].Pin(sorter_->buffer_pool_client_));
  }
  if (HasVarLenPages()) {
    RETURN_IF_ERROR(var_len_pages_[0].Pin(sorter_->buffer_pool_client_));
  }
  *pinned = true;
  return Status::OK();
}

Status Sorter::Run::GetNextBatch(RowBatch** output_batch) {
  DCHECK(buffered_batch_ != NULL);
  buffered_batch_->Reset();
  // Fill more rows into buffered_batch_.
  bool eos;
  if (HasVarLenPages() && !is_pinned_) {
    RETURN_IF_ERROR(GetNext<true>(buffered_batch_.get(), &eos));
  } else {
    RETURN_IF_ERROR(GetNext<false>(buffered_batch_.get(), &eos));
  }

  if (eos) {
    // Setting output_batch to NULL signals eos to the caller, so GetNext() is not
    // allowed to attach resources to the batch on eos.
    DCHECK_EQ(buffered_batch_->num_rows(), 0);
    DCHECK_EQ(buffered_batch_->num_buffers(), 0);
    *output_batch = NULL;
    return Status::OK();
  }
  *output_batch = buffered_batch_.get();
  return Status::OK();
}

template <bool CONVERT_OFFSET_TO_PTR>
Status Sorter::Run::GetNext(RowBatch* output_batch, bool* eos) {
  // Var-len offsets are converted only when reading var-len data from unpinned runs.
  // We shouldn't convert var len offsets if there are no pages, since in that case
  // they must all be null or zero-length strings, which don't point into a valid page.
  DCHECK_EQ(CONVERT_OFFSET_TO_PTR, HasVarLenPages() && !is_pinned_);

  if (end_of_fixed_len_page_
      && fixed_len_pages_index_ >= static_cast<int>(fixed_len_pages_.size()) - 1) {
    if (is_pinned_) {
      // All pages were previously attached to output batches. GetNextBatch() assumes
      // that we don't attach resources to the batch on eos.
      DCHECK_EQ(NumOpenPages(fixed_len_pages_), 0);
      DCHECK_EQ(NumOpenPages(var_len_pages_), 0);

      // Flush resources in case we are in a subplan and need to allocate more pages
      // when the node is reopened.
      output_batch->MarkFlushResources();
    } else {
      // We held onto the last fixed or var len blocks without transferring them to the
      // caller. We signalled MarkNeedsDeepCopy() to the caller, so we can safely delete
      // them now to free memory.
      if (!fixed_len_pages_.empty()) DCHECK_EQ(NumOpenPages(fixed_len_pages_), 1);
      if (!var_len_pages_.empty()) DCHECK_EQ(NumOpenPages(var_len_pages_), 1);
    }
    CloseAllPages();
    *eos = true;
    DCHECK_EQ(num_tuples_returned_, num_tuples_);
    return Status::OK();
  }

  // Advance the fixed or var len page if we reached the end in the previous call to
  // GetNext().
  if (end_of_fixed_len_page_) {
    RETURN_IF_ERROR(PinNextReadPage(&fixed_len_pages_, fixed_len_pages_index_));
    ++fixed_len_pages_index_;
    fixed_len_page_offset_ = 0;
    end_of_fixed_len_page_ = false;
  }
  if (end_of_var_len_page_) {
    RETURN_IF_ERROR(PinNextReadPage(&var_len_pages_, var_len_pages_index_));
    ++var_len_pages_index_;
    end_of_var_len_page_ = false;
  }

  // Fills rows into the output batch until a page boundary is reached.
  Page* fixed_len_page = &fixed_len_pages_[fixed_len_pages_index_];
  DCHECK(fixed_len_page != NULL);

  // Ensure we have a reference to the fixed-length page's buffer.
  RETURN_IF_ERROR(fixed_len_page->WaitForBuffer());

  // If we're converting offsets into unpinned var-len pages, make sure the
  // current var-len page is in memory.
  if (CONVERT_OFFSET_TO_PTR && HasVarLenPages()) {
    RETURN_IF_ERROR(var_len_pages_[var_len_pages_index_].WaitForBuffer());
  }

  while (!output_batch->AtCapacity()
      && fixed_len_page_offset_ < fixed_len_page->valid_data_len()) {
    DCHECK(fixed_len_page != NULL);
    Tuple* input_tuple =
        reinterpret_cast<Tuple*>(fixed_len_page->data() + fixed_len_page_offset_);

    if (CONVERT_OFFSET_TO_PTR && !ConvertOffsetsToPtrs(input_tuple)) {
      DCHECK(!is_pinned_);
      // The var-len data is in the next page. We are done with the current page, so
      // return rows we've accumulated so far and advance to the next page in the next
      // GetNext() call. This is needed for the unpinned case where we will exchange
      // this page for the next in the next GetNext() call. So therefore we must hold
      // onto the current var-len page and signal to the caller that the page is going
      // to be deleted.
      output_batch->MarkNeedsDeepCopy();
      end_of_var_len_page_ = true;
      break;
    }
    output_batch->GetRow(output_batch->AddRow())->SetTuple(0, input_tuple);
    output_batch->CommitLastRow();
    fixed_len_page_offset_ += sort_tuple_size_;
    ++num_tuples_returned_;
  }

  if (fixed_len_page_offset_ >= fixed_len_page->valid_data_len()) {
    // Reached the page boundary, need to move to the next page.
    if (is_pinned_) {
      BufferPool::ClientHandle* client = sorter_->buffer_pool_client_;
      // Attach page to batch. Caller can delete the page when it wants to.
      output_batch->AddBuffer(client,
          fixed_len_pages_[fixed_len_pages_index_].ExtractBuffer(client),
          RowBatch::FlushMode::NO_FLUSH_RESOURCES);

      // Attach the var-len pages at eos once no more rows will reference the pages.
      if (fixed_len_pages_index_ == fixed_len_pages_.size() - 1) {
        for (Page& var_len_page : var_len_pages_) {
          DCHECK(var_len_page.is_open());
          output_batch->AddBuffer(client, var_len_page.ExtractBuffer(client),
              RowBatch::FlushMode::NO_FLUSH_RESOURCES);
        }
        var_len_pages_.clear();
      }
    } else {
      // To iterate over unpinned runs, we need to exchange this page for the next
      // in the next GetNext() call, so we need to hold onto the page and signal to
      // the caller that the page is going to be deleted.
      output_batch->MarkNeedsDeepCopy();
    }
    end_of_fixed_len_page_ = true;
  }
  *eos = false;
  return Status::OK();
}

Status Sorter::Run::PinNextReadPage(vector<Page>* pages, int page_index) {
  DCHECK_GE(page_index, 0);
  DCHECK_LT(page_index, pages->size() - 1);
  Page* curr_page = &(*pages)[page_index];
  Page* next_page = &(*pages)[page_index + 1];
  DCHECK_EQ(is_pinned_, next_page->is_pinned());
  if (is_pinned_) {
    // The current page was attached to a batch and 'next_page' is already pinned.
    DCHECK(!curr_page->is_open());
    return Status::OK();
  }
  // Close the previous page to free memory and pin the next page. Should always succeed
  // since the pages are the same size.
  curr_page->Close(sorter_->buffer_pool_client_);
  RETURN_IF_ERROR(next_page->Pin(sorter_->buffer_pool_client_));
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

Status Sorter::Run::TryAddPage(
    AddPageMode mode, vector<Page>* page_sequence, bool* added) {
  DCHECK(!page_sequence->empty());
  if (mode == KEEP_PREV_PINNED) {
    if (!sorter_->buffer_pool_client_->IncreaseReservationToFit(sorter_->page_len_)) {
      *added = false;
      return Status::OK();
    }
  } else {
    DCHECK(mode == UNPIN_PREV);
    // Unpin the prev page to free up the memory required to pin the next page.
    page_sequence->back().Unpin(sorter_->buffer_pool_client_);
  }

  RETURN_IF_ERROR(AddPage(page_sequence));
  *added = true;
  return Status::OK();
}

Status Sorter::Run::AddPage(vector<Page>* page_sequence) {
  Page new_page;
  RETURN_IF_ERROR(new_page.Init(sorter_));
  page_sequence->push_back(move(new_page));
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
    int page_index, const uint8_t* page_start, uint8_t* dest) {
  DCHECK_GE(page_index, 0);
  DCHECK_GE(dest - page_start, 0);

  for (StringValue* string_val : string_values) {
    memcpy(dest, string_val->ptr, string_val->len);
    DCHECK_LE(dest - page_start, sorter_->page_len_);
    DCHECK_LE(dest - page_start, numeric_limits<uint32_t>::max());
    uint32_t page_offset = dest - page_start;
    uint64_t packed_offset = (static_cast<uint64_t>(page_index) << 32) | page_offset;
    string_val->ptr = reinterpret_cast<char*>(packed_offset);
    dest += string_val->len;
  }
}

bool Sorter::Run::ConvertOffsetsToPtrs(Tuple* tuple) {
  // We need to be careful to handle the case where var_len_pages_ is empty,
  // e.g. if all strings are NULL.
  uint8_t* page_start =
      var_len_pages_.empty() ? NULL : var_len_pages_[var_len_pages_index_].data();

  const vector<SlotDescriptor*>& string_slots = sort_tuple_desc_->string_slots();
  int num_non_null_string_slots = 0;
  for (int i = 0; i < string_slots.size(); ++i) {
    SlotDescriptor* slot_desc = string_slots[i];
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
    ++num_non_null_string_slots;

    DCHECK(slot_desc->type().IsVarLenStringType());
    StringValue* value = reinterpret_cast<StringValue*>(
        tuple->GetSlot(slot_desc->tuple_offset()));
    // packed_offset includes the page index in the upper 32 bits and the page
    // offset in the lower 32 bits. See CopyVarLenDataConvertOffset().
    uint64_t packed_offset = reinterpret_cast<uint64_t>(value->ptr);
    uint32_t page_index = packed_offset >> 32;
    uint32_t page_offset = packed_offset & 0xFFFFFFFF;

    if (page_index > var_len_pages_index_) {
      // We've reached the page boundary for the current var-len page.
      // This tuple will be returned in the next call to GetNext().
      DCHECK_GE(page_index, 0);
      DCHECK_LE(page_index, var_len_pages_.size());
      DCHECK_EQ(page_index, var_len_pages_index_ + 1);
      DCHECK_EQ(page_offset, 0); // The data is the first thing in the next page.
      // This must be the first slot with var len data for the tuple. Var len data
      // for tuple shouldn't be split across blocks.
      DCHECK_EQ(num_non_null_string_slots, 1);
      return false;
    }

    DCHECK_EQ(page_index, var_len_pages_index_);
    if (var_len_pages_.empty()) {
      DCHECK_EQ(value->len, 0);
    } else {
      DCHECK_LE(page_offset + value->len, var_len_pages_[page_index].valid_data_len());
    }
    // Calculate the address implied by the offset and assign it. May be NULL for
    // zero-length strings if there are no pages in the run since page_start is NULL.
    DCHECK(page_start != NULL || page_offset == 0);
    value->ptr = reinterpret_cast<char*>(page_start + page_offset);
  }
  return true;
}

int64_t Sorter::Run::TotalBytes() const {
  int64_t total_bytes = 0;
  for (const Page& page : fixed_len_pages_) {
    if (page.is_open()) total_bytes += page.valid_data_len();
  }

  for (const Page& page : var_len_pages_) {
    if (page.is_open()) total_bytes += page.valid_data_len();
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
  uint32_t page_offset;
  if (UNLIKELY(index == run->num_tuples())) {
    // If the iterator is initialized past the end, set up buffer_start_index_,
    // 'buffer_end_index_' and 'page_index_' for the last page, then set 'tuple' to
    // 'tuple_size' bytes past the last tuple, so everything is correct when Prev() is
    // invoked.
    page_index_ = run->fixed_len_pages_.size() - 1;
    page_offset = ((index - 1) % run->page_capacity_) * tuple_size + tuple_size;
  } else {
    page_index_ = index / run->page_capacity_;
    page_offset = (index % run->page_capacity_) * tuple_size;
  }
  buffer_start_index_ = page_index_ * run->page_capacity_;
  buffer_end_index_ = buffer_start_index_ + run->page_capacity_;
  tuple_ = run->fixed_len_pages_[page_index_].data() + page_offset;
}

void Sorter::TupleIterator::Next(Sorter::Run* run, int tuple_size) {
  DCHECK_LT(index_, run->num_tuples()) << "Can only advance one past end of run";
  tuple_ += tuple_size;
  ++index_;
  if (UNLIKELY(index_ >= buffer_end_index_)) NextPage(run, tuple_size);
}

void Sorter::TupleIterator::NextPage(Sorter::Run* run, int tuple_size) {
  // When moving after the last tuple, stay at the last page.
  if (index_ >= run->num_tuples()) return;
  ++page_index_;
  DCHECK_LT(page_index_, run->fixed_len_pages_.size());
  buffer_start_index_ = page_index_ * run->page_capacity_;
  DCHECK_EQ(index_, buffer_start_index_);
  buffer_end_index_ = buffer_start_index_ + run->page_capacity_;
  tuple_ = run->fixed_len_pages_[page_index_].data();
}

void Sorter::TupleIterator::Prev(Sorter::Run* run, int tuple_size) {
  DCHECK_GE(index_, 0) << "Can only advance one before start of run";
  tuple_ -= tuple_size;
  --index_;
  if (UNLIKELY(index_ < buffer_start_index_)) PrevPage(run, tuple_size);
}

void Sorter::TupleIterator::PrevPage(Sorter::Run* run, int tuple_size) {
  // When moving before the first tuple, stay at the first page.
  if (index_ < 0) return;
  --page_index_;
  DCHECK_GE(page_index_, 0);
  buffer_start_index_ = page_index_ * run->page_capacity_;
  buffer_end_index_ = buffer_start_index_ + run->page_capacity_;
  DCHECK_EQ(index_, buffer_end_index_ - 1);
  int last_tuple_page_offset = run->sort_tuple_size_ * (run->page_capacity_ - 1);
  tuple_ = run->fixed_len_pages_[page_index_].data() + last_tuple_page_offset;
}

Sorter::TupleSorter::TupleSorter(Sorter* parent, const TupleRowComparator& comp,
    int64_t page_size, int tuple_size, RuntimeState* state)
  : parent_(parent),
    tuple_size_(tuple_size),
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
    parent_->expr_results_pool_.Clear();
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

Sorter::Sorter(const std::vector<ScalarExpr*>& ordering_exprs,
      const std::vector<bool>& is_asc_order, const std::vector<bool>& nulls_first,
    const vector<ScalarExpr*>& sort_tuple_exprs, RowDescriptor* output_row_desc,
    MemTracker* mem_tracker, BufferPool::ClientHandle* buffer_pool_client,
    int64_t page_len, RuntimeProfile* profile, RuntimeState* state, int node_id,
    bool enable_spilling)
  : node_id_(node_id),
    state_(state),
    expr_perm_pool_(mem_tracker),
    expr_results_pool_(mem_tracker),
    compare_less_than_(ordering_exprs, is_asc_order, nulls_first),
    in_mem_tuple_sorter_(NULL),
    buffer_pool_client_(buffer_pool_client),
    page_len_(page_len),
    has_var_len_slots_(false),
    sort_tuple_exprs_(sort_tuple_exprs),
    mem_tracker_(mem_tracker),
    output_row_desc_(output_row_desc),
    enable_spilling_(enable_spilling),
    unsorted_run_(NULL),
    merge_output_run_(NULL),
    profile_(profile),
    initial_runs_counter_(NULL),
    num_merges_counter_(NULL),
    in_mem_sort_timer_(NULL),
    sorted_data_size_(NULL),
    run_sizes_(NULL) {}

Sorter::~Sorter() {
  DCHECK(sorted_runs_.empty());
  DCHECK(merging_runs_.empty());
  DCHECK(unsorted_run_ == NULL);
  DCHECK(merge_output_run_ == NULL);
}

Status Sorter::Prepare(ObjectPool* obj_pool) {
  DCHECK(in_mem_tuple_sorter_ == NULL) << "Already prepared";
  // Page byte offsets are packed into uint32_t values, which limits the supported
  // page size.
  if (page_len_ > numeric_limits<uint32_t>::max()) {
    return Status(Substitute(
          "Page size $0 exceeded maximum supported in sorter ($1)",
          PrettyPrinter::PrintBytes(page_len_),
          PrettyPrinter::PrintBytes(numeric_limits<uint32_t>::max())));
  }

  TupleDescriptor* sort_tuple_desc = output_row_desc_->tuple_descriptors()[0];
  if (sort_tuple_desc->byte_size() > page_len_) {
    return Status(TErrorCode::MAX_ROW_SIZE,
        PrettyPrinter::Print(sort_tuple_desc->byte_size(), TUnit::BYTES), node_id_,
        PrettyPrinter::Print(state_->query_options().max_row_size, TUnit::BYTES));
  }
  has_var_len_slots_ = sort_tuple_desc->HasVarlenSlots();
  in_mem_tuple_sorter_.reset(new TupleSorter(this, compare_less_than_, page_len_,
      sort_tuple_desc->byte_size(), state_));

  if (enable_spilling_) {
    initial_runs_counter_ = ADD_COUNTER(profile_, "InitialRunsCreated", TUnit::UNIT);
    spilled_runs_counter_ = ADD_COUNTER(profile_, "SpilledRuns", TUnit::UNIT);
    num_merges_counter_ = ADD_COUNTER(profile_, "TotalMergesPerformed", TUnit::UNIT);
  } else {
    initial_runs_counter_ = ADD_COUNTER(profile_, "RunsCreated", TUnit::UNIT);
  }
  in_mem_sort_timer_ = ADD_TIMER(profile_, "InMemorySortTime");
  sorted_data_size_ = ADD_COUNTER(profile_, "SortDataSize", TUnit::BYTES);
  run_sizes_ = ADD_SUMMARY_STATS_COUNTER(profile_, "NumRowsPerRun", TUnit::UNIT);

  RETURN_IF_ERROR(ScalarExprEvaluator::Create(sort_tuple_exprs_, state_, obj_pool,
      &expr_perm_pool_, &expr_results_pool_, &sort_tuple_expr_evals_));
  return Status::OK();
}

Status Sorter::Codegen(RuntimeState* state) {
  return compare_less_than_.Codegen(state);
}

Status Sorter::Open() {
  DCHECK(in_mem_tuple_sorter_ != NULL) << "Not prepared";
  DCHECK(unsorted_run_ == NULL) << "Already open";
  RETURN_IF_ERROR(compare_less_than_.Open(&obj_pool_, state_, &expr_perm_pool_,
      &expr_results_pool_));
  TupleDescriptor* sort_tuple_desc = output_row_desc_->tuple_descriptors()[0];
  unsorted_run_ = run_pool_.Add(new Run(this, sort_tuple_desc, true));
  RETURN_IF_ERROR(unsorted_run_->Init());
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(sort_tuple_expr_evals_, state_));
  return Status::OK();
}

int64_t Sorter::ComputeMinReservation() {
  // Must be kept in sync with SortNode.computeNodeResourceProfile() in fe.
  int min_buffers_required = enable_spilling_ ? MIN_BUFFERS_PER_MERGE : 1;
  // Fixed and var-length pages are separate, so we need double the pages
  // if there is var-length data.
  if (output_row_desc_->HasVarlenSlots()) min_buffers_required *= 2;
  return min_buffers_required * page_len_;
}

Status Sorter::AddBatch(RowBatch* batch) {
  DCHECK(unsorted_run_ != NULL);
  DCHECK(batch != NULL);
  DCHECK(enable_spilling_);
  int num_processed = 0;
  int cur_batch_index = 0;
  while (cur_batch_index < batch->num_rows()) {
    RETURN_IF_ERROR(AddBatchNoSpill(batch, cur_batch_index, &num_processed));

    cur_batch_index += num_processed;
    if (cur_batch_index < batch->num_rows()) {
      // The current run is full. Sort it, spill it and begin the next one.
      RETURN_IF_ERROR(state_->StartSpilling(mem_tracker_));
      RETURN_IF_ERROR(SortCurrentInputRun());
      RETURN_IF_ERROR(sorted_runs_.back()->UnpinAllPages());
      unsorted_run_ =
          run_pool_.Add(new Run(this, output_row_desc_->tuple_descriptors()[0], true));
      RETURN_IF_ERROR(unsorted_run_->Init());
    }
  }
  // Clear any temporary allocations made while materializing the sort tuples.
  expr_results_pool_.Clear();
  return Status::OK();
}

Status Sorter::AddBatchNoSpill(RowBatch* batch, int start_index, int* num_processed) {
  DCHECK(batch != nullptr);
  RETURN_IF_ERROR(unsorted_run_->AddInputBatch(batch, start_index, num_processed));
  // Clear any temporary allocations made while materializing the sort tuples.
  expr_results_pool_.Clear();
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
  DCHECK(enable_spilling_);

  // Unpin the final run to free up memory for the merge.
  // TODO: we could keep it in memory in some circumstances as an optimisation, once
  // we have a buffer pool with more reliable reservations (IMPALA-3200).
  RETURN_IF_ERROR(sorted_runs_.back()->UnpinAllPages());

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
    RETURN_IF_ERROR(merger_->GetNext(output_batch, eos));
    // Clear any temporary allocations made by the merger.
    expr_results_pool_.Clear();
    return Status::OK();
  }
}

void Sorter::Reset() {
  DCHECK(unsorted_run_ == NULL) << "Cannot Reset() before calling InputDone()";
  merger_.reset();
  // Free resources from the current runs.
  CleanupAllRuns();
  compare_less_than_.Close(state_);
}

void Sorter::Close(RuntimeState* state) {
  CleanupAllRuns();
  compare_less_than_.Close(state);
  ScalarExprEvaluator::Close(sort_tuple_expr_evals_, state);
  expr_perm_pool_.FreeAll();
  expr_results_pool_.FreeAll();
  obj_pool_.Clear();
}

void Sorter::CleanupAllRuns() {
  Run::CleanupRuns(&sorted_runs_);
  Run::CleanupRuns(&merging_runs_);
  if (unsorted_run_ != NULL) unsorted_run_->CloseAllPages();
  unsorted_run_ = NULL;
  if (merge_output_run_ != NULL) merge_output_run_->CloseAllPages();
  merge_output_run_ = NULL;
  run_pool_.Clear();
}

Status Sorter::SortCurrentInputRun() {
  RETURN_IF_ERROR(unsorted_run_->FinalizeInput());

  {
    SCOPED_TIMER(in_mem_sort_timer_);
    RETURN_IF_ERROR(in_mem_tuple_sorter_->Sort(unsorted_run_));
  }
  sorted_runs_.push_back(unsorted_run_);
  sorted_data_size_->Add(unsorted_run_->TotalBytes());
  run_sizes_->UpdateCounter(unsorted_run_->num_tuples());
  unsorted_run_ = NULL;

  RETURN_IF_CANCELLED(state_);
  return Status::OK();
}

Status Sorter::MergeIntermediateRuns() {
  DCHECK_GE(sorted_runs_.size(), 2);
  int pinned_pages_per_run = has_var_len_slots_ ? 2 : 1;
  int max_runs_per_final_merge = MAX_BUFFERS_PER_MERGE / pinned_pages_per_run;

  // During an intermediate merge, the one or two pages from the output sorted run
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
    merge_output_run_ = run_pool_.Add(
        new Run(this, output_row_desc_->tuple_descriptors()[0], false));
    RETURN_IF_ERROR(merge_output_run_->Init());
    RETURN_IF_ERROR(CreateMerger(num_runs_to_merge));

    // If CreateMerger() consumed all the sorted runs, we have set up the final merge.
    if (sorted_runs_.empty()) {
      // Don't need intermediate run for final merge.
      if (merge_output_run_ != NULL) {
        merge_output_run_->CloseAllPages();
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
  RowBatch intermediate_merge_batch(
      output_row_desc_, state_->batch_size(), mem_tracker_);
  bool eos = false;
  while (!eos) {
    // Copy rows into the new run until done.
    int num_copied;
    RETURN_IF_CANCELLED(state_);
    // Clear any temporary allocations made by the merger.
    expr_results_pool_.Clear();
    RETURN_IF_ERROR(merger_->GetNext(&intermediate_merge_batch, &eos));
    RETURN_IF_ERROR(
        merged_run->AddIntermediateBatch(&intermediate_merge_batch, 0, &num_copied));

    DCHECK_EQ(num_copied, intermediate_merge_batch.num_rows());
    intermediate_merge_batch.Reset();
  }

  RETURN_IF_ERROR(merged_run->FinalizeInput());
  return Status::OK();
}

bool Sorter::HasSpilledRuns() const {
  // All runs in 'merging_runs_' are spilled. 'sorted_runs_' can contain at most one
  // non-spilled run.
  return !merging_runs_.empty() || sorted_runs_.size() > 1 ||
      (sorted_runs_.size() == 1 && !sorted_runs_.back()->is_pinned());
}
} // namespace impala
