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

// Definitions of classes that are internal to the Sorter but shared between .cc files.

#pragma once

#include "sorter.h"

#include <random>

#include "common/compiler-util.h"

namespace impala {

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
  Status Init(Sorter* sorter);

  /// Extract the buffer from the page. The page must be in memory. When this function
  /// returns the page is closed.
  BufferPool::BufferHandle ExtractBuffer(BufferPool::ClientHandle* client);

  /// Allocate 'len' bytes in the current page. The page must be in memory, and the
  /// amount to allocate cannot exceed BytesRemaining().
  uint8_t* AllocateBytes(int64_t len);

  /// Free the last 'len' bytes allocated from AllocateBytes(). The page must be in
  /// memory.
  void FreeBytes(int64_t len);

  /// Return number of bytes remaining in page.
  int64_t BytesRemaining() { return len() - valid_data_len_; }

  /// Brings a pinned page into memory, if not already in memory, and sets 'data_' to
  /// point to the page's buffer.
  Status WaitForBuffer();

  /// Helper to pin the page. Caller must ensure the client has enough reservation
  /// remaining to pin the page. Only valid to call on an unpinned page.
  Status Pin(BufferPool::ClientHandle* client);

  /// Helper to unpin the page.
  void Unpin(BufferPool::ClientHandle* client);

  /// Destroy the page with 'client'.
  void Close(BufferPool::ClientHandle* client);

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
  /// Reset the page to an uninitialized state. 'handle_' must already be closed.
  void Reset();

  /// Helper to get the singleton buffer pool.
  static BufferPool* pool();

  BufferPool::PageHandle handle_;

  /// Length of valid data written to the page.
  int64_t valid_data_len_;

  /// Cached pointer to the buffer in 'handle_'. NULL if the page is unpinned. May be
  /// NULL or not NULL if the page is pinned. Can be populated by calling
  /// WaitForBuffer() on a pinned page.
  uint8_t* data_;
};

/// A run is a sequence of tuples. The run can be sorted or unsorted (in which case the
/// Sorter will sort it). A run comprises a sequence of fixed-length pages containing
/// the tuples themselves (i.e. fixed-len slots that may contain ptrs to var-length
/// data), and an optional sequence of var-length pages containing the var-length data.
///
/// Runs are either "initial runs" constructed from the sorter's input by evaluating
/// the expressions in 'sort_tuple_exprs_' or "intermediate runs" constructed
/// by merging already-sorted runs. Initial runs are sorted in-place in memory.
/// Once sorted, runs can be spilled to disk to free up memory. Sorted runs are merged by
/// SortedRunMerger, either to produce the final sorted output or to produce another
/// sorted run.
/// By default, the size of initial runs is determined by the available memory: the
/// sorter tries to add batches to the run until some (memory) limit is reached.
/// Some query options can also limit the size of an initial (or in-memory) run.
/// SORT_RUN_BYTES_LIMIT triggers spilling after the size of data in the run exceeds the
/// given threshold (usually expressed in MB or GB).
/// MAX_SORT_RUN_SIZE allows constructing runs up to a certain size by limiting the
/// number of pages in the initial runs. These smaller in-memory runs are also referred
/// to as 'miniruns'. Miniruns are not spilled immediately, but sorted in-place first,
/// and collected to be merged in memory before spilling the produced output run to disk.
///
/// The expected calling sequence of functions is as follows:
/// * Init() or TryInit() to initialize the run and allocate initial pages.
/// * Add*Batch() to add batches of tuples to the run.
/// * FinalizeInput() to signal that no more batches will be added.
/// * If the run is unsorted, it must be sorted. After that set_sorted() must be called.
/// * Once sorted, the run is ready to read in sorted order for merging or final output.
/// * PrepareRead() to allocate resources for reading the run.
/// * GetNext() (if there was a single run) or GetNextBatch() (when merging multiple
/// * runs) to read from the run.
/// * Once reading is done, CloseAllPages() should be called to free resources.
class Sorter::Run {
 public:
  Run(Sorter* parent, TupleDescriptor* sort_tuple_desc, bool initial_run);

  ~Run();

  /// Initialize the run for input rows by allocating the minimum number of required
  /// pages - one page for fixed-len data added to fixed_len_pages_, one for the
  /// initially unsorted var-len data added to var_len_pages_, and one to copy sorted
  /// var-len data into var_len_copy_page_.
  Status Init();

  /// Similar to Init(), except for the following differences:
  /// It is only used to initialize miniruns (query option MAX_SORT_RUN_SIZE > 0 cases).
  /// The first in-memory run is always initialized by calling Init(), because that must
  /// succeed. The following ones are initialized by TryInit().
  /// TryInit() allocates one fixed-len page and one var-len page if 'has_var_len_slots_'
  /// is true. There is no need for var_len_copy_page here. Returns false if
  /// initialization was successful, returns true, if reservation was not enough.
  Status TryInit(bool* allocation_failed);

  /// Add the rows from 'batch' starting at 'start_index' to the current run. Returns
  /// the number of rows actually added in 'num_processed'. If the run is full (no more
  /// pages can be allocated), 'num_processed' may be less than the number of remaining
  /// rows in the batch. AddInputBatch() materializes the input rows using the
  /// expressions in sorter_->sort_tuple_expr_evals_, while AddIntermediateBatch() just
  /// copies rows.
  Status AddInputBatch(
      RowBatch* batch, int start_index, int* num_processed, bool* allocation_failed);

  Status AddIntermediateBatch(RowBatch* batch, int start_index, int* num_processed);

  /// Called after the final call to Add*Batch() to do any bookkeeping necessary to
  /// finalize the run. Must be called before sorting or merging the run.
  Status FinalizeInput();

  /// Unpins all the pages in a sorted run. Var-length column data is copied into new
  /// pages in sorted order. Pointers in the original tuples are converted to offsets
  /// from the beginning of the sequence of var-len data pages. Returns an error and
  /// may leave some pages pinned if an error is encountered.
  Status UnpinAllPages();

  /// Closes all pages and clears vectors of pages.
  void CloseAllPages();

  /// Prepare to read a sorted run. Pins the first page(s) in the run if the run was
  /// previously unpinned. If the run was unpinned, try to pin the initial fixed and
  /// var len pages in the run. If it couldn't pin them, an error Status is returned.
  Status PrepareRead();

  /// Interface for merger - get the next batch of rows from this run. This run still
  /// owns the returned batch. Calls GetNext(RowBatch*, bool*).
  Status GetNextBatch(RowBatch** sorted_batch);

  /// Fill output_batch with rows from this run. If CONVERT_OFFSET_TO_PTR is true,
  /// offsets in var-length slots are converted back to pointers. Only row pointers are
  /// copied into output_batch. eos is set to true after all rows from the run are
  /// returned. If eos is true, the returned output_batch has zero rows and has no
  /// attached pages. If this run was unpinned, one page (two if there are var-len
  /// slots) is pinned while rows are filled into output_batch. The page is unpinned
  /// before the next page is pinned, so at most one (two if there are var-len slots)
  /// page(s) will be pinned at once. If the run was pinned, the pages are not unpinned
  /// and each page is attached to 'output_batch' once all rows referencing data in the
  /// page have been returned, either in the current batch or previous batches. In both
  /// pinned and unpinned cases, all rows in output_batch will reference at most one
  /// fixed-len and one var-len page.
  template <bool CONVERT_OFFSET_TO_PTR>
  Status GetNext(RowBatch* output_batch, bool* eos);

  /// Delete all pages in 'runs' and clear 'runs'.
  static void CleanupRuns(std::deque<Run*>* runs);

  /// Return total amount of fixed and var len data in run, not including pages that
  /// were already transferred or closed.
  int64_t TotalBytes() const;

  bool is_pinned() const { return is_pinned_; }
  bool is_finalized() const { return is_finalized_; }
  bool is_sorted() const { return is_sorted_; }
  void set_sorted() { is_sorted_ = true; }
  int max_num_of_pages() const { return max_num_of_pages_; }
  int fixed_len_size() { return fixed_len_pages_.size(); }
  int run_size() { return fixed_len_pages_.size() + var_len_pages_.size(); }
  int64_t num_tuples() const { return num_tuples_; }
  /// Returns true if we have var-len pages in the run.
  bool HasVarLenPages() const {
    // Shouldn't have any pages unless there are slots.
    DCHECK(var_len_pages_.empty() || has_var_len_slots_);
    return !var_len_pages_.empty();
  }

 private:
  /// TupleIterator needs access to internals to iterate over tuples.
  friend class TupleIterator;

  /// Templatized implementation of Add*Batch() functions.
  /// INITIAL_RUN and HAS_VAR_LEN_SLOTS are template arguments for performance and must
  /// match 'initial_run_' and 'has_var_len_slots_'.
  template <bool HAS_VAR_LEN_SLOTS, bool INITIAL_RUN>
  Status AddBatchInternal(RowBatch* batch, int start_index, int* num_processed,
        bool* allocation_failed);

  /// Finalize the list of pages: delete empty final pages and unpin the previous page
  /// if the run is unpinned.
  Status FinalizePages(vector<Page>* pages);

  void CheckTypesAreValidInSortingTuple();

  /// Collects the non-null var-len slots (non-smallified strings and collections) from
  /// 'src'. Smallified strings (see Small String Optimization, IMPALA-12373) are not
  /// collected. Strings are returned in 'string_values' and collections are returned,
  /// along with their byte size, in 'collection_values' (any existing elements of
  /// 'string_values' and 'collection_values' are cleared). The total length of all
  /// var-len values is returned in 'total_var_len'. Nested (non-top-level) var-len values
  /// are collected recursively.
  ///
  /// Children are placed before their parents in the vectors (post-order traversal).
  /// This order is chosen because of the way we serialise the values in CopyVarLenData()
  /// and CopyVarLenDataConvertOffset(): we write the var-len part of 'StringValue's and
  /// 'CollectionValue's to the buffer and update the pointers in-place. The order becomes
  /// important if these '(String|Collection)Value's are themselves (var-len) children of
  /// other 'CollectionValue's. If children are written before their parents, then when
  /// the parents are written their pointers have already been updated so they can be
  /// written as-is. If parents were written before their children, updating the pointers
  /// in-place would not be enough, the already serialised pointers would have to be
  /// updated in the byte buffer. Note that to ensure that this order is kept, the
  /// 'StringValue's must be serialised before the 'CollectionValue's - strings can be
  /// children of collections but not the other way around.
  void CollectNonNullNonSmallVarSlots(Tuple* src, const TupleDescriptor& tuple_desc,
      vector<StringValue*>* string_values,
      std::vector<std::pair<CollectionValue*, int64_t>>* collection_values,
      int* total_var_len);

  void CollectNonNullNonSmallVarSlotsHelper(Tuple* src, const TupleDescriptor& tuple_desc,
      vector<StringValue*>* string_values,
      std::vector<std::pair<CollectionValue*, int64_t>>* collection_values,
      int* total_var_len);

  enum AddPageMode { KEEP_PREV_PINNED, UNPIN_PREV };

  /// Try to extend the current run by a page. If 'mode' is KEEP_PREV_PINNED, try to
  /// allocate a new page, which may fail to extend the run due to lack of memory. If
  /// mode is 'UNPIN_PREV', unpin the previous page in page_sequence before allocating
  /// and adding a new page - this never fails due to lack of memory.
  ///
  /// Returns an error status only if the buffer pool returns an error. If no error is
  /// encountered, sets 'added' to indicate whether the run was extended and returns
  /// Status::OK(). The new page is appended to 'page_sequence'.
  Status TryAddPage(AddPageMode mode, vector<Page>* page_sequence, bool* added);

  /// Adds a new page to 'page_sequence' by a page. Caller must ensure enough
  /// reservation is available to create the page.
  ///
  /// Returns an error status only if the buffer pool returns an error. If an error
  /// is returned 'page_sequence' is left unmodified.
  Status AddPage(vector<Page>* page_sequence);

  /// Advance to the next read page. If the run is pinned, has no effect. If the run
  /// is unpinned, the pin at 'page_index' was already attached to an output batch and
  /// this function will pin the page at 'page_index' + 1 in 'pages'.
  Status PinNextReadPage(vector<Page>* pages, int page_index);

  /// Copy the var len data in 'string_values' and 'collection_values_and_sizes' to 'dest'
  /// in order and update the pointers to point to the copied data.
  void CopyVarLenData(const vector<StringValue*>& string_values,
      const vector<std::pair<CollectionValue*, int64_t>>& collection_values_and_sizes,
      uint8_t* dest);

  /// Copy the StringValues in 'var_values' and the CollectionValues referenced in
  /// 'collection_values_and_sizes' to 'dest' in order. Update the StringValue ptrs in
  /// 'dest' to contain a packed offset for the copied data comprising page_index and the
  /// offset relative to page_start.
  void CopyVarLenDataConvertOffset(const vector<StringValue*>& var_values,
      const std::vector<std::pair<CollectionValue*, int64_t>>&
          collection_values_and_sizes,
      int page_index, const uint8_t* page_start, uint8_t* dest);

  /// Convert encoded offsets to valid pointers in 'tuple' with layout 'tuple_desc'.
  /// 'tuple' is modified in-place. Returns true if the pointers refer to the page at
  /// 'var_len_pages_index_' and were successfully converted or false if the var len data
  /// is in the next page, in which case 'tuple' is unmodified.
  bool ConvertOffsetsToPtrs(Tuple* tuple, const TupleDescriptor& tuple_desc)
      WARN_UNUSED_RESULT;

  template <class ValueType>
  WARN_UNUSED_RESULT
  bool ConvertValueOffsetsToPtrs(Tuple* tuple, uint8_t* page_start,
      const vector<SlotDescriptor*>& slots);

  bool ConvertStringValueOffsetsToPtrs(Tuple* tuple, const TupleDescriptor& tuple_desc,
      uint8_t* page_start) WARN_UNUSED_RESULT;
  bool ConvertCollectionValueOffsetsToPtrs(Tuple* tuple,
      const TupleDescriptor& tuple_desc, uint8_t* page_start) WARN_UNUSED_RESULT;

  bool ConvertOffsetsForCollectionChildren(const CollectionValue& cv,
      const SlotDescriptor& slot_desc) WARN_UNUSED_RESULT;

  int NumOpenPages(const vector<Page>& pages);

  /// Close all open pages and clear vector.
  void DeleteAndClearPages(vector<Page>* pages);

  /// Parent sorter object.
  Sorter* const sorter_;

  /// Materialized sort tuple. Input rows are materialized into 1 tuple (with descriptor
  /// sort_tuple_desc_) before sorting.
  const TupleDescriptor* sort_tuple_desc_;

  /// The size in bytes of the sort tuple.
  const int sort_tuple_size_;

  /// Number of tuples per page in a run. This gets multiplied with
  /// TupleIterator::page_index_ in various places and to make sure we don't overflow
  /// the result of that operation we make this int64_t here.
  const int64_t page_capacity_;

  const bool has_var_len_slots_;

  /// True if this is an initial run. False implies this is a sorted intermediate run
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
  /// var-length columns from fixed_len_pages_. In intermediate runs, the var-len data
  /// is always stored in the same order as the fixed-length tuples. In initial runs,
  /// the var-len data is initially in unsorted order, but is reshuffled into sorted
  /// order in UnpinAllPages(). A run can have no var len pages if there are no var len
  /// slots or if all the var len data is empty or NULL.
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
  boost::scoped_ptr<RowBatch> buffered_batch_;

  /// Max number of fixed-len + var-len pages in an in-memory minirun. It defines the
  /// length of a minirun.
  /// The default value is 0 which means that only 1 in-memory run will be created, and
  /// its size will be determined by other limits eg. memory or sort_run_bytes_limit.
  int max_num_of_pages_;

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
  TupleIterator() : index_(-1), tuple_(nullptr), buffer_start_index_(-1),
      buffer_end_index_(-1), page_index_(-1) { }

  /// Create an iterator pointing to the first tuple in the run.
  static TupleIterator Begin(Sorter::Run* run) { return {run, 0}; }

  /// Create an iterator pointing one past the end of the run.
  static TupleIterator End(Sorter::Run* run) { return {run, run->num_tuples()}; }

  /// Increments 'index_' and sets 'tuple_' to point to the next tuple in the run.
  /// Increments 'page_index_' and advances to the next page if the next tuple is in
  /// the next page. Can be advanced one past the last tuple in the run, but is not
  /// valid to dereference 'tuple_' in that case. 'run' and 'tuple_size' are passed as
  /// arguments to avoid redundantly storing the same values in multiple iterators in
  /// perf-critical algorithms.
  void IR_ALWAYS_INLINE Next(Sorter::Run* run, int tuple_size);

  /// The reverse of Next(). Can advance one before the first tuple in the run, but it
  /// is invalid to dereference 'tuple_' in that case.
  void IR_ALWAYS_INLINE Prev(Sorter::Run* run, int tuple_size);

  int64_t IR_ALWAYS_INLINE index() const { return index_; }
  Tuple* IR_ALWAYS_INLINE tuple() const { return reinterpret_cast<Tuple*>(tuple_); }
  /// Returns current tuple in TupleRow format. The caller should not modify the row.
  const TupleRow* IR_ALWAYS_INLINE row() const {
    return reinterpret_cast<const TupleRow*>(&tuple_);
  }

 private:
  /// Move to the next page in the run (or do nothing if at end of run).
  /// This is the slow path for Next();
  void IR_ALWAYS_INLINE NextPage(Sorter::Run* run);

  /// Move to the previous page in the run (or do nothing if at beginning of run).
  /// This is the slow path for Prev();
  void IR_ALWAYS_INLINE PrevPage(Sorter::Run* run);

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
/// Quick sort is used for sequences of tuples larger that 16 elements, and insertion
/// sort is used for smaller sequences. The TupleSorter is initialized with a
/// RuntimeState instance to check for cancellation during an in-memory sort.
class Sorter::TupleSorter {
 public:
  TupleSorter(Sorter* parent, const TupleRowComparator& comparator,
        int tuple_size, RuntimeState* state);

  ~TupleSorter();

  /// Performs a quicksort for tuples in 'run' followed by an insertion sort to
  /// finish smaller ranges. Only valid to call if this is an initial run that has not
  /// yet been sorted. Returns an error status if any error is encountered or if the
  /// query is cancelled.
  Status Sort(Run* run);

  /// Makes an attempt to codegen for method SortHelper(). Stores the resulting
  /// function in codegend_fn and returns Status::OK() if codegen was successful.
  /// Otherwise, a Status("Sorter::TupleSorter::Codegen(): failed to finalize function")
  /// object is returned.
  /// 'compare_fn' is the pointer to the code-gen version of the compare method with
  /// which to replace all non-code-gen versions.
  static Status Codegen(FragmentState* state, llvm::Function* compare_fn,
      int tuple_byte_size, CodegenFnPtr<SortHelperFn>* codegend_fn);

  /// Mangled name of SorterHelper().
  static const char* SORTER_HELPER_SYMBOL;

  /// Class name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 private:
  static const int INSERTION_THRESHOLD = 16;

  Sorter* const parent_;

  /// Size of the tuples in memory.
  const int tuple_size_;

  /// Getter for the size of the tuples in memory. Replaced by a constant during codegen
  int IR_NO_INLINE get_tuple_size() const { return tuple_size_; }

  /// Tuple comparator with method Less() that returns true if lhs < rhs.
  const TupleRowComparator& comparator_;

  /// Number of times comparator_.Less() can be invoked again before
  /// comparator_. expr_results_pool_.Clear() needs to be called.
  int num_comparisons_till_free_;

  /// Runtime state instance to check for cancellation. Not owned.
  RuntimeState* const state_;

  /// The run to be sorted.
  Run* run_;

  /// Temporarily allocated space to copy and swap tuples (Both are used in
  /// Partition2way() and Partition3way()). Owned by this TupleSorter instance.
  uint8_t* temp_tuple_buffer_;
  uint8_t* swap_buffer_;

  /// Random number generator used to randomly choose pivots. We need a RNG that
  /// can generate 64-bit ints. Quality of randomness doesn't need to be especially
  /// high: Mersenne Twister should be more than adequate.
  std::mt19937_64 rng_;

  void IR_ALWAYS_INLINE FreeExprResultPoolIfNeeded();

  /// Wrapper around comparator_.Less(). Also call expr_results_pool_.Clear()
  /// on every 'state_->batch_size()' invocations of comparator_.Less(). Returns true
  /// if 'lhs' is less than 'rhs'.
  bool IR_ALWAYS_INLINE Less(const TupleRow* lhs, const TupleRow* rhs);

  /// Wrapper around comparator_.Compare(). Also call expr_results_pool_.Clear()
  /// on every 'state_->batch_size()' invocations of comparator_.Compare(). Returns -
  /// if 'lhs' is less than 'rhs', + if 'lhs' is greater than 'rhs' and 0 if equal
  int IR_ALWAYS_INLINE Compare(const TupleRow* lhs, const TupleRow* rhs);

  /// Perform an insertion sort for rows in the range [begin, end) in a run.
  /// Only valid to call for ranges of size at least 1.
  Status IR_ALWAYS_INLINE InsertionSort(
      const TupleIterator& begin, const TupleIterator& end);

  /// Partitions the sequence of tuples in the range [begin, end) in a run into two
  /// groups around the pivot tuple - i.e. tuples in first group are <= the pivot, and
  /// tuples in the second group are >= pivot. Tuples are swapped in place to create the
  /// groups and the index to the first element in the second group is returned in
  /// 'cut'. Return an error status if any error is encountered or if the query is
  /// cancelled.
  Status IR_ALWAYS_INLINE Partition2way(TupleIterator begin, TupleIterator end,
      const Tuple* pivot, TupleIterator* cut);

  /// Partitions the sequence of tuples in the range [begin, end) in a run into three
  /// groups around the pivot tuple - i.e. tuples in first group are < the pivot,
  /// tuples in the second group are = pivot, and tuples in the third group are > pivot.
  /// Tuples are swapped in place to create the groups and the index to
  /// the first element in the second group is returned in 'cut_left',
  /// and the index to the first element in the third group is returned in 'cut_right'.
  /// Returns an error status if any error is encountered or if the query is
  /// cancelled.
  Status IR_ALWAYS_INLINE Partition3way(TupleIterator begin, TupleIterator end,
      const Tuple* pivot, TupleIterator* cut_left, TupleIterator* cut_right);

  /// Performs a modified quicksort of rows in the range [begin, end):
  /// If duplicates are found during pivot selection, Partition3way
  /// is called in that iteration, otherwise partitioning goes 2-way.
  /// This adaptive quicksort is followed by insertion sort for smaller groups
  /// of elements.
  /// Return an error status for any errors or if the query is cancelled.
  Status SortHelper(TupleIterator begin, TupleIterator end);

  /// Select a pivot to partition [begin, end).
  Tuple* IR_ALWAYS_INLINE SelectPivot(TupleIterator begin, TupleIterator end,
      bool* has_equals);

  /// Return median of three tuples according to the sort comparator. Sets has_equals
  /// flag true if duplicates found among the pivot candidates.
  Tuple* IR_ALWAYS_INLINE MedianOfThree(Tuple* t1, Tuple* t2, Tuple* t3,
      bool* has_equals);

  /// Swaps tuples pointed to by left and right using 'swap_tuple'.
  static void IR_ALWAYS_INLINE Swap(Tuple* RESTRICT left, Tuple* RESTRICT right,
      Tuple* RESTRICT swap_tuple, int tuple_size);
};

} // namespace impala
