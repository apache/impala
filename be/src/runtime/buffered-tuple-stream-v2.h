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

#ifndef IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_V2_H
#define IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_V2_H

#include <set>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/global-types.h"
#include "common/status.h"
#include "gutil/macros.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/row-batch.h"

namespace impala {

class MemTracker;
class RuntimeState;
class RowDescriptor;
class SlotDescriptor;
class Tuple;
class TupleRow;

/// Class that provides an abstraction for a stream of tuple rows backed by BufferPool
/// Pages. Rows can be added to the stream and read back. Rows are returned in the order
/// they are added.
///
/// The BufferedTupleStream is *not* thread safe from the caller's point of view.
/// Different threads should not concurrently call methods of the same BufferedTupleStream
/// object.
///
/// Reading and writing the stream:
/// The stream supports two modes of reading/writing, depending on whether
/// PrepareForWrite() is called to initialize a write iterator only or
/// PrepareForReadWrite() is called to initialize both read and write iterators to enable
/// interleaved reads and writes.
///
/// To use write-only mode, PrepareForWrite() is called once and AddRow()/AllocateRow()
/// are called repeatedly to initialize then advance a write iterator through the stream.
/// Once the stream is fully written, it can be read back by calling PrepareForRead()
/// then GetNext() repeatedly to advance a read iterator through the stream, or by
/// calling GetRows() to get all of the rows at once.
///
/// To use read/write mode, PrepareForReadWrite() is called once to initialize the read
/// and write iterators. AddRow()/AllocateRow() then advance a write iterator through the
/// stream, and GetNext() advances a trailing read iterator through the stream.
///
/// Buffer management:
/// The tuple stream is backed by a sequence of BufferPool Pages. The tuple stream uses
/// the client's reservation to pin pages in memory. It will automatically try to
/// increase the client's reservation whenever it needs to do so to make progress.
///
/// The stream has both pinned and unpinned modes. In the pinned mode all pages are
/// pinned for reading. The pinned mode avoids I/O by keeping all pages pinned in memory
/// and allows clients to save pointers to rows in the stream and randomly access them.
/// E.g. hash tables can be backed by a BufferedTupleStream. In the unpinned mode, only
/// pages currently being read and written are pinned and other pages are unpinned and
/// therefore do not use the client's reservation and can be spilled to disk.
///
/// When the stream is in read/write mode, the stream always uses one buffer's worth
/// of reservation of writing and at least one buffer's worth of reservation for reading,
/// even if the same page is currently being read and written. This means that
/// UnpinStream() always succeeds, and moving to the next write page or read page on an
/// unpinned stream does not require additional reservation.
/// TODO: IMPALA-3208: variable-length pages will add a caveat here.
///
/// The tuple stream also supports a 'delete_on_read' mode, enabled by passing a flag
/// to PrepareForRead() which deletes the stream's pages as it does a final read
/// pass over the stream.
///
/// TODO: IMPALA-4179: the buffer management can be simplified once we can attach
/// buffers to RowBatches.
///
/// Page layout:
/// Rows are stored back to back starting at the first byte of each page's buffer, with
/// no interleaving of data from different rows. There is no padding or alignment
/// between rows.
///
/// Tuple row layout:
/// If the stream's tuples are nullable (i.e. has_nullable_tuple_ is true), there is a
/// bitstring at the start of each row with null indicators for all tuples in each row
/// (including non-nullable tuples). The bitstring occupies ceil(num_tuples_per_row / 8)
/// bytes. A 1 indicates the tuple is null.
///
/// The fixed length parts of the row's tuples are stored first, followed by var len data
/// for inlined_string_slots_ and inlined_coll_slots_. Other "external" var len slots can
/// point to var len data outside the stream. When reading the stream, the length of each
/// row's var len data in the stream must be computed to find the next row's start.
///
/// The tuple stream supports reading from the stream into RowBatches without copying
/// out any data: the RowBatches' Tuple pointers will point directly into the stream's
/// pages' buffers. The fixed length parts follow Impala's internal tuple format, so for
/// the tuple to be valid, we only need to update pointers to point to the var len data
/// in the stream. These pointers need to be updated by the stream because a spilled
/// page's data may be relocated to a different buffer. The pointers are updated lazily
/// upon reading the stream via GetNext() or GetRows().
///
/// Example layout for a row with two non-nullable tuples ((1, "hello"), (2, "world"))
/// with all var len data stored in the stream:
///  <---- tuple 1 -----> <------ tuple 2 ------> <- var len -> <- next row ...
/// +--------+-----------+-----------+-----------+-------------+
/// | IntVal | StringVal | BigIntVal | StringVal |             | ...
/// +--------+-----------+-----------+-----------++------------+
/// | val: 1 | len: 5    | val: 2    | len: 5    | helloworld  | ...
/// |        | ptr: 0x.. |           | ptr: 0x.. |             | ...
/// +--------+-----------+-----------+-----------+-------------+
///  <--4b--> <---12b---> <----8b---> <---12b---> <----10b---->
///
/// Example layout for a row with the second tuple nullable ((1, "hello"), NULL)
/// with all var len data stored in the stream:
/// <- null tuple bitstring -> <---- tuple 1 -----> <- var len -> <- next row ...
/// +-------------------------+--------+-----------+------------+
/// |                         | IntVal | StringVal |            | ...
/// +-------------------------+--------+-----------+------------+
/// | 0000 0010               | val: 1 | len: 5    | hello      | ...
/// |                         |        | ptr: 0x.. |            | ...
/// +-------------------------+--------+-----------+------------+
///  <---------1b------------> <--4b--> <---12b---> <----5b---->
///
/// Example layout for a row with a single non-nullable tuple (("hello", "world")) with
/// the second string slot stored externally to the stream:
///  <------ tuple 1 ------> <- var len ->  <- next row ...
/// +-----------+-----------+-------------+
/// | StringVal | StringVal |             | ...
/// +-----------+-----------+-------------+
/// | len: 5    | len: 5    |  hello      | ...
/// | ptr: 0x.. | ptr: 0x.. |             | ...
/// +-----------+-----------+-------------+
///  <---12b---> <---12b---> <-----5b---->
///
/// The behavior of reads and writes is as follows:
/// Read:
///   1. Unpinned: Only a single read page is pinned at a time. This means that only
///     enough reservation to pin a single page is needed to read the stream, regardless
///     of the stream's size. Each page is deleted or unpinned (if delete on read is true
///     or false respectively) before advancing to the next page.
///   2. Pinned: All pages in the stream are pinned so do not need to be pinned or
///     unpinned when reading from the stream. If delete on read is true, pages are
///     deleted after being read.
/// Write:
///   1. Unpinned: Unpin pages as they fill up. This means that only a enough reservation
///     to pin a single write page is required to write to the stream, regardless of the
///     stream's size.
///   2. Pinned: Pages are left pinned. If the next page in the stream cannot be pinned
///     because the caller's reservation is insufficient (and could not be increased by
///     the stream), the read call will fail and the caller can either unpin the stream
///     or free up other memory before retrying.
///
/// Memory lifetime of rows read from stream:
/// If the stream is pinned and delete on read is false, it is valid to access any tuples
/// returned via GetNext() or GetRows() until the stream is unpinned. If the stream is
/// unpinned or delete on read is true, then the batch returned from GetNext() may have
/// the needs_deep_copy flag set, which means that any tuple memory returned so far from
/// the stream may be freed on the next call to GetNext().
/// TODO: IMPALA-4179, instead of needs_deep_copy, attach the pages' buffers to the batch.
///
/// Manual construction of rows with AllocateRow():
/// The BufferedTupleStream supports allocation of uninitialized rows with AllocateRow().
/// AllocateRow() is called instead of AddRow() if the caller wants to manually construct
/// a row. The caller of AllocateRow() is responsible for writing the row with exactly the
/// layout described above.
///
/// If a caller constructs a tuple in this way, the caller can set the pointers and they
/// will not be modified until the stream is read via GetNext() or GetRows().
/// TODO: IMPALA-5007: try to remove AllocateRow() by unifying with AddRow().
///
/// TODO: we need to be able to do read ahead for pages. We need some way to indicate a
/// page will need to be pinned soon.
class BufferedTupleStreamV2 {
 public:
  /// A pointer to the start of a flattened TupleRow in the stream.
  typedef uint8_t* FlatRowPtr;

  /// row_desc: description of rows stored in the stream. This is the desc for rows
  /// that are added and the rows being returned.
  /// page_len: the size of pages to use in the stream
  /// TODO:IMPALA-3208: support a default and maximum page length
  /// ext_varlen_slots: set of varlen slots with data stored externally to the stream
  BufferedTupleStreamV2(RuntimeState* state, const RowDescriptor& row_desc,
      BufferPool::ClientHandle* buffer_pool_client, int64_t page_len,
      const std::set<SlotId>& ext_varlen_slots = std::set<SlotId>());

  virtual ~BufferedTupleStreamV2();

  /// Initializes the tuple stream object on behalf of node 'node_id'. Must be called
  /// once before any of the other APIs.
  /// If 'pinned' is true, the tuple stream starts off pinned, otherwise it is unpinned.
  /// 'node_id' is only used for error reporting.
  Status Init(int node_id, bool pinned) WARN_UNUSED_RESULT;

  /// Prepares the stream for writing by attempting to allocate a write buffer. Tries to
  /// increase reservation if there is not enough unused reservation for the buffer.
  /// Called after Init() and before the first AddRow() or AllocateRow() call.
  /// 'got_reservation': set to true if there was enough reservation to initialize the
  ///     first write page and false if there was not enough reservation and no other
  ///     error was encountered. Undefined if an error status is returned.
  Status PrepareForWrite(bool* got_reservation) WARN_UNUSED_RESULT;

  /// Prepares the stream for interleaved reads and writes by allocating read and write
  /// buffers. Called after Init() and before the first AddRow() or AllocateRow() call.
  /// delete_on_read: Pages are deleted after they are read.
  /// 'got_reservation': set to true if there was enough reservation to initialize the
  ///     read and write pages and false if there was not enough reservation and no other
  ///     error was encountered. Undefined if an error status is returned.
  Status PrepareForReadWrite(
      bool delete_on_read, bool* got_reservation) WARN_UNUSED_RESULT;

  /// Prepares the stream for reading, invalidating the write iterator (if there is one).
  /// Therefore must be called after the last AddRow() or AllocateRow() and before
  /// GetNext(). PrepareForRead() can be called multiple times to do multiple read passes
  /// over the stream, unless PrepareForRead() or PrepareForReadWrite() was previously
  /// called with delete_on_read = true.
  /// delete_on_read: Pages are deleted after they are read.
  /// 'got_reservation': set to true if there was enough reservation to initialize the
  ///     first read page and false if there was not enough reservation and no other
  ///     error was encountered. Undefined if an error status is returned.
  Status PrepareForRead(bool delete_on_read, bool* got_reservation) WARN_UNUSED_RESULT;

  /// Adds a single row to the stream. There are three possible outcomes:
  /// a) The append succeeds. True is returned.
  /// b) The append fails because the unused reservation was not sufficient to add
  ///   a new page to the stream and the stream could not increase the reservation
  ///   sufficiently. Returns false and sets 'status' to OK. The append can be retried
  ///   after freeing up memory or unpinning the stream.
  /// c) The append fails with a runtime error. Returns false and sets 'status' to an
  ///   error.
  /// d) The append fails becase the row is too large to fit in a page of a stream.
  ///   Returns false and sets 'status' to an error.
  ///
  /// Unpinned streams avoid case b) because memory is automatically freed up by
  /// unpinning the current write page.
  /// TODO: IMPALA-3808: update to reflect behaviour with variable-length pages
  ///
  /// BufferedTupleStream will do a deep copy of the memory in the row. After AddRow()
  /// returns an error, it should not be called again.
  bool AddRow(TupleRow* row, Status* status) noexcept WARN_UNUSED_RESULT;

  /// Allocates space to store a row of with fixed length 'fixed_size' and variable
  /// length data 'varlen_size'. If successful, returns the pointer where fixed length
  /// data should be stored and assigns 'varlen_data' to where var-len data should
  /// be stored.  AllocateRow does not currently support nullable tuples.
  ///
  /// The meaning of the return values are the same as AddRow(), except failure is
  /// indicated by returning NULL instead of false.
  uint8_t* AllocateRow(
      int fixed_size, int varlen_size, uint8_t** varlen_data, Status* status);

  /// Unflattens 'flat_row' into a regular TupleRow 'row'. Only valid to call if the
  /// stream is pinned. The row must have been allocated with the stream's row desc.
  /// The returned 'row' is backed by memory from the stream so is only valid as long
  /// as the stream is pinned.
  void GetTupleRow(FlatRowPtr flat_row, TupleRow* row) const;

  /// Pins all pages in this stream and switches to pinned mode. Has no effect if the
  /// stream is already pinned.
  /// If the current unused reservation is not sufficient to pin the stream in memory,
  /// this will try to increase the reservation. If that fails, 'pinned' is set to false
  /// and the stream is left unpinned. Otherwise 'pinned' is set to true.
  Status PinStream(bool* pinned) WARN_UNUSED_RESULT;

  /// Modes for UnpinStream().
  enum UnpinMode {
    /// All pages in the stream are unpinned and the read/write positions in the stream
    /// are reset. No more rows can be written to the stream after this. The stream can
    /// be re-read from the beginning by calling PrepareForRead().
    UNPIN_ALL,
    /// All pages are unpinned aside from the current read and write pages (if any),
    /// which is left in the same state. The unpinned stream can continue being read
    /// or written from the current read or write positions.
    UNPIN_ALL_EXCEPT_CURRENT,
  };

  /// Unpins stream with the given 'mode' as described above.
  void UnpinStream(UnpinMode mode);

  /// Get the next batch of output rows, which are backed by the stream's memory.
  /// If the stream is unpinned or 'delete_on_read' is true, the 'needs_deep_copy'
  /// flag may be set on 'batch' to signal that memory will be freed on the next
  /// call to GetNext() and that the caller should copy out any data it needs from
  /// rows in 'batch' or in previous batches returned from GetNext().
  ///
  /// If the stream is pinned and 'delete_on_read' is false, the memory backing the
  /// rows will remain valid until the stream is unpinned, destroyed, etc.
  /// TODO: IMPALA-4179: update when we simplify the memory transfer model.
  Status GetNext(RowBatch* batch, bool* eos) WARN_UNUSED_RESULT;

  /// Same as above, but populate 'flat_rows' with a pointer to the flat version of
  /// each returned row in the pinned stream. The pointers in 'flat_rows' are only
  /// valid as long as the stream remains pinned.
  Status GetNext(
      RowBatch* batch, bool* eos, std::vector<FlatRowPtr>* flat_rows) WARN_UNUSED_RESULT;

  /// Returns all the rows in the stream in batch. This pins the entire stream in the
  /// process. If the current unused reservation is not sufficient to pin the stream in
  /// memory, this will try to increase the reservation. If that fails, 'got_rows' is set
  /// to false.
  Status GetRows(MemTracker* tracker, boost::scoped_ptr<RowBatch>* batch,
      bool* got_rows) WARN_UNUSED_RESULT;

  /// Must be called once at the end to cleanup all resources. If 'batch' is non-NULL,
  /// attaches buffers from any pinned pages to the batch and deletes unpinned
  /// pages. Otherwise deletes all pages. Does nothing if the stream was already
  /// closed. The 'flush' mode is forwarded to RowBatch::AddBuffer() when attaching
  /// buffers.
  void Close(RowBatch* batch, RowBatch::FlushMode flush);

  /// Number of rows in the stream.
  int64_t num_rows() const { return num_rows_; }

  /// Number of rows returned via GetNext().
  int64_t rows_returned() const { return rows_returned_; }

  /// Returns the byte size necessary to store the entire stream in memory.
  int64_t byte_size() const { return total_byte_size_; }

  /// Returns the number of bytes currently pinned in memory by the stream.
  /// If ignore_current is true, the write_page_ memory is not included.
  int64_t BytesPinned(bool ignore_current) const {
    if (ignore_current && write_page_ != nullptr && write_page_->is_pinned()) {
      return bytes_pinned_ - write_page_->len();
    }
    return bytes_pinned_;
  }

  bool is_closed() const { return closed_; }
  bool is_pinned() const { return pinned_; }
  bool has_read_iterator() const { return read_page_ != pages_.end(); }
  bool has_write_iterator() const { return write_page_ != nullptr; }

  std::string DebugString() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(BufferedTupleStreamV2);
  friend class ArrayTupleStreamTest_TestArrayDeepCopy_Test;
  friend class ArrayTupleStreamTest_TestComputeRowSize_Test;
  friend class MultiNullableTupleStreamTest_TestComputeRowSize_Test;
  friend class SimpleTupleStreamTest_TestGetRowsOverflow_Test;

  /// Wrapper around BufferPool::PageHandle that tracks additional info about the page.
  struct Page {
    Page() : num_rows(0) {}

    inline int len() const { return handle.len(); }
    inline uint8_t* data() const { return handle.data(); }
    inline bool is_pinned() const { return handle.is_pinned(); }
    inline int pin_count() const { return handle.pin_count(); }
    std::string DebugString() const;

    BufferPool::PageHandle handle;

    /// Number of rows written to the page.
    int num_rows;
  };

  /// Runtime state instance used to check for cancellation. Not owned.
  RuntimeState* const state_;

  /// Description of rows stored in the stream.
  const RowDescriptor& desc_;

  /// Sum of the fixed length portion of all the tuples in desc_, including any null
  /// indicators.
  int fixed_tuple_row_size_;

  /// The size of the fixed length portion for each tuple in the row.
  std::vector<int> fixed_tuple_sizes_;

  /// Vectors of all the strings slots that have their varlen data stored in stream
  /// grouped by tuple_idx.
  std::vector<std::pair<int, std::vector<SlotDescriptor*>>> inlined_string_slots_;

  /// Vectors of all the collection slots that have their varlen data stored in the
  /// stream, grouped by tuple_idx.
  std::vector<std::pair<int, std::vector<SlotDescriptor*>>> inlined_coll_slots_;

  /// Buffer pool and client used to allocate, pin and release pages. Not owned.
  BufferPool* buffer_pool_;
  BufferPool::ClientHandle* buffer_pool_client_;

  /// List of pages in the stream.
  /// Empty before PrepareForWrite() is called or after the stream has been destructively
  /// read in 'delete_on_read' mode. Non-empty otherwise.
  std::list<Page> pages_;

  /// Total size of pages_, including any pages already deleted in 'delete_on_read'
  /// mode.
  int64_t total_byte_size_;

  /// Iterator pointing to the current page for reading. Equal to list.end() when no
  /// read iterator is active. GetNext() does not advance this past the end of
  /// the stream, so upon eos 'read_page_' points to the last page and
  /// rows_returned_ == num_rows_. Always pinned, unless a Pin() call failed and an
  /// error status was returned.
  std::list<Page>::iterator read_page_;

  /// Number of rows returned from the current read_page_.
  uint32_t read_page_rows_returned_;

  /// Pointer into read_page_ to the byte after the last row read.
  uint8_t* read_ptr_;

  /// Pointer into write_page_ to the byte after the last row written.
  uint8_t* write_ptr_;

  /// Pointer to one byte past the end of write_page_. Cached to speed up computation
  uint8_t* write_end_ptr_;

  /// Number of rows returned to the caller from GetNext() since the last
  /// PrepareForRead() call.
  int64_t rows_returned_;

  /// The current page for writing. NULL if there is no available page to write to.
  /// Always pinned. If 'read_page_' and 'write_page_' reference the same page, then
  /// that page is only pinned once.
  Page* write_page_;

  /// Total bytes of pinned pages in pages_, stored to avoid iterating over the list
  /// to compute it.
  int64_t bytes_pinned_;

  /// Number of rows stored in the stream. Includes rows that were already deleted during
  /// a destructive 'delete_on_read' pass over the stream.
  int64_t num_rows_;

  /// The length in bytes of pages used to store the stream's rows.
  /// TODO: IMPALA-3808: support variable-length pages
  const int64_t page_len_;

  /// Whether any tuple in the rows is nullable.
  const bool has_nullable_tuple_;

  /// If true, pages are deleted after they are read.
  bool delete_on_read_;

  bool closed_; // Used for debugging.

  /// If true, this stream has been explicitly pinned by the caller and all pages are
  /// kept pinned until the caller calls UnpinStream().
  bool pinned_;

  bool is_read_page(const Page* page) const {
    return has_read_iterator() && &*read_page_ == page;
  }

  bool is_write_page(const Page* page) const { return write_page_ == page; }

  /// The slow path for AddRow() that is called if there is not sufficient space in
  /// the current page.
  bool AddRowSlow(TupleRow* row, Status* status) noexcept;

  /// The slow path for AllocateRow() that is called if there is not sufficient space in
  /// the current page.
  uint8_t* AllocateRowSlow(
      int fixed_size, int varlen_size, uint8_t** varlen_data, Status* status) noexcept;

  /// Copies 'row' into write_page_. Returns false if there is not enough space in
  /// 'write_page_'. After returning false, write_ptr_ may be left pointing to the
  /// partially-written row, and no more data can be written to write_page_.
  template <bool HAS_NULLABLE_TUPLE>
  bool DeepCopyInternal(TupleRow* row) noexcept;

  /// Helper function to copy strings in string_slots from tuple into write_page_.
  /// Updates write_ptr_ to the end of the string data added. Returns false if the data
  /// does not fit in the current write page. After returning false, write_ptr_ is left
  /// pointing to the partially-written row, and no more data can be written to
  /// write_page_.
  bool CopyStrings(const Tuple* tuple, const std::vector<SlotDescriptor*>& string_slots);

  /// Helper function to deep copy collections in collection_slots from tuple into
  /// write_page_. Updates write_ptr_ to the end of the collection data added. Returns
  /// false if the data does not fit in the current write page. After returning false,
  /// write_ptr_ is left pointing to the partially-written row, and no more data can be
  /// written to write_page_.
  bool CopyCollections(
      const Tuple* tuple, const std::vector<SlotDescriptor*>& collection_slots);

  /// Wrapper of the templated DeepCopyInternal() function.
  bool DeepCopy(TupleRow* row) noexcept;

  /// Gets a new page of 'page_len_' bytes from buffer_pool_, updating write_page_,
  /// write_ptr_ and write_end_ptr_. The caller must ensure there is sufficient unused
  /// reservation to allocate the page. The caller must reset the write iterator (if
  /// there is one).
  Status NewWritePage() noexcept WARN_UNUSED_RESULT;

  /// Validates that a page can fit a row of 'row_size' bytes.
  /// Returns an error if the row cannot fit in a page.
  Status CheckPageSizeForRow(int64_t row_size);

  /// Wrapper around NewWritePage() that allocates a new write page that fits a row of
  /// 'row_size' bytes. Increases reservation if needed to allocate the next page.
  /// Returns OK and sets 'got_reservation' to true if the write page was successfully
  /// allocated. Returns an error if the row cannot fit in a page. Returns OK and sets
  /// 'got_reservation' to false if the reservation could not be increased and no other
  /// error was encountered.
  Status AdvanceWritePage(
      int64_t row_size, bool* got_reservation) noexcept WARN_UNUSED_RESULT;

  /// Reset the write page, if there is one, and unpin pages accordingly.
  void ResetWritePage();

  /// Same as PrepareForRead(), except the iterators are not invalidated and
  /// the caller is assumed to have checked there is sufficient unused reservation.
  Status PrepareForReadInternal(bool delete_on_read) WARN_UNUSED_RESULT;

  /// Pins the next read page. This blocks reading from disk if necessary to bring the
  /// page's data into memory. Updates read_page_, read_ptr_, and
  /// read_page_rows_returned_.
  Status NextReadPage() WARN_UNUSED_RESULT;

  /// Reset the read page, if there is one, and unpin pages accordingly.
  void ResetReadPage();

  /// Returns the total additional bytes that this row will consume in write_page_ if
  /// appended to the page. This includes the row's null indicators, the fixed length
  /// part of the row and the data for inlined_string_slots_ and inlined_coll_slots_.
  int64_t ComputeRowSize(TupleRow* row) const noexcept;

  /// Pins page and updates tracking stats.
  Status PinPage(Page* page) WARN_UNUSED_RESULT;

  /// Increment the page's pin count if this page needs a higher pin count given the
  /// current read and write iterator positions and whether the stream will be pinned
  /// ('stream_pinned'). Assumes that no scenarios occur when the pin count needs to
  /// be incremented multiple times. The caller is responsible for ensuring sufficient
  /// reservation is available.
  Status PinPageIfNeeded(Page* page, bool stream_pinned) WARN_UNUSED_RESULT;

  /// Decrement the page's pin count if this page needs a lower pin count given the
  /// current read and write iterator positions and whether the stream will be pinned
  /// ('stream_pinned'). Assumes that no scenarios occur when the pin count needs to
  /// be decremented multiple times.
  void UnpinPageIfNeeded(Page* page, bool stream_pinned);

  /// Return the expected pin count for 'page' in the current stream based on the current
  /// read and write pages and whether the stream is pinned.
  int ExpectedPinCount(bool stream_pinned, const Page* page) const;

  /// Templated GetNext implementations.
  template <bool FILL_FLAT_ROWS>
  Status GetNextInternal(RowBatch* batch, bool* eos, std::vector<FlatRowPtr>* flat_rows);
  template <bool FILL_FLAT_ROWS, bool HAS_NULLABLE_TUPLE>
  Status GetNextInternal(RowBatch* batch, bool* eos, std::vector<FlatRowPtr>* flat_rows);

  /// Helper function to convert a flattened TupleRow stored starting at '*data' into
  /// 'row'. *data is updated to point to the first byte past the end of the row.
  template <bool HAS_NULLABLE_TUPLE>
  void UnflattenTupleRow(uint8_t** data, TupleRow* row) const;

  /// Helper function for GetNextInternal(). For each string slot in string_slots,
  /// update StringValue's ptr field to point to the corresponding string data stored
  /// inline in the stream (at the current value of read_ptr_) advance read_ptr_ by the
  /// StringValue's length field.
  void FixUpStringsForRead(const vector<SlotDescriptor*>& string_slots, Tuple* tuple);

  /// Helper function for GetNextInternal(). For each collection slot in collection_slots,
  /// recursively update any pointers in the CollectionValue to point to the corresponding
  /// var len data stored inline in the stream, advancing read_ptr_ as data is read.
  /// Assumes that the collection was serialized to the stream in DeepCopy()'s format.
  void FixUpCollectionsForRead(
      const vector<SlotDescriptor*>& collection_slots, Tuple* tuple);

  /// Returns the number of null indicator bytes per row. Only valid if this stream has
  /// nullable tuples.
  int NullIndicatorBytesPerRow() const;

  /// Returns the total bytes pinned. Only called in DCHECKs to validate bytes_pinned_.
  int64_t CalcBytesPinned() const;

  /// DCHECKs if the stream is internally inconsistent. The stream should always be in
  /// a consistent state after returning success from a public API call.
  void CheckConsistency() const;
};
}

#endif
