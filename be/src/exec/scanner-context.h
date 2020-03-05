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

#pragma once

#include <cstdint>
#include <deque>

#include <boost/scoped_ptr.hpp>

#include "common/compiler-util.h"
#include "common/status.h"
#include "exec/filter-context.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/io/request-ranges.h"

namespace impala {

struct HdfsFileDesc;
class HdfsPartitionDescriptor;
class HdfsScanNodeBase;
class MemPool;
class RowBatch;
class RuntimeState;
class StringBuffer;
class Tuple;
class TupleRow;

/// This class abstracts over getting buffers from the IoMgr. Each ScannerContext is 1:1
/// a HdfsScanner. ScannerContexts contain Streams, which are 1:1 with a ScanRange.
/// Columnar formats have multiple streams per context object.
/// This class handles stitching data split across IO buffers and providing
/// some basic parsing utilities.
/// This class it *not* thread safe. It is designed to have a single scanner thread
/// reading from it.
//
/// Each scanner context maps to a single hdfs split.  There are three threads that
/// are interacting with the context.
///   1. IoMgr threads that read io buffers from the disk and enqueue them to the
///      stream's underlying ScanRange object. This is the producer.
///   2. Scanner thread that calls GetBytes() (which can block), materializing tuples
///      from processing the bytes. This is the consumer.
///   3. The scan node/main thread which calls into the context to trigger cancellation
///      or other end of stream conditions.
///
/// Memory management
/// =================
/// Pointers into memory returned from stream methods remain valid until either
/// ReleaseCompletedResources() is called or an operation advances the stream's read
/// offset past the end of the memory .
/// E.g. if ReadBytes(peek=false) is called, the memory returned is invalidated when
/// ReadBytes(), SkipBytes(), ReadVint(), etc is called. If the memory is obtained by
/// a "peeking" operation, then the memory returned remains valid until the read offset
/// in the stream is advanced past the end of the memory. E.g. if
/// ReadBuffer(n, peek=true) is called, then the memory remains valid if SkipBytes(n)
/// is called the first time, but not if SkipBytes() is called again to advance further.
///
/// Each stream only requires a single I/O buffer to make progress on reading through the
/// stream. Additional I/O buffers allow the I/O manager to read ahead in the scan range.
/// The scanner context also allocates memory from a MemPool for reads that straddle I/O
/// buffers (e.g. a small read at the boundary of I/O buffers or a read larger than
/// a single I/O buffer). The amount of memory allocated from the MemPool is determined
/// by the maximum buffer size read from the stream, plus some overhead. E.g.
/// ReadBytes(length=50KB) requires allocating a 50KB buffer from the MemPool if the
/// read straddles a buffer boundary.
///
/// TODO: Some of the synchronization mechanisms such as cancelled() can be removed
/// once the legacy hdfs scan node has been removed.
class ScannerContext {
 public:
  /// Create a scanner context with the parent scan_node (where materialized row batches
  /// get pushed to) and the scan range to process. Buffers are allocated using
  /// 'bp_client'. 'total_reservation' bytes of 'bp_client''s reservation has been
  /// initally allotted for use by this scanner.
  ScannerContext(RuntimeState* state, HdfsScanNodeBase* scan_node,
      BufferPool::ClientHandle* bp_client, int64_t total_reservation,
      HdfsPartitionDescriptor* partition_desc,
      const std::vector<FilterContext>& filter_ctxs,
      MemPool* expr_results_pool);
  /// Destructor verifies that all stream objects have been released.
  ~ScannerContext();

  /// Encapsulates a stream (continuous byte range) that can be read.  A context
  /// can contain one or more streams.  For non-columnar files, there is only
  /// one stream; for columnar, there is one stream per column.
  class Stream {
   public:
    /// Returns up to requested_len bytes or an error.  This can block if bytes are not
    /// available.
    ///  - requested_len is the number of bytes requested.  This function will return
    ///    those number of bytes unless end of file or an error occurred.
    ///  - If peek is true, the scan range position is not incremented (i.e. repeated calls
    ///    with peek = true will return the same data).
    ///  - *buffer on return is a pointer to the buffer.  The memory is owned by
    ///    the ScannerContext and should not be modified. The contents of the buffer are
    ///    invalidated after subsequent calls to GetBytes()/ReadBytes(). If the buffer
    ///    is entirely from one disk io buffer, a pointer inside that buffer is returned
    ///    directly. If the requested buffer straddles io buffers, a copy is done here.
    ///  - *out_len is the number of bytes returned.
    ///  - *status is set if there is an error.
    /// Returns true if the call was successful (i.e. status->ok())
    /// This should only be called from the scanner thread.
    /// Note that this will return bytes past the end of the scan range until the end of
    /// the file.
    bool GetBytes(int64_t requested_len, uint8_t** buffer, int64_t* out_len,
        Status* status, bool peek = false) WARN_UNUSED_RESULT;

    /// Gets the bytes from the first available buffer within the scan range. This may be
    /// the boundary buffer used to stitch IO buffers together.
    /// If we are past the end of the scan range, no bytes are returned.
    Status GetBuffer(bool peek, uint8_t** buffer, int64_t* out_len);

    /// Callback that returns the buffer size to use when reading past the end of the scan
    /// range. Reading past the end of the scan range is likely a remote read, so we want
    /// find a good trade-off between io requests and data volume. Scanners that have
    /// some information about the optimal read size can provide this callback to
    /// override the default read-size doubling strategy (see GetNextBuffer()). If the
    /// callback returns a positive length, this overrides the default strategy. If the
    /// callback returns a length greater than the max read size, the max read size will
    /// be used.
    ///
    /// The callback takes the file offset of the asynchronous read (this may be more
    /// than file_offset() due to data being assembled in the boundary buffer).
    typedef boost::function<int (int64_t)> ReadPastSizeCallback;
    void set_read_past_size_cb(ReadPastSizeCallback cb) { read_past_size_cb_ = cb; }

    /// Return the number of bytes left in the range for this stream.
    int64_t bytes_left() { return scan_range_->bytes_to_read() - total_bytes_returned_; }

    /// If true, all bytes in this scan range have been returned from this ScannerContext
    /// to callers or we hit eof before reaching the end of the scan range. Callers can
    /// continue to call Read*()/Get*()/Skip*() methods on the stream until eof() is true.
    bool eosr() const {
      return total_bytes_returned_ >= scan_range_->bytes_to_read() || eof();
    }

    /// If true, the stream has reached the end of the file. After this is true, any
    /// Read*()/Get*()/Skip*() methods will not succeed.
    bool eof() const { return file_offset() == file_len_; }

    const char* filename() { return scan_range_->file(); }
    const io::ScanRange* scan_range() { return scan_range_; }
    const HdfsFileDesc* file_desc() { return file_desc_; }
    int64_t reservation() const { return reservation_; }

    /// Returns the buffer's current offset in the file.
    int64_t file_offset() const { return scan_range_->offset() + total_bytes_returned_; }

    /// Returns the total number of bytes returned
    int64_t total_bytes_returned() { return total_bytes_returned_; }

    /// Read a Boolean primitive value written using Java serialization. Returns true
    /// on success, otherwise returns false and sets 'status' to indicate the error.
    /// Equivalent to java.io.DataInput.readBoolean()
    bool ReadBoolean(bool* boolean, Status* status) WARN_UNUSED_RESULT;

    /// Read an Integer primitive value written using Java serialization. Returns true
    /// on success, otherwise returns false and sets 'status' to indicate the error.
    /// Equivalent to java.io.DataInput.readInt()
    bool ReadInt(int32_t* val, Status* status, bool peek = false) WARN_UNUSED_RESULT;

    /// Read a variable-length Long value written using Writable serialization. Returns
    /// true on success, otherwise returns false and sets 'status' to indicate the error.
    /// Ref: org.apache.hadoop.io.WritableUtils.readVLong()
    bool ReadVLong(int64_t* val, Status* status) WARN_UNUSED_RESULT;

    /// Read a variable length Integer value written using Writable serialization. Returns
    /// true on success, otherwise returns false and sets 'status' to indicate the error.
    /// Ref: org.apache.hadoop.io.WritableUtils.readVInt()
    bool ReadVInt(int32_t* val, Status* status) WARN_UNUSED_RESULT;

    /// Read a zigzag encoded long. Returns true on success, otherwise returns false and
    /// sets 'status' to indicate the error.
    bool ReadZLong(int64_t* val, Status* status) WARN_UNUSED_RESULT;

    /// Skip over the next length bytes in the specified HDFS file. Returns true on
    /// success, otherwise returns false and sets 'status' to indicate the error.
    bool SkipBytes(int64_t length, Status* status) WARN_UNUSED_RESULT;

    /// Read length bytes into the supplied buffer.  The returned buffer is owned
    /// by this object The memory is owned by and should not be modified. The contents
    /// of the buffer are invalidated after subsequent calls to GetBytes()/ReadBytes().
    /// Returns true on success, otherwise returns false and sets 'status' to
    /// indicate the error.
    bool ReadBytes(int64_t length, uint8_t** buf, Status* status, bool peek = false)
        WARN_UNUSED_RESULT;

    /// Read a Writable Text value from the supplied file. Returns true on success,
    /// otherwise returns false and sets 'status' to indicate the error.
    /// Ref: org.apache.hadoop.io.WritableUtils.readString()
    /// The returned buffer is owned by this object.
    bool ReadText(uint8_t** buf, int64_t* length, Status* status) WARN_UNUSED_RESULT;

    /// Skip this text object. Returns true on success, otherwise returns false and
    /// sets 'status'
    bool SkipText(Status* status) WARN_UNUSED_RESULT;

    /// Release completed resources, e.g. the last buffer if the current read position is
    /// at the end of the buffer. If 'done' is true all resources are freed, even if the
    /// caller has not read that data yet. After calling this function, any memory
    /// returned from previous Read*()/Get*() functions is invalid to reference.
    ///
    /// Also see the ScannerContext::ReleaseCompletedResources() comment.
    void ReleaseCompletedResources(bool done);

   private:
    friend class ScannerContext;
    ScannerContext* const parent_;
    io::ScanRange* const scan_range_;
    const HdfsFileDesc* const file_desc_;

    /// Reservation given to this stream for allocating I/O buffers. The reservation is
    /// shared with 'scan_range_', so the context must be careful not to use this until
    /// all of 'scan_ranges_'s buffers have been freed. Must be >= the minimum IoMgr
    /// buffer size to allow reading past the end of 'scan_range_'.
    const int64_t reservation_;

    /// Total number of bytes returned from GetBytes()
    int64_t total_bytes_returned_ = 0;

    /// File length. Initialized with file_desc_->file_length but updated if eof is found
    /// earlier, i.e. the file was truncated.
    int64_t file_len_;

    /// Callback if a scanner wants to implement custom logic for guessing how far to
    /// read past the end of the scan range.
    ReadPastSizeCallback read_past_size_cb_;

    /// The next amount we should read past the end of the file, if using the default
    /// doubling algorithm. Unused if 'read_past_size_cb_' is set.
    int64_t next_read_past_size_bytes_;

    /// The current I/O buffer. NULL before we've read any bytes or if the last read
    /// I/O buffer was released.
    std::unique_ptr<io::BufferDescriptor> io_buffer_;

    /// True if 'scan_range_' returned eosr, which means that we read to the end of that
    /// scan range. This is different from eosr() because it tracks whether the
    /// scan range reached eosr, not whether eosr() was returned to the caller.
    bool scan_range_eosr_ = false;

    /// Next byte to read in io_buffer_
    uint8_t* io_buffer_pos_ = nullptr;

    /// Bytes left in io_buffer_
    int64_t io_buffer_bytes_left_ = 0;

    /// The boundary buffer is used to copy multiple IO buffers from the scan range into a
    /// single buffer to return to the scanner.  After copying all or part of an IO buffer
    /// into the boundary buffer, the current buffer's state is updated to no longer
    /// include the copied bytes (e.g., io_buffer_bytes_left_ is decremented).
    /// Conceptually, the data in the boundary buffer always comes before that in the
    /// current buffer, and all the bytes in the stream are either already returned to the
    /// scanner, in the current IO buffer, or in the boundary buffer.
    boost::scoped_ptr<MemPool> boundary_pool_;
    boost::scoped_ptr<StringBuffer> boundary_buffer_;
    uint8_t* boundary_buffer_pos_ = nullptr;
    int64_t boundary_buffer_bytes_left_ = 0;

    /// Points to either io_buffer_pos_ or boundary_buffer_pos_
    /// (initialized to NULL before calling GetBytes())
    uint8_t** output_buffer_pos_ = nullptr;

    /// Points to either io_buffer_bytes_left_ or boundary_buffer_bytes_left_
    /// (initialized to a static zero-value int before calling GetBytes())
    int64_t* output_buffer_bytes_left_ =
        const_cast<int64_t*>(&OUTPUT_BUFFER_BYTES_LEFT_INIT);

    /// We always want output_buffer_bytes_left_ to be non-NULL, so we can avoid a NULL
    /// check in GetBytes(). We use this variable, which is set to 0, to initialize
    /// output_buffer_bytes_left_. After the first successful call to GetBytes(),
    /// output_buffer_bytes_left_ will be set to something else.
    static const int64_t OUTPUT_BUFFER_BYTES_LEFT_INIT = 0;

    /// Private constructor. See AddStream() for public API.
    Stream(ScannerContext* parent, io::ScanRange* scan_range, int64_t reservation,
        const HdfsFileDesc* file_desc);

    /// GetBytes helper to handle the slow path.
    /// If 'peek' is true then return the data but do not move the current offset.
    /// If 'peek' is not true, the returned buffer memory remains valid until next
    /// operation that reads from the stream.
    Status GetBytesInternal(int64_t requested_len, uint8_t** buffer, bool peek,
                            int64_t* out_len);

    /// SkipBytes() helper to handle the slow path where we need to skip past the
    /// current I/O buffer. Called when the current I/O and boundary buffers are
    /// exhausted.  Skips 'bytes_left' bytes in subsequent I/O buffers. 'length' is the
    /// argument to the SkipBytes() call, used for error reporting. Sets 'io_buffer_',
    /// 'io_buffer_pos_', 'io_buffer_bytes_left_' and 'total_bytes_returned_'.
    bool SkipBytesInternal(int64_t length, int64_t bytes_left, Status* status);

    /// Copy 'num_bytes' bytes from the I/O buffer at 'io_buffer_pos_' to the
    /// boundary buffer and set 'output_buffer_pos_' and 'output_buffer_bytes_left_'
    /// to point at the boundary buffer variables. Advances 'io_buffer_pos_' and
    /// 'io_buffer_bytes_left_' by 'num_bytes'. Returns an error if the boundary
    /// buffer cannot be extended to fit the new data.
    ///
    /// Returns 'io_buffer_' to the I/O manager if all its data was copied to the
    /// boundary buffer.
    Status CopyIoToBoundary(int64_t num_bytes);

    /// Returns 'io_buffer_' to the I/O manager, setting it to NULL in the process,
    /// and resets 'io_buffer_bytes_left_' and 'io_buffer_pos_'.
    void ReturnIoBuffer();

    /// Gets (and blocks) for the next io buffer. After fetching all buffers in the scan
    /// range, performs synchronous reads past the scan range until EOF.
    //
    /// When performing a synchronous read, the read size is the max of 'read_past_size'
    /// and either the result of read_past_size_cb_(), or the result of iteratively
    /// doubling INIT_READ_PAST_SIZE up to the max read size. 'read_past_size' is not
    /// used otherwise. This is done to find a balance between reading too much data
    /// and issuing too many small reads.
    ///
    /// Updates 'io_buffer_', 'io_buffer_bytes_left_', and 'io_buffer_pos_'.  If
    /// GetNextBuffer() is called after all bytes in the file have been returned,
    /// 'io_buffer_bytes_left_' will be set to 0. In the non-error case, 'io_buffer_' is
    /// never set to NULL, even if it contains 0 bytes.
    Status GetNextBuffer(int64_t read_past_size = 0);

    /// Helper to advance position and bytes left for a buffer by 'bytes'.
    void AdvanceBufferPos(
        int64_t bytes, uint8_t** buffer_pos, int64_t* buffer_bytes_left);

    /// Validates that the output buffer pointers point to the correct buffer.
    bool ValidateBufferPointers() const;

    /// Error-reporting functions.
    Status ReportIncompleteRead(int64_t length, int64_t bytes_read);
    Status ReportInvalidRead(int64_t length);
    Status ReportInvalidInt();
  };

  Stream* GetStream(int idx = 0) {
    DCHECK_GE(idx, 0);
    DCHECK_LT(idx, streams_.size());
    return streams_[idx].get();
  }

  int NumStreams() const { return streams_.size(); }

  /// Tries to increase 'total_reservation()' to 'ideal_reservation'. May get
  /// none, part or all of the requested increase. total_reservation() can be
  /// checked by the caller to find out the new total reservation. When this
  /// ScannerContext is destroyed, the scan node takes back ownership of
  /// total_reservation().
  void TryIncreaseReservation(int64_t ideal_reservation);

  /// Release completed resources for all streams, e.g. the last buffer in each stream if
  /// the current read position is at the end of the buffer. If 'done' is true all
  /// resources are freed, even if the caller has not read that data yet. After calling
  /// this function, any memory returned from previous Read*()/Get*() functions is
  /// invalid to reference. Callers which want to clear the streams from the context
  /// should also call ClearStreams().
  ///
  /// This must be called with 'done' set when the scanner is complete and no longer needs
  /// any resources. After calling with 'done' set, this should be called again if new
  /// streams are created via AddStream().
  void ReleaseCompletedResources(bool done);

  /// Releases all the Stream objects in the vector 'streams_' and reduces the vector's
  /// size to 0.
  void ClearStreams();

  /// Add a stream to this ScannerContext for 'range'. 'range' must already have any
  /// buffers that it needs allocated. 'reservation' is the amount of reservation that
  /// is given to this stream for allocating I/O buffers. The reservation is shared with
  /// 'range', so the context must be careful not to use this until all of 'range's
  /// buffers have been freed. Must be >= the minimum IoMgr buffer size to allow reading
  /// past the end of 'range'. 'reservation' must be <=
  /// ScannerContext::total_reservation(), i.e. this reservation is included in the total.
  ///
  /// Returns the added stream. The returned stream is owned by this context.
  Stream* AddStream(io::ScanRange* range, int64_t reservation);

  /// Add and start a stream to this ScannerContext for 'range'.
  /// Returns the added stream through output parameter 'stream'.
  Status AddAndStartStream(
      io::ScanRange* range, int64_t reservation, ScannerContext::Stream** stream);

  /// Returns true if RuntimeState::is_cancelled() is true, or if scan node is not
  /// multi-threaded and is done (finished, cancelled or reached it's limit).
  /// In all other cases returns false.
  bool cancelled() const;

  BufferPool::ClientHandle* bp_client() const { return bp_client_; }
  int64_t total_reservation() const { return total_reservation_; }
  HdfsPartitionDescriptor* partition_descriptor() const { return partition_desc_; }
  const std::vector<FilterContext>& filter_ctxs() const { return filter_ctxs_; }
  MemPool* expr_results_pool() const { return expr_results_pool_; }
 private:
  friend class Stream;

  RuntimeState* const state_;
  HdfsScanNodeBase* const scan_node_;

  /// Buffer pool client used to allocate I/O buffers. This is accessed by multiple
  /// threads in the multi-threaded scan node, so those threads must take care to only
  /// call thread-safe BufferPool methods with this client.
  BufferPool::ClientHandle* const bp_client_;

  /// Total reservation from 'bp_client_' that this scanner is allowed to use.
  /// TODO: when we remove the multi-threaded scan node, we may be able to just use
  /// bp_client_->Reservation()
  int64_t total_reservation_;

  HdfsPartitionDescriptor* const partition_desc_;

  /// Vector of streams. Non-columnar formats will always have one stream per context.
  std::vector<std::unique_ptr<Stream>> streams_;

  /// Filter contexts for all filters applicable to this scan. Memory attached to the
  /// context is owned by the scan node.
  std::vector<FilterContext> filter_ctxs_;

  /// MemPool used for allocations that hold results of expression evaluation in the
  /// scanner and 'filter_ctxs_'. Must be thread-local since MemPool is not thread-safe.
  /// Owned by ScannerThread() in the multi-threaded scan node and by the ExecNode in the
  /// single-threaded scan node implementation.
  ///
  /// The scanner is responsible for clearing the MemPool periodically after expression
  /// evaluation to prevent memory from accumulating.
  ///
  /// TODO: IMPALA-6015: it should be possible to simplify the lifecycle of this pool and
  /// filter_ctxs_ once the multithreaded scan node is removed.
  MemPool* const expr_results_pool_;
};

}
