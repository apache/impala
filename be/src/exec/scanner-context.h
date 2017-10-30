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


#ifndef IMPALA_EXEC_SCANNER_CONTEXT_H
#define IMPALA_EXEC_SCANNER_CONTEXT_H

#include <deque>

#include <boost/cstdint.hpp>
#include <boost/scoped_ptr.hpp>

#include "common/compiler-util.h"
#include "common/status.h"
#include "exec/filter-context.h"
#include "runtime/disk-io-mgr.h"

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
/// TODO: Some of the synchronization mechanisms such as cancelled() can be removed
/// once the legacy hdfs scan node has been removed.
class ScannerContext {
 public:
  /// Create a scanner context with the parent scan_node (where materialized row batches
  /// get pushed to) and the scan range to process.
  /// This context starts with 1 stream.
  ScannerContext(RuntimeState*, HdfsScanNodeBase*, HdfsPartitionDescriptor*,
      DiskIoMgr::ScanRange* scan_range, const std::vector<FilterContext>& filter_ctxs,
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
    ///    the ScannerContext and should not be modified.  If the buffer is entirely
    ///    from one disk io buffer, a pointer inside that buffer is returned directly.
    ///    If the requested buffer straddles io buffers, a copy is done here.
    ///  - *out_len is the number of bytes returned.
    ///  - *status is set if there is an error.
    /// Returns true if the call was successful (i.e. status->ok())
    /// This should only be called from the scanner thread.
    /// Note that this will return bytes past the end of the scan range until the end of
    /// the file.
    bool GetBytes(int64_t requested_len, uint8_t** buffer, int64_t* out_len,
        Status* status, bool peek = false);

    /// Gets the bytes from the first available buffer within the scan range. This may be
    /// the boundary buffer used to stitch IO buffers together.
    /// If we are past the end of the scan range, no bytes are returned.
    Status GetBuffer(bool peek, uint8_t** buffer, int64_t* out_len);

    /// Sets whether of not the resulting tuples contain ptrs into memory owned by
    /// the scanner context. This by default, is inferred from the scan_node tuple
    /// descriptor (i.e. contains string slots) but can be overridden.  If possible,
    /// this should be set to false to reduce memory usage as resources can be reused
    /// and recycled more quickly.
    void set_contains_tuple_data(bool v) { contains_tuple_data_ = v; }

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
    int64_t bytes_left() { return scan_range_->len() - total_bytes_returned_; }

    /// If true, all bytes in this scan range have been returned or we have reached eof
    /// (the scan range could be longer than the file).
    bool eosr() const { return total_bytes_returned_ >= scan_range_->len() || eof(); }

    /// If true, the stream has reached the end of the file.
    bool eof() const { return file_offset() == file_len_; }

    const char* filename() { return scan_range_->file(); }
    const DiskIoMgr::ScanRange* scan_range() { return scan_range_; }
    const HdfsFileDesc* file_desc() { return file_desc_; }

    /// Returns the buffer's current offset in the file.
    int64_t file_offset() const { return scan_range_->offset() + total_bytes_returned_; }

    /// Returns the total number of bytes returned
    int64_t total_bytes_returned() { return total_bytes_returned_; }

    /// Read a Boolean primitive value written using Java serialization.
    /// Equivalent to java.io.DataInput.readBoolean()
    bool ReadBoolean(bool* boolean, Status*);

    /// Read an Integer primitive value written using Java serialization.
    /// Equivalent to java.io.DataInput.readInt()
    bool ReadInt(int32_t* val, Status*, bool peek = false);

    /// Read a variable-length Long value written using Writable serialization.
    /// Ref: org.apache.hadoop.io.WritableUtils.readVLong()
    bool ReadVLong(int64_t* val, Status*);

    /// Read a variable length Integer value written using Writable serialization.
    /// Ref: org.apache.hadoop.io.WritableUtils.readVInt()
    bool ReadVInt(int32_t* val, Status*);

    /// Read a zigzag encoded long
    bool ReadZLong(int64_t* val, Status*);

    /// Skip over the next length bytes in the specified HDFS file.
    bool SkipBytes(int64_t length, Status*);

    /// Read length bytes into the supplied buffer.  The returned buffer is owned
    /// by this object.
    bool ReadBytes(int64_t length, uint8_t** buf, Status*, bool peek = false);

    /// Read a Writable Text value from the supplied file.
    /// Ref: org.apache.hadoop.io.WritableUtils.readString()
    /// The returned buffer is owned by this object.
    bool ReadText(uint8_t** buf, int64_t* length, Status*);

    /// Skip this text object.
    bool SkipText(Status*);

    /// If 'batch' is not NULL and 'contains_tuple_data_' is true, attaches all completed
    /// io buffers and the boundary mem pool to 'batch'. If 'done' is set, all in-flight
    /// resources are also attached or released.
    /// If 'batch' is NULL then 'done' must be true or 'contains_tuple_data_' false. Such
    /// a call will release all completed resources. If 'done' is true all in-flight
    /// resources are also freed.
    void ReleaseCompletedResources(RowBatch* batch, bool done);

   private:
    friend class ScannerContext;
    ScannerContext* parent_;
    DiskIoMgr::ScanRange* scan_range_;
    const HdfsFileDesc* file_desc_;

    /// If true, tuples will contain pointers into memory contained in this object.
    /// That memory (io buffers or boundary buffers) must be attached to the row batch.
    bool contains_tuple_data_;

    /// Total number of bytes returned from GetBytes()
    int64_t total_bytes_returned_;

    /// File length. Initialized with file_desc_->file_length but updated if eof is found
    /// earlier, i.e. the file was truncated.
    int64_t file_len_;

    /// Callback if a scanner wants to implement custom logic for guessing how far to
    /// read past the end of the scan range.
    ReadPastSizeCallback read_past_size_cb_;

    /// The next amount we should read past the end of the file, if using the default
    /// doubling algorithm. Unused if 'read_past_size_cb_' is set.
    int64_t next_read_past_size_bytes_;

    /// The current io buffer. This starts as NULL before we've read any bytes.
    std::unique_ptr<DiskIoMgr::BufferDescriptor> io_buffer_;

    /// Next byte to read in io_buffer_
    uint8_t* io_buffer_pos_;

    /// Bytes left in io_buffer_
    int64_t io_buffer_bytes_left_;

    /// The boundary buffer is used to copy multiple IO buffers from the scan range into a
    /// single buffer to return to the scanner.  After copying all or part of an IO buffer
    /// into the boundary buffer, the current buffer's state is updated to no longer
    /// include the copied bytes (e.g., io_buffer_bytes_left_ is decremented).
    /// Conceptually, the data in the boundary buffer always comes before that in the
    /// current buffer, and all the bytes in the stream are either already returned to the
    /// scanner, in the current IO buffer, or in the boundary buffer.
    boost::scoped_ptr<MemPool> boundary_pool_;
    boost::scoped_ptr<StringBuffer> boundary_buffer_;
    uint8_t* boundary_buffer_pos_;
    int64_t boundary_buffer_bytes_left_;

    /// Points to either io_buffer_pos_ or boundary_buffer_pos_
    /// (initialized to NULL before calling GetBytes())
    uint8_t** output_buffer_pos_;

    /// Points to either io_buffer_bytes_left_ or boundary_buffer_bytes_left_
    /// (initialized to a static zero-value int before calling GetBytes())
    int64_t* output_buffer_bytes_left_;

    /// List of buffers that are completed but still have bytes referenced by the caller.
    /// On the next GetBytes() call, these buffers are released (the caller by calling
    /// GetBytes() signals it is done with its previous bytes).  At this point the
    /// buffers are either returned to the io mgr or attached to the current row batch.
    std::deque<std::unique_ptr<DiskIoMgr::BufferDescriptor>> completed_io_buffers_;

    Stream(ScannerContext* parent);

    /// GetBytes helper to handle the slow path.
    /// If peek is set then return the data but do not move the current offset.
    Status GetBytesInternal(int64_t requested_len, uint8_t** buffer, bool peek,
                            int64_t* out_len);

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

  /// If a non-NULL 'batch' is passed, attaches completed io buffers and boundary mem pools
  /// from all streams to 'batch'. Attaching only completed resources ensures that buffers
  /// (and their cleanup) trail the rows that reference them (row batches are consumed and
  /// cleaned up in order by the rest of the query).
  /// If 'done' is true, this is the final call for the current streams and any pending
  /// resources in each stream are also passed to the row batch. Callers which want to
  /// clear the streams from the context should also call ClearStreams().
  ///
  /// A NULL 'batch' may be passed to free all resources. It is only valid to pass a NULL
  /// 'batch' when also passing 'done'.
  ///
  /// This must be called with 'done' set when the scanner is complete and no longer needs
  /// any resources (e.g. tuple memory, io buffers) returned from the current streams.
  /// After calling with 'done' set, this should be called again if new streams are
  /// created via AddStream().
  void ReleaseCompletedResources(RowBatch* batch, bool done);

  /// Releases all the Stream objects in the vector 'streams_' and reduces the vector's
  /// size to 0.
  void ClearStreams();

  /// Add a stream to this ScannerContext for 'range'. Returns the added stream.
  /// The stream is created in the runtime state's object pool
  Stream* AddStream(DiskIoMgr::ScanRange* range);

  /// Returns false if scan_node_ is multi-threaded and has been cancelled.
  /// Always returns false if the scan_node_ is not multi-threaded.
  bool cancelled() const;

  int num_completed_io_buffers() const { return num_completed_io_buffers_; }
  HdfsPartitionDescriptor* partition_descriptor() { return partition_desc_; }
  const std::vector<FilterContext>& filter_ctxs() const { return filter_ctxs_; }
  MemPool* expr_results_pool() const { return expr_results_pool_; }
 private:
  friend class Stream;

  RuntimeState* state_;
  HdfsScanNodeBase* scan_node_;

  HdfsPartitionDescriptor* partition_desc_;

  /// Vector of streams. Non-columnar formats will always have one stream per context.
  std::vector<std::unique_ptr<Stream>> streams_;

  /// Always equal to the sum of completed_io_buffers_.size() across all streams.
  int num_completed_io_buffers_;

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

#endif
