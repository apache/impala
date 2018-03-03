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

#ifndef IMPALA_RUNTIME_IO_REQUEST_RANGES_H
#define IMPALA_RUNTIME_IO_REQUEST_RANGES_H

#include <cstdint>
#include <deque>

#include <boost/thread/mutex.hpp>

#include "common/hdfs.h"
#include "common/status.h"
#include "util/condition-variable.h"
#include "util/internal-queue.h"

namespace impala {
class MemTracker;

namespace io {
class DiskIoMgr;
class RequestContext;
class ExclusiveHdfsFileHandle;
class ScanRange;

/// Buffer struct that is used by the caller and IoMgr to pass read buffers.
/// It is is expected that only one thread has ownership of this object at a
/// time.
class BufferDescriptor {
 public:
  ~BufferDescriptor() {
    DCHECK(buffer_ == nullptr); // Check we didn't leak a buffer.
  }

  ScanRange* scan_range() { return scan_range_; }
  uint8_t* buffer() { return buffer_; }
  int64_t buffer_len() { return buffer_len_; }
  int64_t len() { return len_; }
  bool eosr() { return eosr_; }

  /// Returns the offset within the scan range that this buffer starts at
  int64_t scan_range_offset() const { return scan_range_offset_; }

  /// Transfer ownership of buffer memory from 'mem_tracker_' to 'dst' and set
  /// 'mem_tracker_' to 'dst'. 'mem_tracker_' and 'dst' must be non-NULL. Does not
  /// check memory limits on 'dst': the caller should check the memory limit if a
  /// different memory limit may apply to 'dst'. If the buffer was a client-provided
  /// buffer, transferring is not allowed.
  /// TODO: IMPALA-3209: revisit this as part of scanner memory usage revamp.
  void TransferOwnership(MemTracker* dst);

 private:
  friend class DiskIoMgr;
  friend class ScanRange;
  friend class RequestContext;

  /// Create a buffer descriptor for a new reader, range and data buffer. The buffer
  /// memory should already be accounted against 'mem_tracker'.
  BufferDescriptor(DiskIoMgr* io_mgr, RequestContext* reader,
      ScanRange* scan_range, uint8_t* buffer, int64_t buffer_len,
      MemTracker* mem_tracker);

  /// Return true if this is a cached buffer owned by HDFS.
  bool is_cached() const;

  /// Return true if this is a buffer owner by the client that was provided when
  /// constructing the scan range.
  bool is_client_buffer() const;

  DiskIoMgr* const io_mgr_;

  /// Reader that this buffer is for.
  RequestContext* const reader_;

  /// The current tracker this buffer is associated with. After initialisation,
  /// NULL for cached buffers and non-NULL for all other buffers.
  MemTracker* mem_tracker_;

  /// Scan range that this buffer is for. Non-NULL when initialised.
  ScanRange* const scan_range_;

  /// buffer with the read contents
  uint8_t* buffer_;

  /// length of buffer_. For buffers from cached reads, the length is 0.
  const int64_t buffer_len_;

  /// length of read contents
  int64_t len_ = 0;

  /// true if the current scan range is complete
  bool eosr_ = false;

  /// Status of the read to this buffer. if status is not ok, 'buffer' is nullptr
  Status status_;

  int64_t scan_range_offset_ = 0;
};

/// The request type, read or write associated with a request range.
struct RequestType {
  enum type {
    READ,
    WRITE,
  };
};

/// Represents a contiguous sequence of bytes in a single file.
/// This is the common base class for read and write IO requests - ScanRange and
/// WriteRange. Each disk thread processes exactly one RequestRange at a time.
class RequestRange : public InternalQueue<RequestRange>::Node {
 public:
  hdfsFS fs() const { return fs_; }
  const char* file() const { return file_.c_str(); }
  std::string* file_string() { return &file_; }
  int64_t offset() const { return offset_; }
  int64_t len() const { return len_; }
  int disk_id() const { return disk_id_; }
  RequestType::type request_type() const { return request_type_; }

 protected:
  RequestRange(RequestType::type request_type)
    : fs_(nullptr), offset_(-1), len_(-1), disk_id_(-1), request_type_(request_type) {}

  /// Hadoop filesystem that contains file_, or set to nullptr for local filesystem.
  hdfsFS fs_;

  /// Path to file being read or written.
  std::string file_;

  /// Offset within file_ being read or written.
  int64_t offset_;

  /// Length of data read or written.
  int64_t len_;

  /// Id of disk containing byte range.
  int disk_id_;

  /// The type of IO request, READ or WRITE.
  RequestType::type request_type_;
};

/// Param struct for different combinations of buffering.
struct BufferOpts {
 public:
  // Set options for a read into an IoMgr-allocated or HDFS-cached buffer. Caching is
  // enabled if 'try_cache' is true, the file is in the HDFS cache and 'mtime' matches
  // the modified time of the cached file in the HDFS cache.
  BufferOpts(bool try_cache, int64_t mtime)
    : try_cache_(try_cache),
      mtime_(mtime),
      client_buffer_(nullptr),
      client_buffer_len_(-1) {}

  /// Set options for an uncached read into an IoMgr-allocated buffer.
  static BufferOpts Uncached() {
    return BufferOpts(false, NEVER_CACHE, nullptr, -1);
  }

  /// Set options to read the entire scan range into 'client_buffer'. The length of the
  /// buffer, 'client_buffer_len', must fit the entire scan range. HDFS caching is not
  /// enabled in this case.
  static BufferOpts ReadInto(uint8_t* client_buffer, int64_t client_buffer_len) {
    return BufferOpts(false, NEVER_CACHE, client_buffer, client_buffer_len);
  }

 private:
  friend class ScanRange;

  BufferOpts(
      bool try_cache, int64_t mtime, uint8_t* client_buffer, int64_t client_buffer_len)
    : try_cache_(try_cache),
      mtime_(mtime),
      client_buffer_(client_buffer),
      client_buffer_len_(client_buffer_len) {}

  /// If 'mtime_' is set to NEVER_CACHE, the file handle will never be cached, because
  /// the modification time won't match.
  const static int64_t NEVER_CACHE = -1;

  /// If true, read from HDFS cache if possible.
  const bool try_cache_;

  /// Last modified time of the file associated with the scan range. If set to
  /// NEVER_CACHE, caching is disabled.
  const int64_t mtime_;

  /// A destination buffer provided by the client, nullptr and -1 if no buffer.
  uint8_t* const client_buffer_;
  const int64_t client_buffer_len_;
};

/// ScanRange description. The caller must call Reset() to initialize the fields
/// before calling AddScanRanges(). The private fields are used internally by
/// the IoMgr.
class ScanRange : public RequestRange {
 public:
  ScanRange();

  virtual ~ScanRange();

  /// Resets this scan range object with the scan range description. The scan range
  /// is for bytes [offset, offset + len) in 'file' on 'fs' (which is nullptr for the
  /// local filesystem). The scan range must fall within the file bounds (offset >= 0
  /// and offset + len <= file_length). 'disk_id' is the disk queue to add the range
  /// to. If 'expected_local' is true, a warning is generated if the read did not
  /// come from a local disk. 'buffer_opts' specifies buffer management options -
  /// see the DiskIoMgr class comment and the BufferOpts comments for details.
  /// 'meta_data' is an arbitrary client-provided pointer for any auxiliary data.
  void Reset(hdfsFS fs, const char* file, int64_t len, int64_t offset, int disk_id,
      bool expected_local, const BufferOpts& buffer_opts, void* meta_data = nullptr);

  void* meta_data() const { return meta_data_; }
  bool try_cache() const { return try_cache_; }
  bool expected_local() const { return expected_local_; }

  /// Returns the next buffer for this scan range. buffer is an output parameter.
  /// This function blocks until a buffer is ready or an error occurred. If this is
  /// called when all buffers have been returned, *buffer is set to nullptr and Status::OK
  /// is returned.
  /// Only one thread can be in GetNext() at any time.
  Status GetNext(std::unique_ptr<BufferDescriptor>* buffer) WARN_UNUSED_RESULT;

  /// Cancel this scan range. This cleans up all queued buffers and
  /// wakes up any threads blocked on GetNext().
  /// Status is the reason the range was cancelled. Must not be ok().
  /// Status is returned to the user in GetNext().
  void Cancel(const Status& status);

  /// return a descriptive string for debug.
  std::string DebugString() const;

  int64_t mtime() const { return mtime_; }

 private:
  friend class BufferDescriptor;
  friend class DiskIoMgr;
  friend class RequestContext;

  /// Initialize internal fields
  void InitInternal(DiskIoMgr* io_mgr, RequestContext* reader);

  /// Enqueues a buffer for this range. This does not block.
  /// Returns true if this scan range has hit the queue capacity, false otherwise.
  /// The caller passes ownership of buffer to the scan range and it is not
  /// valid to access buffer after this call. The reader lock must be held by the
  /// caller.
  bool EnqueueBuffer(const boost::unique_lock<boost::mutex>& reader_lock,
      std::unique_ptr<BufferDescriptor> buffer);

  /// Cleanup any queued buffers (i.e. due to cancellation). This cannot
  /// be called with any locks taken.
  void CleanupQueuedBuffers();

  /// Validates the internal state of this range. lock_ must be taken
  /// before calling this.
  bool Validate();

  /// Maximum length in bytes for hdfsRead() calls.
  int64_t MaxReadChunkSize() const;

  /// Opens the file for this range. This function only modifies state in this range.
  /// If 'use_file_handle_cache' is true and this is a local hdfs file, then this scan
  /// range will not maintain an exclusive file handle. It will borrow an hdfs file
  /// handle from the file handle cache for each Read(), so Open() does nothing.
  /// If 'use_file_handle_cache' is false or this is a remote hdfs file or this is
  /// a local OS file, Open() will open a file handle on the scan range for
  /// exclusive use by this scan range. The scan range is the exclusive owner of the
  /// file handle, and the file handle is destroyed in Close().
  /// All local OS files are opened using normal OS file APIs.
  Status Open(bool use_file_handle_cache) WARN_UNUSED_RESULT;

  /// Closes the file for this range. This function only modifies state in this range.
  void Close();

  /// Reads from this range into 'buffer', which has length 'buffer_len' bytes. Returns
  /// the number of bytes read. The read position in this scan range is updated.
  Status Read(uint8_t* buffer, int64_t buffer_len, int64_t* bytes_read,
      bool* eosr) WARN_UNUSED_RESULT;

  /// Get the read statistics from the Hdfs file handle and aggregate them to
  /// the RequestContext. This clears the statistics on this file handle.
  /// It is safe to pass hdfsFile by value, as hdfsFile's underlying type is a
  /// pointer.
  void GetHdfsStatistics(hdfsFile fh);

  /// Reads from the DN cache. On success, sets cached_buffer_ to the DN buffer
  /// and *read_succeeded to true.
  /// If the data is not cached, returns ok() and *read_succeeded is set to false.
  /// Returns a non-ok status if it ran into a non-continuable error.
  ///  The reader lock must be held by the caller.
  Status ReadFromCache(const boost::unique_lock<boost::mutex>& reader_lock,
      bool* read_succeeded) WARN_UNUSED_RESULT;

  /// Pointer to caller specified metadata. This is untouched by the io manager
  /// and the caller can put whatever auxiliary data in here.
  void* meta_data_ = nullptr;

  /// If true, this scan range is expected to be cached. Note that this might be wrong
  /// since the block could have been uncached. In that case, the cached path
  /// will fail and we'll just put the scan range on the normal read path.
  bool try_cache_ = false;

  /// If true, we expect this scan range to be a local read. Note that if this is false,
  /// it does not necessarily mean we expect the read to be remote, and that we never
  /// create scan ranges where some of the range is expected to be remote and some of it
  /// local.
  /// TODO: we can do more with this
  bool expected_local_ = false;

  /// Total number of bytes read remotely. This is necessary to maintain a count of
  /// the number of remote scan ranges. Since IO statistics can be collected multiple
  /// times for a scan range, it is necessary to keep some state about whether this
  /// scan range has already been counted as remote. There is also a requirement to
  /// log the number of unexpected remote bytes for a scan range. To solve both
  /// requirements, maintain num_remote_bytes_ on the ScanRange and push it to the
  /// reader_ once at the close of the scan range.
  int64_t num_remote_bytes_;

  DiskIoMgr* io_mgr_ = nullptr;

  /// Reader/owner of the scan range
  RequestContext* reader_ = nullptr;

  /// File handle either to hdfs or local fs (FILE*)
  /// The hdfs file handle is stored here in three cases:
  /// 1. The file handle cache is off (max_cached_file_handles == 0).
  /// 2. The scan range is using hdfs caching.
  /// -OR-
  /// 3. The hdfs file is expected to be remote (expected_local_ == false)
  /// In each case, the scan range gets a new ExclusiveHdfsFileHandle at Open(),
  /// owns it exclusively, and destroys it in Close().
  union {
    FILE* local_file_ = nullptr;
    ExclusiveHdfsFileHandle* exclusive_hdfs_fh_;
  };

  /// Tagged union that holds a buffer for the cases when there is a buffer allocated
  /// externally from DiskIoMgr that is associated with the scan range.
  enum class ExternalBufferTag { CLIENT_BUFFER, CACHED_BUFFER, NO_BUFFER };
  ExternalBufferTag external_buffer_tag_;
  union {
    /// Valid if the 'external_buffer_tag_' is CLIENT_BUFFER.
    struct {
      /// Client-provided buffer to read the whole scan range into.
      uint8_t* data;

      /// Length of the client-provided buffer.
      int64_t len;
    } client_buffer_;

    /// Valid and non-NULL if the external_buffer_tag_ is CACHED_BUFFER, which means
    /// that a cached read succeeded and all the bytes for the range are in this buffer.
    struct hadoopRzBuffer* cached_buffer_ = nullptr;
  };

  /// Lock protecting fields below.
  /// This lock should not be taken during Open()/Read()/Close().
  /// If RequestContext::lock_ and this lock need to be held simultaneously,
  /// RequestContext::lock_ must be taken first.
  boost::mutex lock_;

  /// Number of bytes read so far for this scan range
  int bytes_read_;

  /// Status for this range. This is non-ok if is_cancelled_ is true.
  /// Note: an individual range can fail without the RequestContext being
  /// cancelled. This allows us to skip individual ranges.
  Status status_;

  /// If true, the last buffer for this scan range has been queued.
  bool eosr_queued_ = false;

  /// If true, the last buffer for this scan range has been returned.
  bool eosr_returned_ = false;

  /// If true, this scan range has been removed from the reader's in_flight_ranges
  /// queue because the ready_buffers_ queue is full.
  bool blocked_on_queue_ = false;

  /// IO buffers that are queued for this scan range.
  /// Condition variable for GetNext
  ConditionVariable buffer_ready_cv_;
  std::deque<std::unique_ptr<BufferDescriptor>> ready_buffers_;

  /// Lock that should be taken during hdfs calls. Only one thread (the disk reading
  /// thread) calls into hdfs at a time so this lock does not have performance impact.
  /// This lock only serves to coordinate cleanup. Specifically it serves to ensure
  /// that the disk threads are finished with HDFS calls before is_cancelled_ is set
  /// to true and cleanup starts.
  /// If this lock and lock_ need to be taken, lock_ must be taken first.
  boost::mutex hdfs_lock_;

  /// If true, this scan range has been cancelled.
  bool is_cancelled_ = false;

  /// Last modified time of the file associated with the scan range
  int64_t mtime_;
};

/// Used to specify data to be written to a file and offset.
/// It is the responsibility of the client to ensure that the data to be written is
/// valid and that the file to be written to exists until the callback is invoked.
/// A callback is invoked to inform the client when the write is done.
class WriteRange : public RequestRange {
 public:
  /// This callback is invoked on each WriteRange after the write is complete or the
  /// context is cancelled. The status returned by the callback parameter indicates
  /// if the write was successful (i.e. Status::OK), if there was an error
  /// TStatusCode::RUNTIME_ERROR) or if the context was cancelled
  /// (TStatusCode::CANCELLED). The callback is only invoked if this WriteRange was
  /// successfully added (i.e. AddWriteRange() succeeded). No locks are held while
  /// the callback is invoked.
  typedef std::function<void(const Status&)> WriteDoneCallback;
  WriteRange(const std::string& file, int64_t file_offset, int disk_id,
      WriteDoneCallback callback);

  /// Change the file and offset of this write range. Data and callbacks are unchanged.
  /// Can only be called when the write is not in flight (i.e. before AddWriteRange()
  /// is called or after the write callback was called).
  void SetRange(const std::string& file, int64_t file_offset, int disk_id);

  /// Set the data and number of bytes to be written for this WriteRange.
  /// Can only be called when the write is not in flight (i.e. before AddWriteRange()
  /// is called or after the write callback was called).
  void SetData(const uint8_t* buffer, int64_t len);

  const uint8_t* data() const { return data_; }

 private:
  friend class DiskIoMgr;
  friend class RequestContext;
  friend class ScanRange;

  /// Data to be written. RequestRange::len_ contains the length of data
  /// to be written.
  const uint8_t* data_;

  /// Callback to invoke after the write is complete.
  WriteDoneCallback callback_;
};

inline bool BufferDescriptor::is_cached() const {
  return scan_range_->external_buffer_tag_
      == ScanRange::ExternalBufferTag::CACHED_BUFFER;
}

inline bool BufferDescriptor::is_client_buffer() const {
  return scan_range_->external_buffer_tag_
      == ScanRange::ExternalBufferTag::CLIENT_BUFFER;
}
}
}

#endif
