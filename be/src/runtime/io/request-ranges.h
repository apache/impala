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
#include <functional>

#include <boost/thread/mutex.hpp>
#include <gtest/gtest_prod.h> // for FRIEND_TEST

#include "common/atomic.h"
#include "common/hdfs.h"
#include "common/status.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "util/condition-variable.h"
#include "util/internal-queue.h"

namespace impala {
namespace io {
class DiskIoMgr;
class DiskQueue;
class ExclusiveHdfsFileHandle;
class FileReader;
class RequestContext;
class ScanRange;

/// Buffer struct that is used by the caller and IoMgr to pass read buffers.
/// It is is expected that only one thread has ownership of this object at a
/// time.
class BufferDescriptor {
 public:
  /// Create a buffer descriptor allocated from the buffer pool. Public to
  /// allow access by DiskIoMgr.
  BufferDescriptor(ScanRange* scan_range, BufferPool::ClientHandle* bp_client,
      BufferPool::BufferHandle handle);

  ~BufferDescriptor() {
    DCHECK(buffer_ == nullptr); // Check we didn't leak a buffer.
  }

  ScanRange* scan_range() { return scan_range_; }
  uint8_t* buffer() { return buffer_; }
  int64_t buffer_len() { return buffer_len_; }
  int64_t len() { return len_; }
  bool eosr() { return eosr_; }

 private:
  DISALLOW_COPY_AND_ASSIGN(BufferDescriptor);
  /// This class is tightly coupled with ScanRange. Making them friends is easiest.
  friend class ScanRange;
  friend class HdfsFileReader;

  /// Create a buffer descriptor for a range and data buffer.
  BufferDescriptor(ScanRange* scan_range, uint8_t* buffer, int64_t buffer_len);

  /// Return true if this is a cached buffer owned by HDFS.
  bool is_cached() const;

  /// Return true if this is a buffer owner by the client that was provided when
  /// constructing the scan range.
  bool is_client_buffer() const;

  /// Releases memory resources for this buffer. If the buffer was allocated with
  /// DiskIoMgr::AllocateBuffersForRange(), frees the buffer. Otherwise (e.g. a client
  /// or HDFS cache buffer), just prepares this descriptor to be destroyed. After this
  /// is called, buffer() is NULL. Does not acquire 'lock_'.
  void Free();

  /// Scan range that this buffer is for. Non-NULL when initialised.
  ScanRange* const scan_range_;

  /// Buffer for the read contents. Must be set to NULL in Free() before destruction
  /// of a descriptor.
  uint8_t* buffer_;

  /// length of buffer_. For buffers from cached reads, the length is 0.
  const int64_t buffer_len_;

  /// length of read contents
  int64_t len_ = 0;

  /// true if the current scan range is complete
  bool eosr_ = false;

  // Handle to an allocated buffer and the client used to allocate it buffer. Only used
  // for non-external buffers.
  BufferPool::ClientHandle* bp_client_ = nullptr;
  BufferPool::BufferHandle handle_;
};

/// The request type, read or write associated with a request range.
struct RequestType {
  enum type {
    READ,
    WRITE,
  };
};

/// ReadOutput describes the possible outcomes of the DoRead() function.
enum class ReadOutcome {
  // The last (eosr) buffer was successfully enqueued.
  SUCCESS_EOSR,
  // The buffer was successfully enqueued but we are not at eosr and can schedule
  // the next read.
  SUCCESS_NO_EOSR,
  // The scan range is blocked waiting for the next buffer.
  BLOCKED_ON_BUFFER,
  // The scan range is cancelled (either by caller or because of an error). No more
  // reads will be scheduled.
  CANCELLED
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

  /// Use only when you don't want to to read the entire scan range, but only sub-ranges
  /// in it. In this case you can copy the relevant parts from the HDFS cache into the
  /// client buffer. The length of the buffer, 'client_buffer_len' must fit the
  /// concatenation of all the sub-ranges.
  static BufferOpts ReadInto(bool try_cache, int64_t mtime, uint8_t* client_buffer,
      int64_t client_buffer_len) {
    return BufferOpts(try_cache, mtime, client_buffer, client_buffer_len);
  }

 private:
  friend class ScanRange;
  friend class HdfsFileReader;
  FRIEND_TEST(DataCacheTest, TestBasics);

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

  /// Defines an internal range within this ScanRange.
  struct SubRange {
    int64_t offset;
    int64_t length;
  };

  /// Resets this scan range object with the scan range description. The scan range
  /// is for bytes [offset, offset + len) in 'file' on 'fs' (which is nullptr for the
  /// local filesystem). The scan range must be non-empty and fall within the file bounds
  /// (len > 0 and offset >= 0 and offset + len <= file_length). 'disk_id' is the disk
  /// queue to add the range to. If 'expected_local' is true, a warning is generated if
  /// the read did not come from a local disk. 'buffer_opts' specifies buffer management
  /// options - see the DiskIoMgr class comment and the BufferOpts comments for details.
  /// 'meta_data' is an arbitrary client-provided pointer for any auxiliary data.
  ///
  /// TODO: IMPALA-4249: clarify if a ScanRange can be reused after Reset(). Currently
  /// it is not generally safe to do so, but some unit tests reuse ranges after
  /// successfully reading to eos.
  void Reset(hdfsFS fs, const char* file, int64_t len, int64_t offset, int disk_id,
      bool expected_local, bool is_erasure_coded, const BufferOpts& buffer_opts,
      void* meta_data = nullptr);

  /// Same as above, but it also adds sub-ranges. No need to merge contiguous sub-ranges
  /// in advance, as this method will do the merge.
  void Reset(hdfsFS fs, const char* file, int64_t len, int64_t offset, int disk_id,
      bool expected_local, bool is_erasure_coded, const BufferOpts& buffer_opts,
      std::vector<SubRange>&& sub_ranges, void* meta_data = nullptr);

  void* meta_data() const { return meta_data_; }
  bool try_cache() const { return try_cache_; }
  bool read_in_flight() const { return read_in_flight_; }
  bool expected_local() const { return expected_local_; }
  bool is_erasure_coded() const { return is_erasure_coded_; }
  int64_t bytes_to_read() const { return bytes_to_read_; }

  /// Returns the next buffer for this scan range. buffer is an output parameter.
  /// This function blocks until a buffer is ready or an error occurred. If this is
  /// called when all buffers have been returned, *buffer is set to nullptr and Status::OK
  /// is returned. If this returns buffer->eos() or an error status, then all buffers
  /// owned by the scan range were either returned to callers of GetNext() or freed.
  /// Only one thread can be in GetNext() at any time.
  Status GetNext(std::unique_ptr<BufferDescriptor>* buffer) WARN_UNUSED_RESULT;

  /// Returns the buffer to the scan range. This must be called for every buffer
  /// returned by GetNext(). After calling this, the buffer descriptor is invalid
  /// and cannot be accessed.
  void ReturnBuffer(std::unique_ptr<BufferDescriptor> buffer);

  /// Cancel this scan range. This waits for any in-flight read operations to complete,
  /// cleans up all buffers owned by the scan range (i.e. queued or unused buffers)
  /// and wakes up any threads blocked on GetNext(). Status is a non-ok status with the
  /// reason the range was cancelled, e.g. CANCELLED_INTERNALLY if the range was cancelled
  /// because it was not needed, or another error if an error was encountered while
  /// scanning the range. Status is returned to the any callers of GetNext().
  void Cancel(const Status& status);

  /// return a descriptive string for debug.
  std::string DebugString() const;

  int64_t mtime() const { return mtime_; }

  bool HasSubRanges() const { return !sub_ranges_.empty(); }

 private:
  DISALLOW_COPY_AND_ASSIGN(ScanRange);

  /////////////////////////////////////////
  /// BEGIN: private members that are accessed by other io:: classes
  friend class BufferDescriptor;
  friend class DiskQueue;
  friend class DiskIoMgr;
  friend class DiskIoMgrTest;
  friend class RequestContext;
  friend class HdfsFileReader;
  friend class LocalFileReader;

  // Tag for the buffer associated with range. See external_buffer_tag_ for details.
  enum class ExternalBufferTag { CLIENT_BUFFER, CACHED_BUFFER, NO_BUFFER };

  /// Initialize internal fields
  void InitInternal(DiskIoMgr* io_mgr, RequestContext* reader);

  /// If data is cached, returns ok() and * read_succeeded is set to true. Also enqueues
  /// a ready buffer from the cached data.
  /// If the data is not cached, returns ok() and *read_succeeded is set to false.
  /// Returns a non-ok status if it ran into a non-continuable error.
  /// The reader lock must be held by the caller.
  Status ReadFromCache(const boost::unique_lock<boost::mutex>& reader_lock,
      bool* read_succeeded) WARN_UNUSED_RESULT;

  /// Add buffers for the range to read data into and schedule the range if blocked.
  /// If 'returned' is true, the buffers returned from GetNext() that are being recycled
  /// via ReturnBuffer(). Otherwise the buffers are newly allocated buffers to be added.
  void AddUnusedBuffers(
      std::vector<std::unique_ptr<BufferDescriptor>>&& buffers, bool returned);

  /// Called from a disk I/O thread to read the next buffer of data for this range. The
  /// returned ReadOutcome describes what the result of the read was. 'disk_id' is the
  /// ID of the disk queue. Caller must not hold 'lock_'.
  ReadOutcome DoRead(int disk_id);

  /// Cleans up a buffer that was not returned to the client.
  /// Either ReturnBuffer() or CleanUpBuffer() is called for every BufferDescriptor.
  /// The caller must hold 'lock_' via 'scan_range_lock'.
  /// This function may acquire 'file_reader_->lock()'
  void CleanUpBuffer(const boost::unique_lock<boost::mutex>& scan_range_lock,
      std::unique_ptr<BufferDescriptor> buffer);

  /// Same as CleanUpBuffer() except cleans up multiple buffers and caller must not
  /// hold 'lock_'.
  void CleanUpBuffers(std::vector<std::unique_ptr<BufferDescriptor>>&& buffers);

  /// Same as Cancel() except it doesn't remove the scan range from
  /// reader_->active_scan_ranges_ or call WaitForInFlightRead(). This allows for
  /// custom handling of in flight reads or active scan ranges. For example, this is
  /// invoked by RequestContext::Cancel(), which removes the range itself to avoid
  /// invalidating its active_scan_ranges_ iterator. It is also invoked by disk IO
  /// threads to propagate a read error for a range that is in flight (i.e. when
  /// read_error is true), so 'read_in_flight_' is set to false and threads in
  /// WaitForInFlightRead() are woken up. Note that this is tearing down the FileReader,
  /// so it may block waiting for other threads that are performing IO.
  void CancelInternal(const Status& status, bool read_error);

  /// Marks the scan range as blocked waiting for a buffer. Caller must not hold 'lock_'.
  void SetBlockedOnBuffer();

  ExternalBufferTag external_buffer_tag() const { return external_buffer_tag_; }

  /// If non-OK, this scan range has been cancelled. This status is the reason for
  /// cancellation - CANCELLED_INTERNALLY if cancelled without error, or another status
  /// if an error caused cancellation. Note that a range can be cancelled without
  /// cancelling the owning context. This means that ranges can be cancelled or hit errors
  /// without aborting all scan ranges.
  //
  /// Writers must hold both 'lock_' and 'file_reader_->lock()'. Readers must hold either
  /// 'lock_' or 'file_reader_->lock()'. This prevents the range from being cancelled
  /// while any thread is inside a critical section.
  Status cancel_status_;

  /// Only for testing
  void SetFileReader(std::unique_ptr<FileReader> file_reader);

  /// END: private members that are accessed by other io:: classes
  /////////////////////////////////////////

  /// Enqueues a ready buffer with valid data for this range. This does not block.
  /// The caller passes ownership of buffer to the scan range and it is not
  /// valid to access buffer after this call. Returns false if the scan range was
  /// cancelled.
  bool EnqueueReadyBuffer(std::unique_ptr<BufferDescriptor> buffer);

  /// Maximum length in bytes for hdfsRead() calls.
  int64_t MaxReadChunkSize() const;

  /// Get the read statistics from the Hdfs file handle and aggregate them to
  /// the RequestContext. This clears the statistics on this file handle.
  /// It is safe to pass hdfsFile by value, as hdfsFile's underlying type is a
  /// pointer.
  void GetHdfsStatistics(hdfsFile fh);

  /// Remove a buffer from 'unused_iomgr_buffers_' and update
  /// 'unused_iomgr_buffer_bytes_'. If 'unused_iomgr_buffers_' is empty, return NULL.
  /// 'lock_' must be held by the caller via 'scan_range_lock'.
  std::unique_ptr<BufferDescriptor> GetUnusedBuffer(
      const boost::unique_lock<boost::mutex>& scan_range_lock);

  /// Clean up all buffers in 'unused_iomgr_buffers_'. Only valid to call when the scan
  /// range is cancelled or at eos. The caller must hold 'lock_' via 'scan_range_lock'.
  void CleanUpUnusedBuffers(const boost::unique_lock<boost::mutex>& scan_range_lock);

  /// Waits for any in-flight read to complete. Called after CancelInternal() to ensure
  /// no more reads will occur for the scan range.
  void WaitForInFlightRead();

  /// Returns true if no more buffers will be returned to clients in the future,
  /// either because of hitting eosr or cancellation.
  bool all_buffers_returned(const boost::unique_lock<boost::mutex>& lock) const {
    DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
    return !cancel_status_.ok() || (eosr_queued_ && ready_buffers_.empty());
  }

  /// Adds sub-ranges to this ScanRange. If sub_ranges is not empty, then ScanRange won't
  /// read everything from its range, but will only read these sub-ranges.
  /// Sub-ranges need to be ordered by 'offset' and cannot overlap with each other.
  /// Doesn't need to merge continuous sub-ranges in advance, this method will do.
  void InitSubRanges(std::vector<SubRange>&& sub_ranges);

  /// Read the sub-ranges into buffer and track the current position in 'sub_range_pos_'.
  /// If cached data is available, then memcpy() from it instead of actually reading the
  /// files.
  Status ReadSubRanges(BufferDescriptor* buffer, bool* eof);

  /// Validates the internal state of this range. lock_ must be taken
  /// before calling this.
  bool Validate();

  /// Validates the sub-ranges. All sub-range must be inside of this ScanRange.
  /// They need to be ordered by offset and cannot overlap.
  bool ValidateSubRanges();

  /// Merges adjacent and continuous sub-ranges.
  void MergeSubRanges();

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

  /// If true, the file associated with the scan range is erasure coded. Set in Reset().
  bool is_erasure_coded_ = false;

  /// Last modified time of the file associated with the scan range. Set in Reset().
  int64_t mtime_;

  DiskIoMgr* io_mgr_ = nullptr;

  /// Reader/owner of the scan range
  RequestContext* reader_ = nullptr;

  /// Tagged union that holds a buffer for the cases when there is a buffer allocated
  /// externally from DiskIoMgr that is associated with the scan range.
  ExternalBufferTag external_buffer_tag_;

  /// Valid if the 'external_buffer_tag_' is CLIENT_BUFFER.
  struct {
    /// Client-provided buffer to read the whole scan range into.
    uint8_t* data = nullptr;

    /// Length of the client-provided buffer.
    int64_t len = 0;
  } client_buffer_;

  /// Valid if reading file contents from cache was successful.
  struct {
    /// Pointer to the contents of the file.
    uint8_t* data = nullptr;
    /// Length of the contents.
    int64_t len = 0;
  } cache_;

  /// The number of buffers that have been returned to a client via GetNext() that have
  /// not yet been returned with ReturnBuffer().
  AtomicInt32 num_buffers_in_reader_{0};

  /// Lock protecting fields below.
  /// This lock should not be taken during FileReader::Open()/Read()/Close().
  /// If RequestContext::lock_ and this lock need to be held simultaneously,
  /// RequestContext::lock_ must be taken first.
  boost::mutex lock_;

  /// Buffers to read into, used if the 'external_buffer_tag_' is NO_BUFFER. These are
  /// initially populated when the client calls AllocateBuffersForRange() and
  /// and are used to read scanned data into. Buffers are taken from this vector for
  /// every read and added back, if needed, when the client calls ReturnBuffer().
  std::vector<std::unique_ptr<BufferDescriptor>> unused_iomgr_buffers_;

  /// Total number of bytes of buffers in 'unused_iomgr_buffers_'.
  int64_t unused_iomgr_buffer_bytes_ = 0;

  /// Cumulative bytes of I/O mgr buffers taken from 'unused_iomgr_buffers_' by DoRead().
  /// Used to infer how many bytes of buffers need to be held onto to read the rest of
  /// the scan range.
  int64_t iomgr_buffer_cumulative_bytes_used_ = 0;

  /// True if a disk thread is currently doing a read for this scan range. Set to true in
  /// DoRead() and set to false in EnqueueReadyBuffer() or CancelInternal() when the
  /// read completes and any buffer used for the read is either enqueued or freed.
  bool read_in_flight_ = false;

  /// If true, the last buffer for this scan range has been queued.
  /// If this is true and 'ready_buffers_' is empty, then no more buffers will be
  /// returned to the caller by this scan range.
  bool eosr_queued_ = false;

  /// If true, this scan range is not scheduled because a buffer is not available for
  /// the next I/O in the range. This can happen when the scan range is initially created
  /// or if the buffers added to the range have all been filled with data an not yet
  /// returned.
  bool blocked_on_buffer_ = false;

  /// IO buffers that are queued for this scan range. When Cancel() is called
  /// this is drained by the cancelling thread. I.e. this is always empty if
  /// 'cancel_status_' is not OK.
  std::deque<std::unique_ptr<BufferDescriptor>> ready_buffers_;

  /// Condition variable for threads in GetNext() that are waiting for the next buffer
  /// and threads in WaitForInFlightRead() that are waiting for a read to finish.
  /// Signalled when a buffer is enqueued in 'ready_buffers_' or the scan range is
  /// cancelled.
  ConditionVariable buffer_ready_cv_;

  /// Number of bytes read by this scan range.
  int64_t bytes_read_ = 0;

  /// Polymorphic object that is responsible for doing file operations.
  std::unique_ptr<FileReader> file_reader_;

  /// If not empty, the ScanRange will only read these parts from the file.
  std::vector<SubRange> sub_ranges_;

  // Read position in the sub-ranges.
  struct SubRangePosition {
    /// Index of SubRange in 'ScanRange::sub_ranges_' to read next
    int64_t index = 0;
    /// Bytes already read from 'ScanRange::sub_ranges_[sub_range_index]'
    int64_t bytes_read = 0;
  };

  /// Current read position in the sub-ranges.
  SubRangePosition sub_range_pos_;

  /// Number of bytes need to be read by this ScanRange. If there are no sub-ranges it
  /// equals to 'len_'. If there are sub-ranges then it equals to the sum of the lengths
  /// of the sub-ranges (which is less than or equal to 'len_').
  int64_t bytes_to_read_ = 0;
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
  /// TErrorCode::RUNTIME_ERROR) or if the context was cancelled
  /// (TErrorCode::CANCELLED_INTERNALLY). The callback is only invoked if this
  /// WriteRange was successfully added (i.e. AddWriteRange() succeeded). No locks are
  /// held while the callback is invoked.
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
  WriteDoneCallback callback() const { return callback_; }

 private:
  DISALLOW_COPY_AND_ASSIGN(WriteRange);
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
