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
#include <functional>
#include <mutex>

#include <boost/thread/shared_mutex.hpp>
#include <gtest/gtest_prod.h> // for FRIEND_TEST

#include "common/atomic.h"
#include "common/hdfs.h"
#include "common/status.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/io/disk-file.h"
#include "runtime/io/scan-buffer-manager.h"
#include "util/internal-queue.h"

namespace impala {
namespace io {
class DiskIoMgr;
class DiskQueue;
class ExclusiveHdfsFileHandle;
class FileReader;
class FileWriter;
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
  friend class ScanBufferManager;

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
/// Ohter than those, request type file_upload and file_fetch are the types for remote
/// file operation ranges, for uploading the file to the remote filesystem or fetching the
/// file from the remote filesystem.
struct RequestType {
  enum type {
    READ,
    WRITE,
    FILE_FETCH,
    FILE_UPLOAD,
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
  bool is_encrypted() const { return is_encrypted_; }
  bool is_erasure_coded() const { return is_erasure_coded_; }
  RequestType::type request_type() const { return request_type_; }

 protected:
  RequestRange(RequestType::type request_type, int disk_id = -1, int64_t offset = -1)
    : fs_(nullptr),
      offset_(offset),
      len_(-1),
      disk_id_(disk_id),
      request_type_(request_type) {}

  /// Hadoop filesystem that contains file_, or set to nullptr for local filesystem.
  hdfsFS fs_;

  /// Path to file being read or written.
  std::string file_;

  /// Offset within file_ being read or written.
  int64_t offset_;

  /// Length of data read or written.
  int64_t len_;

  /// Id of disk queue containing byte range.
  int disk_id_;

  /// Whether file is encrypted.
  bool is_encrypted_;

  /// Whether file is erasure coded.
  bool is_erasure_coded_;

  /// The type of IO request, READ or WRITE.
  RequestType::type request_type_;
};

/// Param struct for different combinations of buffering.
struct BufferOpts {
 public:

  /// Different caching options available for a scan range.
  ///
  /// If USE_HDFS_CACHE is set, a read will first be probed against the HDFS cache for
  /// any hits. If there is a miss, it will fall back to reading from the underlying
  /// storage. Please note that HDFS cache are only used for local reads. Reads from
  /// remote locations (e.g. another HDFS data node) will not be cached in the HDFS cache.
  ///
  /// If USE_DATA_CACHE is set, any read from the underlying storage will first be probed
  /// against the data cache. If there is a cache miss in data cache, data will be
  /// inserted into the data cache upon IO completion. The data cache is usually used for
  /// caching non-local HDFS data (e.g. remote HDFS data or S3).
  enum {
    NO_CACHING  = 0,
    USE_HDFS_CACHE = 1 << 0,
    USE_DATA_CACHE = 1 << 2
  };

  /// Set options for a read into an IoMgr-allocated or HDFS-cached buffer.
  /// 'cache_options' specifies the caching options used. Please see comments
  /// of 'USE_HDFS_CACHE' and 'USE_DATA_CACHE' for details of the caching options.
  BufferOpts(int cache_options)
    : cache_options_(cache_options),
      client_buffer_(nullptr),
      client_buffer_len_(-1) {}

  /// Set options for an uncached read into an IoMgr-allocated buffer.
  static BufferOpts Uncached() {
    return BufferOpts(NO_CACHING, nullptr, -1);
  }

  /// Set options to read the entire scan range into 'client_buffer'. The length of the
  /// buffer, 'client_buffer_len', must fit the entire scan range. HDFS caching shouldn't
  /// be enabled in this case.
  static BufferOpts ReadInto(uint8_t* client_buffer, int64_t client_buffer_len,
      int cache_options) {
    DCHECK_EQ(cache_options & USE_HDFS_CACHE, 0);
    return BufferOpts(cache_options, client_buffer, client_buffer_len);
  }

  /// Use only when you don't want to to read the entire scan range, but only sub-ranges
  /// in it. In this case you can copy the relevant parts from the HDFS cache into the
  /// client buffer. The length of the buffer, 'client_buffer_len' must fit the
  /// concatenation of all the sub-ranges.
  static BufferOpts ReadInto(int cache_options, uint8_t* client_buffer,
      int64_t client_buffer_len) {
    return BufferOpts(cache_options, client_buffer, client_buffer_len);
  }

 private:
  friend class ScanRange;
  friend class HdfsFileReader;
  FRIEND_TEST(DataCacheTest, TestBasics);

  BufferOpts(int cache_options, uint8_t* client_buffer,
      int64_t client_buffer_len)
    : cache_options_(cache_options),
      client_buffer_(client_buffer),
      client_buffer_len_(client_buffer_len) {}

  /// Specify options to enable HDFS and data caches.
  const int cache_options_;

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

  /// Struct for passing file info for constructing ScanRanges. Only contains details
  /// consistent across all ranges for a given file. Filename is only used for the
  /// duration of calls accepting FileInfo.
  struct FileInfo {
    const char *filename;
    hdfsFS fs = nullptr;
    int64_t mtime = ScanRange::INVALID_MTIME;
    bool is_encrypted = false;
    bool is_erasure_coded = false;
  };

  /// Allocate a scan range object stored in the given 'obj_pool' and calls Reset() on it
  /// with the rest of the input variables.
  static ScanRange* AllocateScanRange(ObjectPool* obj_pool, const FileInfo &fi,
      int64_t len, int64_t offset, std::vector<SubRange>&& sub_ranges, void* metadata,
      int disk_id, bool expected_local, const BufferOpts& buffer_opts);

  /// Get file info for the current scan range.
  FileInfo GetFileInfo() const {
    return FileInfo{file_.c_str(), fs_, mtime_, is_encrypted_, is_erasure_coded_};
  }

  /// Resets this scan range object with the scan range description. The scan range
  /// is for bytes [offset, offset + len) in 'file' on 'fs' (which is nullptr for the
  /// local filesystem). The scan range must be non-empty and fall within the file bounds
  /// (len > 0 and offset >= 0 and offset + len <= file_length). 'disk_id' is the disk
  /// queue to add the range to. If 'expected_local' is true, a warning is generated if
  /// the read did not come from a local disk. 'mtime' is the last modification time for
  /// 'file'; the mtime must change when the file changes.
  /// 'buffer_opts' specifies buffer management options - see the DiskIoMgr class comment
  /// and the BufferOpts comments for details.
  /// 'meta_data' is an arbitrary client-provided pointer for any auxiliary data.
  /// 'disk_file' and 'disk_buffer_file' provides methods to confirm and guarantee the
  /// status of the physical file is available to access for the scanning. They are used
  /// for scanning a file related to spilling to a remote filesystem, both should be
  /// non-null, for other cases, like scanning a file related to spilling to local
  /// filesytem, both would not be used and should be null.
  /// TODO: IMPALA-4249: clarify if a ScanRange can be reused after Reset(). Currently
  /// it is not generally safe to do so, but some unit tests reuse ranges after
  /// successfully reading to eos.
  void Reset(const FileInfo &fi, int64_t len, int64_t offset, int disk_id,
      bool expected_local, const BufferOpts& buffer_opts, void* meta_data = nullptr,
      DiskFile* disk_file = nullptr, DiskFile* disk_buffer_file = nullptr);

  /// Same as above, but it also adds sub-ranges. No need to merge contiguous sub-ranges
  /// in advance, as this method will do the merge.
  void Reset(const FileInfo &fi, int64_t len, int64_t offset, int disk_id,
      bool expected_local, const BufferOpts& buffer_opts,
      std::vector<SubRange>&& sub_ranges, void* meta_data = nullptr,
      DiskFile* disk_file = nullptr, DiskFile* disk_buffer_file = nullptr);

  void* meta_data() const { return meta_data_; }
  int cache_options() const { return cache_options_; }
  bool UseHdfsCache() const { return (cache_options_ & BufferOpts::USE_HDFS_CACHE) != 0; }
  bool UseDataCache() const { return (cache_options_ & BufferOpts::USE_DATA_CACHE) != 0; }
  bool read_in_flight() const { return read_in_flight_; }
  bool expected_local() const { return expected_local_; }
  int64_t bytes_to_read() const { return bytes_to_read_; }
  bool use_local_buffer() const { return use_local_buffer_; }
  bool is_cancelled() const { return !cancel_status_.ok(); }
  bool is_blocked_on_buffer() const { return blocked_on_buffer_; }
  bool is_eosr_queued() const { return eosr_queued_; }

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

  /// Closes underlying reader, if no more data will be read from this scan range.
  void CloseReader(const std::unique_lock<std::mutex>& scan_range_lock);

  /// Determine if [offset,offset+length) is expected to be a local range.
  inline bool ExpectedLocalRead(int64_t offset, int64_t length) const {
    return expected_local_ && offset >= offset_ && (offset + length <= offset_ + len_);
  }

  /// return a descriptive string for debug.
  std::string DebugString() const;

  /// Non-HDFS files (e.g. local files) do not use mtime, so they should use this known
  /// bogus mtime.
  const static int64_t INVALID_MTIME = -1;

  int64_t mtime() const { return mtime_; }

  bool HasSubRanges() const { return !sub_ranges_.empty(); }

  // Checks if 'lock_' is held via 'scan_range_lock'
  bool is_locked(const std::unique_lock<std::mutex>& scan_range_lock) {
    return scan_range_lock.owns_lock() && scan_range_lock.mutex() == &lock_;
  }

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
  friend class RemoteOperRange;

  /// Initialize internal fields
  void InitInternal(DiskIoMgr* io_mgr, RequestContext* reader);

  /// If data is cached, returns ok() and * read_succeeded is set to true. Also enqueues
  /// a ready buffer from the cached data.
  /// If the data is not cached, returns ok() and *read_succeeded is set to false.
  /// Returns a non-ok status if it ran into a non-continuable error.
  /// The reader lock must be held by the caller.
  Status ReadFromCache(const std::unique_lock<std::mutex>& reader_lock,
      bool* read_succeeded) WARN_UNUSED_RESULT;

  /// Add buffers for the range to read data into and schedule the range if blocked.
  /// If 'returned' is true, the buffers returned from GetNext() that are being recycled
  /// via ReturnBuffer(). Otherwise the buffers are newly allocated buffers to be added.
  void AddUnusedBuffers(
      std::vector<std::unique_ptr<BufferDescriptor>>&& buffers, bool returned);

  /// Called from a disk I/O thread to read the next buffer of data for this range. The
  /// returned ReadOutcome describes what the result of the read was. 'disk_id' is the
  /// ID of the disk queue. 'queue' is updated with the sizes and latencies of reads from
  /// the underlying filesystem. Caller must not hold 'lock_'.
  ReadOutcome DoRead(DiskQueue* queue, int disk_id);

  /// The function runs the actual read logic to read content with the specific reader.
  /// If use_local_buffer is true, it will read from the local buffer file with the local
  /// buffer reader.
  /// If use_mem_buffer is true, it will read from a memory block in the local buffer.
  /// The local_file_lock is used to guarantee the local file is not deleted while
  /// reading, should not be null if use_mem_buffer is true.
  ReadOutcome DoReadInternal(DiskQueue* queue, int disk_id, bool use_local_buffer,
      bool use_mem_buffer,
      boost::shared_lock<boost::shared_mutex>* local_file_lock = nullptr);

  /// Whether to use file handle caching for the current file.
  bool FileHandleCacheEnabled() const;

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

  /// Get the read statistics from the Hdfs file handle and aggregate them to
  /// the RequestContext. This clears the statistics on this file handle.
  /// It is safe to pass hdfsFile by value, as hdfsFile's underlying type is a
  /// pointer.
  void GetHdfsStatistics(hdfsFile fh);

  /// Waits for any in-flight read to complete. Called after CancelInternal() to ensure
  /// no more reads will occur for the scan range.
  void WaitForInFlightRead();

  /// Returns true if no more buffers will be returned to clients in the future,
  /// either because of hitting eosr or cancellation.
  bool all_buffers_returned(const std::unique_lock<std::mutex>& lock) const {
    DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
    return !cancel_status_.ok() ||
        (eosr_queued_ && buffer_manager_->is_readybuffer_empty());
  }

  /// Adds sub-ranges to this ScanRange. If sub_ranges is not empty, then ScanRange won't
  /// read everything from its range, but will only read these sub-ranges.
  /// Sub-ranges need to be ordered by 'offset' and cannot overlap with each other.
  /// Doesn't need to merge continuous sub-ranges in advance, this method will do.
  void InitSubRanges(std::vector<SubRange>&& sub_ranges);

  /// Read the sub-ranges into buffer and track the current position in 'sub_range_pos_'.
  /// If cached data is available, then memcpy() from it instead of actually reading the
  /// files. 'queue' is updated with the latencies and sizes of reads from the underlying
  /// filesystem.
  Status ReadSubRanges(
      DiskQueue* queue, BufferDescriptor* buffer, bool* eof, FileReader* file_reader);

  /// Validates the internal state of this range. lock_ must be taken
  /// before calling this. Need to take a lock on 'lock_' via
  /// 'scan_range_lock' before invoking this.
  bool Validate(const std::unique_lock<std::mutex>& scan_range_lock);

  /// Validates the sub-ranges. All sub-range must be inside of this ScanRange.
  /// They need to be ordered by offset and cannot overlap.
  bool ValidateSubRanges();

  /// Merges adjacent and continuous sub-ranges.
  void MergeSubRanges();

  /// Adds scan range to the reader's queue and schedules the reader.
  /// 'lock_' should not be acquired before calling this function as it will
  /// acquire RequestContext's lock.
  void ScheduleScanRange();

  /// Allocates up to 'max_bytes' buffers and adds it to unused buffer.
  /// Called from DiskIOMgr::AllocateBuffersForRange
  ///
  /// The buffer sizes are chosen based on 'len_'. 'max_bytes' must be >=
  /// 'min_buffer_size' so that at least one buffer can be allocated. The caller
  /// must ensure that 'bp_client' has at least 'max_bytes' unused reservation.
  /// Returns ok if the buffers were successfully allocated and added to unused buffer.
  Status AllocateBuffersForRange(
      BufferPool::ClientHandle* bp_client, int64_t max_bytes,
      int64_t min_buffer_size, int64_t max_buffer_size);

  /// Pointer to caller specified metadata. This is untouched by the io manager
  /// and the caller can put whatever auxiliary data in here.
  void* meta_data_ = nullptr;

  /// Options for enabling HDFS caching and data cache.
  int cache_options_;

  /// If true, we expect this scan range to be a local read. Note that if this is false,
  /// it does not necessarily mean we expect the read to be remote, and that we never
  /// create scan ranges where some of the range is expected to be remote and some of it
  /// local.
  /// TODO: we can do more with this
  bool expected_local_ = false;

  /// Last modified time of the file associated with the scan range. Set in Reset().
  int64_t mtime_;

  DiskIoMgr* io_mgr_ = nullptr;

  /// Reader/owner of the scan range
  RequestContext* reader_ = nullptr;

  /// Valid if the 'buffer_manager_->buffer_tag_' is CLIENT_BUFFER.
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

  /// Buffer Mangement for this ScanRange will be done by the field below.
  std::unique_ptr<ScanBufferManager> buffer_manager_;

  /// Lock protecting fields below.
  /// This lock should not be taken during FileReader::Open()/Read()/Close().
  /// If RequestContext::lock_ and this lock need to be held simultaneously,
  /// RequestContext::lock_ must be taken first.
  std::mutex lock_;

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

  /// Condition variable for threads in GetNext() that are waiting for the next buffer
  /// and threads in WaitForInFlightRead() that are waiting for a read to finish.
  /// Signalled when a buffer is enqueued in ready buffers managed by buffer manager
  /// (i.e., 'buffer_manager_->ready_buffers_') or the scan range is cancelled.
  ConditionVariable buffer_ready_cv_;

  /// Number of bytes read by this scan range.
  int64_t bytes_read_ = 0;

  /// Polymorphic object that is responsible for doing file operations.
  std::unique_ptr<FileReader> file_reader_;

  /// Polymorphic object that is responsible for doing file operations if the path
  /// is a remote url and the file is in the local buffer.
  std::unique_ptr<FileReader> local_buffer_reader_;

  /// If set to true, the scan range is using local_buffer_reader_ to do scan operations.
  /// The flag is set during DoRead(). If the path is a remote path and the file has
  /// a local buffer, the flag is set to true, otherwise the flag is false.
  bool use_local_buffer_ = false;

  /// If not empty, the ScanRange will only read these parts from the file.
  std::vector<SubRange> sub_ranges_;

  /// The file handle of the physical file to be accessed.
  DiskFile* disk_file_ = nullptr;

  /// The file handle of the physical buffer file to be accessed.
  DiskFile* disk_buffer_file_ = nullptr;

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

  /// Set the offset of this WriteRange.
  /// Caller should guarantee the thread safe of calling the function.
  void SetOffset(int64_t file_offset);

  /// Set the DiskFile pointer which the WriteRange belongs to.
  /// Can only be called when the write is not in flight (i.e. before AddWriteRange()
  /// is called or after the write callback was called).
  void SetDiskFile(DiskFile* disk_file) { disk_file_ = disk_file; }

  /// Set the IO context to this WriteRange.
  /// Can only be called when the write is not in flight (i.e. before AddWriteRange()
  /// is called or after the write callback was called).
  void SetRequestContext(RequestContext* io_ctx) { io_ctx_ = io_ctx; }

  /// Execute writing the this range to the corresponding file.
  Status DoWrite();

  /// Handle the status of Function DoWrite().
  Status DoWriteEnd(DiskQueue* queue, const Status& ret_status);

  /// Return the data to be written.
  const uint8_t* data() const { return data_; }

  /// Return the disk file pointer of the write range.
  DiskFile* disk_file() const { return disk_file_; }

  RequestContext* io_ctx() const { return io_ctx_; }

  /// Return if the disk file that the write range belongs to is completed.
  bool is_full() const { return is_full_; }

  WriteDoneCallback callback() const { return callback_; }

 private:
  DISALLOW_COPY_AND_ASSIGN(WriteRange);

  friend class LocalFileWriter;

  /// Data to be written. RequestRange::len_ contains the length of data
  /// to be written.
  const uint8_t* data_;

  /// Callback to invoke after the write is complete.
  WriteDoneCallback callback_;

  /// IO context is to help to find out the disk queue and corresponding metrics.
  RequestContext* io_ctx_;

  /// The DiskFile is which the WriteRange writes into.
  /// A DiskFile can have multiple WriteRanges.
  DiskFile* disk_file_ = nullptr;

  /// Indicate if the file which the write range belongs to is full after writing.
  bool is_full_ = false;
};

class RemoteOperRange : public RequestRange {
 public:
  /// This callback is invoked on each RemoteOperRange after the operation is complete
  /// or the context is cancelled. The status returned by the callback parameter indicates
  /// if the operation was successful (i.e. Status::OK), if there was an error
  /// TErrorCode::RUNTIME_ERROR) or if the context was cancelled
  /// (TErrorCode::CANCELLED_INTERNALLY). The callback is only invoked if this
  /// RemoteOperRange was successfully added (i.e. AddRemoteOperRange() succeeded).
  /// No locks are held while the callback is invoked.
  typedef std::function<void(const Status&)> RemoteOperDoneCallback;
  RemoteOperRange(DiskFile* src_file, DiskFile* dst_file, int64_t block_size, int disk_id,
      RequestType::type type, DiskIoMgr* io_mgr, RemoteOperDoneCallback callback,
      int64_t file_offset = 0);

  int64_t block_size() { return block_size_; }

  RemoteOperDoneCallback callback() const { return callback_; }

  /// Called from a disk I/O thread to upload the file to a remote filesystem. The
  /// returned Status describes what the result of the read was. 'buff' is the
  /// block buffer which is used for file operations. 'buff_size' is the size of the
  /// block buffer. Caller must not hold 'lock_'.
  Status DoUpload(uint8_t* buff, int64_t buff_size);

  /// Execute the fetch file operation from a remote filesystem.
  /// Caller must not hold 'lock_'.
  Status DoFetch();

 private:
  DISALLOW_COPY_AND_ASSIGN(RemoteOperRange);

  friend class LocalFileWriter;

  /// Callback to invoke after the operation is complete.
  RemoteOperDoneCallback callback_;

  /// IO manager is to help to find out the disk queue and corresponding metrics.
  DiskIoMgr* io_mgr_;

  /// disk_file_src_ contains the information of the file which is the source to
  /// do the file operation. For example, if the operation is file upload, this
  /// is the handle for the source physical file to be uploaded.
  DiskFile* disk_file_src_;

  /// disk_file_dst_ contains the information of the file which is the destination
  /// of the file operation.
  DiskFile* disk_file_dst_;

  /// block size to do the file operation.
  int64_t block_size_;
};

inline bool BufferDescriptor::is_cached() const {
  return scan_range_->buffer_manager_->is_cached();
}

inline bool BufferDescriptor::is_client_buffer() const {
  return scan_range_->buffer_manager_->is_client_buffer();
}
}
}
