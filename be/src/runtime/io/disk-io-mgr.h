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

#ifndef IMPALA_RUNTIME_IO_DISK_IO_MGR_H
#define IMPALA_RUNTIME_IO_DISK_IO_MGR_H

#include <deque>
#include <functional>
#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/unordered_set.hpp>
#include <boost/thread/mutex.hpp>

#include "common/atomic.h"
#include "common/hdfs.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "runtime/io/handle-cache.h"
#include "runtime/io/request-ranges.h"
#include "runtime/thread-resource-mgr.h"
#include "util/aligned-new.h"
#include "util/bit-util.h"
#include "util/condition-variable.h"
#include "util/error-util.h"
#include "util/runtime-profile.h"
#include "util/thread.h"

namespace impala {

class MemTracker;

namespace io {
/// Manager object that schedules IO for all queries on all disks and remote filesystems
/// (such as S3). Each query maps to one or more RequestContext objects, each of which
/// has its own queue of scan ranges and/or write ranges.
//
/// The API splits up requesting scan/write ranges (non-blocking) and reading the data
/// (blocking). The DiskIoMgr has worker threads that will read from and write to
/// disk/hdfs/remote-filesystems, allowing interleaving of IO and CPU. This allows us to
/// keep all disks and all cores as busy as possible.
//
/// All public APIs are thread-safe. It is not valid to call any of the APIs after
/// UnregisterContext() returns.
//
/// For Readers:
/// We can model this problem as a multiple producer (threads for each disk), multiple
/// consumer (scan ranges) problem. There are multiple queues that need to be
/// synchronized. Conceptually, there are two queues:
///   1. The per disk queue: this contains a queue of readers that need reads.
///   2. The per scan range ready-buffer queue: this contains buffers that have been
///      read and are ready for the caller.
/// The disk queue contains a queue of readers and is scheduled in a round robin fashion.
/// Readers map to scan nodes. The reader then contains a queue of scan ranges. The caller
/// asks the IoMgr for the next range to process. The IoMgr then selects the best range
/// to read based on disk activity and begins reading and queuing buffers for that range.
/// TODO: We should map readers to queries. A reader is the unit of scheduling and queries
/// that have multiple scan nodes shouldn't have more 'turns'.
//
/// For Writers:
/// Data is written via AddWriteRange(). This is non-blocking and adds a WriteRange to a
/// per-disk queue. After the write is complete, a callback in WriteRange is invoked.
/// No memory is allocated within IoMgr for writes and no copies are made. It is the
/// responsibility of the client to ensure that the data to be written is valid and that
/// the file to be written to exists until the callback is invoked.
//
/// The IoMgr provides three key APIs.
///  1. AddScanRanges: this is non-blocking and tells the IoMgr all the ranges that
///     will eventually need to be read.
///  2. GetNextRange: returns to the caller the next scan range it should process.
///     This is based on disk load. This also begins reading the data in this scan
///     range. This is blocking.
///  3. ScanRange::GetNext: returns the next buffer for this range.  This is blocking.
//
/// The disk threads do not synchronize with each other. The readers and writers don't
/// synchronize with each other. There is a lock and condition variable for each request
/// context queue and each disk queue.
/// IMPORTANT: whenever both locks are needed, the lock order is to grab the context lock
/// before the disk lock.
//
/// Scheduling: If there are multiple request contexts with work for a single disk, the
/// request contexts are scheduled in round-robin order. Multiple disk threads can
/// operate on the same request context. Exactly one request range is processed by a
/// disk thread at a time. If there are multiple scan ranges scheduled via
/// GetNextRange() for a single context, these are processed in round-robin order.
/// If there are multiple scan and write ranges for a disk, a read is always followed
/// by a write, and a write is followed by a read, i.e. reads and writes alternate.
/// If multiple write ranges are enqueued for a single disk, they will be processed
/// by the disk threads in order, but may complete in any order. No guarantees are made
/// on ordering of writes across disks.
//
/// Resource Management: effective resource management in the IoMgr is key to good
/// performance. The IoMgr helps coordinate two resources: CPU and disk. For CPU,
/// spinning up too many threads causes thrashing.
/// Memory usage in the IoMgr comes from queued read buffers.  If we queue the minimum
/// (i.e. 1), then the disks are idle while we are processing the buffer. If we don't
/// limit the queue, then it possible we end up queueing the entire data set (i.e. CPU
/// is slower than disks) and run out of memory.
/// For both CPU and memory, we want to model the machine as having a fixed amount of
/// resources.  If a single query is running, it should saturate either CPU or Disk
/// as well as using as little memory as possible. With multiple queries, each query
/// should get less CPU. In that case each query will need fewer queued buffers and
/// therefore have less memory usage.
//
/// The IoMgr defers CPU management to the caller. The IoMgr provides a GetNextRange
/// API which will return the next scan range the caller should process. The caller
/// can call this from the desired number of reading threads. Once a scan range
/// has been returned via GetNextRange, the IoMgr will start to buffer reads for
/// that range and it is expected the caller will pull those buffers promptly. For
/// example, if the caller would like to have 1 scanner thread, the read loop
/// would look like:
///   while (more_ranges)
///     range = GetNextRange()
///     while (!range.eosr)
///       buffer = range.GetNext()
/// To have multiple reading threads, the caller would simply spin up the threads
/// and each would process the loops above.
//
/// To control the number of IO buffers, each scan range has a limit of two queued
/// buffers (SCAN_RANGE_READY_BUFFER_LIMIT). If the number of buffers is at capacity,
/// the IoMgr will no longer read for that scan range until the caller has processed
/// a buffer. Assuming the client returns each buffer before requesting the next one
/// from the scan range, then this will consume up to 3 * 8MB = 24MB of I/O buffers per
/// scan range.
//
/// Buffer Management:
/// Buffers for reads are either a) allocated by the IoMgr and transferred to the caller,
/// b) cached HDFS buffers if the scan range uses HDFS caching, or c) provided by the
/// caller when constructing the scan range.
///
/// As a caller reads from a scan range, these buffers are wrapped in BufferDescriptors
/// and returned to the caller. The caller must always call ReturnBuffer() on the buffer
/// descriptor to allow recycling of the associated buffer (if there is an
/// IoMgr-allocated or HDFS cached buffer).
///
/// Caching support:
/// Scan ranges contain metadata on whether or not it is cached on the DN. In that
/// case, we use the HDFS APIs to read the cached data without doing any copies. For these
/// ranges, the reads happen on the caller thread (as opposed to the disk threads).
/// It is possible for the cached read APIs to fail, in which case the ranges are then
/// queued on the disk threads and behave identically to the case where the range
/// is not cached.
/// Resources for these ranges are also not accounted against the reader because none
/// are consumed.
/// While a cached block is being processed, the block is mlocked. We want to minimize
/// the time the mlock is held.
///   - HDFS will time us out if we hold onto the mlock for too long
///   - Holding the lock prevents uncaching this file due to a caching policy change.
/// Therefore, we only issue the cached read when the caller is ready to process the
/// range (GetNextRange()) instead of when the ranges are issued. This guarantees that
/// there will be a CPU available to process the buffer and any throttling we do with
/// the number of scanner threads properly controls the amount of files we mlock.
/// With cached scan ranges, we cannot close the scan range until the cached buffer
/// is returned (HDFS does not allow this). We therefore need to defer the close until
/// the cached buffer is returned (ReturnBuffer()).
//
/// Remote filesystem support (e.g. S3):
/// Remote filesystems are modeled as "remote disks". That is, there is a seperate disk
/// queue for each supported remote filesystem type. In order to maximize throughput,
/// multiple connections are opened in parallel by having multiple threads running per
/// queue. Also note that reading from a remote filesystem service can be more CPU
/// intensive than local disk/hdfs because of non-direct I/O and SSL processing, and can
/// be CPU bottlenecked especially if not enough I/O threads for these queues are
/// started.
//
/// TODO: IoMgr should be able to request additional scan ranges from the coordinator
/// to help deal with stragglers.
/// TODO: look into using a lock free queue
/// TODO: simplify the common path (less locking, memory allocations).
//
/// Structure of the Implementation:
///  - All client APIs are defined in this file, request-ranges.h and request-context.h.
///    Clients can include only the files that they need.
///  - Some internal classes are defined in disk-io-mgr-internal.h
///  - ScanRange APIs are implemented in scan-range.cc
///    This contains the ready buffer queue logic
///  - RequestContext APIs are implemented in request-context.cc
///    This contains the logic for picking scan ranges for a reader.
///  - Disk Thread and general APIs are implemented in disk-io-mgr.cc.
///  - The handle cache is implemented in handle-cache{.inline,}.h

// This is cache line aligned because the FileHandleCache needs cache line alignment
// for its partitions.
class DiskIoMgr : public CacheLineAligned {
 public:
  /// Create a DiskIoMgr object. This constructor is only used for testing.
  ///  - num_disks: The number of disks the IoMgr should use. This is used for testing.
  ///    Specify 0, to have the disk IoMgr query the os for the number of disks.
  ///  - threads_per_rotational_disk: number of read threads to create per rotational
  ///    disk. This is also the max queue depth.
  ///  - threads_per_solid_state_disk: number of read threads to create per solid state
  ///    disk. This is also the max queue depth.
  ///  - min_buffer_size: minimum io buffer size (in bytes)
  ///  - max_buffer_size: maximum io buffer size (in bytes). Also the max read size.
  DiskIoMgr(int num_disks, int threads_per_rotational_disk,
      int threads_per_solid_state_disk, int min_buffer_size, int max_buffer_size);

  /// Create DiskIoMgr with default configs.
  DiskIoMgr();

  /// Clean up all threads and resources. This is mostly useful for testing since
  /// for impalad, this object is never destroyed.
  ~DiskIoMgr();

  /// Initialize the IoMgr. Must be called once before any of the other APIs.
  Status Init(MemTracker* process_mem_tracker) WARN_UNUSED_RESULT;

  /// Allocates tracking structure for a request context.
  /// Register a new request context and return it to the caller. The caller must call
  /// UnregisterContext() for each context.
  /// reader_mem_tracker: Is non-null only for readers. IO buffers
  ///    used for this reader will be tracked by this. If the limit is exceeded
  ///    the reader will be cancelled and MEM_LIMIT_EXCEEDED will be returned via
  ///    GetNext().
  std::unique_ptr<RequestContext> RegisterContext(MemTracker* reader_mem_tracker);

  /// Unregisters context from the disk IoMgr by first cancelling it then blocking until
  /// all references to the context are removed from I/O manager internal data structures.
  /// This must be called for every RegisterContext() to ensure that the context object
  /// can be safely destroyed. It is invalid to add more request ranges to 'context' after
  /// after this call. This call blocks until all the disk threads have finished cleaning
  /// up.
  void UnregisterContext(RequestContext* context);

  /// This function cancels the context asychronously. All outstanding requests
  /// are aborted and tracking structures cleaned up. This does not need to be
  /// called if the context finishes normally.
  /// This will also fail any outstanding GetNext()/Read requests.
  void CancelContext(RequestContext* context);

  /// Adds the scan ranges to the queues. This call is non-blocking. The caller must
  /// not deallocate the scan range pointers before UnregisterContext().
  /// If schedule_immediately, the ranges are immediately put on the read queue
  /// (i.e. the caller should not/cannot call GetNextRange for these ranges).
  /// This can be used to do synchronous reads as well as schedule dependent ranges,
  /// as in the case for columnar formats.
  Status AddScanRanges(RequestContext* reader,
      const std::vector<ScanRange*>& ranges,
      bool schedule_immediately = false) WARN_UNUSED_RESULT;
  Status AddScanRange(RequestContext* reader, ScanRange* range,
      bool schedule_immediately = false) WARN_UNUSED_RESULT;

  /// Add a WriteRange for the writer. This is non-blocking and schedules the context
  /// on the IoMgr disk queue. Does not create any files.
  Status AddWriteRange(
      RequestContext* writer, WriteRange* write_range) WARN_UNUSED_RESULT;

  /// Returns the next unstarted scan range for this reader. When the range is returned,
  /// the disk threads in the IoMgr will already have started reading from it. The
  /// caller is expected to call ScanRange::GetNext on the returned range.
  /// If there are no more unstarted ranges, nullptr is returned.
  /// This call is blocking.
  Status GetNextRange(RequestContext* reader, ScanRange** range) WARN_UNUSED_RESULT;

  /// Reads the range and returns the result in buffer.
  /// This behaves like the typical synchronous read() api, blocking until the data
  /// is read. This can be called while there are outstanding ScanRanges and is
  /// thread safe. Multiple threads can be calling Read() per reader at a time.
  /// range *cannot* have already been added via AddScanRanges.
  /// This can only be used if the scan range fits in a single IO buffer (i.e. is smaller
  /// than max_read_buffer_size()) or if reading into a client-provided buffer.
  Status Read(RequestContext* reader, ScanRange* range,
      std::unique_ptr<BufferDescriptor>* buffer) WARN_UNUSED_RESULT;

  /// Returns the buffer to the IoMgr. This must be called for every buffer
  /// returned by GetNext()/Read() that did not return an error. This is non-blocking.
  /// After calling this, the buffer descriptor is invalid and cannot be accessed.
  void ReturnBuffer(std::unique_ptr<BufferDescriptor> buffer);

  /// Determine which disk queue this file should be assigned to.  Returns an index into
  /// disk_queues_.  The disk_id is the volume ID for the local disk that holds the
  /// files, or -1 if unknown.  Flag expected_local is true iff this impalad is
  /// co-located with the datanode for this file.
  int AssignQueue(const char* file, int disk_id, bool expected_local);

  /// TODO: The functions below can be moved to RequestContext.
  /// Returns the current status of the context.
  Status context_status(RequestContext* context) const WARN_UNUSED_RESULT;

  void set_bytes_read_counter(RequestContext*, RuntimeProfile::Counter*);
  void set_read_timer(RequestContext*, RuntimeProfile::Counter*);
  void set_open_file_timer(RequestContext*, RuntimeProfile::Counter*);
  void set_active_read_thread_counter(RequestContext*, RuntimeProfile::Counter*);
  void set_disks_access_bitmap(RequestContext*, RuntimeProfile::Counter*);

  int64_t queue_size(RequestContext* reader) const;
  int64_t bytes_read_local(RequestContext* reader) const;
  int64_t bytes_read_short_circuit(RequestContext* reader) const;
  int64_t bytes_read_dn_cache(RequestContext* reader) const;
  int num_remote_ranges(RequestContext* reader) const;
  int64_t unexpected_remote_bytes(RequestContext* reader) const;
  int cached_file_handles_hit_count(RequestContext* reader) const;
  int cached_file_handles_miss_count(RequestContext* reader) const;

  /// Returns the read throughput across all readers.
  /// TODO: should this be a sliding window?  This should report metrics for the
  /// last minute, hour and since the beginning.
  int64_t GetReadThroughput();

  /// Returns the maximum read buffer size
  int max_read_buffer_size() const { return max_buffer_size_; }

  /// Returns the total number of disk queues (both local and remote).
  int num_total_disks() const { return disk_queues_.size(); }

  /// Returns the total number of remote "disk" queues.
  int num_remote_disks() const { return REMOTE_NUM_DISKS; }

  /// Returns the number of local disks attached to the system.
  int num_local_disks() const { return num_total_disks() - num_remote_disks(); }

  /// The disk ID (and therefore disk_queues_ index) used for DFS accesses.
  int RemoteDfsDiskId() const { return num_local_disks() + REMOTE_DFS_DISK_OFFSET; }

  /// The disk ID (and therefore disk_queues_ index) used for S3 accesses.
  int RemoteS3DiskId() const { return num_local_disks() + REMOTE_S3_DISK_OFFSET; }

  /// The disk ID (and therefore disk_queues_ index) used for ADLS accesses.
  int RemoteAdlsDiskId() const { return num_local_disks() + REMOTE_ADLS_DISK_OFFSET; }

  /// Dumps the disk IoMgr queues (for readers and disks)
  std::string DebugString();

  /// Validates the internal state is consistent. This is intended to only be used
  /// for debugging.
  bool Validate() const;

  /// Given a FS handle, name and last modified time of the file, construct a new
  /// ExclusiveHdfsFileHandle. This records the time spent opening the handle in
  /// 'reader' and counts this as a cache miss. In the case of an error, returns nullptr.
  ExclusiveHdfsFileHandle* GetExclusiveHdfsFileHandle(const hdfsFS& fs,
      std::string* fname, int64_t mtime, RequestContext* reader);

  /// Releases an exclusive file handle, destroying it
  void ReleaseExclusiveHdfsFileHandle(ExclusiveHdfsFileHandle* fid);

  /// Given a FS handle, name and last modified time of the file, gets a
  /// CachedHdfsFileHandle from the file handle cache. Records the time spent
  /// opening the handle in 'reader'. On success, records statistics
  /// about whether this was a cache hit or miss in the 'reader' as well as at the
  /// system level. In case of an error returns nullptr.
  CachedHdfsFileHandle* GetCachedHdfsFileHandle(const hdfsFS& fs,
      std::string* fname, int64_t mtime, RequestContext* reader);

  /// Releases a file handle back to the file handle cache when it is no longer in use.
  void ReleaseCachedHdfsFileHandle(std::string* fname, CachedHdfsFileHandle* fid);

  /// Reopens a file handle by destroying the file handle and getting a fresh
  /// file handle from the cache. Records the time spent reopening the handle
  /// in 'reader'. Returns an error if the file could not be reopened.
  Status ReopenCachedHdfsFileHandle(const hdfsFS& fs, std::string* fname, int64_t mtime,
      RequestContext* reader, CachedHdfsFileHandle** fid);

  /// Garbage collect unused I/O buffers up to 'bytes_to_free', or all the buffers if
  /// 'bytes_to_free' is -1.
  void GcIoBuffers(int64_t bytes_to_free = -1);

  /// The maximum number of ready buffers that can be queued in a scan range. Having two
  /// queued buffers (plus the buffer that is returned to the client) gives good
  /// performance in most scenarios:
  /// 1. If the consumer is consuming data faster than we can read from disk, then the
  ///    queue will be empty most of the time because the buffer will be immediately
  ///    pulled off the queue as soon as it is added. There will always be an I/O request
  ///    in the disk queue to maximize I/O throughput, which is the bottleneck in this
  ///    case.
  /// 2. If we can read from disk faster than the consumer is consuming data, the queue
  ///    will fill up and there will always be a buffer available for the consumer to
  ///    read, so the consumer will not block and we maximize consumer throughput, which
  ///    is the bottleneck in this case.
  /// 3. If the consumer is consuming data at approximately the same rate as we are
  ///    reading from disk, then the steady state is that the consumer is processing one
  ///    buffer and one buffer is in the disk queue. The additional buffer can absorb
  ///    bursts where the producer runs faster than the consumer or the consumer runs
  ///    faster than the producer without blocking either the producer or consumer.
  static const int SCAN_RANGE_READY_BUFFER_LIMIT = 2;

  /// "Disk" queue offsets for remote accesses.  Offset 0 corresponds to
  /// disk ID (i.e. disk_queue_ index) of num_local_disks().
  enum {
    REMOTE_DFS_DISK_OFFSET = 0,
    REMOTE_S3_DISK_OFFSET,
    REMOTE_ADLS_DISK_OFFSET,
    REMOTE_NUM_DISKS
  };

 private:
  friend class BufferDescriptor;
  friend class RequestContext;
  // TODO: remove io:: prefix - it is required for the "using ScanRange" workaround above.
  friend class io::ScanRange;
  struct DiskQueue;

  friend class DiskIoMgrTest_Buffers_Test;
  friend class DiskIoMgrTest_VerifyNumThreadsParameter_Test;

  /// Memory tracker for unused I/O buffers owned by DiskIoMgr.
  boost::scoped_ptr<MemTracker> free_buffer_mem_tracker_;

  /// Memory tracker for I/O buffers where the RequestContext has no MemTracker.
  /// TODO: once IMPALA-3200 is fixed, there should be no more cases where readers don't
  /// provide a MemTracker.
  boost::scoped_ptr<MemTracker> unowned_buffer_mem_tracker_;

  /// Number of worker(read) threads per rotational disk. Also the max depth of queued
  /// work to the disk.
  const int num_io_threads_per_rotational_disk_;

  /// Number of worker(read) threads per solid state disk. Also the max depth of queued
  /// work to the disk.
  const int num_io_threads_per_solid_state_disk_;

  /// Maximum read size. This is also the maximum size of each allocated buffer.
  const int max_buffer_size_;

  /// The minimum size of each read buffer.
  const int min_buffer_size_;

  /// Thread group containing all the worker threads.
  ThreadGroup disk_thread_group_;

  /// Options object for cached hdfs reads. Set on startup and never modified.
  struct hadoopRzOptions* cached_read_options_ = nullptr;

  /// True if the IoMgr should be torn down. Worker threads watch for this to
  /// know to terminate. This variable is read/written to by different threads.
  volatile bool shut_down_;

  /// Total bytes read by the IoMgr.
  RuntimeProfile::Counter total_bytes_read_counter_;

  /// Total time spent in hdfs reading
  RuntimeProfile::Counter read_timer_;

  /// Protects free_buffers_
  boost::mutex free_buffers_lock_;

  /// Free buffers that can be handed out to clients. There is one list for each buffer
  /// size, indexed by the Log2 of the buffer size in units of min_buffer_size_. The
  /// maximum buffer size is max_buffer_size_, so the maximum index is
  /// Log2(max_buffer_size_ / min_buffer_size_).
  //
  /// E.g. if min_buffer_size_ = 1024 bytes:
  ///  free_buffers_[0]  => list of free buffers with size 1024 B
  ///  free_buffers_[1]  => list of free buffers with size 2048 B
  ///  free_buffers_[10] => list of free buffers with size 1 MB
  ///  free_buffers_[13] => list of free buffers with size 8 MB
  ///  free_buffers_[n]  => list of free buffers with size 2^n * 1024 B
  std::vector<std::deque<uint8_t*>> free_buffers_;

  /// Total number of allocated buffers, used for debugging.
  AtomicInt32 num_allocated_buffers_;

  /// Total number of buffers in readers
  AtomicInt32 num_buffers_in_readers_;

  /// Per disk queues. This is static and created once at Init() time.  One queue is
  /// allocated for each local disk on the system and for each remote filesystem type.
  /// It is indexed by disk id.
  std::vector<DiskQueue*> disk_queues_;

  /// The next disk queue to write to if the actual 'disk_id_' is unknown (i.e. the file
  /// is not associated with a particular local disk or remote queue). Used to implement
  /// round-robin assignment for that case.
  static AtomicInt32 next_disk_id_;

  // Caching structure that maps file names to cached file handles. The cache has an upper
  // limit of entries defined by FLAGS_max_cached_file_handles. Evicted cached file
  // handles are closed.
  FileHandleCache file_handle_cache_;

  /// Returns the index into free_buffers_ for a given buffer size
  int free_buffers_idx(int64_t buffer_size);

  /// Returns a buffer to read into with size between 'buffer_size' and
  /// 'max_buffer_size_', If there is an appropriately-sized free buffer in the
  /// 'free_buffers_', that is returned, otherwise a new one is allocated.
  /// The returned *buffer_size must be between 0 and 'max_buffer_size_'.
  /// The buffer memory is tracked against reader's mem tracker, or
  /// 'unowned_buffer_mem_tracker_' if the reader does not have one.
  std::unique_ptr<BufferDescriptor> GetFreeBuffer(
      RequestContext* reader, ScanRange* range, int64_t buffer_size);

  /// Disassociates the desc->buffer_ memory from 'desc' (which cannot be nullptr), either
  /// freeing it or returning it to 'free_buffers_'. Memory tracking is updated to
  /// reflect the transfer of ownership from desc->mem_tracker_ to the disk I/O mgr.
  void FreeBufferMemory(BufferDescriptor* desc);

  /// Disk worker thread loop. This function retrieves the next range to process on
  /// the disk queue and invokes ReadRange() or Write() depending on the type of Range().
  /// There can be multiple threads per disk running this loop.
  void WorkLoop(DiskQueue* queue);

  /// This is called from the disk thread to get the next range to process. It will
  /// wait until a scan range and buffer are available, or a write range is available.
  /// This functions returns the range to process.
  /// Only returns false if the disk thread should be shut down.
  /// No locks should be taken before this function call and none are left taken after.
  bool GetNextRequestRange(DiskQueue* disk_queue, RequestRange** range,
      RequestContext** request_context);

  /// Updates disk queue and reader state after a read is complete. The read result
  /// is captured in the buffer descriptor.
  void HandleReadFinished(DiskQueue* disk_queue, RequestContext* reader,
      std::unique_ptr<BufferDescriptor> buffer);

  /// Invokes write_range->callback_  after the range has been written and
  /// updates per-disk state and handle state. The status of the write OK/RUNTIME_ERROR
  /// etc. is passed via write_status and to the callback.
  /// The write_status does not affect the writer->status_. That is, an write error does
  /// not cancel the writer context - that decision is left to the callback handler.
  /// TODO: On the read path, consider not canceling the reader context on error.
  void HandleWriteFinished(
      RequestContext* writer, WriteRange* write_range, const Status& write_status);

  /// Validates that range is correctly initialized
  Status ValidateScanRange(ScanRange* range) WARN_UNUSED_RESULT;

  /// Write the specified range to disk and calls HandleWriteFinished when done.
  /// Responsible for opening and closing the file that is written.
  void Write(RequestContext* writer_context, WriteRange* write_range);

  /// Helper method to write a range using the specified FILE handle. Returns Status:OK
  /// if the write succeeded, or a RUNTIME_ERROR with an appropriate message otherwise.
  /// Does not open or close the file that is written.
  Status WriteRangeHelper(FILE* file_handle, WriteRange* write_range) WARN_UNUSED_RESULT;

  /// Reads the specified scan range and calls HandleReadFinished when done.
  void ReadRange(DiskQueue* disk_queue, RequestContext* reader, ScanRange* range);

  /// Try to allocate the next buffer for the scan range, returning the new buffer
  /// if successful. If 'reader' is cancelled, cancels the range and returns nullptr.
  /// If there is memory pressure and buffers are already queued, adds the range
  /// to the blocked ranges and returns nullptr.
  std::unique_ptr<BufferDescriptor> TryAllocateNextBufferForRange(DiskQueue* disk_queue,
      RequestContext* reader, ScanRange* range, int64_t buffer_size);
};
}
}

#endif
