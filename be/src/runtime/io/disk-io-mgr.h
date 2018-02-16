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
#include "runtime/bufferpool/buffer-pool.h"
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

namespace io {
/// Manager object that schedules IO for all queries on all disks and remote filesystems
/// (such as S3). Each query maps to one or more RequestContext objects, each of which
/// has its own queue of scan ranges and/or write ranges.
///
/// The API splits up requesting scan/write ranges (non-blocking) and reading the data
/// (blocking). The DiskIoMgr has worker threads that will read from and write to
/// disk/hdfs/remote-filesystems, allowing interleaving of IO and CPU. This allows us to
/// keep all disks and all cores as busy as possible.
///
/// All public APIs are thread-safe. It is not valid to call any of the APIs after
/// UnregisterContext() returns.
///
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
///
/// For Writers:
/// Data is written via AddWriteRange(). This is non-blocking and adds a WriteRange to a
/// per-disk queue. After the write is complete, a callback in WriteRange is invoked.
/// No memory is allocated within IoMgr for writes and no copies are made. It is the
/// responsibility of the client to ensure that the data to be written is valid and that
/// the file to be written to exists until the callback is invoked.
///
/// There are several key methods for scanning data with the IoMgr.
///  1. StartScanRange(): adds range to the IoMgr to start immediately.
///  2. AddScanRanges(): adds ranges to the IoMgr that the reader wants to scan, but does
///     not start them until GetNextUnstartedRange() is called.
///  3. GetNextUnstartedRange(): returns to the caller the next scan range it should
///     process.
///  4. ScanRange::GetNext(): returns the next buffer for this range, blocking until
///     data is available.
///
/// The disk threads do not synchronize with each other. The readers and writers don't
/// synchronize with each other. There is a lock and condition variable for each request
/// context queue and each disk queue.
/// IMPORTANT: whenever both locks are needed, the lock order is to grab the context lock
/// before the disk lock.
///
/// Scheduling: If there are multiple request contexts with work for a single disk, the
/// request contexts are scheduled in round-robin order. Multiple disk threads can
/// operate on the same request context. Exactly one request range is processed by a
/// disk thread at a time. If there are multiple scan ranges scheduled for a single
/// context, these are processed in round-robin order.
/// If there are multiple scan and write ranges for a disk, a read is always followed
/// by a write, and a write is followed by a read, i.e. reads and writes alternate.
/// If multiple write ranges are enqueued for a single disk, they will be processed
/// by the disk threads in order, but may complete in any order. No guarantees are made
/// on ordering of writes across disks.
///
/// Resource Management: the IoMgr is designed to share the available disk I/O capacity
/// between many clients and to help use the available I/O capacity efficiently. The IoMgr
/// interfaces are designed to let clients manage their own CPU and memory usage while the
/// IoMgr manages the allocation of the I/O capacity of different I/O devices to scan
/// ranges of different clients.
///
/// IoMgr clients may want to work on multiple scan ranges at a time to maximize CPU and
/// I/O utilization. Clients can call GetNextUnstartedRange() to start as many concurrent
/// scan ranges as required, e.g. from each parallel scanner thread. Once a scan range has
/// been returned via GetNextUnstartedRange(), the caller must allocate any memory needed
/// for buffering reads, after which the IoMgr wil start to fill the buffers with data
/// while the caller concurrently consumes and processes the data. For example, the logic
/// in a scanner thread might look like:
///   while (more_ranges)
///     range = GetNextUnstartedRange()
///     while (!range.eosr)
///       buffer = range.GetNext()
///
/// Note that the IoMgr rather than the client is responsible for choosing which scan
/// range to process next, which allows optimizations like distributing load across disks.
///
/// Buffer Management:
/// Buffers for reads are either a) allocated on behalf of the caller with
/// AllocateBuffersForRange() ("IoMgr-allocated"), b) cached HDFS buffers if the scan
/// range was read from the HDFS cache, or c) a client buffer, large enough to fit the
/// whole scan range's data, that is provided by the caller when constructing the
/// scan range.
///
/// All three kinds of buffers are wrapped in BufferDescriptors before returning to the
/// caller. The caller must always call ReturnBuffer() on the buffer descriptor to allow
/// recycling of the buffer memory and to release any resources associated with the buffer
/// or scan range.
///
/// In case a), ReturnBuffer() may re-enqueue the buffer for GetNext() to return again if
/// needed. E.g. if 24MB of buffers were allocated to read a 64MB scan range, each buffer
/// must be returned multiple times. Callers must be careful to call ReturnBuffer() with
/// the previous buffer returned from the range before calling before GetNext() so that
/// at least one buffer is available for the I/O mgr to read data into. Calling GetNext()
/// when the scan range has no buffers to read data into causes a resource deadlock.
/// NB: if the scan range was allocated N buffers, then it's always ok for the caller
/// to hold onto N - 1 buffers, but currently the IoMgr doesn't give the caller a way
/// to determine the value of N.
///
/// If the caller wants to maximize I/O throughput, it can give the range enough memory
/// for 3 max-sized buffers per scan range. Having two queued buffers (plus the buffer
/// that is currently being processed by the client) gives good performance in most
/// scenarios:
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
/// See IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE.
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
/// range (GetNextUnstartedRange()) instead of when the ranges are issued. This guarantees
/// that there will be a CPU available to process the buffer and any throttling we do with
/// the number of scanner threads properly controls the amount of files we mlock.
/// With cached scan ranges, we cannot close the scan range until the cached buffer
/// is returned (HDFS does not allow this). We therefore need to defer the close until
/// the cached buffer is returned (ReturnBuffer()).
///
/// Remote filesystem support (e.g. S3):
/// Remote filesystems are modeled as "remote disks". That is, there is a seperate disk
/// queue for each supported remote filesystem type. In order to maximize throughput,
/// multiple connections are opened in parallel by having multiple threads running per
/// queue. Also note that reading from a remote filesystem service can be more CPU
/// intensive than local disk/hdfs because of non-direct I/O and SSL processing, and can
/// be CPU bottlenecked especially if not enough I/O threads for these queues are
/// started.
///
/// TODO: We should implement more sophisticated resource management. Currently readers
/// are the unit of scheduling and we attempt to distribute IOPS between them. Instead
/// it would be better to have policies based on queries, resource pools, etc.
/// TODO: IoMgr should be able to request additional scan ranges from the coordinator
/// to help deal with stragglers.
///
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
  ///  - min_buffer_size: minimum io buffer size (in bytes). Will be rounded down to the
  //     nearest power-of-two.
  ///  - max_buffer_size: maximum io buffer size (in bytes). Will be rounded up to the
  ///    nearest power-of-two. Also the max read size.
  DiskIoMgr(int num_disks, int threads_per_rotational_disk,
      int threads_per_solid_state_disk, int64_t min_buffer_size, int64_t max_buffer_size);

  /// Create DiskIoMgr with default configs.
  DiskIoMgr();

  /// Clean up all threads and resources. This is mostly useful for testing since
  /// for impalad, this object is never destroyed.
  ~DiskIoMgr();

  /// Initialize the IoMgr. Must be called once before any of the other APIs.
  Status Init() WARN_UNUSED_RESULT;


  /// Allocates tracking structure for a request context.
  /// Register a new request context and return it to the caller. The caller must call
  /// UnregisterContext() for each context.
  std::unique_ptr<RequestContext> RegisterContext();

  /// Unregisters context from the disk IoMgr by first cancelling it then blocking until
  /// all references to the context are removed from I/O manager internal data structures.
  /// This must be called for every RegisterContext() to ensure that the context object
  /// can be safely destroyed. It is invalid to add more request ranges to 'context' after
  /// after this call. This call blocks until all the disk threads have finished cleaning
  /// up.
  void UnregisterContext(RequestContext* context);

  /// Adds the scan ranges to reader's queues, but does not start scheduling it. The range
  /// can be scheduled by a thread calling GetNextUnstartedRange(). This call is
  /// non-blocking. The caller must not deallocate the scan range pointers before
  /// UnregisterContext().
  Status AddScanRanges(
      RequestContext* reader, const std::vector<ScanRange*>& ranges) WARN_UNUSED_RESULT;

  /// Adds the scan range to the queues, as with AddScanRanges(), but immediately
  /// start scheduling the scan range. This can be used to do synchronous reads as well
  /// as schedule dependent ranges, e.g. for columnar formats. This call is non-blocking.
  /// The caller must not deallocate the scan range pointers before UnregisterContext().
  ///
  /// If this returns true in '*needs_buffers', the caller must then call
  /// AllocateBuffersForRange() to add buffers for the data to be read into before the
  /// range can be scheduled. Otherwise, the range is scheduled and the IoMgr will
  /// asynchronously read the data for the range and the caller can call
  /// ScanRange::GetNext() to read the data.
  Status StartScanRange(
      RequestContext* reader, ScanRange* range, bool* needs_buffers) WARN_UNUSED_RESULT;

  /// Add a WriteRange for the writer. This is non-blocking and schedules the context
  /// on the IoMgr disk queue. Does not create any files.
  Status AddWriteRange(
      RequestContext* writer, WriteRange* write_range) WARN_UNUSED_RESULT;

  /// Tries to get an unstarted scan range that was added to 'reader' with
  /// AddScanRanges(). On success, returns OK and returns the range in '*range'.
  /// If 'reader' was cancelled, returns CANCELLED. If another error is encountered,
  /// an error status is returned. Otherwise, if error or cancellation wasn't encountered
  /// and there are no unstarted ranges for 'reader', returns OK and sets '*range' to
  /// nullptr.
  ///
  /// If '*needs_buffers' is returned as true, the caller must call
  /// AllocateBuffersForRange() to add buffers for the data to be read into before the
  /// range can be scheduled. Otherwise, the range is scheduled and the IoMgr will
  /// asynchronously read the data for the range and the caller can call
  /// ScanRange::GetNext() to read the data.
  Status GetNextUnstartedRange(RequestContext* reader, ScanRange** range,
      bool* needs_buffers) WARN_UNUSED_RESULT;

  /// Allocates up to 'max_bytes' buffers to read the data from 'range' into and schedules
  /// the range. Called after StartScanRange() or GetNextUnstartedRange() returns
  /// *needs_buffers=true.
  ///
  /// The buffer sizes are chosen based on range->len(). 'max_bytes' must be >=
  /// min_read_buffer_size() so that at least one buffer can be allocated. The caller
  /// must ensure that 'bp_client' has at least 'max_bytes' unused reservation. Returns ok
  /// if the buffers were successfully allocated and the range was scheduled.
  ///
  /// Setting 'max_bytes' to IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE * max_buffer_size()
  /// will typically maximize I/O throughput. See the "Buffer Management" section of
  /// the class comment for explanation.
  Status AllocateBuffersForRange(RequestContext* reader,
      BufferPool::ClientHandle* bp_client, ScanRange* range, int64_t max_bytes);

  /// Determine which disk queue this file should be assigned to.  Returns an index into
  /// disk_queues_.  The disk_id is the volume ID for the local disk that holds the
  /// files, or -1 if unknown.  Flag expected_local is true iff this impalad is
  /// co-located with the datanode for this file.
  int AssignQueue(const char* file, int disk_id, bool expected_local);

  int64_t min_buffer_size() const { return min_buffer_size_; }
  int64_t max_buffer_size() const { return max_buffer_size_; }

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

  /// "Disk" queue offsets for remote accesses.  Offset 0 corresponds to
  /// disk ID (i.e. disk_queue_ index) of num_local_disks().
  enum {
    REMOTE_DFS_DISK_OFFSET = 0,
    REMOTE_S3_DISK_OFFSET,
    REMOTE_ADLS_DISK_OFFSET,
    REMOTE_NUM_DISKS
  };

  /// The ideal number of max-sized buffers per scan range to maximise throughput.
  /// See "Buffer Management" in the class comment for explanation.
  static const int64_t IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE = 3;

 private:
  friend class BufferDescriptor;
  friend class RequestContext;
  // TODO: remove io:: prefix - it is required for the "using ScanRange" workaround above.
  friend class io::ScanRange;
  struct DiskQueue;

  friend class DiskIoMgrTest_Buffers_Test;
  friend class DiskIoMgrTest_BufferSizeSelection_Test;
  friend class DiskIoMgrTest_VerifyNumThreadsParameter_Test;

  /// Number of worker(read) threads per rotational disk. Also the max depth of queued
  /// work to the disk.
  const int num_io_threads_per_rotational_disk_;

  /// Number of worker(read) threads per solid state disk. Also the max depth of queued
  /// work to the disk.
  const int num_io_threads_per_solid_state_disk_;

  /// Maximum read size. This is also the maximum size of each allocated buffer.
  const int64_t max_buffer_size_;

  /// The minimum size of each read buffer. Must be >= BufferPool::min_buffer_len().
  const int64_t min_buffer_size_;

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

  /// Updates disk queue and reader state after a read is complete. If the read
  /// was successful, 'read_status' is ok and 'buffer' contains the result of the
  /// read. If the read failed with an error, 'read_status' contains the error and
  /// 'buffer' has the buffer that was meant to hold the result of the read.
  void HandleReadFinished(DiskQueue* disk_queue, RequestContext* reader,
      Status read_status, std::unique_ptr<BufferDescriptor> buffer);

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

  /// Reads the specified scan range and calls HandleReadFinished() when done. If no
  /// buffer is available to read the range's data into, the read cannot proceed, the
  /// range becomes blocked and this function returns without doing I/O.
  void ReadRange(DiskQueue* disk_queue, RequestContext* reader, ScanRange* range);

  /// Helper for AllocateBuffersForRange() to compute the buffer sizes for a scan range
  /// with length 'scan_range_len', given that 'max_bytes' of memory should be allocated.
  std::vector<int64_t> ChooseBufferSizes(int64_t scan_range_len, int64_t max_bytes);
};
}
}

#endif
