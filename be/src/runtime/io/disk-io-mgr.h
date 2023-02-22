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

// This macro is used by some plugin text scanners to detect the version of Impala
// they are built against.
#define IMPALA_RUNTIME_IO_DISK_IO_MGR_H

#include <mutex>
#include <vector>

#include "common/atomic.h"
#include "common/hdfs.h"
#include "common/status.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/io/handle-cache.h"
#include "runtime/io/hdfs-monitored-ops.h"
#include "runtime/io/local-file-system.h"
#include "runtime/io/request-ranges.h"
#include "util/aligned-new.h"
#include "util/runtime-profile.h"
#include "util/thread.h"

namespace impala {
namespace io {

class DataCache;
class DiskQueue;

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
/// Data is written via RequestContext::AddWriteRange(). This is non-blocking and adds
/// a WriteRange to a per-disk queue. After the write is complete, a callback in
/// WriteRange is invoked. No memory is allocated within IoMgr for writes and no copies
/// are made. It is the responsibility of the client to ensure that the data to be
/// written is valid. The file to be written is created if not already present.
///
/// For File Operations:
/// In addition to operations for readers and writers, there is a special operation type
/// which is the file operation, allows io operations (upload or fetch) on the entire file
/// between local filesystem and remote filesystem. Each file operation range is enqueued
/// in the specific file operation queue, and processed by a disk thread for doing the
/// file operation. After the operation is done, a callback function of the file operation
/// range is invoked. There is a memory allocation for a block to do the data
/// transmission, and the memory is released immediately after the operation is done.
///
/// There are several key methods for scanning data with the IoMgr.
///  1. RequestContext::StartScanRange(): adds range to the IoMgr to start immediately.
///  2. RequestContext::AddScanRanges(): adds ranges to the IoMgr that the reader wants to
///     scan, but does not start them until RequestContext::GetNextUnstartedRange() is
///     called.
///  3. RequestContext::GetNextUnstartedRange(): returns to the caller the next scan range
///     it should process.
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
/// on ordering of writes across disks. The strategy of scheduling is the same for file
/// operation ranges, but the file operation ranges are in a sperate queue compared to
/// read(scan) or write ranges.
///
/// Resource Management: the IoMgr is designed to share the available disk I/O capacity
/// between many clients and to help use the available I/O capacity efficiently. The IoMgr
/// interfaces are designed to let clients manage their own CPU and memory usage while the
/// IoMgr manages the allocation of the I/O capacity of different I/O devices to scan
/// ranges of different clients.
///
/// IoMgr clients may want to work on multiple scan ranges at a time to maximize CPU and
/// I/O utilization. Clients can call RequestContext::GetNextUnstartedRange() to start as
/// many concurrent scan ranges as required, e.g. from each parallel scanner thread. Once
/// a scan range has been returned via GetNextUnstartedRange(), the caller must allocate
/// any memory needed for buffering reads, after which the IoMgr wil start to fill the
/// buffers with data while the caller concurrently consumes and processes the data. For
/// example, the logic in a scanner thread might look like:
///   while (more_ranges)
///     range = context->GetNextUnstartedRange()
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
/// the previous buffer returned from the range before calling GetNext() so that
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
/// Remote filesystem data caching:
/// To reduce latency and avoid being network bound when reading from remote filesystems,
/// a data cache can be optionally enabled (via --data_cache_config) for caching data read
/// for remote scan ranges on local storage. The cache is independent of file formats.
/// It's merely caching chunks of file blocks directly on local storage to avoid
/// fetching them over network. Please see data-cache.h for details.
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
  virtual ~DiskIoMgr();

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

  /// Allocates up to 'max_bytes' buffers to read the data from 'range' into and schedules
  /// the range. Called after StartScanRange() or reader->GetNextUnstartedRange()
  /// returns *needs_buffers=true.
  ///
  /// The buffer sizes are chosen based on range->len(). 'max_bytes' must be >=
  /// min_read_buffer_size() so that at least one buffer can be allocated. The caller
  /// must ensure that 'bp_client' has at least 'max_bytes' unused reservation. Returns ok
  /// if the buffers were successfully allocated and the range was scheduled.
  ///
  /// Setting 'max_bytes' to IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE * max_buffer_size()
  /// will typically maximize I/O throughput. See the "Buffer Management" section of
  /// the class comment for explanation.
  Status AllocateBuffersForRange(
      BufferPool::ClientHandle* bp_client, ScanRange* range, int64_t max_bytes);

  /// Determine which disk queue this file should be assigned to.  Returns an index into
  /// disk_queues_.  The disk_id is the volume ID for the local disk that holds the
  /// files, or -1 if unknown.  Flag expected_local is true iff this impalad is
  /// co-located with the datanode for this file. Flag check_default_fs is false iff
  /// the file is a temporary file.
  int AssignQueue(
      const char* file, int disk_id, bool expected_local, bool check_default_fs);

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

  /// The disk ID used for DFS File Operations (upload or fetch) accesses.
  int RemoteDfsDiskFileOperId() const {
    return num_local_disks() + REMOTE_DFS_DISK_FILE_OPER_OFFSET;
  }

  /// The disk ID (and therefore disk_queues_ index) used for S3 accesses.
  int RemoteS3DiskId() const { return num_local_disks() + REMOTE_S3_DISK_OFFSET; }

  /// The disk ID used for S3 File Operations (upload or fetch) accesses.
  int RemoteS3DiskFileOperId() const {
    return num_local_disks() + REMOTE_S3_DISK_FILE_OPER_OFFSET;
  }

  /// The disk ID (and therefore disk_queues_ index) used for ABFS accesses.
  int RemoteAbfsDiskId() const { return num_local_disks() + REMOTE_ABFS_DISK_OFFSET; }

  /// The disk ID (and therefore disk_queues_ index) used for ADLS accesses.
  int RemoteAdlsDiskId() const { return num_local_disks() + REMOTE_ADLS_DISK_OFFSET; }

  /// The disk ID (and therefore disk_queues_ index) used for OSS/JindoFS accesses.
  int RemoteOSSDiskId() const { return num_local_disks() + REMOTE_OSS_DISK_OFFSET; }

  /// The disk ID (and therefore disk_queues_ index) used for GCS accesses.
  int RemoteGcsDiskId() const { return num_local_disks() + REMOTE_GCS_DISK_OFFSET; }

  /// The disk ID (and therefore disk_queues_ index) used for COS accesses.
  int RemoteCosDiskId() const { return num_local_disks() + REMOTE_COS_DISK_OFFSET; }

  /// The disk ID (and therefore disk_queues_ index) used for Ozone accesses.
  int RemoteOzoneDiskId() const { return num_local_disks() + REMOTE_OZONE_DISK_OFFSET; }

  /// The disk ID (and therefore disk_queues_ index) used for SFS accesses.
  int RemoteSFSDiskId() const { return num_local_disks() + REMOTE_SFS_DISK_OFFSET; }

  /// The disk ID (and therefore disk_queues_ index) used for OBS accesses.
  int RemoteOBSDiskId() const { return num_local_disks() + REMOTE_OBS_DISK_OFFSET; }

  /// Dumps the disk IoMgr queues (for readers and disks)
  std::string DebugString();

  /// Validates the internal state is consistent. This is intended to only be used
  /// for debugging.
  bool Validate() const;

  /// Given a FS handle, name and last modified time of the file, construct a new
  /// ExclusiveHdfsFileHandle and return it via 'fid'. This records the time spent
  /// opening the handle in 'reader' and counts this as a cache miss. In case of an
  /// error, returns status and 'fid' is untouched.
  Status GetExclusiveHdfsFileHandle(const hdfsFS& fs,
      std::string* fname, int64_t mtime, RequestContext* reader,
      std::unique_ptr<ExclusiveHdfsFileHandle>& fid) WARN_UNUSED_RESULT;

  /// Releases an exclusive file handle, destroying it
  void ReleaseExclusiveHdfsFileHandle(std::unique_ptr<ExclusiveHdfsFileHandle> fid);

  /// Given a FS handle, name and last modified time of the file, gets a
  /// CachedHdfsFileHandle accessor from the file handle cache and returns it via
  /// 'accessor'. Records the time spent opening the handle in 'reader'. On success,
  /// records statistics about whether this was a cache hit or miss in the 'reader' as
  /// well as at the system level. In case of an error, returns status and 'accessor' is
  /// untouched.
  Status GetCachedHdfsFileHandle(const hdfsFS& fs, std::string* fname, int64_t mtime,
      RequestContext* reader, FileHandleCache::Accessor* accessor) WARN_UNUSED_RESULT;

  /// Reopens a file handle by destroying the file handle and getting a fresh
  /// file handle accessor from the cache. Records the time spent reopening the handle
  /// in 'reader'. Returns an error if the file could not be reopened.
  Status ReopenCachedHdfsFileHandle(const hdfsFS& fs, std::string* fname, int64_t mtime,
      RequestContext* reader, FileHandleCache::Accessor* accessor) WARN_UNUSED_RESULT;

  // Function to change the underlying LocalFileSystem object used for disk I/O.
  // DiskIoMgr will also take responsibility of the received LocalFileSystem pointer.
  // It is only for testing purposes to use a fault injected version of LocalFileSystem.
  void SetLocalFileSystem(std::unique_ptr<LocalFileSystem> fs) {
    local_file_system_ = std::move(fs);
  }

  /// "Disk" queue offsets for remote accesses.  Offset 0 corresponds to
  /// disk ID (i.e. disk_queue_ index) of num_local_disks().
  /// DISK_FILE_OPER queues are for the file operations, such as file upload
  /// or fetch, to be isolated from the range read/write operations in ordinary
  /// queues mainly for efficiency consideration.
  enum {
    REMOTE_DFS_DISK_OFFSET = 0,
    REMOTE_S3_DISK_OFFSET,
    REMOTE_ADLS_DISK_OFFSET,
    REMOTE_ABFS_DISK_OFFSET,
    REMOTE_GCS_DISK_OFFSET,
    REMOTE_COS_DISK_OFFSET,
    REMOTE_OZONE_DISK_OFFSET,
    REMOTE_DFS_DISK_FILE_OPER_OFFSET,
    REMOTE_S3_DISK_FILE_OPER_OFFSET,
    REMOTE_SFS_DISK_OFFSET,
    REMOTE_OSS_DISK_OFFSET,
    REMOTE_OBS_DISK_OFFSET,
    REMOTE_NUM_DISKS
  };

  /// Compute the ideal reservation for processing a scan range of 'scan_range_len' bytes.
  /// See "Buffer Management" in the class comment for explanation.
  int64_t ComputeIdealBufferReservation(int64_t scan_range_len);

  /// The ideal number of max-sized buffers per scan range to maximise throughput.
  /// See "Buffer Management" in the class comment for explanation.
  static const int64_t IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE = 3;

  /// Validates that range is correctly initialized. Return an error status if there
  /// is something invalid about the scan range.
  Status ValidateScanRange(ScanRange* range) WARN_UNUSED_RESULT;

  /// Try to dump the data of remote data cache to disk, so it could be loaded when
  /// impalad restart.
  Status DumpDataCache();

  DataCache* remote_data_cache() { return remote_data_cache_.get(); }

 private:
  DISALLOW_COPY_AND_ASSIGN(DiskIoMgr);
  friend class DiskIoMgrTest_Buffers_Test;
  friend class DiskIoMgrTest_BufferSizeSelection_Test;
  friend class DiskIoMgrTest_VerifyNumThreadsParameter_Test;
  friend class DiskIoMgrTest_MetricsOfWriteSizeAndLatency_Test;
  friend class DiskIoMgrTest_MetricsOfWriteIoError_Test;

  /////////////////////////////////////////
  /// BEGIN: private members that are accessed by other io:: classes
  friend class DiskQueue;
  friend class ScanRange;
  friend class WriteRange;
  friend class RemoteOperRange;
  friend class HdfsFileReader;
  friend class LocalFileWriter;

  /// Write the specified range to disk and calls writer_context->WriteDone() when done.
  /// Responsible for opening and closing the file that is written.
  void Write(RequestContext* writer_context, WriteRange* write_range);

  struct hadoopRzOptions* cached_read_options() { return cached_read_options_; }

  /// END: private members that are accessed by other io:: classes
  /////////////////////////////////////////

  // Handles the low level I/O functionality.
  std::unique_ptr<LocalFileSystem> local_file_system_;

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

  /// Per disk queues. This is static and created once at Init() time.  One queue is
  /// allocated for each local disk on the system and for each remote filesystem type.
  /// It is indexed by disk id.
  std::vector<DiskQueue*> disk_queues_;

  /// The next disk queue to write to if the actual 'disk_id_' is unknown (i.e. the file
  /// is not associated with a particular local disk or remote queue). Used to implement
  /// round-robin assignment for that case.
  static AtomicInt32 next_disk_id_;

  /// Thread pool used to implement timeouts for HDFS operations.
  HdfsMonitor hdfs_monitor_;

  // Caching structure that maps file names to cached file handles. The cache has an upper
  // limit of entries defined by FLAGS_max_cached_file_handles. Evicted cached file
  // handles are closed.
  FileHandleCache file_handle_cache_;

  /// Helper method to write a range using the specified FILE handle. Returns Status:OK
  /// if the write succeeded, or a RUNTIME_ERROR with an appropriate message otherwise.
  /// Does not open or close the file that is written.
  Status WriteRangeHelper(FILE* file_handle, WriteRange* write_range) WARN_UNUSED_RESULT;

  /// Helper for AllocateBuffersForRange() to compute the buffer sizes for a scan range
  /// with length 'scan_range_len', given that 'max_bytes' of memory should be allocated.
  std::vector<int64_t> ChooseBufferSizes(int64_t scan_range_len, int64_t max_bytes);

  /// Singleton IO data cache for remote reads. If configured, it will be probed for all
  /// non-local reads and data read from remote data nodes will be stored in it. If not
  /// configured, this would be NULL.
  std::unique_ptr<DataCache> remote_data_cache_;
};
}
}
