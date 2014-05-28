// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_RUNTIME_DISK_IO_MGR_H
#define IMPALA_RUNTIME_DISK_IO_MGR_H

#include <list>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/thread.hpp>

#include "common/atomic.h"
#include "common/hdfs.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "runtime/thread-resource-mgr.h"
#include "util/bit-util.h"
#include "util/internal-queue.h"
#include "util/runtime-profile.h"
#include "util/thread.h"

namespace impala {

class MemTracker;

// Manager object that schedules IO for all queries on all disks. Each query maps
// to one or more readers, each of which has its own queue of scan ranges. The
// API splits up requesting scan ranges (non-blocking) and reading the data (blocking).
// The DiskIoMgr has worker threads that will read from disk/hdfs, allowing interleaving
// of IO and CPU. This allows us to keep all disks and all cores as busy as possible.
//
// All public APIs are thread-safe. It is not valid to call any of the APIs after
// UnregisterReader() returns.
//
// We can model this problem as a multiple producer (threads for each disk), multiple
// consumer (scan ranges) problem. There are multiple queues that need to be
// synchronized. Conceptually, there are two queues:
//   1. The per disk queue: this contains a queue of readers that need reads.
//   2. The per scan range ready-buffer queue: this contains buffers that have been
//      read and are ready for the caller.
// The disk queue contains a queue of readers and is scheduled in a round robin fashion.
// Readers map to scan nodes. The reader then contains a queue of scan ranges. The caller
// asks the IoMgr for the next range to process. The IoMgr then selects the best range
// to read based on disk activity and begins reading and queuing buffers for that range.
// TODO: We should map readers to queries. A reader is the unit of scheduling and queries
// that have multiple scan nodes shouldn't have more 'turns'.
//
// The IoMgr provides three key APIs.
//  1. AddScanRanges: this is non-blocking and tells the IoMgr all the ranges that
//     will eventually need to be read.
//  2. GetNextRange: returns to the caller the next scan range it should process.
//     This is based on disk load. This also begins reading the data in this scan
//     range. This is blocking.
//  3. ScanRange::GetNext: returns the next buffer for this range.  This is blocking.
//
// The disk threads do not synchronize with each other. The readers don't synchronize
// with each other. There is a lock and condition variable for each reader queue and
// each disk queue.
// IMPORTANT: whenever both locks are needed, the lock order is to grab the reader lock
// before the disk lock.
//
// Resource Management: effective resource management in the IoMgr is key to good
// performance. The IoMgr helps coordinate two resources: CPU and disk. For CPU,
// spinning up too many threads causes thrashing.
// Memory usage in the IoMgr comes from queued read buffers.  If we queue the minimum
// (i.e. 1), then the disks are idle while we are processing the buffer. If we don't
// limit the queue, then it possible we end up queueing the entire data set (i.e. CPU
// is slower than disks) and run out of memory.
// For both CPU and memory, we want to model the machine as having a fixed amount of
// resources.  If a single query is running, it should saturate either CPU or Disk
// as well as using as little memory as possible. With multiple queries, each query should
// get less CPU, therefore need fewer queued buffers and therefore less memory usage.
//
// The IoMgr defers CPU management to the caller. The IoMgr provides a GetNextRange
// API which will return the next scan range the caller should process. The caller
// can call this from the desired number of reading threads. Once a scan range
// has been returned via GetNextRange, the IoMgr will start to buffer reads for
// that range and it is expected the caller will pull those buffers promptly. For
// example, if the caller would like to have 1 scanner thread, the read loop
// would look like:
//   while (more_ranges)
//     range = GetNextRange()
//     while (!range.eosr)
//       buffer = range.GetNext()
// To have multiple reading threads, the caller would simply spin up the threads
// and each would process the loops above.
//
// To control the number of IO buffers, each scan range has a soft max capacity for
// the number of queued buffers. If the number of buffers is at capacity, the IoMgr
// will no longer read for that scan range until the caller has processed a buffer.
// This capacity does not need to be fixed, and the caller can dynamically adjust
// it if necessary.
//
// As an example: If we allowed 5 buffers per range on a 24 core, 72 thread
// (we default to allowing 3x threads) machine, we should see at most
// 72 * 5 * 8MB = 2.8GB in io buffers memory usage. This should remain roughly constant
// regardless of how many concurrent readers are running.
//
// Buffer Management:
// Buffers are allocated by the IoMgr as necessary to service reads. These buffers
// are directly returned to the caller. The caller must call Return() on the buffer
// when it is done, at which point the buffer will be recycled for another read. In error
// cases, the IoMgr will recycle the buffers more promptly but regardless, the caller
// must always call Return()
//
// Caching support:
// Scan ranges contain metadata on whether or not it is cached on the DN. In that
// case, we use the HDFS APIs to read the cached data without doing any copies. For these
// ranges, the reads happen on the caller thread (as opposed to the disk threads).
// It is possible for the cached read APIs to fail, in which case the ranges are then
// queued on the disk threads and behave identically to the case where the range
// is not cached.
// Resources for these ranges are also not accounted against the reader because none
// are consumed.
// While a cached block is being processed, the block is mlocked. We want to minimize
// the time the mlock is held.
//   - HDFS will time us out if we hold onto the mlock for too long
//   - Holding the lock prevents uncaching this file due to a caching policy change.
// Therefore, we only issue the cached read when the caller is ready to process the
// range (GetNextRange()) instead of when the ranges are issued. This guarantees that
// there will be a CPU available to process the buffer and any throttling we do with
// the number of scanner threads properly controls the amount of files we mlock.
// With cached scan ranges, we cannot close the scan range until the cached buffer
// is returned (HDFS does not allow this). We therefore need to defer the close until
// the cached buffer is returned (BufferDescriptor::Return()).
//
// TODO: IoMgr should be able to request additional scan ranges from the coordinator
// to help deal with stragglers.
// TODO: look into using a lock free queue
// TODO: simplify the common path (less locking, memory allocations).
//
// Structure of the Implementation:
//  - All client APIs are defined in this file
//  - Internal classes are defined in disk-io-mgr-internal.h
//  - ScanRange APIs are implemented in disk-io-mgr-scan-range.cc
//    This contains the ready buffer queue logic
//  - ReaderContext APIs are implemented in disk-io-mgr-reader-context.cc
//    This contains the logic for picking scan ranges for a reader.
//  - Disk Thread and general APIs are implemented in disk-io-mgr.cc.
class DiskIoMgr {
 public:
  class ReaderContext;
  class ScanRange;

  // Buffer struct that is used by the caller and IoMgr to pass read buffers.
  // It is is expected that only one thread has ownership of this object at a
  // time.
  class BufferDescriptor {
   public:
    ScanRange* scan_range() { return scan_range_; }
    char* buffer() { return buffer_; }
    int64_t buffer_len() { return buffer_len_; }
    int64_t len() { return len_; }
    bool eosr() { return eosr_; }

    // Returns the offset within the scan range that this buffer starts at
    int64_t scan_range_offset() const { return scan_range_offset_; }

    // Updates this buffer buffer to be owned by the new tracker. Consumption is
    // release from the current tracker and added to the new one.
    void SetMemTracker(MemTracker* tracker);

    // Returns the buffer to the IoMgr. This must be called for every buffer
    // returned by GetNext()/Read() that did not return an error. This is non-blocking.
    void Return();

   private:
    friend class DiskIoMgr;
    BufferDescriptor(DiskIoMgr* io_mgr);

    // Resets the buffer descriptor state for a new reader, range and data buffer.
    void Reset(ReaderContext* reader, ScanRange* range, char* buffer, int64_t buffer_len);

    DiskIoMgr* io_mgr_;

    // Reader that this buffer is for
    ReaderContext* reader_;

    // The current tracker this buffer is associated with.
    MemTracker* mem_tracker_;

    // Scan range that this buffer is for.
    ScanRange* scan_range_;

    // buffer with the read contents
    char* buffer_;

    // length of buffer_. For buffers from cached reads, the length is 0.
    int64_t buffer_len_;

    // length of read contents
    int64_t len_;

    // true if the current scan range is complete
    bool eosr_;

    // Status of the read to this buffer. if status is not ok, 'buffer' is NULL
    Status status_;

    int64_t scan_range_offset_;
  };

  // ScanRange description. The caller must call Reset() to initialize the fields
  // before calling AddScanRanges(). The private fields are used internally by
  // the IoMgr.
  class ScanRange : public InternalQueue<ScanRange>::Node {
   public:
    // The initial queue capacity for this.  Specify -1 to use IoMgr default.
    ScanRange(int initial_capacity = -1);

    virtual ~ScanRange();

    // Resets this scan range object with the scan range description.
    void Reset(const char* file, int64_t len,
        int64_t offset, int disk_id, bool try_cache, void* metadata = NULL);

    const char* file() const { return file_; }
    int64_t len() const { return len_; }
    int64_t offset() const { return offset_; }
    void* meta_data() const { return meta_data_; }
    int disk_id() const { return disk_id_; }
    bool try_cache() const { return try_cache_; }
    int ready_buffers_capacity() const { return ready_buffers_capacity_; }

    void set_len(int64_t len) { len_ = len; }
    void set_offset(int64_t offset) { offset_ = offset; }

    // Returns the next buffer for this scan range. buffer is an output parameter.
    // This function blocks until a buffer is ready or an error occurred. If this is
    // called when all buffers have been returned, *buffer is set to NULL and Status::OK
    // is returned.
    // Only one thread can be in GetNext() at any time.
    Status GetNext(BufferDescriptor** buffer);

    // Cancel this scan range. This cleans up all queued buffers and
    // wakes up any threads blocked on GetNext().
    // Status is the reason the range was cancelled. Must not be ok().
    // Status is returned to the user in GetNext().
    void Cancel(const Status& status);

    std::string DebugString() const;

   private:
    friend class DiskIoMgr;

    // Initialize internal fields
    void InitInternal(DiskIoMgr* io_mgr, ReaderContext* reader);

    // Enqueues a buffer for this range. This does not block.
    // Returns true if this scan range has hit the queue capacity, false otherwise.
    bool EnqueueBuffer(BufferDescriptor* buffer);

    // Cleanup any queued buffers (i.e. due to cancellation). This cannot
    // be called with any locks taken.
    void CleanupQueuedBuffers();

    // Validates the internal state of this range. lock_ must be taken
    // before calling this.
    bool Validate();

    // Opens the file for this range. This function only modifies state in this range.
    Status Open();

    // Closes the file for this range. This function only modifies state in this range.
    void Close();

    // Reads from this range into 'buffer'. Buffer is preallocated. Returns the number
    // of bytes read. Updates range to keep track of where in the file we are.
    Status Read(char* buffer, int64_t* bytes_read, bool* eosr);

    // Reads from the DN cache. On success, sets cached_buffer_ to the DN buffer
    // and *read_succeeded to true.
    // If the data is not cached, returns ok() and *read_succeeded is set to false.
    // Returns a non-ok status if it ran into a non-continuable error.
    Status ReadFromCache(bool* read_succeeded);

    // Path to file
    const char* file_;

    // Pointer to caller specified metadata. This is untouched by the io manager
    // and the caller can put whatever auxiliary data in here.
    void* meta_data_;

    // byte offset in file for this range
    int64_t offset_;

    // byte len of this range
    int64_t len_;

    // id of the disk the data is on. This is 0-indexed
    int disk_id_;

    // If true, this scan range is expected to be cached. Note that this might be wrong
    // since the block could have been uncached. In that case, the cached path
    // will fail and we'll just put the scan range on the normal read path.
    bool try_cache_;

    DiskIoMgr* io_mgr_;

    // Reader/owner of the scan range
    ReaderContext* reader_;

    // File handle either to hdfs or local fs (FILE*)
    union {
      FILE* local_file_;
      hdfsFile hdfs_file_;
    };

    // If non-null, this is DN cached buffer. This means the cached read succeeded
    // and all the bytes for the range are in this buffer.
    struct hadoopRzBuffer* cached_buffer_;

    // Lock protecting fields below.
    // This lock should not be taken during Open/Read/Close.
    boost::mutex lock_;

    // Number of bytes read so far for this scan range
    int bytes_read_;

    // Status for this range. This is non-ok if is_cancelled_ is true.
    // Note: an individual range can fail without the ReaderContext being
    // cancelled. This allows us to skip individual ranges.
    Status status_;

    // If true, the last buffer for this scan range has been queued.
    bool eosr_queued_;

    // If true, the last buffer for this scan range has been returned.
    bool eosr_returned_;

    // If true, this scan range has been removed from the reader's in_flight_ranges
    // queue because the ready_buffers_ queue is full.
    bool blocked_on_queue_;

    // IO buffers that are queued for this scan range.
    // Condition variable for GetNext
    boost::condition_variable buffer_ready_cv_;
    std::list<BufferDescriptor*> ready_buffers_;

    // The soft capacity limit for ready_buffers_. ready_buffers_ can exceed
    // the limit temporarily as the capacity is adjusted dynamically.
    // In that case, the capcity is only realized when the caller removes buffers
    // from ready_buffers_.
    int ready_buffers_capacity_;

    // Lock that should be taken during hdfs calls. Only one thread (the disk reading
    // thread) calls into hdfs at a time so this lock does not have performance impact.
    // This lock only serves to coordinate cleanup. Specifically it serves to ensure
    // that the disk threads are finished with HDFS calls before is_cancelled_ is set
    // to true and cleanup starts.
    // If this lock and lock_ need to be taken, lock_ must be taken first.
    boost::mutex hdfs_lock_;

    // If true, this scan range has been cancelled.
    bool is_cancelled_;
  };

  // Create a DiskIoMgr object.
  //  - num_disks: The number of disks the IoMgr should use. This is used for testing.
  //    Specify 0, to have the disk IoMgr query the os for the number of disks.
  //  - threads_per_disk: number of read threads to create per disk. This is also
  //    the max queue depth.
  //  - min_buffer_size: minimum io buffer size (in bytes)
  //  - max_buffer_size: maximum io buffer size (in bytes). Also the max read size.
  DiskIoMgr(int num_disks, int threads_per_disk, int min_buffer_size, int max_buffer_size);

  // Create DiskIoMgr with default configs.
  DiskIoMgr();

  // Clean up all threads and resources. This is mostly useful for testing since
  // for impalad, this object is never destroyed.
  ~DiskIoMgr();

  // Initialize the IoMgr. Must be called once before any of the other APIs.
  Status Init(MemTracker* process_mem_tracker);

  // Allocates tracking structure for this reader. Register a new reader which is
  // returned in *reader.
  // The IoMgr owns the reader object. The caller must call UnregisterReader for
  // each reader.
  // hdfs: is the handle to the hdfs connection. If NULL, it is assumed all
  //    scan ranges are on the local file system
  // reader_mem_tracker: If non-null, the memory tracker for this reader. IO buffers
  //    used for this reader will be tracked by this. If the limit is exceeded
  //    the reader will be cancelled and MEM_LIMIT_EXCEEDED will be returned via
  //    GetNext().
  Status RegisterReader(hdfsFS hdfs, ReaderContext** reader,
      MemTracker* reader_mem_tracker = NULL);

  // Unregisters reader from the disk IoMgr. This must be called for every
  // RegisterReader() regardless of cancellation and must be called in the
  // same thread as GetNext()
  // The 'reader' cannot be used after this call.
  // This call blocks until all the disk threads have finished cleaning up the reader.
  // UnregisterReader also cancels the reader from the disk IoMgr.
  void UnregisterReader(ReaderContext* reader);

  // Cancels the reader and waits for the number of active disks for this reader to reach
  // 0. This call blocks until all the disk threads have finished cleaning up the reader.
  // After calling, the only valid API is returning IO buffers that have already been
  // returned. Takes reader->lock_.
  void WaitForDisksCompletion(ReaderContext* reader);

  // This function cancels the reader asychronously. All outstanding requests
  // are aborted and tracking structures cleaned up. This does not need to be
  // called if the reader finishes normally.
  // This will also fail any outstanding GetNext()/Read requests.
  void CancelReader(ReaderContext* reader);

  // Adds the scan ranges to the queues. This call is non-blocking. The caller must
  // not deallocate the scan range pointers before UnregisterReader.
  // If schedule_immediately, the ranges are immediately put on the read queue
  // (i.e. the caller should not/cannot call GetNextRange for these ranges).
  // This can be used to do synchronous reads as well as schedule dependent ranges,
  // as in the case for columnar formats.
  Status AddScanRanges(ReaderContext* reader, const std::vector<ScanRange*>& ranges,
      bool schedule_immediately = false);

  // Returns the next unstarted scan range for this reader. When the range is returned,
  // the disk threads in the IoMgr will already have started reading from it. The
  // caller is expected to call ScanRange::GetNext on the returned range.
  // If there are no more unstarted ranges, NULL is returned.
  // This call is blocking.
  Status GetNextRange(ReaderContext* reader, ScanRange** range);

  // Reads the range and returns the result in buffer.
  // This behaves like the typical synchronous read() api, blocking until the data
  // is read. This can be called while there are outstanding ScanRanges and is
  // thread safe. Multiple threads can be calling Read() per reader at a time.
  // range *cannot* have already been added via AddScanRanges.
  Status Read(ReaderContext* reader, ScanRange* range, BufferDescriptor** buffer);

  // Returns the current status of the reader.
  Status reader_status(ReaderContext* reader) const;

  // Returns the number of unstarted scan ranges for this reader.
  int num_unstarted_ranges(ReaderContext* reader) const;

  void set_bytes_read_counter(ReaderContext*, RuntimeProfile::Counter*);
  void set_read_timer(ReaderContext*, RuntimeProfile::Counter*);
  void set_active_read_thread_counter(ReaderContext*, RuntimeProfile::Counter*);
  void set_disks_access_bitmap(ReaderContext*, RuntimeProfile::Counter*);

  int64_t queue_size(ReaderContext* reader) const;
  int64_t bytes_read_local(ReaderContext* reader) const;
  int64_t bytes_read_short_circuit(ReaderContext* reader) const;
  int64_t bytes_read_dn_cache(ReaderContext* reader) const;

  // Returns the read throughput across all readers.
  // TODO: should this be a sliding window?  This should report metrics for the
  // last minute, hour and since the beginning.
  int64_t GetReadThroughput();

  // Returns the maximum read buffer size
  int max_read_buffer_size() const { return max_buffer_size_; }

  // Returns the number of disks on the system
  int num_disks() const { return disk_queues_.size(); }

  // Returns the number of allocated buffers.
  int num_allocated_buffers() const { return num_allocated_buffers_; }

  // Returns the number of buffers currently owned by all readers.
  int num_buffers_in_readers() const { return num_buffers_in_readers_; }

  // Dumps the disk IoMgr queues (for readers and disks)
  std::string DebugString();

  // Validates the internal state is consistent. This is intended to only be used
  // for debugging.
  bool Validate() const;

  // Default ready buffer queue capacity. This constant doesn't matter too much
  // since the system dynamically adjusts.
  static const int DEFAULT_QUEUE_CAPACITY;

 private:
  friend class BufferDescriptor;
  struct DiskQueue;
  class ReaderCache;

  friend class DiskIoMgrTest_Buffers_Test;

  // Pool to allocate BufferDescriptors
  ObjectPool pool_;

  // Process memory tracker; needed to account for io buffers
  MemTracker* process_mem_tracker_;

  // Number of worker(read) threads per disk. Also the max depth of queued
  // work to the disk.
  const int num_threads_per_disk_;

  // Maximum read size. This is also the maximum size of each allocated buffer.
  const int max_buffer_size_;

  // The minimum size of each read buffer.
  const int min_buffer_size_;

  // Thread group containing all the worker threads.
  ThreadGroup disk_thread_group_;

  // Options object for cached hdfs reads. Set on startup and never modified.
  struct hadoopRzOptions* cached_read_options_;

  // True if the IoMgr should be torn down. Worker threads watch for this to
  // know to terminate. This variable is read/written to by different threads.
  volatile bool shut_down_;

  // Total bytes read by the IoMgr.
  RuntimeProfile::Counter total_bytes_read_counter_;

  // Total time spent in hdfs reading
  RuntimeProfile::Counter read_timer_;

  // Contains all readers that the IoMgr is tracking. This includes readers that are
  // active as well as those in the process of being cancelled. This is a cache
  // of reader objects that get recycled to minimize object allocations and lock
  // contention.
  boost::scoped_ptr<ReaderCache> reader_cache_;

  // Protects free_buffers_ and free_buffer_descs_
  boost::mutex free_buffers_lock_;

  // Free buffers that can be handed out to clients. There is one list for each buffer
  // size, indexed by the Log2 of the buffer size in units of min_buffer_size_. The
  // maximum buffer size is max_buffer_size_, so the maximum index is
  // Log2(max_buffer_size_ / min_buffer_size_).
  //
  // E.g. if min_buffer_size_ = 1024 bytes:
  //  free_buffers_[0]  => list of free buffers with size 1024 B
  //  free_buffers_[1]  => list of free buffers with size 2048 B
  //  free_buffers_[10] => list of free buffers with size 1 MB
  //  free_buffers_[13] => list of free buffers with size 8 MB
  //  free_buffers_[n]  => list of free buffers with size 2^n * 1024 B
  std::vector<std::list<char*> > free_buffers_;

  // List of free buffer desc objects that can be handed out to clients
  std::list<BufferDescriptor*> free_buffer_descs_;

  // Total number of allocated buffers, used for debugging.
  AtomicInt<int> num_allocated_buffers_;

  // Total number of buffers in readers
  AtomicInt<int> num_buffers_in_readers_;

  // Per disk queues. This is static and created once at Init() time.
  // One queue is allocated for each disk on the system and indexed by disk id
  std::vector<DiskQueue*> disk_queues_;

  // Returns the index into free_buffers_ for a given buffer size
  int free_buffers_idx(int64_t buffer_size);

  // Gets a buffer description object, initialized for this reader, allocating one as
  // necessary. buffer_size / min_buffer_size_ should be a power of 2, and buffer_size
  // should be <= max_buffer_size_. These constraints will be met if buffer was acquired
  // via GetFreeBuffer() (which it should have been).
  BufferDescriptor* GetBufferDesc(
      ReaderContext* reader, ScanRange* range, char* buffer, int64_t buffer_size);

  // Returns a buffer desc object which can now be used for another reader.
  void ReturnBufferDesc(BufferDescriptor* desc);

  // Returns the buffer desc and underlying buffer to the disk IoMgr. This also updates
  // the reader and disk queue state.
  void ReturnBuffer(BufferDescriptor* buffer);

  // Returns a buffer to read into with size between *buffer_size and max_buffer_size_,
  // and *buffer_size is set to the size of the buffer. If there is an
  // appropriately-sized free buffer in the 'free_buffers_', that is returned, otherwise
  // a new one is allocated. *buffer_size must be between 0 and max_buffer_size_.
  char* GetFreeBuffer(int64_t* buffer_size);

  // Garbage collect all unused io buffers. This is currently only triggered when the
  // process wide limit is hit. This is not good enough. While it is sufficient for
  // the IoMgr, other components do not trigger this GC.
  // TODO: make this run periodically?
  void GcIoBuffers();

  // Returns a buffer to the free list. buffer_size / min_buffer_size_ should be a power
  // of 2, and buffer_size should be <= max_buffer_size_. These constraints will be met if
  // buffer was acquired via GetFreeBuffer() (which it should have been).
  void ReturnFreeBuffer(char* buffer, int64_t buffer_size);

  // Returns the buffer in desc (cannot be NULL), sets buffer to NULL and clears the
  // mem tracker.
  void ReturnFreeBuffer(BufferDescriptor* desc);

  // Disk worker thread loop. This function reads the next range from the
  // disk queue if there are available buffers and places the read buffer
  // into the scan ranges outgoing queue.
  // There can be multiple threads per disk running this loop.
  void ReadLoop(DiskQueue* queue);

  // This is called from the disk thread to get the next scan to process. It will
  // wait until a scan range is available and a buffer is available to do the work.
  // This functions returns the scan range, the reader and buffer to read into.
  // This function cycles through readers and scan ranges in the reader.
  // Only returns false if the disk thread should be shut down.
  // No locks should be taken before this function call and none are left taken after.
  bool GetNextScanRange(DiskQueue*, ScanRange** range,
      ReaderContext** reader);

  // Updates disk queue and reader state after a read is complete. The read result
  // is captured in the buffer descriptor.
  void HandleReadFinished(DiskQueue*, ReaderContext*, BufferDescriptor*);

  // Validates that range is correctly initialized
  Status ValidateScanRange(ScanRange* range);
};

}

#endif
