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
#include <boost/unordered_map.hpp>

#include "common/hdfs.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "util/runtime-profile.h"

namespace impala {

class MemLimit;

// Manager object that schedules IO for all queries on all disks.  Each query maps
// to one or more readers, each of which has its own queue of scan ranges.  The
// API splits up requesting scan ranges (non-blocking) and reading the data (blocking).
// The DiskIoMgr has worker threads that will read from disk/hdfs, allowing interleaving
// of io and cpu.  This allows us to keep all disks and all cores as busy as possible.
//
// All public APIs are thread-safe.  For each reader, the RegisterReader(), GetNext()
// and UnregisterReader() must be called from the same thread (presumably the query
// execution thread).  All other functions can be called from any other thread (in
// particular CancelReader())
//
// We can model this problem as a multiple producer (threads for each disk), multiple
// consumer (readers) problem.  There are two queues that need to be synchronized.
//   1. The per disk queue: this contains a queue of readers and a queue of empty
//      buffers.
//   2. The per reader queue: this contains a queue of scan ranges and a queue of
//      of read buffers.
// The disk worker thread will remove scan ranges from the reader queue,
// remove buffers from its empty queue and add read buffers to the readers queue.
// The reader thread (split between AddScanRanges, GetNext() and ReturnBuffer()) will 
// add to its scan ranges queue, remove from its read buffers queue and add to the
// disks empty buffers queue.
// The disk threads do not synchronize with each other.  The readers don't synchronize
// with each other.  There is a lock and condition variable for each reader queue and 
// each disk queue.
// IMPORTANT: whenever both locks are needed, the lock order is to grab the reader lock
// before the disk lock.
// If multiple reader locks are needed, the locks should be taken in increasing reader
// context addresses. This is not currently done ever.
//
// Resource Management: effective resource management in the io mgr is key to good
// performance.  The resource that can be controlled is the number of io buffers
// per reader.  If this number is too low, the disks might not be kept busy OR
// the cores might be idle.  Having this too high causes excessive memory usage,
// particularly in the multiple query scenario. 
// There are two per reader knobs to control resource usage.  The first, is the number
// of io buffers the reader can use.  The second is the number of scan ranges that
// should be processed in parallel.  Buffers can either all come from different ranges,
// all from the same range or something in between.  Since we cannot parallelize
// processing a single range, the number of flight controls the amount of CPU usage
// in the scanner threads.  Both these settings can be changed dynamically and the io mgr
// will ramp them down under higher load.
// TODO: we need to think about this some more.  One way to model this is that the
// machine has a fixed amount of total io throughput and fixed amount of compute.  The
// total CPU usage can be limited by have a total max in flight scan ranges (since
// each scan range maps roughly to one core).
// TODO: IoMgr should be able to request additional scan ranges from the coordinator
// to help deal with stragglers.
// TODO: look into using a lock free queue
class DiskIoMgr {
 public:
  struct ReaderContext;
  struct ScanRangeGroup;

  // ScanRange description.  The public fields should be populated by the caller
  // before calling AddScanRanges().  The private fields are used internally by
  // the io mgr.
  class ScanRange {
   public:
    ScanRange();

    // Resets this scan range object with the scan range description.
    void Reset(const char* file, int64_t len, 
        int64_t offset, int disk_id, void* metadata = NULL);
   
    const char* file() const { return file_; }
    int64_t len() const { return len_; }
    int64_t offset() const { return offset_; }
    void* meta_data() const { return meta_data_; }
    int disk_id() const { return disk_id_; }

    void set_len(int64_t len) { len_ = len; }
    void set_offset(int64_t offset) { offset_ = offset; }

    std::string DebugString() const;

   private:
    friend class DiskIoMgr;
    
    // Initialize internal fields
    void InitInternal(ReaderContext* reader, ScanRangeGroup* group);
    
    // Path to file
    const char* file_;

    // Pointer to caller specified metadata.  This is untouched by the io manager
    // and the reader can put whatever auxiliary data in here.
    void* meta_data_;

    // byte offset in file for this range
    int64_t offset_;

    // byte len of this range
    int64_t len_;

    // id of the disk the data is on.  This is 0-indexed
    int disk_id_;    

    // Reader/owner of the scan range
    ReaderContext* reader_;

    // The group this scan range is part of, if any.
    ScanRangeGroup* group_;

    // File handle either to hdfs or local fs (FILE*)
    union {
      FILE* local_file_;
      hdfsFile hdfs_file_;
    };

    // Number of bytes read so far for this scan range
    int bytes_read_;

    // Number of io buffers that are being used for this scan range. This
    // includes buffers currently with the reader (i.e. returned by GetNext),
    // buffers queued in the ready queue and buffers currently being read into.
    // It is only safe to update this after the lock for this range's reader is
    // taken.
    int num_io_buffers_;
  };
  
  struct ScanRangeGroup {
    std::vector<ScanRange*> ranges;
  };
  
  // Buffer struct that is used by the reader and io mgr to pass read buffers.
  // It is is expected that only one thread has ownership of this object at a 
  // time.
  class BufferDescriptor {
   public:
    ScanRange* scan_range() { return scan_range_; }
    char* buffer() { return buffer_; }
    int64_t len() { return len_; }
    bool eosr() { return eosr_; }

    // Returns the offset within the scan range that this buffer starts at
    int64_t scan_range_offset() const { return scan_range_offset_; }

    // Returns the buffer to the IO Mgr. This must be called for every buffer 
    // returned by GetNext()/Reader() that did not return an error. This is non-blocking.
    void Return();

   private:
    friend class DiskIoMgr;
    BufferDescriptor(DiskIoMgr* io_mgr);

    // Resets the buffer descriptor state for a new reader, range and data buffer.
    void Reset(ReaderContext* reader, ScanRange* range, char* buffer);

    DiskIoMgr* io_mgr_;

    // Reader that this buffer is for
    ReaderContext* reader_;

    // Scan range that this buffer is for.
    ScanRange* scan_range_;
    
    // buffer with the read contents
    char* buffer_;
    
    // len of read contents
    int64_t len_;
    
    // true if the current scan range is complete
    bool eosr_;

    // Status of the read to this buffer.  if status is not ok, 'buffer' is NULL
    Status status_;

    int64_t scan_range_offset_;
  };
  
  // Create a DiskIoMgr object.
  //  - num_disks: The number of disks the io mgr should use.  This is used for testing.
  //    Specify 0, to have the disk io mgr query the os for the number of disks.
  //  - threads_per_disk: number of read threads to create per disk.  This is also
  //    the max queue depth.
  //    TODO: make this more complicated?  global/per query buffers limits?
  //  - max_read_size: maximum read size (in bytes)
  DiskIoMgr(int num_disks, int threads_per_disk, int max_read_size);

  // Create DiskIoMgr with default configs.
  DiskIoMgr();

  // Clean up all threads and resources.  This is mostly useful for testing since
  // for impalad, this object is never destroyed.
  ~DiskIoMgr();

  // Initialize the IoMgr.  Must be called once before any of the other APIs.
  Status Init(MemLimit* process_mem_limit = NULL);

  // Sets the process wide mem limit for.  If this is exceeded, io requests will 
  // fail until we are under the limit again.
  void SetProcessMemLimit(MemLimit* process_mem_limit);

  // Allocates tracking structure for this reader. Register a new reader which is
  // returned in *reader.
  // The io mgr owns the reader object.  The caller must call UnregisterReader for
  // each reader.
  // hdfs: is the handle to the hdfs connection.  If NULL, it is assumed all
  //    scan ranges are on the local file system
  // max_io_buffers: The maximum number of io buffers for this reader.
  //    Reads will not happen if there are no available io buffers.  This limits
  //    the memory usage for this reader.
  // max_parallel_ranges: The maximum number of scan ranges that should be read in
  //    parallel for this reader.  If it's 0, a default will be used. This controls
  //    how many scan ranges can be processed in parallel. 
  // reader_mem_limit: If non-null, the memory limit for this reader.  IO buffers
  //    used for this reader will count against this limit.  If the limit is exceeded
  //    the reader will be cancelled and MEM_LIMIT_EXCEEDED will be returned via
  //    GetNext().
  // If max_parallel_ranges is less than max_io_buffers, multiple buffers will 
  // potentially be queued per range.  If max_parallel_ranges is greater, some ranges
  // will be started without any buffers queued for it.  
  Status RegisterReader(hdfsFS hdfs, int max_io_buffers, int max_parallel_ranges,
      ReaderContext** reader, MemLimit* reader_mem_limit = NULL);

  // Unregisters reader from the disk io mgr.  This must be called for every 
  // RegisterReader() regardless of cancellation and must be called in the
  // same thread as GetNext()
  // The 'reader' cannot be used after this call.
  // This call blocks until all the disk threads have finished cleaning up the reader.
  // UnregisterReader also cancels the reader from the disk io mgr.
  void UnregisterReader(ReaderContext* reader);

  // This function cancels the reader asychronously.  All outstanding requests
  // are aborted and tracking structures cleaned up.  This does not need to be
  // called if the reader finishes normally.
  // This will also fail any outstanding GetNext()/Read requests.
  void CancelReader(ReaderContext* reader);

  // Sets the maximum io buffers for this reader.  This can be higher or lower than
  // the current quota.  The update is not instantenously; in flight buffers are not
  // taken back right away.  
  // A value of 0 will effectively halt the reader.  
  // If 'max_io_buffers' is non-zero, this will return true if the request is possible 
  // and false otherwise.  It might be impossible to satisfy this request if the new
  // quota is lower than the minimum number of buffers required for this reader
  // (due to grouped scan ranges).
  // This is mostly used for testing.  The IO mgr will automatically adjust this for
  // readers.
  // TODO: this needs some policy.
  bool SetMaxIoBuffers(ReaderContext* reader, int max_io_buffers);

  // Adds the scan ranges to the queues. This call is non-blocking.  The caller must
  // not deallocate the scan range pointers before UnregisterReader.
  Status AddScanRanges(ReaderContext* reader, const std::vector<ScanRange*>& ranges);
  
  // Similar to AddScanRanges except adds groups of scan ranges.  For each group,
  // the IO mgr will guarantee that if any of the ranges in the group have ready buffers,
  // all ranges in that group will also have ready buffers without needing to return
  // any of the buffers in the group.
  Status AddScanRangeGroups(ReaderContext* reader, 
      const std::vector<ScanRangeGroup*>& groups);

  // Get the next buffer for this query. Results are returned in buffer. 
  // GetNext can be called from multiple threads for a single reader and has a few
  // guarantees:
  //    1. All bytes within a scan range will be returned sequentially  
  //    2. Only one buffer per scan range will be returned at a time
  // GetNext can ping pong between different scan ranges. 
  // This call is blocking. 
  // If there was an error encountered reading from one of the scan ranges for
  // this reader, the error will be returned. 'buffer' will still be populated
  // with the scan range id. 
  // If the read was cancelled, the cancelled status is returned and *buffer is NULL.
  // If all scan ranges were previously finished, *buffer is set to NULL.
  // If *buffer is non-null, the caller must call Return() on the returned buffer.
  // *eos will be set to true if all scan ranges for this reader have been 
  // returned.
  // On non-cancelled errors, *buffer will be not contain a byte buffer but
  // will contain the description of the read that it was trying to do that
  // caused the error (file, offset, etc).  The buffer descriptor must be
  // returned in this case.
  Status GetNext(ReaderContext* reader, BufferDescriptor** buffer, bool* eos);

  // Identical to GetNext() except that it will not block if this reader has
  // exhausted all its buffers.  In that case.  *buffer will be set to NULL and
  // this function will return right away.
  // Note: this can still be blocking if there are buffers ready but not yet
  // read by a disk thread.
  // Currently, this is only used for testing.
  Status TryGetNext(ReaderContext* reader, BufferDescriptor** buffer, bool* eos);

  // Reads the range and returns the result in buffer.  
  // This behaves like the typical synchronous read() api, blocking until the data 
  // is read.This can be called while there are outstanding ScanRanges and is 
  // thread safe.  Multiple threads can be calling Read() per reader at a time.
  Status Read(hdfsFS, ScanRange* range, BufferDescriptor** buffer);

  void set_bytes_read_counter(ReaderContext*, RuntimeProfile::Counter*);
  void set_read_timer(ReaderContext*, RuntimeProfile::Counter*);
  void set_active_read_thread_counter(ReaderContext*, RuntimeProfile::Counter*);
  void set_disks_access_bitmap(ReaderContext*, RuntimeProfile::Counter*);

  // Returns the read throughput across all readers.    
  // TODO: should this be a sliding window?  This should report metrics for the
  // last minute, hour and since the beginning.
  int64_t GetReadThroughput();

  // Returns the read buffer size
  int read_buffer_size() const { return max_read_size_; }

  // Returns the number of disks on the system
  int num_disks() const { return disk_queues_.size(); }

  // Returns the number of allocated buffers.
  int num_allocated_buffers() const { return num_allocated_buffers_; }

  // Returns the number of buffers currently owned by all readers.
  int num_buffers_in_readers() const { return num_buffers_in_readers_; }

  // Dumps the disk io mgr queues (for readers and disks)
  std::string DebugString();

  // Returns the default number of parallel scan ranges per reader.
  static int default_parallel_scan_ranges();

  // Validates the internal state is consistent.  This is intended to only be used
  // for debugging.
  bool Validate() const;

 private:
  friend class BufferDescriptor;
  struct DiskQueue;
  class ReaderCache;

  // Pool to allocate BufferDescriptors
  ObjectPool pool_;

  // Process memory limit that tracks io buffers.
  MemLimit* process_mem_limit_;

  // Number of worker(read) threads per disk.  Also the max depth of queued
  // work to the disk.
  int num_threads_per_disk_;

  // Maximum read size.  This is also the size of each allocated buffer.
  int max_read_size_;

  // Thread group containing all the worker threads.
  boost::thread_group disk_thread_group_;

  // True if the io mgr should be torn down.  Worker threads watch for this to
  // know to terminate.  This variable is read/written to by different threads.
  volatile bool shut_down_;

  // Total bytes read by the io mgr.
  RuntimeProfile::Counter total_bytes_read_counter_;

  // Total time spent in hdfs reading
  RuntimeProfile::Counter read_timer_;

  // Contains all readers that the io mgr is tracking.  This includes readers that are
  // active as well as those in the process of being cancelled.  This is a cache
  // of reader objects that get recycled to minimize object allocations and lock
  // contention.
  boost::scoped_ptr<ReaderCache> reader_cache_;

  // Protects free_buffers_ and free_buffer_descs_
  boost::mutex free_buffers_lock_;
  
  // List of free buffer descs that can be handed out to clients.
  std::list<char*> free_buffers_;

  // List of free buffer desc objects that can be handed out to clients
  std::list<BufferDescriptor*> free_buffer_descs_;

  // Total number of allocated buffers, used for debugging.
  int num_allocated_buffers_;

  // Total number of buffers in readers
  int num_buffers_in_readers_;

  // Per disk queues.  This is static and created once at Init() time.
  // One queue is allocated for each disk on the system and indexed by disk id
  std::vector<DiskQueue*> disk_queues_;

  // Schedules the ranges onto the disks
  Status AddScanRangesInternal(ReaderContext* reader, 
      const std::vector<ScanRange*>& ranges);

  // Gets a buffer description object, initialized for this reader, allocating
  // one as necessary
  BufferDescriptor* GetBufferDesc(ReaderContext* reader, ScanRange* range, char* buffer);

  // Returns a buffer desc object which can now be used for another reader.
  void ReturnBufferDesc(BufferDescriptor* desc);
  
  // Returns the buffer desc and underlying buffer to the disk io mgr.  This also updates
  // the reader and disk queue state.
  void ReturnBuffer(BufferDescriptor* buffer);

  // Returns a buffer to read into that is the size of max_read_size_.  If there is a
  // free buffer in the 'free_buffers_', that is returned, otherwise a new one is 
  // allocated.
  // Updates mem limits for reader
  char* GetFreeBuffer(ReaderContext* reader);

  // Garbage collect all unused io buffers.  This is currently only triggered when the
  // process wide limit is hit.  This is not good enough.  While it is sufficient for
  // the io mgr, other components do not trigger this GC.  
  // TODO: make this run periodically?
  void GcIoBuffers();

  // Returns a buffer to the free list and updates mem usage for 'reader' 
  void ReturnFreeBuffer(ReaderContext* reader, char* buffer);

  // Disk worker thread loop.  This function reads the next range from the 
  // disk queue if there are available buffers and places the read buffer into 
  // the Reader's 'read_blocks' list.
  // There can be multiple threads per disk running this loop.
  void ReadLoop(DiskQueue* queue);

  // Opens the file for 'range'.  This function only modifies memory in local
  // variables and does not need to be synchronized.
  // if hdfs_connection is NULL, 'range' must be for a local file
  Status OpenScanRange(hdfsFS hdfs_connection, ScanRange* range) const;

  // Closes the file for 'range'.  This function only modifies memory in local
  // variables and does not need to be synchronized.
  // if hdfs_connection is NULL, 'range' must be for a local file
  void CloseScanRange(hdfsFS hdfs_connection, ScanRange* range) const;

  // Reads from 'range' into 'buffer'.  Buffer is preallocated.  Returns the number
  // of bytes read.  Updates range to keep track of where in the file we are. 
  // Only modifies local variables and does not need synchronization.
  // if hdfs_connection is NULL, 'range' must be for a local file
  Status ReadFromScanRange(hdfsFS hdfs_connection, ScanRange* range, 
      char* buffer, int64_t* bytes_read, bool* eosr);

  // This is called from the disk thread to get the next scan to process.  It will
  // wait until a scan range is available and a buffer is available to do the work.
  // This functions returns the scan range, the reader and buffer to read into.
  // This function cycles through readers and scan ranges in the reader.
  // Only returns false if the disk thread should be shut down.
  // No locks should be taken before this function call and none are left taken after.
  bool GetNextScanRange(DiskQueue*, ScanRange** range, 
      ReaderContext** reader, char** buffer);

  // Updates disk queue and reader state after a read is complete.  The read result
  // is captured in the buffer descriptor.
  void HandleReadFinished(DiskQueue*, ReaderContext*, BufferDescriptor*);

  // Cancels the reader with status code 'status'.  GetNext will return 'status'.
  void CancelReaderInternal(ReaderContext* reader, Status status);
};

}

#endif

