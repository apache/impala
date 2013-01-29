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

#include "runtime/disk-io-mgr.h"

#include <queue>
#include <boost/functional/hash.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/locks.hpp>

#include "common/logging.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"

// Control the number of disks on the machine.  If 0, this comes from the system 
// settings.
DEFINE_int32(num_disks, 0, "Number of disks on data node.");
// Default IO Mgr configs.
// The maximum number of the threads per disk is also the max queue depth per disk.
// The read size is the size of the reads sent to hdfs/os.
// There is a trade off of latency and throughout, trying to keep disks busy but
// not introduce seeks.  The literature seems to agree that with 8 MB reads, random
// io and sequential io perform similarly.
DEFINE_int32(num_threads_per_disk, 1, "number of threads per disk");
DEFINE_int32(read_size, 8 * 1024 * 1024, "Read Size (in bytes)");

using namespace boost;
using namespace impala;
using namespace std;

// Per disk state
struct DiskIoMgr::DiskQueue {
  // Disk id (0-based)
  int disk_id;
  
  // Lock that protects access to 'readers' and 'work_available'
  mutex lock;

  // Condition variable to signal the disk threads that there is work to do or the
  // thread should shut down.  A disk thread will be woken up when there is a reader
  // added to the queue.  Readers are only on the queue when they have both work
  // (scan range added) and an available buffer.
  condition_variable work_available;
  
  // list of all readers that have work queued on this disk and might have empty buffers 
  // to read into.
  list<ReaderContext*> readers;

  // Enqueue the reader to the disk queue.  The DiskQueue lock must not be taken.
  inline void EnqueueReader(ReaderContext* reader) {
    {
      unique_lock<mutex> disk_lock(lock);
      // Check that the reader is not already on the queue
      DCHECK(find(readers.begin(), readers.end(), reader) == readers.end());
      readers.push_back(reader);
    }
    work_available.notify_all();
  }

  DiskQueue(int id) : disk_id(id) {
  }
};

// Internal per reader state
struct DiskIoMgr::ReaderContext {
  enum State {
    // Reader is initialized and maps to a client
    Active,

    // Reader is in the process of being cancelled.  Cancellation is coordinated between
    // different threads and when they are all complete, the reader is moved to the
    // inactive state.
    Cancelled,

    // Reader does not map to a client.  Accessing memory in a reader in this state
    // is invalid (i.e. it is equivalent to a dangling pointer).
    Inactive,
  };
  
  friend class DiskIoMgr;

  // Parent object
  DiskIoMgr* parent_;
  
  // Total bytes read for this reader
  RuntimeProfile::Counter* bytes_read_counter_;

  // Total time spent in hdfs reading
  RuntimeProfile::Counter* read_timer_;

  // hdfsFS connection handle.  This is set once and never changed for the duration 
  // of the reader.  NULL if this is a local reader.
  hdfsFS hdfs_connection_;

  // If true, this is a reader from Read()
  bool sync_reader_;

  // Condition variable for GetNext
  condition_variable buffer_ready_cond_var_;

  // Condition variable for UnregisterReader to wait for all disks to complete
  condition_variable disks_complete_cond_var_;

  // All fields below are accessed by multiple threads and the lock needs to be
  // taken before accessing them.
  mutex lock_;
  
  // Current state of the reader
  State state_;
  
  // The total number of scan ranges that have not been returned via GetNext().
  int num_remaining_scan_ranges_;
  
  // The number of disks with scan ranges remaining (always equal to the sum of
  // non-empty ranges in per disk states).
  int num_disks_with_ranges_;

  // The number of io buffers allocated for this reader.  The reader can be temporarily
  // over this limit if the limit dropped while reads were already in flight.  In this
  // case, the limit isn't realized until future reads.
  int num_buffers_quota_;
  
  // The minimum number of buffers this reader needs to make any progress.  For 
  // non-grouped ranges, this will always be 1.  For grouped ranges, this will be the 
  // number of ranges in the group.
  int min_num_buffers_;

  // The number of buffers that are being used for this reader.  This includes buffers
  // currently being used by disk threads as well as buffers currently owned by the
  // reader.
  int num_used_buffers_;

  // The max number of scan ranges that should be in flight at a time.
  int max_parallel_scan_ranges_;

  // The number of ranges currently in flight
  int num_in_flight_ranges_;

  // list of buffers that have been read from disk but not returned to the reader.
  // Also contains dummy buffers (just an error code)  signaling failed reads.  
  // In the non-error state, ready_buffers_.size() + num_empty_buffers_ 
  // will be the total number of buffers allocated for this reader.
  list<BufferDescriptor*> ready_buffers_;
  
  // The number of buffers currently owned by the reader.  Only included for debugging
  // and diagnostics
  int num_outstanding_buffers_;

  // Struct containing state per disk. See comments in the disk read loop on how 
  // they are used.
  struct PerDiskState {
    // If true, this disk is all done for this reader, including any cleanup.
    bool done;

    // For each disks, the number of scan ranges that have not been fully read.
    int num_remaining_scan_ranges;
  
    // Queue of scan ranges for this reader on this disk that are currently in progress.
    // A scan range that is currently being read by one thread cannot be picked up by 
    // another thread and is temporarily removed from the queue.  The size of this 
    // queue is always less than or equal to num_remaining_scan_ranges. 
    list<ScanRange*> in_flight_ranges;

    // Queue of ranges that have not started being read.  This list is exclusive
    // with in_flight_ranges.  
    list<ScanRange*> unscheduled_ranges;

    // For each disk, keeps track if the reader is on that disk's queue.  This saves
    // us from having to walk the disk queue list.
    bool is_on_queue;

    // For each disk, the number of threads issuing the underlying read on behalf of
    // this reader. There are a few places where we release the reader lock, do some
    // work, and then grab the lock again.  Because we don't hold the lock for the
    // entire operation, we need this ref count to keep track of which thread should do
    // final resource cleanup during cancellation.
    // Only the thread that sees the count at 0 should do the final cleanup.
    int num_threads_in_read;

    PerDiskState() {
      Reset();
    }

    void Reset() {
      done = true;
      num_remaining_scan_ranges = 0;
      in_flight_ranges.clear();
      unscheduled_ranges.clear();
      is_on_queue = false;
      num_threads_in_read = 0;
    }
  };

  // Per disk states to synchronize multiple disk threads accessing the same reader.
  vector<PerDiskState> disk_states_;

  // list of disks that this reader has work for but are currently unscheduled because
  // of insufficient buffers.  For example, if there are 10 disks and 5 buffers, 5
  // disks will end up on this queue at any time.
  // This queue allows us to control which disk to start up when a resource becomes
  // available. 
  // TODO: this could be a priority queue to do something fancier than round robin
  // (e.g. start up disks with more remaining work)
  list<DiskQueue*> unscheduled_disks_;

  ReaderContext(DiskIoMgr* parent, int num_disks) 
    : parent_(parent),
      bytes_read_counter_(NULL),
      read_timer_(NULL),
      state_(Inactive),
      disk_states_(num_disks) {
  }

  // Returns a scan range to read for this reader for 'disk_id'.  
  // Returns false if there is no scan range to read from.  This can return false 
  // for a number of reasons:
  // - No more ranges left or all ranges are being worked on by other threads
  // - Reader is at buffer quota and should not issue more reads.
  // Returns the range and buffer if this function returned true
  // This function must be called with the reader lock taken
  bool GetScanRange(int disk_id, ScanRange** range, char** buffer);

  // Resets this object for a new reader
  void Reset(hdfsFS hdfs_connection) {
    DCHECK_EQ(state_, Inactive);

    bytes_read_counter_ = NULL;
    read_timer_ = NULL;

    state_ = Active;
    sync_reader_ = false;
    hdfs_connection_ = hdfs_connection;

    min_num_buffers_ = 1;
    num_buffers_quota_ = 0;

    num_remaining_scan_ranges_ = 0;
    num_disks_with_ranges_ = 0;
    num_used_buffers_ = 0;
    num_outstanding_buffers_ = 0;
    num_in_flight_ranges_ = 0;
    
    for (int i = 0; i < disk_states_.size(); ++i) {
      disk_states_[i].Reset();
    }
  }
  
  // Decrements the number of active disks for this reader.  If the disk count
  // goes to 0, the disk complete condition variable is signaled.
  // Reader lock must be taken before this call.
  void DecrementDiskRefCount() {
    // boost doesn't let us dcheck that the the reader lock is taken
    DCHECK_GT(num_disks_with_ranges_, 0);
    if (--num_disks_with_ranges_ == 0) {
      disks_complete_cond_var_.notify_one(); 
    }
    DCHECK(Validate()) << endl << DebugString();
  }

  // Adds disk_queue to unscheduled_disks_.  Reader lock must be taken before this.
  void EnqueueUnscheduledDisk(DiskQueue* disk_queue) {
    DCHECK(!disk_states_[disk_queue->disk_id].is_on_queue) << endl << DebugString();
#ifndef NDEBUG
    // Validate it is not already on the queue.
    for (list<DiskQueue*>::iterator it = unscheduled_disks_.begin(); 
         it != unscheduled_disks_.end(); ++it) {
      DCHECK(*it != disk_queue) << endl << DebugString() << endl << disk_queue->disk_id;
    }
#endif
    unscheduled_disks_.push_back(disk_queue);
  }

  // Wakes up 'max_disks' this threads that can now be scheduled because there is
  // either more work or more buffers.
  // Reader lock should be taken before this.
  void ScheduleNewDisks(int max_disks) {
    if (max_disks == 0) return;
    DCHECK(Validate()) << endl << DebugString();
    
    // Round-robin across unscheduled disks.
    for (int i = 0; i < max_disks; ++i) {
      // No disks left that are blocked on resources, nothing to do.
      if (unscheduled_disks_.empty()) return;

      DiskQueue* disk_queue = unscheduled_disks_.front();
      unscheduled_disks_.pop_front();
      
      ReaderContext::PerDiskState& state = disk_states_[disk_queue->disk_id];
      DCHECK(!state.is_on_queue);
      state.is_on_queue = true;
      disk_queue->EnqueueReader(this);
    }
  }

  // Validates invariants of reader.  Reader lock must be taken beforehand.
  bool Validate() const;

  // Dumps out reader information.  Lock should be taken by caller
  string DebugString() const;
};

// This class provides a cache of ReaderContext objects.  ReaderContexts are recycled.
// This is good for locality as well as lock contention.  The cache has the property that 
// regardless of how many clients get added/removed, the memory locations for 
// existing clients do not change (not the case with std::vector) minimizing the locks we
// have to take across all readers.
// All functions on this object are thread safe
class DiskIoMgr::ReaderCache {
 public:
  ReaderCache(DiskIoMgr* io_mgr) : io_mgr_(io_mgr) {}
  
  // Returns reader to the cache.  This reader object can now be reused.
  void ReturnReader(ReaderContext* reader) {
    DCHECK(reader->state_ != ReaderContext::Inactive);
    reader->state_ = ReaderContext::Inactive;
    lock_guard<mutex> l(lock_);
    inactive_readers_.push_back(reader);
  }

  // Returns a new reader object.  Allocates a new reader context if necessary.
  ReaderContext* GetNewReader() {
    lock_guard<mutex> l(lock_);
    if (!inactive_readers_.empty()) {
      ReaderContext* reader = inactive_readers_.front();
      inactive_readers_.pop_front();
      return reader;
    } else {
      ReaderContext* reader = new ReaderContext(io_mgr_, io_mgr_->num_disks());
      all_readers_.push_back(reader);
      return reader;
    }
  }
  
  // This object has the same lifetime as the diskiomgr.
  ~ReaderCache() {
    for (list<ReaderContext*>::iterator it = all_readers_.begin();
        it != all_readers_.end(); ++it) {
      delete *it;
    }
  }

  // Validates that all readers are cleaned up and in the inactive state.  No locks
  // are taken since this is only called from the disk io mgr destructor.
  bool ValidateAllInactive() {
    for (list<ReaderContext*>::iterator it = all_readers_.begin();
        it != all_readers_.end(); ++it) {
      if ((*it)->state_ != ReaderContext::Inactive) {
        return false;
      } 
    }
    DCHECK_EQ(all_readers_.size(), inactive_readers_.size());
    return all_readers_.size() == inactive_readers_.size();
  }

  string DebugString() {
    lock_guard<mutex> l(lock_);
    stringstream ss;
    for (list<ReaderContext*>::iterator it = all_readers_.begin();
        it != all_readers_.end(); ++it) {
      unique_lock<mutex> lock((*it)->lock_);
      ss << (*it)->DebugString() << endl;
    }
    return ss.str();
  }

 private:
  DiskIoMgr* io_mgr_;

  // lock to protect all members below
  mutex lock_;

  // List of all reader created.  Used for debugging
  list<ReaderContext*> all_readers_;

  // List of inactive readers.  These objects can be used for a new reader.
  list<ReaderContext*> inactive_readers_;
};


DiskIoMgr::ScanRange::ScanRange() {
  Reset(NULL, -1, -1, -1);
}

void DiskIoMgr::ScanRange::Reset(const char* file, int64_t len, int64_t offset, 
    int disk_id, void* meta_data) {
  file_ = file;
  len_ = len;
  offset_ = offset;
  disk_id_ = disk_id;
  meta_data_ = meta_data;
}
    
void DiskIoMgr::ScanRange::InitInternal(ReaderContext* reader) {
  reader_ = reader;
  local_file_ = NULL;
  hdfs_file_ = NULL;
  bytes_read_ = 0;
}

string DiskIoMgr::ScanRange::DebugString() const{
  stringstream ss;
  ss << "file=" << file_ << " disk_id=" << disk_id_ << " offset=" << offset_
     << " len=" << len_ << " bytes_read=" << bytes_read_;
  return ss.str();
}

DiskIoMgr::BufferDescriptor::BufferDescriptor(DiskIoMgr* io_mgr) :
  io_mgr_(io_mgr), reader_(NULL), buffer_(NULL) {
}

void DiskIoMgr::BufferDescriptor::Reset(ReaderContext* reader, 
      ScanRange* range, char* buffer) {
  DCHECK(io_mgr_ != NULL);
  DCHECK(buffer_ == NULL);
  reader_ = reader;
  scan_range_ = range;
  buffer_ = buffer;
  len_ = 0;
  eosr_ = false;
  status_ = Status::OK;
}

void DiskIoMgr::BufferDescriptor::Return() {
  DCHECK(io_mgr_ != NULL);
  io_mgr_->ReturnBuffer(this);
}

DiskIoMgr::DiskIoMgr() :
    num_threads_per_disk_(FLAGS_num_threads_per_disk),
    max_read_size_(FLAGS_read_size),
    shut_down_(false),
    total_bytes_read_counter_(TCounterType::BYTES),
    read_timer_(TCounterType::TIME_NS),
    num_allocated_buffers_(0) {
  int num_disks = FLAGS_num_disks;
  if (num_disks == 0) num_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_disks);
}

DiskIoMgr::DiskIoMgr(int num_disks, int threads_per_disk, int max_read_size) :
    num_threads_per_disk_(threads_per_disk),
    max_read_size_(max_read_size),
    shut_down_(false),
    total_bytes_read_counter_(TCounterType::BYTES),
    read_timer_(TCounterType::TIME_NS),
    num_allocated_buffers_(0) {
  if (num_disks == 0) num_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_disks);
}

DiskIoMgr::~DiskIoMgr() {
  shut_down_ = true;
  // Notify all worker threads and shut them down. 
  for (int i = 0; i < disk_queues_.size(); ++i) {
    {
      // This lock is necessary to properly use the condition var to notify
      // the disk worker threads.  The readers also grab this lock so updates
      // to shut_down_ are protected.
      unique_lock<mutex> lock(disk_queues_[i]->lock);
    }
    disk_queues_[i]->work_available.notify_all();
  }
  disk_thread_group_.join_all();

  for (int i = 0; i < disk_queues_.size(); ++i) {
    int disk_id = disk_queues_[i]->disk_id;
    for (list<ReaderContext*>::iterator reader = disk_queues_[i]->readers.begin(); 
      reader != disk_queues_[i]->readers.end(); ++reader) { 
      DCHECK_EQ((*reader)->disk_states_[disk_id].num_threads_in_read, 0);
      (*reader)->DecrementDiskRefCount();
    }
  } 

  DCHECK(reader_cache_->ValidateAllInactive()) << endl << DebugString();
  
  // Delete all allocated buffers
  DCHECK_EQ(num_allocated_buffers_, free_buffers_.size());
  for (list<char*>::iterator iter = free_buffers_.begin();
      iter != free_buffers_.end(); ++iter) {
    delete *iter;
  }

  for (int i = 0; i < disk_queues_.size(); ++i) {
    delete disk_queues_[i];
  }
}

Status DiskIoMgr::Init() {
  for (int i = 0; i < disk_queues_.size(); ++i) {
    disk_queues_[i] = new DiskQueue(i);
    for (int j = 0; j < num_threads_per_disk_; ++j) {
      disk_thread_group_.add_thread(
          new thread(&DiskIoMgr::ReadLoop, this, disk_queues_[i]));
    }
  }
  reader_cache_.reset(new ReaderCache(this));
  return Status::OK;
}

Status DiskIoMgr::RegisterReader(hdfsFS hdfs, int max_io_buffers, 
    int max_parallel_ranges, ReaderContext** reader) {
  DCHECK(reader_cache_.get() != NULL) << "Must call Init() first.";
  *reader = reader_cache_->GetNewReader();
  (*reader)->Reset(hdfs);
  if (max_parallel_ranges > 0) {
    (*reader)->max_parallel_scan_ranges_ = max_parallel_ranges;
  } else {
    // TODO: reconsider this heuristic.  We want to keep all the cores busy
    // but not cause excessive context switches.
    (*reader)->max_parallel_scan_ranges_ = 2 * CpuInfo::num_cores();
  }
  SetMaxIoBuffers(*reader, max_io_buffers);
  return Status::OK;
}

void DiskIoMgr::UnregisterReader(ReaderContext* reader) {
  // First cancel the reader.  This is more or less a no-op if the reader is
  // complete (common case).
  CancelReader(reader);
  
  unique_lock<mutex> lock(reader->lock_);
  DCHECK_EQ(reader->num_outstanding_buffers_, 0) << endl << reader->DebugString();
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  DCHECK(reader->ready_buffers_.empty()) << endl << reader->DebugString();
  while (reader->num_disks_with_ranges_ > 0) {
    reader->disks_complete_cond_var_.wait(lock);
  }
  
  if (reader->sync_reader_) {
    DCHECK_EQ(reader->num_used_buffers_, 1) << endl << reader->DebugString();
  } else {
    DCHECK_EQ(reader->num_used_buffers_, 0) << endl << reader->DebugString();
  }

  for (int i = 0; i < reader->disk_states_.size(); ++i) {
    // Close any open scan ranges now.  If the reader is cancelled, then there
    // might be open scan ranges that did not get closed by the disk threads.
    // The ranges are normally closed when the read is complete, which does not
    // happen with cancellation.
    ReaderContext::PerDiskState& state = reader->disk_states_[i];
    list<ScanRange*>::iterator range_it = state.in_flight_ranges.begin();
    for (; range_it != state.in_flight_ranges.end(); ++range_it) {
      CloseScanRange(reader->hdfs_connection_, *range_it);
    }
  }
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  reader_cache_->ReturnReader(reader);
}

// Cancellation requires coordination from multiple threads.  Each thread that currently
// has a reference to the reader must notice the cancel and remove it from its tracking
// structures.  The last thread to touch the reader should deallocate (aka recycle) the
// reader context object.  Potential threads are:
//  1. Disk threads that are currently reading for this reader.  
//  2. Caller thread that is waiting in GetNext. 
//
// The steps are:
// 1. Cancel will immediately set the reader in the Cancelled state.  This prevents any 
// other thread from adding more ready buffers to this reader (they all take a lock and 
// check the state before doing so).
// 2. Return all ready buffers that are not returned from GetNext.  This also triggers 
// waking up the disks that have work queued for this reader.
// 3. Wake up all threads waiting in GetNext().  They will notice the cancelled state and
// return cancelled statuses.
// 4. Disk threads notice the reader is cancelled either when picking the next reader 
// to read for or when they try to enqueue a ready buffer.  Upon noticing the cancelled 
// state, removes the reader from the disk queue.  The last thread per disk with an 
// outstanding reference to the reader decrements the number of disk queues the reader 
// is on.  
void DiskIoMgr::CancelReader(ReaderContext* reader) {
  // copy of ready but unreturned buffers that need to be cleaned up.
  list<BufferDescriptor*> ready_buffers_copy;
  {
    unique_lock<mutex> lock(reader->lock_);
    DCHECK(reader->Validate()) << endl << reader->DebugString();

    // Already being cancelled
    if (reader->state_ == ReaderContext::Cancelled) {
      return;
    }
    
    // The reader will be put into a cancelled state until call cleanup is complete.
    reader->state_ = ReaderContext::Cancelled;
    
    ready_buffers_copy.swap(reader->ready_buffers_);
    reader->num_outstanding_buffers_ += ready_buffers_copy.size();

    // Schedule reader on all disks.  The disks will notice it is cancelled and do any
    // required cleanup
    reader->ScheduleNewDisks(reader->unscheduled_disks_.size());
  }
  
  // Signal reader and unblock the GetNext/Read thread.  That read will fail with
  // a cancelled status.
  reader->buffer_ready_cond_var_.notify_all();

  // Clean up already read blocks.  We can safely work on a local copy of the ready
  // buffer.  Before releasing the reader lock, we set the reader state to Cancelled.  
  // All other threads first check the reader state before modifying ready_buffers_.
  for (list<BufferDescriptor*>::iterator it = ready_buffers_copy.begin();
       it != ready_buffers_copy.end(); ++it) {
    ReturnBuffer(*it);
  }
}

bool DiskIoMgr::SetMaxIoBuffers(ReaderContext* reader, int max_io_buffers) {
  int new_buffers = 0;
  {
    lock_guard<mutex> l(reader->lock_);
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    if (reader->state_ == ReaderContext::Cancelled) return true;
    if (max_io_buffers == reader->num_buffers_quota_) return true;
    if (max_io_buffers != 0 && max_io_buffers < reader->min_num_buffers_) return false;
    new_buffers = max_io_buffers - reader->num_buffers_quota_;
    reader->num_buffers_quota_ = max_io_buffers;
    // This reader just got more buffers.  Wake up more disk threads.
    if (new_buffers > 0) reader->ScheduleNewDisks(new_buffers);
  }
  
  return true;
}

int DiskIoMgr::num_empty_buffers(ReaderContext* reader) {
  unique_lock<mutex> (reader->lock_);
  // This can be less than 0 if the quota was lowered after some reads happened
  return ::max(reader->num_buffers_quota_ - reader->num_used_buffers_, 0);
}

void DiskIoMgr::set_read_timer(ReaderContext* r, RuntimeProfile::Counter* c) {
  r->read_timer_ = c;
}

void DiskIoMgr::set_bytes_read_counter(ReaderContext* r, RuntimeProfile::Counter* c) {
  r->bytes_read_counter_ = c;
}

int64_t DiskIoMgr::GetReadThroughput() {
  return RuntimeProfile::UnitsPerSecond(&total_bytes_read_counter_, &read_timer_);
}

Status DiskIoMgr::AddScanRanges(ReaderContext* reader, const vector<ScanRange*>& ranges,
    bool is_grouped) {
  DCHECK(!ranges.empty());

  // Validate all ranges before adding any
  for (int i = 0; i < ranges.size(); ++i) {
    int disk_id = ranges[i]->disk_id_;
    if (disk_id < 0 || disk_id >= disk_queues_.size()) {
      stringstream ss;
      ss << "Invalid scan range.  Bad disk id: " << disk_id;
      DCHECK(false) << ss.str();
      return Status(ss.str());
    }
  }

  // disks that this reader needs to be scheduled on.
  int num_new_disks = 0;
  {
    unique_lock<mutex> lock(reader->lock_);
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    
    if (reader->state_ == ReaderContext::Cancelled) return Status::CANCELLED;

    // Add each range to the queue of the disk the range is on
    for (int i = 0; i < ranges.size(); ++i) {
      // Don't add empty ranges.
      DCHECK_NE(ranges[i]->len(), 0);
      ScanRange* range = ranges[i];
      range->InitInternal(reader);
      ReaderContext::PerDiskState& state = reader->disk_states_[range->disk_id_];
      if (state.done) {
        DCHECK_EQ(state.num_threads_in_read, 0);
        DCHECK_EQ(state.num_remaining_scan_ranges, 0);

        state.done = false;
        ++num_new_disks;
        ++reader->num_disks_with_ranges_;
        reader->EnqueueUnscheduledDisk(disk_queues_[range->disk_id_]);
      }
      state.unscheduled_ranges.push_back(range);
      ++state.num_remaining_scan_ranges;
    }
    reader->num_remaining_scan_ranges_ += ranges.size();
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    
    reader->ScheduleNewDisks(num_new_disks);
  }

  // If the scan ranges are grouped, we potentially need to update the number of
  // buffers per disk for this reader.  For example, if the group contains 10 ranges
  // all on one disk (e.g. columnar file), then the reader needs at least 10 buffers
  // to make any progress.
  if (is_grouped) {
    DCHECK(false) << "TODO";
    int group_size = ranges.size();
    int quota = reader->num_buffers_quota_;
    DCHECK_LE(quota, reader->min_num_buffers_);
    if (group_size > quota) {
      SetMaxIoBuffers(reader, quota);
      reader->min_num_buffers_ = ::max(group_size, reader->min_num_buffers_);
    }
  }

  return Status::OK;
}

Status DiskIoMgr::Read(hdfsFS hdfs, ScanRange* range, BufferDescriptor** buffer) {
  ReaderContext* local_context = NULL;
  // Make a local context for doing the synchronous read
  // Local reader always has one buffer.  Since this is synchronous, it can't use
  // the same resource pool as the parent reader.
  RETURN_IF_ERROR(RegisterReader(hdfs, 1, 1, &local_context));
  local_context->sync_reader_ = true;

  vector<ScanRange*> ranges;
  ranges.push_back(range);
  Status status = AddScanRanges(local_context, ranges);
  if (status.ok()) {
    bool eos;
    status = GetNext(local_context, buffer, &eos);
    DCHECK(eos);
  }

  // Reassign the buffer's reader to the external context and clean up the temp context
  if (*buffer != NULL) (*buffer)->reader_ = NULL;
  
  // The local context doesn't track its resources the same way.
  local_context->num_outstanding_buffers_ = 0;

  UnregisterReader(local_context);
  return status;
}

Status DiskIoMgr::GetNext(ReaderContext* reader, BufferDescriptor** buffer, bool* eos) {
  unique_lock<mutex> lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  
  *buffer = NULL;
  *eos = true;

  if (reader->state_ == ReaderContext::Cancelled) return Status::CANCELLED;

  VLOG_FILE << "GetNext(): reader=" << reader->DebugString();

  // Wait until a block is read, all blocks have been read and returned or 
  // reader is cancelled
  while (reader->ready_buffers_.empty() && reader->state_ == ReaderContext::Active) {
    VLOG_ROW << "GetNext: Waiting " << reader;
    reader->buffer_ready_cond_var_.wait(lock);
  }
  
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  if (reader->state_ == ReaderContext::Cancelled) return Status::CANCELLED;

  // Remove the first read block from the queue and return it
  DCHECK(!reader->ready_buffers_.empty());
  *buffer = reader->ready_buffers_.front();
  reader->ready_buffers_.pop_front();

  RETURN_IF_ERROR((*buffer)->status_);

  *eos = false;
  
  if ((*buffer)->eosr_) {
    --reader->num_remaining_scan_ranges_;
    DCHECK_GE(reader->num_remaining_scan_ranges_, 0);
    if (reader->num_remaining_scan_ranges_ == 0) {
      *eos = true;
      // All scan ranges complete, notify all other threads on this reader currently
      // in GetNext()  
      // TODO: is it clearer if the disk thread does this?
      reader->buffer_ready_cond_var_.notify_all();
    }
  }

  ++reader->num_outstanding_buffers_;
  DCHECK((*buffer)->buffer_ != NULL);
  return Status::OK;
}

Status DiskIoMgr::TryGetNext(ReaderContext* reader, 
    BufferDescriptor** buffer, bool* eos) {
  {
    unique_lock<mutex> lock(reader->lock_);
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    if (reader->state_ == ReaderContext::Cancelled) return Status::CANCELLED;
    if (reader->num_used_buffers_ >= reader->num_buffers_quota_
        && reader->ready_buffers_.empty()) {
      // No empty buffers to read into and none are ready.
      *eos = false;
      *buffer = NULL;
      return Status::OK;
    }
  }

  // Now it is safe to call get next.
  return GetNext(reader, buffer, eos);
}

void DiskIoMgr::ReturnBuffer(BufferDescriptor* buffer_desc) {
  DCHECK(buffer_desc != NULL);
  if (!buffer_desc->status_.ok()) DCHECK(buffer_desc->buffer_ == NULL);

  // Null buffer meant there was an error or GetNext was called after eos.  Protect
  // against returning those buffers.
  if (buffer_desc->buffer_ == NULL) {
    ReturnBufferDesc(buffer_desc);
    return;
  }

  ReturnFreeBuffer(buffer_desc->buffer_);
  buffer_desc->buffer_ = NULL;
  
  ReaderContext* reader = buffer_desc->reader_;
  ReturnBufferDesc(buffer_desc);

  if (reader == NULL) {
    // Null reader indicates this was a synchronous reader.  There is no more work
    // to be done for this reader.
    return;
  }

  {
    unique_lock<mutex> reader_lock(reader->lock_);
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    DCHECK_GT(reader->num_used_buffers_, 0) << reader->DebugString();
    --reader->num_used_buffers_;
    --reader->num_outstanding_buffers_;
    // At this point we just got a buffer back meaning we might want to enqueue a
    // new disk.
    reader->ScheduleNewDisks(1);
  }
}

void DiskIoMgr::ReturnBufferDesc(BufferDescriptor* desc) {
  DCHECK(desc != NULL);
  unique_lock<mutex> lock(free_buffers_lock_);
  free_buffer_descs_.push_back(desc);
}

DiskIoMgr::BufferDescriptor* DiskIoMgr::GetBufferDesc(
    ReaderContext* reader, ScanRange* range, char* buffer) {
  BufferDescriptor* buffer_desc;
  {
    unique_lock<mutex> lock(free_buffers_lock_);
    if (free_buffer_descs_.empty()) {
      buffer_desc = pool_.Add(new BufferDescriptor(this));
    } else {
      buffer_desc = free_buffer_descs_.front();
      free_buffer_descs_.pop_front();
    }
  }
  buffer_desc->Reset(reader, range, buffer);
  return buffer_desc;
}

char* DiskIoMgr::GetFreeBuffer() {
  unique_lock<mutex> lock(free_buffers_lock_);
  if (free_buffers_.empty()) {
    ++num_allocated_buffers_;
    if (ImpaladMetrics::IO_MGR_NUM_BUFFERS != NULL) {
      ImpaladMetrics::IO_MGR_NUM_BUFFERS->Increment(1L);
    }
    return new char[max_read_size_];
  } else {
    char* buffer = free_buffers_.front();
    free_buffers_.pop_front();
    return buffer;
  }
}

void DiskIoMgr::ReturnFreeBuffer(char* buffer) {
  DCHECK(buffer != NULL);
  unique_lock<mutex> lock(free_buffers_lock_);
  free_buffers_.push_back(buffer);
}
  
bool DiskIoMgr::ReaderContext::Validate() const {
  if (state_ == ReaderContext::Inactive) return false;

  int num_disks_with_ranges = 0;
  int num_reading_threads = 0;

  if (num_remaining_scan_ranges_ == 0 && !ready_buffers_.empty()) return false;
  
  for (int i = 0; i < disk_states_.size(); ++i) {
    const PerDiskState& state = disk_states_[i];
    num_reading_threads += state.num_threads_in_read;

    if (state.num_remaining_scan_ranges > 0) ++num_disks_with_ranges;
    if (state.num_threads_in_read < 0) return false;
    
    // If there are no more ranges, there should be at most 1 thread working
    // for this reader for the final cleanup
    if (num_disks_with_ranges_ == 0 && state.num_threads_in_read > 1) return false;

    if (state_ != ReaderContext::Cancelled) {
      if (state.unscheduled_ranges.size() + state.in_flight_ranges.size() >
          state.num_remaining_scan_ranges) {
        return false;
      }
    }
  }

  int num_ok_ready_buffers = 0;

  for (list<BufferDescriptor*>::const_iterator it = ready_buffers_.begin();
      it != ready_buffers_.end(); ++it) {
    if ((*it)->buffer_ != NULL) ++num_ok_ready_buffers;
  }

  // Buffers either need to be unused (empty) or ready (num_ok_ready_buffers) or
  // being read (num_reading_threads) or owned by the reader (unaccounted)
  // TODO: keep track of that as well
  if (num_used_buffers_ < 0) return false;
  if (num_used_buffers_ < num_outstanding_buffers_) return false;
  if (num_disks_with_ranges_ == 0 && num_reading_threads > 1) return false;

  return true;
}
  
// Dumps out reader information.  Lock should be taken by caller
string DiskIoMgr::ReaderContext::DebugString() const {
  stringstream ss;
  ss << endl << "  Reader: " << (void*)this << " (state=";
  if (state_ == ReaderContext::Inactive) ss << "Inactive";
  if (state_ == ReaderContext::Cancelled) ss << "Cancelled";
  if (state_ == ReaderContext::Active) ss << "Active";
  if (state_ != ReaderContext::Inactive) {
    ss << " sync_reader=" << (sync_reader_ ? "true" : "false")
        << " max_parallel_scan_ranges=" << max_parallel_scan_ranges_
        << " num_in_flight_ranges=" << num_in_flight_ranges_
        << " buffer_quota=" << num_buffers_quota_
        << " #ready_buffers=" << ready_buffers_.size()
        << " #scan_ranges=" << num_remaining_scan_ranges_
        << " #used_buffers=" << num_used_buffers_
        << " #outstanding_buffers=" << num_outstanding_buffers_
        << " #remaining_scan_ranges=" << num_remaining_scan_ranges_
        << " #disk_with_ranges=" << num_disks_with_ranges_
        << " #disks=" << num_disks_with_ranges_;
    for (int i = 0; i < disk_states_.size(); ++i) {
      ss << endl << "   " << i << ": "
          << "is_on_queue=" << disk_states_[i].is_on_queue
          << " done=" << disk_states_[i].done
          << " #num_remaining_scan_ranges=" << disk_states_[i].num_remaining_scan_ranges
          << " #in_flight_ranges=" << disk_states_[i].in_flight_ranges.size()
          << " #unscheduled_ranges=" << disk_states_[i].unscheduled_ranges.size()
          << " #reading_threads=" << disk_states_[i].num_threads_in_read;
    }
    if (!unscheduled_disks_.empty()) {
      ss << endl << "   Unscheduled disks: ";
      for (list<DiskQueue*>::const_iterator it = unscheduled_disks_.begin();
          it != unscheduled_disks_.end(); ++it) {
        ss << " " << (*it)->disk_id;
      }
    }
  }
  ss << ")";
  return ss.str();
}

string DiskIoMgr::DebugString() {
  stringstream ss;
  ss << "Readers: " << endl << reader_cache_->DebugString() << endl;

  ss << "Disks: " << endl;
  for (int i = 0; i < disk_queues_.size(); ++i) {
    unique_lock<mutex> lock(disk_queues_[i]->lock);
    ss << "  " << (void*) disk_queues_[i] << ":" ;
    if (!disk_queues_[i]->readers.empty()) {
      ss << " Readers: ";
      for (list<ReaderContext*>::iterator it = disk_queues_[i]->readers.begin();
          it != disk_queues_[i]->readers.end(); ++it) {
        ss << (void*)*it;
      }
    }
    ss << endl;
  }
  return ss.str();
}

Status DiskIoMgr::OpenScanRange(hdfsFS hdfs_connection, ScanRange* range) const {
  if (hdfs_connection != NULL) {
    if (range->hdfs_file_ != NULL) return Status::OK;

    // TODO: is there much overhead opening hdfs files?  Should we try to preserve
    // the handle across multiple scan ranges of a file?
    range->hdfs_file_ = 
        hdfsOpenFile(hdfs_connection, range->file_, O_RDONLY, 0, 0, 0);
    if (range->hdfs_file_ == NULL) {
      return Status(AppendHdfsErrorMessage("Failed to open HDFS file ", range->file_));
    }

    if (hdfsSeek(hdfs_connection, range->hdfs_file_, range->offset_) != 0) {
      stringstream ss;
      ss << "Error seeking to " << range->offset_ << " in file: " << range->file_;
      return Status(AppendHdfsErrorMessage(ss.str()));
    }
  } else {
    if (range->local_file_ != NULL) return Status::OK;

    range->local_file_ = fopen(range->file_, "r");
    if (range->local_file_ == NULL) {
      stringstream ss;
      ss << "Could not open file: " << range->file_ << ": " << strerror(errno);
      return Status(ss.str());
    }
    if (fseek(range->local_file_, range->offset_, SEEK_SET) == -1) {
      stringstream ss;
      ss << "Could not seek to " << range->offset_ << " for file: " << range->file_
         << ": " << strerror(errno);
      return Status(ss.str());
    }
  } 
  if (ImpaladMetrics::IO_MGR_NUM_OPEN_FILES != NULL) {
    ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(1L);
  }
  return Status::OK;
}

void DiskIoMgr::CloseScanRange(hdfsFS hdfs_connection, ScanRange* range) const {
  if (range == NULL) return;
 
  if (hdfs_connection != NULL) {
    if (range->hdfs_file_ == NULL) return;
    hdfsCloseFile(hdfs_connection, range->hdfs_file_);
    range->hdfs_file_ = NULL;
  } else {
    if (range->local_file_ == NULL) return;
    fclose(range->local_file_);
    range->local_file_ = NULL;
  }
  if (ImpaladMetrics::IO_MGR_NUM_OPEN_FILES != NULL) {
    ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(-1L);
  }
}

// TODO: how do we best use the disk here.  e.g. is it good to break up a
// 1MB read into 8 128K reads?
// TODO: look at linux disk scheduling
Status DiskIoMgr::ReadFromScanRange(hdfsFS hdfs_connection, ScanRange* range, 
    char* buffer, int64_t* bytes_read, bool* eosr) {
  *eosr = false;
  *bytes_read = 0;
  int bytes_to_read = min(static_cast<int64_t>(max_read_size_), 
      range->len_ - range->bytes_read_);

  if (hdfs_connection != NULL) {
    DCHECK(range->hdfs_file_ != NULL);
    // TODO: why is this loop necessary? Can hdfs reads come up short?
    while (*bytes_read < bytes_to_read) {
      int last_read = hdfsRead(hdfs_connection, range->hdfs_file_,
          buffer + *bytes_read, bytes_to_read - *bytes_read);
      if (last_read == -1) {
        return Status(
            AppendHdfsErrorMessage("Error reading from HDFS file: ", range->file_));
      } else if (last_read == 0) {
        // No more bytes in the file.  The scan range went past the end
        *eosr = true;
        break;
      }
      *bytes_read += last_read;
    }
  } else {
    DCHECK(range->local_file_ != NULL);
    *bytes_read = fread(buffer, 1, bytes_to_read, range->local_file_);
    if (*bytes_read < 0) {
      stringstream ss;
      ss << "Could not read from " << range->file_ << " at byte offset: " 
         << range->bytes_read_ << ": " << strerror(errno);
      return Status(ss.str());
    }
  }
  range->bytes_read_ += *bytes_read;
  DCHECK_LE(range->bytes_read_, range->len_);
  if (range->bytes_read_ == range->len_) {
    *eosr = true;
  }
  return Status::OK;
}

inline bool DiskIoMgr::ReaderContext::GetScanRange(int disk_id, ScanRange** range, 
    char** buffer) {
  DCHECK(Validate()) << endl << DebugString();
  DCHECK_EQ(state_, Active);

  PerDiskState& state = disk_states_[disk_id];

  if (state.num_remaining_scan_ranges == 0) {
    // In this case, there is no more work on this disk.
    DecrementDiskRefCount();
    state.done = true;
    ScheduleNewDisks(1);
    return false;
  }

  if (num_used_buffers_ >= num_buffers_quota_) {
    // This reader has no more buffers, it can't do any more work now.
    EnqueueUnscheduledDisk(parent_->disk_queues_[disk_id]);
    return false;
  }

  if (num_in_flight_ranges_ < max_parallel_scan_ranges_ && 
      !state.unscheduled_ranges.empty()) {
    // We have not start enough ranges for this reader, start a new one.
    // The number of parallel ranges dictates the number of cores that
    // can be used to process the bytes (1 range == 1 thread).  Therefore,
    // we want to have enough ranges in parallel going to use all the cores.
    // We don't want too many though, since that causes excessive resource
    // utilization and thrashing.
    *range = state.unscheduled_ranges.front();
    state.unscheduled_ranges.pop_front();
    ++num_in_flight_ranges_;
  } else if (state.in_flight_ranges.empty()) {
    // In this case, we don't have enough quota to kick off a new range.
    // or all scan ranges are already being processed by other threads.
    // We'll have to wait until we can read a new range.
    ScheduleNewDisks(1);
    EnqueueUnscheduledDisk(parent_->disk_queues_[disk_id]);
    return false;
  } else {
    DCHECK(!state.in_flight_ranges.empty());
    *range = state.in_flight_ranges.front();
    state.in_flight_ranges.pop_front();
  }

  // Get a free buffer from the disk io mgr.  It's a global pool for all readers but
  // they are lazily allocated.  Each reader is guaranteed its share.
  *buffer = parent_->GetFreeBuffer();
  DCHECK(*buffer != NULL);
  ++num_used_buffers_;

  return true;
}

// This functions gets the next scan range to work on. 
//  - wait until there is a reader with work and available buffer or the thread should
//    terminate.  
//  - Remove the scan range, available buffer and cycle to the next reader
// There are a few guarantees this makes which causes some complications.
//  1) Readers are round-robined. 
//  2) Multiple threads (including per disk) can work on the same reader.
//  3) Scan ranges within a reader are round-robined.
bool DiskIoMgr::GetNextScanRange(DiskQueue* disk_queue, ScanRange** range, 
    ReaderContext** reader, char** buffer) {
  int disk_id = disk_queue->disk_id;

  // This loops returns either with work to do or when the disk io mgr shuts down.
  while (true) {
    *reader = NULL;
    ReaderContext::PerDiskState* state = NULL;
    {
      unique_lock<mutex> disk_lock(disk_queue->lock);

      while (!shut_down_ && disk_queue->readers.empty()) {
        // wait if there are no readers on the queue
        disk_queue->work_available.wait(disk_lock);
      }
      if (shut_down_) break;
      DCHECK(!disk_queue->readers.empty());

      // Get the next reader and remove the reader so that another disk thread 
      // can't pick it up.  It will be enqueued before issuing the read to HDFS 
      // so this is not a big deal (i.e. multiple disk threads can read for the
      // same reader).
      *reader = disk_queue->readers.front();
      DCHECK(*reader != NULL);
      state = &((*reader)->disk_states_[disk_id]);

      // Increment the ref count on reader.  We need to track the number of threads per
      // reader per disk that is the in unlocked hdfs read code section.
      // We don't have the reader lock so we'll use an atomic increment
      __sync_add_and_fetch(&state->num_threads_in_read, 1);
      __sync_synchronize();
      
      disk_queue->readers.pop_front();
      state->is_on_queue = false;
    }

    // NOTE: no locks were taken in between.  We need to be careful about what state
    // could have changed to the reader and disk in between.
    // There are some invariants here.  Only one disk thread can have the
    // same reader here (the reader is removed from the queue).  There can be
    // other disk threads operating on this reader in other functions though.

    unique_lock<mutex> reader_lock((*reader)->lock_);
    VLOG_FILE << "Disk (id=" << disk_id << ") reading for " << (*reader)->DebugString();
    
    // Check if reader has been cancelled
    if ((*reader)->state_ == ReaderContext::Cancelled) {
      --state->num_threads_in_read;
      if (state->num_threads_in_read == 0) {
        state->num_remaining_scan_ranges = 0;
        (*reader)->DecrementDiskRefCount();
        state->done = true;
      }
      continue;
    }

    // Validate invariants.  The reader should be active and think it is on this
    // disk queue.
    DCHECK_EQ((*reader)->state_, ReaderContext::Active);

    // Ask the reader for the next scan range to work on.
    bool got_range = (*reader)->GetScanRange(disk_id, range, buffer);

    if (!got_range) {
      DCHECK(!state->is_on_queue);
      // Nothing to do for this reader
      --state->num_threads_in_read;
      continue;
    }

    // Validate more invariants.  
    DCHECK_GE((*reader)->num_used_buffers_, 0);

    // Now that we've picked a scan range, put the reader back on the queue so
    // another thread can pick up another scan range for this reader.
    unique_lock<mutex> disk_lock(disk_queue->lock);
    disk_queue->readers.push_back(*reader);
    state->is_on_queue = true;
    
    return true;
  }

  DCHECK(shut_down_);
  return false;
}

void DiskIoMgr::HandleReadFinished(DiskQueue* disk_queue, ReaderContext* reader,
    BufferDescriptor* buffer) {
  {
    unique_lock<mutex> reader_lock(reader->lock_);

    ReaderContext::PerDiskState& state = reader->disk_states_[disk_queue->disk_id];
    
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    DCHECK_GT(state.num_threads_in_read, 0);
    DCHECK(buffer->buffer_ != NULL);

    // This variable is updated in the disk thread (GetScanRange) without the
    // reader lock.  Therefore, we need to use an atomic update.
    __sync_add_and_fetch(&state.num_threads_in_read, -1);

    if (reader->state_ == ReaderContext::Cancelled) {
      CloseScanRange(reader->hdfs_connection_, buffer->scan_range_);
      ReturnFreeBuffer(buffer->buffer_);
      buffer->buffer_ = NULL;
      ReturnBufferDesc(buffer);
      --reader->num_used_buffers_;

      if (!state.is_on_queue && state.num_threads_in_read == 0) {
        // This thread is the last one for this reader on this disk, do final
        // cleanup
        reader->DecrementDiskRefCount();
        state.done = true;
      }
      return;
    }
        
    DCHECK_EQ(reader->state_, ReaderContext::Active);
    DCHECK(buffer->buffer_ != NULL);

    // Update the reader's scan ranges.  There are a three cases here:
    //  1. Read error
    //  2. End of scan range
    //  3. Middle of scan range
    if (!buffer->status_.ok()) {
      // Error case
      CloseScanRange(reader->hdfs_connection_, buffer->scan_range_);
      --reader->num_used_buffers_;
      ReturnFreeBuffer(buffer->buffer_);
      buffer->buffer_ = NULL;
      buffer->eosr_ = true;
      --state.num_remaining_scan_ranges;
      --reader->num_in_flight_ranges_;
    } else {
      if (buffer->eosr_) {
        CloseScanRange(reader->hdfs_connection_, buffer->scan_range_);
        --state.num_remaining_scan_ranges;
        --reader->num_in_flight_ranges_;
      } else {
        DCHECK_LT(buffer->scan_range_->bytes_read_, buffer->scan_range_->len_);
        state.in_flight_ranges.push_back(buffer->scan_range_);
      }
    }

    // Add the result to the reader's queue and notify the reader
    reader->ready_buffers_.push_back(buffer);
  }
  reader->buffer_ready_cond_var_.notify_one();
}

// The thread waits until there is both work (which is guaranteed to show up with an
// empty buffer) or the entire system is being shut down.  If there is work, it reads the 
// next chunk of the next scan range for the first reader in the queue and round robins 
// across the readers.
// Locks are not taken when reading from disk.  The main loop has three parts:
//   1. GetNextScanRange(): Take locks and figure out what the next scan range to read is
//   2. Open/Read the scan range.  No locks are taken
//   3. HandleReadFinished(): Take locks and update the disk and reader with the 
//      results of the io.
// Cancellation checking needs to happen in both steps 1 and 3.
void DiskIoMgr::ReadLoop(DiskQueue* disk_queue) {
  while (true) {
    char* buffer = NULL;
    ReaderContext* reader = NULL;;
    ScanRange* range = NULL;
    
    // Get the next scan range to read
    if (!GetNextScanRange(disk_queue, &range, &reader, &buffer)) {
      DCHECK(shut_down_);
      break;
    }
    DCHECK(range != NULL);
    DCHECK(reader != NULL);
    DCHECK(buffer != NULL);

    BufferDescriptor* buffer_desc = GetBufferDesc(reader, range, buffer);
    DCHECK(buffer_desc != NULL);

    // No locks in this section.  Only working on local vars.  We don't want to hold a 
    // lock across the read call.
    buffer_desc->status_ = OpenScanRange(reader->hdfs_connection_, range);
    if (buffer_desc->status_.ok()) {
      // Update counters.
      SCOPED_TIMER(&read_timer_);
      SCOPED_TIMER(reader->read_timer_);
      
      buffer_desc->status_ = ReadFromScanRange(
          reader->hdfs_connection_, range, buffer, &buffer_desc->len_,
          &buffer_desc->eosr_);
      buffer_desc->scan_range_offset_ = range->bytes_read_ - buffer_desc->len_;
    
      if (reader->bytes_read_counter_ != NULL) {
        COUNTER_UPDATE(reader->bytes_read_counter_, buffer_desc->len_);
      }
      COUNTER_UPDATE(&total_bytes_read_counter_, buffer_desc->len_);
    }

    // Finished read, update reader/disk based on the results
    HandleReadFinished(disk_queue, reader, buffer_desc);
  } 

  DCHECK(shut_down_);
}

