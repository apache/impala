// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "runtime/disk-io-mgr.h"

#include <queue>
#include <boost/functional/hash.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/locks.hpp>

#include "common/logging.h"
#include "util/disk-info.h"
#include "util/hdfs-util.h"

// Default IO Mgr configs
DEFINE_int32(max_io_buffers, 10, "number of io buffers per disk");
DEFINE_int32(num_threads_per_disk, 3, "number of threads per disk");
DEFINE_int32(num_disks, 0, "Number of disks on data node.");
DEFINE_int32(read_size, 1024 * 1024, "Read Size (in bytes)");

using namespace boost;
using namespace impala;
using namespace std;

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
  
  // Total bytes read for this reader
  RuntimeProfile::Counter* bytes_read_counter_;

  // Total time spent in hdfs reading
  RuntimeProfile::Counter* read_timer_;

  // hdfsFS connection handle.  This is set once and never changed for the duration 
  // of the reader.  NULL if this is a local reader.
  hdfsFS hdfs_connection_;

  // The number of io buffers this reader has per disk.  This is constant for a single
  // reader.
  int num_buffers_per_disk_;

  // Condition variable for GetNext
  condition_variable buffer_ready_cond_var_;

  // Condition variable for UnregisterReader to wait for all disks to complete
  condition_variable disks_complete_cond_var_;

  // All fields below are accessed by multiple threads and the lock needs to be
  // taken before accessing them.
  mutex lock_;
  
  // Current state of the reader
  State state_;

  // The number of buffers allocated for this reader that are empty.  
  int num_empty_buffers_;

  // The number of buffers currently owned by the reader
  int num_outstanding_buffers_;

  // The total number of scan ranges that have not been returned via GetNext().
  int num_remaining_scan_ranges_;

  // list of buffers that have been read from disk but not returned to the reader.
  // Also contains dummy buffers (just an error code)  signaling failed reads.  
  // In the non-error state, ready_buffers_.size() + num_empty_buffers_ 
  // will be the total number of buffers allocated for this reader.
  list<BufferDescriptor*> ready_buffers_;
  
  // The number of disks with scan ranges remaining (always equal to the sum of
  // non-empty ranges in per disk states).
  int num_disks_with_ranges_;

  // If true, this is a reader from Read()
  bool sync_reader_;

  // Struct containing state per disk. See comments in the disk read loop on how 
  // they are used.
  struct PerDiskState {
    // For each disks, the number of scan ranges that have not been fully read.
    int num_scan_ranges;
  
    // Queue of scan ranges that can be scheduled.  A scan range that is currently being
    // read by one thread, cannot be picked up by another thread and is temporarily
    // removed from the queue.  The size of this queue is always less than or equal
    // to num_scan_ranges. 
    // TODO: this is simpler to get right but not optimal.  We do not parallelize a
    // read from a single scan range.  Parallelizing a scan range requires more logic
    // to make sure the reader gets the scan range buffers in order.  We should do this.
    list<ScanRange*> ranges;

    // For each disk, keeps track if the reader is on that disk's queue.  This saves
    // us from having to walk the consumer list on the common path.
    // TODO: we should instead maintain the per queue double-linked links within the 
    // ReaderContext struct instead of using the stl.
    bool is_on_queue;

    // For each disk, the number of threads issuing the underlying read on behalf of
    // this reader. These threads do not have any locks taken and need to be accounted 
    // for during cancellation.  We need to make sure that their resources are not 
    // cleaned up while they are still in progress.  Only the thread that sees the count 
    // at 0 should do the final cleanup.
    int num_threads_in_read;

    // The number of empty buffers on this disk
    int num_empty_buffers;

    PerDiskState() {
      Reset(0);
    }

    void Reset(int per_disk_buffers) {
      num_scan_ranges = 0;
      ranges.clear();
      is_on_queue = false;
      num_threads_in_read = 0;
      num_empty_buffers = per_disk_buffers;
    }
  };

  // Per disk states to synchronize multiple disk threads accessing the same reader.
  vector<PerDiskState> disk_states_;

  ReaderContext(int num_disks) 
    : bytes_read_counter_(NULL),
      read_timer_(NULL),
      state_(Inactive),
      disk_states_(num_disks) {
  }

  // Resets this object for a new reader
  void Reset(hdfsFS hdfs_connection, int per_disk_buffers) {
    DCHECK_EQ(state_, Inactive);

    bytes_read_counter_ = NULL;
    read_timer_ = NULL;

    state_ = Active;
    num_buffers_per_disk_ = per_disk_buffers;
    hdfs_connection_ = hdfs_connection;
    num_remaining_scan_ranges_ = 0;
    num_disks_with_ranges_ = 0;

    for (int i = 0; i < disk_states_.size(); ++i) {
      disk_states_[i].Reset(per_disk_buffers);
    }

    num_empty_buffers_ = per_disk_buffers * disk_states_.size();
    num_outstanding_buffers_ = 0;
    sync_reader_ = false;
  }

  // Validates invariants of reader.  Reader lock should be taken before hand.
  bool Validate() {
    if (state_ == ReaderContext::Inactive) return false;

    int num_disks_with_ranges = 0;
    int num_empty_buffers = 0;
    int num_reading_threads = 0;

    if (num_remaining_scan_ranges_ == 0 && !ready_buffers_.empty()) return false;
    
    for (int i = 0; i < disk_states_.size(); ++i) {
      PerDiskState& state = disk_states_[i];
      num_empty_buffers += state.num_empty_buffers;
      num_reading_threads += state.num_threads_in_read;

      if (state.num_scan_ranges > 0) ++num_disks_with_ranges;
      if (state.num_threads_in_read < 0) return false;
      if (state.num_empty_buffers < 0) return false;
      if (state.num_empty_buffers > num_buffers_per_disk_) return false;
      
      // If there are no more ranges, no threads should be reading
      if (num_disks_with_ranges_ == 0 && state.num_threads_in_read != 0) return false;
    }
    int num_ok_ready_buffers = 0;

    for (list<BufferDescriptor*>::iterator it = ready_buffers_.begin();
        it != ready_buffers_.end(); ++it) {
      if ((*it)->buffer_ != NULL) ++num_ok_ready_buffers;
    }
      
    // Buffers either need to be unused (empty) or read (num_ok_ready_buffers) or
    // being read (num_reading_threads) or owned by the reader (unaccounted)
    // TODO: keep track of that as well
    // That should never be more than the allocated empty buffers for the reader
    if (num_empty_buffers_ + num_ok_ready_buffers + num_reading_threads > 
          num_buffers_per_disk_ * disk_states_.size()) {
      return false;
    }
    
    if (num_empty_buffers != num_empty_buffers_) return false;
    if (num_disks_with_ranges_ == 0 && num_reading_threads > 0) return false;
    if (num_disks_with_ranges != num_disks_with_ranges_) return false;

    return true;
  }

  // Dumps out reader information.  Lock should be taken by caller
  string DebugString() const {
    stringstream ss;
    ss << "  Reader: " << (void*)this << " (state=";
    if (state_ == ReaderContext::Inactive) ss << "Inactive";
    if (state_ == ReaderContext::Cancelled) ss << "Cancelled";
    if (state_ == ReaderContext::Active) ss << "Active";
    if (state_ != ReaderContext::Inactive) {
      ss << " #buffers_per_disk=" << num_buffers_per_disk_;
      ss << " #ready_buffers=" << ready_buffers_.size();
      ss << " #scan_ranges=" << num_remaining_scan_ranges_;
      ss << " #empty_buffers=" << num_empty_buffers_;
      ss << " #outstanding_buffers=" << num_outstanding_buffers_;
      ss << " #remaining_scan_ranges=" << num_remaining_scan_ranges_;
      ss << " #disk_with_ranges=" << num_disks_with_ranges_;
      ss << " #disks=" << num_disks_with_ranges_;
      for (int i = 0; i < disk_states_.size(); ++i) {
        ss << endl << "    " << i << ": ";
        ss << "is_on_queue=" << disk_states_[i].is_on_queue
            << " #scan_ranges=" << disk_states_[i].num_scan_ranges
            << " #reading_threads=" << disk_states_[i].num_threads_in_read
            << " #empty_buffers=" << disk_states_[i].num_empty_buffers;
      }
    }
    ss << ")";
    return ss.str();
  }
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
      ReaderContext* reader = new ReaderContext(io_mgr_->num_disks());
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

// Per disk state
struct DiskIoMgr::DiskQueue {
  // Disk id (0-based)
  int disk_id;

  // Condition variable to signal the disk threads that there is work to do or the
  // thread should shut down.  A disk thread will be woken up when there is a reader
  // added to the queue.  Readers are only on the queue when they have both work
  // (scan range added) and an available buffer.
  condition_variable work_available;
  
  // Lock that protects access to 'readers'
  mutex lock;

  // list of all readers that have work queued on this disk and empty buffers to read
  // into.
  list<ReaderContext*> readers;

  // The reader at the head of the queue that will get their io issued next
  list<ReaderContext*>::iterator next_reader;

  DiskQueue(int id) : disk_id(id) {
    next_reader = readers.end();
  }
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
    read_timer_(TCounterType::CPU_TICKS),
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
    read_timer_(TCounterType::CPU_TICKS),
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
      DecrementDiskRefCount(*reader);
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

Status DiskIoMgr::RegisterReader(hdfsFS hdfs, int num_buffers_per_disk, 
    ReaderContext** reader) {
  DCHECK(reader_cache_.get() != NULL) << "Must call Init() first.";
  *reader = reader_cache_->GetNewReader();
  (*reader)->Reset(hdfs, num_buffers_per_disk);
  return Status::OK;
}

void DiskIoMgr::UnregisterReader(ReaderContext* reader) {
  // First cancel the reader.  This is more or less a no-op if the reader is
  // complete (common case).
  CancelReader(reader);
  
  unique_lock<mutex> lock(reader->lock_);
  DCHECK_EQ(reader->num_outstanding_buffers_, 0);
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  DCHECK(reader->ready_buffers_.empty());
  while (reader->num_disks_with_ranges_ > 0) {
    reader->disks_complete_cond_var_.wait(lock);
  }
  if (!reader->sync_reader_) {
    DCHECK_EQ(reader->num_empty_buffers_, 
        reader->num_buffers_per_disk_ * disk_queues_.size());
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

int DiskIoMgr::num_empty_buffers(ReaderContext* reader) {
  unique_lock<mutex> (reader->lock_);
  return reader->num_empty_buffers_;
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

Status DiskIoMgr::AddScanRanges(ReaderContext* reader, const vector<ScanRange*>& ranges) {
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
  vector<DiskQueue*> disks_to_add;

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
      if (reader->disk_states_[range->disk_id_].num_scan_ranges == 0) {
        ++reader->num_disks_with_ranges_;
        DCHECK(!reader->disk_states_[range->disk_id_].is_on_queue);
        if (reader->disk_states_[range->disk_id_].num_empty_buffers > 0) {
          reader->disk_states_[range->disk_id_].is_on_queue = true;
          disks_to_add.push_back(disk_queues_[range->disk_id_]);
        }
      }
      reader->disk_states_[range->disk_id_].ranges.push_back(range);
      ++reader->disk_states_[range->disk_id_].num_scan_ranges;
    }
    reader->num_remaining_scan_ranges_ += ranges.size();
    DCHECK(reader->Validate()) << endl << reader->DebugString();
  }

  // This requires taking a disk lock, reader lock must not be taken
  for (int i = 0; i < disks_to_add.size(); ++i) {
    {
      unique_lock<mutex> lock(disks_to_add[i]->lock);
      disks_to_add[i]->readers.push_back(reader);
    }
    disks_to_add[i]->work_available.notify_all();
  }

  return Status::OK;
}

Status DiskIoMgr::Read(hdfsFS hdfs, ScanRange* range, BufferDescriptor** buffer) {
  ReaderContext* local_context = NULL;
  // Make a local context for doing the synchronous read
  // Local reader always has one buffer.  Since this is synchronous, it can't use
  // the same resource pool as the parent reader.
  // TODO: more careful resource management?  We're okay as long as one 
  // sync reader is able to get through (otherwise we can deadlock).
  RETURN_IF_ERROR(RegisterReader(hdfs, 1, &local_context));
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

  // TODO: demote this to VLOG_FILE after we caught the deadlock
  VLOG_QUERY << "GetNext(): reader=" << reader->DebugString();

  // Wait until a block is read, all blocks have been read and returned or 
  // reader is cancelled
  while (reader->ready_buffers_.empty() && reader->state_ == ReaderContext::Active) {
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

void DiskIoMgr::ReturnBuffer(BufferDescriptor* buffer_desc) {
  DCHECK(buffer_desc != NULL);
  if (!buffer_desc->status_.ok()) {
    DCHECK(buffer_desc->buffer_ == NULL);
  }

  // Null buffer meant there was an error or GetNext was called after eos.  Protect
  // against returning those buffers.
  if (buffer_desc->buffer_ != NULL) {
    ReturnFreeBuffer(buffer_desc->buffer_);
    buffer_desc->buffer_ = NULL;

    ReaderContext* reader = buffer_desc->reader_;
    if (reader != NULL) {
      DCHECK(buffer_desc->reader_ != NULL);
      int disk_id = buffer_desc->scan_range_->disk_id();
      DiskQueue* disk_queue = disk_queues_[disk_id];
      
      unique_lock<mutex> disk_lock(disk_queue->lock);
      unique_lock<mutex> reader_lock(reader->lock_);

      DCHECK(reader->Validate()) << endl << reader->DebugString();
      ReaderContext::PerDiskState& state = reader->disk_states_[disk_id];

      ++reader->num_empty_buffers_;
      ++state.num_empty_buffers;
      --reader->num_outstanding_buffers_;
      
      // Add the reader back on the disk queue if necessary.  We want to add the reader
      // on the queue if these conditions are true:
      //  1. it is not already on the queue
      //  2. it is not been completely processed for this disk 
      //  3a. it is cancelled and there are no threads currently processing it
      //  3b. or, it has schedulable scan ranges.
      if (!state.is_on_queue && state.num_scan_ranges > 0 &&
          ((reader->state_ == ReaderContext::Cancelled && state.num_threads_in_read == 0) 
          || !state.ranges.empty())) {
        reader->disk_states_[disk_id].is_on_queue = true;
        disk_queue->readers.push_back(reader);
        disk_queue->work_available.notify_one();
      }
    } 
  }

  ReturnBufferDesc(buffer_desc);
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

void DiskIoMgr::DecrementDiskRefCount(ReaderContext* reader) {
  // boost doesn't let us dcheck that the the reader lock is taken
  DCHECK_GT(reader->num_disks_with_ranges_, 0);
  if (--reader->num_disks_with_ranges_ == 0) {
    reader->disks_complete_cond_var_.notify_one(); 
  }
  DCHECK(reader->Validate()) << endl << reader->DebugString();
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

void DiskIoMgr::RemoveReaderFromDiskQueue(DiskQueue* disk_queue, ReaderContext* reader) {
  DCHECK(reader->disk_states_[disk_queue->disk_id].is_on_queue);
  reader->disk_states_[disk_queue->disk_id].is_on_queue = false;
  list<ReaderContext*>::iterator it = disk_queue->readers.begin();
  for (; it != disk_queue->readers.end(); ++it) {
    if (*it == reader) {
      if (it == disk_queue->next_reader) ++disk_queue->next_reader;
      disk_queue->readers.erase(it);
      return;
    }
  }
  DCHECK(false) << "Reader was not on the queue";
}

// This functions gets the next scan range to work on. 
//  - wait until there is a reader with work and available buffer or the thread should
//    terminate.  Note: the disk's reader queue only contains readers that have both work
//    and buffers so this thread does not have to busy spin on readers.
//  - Remove the scan range, available buffer and cycle to the next reader
// There are a few guarantees this makes which causes some complications.
//  1) Readers are round-robined. 
//  2) Multiple threads (including per disk) can work on the same reader.
//  3) Scan ranges within a reader are round-robined.
bool DiskIoMgr::GetNextScanRange(DiskQueue* disk_queue, ScanRange** range, 
    ReaderContext** reader, char** buffer) {
  // This loops returns either with work to do or when the disk io mgr shuts down.
  while (true) {
    unique_lock<mutex> disk_lock(disk_queue->lock);

    while (!shut_down_ && disk_queue->readers.empty()) {
      // wait if there are no readers on the queue
      disk_queue->work_available.wait(disk_lock);
    }
    if (shut_down_) break;
    DCHECK(!disk_queue->readers.empty());

    // Get reader and advance to the next reader
    if (disk_queue->next_reader == disk_queue->readers.end()) {
      disk_queue->next_reader = disk_queue->readers.begin();
    }
    DCHECK(disk_queue->next_reader != disk_queue->readers.end());

    list<ReaderContext*>::iterator reader_it = disk_queue->next_reader;
    ++disk_queue->next_reader;
    *reader = *reader_it;
    
    // Grab reader lock, both locks are held now
    unique_lock<mutex> reader_lock((*reader)->lock_);
    DCHECK((*reader)->Validate()) << endl << (*reader)->DebugString();
  
    ReaderContext::PerDiskState& state = (*reader)->disk_states_[disk_queue->disk_id];
    
    // Check if reader has been cancelled
    if ((*reader)->state_ == ReaderContext::Cancelled) {
      DCHECK(disk_queue->next_reader != reader_it);
      disk_queue->readers.erase(reader_it);
      state.is_on_queue = false;
      if (state.num_threads_in_read == 0) {
        state.num_scan_ranges = 0;
        DecrementDiskRefCount(*reader);
      }
      continue;
    }

    // Validate invariants.  The reader should be active, it should have
    // a buffer available on this disk and it should have a scan range to read.
    DCHECK(state.is_on_queue);
    DCHECK_GT(state.num_empty_buffers, 0);
    DCHECK_GT((*reader)->num_empty_buffers_, 0);
    DCHECK(!state.ranges.empty());
    
    // Increment the ref count on reader.  We need to track the number of threads per
    // reader per disk that is the in unlocked hdfs read code section.
    ++state.num_threads_in_read;
    
    // Get a free buffer from the disk io mgr.  It's a global pool for all readers but
    // they are lazily allocated.  Each reader is guaranteed its share.
    *buffer = GetFreeBuffer();
    --(*reader)->num_empty_buffers_;
    --state.num_empty_buffers;
    DCHECK(*buffer != NULL);

    // Round robin ranges 
    *range = *state.ranges.begin();
    state.ranges.pop_front();

    if (state.ranges.empty() || state.num_empty_buffers == 0) {
      // If this is the last scan range for this reader on this disk or the
      // last buffer on this disk, temporarily remove
      // the reader from the disk queue.  We don't want another disk thread to
      // pick up this reader when it can't do work.  The reader is added back
      // on the queue when the scan range is read or when the buffer is returned.
      DCHECK(disk_queue->next_reader != reader_it);
      disk_queue->readers.erase(reader_it);
      state.is_on_queue = false;
    }
    DCHECK_LT((*range)->bytes_read_, (*range)->len_);
    return true;
  }

  DCHECK(shut_down_);
  return false;
}

void DiskIoMgr::HandleReadFinished(DiskQueue* disk_queue, ReaderContext* reader,
    BufferDescriptor* buffer) {
  {
    unique_lock<mutex> disk_lock(disk_queue->lock);
    unique_lock<mutex> reader_lock(reader->lock_);

    ReaderContext::PerDiskState& state = reader->disk_states_[disk_queue->disk_id];
    
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    DCHECK_GT(state.num_threads_in_read, 0);
    DCHECK(buffer->buffer_ != NULL);

    --state.num_threads_in_read;

    if (reader->state_ == ReaderContext::Cancelled) {
      // For a cancelled reader, remove it from the disk queue right away.  We don't
      // do the final cleanup though unless we are the last thread.
      if (state.is_on_queue) {
        RemoveReaderFromDiskQueue(disk_queue, reader);
      }
      CloseScanRange(reader->hdfs_connection_, buffer->scan_range_);
      ++reader->num_empty_buffers_;
      ++state.num_empty_buffers;
      ReturnFreeBuffer(buffer->buffer_);
      buffer->buffer_ = NULL;
      ReturnBufferDesc(buffer);
      if (state.num_threads_in_read == 0) {
        state.num_scan_ranges = 0;
        DecrementDiskRefCount(reader);
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
      CloseScanRange(reader->hdfs_connection_, buffer->scan_range_);
      ++reader->num_empty_buffers_;
      ++state.num_empty_buffers;
      ReturnFreeBuffer(buffer->buffer_);
      buffer->buffer_ = NULL;
      buffer->eosr_ = true;
    } else {
      if (!buffer->eosr_) {
        DCHECK_LT(buffer->scan_range_->bytes_read_, buffer->scan_range_->len_);
        // Push the scan range to the front of the queue.  This lets us minimize the
        // number of scan ranges in flight (== to the number of scanner threads).  We
        // need to do this instead of round robin because it is not possible to keep
        // all scan ranges in flight (too many open hdfs files).
        // TODO: this needs to be merged with what the scan node is currently doing
        // to manage in flight ranges.  The scan node logic should be pushed down
        // to the io mgr.
        state.ranges.push_front(buffer->scan_range_);
      } else {
        CloseScanRange(reader->hdfs_connection_, buffer->scan_range_);
      }
    }

    if (buffer->eosr_) --state.num_scan_ranges;

    // We now update the disk state, adding or removing the reader from the disk
    // queue if necessary.  If all the scan ranges on this disk for this reader
    // are complete, remove the reader.  Otherwise, add the reader.
    if (state.num_scan_ranges == 0) {
      // All scan ranges are finished
      if (state.is_on_queue) {
        RemoveReaderFromDiskQueue(disk_queue, reader);
      }
      DecrementDiskRefCount(reader);
    } else if (!state.is_on_queue) {
      if (!state.ranges.empty() && state.num_empty_buffers > 0) {
        // The reader could have been removed by the thread working on the last 
        // scan range or last buffer.  In that case, we need to add the reader back 
        // on the disk queue since there is still more work to do.
        disk_queue->readers.push_back(reader);
        state.is_on_queue = true;
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
