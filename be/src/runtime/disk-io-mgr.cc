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
#include "runtime/disk-io-mgr-internal.h"

using namespace boost;
using namespace impala;
using namespace std;

// Control the number of disks on the machine.  If 0, this comes from the system
// settings.
DEFINE_int32(num_disks, 0, "Number of disks on data node.");
// Default IoMgr configs.
// The maximum number of the threads per disk is also the max queue depth per disk.
// The read size is the size of the reads sent to hdfs/os.
// There is a trade off of latency and throughout, trying to keep disks busy but
// not introduce seeks.  The literature seems to agree that with 8 MB reads, random
// io and sequential io perform similarly.
DEFINE_int32(num_threads_per_disk, 0, "number of threads per disk");
DEFINE_int32(read_size, 8 * 1024 * 1024, "Read Size (in bytes)");
DEFINE_int32(min_buffer_size, 1024, "The minimum read buffer size (in bytes)");

// Turning this to false will make asan much more effective for IO buffer related
// bugs.
DEFINE_bool(reuse_io_buffers, true, "(Advanced) If true, IoMgr will reuse IoBuffers "
                                     "across queries.");

// Rotational disks should have 1 thread per disk to minimize seeks.  Non-rotational
// don't have this penalty and benefit from multiple concurrent IO requests.
static const int THREADS_PER_ROTATIONAL_DISK = 1;
static const int THREADS_PER_FLASH_DISK = 8;

// The IoMgr is able to run with a wide range of memory usage. If a query has memory
// remaining less than this value, the IoMgr will stop all buffering regardless of the
// current queue size.
static const int LOW_MEMORY = 64 * 1024 * 1024;

const int DiskIoMgr::DEFAULT_QUEUE_CAPACITY = 2;

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

  // This object has the same lifetime as the disk IoMgr.
  ~ReaderCache() {
    for (list<ReaderContext*>::iterator it = all_readers_.begin();
        it != all_readers_.end(); ++it) {
      delete *it;
    }
  }

  // Validates that all readers are cleaned up and in the inactive state.  No locks
  // are taken since this is only called from the disk IoMgr destructor.
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

  string DebugString();

 private:
  DiskIoMgr* io_mgr_;

  // lock to protect all members below
  mutex lock_;

  // List of all reader created.  Used for debugging
  list<ReaderContext*> all_readers_;

  // List of inactive readers.  These objects can be used for a new reader.
  list<ReaderContext*> inactive_readers_;
};

string DiskIoMgr::ReaderCache::DebugString() {
  lock_guard<mutex> l(lock_);
  stringstream ss;
  for (list<ReaderContext*>::iterator it = all_readers_.begin();
      it != all_readers_.end(); ++it) {
    unique_lock<mutex> lock((*it)->lock_);
    ss << (*it)->DebugString() << endl;
  }
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

DiskIoMgr::BufferDescriptor::BufferDescriptor(DiskIoMgr* io_mgr) :
  io_mgr_(io_mgr), reader_(NULL), buffer_(NULL) {
}

void DiskIoMgr::BufferDescriptor::Reset(ReaderContext* reader,
      ScanRange* range, char* buffer, int64_t buffer_len) {
  DCHECK(io_mgr_ != NULL);
  DCHECK(buffer_ == NULL);
  DCHECK(range != NULL);
  DCHECK(buffer != NULL);
  DCHECK_GE(buffer_len, 0);
  reader_ = reader;
  scan_range_ = range;
  buffer_ = buffer;
  buffer_len_ = buffer_len;
  len_ = 0;
  eosr_ = false;
  status_ = Status::OK;
  mem_tracker_ = NULL;
}

void DiskIoMgr::BufferDescriptor::Return() {
  DCHECK(io_mgr_ != NULL);
  io_mgr_->ReturnBuffer(this);
}

void DiskIoMgr::BufferDescriptor::SetMemTracker(MemTracker* tracker) {
  // Cached buffers don't count towards mem usage.
  if (scan_range_->cached_buffer_ != NULL) return;
  if (mem_tracker_ == tracker) return;
  if (mem_tracker_ != NULL) mem_tracker_->Release(buffer_len_);
  mem_tracker_ = tracker;
  if (mem_tracker_ != NULL) mem_tracker_->Consume(buffer_len_);
}

static void CheckSseSupport() {
  if (!CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    LOG(WARNING) << "This machine does not support sse4_2.  The default IO system "
                    "configurations are suboptimal for this hardware.  Consider "
                    "increasing the number of threads per disk by restarting impalad "
                    "using the --num_threads_per_disk flag with a higher value";
  }
}

DiskIoMgr::DiskIoMgr() :
    num_threads_per_disk_(FLAGS_num_threads_per_disk),
    max_buffer_size_(FLAGS_read_size),
    min_buffer_size_(FLAGS_min_buffer_size),
    cached_read_options_(NULL),
    shut_down_(false),
    total_bytes_read_counter_(TCounterType::BYTES),
    read_timer_(TCounterType::TIME_NS) {
  int64_t max_buffer_size_scaled = BitUtil::Ceil(max_buffer_size_, min_buffer_size_);
  free_buffers_.resize(BitUtil::Log2(max_buffer_size_scaled) + 1);
  int num_disks = FLAGS_num_disks;
  if (num_disks == 0) num_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_disks);
  CheckSseSupport();
}

DiskIoMgr::DiskIoMgr(int num_disks, int threads_per_disk, int min_buffer_size,
                     int max_buffer_size) :
    num_threads_per_disk_(threads_per_disk),
    max_buffer_size_(max_buffer_size),
    min_buffer_size_(min_buffer_size),
    cached_read_options_(NULL),
    shut_down_(false),
    total_bytes_read_counter_(TCounterType::BYTES),
    read_timer_(TCounterType::TIME_NS) {
  int64_t max_buffer_size_scaled = BitUtil::Ceil(max_buffer_size_, min_buffer_size_);
  free_buffers_.resize(BitUtil::Log2(max_buffer_size_scaled) + 1);
  if (num_disks == 0) num_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_disks);
  CheckSseSupport();
}

DiskIoMgr::~DiskIoMgr() {
  shut_down_ = true;
  // Notify all worker threads and shut them down.
  for (int i = 0; i < disk_queues_.size(); ++i) {
    if (disk_queues_[i] == NULL) continue;
    {
      // This lock is necessary to properly use the condition var to notify
      // the disk worker threads.  The readers also grab this lock so updates
      // to shut_down_ are protected.
      unique_lock<mutex> disk_lock(disk_queues_[i]->lock);
    }
    disk_queues_[i]->work_available.notify_all();
  }
  disk_thread_group_.JoinAll();

  for (int i = 0; i < disk_queues_.size(); ++i) {
    if (disk_queues_[i] == NULL) continue;
    int disk_id = disk_queues_[i]->disk_id;
    for (list<ReaderContext*>::iterator it = disk_queues_[i]->readers.begin();
        it != disk_queues_[i]->readers.end(); ++it) {
      DCHECK_EQ((*it)->disk_states_[disk_id].num_threads_in_read(), 0);
      DCHECK((*it)->disk_states_[disk_id].done());
      (*it)->DecrementDiskRefCount();
    }
  }

  DCHECK(reader_cache_.get() == NULL || reader_cache_->ValidateAllInactive())
      << endl << DebugString();
  DCHECK_EQ(num_buffers_in_readers_, 0);

  // Delete all allocated buffers
  int num_free_buffers = 0;
  for (int idx = 0; idx < free_buffers_.size(); ++idx) {
    num_free_buffers += free_buffers_[idx].size();
  }
  DCHECK_EQ(num_allocated_buffers_, num_free_buffers);
  GcIoBuffers();

  for (int i = 0; i < disk_queues_.size(); ++i) {
    delete disk_queues_[i];
  }

  if (cached_read_options_ != NULL) hadoopRzOptionsFree(cached_read_options_);
}

Status DiskIoMgr::Init(MemTracker* process_mem_tracker) {
  DCHECK(process_mem_tracker != NULL);
  process_mem_tracker_ = process_mem_tracker;
  // If we hit the process limit, see if we can reclaim some memory by removing
  // previously allocated (but unused) io buffers.
  process_mem_tracker->AddGcFunction(boost::bind(&DiskIoMgr::GcIoBuffers, this));

  for (int i = 0; i < disk_queues_.size(); ++i) {
    disk_queues_[i] = new DiskQueue(i);
    int num_threads_per_disk = num_threads_per_disk_;
    if (num_threads_per_disk == 0) {
      if (DiskInfo::is_rotational(i)) {
        num_threads_per_disk = THREADS_PER_ROTATIONAL_DISK;
      } else {
        num_threads_per_disk = THREADS_PER_FLASH_DISK;
      }
    }
    for (int j = 0; j < num_threads_per_disk; ++j) {
      stringstream ss;
      ss << "read-loop(Disk: " << i << ", Thread: " << j << ")";
      disk_thread_group_.AddThread(new Thread("disk-io-mgr", ss.str(),
          &DiskIoMgr::ReadLoop, this, disk_queues_[i]));
    }
  }
  reader_cache_.reset(new ReaderCache(this));

  cached_read_options_ = hadoopRzOptionsAlloc();
  DCHECK(cached_read_options_ != NULL);
  // Disable checksumming for cached reads.
  int ret = hadoopRzOptionsSetSkipChecksum(cached_read_options_, true);
  DCHECK_EQ(ret, 0);
  // Disable automatic fallback for cached reads.
  ret = hadoopRzOptionsSetByteBufferPool(cached_read_options_, NULL);
  DCHECK_EQ(ret, 0);

  return Status::OK;
}

Status DiskIoMgr::RegisterReader(hdfsFS hdfs, ReaderContext** reader,
    MemTracker* mem_tracker) {
  DCHECK(reader_cache_.get() != NULL) << "Must call Init() first.";
  *reader = reader_cache_->GetNewReader();
  (*reader)->Reset(hdfs, mem_tracker);
  return Status::OK;
}

void DiskIoMgr::WaitForDisksCompletion(ReaderContext* reader) {
  // First cancel the reader.  This is more or less a no-op if the reader is
  // complete (common case).
  reader->Cancel(Status::CANCELLED);

  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  while (reader->num_disks_with_ranges_ > 0) {
    reader->disks_complete_cond_var_.wait(reader_lock);
  }
}

void DiskIoMgr::UnregisterReader(ReaderContext* reader) {
  WaitForDisksCompletion(reader);

  // All the disks are done with clean, validate nothing is leaking.
  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK_EQ(reader->num_buffers_in_reader_, 0) << endl << reader->DebugString();
  DCHECK_EQ(reader->num_used_buffers_, 0) << endl << reader->DebugString();

  DCHECK(reader->Validate()) << endl << reader->DebugString();
  reader_cache_->ReturnReader(reader);
}

// Cancellation requires coordination from multiple threads.  Each thread that currently
// has a reference to the reader must notice the cancel and remove it from its tracking
// structures.  The last thread to touch the reader should deallocate (aka recycle) the
// reader context object.  Potential threads are:
//  1. Disk threads that are currently reading for this reader.
//  2. Caller threads that are waiting in GetNext.
//
// The steps are:
// 1. Cancel will immediately set the reader in the Cancelled state.  This prevents any
// other thread from adding more ready buffers to this reader (they all take a lock and
// check the state before doing so).
// 2. Cancel will call cancel on each ScanRange that is not yet complete, unblocking
// any threads in GetNext(). The reader will see the cancelled Status returned.
// 3. Disk threads notice the reader is cancelled either when picking the next reader
// to read for or when they try to enqueue a ready buffer.  Upon noticing the cancelled
// state, removes the reader from the disk queue.  The last thread per disk with an
// outstanding reference to the reader decrements the number of disk queues the reader
// is on.
void DiskIoMgr::CancelReader(ReaderContext* reader) {
  reader->Cancel(Status::CANCELLED);
}

void DiskIoMgr::set_read_timer(ReaderContext* r, RuntimeProfile::Counter* c) {
  r->read_timer_ = c;
}

void DiskIoMgr::set_bytes_read_counter(ReaderContext* r, RuntimeProfile::Counter* c) {
  r->bytes_read_counter_ = c;
}

void DiskIoMgr::set_active_read_thread_counter(ReaderContext* r,
    RuntimeProfile::Counter* c) {
  r->active_read_thread_counter_ = c;
}

void DiskIoMgr::set_disks_access_bitmap(ReaderContext* r,
    RuntimeProfile::Counter* c) {
  r->disks_accessed_bitmap_ = c;
}

int64_t DiskIoMgr::queue_size(ReaderContext* reader) const {
  return reader->num_ready_buffers_;
}

Status DiskIoMgr::reader_status(ReaderContext* reader) const {
  unique_lock<mutex> reader_lock(reader->lock_);
  return reader->status_;
}

int DiskIoMgr::num_unstarted_ranges(ReaderContext* reader) const {
  return reader->num_unstarted_ranges_;
}

int64_t DiskIoMgr::bytes_read_local(ReaderContext* reader) const {
  return reader->bytes_read_local_;
}

int64_t DiskIoMgr::bytes_read_short_circuit(ReaderContext* reader) const {
  return reader->bytes_read_short_circuit_;
}

int64_t DiskIoMgr::bytes_read_dn_cache(ReaderContext* reader) const {
  return reader->bytes_read_dn_cache_;
}

int64_t DiskIoMgr::GetReadThroughput() {
  return RuntimeProfile::UnitsPerSecond(&total_bytes_read_counter_, &read_timer_);
}

Status DiskIoMgr::ValidateScanRange(ScanRange* range) {
  int disk_id = range->disk_id_;
  if (disk_id < 0 || disk_id >= disk_queues_.size()) {
    stringstream ss;
    ss << "Invalid scan range.  Bad disk id: " << disk_id;
    DCHECK(false) << ss.str();
    return Status(ss.str());
  }
  return Status::OK;
}

Status DiskIoMgr::AddScanRanges(ReaderContext* reader,
    const vector<ScanRange*>& ranges, bool schedule_immediately) {
  if (ranges.empty()) return Status::OK;

  // Validate and initialize all ranges
  for (int i = 0; i < ranges.size(); ++i) {
    RETURN_IF_ERROR(ValidateScanRange(ranges[i]));
    ranges[i]->InitInternal(this, reader);
  }

  // disks that this reader needs to be scheduled on.
  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();

  if (reader->state_ == ReaderContext::Cancelled) {
    DCHECK(!reader->status_.ok());
    return reader->status_;
  }

  // Add each range to the queue of the disk the range is on
  for (int i = 0; i < ranges.size(); ++i) {
    // Don't add empty ranges.
    DCHECK_NE(ranges[i]->len(), 0);
    ScanRange* range = ranges[i];

    if (range->try_cache_) {
      if (schedule_immediately) {
        bool cached_read_succeeded;
        RETURN_IF_ERROR(range->ReadFromCache(&cached_read_succeeded));
        if (cached_read_succeeded) continue;
        // Cached read failed, fall back to AddScanRange() below.
      } else {
        reader->cached_ranges_.Enqueue(range);
        continue;
      }
    }
    reader->AddScanRange(range, schedule_immediately);
  }
  DCHECK(reader->Validate()) << endl << reader->DebugString();

  return Status::OK;
}

// This function returns the next scan range the reader should work on, checking
// for eos and error cases. If there isn't already a cached scan range or a scan
// range prepared by the disk threads, the caller waits on the disk threads.
Status DiskIoMgr::GetNextRange(ReaderContext* reader, ScanRange** range) {
  DCHECK(reader != NULL);
  DCHECK(range != NULL);
  *range = NULL;

  Status status;

  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();

  while (true) {
    if (reader->state_ == ReaderContext::Cancelled) {
      DCHECK(!reader->status_.ok());
      status = reader->status_;
      break;
    }

    if (reader->num_unstarted_ranges_ == 0 && reader->ready_to_start_ranges_.empty() &&
        reader->cached_ranges_.empty()) {
      // All ranges are done, just return.
      break;
    }

    if (!reader->cached_ranges_.empty()) {
      // We have a cached range.
      *range = reader->cached_ranges_.Dequeue();
      DCHECK((*range)->try_cache_);
      bool cached_read_succeeded;
      RETURN_IF_ERROR((*range)->ReadFromCache(&cached_read_succeeded));
      if (cached_read_succeeded) return Status::OK;

      // This range ended up not being cached. Loop again and pick up a new range.
      reader->AddScanRange(*range, false);
      DCHECK(reader->Validate()) << endl << reader->DebugString();
      *range = NULL;
      continue;
    }

    if (reader->ready_to_start_ranges_.empty()) {
      reader->ready_to_start_ranges_cv_.wait(reader_lock);
    } else {
      *range = reader->ready_to_start_ranges_.Dequeue();
      DCHECK(*range != NULL);
      int disk_id = (*range)->disk_id();
      DCHECK(*range == reader->disk_states_[disk_id].next_range_to_start());
      // Set this to NULL, the next time this disk runs for this reader, it will
      // get another range ready.
      reader->disk_states_[disk_id].set_next_range_to_start(NULL);
      reader->ScheduleScanRange(*range);
      break;
    }
  }

  return status;
}

Status DiskIoMgr::Read(ReaderContext* reader,
    ScanRange* range, BufferDescriptor** buffer) {
  DCHECK(range != NULL);
  DCHECK(buffer != NULL);
  *buffer = NULL;

  if (range->len() > max_buffer_size_) {
    stringstream ss;
    ss << "Cannot perform sync read larger than " << max_buffer_size_
       << ". Request was " << range->len();
    return Status(ss.str());
  }

  vector<DiskIoMgr::ScanRange*> ranges;
  ranges.push_back(range);
  RETURN_IF_ERROR(AddScanRanges(reader, ranges, true));
  RETURN_IF_ERROR(range->GetNext(buffer));
  DCHECK((*buffer) != NULL);
  DCHECK((*buffer)->eosr());
  return Status::OK;
}

void DiskIoMgr::ReturnBuffer(BufferDescriptor* buffer_desc) {
  DCHECK(buffer_desc != NULL);
  if (!buffer_desc->status_.ok()) DCHECK(buffer_desc->buffer_ == NULL);

  // A NULL buffer means there was an error or the buffer desc is from a cached
  // read. In either of those cases, we only need to return the descriptor object.
  if (buffer_desc->scan_range_->cached_buffer_ != NULL) {
    buffer_desc->buffer_ = NULL;
    // Returning the cached buffer means we're done with the scan range. It is
    // safe to close it (this is the only buffer that will be used for this range).
    buffer_desc->scan_range_->Close();
  }

  if (buffer_desc->buffer_ == NULL) {
    ReturnBufferDesc(buffer_desc);
    return;
  }

  ReaderContext* reader = buffer_desc->reader_;
  ReturnFreeBuffer(buffer_desc->buffer_, buffer_desc->buffer_len_);
  buffer_desc->SetMemTracker(NULL);
  buffer_desc->buffer_ = NULL;
  ReturnBufferDesc(buffer_desc);

  --num_buffers_in_readers_;
  --reader->num_buffers_in_reader_;
}

void DiskIoMgr::ReturnBufferDesc(BufferDescriptor* desc) {
  DCHECK(desc != NULL);
  unique_lock<mutex> lock(free_buffers_lock_);
  DCHECK(find(free_buffer_descs_.begin(), free_buffer_descs_.end(), desc)
         == free_buffer_descs_.end());
  free_buffer_descs_.push_back(desc);
}

DiskIoMgr::BufferDescriptor* DiskIoMgr::GetBufferDesc(
    ReaderContext* reader, ScanRange* range, char* buffer, int64_t buffer_size) {
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
  buffer_desc->Reset(reader, range, buffer, buffer_size);
  buffer_desc->SetMemTracker(reader->mem_tracker_);
  return buffer_desc;
}

char* DiskIoMgr::GetFreeBuffer(int64_t* buffer_size) {
  DCHECK_LE(*buffer_size, max_buffer_size_);
  DCHECK_GT(*buffer_size, 0);
  *buffer_size = min(static_cast<int64_t>(max_buffer_size_), *buffer_size);
  int idx = free_buffers_idx(*buffer_size);
  // Quantize buffer size to nearest power of 2 greater than the specified buffer size and
  // convert to bytes
  *buffer_size = (1 << idx) * min_buffer_size_;

  unique_lock<mutex> lock(free_buffers_lock_);
  char* buffer = NULL;
  if (free_buffers_[idx].empty()) {
    ++num_allocated_buffers_;
    if (ImpaladMetrics::IO_MGR_NUM_BUFFERS != NULL) {
      ImpaladMetrics::IO_MGR_NUM_BUFFERS->Increment(1L);
    }
    if (ImpaladMetrics::IO_MGR_TOTAL_BYTES != NULL) {
      ImpaladMetrics::IO_MGR_TOTAL_BYTES->Increment(*buffer_size);
    }
    // Update the process mem usage.  This is checked the next time we start
    // a read for the next reader (DiskIoMgr::GetNextScanRange)
    process_mem_tracker_->Consume(*buffer_size);
    buffer = new char[*buffer_size];
  } else {
    if (ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS != NULL) {
      ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS->Increment(-1L);
    }
    buffer = free_buffers_[idx].front();
    free_buffers_[idx].pop_front();
  }
  DCHECK(buffer != NULL);
  return buffer;
}

void DiskIoMgr::GcIoBuffers() {
  unique_lock<mutex> lock(free_buffers_lock_);
  int buffers_freed = 0;
  int bytes_freed = 0;
  for (int idx = 0; idx < free_buffers_.size(); ++idx) {
    for (list<char*>::iterator iter = free_buffers_[idx].begin();
         iter != free_buffers_[idx].end(); ++iter) {
      int64_t buffer_size = (1 << idx) * min_buffer_size_;
      process_mem_tracker_->Release(buffer_size);
      --num_allocated_buffers_;
      delete[] *iter;

      ++buffers_freed;
      bytes_freed += buffer_size;
    }
    free_buffers_[idx].clear();
  }

  if (ImpaladMetrics::IO_MGR_NUM_BUFFERS != NULL) {
    ImpaladMetrics::IO_MGR_NUM_BUFFERS->Increment(-buffers_freed);
  }
  if (ImpaladMetrics::IO_MGR_TOTAL_BYTES != NULL) {
    ImpaladMetrics::IO_MGR_TOTAL_BYTES->Increment(-bytes_freed);
  }
  if (ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS != NULL) {
    ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS->Update(0);
  }
}

void DiskIoMgr::ReturnFreeBuffer(char* buffer, int64_t buffer_size) {
  DCHECK(buffer != NULL);
  int idx = free_buffers_idx(buffer_size);
  DCHECK_EQ(BitUtil::Ceil(buffer_size, min_buffer_size_) & ~(1 << idx), 0)
      << "buffer_size_ / min_buffer_size_ should be power of 2, got buffer_size = "
      << buffer_size << ", min_buffer_size_ = " << min_buffer_size_;
  if (FLAGS_reuse_io_buffers) {
    unique_lock<mutex> lock(free_buffers_lock_);
    free_buffers_[idx].push_back(buffer);
  } else {
    process_mem_tracker_->Release(buffer_size);
    --num_allocated_buffers_;
    delete[] buffer;
  }
  if (ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS != NULL) {
    ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS->Increment(1L);
  }
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
    ReaderContext** reader) {
  int disk_id = disk_queue->disk_id;
  *range = NULL;

  // This loops returns either with work to do or when the disk IoMgr shuts down.
  while (true) {
    *reader = NULL;
    ReaderContext::PerDiskState* reader_disk_state = NULL;
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
      // TODO: revisit.
      *reader = disk_queue->readers.front();
      disk_queue->readers.pop_front();
      DCHECK(*reader != NULL);
      reader_disk_state = &((*reader)->disk_states_[disk_id]);
      reader_disk_state->IncrementReadThreadAndDequeue();
    }

    // NOTE: no locks were taken in between.  We need to be careful about what state
    // could have changed to the reader and disk in between.
    // There are some invariants here.  Only one disk thread can have the
    // same reader here (the reader is removed from the queue).  There can be
    // other disk threads operating on this reader in other functions though.

    // We just picked a reader, check the mem limits.
    // TODO: we can do a lot better here.  The reader can likely make progress
    // with fewer io buffers.
    bool process_limit_exceeded = process_mem_tracker_->LimitExceeded();
    bool reader_limit_exceeded = (*reader)->mem_tracker_ != NULL
        ? (*reader)->mem_tracker_->AnyLimitExceeded() : false;

    if (process_limit_exceeded || reader_limit_exceeded) {
      (*reader)->Cancel(Status::MEM_LIMIT_EXCEEDED);
    }

    unique_lock<mutex> reader_lock((*reader)->lock_);
    VLOG_FILE << "Disk (id=" << disk_id << ") reading for " << (*reader)->DebugString();

    // Check if reader has been cancelled
    if ((*reader)->state_ == ReaderContext::Cancelled) {
      reader_disk_state->DecrementReadThreadAndCheckDone(*reader);
      continue;
    }

    DCHECK_EQ((*reader)->state_, ReaderContext::Active) << (*reader)->DebugString();

    if (reader_disk_state->next_range_to_start() == NULL &&
        !reader_disk_state->unstarted_ranges()->empty()) {
      // We don't have a range queued for this disk for what the caller should
      // read next. Populate that.  We want to have one range waiting to minimize
      // wait time in GetNextRange.
      ScanRange* new_range = reader_disk_state->unstarted_ranges()->Dequeue();
      --(*reader)->num_unstarted_ranges_;
      (*reader)->ready_to_start_ranges_.Enqueue(new_range);
      reader_disk_state->set_next_range_to_start(new_range);

      if ((*reader)->num_unstarted_ranges_ == 0) {
        // All the ranges have been started, notify everyone blocked on GetNextRange.
        // Only one of them will get work so make sure to return NULL to the other
        // caller threads.
        (*reader)->ready_to_start_ranges_cv_.notify_all();
      } else {
        (*reader)->ready_to_start_ranges_cv_.notify_one();
      }
    }

    // Get the next scan range to work on from the reader. Only in_flight_ranges
    // are eligible since the disk threads do not start new ranges on their own.

    // There are no inflight ranges, nothing to do.
    if (reader_disk_state->in_flight_ranges()->empty()) {
      reader_disk_state->DecrementReadThread();
      continue;
    }
    DCHECK_GT(reader_disk_state->num_remaining_ranges(), 0);
    *range = reader_disk_state->in_flight_ranges()->Dequeue();
    DCHECK(*range != NULL);
    DCHECK_LT((*range)->bytes_read_, (*range)->len_);

    // Now that we've picked a scan range, put the reader back on the queue so
    // another thread can pick up another scan range for this reader.
    reader_disk_state->ScheduleReader(*reader, disk_id);
    DCHECK((*reader)->Validate()) << endl << (*reader)->DebugString();
    return true;
  }

  DCHECK(shut_down_);
  return false;
}

void DiskIoMgr::HandleReadFinished(DiskQueue* disk_queue, ReaderContext* reader,
    BufferDescriptor* buffer) {
  unique_lock<mutex> reader_lock(reader->lock_);

  ReaderContext::PerDiskState& state = reader->disk_states_[disk_queue->disk_id];
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  DCHECK_GT(state.num_threads_in_read(), 0);
  DCHECK(buffer->buffer_ != NULL);

  if (reader->state_ == ReaderContext::Cancelled) {
    state.DecrementReadThreadAndCheckDone(reader);
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    ReturnFreeBuffer(buffer->buffer_, buffer->buffer_len_);
    buffer->SetMemTracker(NULL);
    buffer->buffer_ = NULL;
    buffer->scan_range_->Cancel(reader->status_);
    // Enqueue the buffer to use the scan range's buffer cleanup path.
    buffer->scan_range_->EnqueueBuffer(buffer);
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
    ReturnFreeBuffer(buffer->buffer_, buffer->buffer_len_);
    buffer->SetMemTracker(NULL);
    buffer->buffer_ = NULL;
    buffer->eosr_ = true;
    --state.num_remaining_ranges();
    buffer->scan_range_->Cancel(buffer->status_);
  } else if (buffer->eosr_) {
    buffer->scan_range_->Close();
    --state.num_remaining_ranges();
  }

  bool queue_full = buffer->scan_range_->EnqueueBuffer(buffer);
  if (!buffer->eosr_) {
    if (queue_full) {
      reader->blocked_ranges_.Enqueue(buffer->scan_range_);
    } else {
      reader->ScheduleScanRange(buffer->scan_range_);
    }
  }
  state.DecrementReadThread();
}

// The thread waits until there is work or the entire system is being shut down.
// If there is work, it reads the next chunk of the next scan range for the first
// reader in the queue and round robins across the readers.
// Locks are not taken when reading from disk.  The main loop has three parts:
//   1. GetNextScanRange(): Take locks and figure out what the next scan range to read is
//   2. Open/Read the scan range.  No locks are taken
//   3. HandleReadFinished(): Take locks and update the disk and reader with the
//      results of the io.
// Cancellation checking needs to happen in both steps 1 and 3.
void DiskIoMgr::ReadLoop(DiskQueue* disk_queue) {
  int64_t disk_bit = 1 << disk_queue->disk_id;
  while (true) {
    char* buffer = NULL;
    ReaderContext* reader = NULL;;
    ScanRange* range = NULL;

    // Get the next scan range to read
    if (!GetNextScanRange(disk_queue, &range, &reader)) {
      DCHECK(shut_down_);
      break;
    }

    int64_t bytes_remaining = range->len_ - range->bytes_read_;
    int64_t buffer_size = ::min(bytes_remaining, static_cast<int64_t>(max_buffer_size_));
    bool enough_memory = true;
    if (reader->mem_tracker_ != NULL) {
      enough_memory = reader->mem_tracker_->SpareCapacity() > LOW_MEMORY;
      if (!enough_memory) {
        // Low memory, GC and try again.
        GcIoBuffers();
        enough_memory = reader->mem_tracker_->SpareCapacity() > LOW_MEMORY;
      }
    }

    if (!enough_memory) {
      ReaderContext::PerDiskState& state = reader->disk_states_[disk_queue->disk_id];
      unique_lock<mutex> reader_lock(reader->lock_);
      if (!range->ready_buffers_.empty()) {
        // We have memory pressure and this range doesn't need another buffer
        // (it already has one queued). Skip this range and pick it up later.
        range->blocked_on_queue_ = true;
        reader->blocked_ranges_.Enqueue(range);
        state.DecrementReadThread();
        continue;
      } else {
        // We need to get a buffer anyway since there are none queued. The query
        // is likely to fail due to mem limits but there's nothing we can do about that
        // now.
      }
    }

    buffer = GetFreeBuffer(&buffer_size);
    ++reader->num_used_buffers_;

    // Validate more invariants.
    DCHECK_GT(reader->num_used_buffers_, 0);
    DCHECK(range != NULL);
    DCHECK(reader != NULL);
    DCHECK(buffer != NULL);

    BufferDescriptor* buffer_desc = GetBufferDesc(reader, range, buffer, buffer_size);
    DCHECK(buffer_desc != NULL);

    // No locks in this section.  Only working on local vars.  We don't want to hold a
    // lock across the read call.
    buffer_desc->status_ = range->Open();
    if (buffer_desc->status_.ok()) {
      // Update counters.
      if (reader->active_read_thread_counter_) {
        reader->active_read_thread_counter_->Update(1L);
      }
      if (reader->disks_accessed_bitmap_) {
        reader->disks_accessed_bitmap_->BitOr(disk_bit);
      }
      SCOPED_TIMER(&read_timer_);
      SCOPED_TIMER(reader->read_timer_);

      buffer_desc->status_ = range->Read(buffer, &buffer_desc->len_, &buffer_desc->eosr_);
      buffer_desc->scan_range_offset_ = range->bytes_read_ - buffer_desc->len_;

      if (reader->bytes_read_counter_ != NULL) {
        COUNTER_UPDATE(reader->bytes_read_counter_, buffer_desc->len_);
      }
      COUNTER_UPDATE(&total_bytes_read_counter_, buffer_desc->len_);
      if (reader->active_read_thread_counter_) {
        reader->active_read_thread_counter_->Update(-1L);
      }
    }

    // Finished read, update reader/disk based on the results
    HandleReadFinished(disk_queue, reader, buffer_desc);
  }

  DCHECK(shut_down_);
}

int DiskIoMgr::free_buffers_idx(int64_t buffer_size) {
  int64_t buffer_size_scaled = BitUtil::Ceil(buffer_size, min_buffer_size_);
  int idx = BitUtil::Log2(buffer_size_scaled);
  DCHECK_GE(idx, 0);
  DCHECK_LT(idx, free_buffers_.size());
  return idx;
}
