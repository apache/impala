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
#include "util/hdfs-util.h"

#include <gutil/strings/substitute.h>
#include <boost/algorithm/string.hpp>

DECLARE_bool(disable_mem_pools);

#include "common/names.h"

using namespace impala;
using namespace strings;

// Control the number of disks on the machine.  If 0, this comes from the system
// settings.
DEFINE_int32(num_disks, 0, "Number of disks on data node.");
// Default IoMgr configs:
// The maximum number of the threads per disk is also the max queue depth per disk.
DEFINE_int32(num_threads_per_disk, 0, "number of threads per disk");
// The maximum number of remote HDFS I/O threads.  HDFS access that are expected to be
// remote are placed on a separate remote disk queue.  This is the queue depth for that
// queue.  If 0, then the remote queue is not used and instead ranges are round-robined
// across the local disk queues.
DEFINE_int32(num_remote_hdfs_io_threads, 8, "number of remote HDFS I/O threads");
// The maximum number of S3 I/O threads. The default value of 16 was chosen emperically
// to maximize S3 throughput. Maximum throughput is achieved with multiple connections
// open to S3 and use of multiple CPU cores since S3 reads are relatively compute
// expensive (SSL and JNI buffer overheads).
DEFINE_int32(num_s3_io_threads, 16, "number of S3 I/O threads");
// The read size is the size of the reads sent to hdfs/os.
// There is a trade off of latency and throughout, trying to keep disks busy but
// not introduce seeks.  The literature seems to agree that with 8 MB reads, random
// io and sequential io perform similarly.
DEFINE_int32(read_size, 8 * 1024 * 1024, "Read Size (in bytes)");
DEFINE_int32(min_buffer_size, 1024, "The minimum read buffer size (in bytes)");

// With 1024B through 8MB buffers, this is up to ~2GB of buffers.
DEFINE_int32(max_free_io_buffers, 128,
    "For each io buffer size, the maximum number of buffers the IoMgr will hold onto");

// The number of cached file handles defines how much memory can be used per backend for
// caching frequently used file handles. Currently, we assume that approximately 2kB data
// are associated with a single file handle. 10k file handles will thus reserve ~20MB
// data. The actual amount of memory that is associated with a file handle can be larger
// or smaller, depending on the replication factor for this file or the path name.
DEFINE_uint64(max_cached_file_handles, 0, "Maximum number of HDFS file handles "
    "that will be cached. Disabled if set to 0.");

// Rotational disks should have 1 thread per disk to minimize seeks.  Non-rotational
// don't have this penalty and benefit from multiple concurrent IO requests.
static const int THREADS_PER_ROTATIONAL_DISK = 1;
static const int THREADS_PER_FLASH_DISK = 8;

// The IoMgr is able to run with a wide range of memory usage. If a query has memory
// remaining less than this value, the IoMgr will stop all buffering regardless of the
// current queue size.
static const int LOW_MEMORY = 64 * 1024 * 1024;

const int DiskIoMgr::DEFAULT_QUEUE_CAPACITY = 2;

namespace detail {
// Indicates if file handle caching should be used
static inline bool is_file_handle_caching_enabled() {
  return FLAGS_max_cached_file_handles > 0;
}
}

/// This method is used to clean up resources upon eviction of a cache file handle.
void DiskIoMgr::HdfsCachedFileHandle::Release(DiskIoMgr::HdfsCachedFileHandle** h) {
  ImpaladMetrics::IO_MGR_NUM_CACHED_FILE_HANDLES->Increment(-1L);
  VLOG_FILE << "Cached file handle evicted, hdfsCloseFile() fid=" << (*h)->hdfs_file_;
  delete (*h);
}

DiskIoMgr::HdfsCachedFileHandle::HdfsCachedFileHandle(const hdfsFS& fs, const char* fname,
    int64_t mtime)
    : fs_(fs), hdfs_file_(hdfsOpenFile(fs, fname, O_RDONLY, 0, 0, 0)), mtime_(mtime) {
  VLOG_FILE << "hdfsOpenFile() file=" << fname << " fid=" << hdfs_file_;
}

DiskIoMgr::HdfsCachedFileHandle::~HdfsCachedFileHandle() {
  if (hdfs_file_ != NULL && fs_ != NULL) {
    VLOG_FILE << "hdfsCloseFile() fid=" << hdfs_file_;
    hdfsCloseFile(fs_, hdfs_file_);
  }
  fs_ = NULL;
  hdfs_file_ = NULL;
}

// This class provides a cache of RequestContext objects.  RequestContexts are recycled.
// This is good for locality as well as lock contention.  The cache has the property that
// regardless of how many clients get added/removed, the memory locations for
// existing clients do not change (not the case with std::vector) minimizing the locks we
// have to take across all readers.
// All functions on this object are thread safe
class DiskIoMgr::RequestContextCache {
 public:
  RequestContextCache(DiskIoMgr* io_mgr) : io_mgr_(io_mgr) {}

  // Returns a context to the cache.  This object can now be reused.
  void ReturnContext(RequestContext* reader) {
    DCHECK(reader->state_ != RequestContext::Inactive);
    reader->state_ = RequestContext::Inactive;
    lock_guard<mutex> l(lock_);
    inactive_contexts_.push_back(reader);
  }

  // Returns a new RequestContext object.  Allocates a new object if necessary.
  RequestContext* GetNewContext() {
    lock_guard<mutex> l(lock_);
    if (!inactive_contexts_.empty()) {
      RequestContext* reader = inactive_contexts_.front();
      inactive_contexts_.pop_front();
      return reader;
    } else {
      RequestContext* reader = new RequestContext(io_mgr_, io_mgr_->num_total_disks());
      all_contexts_.push_back(reader);
      return reader;
    }
  }

  // This object has the same lifetime as the disk IoMgr.
  ~RequestContextCache() {
    for (list<RequestContext*>::iterator it = all_contexts_.begin();
        it != all_contexts_.end(); ++it) {
      delete *it;
    }
  }

  // Validates that all readers are cleaned up and in the inactive state.  No locks
  // are taken since this is only called from the disk IoMgr destructor.
  bool ValidateAllInactive() {
    for (list<RequestContext*>::iterator it = all_contexts_.begin();
        it != all_contexts_.end(); ++it) {
      if ((*it)->state_ != RequestContext::Inactive) {
        return false;
      }
    }
    DCHECK_EQ(all_contexts_.size(), inactive_contexts_.size());
    return all_contexts_.size() == inactive_contexts_.size();
  }

  string DebugString();

 private:
  DiskIoMgr* io_mgr_;

  // lock to protect all members below
  mutex lock_;

  // List of all request contexts created.  Used for debugging
  list<RequestContext*> all_contexts_;

  // List of inactive readers.  These objects can be used for a new reader.
  list<RequestContext*> inactive_contexts_;
};

string DiskIoMgr::RequestContextCache::DebugString() {
  lock_guard<mutex> l(lock_);
  stringstream ss;
  for (list<RequestContext*>::iterator it = all_contexts_.begin();
      it != all_contexts_.end(); ++it) {
    unique_lock<mutex> lock((*it)->lock_);
    ss << (*it)->DebugString() << endl;
  }
  return ss.str();
}

string DiskIoMgr::DebugString() {
  stringstream ss;
  ss << "RequestContexts: " << endl << request_context_cache_->DebugString() << endl;

  ss << "Disks: " << endl;
  for (int i = 0; i < disk_queues_.size(); ++i) {
    unique_lock<mutex> lock(disk_queues_[i]->lock);
    ss << "  " << (void*) disk_queues_[i] << ":" ;
    if (!disk_queues_[i]->request_contexts.empty()) {
      ss << " Readers: ";
      BOOST_FOREACH(RequestContext* req_context, disk_queues_[i]->request_contexts) {
        ss << (void*)req_context;
      }
    }
    ss << endl;
  }
  return ss.str();
}

DiskIoMgr::BufferDescriptor::BufferDescriptor(DiskIoMgr* io_mgr) :
  io_mgr_(io_mgr), reader_(NULL), buffer_(NULL) {
}

void DiskIoMgr::BufferDescriptor::Reset(RequestContext* reader,
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
  status_ = Status::OK();
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

DiskIoMgr::WriteRange::WriteRange(const string& file, int64_t file_offset, int disk_id,
    WriteDoneCallback callback) {
  file_ = file;
  offset_ = file_offset;
  disk_id_ = disk_id;
  callback_ = callback;
  request_type_ = RequestType::WRITE;
}

void DiskIoMgr::WriteRange::SetData(const uint8_t* buffer, int64_t len) {
  data_ = buffer;
  len_ = len;
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
    total_bytes_read_counter_(TUnit::BYTES),
    read_timer_(TUnit::TIME_NS),
    file_handle_cache_(min(FLAGS_max_cached_file_handles,
        FileSystemUtil::MaxNumFileHandles()),
        &HdfsCachedFileHandle::Release) {
  int64_t max_buffer_size_scaled = BitUtil::Ceil(max_buffer_size_, min_buffer_size_);
  free_buffers_.resize(BitUtil::Log2(max_buffer_size_scaled) + 1);
  int num_local_disks = FLAGS_num_disks == 0 ? DiskInfo::num_disks() : FLAGS_num_disks;
  disk_queues_.resize(num_local_disks + REMOTE_NUM_DISKS);
  CheckSseSupport();
}

DiskIoMgr::DiskIoMgr(int num_local_disks, int threads_per_disk, int min_buffer_size,
                     int max_buffer_size) :
    num_threads_per_disk_(threads_per_disk),
    max_buffer_size_(max_buffer_size),
    min_buffer_size_(min_buffer_size),
    cached_read_options_(NULL),
    shut_down_(false),
    total_bytes_read_counter_(TUnit::BYTES),
    read_timer_(TUnit::TIME_NS),
    file_handle_cache_(min(FLAGS_max_cached_file_handles,
            FileSystemUtil::MaxNumFileHandles()), &HdfsCachedFileHandle::Release) {
  int64_t max_buffer_size_scaled = BitUtil::Ceil(max_buffer_size_, min_buffer_size_);
  free_buffers_.resize(BitUtil::Log2(max_buffer_size_scaled) + 1);
  if (num_local_disks == 0) num_local_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_local_disks + REMOTE_NUM_DISKS);
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
    for (list<RequestContext*>::iterator it = disk_queues_[i]->request_contexts.begin();
        it != disk_queues_[i]->request_contexts.end(); ++it) {
      DCHECK_EQ((*it)->disk_states_[disk_id].num_threads_in_op(), 0);
      DCHECK((*it)->disk_states_[disk_id].done());
      (*it)->DecrementDiskRefCount();
    }
  }

  DCHECK(request_context_cache_.get() == NULL ||
      request_context_cache_->ValidateAllInactive())
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
  process_mem_tracker->AddGcFunction(bind(&DiskIoMgr::GcIoBuffers, this));

  for (int i = 0; i < disk_queues_.size(); ++i) {
    disk_queues_[i] = new DiskQueue(i);
    int num_threads_per_disk;
    if (i == RemoteDfsDiskId()) {
      num_threads_per_disk = FLAGS_num_remote_hdfs_io_threads;
    } else if (i == RemoteS3DiskId()) {
      num_threads_per_disk = FLAGS_num_s3_io_threads;
    } else if (num_threads_per_disk_ != 0) {
      num_threads_per_disk = num_threads_per_disk_;
    } else if (DiskInfo::is_rotational(i)) {
      num_threads_per_disk = THREADS_PER_ROTATIONAL_DISK;
    } else {
      num_threads_per_disk = THREADS_PER_FLASH_DISK;
    }
    for (int j = 0; j < num_threads_per_disk; ++j) {
      stringstream ss;
      ss << "work-loop(Disk: " << i << ", Thread: " << j << ")";
      disk_thread_group_.AddThread(new Thread("disk-io-mgr", ss.str(),
          &DiskIoMgr::WorkLoop, this, disk_queues_[i]));
    }
  }
  request_context_cache_.reset(new RequestContextCache(this));

  cached_read_options_ = hadoopRzOptionsAlloc();
  DCHECK(cached_read_options_ != NULL);
  // Disable checksumming for cached reads.
  int ret = hadoopRzOptionsSetSkipChecksum(cached_read_options_, true);
  DCHECK_EQ(ret, 0);
  // Disable automatic fallback for cached reads.
  ret = hadoopRzOptionsSetByteBufferPool(cached_read_options_, NULL);
  DCHECK_EQ(ret, 0);

  return Status::OK();
}

Status DiskIoMgr::RegisterContext(RequestContext** request_context,
    MemTracker* mem_tracker) {
  DCHECK(request_context_cache_.get() != NULL) << "Must call Init() first.";
  *request_context = request_context_cache_->GetNewContext();
  (*request_context)->Reset(mem_tracker);
  return Status::OK();
}

void DiskIoMgr::UnregisterContext(RequestContext* reader) {
  // Blocking cancel (waiting for disks completion).
  CancelContext(reader, true);

  // All the disks are done with clean, validate nothing is leaking.
  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK_EQ(reader->num_buffers_in_reader_, 0) << endl << reader->DebugString();
  DCHECK_EQ(reader->num_used_buffers_, 0) << endl << reader->DebugString();

  DCHECK(reader->Validate()) << endl << reader->DebugString();
  request_context_cache_->ReturnContext(reader);
}

// Cancellation requires coordination from multiple threads.  Each thread that currently
// has a reference to the request context must notice the cancel and remove it from its
// tracking structures.  The last thread to touch the context should deallocate (aka
// recycle) the request context object.  Potential threads are:
//  1. Disk threads that are currently reading for this reader.
//  2. Caller threads that are waiting in GetNext.
//
// The steps are:
// 1. Cancel will immediately set the context in the Cancelled state.  This prevents any
// other thread from adding more ready buffers to the context (they all take a lock and
// check the state before doing so), or any write ranges to the context.
// 2. Cancel will call cancel on each ScanRange that is not yet complete, unblocking
// any threads in GetNext(). The reader will see the cancelled Status returned. Cancel
// also invokes the callback for the WriteRanges with the cancelled state.
// 3. Disk threads notice the context is cancelled either when picking the next context
// to process or when they try to enqueue a ready buffer.  Upon noticing the cancelled
// state, removes the context from the disk queue.  The last thread per disk with an
// outstanding reference to the context decrements the number of disk queues the context
// is on.
// If wait_for_disks_completion is true, wait for the number of active disks to become 0.
void DiskIoMgr::CancelContext(RequestContext* context, bool wait_for_disks_completion) {
  context->Cancel(Status::CANCELLED);

  if (wait_for_disks_completion) {
    unique_lock<mutex> lock(context->lock_);
    DCHECK(context->Validate()) << endl << context->DebugString();
    while (context->num_disks_with_ranges_ > 0) {
      context->disks_complete_cond_var_.wait(lock);
    }
  }
}

void DiskIoMgr::set_read_timer(RequestContext* r, RuntimeProfile::Counter* c) {
  r->read_timer_ = c;
}

void DiskIoMgr::set_bytes_read_counter(RequestContext* r, RuntimeProfile::Counter* c) {
  r->bytes_read_counter_ = c;
}

void DiskIoMgr::set_active_read_thread_counter(RequestContext* r,
    RuntimeProfile::Counter* c) {
  r->active_read_thread_counter_ = c;
}

void DiskIoMgr::set_disks_access_bitmap(RequestContext* r,
    RuntimeProfile::Counter* c) {
  r->disks_accessed_bitmap_ = c;
}

int64_t DiskIoMgr::queue_size(RequestContext* reader) const {
  return reader->num_ready_buffers_;
}

Status DiskIoMgr::context_status(RequestContext* context) const {
  unique_lock<mutex> lock(context->lock_);
  return context->status_;
}

int DiskIoMgr::num_unstarted_ranges(RequestContext* reader) const {
  return reader->num_unstarted_scan_ranges_;
}

int64_t DiskIoMgr::bytes_read_local(RequestContext* reader) const {
  return reader->bytes_read_local_;
}

int64_t DiskIoMgr::bytes_read_short_circuit(RequestContext* reader) const {
  return reader->bytes_read_short_circuit_;
}

int64_t DiskIoMgr::bytes_read_dn_cache(RequestContext* reader) const {
  return reader->bytes_read_dn_cache_;
}

int DiskIoMgr::num_remote_ranges(RequestContext* reader) const {
  return reader->num_remote_ranges_;
}

int64_t DiskIoMgr::unexpected_remote_bytes(RequestContext* reader) const {
  return reader->unexpected_remote_bytes_;
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
  return Status::OK();
}

Status DiskIoMgr::AddScanRanges(RequestContext* reader,
    const vector<ScanRange*>& ranges, bool schedule_immediately) {
  if (ranges.empty()) return Status::OK();

  // Validate and initialize all ranges
  for (int i = 0; i < ranges.size(); ++i) {
    RETURN_IF_ERROR(ValidateScanRange(ranges[i]));
    ranges[i]->InitInternal(this, reader);
  }

  // disks that this reader needs to be scheduled on.
  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();

  if (reader->state_ == RequestContext::Cancelled) {
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
        // Cached read failed, fall back to AddRequestRange() below.
      } else {
        reader->cached_ranges_.Enqueue(range);
        continue;
      }
    }
    reader->AddRequestRange(range, schedule_immediately);
  }
  DCHECK(reader->Validate()) << endl << reader->DebugString();

  return Status::OK();
}

// This function returns the next scan range the reader should work on, checking
// for eos and error cases. If there isn't already a cached scan range or a scan
// range prepared by the disk threads, the caller waits on the disk threads.
Status DiskIoMgr::GetNextRange(RequestContext* reader, ScanRange** range) {
  DCHECK(reader != NULL);
  DCHECK(range != NULL);
  *range = NULL;
  Status status = Status::OK();

  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();

  while (true) {
    if (reader->state_ == RequestContext::Cancelled) {
      DCHECK(!reader->status_.ok());
      status = reader->status_;
      break;
    }

    if (reader->num_unstarted_scan_ranges_ == 0 &&
        reader->ready_to_start_ranges_.empty() && reader->cached_ranges_.empty()) {
      // All ranges are done, just return.
      break;
    }

    if (!reader->cached_ranges_.empty()) {
      // We have a cached range.
      *range = reader->cached_ranges_.Dequeue();
      DCHECK((*range)->try_cache_);
      bool cached_read_succeeded;
      RETURN_IF_ERROR((*range)->ReadFromCache(&cached_read_succeeded));
      if (cached_read_succeeded) return Status::OK();

      // This range ended up not being cached. Loop again and pick up a new range.
      reader->AddRequestRange(*range, false);
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
      DCHECK_EQ(*range, reader->disk_states_[disk_id].next_scan_range_to_start());
      // Set this to NULL, the next time this disk runs for this reader, it will
      // get another range ready.
      reader->disk_states_[disk_id].set_next_scan_range_to_start(NULL);
      reader->ScheduleScanRange(*range);
      break;
    }
  }
  return status;
}

Status DiskIoMgr::Read(RequestContext* reader,
    ScanRange* range, BufferDescriptor** buffer) {
  DCHECK(range != NULL);
  DCHECK(buffer != NULL);
  *buffer = NULL;

  if (range->len() > max_buffer_size_) {
    return Status(Substitute("Cannot perform sync read larger than $0. Request was $1",
                             max_buffer_size_, range->len()));
  }

  vector<DiskIoMgr::ScanRange*> ranges;
  ranges.push_back(range);
  RETURN_IF_ERROR(AddScanRanges(reader, ranges, true));
  RETURN_IF_ERROR(range->GetNext(buffer));
  DCHECK((*buffer) != NULL);
  DCHECK((*buffer)->eosr());
  return Status::OK();
}

void DiskIoMgr::ReturnBuffer(BufferDescriptor* buffer_desc) {
  DCHECK(buffer_desc != NULL);
  if (!buffer_desc->status_.ok()) DCHECK(buffer_desc->buffer_ == NULL);

  RequestContext* reader = buffer_desc->reader_;
  if (buffer_desc->buffer_ != NULL) {
    if (buffer_desc->scan_range_->cached_buffer_ == NULL) {
      // Not a cached buffer. Return the io buffer and update mem tracking.
      ReturnFreeBuffer(buffer_desc);
    }
    buffer_desc->buffer_ = NULL;
    --num_buffers_in_readers_;
    --reader->num_buffers_in_reader_;
  } else {
    // A NULL buffer means there was an error in which case there is no buffer
    // to return.
  }

  if (buffer_desc->eosr_ || buffer_desc->scan_range_->is_cancelled_) {
    // Need to close the scan range if returning the last buffer or the scan range
    // has been cancelled (and the caller might never get the last buffer).
    // Close() is idempotent so multiple cancelled buffers is okay.
    buffer_desc->scan_range_->Close();
  }
  ReturnBufferDesc(buffer_desc);
}

void DiskIoMgr::ReturnBufferDesc(BufferDescriptor* desc) {
  DCHECK(desc != NULL);
  unique_lock<mutex> lock(free_buffers_lock_);
  DCHECK(find(free_buffer_descs_.begin(), free_buffer_descs_.end(), desc)
         == free_buffer_descs_.end());
  free_buffer_descs_.push_back(desc);
}

DiskIoMgr::BufferDescriptor* DiskIoMgr::GetBufferDesc(
    RequestContext* reader, ScanRange* range, char* buffer, int64_t buffer_size) {
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
    ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS->set_value(0);
  }
}

void DiskIoMgr::ReturnFreeBuffer(BufferDescriptor* desc) {
  ReturnFreeBuffer(desc->buffer_, desc->buffer_len_);
  desc->SetMemTracker(NULL);
  desc->buffer_ = NULL;
}

void DiskIoMgr::ReturnFreeBuffer(char* buffer, int64_t buffer_size) {
  DCHECK(buffer != NULL);
  int idx = free_buffers_idx(buffer_size);
  DCHECK_EQ(BitUtil::Ceil(buffer_size, min_buffer_size_) & ~(1 << idx), 0)
      << "buffer_size_ / min_buffer_size_ should be power of 2, got buffer_size = "
      << buffer_size << ", min_buffer_size_ = " << min_buffer_size_;
  unique_lock<mutex> lock(free_buffers_lock_);
  if (!FLAGS_disable_mem_pools && free_buffers_[idx].size() < FLAGS_max_free_io_buffers) {
    free_buffers_[idx].push_back(buffer);
    if (ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS != NULL) {
      ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS->Increment(1L);
    }
  } else {
    process_mem_tracker_->Release(buffer_size);
    --num_allocated_buffers_;
    delete[] buffer;
    if (ImpaladMetrics::IO_MGR_NUM_BUFFERS != NULL) {
      ImpaladMetrics::IO_MGR_NUM_BUFFERS->Increment(-1L);
    }
    if (ImpaladMetrics::IO_MGR_TOTAL_BYTES != NULL) {
      ImpaladMetrics::IO_MGR_TOTAL_BYTES->Increment(-buffer_size);
    }
  }
}

// This function gets the next RequestRange to work on for this disk. It checks for
// cancellation and
// a) Updates ready_to_start_ranges if there are no scan ranges queued for this disk.
// b) Adds an unstarted write range to in_flight_ranges_. The write range is processed
//    immediately if there are no preceding scan ranges in in_flight_ranges_
// It blocks until work is available or the thread is shut down.
// Work is available if there is a RequestContext with
//  - A ScanRange with a buffer available, or
//  - A WriteRange in unstarted_write_ranges_.
bool DiskIoMgr::GetNextRequestRange(DiskQueue* disk_queue, RequestRange** range,
    RequestContext** request_context) {
  int disk_id = disk_queue->disk_id;
  *range = NULL;

  // This loops returns either with work to do or when the disk IoMgr shuts down.
  while (true) {
    *request_context = NULL;
    RequestContext::PerDiskState* request_disk_state = NULL;
    {
      unique_lock<mutex> disk_lock(disk_queue->lock);

      while (!shut_down_ && disk_queue->request_contexts.empty()) {
        // wait if there are no readers on the queue
        disk_queue->work_available.wait(disk_lock);
      }
      if (shut_down_) break;
      DCHECK(!disk_queue->request_contexts.empty());

      // Get the next reader and remove the reader so that another disk thread
      // can't pick it up.  It will be enqueued before issuing the read to HDFS
      // so this is not a big deal (i.e. multiple disk threads can read for the
      // same reader).
      // TODO: revisit.
      *request_context = disk_queue->request_contexts.front();
      disk_queue->request_contexts.pop_front();
      DCHECK(*request_context != NULL);
      request_disk_state = &((*request_context)->disk_states_[disk_id]);
      request_disk_state->IncrementRequestThreadAndDequeue();
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
    bool reader_limit_exceeded = (*request_context)->mem_tracker_ != NULL
        ? (*request_context)->mem_tracker_->AnyLimitExceeded() : false;

    if (process_limit_exceeded || reader_limit_exceeded) {
      (*request_context)->Cancel(Status::MemLimitExceeded());
    }

    unique_lock<mutex> request_lock((*request_context)->lock_);
    VLOG_FILE << "Disk (id=" << disk_id << ") reading for "
        << (*request_context)->DebugString();

    // Check if reader has been cancelled
    if ((*request_context)->state_ == RequestContext::Cancelled) {
      request_disk_state->DecrementRequestThreadAndCheckDone(*request_context);
      continue;
    }

    DCHECK_EQ((*request_context)->state_, RequestContext::Active)
        << (*request_context)->DebugString();

    if (request_disk_state->next_scan_range_to_start() == NULL &&
        !request_disk_state->unstarted_scan_ranges()->empty()) {
      // We don't have a range queued for this disk for what the caller should
      // read next. Populate that.  We want to have one range waiting to minimize
      // wait time in GetNextRange.
      ScanRange* new_range = request_disk_state->unstarted_scan_ranges()->Dequeue();
      --(*request_context)->num_unstarted_scan_ranges_;
      (*request_context)->ready_to_start_ranges_.Enqueue(new_range);
      request_disk_state->set_next_scan_range_to_start(new_range);

      if ((*request_context)->num_unstarted_scan_ranges_ == 0) {
        // All the ranges have been started, notify everyone blocked on GetNextRange.
        // Only one of them will get work so make sure to return NULL to the other
        // caller threads.
        (*request_context)->ready_to_start_ranges_cv_.notify_all();
      } else {
        (*request_context)->ready_to_start_ranges_cv_.notify_one();
      }
    }

    // Always enqueue a WriteRange to be processed into in_flight_ranges_.
    // This is done so in_flight_ranges_ does not exclusively contain ScanRanges.
    // For now, enqueuing a WriteRange on each invocation of GetNextRequestRange()
    // does not flood in_flight_ranges() with WriteRanges because the entire
    // WriteRange is processed and removed from the queue after GetNextRequestRange()
    // returns. (A DCHECK is used to ensure that writes do not exceed 8MB).
    if (!request_disk_state->unstarted_write_ranges()->empty()) {
      WriteRange* write_range = request_disk_state->unstarted_write_ranges()->Dequeue();
      request_disk_state->in_flight_ranges()->Enqueue(write_range);
    }

    // Get the next scan range to work on from the reader. Only in_flight_ranges
    // are eligible since the disk threads do not start new ranges on their own.

    // There are no inflight ranges, nothing to do.
    if (request_disk_state->in_flight_ranges()->empty()) {
      request_disk_state->DecrementRequestThread();
      continue;
    }
    DCHECK_GT(request_disk_state->num_remaining_ranges(), 0);
    *range = request_disk_state->in_flight_ranges()->Dequeue();
    DCHECK(*range != NULL);

    // Now that we've picked a request range, put the context back on the queue so
    // another thread can pick up another request range for this context.
    request_disk_state->ScheduleContext(*request_context, disk_id);
    DCHECK((*request_context)->Validate()) << endl << (*request_context)->DebugString();
    return true;
  }

  DCHECK(shut_down_);
  return false;
}

void DiskIoMgr::HandleWriteFinished(RequestContext* writer, WriteRange* write_range,
    const Status& write_status) {
  // Execute the callback before decrementing the thread count. Otherwise CancelContext()
  // that waits for the disk ref count to be 0 will return, creating a race, e.g.
  // between BufferedBlockMgr::WriteComplete() and BufferedBlockMgr::~BufferedBlockMgr().
  // See IMPALA-1890.
  // The status of the write does not affect the status of the writer context.
  write_range->callback_(write_status);
  {
    unique_lock<mutex> writer_lock(writer->lock_);
    DCHECK(writer->Validate()) << endl << writer->DebugString();
    RequestContext::PerDiskState& state = writer->disk_states_[write_range->disk_id_];
    if (writer->state_ == RequestContext::Cancelled) {
      state.DecrementRequestThreadAndCheckDone(writer);
    } else {
      state.DecrementRequestThread();
    }
    --state.num_remaining_ranges();
  }
}

void DiskIoMgr::HandleReadFinished(DiskQueue* disk_queue, RequestContext* reader,
    BufferDescriptor* buffer) {
  unique_lock<mutex> reader_lock(reader->lock_);

  RequestContext::PerDiskState& state = reader->disk_states_[disk_queue->disk_id];
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  DCHECK_GT(state.num_threads_in_op(), 0);
  DCHECK(buffer->buffer_ != NULL);

  if (reader->state_ == RequestContext::Cancelled) {
    state.DecrementRequestThreadAndCheckDone(reader);
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    ReturnFreeBuffer(buffer);
    buffer->buffer_ = NULL;
    buffer->scan_range_->Cancel(reader->status_);
    // Enqueue the buffer to use the scan range's buffer cleanup path.
    buffer->scan_range_->EnqueueBuffer(buffer);
    return;
  }

  DCHECK_EQ(reader->state_, RequestContext::Active);
  DCHECK(buffer->buffer_ != NULL);

  // Update the reader's scan ranges.  There are a three cases here:
  //  1. Read error
  //  2. End of scan range
  //  3. Middle of scan range
  if (!buffer->status_.ok()) {
    // Error case
    ReturnFreeBuffer(buffer);
    buffer->eosr_ = true;
    --state.num_remaining_ranges();
    buffer->scan_range_->Cancel(buffer->status_);
  } else if (buffer->eosr_) {
    --state.num_remaining_ranges();
  }

  // After calling EnqueueBuffer(), it is no longer valid to read from buffer.
  // Store the state we need before calling EnqueueBuffer().
  bool eosr = buffer->eosr_;
  ScanRange* scan_range = buffer->scan_range_;
  bool queue_full = buffer->scan_range_->EnqueueBuffer(buffer);
  if (eosr) {
    // For cached buffers, we can't close the range until the cached buffer is returned.
    // Close() is called from DiskIoMgr::ReturnBuffer().
    if (scan_range->cached_buffer_ == NULL) scan_range->Close();
  } else {
    if (queue_full) {
      reader->blocked_ranges_.Enqueue(scan_range);
    } else {
      reader->ScheduleScanRange(scan_range);
    }
  }
  state.DecrementRequestThread();
}

void DiskIoMgr::WorkLoop(DiskQueue* disk_queue) {
  // The thread waits until there is work or the entire system is being shut down.
  // If there is work, performs the read or write requested and re-enqueues the
  // requesting context.
  // Locks are not taken when reading from or writing to disk.
  // The main loop has three parts:
  //   1. GetNextRequestContext(): get the next request context (read or write) to
  //      process and dequeue it.
  //   2. For the dequeued request, gets the next scan- or write-range to process and
  //      re-enqueues the request.
  //   3. Perform the read or write as specified.
  // Cancellation checking needs to happen in both steps 1 and 3.
  while (true) {
    RequestContext* worker_context = NULL;;
    RequestRange* range = NULL;

    if (!GetNextRequestRange(disk_queue, &range, &worker_context)) {
      DCHECK(shut_down_);
      break;
    }

    if (range->request_type() == RequestType::READ) {
      ReadRange(disk_queue, worker_context, static_cast<ScanRange*>(range));
    } else {
      DCHECK(range->request_type() == RequestType::WRITE);
      Write(worker_context, static_cast<WriteRange*>(range));
    }
  }

  DCHECK(shut_down_);
}

// This function reads the specified scan range associated with the
// specified reader context and disk queue.
void DiskIoMgr::ReadRange(DiskQueue* disk_queue, RequestContext* reader,
    ScanRange* range) {
  char* buffer = NULL;
  int64_t bytes_remaining = range->len_ - range->bytes_read_;
  DCHECK_GT(bytes_remaining, 0);
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
    RequestContext::PerDiskState& state = reader->disk_states_[disk_queue->disk_id];
    unique_lock<mutex> reader_lock(reader->lock_);

    // Just grabbed the reader lock, check for cancellation.
    if (reader->state_ == RequestContext::Cancelled) {
      DCHECK(reader->Validate()) << endl << reader->DebugString();
      state.DecrementRequestThreadAndCheckDone(reader);
      range->Cancel(reader->status_);
      DCHECK(reader->Validate()) << endl << reader->DebugString();
      return;
    }

    if (!range->ready_buffers_.empty()) {
      // We have memory pressure and this range doesn't need another buffer
      // (it already has one queued). Skip this range and pick it up later.
      range->blocked_on_queue_ = true;
      reader->blocked_ranges_.Enqueue(range);
      state.DecrementRequestThread();
      return;
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
      reader->active_read_thread_counter_->Add(1L);
    }
    if (reader->disks_accessed_bitmap_) {
      int64_t disk_bit = 1 << disk_queue->disk_id;
      reader->disks_accessed_bitmap_->BitOr(disk_bit);
    }
    SCOPED_TIMER(&read_timer_);
    SCOPED_TIMER(reader->read_timer_);

    buffer_desc->status_ = range->Read(buffer, &buffer_desc->len_, &buffer_desc->eosr_);
    buffer_desc->scan_range_offset_ = range->bytes_read_ - buffer_desc->len_;

    if (reader->bytes_read_counter_ != NULL) {
      COUNTER_ADD(reader->bytes_read_counter_, buffer_desc->len_);
    }

    COUNTER_ADD(&total_bytes_read_counter_, buffer_desc->len_);
    if (reader->active_read_thread_counter_) {
      reader->active_read_thread_counter_->Add(-1L);
    }
  }

  // Finished read, update reader/disk based on the results
  HandleReadFinished(disk_queue, reader, buffer_desc);
}

void DiskIoMgr::Write(RequestContext* writer_context, WriteRange* write_range) {
  FILE* file_handle = fopen(write_range->file(), "rb+");
  Status ret_status;
  if (file_handle == NULL) {
    ret_status = Status(ErrorMsg(TErrorCode::RUNTIME_ERROR,
        Substitute("fopen($0, \"rb+\") failed with errno=$1 description=$2",
            write_range->file_, errno, GetStrErrMsg())));
  } else {
    ret_status = WriteRangeHelper(file_handle, write_range);

    int success = fclose(file_handle);
    if (ret_status.ok() && success != 0) {
      ret_status = Status(ErrorMsg(TErrorCode::RUNTIME_ERROR,
          Substitute("fclose($0) failed", write_range->file_)));
    }
  }

  HandleWriteFinished(writer_context, write_range, ret_status);
}

Status DiskIoMgr::WriteRangeHelper(FILE* file_handle, WriteRange* write_range) {
  // Seek to the correct offset and perform the write.
  int success = fseek(file_handle, write_range->offset(), SEEK_SET);
  if (success != 0) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR,
        Substitute("fseek($0, $1, SEEK_SET) failed with errno=$2 description=$3",
        write_range->file_, write_range->offset(), errno, GetStrErrMsg())));
  }

  int64_t bytes_written = fwrite(write_range->data_, 1, write_range->len_, file_handle);
  if (bytes_written < write_range->len_) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR,
        Substitute("fwrite(buffer, 1, $0, $1) failed with errno=$2 description=$3",
        write_range->len_, write_range->file_, errno, GetStrErrMsg())));
  }
  if (ImpaladMetrics::IO_MGR_BYTES_WRITTEN != NULL) {
    ImpaladMetrics::IO_MGR_BYTES_WRITTEN->Increment(write_range->len_);
  }

  return Status::OK();
}

int DiskIoMgr::free_buffers_idx(int64_t buffer_size) {
  int64_t buffer_size_scaled = BitUtil::Ceil(buffer_size, min_buffer_size_);
  int idx = BitUtil::Log2(buffer_size_scaled);
  DCHECK_GE(idx, 0);
  DCHECK_LT(idx, free_buffers_.size());
  return idx;
}

Status DiskIoMgr::AddWriteRange(RequestContext* writer, WriteRange* write_range) {
  DCHECK_LE(write_range->len(), max_buffer_size_);
  unique_lock<mutex> writer_lock(writer->lock_);

  if (writer->state_ == RequestContext::Cancelled) {
    DCHECK(!writer->status_.ok());
    return writer->status_;
  }

  writer->AddRequestRange(write_range, false);
  return Status::OK();
}

int DiskIoMgr::AssignQueue(const char* file, int disk_id, bool expected_local) {
  // If it's a remote range, check for an appropriate remote disk queue.
  if (!expected_local) {
    if (IsDfsPath(file) && FLAGS_num_remote_hdfs_io_threads > 0) return RemoteDfsDiskId();
    if (IsS3APath(file)) return RemoteS3DiskId();
  }
  // Assign to a local disk queue.
  DCHECK(!IsS3APath(file)); // S3 is always remote.
  if (disk_id == -1) {
    // disk id is unknown, assign it a random one.
    static int next_disk_id = 0;
    disk_id = next_disk_id++;
  }
  // TODO: we need to parse the config for the number of dirs configured for this
  // data node.
  return disk_id % num_local_disks();
}

DiskIoMgr::HdfsCachedFileHandle* DiskIoMgr::OpenHdfsFile(const hdfsFS& fs,
    const char* fname, int64_t mtime) {
  HdfsCachedFileHandle* fh = NULL;

  // Check if a cached file handle exists and validate the mtime, if the mtime of the
  // cached handle is not matching the mtime of the requested file, reopen.
  if (detail::is_file_handle_caching_enabled() && file_handle_cache_.Pop(fname, &fh)) {
    ImpaladMetrics::IO_MGR_NUM_CACHED_FILE_HANDLES->Increment(-1L);
    if (fh->mtime() == mtime) {
      ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO->Update(1L);
      ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_COUNT->Increment(1L);
      ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(1L);
      return fh;
    }
    VLOG_FILE << "mtime mismatch, closing cached file handle. Closing file=" << fname;
    delete fh;
  }

  // Update cache hit ratio
  ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO->Update(0L);
  ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT->Increment(1L);
  fh = new HdfsCachedFileHandle(fs, fname, mtime);

  // Check if the file handle was opened correctly
  if (!fh->ok())  {
    VLOG_FILE << "Opening the file " << fname << " failed.";
    delete fh;
    return NULL;
  }

  ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(1L);
  return fh;
}

void DiskIoMgr::CacheOrCloseFileHandle(const char* fname,
    DiskIoMgr::HdfsCachedFileHandle* fid, bool close) {
  ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(-1L);
  // Try to unbuffer the handle, on filesystems that do not support this call a non-zero
  // return code indicates that the operation was not successful and thus the file is
  // closed.
  if (detail::is_file_handle_caching_enabled() &&
      !close && hdfsUnbufferFile(fid->file()) == 0) {
    // Clear read statistics before returning
    hdfsFileClearReadStatistics(fid->file());
    file_handle_cache_.Put(fname, fid);
    ImpaladMetrics::IO_MGR_NUM_CACHED_FILE_HANDLES->Increment(1L);
  } else {
    if (close) {
      VLOG_FILE << "Closing file=" << fname;
    } else {
      VLOG_FILE << "FS does not support file handle unbuffering, closing file="
                << fname;
    }
    delete fid;
  }
}
