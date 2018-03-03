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

#include "common/global-flags.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/handle-cache.inline.h"

#include <boost/algorithm/string.hpp>

#include "gutil/strings/substitute.h"
#include "util/bit-util.h"
#include "util/hdfs-util.h"
#include "util/time.h"

DECLARE_bool(disable_mem_pools);
#ifndef NDEBUG
DECLARE_int32(stress_scratch_write_delay_ms);
#endif

#include "common/names.h"

using namespace impala;
using namespace impala::io;
using namespace strings;

using std::to_string;

// Control the number of disks on the machine.  If 0, this comes from the system
// settings.
DEFINE_int32(num_disks, 0, "Number of disks on data node.");
// Default IoMgr configs:
// The maximum number of the threads per disk is also the max queue depth per disk.
DEFINE_int32(num_threads_per_disk, 0, "Number of I/O threads per disk");

// Rotational disks should have 1 thread per disk to minimize seeks.  Non-rotational
// don't have this penalty and benefit from multiple concurrent IO requests.
static const int THREADS_PER_ROTATIONAL_DISK = 1;
static const int THREADS_PER_SOLID_STATE_DISK = 8;

// The maximum number of the threads per rotational disk is also the max queue depth per
// rotational disk.
static const string num_io_threads_per_rotational_disk_help_msg = Substitute("Number of "
    "I/O threads per rotational disk. Has priority over num_threads_per_disk. If neither"
    " is set, defaults to $0 thread(s) per rotational disk", THREADS_PER_ROTATIONAL_DISK);
DEFINE_int32(num_io_threads_per_rotational_disk, 0,
    num_io_threads_per_rotational_disk_help_msg.c_str());
// The maximum number of the threads per solid state disk is also the max queue depth per
// solid state disk.
static const string num_io_threads_per_solid_state_disk_help_msg = Substitute("Number of"
    " I/O threads per solid state disk. Has priority over num_threads_per_disk. If "
    "neither is set, defaults to $0 thread(s) per solid state disk",
    THREADS_PER_SOLID_STATE_DISK);
DEFINE_int32(num_io_threads_per_solid_state_disk, 0,
    num_io_threads_per_solid_state_disk_help_msg.c_str());
// The maximum number of remote HDFS I/O threads.  HDFS access that are expected to be
// remote are placed on a separate remote disk queue.  This is the queue depth for that
// queue.  If 0, then the remote queue is not used and instead ranges are round-robined
// across the local disk queues.
DEFINE_int32(num_remote_hdfs_io_threads, 8, "Number of remote HDFS I/O threads");
// The maximum number of S3 I/O threads. The default value of 16 was chosen emperically
// to maximize S3 throughput. Maximum throughput is achieved with multiple connections
// open to S3 and use of multiple CPU cores since S3 reads are relatively compute
// expensive (SSL and JNI buffer overheads).
DEFINE_int32(num_s3_io_threads, 16, "Number of S3 I/O threads");
// The maximum number of ADLS I/O threads. This number is a good default to have for
// clusters that may vary widely in size, due to an undocumented concurrency limit
// enforced by ADLS for a cluster, which spans between 500-700. For smaller clusters
// (~10 nodes), 64 threads would be more ideal.
DEFINE_int32(num_adls_io_threads, 16, "Number of ADLS I/O threads");

DECLARE_int64(min_buffer_size);

// With 1024B through 8MB buffers, this is up to ~2GB of buffers.
DEFINE_int32(max_free_io_buffers, 128,
    "For each io buffer size, the maximum number of buffers the IoMgr will hold onto");

// The number of cached file handles defines how much memory can be used per backend for
// caching frequently used file handles. Measurements indicate that a single file handle
// uses about 6kB of memory. 20k file handles will thus reserve ~120MB of memory.
// The actual amount of memory that is associated with a file handle can be larger
// or smaller, depending on the replication factor for this file or the path name.
DEFINE_uint64(max_cached_file_handles, 20000, "Maximum number of HDFS file handles "
    "that will be cached. Disabled if set to 0.");

// The unused file handle timeout specifies how long a file handle will remain in the
// cache if it is not being used. Aging out unused handles ensures that the cache is not
// wasting memory on handles that aren't useful. This allows users to specify a larger
// cache size, as the system will only use the memory on useful file handles.
// Additionally, cached file handles keep an open file descriptor for local files.
// If a file is deleted through HDFS, this open file descriptor can keep the disk space
// from being freed. When the metadata sees that a file has been deleted, the file handle
// will no longer be used by future queries. Aging out this file handle allows the
// disk space to be freed in an appropriate period of time. The default value is
// 6 hours. This was chosen to be less than a typical value for HDFS's fs.trash.interval.
// This means that when files are deleted via the trash, the file handle cache will
// have evicted the file handle before the files are flushed from the trash. This
// means that the file handle cache won't impact available disk space.
DEFINE_uint64(unused_file_handle_timeout_sec, 21600, "Maximum time, in seconds, that an "
    "unused HDFS file handle will remain in the file handle cache. Disabled if set "
    "to 0.");

// The file handle cache is split into multiple independent partitions, each with its
// own lock and structures. A larger number of partitions reduces contention by
// concurrent accesses, but it also reduces the efficiency of the cache due to
// separate LRU lists.
// TODO: Test different number of partitions to determine an appropriate default
DEFINE_uint64(num_file_handle_cache_partitions, 16, "Number of partitions used by the "
    "file handle cache.");

// The IoMgr is able to run with a wide range of memory usage. If a query has memory
// remaining less than this value, the IoMgr will stop all buffering regardless of the
// current queue size.
static const int LOW_MEMORY = 64 * 1024 * 1024;

const int DiskIoMgr::SCAN_RANGE_READY_BUFFER_LIMIT;

AtomicInt32 DiskIoMgr::next_disk_id_;

namespace detail {
// Indicates if file handle caching should be used
static inline bool is_file_handle_caching_enabled() {
  return FLAGS_max_cached_file_handles > 0;
}
}

string DiskIoMgr::DebugString() {
  stringstream ss;
  ss << "Disks: " << endl;
  for (int i = 0; i < disk_queues_.size(); ++i) {
    unique_lock<mutex> lock(disk_queues_[i]->lock);
    ss << "  " << (void*) disk_queues_[i] << ":" ;
    if (!disk_queues_[i]->request_contexts.empty()) {
      ss << " Readers: ";
      for (RequestContext* req_context: disk_queues_[i]->request_contexts) {
        ss << (void*)req_context;
      }
    }
    ss << endl;
  }
  return ss.str();
}

BufferDescriptor::BufferDescriptor(DiskIoMgr* io_mgr,
    RequestContext* reader, ScanRange* scan_range, uint8_t* buffer,
    int64_t buffer_len, MemTracker* mem_tracker)
  : io_mgr_(io_mgr),
    reader_(reader),
    mem_tracker_(mem_tracker),
    scan_range_(scan_range),
    buffer_(buffer),
    buffer_len_(buffer_len) {
  DCHECK(io_mgr != nullptr);
  DCHECK(scan_range != nullptr);
  DCHECK(buffer != nullptr);
  DCHECK_GE(buffer_len, 0);
  DCHECK_NE(scan_range->external_buffer_tag_ == ScanRange::ExternalBufferTag::NO_BUFFER,
      mem_tracker == nullptr);
}

void BufferDescriptor::TransferOwnership(MemTracker* dst) {
  DCHECK(dst != nullptr);
  DCHECK(!is_client_buffer());
  // Memory of cached buffers is not tracked against a tracker.
  if (is_cached()) return;
  DCHECK(mem_tracker_ != nullptr);
  dst->Consume(buffer_len_);
  mem_tracker_->Release(buffer_len_);
  mem_tracker_ = dst;
}

WriteRange::WriteRange(
    const string& file, int64_t file_offset, int disk_id, WriteDoneCallback callback)
  : RequestRange(RequestType::WRITE), callback_(callback) {
  SetRange(file, file_offset, disk_id);
}

void WriteRange::SetRange(
    const std::string& file, int64_t file_offset, int disk_id) {
  file_ = file;
  offset_ = file_offset;
  disk_id_ = disk_id;
}

void WriteRange::SetData(const uint8_t* buffer, int64_t len) {
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

// Utility function to select flag that is set (has a positive value) based on precedence
static inline int GetFirstPositiveVal(const int first_val, const int second_val,
    const int default_val) {
  return first_val > 0 ? first_val : (second_val > 0 ? second_val : default_val);
}

DiskIoMgr::DiskIoMgr() :
    num_io_threads_per_rotational_disk_(GetFirstPositiveVal(
        FLAGS_num_io_threads_per_rotational_disk, FLAGS_num_threads_per_disk,
        THREADS_PER_ROTATIONAL_DISK)),
    num_io_threads_per_solid_state_disk_(GetFirstPositiveVal(
        FLAGS_num_io_threads_per_solid_state_disk, FLAGS_num_threads_per_disk,
        THREADS_PER_SOLID_STATE_DISK)),
    max_buffer_size_(FLAGS_read_size),
    min_buffer_size_(FLAGS_min_buffer_size),
    shut_down_(false),
    total_bytes_read_counter_(TUnit::BYTES),
    read_timer_(TUnit::TIME_NS),
    file_handle_cache_(min(FLAGS_max_cached_file_handles,
        FileSystemUtil::MaxNumFileHandles()),
        FLAGS_num_file_handle_cache_partitions,
        FLAGS_unused_file_handle_timeout_sec) {
  DCHECK_LE(READ_SIZE_MIN_VALUE, FLAGS_read_size);
  int64_t max_buffer_size_scaled = BitUtil::Ceil(max_buffer_size_, min_buffer_size_);
  free_buffers_.resize(BitUtil::Log2Ceiling64(max_buffer_size_scaled) + 1);
  int num_local_disks = DiskInfo::num_disks();
  if (FLAGS_num_disks < 0 || FLAGS_num_disks > DiskInfo::num_disks()) {
    LOG(WARNING) << "Number of disks specified should be between 0 and the number of "
        "logical disks on the system. Defaulting to system setting of " <<
        DiskInfo::num_disks() << " disks";
  } else if (FLAGS_num_disks > 0) {
    num_local_disks = FLAGS_num_disks;
  }
  disk_queues_.resize(num_local_disks + REMOTE_NUM_DISKS);
  CheckSseSupport();
}

DiskIoMgr::DiskIoMgr(int num_local_disks, int threads_per_rotational_disk,
    int threads_per_solid_state_disk, int min_buffer_size, int max_buffer_size) :
    num_io_threads_per_rotational_disk_(threads_per_rotational_disk),
    num_io_threads_per_solid_state_disk_(threads_per_solid_state_disk),
    max_buffer_size_(max_buffer_size),
    min_buffer_size_(min_buffer_size),
    shut_down_(false),
    total_bytes_read_counter_(TUnit::BYTES),
    read_timer_(TUnit::TIME_NS),
    file_handle_cache_(min(FLAGS_max_cached_file_handles,
        FileSystemUtil::MaxNumFileHandles()),
        FLAGS_num_file_handle_cache_partitions,
        FLAGS_unused_file_handle_timeout_sec) {
  int64_t max_buffer_size_scaled = BitUtil::Ceil(max_buffer_size_, min_buffer_size_);
  free_buffers_.resize(BitUtil::Log2Ceiling64(max_buffer_size_scaled) + 1);
  if (num_local_disks == 0) num_local_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_local_disks + REMOTE_NUM_DISKS);
  CheckSseSupport();
}

DiskIoMgr::~DiskIoMgr() {
  shut_down_ = true;
  // Notify all worker threads and shut them down.
  for (int i = 0; i < disk_queues_.size(); ++i) {
    if (disk_queues_[i] == nullptr) continue;
    {
      // This lock is necessary to properly use the condition var to notify
      // the disk worker threads.  The readers also grab this lock so updates
      // to shut_down_ are protected.
      unique_lock<mutex> disk_lock(disk_queues_[i]->lock);
    }
    disk_queues_[i]->work_available.NotifyAll();
  }
  disk_thread_group_.JoinAll();

  for (int i = 0; i < disk_queues_.size(); ++i) {
    if (disk_queues_[i] == nullptr) continue;
    int disk_id = disk_queues_[i]->disk_id;
    for (list<RequestContext*>::iterator it = disk_queues_[i]->request_contexts.begin();
        it != disk_queues_[i]->request_contexts.end(); ++it) {
      DCHECK_EQ((*it)->disk_states_[disk_id].num_threads_in_op(), 0);
      DCHECK((*it)->disk_states_[disk_id].done());
      (*it)->DecrementDiskRefCount();
    }
  }

  DCHECK_EQ(num_buffers_in_readers_.Load(), 0);

  // Delete all allocated buffers
  int num_free_buffers = 0;
  for (int idx = 0; idx < free_buffers_.size(); ++idx) {
    num_free_buffers += free_buffers_[idx].size();
  }
  DCHECK_EQ(num_allocated_buffers_.Load(), num_free_buffers);
  GcIoBuffers();

  for (int i = 0; i < disk_queues_.size(); ++i) {
    delete disk_queues_[i];
  }

  if (free_buffer_mem_tracker_ != nullptr) free_buffer_mem_tracker_->Close();
  if (cached_read_options_ != nullptr) hadoopRzOptionsFree(cached_read_options_);
}

Status DiskIoMgr::Init(MemTracker* process_mem_tracker) {
  DCHECK(process_mem_tracker != nullptr);
  free_buffer_mem_tracker_.reset(
      new MemTracker(-1, "Free Disk IO Buffers", process_mem_tracker, false));

  for (int i = 0; i < disk_queues_.size(); ++i) {
    disk_queues_[i] = new DiskQueue(i);
    int num_threads_per_disk;
    string device_name;
    if (i == RemoteDfsDiskId()) {
      num_threads_per_disk = FLAGS_num_remote_hdfs_io_threads;
      device_name = "HDFS remote";
    } else if (i == RemoteS3DiskId()) {
      num_threads_per_disk = FLAGS_num_s3_io_threads;
      device_name = "S3 remote";
    } else if (i == RemoteAdlsDiskId()) {
      num_threads_per_disk = FLAGS_num_adls_io_threads;
      device_name = "ADLS remote";
    } else if (DiskInfo::is_rotational(i)) {
      num_threads_per_disk = num_io_threads_per_rotational_disk_;
      // During tests, i may not point to an existing disk.
      device_name = i < DiskInfo::num_disks() ? DiskInfo::device_name(i) : to_string(i);
    } else {
      num_threads_per_disk = num_io_threads_per_solid_state_disk_;
      // During tests, i may not point to an existing disk.
      device_name = i < DiskInfo::num_disks() ? DiskInfo::device_name(i) : to_string(i);
    }
    for (int j = 0; j < num_threads_per_disk; ++j) {
      stringstream ss;
      ss << "work-loop(Disk: " << device_name << ", Thread: " << j << ")";
      std::unique_ptr<Thread> t;
      RETURN_IF_ERROR(Thread::Create("disk-io-mgr", ss.str(), &DiskIoMgr::WorkLoop,
          this, disk_queues_[i], &t));
      disk_thread_group_.AddThread(move(t));
    }
  }
  RETURN_IF_ERROR(file_handle_cache_.Init());

  cached_read_options_ = hadoopRzOptionsAlloc();
  DCHECK(cached_read_options_ != nullptr);
  // Disable checksumming for cached reads.
  int ret = hadoopRzOptionsSetSkipChecksum(cached_read_options_, true);
  DCHECK_EQ(ret, 0);
  // Disable automatic fallback for cached reads.
  ret = hadoopRzOptionsSetByteBufferPool(cached_read_options_, nullptr);
  DCHECK_EQ(ret, 0);

  return Status::OK();
}

unique_ptr<RequestContext> DiskIoMgr::RegisterContext(MemTracker* mem_tracker) {
  return unique_ptr<RequestContext>(
      new RequestContext(this, num_total_disks(), mem_tracker));
}

void DiskIoMgr::UnregisterContext(RequestContext* reader) {
  reader->CancelAndMarkInactive();
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
void DiskIoMgr::CancelContext(RequestContext* context) {
  context->Cancel(Status::CANCELLED);
}

void DiskIoMgr::set_read_timer(RequestContext* r, RuntimeProfile::Counter* c) {
  r->read_timer_ = c;
}

void DiskIoMgr::set_open_file_timer(RequestContext* r, RuntimeProfile::Counter* c) {
  r->open_file_timer_ = c;
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
  return reader->num_ready_buffers_.Load();
}

Status DiskIoMgr::context_status(RequestContext* context) const {
  unique_lock<mutex> lock(context->lock_);
  return context->status_;
}

int64_t DiskIoMgr::bytes_read_local(RequestContext* reader) const {
  return reader->bytes_read_local_.Load();
}

int64_t DiskIoMgr::bytes_read_short_circuit(RequestContext* reader) const {
  return reader->bytes_read_short_circuit_.Load();
}

int64_t DiskIoMgr::bytes_read_dn_cache(RequestContext* reader) const {
  return reader->bytes_read_dn_cache_.Load();
}

int DiskIoMgr::num_remote_ranges(RequestContext* reader) const {
  return reader->num_remote_ranges_.Load();
}

int64_t DiskIoMgr::unexpected_remote_bytes(RequestContext* reader) const {
  return reader->unexpected_remote_bytes_.Load();
}

int DiskIoMgr::cached_file_handles_hit_count(RequestContext* reader) const {
  return reader->cached_file_handles_hit_count_.Load();
}

int DiskIoMgr::cached_file_handles_miss_count(RequestContext* reader) const {
  return reader->cached_file_handles_miss_count_.Load();
}

int64_t DiskIoMgr::GetReadThroughput() {
  return RuntimeProfile::UnitsPerSecond(&total_bytes_read_counter_, &read_timer_);
}

Status DiskIoMgr::ValidateScanRange(ScanRange* range) {
  int disk_id = range->disk_id_;
  if (disk_id < 0 || disk_id >= disk_queues_.size()) {
    return Status(TErrorCode::DISK_IO_ERROR,
        Substitute("Invalid scan range.  Bad disk id: $0", disk_id));
  }
  if (range->offset_ < 0) {
    return Status(TErrorCode::DISK_IO_ERROR,
        Substitute("Invalid scan range. Negative offset $0", range->offset_));
  }
  if (range->len_ < 0) {
    return Status(TErrorCode::DISK_IO_ERROR,
        Substitute("Invalid scan range. Negative length $0", range->len_));
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
        RETURN_IF_ERROR(range->ReadFromCache(reader_lock, &cached_read_succeeded));
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

Status DiskIoMgr::AddScanRange(
    RequestContext* reader, ScanRange* range, bool schedule_immediately) {
  return AddScanRanges(reader, vector<ScanRange*>({range}), schedule_immediately);
}

// This function returns the next scan range the reader should work on, checking
// for eos and error cases. If there isn't already a cached scan range or a scan
// range prepared by the disk threads, the caller waits on the disk threads.
Status DiskIoMgr::GetNextRange(RequestContext* reader, ScanRange** range) {
  DCHECK(reader != nullptr);
  DCHECK(range != nullptr);
  *range = nullptr;
  Status status = Status::OK();

  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();

  while (true) {
    if (reader->state_ == RequestContext::Cancelled) {
      DCHECK(!reader->status_.ok());
      status = reader->status_;
      break;
    }

    if (reader->num_unstarted_scan_ranges_.Load() == 0 &&
        reader->ready_to_start_ranges_.empty() && reader->cached_ranges_.empty()) {
      // All ranges are done, just return.
      break;
    }

    if (!reader->cached_ranges_.empty()) {
      // We have a cached range.
      *range = reader->cached_ranges_.Dequeue();
      DCHECK((*range)->try_cache_);
      bool cached_read_succeeded;
      RETURN_IF_ERROR((*range)->ReadFromCache(reader_lock, &cached_read_succeeded));
      if (cached_read_succeeded) return Status::OK();

      // This range ended up not being cached. Loop again and pick up a new range.
      reader->AddRequestRange(*range, false);
      DCHECK(reader->Validate()) << endl << reader->DebugString();
      *range = nullptr;
      continue;
    }

    if (reader->ready_to_start_ranges_.empty()) {
      reader->ready_to_start_ranges_cv_.Wait(reader_lock);
    } else {
      *range = reader->ready_to_start_ranges_.Dequeue();
      DCHECK(*range != nullptr);
      int disk_id = (*range)->disk_id();
      DCHECK_EQ(*range, reader->disk_states_[disk_id].next_scan_range_to_start());
      // Set this to nullptr, the next time this disk runs for this reader, it will
      // get another range ready.
      reader->disk_states_[disk_id].set_next_scan_range_to_start(nullptr);
      reader->ScheduleScanRange(*range);
      break;
    }
  }
  return status;
}

Status DiskIoMgr::Read(RequestContext* reader,
    ScanRange* range, std::unique_ptr<BufferDescriptor>* buffer) {
  DCHECK(range != nullptr);
  DCHECK(buffer != nullptr);
  *buffer = nullptr;

  if (range->len() > max_buffer_size_
      && range->external_buffer_tag_ != ScanRange::ExternalBufferTag::CLIENT_BUFFER) {
    return Status(TErrorCode::DISK_IO_ERROR, Substitute("Internal error: cannot "
        "perform sync read of '$0' bytes that is larger than the max read buffer size "
        "'$1'.", range->len(), max_buffer_size_));
  }

  vector<ScanRange*> ranges;
  ranges.push_back(range);
  RETURN_IF_ERROR(AddScanRanges(reader, ranges, true));
  RETURN_IF_ERROR(range->GetNext(buffer));
  DCHECK((*buffer) != nullptr);
  DCHECK((*buffer)->eosr());
  return Status::OK();
}

void DiskIoMgr::ReturnBuffer(unique_ptr<BufferDescriptor> buffer_desc) {
  DCHECK(buffer_desc != nullptr);
  if (!buffer_desc->status_.ok()) DCHECK(buffer_desc->buffer_ == nullptr);

  RequestContext* reader = buffer_desc->reader_;
  if (buffer_desc->buffer_ != nullptr) {
    if (!buffer_desc->is_cached() && !buffer_desc->is_client_buffer()) {
      // Buffers the were not allocated by DiskIoMgr don't need to be freed.
      FreeBufferMemory(buffer_desc.get());
    }
    buffer_desc->buffer_ = nullptr;
    num_buffers_in_readers_.Add(-1);
    reader->num_buffers_in_reader_.Add(-1);
  } else {
    // A nullptr buffer means there was an error in which case there is no buffer
    // to return.
  }

  if (buffer_desc->eosr_ || buffer_desc->scan_range_->is_cancelled_) {
    // Need to close the scan range if returning the last buffer or the scan range
    // has been cancelled (and the caller might never get the last buffer).
    // Close() is idempotent so multiple cancelled buffers is okay.
    buffer_desc->scan_range_->Close();
  }
}

unique_ptr<BufferDescriptor> DiskIoMgr::GetFreeBuffer(
    RequestContext* reader, ScanRange* range, int64_t buffer_size) {
  DCHECK_LE(buffer_size, max_buffer_size_);
  DCHECK_GT(buffer_size, 0);
  buffer_size = min(static_cast<int64_t>(max_buffer_size_), buffer_size);
  int idx = free_buffers_idx(buffer_size);
  // Quantize buffer size to nearest power of 2 greater than the specified buffer size and
  // convert to bytes
  buffer_size = (1LL << idx) * min_buffer_size_;

  // Track memory against the reader. This is checked the next time we start
  // a read for the next reader in DiskIoMgr::GetNextScanRange().
  DCHECK(reader->mem_tracker_ != nullptr);
  reader->mem_tracker_->Consume(buffer_size);

  uint8_t* buffer = nullptr;
  {
    unique_lock<mutex> lock(free_buffers_lock_);
    if (free_buffers_[idx].empty()) {
      num_allocated_buffers_.Add(1);
      if (ImpaladMetrics::IO_MGR_NUM_BUFFERS != nullptr) {
        ImpaladMetrics::IO_MGR_NUM_BUFFERS->Increment(1L);
      }
      if (ImpaladMetrics::IO_MGR_TOTAL_BYTES != nullptr) {
        ImpaladMetrics::IO_MGR_TOTAL_BYTES->Increment(buffer_size);
      }
      // We already tracked this memory against the reader's MemTracker.
      buffer = new uint8_t[buffer_size];
    } else {
      if (ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS != nullptr) {
        ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS->Increment(-1L);
      }
      buffer = free_buffers_[idx].front();
      free_buffers_[idx].pop_front();
      free_buffer_mem_tracker_->Release(buffer_size);
      ASAN_UNPOISON_MEMORY_REGION(buffer, buffer_size);
    }
  }

  // Validate more invariants.
  DCHECK(range != nullptr);
  DCHECK(reader != nullptr);
  DCHECK(buffer != nullptr);
  return unique_ptr<BufferDescriptor>(new BufferDescriptor(
      this, reader, range, buffer, buffer_size, reader->mem_tracker_));
}

void DiskIoMgr::GcIoBuffers(int64_t bytes_to_free) {
  unique_lock<mutex> lock(free_buffers_lock_);
  int buffers_freed = 0;
  int bytes_freed = 0;
  // Free small-to-large to avoid retaining many small buffers and fragmenting memory.
  for (int idx = 0; idx < free_buffers_.size(); ++idx) {
    deque<uint8_t*>* free_buffers = &free_buffers_[idx];
    while (
        !free_buffers->empty() && (bytes_to_free == -1 || bytes_freed <= bytes_to_free)) {
      uint8_t* buffer = free_buffers->front();
      free_buffers->pop_front();
      int64_t buffer_size = (1LL << idx) * min_buffer_size_;
      ASAN_UNPOISON_MEMORY_REGION(buffer, buffer_size);
      delete[] buffer;
      free_buffer_mem_tracker_->Release(buffer_size);
      num_allocated_buffers_.Add(-1);

      ++buffers_freed;
      bytes_freed += buffer_size;
    }
    if (bytes_to_free != -1 && bytes_freed >= bytes_to_free) break;
  }

  if (ImpaladMetrics::IO_MGR_NUM_BUFFERS != nullptr) {
    ImpaladMetrics::IO_MGR_NUM_BUFFERS->Increment(-buffers_freed);
  }
  if (ImpaladMetrics::IO_MGR_TOTAL_BYTES != nullptr) {
    ImpaladMetrics::IO_MGR_TOTAL_BYTES->Increment(-bytes_freed);
  }
  if (ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS != nullptr) {
    ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS->Increment(-buffers_freed);
  }
}

void DiskIoMgr::FreeBufferMemory(BufferDescriptor* desc) {
  DCHECK(!desc->is_cached());
  DCHECK(!desc->is_client_buffer());
  uint8_t* buffer = desc->buffer_;
  int64_t buffer_size = desc->buffer_len_;
  int idx = free_buffers_idx(buffer_size);
  DCHECK_EQ(BitUtil::Ceil(buffer_size, min_buffer_size_) & ~(1LL << idx), 0)
      << "buffer_size_ / min_buffer_size_ should be power of 2, got buffer_size = "
      << buffer_size << ", min_buffer_size_ = " << min_buffer_size_;

  {
    unique_lock<mutex> lock(free_buffers_lock_);
    if (!FLAGS_disable_mem_pools &&
        free_buffers_[idx].size() < FLAGS_max_free_io_buffers) {
      // Poison buffers stored in cache.
      ASAN_POISON_MEMORY_REGION(buffer, buffer_size);
      free_buffers_[idx].push_back(buffer);
      if (ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS != nullptr) {
        ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS->Increment(1L);
      }
      // This consume call needs to be protected by 'free_buffers_lock_' to avoid a race
      // with a Release() call for the same buffer that could make consumption negative.
      // Note: we can't use TryConsume(), which can indirectly call GcIoBuffers().
      // TODO: after IMPALA-3200 is completed, we should be able to leverage the buffer
      // pool's free lists, and remove these free lists.
      free_buffer_mem_tracker_->Consume(buffer_size);
    } else {
      num_allocated_buffers_.Add(-1);
      delete[] buffer;
      if (ImpaladMetrics::IO_MGR_NUM_BUFFERS != nullptr) {
        ImpaladMetrics::IO_MGR_NUM_BUFFERS->Increment(-1L);
      }
      if (ImpaladMetrics::IO_MGR_TOTAL_BYTES != nullptr) {
        ImpaladMetrics::IO_MGR_TOTAL_BYTES->Increment(-buffer_size);
      }
    }
  }

  // We transferred the buffer ownership from the BufferDescriptor to the DiskIoMgr.
  desc->mem_tracker_->Release(buffer_size);
  desc->buffer_ = nullptr;
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
  *range = nullptr;

  // This loops returns either with work to do or when the disk IoMgr shuts down.
  while (true) {
    *request_context = nullptr;
    RequestContext::PerDiskState* request_disk_state = nullptr;
    {
      unique_lock<mutex> disk_lock(disk_queue->lock);

      while (!shut_down_ && disk_queue->request_contexts.empty()) {
        // wait if there are no readers on the queue
        disk_queue->work_available.Wait(disk_lock);
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
      DCHECK(*request_context != nullptr);
      request_disk_state = &((*request_context)->disk_states_[disk_id]);
      request_disk_state->IncrementRequestThreadAndDequeue();
    }

    // NOTE: no locks were taken in between.  We need to be careful about what state
    // could have changed to the reader and disk in between.
    // There are some invariants here.  Only one disk thread can have the
    // same reader here (the reader is removed from the queue).  There can be
    // other disk threads operating on this reader in other functions though.

    // We just picked a reader. Before we may allocate a buffer on its behalf, check that
    // it has not exceeded any memory limits (e.g. the query or process limit).
    // TODO: once IMPALA-3200 is fixed, we should be able to remove the free lists and
    // move these memory limit checks to GetFreeBuffer().
    // Note that calling AnyLimitExceeded() can result in a call to GcIoBuffers().
    // TODO: IMPALA-3209: we should not force a reader over its memory limit by
    // pushing more buffers to it. Most readers can make progress and operate within
    // a fixed memory limit.
    if ((*request_context)->mem_tracker_ != nullptr
        && (*request_context)->mem_tracker_->AnyLimitExceeded()) {
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

    if (request_disk_state->next_scan_range_to_start() == nullptr &&
        !request_disk_state->unstarted_scan_ranges()->empty()) {
      // We don't have a range queued for this disk for what the caller should
      // read next. Populate that.  We want to have one range waiting to minimize
      // wait time in GetNextRange.
      ScanRange* new_range = request_disk_state->unstarted_scan_ranges()->Dequeue();
      (*request_context)->num_unstarted_scan_ranges_.Add(-1);
      (*request_context)->ready_to_start_ranges_.Enqueue(new_range);
      request_disk_state->set_next_scan_range_to_start(new_range);

      if ((*request_context)->num_unstarted_scan_ranges_.Load() == 0) {
        // All the ranges have been started, notify everyone blocked on GetNextRange.
        // Only one of them will get work so make sure to return nullptr to the other
        // caller threads.
        (*request_context)->ready_to_start_ranges_cv_.NotifyAll();
      } else {
        (*request_context)->ready_to_start_ranges_cv_.NotifyOne();
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
    DCHECK(*range != nullptr);

    // Now that we've picked a request range, put the context back on the queue so
    // another thread can pick up another request range for this context.
    request_disk_state->ScheduleContext(*request_context, disk_id);
    DCHECK((*request_context)->Validate()) << endl << (*request_context)->DebugString();
    return true;
  }

  DCHECK(shut_down_);
  return false;
}

void DiskIoMgr::HandleWriteFinished(
    RequestContext* writer, WriteRange* write_range, const Status& write_status) {
  // Copy disk_id before running callback: the callback may modify write_range.
  int disk_id = write_range->disk_id_;

  // Execute the callback before decrementing the thread count. Otherwise CancelContext()
  // that waits for the disk ref count to be 0 will return, creating a race, e.g. see
  // IMPALA-1890.
  // The status of the write does not affect the status of the writer context.
  write_range->callback_(write_status);
  {
    unique_lock<mutex> writer_lock(writer->lock_);
    DCHECK(writer->Validate()) << endl << writer->DebugString();
    RequestContext::PerDiskState& state = writer->disk_states_[disk_id];
    if (writer->state_ == RequestContext::Cancelled) {
      state.DecrementRequestThreadAndCheckDone(writer);
    } else {
      state.DecrementRequestThread();
    }
    --state.num_remaining_ranges();
  }
}

void DiskIoMgr::HandleReadFinished(DiskQueue* disk_queue, RequestContext* reader,
    unique_ptr<BufferDescriptor> buffer) {
  unique_lock<mutex> reader_lock(reader->lock_);

  RequestContext::PerDiskState& state = reader->disk_states_[disk_queue->disk_id];
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  DCHECK_GT(state.num_threads_in_op(), 0);
  DCHECK(buffer->buffer_ != nullptr);

  if (reader->state_ == RequestContext::Cancelled) {
    state.DecrementRequestThreadAndCheckDone(reader);
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    if (!buffer->is_client_buffer()) FreeBufferMemory(buffer.get());
    buffer->buffer_ = nullptr;
    ScanRange* scan_range = buffer->scan_range_;
    scan_range->Cancel(reader->status_);
    // Enqueue the buffer to use the scan range's buffer cleanup path.
    scan_range->EnqueueBuffer(reader_lock, move(buffer));
    return;
  }

  DCHECK_EQ(reader->state_, RequestContext::Active);
  DCHECK(buffer->buffer_ != nullptr);

  // Update the reader's scan ranges.  There are a three cases here:
  //  1. Read error
  //  2. End of scan range
  //  3. Middle of scan range
  if (!buffer->status_.ok()) {
    // Error case
    if (!buffer->is_client_buffer()) FreeBufferMemory(buffer.get());
    buffer->buffer_ = nullptr;
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
  bool is_cached = buffer->is_cached();
  bool queue_full = scan_range->EnqueueBuffer(reader_lock, move(buffer));
  if (eosr) {
    // For cached buffers, we can't close the range until the cached buffer is returned.
    // Close() is called from DiskIoMgr::ReturnBuffer().
    if (!is_cached) scan_range->Close();
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
    RequestContext* worker_context = nullptr;;
    RequestRange* range = nullptr;

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
void DiskIoMgr::ReadRange(
    DiskQueue* disk_queue, RequestContext* reader, ScanRange* range) {
  int64_t bytes_remaining = range->len_ - range->bytes_read_;
  DCHECK_GT(bytes_remaining, 0);
  unique_ptr<BufferDescriptor> buffer_desc;
  if (range->external_buffer_tag_ == ScanRange::ExternalBufferTag::CLIENT_BUFFER) {
    buffer_desc = unique_ptr<BufferDescriptor>(new BufferDescriptor(this, reader, range,
        range->client_buffer_.data, range->client_buffer_.len, nullptr));
  } else {
    // Need to allocate a buffer to read into.
    int64_t buffer_size = ::min(bytes_remaining, static_cast<int64_t>(max_buffer_size_));
    buffer_desc = TryAllocateNextBufferForRange(disk_queue, reader, range, buffer_size);
    if (buffer_desc == nullptr) return;
  }
  reader->num_used_buffers_.Add(1);

  // No locks in this section.  Only working on local vars.  We don't want to hold a
  // lock across the read call.
  buffer_desc->status_ = range->Open(detail::is_file_handle_caching_enabled());
  if (buffer_desc->status_.ok()) {
    // Update counters.
    if (reader->active_read_thread_counter_) {
      reader->active_read_thread_counter_->Add(1L);
    }
    if (reader->disks_accessed_bitmap_) {
      int64_t disk_bit = 1LL << disk_queue->disk_id;
      reader->disks_accessed_bitmap_->BitOr(disk_bit);
    }

    buffer_desc->status_ = range->Read(buffer_desc->buffer_, buffer_desc->buffer_len_,
        &buffer_desc->len_, &buffer_desc->eosr_);
    buffer_desc->scan_range_offset_ = range->bytes_read_ - buffer_desc->len_;

    if (reader->bytes_read_counter_ != nullptr) {
      COUNTER_ADD(reader->bytes_read_counter_, buffer_desc->len_);
    }

    COUNTER_ADD(&total_bytes_read_counter_, buffer_desc->len_);
    if (reader->active_read_thread_counter_) {
      reader->active_read_thread_counter_->Add(-1L);
    }
  }

  // Finished read, update reader/disk based on the results
  HandleReadFinished(disk_queue, reader, move(buffer_desc));
}

unique_ptr<BufferDescriptor> DiskIoMgr::TryAllocateNextBufferForRange(
    DiskQueue* disk_queue, RequestContext* reader, ScanRange* range,
    int64_t buffer_size) {
  DCHECK(reader->mem_tracker_ != nullptr);
  bool enough_memory = reader->mem_tracker_->SpareCapacity() > LOW_MEMORY;
  if (!enough_memory) {
    // Low memory, GC all the buffers and try again.
    GcIoBuffers();
    enough_memory = reader->mem_tracker_->SpareCapacity() > LOW_MEMORY;
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
      return nullptr;
    }

    if (!range->ready_buffers_.empty()) {
      // We have memory pressure and this range doesn't need another buffer
      // (it already has one queued). Skip this range and pick it up later.
      range->blocked_on_queue_ = true;
      reader->blocked_ranges_.Enqueue(range);
      state.DecrementRequestThread();
      return nullptr;
    } else {
      // We need to get a buffer anyway since there are none queued. The query
      // is likely to fail due to mem limits but there's nothing we can do about that
      // now.
    }
  }
  unique_ptr<BufferDescriptor> buffer_desc = GetFreeBuffer(reader, range, buffer_size);
  DCHECK(buffer_desc != nullptr);
  return buffer_desc;
}

void DiskIoMgr::Write(RequestContext* writer_context, WriteRange* write_range) {
  Status ret_status = Status::OK();
  FILE* file_handle = nullptr;
  // Raw open() syscall will create file if not present when passed these flags.
  int fd = open(write_range->file(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    ret_status = Status(ErrorMsg(TErrorCode::DISK_IO_ERROR,
        Substitute("Opening '$0' for write failed with errno=$1 description=$2",
                                     write_range->file_, errno, GetStrErrMsg())));
  } else {
    file_handle = fdopen(fd, "wb");
    if (file_handle == nullptr) {
      ret_status = Status(ErrorMsg(TErrorCode::DISK_IO_ERROR,
          Substitute("fdopen($0, \"wb\") failed with errno=$1 description=$2", fd, errno,
                                       GetStrErrMsg())));
    }
  }

  if (file_handle != nullptr) {
    ret_status = WriteRangeHelper(file_handle, write_range);

    int success = fclose(file_handle);
    if (ret_status.ok() && success != 0) {
      ret_status = Status(ErrorMsg(TErrorCode::DISK_IO_ERROR,
          Substitute("fclose($0) failed", write_range->file_)));
    }
  }

  HandleWriteFinished(writer_context, write_range, ret_status);
}

Status DiskIoMgr::WriteRangeHelper(FILE* file_handle, WriteRange* write_range) {
  // Seek to the correct offset and perform the write.
  int success = fseek(file_handle, write_range->offset(), SEEK_SET);
  if (success != 0) {
    return Status(ErrorMsg(TErrorCode::DISK_IO_ERROR,
        Substitute("fseek($0, $1, SEEK_SET) failed with errno=$2 description=$3",
        write_range->file_, write_range->offset(), errno, GetStrErrMsg())));
  }

#ifndef NDEBUG
  if (FLAGS_stress_scratch_write_delay_ms > 0) {
    SleepForMs(FLAGS_stress_scratch_write_delay_ms);
  }
#endif
  int64_t bytes_written = fwrite(write_range->data_, 1, write_range->len_, file_handle);
  if (bytes_written < write_range->len_) {
    return Status(ErrorMsg(TErrorCode::DISK_IO_ERROR,
        Substitute("fwrite(buffer, 1, $0, $1) failed with errno=$2 description=$3",
        write_range->len_, write_range->file_, errno, GetStrErrMsg())));
  }
  if (ImpaladMetrics::IO_MGR_BYTES_WRITTEN != nullptr) {
    ImpaladMetrics::IO_MGR_BYTES_WRITTEN->Increment(write_range->len_);
  }

  return Status::OK();
}

int DiskIoMgr::free_buffers_idx(int64_t buffer_size) {
  int64_t buffer_size_scaled = BitUtil::Ceil(buffer_size, min_buffer_size_);
  int idx = BitUtil::Log2Ceiling64(buffer_size_scaled);
  DCHECK_GE(idx, 0);
  DCHECK_LT(idx, free_buffers_.size());
  return idx;
}

Status DiskIoMgr::AddWriteRange(RequestContext* writer, WriteRange* write_range) {
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
    if (IsHdfsPath(file) && FLAGS_num_remote_hdfs_io_threads > 0) {
      return RemoteDfsDiskId();
    }
    if (IsS3APath(file)) return RemoteS3DiskId();
    if (IsADLSPath(file)) return RemoteAdlsDiskId();
  }
  // Assign to a local disk queue.
  DCHECK(!IsS3APath(file)); // S3 is always remote.
  DCHECK(!IsADLSPath(file)); // ADLS is always remote.
  if (disk_id == -1) {
    // disk id is unknown, assign it an arbitrary one.
    disk_id = next_disk_id_.Add(1);
  }
  // TODO: we need to parse the config for the number of dirs configured for this
  // data node.
  return disk_id % num_local_disks();
}

ExclusiveHdfsFileHandle* DiskIoMgr::GetExclusiveHdfsFileHandle(const hdfsFS& fs,
    std::string* fname, int64_t mtime, RequestContext *reader) {
  SCOPED_TIMER(reader->open_file_timer_);
  ExclusiveHdfsFileHandle* fid = new ExclusiveHdfsFileHandle(fs, fname->data(), mtime);
  if (!fid->ok()) {
    VLOG_FILE << "Opening the file " << fname << " failed.";
    delete fid;
    return nullptr;
  }
  ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(1L);
  // Every exclusive file handle is considered a cache miss
  ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO->Update(0L);
  ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT->Increment(1L);
  reader->cached_file_handles_miss_count_.Add(1L);
  return fid;
}

void DiskIoMgr::ReleaseExclusiveHdfsFileHandle(ExclusiveHdfsFileHandle* fid) {
  DCHECK(fid != nullptr);
  ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(-1L);
  delete fid;
}

CachedHdfsFileHandle* DiskIoMgr::GetCachedHdfsFileHandle(const hdfsFS& fs,
    std::string* fname, int64_t mtime, RequestContext *reader) {
  bool cache_hit;
  SCOPED_TIMER(reader->open_file_timer_);
  CachedHdfsFileHandle* fh = file_handle_cache_.GetFileHandle(fs, fname, mtime, false,
      &cache_hit);
  if (fh == nullptr) return nullptr;
  ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(1L);
  if (cache_hit) {
    ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO->Update(1L);
    ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_COUNT->Increment(1L);
    reader->cached_file_handles_hit_count_.Add(1L);
  } else {
    ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO->Update(0L);
    ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT->Increment(1L);
    reader->cached_file_handles_miss_count_.Add(1L);
  }
  return fh;
}

void DiskIoMgr::ReleaseCachedHdfsFileHandle(std::string* fname,
    CachedHdfsFileHandle* fid) {
  file_handle_cache_.ReleaseFileHandle(fname, fid, false);
  ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(-1L);
}

Status DiskIoMgr::ReopenCachedHdfsFileHandle(const hdfsFS& fs, std::string* fname,
    int64_t mtime, RequestContext* reader, CachedHdfsFileHandle** fid) {
  bool cache_hit;
  SCOPED_TIMER(reader->open_file_timer_);
  ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_REOPENED->Increment(1L);
  file_handle_cache_.ReleaseFileHandle(fname, *fid, true);
  // The old handle has been destroyed, so *fid must be overwritten before returning.
  *fid = file_handle_cache_.GetFileHandle(fs, fname, mtime, true,
      &cache_hit);
  if (*fid == nullptr) {
    ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(-1L);
    return Status(TErrorCode::DISK_IO_ERROR,
        GetHdfsErrorMsg("Failed to open HDFS file ", fname->data()));
  }
  DCHECK(!cache_hit);
  return Status::OK();
}
