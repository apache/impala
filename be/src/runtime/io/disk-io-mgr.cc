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

#include "runtime/io/disk-io-mgr.h"

#include "common/global-flags.h"
#include "runtime/exec-env.h"
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

const int64_t DiskIoMgr::IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE;

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
    max_buffer_size_(BitUtil::RoundUpToPowerOfTwo(FLAGS_read_size)),
    min_buffer_size_(BitUtil::RoundDownToPowerOfTwo(FLAGS_min_buffer_size)),
    shut_down_(false),
    total_bytes_read_counter_(TUnit::BYTES),
    read_timer_(TUnit::TIME_NS),
    file_handle_cache_(min(FLAGS_max_cached_file_handles,
        FileSystemUtil::MaxNumFileHandles()),
        FLAGS_num_file_handle_cache_partitions,
        FLAGS_unused_file_handle_timeout_sec) {
  DCHECK_LE(READ_SIZE_MIN_VALUE, FLAGS_read_size);
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
    int threads_per_solid_state_disk, int64_t min_buffer_size, int64_t max_buffer_size) :
    num_io_threads_per_rotational_disk_(threads_per_rotational_disk),
    num_io_threads_per_solid_state_disk_(threads_per_solid_state_disk),
    max_buffer_size_(BitUtil::RoundUpToPowerOfTwo(max_buffer_size)),
    min_buffer_size_(BitUtil::RoundDownToPowerOfTwo(min_buffer_size)),
    shut_down_(false),
    total_bytes_read_counter_(TUnit::BYTES),
    read_timer_(TUnit::TIME_NS),
    file_handle_cache_(min(FLAGS_max_cached_file_handles,
        FileSystemUtil::MaxNumFileHandles()),
        FLAGS_num_file_handle_cache_partitions,
        FLAGS_unused_file_handle_timeout_sec) {
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
    for (RequestContext* context : disk_queues_[i]->request_contexts) {
      unique_lock<mutex> context_lock(context->lock_);
      DCHECK_EQ(context->disk_states_[disk_id].num_threads_in_op(), 0);
      DCHECK(context->disk_states_[disk_id].done());
      context->DecrementDiskRefCount(context_lock);
    }
  }

  for (int i = 0; i < disk_queues_.size(); ++i) {
    delete disk_queues_[i];
  }

  if (cached_read_options_ != nullptr) hadoopRzOptionsFree(cached_read_options_);
}

Status DiskIoMgr::Init() {
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

unique_ptr<RequestContext> DiskIoMgr::RegisterContext() {
  return unique_ptr<RequestContext>(new RequestContext(this, num_total_disks()));
}

void DiskIoMgr::UnregisterContext(RequestContext* reader) {
  reader->CancelAndMarkInactive();
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
  if (range->len_ <= 0) {
    return Status(TErrorCode::DISK_IO_ERROR,
        Substitute("Invalid scan range. Non-positive length $0", range->len_));
  }
  return Status::OK();
}

Status DiskIoMgr::AddScanRanges(
    RequestContext* reader, const vector<ScanRange*>& ranges) {
  // Validate and initialize all ranges
  for (int i = 0; i < ranges.size(); ++i) {
    RETURN_IF_ERROR(ValidateScanRange(ranges[i]));
    ranges[i]->InitInternal(this, reader);
  }

  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();

  if (reader->state_ == RequestContext::Cancelled) return Status::CANCELLED;

  // Add each range to the queue of the disk the range is on
  for (ScanRange* range : ranges) {
    // Don't add empty ranges.
    DCHECK_NE(range->len(), 0);
    reader->AddActiveScanRangeLocked(reader_lock, range);
    if (range->try_cache_) {
      reader->cached_ranges_.Enqueue(range);
    } else {
      reader->AddRangeToDisk(reader_lock, range, ScheduleMode::UPON_GETNEXT);
    }
  }
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  return Status::OK();
}

Status DiskIoMgr::StartScanRange(RequestContext* reader, ScanRange* range,
    bool* needs_buffers) {
  RETURN_IF_ERROR(ValidateScanRange(range));
  range->InitInternal(this, reader);

  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  if (reader->state_ == RequestContext::Cancelled) return Status::CANCELLED;

  DCHECK_NE(range->len(), 0);
  if (range->try_cache_) {
    bool cached_read_succeeded;
    RETURN_IF_ERROR(range->ReadFromCache(reader_lock, &cached_read_succeeded));
    if (cached_read_succeeded) {
      DCHECK(reader->Validate()) << endl << reader->DebugString();
      *needs_buffers = false;
      return Status::OK();
    }
    // Cached read failed, fall back to normal read path.
  }
  // If we don't have a buffer yet, the caller must allocate buffers for the range.
  *needs_buffers = range->external_buffer_tag_ == ScanRange::ExternalBufferTag::NO_BUFFER;
  if (*needs_buffers) range->SetBlockedOnBuffer();
  reader->AddActiveScanRangeLocked(reader_lock, range);
  reader->AddRangeToDisk(reader_lock, range,
      *needs_buffers ? ScheduleMode::BY_CALLER : ScheduleMode::IMMEDIATELY);
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  return Status::OK();
}

// This function returns the next scan range the reader should work on, checking
// for eos and error cases. If there isn't already a cached scan range or a scan
// range prepared by the disk threads, the caller waits on the disk threads.
Status DiskIoMgr::GetNextUnstartedRange(RequestContext* reader, ScanRange** range,
    bool* needs_buffers) {
  DCHECK(reader != nullptr);
  DCHECK(range != nullptr);
  *range = nullptr;
  *needs_buffers = false;

  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  while (true) {
    if (reader->state_ == RequestContext::Cancelled) return Status::CANCELLED;

    if (reader->num_unstarted_scan_ranges_.Load() == 0 &&
        reader->ready_to_start_ranges_.empty() && reader->cached_ranges_.empty()) {
      // All ranges are done, just return.
      return Status::OK();
    }

    if (!reader->cached_ranges_.empty()) {
      // We have a cached range.
      *range = reader->cached_ranges_.Dequeue();
      DCHECK((*range)->try_cache_);
      bool cached_read_succeeded;
      RETURN_IF_ERROR((*range)->ReadFromCache(reader_lock, &cached_read_succeeded));
      if (cached_read_succeeded) return Status::OK();

      // This range ended up not being cached. Loop again and pick up a new range.
      reader->AddRangeToDisk(reader_lock, *range, ScheduleMode::UPON_GETNEXT);
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
      ScanRange::ExternalBufferTag buffer_tag = (*range)->external_buffer_tag_;
      if (buffer_tag == ScanRange::ExternalBufferTag::NO_BUFFER) {
        // We can't schedule this range until the client gives us buffers. The context
        // must be rescheduled regardless to ensure that 'next_scan_range_to_start' is
        // refilled.
        reader->disk_states_[disk_id].ScheduleContext(reader_lock, reader, disk_id);
        (*range)->SetBlockedOnBuffer();
        *needs_buffers = true;
      } else {
        reader->ScheduleScanRange(reader_lock, *range);
      }
      return Status::OK();
    }
  }
}

Status DiskIoMgr::AllocateBuffersForRange(RequestContext* reader,
    BufferPool::ClientHandle* bp_client, ScanRange* range, int64_t max_bytes) {
  DCHECK_GE(max_bytes, min_buffer_size_);
  DCHECK(range->external_buffer_tag_ == ScanRange::ExternalBufferTag::NO_BUFFER)
     << static_cast<int>(range->external_buffer_tag_) << " invalid to allocate buffers "
     << "when already reading into an external buffer";
  BufferPool* bp = ExecEnv::GetInstance()->buffer_pool();
  Status status;
  vector<unique_ptr<BufferDescriptor>> buffers;
  for (int64_t buffer_size : ChooseBufferSizes(range->len(), max_bytes)) {
    BufferPool::BufferHandle handle;
    status = bp->AllocateBuffer(bp_client, buffer_size, &handle);
    if (!status.ok()) goto error;
    buffers.emplace_back(new BufferDescriptor(
        this, reader, range, bp_client, move(handle)));
  }
  range->AddUnusedBuffers(move(buffers), false);
  return Status::OK();
 error:
  DCHECK(!status.ok());
  range->CleanUpBuffers(move(buffers));
  return status;
}

vector<int64_t> DiskIoMgr::ChooseBufferSizes(int64_t scan_range_len, int64_t max_bytes) {
  DCHECK_GE(max_bytes, min_buffer_size_);
  vector<int64_t> buffer_sizes;
  int64_t bytes_allocated = 0;
  while (bytes_allocated < scan_range_len) {
    int64_t bytes_remaining = scan_range_len - bytes_allocated;
    // Either allocate a max-sized buffer or a smaller buffer to fit the rest of the
    // range.
    int64_t next_buffer_size;
    if (bytes_remaining >= max_buffer_size_) {
      next_buffer_size = max_buffer_size_;
    } else {
      next_buffer_size =
          max(min_buffer_size_, BitUtil::RoundUpToPowerOfTwo(bytes_remaining));
    }
    if (next_buffer_size + bytes_allocated > max_bytes) {
      // Can't allocate the desired buffer size. Make sure to allocate at least one
      // buffer.
      if (bytes_allocated > 0) break;
      next_buffer_size = BitUtil::RoundDownToPowerOfTwo(max_bytes);
    }
    DCHECK(BitUtil::IsPowerOf2(next_buffer_size)) << next_buffer_size;
    buffer_sizes.push_back(next_buffer_size);
    bytes_allocated += next_buffer_size;
  }
  return buffer_sizes;
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
      request_disk_state->IncrementDiskThreadAndDequeue();
    }

    // NOTE: no locks were taken in between.  We need to be careful about what state
    // could have changed to the reader and disk in between.
    // There are some invariants here.  Only one disk thread can have the
    // same reader here (the reader is removed from the queue).  There can be
    // other disk threads operating on this reader in other functions though.
    unique_lock<mutex> request_lock((*request_context)->lock_);
    VLOG_FILE << "Disk (id=" << disk_id << ") reading for "
        << (*request_context)->DebugString();

    // Check if reader has been cancelled
    if ((*request_context)->state_ == RequestContext::Cancelled) {
      request_disk_state->DecrementDiskThread(request_lock, *request_context);
      continue;
    }

    DCHECK_EQ((*request_context)->state_, RequestContext::Active)
        << (*request_context)->DebugString();

    if (request_disk_state->next_scan_range_to_start() == nullptr &&
        !request_disk_state->unstarted_scan_ranges()->empty()) {
      // We don't have a range queued for this disk for what the caller should
      // read next. Populate that.  We want to have one range waiting to minimize
      // wait time in GetNextUnstartedRange().
      ScanRange* new_range = request_disk_state->unstarted_scan_ranges()->Dequeue();
      (*request_context)->num_unstarted_scan_ranges_.Add(-1);
      (*request_context)->ready_to_start_ranges_.Enqueue(new_range);
      request_disk_state->set_next_scan_range_to_start(new_range);

      if ((*request_context)->num_unstarted_scan_ranges_.Load() == 0) {
        // All the ranges have been started, notify everyone blocked on
        // GetNextUnstartedRange(). Only one of them will get work so make sure to return
        // nullptr to the other caller threads.
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
      request_disk_state->DecrementDiskThread(request_lock, *request_context);
      continue;
    }
    DCHECK_GT(request_disk_state->num_remaining_ranges(), 0);
    *range = request_disk_state->in_flight_ranges()->Dequeue();
    DCHECK(*range != nullptr);

    // Now that we've picked a request range, put the context back on the queue so
    // another thread can pick up another request range for this context.
    request_disk_state->ScheduleContext(request_lock, *request_context, disk_id);
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

  // Execute the callback before decrementing the thread count. Otherwise
  // RequestContext::Cancel() that waits for the disk ref count to be 0 will
  // return, creating a race, e.g. see IMPALA-1890.
  // The status of the write does not affect the status of the writer context.
  write_range->callback_(write_status);
  {
    unique_lock<mutex> writer_lock(writer->lock_);
    DCHECK(writer->Validate()) << endl << writer->DebugString();
    RequestContext::PerDiskState& state = writer->disk_states_[disk_id];
    state.DecrementDiskThread(writer_lock, writer);
    --state.num_remaining_ranges();
  }
}

void DiskIoMgr::HandleReadFinished(DiskQueue* disk_queue, RequestContext* reader,
    Status read_status, unique_ptr<BufferDescriptor> buffer) {
  unique_lock<mutex> reader_lock(reader->lock_);

  RequestContext::PerDiskState* disk_state = &reader->disk_states_[disk_queue->disk_id];
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  DCHECK_GT(disk_state->num_threads_in_op(), 0);
  DCHECK(buffer->buffer_ != nullptr);
  DCHECK(!buffer->is_cached()) << "HDFS cache reads don't go through this code path.";

  // After calling EnqueueReadyBuffer() below, it is no longer valid to read from buffer.
  // Store the state we need before calling EnqueueReadyBuffer().
  bool eosr = buffer->eosr_;

  // TODO: IMPALA-4249: it safe to touch 'scan_range' until DecrementDiskThread() is
  // called because all clients of DiskIoMgr keep ScanRange objects alive until they
  // unregister their RequestContext.
  ScanRange* scan_range = buffer->scan_range_;
  bool scan_range_done = eosr;
  if (read_status.ok() && reader->state_ != RequestContext::Cancelled) {
    DCHECK_EQ(reader->state_, RequestContext::Active);
    // Read successfully - update the reader's scan ranges.  There are two cases here:
    //  1. End of scan range or cancelled scan range - don't need to reschedule.
    //  2. Middle of scan range - need to schedule to read next buffer.
    bool enqueued = scan_range->EnqueueReadyBuffer(reader_lock, move(buffer));
    if (!eosr && enqueued) reader->ScheduleScanRange(reader_lock, scan_range);
  } else {
    // The scan range will be cancelled, either because we hit an error or because the
    // request context was cancelled.  The buffer is not needed - we must free it.
    reader->FreeBuffer(buffer.get());
    // Propagate 'read_status' to the scan range. If we are here because the context
    // was cancelled, the scan range is already cancelled so we do not need to re-cancel
    // it.
    if (!read_status.ok()) scan_range->CancelFromReader(reader_lock, read_status);
    scan_range_done = true;
  }
  if (scan_range_done) {
    scan_range->Close();
    --disk_state->num_remaining_ranges();
  }
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  disk_state->DecrementDiskThread(reader_lock, reader);
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
    RequestContext* worker_context = nullptr;
    RequestRange* range = nullptr;
    if (!GetNextRequestRange(disk_queue, &range, &worker_context)) {
      DCHECK(shut_down_);
      return;
    }
    if (range->request_type() == RequestType::READ) {
      ReadRange(disk_queue, worker_context, static_cast<ScanRange*>(range));
    } else {
      DCHECK(range->request_type() == RequestType::WRITE);
      Write(worker_context, static_cast<WriteRange*>(range));
    }
  }
}

void DiskIoMgr::ReadRange(
    DiskQueue* disk_queue, RequestContext* reader, ScanRange* range) {
  int64_t bytes_remaining = range->len_ - range->bytes_read_;
  DCHECK_GT(bytes_remaining, 0);
  unique_ptr<BufferDescriptor> buffer_desc;
  if (range->external_buffer_tag_ == ScanRange::ExternalBufferTag::CLIENT_BUFFER) {
    buffer_desc = unique_ptr<BufferDescriptor>(new BufferDescriptor(this, reader, range,
        range->client_buffer_.data, range->client_buffer_.len));
  } else {
    DCHECK(range->external_buffer_tag_ == ScanRange::ExternalBufferTag::NO_BUFFER)
        << "This code path does not handle other buffer types, i.e. HDFS cache"
        << static_cast<int>(range->external_buffer_tag_);
    buffer_desc = range->GetNextUnusedBufferForRange();
    if (buffer_desc == nullptr) {
      // No buffer available - the range will be rescheduled when a buffer is added.
      unique_lock<mutex> reader_lock(reader->lock_);
      reader->disk_states_[disk_queue->disk_id].DecrementDiskThread(reader_lock, reader);
      DCHECK(reader->Validate()) << endl << reader->DebugString();
      return;
    }
  }

  // No locks in this section.  Only working on local vars.  We don't want to hold a
  // lock across the read call.
  Status read_status = range->Open(detail::is_file_handle_caching_enabled());
  if (read_status.ok()) {
    // Update counters.
    COUNTER_ADD_IF_NOT_NULL(reader->active_read_thread_counter_, 1L);
    COUNTER_BITOR_IF_NOT_NULL(reader->disks_accessed_bitmap_, 1LL << disk_queue->disk_id);

    read_status = range->Read(buffer_desc->buffer_, buffer_desc->buffer_len_,
        &buffer_desc->len_, &buffer_desc->eosr_);
    buffer_desc->scan_range_offset_ = range->bytes_read_ - buffer_desc->len_;

    COUNTER_ADD_IF_NOT_NULL(reader->bytes_read_counter_, buffer_desc->len_);
    COUNTER_ADD(&total_bytes_read_counter_, buffer_desc->len_);
    COUNTER_ADD_IF_NOT_NULL(reader->active_read_thread_counter_, -1L);
  }

  // Finished read, update reader/disk based on the results
  HandleReadFinished(disk_queue, reader, read_status, move(buffer_desc));
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
  ImpaladMetrics::IO_MGR_BYTES_WRITTEN->Increment(write_range->len_);
  return Status::OK();
}

Status DiskIoMgr::AddWriteRange(RequestContext* writer, WriteRange* write_range) {
  unique_lock<mutex> writer_lock(writer->lock_);
  if (writer->state_ == RequestContext::Cancelled) return Status::CANCELLED;
  writer->AddRangeToDisk(writer_lock, write_range, ScheduleMode::IMMEDIATELY);
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
