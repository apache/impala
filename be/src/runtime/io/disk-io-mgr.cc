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
#include "runtime/io/error-converter.h"

#include <boost/algorithm/string.hpp>

#include "gutil/strings/substitute.h"
#include "util/bit-util.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"
#include "util/hdfs-util.h"
#include "util/time.h"

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

DEFINE_int32_hidden(max_free_io_buffers, 128, "Deprecated - has no effect.");

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

string DiskIoMgr::DebugString() {
  stringstream ss;
  ss << "Disks: " << endl;
  for (DiskQueue* disk_queue : disk_queues_) {
    disk_queue->DebugString(&ss);
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
  local_file_system_.reset(new LocalFileSystem());
}

DiskIoMgr::DiskIoMgr(int num_local_disks, int threads_per_rotational_disk,
    int threads_per_solid_state_disk, int64_t min_buffer_size, int64_t max_buffer_size) :
    num_io_threads_per_rotational_disk_(threads_per_rotational_disk),
    num_io_threads_per_solid_state_disk_(threads_per_solid_state_disk),
    max_buffer_size_(BitUtil::RoundUpToPowerOfTwo(max_buffer_size)),
    min_buffer_size_(BitUtil::RoundDownToPowerOfTwo(min_buffer_size)),
    total_bytes_read_counter_(TUnit::BYTES),
    read_timer_(TUnit::TIME_NS),
    file_handle_cache_(min(FLAGS_max_cached_file_handles,
        FileSystemUtil::MaxNumFileHandles()),
        FLAGS_num_file_handle_cache_partitions,
        FLAGS_unused_file_handle_timeout_sec) {
  if (num_local_disks == 0) num_local_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_local_disks + REMOTE_NUM_DISKS);
  CheckSseSupport();
  local_file_system_.reset(new LocalFileSystem());
}

DiskIoMgr::~DiskIoMgr() {
  // Signal all threads to shut down, then wait for them to do so.
  for (DiskQueue* disk_queue : disk_queues_) {
    if (disk_queue != nullptr) disk_queue->ShutDown();
  }
  disk_thread_group_.JoinAll();
  for (DiskQueue* disk_queue : disk_queues_) delete disk_queue;
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
      RETURN_IF_ERROR(Thread::Create("disk-io-mgr", ss.str(), &DiskQueue::DiskThreadLoop,
          disk_queues_[i], this, &t));
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
  int disk_id = range->disk_id();
  if (disk_id < 0 || disk_id >= disk_queues_.size()) {
    return Status(TErrorCode::DISK_IO_ERROR,
        Substitute("Invalid scan range.  Bad disk id: $0", disk_id));
  }
  if (range->offset() < 0) {
    return Status(TErrorCode::DISK_IO_ERROR,
        Substitute("Invalid scan range. Negative offset $0", range->offset()));
  }
  if (range->len() <= 0) {
    return Status(TErrorCode::DISK_IO_ERROR,
        Substitute("Invalid scan range. Non-positive length $0", range->len()));
  }
  return Status::OK();
}

Status DiskIoMgr::AddScanRanges(
    RequestContext* reader, const vector<ScanRange*>& ranges) {
  DCHECK_GT(ranges.size(), 0);
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
    if (range->try_cache()) {
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
  if (range->try_cache()) {
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
  *needs_buffers =
      range->external_buffer_tag() == ScanRange::ExternalBufferTag::NO_BUFFER;
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
      DCHECK((*range)->try_cache());
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
      ScanRange::ExternalBufferTag buffer_tag = (*range)->external_buffer_tag();
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
  DCHECK(range->external_buffer_tag() == ScanRange::ExternalBufferTag::NO_BUFFER)
     << static_cast<int>(range->external_buffer_tag()) << " invalid to allocate buffers "
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

int64_t DiskIoMgr::ComputeIdealBufferReservation(int64_t scan_range_len) {
  if (scan_range_len < max_buffer_size_) {
    // Round up to nearest power-of-two buffer size - ideally we should do a single read
    // I/O for this range.
    return max(min_buffer_size_, BitUtil::RoundUpToPowerOfTwo(scan_range_len));
  } else {
    // Round up to the nearest max-sized I/O buffer, capped by
    // IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE - we should do one or more max-sized read
    // I/Os for this range.
    return min(IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE * max_buffer_size_,
            BitUtil::RoundUpToPowerOf2(scan_range_len, max_buffer_size_));
  }
}

// This function gets the next RequestRange to work on for this disk. It blocks until
// work is available or the thread is shut down.
// Work is available if there is a RequestContext with
//  - A ScanRange with a buffer available, or
//  - A WriteRange in unstarted_write_ranges_.
RequestRange* DiskQueue::GetNextRequestRange(RequestContext** request_context) {
  // This loops returns either with work to do or when the disk IoMgr shuts down.
  while (true) {
    *request_context = nullptr;
    {
      unique_lock<mutex> disk_lock(lock_);
      while (!shut_down_ && request_contexts_.empty()) {
        // wait if there are no readers on the queue
        work_available_.Wait(disk_lock);
      }
      if (shut_down_) break;
      DCHECK(!request_contexts_.empty());

      // Get the next reader and remove the reader so that another disk thread
      // can't pick it up. It will be enqueued before issuing the read to HDFS
      // so this is not a big deal (i.e. multiple disk threads can read for the
      // same reader).
      *request_context = request_contexts_.front();
      request_contexts_.pop_front();
      DCHECK(*request_context != nullptr);
      // Must increment refcount to keep RequestContext after dropping 'disk_lock'
      (*request_context)->IncrementDiskThreadAfterDequeue(disk_id_);
    }
    // Get the next range to process for this reader. If this context does not have a
    // range, rinse and repeat.
    RequestRange* range = (*request_context)->GetNextRequestRange(disk_id_);
    if (range != nullptr) return range;
  }
  DCHECK(shut_down_);
  return nullptr;
}

void DiskIoMgr::HandleWriteFinished(
    RequestContext* writer, WriteRange* write_range, const Status& write_status) {
  // Copy disk_id before running callback: the callback may modify write_range.
  int disk_id = write_range->disk_id();

  // Execute the callback before decrementing the thread count. Otherwise
  // RequestContext::Cancel() that waits for the disk ref count to be 0 will
  // return, creating a race, e.g. see IMPALA-1890.
  // The status of the write does not affect the status of the writer context.
  write_range->callback()(write_status);
  {
    unique_lock<mutex> writer_lock(writer->lock_);
    DCHECK(writer->Validate()) << endl << writer->DebugString();
    RequestContext::PerDiskState& state = writer->disk_states_[disk_id];
    state.DecrementDiskThread(writer_lock, writer);
    --state.num_remaining_ranges();
  }
}

void DiskQueue::DiskThreadLoop(DiskIoMgr* io_mgr) {
  // The thread waits until there is work or the queue is shut down. If there is work,
  // performs the read or write requested. Locks are not taken when reading from or
  // writing to disk.
  while (true) {
    RequestContext* worker_context = nullptr;
    RequestRange* range = GetNextRequestRange(&worker_context);
    if (range == nullptr) {
      DCHECK(shut_down_);
      return;
    }
    if (range->request_type() == RequestType::READ) {
      ScanRange* scan_range = static_cast<ScanRange*>(range);
      ReadOutcome outcome = scan_range->DoRead(disk_id_);
      worker_context->ReadDone(disk_id_, outcome, scan_range);
    } else {
      DCHECK(range->request_type() == RequestType::WRITE);
      io_mgr->Write(worker_context, static_cast<WriteRange*>(range));
    }
  }
}

void DiskIoMgr::Write(RequestContext* writer_context, WriteRange* write_range) {
  Status ret_status = Status::OK();
  FILE* file_handle = nullptr;
  Status close_status = Status::OK();
  ret_status = local_file_system_->OpenForWrite(write_range->file(), O_RDWR | O_CREAT,
      S_IRUSR | S_IWUSR, &file_handle);
  if (!ret_status.ok()) goto end;

  ret_status = WriteRangeHelper(file_handle, write_range);

  close_status = local_file_system_->Fclose(file_handle, write_range);
  if (ret_status.ok() && !close_status.ok()) ret_status = close_status;

end:
  HandleWriteFinished(writer_context, write_range, ret_status);
}

Status DiskIoMgr::WriteRangeHelper(FILE* file_handle, WriteRange* write_range) {
  // Seek to the correct offset and perform the write.
  RETURN_IF_ERROR(local_file_system_->Fseek(file_handle, write_range->offset(), SEEK_SET,
      write_range));

#ifndef NDEBUG
  if (FLAGS_stress_scratch_write_delay_ms > 0) {
    SleepForMs(FLAGS_stress_scratch_write_delay_ms);
  }
#endif
  RETURN_IF_ERROR(local_file_system_->Fwrite(file_handle, write_range));

  ImpaladMetrics::IO_MGR_BYTES_WRITTEN->Increment(write_range->len());
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

void DiskQueue::ShutDown() {
  {
    unique_lock<mutex> disk_lock(lock_);
    shut_down_ = true;
  }
  // All waiting threads should exit, so wake them all up.
  work_available_.NotifyAll();
}

void DiskQueue::DebugString(stringstream* ss) {
  unique_lock<mutex> lock(lock_);
  *ss << "DiskQueue id=" << disk_id_ << " ptr=" << static_cast<void*>(this) << ":" ;
  if (!request_contexts_.empty()) {
    *ss << " Readers: ";
    for (RequestContext* req_context: request_contexts_) {
      *ss << static_cast<void*>(req_context);
    }
  }
}

DiskQueue::~DiskQueue() {
  for (RequestContext* context : request_contexts_) {
    context->UnregisterDiskQueue(disk_id_);
  }
}
