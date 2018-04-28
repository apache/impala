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

#include "runtime/exec-env.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/disk-io-mgr-internal.h"
#include "util/error-util.h"
#include "util/hdfs-util.h"

#include "common/names.h"

using namespace impala;
using namespace impala::io;

DEFINE_bool(use_hdfs_pread, false, "Enables using hdfsPread() instead of hdfsRead() "
    "when performing HDFS read operations. This is necessary to use HDFS hedged reads "
    "(assuming the HDFS client is configured to do so).");

// TODO: Run perf tests and empirically settle on the most optimal default value for the
// read buffer size. Currently setting it as 128k for the same reason as for S3, i.e.
// due to JNI array allocation and memcpy overhead, 128k was emperically found to have the
// least overhead.
DEFINE_int64(adls_read_chunk_size, 128 * 1024, "The maximum read chunk size to use when "
    "reading from ADLS.");

#ifndef NDEBUG
DECLARE_int32(stress_disk_read_delay_ms);
#endif

// Implementation of the ScanRange functionality. Each ScanRange contains a queue
// of ready buffers. For each ScanRange, there is only a single producer and
// consumer thread, i.e. only one disk thread will push to a scan range at
// any time and only one thread will remove from the queue. This is to guarantee
// that buffers are queued and read in file order.
bool ScanRange::EnqueueReadyBuffer(unique_ptr<BufferDescriptor> buffer) {
  DCHECK(buffer->buffer_ != nullptr) << "Cannot enqueue freed buffer";
  {
    unique_lock<mutex> scan_range_lock(lock_);
    DCHECK(Validate()) << DebugString();
    DCHECK(!eosr_queued_);
    if (!buffer->is_cached()) {
      // All non-cached buffers are enqueued by disk threads. Indicate that the read
      // finished.
      DCHECK(read_in_flight_);
      read_in_flight_ = false;
    }
    if (!cancel_status_.ok()) {
      // This range has been cancelled, no need to enqueue the buffer.
      CleanUpBuffer(scan_range_lock, move(buffer));
      // One or more threads may be blocked in WaitForInFlightRead() waiting for the read
      // to complete. Wake up all of them.
      buffer_ready_cv_.NotifyAll();
      return false;
    }
    // Clean up any surplus buffers. E.g. we may have allocated too many if the file was
    // shorter than expected.
    if (buffer->eosr()) CleanUpUnusedBuffers(scan_range_lock);
    eosr_queued_ = buffer->eosr();
    ready_buffers_.emplace_back(move(buffer));
  }
  buffer_ready_cv_.NotifyOne();
  return true;
}

Status ScanRange::GetNext(unique_ptr<BufferDescriptor>* buffer) {
  DCHECK(*buffer == nullptr);
  bool eosr;
  {
    unique_lock<mutex> scan_range_lock(lock_);
    DCHECK(Validate()) << DebugString();
    // No more buffers to return - return the cancel status or OK if not cancelled.
    if (all_buffers_returned(scan_range_lock)) return cancel_status_;

    while (ready_buffers_.empty() && cancel_status_.ok()) {
      buffer_ready_cv_.Wait(scan_range_lock);
    }
    /// Propagate cancellation to the client if it happened while we were waiting.
    RETURN_IF_ERROR(cancel_status_);

    // Remove the first ready buffer from the queue and return it
    DCHECK(!ready_buffers_.empty());
    *buffer = move(ready_buffers_.front());
    ready_buffers_.pop_front();
    eosr = (*buffer)->eosr();
    DCHECK(!eosr || unused_iomgr_buffers_.empty()) << DebugString();
  }

  // Update tracking counters. The buffer has now moved from the IoMgr to the caller.
  if (eosr) reader_->RemoveActiveScanRange(this);
  num_buffers_in_reader_.Add(1);
  return Status::OK();
}

void ScanRange::ReturnBuffer(unique_ptr<BufferDescriptor> buffer_desc) {
  vector<unique_ptr<BufferDescriptor>> buffers;
  buffers.emplace_back(move(buffer_desc));
  AddUnusedBuffers(move(buffers), true);
}

void ScanRange::AddUnusedBuffers(vector<unique_ptr<BufferDescriptor>>&& buffers,
      bool returned) {
  DCHECK_GT(buffers.size(), 0);
  /// Keep track of whether the range was unblocked in this function. If so, we need
  /// to schedule it so it resumes progress.
  bool unblocked = false;
  {
    unique_lock<mutex> scan_range_lock(lock_);
    if (returned) {
      // Buffers were in reader but now aren't.
      num_buffers_in_reader_.Add(-buffers.size());
    }

    for (unique_ptr<BufferDescriptor>& buffer : buffers) {
      // We should not hold onto the buffers in the following cases:
      // 1. the scan range is using external buffers, e.g. cached buffers.
      // 2. the scan range is cancelled
      // 3. the scan range already hit eosr
      // 4. we already have enough buffers to read the remainder of the scan range.
      if (external_buffer_tag_ != ExternalBufferTag::NO_BUFFER
          || !cancel_status_.ok()
          || eosr_queued_
          || unused_iomgr_buffer_bytes_ >= len_ - iomgr_buffer_cumulative_bytes_used_) {
        CleanUpBuffer(scan_range_lock, move(buffer));
      } else {
        unused_iomgr_buffer_bytes_ += buffer->buffer_len();
        unused_iomgr_buffers_.emplace_back(move(buffer));
        if (blocked_on_buffer_) {
          blocked_on_buffer_ = false;
          unblocked = true;
        }
      }
    }
  }
  // Must drop the ScanRange lock before acquiring the RequestContext lock.
  if (unblocked) {
    unique_lock<mutex> reader_lock(reader_->lock_);
    // Reader may have been cancelled after we dropped 'scan_range_lock' above.
    if (reader_->state_ == RequestContext::Cancelled) {
      DCHECK(!cancel_status_.ok());
    } else {
      reader_->ScheduleScanRange(reader_lock, this);
    }
  }
}

unique_ptr<BufferDescriptor> ScanRange::GetUnusedBuffer(
    const unique_lock<mutex>& scan_range_lock) {
  DCHECK(scan_range_lock.mutex() == &lock_ && scan_range_lock.owns_lock());
  if (unused_iomgr_buffers_.empty()) return nullptr;
  unique_ptr<BufferDescriptor> result = move(unused_iomgr_buffers_.back());
  unused_iomgr_buffers_.pop_back();
  unused_iomgr_buffer_bytes_ -= result->buffer_len();
  return result;
}

ReadOutcome ScanRange::DoRead(int disk_id) {
  int64_t bytes_remaining = len_ - bytes_read_;
  DCHECK_GT(bytes_remaining, 0);

  unique_ptr<BufferDescriptor> buffer_desc;
  {
    unique_lock<mutex> lock(lock_);
    DCHECK(!read_in_flight_);
    if (!cancel_status_.ok()) return ReadOutcome::CANCELLED;

    if (external_buffer_tag_ == ScanRange::ExternalBufferTag::CLIENT_BUFFER) {
      buffer_desc = unique_ptr<BufferDescriptor>(new BufferDescriptor(
          io_mgr_, reader_, this, client_buffer_.data, client_buffer_.len));
    } else {
      DCHECK(external_buffer_tag_ == ScanRange::ExternalBufferTag::NO_BUFFER)
          << "This code path does not handle other buffer types, i.e. HDFS cache"
          << static_cast<int>(external_buffer_tag_);
      buffer_desc = GetUnusedBuffer(lock);
      if (buffer_desc == nullptr) {
        // No buffer available - the range will be rescheduled when a buffer is added.
        blocked_on_buffer_ = true;
        return ReadOutcome::BLOCKED_ON_BUFFER;
      }
      iomgr_buffer_cumulative_bytes_used_ += buffer_desc->buffer_len();
    }
    read_in_flight_ = true;
  }

  // No locks in this section.  Only working on local vars.  We don't want to hold a
  // lock across the read call.
  Status read_status = Open(is_file_handle_caching_enabled());
  if (read_status.ok()) {
    COUNTER_ADD_IF_NOT_NULL(reader_->active_read_thread_counter_, 1L);
    COUNTER_BITOR_IF_NOT_NULL(reader_->disks_accessed_bitmap_, 1LL << disk_id);

    read_status = Read(buffer_desc->buffer_, buffer_desc->buffer_len_,
        &buffer_desc->len_, &buffer_desc->eosr_);
    buffer_desc->scan_range_offset_ = bytes_read_ - buffer_desc->len_;

    COUNTER_ADD_IF_NOT_NULL(reader_->bytes_read_counter_, buffer_desc->len_);
    COUNTER_ADD(io_mgr_->total_bytes_read_counter(), buffer_desc->len_);
    COUNTER_ADD_IF_NOT_NULL(reader_->active_read_thread_counter_, -1L);
  }

  DCHECK(buffer_desc->buffer_ != nullptr);
  DCHECK(!buffer_desc->is_cached()) << "HDFS cache reads don't go through this code path.";
  if (!read_status.ok()) {
    // Free buffer to release resources before we cancel the range so that all buffers
    // are freed at cancellation.
    buffer_desc->Free();
    buffer_desc.reset();

    // Propagate 'read_status' to the scan range. This will also wake up any waiting
    // threads.
    CancelInternal(read_status, true);
    // No more reads for this scan range - we can close it.
    Close();
    return ReadOutcome::CANCELLED;
  }

  // After calling EnqueueReadyBuffer(), it is no longer valid to touch 'buffer_desc'.
  // Store the state we need before calling EnqueueReadyBuffer().
  bool eosr = buffer_desc->eosr_;
  // Read successful - enqueue the buffer and return the appropriate outcome.
  if (!EnqueueReadyBuffer(move(buffer_desc))) return ReadOutcome::CANCELLED;
  if (eosr) {
    // No more reads for this scan range - we can close it.
    Close();
    return ReadOutcome::SUCCESS_EOSR;
  }
  return ReadOutcome::SUCCESS_NO_EOSR;
}

void ScanRange::SetBlockedOnBuffer() {
  unique_lock<mutex> lock(lock_);
  blocked_on_buffer_ = true;
}

void ScanRange::CleanUpBuffer(
    const boost::unique_lock<boost::mutex>& scan_range_lock,
    unique_ptr<BufferDescriptor> buffer_desc) {
  DCHECK(scan_range_lock.mutex() == &lock_ && scan_range_lock.owns_lock());
  DCHECK(buffer_desc != nullptr);
  DCHECK_EQ(this, buffer_desc->scan_range_);
  buffer_desc->Free();

  if (all_buffers_returned(scan_range_lock) && num_buffers_in_reader_.Load() == 0) {
    // Close the scan range if there are no more buffers in the reader and no more buffers
    // will be returned to readers in future. Close() is idempotent so it is ok to call
    // multiple times during cleanup so long as the range is actually finished.
    Close();
  }
}

void ScanRange::CleanUpBuffers(vector<unique_ptr<BufferDescriptor>>&& buffers) {
  unique_lock<mutex> lock(lock_);
  for (unique_ptr<BufferDescriptor>& buffer : buffers) CleanUpBuffer(lock, move(buffer));
}

void ScanRange::CleanUpUnusedBuffers(const unique_lock<mutex>& scan_range_lock) {
  while (!unused_iomgr_buffers_.empty()) {
    CleanUpBuffer(scan_range_lock, GetUnusedBuffer(scan_range_lock));
  }
}

void ScanRange::Cancel(const Status& status) {
  // Cancelling a range that was never started, ignore.
  if (io_mgr_ == nullptr) return;
  CancelInternal(status, false);
  // Wait until an in-flight read is finished. The read thread will clean up the
  // buffer it used. Once the range is cancelled, no more reads should be started.
  WaitForInFlightRead();
  reader_->RemoveActiveScanRange(this);
}

void ScanRange::CancelInternal(const Status& status, bool read_error) {
  DCHECK(io_mgr_ != nullptr);
  DCHECK(!status.ok());
  {
    // Grab both locks to make sure that we don't change 'cancel_status_' while other
    // threads are in critical sections.
    unique_lock<mutex> scan_range_lock(lock_);
    {
      unique_lock<mutex> hdfs_lock(hdfs_lock_);
      DCHECK(Validate()) << DebugString();
      // If already cancelled, preserve the original reason for cancellation. Most of the
      // cleanup is not required if already cancelled, but we need to set
      // 'read_in_flight_' to false.
      if (cancel_status_.ok()) cancel_status_ = status;
    }

    /// Clean up 'ready_buffers_' while still holding 'lock_' to prevent other threads
    /// from seeing inconsistent state.
    while (!ready_buffers_.empty()) {
      CleanUpBuffer(scan_range_lock, move(ready_buffers_.front()));
      ready_buffers_.pop_front();
    }

    /// Clean up buffers that we don't need any more because we won't read any more data.
    CleanUpUnusedBuffers(scan_range_lock);
    if (read_error) {
      DCHECK(read_in_flight_);
      read_in_flight_ = false;
    }
  }
  buffer_ready_cv_.NotifyAll();

  // For cached buffers, we can't close the range until the cached buffer is returned.
  // Close() is called from ScanRange::CleanUpBufferLocked().
  if (external_buffer_tag_ != ExternalBufferTag::CACHED_BUFFER) Close();
}

void ScanRange::WaitForInFlightRead() {
  unique_lock<mutex> scan_range_lock(lock_);
  while (read_in_flight_) buffer_ready_cv_.Wait(scan_range_lock);
}

string ScanRange::DebugString() const {
  stringstream ss;
  ss << "file=" << file_ << " disk_id=" << disk_id_ << " offset=" << offset_
     << " len=" << len_ << " bytes_read=" << bytes_read_
     << " cancel_status=" << cancel_status_.GetDetail()
     << " buffer_queue=" << ready_buffers_.size()
     << " num_buffers_in_readers=" << num_buffers_in_reader_.Load()
     << " unused_iomgr_buffers=" << unused_iomgr_buffers_.size()
     << " unused_iomgr_buffer_bytes=" << unused_iomgr_buffer_bytes_
     << " blocked_on_buffer=" << blocked_on_buffer_
     << " hdfs_file=" << exclusive_hdfs_fh_;
  return ss.str();
}

bool ScanRange::Validate() {
  if (bytes_read_ > len_) {
    LOG(ERROR) << "Bytes read tracking is wrong. Shouldn't read past the scan range."
                 << " bytes_read_=" << bytes_read_ << " len_=" << len_;
    return false;
  }
  if (!cancel_status_.ok() && !ready_buffers_.empty()) {
    LOG(ERROR) << "Cancelled range should not have queued buffers " << DebugString();
    return false;
  }
  int64_t unused_iomgr_buffer_bytes = 0;
  for (auto& buffer : unused_iomgr_buffers_)
    unused_iomgr_buffer_bytes += buffer->buffer_len();
  if (unused_iomgr_buffer_bytes != unused_iomgr_buffer_bytes_) {
    LOG(ERROR) << "unused_iomgr_buffer_bytes_ incorrect actual: "
               << unused_iomgr_buffer_bytes_
               << " vs. expected: " << unused_iomgr_buffer_bytes;
    return false;
  }
  bool is_finished = !cancel_status_.ok() || eosr_queued_;
  if (is_finished && !unused_iomgr_buffers_.empty()) {
    LOG(ERROR) << "Held onto too many buffers " << unused_iomgr_buffers_.size()
               << " bytes: " << unused_iomgr_buffer_bytes_
               << " cancel_status: " << cancel_status_.GetDetail()
               << " eosr_queued: " << eosr_queued_;
    return false;
  }
  if (!is_finished && blocked_on_buffer_ && !unused_iomgr_buffers_.empty()) {
    LOG(ERROR) << "Blocked despite having buffers: " << DebugString();
    return false;
  }
  return true;
}

ScanRange::ScanRange()
  : RequestRange(RequestType::READ),
    num_remote_bytes_(0),
    external_buffer_tag_(ExternalBufferTag::NO_BUFFER) {}

ScanRange::~ScanRange() {
  DCHECK(exclusive_hdfs_fh_ == nullptr) << "File was not closed.";
  DCHECK(external_buffer_tag_ != ExternalBufferTag::CACHED_BUFFER)
      << "Cached buffer was not released.";
  DCHECK(!read_in_flight_);
  DCHECK_EQ(0, ready_buffers_.size());
  DCHECK_EQ(0, num_buffers_in_reader_.Load());
}

void ScanRange::Reset(hdfsFS fs, const char* file, int64_t len, int64_t offset,
    int disk_id, bool expected_local, const BufferOpts& buffer_opts, void* meta_data) {
  DCHECK(ready_buffers_.empty());
  DCHECK(!read_in_flight_);
  DCHECK(file != nullptr);
  DCHECK_GE(len, 0);
  DCHECK_GE(offset, 0);
  DCHECK(buffer_opts.client_buffer_ == nullptr ||
         buffer_opts.client_buffer_len_ >= len_);
  fs_ = fs;
  file_ = file;
  len_ = len;
  offset_ = offset;
  disk_id_ = disk_id;
  try_cache_ = buffer_opts.try_cache_;
  mtime_ = buffer_opts.mtime_;
  expected_local_ = expected_local;
  num_remote_bytes_ = 0;
  meta_data_ = meta_data;
  if (buffer_opts.client_buffer_ != nullptr) {
    external_buffer_tag_ = ExternalBufferTag::CLIENT_BUFFER;
    client_buffer_.data = buffer_opts.client_buffer_;
    client_buffer_.len = buffer_opts.client_buffer_len_;
  } else {
    external_buffer_tag_ = ExternalBufferTag::NO_BUFFER;
  }
  io_mgr_ = nullptr;
  reader_ = nullptr;
  exclusive_hdfs_fh_ = nullptr;
}

void ScanRange::InitInternal(DiskIoMgr* io_mgr, RequestContext* reader) {
  DCHECK(exclusive_hdfs_fh_ == nullptr);
  DCHECK(local_file_ == nullptr);
  DCHECK(!read_in_flight_);
  io_mgr_ = io_mgr;
  reader_ = reader;
  local_file_ = nullptr;
  exclusive_hdfs_fh_ = nullptr;
  bytes_read_ = 0;
  unused_iomgr_buffer_bytes_ = 0;
  iomgr_buffer_cumulative_bytes_used_ = 0;
  cancel_status_ = Status::OK();
  eosr_queued_ = false;
  blocked_on_buffer_ = false;
  DCHECK(Validate()) << DebugString();
}

Status ScanRange::Open(bool use_file_handle_cache) {
  unique_lock<mutex> hdfs_lock(hdfs_lock_);
  RETURN_IF_ERROR(cancel_status_);

  if (fs_ != nullptr) {
    if (exclusive_hdfs_fh_ != nullptr) return Status::OK();
    // With file handle caching, the scan range does not maintain its own
    // hdfs file handle. File handle caching is only used for local files,
    // so s3 and remote filesystems should obtain an exclusive file handle
    // for each scan range.
    if (use_file_handle_cache && expected_local_) return Status::OK();
    // Get a new exclusive file handle.
    exclusive_hdfs_fh_ = io_mgr_->GetExclusiveHdfsFileHandle(fs_, file_string(),
        mtime(), reader_);
    if (exclusive_hdfs_fh_ == nullptr) {
      return Status(TErrorCode::DISK_IO_ERROR,
          GetHdfsErrorMsg("Failed to open HDFS file ", file_));
    }

    if (hdfsSeek(fs_, exclusive_hdfs_fh_->file(), offset_) != 0) {
      // Destroy the file handle
      io_mgr_->ReleaseExclusiveHdfsFileHandle(exclusive_hdfs_fh_);
      exclusive_hdfs_fh_ = nullptr;
      return Status(TErrorCode::DISK_IO_ERROR,
          Substitute("Error seeking to $0 in file: $1 $2", offset_, file_,
          GetHdfsErrorMsg("")));
    }
  } else {
    if (local_file_ != nullptr) return Status::OK();

    local_file_ = fopen(file(), "r");
    if (local_file_ == nullptr) {
      return Status(TErrorCode::DISK_IO_ERROR, Substitute("Could not open file: $0: $1",
            file_, GetStrErrMsg()));
    }
    if (fseek(local_file_, offset_, SEEK_SET) == -1) {
      fclose(local_file_);
      local_file_ = nullptr;
      return Status(TErrorCode::DISK_IO_ERROR, Substitute("Could not seek to $0 "
          "for file: $1: $2", offset_, file_, GetStrErrMsg()));
    }
  }
  ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(1L);
  return Status::OK();
}

void ScanRange::Close() {
  unique_lock<mutex> hdfs_lock(hdfs_lock_);
  bool closed_file = false;
  if (fs_ != nullptr) {
    if (exclusive_hdfs_fh_ != nullptr) {
      GetHdfsStatistics(exclusive_hdfs_fh_->file());

      if (external_buffer_tag_ == ExternalBufferTag::CACHED_BUFFER) {
        hadoopRzBufferFree(exclusive_hdfs_fh_->file(), cached_buffer_);
        cached_buffer_ = nullptr;
        external_buffer_tag_ = ExternalBufferTag::NO_BUFFER;
      }

      // Destroy the file handle.
      io_mgr_->ReleaseExclusiveHdfsFileHandle(exclusive_hdfs_fh_);
      exclusive_hdfs_fh_ = nullptr;
      closed_file = true;
    }

    if (FLAGS_use_hdfs_pread && IsHdfsPath(file())) {
      // Update Hedged Read Metrics.
      // We call it only if the --use_hdfs_pread flag is set, to avoid having the
      // libhdfs client malloc and free a hdfsHedgedReadMetrics object unnecessarily
      // otherwise. 'hedged_metrics' is only set upon success.
      // We also avoid calling hdfsGetHedgedReadMetrics() when the file is not on HDFS
      // (see HDFS-13417).
      struct hdfsHedgedReadMetrics* hedged_metrics;
      int success = hdfsGetHedgedReadMetrics(fs_, &hedged_metrics);
      if (success == 0) {
        ImpaladMetrics::HEDGED_READ_OPS->SetValue(hedged_metrics->hedgedReadOps);
        ImpaladMetrics::HEDGED_READ_OPS_WIN->SetValue(hedged_metrics->hedgedReadOpsWin);
        hdfsFreeHedgedReadMetrics(hedged_metrics);
      }
    }

    if (num_remote_bytes_ > 0) {
      reader_->num_remote_ranges_.Add(1L);
      if (expected_local_) {
        reader_->unexpected_remote_bytes_.Add(num_remote_bytes_);
        VLOG_FILE << "Unexpected remote HDFS read of "
                  << PrettyPrinter::Print(num_remote_bytes_, TUnit::BYTES)
                  << " for file '" << file_ << "'";
      }
    }
  } else {
    if (local_file_ == nullptr) return;
    fclose(local_file_);
    local_file_ = nullptr;
    closed_file = true;
  }
  if (closed_file) ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(-1L);
}

int64_t ScanRange::MaxReadChunkSize() const {
  // S3 InputStreams don't support DIRECT_READ (i.e. java.nio.ByteBuffer read()
  // interface).  So, hdfsRead() needs to allocate a Java byte[] and copy the data out.
  // Profiles show that both the JNI array allocation and the memcpy adds much more
  // overhead for larger buffers, so limit the size of each read request.  128K was
  // chosen empirically by trying values between 4K and 8M and optimizing for lower CPU
  // utilization and higher S3 througput.
  if (disk_id_ == io_mgr_->RemoteS3DiskId()) {
    DCHECK(IsS3APath(file()));
    return 128 * 1024;
  }
  if (disk_id_ == io_mgr_->RemoteAdlsDiskId()) {
    DCHECK(IsADLSPath(file()));
    return FLAGS_adls_read_chunk_size;
  }
  // The length argument of hdfsRead() is an int. Ensure we don't overflow it.
  return numeric_limits<int>::max();
}

// TODO: how do we best use the disk here.  e.g. is it good to break up a
// 1MB read into 8 128K reads?
// TODO: look at linux disk scheduling
Status ScanRange::Read(
    uint8_t* buffer, int64_t buffer_len, int64_t* bytes_read, bool* eosr) {
  DCHECK(read_in_flight_);
  // Delay before acquiring the lock, to allow triggering IMPALA-6587 race.
#ifndef NDEBUG
  if (FLAGS_stress_disk_read_delay_ms > 0) {
    SleepForMs(FLAGS_stress_disk_read_delay_ms);
  }
#endif
  unique_lock<mutex> hdfs_lock(hdfs_lock_);
  RETURN_IF_ERROR(cancel_status_);

  *eosr = false;
  *bytes_read = 0;
  // Read until the end of the scan range or the end of the buffer.
  int bytes_to_read = min(len_ - bytes_read_, buffer_len);
  DCHECK_GE(bytes_to_read, 0);

  if (fs_ != nullptr) {
    CachedHdfsFileHandle* borrowed_hdfs_fh = nullptr;
    hdfsFile hdfs_file;

    // If the scan range has an exclusive file handle, use it. Otherwise, borrow
    // a file handle from the cache.
    if (exclusive_hdfs_fh_ != nullptr) {
      hdfs_file = exclusive_hdfs_fh_->file();
    } else {
      borrowed_hdfs_fh = io_mgr_->GetCachedHdfsFileHandle(fs_, file_string(),
          mtime(), reader_);
      if (borrowed_hdfs_fh == nullptr) {
        return Status(TErrorCode::DISK_IO_ERROR,
            GetHdfsErrorMsg("Failed to open HDFS file ", file_));
      }
      hdfs_file = borrowed_hdfs_fh->file();
    }

    int64_t max_chunk_size = MaxReadChunkSize();
    Status status = Status::OK();
    {
      ScopedTimer<MonotonicStopWatch> io_mgr_read_timer(io_mgr_->read_timer());
      ScopedTimer<MonotonicStopWatch> req_context_read_timer(reader_->read_timer_);
      while (*bytes_read < bytes_to_read) {
        int chunk_size = min(bytes_to_read - *bytes_read, max_chunk_size);
        DCHECK_GE(chunk_size, 0);
        // The hdfsRead() length argument is an int.
        DCHECK_LE(chunk_size, numeric_limits<int>::max());
        int current_bytes_read = -1;
        // bytes_read_ is only updated after the while loop
        int64_t position_in_file = offset_ + bytes_read_ + *bytes_read;
        int num_retries = 0;
        while (true) {
          status = Status::OK();
          // For file handles from the cache, any of the below file operations may fail
          // due to a bad file handle. In each case, record the error, but allow for a
          // retry to fix it.
          if (FLAGS_use_hdfs_pread) {
            current_bytes_read = hdfsPread(fs_, hdfs_file, position_in_file,
                buffer + *bytes_read, chunk_size);
            if (current_bytes_read == -1) {
              status = Status(TErrorCode::DISK_IO_ERROR,
                  GetHdfsErrorMsg("Error reading from HDFS file: ", file_));
            }
          } else {
            // If the file handle is borrowed, it may not be at the appropriate
            // location. Seek to the appropriate location.
            bool seek_failed = false;
            if (borrowed_hdfs_fh != nullptr) {
              if (hdfsSeek(fs_, hdfs_file, position_in_file) != 0) {
                status = Status(TErrorCode::DISK_IO_ERROR,
                  Substitute("Error seeking to $0 in file: $1: $2", position_in_file,
                      file_, GetHdfsErrorMsg("")));
                seek_failed = true;
              }
            }
            if (!seek_failed) {
              current_bytes_read = hdfsRead(fs_, hdfs_file, buffer + *bytes_read,
                  chunk_size);
              if (current_bytes_read == -1) {
                status = Status(TErrorCode::DISK_IO_ERROR,
                    GetHdfsErrorMsg("Error reading from HDFS file: ", file_));
              }
            }
          }

          // Do not retry:
          // - if read was successful (current_bytes_read != -1)
          // - or if already retried once
          // - or if this not using a borrowed file handle
          DCHECK_LE(num_retries, 1);
          if (current_bytes_read != -1 || borrowed_hdfs_fh == nullptr ||
              num_retries == 1) {
            break;
          }
          // The error may be due to a bad file handle. Reopen the file handle and retry.
          // Exclude this time from the read timers.
          io_mgr_read_timer.Stop();
          req_context_read_timer.Stop();
          ++num_retries;
          RETURN_IF_ERROR(io_mgr_->ReopenCachedHdfsFileHandle(fs_, file_string(),
              mtime(), reader_, &borrowed_hdfs_fh));
          hdfs_file = borrowed_hdfs_fh->file();
          io_mgr_read_timer.Start();
          req_context_read_timer.Start();
        }
        if (!status.ok()) break;
        if (current_bytes_read == 0) {
          // No more bytes in the file. The scan range went past the end.
          *eosr = true;
          break;
        }
        *bytes_read += current_bytes_read;

        // Collect and accumulate statistics
        GetHdfsStatistics(hdfs_file);
      }
    }

    if (borrowed_hdfs_fh != nullptr) {
      io_mgr_->ReleaseCachedHdfsFileHandle(file_string(), borrowed_hdfs_fh);
    }
    if (!status.ok()) return status;
  } else {
    DCHECK(local_file_ != nullptr);
    *bytes_read = fread(buffer, 1, bytes_to_read, local_file_);
    DCHECK_GE(*bytes_read, 0);
    DCHECK_LE(*bytes_read, bytes_to_read);
    if (*bytes_read < bytes_to_read) {
      if (ferror(local_file_) != 0) {
        return Status(TErrorCode::DISK_IO_ERROR, Substitute("Error reading from $0"
            "at byte offset: $1: $2", file_, offset_ + bytes_read_, GetStrErrMsg()));
      } else {
        // On Linux, we should only get partial reads from block devices on error or eof.
        DCHECK(feof(local_file_) != 0);
        *eosr = true;
      }
    }
  }
  bytes_read_ += *bytes_read;
  DCHECK_LE(bytes_read_, len_);
  if (bytes_read_ == len_) *eosr = true;
  return Status::OK();
}

Status ScanRange::ReadFromCache(
    const unique_lock<mutex>& reader_lock, bool* read_succeeded) {
  DCHECK(reader_lock.mutex() == &reader_->lock_ && reader_lock.owns_lock());
  DCHECK(try_cache_);
  DCHECK_EQ(bytes_read_, 0);
  *read_succeeded = false;
  Status status = Open(false);
  if (!status.ok()) return status;

  // Cached reads not supported on local filesystem.
  if (fs_ == nullptr) return Status::OK();

  {
    unique_lock<mutex> hdfs_lock(hdfs_lock_);
    RETURN_IF_ERROR(cancel_status_);

    DCHECK(exclusive_hdfs_fh_ != nullptr);
    DCHECK(external_buffer_tag_ == ExternalBufferTag::NO_BUFFER);
    cached_buffer_ =
      hadoopReadZero(exclusive_hdfs_fh_->file(), io_mgr_->cached_read_options(), len());
    if (cached_buffer_ != nullptr) {
      external_buffer_tag_ = ExternalBufferTag::CACHED_BUFFER;
    }
  }
  // Data was not cached, caller will fall back to normal read path.
  if (external_buffer_tag_ != ExternalBufferTag::CACHED_BUFFER) {
    VLOG_QUERY << "Cache read failed for scan range: " << DebugString()
               << ". Switching to disk read path.";
    // Clean up the scan range state before re-issuing it.
    Close();
    return Status::OK();
  }

  // Cached read returned a buffer, verify we read the correct amount of data.
  void* buffer = const_cast<void*>(hadoopRzBufferGet(cached_buffer_));
  int32_t bytes_read = hadoopRzBufferLength(cached_buffer_);
  // A partial read can happen when files are truncated.
  // TODO: If HDFS ever supports partially cached blocks, we'll have to distinguish
  // between errors and partially cached blocks here.
  if (bytes_read < len()) {
    VLOG_QUERY << "Error reading file from HDFS cache: " << file_ << ". Expected "
      << len() << " bytes, but read " << bytes_read << ". Switching to disk read path.";
    // Close the scan range. 'read_succeeded' is still false, so the caller will fall back
    // to non-cached read of this scan range.
    Close();
    return Status::OK();
  }

  // Create a single buffer desc for the entire scan range and enqueue that.
  // The memory is owned by the HDFS java client, not the Impala backend.
  unique_ptr<BufferDescriptor> desc = unique_ptr<BufferDescriptor>(new BufferDescriptor(
      io_mgr_, reader_, this, reinterpret_cast<uint8_t*>(buffer), 0));
  desc->len_ = bytes_read;
  desc->scan_range_offset_ = 0;
  desc->eosr_ = true;
  bytes_read_ = bytes_read;
  EnqueueReadyBuffer(move(desc));
  COUNTER_ADD_IF_NOT_NULL(reader_->bytes_read_counter_, bytes_read);
  *read_succeeded = true;
  return Status::OK();
}

void ScanRange::GetHdfsStatistics(hdfsFile hdfs_file) {
  struct hdfsReadStatistics* stats;
  if (IsHdfsPath(file())) {
    int success = hdfsFileGetReadStatistics(hdfs_file, &stats);
    if (success == 0) {
      reader_->bytes_read_local_.Add(stats->totalLocalBytesRead);
      reader_->bytes_read_short_circuit_.Add(stats->totalShortCircuitBytesRead);
      reader_->bytes_read_dn_cache_.Add(stats->totalZeroCopyBytesRead);
      if (stats->totalLocalBytesRead != stats->totalBytesRead) {
        num_remote_bytes_ += stats->totalBytesRead - stats->totalLocalBytesRead;
      }
      hdfsFileFreeReadStatistics(stats);
    }
    hdfsFileClearReadStatistics(hdfs_file);
  }
}

BufferDescriptor::BufferDescriptor(DiskIoMgr* io_mgr,
    RequestContext* reader, ScanRange* scan_range, uint8_t* buffer,
    int64_t buffer_len)
  : io_mgr_(io_mgr),
    reader_(reader),
    scan_range_(scan_range),
    buffer_(buffer),
    buffer_len_(buffer_len) {
  DCHECK(io_mgr != nullptr);
  DCHECK(scan_range != nullptr);
  DCHECK(buffer != nullptr);
  DCHECK_GE(buffer_len, 0);
}

BufferDescriptor::BufferDescriptor(DiskIoMgr* io_mgr, RequestContext* reader,
    ScanRange* scan_range, BufferPool::ClientHandle* bp_client,
    BufferPool::BufferHandle handle) :
  io_mgr_(io_mgr),
  reader_(reader),
  scan_range_(scan_range),
  buffer_(handle.data()),
  buffer_len_(handle.len()),
  bp_client_(bp_client),
  handle_(move(handle)) {
  DCHECK(io_mgr != nullptr);
  DCHECK(scan_range != nullptr);
  DCHECK(bp_client_->is_registered());
  DCHECK(handle_.is_open());
}

void BufferDescriptor::Free() {
  DCHECK(buffer_ != nullptr);
  if (!is_cached() && !is_client_buffer()) {
    // Only buffers that were allocated by DiskIoMgr need to be freed.
    ExecEnv::GetInstance()->buffer_pool()->FreeBuffer(
        bp_client_, &handle_);
  }
  buffer_ = nullptr;
}
