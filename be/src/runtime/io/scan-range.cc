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
#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/hdfs-file-reader.h"
#include "runtime/io/local-file-reader.h"
#include "util/error-util.h"
#include "util/hdfs-util.h"

#include "common/names.h"

using namespace impala;
using namespace impala::io;
using namespace std;
using std::mutex;
using std::unique_lock;

DECLARE_bool(cache_remote_file_handles);
DECLARE_bool(cache_s3_file_handles);
DECLARE_bool(cache_abfs_file_handles);
DECLARE_bool(cache_ozone_file_handles);

// Implementation of the ScanRange functionality. Each ScanRange contains a queue
// of ready buffers. For each ScanRange, there is only a single producer and
// consumer thread, i.e. only one disk thread will push to a scan range at
// any time and only one thread will remove from the queue. This is to guarantee
// that buffers are queued and read in file order.
bool ScanRange::EnqueueReadyBuffer(unique_ptr<BufferDescriptor> buffer) {
  DCHECK(buffer->buffer_ != nullptr) << "Cannot enqueue freed buffer";
  {
    unique_lock<mutex> scan_range_lock(lock_);
    DCHECK(Validate(scan_range_lock)) << DebugString();
    DCHECK(!eosr_queued_);
    if (!buffer->is_cached()) {
      // All non-cached buffers are enqueued by disk threads. Indicate that the read
      // finished.
      DCHECK(read_in_flight_);
      read_in_flight_ = false;
    }
    bool buffer_eosr_queued = buffer->eosr();
    // Don't access buffer after this as ownership is given to 'buffer_manager_'.
    buffer_manager_->EnqueueReadyBuffer(scan_range_lock, move(buffer));
    if (!cancel_status_.ok()) {
      // This range is cancelled and one or more threads may be blocked in
      // WaitForInFlightRead() waiting for the read to complete. Wake up all of them.
      buffer_ready_cv_.NotifyAll();
      return false;
    }
    eosr_queued_ = buffer_eosr_queued;
  }
  buffer_ready_cv_.NotifyOne();
  return true;
}

Status ScanRange::GetNext(unique_ptr<BufferDescriptor>* buffer) {
  DCHECK(*buffer == nullptr);
  bool eosr;
  {
    unique_lock<mutex> scan_range_lock(lock_);
    DCHECK(Validate(scan_range_lock)) << DebugString();
    while (!all_buffers_returned(scan_range_lock) &&
        buffer_manager_->is_readybuffer_empty()) {
      buffer_ready_cv_.Wait(scan_range_lock);
    }
    // No more buffers to return - return the cancel status or OK if not cancelled.
    if (all_buffers_returned(scan_range_lock)) {
      // Wait until read finishes to ensure buffers are freed.
      while (read_in_flight_) buffer_ready_cv_.Wait(scan_range_lock);
      DCHECK(buffer_manager_->is_readybuffer_empty());
      return cancel_status_;
    }

    // Remove the first ready buffer from the queue and return it
    buffer_manager_->PopFirstReadyBuffer(scan_range_lock, buffer);
    eosr = (*buffer)->eosr();
  }

  if (eosr) reader_->RemoveActiveScanRange(this);
  // Update tracking counters. The buffer has now moved from the IoMgr to the caller.
  buffer_manager_->add_buffers_in_reader(1);
  return Status::OK();
}

void ScanRange::ReturnBuffer(unique_ptr<BufferDescriptor> buffer_desc) {
  DCHECK(buffer_desc != nullptr);
  DCHECK_EQ(buffer_desc->scan_range(), this);
  vector<unique_ptr<BufferDescriptor>> buffers;
  buffers.emplace_back(move(buffer_desc));
  AddUnusedBuffers(move(buffers), true);
}

void ScanRange::AddUnusedBuffers(vector<unique_ptr<BufferDescriptor>>&& buffers,
    bool returned) {
  bool unblocked = false;
  {
    unique_lock<mutex> scan_range_lock(lock_);
    bool buffer_added = buffer_manager_->AddUnusedBuffers(scan_range_lock,
      move(buffers), returned);
    // Unblock scan range if new buffer has been added to unused buffer queue.
    if (buffer_added && blocked_on_buffer_) {
      blocked_on_buffer_ = false;
      unblocked = true;
    }
  }
  // Need to schedule scan range, if it has been blocked above.
  // Must drop the ScanRange lock before invoking ScheduleScanRange.
  if (unblocked) ScheduleScanRange();
}

bool ScanRange::FileHandleCacheEnabled() const {
  // Global flag for all file handle caching
  if (!is_file_handle_caching_enabled()) return false;

  if (expected_local_ && IsHdfsPath(file())) return true;
  if (FLAGS_cache_remote_file_handles) {
    if (disk_id_ == io_mgr_->RemoteDfsDiskId()) return true;
  }

  if (FLAGS_cache_ozone_file_handles) {
    if (expected_local_ && IsOzonePath(file())) return true;
    if (disk_id_ == io_mgr_->RemoteOzoneDiskId()) return true;
  }

  if (FLAGS_cache_s3_file_handles) {
    if (disk_id_ == io_mgr_->RemoteS3DiskId()) return true;
  }

  if (FLAGS_cache_abfs_file_handles) {
    if (disk_id_ == io_mgr_->RemoteAbfsDiskId()) return true;
  }

  return false;
}

ReadOutcome ScanRange::DoReadInternal(DiskQueue* queue, int disk_id, bool use_local_buff,
    bool use_mem_buffer, shared_lock<shared_mutex>* local_file_lock) {
  int64_t bytes_remaining = bytes_to_read_ - bytes_read_;
  DCHECK_GT(bytes_remaining, 0);
  // Can't be set to true together.
  DCHECK(!(use_local_buff && use_mem_buffer));

  unique_ptr<BufferDescriptor> buffer_desc;
  FileReader* file_reader = nullptr;
  {
    unique_lock<mutex> lock(lock_);
    DCHECK(!read_in_flight_);
    if (!cancel_status_.ok()) return ReadOutcome::CANCELLED;

    if (buffer_manager_->is_client_buffer()) {
      buffer_desc = unique_ptr<BufferDescriptor>(new BufferDescriptor(
          this, client_buffer_.data, client_buffer_.len));
    } else {
      DCHECK(buffer_manager_->is_internal_buffer())
          << "This code path does not handle other buffer types, i.e. HDFS cache. "
          << "Buffer tag = "
          << static_cast<int>(buffer_manager_->buffer_tag());
      buffer_desc = buffer_manager_->GetUnusedBuffer(lock);
      if (buffer_desc == nullptr) {
        // No buffer available - the range will be rescheduled when a buffer is added.
        blocked_on_buffer_ = true;
        return ReadOutcome::BLOCKED_ON_BUFFER;
      }
      buffer_manager_->add_iomgr_buffer_cumulative_bytes_used(buffer_desc->buffer_len());
    }
    read_in_flight_ = true;
    // Set the correct reader to read the range if the memory buffer is not available.
    if (!use_mem_buffer) {
      if (use_local_buff) {
        file_reader = local_buffer_reader_.get();
        file_ = disk_buffer_file_->path();
      } else {
        file_reader = file_reader_.get();
      }
      use_local_buffer_ = use_local_buff;
    }
  }

  bool eof = false;
  Status read_status = Status::OK();

  if (use_mem_buffer) {
    // The only scenario to use the memory buffer is for the temporary files, the range
    // is supposed to be read in one round.
    // For the efficiency consideration, don't have the lock of the memory block, the
    // safety is implicitly guaranteed by the physical lock of the disk file, which is
    // required while removing the disk file and the memory blocks. The other case of
    // removing the memory block is when all of the pages have been read, and that could
    // only happen after this read.
    DCHECK(local_file_lock != nullptr);
    read_status = disk_buffer_file_->ReadFromMemBuffer(
        offset_, bytes_to_read_, buffer_desc->buffer_, *local_file_lock);
    if (read_status.ok()) {
      buffer_desc->len_ = bytes_to_read_;
      eof = true;
      COUNTER_ADD_IF_NOT_NULL(reader_->read_use_mem_counter_, 1L);
      COUNTER_ADD_IF_NOT_NULL(reader_->bytes_read_use_mem_counter_, buffer_desc->len_);
      COUNTER_ADD_IF_NOT_NULL(reader_->bytes_read_counter_, buffer_desc->len_);
    }
  } else {
    DCHECK(file_reader != nullptr);

    // No locks in this section.  Only working on local vars.  We don't want to hold a
    // lock across the read call.
    // To use the file handle cache:
    // 1. It must be enabled at the daemon level.
    // 2. It must be enabled for the particular filesystem.
    bool use_file_handle_cache = FileHandleCacheEnabled();
    VLOG_FILE << (use_file_handle_cache ? "Using" : "Skipping")
              << " file handle cache for " << (expected_local_ ? "local" : "remote")
              << " file " << file();
    // Delay open if configured to use a file handle cache or data cache as cache hits
    // don't require an explicit Open.
    if (!file_reader->SupportsDelayedOpen()
        || !(use_file_handle_cache || UseDataCache())) {
      read_status = file_reader->Open();
    }
    if (read_status.ok()) {
      COUNTER_ADD_IF_NOT_NULL(reader_->active_read_thread_counter_, 1L);
      COUNTER_BITOR_IF_NOT_NULL(reader_->disks_accessed_bitmap_, 1LL << disk_id);

      if (sub_ranges_.empty()) {
        DCHECK(cache_.data == nullptr);
        read_status =
            file_reader->ReadFromPos(queue, offset_ + bytes_read_, buffer_desc->buffer_,
                min(bytes_to_read() - bytes_read_, buffer_desc->buffer_len_),
                &buffer_desc->len_, &eof);
      } else {
        read_status = ReadSubRanges(queue, buffer_desc.get(), &eof, file_reader);
      }

      COUNTER_ADD_IF_NOT_NULL(reader_->bytes_read_counter_, buffer_desc->len_);
      COUNTER_ADD_IF_NOT_NULL(reader_->active_read_thread_counter_, -1L);
      if (use_local_buffer_) {
        COUNTER_ADD_IF_NOT_NULL(reader_->read_use_local_disk_counter_, 1L);
        COUNTER_ADD_IF_NOT_NULL(
            reader_->bytes_read_use_local_disk_counter_, buffer_desc->len_);
      }
    }
  }

  DCHECK(buffer_desc->buffer_ != nullptr);
  DCHECK(!buffer_desc->is_cached())
      << "Pure HDFS cache reads don't go through this code path.";
  if (!read_status.ok()) {
    // Free buffer to release resources before we cancel the range so that all buffers
    // are freed at cancellation.
    buffer_desc->Free();
    buffer_desc.reset();

    // Propagate 'read_status' to the scan range. This will also wake up any waiting
    // threads.
    CancelInternal(read_status, true);
    // At this point we cannot touch the state of this range because the client
    // may notice cancellation, then reuse the scan range.
    return ReadOutcome::CANCELLED;
  }

  {
    unique_lock<mutex> lock(lock_);
    bytes_read_ += buffer_desc->len();
    DCHECK_LE(bytes_read_, bytes_to_read_);

    // It is end of stream if it is end of file, or read all the bytes.
    buffer_desc->eosr_ = eof || bytes_read_ == bytes_to_read_;
  }

  // After calling EnqueueReadyBuffer(), it is no longer valid to touch 'buffer_desc'.
  // Store the state we need before calling EnqueueReadyBuffer().
  bool eosr = buffer_desc->eosr();
  // No more reads for this scan range - we can close it.
  if (eosr && file_reader != nullptr) file_reader->Close();
  // Read successful - enqueue the buffer and return the appropriate outcome.
  if (!EnqueueReadyBuffer(move(buffer_desc))) return ReadOutcome::CANCELLED;
  // At this point, if eosr=true, then we cannot touch the state of this scan range
  // because the client may notice eos, then reuse the scan range.
  return eosr ? ReadOutcome::SUCCESS_EOSR : ReadOutcome::SUCCESS_NO_EOSR;
}

ReadOutcome ScanRange::DoRead(DiskQueue* queue, int disk_id) {
  bool use_local_buffer = false;
  bool use_mem_buffer = false;
  if (disk_file_ != nullptr && disk_file_->disk_type() != DiskFileType::LOCAL
      && disk_buffer_file_ != nullptr) {
    // The sequence for acquiring the locks should always be from the local to
    // the remote to avoid deadlocks.
    shared_lock<shared_mutex> local_file_lock(*(disk_buffer_file_->GetFileLock()));
    shared_lock<shared_mutex> remote_file_lock(*(disk_file_->GetFileLock()));
    {
      unique_lock<SpinLock> buffer_file_lock(*(disk_buffer_file_->GetStatusLock()));
      unique_lock<SpinLock> file_lock(*(disk_file_->GetStatusLock()));
      if (disk_buffer_file_->is_deleted(buffer_file_lock)
          && disk_file_->is_deleted(file_lock)) {
        // If both of the local buffer file and the remote file have been deleted,
        // the only case could be the query is cancelled, so that both files are deleted.
        return ReadOutcome::CANCELLED;
      }

      // The range can be read from local for two cases.
      // 1. If the local buffer file is not deleted(evicted) yet.
      // 2. A block of the file, which contains the range, has been read and stored in
      // the memory.
      // If we don't meet any of the cases, the range needs to be read from the remote.
      if (!disk_buffer_file_->is_deleted(buffer_file_lock)) {
        use_local_buffer = true;
      } else if (disk_buffer_file_->CanReadFromReadBuffer(local_file_lock, offset_)) {
        use_mem_buffer = true;
      } else {
        // Read from the remote file. The remote file must be in persisted status.
        DCHECK(disk_file_->is_persisted(file_lock));
      }
    }
    return DoReadInternal(
        queue, disk_id, use_local_buffer, use_mem_buffer, &local_file_lock);
  }
  return DoReadInternal(queue, disk_id, use_local_buffer, use_mem_buffer);
}

Status ScanRange::ReadSubRanges(
    DiskQueue* queue, BufferDescriptor* buffer_desc, bool* eof, FileReader* file_reader) {
  buffer_desc->len_ = 0;
  while (buffer_desc->len() < buffer_desc->buffer_len()
      && sub_range_pos_.index < sub_ranges_.size()) {
    SubRange& sub_range = sub_ranges_[sub_range_pos_.index];
    int64_t offset = sub_range.offset + sub_range_pos_.bytes_read;
    int64_t bytes_to_read = min(sub_range.length - sub_range_pos_.bytes_read,
        buffer_desc->buffer_len() - buffer_desc->len());

    if (cache_.data != nullptr) {
      // The cache_.data buffer starts at offset_, so adjust the starting
      // offset for the copies.
      int64_t buffer_offset = offset - offset_;
      DCHECK_LE(buffer_offset + bytes_to_read, cache_.len);
      // DCHECKs are only effective with test coverage, so also return an error
      // if this would read past the edge of the cache_.data buffer. We wanted
      // bytes_to_read, but only cache_.len - buffer_offset bytes were available.
      if (buffer_offset + bytes_to_read > cache_.len) {
        return Status(TErrorCode::SCANNER_INCOMPLETE_READ, bytes_to_read,
            cache_.len - buffer_offset, file(), offset);
      }
      memcpy(buffer_desc->buffer_ + buffer_desc->len(),
          cache_.data + buffer_offset, bytes_to_read);
    } else {
      int64_t current_bytes_read;
      Status read_status = file_reader->ReadFromPos(queue, offset,
          buffer_desc->buffer_ + buffer_desc->len(), bytes_to_read, &current_bytes_read,
          eof);
      if (!read_status.ok()) return read_status;
      if (current_bytes_read != bytes_to_read) {
        DCHECK(*eof);
        DCHECK_LT(current_bytes_read, bytes_to_read);
        return Status(TErrorCode::SCANNER_INCOMPLETE_READ, bytes_to_read,
            current_bytes_read, file(), offset);
      }
    }

    buffer_desc->len_ += bytes_to_read;
    sub_range_pos_.bytes_read += bytes_to_read;
    if (sub_range_pos_.bytes_read == sub_range.length) {
      sub_range_pos_.index += 1;
      sub_range_pos_.bytes_read = 0;
    }
  }
  return Status::OK();
}

void ScanRange::SetBlockedOnBuffer() {
  unique_lock<mutex> lock(lock_);
  blocked_on_buffer_ = true;
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
  FileReader* file_reader = nullptr;
  {
    // Grab both locks to make sure that we don't change 'cancel_status_' while other
    // threads are in critical sections.
    unique_lock<mutex> scan_range_lock(lock_);
    {
      file_reader = use_local_buffer_ ? local_buffer_reader_.get() : file_reader_.get();
      unique_lock<SpinLock> fs_lock(file_reader->lock());
      DCHECK(Validate(scan_range_lock)) << DebugString();
      // If already cancelled, preserve the original reason for cancellation. Most of the
      // cleanup is not required if already cancelled, but we need to set
      // 'read_in_flight_' to false.
      if (cancel_status_.ok()) cancel_status_ = status;
    }

    /// Clean up ready buffers (i.e., 'buffer_manager_->ready_buffers_') while still
    /// holding 'lock_' to prevent other threads from seeing inconsistent state.
    buffer_manager_->CleanUpReadyBuffers(scan_range_lock);

    /// Clean up buffers that we don't need any more because we won't read any more data.
    buffer_manager_->CleanUpUnusedBuffers(scan_range_lock);
    if (read_error) {
      DCHECK(read_in_flight_);
      read_in_flight_ = false;
    }
  }
  buffer_ready_cv_.NotifyAll();

  // For cached buffers, we can't close the range until the cached buffer is returned.
  // Close() is called from ScanRange::CleanUpBufferLocked().
  // TODO: IMPALA-4249 - this Close() call makes it unsafe to reuse a cancelled scan
  // range, because there is no synchronisation between this Close() call and the
  // client adding the ScanRange back into the IoMgr.
  if (!buffer_manager_->is_cached()) {
    DCHECK(file_reader != nullptr);
    file_reader->Close();
  }
}

void ScanRange::WaitForInFlightRead() {
  unique_lock<mutex> scan_range_lock(lock_);
  while (read_in_flight_) {
    buffer_ready_cv_.Wait(scan_range_lock);
  }
}

string ScanRange::DebugString() const {
  stringstream ss;
  ss << "file=" << file_ << " disk_id=" << disk_id_ << " offset=" << offset_;
  if (file_reader_) ss << " " << file_reader_->DebugString();
  ss << " cancel_status=" << cancel_status_.GetDetail()
     << buffer_manager_->DebugString()
     << " blocked_on_buffer=" << blocked_on_buffer_;
  return ss.str();
}

bool ScanRange::Validate(const std::unique_lock<std::mutex>& scan_range_lock) {
  if (bytes_read_ > bytes_to_read_) {
    LOG(ERROR) << "Bytes read tracking is wrong. Too many bytes have been read."
                 << " bytes_read_=" << bytes_read_
                 << " bytes_to_read_=" << bytes_to_read_;
    return false;
  }
  // Perform validation for buffer state
  if (!buffer_manager_->Validate(scan_range_lock)) {
    LOG(ERROR) << "Buffer state validation failed for this scan range: "
        << DebugString();
    return false;
  }

  return true;
}

ScanRange::ScanRange()
  : RequestRange(RequestType::READ) {
  buffer_manager_ = make_unique<ScanBufferManager>(this);
}

ScanRange::~ScanRange() {
  DCHECK(!read_in_flight_);
}

void ScanRange::Reset(const FileInfo &fi, int64_t len, int64_t offset, int disk_id,
    bool expected_local, const BufferOpts& buffer_opts, void* meta_data,
    DiskFile* disk_file, DiskFile* disk_buffer_file) {
  Reset(fi, len, offset, disk_id, expected_local, buffer_opts, {},
      meta_data, disk_file, disk_buffer_file);
}

ScanRange* ScanRange::AllocateScanRange(ObjectPool* obj_pool, const FileInfo &fi,
    int64_t len, int64_t offset, std::vector<SubRange>&& sub_ranges, void* metadata,
    int disk_id, bool expected_local, const BufferOpts& buffer_opts) {
  DCHECK_GE(disk_id, -1);
  DCHECK_GE(offset, 0);
  DCHECK_GE(len, 0);
  disk_id = ExecEnv::GetInstance()->disk_io_mgr()->AssignQueue(
      fi.filename, disk_id, expected_local, /* check_default_fs */ true);
  ScanRange* range = obj_pool->Add(new ScanRange);
  range->Reset(fi, len, offset, disk_id, expected_local, buffer_opts,
      move(sub_ranges), metadata);
  return range;
}

void ScanRange::Reset(const FileInfo &fi, int64_t len, int64_t offset, int disk_id,
    bool expected_local, const BufferOpts& buffer_opts, vector<SubRange>&& sub_ranges,
    void* meta_data, DiskFile* disk_file, DiskFile* disk_buffer_file) {
  DCHECK(buffer_manager_->is_readybuffer_empty());
  DCHECK(!read_in_flight_);
  DCHECK(fi.filename != nullptr);
  DCHECK_GE(len, 0);
  DCHECK_GE(offset, 0);
  DCHECK(buffer_opts.client_buffer_ == nullptr ||
         buffer_opts.client_buffer_len_ >= len_);
  fs_ = fi.fs;
  if (fs_ != nullptr) {
    file_reader_ = make_unique<HdfsFileReader>(this, fs_, false);
    local_buffer_reader_ = make_unique<LocalFileReader>(this);
  } else {
    file_reader_ = make_unique<LocalFileReader>(this);
  }
  file_ = fi.filename;
  len_ = len;
  bytes_to_read_ = len;
  offset_ = offset;
  disk_id_ = disk_id;
  is_encrypted_ = fi.is_encrypted;
  is_erasure_coded_ = fi.is_erasure_coded;
  cache_options_ = buffer_opts.cache_options_;
  disk_file_ = disk_file;
  disk_buffer_file_ = disk_buffer_file;

  // HDFS ranges must have an mtime > 0. Local ranges do not use mtime.
  mtime_ = fi.mtime;
  if (fs_) DCHECK_GT(mtime_, 0);
  meta_data_ = meta_data;
  if (buffer_opts.client_buffer_ != nullptr) {
    buffer_manager_->set_client_buffer();
    client_buffer_.data = buffer_opts.client_buffer_;
    client_buffer_.len = buffer_opts.client_buffer_len_;
  } else {
    buffer_manager_->set_internal_buffer();
  }

  expected_local_ = expected_local;
  io_mgr_ = nullptr;
  reader_ = nullptr;
  sub_ranges_.clear();
  sub_range_pos_ = {};
  InitSubRanges(move(sub_ranges));
}

void ScanRange::InitSubRanges(vector<SubRange>&& sub_ranges) {
  sub_ranges_ = std::move(sub_ranges);
  DCHECK(ValidateSubRanges());
  MergeSubRanges();
  DCHECK(ValidateSubRanges());
  sub_range_pos_ = {};

  if (sub_ranges_.empty()) return;

  int length_sum = 0;
  for (auto& sub_range : sub_ranges_) {
    length_sum += sub_range.length;
  }
  bytes_to_read_ = length_sum;
}

bool ScanRange::ValidateSubRanges() {
  for (int i = 0; i < sub_ranges_.size(); ++i) {
    SubRange& sub_range = sub_ranges_[i];
    if (sub_range.length <= 0) return false;
    if (sub_range.offset < offset_) return false;
    if (sub_range.offset + sub_range.length > offset_ + len_) return false;

    if (i == sub_ranges_.size() - 1) break;

    SubRange& next_sub_range = sub_ranges_[i+1];
    if (sub_range.offset + sub_range.length > next_sub_range.offset) return false;
  }
  return true;
}

void ScanRange::MergeSubRanges() {
  if (sub_ranges_.empty()) return;
  for (int i = 0; i < sub_ranges_.size() - 1; ++i) {
    SubRange& current = sub_ranges_[i];
    int j = i + 1;
    for (; j < sub_ranges_.size(); ++j) {
      SubRange& sr_j = sub_ranges_[j];
      if (sr_j.offset == current.offset + current.length) {
        current.length += sr_j.length;
      } else {
        break;
      }
    }
    if (j > i + 1) {
      sub_ranges_.erase(sub_ranges_.begin() + i + 1, sub_ranges_.begin() + j);
    }
  }
}

void ScanRange::InitInternal(DiskIoMgr* io_mgr, RequestContext* reader) {
  DCHECK(!read_in_flight_);
  io_mgr_ = io_mgr;
  reader_ = reader;
  buffer_manager_->Init();
  cancel_status_ = Status::OK();
  eosr_queued_ = false;
  blocked_on_buffer_ = false;
  bytes_read_ = 0;
  sub_range_pos_ = {};
  file_reader_->ResetState();
  if (local_buffer_reader_ != nullptr) local_buffer_reader_->ResetState();
  unique_lock<mutex> scan_range_lock(lock_);
  DCHECK(Validate(scan_range_lock)) << DebugString();
}

void ScanRange::SetFileReader(unique_ptr<FileReader> file_reader) {
  file_reader_ = move(file_reader);
}

Status ScanRange::ReadFromCache(
    const unique_lock<mutex>& reader_lock, bool* read_succeeded) {
  DCHECK(reader_lock.mutex() == &reader_->lock_ && reader_lock.owns_lock());
  DCHECK(UseHdfsCache());
  DCHECK_EQ(bytes_read_, 0);
  *read_succeeded = false;
  Status status = file_reader_->Open();
  if (!status.ok()) return status;

  // Check cancel status.
  {
    unique_lock<mutex> lock(lock_);
    RETURN_IF_ERROR(cancel_status_);
  }

  file_reader_->CachedFile(&cache_.data, &cache_.len);
  // Data was not cached, caller will fall back to normal read path.
  if (cache_.data == nullptr) {
    VLOG_QUERY << "Cache read failed for scan range: " << DebugString()
               << ". Switching to disk read path.";
    // Clean up the scan range state before re-issuing it.
    file_reader_->Close();
    return Status::OK();
  }
  // A partial read can happen when files are truncated.
  // TODO: If HDFS ever supports partially cached blocks, we'll have to distinguish
  // between errors and partially cached blocks here.
  if (cache_.len < len()) {
    VLOG_QUERY << "Error reading file from HDFS cache: " << file_ << ". Expected "
      << len() << " bytes, but read " << cache_.len << ". Switching to disk read path.";
    // Null out the cache buffer to avoid any interactions when this falls
    // back to the regular read path.
    cache_.len = 0;
    cache_.data = nullptr;
    // Close the scan range. 'read_succeeded' is still false, so the caller will fall back
    // to non-cached read of this scan range.
    file_reader_->Close();
    return Status::OK();
  }

  *read_succeeded = true;
  // If there are sub-ranges, then we need to memcpy() them from the cached buffer.
  if (HasSubRanges()) return Status::OK();

  DCHECK(!buffer_manager_->is_client_buffer());
  buffer_manager_->set_cached_buffer();
  bytes_read_ = cache_.len;

  // Create a single buffer desc for the entire scan range and enqueue that.
  // The memory is owned by the HDFS java client, not the Impala backend.
  unique_ptr<BufferDescriptor> desc = unique_ptr<BufferDescriptor>(new BufferDescriptor(
      this, cache_.data, 0));
  desc->len_ = cache_.len;
  desc->eosr_ = true;
  EnqueueReadyBuffer(move(desc));
  COUNTER_ADD_IF_NOT_NULL(reader_->bytes_read_counter_, cache_.len);
  return Status::OK();
}

Status ScanRange::AllocateBuffersForRange(
    BufferPool::ClientHandle* bp_client, int64_t max_bytes,
    int64_t min_buffer_size, int64_t max_buffer_size) {
  vector<unique_ptr<BufferDescriptor>> buffers;
  Status status = buffer_manager_->AllocateBuffersForRange(bp_client, max_bytes, buffers,
      min_buffer_size, max_buffer_size);
  if (!status.ok() && !buffers.empty()) {
    unique_lock<mutex> lock(lock_);
    buffer_manager_->CleanUpBuffers(lock, move(buffers));
  } else {
    AddUnusedBuffers(move(buffers), false);
  }
  return status;
}

void ScanRange::ScheduleScanRange() {
  unique_lock<mutex> reader_lock(reader_->lock_);
  // Reader may have been cancelled after we dropped 'scan_range_lock' above.
  if (reader_->state_ == RequestContext::Cancelled) {
    DCHECK(!cancel_status_.ok());
  } else {
    reader_->ScheduleScanRange(reader_lock, this);
  }
}

void ScanRange::CloseReader(const unique_lock<mutex>& scan_range_lock) {
  if (all_buffers_returned(scan_range_lock) &&
      buffer_manager_->num_buffers_in_reader() == 0) {
    // Close the readers if there are no more buffers in the reader and no more buffers
    // will be returned to readers in future. Close() is idempotent so it is ok to call
    // multiple times during cleanup so long as the range is actually finished.
    if (!use_local_buffer_) {
      file_reader_->Close();
    } else {
      local_buffer_reader_->Close();
    }
  }
}

BufferDescriptor::BufferDescriptor(ScanRange* scan_range,
    uint8_t* buffer, int64_t buffer_len)
  : scan_range_(scan_range),
    buffer_(buffer),
    buffer_len_(buffer_len) {
  DCHECK(scan_range != nullptr);
  DCHECK(buffer != nullptr);
  DCHECK_GE(buffer_len, 0);
}

BufferDescriptor::BufferDescriptor(ScanRange* scan_range,
    BufferPool::ClientHandle* bp_client, BufferPool::BufferHandle handle) :
  scan_range_(scan_range),
  buffer_(handle.data()),
  buffer_len_(handle.len()),
  bp_client_(bp_client),
  handle_(move(handle)) {
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
