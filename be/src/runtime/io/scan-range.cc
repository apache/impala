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

// Implementation of the ScanRange functionality. Each ScanRange contains a queue
// of ready buffers. For each ScanRange, there is only a single producer and
// consumer thread, i.e. only one disk thread will push to a scan range at
// any time and only one thread will remove from the queue. This is to guarantee
// that buffers are queued and read in file order.

bool ScanRange::EnqueueBuffer(
    const unique_lock<mutex>& reader_lock, unique_ptr<BufferDescriptor> buffer) {
  DCHECK(reader_lock.mutex() == &reader_->lock_ && reader_lock.owns_lock());
  {
    unique_lock<mutex> scan_range_lock(lock_);
    DCHECK(Validate()) << DebugString();
    DCHECK(!eosr_returned_);
    DCHECK(!eosr_queued_);
    if (is_cancelled_) {
      // Return the buffer, this range has been cancelled
      if (buffer->buffer_ != nullptr) {
        io_mgr_->num_buffers_in_readers_.Add(1);
        reader_->num_buffers_in_reader_.Add(1);
      }
      reader_->num_used_buffers_.Add(-1);
      io_mgr_->ReturnBuffer(move(buffer));
      return false;
    }
    reader_->num_ready_buffers_.Add(1);
    eosr_queued_ = buffer->eosr();
    ready_buffers_.emplace_back(move(buffer));

    DCHECK_LE(ready_buffers_.size(), DiskIoMgr::SCAN_RANGE_READY_BUFFER_LIMIT);
    blocked_on_queue_ = ready_buffers_.size() == DiskIoMgr::SCAN_RANGE_READY_BUFFER_LIMIT;
  }

  buffer_ready_cv_.NotifyOne();

  return blocked_on_queue_;
}

Status ScanRange::GetNext(unique_ptr<BufferDescriptor>* buffer) {
  DCHECK(*buffer == nullptr);
  bool eosr;
  {
    unique_lock<mutex> scan_range_lock(lock_);
    if (eosr_returned_) return Status::OK();
    DCHECK(Validate()) << DebugString();

    while (ready_buffers_.empty() && !is_cancelled_) {
      buffer_ready_cv_.Wait(scan_range_lock);
    }

    if (is_cancelled_) {
      DCHECK(!status_.ok());
      return status_;
    }

    // Remove the first ready buffer from the queue and return it
    DCHECK(!ready_buffers_.empty());
    DCHECK_LE(ready_buffers_.size(), DiskIoMgr::SCAN_RANGE_READY_BUFFER_LIMIT);
    *buffer = move(ready_buffers_.front());
    ready_buffers_.pop_front();
    eosr_returned_ = (*buffer)->eosr();
    eosr = (*buffer)->eosr();
  }

  // Update tracking counters. The buffer has now moved from the IoMgr to the
  // caller.
  io_mgr_->num_buffers_in_readers_.Add(1);
  reader_->num_buffers_in_reader_.Add(1);
  reader_->num_ready_buffers_.Add(-1);
  reader_->num_used_buffers_.Add(-1);
  if (eosr) reader_->num_finished_ranges_.Add(1);

  Status status = (*buffer)->status_;
  if (!status.ok()) {
    io_mgr_->ReturnBuffer(move(*buffer));
    return status;
  }

  unique_lock<mutex> reader_lock(reader_->lock_);

  DCHECK(reader_->Validate()) << endl << reader_->DebugString();
  if (reader_->state_ == RequestContext::Cancelled) {
    reader_->blocked_ranges_.Remove(this);
    Cancel(reader_->status_);
    io_mgr_->ReturnBuffer(move(*buffer));
    return status_;
  }

  {
    // Check to see if we can re-schedule a blocked range. Note that EnqueueBuffer()
    // may have been called after we released 'lock_' above so we need to re-check
    // whether the queue is full.
    unique_lock<mutex> scan_range_lock(lock_);
    if (blocked_on_queue_
        && ready_buffers_.size() < DiskIoMgr::SCAN_RANGE_READY_BUFFER_LIMIT
        && !eosr_queued_) {
      blocked_on_queue_ = false;
      // This scan range was blocked and is no longer, add it to the reader
      // queue again.
      reader_->blocked_ranges_.Remove(this);
      reader_->ScheduleScanRange(this);
    }
  }
  return Status::OK();
}

void ScanRange::Cancel(const Status& status) {
  // Cancelling a range that was never started, ignore.
  if (io_mgr_ == nullptr) return;

  DCHECK(!status.ok());
  {
    // Grab both locks to make sure that all working threads see is_cancelled_.
    unique_lock<mutex> scan_range_lock(lock_);
    unique_lock<mutex> hdfs_lock(hdfs_lock_);
    DCHECK(Validate()) << DebugString();
    if (is_cancelled_) return;
    is_cancelled_ = true;
    status_ = status;
  }
  buffer_ready_cv_.NotifyAll();
  CleanupQueuedBuffers();

  // For cached buffers, we can't close the range until the cached buffer is returned.
  // Close() is called from DiskIoMgr::ReturnBuffer().
  if (external_buffer_tag_ != ExternalBufferTag::CACHED_BUFFER) Close();
}

void ScanRange::CleanupQueuedBuffers() {
  DCHECK(is_cancelled_);
  io_mgr_->num_buffers_in_readers_.Add(ready_buffers_.size());
  reader_->num_buffers_in_reader_.Add(ready_buffers_.size());
  reader_->num_used_buffers_.Add(-ready_buffers_.size());
  reader_->num_ready_buffers_.Add(-ready_buffers_.size());

  while (!ready_buffers_.empty()) {
    io_mgr_->ReturnBuffer(move(ready_buffers_.front()));
    ready_buffers_.pop_front();
  }
}

string ScanRange::DebugString() const {
  stringstream ss;
  ss << "file=" << file_ << " disk_id=" << disk_id_ << " offset=" << offset_
     << " len=" << len_ << " bytes_read=" << bytes_read_
     << " buffer_queue=" << ready_buffers_.size()
     << " hdfs_file=" << exclusive_hdfs_fh_;
  return ss.str();
}

bool ScanRange::Validate() {
  if (bytes_read_ > len_) {
    LOG(WARNING) << "Bytes read tracking is wrong. Shouldn't read past the scan range."
                 << " bytes_read_=" << bytes_read_ << " len_=" << len_;
    return false;
  }
  if (eosr_returned_ && !eosr_queued_) {
    LOG(WARNING) << "Returned eosr to reader before finishing reading the scan range"
                 << " eosr_returned_=" << eosr_returned_
                 << " eosr_queued_=" << eosr_queued_;
    return false;
  }
  return true;
}

ScanRange::ScanRange()
  : RequestRange(RequestType::READ),
    num_remote_bytes_(0),
    external_buffer_tag_(ExternalBufferTag::NO_BUFFER),
    mtime_(-1) {}

ScanRange::~ScanRange() {
  DCHECK(exclusive_hdfs_fh_ == nullptr) << "File was not closed.";
  DCHECK(external_buffer_tag_ != ExternalBufferTag::CACHED_BUFFER)
      << "Cached buffer was not released.";
}

void ScanRange::Reset(hdfsFS fs, const char* file, int64_t len, int64_t offset,
    int disk_id, bool expected_local, const BufferOpts& buffer_opts, void* meta_data) {
  DCHECK(ready_buffers_.empty());
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
  // Reader must provide MemTracker or a buffer.
  DCHECK(external_buffer_tag_ == ExternalBufferTag::CLIENT_BUFFER
      || reader->mem_tracker_ != nullptr);
  io_mgr_ = io_mgr;
  reader_ = reader;
  local_file_ = nullptr;
  exclusive_hdfs_fh_ = nullptr;
  bytes_read_ = 0;
  is_cancelled_ = false;
  eosr_queued_= false;
  eosr_returned_= false;
  blocked_on_queue_ = false;
  DCHECK(Validate()) << DebugString();
}

Status ScanRange::Open(bool use_file_handle_cache) {
  unique_lock<mutex> hdfs_lock(hdfs_lock_);
  if (is_cancelled_) return Status::CANCELLED;

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
  if (ImpaladMetrics::IO_MGR_NUM_OPEN_FILES != nullptr) {
    ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(1L);
  }
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

    if (FLAGS_use_hdfs_pread) {
      // Update Hedged Read Metrics.
      // We call it only if the --use_hdfs_pread flag is set, to avoid having the
      // libhdfs client malloc and free a hdfsHedgedReadMetrics object unnecessarily
      // otherwise. 'hedged_metrics' is only set upon success.
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
  if (closed_file && ImpaladMetrics::IO_MGR_NUM_OPEN_FILES != nullptr) {
    ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(-1L);
  }
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
  unique_lock<mutex> hdfs_lock(hdfs_lock_);
  if (is_cancelled_) return Status::CANCELLED;

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
      ScopedTimer<MonotonicStopWatch> io_mgr_read_timer(&io_mgr_->read_timer_);
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
    if (is_cancelled_) return Status::CANCELLED;

    DCHECK(exclusive_hdfs_fh_ != nullptr);
    DCHECK(external_buffer_tag_ == ExternalBufferTag::NO_BUFFER);
    cached_buffer_ =
      hadoopReadZero(exclusive_hdfs_fh_->file(), io_mgr_->cached_read_options_, len());
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
  // 'mem_tracker' is nullptr because the memory is owned by the HDFS java client,
  // not the Impala backend.
  unique_ptr<BufferDescriptor> desc = unique_ptr<BufferDescriptor>(new BufferDescriptor(
      io_mgr_, reader_, this, reinterpret_cast<uint8_t*>(buffer), 0, nullptr));
  desc->len_ = bytes_read;
  desc->scan_range_offset_ = 0;
  desc->eosr_ = true;
  bytes_read_ = bytes_read;
  EnqueueBuffer(reader_lock, move(desc));
  if (reader_->bytes_read_counter_ != nullptr) {
    COUNTER_ADD(reader_->bytes_read_counter_, bytes_read);
  }
  *read_succeeded = true;
  reader_->num_used_buffers_.Add(1);
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
