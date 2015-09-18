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
#include "util/error-util.h"
#include "util/hdfs-util.h"

#include "common/names.h"

using namespace impala;

// A very large max value to prevent things from going out of control. Not
// expected to ever hit this value (1GB of buffered data per range).
const int MAX_QUEUE_CAPACITY = 128;
const int MIN_QUEUE_CAPACITY = 2;

// Implementation of the ScanRange functionality. Each ScanRange contains a queue
// of ready buffers. For each ScanRange, there is only a single producer and
// consumer thread, i.e. only one disk thread will push to a scan range at
// any time and only one thread will remove from the queue. This is to guarantee
// that buffers are queued and read in file order.

// This must be called with the reader lock taken.
bool DiskIoMgr::ScanRange::EnqueueBuffer(BufferDescriptor* buffer) {
  {
    unique_lock<mutex> scan_range_lock(lock_);
    DCHECK(Validate()) << DebugString();
    DCHECK(!eosr_returned_);
    DCHECK(!eosr_queued_);
    if (is_cancelled_) {
      // Return the buffer, this range has been cancelled
      if (buffer->buffer_ != NULL) {
        ++io_mgr_->num_buffers_in_readers_;
        ++reader_->num_buffers_in_reader_;
      }
      --reader_->num_used_buffers_;
      buffer->Return();
      return false;
    }
    ++reader_->num_ready_buffers_;
    ready_buffers_.push_back(buffer);
    eosr_queued_ = buffer->eosr();

    blocked_on_queue_ = ready_buffers_.size() >= ready_buffers_capacity_;
    if (blocked_on_queue_ && ready_buffers_capacity_ > MIN_QUEUE_CAPACITY) {
      // We have filled the queue, indicating we need back pressure on
      // the producer side (i.e. we are pushing buffers faster than they
      // are pulled off, throttle this range more).
      --ready_buffers_capacity_;
    }
  }

  buffer_ready_cv_.notify_one();

  return blocked_on_queue_;
}

Status DiskIoMgr::ScanRange::GetNext(BufferDescriptor** buffer) {
  *buffer = NULL;

  {
    unique_lock<mutex> scan_range_lock(lock_);
    if (eosr_returned_) return Status::OK();
    DCHECK(Validate()) << DebugString();

    if (ready_buffers_.empty()) {
      // The queue is empty indicating this thread could use more
      // IO. Increase the capacity to allow for more queueing.
      ++ready_buffers_capacity_ ;
      ready_buffers_capacity_ = ::min(ready_buffers_capacity_, MAX_QUEUE_CAPACITY);
    }

    while (ready_buffers_.empty() && !is_cancelled_) {
      buffer_ready_cv_.wait(scan_range_lock);
    }

    if (is_cancelled_) {
      DCHECK(!status_.ok());
      return status_;
    }

    // Remove the first ready buffer from the queue and return it
    DCHECK(!ready_buffers_.empty());
    *buffer = ready_buffers_.front();
    ready_buffers_.pop_front();
    eosr_returned_ = (*buffer)->eosr();
  }

  // Update tracking counters. The buffer has now moved from the IoMgr to the
  // caller.
  ++io_mgr_->num_buffers_in_readers_;
  ++reader_->num_buffers_in_reader_;
  --reader_->num_ready_buffers_;
  --reader_->num_used_buffers_;

  Status status = (*buffer)->status_;
  if (!status.ok()) {
    (*buffer)->Return();
    *buffer = NULL;
    return status;
  }

  unique_lock<mutex> reader_lock(reader_->lock_);
  if (eosr_returned_) {
    reader_->total_range_queue_capacity_ += ready_buffers_capacity_;
    ++reader_->num_finished_ranges_;
    reader_->initial_queue_capacity_ =
        reader_->total_range_queue_capacity_ / reader_->num_finished_ranges_;
  }

  DCHECK(reader_->Validate()) << endl << reader_->DebugString();
  if (reader_->state_ == RequestContext::Cancelled) {
    reader_->blocked_ranges_.Remove(this);
    Cancel(reader_->status_);
    (*buffer)->Return();
    *buffer = NULL;
    return status_;
  }

  bool was_blocked = blocked_on_queue_;
  blocked_on_queue_ = ready_buffers_.size() >= ready_buffers_capacity_;
  if (was_blocked && !blocked_on_queue_ && !eosr_queued_) {
    // This scan range was blocked and is no longer, add it to the reader
    // queue again.
    reader_->blocked_ranges_.Remove(this);
    reader_->ScheduleScanRange(this);
  }
  return Status::OK();
}

void DiskIoMgr::ScanRange::Cancel(const Status& status) {
  // Cancelling a range that was never started, ignore.
  if (io_mgr_ == NULL) return;

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
  buffer_ready_cv_.notify_all();
  CleanupQueuedBuffers();

  // For cached buffers, we can't close the range until the cached buffer is returned.
  // Close() is called from DiskIoMgr::ReturnBuffer().
  if (cached_buffer_ == NULL) Close();
}

void DiskIoMgr::ScanRange::CleanupQueuedBuffers() {
  DCHECK(is_cancelled_);
  io_mgr_->num_buffers_in_readers_ += ready_buffers_.size();
  reader_->num_buffers_in_reader_ += ready_buffers_.size();
  reader_->num_used_buffers_ -= ready_buffers_.size();
  reader_->num_ready_buffers_ -= ready_buffers_.size();

  while (!ready_buffers_.empty()) {
    BufferDescriptor* buffer = ready_buffers_.front();
    buffer->Return();
    ready_buffers_.pop_front();
  }
}

string DiskIoMgr::ScanRange::DebugString() const {
  stringstream ss;
  ss << "file=" << file_ << " disk_id=" << disk_id_ << " offset=" << offset_
     << " len=" << len_ << " bytes_read=" << bytes_read_
     << " buffer_queue=" << ready_buffers_.size()
     << " capacity=" << ready_buffers_capacity_
     << " hdfs_file=" << hdfs_file_;
  return ss.str();
}

bool DiskIoMgr::ScanRange::Validate() {
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

DiskIoMgr::ScanRange::ScanRange(int capacity)
  : ready_buffers_capacity_(capacity) {
  request_type_ = RequestType::READ;
  Reset(NULL, "", -1, -1, -1, false, false, NEVER_CACHE);
}

DiskIoMgr::ScanRange::~ScanRange() {
  DCHECK(hdfs_file_ == NULL) << "File was not closed.";
  DCHECK(cached_buffer_ == NULL) << "Cached buffer was not released.";
}

void DiskIoMgr::ScanRange::Reset(hdfsFS fs, const char* file, int64_t len, int64_t offset,
    int disk_id, bool try_cache, bool expected_local, int64_t mtime, void* meta_data) {
  DCHECK(ready_buffers_.empty());
  fs_ = fs;
  file_ = file;
  len_ = len;
  offset_ = offset;
  disk_id_ = disk_id;
  try_cache_ = try_cache;
  expected_local_ = expected_local;
  meta_data_ = meta_data;
  cached_buffer_ = NULL;
  io_mgr_ = NULL;
  reader_ = NULL;
  hdfs_file_ = NULL;
  mtime_ = mtime;
}

void DiskIoMgr::ScanRange::InitInternal(DiskIoMgr* io_mgr, RequestContext* reader) {
  DCHECK(hdfs_file_ == NULL);
  io_mgr_ = io_mgr;
  reader_ = reader;
  local_file_ = NULL;
  hdfs_file_ = NULL;
  bytes_read_ = 0;
  is_cancelled_ = false;
  eosr_queued_= false;
  eosr_returned_= false;
  blocked_on_queue_ = false;
  if (ready_buffers_capacity_ <= 0) {
    ready_buffers_capacity_ = reader->initial_scan_range_queue_capacity();
    DCHECK_GE(ready_buffers_capacity_, MIN_QUEUE_CAPACITY);
  }
  DCHECK(Validate()) << DebugString();
}

Status DiskIoMgr::ScanRange::Open() {
  unique_lock<mutex> hdfs_lock(hdfs_lock_);
  if (is_cancelled_) return Status::CANCELLED;

  if (fs_ != NULL) {
    if (hdfs_file_ != NULL) return Status::OK();
    hdfs_file_ = io_mgr_->OpenHdfsFile(fs_, file(), mtime());
    if (hdfs_file_ == NULL) {
      return Status(GetHdfsErrorMsg("Failed to open HDFS file ", file_));
    }

    if (hdfsSeek(fs_, hdfs_file_->file(), offset_) != 0) {
      io_mgr_->CacheOrCloseFileHandle(file(), hdfs_file_, false);
      hdfs_file_ = NULL;
      string error_msg = GetHdfsErrorMsg("");
      stringstream ss;
      ss << "Error seeking to " << offset_ << " in file: " << file_ << " " << error_msg;
      return Status(ss.str());
    }
  } else {
    if (local_file_ != NULL) return Status::OK();

    local_file_ = fopen(file(), "r");
    if (local_file_ == NULL) {
      string error_msg = GetStrErrMsg();
      stringstream ss;
      ss << "Could not open file: " << file_ << ": " << error_msg;
      return Status(ss.str());
    }
    if (fseek(local_file_, offset_, SEEK_SET) == -1) {
      fclose(local_file_);
      local_file_ = NULL;
      string error_msg = GetStrErrMsg();
      stringstream ss;
      ss << "Could not seek to " << offset_ << " for file: " << file_
         << ": " << error_msg;
      return Status(ss.str());
    }
  }
  if (ImpaladMetrics::IO_MGR_NUM_OPEN_FILES != NULL) {
    ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(1L);
  }
  return Status::OK();
}

void DiskIoMgr::ScanRange::Close() {
  unique_lock<mutex> hdfs_lock(hdfs_lock_);
  if (fs_ != NULL) {
    if (hdfs_file_ == NULL) return;

    struct hdfsReadStatistics* stats;
    if (IsDfsPath(file())) {
      int success = hdfsFileGetReadStatistics(hdfs_file_->file(), &stats);
      if (success == 0) {
        reader_->bytes_read_local_ += stats->totalLocalBytesRead;
        reader_->bytes_read_short_circuit_ += stats->totalShortCircuitBytesRead;
        reader_->bytes_read_dn_cache_ += stats->totalZeroCopyBytesRead;
        if (stats->totalLocalBytesRead != stats->totalBytesRead) {
          ++reader_->num_remote_ranges_;
          if (expected_local_) {
            int remote_bytes = stats->totalBytesRead - stats->totalLocalBytesRead;
            reader_->unexpected_remote_bytes_ += remote_bytes;
            VLOG_FILE << "Unexpected remote HDFS read of "
                      << PrettyPrinter::Print(remote_bytes, TUnit::BYTES)
                      << " for file '" << file_ << "'";
          }
        }
        hdfsFileFreeReadStatistics(stats);
      }
    }
    if (cached_buffer_ != NULL) {
      hadoopRzBufferFree(hdfs_file_->file(), cached_buffer_);
      cached_buffer_ = NULL;
    }
    io_mgr_->CacheOrCloseFileHandle(file(), hdfs_file_, false);
    VLOG_FILE << "Cache HDFS file handle file=" << file();
    hdfs_file_ = NULL;
  } else {
    if (local_file_ == NULL) return;
    fclose(local_file_);
    local_file_ = NULL;
  }
  if (ImpaladMetrics::IO_MGR_NUM_OPEN_FILES != NULL) {
    ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(-1L);
  }
}

int64_t DiskIoMgr::ScanRange::MaxReadChunkSize() const {
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
  return numeric_limits<int64_t>::max();
}

// TODO: how do we best use the disk here.  e.g. is it good to break up a
// 1MB read into 8 128K reads?
// TODO: look at linux disk scheduling
Status DiskIoMgr::ScanRange::Read(char* buffer, int64_t* bytes_read, bool* eosr) {
  unique_lock<mutex> hdfs_lock(hdfs_lock_);
  if (is_cancelled_) return Status::CANCELLED;

  *eosr = false;
  *bytes_read = 0;
  // hdfsRead() length argument is an int.  Since max_buffer_size_ type is no bigger
  // than an int, this min() will ensure that we don't overflow the length argument.
  DCHECK_LE(sizeof(io_mgr_->max_buffer_size_), sizeof(int));
  int bytes_to_read =
      min(static_cast<int64_t>(io_mgr_->max_buffer_size_), len_ - bytes_read_);
  DCHECK_GE(bytes_to_read, 0);

  if (fs_ != NULL) {
    DCHECK(hdfs_file_ != NULL);
    int64_t max_chunk_size = MaxReadChunkSize();
    while (*bytes_read < bytes_to_read) {
      int chunk_size = min(bytes_to_read - *bytes_read, max_chunk_size);
      int last_read = hdfsRead(fs_, hdfs_file_->file(), buffer + *bytes_read, chunk_size);
      if (last_read == -1) {
        return Status(GetHdfsErrorMsg("Error reading from HDFS file: ", file_));
      } else if (last_read == 0) {
        // No more bytes in the file. The scan range went past the end.
        *eosr = true;
        break;
      }
      *bytes_read += last_read;
    }
  } else {
    DCHECK(local_file_ != NULL);
    *bytes_read = fread(buffer, 1, bytes_to_read, local_file_);
    DCHECK_GE(*bytes_read, 0);
    DCHECK_LE(*bytes_read, bytes_to_read);
    if (*bytes_read < bytes_to_read) {
      if (ferror(local_file_) != 0) {
        string error_msg = GetStrErrMsg();
        stringstream ss;
        ss << "Error reading from " << file_ << " at byte offset: "
           << (offset_ + bytes_read_) << ": " << error_msg;
        return Status(ss.str());
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

Status DiskIoMgr::ScanRange::ReadFromCache(bool* read_succeeded) {
  DCHECK(try_cache_);
  DCHECK_EQ(bytes_read_, 0);
  *read_succeeded = false;
  Status status = Open();
  if (!status.ok()) return status;

  // Cached reads not supported on local filesystem.
  if (fs_ == NULL) return Status::OK();

  {
    unique_lock<mutex> hdfs_lock(hdfs_lock_);
    if (is_cancelled_) return Status::CANCELLED;

    DCHECK(hdfs_file_ != NULL);
    DCHECK(cached_buffer_ == NULL);
    cached_buffer_ = hadoopReadZero(hdfs_file_->file(),
        io_mgr_->cached_read_options_, len());

    // Data was not cached, caller will fall back to normal read path.
    if (cached_buffer_ == NULL) return Status::OK();
  }

  // Cached read succeeded.
  void* buffer = const_cast<void*>(hadoopRzBufferGet(cached_buffer_));
  int32_t bytes_read = hadoopRzBufferLength(cached_buffer_);
  // For now, entire the entire block is cached or none of it.
  // TODO: if HDFS ever changes this, we'll have to handle the case where half
  // the block is cached.
  DCHECK_EQ(bytes_read, len());

  // Create a single buffer desc for the entire scan range and enqueue that.
  BufferDescriptor* desc = io_mgr_->GetBufferDesc(
      reader_, this, reinterpret_cast<char*>(buffer), 0);
  desc->len_ = bytes_read;
  desc->scan_range_offset_ = 0;
  desc->eosr_ = true;
  bytes_read_ = bytes_read;
  EnqueueBuffer(desc);
  if (reader_->bytes_read_counter_ != NULL) {
    COUNTER_ADD(reader_->bytes_read_counter_, bytes_read);
  }
  *read_succeeded = true;
  ++reader_->num_used_buffers_;
  return Status::OK();
}
