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
    if (eosr_returned_) return Status::OK;
    DCHECK(Validate()) << DebugString();

    if (ready_buffers_.empty()) {
      // The queue is empty indicating this thread could use more
      // IO. Double the capacity to allow for more queueing.
      ready_buffers_capacity_ *= 2;
      ready_buffers_capacity_ = ::min(ready_buffers_capacity_, MAX_QUEUE_CAPACITY);
    }

    while (ready_buffers_.empty() && !is_cancelled_) {
      buffer_ready_cv_.wait(scan_range_lock);
    }
  
    if (is_cancelled_) {
      DCHECK(ready_buffers_.empty());
      return reader_->status_;
    }

    // Remove the first ready buffer from the queue and return it
    DCHECK(!ready_buffers_.empty());
    *buffer = ready_buffers_.front();
    ready_buffers_.pop_front();
    eosr_returned_ = (*buffer)->eosr();
  }
  
  // Update tracking counters.  The buffer has now moved from the IoMgr to the
  // caller.
  if (eosr_returned_) {
    reader_->total_range_queue_capacity_ += ready_buffers_capacity_;
    ++reader_->num_finished_ranges_;
  }
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
  DCHECK(reader_->Validate()) << endl << reader_->DebugString();
  if (reader_->state_ == ReaderContext::Cancelled) {
    reader_->blocked_ranges_.Remove(this);
    Cancel();
    (*buffer)->Return();
    *buffer = NULL;
    return reader_->status_;
  }

  bool was_blocked = blocked_on_queue_;
  blocked_on_queue_ = ready_buffers_.size() >= ready_buffers_capacity_;
  if (was_blocked && !blocked_on_queue_ && !eosr_queued_) {
    // This scan range was blocked and is no longer, add it to the reader
    // queue again.
    reader_->blocked_ranges_.Remove(this);
    reader_->ScheduleScanRange(this);
  }
  return Status::OK;
}

void DiskIoMgr::ScanRange::Cancel() {
  {
    unique_lock<mutex> scan_range_lock(lock_);
    DCHECK(Validate()) << DebugString();
    if (is_cancelled_) {
      DCHECK(ready_buffers_.empty());
      return;
    }
    is_cancelled_ = true;
    CleanupQueuedBuffers();
  }
  buffer_ready_cv_.notify_all();
  CloseScanRange(reader_->hdfs_connection_, reader_);
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
     << " capacity=" << ready_buffers_capacity_;
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
  if (is_cancelled_ && !ready_buffers_.empty()) {
    LOG(WARNING) << "Cancelling the reader must clean up queued buffers."
                 << " is_cancelled_=" << is_cancelled_ 
                 << " ready_buffers_.size()=" << ready_buffers_.size();
    return false;
  }
  return true;
}

DiskIoMgr::ScanRange::ScanRange(int capacity) 
  : ready_buffers_capacity_(capacity) {
  Reset(NULL, -1, -1, -1);
}

void DiskIoMgr::ScanRange::Reset(const char* file, int64_t len, int64_t offset,
    int disk_id, void* meta_data) {
  DCHECK(ready_buffers_.empty());
  file_ = file;
  len_ = len;
  offset_ = offset;
  disk_id_ = disk_id;
  meta_data_ = meta_data;
}

void DiskIoMgr::ScanRange::InitInternal(DiskIoMgr* io_mgr, ReaderContext* reader) {
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

Status DiskIoMgr::ScanRange::OpenScanRange(hdfsFS hdfs_connection) {
  unique_lock<mutex> scan_range_lock(lock_);
  if (is_cancelled_) return Status::CANCELLED;

  if (hdfs_connection != NULL) {
    if (hdfs_file_ != NULL) return Status::OK;

    // TODO: is there much overhead opening hdfs files?  Should we try to preserve
    // the handle across multiple scan ranges of a file?
    hdfs_file_ = hdfsOpenFile(hdfs_connection, file_, O_RDONLY, 0, 0, 0);
    if (hdfs_file_ == NULL) {
      return Status(AppendHdfsErrorMessage("Failed to open HDFS file ", file_));
    }

    if (hdfsSeek(hdfs_connection, hdfs_file_, offset_) != 0) {
      stringstream ss;
      ss << "Error seeking to " << offset_ << " in file: " << file_;
      return Status(AppendHdfsErrorMessage(ss.str()));
    }
  } else {
    if (local_file_ != NULL) return Status::OK;

    local_file_ = fopen(file_, "r");
    if (local_file_ == NULL) {
      stringstream ss;
      ss << "Could not open file: " << file_ << ": " << strerror(errno);
      return Status(ss.str());
    }
    if (fseek(local_file_, offset_, SEEK_SET) == -1) {
      stringstream ss;
      ss << "Could not seek to " << offset_ << " for file: " << file_
         << ": " << strerror(errno);
      return Status(ss.str());
    }
  }
  if (ImpaladMetrics::IO_MGR_NUM_OPEN_FILES != NULL) {
    ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(1L);
  }
  return Status::OK;
}

void DiskIoMgr::ScanRange::CloseScanRange(hdfsFS hdfs_connection, ReaderContext* reader) {
  unique_lock<mutex> scan_range_lock(lock_);
  if (hdfs_connection != NULL) {
    if (hdfs_file_ == NULL) return;

    struct hdfsReadStatistics* read_statistics;
    int success = hdfsFileGetReadStatistics(hdfs_file_, &read_statistics);
    if (success == 0) {
      reader->bytes_read_local_ += read_statistics->totalLocalBytesRead;
      reader->bytes_read_short_circuit_ += read_statistics->totalShortCircuitBytesRead;
      hdfsFileFreeReadStatistics(read_statistics);
    }
    
    hdfsCloseFile(hdfs_connection, hdfs_file_);
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

// TODO: how do we best use the disk here.  e.g. is it good to break up a
// 1MB read into 8 128K reads?
// TODO: look at linux disk scheduling
Status DiskIoMgr::ScanRange::ReadFromScanRange(hdfsFS hdfs_connection,
    char* buffer, int64_t* bytes_read, bool* eosr) {
  unique_lock<mutex> scan_range_lock(lock_);
  if (is_cancelled_) return Status::CANCELLED;

  *eosr = false;
  *bytes_read = 0;
  int bytes_to_read = 
      min(static_cast<int64_t>(io_mgr_->max_read_size_), len_ - bytes_read_);

  if (hdfs_connection != NULL) {
    DCHECK(hdfs_file_ != NULL);
    // TODO: why is this loop necessary? Can hdfs reads come up short?
    while (*bytes_read < bytes_to_read) {
      int last_read = hdfsRead(hdfs_connection, hdfs_file_,
          buffer + *bytes_read, bytes_to_read - *bytes_read);
      if (last_read == -1) {
        return Status(
            AppendHdfsErrorMessage("Error reading from HDFS file: ", file_));
      } else if (last_read == 0) {
        // No more bytes in the file.  The scan range went past the end
        *eosr = true;
        break;
      }
      *bytes_read += last_read;
    }
  } else {
    DCHECK(local_file_ != NULL);
    *bytes_read = fread(buffer, 1, bytes_to_read, local_file_);
    if (*bytes_read < 0) {
      stringstream ss;
      ss << "Could not read from " << file_ << " at byte offset: "
         << bytes_read_ << ": " << strerror(errno);
      return Status(ss.str());
    }
  }
  bytes_read_ += *bytes_read;
  DCHECK_LE(bytes_read_, len_);
  if (bytes_read_ == len_) *eosr = true;
  return Status::OK;
}




