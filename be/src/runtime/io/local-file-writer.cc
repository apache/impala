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

#include <stdio.h>
#include <algorithm>

#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/local-file-writer.h"
#include "runtime/io/request-ranges.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"

#include "common/names.h"

namespace impala {
namespace io {

LocalFileWriter::~LocalFileWriter() {
  // Ensure the file handle is released.
  DCHECK(file_ == nullptr);
}

Status LocalFileWriter::Open() {
  lock_guard<mutex> lock(lock_);
  if (file_ != nullptr) return Status::OK();
  return io_mgr_->local_file_system_->OpenForWrite(
      file_path_, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR, &file_);
}

Status LocalFileWriter::Write(WriteRange* range, int64_t* written_bytes) {
  lock_guard<mutex> lock(lock_);
  if (file_ == nullptr) {
    return Status(Substitute("File handle of $0 has been closed.", file_path_));
  }
  // Use write() instead of fwrite(), because the file handle is shared by multiple write
  // ranges before the file is full and closed, during the period, if using fwrite(), it
  // is not safe to read the file because the buffer fwrite() is using may not flush to
  // the disk. But write() is safe to read immediately after the write because it writes
  // without a buffer.
  RETURN_IF_ERROR(io_mgr_->local_file_system_->Write(fileno(file_), range));
  ImpaladMetrics::IO_MGR_BYTES_WRITTEN->Increment(range->len());
  range->SetOffset(written_bytes_);
  written_bytes_ += range->len();
  *written_bytes = written_bytes_;
  return Status::OK();
}

Status LocalFileWriter::Close() {
  lock_guard<mutex> lock(lock_);
  if (file_ == nullptr) return Status::OK();
  RETURN_IF_ERROR(io_mgr_->local_file_system_->Fclose(file_, file_path_));
  file_ = nullptr;
  return Status::OK();
}

Status LocalFileWriter::WriteOne(WriteRange* write_range) {
  DCHECK(write_range != nullptr);
  Status ret_status = Status::OK();
  // Do not need to acquire the lock_ because we open a new file handle in WriteOne
  // for writing, instead of sharing the same file handle.
  FILE* file_handle = nullptr;
  Status close_status = Status::OK();
  DiskQueue* queue = io_mgr_->disk_queues_[write_range->disk_id()];

  {
    ScopedHistogramTimer write_timer(queue->write_latency());
    ret_status = io_mgr_->local_file_system_->OpenForWrite(
        write_range->file(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR, &file_handle);
    if (!ret_status.ok()) goto end;

    ret_status = io_mgr_->WriteRangeHelper(file_handle, write_range);

    close_status = io_mgr_->local_file_system_->Fclose(file_handle, write_range->file());
    if (ret_status.ok() && !close_status.ok()) ret_status = close_status;
  }

end:
  if (ret_status.ok()) {
    queue->write_size()->Update(write_range->len());
  } else {
    queue->write_io_err()->Increment(1);
  }
  return ret_status;
}
} // namespace io
} // namespace impala
