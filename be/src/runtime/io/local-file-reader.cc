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

#include <algorithm>
#include <stdio.h>

#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/local-file-reader.h"
#include "runtime/io/request-ranges.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"

#include "common/names.h"

#ifndef NDEBUG
DECLARE_int32(stress_disk_read_delay_ms);
#endif

namespace impala {
namespace io {

Status LocalFileReader::Open() {
  unique_lock<SpinLock> fs_lock(lock_);
  RETURN_IF_ERROR(scan_range_->cancel_status_);

  if (file_ != nullptr)
    return Status::OK();

  file_ = fopen(scan_range_->file(), "r");
  if (file_ == nullptr) {
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        Substitute("Could not open file: $0: $1", *scan_range_->file_string(),
            GetStrErrMsg()));
  }
  ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(1L);
  return Status::OK();
}

Status LocalFileReader::ReadFromPos(DiskQueue* queue, int64_t file_offset,
    uint8_t* buffer, int64_t bytes_to_read, int64_t* bytes_read, bool* eof) {
  DCHECK(scan_range_->read_in_flight());
  DCHECK_GE(bytes_to_read, 0);
  // Delay before acquiring the lock, to allow triggering IMPALA-6587 race.
#ifndef NDEBUG
  if (FLAGS_stress_disk_read_delay_ms > 0) {
    SleepForMs(FLAGS_stress_disk_read_delay_ms);
  }
#endif
  unique_lock<SpinLock> fs_lock(lock_);
  RETURN_IF_ERROR(scan_range_->cancel_status_);

  *eof = false;
  *bytes_read = 0;

  DCHECK(file_ != nullptr);
  if (fseek(file_, file_offset, SEEK_SET) == -1) {
    fclose(file_);
    file_ = nullptr;
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        Substitute("Could not seek to $0 for file: $1: $2",
            scan_range_->offset(), *scan_range_->file_string(), GetStrErrMsg()));
  }
  {
    ScopedHistogramTimer read_timer(queue->read_latency());
    *bytes_read = fread(buffer, 1, bytes_to_read, file_);
  }
  DCHECK_GE(*bytes_read, 0);
  DCHECK_LE(*bytes_read, bytes_to_read);
  queue->read_size()->Update(*bytes_read);
  if (*bytes_read < bytes_to_read) {
    if (ferror(file_) != 0) {
      return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
          Substitute("Error reading from $0"
              "at byte offset: $1: $2", file_,
              file_offset, GetStrErrMsg()));
    }
    // On Linux, we should only get partial reads from block devices on error or eof.
    DCHECK(feof(file_) != 0);
    *eof = true;
  }
  return Status::OK();
}

void LocalFileReader::CachedFile(uint8_t** data, int64_t* length) {
  *data = nullptr;
  *length = 0;
}

void LocalFileReader::Close() {
  unique_lock<SpinLock> fs_lock(lock_);
  if (file_ == nullptr) return;
  fclose(file_);
  file_ = nullptr;
  ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(-1L);
  return;
}

}
}

