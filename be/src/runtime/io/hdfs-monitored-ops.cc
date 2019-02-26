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

#include "gutil/strings/substitute.h"

#include "common/names.h"
#include "common/status.h"
#include "runtime/io/hdfs-monitored-ops.h"
#include "util/debug-util.h"
#include "util/hdfs-util.h"
#include "util/time.h"

namespace impala {

namespace io {

// Timeout for HDFS operations. This defaults to 5 minutes
DEFINE_uint64(hdfs_operation_timeout_sec, 300, "Maximum time, in seconds, that an "
    "HDFS operation should wait before timing out and failing.");

Status HdfsMonitor::Init(int32_t num_threads) {
  // The thread pool sets its queue size to be equal to the number of threads.
  // TODO: is there a better queue size?
  hdfs_worker_pool_.reset(new SynchronousThreadPool("hdfs monitor",
      "hdfs_monitor_", num_threads, num_threads));
  return hdfs_worker_pool_->Init();
}

class OpenHdfsFileOp : public SynchronousWorkItem {
 public:
  // To guarantee the appropriate lifetime, the 'fname' argument is copied. The 'fs'
  // is safe, because it is kept open until the process terminates (see hdfs-fs-cache.h).
  OpenHdfsFileOp(const hdfsFS& fs, const std::string* fname, int flags,
      uint64_t blocksize)
    : fs_(fs), fname_(*fname), flags_(flags), blocksize_(blocksize) {}

  // Run hdfsOpenFile() and return error status if hdfsOpenFile() fails. If hdfsOpenFile()
  // succeeds, it is valid to call GetFileHandle().
  virtual Status Execute() override;

  virtual std::string GetDescription() override;

  // If the handle is successfully opened, GetHandle() will return the handle.
  hdfsFile GetFileHandle() { return hdfs_file_; }

 private:
  const hdfsFS& fs_;
  const std::string fname_;
  int flags_;
  uint64_t blocksize_;
  hdfsFile hdfs_file_;
};

Status OpenHdfsFileOp::Execute() {
  hdfs_file_ = hdfsOpenFile(fs_, fname_.data(), flags_, 0, 0, blocksize_);
  if (hdfs_file_ == nullptr) {
    // GetHdfsErrorMsg references thread local state to get error information, so it
    // must happen in the same thread as the hdfsOpenFile().
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        GetHdfsErrorMsg("Failed to open HDFS file ", fname_));
  }
  return Status::OK();
}

std::string OpenHdfsFileOp::GetDescription() {
  return Substitute("hdfsOpenFile() for $0 at backend $1", fname_, GetBackendString());
}

Status HdfsMonitor::OpenHdfsFileWithTimeout(const hdfsFS& fs, const std::string* fname,
  int flags, uint64_t blocksize, hdfsFile* fid_out) {
  std::shared_ptr<OpenHdfsFileOp> op(new OpenHdfsFileOp(fs, fname, flags, blocksize));

  // Submit the operation to the synchronous thread pool. If this times out, it returns
  // an error status.
  Status status = hdfs_worker_pool_->SynchronousOffer(op,
      FLAGS_hdfs_operation_timeout_sec * MILLIS_PER_SEC);
  RETURN_IF_ERROR(status);

  // The operation didn't time out, and there wasn't an error. Get the handle.
  *fid_out = op->GetFileHandle();
  return Status::OK();
}

}

}
