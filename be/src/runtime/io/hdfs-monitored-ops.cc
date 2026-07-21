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

#include <cerrno>
#include <cstring>

#include "gutil/strings/substitute.h"

#include "common/names.h"
#include "common/status.h"
#include "runtime/io/hdfs-monitored-ops.h"
#include "util/debug-util.h"
#include "util/hdfs-util.h"
#include "util/time.h"

namespace impala {

namespace {
// hdfsOpenFileFutureGet() reports failure by returning a null handle without
// translating the underlying Java exception to errno, unlike the hdfsOpenFile()
// call it replaced (IMPALA-8523). Recognize a file-not-found root cause so the
// open path can restore the classic ENOENT ("No such file or directory") surface
// that scanners and tests depend on.
bool IsFileNotFoundRootCause(const char* root_cause) {
  if (root_cause == nullptr) return false;
  return strstr(root_cause, "FileNotFoundException") != nullptr
      || strstr(root_cause, "File does not exist") != nullptr;
}
}

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
  OpenHdfsFileOp(const hdfsFS& fs, const std::string* fname, OpenFileOptions open_options)
    : fs_(fs), fname_(*fname), open_options_(std::move(open_options)) {}

  // Open the file via the builder-based openFile API and return an error status on
  // failure. On success, it is valid to call GetFileHandle().
  virtual Status Execute() override;

  virtual std::string GetDescription() override;

  // If the handle is successfully opened, GetHandle() will return the handle.
  hdfsFile GetFileHandle() { return hdfs_file_; }

 private:
  const hdfsFS& fs_;
  const std::string fname_;
  // Optional openFile builder parameters, passed via hdfsOpenFileBuilderOpt().
  const OpenFileOptions open_options_;
  hdfsFile hdfs_file_;
};

Status OpenHdfsFileOp::Execute() {
  // GetHdfsErrorMsg references thread local state to get error information, so any
  // error message must be built in the same thread as the failing libhdfs call.
  hdfsOpenFileBuilder* builder = hdfsOpenFileBuilderAlloc(fs_, fname_.data());
  if (builder == nullptr) {
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        GetHdfsErrorMsg("Failed to allocate open file builder for HDFS file ", fname_));
  }
  for (const auto& opt : open_options_) {
    // Opt() sets an optional parameter, keys a filesystem doesn't understand are ignored,
    // so this is safe for all filesystems. On failure it returns nullptr without freeing
    // the builder, and Build() (which normally frees the builder) is never reached,
    // so free it explicitly before bailing out.
    hdfsOpenFileBuilder* opt_builder =
        hdfsOpenFileBuilderOpt(builder, opt.first.c_str(), opt.second.c_str());
    if (opt_builder == nullptr) {
      hdfsOpenFileBuilderFree(builder);
      return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
          Substitute("Failed to set open file builder option '$0' for HDFS file $1",
              opt.first, fname_));
    }
    builder = opt_builder;
  }
  // hdfsOpenFileBuilderBuild() consumes (frees) the builder.
  hdfsOpenFileFuture* future = hdfsOpenFileBuilderBuild(builder);
  if (future == nullptr) {
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        GetHdfsErrorMsg("Failed to build open file future for HDFS file ", fname_));
  }
  // Block on the future here inside the thread-pool worker; the pool provides the
  // outer timeout (see OpenHdfsFileWithTimeout).
  hdfs_file_ = hdfsOpenFileFutureGet(future);
  hdfsOpenFileFutureFree(future);
  if (hdfs_file_ == nullptr) {
    // The builder future does not map the Java exception to errno the way
    // hdfsOpenFile() did, so a missing file would otherwise report "Unknown error"
    // instead of ENOENT. Restore the classic surface (IMPALA-8523) by mapping a
    // file-not-found root cause back to ENOENT before building the message, so it
    // reads "No such file or directory". The message is captured into a local
    // first because argument evaluation order is unspecified and errno must still
    // be ENOENT when GetHdfsErrorMsg() reads it.
    if (IsFileNotFoundRootCause(hdfsGetLastExceptionRootCause())) errno = ENOENT;
    const string msg = GetHdfsErrorMsg("Failed to open HDFS file ", fname_);
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(), msg);
  }
  return Status::OK();
}

std::string OpenHdfsFileOp::GetDescription() {
  return Substitute("openFile builder for $0 at backend $1", fname_, GetBackendString());
}

Status HdfsMonitor::OpenHdfsFileWithTimeout(const hdfsFS& fs, const std::string* fname,
  hdfsFile* fid_out, const OpenFileOptions& open_options) {
  std::shared_ptr<OpenHdfsFileOp> op(new OpenHdfsFileOp(fs, fname, open_options));

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
