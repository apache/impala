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

#ifndef IMPALA_RUNTIME_IO_HDFS_MONITORED_OPS_H
#define IMPALA_RUNTIME_IO_HDFS_MONITORED_OPS_H

#include "runtime/io/handle-cache.h"
#include "util/thread-pool.h"

namespace impala {

namespace io {

/// The HdfsMonitor implements timeouts on HDFS operations that otherwise would block
/// indefinitely. It submits the operations to a thread pool and waits with a timeout
/// for a response.
class HdfsMonitor {
 public:
  HdfsMonitor() {}

  // Initialize the thread pool with 'num_threads'
  Status Init(int32_t num_threads) WARN_UNUSED_RESULT;

  // Open the specified HDFS file. If the operation times out, returns an error status
  // and leaves 'hdfs_file_out' untouched. If the operation does not time out, then
  // the operation can still fail if the file does not exist. In this case,
  // 'hdfs_file_out' is null and an error status is returned. If the operation succeeds,
  // then 'hdfs_file_out' contains the file handle.
  //
  // This is a thin wrapper around hdfsOpenFile() that implements a timeout.
  // The differences from hdfsOpenFile() are:
  //  - The filename is taken as a const string* rather than a const char*
  //  - The bufferSize and replication are omitted, since no Impala code sets
  //    these to anything other than 0.
  //  - It has be rearranged to return Status.
  Status OpenHdfsFileWithTimeout(const hdfsFS& fs, const std::string* fname, int flags,
      uint64_t blocksize, hdfsFile* hdfs_file_out) WARN_UNUSED_RESULT;

 private:
  // Pool of threads handling HDFS operations.
  std::unique_ptr<SynchronousThreadPool> hdfs_worker_pool_;
};

}

}

#endif
