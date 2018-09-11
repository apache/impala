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

#pragma once

#include "common/hdfs.h"
#include "runtime/io/file-reader.h"

namespace impala {
namespace io {

/// File reader class for HDFS.
class HdfsFileReader : public FileReader {
public:
  HdfsFileReader(ScanRange* scan_range, hdfsFS hdfs_fs, bool expected_local) :
      FileReader(scan_range), hdfs_fs_(hdfs_fs), expected_local_(expected_local) {
  }

  ~HdfsFileReader();

  virtual Status Open(bool use_file_handle_cache) override;
  virtual Status ReadFromPos(int64_t file_offset, uint8_t* buffer,
      int64_t bytes_to_read, int64_t* bytes_read, bool* eosr) override;
  /// Reads from the DN cache. On success, sets cached_buffer_ to the DN
  /// buffer and returns a pointer to the underlying raw buffer.
  /// Returns nullptr if the data is not cached.
  virtual void* CachedFile() override;
  virtual void Close() override;
  virtual void ResetState() override;
  virtual std::string DebugString() const override;
private:
  Status ReadFromPosInternal(hdfsFile hdfs_file, int64_t position_in_file,
      bool is_borrowed_fh, uint8_t* buffer, int64_t chunk_size, int* bytes_read);
  void GetHdfsStatistics(hdfsFile hdfs_file);

  /// Hadoop filesystem that contains the file being read.
  hdfsFS const hdfs_fs_;

  /// The hdfs file handle is stored here in three cases:
  /// 1. The file handle cache is off (max_cached_file_handles == 0).
  /// 2. The scan range is using hdfs caching.
  /// -OR-
  /// 3. The hdfs file is expected to be remote (expected_local_ == false)
  /// In each case, the scan range gets a new ExclusiveHdfsFileHandle at Open(),
  /// owns it exclusively, and destroys it in Close().
  ExclusiveHdfsFileHandle* exclusive_hdfs_fh_ = nullptr;

  /// If true, we expect the reads to be a local read. Note that if this is false,
  /// it does not necessarily mean we expect the read to be remote, and that we never
  /// create scan ranges where some of the range is expected to be remote and some of it
  /// local.
  /// TODO: we can do more with this
  const bool expected_local_;

  /// Total number of bytes read remotely. This is necessary to maintain a count of
  /// the number of remote scan ranges. Since IO statistics can be collected multiple
  /// times for a scan range, it is necessary to keep some state about whether this
  /// scan range has already been counted as remote. There is also a requirement to
  /// log the number of unexpected remote bytes for a scan range. To solve both
  /// requirements, maintain num_remote_bytes_ on the ScanRange and push it to the
  /// reader_ once at the close of the scan range.
  int64_t num_remote_bytes_ = 0;

  /// Non-NULL if a cached read succeeded. Then all the bytes for the file are in
  /// this buffer.
  hadoopRzBuffer* cached_buffer_ = nullptr;
};

}
}
