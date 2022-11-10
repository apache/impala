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

class DataCache;

/// File reader class for HDFS.
class HdfsFileReader : public FileReader {
public:
  HdfsFileReader(ScanRange* scan_range, hdfsFS hdfs_fs, bool expected_local) :
      FileReader(scan_range), hdfs_fs_(hdfs_fs), expected_local_(expected_local) {
  }

  ~HdfsFileReader();

  virtual Status Open() override;
  virtual Status ReadFromPos(DiskQueue* queue, int64_t file_offset, uint8_t* buffer,
      int64_t bytes_to_read, int64_t* bytes_read, bool* eof) override;
  virtual void Close() override;
  virtual bool SupportsDelayedOpen() const override { return true; }
  virtual void ResetState() override;
  virtual std::string DebugString() const override;

  /// Reads from the DN cache. On success, sets cached_buffer_ to the DN buffer
  /// and returns a pointer to the underlying raw buffer. 'cached_buffer_' is set to
  /// nullptr if the data is not cached and 'length' is set to 0.
  ///
  /// Please note that this interface is only effective for local HDFS reads as it
  /// relies on HDFS caching. For remote reads, this interface is not used.
  virtual void CachedFile(uint8_t** data, int64_t* length) override;

private:

  /// Performs the actual work of opening a file handle. When using a file handle or data
  /// cache, opening a file handle is delayed until remote data needs to be read. Not
  /// thread safe, claim lock_ before calling.
  Status DoOpen();

  /// Probes 'remote_data_cache' for a hit. The requested file's name and mtime
  /// are stored in 'scan_range_'. 'file_offset' is the offset into the file to read
  /// and 'bytes_to_read' is the number of bytes requested. On success, copies the
  /// content of the cache into 'buffer' and returns the number of bytes read.
  /// Also updates various cache metrics. Returns 0 on cache miss.
  int64_t ReadDataCache(DataCache* remote_data_cache, int64_t file_offset,
      uint8_t* buffer, int64_t bytes_to_read);

  /// Inserts into 'remote_data_cache' with 'buffer' which contains the data read
  /// from a file at 'file_offset'. 'buffer_len' is the length of the buffer in bytes.
  /// The file's name and mtime are stored in 'scan_range_'. 'cached_bytes_missed' is
  /// the number of bytes missed in the cache. Used for updating cache metrics.
  /// No guarantee that the entry is inserted as caching is opportunistic.
  void WriteDataCache(DataCache* remote_data_cache, int64_t file_offset,
      const uint8_t* buffer, int64_t buffer_len, int64_t cached_bytes_missed);

  /// Read [position_in_file, position_in_file + bytes_to_read) from 'hdfs_file'
  /// into 'buffer'. Update 'bytes_read' on success. Returns error status on
  /// failure. When not using HDFS pread, this function will always implicitly
  /// seek to 'position_in_file' if 'hdfs_file' is not at it already.
  /// 'disk_queue' metrics are updated based on the operation.
  Status ReadFromPosInternal(hdfsFile hdfs_file, DiskQueue* disk_queue,
      int64_t position_in_file, uint8_t* buffer, int64_t bytes_to_read, int* bytes_read);

  /// Update counters with HDFS read statistics from 'hdfs_file'. If 'log_stats' is
  /// true, the statistics are logged.
  void GetHdfsStatistics(hdfsFile hdfs_file, bool log_stats);

  /// Return a string that contains the block indexes and list of hosts where
  /// each block resides. i.e. [0] { hdfshost1, hdfshost2, hdfshost3 }

  std::string GetHostList(int64_t file_offset, int64_t bytes_to_read) const;

  /// Hadoop filesystem that contains the file being read.
  hdfsFS const hdfs_fs_;

  /// The hdfs file handle is stored here in three cases:
  /// 1. The file handle cache is off (max_cached_file_handles == 0).
  /// 2. The scan range is using hdfs caching.
  /// -OR-
  /// 3. The hdfs file is expected to be remote (expected_local_ == false)
  /// In each case, the scan range gets a new ExclusiveHdfsFileHandle at Open(),
  /// owns it exclusively, and destroys it in Close().
  std::unique_ptr<ExclusiveHdfsFileHandle> exclusive_hdfs_fh_;

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
