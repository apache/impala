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

#include <string>

#include "common/status.h"

namespace impala {
class TmpFile;
namespace io {
class DiskIoMgr;
class WriteRange;

/// Abstract class that provides interface for file writing operations.
/// Child classes implement these operations for the local file system
/// and for HDFS.
/// A typical process is calling Open(), then Write() with certain
/// WriteRange objects, after the writing finished, call the Close() to
/// close the file handle. The writes are the sequential operations to
/// the same file.
class FileWriter {
 public:
  FileWriter(DiskIoMgr* io_mgr, const char* file_path, const int64_t file_size)
    : io_mgr_(io_mgr), file_path_(file_path), file_size_(file_size) {}
  virtual ~FileWriter() {}

  /// The set of Open/Write/Close function is used for sequential range writing
  /// for a specific file. The file handle is opened and closed only once per
  /// file by calling Open() and Close(). By calling the Write(), the write
  /// ranges are written into the file sequentially in different threads using
  /// the same file handle.
  /// The caller needs to call the Close() to close the file handle when is_full
  /// is returned True in Write().
  virtual Status Open() = 0;
  virtual Status Write(WriteRange* range, int64_t* written_bytes) = 0;
  virtual Status Close() = 0;

  /// The WriteOne function is used for a random range writing. The caller would
  /// expect the function to open a file handle of the file, write the range, and
  /// close the file handle.
  virtual Status WriteOne(WriteRange*) = 0;

 protected:
  /// DiskIoMgr used for the I/O operations and the statistics.
  DiskIoMgr* io_mgr_ = nullptr;

  /// The lock_ is to guarantee only one thread is using the file handle.
  /// Ideally, Open() and Close() would be called once for each file writer, so
  /// the lock_ is a exclusive lock mainly for running the Write().
  /// WriteOne() doesn't need the lock_ because it opens its own file handle,
  /// and doesn't share it with other WriteRanges.
  std::mutex lock_;

  /// The bytes have been written by the file writer.
  int64_t written_bytes_ = 0;

  /// The file path to be written.
  const char* file_path_;

  /// The size of the file.
  const int64_t file_size_;
};
} // namespace io
} // namespace impala
