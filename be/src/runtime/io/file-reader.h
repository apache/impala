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

#include "util/spinlock.h"

#include "common/status.h"

namespace impala {
namespace io {

class DiskIoMgr;
class RequestContext;
class ScanRange;

/// Abstract class that provides interface for file operations
/// Child classes implement these operations for the local file system
/// and for HDFS.
/// A FileReader object is owned by a single ScanRange object, and
/// a ScanRange object only has a single FileReader object.
class FileReader {
public:
  FileReader(ScanRange* scan_range) : scan_range_(scan_range) {}
  virtual ~FileReader() {}

  /// Returns number of bytes read by this file reader.
  int bytes_read() const { return bytes_read_; }

  /// Opens file that is associated with 'scan_range_'.
  /// 'use_file_handle_cache' currently only used by HdfsFileReader.
  virtual Status Open(bool use_file_handle_cache) = 0;

  /// Reads bytes from given position ('file_offset'). Tries to read
  /// 'bytes_to_read' amount of bytes. 'bytes_read' contains the number of
  /// bytes actually read. 'eosr' is set to true when end of file has reached,
  /// or the file reader has read all the bytes needed by 'scan_range_'.
  virtual Status ReadFromPos(int64_t file_offset, uint8_t* buffer,
      int64_t bytes_to_read, int64_t* bytes_read, bool* eosr) = 0;

  /// ***Currently only for HDFS***
  /// Returns a pointer to a cached buffer that contains the contents of the file.
  virtual void* CachedFile() = 0;

  /// Closes the file associated with 'scan_range_'. It doesn't have effect on other
  /// scan ranges.
  virtual void Close() = 0;

  /// Reset internal bookkeeping, e.g. how many bytes have been read.
  virtual void ResetState() { bytes_read_ = 0; }

  // Debug string of this file reader.
  virtual std::string DebugString() const;

  SpinLock& lock() { return lock_; }
protected:
  /// Lock that should be taken during fs calls. Only one thread (the disk reading
  /// thread) calls into fs at a time so this lock does not have performance impact.
  /// This lock only serves to coordinate cleanup. Specifically it serves to ensure
  /// that the disk threads are finished with FS calls before scan_range_->is_cancelled_
  /// is set to true and cleanup starts.
  /// If this lock and scan_range_->lock_ need to be taken, scan_range_->lock_ must be
  /// taken first.
  SpinLock lock_;

  /// The scan range this file reader serves.
  ScanRange* const scan_range_;

  /// Number of bytes read by this reader.
  int bytes_read_ = 0;
};

}
}
