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
class DiskQueue;
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

  /// Opens file that is associated with 'scan_range_'.
  virtual Status Open() = 0;

  /// Reads bytes from given position ('file_offset'). Tries to read
  /// 'bytes_to_read' amount of bytes. 'bytes_read' contains the number of
  /// bytes actually read. 'eof' is set to true when end of file has reached.
  /// Metrics in 'queue' are updated with the size and latencies of the read
  /// operations on the underlying file system.
  virtual Status ReadFromPos(DiskQueue* queue, int64_t file_offset, uint8_t* buffer,
      int64_t bytes_to_read, int64_t* bytes_read, bool* eof) = 0;

  /// ***Currently only for HDFS***
  /// When successful, sets 'data' to a buffer that contains the contents of a file,
  /// and 'length' is set to the length of the data. Does not support delayed open.
  /// When unsuccessful, 'data' is set to nullptr.
  virtual void CachedFile(uint8_t** data, int64_t* length) = 0;

  /// Closes the file associated with 'scan_range_'. It doesn't have effect on other
  /// scan ranges.
  virtual void Close() = 0;

  /// Override to return true if the implementation supports ReadFromPos without first
  /// calling Open. This can be useful when caching data or file handles.
  virtual bool SupportsDelayedOpen() const { return false; }

  /// Resets internal bookkeeping
  virtual void ResetState() {}

  // Debug string of this file reader.
  virtual std::string DebugString() const { return ""; }

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
};

}
}
