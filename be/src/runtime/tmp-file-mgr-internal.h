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

#ifndef IMPALA_RUNTIME_TMP_FILE_MGR_INTERNAL_H
#define IMPALA_RUNTIME_TMP_FILE_MGR_INTERNAL_H

#include <string>

#include "runtime/tmp-file-mgr.h"

namespace impala {

/// File is a handle to a physical file in a temporary directory. File space
/// can be allocated and files removed using AllocateSpace() and Remove(). Used
/// internally by TmpFileMgr.
///
/// Creation of the physical file in the file system is deferred until the file is
/// written by DiskIoMgr.
///
/// Methods of File are not thread-safe.
class TmpFileMgr::File {
 public:
  File(FileGroup* file_group, DeviceId device_id, const std::string& path);

  /// Allocates 'num_bytes' bytes in this file for a new block of data.
  /// The file size is increased by a call to truncate() if necessary.
  /// Sets 'offset' to the file offset of the first byte in the allocated
  /// range on success.
  void AllocateSpace(int64_t num_bytes, int64_t* offset);

  /// Called when an IO error is encountered for this file. Logs the error and blacklists
  /// the file.
  void Blacklist(const ErrorMsg& msg);

  /// Delete the physical file on disk, if one was created.
  /// It is not valid to read or write to a file after calling Remove().
  Status Remove();

  /// Get the disk ID that should be used for IO mgr queueing.
  int AssignDiskQueue() const;

  const std::string& path() const { return path_; }
  bool is_blacklisted() const { return blacklisted_; }

  std::string DebugString();

 private:
  friend class TmpFileMgrTest;
  /// The name of the sub-directory that Impala creates within each configured scratch
  /// directory.
  const static std::string TMP_SUB_DIR_NAME;

  /// Space (in MB) that must ideally be available for writing on a scratch
  /// directory. A warning is issued if available space is less than this threshold.
  const static uint64_t AVAILABLE_SPACE_THRESHOLD_MB;

  /// The FileGroup this belongs to. Cannot be null.
  FileGroup* const file_group_;

  /// Path of the physical file in the filesystem.
  const std::string path_;

  /// The temporary device this file is stored on.
  const DeviceId device_id_;

  /// The id of the disk on which the physical file lies.
  const int disk_id_;

  /// Current bytes allocated in the file. Modified by AllocateSpace().
  int64_t bytes_allocated_;

  /// Set to true to indicate that we shouldn't allocate any more space in this file.
  bool blacklisted_;
};
}

#endif
