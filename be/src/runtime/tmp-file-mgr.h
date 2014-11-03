// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef IMPALA_RUNTIME_TMP_FILE_MGR_H
#define IMPALA_RUNTIME_TMP_FILE_MGR_H

#include <common/status.h>
#include "gen-cpp/Types_types.h"  // for TUniqueId

namespace impala {

// TmpFileMgr is a utility class that creates temporary directories on the local
// filesystem.
//
// TmpFileMgr::GetFile() is used to return a TmpFileMgr::File handle with a unique
// filename on a specified temp file device - the client owns the handle.
class TmpFileMgr {
 public:
  // TmpFileMgr::File is a handle to a physical file in a temporary directory. Clients
  // can allocate file space and remove files using AllocateSpace() and Remove().
  // Creation of the file is deferred until the first call to AllocateSpace().
  class File {
   public:
    // Allocates 'write_size' bytes in this file for a new block of data.
    // The file size is increased by a call to truncate() if necessary.
    // The physical file is created on the first call to AllocateSpace().
    Status AllocateSpace(int64_t write_size, int64_t* offset);

    // Delete the physical file on disk, if one was created.
    // It is not valid to read or write to a file after calling Remove().
    Status Remove();

    const std::string& path() const { return path_; }
    int disk_id() const { return disk_id_; }

   private:
    friend class TmpFileMgr;

    // The name of the sub-directory that Impala created within each configured scratch
    // directory.
    const static std::string TMP_SUB_DIR_NAME;

    // Space (in MB) that must ideally be available for writing on a scratch
    // directory. A warning is issued if available space is less than this threshold.
    const static uint64_t AVAILABLE_SPACE_THRESHOLD_MB;

    File(const std::string& path);

    // Path of the physical file in the filesystem.
    std::string path_;

    // The id of the disk on which the physical file lies.
    int disk_id_;

    // Offset to which the next block will be written.
    int64_t current_offset_;

    // Current file size. Modified by AllocateSpace(). Is always >= current offset.
    // Size is 0 before the file is created.
    int64_t current_size_;
  };

  // Creates the tmp directories configured by CM. If multiple directories are specified
  // per disk, only one is created and used. Must be called after DiskInfo::Init().
  static Status Init();

  // Return a new File handle with a unique path for a query instance. The file path
  // is within the (single) tmp directory on the specified device id. The caller owns
  // the returned handle and is responsible for deleting it. The file is not created -
  // creation is deferred until the first call to File::AllocateSpace().
  static Status GetFile(int tmp_device_id, const TUniqueId& query_id,
      File** new_file);

  // Total number of devices with tmp directories. This is the same as the number
  // of tmp directories created.
  static int num_tmp_devices() { return tmp_dirs_.size(); }

 private:
  static bool initialized_;

  // The created tmp directories, atmost one per device.
  static std::vector<std::string> tmp_dirs_;
};

}

#endif
