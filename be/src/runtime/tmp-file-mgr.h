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

#include "common/status.h"
#include "gen-cpp/Types_types.h"  // for TUniqueId
#include "util/collection-metrics.h"
#include "util/spinlock.h"

namespace impala {

/// TmpFileMgr creates and manages temporary files and directories on the local
/// filesystem. It can manage multiple temporary directories across multiple devices.
/// TmpFileMgr ensures that at most one directory per device is used unless overridden
/// for testing. GetFile() returns a File handle with a unique filename on a device. The
/// client owns the File handle and can use it to expand the file.
/// TODO: we could notify block managers about the failure so they can more take
/// proactive action to avoid using the device.
class TmpFileMgr {
 public:
  /// DeviceId is a unique identifier for a temporary device managed by TmpFileMgr.
  /// It is used as a handle for external classes to identify devices.
  typedef int DeviceId;

  /// File is a handle to a physical file in a temporary directory. Clients
  /// can allocate file space and remove files using AllocateSpace() and Remove().
  /// Creation of the file is deferred until the first call to AllocateSpace().
  class File {
   public:
    /// Allocates 'write_size' bytes in this file for a new block of data.
    /// The file size is increased by a call to truncate() if necessary.
    /// The physical file is created on the first call to AllocateSpace().
    /// Returns Status::OK() and sets offset on success.
    /// Returns an error status if an unexpected error occurs.
    /// If an error status is returned, the caller can try a different temporary file.
    Status AllocateSpace(int64_t write_size, int64_t* offset);

    /// Called to notify TmpFileMgr that an IO error was encountered for this file
    void ReportIOError(const ErrorMsg& msg);

    /// Delete the physical file on disk, if one was created.
    /// It is not valid to read or write to a file after calling Remove().
    Status Remove();

    const std::string& path() const { return path_; }
    int disk_id() const { return disk_id_; }
    bool is_blacklisted() const { return blacklisted_; }

   private:
    friend class TmpFileMgr;

    /// The name of the sub-directory that Impala created within each configured scratch
    /// directory.
    const static std::string TMP_SUB_DIR_NAME;

    /// Space (in MB) that must ideally be available for writing on a scratch
    /// directory. A warning is issued if available space is less than this threshold.
    const static uint64_t AVAILABLE_SPACE_THRESHOLD_MB;

    File(TmpFileMgr* mgr, DeviceId device_id, const std::string& path);

    /// TmpFileMgr this belongs to.
    TmpFileMgr* mgr_;

    /// Path of the physical file in the filesystem.
    std::string path_;

    /// The temporary device this file is stored on.
    DeviceId device_id_;

    /// The id of the disk on which the physical file lies.
    int disk_id_;

    /// Current file size. Modified by AllocateSpace(). Size is 0 before file creation.
    int64_t current_size_;

    /// Set to true to indicate that file can't be expanded. This is useful to keep here
    /// even though it is redundant with the global per-device blacklisting in TmpFileMgr
    /// because it can be checked without acquiring a global lock. If a file is
    /// blacklisted, the corresponding device will always be blacklisted.
    bool blacklisted_;
  };

  TmpFileMgr();

  /// Creates the configured tmp directories. If multiple directories are specified per
  /// disk, only one is created and used. Must be called after DiskInfo::Init().
  Status Init(MetricGroup* metrics);

  /// Custom initialization - initializes with the provided list of directories.
  /// If one_dir_per_device is true, only use one temporary directory per device.
  /// This interface is intended for testing purposes.
  Status InitCustom(const std::vector<std::string>& tmp_dirs, bool one_dir_per_device,
      MetricGroup* metrics);

  /// Return a new File handle with a unique path for a query instance. The file path
  /// is within the (single) tmp directory on the specified device id. The caller owns
  /// the returned handle and is responsible for deleting it. The file is not created -
  /// creation is deferred until the first call to File::AllocateSpace().
  Status GetFile(const DeviceId& device_id, const TUniqueId& query_id,
      File** new_file);

  /// Return the scratch directory path for the device.
  std::string GetTmpDirPath(DeviceId device_id) const;

  /// Total number of devices with tmp directories that are active. There is one tmp
  /// directory per device.
  int num_active_tmp_devices();

  /// Return vector with device ids of all tmp devices being actively used.
  /// I.e. those that haven't been blacklisted.
  std::vector<DeviceId> active_tmp_devices();

 private:
  /// Dir stores information about a temporary directory.
  class Dir {
   public:
    const std::string& path() const { return path_; }

    // Return true if it was newly added to blacklist.
    bool blacklist() {
      bool was_blacklisted = blacklisted_;
      blacklisted_ = true;
      return !was_blacklisted;
    }
    bool is_blacklisted() const { return blacklisted_; }

   private:
    friend class TmpFileMgr;

    /// path should be a absolute path to a writable scratch directory.
    Dir(const std::string& path, bool blacklisted)
        : path_(path), blacklisted_(blacklisted) {}

    std::string path_;

    bool blacklisted_;
  };

  /// Remove a device from the rotation. Subsequent attempts to allocate a file on that
  /// device will fail and the device will not be included in active tmp devices.
  void BlacklistDevice(DeviceId device_id);

  bool IsBlacklisted(DeviceId device_id);

  bool initialized_;

  /// Protects the status of tmp dirs (i.e. whether they're blacklisted).
  SpinLock dir_status_lock_;

  /// The created tmp directories.
  std::vector<Dir> tmp_dirs_;

  /// Metrics to track active scratch directories.
  IntGauge* num_active_scratch_dirs_metric_;
  SetMetric<std::string>* active_scratch_dirs_metric_;
};

}

#endif
