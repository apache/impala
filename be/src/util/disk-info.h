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

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "common/logging.h"

namespace impala {

/// DiskInfo is an interface to query for the disk information at runtime.  This
/// contains information about the system as well as the specific data node
/// configuration.
/// This information is pulled from /proc/partitions.
/// TODO: datanode information not implemented
///
/// Arguments are used for mocking disk info in tests.
class DiskInfo {
 public:
  /// Initialize DiskInfo.  Just be called before any other functions.
  static void Init(std::string proc = "/proc", std::string sys = "/sys");

  /// Returns the number of (logical) disks on the system
  static int num_disks() {
    DCHECK(initialized_);
    return disks_.size();
  }

  /// Returns the 0-based disk index for 'path' (path must be a FS path, not
  /// hdfs path). Returns -1 if the disk index is unknown.
  static int disk_id(const char* path);

  /// Returns the device name (e.g. sda) for disk_id
  static const std::string& device_name(int disk_id) {
    DCHECK_GE(disk_id, 0);
    DCHECK_LT(disk_id, disks_.size());
    return disks_[disk_id].name;
  }

  /// Returns true if the disk with disk_id exists and is a rotational disk, is false
  /// otherwise
  static bool is_rotational(int disk_id) {
    DCHECK_GE(disk_id, 0);
    // TODO: temporarily removed DCHECK due to an issue tracked in IMPALA-5574, put it
    // back after its resolved
    if (disk_id >= disks_.size()) return false;
    return disks_[disk_id].is_rotational;
  }

  /// Returns true if the disk with name 'device_name' is known to the DiskInfo class,
  /// i.e. we consider it to be useful to include in metrics and such.
  static bool is_known_disk(const std::string& device_name) {
    return disk_name_to_disk_id_.find(device_name) != disk_name_to_disk_id_.end();
  }

  static std::string DebugString();

 private:
  friend class DiskInfoTest;

  static bool initialized_;

  struct Disk {
    /// Name of the disk (e.g. sda)
    std::string name;

    /// 0 based index.  Does not map to anything in the system, useful to index into
    /// our structures
    int id;

    bool is_rotational;

    Disk(const std::string& name = "", int id = -1, bool is_rotational = true)
      : name(name), id(id), is_rotational(is_rotational) {}
  };

  /// All disks
  static std::vector<Disk> disks_;

  /// mapping of dev_ts to disk ids
  static std::map<dev_t, int> device_id_to_disk_id_;

  /// mapping of devices names to disk ids
  static std::map<std::string, int> disk_name_to_disk_id_;

  static void GetDeviceNames(const std::string& proc, const std::string &sys);

  /// See if 'name_in' is an NVME device. If it is, set 'basename_out' to the base
  /// NVME device name (i.e. nvme0n1p1 -> nvme0n1) and return true. Otherwise,
  /// return false and leave 'basename_out' unmodified.
  static bool TryNVMETrim(const std::string& name_in, std::string* basename_out);
};
}
