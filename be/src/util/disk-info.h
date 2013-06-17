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


#ifndef IMPALA_UTIL_DISK_INFO_H
#define IMPALA_UTIL_DISK_INFO_H

#include <map>
#include <string>

#include <boost/cstdint.hpp>
#include "common/logging.h"

namespace impala {

// DiskInfo is an interface to query for the disk information at runtime.  This
// contains information about the system as well as the specific data node 
// configuration.
// This information is pulled from /proc/partitions.
// TODO: datanode information not implemented
class DiskInfo {
 public:
  // Initialize DiskInfo.  Just be called before any other functions.
  static void Init();

  // Returns the number of (logical) disks on the system
  static int num_disks() {
    DCHECK(initialized_);
    return disks_.size();
  }

#if 0
  // Returns the number of (logical) disks the data node is using.
  // It is possible for this to be more than num_disks since the datanode
  // can be configured to have multiple data directories on the same physical
  // disk.
  static int num_datanode_dirs() {
    DCHECK(initialized_);
    return num_datanode_dirs_;
  }

  // Returns a 0-based disk index for the data node dirs index.
  static int disk_id(int datanode_dir_idx) {
    return 0;
  }
#endif

  // Returns the 0-based disk index for 'path' (path must be a FS path, not
  // hdfs path).
  static int disk_id(const char* path);

  // Returns the device name (e.g. sda) for disk_id
  static const std::string& device_name(int disk_id) {
    DCHECK_GE(disk_id, 0);
    DCHECK_LT(disk_id, disks_.size());
    return disks_[disk_id].name;
  }

  static bool is_rotational(int disk_id) {
    DCHECK_GE(disk_id, 0);
    DCHECK_LT(disk_id, disks_.size());
    return disks_[disk_id].is_rotational;
  }
  
  static std::string DebugString();

 private:
  static bool initialized_;

  struct Disk {
    // Name of the disk (e.g. sda)
    std::string name;

    // 0 based index.  Does not map to anything in the system, useful to index into 
    // our structures
    int id;

    bool is_rotational;

    Disk(const std::string& name = "", int id = -1, bool is_rotational = true) 
      : name(name), id(id), is_rotational(is_rotational) {}
  };

  // All disks
  static std::vector<Disk> disks_;

  // mapping of dev_ts to disk ids
  static std::map<dev_t, int> device_id_to_disk_id_;
  
  // mapping of devices names to disk ids
  static std::map<std::string, int> disk_name_to_disk_id_;

  static int num_datanode_dirs_;

  static void GetDeviceNames();
};


}
#endif
