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

#include "util/disk-info.h"

#include <stdlib.h>
#include <regex>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/types.h>

#include <fstream>
#include <utility>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/constants.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include "gutil/strings/substitute.h"
#include "util/filesystem-util.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using boost::algorithm::trim;
using boost::algorithm::trim_right_if;

namespace impala {

bool DiskInfo::initialized_;
vector<DiskInfo::Disk> DiskInfo::disks_;
map<dev_t, int> DiskInfo::device_id_to_disk_id_;
map<string, int> DiskInfo::disk_name_to_disk_id_;

bool DiskInfo::TryNVMETrim(const std::string& name_in, std::string* basename_out) {
  // NVME drives do not follow the typical device naming pattern. The pattern for NVME
  // drives is nvme{device_id}n{namespace_id}p{partition_id}. The appropriate thing
  // to do for this pattern is to trim the "p{partition_id}" part.
  std::regex nvme_regex = std::regex("(nvme[0-9]+n[0-9]+)(p[0-9]+)*");
  std::smatch nvme_match_result;
  if (std::regex_match(name_in, nvme_match_result, nvme_regex)) {
    DCHECK_GE(nvme_match_result.size(), 2);
    // Match 0 contains the whole string.
    // Match 1 contains the base nvme device without the partition.
    *basename_out = nvme_match_result[1];
    return true;
  }
  return false;
}

// Parses /proc/partitions to get the number of disks.  A bit of looking around
// seems to indicate this as the best way to do this.
// TODO: is there not something better than this?
void DiskInfo::GetDeviceNames(const std::string &proc, const std::string &sys) {
  disks_.clear();
  device_id_to_disk_id_.clear();
  disk_name_to_disk_id_.clear();

  // Format of this file is:
  //    major, minor, #blocks, name
  // We are only interesting in name which is formatted as device_name<partition #>
  // The same device will show up multiple times for each partition (e.g. sda1, sda2).
  ifstream partitions(Substitute("$0/partitions", proc), ios::in);
  while (partitions.good() && !partitions.eof()) {
    string line;
    getline(partitions, line);
    trim(line);

    vector<string> fields;
    split(fields, line, is_any_of(" "), token_compress_on);
    if (fields.size() != 4) continue;
    const string& partition_name = fields[3];
    if (partition_name == "name") continue;

    // Check if this is the top-level block device. If not, try to guess the
    // name of the top-level block device.
    bool found_device = false;
    string dev_name = partition_name;
    Status status = FileSystemUtil::PathExists(
      Substitute("$0/block/$1", sys, dev_name), &found_device);
    if (!status.ok()) LOG(WARNING) << status.GetDetail();
    if (!found_device) {
      // NVME devices have a special format. Try to detect that, falling back to the normal
      // method if this is not an NVME device.
      std::string nvme_basename;
      if (TryNVMETrim(dev_name, &nvme_basename)) {
        // This is an NVME device, use the returned basename
        dev_name = nvme_basename;
      } else {
        // Does not follow the NVME pattern, so use the logic for a normal disk device
        // Remove the partition# from the name.  e.g. sda2 --> sda
        trim_right_if(dev_name, is_any_of("0123456789"));
      }
    }

    // Create a mapping of all device ids (one per partition) to the disk id.
    int major_dev_id = atoi(fields[0].c_str());
    int minor_dev_id = atoi(fields[1].c_str());
    dev_t dev = makedev(major_dev_id, minor_dev_id);
    DCHECK(device_id_to_disk_id_.find(dev) == device_id_to_disk_id_.end());

    int disk_id = -1;
    map<string, int>::iterator it = disk_name_to_disk_id_.find(dev_name);
    if (it == disk_name_to_disk_id_.end()) {
      // First time seeing this disk
      disk_id = disks_.size();
      disks_.push_back(Disk(dev_name, disk_id));
      disk_name_to_disk_id_[dev_name] = disk_id;
    } else {
      disk_id = it->second;
    }
    device_id_to_disk_id_[dev] = disk_id;
  }

  if (partitions.is_open()) partitions.close();

  if (disks_.empty()) {
    // If all else fails, return 1
    LOG(WARNING) << "Could not determine number of disks on this machine.";
    disks_.push_back(Disk("sda", 0));
    return;
  }

  // Determine if the disk is rotational or not.
  for (int i = 0; i < disks_.size(); ++i) {
    // We can check if it is rotational by reading:
    // /sys/block/<device>/queue/rotational
    // If the file is missing or has unexpected data, default to rotational.
    std::string block_rot =
      Substitute("$0/block/$1/queue/rotational", sys, disks_[i].name);
    ifstream rotational(block_rot, ios::in);
    if (rotational.good()) {
      string line;
      getline(rotational, line);
      if (line == "0") disks_[i].is_rotational = false;
    } else {
      LOG(INFO) << "Could not read " << block_rot << " for " << disks_[i].name
                << " , assuming rotational.";
    }
    if (rotational.is_open()) rotational.close();
  }
}

void DiskInfo::Init(std::string proc, std::string sys) {
  GetDeviceNames(proc, sys);
  initialized_ = true;
}

int DiskInfo::disk_id(const char* path) {
  struct stat s;
  stat(path, &s);
  map<dev_t, int>::iterator it = device_id_to_disk_id_.find(s.st_dev);
  if (it == device_id_to_disk_id_.end()) return -1;
  return it->second;
}

string DiskInfo::DebugString() {
  DCHECK(initialized_);
  stringstream stream;
  stream << "Disk Info: " << endl;
  stream << "  Num disks " << num_disks() << ": " << endl;
  for (int i = 0; i < disks_.size(); ++i) {
    stream << "    " << disks_[i].name
           << " (rotational=" << (disks_[i].is_rotational ? "true" : "false") << ")\n";
  }
  stream << endl;
  return stream.str();
}


}
