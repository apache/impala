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

#include "util/disk-info.h"

#ifdef __APPLE__
#include <sys/mount.h>
#else
#include <sys/vfs.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <iostream>
#include <fstream>
#include <sstream>

#include "util/debug-util.h"

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
int DiskInfo::num_datanode_dirs_;

// Parses /proc/partitions to get the number of disks.  A bit of looking around
// seems to indicate this as the best way to do this.
// TODO: is there not something better than this?
void DiskInfo::GetDeviceNames() {
  // Format of this file is:
  //    major, minor, #blocks, name
  // We are only interesting in name which is formatted as device_name<partition #>
  // The same device will show up multiple times for each partition (e.g. sda1, sda2).
  ifstream partitions("/proc/partitions", ios::in);
  while (partitions.good() && !partitions.eof()) {
    string line;
    getline(partitions, line);
    trim(line);

    vector<string> fields;
    split(fields, line, is_any_of(" "), token_compress_on);
    if (fields.size() != 4) continue;
    string name = fields[3];
    if (name == "name") continue;

    // Remove the partition# from the name.  e.g. sda2 --> sda
    trim_right_if(name, is_any_of("0123456789"));

    // Create a mapping of all device ids (one per partition) to the disk id.
    int major_dev_id = atoi(fields[0].c_str());
    int minor_dev_id = atoi(fields[1].c_str());
    dev_t dev = makedev(major_dev_id, minor_dev_id);
    DCHECK(device_id_to_disk_id_.find(dev) == device_id_to_disk_id_.end());

    int disk_id = -1;
    map<string, int>::iterator it = disk_name_to_disk_id_.find(name);
    if (it == disk_name_to_disk_id_.end()) {
      // First time seeing this disk
      disk_id = disks_.size();
      disks_.push_back(Disk(name, disk_id));
      disk_name_to_disk_id_[name] = disk_id;
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
    stringstream ss;
    ss << "/sys/block/" << disks_[i].name << "/queue/rotational";
    ifstream rotational(ss.str().c_str(), ios::in);
    if (rotational.good()) {
      string line;
      getline(rotational, line);
      if (line == "0") disks_[i].is_rotational = false;
    }
    if (rotational.is_open()) rotational.close();
  }
}

void DiskInfo::Init() {
  GetDeviceNames();
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
