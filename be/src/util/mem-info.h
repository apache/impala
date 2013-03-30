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

#ifndef IMPALA_UTIL_MEM_INFO_H
#define IMPALA_UTIL_MEM_INFO_H

#include <string>
#include <boost/cstdint.hpp>

#include "common/logging.h"

namespace impala {

// Provides the amount of physical memory available.
// Populated from /proc/meminfo.
// TODO: Allow retrieving of cgroup memory limits,
// e.g., by checking /sys/fs/cgroup/memory/groupname/impala/memory.limit_in_bytes
// TODO: Combine mem-info, cpu-info and disk-info into hardware-info?
class MemInfo {
 public:
  // Initialize MemInfo.
  static void Init();

  // Get total physical memory in bytes (ignores cgroups memory limits).
  static int64_t physical_mem() {
    DCHECK(initialized_);
    return physical_mem_;
  }

  static std::string DebugString();

 private:
  static bool initialized_;
  static int64_t physical_mem_;
};

}
#endif
