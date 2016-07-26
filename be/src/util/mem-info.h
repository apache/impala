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

#ifndef IMPALA_UTIL_MEM_INFO_H
#define IMPALA_UTIL_MEM_INFO_H

#include <string>
#include <boost/cstdint.hpp>

#include "common/logging.h"

namespace impala {

/// Provides the amount of physical memory available.
/// Populated from /proc/meminfo.
/// TODO: Allow retrieving of cgroup memory limits,
/// e.g., by checking /sys/fs/cgroup/memory/groupname/impala/memory.limit_in_bytes
/// TODO: Combine mem-info, cpu-info and disk-info into hardware-info?
class MemInfo {
 public:
  /// Initialize MemInfo.
  static void Init();

  /// Get total physical memory in bytes (ignores cgroups memory limits).
  static int64_t physical_mem() {
    DCHECK(initialized_);
    return physical_mem_;
  }

  /// Returns the systems memory overcommit settings, typically the values are 0,1, and 2
  static int32_t vm_overcommit() {
    DCHECK(initialized_);
    return vm_overcommit_;
  }

  /// Returns the systems memory commit limit. This value is only of importance if the
  /// memory overcommit setting is set to 2.
  static int64_t commit_limit() {
    DCHECK(initialized_);
    return commit_limit_;
  }

  static std::string DebugString();

 private:

  static void ParseOvercommit();

  static bool initialized_;
  static int64_t physical_mem_;

  /// See https://www.kernel.org/doc/Documentation/filesystems/proc.txt for more details on
  /// the specific meaning of the below variables and
  /// https://www.kernel.org/doc/Documentation/vm/overcommit-accounting for more details on
  /// overcommit accounting.

  /// Indicating the kernel overcommit settings
  //(e.g. heuristic based / always / strict)
  static int32_t vm_overcommit_;

  /// If overcommit is turned off the maximum allocatable memory
  static int64_t commit_limit_;
};

}
#endif
