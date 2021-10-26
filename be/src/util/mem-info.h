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
#include <string>

#include "common/logging.h"
#include "common/status.h"

namespace impala {

/// Information obtained from /proc/<pid>/smaps.
struct MappedMemInfo {
  // Number of memory maps.
  int64_t num_maps = 0;

  // Total size of memory maps (i.e. virtual memory size) in kilobytes.
  int64_t size_kb = 0;

  // RSS in kilobytes
  int64_t rss_kb = 0;

  // Kilobytes of anonymous huge pages.
  int64_t anon_huge_pages_kb = 0;

  std::string DebugString() const;
};

/// Information about the system transparent huge pages config.
struct ThpConfig {
  // Whether THP is enabled. Just contains the raw string, e.g. "[always] madvise never".
  std::string enabled;

  // Whether synchronous THP defrag is enabled, e.g. "[always] madvise never".
  std::string defrag;

  // Whether THP defrag via khugepaged is enabled. Usually "0"/"1".
  std::string khugepaged_defrag;

  std::string DebugString() const;
};

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

  /// Return true if the /proc/<pid>/smaps file is present and can be opened.
  static bool HaveSmaps();

  /// Parse /proc/<pid>/smaps for this process and extract relevant information.
  /// Logs a warning if the file could not be opened or had an unexpected format.
  static MappedMemInfo ParseSmaps();

  /// Parse the transparent huge pages configs.
  /// Logs a warning if a file could not be opened or had an unexpected format.
  static ThpConfig ParseThpConfig();

  static std::string DebugString();

 private:
  static void ParseOvercommit();

  /// Get the config value from a file, trying the path relative to both
  /// /sys/kernel/mm/transparent_hugepage and /sys/kernel/mm/redhat_transparent_hugepage.
  /// Assumes the file has a single line only.
  static std::string GetThpConfigVal(const std::string& relative_path);

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

/// Choose a memory limit (returned in *bytes_limit) based on the --mem_limit flag and
/// the memory available to the daemon process. Returns an error if the memory limit is
/// invalid or another error is encountered that should prevent starting up the daemon.
/// Logs the memory limit chosen and any relevant diagnostics related to that choice.
/// If avail_mem is not nullptr, the bytes of system available memory will be returned.
Status ChooseProcessMemLimit(int64_t* bytes_limit, int64_t* avail_mem = nullptr);
}
