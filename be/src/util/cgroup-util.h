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
#include <utility>

#include "common/status.h"

namespace impala {

class CGroupUtil {
 public:
  /// Determines the CGroup memory limit from the current processes' cgroup.
  /// If the limit is more than INT64_MAX, INT64_MAX is returned (since that is
  /// effectively unlimited anyway). Does not take into account memory limits
  /// set on any ancestor CGroups.
  static Status FindCGroupMemLimit(int64_t* bytes);

  /// Returns a human-readable string with information about CGroups.
  static std::string DebugString();

 private:
  friend class CGroupInfo_ErrorHandling_Test;
  /// Finds the path of the cgroup of 'subsystem' for the current process.
  /// E.g. FindGlobalCGroup("memory") will return the memory cgroup
  /// that this process belongs to. This is a path relative to the system-wide root
  /// cgroup for 'subsystem'.
  static Status FindGlobalCGroup(const std::string& subsystem, std::string* path);

  /// Returns the absolute path to the CGroup from inside the container.
  /// E.g. if this process belongs to
  /// /sys/fs/cgroup/memory/kubepods/burstable/pod-<long unique id>, which is mounted at
  /// /sys/fs/cgroup/memory inside the container, this function returns
  /// "/sys/fs/cgroup/memory".
  static Status FindAbsCGroupPath(const std::string& subsystem, std::string* path);

  /// Figures out the mapping of the cgroup root from the container's point of view to
  /// the full path relative to the system-wide cgroups outside of the container.
  /// E.g. /sys/fs/cgroup/memory/kubepods/burstable/pod-<long unique id> may be mounted at
  /// /sys/fs/cgroup/memory inside the container. In that case this function would return
  /// ("/sys/fs/cgroup/memory", "kubepods/burstable/pod-<long unique id>").
  static Status FindCGroupMounts(
      const std::string& subsystem, std::pair<std::string, std::string>* result);
};
} // namespace impala
