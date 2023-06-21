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

#include "util/cgroup-util.h"

#include <algorithm>
#include <fstream>
#include <vector>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/constants.hpp>
#include <boost/algorithm/string/split.hpp>

#include "common/logging.h"
#include "gutil/strings/escaping.h"
#include "gutil/strings/substitute.h"
#include "util/error-util.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using strings::CUnescape;

namespace impala {

Status CGroupUtil::FindGlobalCGroup(const string& subsystem, string* path) {
  ifstream proc_cgroups("/proc/self/cgroup", ios::in);
  if (!proc_cgroups.is_open()) {
    return Status(Substitute("Error opening /proc/self/cgroup: $0", GetStrErrMsg()));
  }

  string line;
  while (getline(proc_cgroups, line)) {
    // The line format looks like this:
    // 4:memory:/user.slice
    // 9:cpu,cpuacct:/user.slice
    // 0::/user.slice
    vector<string> fields;
    split(fields, line, is_any_of(":"));
    DCHECK_GE(fields.size(), 3);
    // ":" in the path does not appear to be escaped - bail in the unusual case that
    // we get too many tokens.
    if (fields.size() > 3) {
      return Status(Substitute(
          "Could not parse line from /proc/self/cgroup - had $0 > 3 tokens: '$1'",
          fields.size(), line));
    }
    vector<string> subsystems;
    split(subsystems, fields[1], is_any_of(","));
    auto it = std::find(subsystems.begin(), subsystems.end(), subsystem);
    if (it != subsystems.end()) {
      *path = move(fields[2]);
      return Status::OK();
    }
  }

  if (proc_cgroups.bad()) {
    // Only badbit sets errno.
    return Status(Substitute("Error reading /proc/self/cgroup: $0", GetStrErrMsg()));
  } else {
    DCHECK(proc_cgroups.eof());
    return Status(
        Substitute("Could not find subsystem '$0' in /proc/self/cgroup", subsystem));
  }
}

static Status UnescapePath(const string& escaped, string* unescaped) {
  string err;
  if (!CUnescape(escaped, unescaped, &err)) {
    return Status(Substitute("Could not unescape path '$0': $1", escaped, err));
  }
  return Status::OK();
}

Status CGroupUtil::FindCGroupMounts(
    const string& subsystem, string* mount_path, string* system_path, bool* is_v2) {
  ifstream mountinfo("/proc/self/mountinfo", ios::in);
  if (!mountinfo.is_open()) {
    return Status(Substitute("Error opening /proc/self/mountinfo: $0", GetStrErrMsg()));
  }

  string line;
  bool found_cgroup_v2 = false;
  while (getline(mountinfo, line)) {
    // The relevant lines look like below (see proc manpage for full documentation). The
    // first example is running outside of a container, the second example is running
    // inside a docker container. Field 3 is the path relative to the root CGroup on
    // the host and Field 4 is the mount point from this process's point of view.
    // 34 29 0:28 / /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime shared:15 -
    //    cgroup cgroup rw,memory
    // 275 271 0:28 /docker/f23eee6f88c2ba99fcce /sys/fs/cgroup/memory
    //    ro,nosuid,nodev,noexec,relatime master:15 - cgroup cgroup rw,memory
    //
    // CGroup v2 will contain only a single entry marked cgroup2
    // 29 23 0:26 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:4 -
    //    cgroup2 cgroup2 rw,nsdelegate,memory_recursiveprot
    // Some systems (Ubuntu) list both, but CGroup v2 isn't usable, so if we identify
    // a subsystem mount return that, else if we see a CGroup v2 entry return it.
    // 34 24 0:29 / /sys/fs/cgroup ro,nosuid,nodev,noexec shared:9 -
    //    tmpfs tmpfs ro,mode=755,inode64
    // 35 34 0:30 / /sys/fs/cgroup/unified rw,nosuid,nodev,noexec,relatime shared:10 -
    //    cgroup2 cgroup2 rw,nsdelegate
    // 50 34 0:45 / /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime shared:26 -
    //    cgroup cgroup rw,memory
    vector<string> fields;
    split(fields, line, is_any_of(" "), token_compress_on);
    DCHECK_GE(fields.size(), 7);

    bool is_cgroup_v1 = fields[fields.size() - 3] == "cgroup";
    if (!is_cgroup_v1 && fields[fields.size() - 3] != "cgroup2") continue;

    // This is a cgroup mount.
    if (is_cgroup_v1) {
      // Check if it's the subsystem we're looking for.
      vector<string> cgroup_opts;
      split(cgroup_opts, fields[fields.size() - 1], is_any_of(","), token_compress_on);
      auto it = std::find(cgroup_opts.begin(), cgroup_opts.end(), subsystem);
      if (it == cgroup_opts.end()) continue;
    }

    // This is the right mount.
    RETURN_IF_ERROR(UnescapePath(fields[4], mount_path));
    RETURN_IF_ERROR(UnescapePath(fields[3], system_path));
    // Strip trailing "/" so that both returned paths match in whether they have a
    // trailing "/".
    if (system_path->back() == '/') system_path->pop_back();
    if (is_cgroup_v1) {
      *is_v2 = false;
      return Status::OK();
    } else {
      // Keep looking in case there are also CGroupv1 mounts.
      found_cgroup_v2 = true;
    }
  }

  if (mountinfo.bad()) {
    // Only badbit sets errno.
    return Status(Substitute("Error reading /proc/self/mountinfo: $0", GetStrErrMsg()));
  } else if (found_cgroup_v2) {
    *is_v2 = true;
    return Status::OK();
  } else {
    DCHECK(mountinfo.eof());
    return Status(
        Substitute("Could not find subsystem '$0' in /proc/self/mountinfo", subsystem));
  }
}

Status CGroupUtil::FindCGroupMemLimit(int64_t* bytes, bool* is_v2) {
  string mount_path, system_path, cgroup_path, line;
  bool local_is_v2;
  RETURN_IF_ERROR(FindCGroupMounts("memory", &mount_path, &system_path, &local_is_v2));
  if (is_v2 != nullptr) {
    *is_v2 = local_is_v2;
  }
  RETURN_IF_ERROR(FindGlobalCGroup(local_is_v2 ? "" : "memory", &cgroup_path));
  if (cgroup_path.compare(0, system_path.size(), system_path) != 0) {
    return Status(Substitute(
        "Expected CGroup path '$0' to start with '$1'", cgroup_path, system_path));
  }
  cgroup_path.replace(0, system_path.size(), mount_path);

  string limit_file_name = local_is_v2 ? "memory.max" : "memory.limit_in_bytes";
  string limit_file_path = cgroup_path + "/" + limit_file_name;
  ifstream limit_file(limit_file_path);
  if (!limit_file.is_open()) {
    return Status(Substitute("Error opening $0: $1", limit_file_path, GetStrErrMsg()));
  }
  getline(limit_file, line);
  if (limit_file.bad()) {
    return Status(Substitute("Error reading $0: $1", limit_file_path, GetStrErrMsg()));
  }

  if (line == "max") {
    *bytes = std::numeric_limits<int64_t>::max();
    return Status::OK();
  }

  StringParser::ParseResult pr;
  // Parse into an an int64_t, since that is what we use for memory amounts elsewhere in
  // the codebase. If it overflows, returning the max value of int64_t is ok because that
  // is effectively unlimited.
  *bytes = StringParser::StringToInt<int64_t>(line.c_str(), line.size(), &pr);
  if ((pr != StringParser::PARSE_SUCCESS && pr != StringParser::PARSE_OVERFLOW)
      || *bytes < 0) {
    return Status(
        Substitute("Failed to parse $0 as positive int64: '$1'", limit_file_path, line));
  }
  return Status::OK();
}

std::string CGroupUtil::DebugString() {
  string mem_limit_str;
  int64_t mem_limit;
  bool is_v2;
  Status status = FindCGroupMemLimit(&mem_limit, &is_v2);
  if (status.ok()) {
    mem_limit_str = Substitute("$0", mem_limit);
  } else {
    mem_limit_str = status.GetDetail();
  }
  string property = is_v2 ? "memory.max" : "memory.limit_in_bytes";
  return Substitute("Process CGroup Info: $0=$1", property, mem_limit_str);
}

} // namespace impala
