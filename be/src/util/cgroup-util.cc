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
#include <iostream>
#include <utility>

#include <boost/algorithm/string.hpp>

#include "gutil/strings/escaping.h"
#include "gutil/strings/substitute.h"
#include "util/error-util.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using strings::CUnescape;
using std::pair;

namespace impala {

Status CGroupUtil::FindGlobalCGroup(const string& subsystem, string* path) {
  ifstream proc_cgroups("/proc/self/cgroup", ios::in);
  string line;
  while (true) {
    if (proc_cgroups.fail() || proc_cgroups.bad()) {
      return Status(Substitute("Error reading /proc/self/cgroup: $0", GetStrErrMsg()));
    } else if (proc_cgroups.eof()) {
      return Status(
          Substitute("Could not find subsystem $0 in /proc/self/cgroup", subsystem));
    }
    // The line format looks like this:
    // 4:memory:/user.slice
    // 9:cpu,cpuacct:/user.slice
    getline(proc_cgroups, line);
    if (!proc_cgroups.good()) continue;
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
}

static Status UnescapePath(const string& escaped, string* unescaped) {
  string err;
  if (!CUnescape(escaped, unescaped, &err)) {
    return Status(Substitute("Could not unescape path '$0': $1", escaped, err));
  }
  return Status::OK();
}

Status CGroupUtil::FindCGroupMounts(
    const string& subsystem, pair<string, string>* result) {
  ifstream mountinfo("/proc/self/mountinfo", ios::in);
  string line;
  while (true) {
    if (mountinfo.fail() || mountinfo.bad()) {
      return Status(Substitute("Error reading /proc/self/mountinfo: $0", GetStrErrMsg()));
    } else if (mountinfo.eof()) {
      return Status(
          Substitute("Could not find subsystem $0 in /proc/self/mountinfo", subsystem));
    }
    // The relevant lines look like below (see proc manpage for full documentation). The
    // first example is running outside of a container, the second example is running
    // inside a docker container. Field 3 is the path relative to the root CGroup on
    // the host and Field 4 is the mount point from this process's point of view.
    // 34 29 0:28 / /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime shared:15 -
    //    cgroup cgroup rw,memory
    // 275 271 0:28 /docker/f23eee6f88c2ba99fcce /sys/fs/cgroup/memory
    //    ro,nosuid,nodev,noexec,relatime master:15 - cgroup cgroup rw,memory
    getline(mountinfo, line);
    if (!mountinfo.good()) continue;
    vector<string> fields;
    split(fields, line, is_any_of(" "), token_compress_on);
    DCHECK_GE(fields.size(), 7);

    if (fields[fields.size() - 3] != "cgroup") continue;
    // This is a cgroup mount. Check if it's the mount we're looking for.
    vector<string> cgroup_opts;
    split(cgroup_opts, fields[fields.size() - 1], is_any_of(","), token_compress_on);
    auto it = std::find(cgroup_opts.begin(), cgroup_opts.end(), subsystem);
    if (it == cgroup_opts.end()) continue;
    // This is the right mount.
    string mount_path, system_path;
    RETURN_IF_ERROR(UnescapePath(fields[4], &mount_path));
    RETURN_IF_ERROR(UnescapePath(fields[3], &system_path));
    // Strip trailing "/" so that both returned paths match in whether they have a
    // trailing "/".
    if (system_path[system_path.size() - 1] == '/') system_path.pop_back();
    *result = {mount_path, system_path};
    return Status::OK();
  }
}

Status CGroupUtil::FindAbsCGroupPath(const string& subsystem, string* path) {
  RETURN_IF_ERROR(FindGlobalCGroup(subsystem, path));
  pair<string, string> paths;
  RETURN_IF_ERROR(FindCGroupMounts(subsystem, &paths));
  const string& mount_path = paths.first;
  const string& system_path = paths.second;
  if (path->compare(0, system_path.size(), system_path) != 0) {
    return Status(
        Substitute("Expected CGroup path '$0' to start with '$1'", *path, system_path));
  }
  path->replace(0, system_path.size(), mount_path);
  return Status::OK();
}

Status CGroupUtil::FindCGroupMemLimit(int64_t* bytes) {
  string cgroup_path;
  RETURN_IF_ERROR(FindAbsCGroupPath("memory", &cgroup_path));
  string limit_file_path = cgroup_path + "/memory.limit_in_bytes";
  ifstream limit_file(limit_file_path, ios::in);
  string line;
  getline(limit_file, line);
  if (limit_file.fail() || limit_file.bad()) {
    return Status(Substitute("Error reading $0: $1", limit_file_path, GetStrErrMsg()));
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
  Status status = FindCGroupMemLimit(&mem_limit);
  if (status.ok()) {
    mem_limit_str = Substitute("$0", mem_limit);
  } else {
    mem_limit_str = status.GetDetail();
  }
  return Substitute("Process CGroup Info: memory.limit_in_bytes=$0", mem_limit_str);
}

} // namespace impala
