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
//
// Imported from Impala. Changes include:
// - Namespace and imports.
// - Replaced GetStrErrMsg with ErrnoToString.
// - Replaced StringParser with strings/numbers.
// - Fixes for cpplint.
// - Fixed parsing when thread names have spaces.

#include "kudu/util/os-util.h"

#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <string>
#include <sys/resource.h>
#include <vector>
#include <unistd.h>

#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/errno.h"

using std::ifstream;
using std::istreambuf_iterator;
using std::ostringstream;
using strings::Split;
using strings::Substitute;

namespace kudu {

// Ensure that Impala compiles on earlier kernels. If the target kernel does not support
// _SC_CLK_TCK, sysconf(_SC_CLK_TCK) will return -1.
#ifndef _SC_CLK_TCK
#define _SC_CLK_TCK 2
#endif

static const int64_t kTicksPerSec = sysconf(_SC_CLK_TCK);

// Offsets into the ../stat file array of per-thread statistics.
//
// They are themselves offset by two because the pid and comm fields of the
// file are parsed separately.
static const int64_t kUserTicks = 13 - 2;
static const int64_t kKernelTicks = 14 - 2;
static const int64_t kIoWait = 41 - 2;

// Largest offset we are interested in, to check we get a well formed stat file.
static const int64_t kMaxOffset = kIoWait;

Status ParseStat(const std::string& buffer, std::string* name, ThreadStats* stats) {
  DCHECK(stats != nullptr);

  // The thread name should be the only field with parentheses. But the name
  // itself may contain parentheses.
  size_t open_paren = buffer.find('(');
  size_t close_paren = buffer.rfind(')');
  if (open_paren == string::npos  ||      // '(' must exist
      close_paren == string::npos ||      // ')' must exist
      open_paren >= close_paren   ||      // '(' must come before ')'
      close_paren + 2 == buffer.size()) { // there must be at least two chars after ')'
    return Status::IOError("Unrecognised /proc format");
  }
  string extracted_name = buffer.substr(open_paren + 1, close_paren - (open_paren + 1));
  string rest = buffer.substr(close_paren + 2);
  vector<string> splits = Split(rest, " ", strings::SkipEmpty());
  if (splits.size() < kMaxOffset) {
    return Status::IOError("Unrecognised /proc format");
  }

  int64 tmp;
  if (safe_strto64(splits[kUserTicks], &tmp)) {
    stats->user_ns = tmp * (1e9 / kTicksPerSec);
  }
  if (safe_strto64(splits[kKernelTicks], &tmp)) {
    stats->kernel_ns = tmp * (1e9 / kTicksPerSec);
  }
  if (safe_strto64(splits[kIoWait], &tmp)) {
    stats->iowait_ns = tmp * (1e9 / kTicksPerSec);
  }
  if (name != nullptr) {
    *name = extracted_name;
  }
  return Status::OK();

}

Status GetThreadStats(int64_t tid, ThreadStats* stats) {
  DCHECK(stats != nullptr);
  if (kTicksPerSec <= 0) {
    return Status::NotSupported("ThreadStats not supported");
  }

  ostringstream proc_path;
  proc_path << "/proc/self/task/" << tid << "/stat";
  ifstream proc_file(proc_path.str().c_str());
  if (!proc_file.is_open()) {
    return Status::IOError("Could not open ifstream");
  }

  string buffer((istreambuf_iterator<char>(proc_file)),
      istreambuf_iterator<char>());

  return ParseStat(buffer, nullptr, stats); // don't want the name
}

void DisableCoreDumps() {
  struct rlimit lim;
  PCHECK(getrlimit(RLIMIT_CORE, &lim) == 0);
  lim.rlim_cur = 0;
  PCHECK(setrlimit(RLIMIT_CORE, &lim) == 0);

  // Set coredump_filter to not dump any parts of the address space.
  // Although the above disables core dumps to files, if core_pattern
  // is set to a pipe rather than a file, it's not sufficient. Setting
  // this pattern results in piping a very minimal dump into the core
  // processor (eg abrtd), thus speeding up the crash.
  int f = open("/proc/self/coredump_filter", O_WRONLY);
  if (f >= 0) {
    write(f, "00000000", 8);
    close(f);
  }
}


} // namespace kudu
