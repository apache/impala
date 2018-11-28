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

#ifndef IMPALA_UTIL_PROC_UTIL_H
#define IMPALA_UTIL_PROC_UTIL_H

#include "common/logging.h"
#include "common/status.h"

namespace impala {

/// Utility methods to read interesting values from /proc.
/// TODO: Get stats for parent process.

/// Container struct for statistics read from the /proc filesystem for a thread.
struct ThreadStats {
  int64_t user_ns;
  int64_t kernel_ns;
  int64_t iowait_ns;

  /// Default constructor zeroes all members in case structure can't be filled by
  /// GetThreadStats.
  ThreadStats() : user_ns(0), kernel_ns(0), iowait_ns(0) { }
};

/// Populates ThreadStats object for a given thread by reading from
/// /proc/<pid>/task/<tid>/stats. Returns OK unless the file cannot be read or is in an
/// unrecognised format, or if the kernel version is not modern enough.
Status GetThreadStats(int64_t tid, ThreadStats* stats);

/// Runs a shell command. Returns false if there was any error (either failure to launch
/// or non-0 exit code), and true otherwise. *msg is set to an error message including the
/// OS error string, if any, and the first 1k of output if there was any error, or just
/// the first 1k of output otherwise.
/// If do_trim is 'true', all trailing whitespace is removed from the output.
/// unset_environment is used to unset a set of environment variables prior to running
/// the cmd.
bool RunShellProcess(const std::string& cmd, std::string* msg, bool do_trim=false,
    const std::set<std::string>& unset_environment=std::set<std::string>());

}
#endif
