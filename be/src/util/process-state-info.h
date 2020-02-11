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
#include <map>
#include <string>

#include "common/logging.h"

namespace impala {

/// ProcessStateInfo is an interface to query for process state information
/// at runtime. This contains information about I/O, Scheduler, Process status,
/// as well as the file descriptors that belong to the process.
/// Below are some of the I/O information will be read from /proc/self/io.
/// io/rchar[int64]: Number of bytes which this task has caused to be read from storage.
/// io/wchar[int64]: Number of bytes which this task has caused to be written to storage.
/// io/syscr[int64]: Number of read I/O operations.
/// io/syscw[int64]: Number of write I/O operations.
/// io/read_bytes[int64]: Number of bytes which this process fetched from storage.
/// io/write_bytes[int64]: Number of bytes which this process wrote to storage.
/// io/cancelled_write_bytes[int64]: Number of bytes which this process did not write,
/// by truncating pagecache.

/// Below are some of the Scheduler information that will be read from /proc/self/sched.
/// sched/se.sum_exec_runtime[string]: Sum of execute time.
/// sched/se.statistics.wait_max[string]: Max wait time in ready queue.
/// sched/se.statistics.wait_sum[string]: Sum of wait time in ready queue.
/// sched/se.statistics.wait_count[int64]: Total of wait times.
/// sched/se.statistics.iowait_sum[string]: Sum of wait time for I/O.
/// sched/se.statistics.iowait_count[int64]: Total of wait times for I/O.
/// sched/se.statistics.nr_wakeups_migrate[int64]: Number of wakeups with cpu migration.
/// sched/nr_switches[int64]: Total number of context switches.
/// sched/nr_voluntary_switches[int]: Number of voluntary context switches.
/// sched/nr_involuntary_switches[int]: Number of involuntary context switches.
/// sched/prio[int]: The process priority.

/// Below are some of the Process status information that will be read
/// from /proc/self/status.
/// status/Threads[int]: Number of threads in process.
/// status/VmPeak[string]: Peak virtual memory size.
/// status/VmSize[string]: Virtual memory size.
/// status/VmLck[string]: Locked memory size.
/// status/VmPin[string]: Pinned memory size.
/// status/VmHWM[string]: Peak resident set size ("high water mark").
/// status/VmRSS[string]: Resident Set Size.
/// status/VmData[string], status/VmStk[string], status/VmExe[string]:
/// Size of data, stack, and text segments.
/// status/VmLib[string]: Shared library code size.
/// status/VmPTE[string]: Page table entries size (since Linux 2.6.10).
/// status/VmSwap[string]: Swap memory size.
/// status/Cpus_allowed_list[string]: List of CPUs on which this process may run.
/// status/Mems_allowed_list[string]: Memory nodes allowed to this process.

/// File Descriptors information will be read from /proc/self/fd.
/// fd/count[int]: The number of files the process has open.

class ProcessStateInfo {
 public:
  /// Read the current process state info when constructed. There is no need
  /// to be thread safe. If 'get_extended_metrics' is false, only the contents
  /// of /status are collected. It it is true, all metrics are collected.
  ProcessStateInfo(bool get_extended_metrics=true);

  std::string DebugString() const;

  int GetInt(const std::string& state_key) const;
  int64_t GetInt64(const std::string& state_key) const;
  std::string GetString(const std::string& state_key) const;

  /// Original data's unit is B or KB.
  int64_t GetBytes(const std::string& state_key) const;

  /// Return the virtual memory size in bytes.
  int64_t GetVmSize() const { return GetBytes("status/VmSize"); }

  //// Return the process RSS in bytes.
  int64_t GetRss() const { return GetBytes("status/VmRSS"); }
 private:
  typedef std::map<std::string, std::string> ProcessStateMap;
  ProcessStateMap process_state_map_;

  const bool have_extended_metrics_;

  /// Read I/O info from /proc/self/io.
  void ReadProcIO();

  /// Read schedule info from /proc/self/sched.
  void ReadProcSched();

  /// Read status from /proc/self/status.
  void ReadProcStatus();

  /// Read the number of currently open file descriptors from /proc/self/fd.
  void ReadProcFileDescriptorCount();
};
}
