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

#include "gutil/strings/split.h"
#include "util/error-util.h"
#include "util/string-parser.h"
#include "util/system-state-info.h"

#include <algorithm>
#include <fstream>
#include <iostream>

#include "common/names.h"

using std::accumulate;

namespace impala {

// Partially initializing cpu_ratios_ will default-initialize the remaining members.
SystemStateInfo::SystemStateInfo() {
  memset(&cpu_ratios_, 0, sizeof(cpu_ratios_));
  ReadCurrentProcStat();
}

void SystemStateInfo::CaptureSystemStateSnapshot() {
  // Capture and compute CPU usage
  ReadCurrentProcStat();
  ComputeCpuRatios();
}

int64_t SystemStateInfo::ParseInt64(const string& val) const {
  StringParser::ParseResult result;
  int64_t parsed = StringParser::StringToInt<int64_t>(val.c_str(), val.size(), &result);
  if (result == StringParser::PARSE_SUCCESS) return parsed;
  return -1;
}

void SystemStateInfo::ReadFirstLineFromFile(const char* path, string* out) const {
  ifstream proc_file(path);
  if (!proc_file.is_open()) {
    LOG(WARNING) << "Could not open " << path << ": " << GetStrErrMsg() << endl;
    return;
  }
  DCHECK(proc_file.is_open());
  DCHECK(out != nullptr);
  getline(proc_file, *out);
}

void SystemStateInfo::ReadCurrentProcStat() {
  string line;
  ReadFirstLineFromFile("/proc/stat", &line);
  ReadProcStatString(line);
}

void SystemStateInfo::ReadProcStatString(const string& stat_string) {
  stringstream ss(stat_string);

  // Skip the first value ('cpu  ');
  ss.ignore(5);

  cur_val_idx_ = 1 - cur_val_idx_;
  CpuValues& next_values = cpu_values_[cur_val_idx_];

  for (int i = 0; i < NUM_CPU_VALUES; ++i) {
    int64_t v = -1;
    ss >> v;
    DCHECK_GE(v, 0) << "Value " << i << ": " << v;
    // Clamp invalid entries at 0
    next_values[i] = max(v, 0L);
  }
}

void SystemStateInfo::ComputeCpuRatios() {
  const CpuValues& cur = cpu_values_[cur_val_idx_];
  const CpuValues& old = cpu_values_[1 - cur_val_idx_];

  // Sum up all counters
  int64_t cur_sum = accumulate(cur.begin(), cur.end(), 0L);
  int64_t old_sum = accumulate(old.begin(), old.end(), 0L);

  int64_t total_tics = cur_sum - old_sum;
  // If less than 1/USER_HZ time has time has passed for any of the counters, the ratio is
  // zero (to avoid dividing by zero).
  if (total_tics == 0) {
    memset(&cpu_ratios_, 0, sizeof(cpu_ratios_));
    return;
  }
  DCHECK_GT(total_tics, 0);
  // Convert each ratio to basis points.
  const int BASIS_MAX = 10000;
  cpu_ratios_.user = ((cur[CPU_USER] - old[CPU_USER]) * BASIS_MAX) / total_tics;
  cpu_ratios_.system = ((cur[CPU_SYSTEM] - old[CPU_SYSTEM]) * BASIS_MAX) / total_tics;
  cpu_ratios_.iowait = ((cur[CPU_IOWAIT] - old[CPU_IOWAIT]) * BASIS_MAX) / total_tics;
}

} // namespace impala
