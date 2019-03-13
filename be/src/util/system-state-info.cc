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
#include "gutil/strings/util.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "util/error-util.h"
#include "util/string-parser.h"
#include "util/system-state-info.h"
#include "util/time.h"

#include <algorithm>
#include <fstream>
#include <iostream>

#include "common/names.h"

using std::accumulate;
using kudu::Env;
using kudu::faststring;
using kudu::ReadFileToString;
using strings::Split;

namespace impala {

// Partially initializing cpu_ratios_ will default-initialize the remaining members.
SystemStateInfo::SystemStateInfo() {
  memset(&cpu_ratios_, 0, sizeof(cpu_ratios_));
  memset(&network_usage_, 0, sizeof(network_usage_));
  ReadCurrentProcStat();
  ReadCurrentProcNetDev();
  last_net_update_ms_ = MonotonicMillis();
}

void SystemStateInfo::CaptureSystemStateSnapshot() {
  // Capture and compute CPU usage
  ReadCurrentProcStat();
  ComputeCpuRatios();
  int64_t now = MonotonicMillis();
  ReadCurrentProcNetDev();
  DCHECK_GT(last_net_update_ms_, 0L);
  ComputeNetworkUsage(now - last_net_update_ms_);
  last_net_update_ms_ = now;
}

int64_t SystemStateInfo::ParseInt64(const string& val) const {
  StringParser::ParseResult result;
  int64_t parsed = StringParser::StringToInt<int64_t>(val.c_str(), val.size(), &result);
  if (result == StringParser::PARSE_SUCCESS) return parsed;
  return -1;
}

void SystemStateInfo::ReadCurrentProcStat() {
  string path = "/proc/stat";
  faststring buf;
  kudu::Status status = ReadFileToString(Env::Default(), path, &buf);
  if (!status.ok()) {
    LOG(WARNING) << "Could not open " << path << ": " << status.message() << endl;
    return;
  }
  StringPiece buf_sp(reinterpret_cast<const char*>(buf.data()), buf.size());
  vector<StringPiece> lines = strings::Split(buf_sp, "\n");
  DCHECK_GT(lines.size(), 0);
  ReadProcStatString(lines[0].ToString());
}

void SystemStateInfo::ReadProcStatString(const string& stat_string) {
  stringstream ss(stat_string);

  // Skip the first value ('cpu  ');
  ss.ignore(5);

  cpu_val_idx_ = 1 - cpu_val_idx_;
  CpuValues& next_values = cpu_values_[cpu_val_idx_];

  for (int i = 0; i < NUM_CPU_VALUES; ++i) {
    int64_t v = -1;
    ss >> v;
    DCHECK_GE(v, 0) << "Value " << i << ": " << v;
    // Clamp invalid entries at 0
    next_values[i] = max(v, 0L);
  }
}

void SystemStateInfo::ComputeCpuRatios() {
  const CpuValues& cur = cpu_values_[cpu_val_idx_];
  const CpuValues& old = cpu_values_[1 - cpu_val_idx_];

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

void SystemStateInfo::ReadCurrentProcNetDev() {
  string path = "/proc/net/dev";
  faststring buf;
  kudu::Status status = ReadFileToString(Env::Default(), path, &buf);
  if (!status.ok()) {
    LOG(WARNING) << "Could not open " << path << ": " << status.message() << endl;
    return;
  }
  ReadProcNetDevString(buf.ToString());
}

void SystemStateInfo::ReadProcNetDevString(const string& dev_string) {
  stringstream ss(dev_string);
  string line;

  // Discard the first two lines that contain the header
  getline(ss, line);
  getline(ss, line);

  net_val_idx_ = 1 - net_val_idx_;
  NetworkValues& sum_values = network_values_[net_val_idx_];
  memset(&sum_values, 0, sizeof(sum_values));

  while (getline(ss, line)) {
    NetworkValues line_values;
    ReadProcNetDevLine(line, &line_values);
    AddNetworkValues(line_values, &sum_values);
  }
}

void SystemStateInfo::ReadProcNetDevLine(
    const string& dev_string, NetworkValues* result) {
  stringstream ss(dev_string);

  // Read the interface name
  string if_name;
  ss >> if_name;

  // Don't include the loopback interface
  if (HasPrefixString(if_name, "lo:")) {
    memset(result, 0, sizeof(*result));
    return;
  }

  for (int i = 0; i < NUM_NET_VALUES; ++i) {
    int64_t v = -1;
    ss >> v;
    DCHECK_GE(v, 0) << "Value " << i << ": " << v;
    // Clamp invalid entries at 0
    (*result)[i] = max(v, 0L);
  }
}

void SystemStateInfo::AddNetworkValues(const NetworkValues& a, NetworkValues* b) {
  for (int i = 0; i < NUM_NET_VALUES; ++i) (*b)[i] += a[i];
}

void SystemStateInfo::ComputeNetworkUsage(int64_t period_ms) {
  if (period_ms == 0) return;
  // Compute network throughput in bytes per second
  const NetworkValues& cur = network_values_[net_val_idx_];
  const NetworkValues& old = network_values_[1 - net_val_idx_];
  int64_t rx_bytes = cur[NET_RX_BYTES] - old[NET_RX_BYTES];
  network_usage_.rx_rate = rx_bytes * 1000L / period_ms;

  int64_t tx_bytes = cur[NET_TX_BYTES] - old[NET_TX_BYTES];
  network_usage_.tx_rate = tx_bytes * 1000L / period_ms;
}

} // namespace impala
