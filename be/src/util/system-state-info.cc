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

#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/util.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/logging.h"
#include "util/disk-info.h"
#include "util/system-state-info.h"
#include "util/time.h"

#include <numeric>
#include <fstream>

#include "common/names.h"

using std::accumulate;
using kudu::Env;
using kudu::faststring;
using kudu::ReadFileToString;
using strings::Split;
using strings::SkipWhitespace;

namespace {
/// Reads a file into a buffer and returns whether the read was successful. If not
/// successful, writes a warning into the log.
bool ReadFileOrWarn(const string& path, faststring* buf) {
  kudu::Status status = ReadFileToString(Env::Default(), path, buf);
  if (!status.ok()) {
    KLOG_EVERY_N_SECS(WARNING, 60) << "Could not open " << path << ": "
        << status.message() << endl;
    return false;
  }
  return true;
}

template <class T>
void ReadIntArray(const StringPiece& sp, int expected_num_values, T* out) {
  vector<StringPiece> counters = Split(sp, " ", SkipWhitespace());

  // Something is wrong with the number of values that we read
  if (expected_num_values > counters.size()) {
    memset(out, 0, sizeof(*out));
    KLOG_EVERY_N_SECS(WARNING, 60) << "Failed to parse enough values: expected "
        << expected_num_values << " but found " << counters.size() << ". Input was: "
        << sp;
    return;
  }

  for (int i = 0; i < expected_num_values; ++i) {
    int64_t* v = &(*out)[i];
    if (!safe_strto64(counters[i].as_string(), v)) {
      KLOG_EVERY_N_SECS(WARNING, 60) << "Failed to parse value: " << *v;
      *v = 0;
    }
    DCHECK_GE(*v, 0) << "Value " << i << ": " << *v;
  }
}
} // anonymous namespace

namespace impala {

// Partially initializing cpu_ratios_ will default-initialize the remaining members.
SystemStateInfo::SystemStateInfo() {
  memset(&cpu_ratios_, 0, sizeof(cpu_ratios_));
  memset(&network_usage_, 0, sizeof(network_usage_));
  memset(&disk_stats_, 0, sizeof(disk_stats_));
  ReadCurrentProcStat();
  ReadCurrentProcNetDev();
  last_net_update_ms_ = MonotonicMillis();
  ReadCurrentProcDiskStats();
  last_disk_update_ms_ = MonotonicMillis();
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

  now = MonotonicMillis();
  ReadCurrentProcDiskStats();
  DCHECK_GT(last_disk_update_ms_, 0L);
  ComputeDiskStats(now - last_disk_update_ms_);
  last_disk_update_ms_ = now;
}

void SystemStateInfo::ReadCurrentProcStat() {
  string path = "/proc/stat";
  faststring buf;
  if (!ReadFileOrWarn(path, &buf)) return;
  StringPiece buf_sp(reinterpret_cast<const char*>(buf.data()), buf.size());
  vector<StringPiece> lines = strings::Split(buf_sp, "\n");
  DCHECK_GT(lines.size(), 0);
  ReadProcStatString(lines[0].ToString());
}

void SystemStateInfo::ReadProcStatString(const string& stat_string) {
  cpu_val_idx_ = 1 - cpu_val_idx_;
  DCHECK(cpu_val_idx_ == 0 || cpu_val_idx_ == 1) << "cpu_val_idx_: " << cpu_val_idx_;
  CpuValues& next_values = cpu_values_[cpu_val_idx_];

  // Skip the first value ('cpu  ');
  StringPiece sp(stat_string);
  DCHECK(sp.starts_with("cpu  "));
  ReadIntArray(sp.substr(5), NUM_CPU_VALUES, &next_values);
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
  cpu_ratios_.user.Store(((cur[CPU_USER] - old[CPU_USER]) * BASIS_MAX) / total_tics);
  cpu_ratios_.system.Store(
      ((cur[CPU_SYSTEM] - old[CPU_SYSTEM]) * BASIS_MAX) / total_tics);
  cpu_ratios_.iowait.Store(
      ((cur[CPU_IOWAIT] - old[CPU_IOWAIT]) * BASIS_MAX) / total_tics);
}

void SystemStateInfo::ReadCurrentProcNetDev() {
  string path = "/proc/net/dev";
  faststring buf;
  if (!ReadFileOrWarn(path, &buf)) return;
  ReadProcNetDevString(buf.ToString());
}

void SystemStateInfo::ReadProcNetDevString(const string& dev_string) {
  net_val_idx_ = 1 - net_val_idx_;
  DCHECK(net_val_idx_ == 0 || net_val_idx_ == 1) << "net_val_idx_: " << net_val_idx_;
  NetworkValues& sum_values = network_values_[net_val_idx_];
  memset(&sum_values, 0, sizeof(sum_values));

  StringPiece sp(dev_string);
  vector<StringPiece> lines = Split(sp, "\n", SkipWhitespace());
  // The first two lines contain the header, skip them.
  DCHECK_GT(lines.size(), 2);
  for (int i = 2; i < lines.size(); ++i) {
    const StringPiece& line = lines[i];
    NetworkValues line_values;
    ReadProcNetDevLine(line, &line_values);
    AddNetworkValues(line_values, &sum_values);
  }
}

void SystemStateInfo::ReadProcNetDevLine(
    const StringPiece& dev_string, NetworkValues* result) {
  StringPiece sp(dev_string);
  // Lines can have leading whitespace
  StripWhiteSpace(&sp);

  // Initialize result to 0 to simplify error handling below
  memset(result, 0, sizeof(*result));

  // Don't include the loopback interface
  if (sp.starts_with("lo:")) return;

  int colon_idx = sp.find_first_of(':');
  if (colon_idx == StringPiece::npos) {
    KLOG_EVERY_N_SECS(WARNING, 60) << "Failed to parse interface name in line: "
        << sp.as_string();
    return;
  }

  ReadIntArray(sp.substr(colon_idx + 1), NUM_NET_VALUES, result);
}

void SystemStateInfo::AddNetworkValues(const NetworkValues& a, NetworkValues* b) {
  for (int i = 0; i < NUM_NET_VALUES; ++i) (*b)[i] += a[i];
}

void SystemStateInfo::ComputeNetworkUsage(int64_t period_ms) {
  if (period_ms <= 0) return;
  // Compute network throughput in bytes per second
  const NetworkValues& cur = network_values_[net_val_idx_];
  const NetworkValues& old = network_values_[1 - net_val_idx_];
  int64_t rx_bytes = cur[NET_RX_BYTES] - old[NET_RX_BYTES];
  network_usage_.rx_rate.Store(rx_bytes * 1000L / period_ms);

  int64_t tx_bytes = cur[NET_TX_BYTES] - old[NET_TX_BYTES];
  network_usage_.tx_rate.Store(tx_bytes * 1000L / period_ms);
}

void SystemStateInfo::ReadCurrentProcDiskStats() {
  string path = "/proc/diskstats";
  faststring buf;
  if (!ReadFileOrWarn(path, &buf)) return;
  ReadProcDiskStatsString(buf.ToString());
}

void SystemStateInfo::ReadProcDiskStatsString(const string& disk_stats) {
  stringstream ss(disk_stats);
  string line;

  disk_val_idx_ = 1 - disk_val_idx_;
  DCHECK(disk_val_idx_ == 0 || disk_val_idx_ == 1) << "disk_val_idx_: " << disk_val_idx_;
  DiskValues& sum_values = disk_values_[disk_val_idx_];
  memset(&sum_values, 0, sizeof(sum_values));

  while (getline(ss, line)) {
    DiskValues line_values;
    ReadProcDiskStatsLine(line, &line_values);
    AddDiskValues(line_values, &sum_values);
  }
}

void SystemStateInfo::ReadProcDiskStatsLine(
    const string& disk_stats, DiskValues* result) {
  stringstream ss(disk_stats);

  // Read the device IDs and name
  string major, minor, disk_name;
  ss >> major >> minor >> disk_name;

  if (!DiskInfo::is_known_disk(disk_name)) {
    memset(result, 0, sizeof(*result));
    return;
  }

  ReadIntArray(StringPiece(disk_stats).substr(ss.tellg()), NUM_DISK_VALUES, result);
}

void SystemStateInfo::AddDiskValues(const DiskValues& a, DiskValues* b) {
  for (int i = 0; i < NUM_DISK_VALUES; ++i) (*b)[i] += a[i];
}

void SystemStateInfo::ComputeDiskStats(int64_t period_ms) {
  if (period_ms <= 0) return;
  // Compute disk throughput in bytes per second
  const DiskValues& cur = disk_values_[disk_val_idx_];
  const DiskValues& old = disk_values_[1 - disk_val_idx_];
  int64_t read_sectors = cur[DISK_SECTORS_READ] - old[DISK_SECTORS_READ];
  disk_stats_.read_rate.Store(read_sectors * BYTES_PER_SECTOR * 1000 / period_ms);

  int64_t write_sectors = cur[DISK_SECTORS_WRITTEN] - old[DISK_SECTORS_WRITTEN];
  disk_stats_.write_rate.Store(write_sectors * BYTES_PER_SECTOR * 1000 / period_ms);
}

} // namespace impala
