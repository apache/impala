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

#include <array>
#include <cstdint>
#include <string>

#include <gtest/gtest_prod.h> // for FRIEND_TEST

#include "common/names.h"
namespace impala {

/// Utility class to track and compute system resource consumption.
///
/// This class can be used to capture snapshots of various metrics of system resource
/// consumption (e.g. CPU usage) and compute usage ratios and derived metrics between
/// subsequent snapshots. Users of this class must call CaptureSystemStateSnapshot() and
/// can then obtain various resource utilization metrics through getter methods (e.g.
/// GetCpuUsageRatios()).
class SystemStateInfo {
 public:
  SystemStateInfo();
  /// Takes a snapshot of the current system resource usage and compute the usage ratios
  /// for the time since the previous snapshot was taken.
  void CaptureSystemStateSnapshot();

  /// Ratios use basis points as their unit (1/100th of a percent, i.e. 0.0001).
  struct CpuUsageRatios {
    int32_t user;
    int32_t system;
    int32_t iowait;
  };
  /// Returns a struct containing the CPU usage ratios for the interval between the last
  /// two calls to CaptureSystemStateSnapshot().
  const CpuUsageRatios& GetCpuUsageRatios() { return cpu_ratios_; }

 private:
  int64_t ParseInt64(const std::string& val) const;
  void ReadFirstLineFromFile(const char* path, std::string* out) const;

  /// Rotates 'cur_val_idx_' and reads the current CPU usage values from /proc/stat into
  /// the current set of values.
  void ReadCurrentProcStat();

  /// Rotates 'cur_val_idx_' and reads the CPU usage values from 'stat_string' into the
  /// current set of values.
  void ReadProcStatString(const string& stat_string);

  /// Computes the CPU usage ratios for the interval between the last two calls to
  /// CaptureSystemStateSnapshot() and stores the result in 'cpu_ratios_'.
  void ComputeCpuRatios();

  enum PROC_STAT_CPU_VALUES {
    CPU_USER = 0,
    CPU_NICE,
    CPU_SYSTEM,
    CPU_IDLE,
    CPU_IOWAIT,
    NUM_CPU_VALUES
  };

  /// We store the CPU usage values in an array so that we can iterate over them, e.g.
  /// when reading them from a file or summing them up.
  typedef std::array<int64_t, NUM_CPU_VALUES> CpuValues;
  /// Two buffers to keep the current and previous set of CPU usage values.
  CpuValues cpu_values_[2];
  int cur_val_idx_ = 0;

  /// The computed CPU usage ratio between the current and previous snapshots in
  /// cpu_values_. Updated in ComputeCpuRatios().
  CpuUsageRatios cpu_ratios_;

  FRIEND_TEST(SystemStateInfoTest, ComputeCpuRatios);
  FRIEND_TEST(SystemStateInfoTest, ComputeCpuRatiosIntOverflow);
  FRIEND_TEST(SystemStateInfoTest, ParseProcStat);
  FRIEND_TEST(SystemStateInfoTest, ReadProcStat);
};

} // namespace impala
