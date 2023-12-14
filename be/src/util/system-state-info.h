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

#include "common/atomic.h"
#include "common/names.h"
#include "common/logging.h"

class StringPiece;

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
    AtomicInt32 user;
    AtomicInt32 system;
    AtomicInt32 iowait;
  };

  /// Returns a struct containing the CPU usage ratios for the interval between the last
  /// two calls to CaptureSystemStateSnapshot().
  const CpuUsageRatios& GetCpuUsageRatios() { return cpu_ratios_; }

  /// Network usage rates in bytes per second.
  struct NetworkUsage {
    AtomicInt64 rx_rate;
    AtomicInt64 tx_rate;
  };

  /// Returns a struct containing the network usage for the interval between the last two
  /// calls to CaptureSystemStateSnapshot().
  const NetworkUsage& GetNetworkUsage() { return network_usage_; }

  /// Disk statistics rates in bytes per second
  struct DiskStats {
    AtomicInt64 read_rate;
    AtomicInt64 write_rate;
  };

  /// Returns a struct containing the disk throughput for the interval between the last
  /// two calls to CaptureSystemStateSnapshot().
  const DiskStats& GetDiskStats() { return disk_stats_; }

 private:
  /// Rotates 'cpu_val_idx_' and reads the current CPU usage values from /proc/stat into
  /// the current set of values.
  void ReadCurrentProcStat();

  /// Rotates 'cpu_val_idx_' and reads the CPU usage values from 'stat_string' into the
  /// current set of values.
  void ReadProcStatString(const string& stat_string);

  /// Computes the CPU usage ratios for the interval between the last two calls to
  /// CaptureSystemStateSnapshot() and stores the result in 'cpu_ratios_'.
  void ComputeCpuRatios();

  /// The enum names correspond to the fields of /proc/stat.
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
  /// Index into cpu_values_ that points to the current set of values. We maintain a
  /// separate index for CPU and network to be able to update them independently, e.g. in
  /// tests.
  int cpu_val_idx_ = 0;

  /// The computed CPU usage ratio between the current and previous snapshots in
  /// cpu_values_. Updated in ComputeCpuRatios().
  CpuUsageRatios cpu_ratios_;

  /// The enum names correspond to the fields of /proc/net/dev
  enum PROC_NET_DEV_VALUES {
    NET_RX_BYTES = 0,
    NET_RX_PACKETS,
    NET_RX_ERRS,
    NET_RX_DROP,
    NET_RX_FIFO,
    NET_RX_FRAME,
    NET_RX_COMPRESSED,
    NET_RX_MULTICAST,
    NET_TX_BYTES,
    NET_TX_PACKETS,
    NET_TX_ERRS,
    NET_TX_DROP,
    NET_TX_COLLS,
    NET_TX_CARRIER,
    NET_TX_COMPRESSED,
    NUM_NET_VALUES
  };

  /// We store these values in an array so that we can iterate over them, e.g. when
  /// reading them from a file or summing them up.
  typedef std::array<int64_t, NUM_NET_VALUES> NetworkValues;
  /// Two buffers to keep the current and previous set of network counter values.
  NetworkValues network_values_[2];
  /// Index into network_values_ that points to the current set of values. We maintain a
  /// separate index for network values to be able to update them independently, e.g. in
  /// tests.
  int net_val_idx_ = 0;

  /// Rotates net_val_idx_ and reads the current set of values from /proc/net/dev into
  /// network_values_.
  void ReadCurrentProcNetDev();

  /// Rotates net_val_idx_ and parses the content of 'dev_string' into network_values_.
  /// dev_string must be in the format of /proc/net/dev.
  void ReadProcNetDevString(const string& dev_string);

  /// Parses a single line as they appear in /proc/net/dev into 'result'. Entries are set
  /// to 0 for the local loopback interface and for invalid entries.
  void ReadProcNetDevLine(const StringPiece& dev_string, NetworkValues* result);

  /// Computes b = b + a.
  void AddNetworkValues(const NetworkValues& a, NetworkValues* b);

  /// Compute the network usage.
  void ComputeNetworkUsage(int64_t period_ms);

  /// The network usage between the current and previous snapshots in
  /// network_values_. Updated in ComputeNetworkUsage().
  NetworkUsage network_usage_;

  /// The last time of reading network usage values from /proc/net/dev. Used by
  /// CaptureSystemStateSnapshot().
  int64_t last_net_update_ms_;

  /// The enum names correspond to the fields of /proc/diskstats
  /// https://www.kernel.org/doc/Documentation/iostats.txt
  enum PROC_DISK_STAT_VALUES {
    DISK_READS_COMPLETED,
    DISK_READS_MERGED,
    DISK_SECTORS_READ,
    DISK_TIME_READING_MS,
    DISK_WRITES_COMPLETED,
    DISK_WRITES_MERGED,
    DISK_SECTORS_WRITTEN,
    DISK_TIME_WRITING_MS,
    DISK_IO_IN_PROGRESS,
    DISK_TIME_IN_IO_MS,
    DISK_WEIGHTED_TIME_IN_IO_MS,
    NUM_DISK_VALUES
  };

  /// Number of bytes per disk sector.
  static const int BYTES_PER_SECTOR = 512;

  /// We store these values in an array so that we can iterate over them, e.g. when
  /// reading them from a file or summing them up.
  typedef std::array<int64_t, NUM_DISK_VALUES> DiskValues;
  /// Two buffers to keep the current and previous set of disk counter values.
  DiskValues disk_values_[2];
  /// Index into disk_values_ that points to the current set of values. We maintain a
  /// separate index for disk values to be able to update them independently, e.g. in
  /// tests.
  int disk_val_idx_ = 0;

  /// Rotates disk_val_idx_ and reads the current set of values from /proc/diskstats into
  /// disk_values_.
  void ReadCurrentProcDiskStats();

  /// Rotates disk_val_idx_ and parses the content of 'disk_stats' into disk_values_.
  /// disk_stats must be in the format of /proc/diskstats.
  void ReadProcDiskStatsString(const string& disk_stats);

  /// Parses a single line as they appear in /proc/diskstats into 'result'. Invalid
  /// entries are set to 0.
  void ReadProcDiskStatsLine(const string& disk_stats, DiskValues* result);

  /// Computes b = b + a.
  void AddDiskValues(const DiskValues& a, DiskValues* b);

  /// Computes the read and write rate for the most recent period.
  void ComputeDiskStats(int64_t period_ms);

  /// The disk statistics between the current and previous snapshots in
  /// disk_values_. Updated in ComputeDiskStats().
  DiskStats disk_stats_;

  /// The last time of reading disk stats values from /proc/diskstats. Used by
  /// CaptureSystemStateSnapshot().
  int64_t last_disk_update_ms_;

  FRIEND_TEST(SystemStateInfoTest, ComputeCpuRatios);
  FRIEND_TEST(SystemStateInfoTest, ComputeCpuRatiosIntOverflow);
  FRIEND_TEST(SystemStateInfoTest, ComputeDiskStats);
  FRIEND_TEST(SystemStateInfoTest, ComputeNetworkUsage);
  FRIEND_TEST(SystemStateInfoTest, ParseProcDiskStatsString);
  FRIEND_TEST(SystemStateInfoTest, ParseProcNetDevString);
  FRIEND_TEST(SystemStateInfoTest, ParseProcNetDevStringCentos6);
  FRIEND_TEST(SystemStateInfoTest, ParseProcStat);
  FRIEND_TEST(SystemStateInfoTest, ReadProcDiskStats);
  FRIEND_TEST(SystemStateInfoTest, ReadProcNetDev);
  FRIEND_TEST(SystemStateInfoTest, ReadProcStat);
};

} // namespace impala
