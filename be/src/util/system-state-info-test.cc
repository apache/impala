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

#include "common/atomic.h"
#include "testutil/gtest-util.h"
#include "util/disk-info.h"
#include "util/system-state-info.h"
#include "util/time.h"

#include <thread>

namespace impala {

class SystemStateInfoTest : public testing::Test {
 protected:
  SystemStateInfo info;
};

TEST_F(SystemStateInfoTest, FirstCallReturnsZero) {
  const SystemStateInfo::CpuUsageRatios& r = info.GetCpuUsageRatios();
  EXPECT_EQ(0, r.user.Load() + r.system.Load() + r.iowait.Load());

  const SystemStateInfo::NetworkUsage& n = info.GetNetworkUsage();
  EXPECT_EQ(0, n.rx_rate.Load() + n.tx_rate.Load());
}

// Smoke test to make sure that we read non-zero values from /proc/stat.
TEST_F(SystemStateInfoTest, ReadProcStat) {
  info.ReadCurrentProcStat();
  const SystemStateInfo::CpuValues& state = info.cpu_values_[info.cpu_val_idx_];
  EXPECT_GT(state[SystemStateInfo::CPU_USER], 0);
  EXPECT_GT(state[SystemStateInfo::CPU_SYSTEM], 0);
  EXPECT_GT(state[SystemStateInfo::CPU_IDLE], 0);
  EXPECT_GT(state[SystemStateInfo::CPU_IOWAIT], 0);
}

// Smoke test to make sure that we read non-zero values from /proc/net/dev.
TEST_F(SystemStateInfoTest, ReadProcNetDev) {
  info.ReadCurrentProcNetDev();
  const SystemStateInfo::NetworkValues& state = info.network_values_[info.net_val_idx_];
  // IMPALA-8413: Don't assume that counters are non-zero as that is sometimes not the
  // case, e.g. in virtualized environments.
  EXPECT_GE(state[SystemStateInfo::NET_RX_BYTES], 0);
  EXPECT_GE(state[SystemStateInfo::NET_RX_PACKETS], 0);
  EXPECT_GE(state[SystemStateInfo::NET_TX_BYTES], 0);
  EXPECT_GE(state[SystemStateInfo::NET_TX_PACKETS], 0);
}

// Smoke test to make sure that we read non-zero values from /proc/diskstats.
TEST_F(SystemStateInfoTest, ReadProcDiskStats) {
  info.ReadCurrentProcDiskStats();
  const SystemStateInfo::DiskValues& state = info.disk_values_[info.disk_val_idx_];
  EXPECT_GT(state[SystemStateInfo::DISK_READS_COMPLETED], 0);
  EXPECT_GT(state[SystemStateInfo::DISK_SECTORS_READ], 0);
  EXPECT_GT(state[SystemStateInfo::DISK_WRITES_COMPLETED], 0);
  EXPECT_GT(state[SystemStateInfo::DISK_SECTORS_WRITTEN], 0);
}

// Tests parsing a line similar to the first line of /proc/stat.
TEST_F(SystemStateInfoTest, ParseProcStat) {
  // Fields are: user nice system idle iowait irq softirq steal guest guest_nice
  info.ReadProcStatString("cpu  20 30 10 70 100 0 0 0 0 0");
  const SystemStateInfo::CpuValues& state = info.cpu_values_[info.cpu_val_idx_];
  EXPECT_EQ(state[SystemStateInfo::CPU_USER], 20);
  EXPECT_EQ(state[SystemStateInfo::CPU_SYSTEM], 10);
  EXPECT_EQ(state[SystemStateInfo::CPU_IDLE], 70);
  EXPECT_EQ(state[SystemStateInfo::CPU_IOWAIT], 100);

  // Test that values larger than INT_MAX parse without error.
  info.ReadProcStatString("cpu  3000000000 30 10 70 100 0 0 0 0 0");
  const SystemStateInfo::CpuValues& changed_state = info.cpu_values_[info.cpu_val_idx_];
  EXPECT_EQ(changed_state[SystemStateInfo::CPU_USER], 3000000000);
}

// Tests parsing a string similar to the contents of /proc/net/dev.
TEST_F(SystemStateInfoTest, ParseProcNetDevString) {
  // Fields are: user nice system idle iowait irq softirq steal guest guest_nice
  string dev_net = R"(Inter-|   Receive                                                |  Transmit
   face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
       lo: 12178099975318 24167151765    0    0    0     0          0         0 12178099975318 24167151765    0    0    0     0       0          0
       br-c4d9b4cafca2: 1025814     409    0    0    0     0          0         0 70616330 1638104    0    0    0     0       0          0
         eth0: 366039609986 388580561    0    2    0     0          0  62231368 95492744135 174535524    0    0    0     0       0          0)";
  info.ReadProcNetDevString(dev_net);
  const SystemStateInfo::NetworkValues& state = info.network_values_[info.net_val_idx_];
  EXPECT_EQ(state[SystemStateInfo::NET_RX_BYTES], 366040635800);
  EXPECT_EQ(state[SystemStateInfo::NET_RX_PACKETS], 388580970);
  EXPECT_EQ(state[SystemStateInfo::NET_TX_BYTES], 95563360465);
  EXPECT_EQ(state[SystemStateInfo::NET_TX_PACKETS], 176173628);
}

// Tests parsing a string similar to the contents of /proc/net/dev on older kernels, such
// as the one on Centos6.
TEST_F(SystemStateInfoTest, ParseProcNetDevStringCentos6) {
  // Fields are: user nice system idle iowait irq softirq steal guest guest_nice
  string dev_net = R"(Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:    5829      53    0    0    0     0          0         0     5829      53    0    0    0     0       0          0
  eth0:285025090  212208    0    0    0     0          0         0  9137793   84770    0    0    0     0       0          0)";
  info.ReadProcNetDevString(dev_net);
  const SystemStateInfo::NetworkValues& state = info.network_values_[info.net_val_idx_];
  EXPECT_EQ(state[SystemStateInfo::NET_RX_BYTES], 285025090);
  EXPECT_EQ(state[SystemStateInfo::NET_RX_PACKETS], 212208);
  EXPECT_EQ(state[SystemStateInfo::NET_TX_BYTES], 9137793);
  EXPECT_EQ(state[SystemStateInfo::NET_TX_PACKETS], 84770);
}

// Tests parsing a string similar to the contents of /proc/diskstats. This test also makes
// sure that values of a partition are not accounted towards the disk it is on.
TEST_F(SystemStateInfoTest, ParseProcDiskStatsString) {
  // We cannot hard-code the device name since our Jenkins workers might see different
  // device names in their virtual environments.
  string disk_stats =
    R"(   8       0 $0 17124835 222797 716029974 8414920 43758807 38432095 7867287712 630179264 0 32547524 638999340
       8       1 $01 17124482 222797 716027002 8414892 43546943 38432095 7867287712 629089180 0 31590972 637917344)";
  string device = DiskInfo::device_name(0);
  cout << Substitute(disk_stats, device);
  info.ReadProcDiskStatsString(Substitute(disk_stats, device));
  const SystemStateInfo::DiskValues& state = info.disk_values_[info.disk_val_idx_];
  EXPECT_EQ(state[SystemStateInfo::DISK_SECTORS_READ], 716029974);
  EXPECT_EQ(state[SystemStateInfo::DISK_READS_COMPLETED], 17124835);
  EXPECT_EQ(state[SystemStateInfo::DISK_SECTORS_WRITTEN], 7867287712);
  EXPECT_EQ(state[SystemStateInfo::DISK_WRITES_COMPLETED], 43758807);
}

// Tests that computing CPU ratios doesn't overflow
TEST_F(SystemStateInfoTest, ComputeCpuRatiosIntOverflow) {
  // Load old and new values for CPU counters. These values are from a system where we
  // have seen this code overflow before.
  info.ReadProcStatString("cpu  100952877 534 18552749 6318822633 4119619 0 0 0 0 0");
  info.ReadProcStatString("cpu  100953598 534 18552882 6318826150 4119619 0 0 0 0 0");
  info.ComputeCpuRatios();
  const SystemStateInfo::CpuUsageRatios& r = info.GetCpuUsageRatios();
  EXPECT_EQ(r.user.Load(), 1649);
  EXPECT_EQ(r.system.Load(), 304);
  EXPECT_EQ(r.iowait.Load(), 0);
}

// Smoke test for the public interface.
TEST_F(SystemStateInfoTest, GetCpuUsageRatios) {
  AtomicBool running(true);
  // Generate some load to observe counters > 0.
  std::thread t([&running]() { while (running.Load()); });
  for (int i = 0; i < 3; ++i) {
    SleepForMs(200);
    info.CaptureSystemStateSnapshot();
    const SystemStateInfo::CpuUsageRatios& r = info.GetCpuUsageRatios();
    EXPECT_GT(r.user.Load() + r.system.Load() + r.iowait.Load(), 0);
  }
  running.Store(false);
  t.join();
}

// Tests the computation logic for CPU ratios.
TEST_F(SystemStateInfoTest, ComputeCpuRatios) {
  info.ReadProcStatString("cpu  20 30 10 70 100 0 0 0 0 0");
  info.ReadProcStatString("cpu  30 30 20 70 100 0 0 0 0 0");
  info.ComputeCpuRatios();
  const SystemStateInfo::CpuUsageRatios& r = info.GetCpuUsageRatios();
  EXPECT_EQ(r.user.Load(), 5000);
  EXPECT_EQ(r.system.Load(), 5000);
  EXPECT_EQ(r.iowait.Load(), 0);
}

// Tests the computation logic for network usage.
TEST_F(SystemStateInfoTest, ComputeNetworkUsage) {
  // Two sets of values in the format of /proc/net/dev
  string dev_net_1 = R"(Inter-|   Receive                                                |  Transmit
   face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
       lo: 1000 2000    0    0    0     0          0         0 3000 4000    0    0    0     0       0          0
       br-c4d9b4cafca2: 5000     409    0    0    0     0          0         0 6000 7000    0    0    0     0       0          0
         eth0: 8000 9000    0    2    0     0          0  10000 11000 12000    0    0    0     0       0          0)";
  string dev_net_2 = R"(Inter-|   Receive                                                |  Transmit
   face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
       lo: 2000 4000    0    0    0     0          0         0 6000 8000    0    0    0     0       0          0
       br-c4d9b4cafca2: 7000     609    0    0    0     0          0         0 12000 14000    0    0    0     0       0          0
         eth0: 10000 11000    0    2    0     0          0  10000 11000 12000    0    0    0     0       0          0)";

  info.ReadProcNetDevString(dev_net_1);
  info.ReadProcNetDevString(dev_net_2);
  int period_ms = 500;
  info.ComputeNetworkUsage(period_ms);
  const SystemStateInfo::NetworkUsage& n = info.GetNetworkUsage();
  EXPECT_EQ(n.rx_rate.Load(), 8000);
  EXPECT_EQ(n.tx_rate.Load(), 12000);
}

// Tests the computation logic for disk statistics.
TEST_F(SystemStateInfoTest, ComputeDiskStats) {
  // Two sets of values in the format of /proc/diskstats. We cannot hard-code the device
  // name since our Jenkins workers might see different device names in their virtual
  // environments.
  string disk_stats_1 = R"(   1      15 ram15 0 0 0 0 0 0 0 0 0 0 0
     7       0 loop0 0 0 0 0 0 0 0 0 0 0 0
     8       0 $0 0 0 1000 0 0 0 2000 0 0 0 0
     8       1 $01 0 0 1000 0 0 0 2000 0 0 0 0)";
  string disk_stats_2 = R"(   8       0 $0 0 0 2000 0 0 0 4000 0 0 0 0
     8       1 $01 0 0 2000 0 0 0 4000 0 0 0 0)";

  string device = DiskInfo::device_name(0);
  info.ReadProcDiskStatsString(Substitute(disk_stats_1, device));
  info.ReadProcDiskStatsString(Substitute(disk_stats_2, device));
  int period_ms = 500;
  info.ComputeDiskStats(period_ms);
  const SystemStateInfo::DiskStats& ds = info.GetDiskStats();
  EXPECT_EQ(ds.read_rate.Load(), 2 * 1000 * SystemStateInfo::BYTES_PER_SECTOR);
  EXPECT_EQ(ds.write_rate.Load(), 2 * 2000 * SystemStateInfo::BYTES_PER_SECTOR);
}

} // namespace impala

