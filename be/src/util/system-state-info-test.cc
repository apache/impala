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
  EXPECT_EQ(0, r.user + r.system + r.iowait);
}

// Smoke test to make sure that we read non-zero values from /proc/stat.
TEST_F(SystemStateInfoTest, ReadProcStat) {
  info.ReadCurrentProcStat();
  const SystemStateInfo::CpuValues& state = info.cpu_values_[info.cur_val_idx_];
  EXPECT_GT(state[SystemStateInfo::CPU_USER], 0);
  EXPECT_GT(state[SystemStateInfo::CPU_SYSTEM], 0);
  EXPECT_GT(state[SystemStateInfo::CPU_IDLE], 0);
  EXPECT_GT(state[SystemStateInfo::CPU_IOWAIT], 0);
}

// Tests parsing a line similar to the first line of /proc/stat.
TEST_F(SystemStateInfoTest, ParseProcStat) {
  // Fields are: user nice system idle iowait irq softirq steal guest guest_nice
  info.ReadProcStatString("cpu  20 30 10 70 100 0 0 0 0 0");
  const SystemStateInfo::CpuValues& state = info.cpu_values_[info.cur_val_idx_];
  EXPECT_EQ(state[SystemStateInfo::CPU_USER], 20);
  EXPECT_EQ(state[SystemStateInfo::CPU_SYSTEM], 10);
  EXPECT_EQ(state[SystemStateInfo::CPU_IDLE], 70);
  EXPECT_EQ(state[SystemStateInfo::CPU_IOWAIT], 100);

  // Test that values larger than INT_MAX parse without error.
  info.ReadProcStatString("cpu  3000000000 30 10 70 100 0 0 0 0 0");
  const SystemStateInfo::CpuValues& changed_state = info.cpu_values_[info.cur_val_idx_];
  EXPECT_EQ(changed_state[SystemStateInfo::CPU_USER], 3000000000);
}

// Tests that computing CPU ratios doesn't overflow
TEST_F(SystemStateInfoTest, ComputeCpuRatiosIntOverflow) {
  // Load old and new values for CPU counters. These values are from a system where we
  // have seen this code overflow before.
  info.ReadProcStatString("cpu  100952877 534 18552749 6318822633 4119619 0 0 0 0 0");
  info.ReadProcStatString("cpu  100953598 534 18552882 6318826150 4119619 0 0 0 0 0");
  info.ComputeCpuRatios();
  const SystemStateInfo::CpuUsageRatios& r = info.GetCpuUsageRatios();
  EXPECT_EQ(r.user, 1649);
  EXPECT_EQ(r.system, 304);
  EXPECT_EQ(r.iowait, 0);
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
    EXPECT_GT(r.user + r.system + r.iowait, 0);
  }
  running.Store(false);
  t.join();
}

// Tests the computation logic.
TEST_F(SystemStateInfoTest, ComputeCpuRatios) {
  info.ReadProcStatString("cpu  20 30 10 70 100 0 0 0 0 0");
  info.ReadProcStatString("cpu  30 30 20 70 100 0 0 0 0 0");
  info.ComputeCpuRatios();
  const SystemStateInfo::CpuUsageRatios& r = info.GetCpuUsageRatios();
  EXPECT_EQ(r.user, 5000);
  EXPECT_EQ(r.system, 5000);
  EXPECT_EQ(r.iowait, 0);
}

} // namespace impala

