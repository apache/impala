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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include "testutil/cpu-util.h"
#include "testutil/gtest-util.h"
#include "util/disk-info.h"
#include "util/scope-exit-trigger.h"

#include "common/names.h"

namespace impala {

TEST(CpuInfoTest, Basic) {
  cout << CpuInfo::DebugString();
}

// Regression test for IMPALA-6500: make sure we map bad return values from
// sched_getcpu() into the expected range.
TEST(CpuInfoTest, InvalidSchedGetCpuValue) {
  cout << CpuInfo::DebugString();
  int real_max_num_cores = CpuInfo::GetMaxNumCores();
  const auto reset_state = MakeScopeExitTrigger([real_max_num_cores]() {
    CpuTestUtil::SetMaxNumCores(real_max_num_cores);
    CpuTestUtil::ResetAffinity();
  });

  // Pretend that we only have two cores. If this test is running on a system
  // with > 2 cores, sched_getcpu() will return out-of-range values..
  const int FAKE_NUM_CORES = 2;
  CpuTestUtil::SetMaxNumCores(FAKE_NUM_CORES);
  for (int core = 0; core < CpuInfo::num_cores(); ++core) {
    CpuTestUtil::PinToCore(core, false);
    int reported_core = CpuInfo::GetCurrentCore();
    // Check that the reported core is within the expected bounds.
    ASSERT_GE(reported_core, 0);
    ASSERT_LT(reported_core, FAKE_NUM_CORES);
  }

}

class DiskInfoTest : public ::testing::Test {
 protected:
  void TestTryNVMETrimPositive(const string& name, const string& expected_basename) {
    string nvme_basename;
    ASSERT_TRUE(DiskInfo::TryNVMETrim(name, &nvme_basename));
    ASSERT_EQ(nvme_basename, expected_basename);
  }

  void TestTryNVMETrimNegative(const string& name) {
    string nvme_basename;
    ASSERT_FALSE(DiskInfo::TryNVMETrim(name, &nvme_basename));
    ASSERT_EQ(nvme_basename, "");
  }
};

TEST_F(DiskInfoTest, Basic) {
  cout << DiskInfo::DebugString();
  cout << "Device name for disk 0: " << DiskInfo::device_name(0) << endl;

  int disk_id_home_dir = DiskInfo::disk_id("/home");
  if (disk_id_home_dir != -1) {
    cout << "Device name for '/home': "
        << DiskInfo::device_name(disk_id_home_dir) << endl;
  } else {
    // Often happens in Docker containers.
    cout << "No device name for /home";
  }
}

TEST_F(DiskInfoTest, NVMEDetection) {
  // Positive case 1: NVME device with a partition specified
  TestTryNVMETrimPositive("nvme0n1p2", "nvme0n1");

  // Positive case 2: NVME device without partition specified
  TestTryNVMETrimPositive("nvme0n1", "nvme0n1");

  // Positive case 3: NVME multi-digit device id, namespace id, partition id
  TestTryNVMETrimPositive("nvme15n13p24", "nvme15n13");

  // Negative case 1: disk drive
  TestTryNVMETrimNegative("sda3");
  TestTryNVMETrimNegative("sda");

  // Negative case 2: malformed NVME, missing namespace
  TestTryNVMETrimNegative("nvme0p1");
  TestTryNVMETrimNegative("nvme0");

  // Negative case 3: malformed NVME, missing device id
  TestTryNVMETrimNegative("nvmen1p3");
  TestTryNVMETrimNegative("nvmen1");

  // Negative case 4: extra garbage before / after
  TestTryNVMETrimNegative("sdanvme0n1");
  TestTryNVMETrimNegative("nvme0n1p0blah");
}

}

