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

#ifndef IMPALA_TESTUTIL_CPU_UTIL_H_
#define IMPALA_TESTUTIL_CPU_UTIL_H_

#include <pthread.h>
#include <sched.h>

#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"
#include "util/cpu-info.h"

namespace impala {

class CpuTestUtil {
 public:
  /// Set the thread affinity so that it always runs on 'core'. Fail the test if
  /// unsuccessful. If 'validate_current_cpu' is true, checks that
  /// CpuInfo::GetCurrentCore() returns 'core'.
  static void PinToCore(int core, bool validate_current_cpu = true) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    ASSERT_EQ(0, pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset))
        << core;
    if (validate_current_cpu) {
      ASSERT_EQ(core, CpuInfo::GetCurrentCore());
    }
  }

  /// Reset the thread affinity of the current thread to all cores.
  static void ResetAffinity() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (int i = 0; i < CpuInfo::GetMaxNumCores(); ++i) {
      CPU_SET(i, &cpuset);
    }
    ASSERT_EQ(0, pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset));
  }

  /// Choose a random core in [0, CpuInfo::num_cores()) and pin the current thread to it.
  /// Uses 'rng' for randomness.
  static void PinToRandomCore(std::mt19937* rng) {
    int core = std::uniform_int_distribution<int>(0, CpuInfo::num_cores() - 1)(*rng);
    PinToCore(core);
  }

  /// Setup a fake NUMA setup where CpuInfo will report a NUMA configuration other than
  /// the system's actual configuration. If 'has_numa' is true, sets it up as three NUMA
  /// nodes with the cores distributed between them. Otherwise sets it up as a single
  /// NUMA node.
  static void SetupFakeNuma(bool has_numa) {
    std::vector<int> core_to_node(CpuInfo::GetMaxNumCores());
    int num_nodes = has_numa ? 3 : 1;
    for (int i = 0; i < core_to_node.size(); ++i) core_to_node[i] = i % num_nodes;
    CpuInfo::InitFakeNumaForTest(num_nodes, core_to_node);
    LOG(INFO) << CpuInfo::DebugString();
  }

  /// Set CpuInfo::max_num_cores_ to 'max_num_cores'. The caller is responsible for
  /// resetting it to its original value.
  static void SetMaxNumCores(int max_num_cores) {
    CpuInfo::max_num_cores_ = max_num_cores;
  }
};
}

#endif
