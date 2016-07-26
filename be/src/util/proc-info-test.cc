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

#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>

#include <gtest/gtest.h>

#include "common/init.h"
#include "service/fe-support.h"
#include "util/mem-info.h"
#include "util/process-state-info.h"
#include "util/test-info.h"

#include "common/names.h"

namespace impala {

TEST(MemInfo, Basic) {
  ASSERT_GT(MemInfo::physical_mem(), 0);
  ASSERT_LT(MemInfo::vm_overcommit(), 3);
  ASSERT_GE(MemInfo::vm_overcommit(), 0);
  ASSERT_GT(MemInfo::commit_limit(), 0);
}

TEST(ProcessStateInfo, Basic) {
  ProcessStateInfo process_state_info;
  ASSERT_GE(process_state_info.GetBytes("io/read_bytes"), 0);
  ASSERT_GE(process_state_info.GetInt("sched/prio"), 0);
  ASSERT_GE(process_state_info.GetInt("status/Threads"), 0);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  return RUN_ALL_TESTS();
}
