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
#include "testutil/gtest-util.h"
#include "util/cgroup-util.h"
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

TEST(CGroupInfo, Basic) {
  int64_t mem_limit;
  ASSERT_OK(CGroupUtil::FindCGroupMemLimit(&mem_limit));
  EXPECT_GT(mem_limit, 0);
}

// Test error handling when cgroup is not present.
TEST(CGroupInfo, ErrorHandling) {
  string path;
  Status err = CGroupUtil::FindGlobalCGroup("fake-cgroup", &path);
  LOG(INFO) << err.msg().msg();
  EXPECT_FALSE(err.ok());
  err = CGroupUtil::FindAbsCGroupPath("fake-cgroup", &path);
  EXPECT_FALSE(err.ok());
  pair<string, string> p;
  err = CGroupUtil::FindCGroupMounts("fake-cgroup", &p);
  EXPECT_FALSE(err.ok());
}

TEST(ProcessStateInfo, Basic) {
  ProcessStateInfo process_state_info;
  ASSERT_GE(process_state_info.GetBytes("io/read_bytes"), 0);
  ASSERT_GE(process_state_info.GetInt("sched/prio"), 0);
  ASSERT_GE(process_state_info.GetInt("status/Threads"), 0);
  ASSERT_GT(process_state_info.GetInt("fd/count"), 0);
}

TEST(MappedMapInfo, Basic) {
  MappedMemInfo result = MemInfo::ParseSmaps();
  ASSERT_GT(result.num_maps, 0);
  ASSERT_GT(result.size_kb, 0);
  ASSERT_GT(result.rss_kb, 0);
  ASSERT_GE(result.anon_huge_pages_kb, 0);
}

}

