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

#include "scheduling/executor-group.h"

#include "common/logging.h"
#include "common/names.h"
#include "scheduling/cluster-membership-test-util.h"
#include "testutil/gtest-util.h"
#include "util/network-util.h"
#include "util/thread.h"

using namespace impala;
using namespace impala::test;

/// Test adding multiple backends on different hosts.
TEST(ExecutorGroupTest, AddExecutors) {
  ExecutorGroup group1("group1");
  ASSERT_EQ(0, group1.GetPerExecutorMemLimitForAdmission());
  int64_t mem_limit_admission1 = 100L * MEGABYTE;
  group1.AddExecutor(MakeBackendDescriptor(1, group1, /* port_offset=*/0,
      mem_limit_admission1));
  int64_t mem_limit_admission2 = 120L * MEGABYTE;
  group1.AddExecutor(MakeBackendDescriptor(2, group1, /* port_offset=*/0,
      mem_limit_admission2));
  ASSERT_EQ(mem_limit_admission1, group1.GetPerExecutorMemLimitForAdmission());
  ASSERT_EQ(2, group1.NumExecutors());
  IpAddr backend_ip;
  ASSERT_TRUE(group1.LookUpExecutorIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  ASSERT_TRUE(group1.LookUpExecutorIp("host_2", &backend_ip));
  EXPECT_EQ("10.0.0.2", backend_ip);
}

/// Test adding multiple backends on the same host.
TEST(ExecutorGroupTest, MultipleExecutorsOnSameHost) {
  ExecutorGroup group1("group1");
  int64_t mem_limit_admission1 = 120L * MEGABYTE;
  group1.AddExecutor(MakeBackendDescriptor(1, group1, /* port_offset=*/0,
      mem_limit_admission1));
  int64_t mem_limit_admission2 = 100L * MEGABYTE;
  group1.AddExecutor(MakeBackendDescriptor(1, group1, /* port_offset=*/1,
      mem_limit_admission2));
  ASSERT_EQ(mem_limit_admission2, group1.GetPerExecutorMemLimitForAdmission());
  IpAddr backend_ip;
  ASSERT_TRUE(group1.LookUpExecutorIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  const ExecutorGroup::Executors& backend_list = group1.GetExecutorsForHost("10.0.0.1");
  EXPECT_EQ(2, backend_list.size());
}

/// Test removing a backend.
TEST(ExecutorGroupTest, RemoveExecutor) {
  ExecutorGroup group1("group1");
  int64_t mem_limit_admission1 = 120L * MEGABYTE;
  const BackendDescriptorPB& executor1 = MakeBackendDescriptor(1, group1,
      /* port_offset=*/0, mem_limit_admission1);
  group1.AddExecutor(executor1);
  int64_t mem_limit_admission2 = 100L * MEGABYTE;
  const BackendDescriptorPB& executor2 = MakeBackendDescriptor(2, group1,
      /* port_offset=*/0, mem_limit_admission2);
  group1.AddExecutor(executor2);
  ASSERT_EQ(mem_limit_admission2, group1.GetPerExecutorMemLimitForAdmission());
  group1.RemoveExecutor(executor2);
  ASSERT_EQ(mem_limit_admission1, group1.GetPerExecutorMemLimitForAdmission());
  IpAddr backend_ip;
  ASSERT_TRUE(group1.LookUpExecutorIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  ASSERT_FALSE(group1.LookUpExecutorIp("host_2", &backend_ip));
}

/// Test removing one of multiple backends on the same host (IMPALA-3944).
TEST(ExecutorGroupTest, RemoveExecutorOnSameHost) {
  ExecutorGroup group1("group1");
  int64_t mem_limit_admission1 = 100L * MEGABYTE;
  const BackendDescriptorPB& executor1 = MakeBackendDescriptor(1, group1,
      /* port_offset=*/0, mem_limit_admission1);
  group1.AddExecutor(executor1);
  int64_t mem_limit_admission2 = 120L * MEGABYTE;
  const BackendDescriptorPB& executor2 = MakeBackendDescriptor(1, group1,
      /* port_offset=*/1, mem_limit_admission2);
  group1.AddExecutor(executor2);
  ASSERT_EQ(mem_limit_admission1, group1.GetPerExecutorMemLimitForAdmission());
  group1.RemoveExecutor(executor2);
  ASSERT_EQ(mem_limit_admission1, group1.GetPerExecutorMemLimitForAdmission());
  IpAddr backend_ip;
  ASSERT_TRUE(group1.LookUpExecutorIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  const ExecutorGroup::Executors& backend_list = group1.GetExecutorsForHost("10.0.0.1");
  EXPECT_EQ(1, backend_list.size());
  group1.RemoveExecutor(executor1);
  ASSERT_EQ(0, group1.GetPerExecutorMemLimitForAdmission());
}

/// Test that exercises the size-based group health check.
TEST(ExecutorGroupTest, HealthCheck) {
  ExecutorGroup group1("group1", 2);
  group1.AddExecutor(MakeBackendDescriptor(1, group1));
  ASSERT_FALSE(group1.IsHealthy());
  group1.AddExecutor(MakeBackendDescriptor(2, group1));
  ASSERT_TRUE(group1.IsHealthy());
  group1.RemoveExecutor(MakeBackendDescriptor(2, group1));
  ASSERT_FALSE(group1.IsHealthy());
}

/// Tests that adding an inconsistent backend to a group fails.
TEST(ExecutorGroupTest, TestAddInconsistent) {
  ExecutorGroup group1("group1", 2);
  group1.AddExecutor(MakeBackendDescriptor(1, group1));
  // Backend for a group with a matching name but mismatching size is not allowed.
  ExecutorGroup group_size_mismatch("group1", 3);
  group1.AddExecutor(MakeBackendDescriptor(3, group_size_mismatch));
  ASSERT_EQ(1, group1.NumExecutors());
  // Backend for a group with a mismatching name can be added (See AddExecutor() for
  // details).
  ExecutorGroup group_name_mismatch("group_name_mismatch", 2);
  group1.AddExecutor(MakeBackendDescriptor(2, group_name_mismatch));
  ASSERT_EQ(2, group1.NumExecutors());
}
