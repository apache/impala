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
  ExecutorGroup executor_group;
  executor_group.AddExecutor(MakeBackendDescriptor(1));
  executor_group.AddExecutor(MakeBackendDescriptor(2));
  ASSERT_EQ(2, executor_group.NumExecutors());
  IpAddr backend_ip;
  ASSERT_TRUE(executor_group.LookUpExecutorIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  ASSERT_TRUE(executor_group.LookUpExecutorIp("host_2", &backend_ip));
  EXPECT_EQ("10.0.0.2", backend_ip);
}

/// Test adding multiple backends on the same host.
TEST(ExecutorGroupTest, MultipleExecutorsOnSameHost) {
  ExecutorGroup executor_group;
  executor_group.AddExecutor(MakeBackendDescriptor(1, /* port_offset=*/0));
  executor_group.AddExecutor(MakeBackendDescriptor(1, /* port_offset=*/1));
  IpAddr backend_ip;
  ASSERT_TRUE(executor_group.LookUpExecutorIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  const ExecutorGroup::Executors& backend_list =
      executor_group.GetExecutorsForHost("10.0.0.1");
  EXPECT_EQ(2, backend_list.size());
}

/// Test removing a backend.
TEST(ExecutorGroupTest, RemoveExecutor) {
  ExecutorGroup executor_group;
  executor_group.AddExecutor(MakeBackendDescriptor(1));
  executor_group.AddExecutor(MakeBackendDescriptor(2));
  executor_group.RemoveExecutor(MakeBackendDescriptor(2));
  IpAddr backend_ip;
  ASSERT_TRUE(executor_group.LookUpExecutorIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  ASSERT_FALSE(executor_group.LookUpExecutorIp("host_2", &backend_ip));
}

/// Test removing one of multiple backends on the same host (IMPALA-3944).
TEST(ExecutorGroupTest, RemoveExecutorOnSameHost) {
  ExecutorGroup executor_group;
  executor_group.AddExecutor(MakeBackendDescriptor(1, /* port_offset=*/0));
  executor_group.AddExecutor(MakeBackendDescriptor(1, /* port_offset=*/1));
  executor_group.RemoveExecutor(MakeBackendDescriptor(1, /* port_offset=*/1));
  IpAddr backend_ip;
  ASSERT_TRUE(executor_group.LookUpExecutorIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  const ExecutorGroup::Executors& backend_list =
      executor_group.GetExecutorsForHost("10.0.0.1");
  EXPECT_EQ(1, backend_list.size());
}

IMPALA_TEST_MAIN();
