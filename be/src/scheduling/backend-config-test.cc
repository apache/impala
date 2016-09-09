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

#include "scheduling/backend-config.h"

#include "common/logging.h"
#include "common/names.h"
#include "testutil/gtest-util.h"
#include "util/network-util.h"
#include "util/thread.h"

namespace impala {

/// Test that BackendConfig can be created from a vector of backends.
TEST(BackendConfigTest, MakeFromBackendVector) {
  // This address needs to be resolvable using getaddrinfo().
  vector<TNetworkAddress> backends {MakeNetworkAddress("localhost", 1001)};
  BackendConfig backend_config(backends);
  IpAddr backend_ip;
  bool ret = backend_config.LookUpBackendIp(backends[0].hostname, &backend_ip);
  ASSERT_TRUE(ret);
  EXPECT_EQ("127.0.0.1", backend_ip);
}

/// Test adding multiple backends on different hosts.
TEST(BackendConfigTest, AddBackends) {
  BackendConfig backend_config;
  backend_config.AddBackend(MakeBackendDescriptor("host_1", "10.0.0.1", 1001));
  backend_config.AddBackend(MakeBackendDescriptor("host_2", "10.0.0.2", 1002));
  ASSERT_EQ(2, backend_config.NumBackends());
  IpAddr backend_ip;
  ASSERT_TRUE(backend_config.LookUpBackendIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  ASSERT_TRUE(backend_config.LookUpBackendIp("host_2", &backend_ip));
  EXPECT_EQ("10.0.0.2", backend_ip);
}

/// Test adding multiple backends on the same host.
TEST(BackendConfigTest, MultipleBackendsOnSameHost) {
  BackendConfig backend_config;
  backend_config.AddBackend(MakeBackendDescriptor("host_1", "10.0.0.1", 1001));
  backend_config.AddBackend(MakeBackendDescriptor("host_1", "10.0.0.1", 1002));
  IpAddr backend_ip;
  ASSERT_TRUE(backend_config.LookUpBackendIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  const BackendConfig::BackendList& backend_list =
      backend_config.GetBackendListForHost("10.0.0.1");
  EXPECT_EQ(2, backend_list.size());
}

/// Test removing a backend.
TEST(BackendConfigTest, RemoveBackend) {
  BackendConfig backend_config;
  backend_config.AddBackend(MakeBackendDescriptor("host_1", "10.0.0.1", 1001));
  backend_config.AddBackend(MakeBackendDescriptor("host_2", "10.0.0.2", 1002));
  backend_config.RemoveBackend(MakeBackendDescriptor("host_2", "10.0.0.2", 1002));
  IpAddr backend_ip;
  ASSERT_TRUE(backend_config.LookUpBackendIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  ASSERT_FALSE(backend_config.LookUpBackendIp("host_2", &backend_ip));
}

/// Test removing one of multiple backends on the same host (IMPALA-3944).
TEST(BackendConfigTest, RemoveBackendOnSameHost) {
  BackendConfig backend_config;
  backend_config.AddBackend(MakeBackendDescriptor("host_1", "10.0.0.1", 1001));
  backend_config.AddBackend(MakeBackendDescriptor("host_1", "10.0.0.1", 1002));
  backend_config.RemoveBackend(MakeBackendDescriptor("host_1", "10.0.0.1", 1002));
  IpAddr backend_ip;
  ASSERT_TRUE(backend_config.LookUpBackendIp("host_1", &backend_ip));
  EXPECT_EQ("10.0.0.1", backend_ip);
  const BackendConfig::BackendList& backend_list =
      backend_config.GetBackendListForHost("10.0.0.1");
  EXPECT_EQ(1, backend_list.size());
}

}  // end namespace impala

IMPALA_TEST_MAIN();
