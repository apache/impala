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

#include <string>
#include <boost/scoped_ptr.hpp>

#include "rpc/thrift-client.h"
#include "service/fe-support.h"
#include "service/impala-server.h"
#include "statestore/statestore.h"
#include "testutil/gtest-util.h"
#include "testutil/in-process-servers.h"
#include "util/asan.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/time.h"

#include "common/names.h"

using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift;
using namespace impala;

DECLARE_bool(abort_on_config_error);
DECLARE_int32(idle_session_timeout);
DECLARE_int32(beeswax_port);

// TODO: When sleep(..) queries can be cancelled, write a test that confirms long-running
// queries are cancelled during session expiry.
// TODO: Come up with a short-running test that confirms a session will keep itself alive
// that doesn't depend upon being rescheduled in a timely fashion.

// Object pool containing all objects that must live for the duration of the process.
// E.g. objects that are singletons and never destroyed in a real daemon (so don't support
// tear-down logic), but which we create multiple times in unit tests. We leak this pool
// instead of destroying it to avoid destroying the contained objects.
static ObjectPool* perm_objects;

namespace impala {

TEST(SessionTest, TestExpiry) {
  const int NUM_SESSIONS = 5;
  const int MAX_IDLE_TIMEOUT_MS = 4000;
  FLAGS_idle_session_timeout = 1;
  // Skip validation checks for in-process backend.
  FLAGS_abort_on_config_error = false;
  MetricGroup* metrics = perm_objects->Add(new MetricGroup("statestore"));
  Statestore* statestore = perm_objects->Add(new Statestore(metrics));
  IGNORE_LEAKING_OBJECT(statestore);
  // Pass in 0 to have the statestore use an ephemeral port for the service.
  ABORT_IF_ERROR(statestore->Init(0));
  InProcessImpalaServer* impala;
  ASSERT_OK(InProcessImpalaServer::StartWithEphemeralPorts(
      "localhost", statestore->port(), &impala));
  IntCounter* expired_metric =
      impala->metrics()->FindMetricForTesting<IntCounter>(
          ImpaladMetricKeys::NUM_SESSIONS_EXPIRED);
  ASSERT_TRUE(expired_metric != NULL);
  IntGauge* beeswax_session_metric =
      impala->metrics()->FindMetricForTesting<IntGauge>(
          ImpaladMetricKeys::IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS);
  IntGauge* hs2_session_metric =
      impala->metrics()->FindMetricForTesting<IntGauge>(
          ImpaladMetricKeys::IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS);
  EXPECT_EQ(expired_metric->GetValue(), 0L);
  EXPECT_EQ(beeswax_session_metric->GetValue(), 0L);

  {
    scoped_ptr<ThriftClient<ImpalaServiceClient>> beeswax_clients[NUM_SESSIONS];
    scoped_ptr<ThriftClient<ImpalaHiveServer2ServiceClient>> hs2_clients[NUM_SESSIONS];

    // Create five Beeswax clients and five HS2 clients (each HS2 gets one session each)
    for (int i = 0; i < NUM_SESSIONS; ++i) {
      beeswax_clients[i].reset(new ThriftClient<ImpalaServiceClient>(
              "localhost", impala->GetBeeswaxPort()));
      EXPECT_OK(beeswax_clients[i]->Open());

      hs2_clients[i].reset(new ThriftClient<ImpalaHiveServer2ServiceClient>(
              "localhost", impala->GetHS2Port()));
      EXPECT_OK(hs2_clients[i]->Open());
      TOpenSessionResp response;
      TOpenSessionReq request;
      hs2_clients[i]->iface()->OpenSession(response, request);
    }

    int64_t start = UnixMillis();
    while (expired_metric->GetValue() != NUM_SESSIONS * 2 &&
      UnixMillis() - start < MAX_IDLE_TIMEOUT_MS) {
      SleepForMs(100);
    }

    ASSERT_EQ(expired_metric->GetValue(), NUM_SESSIONS * 2)
        << "Sessions did not expire within "<< MAX_IDLE_TIMEOUT_MS / 1000 <<" secs";
    ASSERT_EQ(beeswax_session_metric->GetValue(), NUM_SESSIONS)
        << "Beeswax sessions unexpectedly closed after expiration";
    ASSERT_EQ(hs2_session_metric->GetValue(), NUM_SESSIONS)
        << "HiveServer2 sessions unexpectedly closed after expiration";

    TPingImpalaServiceResp resp;
    ASSERT_THROW({beeswax_clients[0]->iface()->PingImpalaService(resp);}, TException)
        << "Ping succeeded even after session expired";
  }
  // The TThreadedServer within 'impala' has no mechanism to join on its worker threads
  // (it looks like there's code that's meant to do this, but it doesn't appear to
  // work). Sleep to allow the threads closing the session to complete before tearing down
  // the server.
  SleepForMs(1000);
  statestore->ShutdownForTesting();
}

} // namespace impala

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  InitFeSupport();
  perm_objects = new ObjectPool;
  IGNORE_LEAKING_OBJECT(perm_objects);
  return RUN_ALL_TESTS();
}
