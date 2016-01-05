// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <boost/scoped_ptr.hpp>
#include <string>
#include <gtest/gtest.h>

#include "rpc/thrift-client.h"
#include "service/impala-server.h"
#include "testutil/in-process-servers.h"
#include "common/init.h"
#include "service/fe-support.h"
#include "util/impalad-metrics.h"
#include "util/time.h"

#include "common/names.h"

using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift;
using namespace impala;

DECLARE_int32(idle_session_timeout);
DECLARE_int32(be_port);
DECLARE_int32(beeswax_port);

// TODO: When sleep(..) queries can be cancelled, write a test that confirms long-running
// queries are cancelled during session expiry.
// TODO: Come up with a short-running test that confirms a session will keep itself alive
// that doesn't depend upon being rescheduled in a timely fashion.

TEST(SessionTest, TestExpiry) {
  FLAGS_idle_session_timeout = 1;
  InProcessImpalaServer* impala = InProcessImpalaServer::StartWithEphemeralPorts();
  IntCounter* expired_metric =
      impala->metrics()->FindMetricForTesting<IntCounter>(
          ImpaladMetricKeys::NUM_SESSIONS_EXPIRED);
  DCHECK(expired_metric != NULL);
  IntGauge* beeswax_session_metric =
      impala->metrics()->FindMetricForTesting<IntGauge>(
          ImpaladMetricKeys::IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS);
  IntGauge* hs2_session_metric =
      impala->metrics()->FindMetricForTesting<IntGauge>(
          ImpaladMetricKeys::IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS);
  EXPECT_EQ(expired_metric->value(), 0L);
  EXPECT_EQ(beeswax_session_metric->value(), 0L);

  {
    scoped_ptr<ThriftClient<ImpalaServiceClient> > beeswax_clients[5];
    scoped_ptr<ThriftClient<ImpalaHiveServer2ServiceClient> > hs2_clients[5];

    // Create five Beeswax clients and five HS2 clients (each HS2 gets one session each)
    for (int i = 0; i < 5; ++i) {
      beeswax_clients[i].reset(new ThriftClient<ImpalaServiceClient>(
              "localhost", impala->beeswax_port()));
      EXPECT_TRUE(beeswax_clients[i]->Open().ok());

      hs2_clients[i].reset(new ThriftClient<ImpalaHiveServer2ServiceClient>(
              "localhost", impala->hs2_port()));
      EXPECT_TRUE(hs2_clients[i]->Open().ok());
      TOpenSessionResp response;
      TOpenSessionReq request;
      hs2_clients[i]->iface()->OpenSession(response, request);
    }

    int64_t start = UnixMillis();
    while (expired_metric->value() != 10 && UnixMillis() - start < 5000) {
      SleepForMs(100);
    }

    ASSERT_EQ(expired_metric->value(), 10L) << "Sessions did not expire within 5s";
    ASSERT_EQ(beeswax_session_metric->value(), 5L)
        << "Beeswax sessions unexpectedly closed after expiration";
    ASSERT_EQ(hs2_session_metric->value(), 5L)
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
}

int main(int argc, char** argv) {
  InitCommonRuntime(argc, argv, true);
  InitFeSupport();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
