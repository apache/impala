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

#include "testutil/in-process-servers.h"

#include <gtest/gtest.h>
#include "common/init.h"
#include "util/metrics.h"
#include "statestore/statestore-subscriber.h"

#include "common/names.h"

using namespace impala;

DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_client_ca_certificate);

DECLARE_int32(webserver_port);
DECLARE_int32(state_store_port);

namespace impala {

TEST(StatestoreTest, SmokeTest) {
  // All allocations done by 'new' to avoid problems shutting down Thrift servers
  // gracefully.

  InProcessStatestore* statestore =
      new InProcessStatestore(FLAGS_state_store_port, FLAGS_webserver_port);
  ASSERT_TRUE(statestore->Start().ok());

  InProcessStatestore* statestore_wont_start =
      new InProcessStatestore(FLAGS_state_store_port, FLAGS_webserver_port);
  ASSERT_FALSE(statestore_wont_start->Start().ok());

  StatestoreSubscriber* sub_will_start = new StatestoreSubscriber("sub1",
      MakeNetworkAddress("localhost", 12345),
      MakeNetworkAddress("localhost", FLAGS_state_store_port), new MetricGroup(""));
  ASSERT_TRUE(sub_will_start->Start().ok());

  // Confirm that a subscriber trying to use an in-use port will fail to start.
  StatestoreSubscriber* sub_will_not_start = new StatestoreSubscriber("sub2",
      MakeNetworkAddress("localhost", 12345),
      MakeNetworkAddress("localhost", FLAGS_state_store_port), new MetricGroup(""));
  ASSERT_FALSE(sub_will_not_start->Start().ok());

}

TEST(StatestoreSslTest, SmokeTest) {
  string impala_home(getenv("IMPALA_HOME"));
  stringstream server_cert;
  server_cert << impala_home << "/be/src/testutil/server-cert.pem";
  FLAGS_ssl_server_certificate = server_cert.str();
  FLAGS_ssl_client_ca_certificate = server_cert.str();
  stringstream server_key;
  server_key << impala_home << "/be/src/testutil/server-key.pem";
  FLAGS_ssl_private_key = server_key.str();

  // Change the ports, otherwise it will conflict with above test.
  FLAGS_state_store_port = 24001;
  FLAGS_webserver_port = 25001;

  InProcessStatestore* statestore =
      new InProcessStatestore(FLAGS_state_store_port, FLAGS_webserver_port);
  ASSERT_TRUE(statestore->Start().ok());

  StatestoreSubscriber* sub_will_start = new StatestoreSubscriber("sub1",
      MakeNetworkAddress("localhost", 23456),
      MakeNetworkAddress("localhost", FLAGS_state_store_port), new MetricGroup(""));
  ASSERT_TRUE(sub_will_start->Start().ok());

  stringstream invalid_server_cert;
  invalid_server_cert << impala_home << "/be/src/testutil/invalid-server-cert.pem";
  FLAGS_ssl_client_ca_certificate = invalid_server_cert.str();
  StatestoreSubscriber* sub_will_not_start = new StatestoreSubscriber("sub2",
      MakeNetworkAddress("localhost", 23457),
      MakeNetworkAddress("localhost", FLAGS_state_store_port), new MetricGroup(""));
  ASSERT_FALSE(sub_will_not_start->Start().ok());
}

}

int main(int argc, char **argv) {
  InitCommonRuntime(argc, argv, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
