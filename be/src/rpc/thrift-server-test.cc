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
#include "rpc/thrift-client.h"
#include "service/impala-server.h"
#include <string>
#include <gtest/gtest.h>
#include "common/init.h"
#include "service/fe-support.h"

using namespace impala;
using namespace std;

DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_client_ca_certificate);

DECLARE_int32(be_port);
DECLARE_int32(beeswax_port);

TEST(SslTest, Connectivity) {
  // Start a server using SSL and confirm that an SSL client can connect, while a non-SSL
  // client cannot.
  string impala_home(getenv("IMPALA_HOME"));
  stringstream server_cert;
  server_cert << impala_home << "/be/src/testutil/server-cert.pem";
  FLAGS_ssl_server_certificate = server_cert.str();
  FLAGS_ssl_client_ca_certificate = server_cert.str();
  stringstream server_key;
  server_key << impala_home << "/be/src/testutil/server-key.pem";
  FLAGS_ssl_private_key = server_key.str();

  // TODO: Revert to stack-allocated when IMPALA-618 is fixed.
  InProcessImpalaServer* impala =
      new InProcessImpalaServer("localhost", FLAGS_be_port, 0, 0, "", 0);
  EXIT_IF_ERROR(
      impala->StartWithClientServers(FLAGS_beeswax_port, FLAGS_beeswax_port + 1, false));

  ThriftClient<ImpalaServiceClient> ssl_client(
      "localhost", FLAGS_beeswax_port, "", NULL, true);
  EXPECT_TRUE(ssl_client.Open().ok());

  TPingImpalaServiceResp resp;
  EXPECT_NO_THROW({
    ssl_client.iface()->PingImpalaService(resp);
  });

  ThriftClient<ImpalaServiceClient> non_ssl_client(
      "localhost", FLAGS_beeswax_port, "", NULL, false);
  EXPECT_TRUE(non_ssl_client.Open().ok());
  EXPECT_THROW(non_ssl_client.iface()->PingImpalaService(resp), TTransportException);
}

int main(int argc, char** argv) {
  InitCommonRuntime(argc, argv, true);
  InitFeSupport();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
