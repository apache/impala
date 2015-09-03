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

#include "rpc/thrift-client.h"
#include "service/fe-support.h"
#include "service/impala-server.h"
#include <string>
#include <gtest/gtest.h>
#include "common/init.h"
#include "gen-cpp/StatestoreService.h"
#include "gutil/strings/substitute.h"

#include "common/names.h"

using namespace impala;
using namespace strings;
using namespace apache::thrift;

DECLARE_string(ssl_client_ca_certificate);

DECLARE_int32(state_store_port);

DECLARE_int32(be_port);
DECLARE_int32(beeswax_port);

string IMPALA_HOME(getenv("IMPALA_HOME"));
const string& SERVER_CERT =
    Substitute("$0/be/src/testutil/server-cert.pem", IMPALA_HOME);
const string& PRIVATE_KEY =
    Substitute("$0/be/src/testutil/server-key.pem", IMPALA_HOME);
const string& PASSWORD_PROTECTED_PRIVATE_KEY =
    Substitute("$0/be/src/testutil/server-key-password.pem", IMPALA_HOME);

/// Dummy server class (chosen because it has the smallest interface to implement) that
/// tests can use to start Thrift servers.
class DummyStatestoreService : public StatestoreServiceIf {
 public:
  virtual void RegisterSubscriber(TRegisterSubscriberResponse& response,
      const TRegisterSubscriberRequest& request) {
  }
};

shared_ptr<TProcessor> MakeProcessor() {
  shared_ptr<DummyStatestoreService> service(new DummyStatestoreService());
  return shared_ptr<TProcessor>(new StatestoreServiceProcessor(service));
}

TEST(SslTest, Connectivity) {
  // Start a server using SSL and confirm that an SSL client can connect, while a non-SSL
  // client cannot.
  // Here and elsewhere - allocate ThriftServers on the heap to avoid race during
  // destruction. See IMPALA-2283.
  ThriftServer* server = new ThriftServer("DummyStatestore", MakeProcessor(),
      FLAGS_state_store_port + 1, NULL, NULL, 5);
  EXPECT_TRUE(server->EnableSsl(SERVER_CERT, PRIVATE_KEY, "echo password").ok());
  EXPECT_TRUE(server->Start().ok());

  FLAGS_ssl_client_ca_certificate = SERVER_CERT;
  ThriftClient<StatestoreServiceClient> ssl_client(
      "localhost", FLAGS_state_store_port + 1, "", NULL, true);
  EXPECT_TRUE(ssl_client.Open().ok());
  TRegisterSubscriberResponse resp;
  EXPECT_NO_THROW({
    ssl_client.iface()->RegisterSubscriber(resp, TRegisterSubscriberRequest());
  });

  // Disable SSL for this client.
  ThriftClient<StatestoreServiceClient> non_ssl_client(
      "localhost", FLAGS_state_store_port + 1, "", NULL, false);
  EXPECT_TRUE(ssl_client.Open().ok());
  EXPECT_THROW(non_ssl_client.iface()->RegisterSubscriber(
      resp, TRegisterSubscriberRequest()), TTransportException);
}

TEST(PasswordProtectedPemFile, CorrectOperation) {
  // Require the server to execute a shell command to read the password to the private key
  // file.
  ThriftServer* server = new ThriftServer("DummyStatestore", MakeProcessor(),
      FLAGS_state_store_port + 4, NULL, NULL, 5);
  EXPECT_TRUE(server->EnableSsl(
      SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY, "echo password").ok());
  EXPECT_TRUE(server->Start().ok());
  FLAGS_ssl_client_ca_certificate = SERVER_CERT;
  ThriftClient<StatestoreServiceClient> ssl_client(
      "localhost", FLAGS_state_store_port + 4, "", NULL, true);
  EXPECT_TRUE(ssl_client.Open().ok());
  TRegisterSubscriberResponse resp;
  EXPECT_NO_THROW({
    ssl_client.iface()->RegisterSubscriber(resp, TRegisterSubscriberRequest());
  });
}

TEST(PasswordProtectedPemFile, BadPassword) {
  // Test failure when password to private key is wrong.
  ThriftServer server("DummyStatestore", MakeProcessor(),
      FLAGS_state_store_port + 2, NULL, NULL, 5);
  EXPECT_TRUE(server.EnableSsl(
      SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY, "echo wrongpassword").ok());
  EXPECT_FALSE(server.Start().ok());
}

TEST(PasswordProtectedPemFile, BadCommand) {
  // Test failure when password command is badly formed.
  ThriftServer server("DummyStatestore", MakeProcessor(),
      FLAGS_state_store_port + 3, NULL, NULL, 5);
  EXPECT_FALSE(server.EnableSsl(
      SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY, "cmd-no-exist").ok());
}

int main(int argc, char** argv) {
  InitCommonRuntime(argc, argv, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
