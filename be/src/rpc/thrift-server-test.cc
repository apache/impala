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

#include <atomic>
#include <string>

#include "gen-cpp/StatestoreService.h"
#include "gutil/strings/substitute.h"
#include "rpc/authentication.h"
#include "rpc/thrift-client.h"
#include "service/fe-support.h"
#include "service/impala-server.h"
#include "testutil/gtest-util.h"
#include "testutil/mini-kdc-wrapper.h"
#include "testutil/scoped-flag-setter.h"

#include "common/names.h"

using namespace impala;
using namespace strings;
using namespace apache::thrift;
using apache::thrift::transport::SSLProtocol;

DECLARE_string(principal);
DECLARE_string(be_principal);
DECLARE_string(keytab_file);
DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);

DECLARE_int32(state_store_port);

DECLARE_int32(be_port);
DECLARE_int32(beeswax_port);

static string IMPALA_HOME(getenv("IMPALA_HOME"));
static const string& SERVER_CERT =
    Substitute("$0/be/src/testutil/server-cert.pem", IMPALA_HOME);
static const string& PRIVATE_KEY =
    Substitute("$0/be/src/testutil/server-key.pem", IMPALA_HOME);
static const string& BAD_SERVER_CERT =
    Substitute("$0/be/src/testutil/bad-cert.pem", IMPALA_HOME);
static const string& BAD_PRIVATE_KEY =
    Substitute("$0/be/src/testutil/bad-key.pem", IMPALA_HOME);
static const string& PASSWORD_PROTECTED_PRIVATE_KEY =
    Substitute("$0/be/src/testutil/server-key-password.pem", IMPALA_HOME);

// The principal name and the realm used for creating the mini-KDC.
static const string principal = "impala/localhost";
static const string realm = "KRBTEST.COM";
static string principal_kt_path;

// Only use TLSv1.0 compatible ciphers, as tests might run on machines with only TLSv1.0
// support.
static const string TLS1_0_COMPATIBLE_CIPHER = "AES128-SHA";
static const string TLS1_0_COMPATIBLE_CIPHER_2 = "AES256-SHA";

/// Dummy server class (chosen because it has the smallest interface to implement) that
/// tests can use to start Thrift servers.
class DummyStatestoreService : public StatestoreServiceIf {
 public:
  virtual void RegisterSubscriber(TRegisterSubscriberResponse& response,
      const TRegisterSubscriberRequest& request) {
  }
};

boost::shared_ptr<TProcessor> MakeProcessor() {
  boost::shared_ptr<DummyStatestoreService> service(new DummyStatestoreService());
  return boost::shared_ptr<TProcessor>(new StatestoreServiceProcessor(service));
}

int GetServerPort() {
  int port = FindUnusedEphemeralPort();
  EXPECT_FALSE(port == -1);
  return port;
}

template <class T> class ThriftTestBase : public T {
 protected:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

// The path of the current executable file that is required for passing into the SASL
// library as the 'application name'.
static string CURRENT_EXECUTABLE_PATH;

class ThriftKerberizedParamsTest :
    public ThriftTestBase<testing::TestWithParam<KerberosSwitch> > {

  virtual void SetUp() override {
    KerberosSwitch k = GetParam();
    if (k == KERBEROS_OFF) {
      FLAGS_principal.clear();
      FLAGS_be_principal.clear();
      FLAGS_keytab_file.clear();
    } else {
      FLAGS_principal = "dummy-service/host@realm";
      FLAGS_be_principal = strings::Substitute("$0@$1", principal, realm);
      FLAGS_keytab_file = principal_kt_path;
    }
    ASSERT_OK(InitAuth(CURRENT_EXECUTABLE_PATH));
    ThriftTestBase::SetUp();
  }

  virtual void TearDown() override {
    FLAGS_principal.clear();
    FLAGS_be_principal.clear();
    FLAGS_keytab_file.clear();
  }
};

INSTANTIATE_TEST_CASE_P(KerberosOnAndOff,
                        ThriftKerberizedParamsTest,
                        ::testing::Values(KERBEROS_OFF,
                                          KERBEROS_ON));

TEST(ThriftTestBase, Connectivity) {
  int port = GetServerPort();
  ThriftClient<StatestoreServiceClientWrapper> wrong_port_client(
      "localhost", port, "", nullptr, false);
  ASSERT_FALSE(wrong_port_client.Open().ok());

  ThriftServer* server;
  EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port).Build(&server));
  ASSERT_OK(server->Start());

  // Test that client recovers from failure to connect.
  ASSERT_OK(wrong_port_client.Open());
}

TEST_P(ThriftKerberizedParamsTest, SslConnectivity) {
  int port = GetServerPort();
  // Start a server using SSL and confirm that an SSL client can connect, while a non-SSL
  // client cannot.
  // Here and elsewhere - allocate ThriftServers on the heap to avoid race during
  // destruction. See IMPALA-2283.
  ThriftServer* server;
  EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
                .ssl(SERVER_CERT, PRIVATE_KEY)
                .Build(&server));
  ASSERT_OK(server->Start());

  FLAGS_ssl_client_ca_certificate = SERVER_CERT;
  ThriftClient<StatestoreServiceClientWrapper> ssl_client(
      "localhost", port, "", nullptr, true);
  ASSERT_OK(ssl_client.Open());
  TRegisterSubscriberResponse resp;
  bool send_done = false;
  EXPECT_NO_THROW({ssl_client.iface()->RegisterSubscriber(resp,
      TRegisterSubscriberRequest(), &send_done);
  });

  // Disable SSL for this client.
  ThriftClient<StatestoreServiceClientWrapper> non_ssl_client(
      "localhost", port, "", nullptr, false);

  if (GetParam() == KERBEROS_OFF) {
    // When Kerberos is OFF, Open() succeeds as there's no data transfer over the wire.
    ASSERT_OK(non_ssl_client.Open());
    send_done = false;
    // Verify that data transfer over the wire is not possible.
    EXPECT_THROW(non_ssl_client.iface()->RegisterSubscriber(
        resp, TRegisterSubscriberRequest(), &send_done), TTransportException);
  } else {
    // When Kerberos is ON, the SASL negotiation happens inside Open(). We expect that to
    // fail beacuse the server expects the client to negotiate over an encrypted
    // connection.
    // The expected error message can either state "No more data to read" or
    // "Couldn't open transport".
    const std::string& status = non_ssl_client.Open().GetDetail();
    size_t found_substr = status.find("No more data to read");
    if (found_substr == string::npos) {
      EXPECT_STR_CONTAINS(non_ssl_client.Open().GetDetail(),
          "Couldn't open transport");
    }
  }

}

TEST(SslTest, BadCertificate) {
  FLAGS_ssl_client_ca_certificate = "unknown";
  int port = GetServerPort();
  ThriftClient<StatestoreServiceClientWrapper> ssl_client(
      "localhost", port, "", nullptr, true);
  ASSERT_FALSE(ssl_client.Open().ok());

  ThriftServer* server;
  EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
                .ssl(SERVER_CERT, PRIVATE_KEY)
                .Build(&server));
  ASSERT_OK(server->Start());

  // Check that client does not recover from failure to create socket.
  ASSERT_FALSE(ssl_client.Open().ok());
}

TEST(PasswordProtectedPemFile, CorrectOperation) {
  // Require the server to execute a shell command to read the password to the private key
  // file.
  int port = GetServerPort();
  ThriftServer* server;
  EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
                .ssl(SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY)
                .pem_password_cmd("echo password")
                .Build(&server));
  ASSERT_OK(server->Start());

  auto s = ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, SERVER_CERT);
  ThriftClient<StatestoreServiceClientWrapper> ssl_client(
      "localhost", port, "", nullptr, true);
  ASSERT_OK(ssl_client.Open());
  TRegisterSubscriberResponse resp;
  bool send_done = false;
  EXPECT_NO_THROW({ssl_client.iface()->RegisterSubscriber(resp,
      TRegisterSubscriberRequest(), &send_done);});
}

TEST(PasswordProtectedPemFile, BadPassword) {
  // Test failure when password to private key is wrong.
  ThriftServer* server;
  EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), GetServerPort())
                .ssl(SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY)
                .pem_password_cmd("echo wrongpassword")
                .Build(&server));
  EXPECT_FALSE(server->Start().ok());
}

TEST(PasswordProtectedPemFile, BadCommand) {
  // Test failure when password command is badly formed.
  ThriftServer* server;

  // Keep clang-tdy happy - NOLINT (which here is due to deliberately leaked 'server')
  // does not get pushed into EXPECT_ERROR.
  Status s = ThriftServerBuilder("DummyStatestore", MakeProcessor(), GetServerPort()) // NOLINT
      .ssl(SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY)
      .pem_password_cmd("cmd-no-exist")
      .Build(&server);
  EXPECT_ERROR(s, TErrorCode::SSL_PASSWORD_CMD_FAILED);
}

TEST(SslTest, ClientBeforeServer) {
  // Instantiate a thrift client before a thrift server and test if it works (IMPALA-2747)
  auto s = ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, SERVER_CERT);
  int port = GetServerPort();
  ThriftClient<StatestoreServiceClientWrapper> ssl_client(
      "localhost", port, "", nullptr, true);

  ThriftServer* server;
  EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
                .ssl(SERVER_CERT, PRIVATE_KEY)
                .Build(&server));
  ASSERT_OK(server->Start());

  ASSERT_OK(ssl_client.Open());
  bool send_done = false;
  TRegisterSubscriberResponse resp;
  ssl_client.iface()->RegisterSubscriber(resp, TRegisterSubscriberRequest(), &send_done);
}

TEST(SslTest, BadCiphers) {
  int port = GetServerPort();
#ifndef __aarch64__
  {
    ThriftServer* server;
    EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
                  .ssl(SERVER_CERT, PRIVATE_KEY)
                  .cipher_list("this_is_not_a_cipher")
                  .Build(&server));
    EXPECT_FALSE(server->Start().ok());
  }
#endif
  {
    ThriftServer* server;
    EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
                  .ssl(SERVER_CERT, PRIVATE_KEY)
                  .Build(&server));
    EXPECT_OK(server->Start());
#ifndef __aarch64__
    auto s1 =
        ScopedFlagSetter<string>::Make(&FLAGS_ssl_cipher_list, "this_is_not_a_cipher");
    auto s2 =
        ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, SERVER_CERT);

    ThriftClient<StatestoreServiceClientWrapper> ssl_client(
        "localhost", port, "", nullptr, true);
    EXPECT_FALSE(ssl_client.Open().ok());
#endif
  }
}

TEST(SslTest, MismatchedCiphers) {
  int port = GetServerPort();
  FLAGS_ssl_client_ca_certificate = SERVER_CERT;

  ThriftServer* server;
  EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
                .ssl(SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY)
                .pem_password_cmd("echo password")
                .cipher_list(TLS1_0_COMPATIBLE_CIPHER)
                .Build(&server));
  EXPECT_OK(server->Start());
  auto s =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_cipher_list, TLS1_0_COMPATIBLE_CIPHER_2);
  ThriftClient<StatestoreServiceClientWrapper> ssl_client(
      "localhost", port, "", nullptr, true);

  // Failure to negotiate a cipher will show up when data is sent, not when socket is
  // opened.
  EXPECT_OK(ssl_client.Open());
#ifndef __aarch64__
  bool send_done = false;
  TRegisterSubscriberResponse resp;
  EXPECT_THROW(ssl_client.iface()->RegisterSubscriber(
                   resp, TRegisterSubscriberRequest(), &send_done),
      TTransportException);
#endif
}

// Test that StringToProtocol() correctly maps strings to their symbolic protocol
// equivalents.
TEST(SslTest, StringToProtocol) {
  SSLProtocol version;
  map<string, SSLProtocol> TEST_CASES = {
      {"tlsv1", TLSv1_0}, {"tlsv1.1", TLSv1_1}, {"tlsv1.2", TLSv1_2}};
  for (auto p : TEST_CASES) {
    EXPECT_OK(SSLProtoVersions::StringToProtocol(p.first, &version));
    EXPECT_EQ(p.second, version) << "TLS version: " << p.first;
  }
}

TEST(SslTest, TLSVersionControl) {
  auto flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, SERVER_CERT);

  // A config is really a pair (server_version, whitelist), where 'server_version' is the
  // server TLS version to test, and 'whitelist' is the set of client protocols that
  // should be able to connect successfully. This test tries all client protocols,
  // expecting those in the whitelist to succeed, and those that are not to fail.
  struct Config {
    SSLProtocol server_version;
    set<SSLProtocol> whitelist;
  };

  // All configurations supported by linked OpenSSL should work. We catch unsupported
  // protocols by checking IsSupported() for both the client and the server.
  vector<Config> configs = {
      {TLSv1_0, {TLSv1_0, TLSv1_1, TLSv1_2}},
      {TLSv1_1, {TLSv1_0, TLSv1_1, TLSv1_2}},
      {TLSv1_2, {TLSv1_0, TLSv1_1, TLSv1_2}}};

  for (const auto& config : configs) {
    // For each config, start a server with the requested protocol spec, and then try to
    // connect a client to it with every possible spec. This is an N^2 test, but the value
    // of N is 6.
    int port = GetServerPort();

    ThriftServer* server;
    EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
                  .ssl(SERVER_CERT, PRIVATE_KEY)
                  .ssl_version(config.server_version)
                  .Build(&server));
    if (!SSLProtoVersions::IsSupported(config.server_version)) {
      EXPECT_FALSE(server->Start().ok());
      continue;
    }
    ASSERT_OK(server->Start());

    for (auto client_version : SSLProtoVersions::PROTO_MAP) {
      auto s = ScopedFlagSetter<string>::Make(
          &FLAGS_ssl_minimum_version, client_version.first);
      ThriftClient<StatestoreServiceClientWrapper> ssl_client(
          "localhost", port, "", nullptr, true);
      if (!SSLProtoVersions::IsSupported(client_version.second)) {
        EXPECT_FALSE(ssl_client.Open().ok());
        continue;
      }
      EXPECT_OK(ssl_client.Open());
      bool send_done = false;
      TRegisterSubscriberResponse resp;
      if (config.whitelist.find(client_version.second) == config.whitelist.end()) {
        EXPECT_THROW(ssl_client.iface()->RegisterSubscriber(
                         resp, TRegisterSubscriberRequest(), &send_done),
            TTransportException)
            << "TLS version: " << config.server_version
            << ", client version: " << client_version.first;
      } else {
        EXPECT_NO_THROW({
          ssl_client.iface()->RegisterSubscriber(
              resp, TRegisterSubscriberRequest(), &send_done);
        }) << "TLS version: "
           << config.server_version << ", client version: " << client_version.first;
      }
    }
  }
}

TEST(SslTest, MatchedCiphers) {
  int port = GetServerPort();
  ThriftServer* server;
  EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
                .ssl(SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY)
                .pem_password_cmd("echo password")
                .cipher_list(TLS1_0_COMPATIBLE_CIPHER)
                .Build(&server));
  EXPECT_OK(server->Start());

  FLAGS_ssl_client_ca_certificate = SERVER_CERT;
  auto s =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_cipher_list, TLS1_0_COMPATIBLE_CIPHER);
  ThriftClient<StatestoreServiceClientWrapper> ssl_client(
      "localhost", port, "", nullptr, true);

  EXPECT_OK(ssl_client.Open());

  bool send_done = false;
  TRegisterSubscriberResponse resp;
  EXPECT_NO_THROW({
    ssl_client.iface()->RegisterSubscriber(
        resp, TRegisterSubscriberRequest(), &send_done);
  });
}

TEST(SslTest, OverlappingMatchedCiphers) {
  int port = GetServerPort();
  const string CIPHER_LIST = Substitute("$0,$1", TLS1_0_COMPATIBLE_CIPHER,
      TLS1_0_COMPATIBLE_CIPHER_2);
  ThriftServer* server;
  EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
      .ssl(SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY)
      .pem_password_cmd("echo password")
      .cipher_list(CIPHER_LIST)
      .Build(&server));
  EXPECT_OK(server->Start());

  FLAGS_ssl_client_ca_certificate = SERVER_CERT;
  auto s = ScopedFlagSetter<string>::Make(&FLAGS_ssl_cipher_list,
      Substitute("$0,not-a-cipher", TLS1_0_COMPATIBLE_CIPHER));
  ThriftClient<StatestoreServiceClientWrapper> ssl_client(
      "localhost", port, "", nullptr, true);

  EXPECT_OK(ssl_client.Open());

  bool send_done = false;
  TRegisterSubscriberResponse resp;
  EXPECT_NO_THROW({
        ssl_client.iface()->RegisterSubscriber(
            resp, TRegisterSubscriberRequest(), &send_done);
      });
}

TEST(ConcurrencyTest, MaxConcurrentConnections) {
  // Tests if max concurrent connections is being enforced by the ThriftServer
  // implementation. It creates a ThriftServer with max_concurrent_connections set to 2
  // and a ThreadPool of clients that attempt to connect concurrently and sleep for a
  // small amount of time. The test fails if the number of concurrently connected clients
  // exceeds the requested max_concurrent_connections limit. The test will also fail if
  // the number of concurrently connected clients never reaches the limit of
  // max_concurrent_connections.
  int port = GetServerPort();
  int max_connections = 2;
  ThriftServer* server;
  std::atomic<int> num_concurrent_connections{0};
  std::atomic<bool> did_reach_max{false};
  EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
      .max_concurrent_connections(max_connections)
      .Build(&server));
  EXPECT_OK(server->Start());

  ThreadPool<int> pool("ConcurrentTest", "MaxConcurrentConnections", 10, 10,
      [&num_concurrent_connections, &did_reach_max, max_connections, port](int tid,
            const int& item) {
        ThriftClient<StatestoreServiceClientWrapper> client("localhost", port, "",
            nullptr, false);
        EXPECT_OK(client.Open());
        bool send_done = false;
        TRegisterSubscriberResponse resp;
        EXPECT_NO_THROW({
            client.iface()->RegisterSubscriber(resp, TRegisterSubscriberRequest(),
                &send_done);
          });
        int connection_count = ++num_concurrent_connections;
        // Check that we have not exceeded the expected limit
        EXPECT_TRUE(connection_count <= max_connections);
        if (connection_count == max_connections) did_reach_max = true;
        SleepForMs(100);
        --num_concurrent_connections;
  });
  ASSERT_OK(pool.Init());

  for (int i = 0; i < 10; ++i) pool.Offer(i);
  pool.DrainAndShutdown();

  // If we did not reach the maximum number of concurrent connections, the test was not
  // effective.
  EXPECT_TRUE(did_reach_max);
}

/// Test disabled because requires a high ulimit -n on build machines. Since the test does
/// not always fail, we don't lose much coverage by disabling it until we fix the build
/// infra issue.
TEST(ConcurrencyTest, DISABLED_ManyConcurrentConnections) {
  // Test that a large number of concurrent connections will all succeed and not time out
  // waiting to be accepted. (IMPALA-4135)
  // Note that without the fix for IMPALA-4135, this test won't always fail, depending on
  // the hardware that it is run on.
  int port = GetServerPort();
  ThriftServer* server;
  EXPECT_OK(ThriftServerBuilder("DummyServer", MakeProcessor(), port).Build(&server));
  ASSERT_OK(server->Start());

  ThreadPool<int64_t> pool(
      "group", "test", 256, 10000, [port](int tid, const int64_t& item) {
        using Client = ThriftClient<ImpalaInternalServiceClient>;
        Client* client = new Client("127.0.0.1", port, "", nullptr, false);
        Status status = client->Open();
        ASSERT_OK(status);
      });
  ASSERT_OK(pool.Init());
  for (int i = 0; i < 1024 * 16; ++i) pool.Offer(i);
  pool.DrainAndShutdown();
}

TEST(NoPasswordPemFile, BadServerCertificate) {
  int port = GetServerPort();
  ThriftServer* server;
  EXPECT_OK(ThriftServerBuilder("DummyStatestore", MakeProcessor(), port)
                .ssl(BAD_SERVER_CERT, BAD_PRIVATE_KEY)
                .Build(&server));
  ASSERT_OK(server->Start());

  auto s = ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, SERVER_CERT);
  ThriftClient<StatestoreServiceClientWrapper> ssl_client(
      "localhost", port, "", nullptr, true);
  EXPECT_OK(ssl_client.Open());
  TRegisterSubscriberResponse resp;
  bool send_done = false;
  EXPECT_THROW({ssl_client.iface()->RegisterSubscriber(resp, TRegisterSubscriberRequest(),
      &send_done);
  }, TSSLException);
  // Close and reopen the socket
  ssl_client.Close();
  EXPECT_OK(ssl_client.Open());
  EXPECT_THROW({ssl_client.iface()->RegisterSubscriber(resp, TRegisterSubscriberRequest(),
      &send_done);
  }, TSSLException);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, false, impala::TestInfo::BE_TEST);

  int port = impala::FindUnusedEphemeralPort();
  std::unique_ptr<impala::MiniKdcWrapper> kdc;
  Status status =
      impala::MiniKdcWrapper::SetupAndStartMiniKDC(realm, "24h", "7d", port, &kdc);
  DCHECK(status.ok());

  // Create the service principal and keytab used for this test.
  status = kdc->CreateServiceKeytab(principal, &principal_kt_path);
  DCHECK(status.ok());

  // Fill in the path of the current binary for use by the tests.
  CURRENT_EXECUTABLE_PATH = argv[0];
  int retval = RUN_ALL_TESTS();

  status = kdc->TearDownMiniKDC();
  DCHECK(status.ok());
  return retval;
}
