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

#include <gflags/gflags.h>
#include <gutil/strings/substitute.h>
#include <sstream>
#include <vector>

#include "service/impala-server.h"
#include "testutil/gtest-util.h"

using namespace impala;
using namespace std;
using namespace strings;

namespace impala {

using AuthorizedProxyMap =
  boost::unordered_map<std::string, boost::unordered_set<std::string>>;

class ImpalaServerTest : public testing::Test {
public:
  static Status PopulateAuthorizedProxyConfig(
      const string& authorized_proxy_config,
      const string& authorized_proxy_config_delimiter,
      AuthorizedProxyMap* authorized_proxy_map) {
    return ImpalaServer::PopulateAuthorizedProxyConfig(authorized_proxy_config,
        authorized_proxy_config_delimiter, authorized_proxy_map);
  }
};

}

TEST(ImpalaServerTest, PopulateAuthorizedProxyConfig) {
  vector<string> delimiters{",", "@", " "};
  for (auto& delimiter : delimiters) {
    AuthorizedProxyMap proxy_map;
    Status status = ImpalaServerTest::PopulateAuthorizedProxyConfig(
        Substitute("hue=user1$0user2;impala = user3 ;hive=* ", delimiter), delimiter,
        &proxy_map);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(3ul, proxy_map.size());

    auto proxies = proxy_map["hue"];
    EXPECT_EQ(2ul, proxies.size());
    EXPECT_EQ("user1", *proxies.find("user1"));
    EXPECT_EQ("user2", *proxies.find("user2"));

    proxies = proxy_map["impala"];
    EXPECT_EQ(1ul, proxies.size());
    EXPECT_EQ("user3", *proxies.find("user3"));

    proxies = proxy_map["hive"];
    EXPECT_EQ(1ul, proxies.size());
    EXPECT_EQ("*", *proxies.find("*"));

    EXPECT_EQ(proxy_map.end(), proxy_map.find("doesnotexist"));
  }
}

// ssl_minimum_version flag validation tests.
const auto VALID_TLS_VERSIONS = {"tlsv1", "tlsv1.1", "tlsv1.2"};
TEST(ImpalaServerTest, FlagSSLMinimumVersionValidValues) {
  for (const auto& tls_ver : VALID_TLS_VERSIONS) {
    gflags::FlagSaver s;
    EXPECT_EQ(Substitute("ssl_minimum_version set to $0\n", tls_ver),
        gflags::SetCommandLineOption("ssl_minimum_version", tls_ver));
  }
}

TEST(ImpalaServerTest, FlagSSLMinimumVersionInvalidValues) {
  {
    gflags::FlagSaver s;
    EXPECT_TRUE(gflags::SetCommandLineOption("ssl_minimum_version", "tlsv1.0").empty());
  }

  {
    gflags::FlagSaver s;
    EXPECT_TRUE(gflags::SetCommandLineOption("ssl_minimum_version", "TLSv1").empty());
  }

  {
    gflags::FlagSaver s;
    EXPECT_TRUE(gflags::SetCommandLineOption("ssl_minimum_version", "TLSv1.1").empty());
  }

  {
    gflags::FlagSaver s;
    EXPECT_TRUE(gflags::SetCommandLineOption("ssl_minimum_version", "TLSv1.2").empty());
  }

  {
    gflags::FlagSaver s;
    EXPECT_TRUE(gflags::SetCommandLineOption("ssl_minimum_version", "tlsv1.3").empty());
  }

  {
    gflags::FlagSaver s;
    EXPECT_TRUE(gflags::SetCommandLineOption("ssl_minimum_version", "TLSv1.3").empty());
  }

  {
    gflags::FlagSaver s;
    EXPECT_TRUE(gflags::SetCommandLineOption("ssl_minimum_version", " ").empty());
  }
}

TEST(ImpalaServerTest, FlagSSLMinimumVersionEmpty) {
  {
    // Flag can be empty if TLS is not configured.
    gflags::FlagSaver s;
    EXPECT_EQ("ssl_minimum_version set to \n",
        gflags::SetCommandLineOption("ssl_minimum_version", ""));
  }

  {
    // Flag cannot be empty if external TLS is configured.
    gflags::FlagSaver s;
    gflags::SetCommandLineOption("ssl_server_certificate", "some_cert.pem");
    gflags::SetCommandLineOption("ssl_private_key", "some_key.pem");
    EXPECT_TRUE(gflags::SetCommandLineOption("ssl_minimum_version", "").empty());
  }

  {
    // Flag cannot be empty if internal TLS is configured.
    gflags::FlagSaver s;
    gflags::SetCommandLineOption("ssl_client_ca_certificate", "some_ca_cert.pem");
    gflags::SetCommandLineOption("ssl_server_certificate", "some_cert.pem");
    gflags::SetCommandLineOption("ssl_private_key", "some_key.pem");
    EXPECT_TRUE(gflags::SetCommandLineOption("ssl_minimum_version", "").empty());
  }
}

TEST(ImpalaServerTest, FlagSSLMinimumVersionExternalTLSConfigured) {
  for (const auto& tls_ver : VALID_TLS_VERSIONS) {
    // Flag validation passes if external TLS is configured.
    gflags::FlagSaver s;
    gflags::SetCommandLineOption("ssl_server_certificate", "some_cert.pem");
    gflags::SetCommandLineOption("ssl_private_key", "some_key.pem");
    EXPECT_EQ(Substitute("ssl_minimum_version set to $0\n", tls_ver),
        gflags::SetCommandLineOption("ssl_minimum_version", tls_ver));
  }

  {
    // Flag validation fails if external TLS is configured and flag has an invalid value.
    gflags::FlagSaver s;
    gflags::SetCommandLineOption("ssl_server_certificate", "some_cert.pem");
    gflags::SetCommandLineOption("ssl_private_key", "some_key.pem");
    EXPECT_TRUE(gflags::SetCommandLineOption("ssl_minimum_version", "foo").empty());
  }
}

TEST(ImpalaServerTest, FlagSSLMinimumVersionInternalTLSConfigured) {
  for (const auto& tls_ver : VALID_TLS_VERSIONS) {
    // Flag validation passes if internal TLS is configured.
    gflags::FlagSaver s;
    gflags::SetCommandLineOption("ssl_server_certificate", "some_cert.pem");
    gflags::SetCommandLineOption("ssl_private_key", "some_key.pem");
    EXPECT_EQ(Substitute("ssl_minimum_version set to $0\n", tls_ver),
        gflags::SetCommandLineOption("ssl_minimum_version", tls_ver));
  }

  {
    // Flag validation fails if internal TLS is configured and flag has an invalid value.
    gflags::FlagSaver s;
    gflags::SetCommandLineOption("ssl_client_ca_certificate", "some_ca_cert.pem");
    gflags::SetCommandLineOption("ssl_server_certificate", "some_cert.pem");
    gflags::SetCommandLineOption("ssl_private_key", "some_key.pem");
    EXPECT_TRUE(gflags::SetCommandLineOption("ssl_minimum_version", "foo").empty());
  }
}