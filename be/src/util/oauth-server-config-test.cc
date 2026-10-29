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
#include <gtest/gtest.h>

#include "gutil/strings/substitute.h"
#include "testutil/gtest-util.h"
#include "util/oauth-server-config.h"

#include "common/names.h"

DECLARE_string(oauth_servers);
DECLARE_string(jwks_file_path);
DECLARE_string(jwks_url);
DECLARE_bool(jwt_token_auth);
DECLARE_string(jwt_custom_claim_username);
DECLARE_string(oauth_jwks_file_path);
DECLARE_bool(oauth_token_auth);
DECLARE_string(oauth_jwt_custom_claim_username);

namespace impala {

TEST(OAuthServerConfigTest, ParseOAuthServersJson) {
  google::FlagSaver flag_saver;
  vector<OAuthServerConfig> configs;
  const string json =
      R"([
        {
          "caCertFilePath": "/opt/ca-custom/ca.pem",
          "verifyServerCert": true,
          "jwksUrl": "https://example.com/jwks.json",
          "jwksPullTimeoutSecs": 15,
          "jwksUpdateFrequencySecs": 14400,
          "usernameClaim": "preferred_username",
          "audienceClaims": ["impala-service"],
          "issuerClaims": ["auth0"]
        },
        {
          "jwksFilePath": "/opt/auth-servers/jwks.json"
        }
      ])";
  FLAGS_oauth_servers = json;
  ASSERT_OK(BuildOAuthServerConfigs(&configs));
  ASSERT_EQ(2, configs.size());
  EXPECT_EQ("/opt/ca-custom/ca.pem", configs[0].ca_cert_file_path);
  EXPECT_TRUE(configs[0].verify_server_cert);
  EXPECT_EQ("https://example.com/jwks.json", configs[0].jwks_uri);
  EXPECT_FALSE(configs[0].is_local_jwks);
  EXPECT_EQ(15, configs[0].jwks_pull_timeout_secs);
  EXPECT_EQ(14400, configs[0].jwks_update_frequency_secs);
  EXPECT_EQ("preferred_username", configs[0].username_claim);
  ASSERT_EQ(1, configs[0].audience_claims.size());
  EXPECT_EQ("impala-service", configs[0].audience_claims[0]);
  ASSERT_EQ(1, configs[0].issuer_claims.size());
  EXPECT_EQ("auth0", configs[0].issuer_claims[0]);
  EXPECT_EQ("/opt/auth-servers/jwks.json", configs[1].jwks_uri);
  EXPECT_TRUE(configs[1].is_local_jwks);
  EXPECT_EQ("username", configs[1].username_claim);
  EXPECT_TRUE(configs[1].audience_claims.empty());
  EXPECT_TRUE(configs[1].issuer_claims.empty());
}

TEST(OAuthServerConfigTest, GetJwksUriPrefersFilePath) {
  google::FlagSaver flag_saver;
  FLAGS_jwt_token_auth = true;
  FLAGS_jwks_file_path = "/tmp/jwks.json";
  FLAGS_jwks_url = "https://example.com/jwks.json";
  vector<OAuthServerConfig> configs;
  ASSERT_OK(BuildOAuthServerConfigs(&configs));
  ASSERT_EQ(1, configs.size());
  EXPECT_EQ("/tmp/jwks.json", configs[0].jwks_uri);
  EXPECT_TRUE(configs[0].is_local_jwks);
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonRejectsInvalidJson) {
  google::FlagSaver flag_saver;
  vector<OAuthServerConfig> configs;
  FLAGS_oauth_servers = "{not json";
  Status status = BuildOAuthServerConfigs(&configs);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos, status.GetDetail().find("Failed to parse"));
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonRejectsNonArray) {
  google::FlagSaver flag_saver;
  vector<OAuthServerConfig> configs;
  FLAGS_oauth_servers = R"({"jwksFilePath":"/tmp/jwks.json"})";
  Status status = BuildOAuthServerConfigs(&configs);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos, status.GetDetail().find("must be a JSON array"));
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonRejectsBothJwksFilePathAndUrl) {
  google::FlagSaver flag_saver;
  vector<OAuthServerConfig> configs;
  const string json =
      R"([{"jwksFilePath":"/tmp/jwks.json","jwksUrl":"https://example.com/jwks.json"}])";
  FLAGS_oauth_servers = json;
  Status status = BuildOAuthServerConfigs(&configs);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos,
      status.GetDetail().find("cannot specify both jwksFilePath and jwksUrl"));
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonRejectsNonArrayAudienceClaims) {
  google::FlagSaver flag_saver;
  vector<OAuthServerConfig> configs;
  FLAGS_oauth_servers = R"([{
    "jwksFilePath":"/tmp/jwks.json",
    "audienceClaims":"impala-service"
  }])";
  Status status = BuildOAuthServerConfigs(&configs);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos,
      status.GetDetail().find(
          "oauth_servers entry field 'audienceClaims' "
          "must be an array"));
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonRejectsNonArrayIssuerClaims) {
  google::FlagSaver flag_saver;
  vector<OAuthServerConfig> configs;
  FLAGS_oauth_servers = R"([{
    "jwksFilePath":"/tmp/jwks.json",
    "issuerClaims":"auth0"
  }])";
  Status status = BuildOAuthServerConfigs(&configs);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos,
      status.GetDetail().find(
          "oauth_servers entry field 'issuerClaims' "
          "must be an array"));
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonAcceptsEmptyClaimValuesAsUnset) {
  google::FlagSaver flag_saver;
  vector<OAuthServerConfig> configs;
  FLAGS_oauth_servers = R"([{
    "jwksFilePath":"/tmp/jwks.json",
    "usernameClaim":"sub",
    "audienceClaims":[],
    "issuerClaims":[""]
  }])";
  ASSERT_OK(BuildOAuthServerConfigs(&configs));
  ASSERT_EQ(1, configs.size());
  EXPECT_TRUE(configs[0].audience_claims.empty());
  EXPECT_TRUE(configs[0].issuer_claims.empty());
}

TEST(OAuthServerConfigTest, BuildOAuthServerConfigsUsesOauthServersJson) {
  google::FlagSaver flag_saver;
  const string oauth_servers_json =
      R"([{"jwksFilePath":"/oauth/jwks.json","usernameClaim":"preferred_username"}])";
  FLAGS_oauth_servers = oauth_servers_json;

  vector<OAuthServerConfig> configs;
  ASSERT_OK(BuildOAuthServerConfigs(&configs));
  ASSERT_EQ(1, configs.size());
  EXPECT_EQ("/oauth/jwks.json", configs[0].jwks_uri);
  EXPECT_TRUE(configs[0].is_local_jwks);
  EXPECT_EQ("preferred_username", configs[0].username_claim);
}

TEST(OAuthServerConfigTest, BuildOAuthServerConfigsMergesLegacyJwtFlags) {
  google::FlagSaver flag_saver;
  FLAGS_jwt_token_auth = true;
  FLAGS_jwks_file_path = "/legacy/jwks.json";
  FLAGS_jwt_custom_claim_username = "sub";

  vector<OAuthServerConfig> configs;
  ASSERT_OK(BuildOAuthServerConfigs(&configs));
  ASSERT_EQ(1, configs.size());
  EXPECT_EQ("/legacy/jwks.json", configs[0].jwks_uri);
  EXPECT_TRUE(configs[0].is_local_jwks);
  EXPECT_EQ("sub", configs[0].username_claim);
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonRequiresJwksSource) {
  google::FlagSaver flag_saver;
  FLAGS_oauth_servers = R"([{"usernameClaim":"sub"}])";

  vector<OAuthServerConfig> configs;
  Status status = BuildOAuthServerConfigs(&configs);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos,
      status.GetDetail().find("must specify either jwksFilePath or jwksUrl"));
}

TEST(OAuthServerConfigTest,
    BuildOAuthServerConfigsIgnoresLegacyUsernameClaimWithoutJwks) {
  google::FlagSaver flag_saver;
  FLAGS_jwt_custom_claim_username = "sub";

  vector<OAuthServerConfig> configs;
  ASSERT_OK(BuildOAuthServerConfigs(&configs));
  EXPECT_TRUE(configs.empty());
}

TEST(OAuthServerConfigTest, BuildOAuthServerConfigsCombinesJsonAndLegacy) {
  google::FlagSaver flag_saver;
  FLAGS_jwt_token_auth = true;
  FLAGS_oauth_servers =
      R"([{"jwksFilePath":"/oauth/jwks.json","usernameClaim":"preferred_username"}])";
  FLAGS_jwks_file_path = "/legacy/jwks.json";

  vector<OAuthServerConfig> configs;
  ASSERT_OK(BuildOAuthServerConfigs(&configs));
  ASSERT_EQ(2, configs.size());
  EXPECT_EQ("/oauth/jwks.json", configs[0].jwks_uri);
  EXPECT_EQ("preferred_username", configs[0].username_claim);
  EXPECT_EQ("/legacy/jwks.json", configs[1].jwks_uri);
}

TEST(OAuthServerConfigTest, BuildOAuthServerConfigsPrefersLegacyJwtFileOverUrl) {
  google::FlagSaver flag_saver;
  FLAGS_jwt_token_auth = true;
  FLAGS_jwks_file_path = "/legacy/jwks.json";
  FLAGS_jwks_url = "https://example.com/jwks.json";

  vector<OAuthServerConfig> configs;
  ASSERT_OK(BuildOAuthServerConfigs(&configs));
  ASSERT_EQ(1, configs.size());
  EXPECT_EQ("/legacy/jwks.json", configs[0].jwks_uri);
  EXPECT_TRUE(configs[0].is_local_jwks);
}

TEST(OAuthServerConfigTest, BuildOAuthServerConfigsMergesLegacyOAuthFlags) {
  google::FlagSaver flag_saver;
  FLAGS_oauth_token_auth = true;
  FLAGS_oauth_jwks_file_path = "/legacy/oauth-jwks.json";
  FLAGS_oauth_jwt_custom_claim_username = "sub";

  vector<OAuthServerConfig> configs;
  ASSERT_OK(BuildOAuthServerConfigs(&configs));
  ASSERT_EQ(1, configs.size());
  EXPECT_EQ("/legacy/oauth-jwks.json", configs[0].jwks_uri);
  EXPECT_TRUE(configs[0].is_local_jwks);
  EXPECT_EQ("sub", configs[0].username_claim);
}

TEST(OAuthServerConfigTest, BuildOAuthServerConfigsCombinesJsonLegacyJwtAndLegacyOAuth) {
  google::FlagSaver flag_saver;
  FLAGS_jwt_token_auth = true;
  FLAGS_oauth_token_auth = true;
  FLAGS_oauth_servers =
      R"([{"jwksUrl":"https://example.com/oauth-servers-jwks.json",)"
      R"("usernameClaim":"sub"}])";
  FLAGS_jwks_file_path = "/legacy/jwt-jwks.json";
  FLAGS_oauth_jwks_file_path = "/legacy/oauth-jwks.json";

  vector<OAuthServerConfig> configs;
  ASSERT_OK(BuildOAuthServerConfigs(&configs));
  ASSERT_EQ(3, configs.size());
  EXPECT_EQ("https://example.com/oauth-servers-jwks.json", configs[0].jwks_uri);
  EXPECT_FALSE(configs[0].is_local_jwks);
  EXPECT_EQ("/legacy/jwt-jwks.json", configs[1].jwks_uri);
  EXPECT_TRUE(configs[1].is_local_jwks);
  EXPECT_EQ("/legacy/oauth-jwks.json", configs[2].jwks_uri);
  EXPECT_TRUE(configs[2].is_local_jwks);
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonJwksProxy) {
  google::FlagSaver flag_saver;
  for (const char* proxy : {"http://proxy.example.com:3128",
           "https://proxy.example.com:3129", "HTTPS://proxy.example.com",
           "socks4://10.0.0.1:1080", "socks4a://proxy.example.com:1080",
           "socks5://[::1]:1080", "socks5h://proxy.example.com:1080/",
           "proxy.example.com:3128", "socks6://proxy.example.com:1080"}) {
    FLAGS_oauth_servers = Substitute(R"([{"jwksUrl":"https://example.com/jwks.json",)"
        R"("jwksProxy":"$0"}])", proxy);
    vector<OAuthServerConfig> configs;
    ASSERT_OK(BuildOAuthServerConfigs(&configs));
    ASSERT_EQ(1, configs.size());
    EXPECT_EQ(proxy, configs[0].jwks_proxy);
    EXPECT_EQ("https://example.com/jwks.json", configs[0].jwks_uri);
  }
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonJwksProxyDefaultsToEmpty) {
  google::FlagSaver flag_saver;
  for (const char* proxy_field : {"", R"(,"jwksProxy":"")"}) {
    FLAGS_oauth_servers = Substitute(
        R"([{"jwksUrl":"https://example.com/jwks.json"$0}])", proxy_field);
    vector<OAuthServerConfig> configs;
    ASSERT_OK(BuildOAuthServerConfigs(&configs));
    ASSERT_EQ(1, configs.size());
    EXPECT_TRUE(configs[0].jwks_proxy.empty());
    EXPECT_TRUE(configs[0].jwks_proxy_pinned_public_key.empty());
  }
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonRejectsNonStringJwksProxy) {
  google::FlagSaver flag_saver;
  FLAGS_oauth_servers =
      R"([{"jwksUrl":"https://example.com/jwks.json","jwksProxy":3128}])";
  vector<OAuthServerConfig> configs;
  Status status = BuildOAuthServerConfigs(&configs);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos, status.GetDetail().find("'jwksProxy' must be a string"))
      << status.GetDetail();
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonJwksProxyPinnedPublicKey) {
  google::FlagSaver flag_saver;
  for (const char* key : {"/etc/proxy/pubkey.pem",
           "sha256//YhKJKSzoTt2b5FP18fvpHo7fJYqQCjAa3HWY3tvRMwE=",
           "sha256//YhKJKSzoTt2b5FP18fvpHo7fJYqQCjAa3HWY3tvRMwE=;"
           "sha256//t62CeU2tQiqkexU74Gxa2eg7fRbEgoChTociMee9wno="}) {
    FLAGS_oauth_servers = Substitute(R"([{"jwksUrl":"https://example.com/jwks.json",)"
        R"("jwksProxy":"https://proxy.example.com:3129",)"
        R"("jwksProxyPinnedPublicKey":"$0"}])", key);
    vector<OAuthServerConfig> configs;
    ASSERT_OK(BuildOAuthServerConfigs(&configs));
    ASSERT_EQ(1, configs.size());
    EXPECT_EQ("https://proxy.example.com:3129", configs[0].jwks_proxy);
    EXPECT_EQ(key, configs[0].jwks_proxy_pinned_public_key);
  }
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonRejectsInvalidJwksProxy) {
  google::FlagSaver flag_saver;
  const vector<pair<string, string>> cases = {
      {"http://", "'jwksProxy' is not a valid URL"},
      {"http://proxyuser:secret@proxy.example.com:3128",
          "'jwksProxy' must not contain a user name or password"},
      {"proxyuser@proxy.example.com:3128",
          "'jwksProxy' must not contain a user name or password"},
      {"htps://proxyuser:secret@proxy.example.com:3128",
          "'jwksProxy' must not contain a user name or password"}};
  for (const auto& test_case : cases) {
    FLAGS_oauth_servers = Substitute(R"([{"jwksUrl":"https://example.com/jwks.json",)"
        R"("jwksProxy":"$0"}])", test_case.first);
    vector<OAuthServerConfig> configs;
    Status status = BuildOAuthServerConfigs(&configs);
    ASSERT_FALSE(status.ok()) << test_case.first;
    EXPECT_NE(string::npos, status.GetDetail().find(test_case.second))
        << test_case.first << ": " << status.GetDetail();
    EXPECT_EQ(string::npos, status.GetDetail().find("proxyuser")) << status.GetDetail();
    EXPECT_EQ(string::npos, status.GetDetail().find("secret")) << status.GetDetail();
  }
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonJwksProxyCaCertFilePath) {
  google::FlagSaver flag_saver;
  const string base = R"({"jwksUrl":"https://example.com/jwks.json",)"
      R"("jwksProxy":"https://proxy.example.com:3129")";
  // Each case is the extra JSON fields and the expected jwks_proxy_ca_certificate.
  const vector<pair<string, string>> cases = {
      {"", ""},
      {R"(,"caCertFilePath":"/idp/ca.pem")", "/idp/ca.pem"},
      {R"(,"jwksProxyCaCertFilePath":"/proxy/ca.pem")", "/proxy/ca.pem"},
      {R"(,"caCertFilePath":"/idp/ca.pem","jwksProxyCaCertFilePath":"/proxy/ca.pem")",
          "/proxy/ca.pem"},
      {R"(,"caCertFilePath":"/idp/ca.pem","jwksProxyCaCertFilePath":"")", ""}};
  for (const auto& test_case : cases) {
    FLAGS_oauth_servers = Substitute("[$0$1}]", base, test_case.first);
    vector<OAuthServerConfig> configs;
    ASSERT_OK(BuildOAuthServerConfigs(&configs));
    ASSERT_EQ(1, configs.size());
    EXPECT_EQ(test_case.second, configs[0].jwks_proxy_ca_certificate)
        << test_case.first;
  }
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonRejectsPinnedKeyWithoutHttpsProxy) {
  google::FlagSaver flag_saver;
  for (const char* proxy : {"http://proxy.example.com:3128",
           "socks5h://proxy.example.com:1080", "proxy.example.com:3128"}) {
    FLAGS_oauth_servers = Substitute(R"([{"jwksUrl":"https://example.com/jwks.json",)"
        R"("jwksProxy":"$0","jwksProxyPinnedPublicKey":"/etc/proxy/pubkey.pem"}])",
        proxy);
    vector<OAuthServerConfig> configs;
    Status status = BuildOAuthServerConfigs(&configs);
    ASSERT_FALSE(status.ok()) << proxy;
    EXPECT_NE(string::npos, status.GetDetail().find(
        "'jwksProxyPinnedPublicKey' requires an https 'jwksProxy'"))
        << proxy << ": " << status.GetDetail();
  }
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonRejectsPinnedPublicKeyWithoutProxy) {
  google::FlagSaver flag_saver;
  FLAGS_oauth_servers = R"([{"jwksUrl":"https://example.com/jwks.json",)"
      R"("jwksProxyPinnedPublicKey":"/etc/proxy/pubkey.pem"}])";
  vector<OAuthServerConfig> configs;
  Status status = BuildOAuthServerConfigs(&configs);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos, status.GetDetail().find(
      "field 'jwksProxyPinnedPublicKey' requires 'jwksProxy'")) << status.GetDetail();
}

TEST(OAuthServerConfigTest, ParseOAuthServersJsonRejectsJwksProxyWithJwksFilePath) {
  google::FlagSaver flag_saver;
  FLAGS_oauth_servers = R"([{"jwksFilePath":"/tmp/jwks.json",)"
      R"("jwksProxy":"http://proxy.example.com:3128"}])";
  vector<OAuthServerConfig> configs;
  Status status = BuildOAuthServerConfigs(&configs);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos,
      status.GetDetail().find("field 'jwksProxy' requires 'jwksUrl'"));
}

} // namespace impala
