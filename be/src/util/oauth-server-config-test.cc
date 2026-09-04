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
          "usernameClaim": "preferred_username"
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
  EXPECT_EQ("/opt/auth-servers/jwks.json", configs[1].jwks_uri);
  EXPECT_TRUE(configs[1].is_local_jwks);
  EXPECT_EQ("username", configs[1].username_claim);
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

} // namespace impala
