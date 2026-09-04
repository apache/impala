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

#include <fstream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "gutil/strings/substitute.h"
#include "testutil/gtest-util.h"
#include "util/jwt-util.h"
#include "util/oauth-server-config.h"
#include "util/oauth-servers-manager.h"

#include "common/names.h"

DECLARE_string(oauth_servers);

namespace impala {

namespace {

string GetImpalaHome() {
  const char* impala_home = getenv("IMPALA_HOME");
  EXPECT_TRUE(impala_home != nullptr && impala_home[0] != '\0');
  return impala_home == nullptr ? "" : string(impala_home);
}

string ReadTrimmedFile(const string& path) {
  ifstream input(path.c_str());
  EXPECT_TRUE(input.is_open()) << path;
  string contents((std::istreambuf_iterator<char>(input)),
      std::istreambuf_iterator<char>());
  while (!contents.empty() && (contents.back() == '\n' || contents.back() == '\r')) {
    contents.pop_back();
  }
  return contents;
}

} // namespace

TEST(OAuthServersManagerTest, InitRequiresJwksSourceWhenConfigured) {
  google::FlagSaver flag_saver;
  FLAGS_oauth_servers = R"([{"usernameClaim":"sub"}])";
  OAuthServersManager manager;
  Status status = manager.Init();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos,
      status.GetDetail().find("must specify either jwksFilePath or jwksUrl"));
}

TEST(OAuthServersManagerTest, VerifyFailsWhenNoServersConfigured) {
  google::FlagSaver flag_saver;
  OAuthServersManager manager;
  Status status = manager.Init();
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos,
      status.GetDetail().find("No OAuth servers are configured for token verification"));
}

TEST(OAuthServersManagerTest, BearerAuthFailureHeaderIncludesStatusMessage) {
  const Status status("token verification failed");
  const string header = OAuthServersManager::BearerAuthFailureHeader(status);
  EXPECT_NE(string::npos,
      header.find("WWW-Authenticate: Bearer error=\"invalid_token\""));
  EXPECT_NE(string::npos, header.find("token verification failed"));
}

TEST(OAuthServersManagerTest, VerifyTokenFromMatchingServer) {
  google::FlagSaver flag_saver;
  const string impala_home = GetImpalaHome();
  const string jwks_path =
      Substitute("$0/testdata/jwt/jwks_signing.json", impala_home);
  const string jwt_path = Substitute("$0/testdata/jwt/jwt_signed", impala_home);
  const string token = ReadTrimmedFile(jwt_path);
  ASSERT_FALSE(token.empty());

  FLAGS_oauth_servers = Substitute(
      R"([{"jwksFilePath":"$0","usernameClaim":"sub"}])", jwks_path);
  OAuthServersManager manager;
  ASSERT_OK(manager.Init());
  ASSERT_EQ(1, manager.size());

  string username;
  ASSERT_OK(manager.AuthenticateBearerToken(token, &username));
  EXPECT_EQ("test-user", username);
}

TEST(OAuthServersManagerTest, VerifyTokenFromSecondServerWhenFirstDoesNotMatch) {
  google::FlagSaver flag_saver;
  const string impala_home = GetImpalaHome();
  const string wrong_jwks_path =
      Substitute("$0/testdata/jwt/jwks_rs256.json", impala_home);
  const string right_jwks_path =
      Substitute("$0/testdata/jwt/jwks_signing.json", impala_home);
  const string jwt_path = Substitute("$0/testdata/jwt/jwt_signed", impala_home);
  const string token = ReadTrimmedFile(jwt_path);
  ASSERT_FALSE(token.empty());

  FLAGS_oauth_servers = Substitute(
      R"([{"jwksFilePath":"$0","usernameClaim":"sub"},)"
      R"({"jwksFilePath":"$1","usernameClaim":"sub"}])",
      wrong_jwks_path, right_jwks_path);
  OAuthServersManager manager;
  ASSERT_OK(manager.Init());
  ASSERT_EQ(2, manager.size());

  string username;
  ASSERT_OK(manager.AuthenticateBearerToken(token, &username));
  EXPECT_EQ("test-user", username);
}

TEST(OAuthServersManagerTest, VerifyFailsWhenOnlyWrongJwksConfigured) {
  google::FlagSaver flag_saver;
  const string impala_home = GetImpalaHome();
  const string wrong_jwks_path =
      Substitute("$0/testdata/jwt/jwks_rs256.json", impala_home);
  const string jwt_path = Substitute("$0/testdata/jwt/jwt_signed", impala_home);
  const string token = ReadTrimmedFile(jwt_path);
  ASSERT_FALSE(token.empty());

  FLAGS_oauth_servers = Substitute(
      R"([{"jwksFilePath":"$0","usernameClaim":"sub"}])", wrong_jwks_path);
  OAuthServersManager manager;
  ASSERT_OK(manager.Init());

  string username;
  Status status = manager.AuthenticateBearerToken(token, &username);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(string::npos, status.GetDetail().find("no matching key"));
}

} // namespace impala
