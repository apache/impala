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

#include <gtest/gtest.h>

#include "common/logging.h"
#include "rpc/authentication.h"
#include "util/network-util.h"
#include "util/thread.h"

using namespace std;

namespace impala {

TEST(Auth, PrincipalSubstitution) {
  string hostname;
  ASSERT_TRUE(GetHostname(&hostname).ok());
  KerberosAuthProvider kerberos("service_name/_HOST@some.realm", "", false);
  ASSERT_TRUE(kerberos.Start().ok());
  ASSERT_EQ(string::npos, kerberos.principal().find("_HOST"));
  ASSERT_NE(string::npos, kerberos.principal().find(hostname));
  ASSERT_EQ("service_name", kerberos.service_name());
  ASSERT_EQ(hostname, kerberos.hostname());
}

TEST(Auth, ValidAuthProviders) {
  ASSERT_TRUE(AuthManager::GetInstance()->Init().ok());
  ASSERT_TRUE(AuthManager::GetInstance()->GetClientFacingAuthProvider() != NULL);
  ASSERT_TRUE(AuthManager::GetInstance()->GetServerFacingAuthProvider() != NULL);
}

}

int main(int argc, char** argv) {
  impala::InitGoogleLoggingSafe(argv[0]);
  impala::InitThreading();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
