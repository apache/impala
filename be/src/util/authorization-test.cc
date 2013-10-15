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
#include "util/network-util.h"
#include "util/authorization.h"
#include "common/init.h"
#include "util/logging.h"

using namespace std;

DECLARE_string(principal);

namespace impala {

TEST(KerberosTest, PrincipalSubstitution) {
  FLAGS_principal = "username/_HOST";
  // Warning: this kicks off a kinit thread
  EXPECT_TRUE(InitKerberos("test-app").ok());
  string hostname;
  EXPECT_TRUE(GetHostname(&hostname).ok());
  EXPECT_EQ(string::npos, FLAGS_principal.find("_HOST"));
  EXPECT_NE(string::npos, FLAGS_principal.find(hostname));
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, false);
  return RUN_ALL_TESTS();
}
