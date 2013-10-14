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

#include "util/webserver.h"

#include <gtest/gtest.h>
#include <string>

DECLARE_int32(webserver_port);
DECLARE_string(webserver_password_file);

using namespace impala;
using namespace std;

TEST(Webserver, SmokeTest) {
  Webserver webserver(FLAGS_webserver_port);
  ASSERT_TRUE(webserver.Start().ok());
}

TEST(Webserver, StartWithPasswordFileTest) {
  stringstream password_file;
  password_file << getenv("IMPALA_HOME") << "/be/src/testutil/htpasswd";
  FLAGS_webserver_password_file = password_file.str();

  Webserver webserver(FLAGS_webserver_port);
  ASSERT_TRUE(webserver.Start().ok());
}

TEST(Webserver, StartWithMissingPasswordFileTest) {
  stringstream password_file;
  password_file << getenv("IMPALA_HOME") << "/be/src/testutil/doesntexist";
  FLAGS_webserver_password_file = password_file.str();

  Webserver webserver(FLAGS_webserver_port);
  ASSERT_FALSE(webserver.Start().ok());
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
