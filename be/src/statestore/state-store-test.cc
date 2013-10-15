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

#include <gtest/gtest.h>
#include "common/init.h"

using namespace boost;
using namespace std;
using namespace impala;

DECLARE_int32(webserver_port);
DECLARE_int32(state_store_port);

namespace impala {

TEST(StatestoreTest, SmokeTest) {
  InProcessStateStore* state_store =
      new InProcessStateStore(FLAGS_state_store_port, FLAGS_webserver_port);
  ASSERT_TRUE(state_store->Start().ok());

  InProcessStateStore* state_store_wont_start =
      new InProcessStateStore(FLAGS_state_store_port, FLAGS_webserver_port);
  ASSERT_FALSE(state_store_wont_start->Start().ok());
}

}

int main(int argc, char **argv) {
  InitCommonRuntime(argc, argv, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
