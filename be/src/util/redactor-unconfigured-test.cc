// Copyright 2015 Cloudera Inc.
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

#include "redactor.h"
#include "redactor.detail.h"

#include <gtest/gtest.h>

#include "redactor-test-utils.h"

namespace impala {

extern std::vector<Rule>* g_rules;

// If the rules were never set, nothing should be redacted.
TEST(RedactorTest, Unconfigured) {
  ASSERT_EQ(NULL, g_rules);
  ASSERT_UNREDACTED("foo33");
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
