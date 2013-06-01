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
#include "runtime/raw-value.h"

using namespace impala;

namespace impala {

class RawValueTest : public testing::Test {
};

TEST_F(RawValueTest, Compare) {
  int64_t v1, v2;
  v1 = -2128609280;
  v2 = 9223372036854775807;
  EXPECT_LT(RawValue::Compare(&v1, &v2, TYPE_BIGINT), 0);
  EXPECT_GT(RawValue::Compare(&v2, &v1, TYPE_BIGINT), 0);

  int32_t i1, i2;
  i1 = 2147483647;
  i2 = -2147483640;
  EXPECT_GT(RawValue::Compare(&i1, &i2, TYPE_INT), 0);
  EXPECT_LT(RawValue::Compare(&i2, &i1, TYPE_INT), 0);

  int16_t s1, s2;
  s1 = 32767;
  s2 = -32767;
  EXPECT_GT(RawValue::Compare(&s1, &s2, TYPE_SMALLINT), 0);
  EXPECT_LT(RawValue::Compare(&s2, &s1, TYPE_SMALLINT), 0);
}

}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
