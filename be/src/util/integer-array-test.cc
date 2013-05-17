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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <gtest/gtest.h>
#include "util/integer-array.h"
#include "runtime/mem-pool.h"

using namespace std;

namespace impala {

// Test various bit sized arrays. Check that the values that are put in are the same
// that come out
TEST(IntegerArrayTest, Basic) { 
  MemPool mempool(NULL);
  for (int size = 1; size <= 12; ++size) {
    IntegerArrayBuilder build(size, 1000, &mempool);
    uint32_t value = 0;
    for (int i = 0; i < 1000; ++i) {
      EXPECT_TRUE(build.Put(value));
      ++value;
      value %= 1 << size;
    }
    EXPECT_EQ(build.count(), 1000);
    IntegerArray int_array(size, 1000, build.array());
    value = 0;
    for (int i = 0; i < 1000; ++i) {
      EXPECT_EQ(int_array.GetNextValue(), value);
      ++value;
      value %= 1 << size;
    }
  }
}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

