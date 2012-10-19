// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

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
  MemPool mempool;
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

