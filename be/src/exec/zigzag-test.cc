// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <limits.h>
#include <gtest/gtest.h>
#include "exec/read-write-util.h"
#include "util/hash-util.h"

using namespace std;

namespace impala {

void TestZInt(int32_t value) {
  uint8_t buf[ReadWriteUtil::MAX_ZINT_LEN];
  int plen = ReadWriteUtil::PutZInt(value, static_cast<uint8_t*>(buf));
  EXPECT_TRUE(plen <= ReadWriteUtil::MAX_ZINT_LEN);
  int32_t output;
  int glen = ReadWriteUtil::GetZInt(static_cast<uint8_t*>(buf), &output);
  EXPECT_EQ(plen, glen);
  EXPECT_EQ(value, output);
}

void TestZLong(int64_t value) {
  uint8_t buf[ReadWriteUtil::MAX_ZLONG_LEN];
  int plen = ReadWriteUtil::PutZLong(value, static_cast<uint8_t*>(buf));
  EXPECT_TRUE(plen <= ReadWriteUtil::MAX_ZLONG_LEN);
  int64_t output;
  int glen = ReadWriteUtil::GetZLong(static_cast<uint8_t*>(buf), &output);
  EXPECT_EQ(plen, glen);
  EXPECT_EQ(value, output);
}


// Test put and get of zigzag integers and longs.
TEST(ZigzagTest, Basic) { 
  // Test min/max of all sizes.
  TestZInt(0);
  TestZInt(INT_MAX);
  TestZInt(INT_MIN);
  TestZInt(SHRT_MIN);
  TestZInt(SHRT_MAX);
  TestZInt(0);
  TestZLong(LONG_MAX);
  TestZLong(LONG_MIN);
  TestZLong(INT_MAX);
  TestZLong(INT_MIN);
  TestZLong(SHRT_MIN);
  TestZLong(SHRT_MAX);
  TestZLong(SCHAR_MIN);
  TestZLong(SCHAR_MAX);
  // Test somewhat random bit patterns.
  int32_t value = 0xa2a2a2a2;
  for (int i = 0; i < 1000; ++i) {
    value = HashUtil::CrcHash(&value, sizeof (value), i);
    TestZInt(value);
    TestZLong(value);
    TestZLong((static_cast<int64_t>(value) << 32) | value);
  }

}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

