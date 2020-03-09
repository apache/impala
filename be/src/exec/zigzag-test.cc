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

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include "common/status.h"
#include "exec/read-write-util.h"
#include "util/hash-util.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

void TestZInt(uint8_t* buf, int64_t buf_len, int32_t expected_val,
    int expected_encoded_len) {
  uint8_t* new_buf = buf;
  ReadWriteUtil::ZIntResult r = ReadWriteUtil::ReadZInt(&new_buf, buf + buf_len);
  EXPECT_TRUE(r.ok);
  EXPECT_EQ(r.val, expected_val);
  EXPECT_EQ(new_buf - buf, expected_encoded_len);
}

void TestZInt(int32_t value) {
  uint8_t buf[ReadWriteUtil::MAX_ZINT_LEN];
  int plen = ReadWriteUtil::PutZInt(value, static_cast<uint8_t*>(buf));
  EXPECT_TRUE(plen <= ReadWriteUtil::MAX_ZINT_LEN);
  TestZInt(buf, sizeof(buf), value, plen);
}

void TestZLong(uint8_t* buf, int64_t buf_len, int64_t expected_val,
    int expected_encoded_len) {
  uint8_t* new_buf = buf;
  ReadWriteUtil::ZLongResult r = ReadWriteUtil::ReadZLong(&new_buf, buf + buf_len);
  EXPECT_TRUE(r.ok);
  EXPECT_EQ(r.val, expected_val);
  EXPECT_EQ(new_buf - buf, expected_encoded_len);
}

void TestZLong(int64_t value) {
  uint8_t buf[ReadWriteUtil::MAX_ZLONG_LEN];
  int plen = ReadWriteUtil::PutZLong(value, static_cast<uint8_t*>(buf));
  EXPECT_TRUE(plen <= ReadWriteUtil::MAX_ZLONG_LEN);
  TestZLong(buf, sizeof(buf), value, plen);
}

// No expected value
void TestZInt(uint8_t* buf, int64_t buf_len, int expected_encoded_len) {
  uint8_t* new_buf = buf;
  ReadWriteUtil::ZIntResult r = ReadWriteUtil::ReadZInt(&new_buf, buf + buf_len);
  EXPECT_TRUE(r.ok);
  EXPECT_EQ(new_buf - buf, expected_encoded_len);
}

void TestZLong(uint8_t* buf, int64_t buf_len, int expected_encoded_len) {
  uint8_t* new_buf = buf;
  ReadWriteUtil::ZLongResult r = ReadWriteUtil::ReadZLong(&new_buf, buf + buf_len);
  EXPECT_TRUE(r.ok);
  EXPECT_EQ(new_buf - buf, expected_encoded_len);
}

// Test put and get of zigzag integers and longs.
TEST(ZigzagTest, Basic) {
  // Test min/max of all sizes.
  TestZInt(0);
  TestZInt(INT_MAX);
  TestZInt(INT_MIN);
  TestZInt(SHRT_MIN);
  TestZInt(SHRT_MAX);
  TestZLong(0);
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
    value = HashUtil::Hash(&value, sizeof (value), i);
    TestZInt(value);
    TestZLong(value);
    TestZLong((static_cast<uint64_t>(value) << 32) | value);
  }
}

TEST(ZigzagTest, Errors) {
  uint8_t buf[100];
  memset(buf, 0x80, sizeof(buf));

  // Test 100-byte int
  uint8_t* buf_ptr = static_cast<uint8_t*>(buf);
  int64_t buf_len = sizeof(buf);
  EXPECT_FALSE(ReadWriteUtil::ReadZLong(&buf_ptr, buf + buf_len).ok);
  EXPECT_FALSE(ReadWriteUtil::ReadZInt(&buf_ptr, buf + buf_len).ok);

  // Test truncated int
  buf_ptr = static_cast<uint8_t*>(buf);
  buf_len = ReadWriteUtil::MAX_ZLONG_LEN - 1;
  EXPECT_FALSE(ReadWriteUtil::ReadZLong(&buf_ptr, buf + buf_len).ok);
  buf_len = ReadWriteUtil::MAX_ZINT_LEN - 1;
  EXPECT_FALSE(ReadWriteUtil::ReadZInt(&buf_ptr, buf + buf_len).ok);
}

  // Test weird encodings and values that are arguably invalid but we still accept
TEST(ZigzagTest, Weird) {
  uint8_t buf[100];

  // Decodes to 0 but encoded in two bytes
  buf[0] = 0x80;
  buf[1] = 0x0;
  TestZInt(buf, 2, 0, 2);
  TestZLong(buf, 2, 0, 2);
  TestZInt(buf, sizeof(buf), 0, 2);
  TestZLong(buf, sizeof(buf), 0, 2);

  // Decodes to 1 but encoded in MAX_ZINT_LEN bytes
  memset(buf, 0x80, ReadWriteUtil::MAX_ZINT_LEN);
  buf[0] = 0x82;
  buf[ReadWriteUtil::MAX_ZINT_LEN - 1] = 0x0;
  TestZInt(buf, ReadWriteUtil::MAX_ZINT_LEN, 1, ReadWriteUtil::MAX_ZINT_LEN);
  TestZLong(buf, ReadWriteUtil::MAX_ZINT_LEN, 1, ReadWriteUtil::MAX_ZINT_LEN);
  TestZInt(buf, sizeof(buf), 1, ReadWriteUtil::MAX_ZINT_LEN);
  TestZLong(buf, sizeof(buf), 1, ReadWriteUtil::MAX_ZINT_LEN);

  // Decodes to 1 but encoded in MAX_ZLONG_LEN bytes
  memset(buf, 0x80, ReadWriteUtil::MAX_ZLONG_LEN);
  buf[0] = 0x82;
  buf[ReadWriteUtil::MAX_ZLONG_LEN - 1] = 0x0;
  TestZLong(buf, ReadWriteUtil::MAX_ZLONG_LEN, 1, ReadWriteUtil::MAX_ZLONG_LEN);
  TestZLong(buf, sizeof(buf), 1, ReadWriteUtil::MAX_ZLONG_LEN);

  // Overflows a long. Check that we don't crash and decode the correct number of bytes,
  // but don't check for a particular value.
  memset(buf, 0xff, ReadWriteUtil::MAX_ZLONG_LEN);
  buf[ReadWriteUtil::MAX_ZLONG_LEN - 1] ^= 0x80;
  TestZLong(buf, ReadWriteUtil::MAX_ZLONG_LEN, ReadWriteUtil::MAX_ZLONG_LEN);

  // Overflows an int. Check that we don't crash and decode the correct number of bytes,
  // but don't check for a particular value.
  memset(buf, 0xff, ReadWriteUtil::MAX_ZINT_LEN);
  buf[ReadWriteUtil::MAX_ZINT_LEN - 1] ^= 0x80;
  TestZInt(buf, ReadWriteUtil::MAX_ZINT_LEN, ReadWriteUtil::MAX_ZINT_LEN);

}
}


