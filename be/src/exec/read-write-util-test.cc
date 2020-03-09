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

#include "exec/read-write-util.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

template<typename T>
void TestBigEndian(T value) {
  uint8_t buffer[sizeof(T)];
  ReadWriteUtil::PutInt(buffer, value);
  EXPECT_EQ(value, ReadWriteUtil::GetInt<T>(buffer));
}

// Test put and get of big endian values
TEST(ReadWriteUtil, BigEndian) {
  TestBigEndian<uint16_t>(0);
  TestBigEndian<uint16_t>(0xff);
  TestBigEndian<uint16_t>(0xffff);

  TestBigEndian<uint32_t>(0);
  TestBigEndian<uint32_t>(0xff);
  TestBigEndian<uint32_t>(0xffff);
  TestBigEndian<uint32_t>(0xffffff);
  TestBigEndian<uint32_t>(0xffffffff);

  TestBigEndian<uint64_t>(0);
  TestBigEndian<uint64_t>(0xff);
  TestBigEndian<uint64_t>(0xffff);
  TestBigEndian<uint64_t>(0xffffff);
  TestBigEndian<uint64_t>(0xffffffff);
  TestBigEndian<uint64_t>(0xffffffffff);
  TestBigEndian<uint64_t>(0xffffffffffff);
  TestBigEndian<uint64_t>(0xffffffffffffff);
}

TEST(ReadWriteUtil, ZeroCompressedLongRequiredBytes) {
  // Small longs stored in 1 byte
  for (int64_t val = -112; val <= 127; val++) {
    EXPECT_EQ(1, ReadWriteUtil::VLongRequiredBytes(val));
  }
  // Small longs stored in 2 bytes
  for (int64_t val = -128; val < -112; val++) {
    EXPECT_EQ(2, ReadWriteUtil::VLongRequiredBytes(val));
  }
  // Positive longs stored in 3-9 bytes
  int64_t val = 0x7000ab00cd00ef00;
  for (int sh = 0; sh <= 6; sh++) {
    EXPECT_EQ(9 - sh, ReadWriteUtil::VLongRequiredBytes(val));
    val = val >> 8;
  }
  // Negative longs stored 3-9 bytes
  val = 0x8000ab00cd00ef00;
  for (int sh = 0; sh <= 6; sh++) {
    EXPECT_EQ(9 - sh, ReadWriteUtil::VLongRequiredBytes(val));
    val = val >> 8;
  }
  //Max/min long is stored in 9 bytes
  EXPECT_EQ(9, ReadWriteUtil::VLongRequiredBytes(0x7fffffffffffffff));
  EXPECT_EQ(9, ReadWriteUtil::VLongRequiredBytes(0x8000000000000000));
}

void TestPutGetZeroCompressedLong(int64_t val) {
  const int32_t BUFSZ = 9;
  uint8_t buffer[BUFSZ];
  int64_t read_val;
  int64_t num_bytes = ReadWriteUtil::PutVLong(val, buffer);
  int64_t read_bytes = ReadWriteUtil::GetVLong(buffer, &read_val, BUFSZ);
  EXPECT_EQ(read_bytes, num_bytes);
  EXPECT_EQ(read_val, val);
  // Out of bound access check, -1 should be returned because buffer size is passed
  // as 1 byte.
  if (read_bytes > 1) {
    read_bytes = ReadWriteUtil::GetVLong(buffer, &read_val, 1);
    EXPECT_EQ(read_bytes, -1);
  }
}

TEST(ReadWriteUtil, ZeroCompressedLong) {
  //1 byte longs
  for (int64_t val = -128; val <= 127; val++) {
    TestPutGetZeroCompressedLong(val);
  }
  //2+ byte positive longs
  int64_t val = 0x70100000200000ab;
  for (int sh = 0; sh <= 6; sh++) {
    TestPutGetZeroCompressedLong(val);
    val = val >> 8;
  }
  //2+ byte negative longs
  val = 0x80100000200000ab;
  for (int sh = 0; sh <= 6; sh++) {
    TestPutGetZeroCompressedLong(val);
    val = val >> 8;
  }
  //Max/min long
  TestPutGetZeroCompressedLong(0x7fffffffffffffff);
  TestPutGetZeroCompressedLong(0x8000000000000000);
}

}

