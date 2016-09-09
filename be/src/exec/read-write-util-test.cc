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
#include <iostream>
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

}

IMPALA_TEST_MAIN();
