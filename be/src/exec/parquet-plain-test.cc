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
#include <limits.h>
#include <gtest/gtest.h>
#include "exec/parquet-common.h"
#include "runtime/timestamp-value.h"

using namespace std;

namespace impala {

template <typename T>
void TestType(const T& v, int expected_byte_size) {
  uint8_t buffer[expected_byte_size];
  EXPECT_EQ(ParquetPlainEncoder::ByteSize(v), expected_byte_size);
  int encoded_size = ParquetPlainEncoder::Encode(buffer, v);
  EXPECT_EQ(encoded_size, expected_byte_size);

  T result;
  int decoded_size = ParquetPlainEncoder::Decode(buffer, &result);
  EXPECT_EQ(decoded_size, expected_byte_size);
  EXPECT_EQ(result, v);
}

TEST(PlainEncoding, Basic) {
  int8_t i8 = 12;
  int16_t i16 = 123;
  int32_t i32 = 1234;
  int64_t i64 = 12345;
  float f = 1.23;
  double d = 1.23456;
  StringValue sv("Hello");
  TimestampValue tv;

  TestType(i8, sizeof(int32_t));
  TestType(i16, sizeof(int32_t));
  TestType(i32, sizeof(int32_t));
  TestType(i64, sizeof(int64_t));
  TestType(f, sizeof(float));
  TestType(d, sizeof(double));
  TestType(sv, sizeof(int32_t) + sv.len);
  TestType(tv, 12);
}

}

int main(int argc, char **argv) {
  impala::CpuInfo::Init();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
