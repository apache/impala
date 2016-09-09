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
#include "exec/parquet-common.h"
#include "runtime/decimal-value.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

/// Test that the decoder fails when asked to decode a truncated value.
template <typename T>
void TestTruncate(const T& v, int expected_byte_size) {
  uint8_t buffer[expected_byte_size];
  int encoded_size = ParquetPlainEncoder::Encode(buffer, expected_byte_size, v);
  EXPECT_EQ(encoded_size, expected_byte_size);

  // Check all possible truncations of the buffer.
  for (int truncated_size = encoded_size - 1; truncated_size >= 0; --truncated_size) {
    T result;
    /// Copy to heap-allocated buffer so that ASAN can detect buffer overruns.
    uint8_t* truncated_buffer = new uint8_t[truncated_size];
    memcpy(truncated_buffer, buffer, truncated_size);
    int decoded_size = ParquetPlainEncoder::Decode(truncated_buffer,
        truncated_buffer + truncated_size, expected_byte_size, &result);
    EXPECT_EQ(-1, decoded_size);
    delete[] truncated_buffer;
  }
}

template <typename T>
void TestType(const T& v, int expected_byte_size) {
  uint8_t buffer[expected_byte_size];
  int encoded_size = ParquetPlainEncoder::Encode(buffer, expected_byte_size, v);
  EXPECT_EQ(encoded_size, expected_byte_size);

  T result;
  int decoded_size = ParquetPlainEncoder::Decode(buffer, buffer + expected_byte_size,
      expected_byte_size, &result);
  EXPECT_EQ(decoded_size, expected_byte_size);
  EXPECT_EQ(result, v);

  TestTruncate(v, expected_byte_size);
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

  TestType(Decimal4Value(1234), sizeof(Decimal4Value));
  TestType(Decimal4Value(-1234), sizeof(Decimal4Value));

  TestType(Decimal8Value(1234), sizeof(Decimal8Value));
  TestType(Decimal8Value(-1234), sizeof(Decimal8Value));
  TestType(Decimal8Value(std::numeric_limits<int32_t>::max()), sizeof(Decimal8Value));
  TestType(Decimal8Value(std::numeric_limits<int32_t>::min()), sizeof(Decimal8Value));

  TestType(Decimal16Value(1234), 16);
  TestType(Decimal16Value(-1234), 16);
  TestType(Decimal16Value(std::numeric_limits<int32_t>::max()), sizeof(Decimal16Value));
  TestType(Decimal16Value(std::numeric_limits<int32_t>::min()), sizeof(Decimal16Value));
  TestType(Decimal16Value(std::numeric_limits<int64_t>::max()), sizeof(Decimal16Value));
  TestType(Decimal16Value(std::numeric_limits<int64_t>::min()), sizeof(Decimal16Value));

  // two digit values can be encoded with any byte size.
  for (int i = 1; i <=16; ++i) {
    if (i <= 4) {
      TestType(Decimal4Value(i), i);
      TestType(Decimal4Value(-i), i);
    }
    if (i <= 8) {
      TestType(Decimal8Value(i), i);
      TestType(Decimal8Value(-i), i);
    }
    TestType(Decimal16Value(i), i);
    TestType(Decimal16Value(-i), i);
  }
}

TEST(PlainEncoding, DecimalBigEndian) {
  // Test Basic can pass if we make the same error in encode and decode.
  // Verify the bytes are actually big endian.
  uint8_t buffer[] = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
  };

  // Manually generate this to avoid potential bugs in BitUtil
  uint8_t buffer_swapped[] = {
    15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0
  };
  uint8_t result_buffer[16];

  Decimal4Value d4;
  Decimal8Value d8;
  Decimal16Value d16;

  memcpy(&d4, buffer, sizeof(d4));
  memcpy(&d8, buffer, sizeof(d8));
  memcpy(&d16, buffer, sizeof(d16));

  int size = ParquetPlainEncoder::Encode(result_buffer, sizeof(d4), d4);
  ASSERT_EQ(size, sizeof(d4));
  ASSERT_EQ(memcmp(result_buffer, buffer_swapped + 16 - sizeof(d4), sizeof(d4)), 0);

  size = ParquetPlainEncoder::Encode(result_buffer, sizeof(d8), d8);
  ASSERT_EQ(size, sizeof(d8));
  ASSERT_EQ(memcmp(result_buffer, buffer_swapped + 16 - sizeof(d8), sizeof(d8)), 0);

  size = ParquetPlainEncoder::Encode(result_buffer, sizeof(d16), d16);
  ASSERT_EQ(size, sizeof(d16));
  ASSERT_EQ(memcmp(result_buffer, buffer_swapped + 16 - sizeof(d16), sizeof(d16)), 0);
}

/// Test that corrupt strings are handled correctly.
TEST(PlainEncoding, CorruptString) {
  // Test string with negative length.
  uint8_t buffer[sizeof(int32_t) + 10];
  int32_t len = -10;
  memcpy(buffer, &len, sizeof(int32_t));

  StringValue result;
  int decoded_size =
      ParquetPlainEncoder::Decode(buffer, buffer + sizeof(buffer), 0, &result);
  EXPECT_EQ(decoded_size, -1);
}

}

IMPALA_TEST_MAIN();
