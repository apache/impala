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

template <typename InternalType>
int Encode(const InternalType& v, int encoded_byte_size, uint8_t* buffer,
    parquet::Type::type physical_type){
  return ParquetPlainEncoder::Encode(v, encoded_byte_size, buffer);
}

// Handle special case of encoding decimal types stored as BYTE_ARRAY since it is not
// implemented in Impala.
// When parquet_type equals BYTE_ARRAY: 'encoded_byte_size' is the sum of the
// minimum number of bytes required to store the unscaled value and the bytes required to
// store the size. Value 'v' passed to it should not contain leading zeros as this
// method does not strictly conform to the parquet spec in removing those.
template <typename DecimalType>
int EncodeDecimal(const DecimalType& v, int encoded_byte_size, uint8_t* buffer,
    parquet::Type::type parquet_type) {
  if (parquet_type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
    return ParquetPlainEncoder::Encode(v, encoded_byte_size, buffer);
  } else if (parquet_type == parquet::Type::BYTE_ARRAY) {
    int decimal_size = encoded_byte_size - sizeof(int32_t);
    memcpy(buffer, &decimal_size, sizeof(int32_t));
    DecimalUtil::EncodeToFixedLenByteArray(buffer + sizeof(int32_t), decimal_size, v);
    return encoded_byte_size;
  }
  return -1;
}

template<>
int Encode(const Decimal4Value& v, int encoded_byte_size, uint8_t* buffer,
    parquet::Type::type parquet_type) {
  return EncodeDecimal(v, encoded_byte_size, buffer, parquet_type);
}

template<>
int Encode(const Decimal8Value& v, int encoded_byte_size, uint8_t* buffer,
    parquet::Type::type parquet_type) {
  return EncodeDecimal(v, encoded_byte_size, buffer, parquet_type);
}

template<>
int Encode(const Decimal16Value& v, int encoded_byte_size, uint8_t* buffer,
    parquet::Type::type parquet_type){
  return EncodeDecimal(v, encoded_byte_size, buffer, parquet_type);
}

/// Test that the decoder fails when asked to decode a truncated value.
template <typename InternalType, parquet::Type::type PARQUET_TYPE>
void TestTruncate(const InternalType& v, int expected_byte_size) {
  uint8_t buffer[expected_byte_size];
  int encoded_size = Encode(v, expected_byte_size, buffer, PARQUET_TYPE);
  EXPECT_EQ(encoded_size, expected_byte_size);

  // Check all possible truncations of the buffer.
  for (int truncated_size = encoded_size - 1; truncated_size >= 0; --truncated_size) {
    InternalType result;
    /// Copy to heap-allocated buffer so that ASAN can detect buffer overruns.
    uint8_t* truncated_buffer = new uint8_t[truncated_size];
    memcpy(truncated_buffer, buffer, truncated_size);
    int decoded_size = ParquetPlainEncoder::Decode<InternalType, PARQUET_TYPE>(
        truncated_buffer, truncated_buffer + truncated_size, expected_byte_size, &result);
    EXPECT_EQ(-1, decoded_size);
    delete[] truncated_buffer;
  }
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE>
void TestType(const InternalType& v, int expected_byte_size) {
  uint8_t buffer[expected_byte_size];
  int encoded_size = Encode(v, expected_byte_size, buffer, PARQUET_TYPE);
  EXPECT_EQ(encoded_size, expected_byte_size);

  InternalType result;
  int decoded_size = ParquetPlainEncoder::Decode<InternalType, PARQUET_TYPE>(buffer,
      buffer + expected_byte_size, expected_byte_size, &result);
  EXPECT_EQ(decoded_size, expected_byte_size);
  EXPECT_EQ(result, v);

  TestTruncate<InternalType, PARQUET_TYPE>(v, expected_byte_size);
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

  TestType<int8_t, parquet::Type::INT32>(i8, sizeof(int32_t));
  TestType<int16_t, parquet::Type::INT32>(i16, sizeof(int32_t));
  TestType<int32_t, parquet::Type::INT32>(i32, sizeof(int32_t));
  TestType<int64_t, parquet::Type::INT64>(i64, sizeof(int64_t));
  TestType<float, parquet::Type::FLOAT>(f, sizeof(float));
  TestType<double, parquet::Type::DOUBLE>(d, sizeof(double));
  TestType<StringValue, parquet::Type::BYTE_ARRAY>(sv, sizeof(int32_t) + sv.len);
  TestType<TimestampValue, parquet::Type::INT96>(tv, 12);

  int test_val = 1234;
  int var_len_decimal_size = sizeof(int32_t)
      + 2 /*min bytes required for storing test_val*/;
  // Decimal4Value: General test case
  TestType<Decimal4Value, parquet::Type::BYTE_ARRAY>(Decimal4Value(test_val),
      var_len_decimal_size);
  TestType<Decimal4Value, parquet::Type::BYTE_ARRAY>(Decimal4Value(test_val * -1),
      var_len_decimal_size);
  TestType<Decimal4Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal4Value(test_val),
      sizeof(Decimal4Value));
  TestType<Decimal4Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
      Decimal4Value(test_val * -1), sizeof(Decimal4Value));

  // Decimal8Value: General test case
  TestType<Decimal8Value, parquet::Type::BYTE_ARRAY>(Decimal8Value(test_val),
      var_len_decimal_size);
  TestType<Decimal8Value, parquet::Type::BYTE_ARRAY>(Decimal8Value(test_val * -1),
      var_len_decimal_size);
  TestType<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal8Value(test_val),
      sizeof(Decimal8Value));
  TestType<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
      Decimal8Value(test_val * -1), sizeof(Decimal8Value));

  // Decimal16Value: General test case
  TestType<Decimal16Value, parquet::Type::BYTE_ARRAY>(Decimal16Value(test_val),
      var_len_decimal_size);
  TestType<Decimal16Value, parquet::Type::BYTE_ARRAY>(Decimal16Value(test_val * -1),
      var_len_decimal_size);
  TestType<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>( Decimal16Value(test_val),
      sizeof(Decimal16Value));
  TestType<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
      Decimal16Value(test_val * -1), sizeof(Decimal16Value));

  // Decimal8Value: int32 limits test
  TestType<Decimal8Value, parquet::Type::BYTE_ARRAY>(
      Decimal8Value(std::numeric_limits<int32_t>::max()),
      sizeof(int32_t) + sizeof(int32_t));
  TestType<Decimal8Value, parquet::Type::BYTE_ARRAY>(
      Decimal8Value(std::numeric_limits<int32_t>::min()),
      sizeof(int32_t) + sizeof(int32_t));
  TestType<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
      Decimal8Value(std::numeric_limits<int32_t>::max()), sizeof(Decimal8Value));
  TestType<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
      Decimal8Value(std::numeric_limits<int32_t>::min()), sizeof(Decimal8Value));

  // Decimal16Value: int32 limits test
  TestType<Decimal16Value, parquet::Type::BYTE_ARRAY>(
      Decimal16Value(std::numeric_limits<int32_t>::max()),
      sizeof(int32_t) + sizeof(int32_t));
  TestType<Decimal16Value, parquet::Type::BYTE_ARRAY>(
      Decimal16Value(std::numeric_limits<int32_t>::min()),
      sizeof(int32_t) + sizeof(int32_t));
  TestType<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
      Decimal16Value(std::numeric_limits<int32_t>::max()), sizeof(Decimal16Value));
  TestType<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
      Decimal16Value(std::numeric_limits<int32_t>::min()), sizeof(Decimal16Value));

  // Decimal16Value: int64 limits test
  TestType<Decimal16Value, parquet::Type::BYTE_ARRAY>(
      Decimal16Value(std::numeric_limits<int64_t>::max()),
      sizeof(int32_t) + sizeof(int64_t));
  TestType<Decimal16Value, parquet::Type::BYTE_ARRAY>(
      Decimal16Value(std::numeric_limits<int64_t>::min()),
      sizeof(int32_t) + sizeof(int64_t));
  TestType<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
      Decimal16Value(std::numeric_limits<int64_t>::max()), sizeof(Decimal16Value));
  TestType<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
      Decimal16Value(std::numeric_limits<int64_t>::min()), sizeof(Decimal16Value));

  // two digit values can be encoded with any byte size.
  for (int i = 1; i <=16; ++i) {
    if (i <= 4) {
      TestType<Decimal4Value, parquet::Type::BYTE_ARRAY>(Decimal4Value(i),
          i + sizeof(int32_t));
      TestType<Decimal4Value, parquet::Type::BYTE_ARRAY>(Decimal4Value(-i),
          i + sizeof(int32_t));
      TestType<Decimal4Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal4Value(i), i);
      TestType<Decimal4Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal4Value(-i), i);
    }
    if (i <= 8) {
      TestType<Decimal8Value, parquet::Type::BYTE_ARRAY>(Decimal8Value(i),
          i + sizeof(int32_t));
      TestType<Decimal8Value, parquet::Type::BYTE_ARRAY>(Decimal8Value(-i),
          i + sizeof(int32_t));
      TestType<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal8Value(i), i);
      TestType<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal8Value(-i), i);
    }
    TestType<Decimal16Value, parquet::Type::BYTE_ARRAY>(Decimal16Value(i),
        i + sizeof(int32_t));
    TestType<Decimal16Value, parquet::Type::BYTE_ARRAY>(Decimal16Value(-i),
        i + sizeof(int32_t));
    TestType<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal16Value(i), i);
    TestType<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal16Value(-i), i);
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

  int size = ParquetPlainEncoder::Encode(d4, sizeof(d4), result_buffer);
  ASSERT_EQ(size, sizeof(d4));
  ASSERT_EQ(memcmp(result_buffer, buffer_swapped + 16 - sizeof(d4), sizeof(d4)), 0);

  size = ParquetPlainEncoder::Encode(d8, sizeof(d8), result_buffer);
  ASSERT_EQ(size, sizeof(d8));
  ASSERT_EQ(memcmp(result_buffer, buffer_swapped + 16 - sizeof(d8), sizeof(d8)), 0);

  size = ParquetPlainEncoder::Encode(d16, sizeof(d16), result_buffer);
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
  int decoded_size = ParquetPlainEncoder::Decode<StringValue, parquet::Type::BYTE_ARRAY>(
      buffer, buffer + sizeof(buffer), 0, &result);
  EXPECT_EQ(decoded_size, -1);
}

}

IMPALA_TEST_MAIN();
