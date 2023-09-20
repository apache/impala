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

#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <iostream>
#include "exec/parquet/parquet-common.h"
#include "runtime/decimal-value.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "testutil/gtest-util.h"
#include "testutil/random-vector-generators.h"
#include "testutil/rand-util.h"

#include "common/names.h"

namespace impala {

template <typename InternalType>
int Encode(const InternalType& v, int encoded_byte_size, uint8_t* buffer,
    parquet::Type::type physical_type){
  return ParquetPlainEncoder::Encode(v, encoded_byte_size, buffer);
}

// Handle special case of encoding decimal types stored as BYTE_ARRAY, INT32, and INT64,
// since these are not implemented in Impala.
// When parquet_type equals BYTE_ARRAY: 'encoded_byte_size' is the sum of the
// minimum number of bytes required to store the unscaled value and the bytes required to
// store the size. Value 'v' passed to it should not contain leading zeros as this
// method does not strictly conform to the parquet spec in removing those.
// When parquet_type is INT32 or INT64, we simply write the unscaled value to the buffer.
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
  } else if (parquet_type == parquet::Type::INT32 ||
             parquet_type == parquet::Type::INT64) {
    return ParquetPlainEncoder::Encode(v.value(), encoded_byte_size, buffer);
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
/// This function can be used for type widening tests but also tests without type
/// widening, in which case `WidenInternalType` is the same as `InternalType`.
template <typename InternalType, typename WidenInternalType,
    parquet::Type::type PARQUET_TYPE>
void TestTruncate(const InternalType& v, int expected_byte_size) {
  uint8_t buffer[expected_byte_size];
  int encoded_size = Encode(v, expected_byte_size, buffer, PARQUET_TYPE);
  EXPECT_EQ(encoded_size, expected_byte_size);

  // Check all possible truncations of the buffer.
  for (int truncated_size = encoded_size - 1; truncated_size >= 0; --truncated_size) {
    WidenInternalType result;
    /// Copy to heap-allocated buffer so that ASAN can detect buffer overruns.
    uint8_t* truncated_buffer = new uint8_t[truncated_size];
    memcpy(truncated_buffer, buffer, truncated_size);
    int decoded_size = ParquetPlainEncoder::Decode<WidenInternalType, PARQUET_TYPE>(
        truncated_buffer, truncated_buffer + truncated_size, expected_byte_size, &result);
    EXPECT_EQ(-1, decoded_size);
    delete[] truncated_buffer;
  }
}

/// This function can be used for type widening tests but also tests without type
/// widening, in which case `WidenInternalType` is the same as `InternalType`.
template <typename InternalType, typename WidenInternalType,
    parquet::Type::type PARQUET_TYPE>
void TestTypeWidening(const InternalType& v, int expected_byte_size) {
  uint8_t buffer[expected_byte_size];
  int encoded_size = Encode(v, expected_byte_size, buffer, PARQUET_TYPE);
  EXPECT_EQ(encoded_size, expected_byte_size);

  WidenInternalType result;
  int decoded_size = ParquetPlainEncoder::Decode<WidenInternalType, PARQUET_TYPE>(
      buffer, buffer + expected_byte_size, expected_byte_size, &result);
  EXPECT_EQ(expected_byte_size, decoded_size);
  EXPECT_EQ(v, result);

  WidenInternalType batch_result;
  int batch_decoded_size
      = ParquetPlainEncoder::DecodeBatch<WidenInternalType, PARQUET_TYPE>(
          buffer, buffer + expected_byte_size, expected_byte_size, 1,
          sizeof(WidenInternalType), &batch_result);
  EXPECT_EQ(expected_byte_size, batch_decoded_size);
  EXPECT_EQ(v, batch_result);

  TestTruncate<InternalType, WidenInternalType, PARQUET_TYPE>(
      v, expected_byte_size);
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE>
void TestType(const InternalType& v, int expected_byte_size) {
  return TestTypeWidening<InternalType, InternalType, PARQUET_TYPE>(
      v, expected_byte_size);
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
  TestType<StringValue, parquet::Type::BYTE_ARRAY>(sv, sizeof(int32_t) + sv.Len());
  TestType<TimestampValue, parquet::Type::INT96>(tv, 12);

  // Test type widening.
  TestTypeWidening<int32_t, int64_t, parquet::Type::INT32>(i32, sizeof(int32_t));
  TestTypeWidening<int32_t, double, parquet::Type::INT32>(i32, sizeof(int32_t));
  TestTypeWidening<float, double, parquet::Type::FLOAT>(f, sizeof(float));

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
  TestType<Decimal4Value, parquet::Type::INT32>(Decimal4Value(test_val),
      sizeof(int32_t));
  TestType<Decimal4Value, parquet::Type::INT32>(Decimal4Value(test_val * -1),
      sizeof(int32_t));

  // Decimal8Value: General test case
  TestType<Decimal8Value, parquet::Type::BYTE_ARRAY>(Decimal8Value(test_val),
      var_len_decimal_size);
  TestType<Decimal8Value, parquet::Type::BYTE_ARRAY>(Decimal8Value(test_val * -1),
      var_len_decimal_size);
  TestType<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal8Value(test_val),
      sizeof(Decimal8Value));
  TestType<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
      Decimal8Value(test_val * -1), sizeof(Decimal8Value));
  TestType<Decimal8Value, parquet::Type::INT64>(Decimal8Value(test_val),
      sizeof(int64_t));
  TestType<Decimal8Value, parquet::Type::INT64>(Decimal8Value(test_val * -1),
      sizeof(int64_t));

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
  TestType<Decimal8Value, parquet::Type::INT64>(
      Decimal8Value(std::numeric_limits<int32_t>::max()), sizeof(int64_t));
  TestType<Decimal8Value, parquet::Type::INT64>(
      Decimal8Value(std::numeric_limits<int32_t>::min()), sizeof(int64_t));

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
      TestType<Decimal4Value, parquet::Type::INT32>(Decimal4Value(i), sizeof(int32_t));
      TestType<Decimal4Value, parquet::Type::INT32>(Decimal4Value(-i), sizeof(int32_t));
    }
    if (i <= 8) {
      TestType<Decimal8Value, parquet::Type::BYTE_ARRAY>(Decimal8Value(i),
          i + sizeof(int32_t));
      TestType<Decimal8Value, parquet::Type::BYTE_ARRAY>(Decimal8Value(-i),
          i + sizeof(int32_t));
      TestType<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal8Value(i), i);
      TestType<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal8Value(-i), i);
      TestType<Decimal8Value, parquet::Type::INT64>(Decimal8Value(i), sizeof(int64_t));
      TestType<Decimal8Value, parquet::Type::INT64>(Decimal8Value(-i), sizeof(int64_t));
    }
    TestType<Decimal16Value, parquet::Type::BYTE_ARRAY>(Decimal16Value(i),
        i + sizeof(int32_t));
    TestType<Decimal16Value, parquet::Type::BYTE_ARRAY>(Decimal16Value(-i),
        i + sizeof(int32_t));
    TestType<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal16Value(i), i);
    TestType<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(Decimal16Value(-i), i);
  }
}

template <typename InputType, typename OutputType>
void ExpectEqualWithStride(const std::vector<InputType>& input,
    const std::vector<uint8_t>& output, int stride) {
  ASSERT_EQ(input.size() * stride, output.size());

  for (int i = 0; i < input.size(); i++) {
    const InputType& input_value = input[i];
    OutputType output_value;

    memcpy(&output_value, &output[i * stride], sizeof(OutputType));
    EXPECT_EQ(input_value, output_value);
  }
}

/// This function can be used for type widening tests but also tests without type
/// widening, in which case `WidenInternalType` is the same as `InternalType`.
template <typename InternalType, typename WidenInternalType,
         parquet::Type::type PARQUET_TYPE>
void TestTypeWideningBatch(const std::vector<InternalType>& values,
    int expected_byte_size, int stride) {
  ASSERT_GE(stride, sizeof(WidenInternalType));

  constexpr bool var_length = PARQUET_TYPE == parquet::Type::BYTE_ARRAY;

  std::vector<uint8_t> buffer(values.size() * expected_byte_size, 0);
  uint8_t* output_pos = buffer.data();
  for (int i = 0; i < values.size(); i++) {
    int encoded_size = Encode(values[i], expected_byte_size, output_pos, PARQUET_TYPE);
    if (var_length) {
      /// For variable length types, the size is variable and `expected_byte_size` should
      /// be the maximum.
      EXPECT_GE(expected_byte_size, encoded_size);
    } else {
      EXPECT_EQ(expected_byte_size, encoded_size);
    }

    output_pos += encoded_size;
  }

  /// Decode one by one.
  std::vector<uint8_t> output_1by1(values.size() * stride);
  uint8_t* input_pos = buffer.data();
  for (int i = 0; i < values.size(); i++) {
    WidenInternalType* dest = reinterpret_cast<WidenInternalType*>(
        &output_1by1[i * stride]);
    int decoded_size = ParquetPlainEncoder::Decode<WidenInternalType, PARQUET_TYPE>(
        input_pos, buffer.data() + buffer.size(), expected_byte_size, dest);
    if (var_length) {
      EXPECT_GE(expected_byte_size, decoded_size);
    } else {
      EXPECT_EQ(expected_byte_size, decoded_size);
    }

    input_pos += decoded_size;
  }

  ExpectEqualWithStride<InternalType, WidenInternalType>(values, output_1by1, stride);

  /// Decode in batch.
  std::vector<uint8_t> output_batch(values.size() * stride);
  int decoded_size = ParquetPlainEncoder::DecodeBatch<WidenInternalType, PARQUET_TYPE>(
      buffer.data(), buffer.data() + buffer.size(), expected_byte_size, values.size(),
      stride, reinterpret_cast<WidenInternalType*>(output_batch.data()));
  if (var_length) {
    EXPECT_GE(buffer.size(), decoded_size);
  } else {
    EXPECT_EQ(buffer.size(), decoded_size);
  }

  ExpectEqualWithStride<InternalType, WidenInternalType>(values, output_batch, stride);
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE>
void TestTypeBatch(const std::vector<InternalType>& values, int expected_byte_size,
    int stride) {
  return TestTypeWideningBatch<InternalType, InternalType, PARQUET_TYPE>(values,
      expected_byte_size, stride);
}

TEST(PlainEncoding, Batch) {
  std::mt19937 gen;
  RandTestUtil::SeedRng("PARQUET_PLAIN_ENCODING_TEST_RANDOM_SEED", &gen);

  constexpr int NUM_ELEMENTS = 1024 * 5 + 10;
  constexpr int stride = 20;

  const std::vector<int8_t> int8_vec = RandomNumberVec<int8_t>(gen, NUM_ELEMENTS);
  TestTypeBatch<int8_t, parquet::Type::INT32>(int8_vec, sizeof(int32_t), stride);

  const std::vector<int16_t> int16_vec = RandomNumberVec<int16_t>(gen, NUM_ELEMENTS);
  TestTypeBatch<int16_t, parquet::Type::INT32>(int16_vec, sizeof(int32_t), stride);

  const std::vector<int32_t> int32_vec = RandomNumberVec<int32_t>(gen, NUM_ELEMENTS);
  TestTypeBatch<int32_t, parquet::Type::INT32>(int32_vec, sizeof(int32_t), stride);

  const std::vector<int64_t> int64_vec = RandomNumberVec<int64_t>(gen, NUM_ELEMENTS);
  TestTypeBatch<int64_t, parquet::Type::INT64>(int64_vec, sizeof(int64_t), stride);

  const std::vector<float> float_vec = RandomNumberVec<float>(gen, NUM_ELEMENTS);
  TestTypeBatch<float, parquet::Type::FLOAT>(float_vec, sizeof(float), stride);

  const std::vector<double> double_vec = RandomNumberVec<double>(gen, NUM_ELEMENTS);
  TestTypeBatch<double, parquet::Type::DOUBLE>(double_vec, sizeof(double), stride);

  constexpr int max_str_length = 100;
  const std::vector<std::string> str_vec = RandomStrVec(gen, NUM_ELEMENTS,
      max_str_length);
  std::vector<StringValue> sv_vec(str_vec.size());
  std::transform(str_vec.begin(), str_vec.end(), sv_vec.begin(),
      [] (const std::string& s) { return StringValue(s); });
  TestTypeBatch<StringValue, parquet::Type::BYTE_ARRAY>(sv_vec,
      sizeof(int32_t) + max_str_length, stride);

  const std::vector<TimestampValue> tv_vec = RandomTimestampVec(gen, NUM_ELEMENTS);
  TestTypeBatch<TimestampValue, parquet::Type::INT96>(tv_vec, 12, stride);

  // Test type widening.
  TestTypeWideningBatch<int32_t, int64_t, parquet::Type::INT32>(int32_vec,
      sizeof(int32_t), stride);
  TestTypeWideningBatch<int32_t, double, parquet::Type::INT32>(int32_vec, sizeof(int32_t),
      stride);
  TestTypeWideningBatch<float, double, parquet::Type::FLOAT>(float_vec, sizeof(float),
      stride);

  // In the Decimal batch tests, when writing the decimals as BYTE_ARRAYs, we always use
  // the size of the underlying type as the array length for simplicity.
  // The non-batch tests take care of storing them on as many bytes as needed.

  // BYTE_ARRAYs store the length of the array on 4 bytes.
  constexpr int decimal_size_bytes = sizeof(int32_t);

  // Decimal4Value
  std::vector<Decimal4Value> decimal4_vec(int32_vec.size());
  std::transform(int32_vec.begin(), int32_vec.end(), decimal4_vec.begin(),
      [] (const int32_t i) { return Decimal4Value(i); });

  TestTypeBatch<Decimal4Value, parquet::Type::BYTE_ARRAY>(decimal4_vec,
      decimal_size_bytes + sizeof(Decimal4Value::StorageType), stride);
  TestTypeBatch<Decimal4Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(decimal4_vec,
      sizeof(Decimal4Value::StorageType), stride);
  TestTypeBatch<Decimal4Value, parquet::Type::INT32>(decimal4_vec,
      sizeof(Decimal4Value::StorageType), stride);

  // Decimal8Value
  std::vector<Decimal8Value> decimal8_vec(int64_vec.size());
  std::transform(int64_vec.begin(), int64_vec.end(), decimal8_vec.begin(),
      [] (const int64_t i) { return Decimal8Value(i); });

  TestTypeBatch<Decimal8Value, parquet::Type::BYTE_ARRAY>(decimal8_vec,
      decimal_size_bytes + sizeof(Decimal8Value::StorageType), stride);
  TestTypeBatch<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(decimal8_vec,
      sizeof(Decimal8Value::StorageType), stride);
  TestTypeBatch<Decimal8Value, parquet::Type::INT64>(decimal8_vec,
      sizeof(Decimal8Value::StorageType), stride);

  // Decimal16Value
  // We do not test the whole 16 byte range as generating random int128_t values is
  // complicated.
  std::vector<Decimal16Value> decimal16_vec(int64_vec.size());
  std::transform(int64_vec.begin(), int64_vec.end(), decimal16_vec.begin(),
      [] (const int64_t i) { return Decimal16Value(i); });

  TestTypeBatch<Decimal16Value, parquet::Type::BYTE_ARRAY>(decimal16_vec,
      decimal_size_bytes + sizeof(Decimal16Value::StorageType), stride);
  TestTypeBatch<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(decimal16_vec,
      sizeof(Decimal16Value::StorageType), stride);
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

