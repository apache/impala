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

#include "runtime/mem-tracker.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "testutil/gtest-util.h"
#include "util/dict-encoding.h"

#include "common/names.h"

namespace impala {

template<typename InternalType, parquet::Type::type PARQUET_TYPE>
void ValidateDict(const vector<InternalType>& values,
    const vector<InternalType>& dict_values, int fixed_buffer_byte_size) {
  set<InternalType> values_set(values.begin(), values.end());

  MemTracker tracker;
  MemPool pool(&tracker);
  DictEncoder<InternalType> encoder(&pool, fixed_buffer_byte_size);
  for (InternalType i: values) encoder.Put(i);
  EXPECT_EQ(encoder.num_entries(), values_set.size());

  uint8_t dict_buffer[encoder.dict_encoded_size()];
  encoder.WriteDict(dict_buffer);

  int data_buffer_len = encoder.EstimatedDataEncodedSize();
  uint8_t data_buffer[data_buffer_len];
  int data_len = encoder.WriteData(data_buffer, data_buffer_len);
  EXPECT_GT(data_len, 0);
  encoder.ClearIndices();

  DictDecoder<InternalType> decoder;
  ASSERT_TRUE(decoder.template Reset<PARQUET_TYPE>(dict_buffer,
      encoder.dict_encoded_size(),fixed_buffer_byte_size));

  // Test direct access to the dictionary via indexes
  for (int i = 0; i < dict_values.size(); ++i) {
    InternalType expected_value = dict_values[i];
    InternalType out_value;

    decoder.GetValue(i, &out_value);
    EXPECT_EQ(expected_value, out_value);
  }
  // Test access to dictionary via internal stream
  ASSERT_OK(decoder.SetData(data_buffer, data_len));
  for (InternalType i: values) {
    InternalType j;
    decoder.GetNextValue(&j);
    EXPECT_EQ(i, j);
  }
  pool.FreeAll();
}

TEST(DictTest, TestStrings) {
  StringValue sv1("hello world");
  StringValue sv2("foo");
  StringValue sv3("bar");
  StringValue sv4("baz");

  vector<StringValue> dict_values;
  dict_values.push_back(sv1);
  dict_values.push_back(sv2);
  dict_values.push_back(sv3);
  dict_values.push_back(sv4);

  vector<StringValue> values;
  values.push_back(sv1);
  values.push_back(sv1);
  values.push_back(sv1);
  values.push_back(sv2);
  values.push_back(sv1);
  values.push_back(sv2);
  values.push_back(sv2);
  values.push_back(sv3);
  values.push_back(sv3);
  values.push_back(sv3);
  values.push_back(sv4);

  ValidateDict<StringValue, parquet::Type::BYTE_ARRAY>(values, dict_values, -1);
}

TEST(DictTest, TestTimestamps) {
  TimestampValue tv1 = TimestampValue::Parse("2011-01-01 09:01:01");
  TimestampValue tv2 = TimestampValue::Parse("2012-01-01 09:01:01");
  TimestampValue tv3 = TimestampValue::Parse("2011-01-01 09:01:02");

  vector<TimestampValue> dict_values;
  dict_values.push_back(tv1);
  dict_values.push_back(tv2);
  dict_values.push_back(tv3);

  vector<TimestampValue> values;
  values.push_back(tv1);
  values.push_back(tv2);
  values.push_back(tv3);
  values.push_back(tv1);
  values.push_back(tv1);
  values.push_back(tv1);

  ValidateDict<TimestampValue, parquet::Type::INT96>(values, dict_values,
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_TIMESTAMP)));
}

template<typename InternalType>
void IncrementValue(InternalType* t) { ++(*t); }

template <> void IncrementValue(Decimal4Value* t) { ++(t->value()); }
template <> void IncrementValue(Decimal8Value* t) { ++(t->value()); }
template <> void IncrementValue(Decimal16Value* t) { ++(t->value()); }

template<typename InternalType, parquet::Type::type PARQUET_TYPE>
void TestNumbers(int max_value, int repeat, int value_byte_size) {
  vector<InternalType> values;
  vector<InternalType> dict_values;
  for (InternalType val = 0; val < max_value; IncrementValue(&val)) {
    for (int i = 0; i < repeat; ++i) {
      values.push_back(val);
    }
    dict_values.push_back(val);
  }

  ValidateDict<InternalType, PARQUET_TYPE>(values, dict_values, value_byte_size);
}

template<typename InternalType, parquet::Type::type PARQUET_TYPE>
void TestNumbers(int value_byte_size) {
  TestNumbers<InternalType, PARQUET_TYPE>(100, 1, value_byte_size);
  TestNumbers<InternalType, PARQUET_TYPE>(1, 100, value_byte_size);
  TestNumbers<InternalType, PARQUET_TYPE>(1, 1, value_byte_size);
  TestNumbers<InternalType, PARQUET_TYPE>(1, 2, value_byte_size);
}

TEST(DictTest, TestNumbers) {
  TestNumbers<int8_t, parquet::Type::INT32>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_TINYINT)));
  TestNumbers<int16_t, parquet::Type::INT32>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_SMALLINT)));
  TestNumbers<int32_t, parquet::Type::INT32>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_INT)));
  TestNumbers<int64_t, parquet::Type::INT64>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_BIGINT)));
  TestNumbers<float, parquet::Type::FLOAT>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_FLOAT)));
  TestNumbers<double, parquet::Type::DOUBLE>(
      ParquetPlainEncoder::EncodedByteSize(ColumnType(TYPE_DOUBLE)));

  for (int i = 1; i <= 16; ++i) {
    if (i <= 4) TestNumbers<Decimal4Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(i);
    if (i <= 8) TestNumbers<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(i);
    TestNumbers<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(i);
  }
}

TEST(DictTest, TestInvalidStrings) {
  uint8_t buffer[sizeof(int32_t) + 10];
  int32_t len = -10;
  memcpy(buffer, &len, sizeof(int32_t));

  // Test a dictionary with a string encoded with negative length. Initializing
  // the decoder should fail.
  DictDecoder<StringValue> decoder;
  ASSERT_FALSE(decoder.template Reset<parquet::Type::BYTE_ARRAY>(buffer, sizeof(buffer),
      0));
}

TEST(DictTest, TestStringBufferOverrun) {
  // Test string length past end of buffer.
  uint8_t buffer[sizeof(int32_t) + 10];
  int32_t len = 100;
  memcpy(buffer, &len, sizeof(int32_t));

  // Initializing the dictionary should fail, since the string would reference
  // invalid memory.
  DictDecoder<StringValue> decoder;
  ASSERT_FALSE(decoder.template Reset<parquet::Type::BYTE_ARRAY>(buffer, sizeof(buffer),
      0));
}

}

IMPALA_TEST_MAIN();
