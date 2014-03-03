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

#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include "common/init.h"
#include "runtime/mem-tracker.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "util/dict-encoding.h"

using namespace std;

namespace impala {

template<typename T>
void ValidateDict(const vector<T>& values, int fixed_buffer_byte_size) {
  set<T> values_set(values.begin(), values.end());

  MemTracker tracker;
  MemPool pool(&tracker);
  DictEncoder<T> encoder(&pool, fixed_buffer_byte_size);
  BOOST_FOREACH(T i, values) {
    encoder.Put(i);
  }
  EXPECT_EQ(encoder.num_entries(), values_set.size());

  uint8_t dict_buffer[encoder.dict_encoded_size()];
  encoder.WriteDict(dict_buffer);

  int data_buffer_len = encoder.EstimatedDataEncodedSize();
  uint8_t data_buffer[data_buffer_len];
  int data_len = encoder.WriteData(data_buffer, data_buffer_len);
  EXPECT_GT(data_len, 0);
  encoder.ClearIndices();

  DictDecoder<T> decoder(
      dict_buffer, encoder.dict_encoded_size(), fixed_buffer_byte_size);
  decoder.SetData(data_buffer, data_len);
  BOOST_FOREACH(T i, values) {
    T j;
    decoder.GetValue(&j);
    EXPECT_EQ(i, j);
  }
  pool.FreeAll();
}

TEST(DictTest, TestStrings) {
  StringValue sv1("hello world");
  StringValue sv2("foo");
  StringValue sv3("bar");
  StringValue sv4("baz");

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

  ValidateDict(values, -1);
}

TEST(DictTest, TestTimestamps) {
  TimestampValue tv1("2011-01-01 09:01:01");
  TimestampValue tv2("2012-01-01 09:01:01");
  TimestampValue tv3("2011-01-01 09:01:02");

  vector<TimestampValue> values;
  values.push_back(tv1);
  values.push_back(tv2);
  values.push_back(tv3);
  values.push_back(tv1);
  values.push_back(tv1);
  values.push_back(tv1);

  ValidateDict(values, ParquetPlainEncoder::ByteSize(ColumnType(TYPE_TIMESTAMP)));
}

template<typename T>
void IncrementValue(T* t) { ++(*t); }

template <> void IncrementValue(Decimal4Value* t) { ++(t->value()); }
template <> void IncrementValue(Decimal8Value* t) { ++(t->value()); }
template <> void IncrementValue(Decimal16Value* t) { ++(t->value()); }

template<typename T>
void TestNumbers(int max_value, int repeat, int value_byte_size) {
  vector<T> values;
  for (T val = 0; val < max_value; IncrementValue(&val)) {
    for (int i = 0; i < repeat; ++i) {
      values.push_back(val);
    }
  }
  ValidateDict(values, value_byte_size);
}

template<typename T>
void TestNumbers(int value_byte_size) {
  TestNumbers<T>(100, 1, value_byte_size);
  TestNumbers<T>(1, 100, value_byte_size);
  TestNumbers<T>(1, 1, value_byte_size);
  TestNumbers<T>(1, 2, value_byte_size);
}

TEST(DictTest, TestNumbers) {
  TestNumbers<int8_t>(ParquetPlainEncoder::ByteSize(ColumnType(TYPE_TINYINT)));
  TestNumbers<int16_t>(ParquetPlainEncoder::ByteSize(ColumnType(TYPE_SMALLINT)));
  TestNumbers<int32_t>(ParquetPlainEncoder::ByteSize(ColumnType(TYPE_INT)));
  TestNumbers<int64_t>(ParquetPlainEncoder::ByteSize(ColumnType(TYPE_BIGINT)));
  TestNumbers<float>(ParquetPlainEncoder::ByteSize(ColumnType(TYPE_FLOAT)));
  TestNumbers<double>(ParquetPlainEncoder::ByteSize(ColumnType(TYPE_DOUBLE)));

  for (int i = 1; i <=16; ++i) {
    if (i <= 4) TestNumbers<Decimal4Value>(i);
    if (i <= 8) TestNumbers<Decimal8Value>(i);
    TestNumbers<Decimal16Value>(i);
  }
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true);
  return RUN_ALL_TESTS();
}
