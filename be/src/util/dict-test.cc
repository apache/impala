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

#include "util/dict-encoding.h"
#include "runtime/timestamp-value.h"

using namespace std;

namespace impala {

template<typename T>
void ValidateDict(const vector<T>& values) {
  set<T> values_set(values.begin(), values.end());

  DictEncoder<T> encoder(/* mem limits */ NULL);
  BOOST_FOREACH(T i, values) {
    encoder.Put(i);
  }
  EXPECT_EQ(encoder.num_entries(), values_set.size());

  uint8_t dict_buffer[encoder.dict_encoded_size()];
  encoder.WriteDict(dict_buffer);

  int data_buffer_len = encoder.EstimatedDataEncodedSize() * 2;
  uint8_t data_buffer[data_buffer_len];
  int data_len = encoder.WriteData(data_buffer, data_buffer_len);
  EXPECT_GT(data_len, 0);

  DictDecoder<T> decoder(dict_buffer, encoder.dict_encoded_size());
  decoder.SetData(data_buffer, data_len);
  BOOST_FOREACH(T i, values) {
    T j;
    decoder.GetValue(&j);
    EXPECT_EQ(i, j);
  }
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

  ValidateDict(values);
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

  ValidateDict(values);
}

template<typename T>
void TestNumbers(int max_value, int repeat) {
  vector<T> values;
  for (T val = 0; val < max_value; ++val) {
    for (int i = 0; i < repeat; ++i) {
      values.push_back(val);
    }
  }
  ValidateDict(values);
}

template<typename T>
void TestNumbers() {
  TestNumbers<T>(100, 1);
  TestNumbers<T>(1, 100);
  TestNumbers<T>(1, 1);
  TestNumbers<T>(1, 2);
}

TEST(DictTest, TestNumbers) {
  TestNumbers<int8_t>();
  TestNumbers<int16_t>();
  TestNumbers<int32_t>();
  TestNumbers<int64_t>();
  TestNumbers<float>();
  TestNumbers<double>();
}

}

int main(int argc, char **argv) {
  impala::CpuInfo::Init();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
