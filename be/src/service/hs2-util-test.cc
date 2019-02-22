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

#include "service/hs2-util.h"

#include <string>
#include <utility>

#include "common/init.h"
#include "common/names.h"
#include "testutil/gtest-util.h"

using namespace impala;
using namespace std;

// Test that a single byte can be stitched to an empty string at all offsets.
TEST(StitchNullsTest, OneByteStitch) {
  string from;
  from += 0xF;
  for (int i = 0; i < 8; ++i) {
    string to;
    StitchNulls(0, 8 - i, i, from, &to);
    ASSERT_TRUE(to.size() == 1);
    ASSERT_EQ(to[0], 0xF >> i);
  }
}

// Test that a bit string of more than one byte is stitched in correctly.
TEST(StitchNullsTest, MultiByteStitch) {
  string from;
  from += 0xFF;
  from += 0xFF;
  for (int i = 0; i < 16; ++i) {
    string to;
    // Stitch any from 1-16 bits, starting at the i'th bit in from.
    StitchNulls(0, 16 - i, i, from, &to);
    if (i < 8) {
      // For more than 8 bits added, the result should be two bytes long.
      ASSERT_EQ(to.size(), 2);
      ASSERT_EQ(to[0], (char)0xFF);
      ASSERT_EQ(to[1], (char)(0xFF >> i));
    } else {
      // For the first 8 bits, the result should be less than one byte long.
      ASSERT_EQ(to.size(), 1);
      ASSERT_EQ(to[0], (char)(0xFF >> (i - 8)));
    }
  }
}

// Test stitching two bitstrings whose combined length is still less than a byte.
TEST(StitchNullsTest, StitchOverlapping) {
  string from;
  from += 0x1;
  for (int i = 1; i < 9; ++i) {
    string to;
    to += 0x1;
    // Result's first byte should always be 1100 0000 (LSB->MSB). Once i is larger than 7,
    // an extra byte will be needed.
    StitchNulls(1, i, 0, from, &to);
    ASSERT_EQ(to[0], 0x3);
    if (i < 8) {
      ASSERT_EQ(to.size(), 1);
    } else {
      ASSERT_EQ(to.size(), 2);
      ASSERT_EQ(to[1], 0x0);
    }
  }
}

// Test stitching in a multi-byte bit string with an offset; i.e. not starting at the 0'th
// bit.
TEST(StitchNullsTest, StitchWithOffset) {
  string from;
  from += 0x1;
  from += 0x2;
  from += 0x4;
  from += 0x8;

  for (int i = 0; i < 4; ++i) {
    string to;
    to += 0x1;
    StitchNulls(8, 8, 8 * i, from, &to);
    ASSERT_EQ(to.size(), 2);
    ASSERT_EQ(to[1], from[i]);
  }

  for (int i = 0; i < 4; ++i) {
    string to;
    to += 0x1;
    // Add one bit, starting at the least-significant set-bit in the i'th byte of
    // 'from'. The effect is to always append exactly one bit.
    StitchNulls(1, 1, (8 * i) + i, from, &to);
    ASSERT_EQ(to.size(), 1);
    // Result is always 1100 0000 (LSB->MSB)
    ASSERT_EQ(to[0], 0x3);
  }
}

TEST(PrintTColumnValueTest, TestAllTypes) {
  using namespace apache::hive::service::cli::thrift;

  vector<pair<apache::hive::service::cli::thrift::TColumnValue, string>> values;
  values.resize(9);

  values[0].first.__set_boolVal(TBoolValue());
  values[0].first.boolVal.__set_value(false);
  values[0].second = "false";

  values[1].first.__set_boolVal(TBoolValue());
  values[1].first.boolVal.__set_value(true);
  values[1].second = "true";

  values[2].first.__set_doubleVal(TDoubleValue());
  values[2].first.doubleVal.__set_value(1.23);
  values[2].second = "1.23";

  values[3].first.__set_byteVal(TByteValue());
  values[3].first.byteVal.__set_value(12);
  values[3].second = "12";

  values[4].first.__set_i16Val(TI16Value());
  values[4].first.i16Val.__set_value(123);
  values[4].second = "123";

  values[5].first.__set_i32Val(TI32Value());
  values[5].first.i32Val.__set_value(1234);
  values[5].second = "1234";

  values[6].first.__set_i64Val(TI64Value());
  values[6].first.i64Val.__set_value(12345);
  values[6].second = "12345";

  values[7].first.__set_stringVal(TStringValue());
  values[7].first.stringVal.__set_value("hello world");
  values[7].second = "hello world";

  values[8].first.__set_i64Val(TI64Value());
  values[8].second = "NULL";

  for (int i = 0; i < values.size(); ++i) {
    stringstream ss;
    PrintTColumnValue(values[i].first, &ss);
    ASSERT_EQ(ss.str(), values[i].second);
  }
}

