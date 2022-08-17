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
#include <cstdint>
#include <iostream>
#include <limits>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include "runtime/decimal-value.inline.h"
#include "runtime/raw-value.h"
#include "runtime/types.h"
#include "testutil/gtest-util.h"
#include "util/decimal-util.h"
#include "util/string-parser.h"

#include "common/names.h"

using std::max;
using std::min;

namespace impala {

// Compare decimal result against double.
static const double MAX_ERROR = 0.00005;

template <typename T>
void VerifyEquals(const DecimalValue<T>& t1, const DecimalValue<T>& t2) {
  if (t1 != t2) {
    LOG(ERROR) << t1 << " != " << t2;
    EXPECT_TRUE(false);
  }
}

template <typename T>
void VerifyParse(const string& s, int precision, int scale, bool round,
    const DecimalValue<T>& expected_val, StringParser::ParseResult expected_result) {
  StringParser::ParseResult parse_result;
  DecimalValue<T> val = StringParser::StringToDecimal<T>(
      s.c_str(), s.size(), precision, scale, round, &parse_result);
  EXPECT_EQ(expected_result, parse_result) << "Failed test string: " << s;
  if (expected_result == StringParser::PARSE_SUCCESS ||
      expected_result == StringParser::PARSE_UNDERFLOW) {
    VerifyEquals(expected_val, val);
  }
}

template <typename T>
void VerifyParse(const string& s, int precision, int scale,
    const DecimalValue<T>& expected_val, StringParser::ParseResult expected_result) {
  VerifyParse(s, precision, scale, false, expected_val, expected_result);
  VerifyParse(s, precision, scale, true, expected_val, expected_result);
}

template <typename T>
void VerifyParse(const string& s, int precision, int scale,
    const DecimalValue<T>& expected_val_v1, StringParser::ParseResult expected_result_v1,
    const DecimalValue<T>& expected_val_v2, StringParser::ParseResult expected_result_v2)
{
  VerifyParse(s, precision, scale, false, expected_val_v1, expected_result_v1);
  VerifyParse(s, precision, scale, true, expected_val_v2, expected_result_v2);
}

template<typename T>
void VerifyToString(const T& decimal, int precision, int scale, const string& expected) {
  EXPECT_EQ(decimal.ToString(precision, scale), expected);
}

void StringToAllDecimals(const string& s, int precision, int scale, int32_t val,
    StringParser::ParseResult result) {
  VerifyParse(s, precision, scale, Decimal4Value(val), result);
  VerifyParse(s, precision, scale, Decimal8Value(val), result);
  VerifyParse(s, precision, scale, Decimal16Value(val), result);
}

void StringToAllDecimals(const string& s, int precision, int scale,
    int32_t val_v1, StringParser::ParseResult result_v1,
    int32_t val_v2, StringParser::ParseResult result_v2) {
  VerifyParse(s, precision, scale,
      Decimal4Value(val_v1), result_v1, Decimal4Value(val_v2), result_v2);
  VerifyParse(s, precision, scale,
      Decimal8Value(val_v1), result_v1, Decimal8Value(val_v2), result_v2);
  VerifyParse(s, precision, scale,
      Decimal16Value(val_v1), result_v1, Decimal16Value(val_v2), result_v2);
}

TEST(DecimalTest, IntToDecimal) {
  Decimal16Value d16;
  bool overflow = false;

  d16 = Decimal16Value::FromInt(27, 18, -25559, &overflow);
  EXPECT_FALSE(overflow);
  VerifyToString(d16, 27, 18, "-25559.000000000000000000");

  d16 = Decimal16Value::FromInt(36, 29, 32130, &overflow);
  EXPECT_FALSE(overflow);
  VerifyToString(d16, 36, 29, "32130.00000000000000000000000000000");

  d16 = Decimal16Value::FromInt(38, 38, 1, &overflow);
  EXPECT_TRUE(overflow);

  // Smaller decimal types can't overflow here since the FE should never generate
  // that.
}

TEST(DecimalTest, DoubleToDecimal) {
  Decimal4Value d4;
  Decimal8Value d8;
  Decimal16Value d16;

  bool overflow = false;
  d4 = Decimal4Value::FromDouble(9, 0, 1.9, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 2);
  VerifyToString(d4, 9, 0, "2");

  d4 = Decimal4Value::FromDouble(9, 0, 1.9, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 1);
  VerifyToString(d4, 9, 0, "1");

  d4 = Decimal4Value::FromDouble(1, 0, 1, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 1);
  VerifyToString(d4, 1, 0, "1");

  d4 = Decimal4Value::FromDouble(1, 0, 0, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 0);
  VerifyToString(d4, 1, 0, "0");

  d4 = Decimal4Value::FromDouble(1, 0, 9.9, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 9);
  VerifyToString(d4, 1, 0, "9");

  d4 = Decimal4Value::FromDouble(1, 0, 9.9, true, &overflow);
  EXPECT_TRUE(overflow);
  overflow = false;

  d4 = Decimal4Value::FromDouble(1, 0, -1, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), -1);
  VerifyToString(d4, 1, 0, "-1");

  d4 = Decimal4Value::FromDouble(1, 0, -9.9, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), -9);
  VerifyToString(d4, 1, 0, "-9");

  d4 = Decimal4Value::FromDouble(1, 0, -9.9, true, &overflow);
  EXPECT_TRUE(overflow);
  overflow = false;

  d4 = Decimal4Value::FromDouble(1, 1, 0.1, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 1);
  VerifyToString(d4, 1, 1, "0.1");

  d4 = Decimal4Value::FromDouble(1, 1, 0.0, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 0);
  VerifyToString(d4, 1, 1, "0.0");

  d4 = Decimal4Value::FromDouble(1, 1, -0.1, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), -1);
  VerifyToString(d4, 1, 1, "-0.1");

  overflow = false;
  d8 = Decimal8Value::FromDouble(10, 5, -100.1, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d8.value(), -10010000);
  VerifyToString(d8, 10, 5, "-100.10000");

  overflow = false;
  d16 = Decimal16Value::FromDouble(10, 10, -0.1, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d16.value(), -1000000000);
  VerifyToString(d16, 10, 10, "-0.1000000000");

  // Test overflow
  overflow = false;
  Decimal4Value::FromDouble(9, 0, 999999999.123, false, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  Decimal4Value::FromDouble(9, 0, 1234567890.1, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  Decimal8Value::FromDouble(9, 0, -1234567890.123, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  Decimal8Value::FromDouble(10, 5, 99999.1234567, false, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  Decimal8Value::FromDouble(10, 5, 100000.1, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  Decimal8Value::FromDouble(10, 5, -123456.123, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  d16 = Decimal16Value::FromDouble(10, 10, 0.1234, true, &overflow);
  EXPECT_FALSE(overflow);
  VerifyToString(d16, 10, 10, "0.1234000000");

  overflow = false;
  Decimal16Value::FromDouble(10, 10, 1.1, true, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  Decimal16Value::FromDouble(10, 10, -1.1, true, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  Decimal16Value::FromDouble(10, 10, 0.99999999999, false, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  Decimal16Value::FromDouble(10, 10, 0.99999999999, true, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  Decimal16Value::FromDouble(10, 10, -0.99999999999, false, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  Decimal16Value::FromDouble(10, 10, -0.99999999999, true, &overflow);
  EXPECT_TRUE(overflow);
  overflow = false;

  // Test half rounding behavior
  d4 = Decimal4Value::FromDouble(1, 0, 0.499999999, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 0);
  VerifyToString(d4, 1, 0, "0");

  d4 = Decimal4Value::FromDouble(1, 0, 0.5, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 1);
  VerifyToString(d4, 1, 0, "1");

  d4 = Decimal4Value::FromDouble(1, 0, -0.499999999, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 0);
  VerifyToString(d4, 1, 0, "0");

  d4 = Decimal4Value::FromDouble(1, 0, -0.5, true, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), -1);
  VerifyToString(d4, 1, 0, "-1");
}

TEST(DecimalTest, StringToDecimalBasic) {
  StringToAllDecimals("       1234", 10, 0, 1234, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("   ", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals(" 0 0 ", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0 0", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("++0", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("--0", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("..0", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0..", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals(".0.", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0.0.", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0-", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0+", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("X", 10, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("+", 2, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("-", 2, 0, 0, StringParser::PARSE_FAILURE);

  StringToAllDecimals("1234", 10, 0, 1234, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("1234", 10, 2, 123400, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-1234", 10, 2, -123400, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("123", 2, 0, 123, StringParser::PARSE_OVERFLOW);
  StringToAllDecimals("  12  ", 2, 0, 12, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("000", 2, 0, 0, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(" . ", 2, 0, 0, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-.", 2, 0, 0, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("1.", 2, 0, 1, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(".1", 10, 2, 10, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("00012.3", 10, 2, 1230, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-00012.3", 10, 2, -1230, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("0.00000", 6, 5, 0, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("1.00000", 6, 5, 100000, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("0.000000", 6, 5, 0, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("1.000000", 6, 5, 100000, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("1.000004", 6, 5, 100000, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("1.000005", 6, 5,
      100000, StringParser::PARSE_UNDERFLOW,
      100001, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("0.4", 5, 0, 0, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("0.5", 5, 0,
      0, StringParser::PARSE_UNDERFLOW,
      1, StringParser::PARSE_UNDERFLOW);

  StringToAllDecimals("123.45", 10, 2, 12345, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(".45", 10, 2, 45, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-.45", 10, 2, -45, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(" 123.4 ", 10, 5, 12340000, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-123.45", 10, 5, -12345000, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-123.456", 10, 2,
      -12345, StringParser::PARSE_UNDERFLOW,
      -12346, StringParser::PARSE_UNDERFLOW);

  StringToAllDecimals("e", 2, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("E", 2, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("+E", 2, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals(".E", 2, 0, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals(" 0 e 0 ", 10, 0, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0e 0", 10, 0, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0e1.0", 10, 0, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0e1.", 10, 0, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0e.1", 10, 0, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0ee0", 10, 0, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("e1", 10, 0, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("1e", 10, 0, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("1.1.0e0", 10, 0, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("1e1.0", 10, 0, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("1..1e1", 10, 0, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("1e9999999999999999999", 10, 0, 0, StringParser::PARSE_OVERFLOW);
  StringToAllDecimals("1e-38", 10, 2, 0, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("1e-38", 38, 38, 1, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-1e-38", 38, 38, -1, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("1e-39", 38, 38, 0, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("-1e-39", 38, 38, 0, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("1e-9999999999999999999", 10, 0, 0, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("-1e-9999999999999999999", 10, 0, 0, StringParser::PARSE_UNDERFLOW);

  StringToAllDecimals(" 1e0 ", 10, 2, 100, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-1e0", 10, 2, -100, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("1e-0", 10, 2, 100, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(" 1e2 ", 10, 2, 10000, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-1e-2", 10, 2, -1, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(".011e3 ", 2, 0, 11, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(".00011e5 ", 2, 0, 11, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("110e-1 ", 2, 0, 11, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("1.10e1 ", 2, 0, 11, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("1.10e3 ", 2, 0, 11, StringParser::PARSE_OVERFLOW);
}

TEST(DecimalTest, StringToDecimalLarge) {
  StringToAllDecimals("1", 1, 0, 1, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-1", 1, 0, -1, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(".1", 1, 0, 0, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("10", 1, 0, 10, StringParser::PARSE_OVERFLOW);
  StringToAllDecimals("-10", 1, 0, -10, StringParser::PARSE_OVERFLOW);

  VerifyParse(".1234567890", 10, 10,
      Decimal8Value(1234567890L), StringParser::PARSE_SUCCESS);
  VerifyParse("-.1234567890", 10, 10,
      Decimal8Value(-1234567890L), StringParser::PARSE_SUCCESS);
  VerifyParse(".12345678900", 10, 10,
      Decimal8Value(1234567890L), StringParser::PARSE_UNDERFLOW);
  VerifyParse("-.12345678900", 10, 10,
      Decimal8Value(-1234567890L), StringParser::PARSE_UNDERFLOW);
  VerifyParse(".1234567890", 10, 10,
      Decimal16Value(1234567890L), StringParser::PARSE_SUCCESS);
  VerifyParse("-.1234567890", 10, 10,
      Decimal16Value(-1234567890L), StringParser::PARSE_SUCCESS);
  VerifyParse(".12345678900", 10, 10,
      Decimal16Value(1234567890L), StringParser::PARSE_UNDERFLOW);
  VerifyParse("-.12345678900", 10, 10,
      Decimal16Value(-1234567890L), StringParser::PARSE_UNDERFLOW);

  // Up to 8 digits with 5 before the decimal and 3 after.
  VerifyParse("12345.678", 8, 3,
      Decimal8Value(12345678L), StringParser::PARSE_SUCCESS);
  VerifyParse("-12345.678", 8, 3,
      Decimal8Value(-12345678L), StringParser::PARSE_SUCCESS);
  VerifyParse("123456.78", 8, 3,
      Decimal8Value(12345678L), StringParser::PARSE_OVERFLOW);
  VerifyParse("1234.5678", 8, 3,
      Decimal8Value(1234567L), StringParser::PARSE_UNDERFLOW,
      Decimal8Value(1234568L), StringParser::PARSE_UNDERFLOW);
  VerifyParse("12345.678", 8, 3,
      Decimal16Value(12345678L), StringParser::PARSE_SUCCESS);
  VerifyParse("-12345.678", 8, 3,
      Decimal16Value(-12345678L), StringParser::PARSE_SUCCESS);
  VerifyParse("123456.78", 8, 3,
      Decimal16Value(12345678L), StringParser::PARSE_OVERFLOW);
  VerifyParse("1234.5678", 8, 3,
      Decimal16Value(1234567L), StringParser::PARSE_UNDERFLOW,
      Decimal16Value(1234568L), StringParser::PARSE_UNDERFLOW);

  // Test max unscaled value for each of the decimal types.
  VerifyParse("999999999", 9, 0,
      Decimal4Value(999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("99999.9999", 9, 4,
      Decimal4Value(999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("0.999999999", 9, 9,
      Decimal4Value(999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("-999999999", 9, 0,
      Decimal4Value(-999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("-99999.9999", 9, 4,
      Decimal4Value(-999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("-0.999999999", 9, 9,
      Decimal4Value(-999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("1000000000", 9, 0,
      Decimal4Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("-1000000000", 9, 0,
      Decimal4Value(0), StringParser::PARSE_OVERFLOW);

  VerifyParse("999999999999999999", 18, 0,
      Decimal8Value(999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse("999999.999999999999", 18, 12,
      Decimal8Value(999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse(".999999999999999999", 18, 18,
      Decimal8Value(999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse("-999999999999999999", 18, 0,
      Decimal8Value(-999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse("-999999.999999999999", 18, 12,
      Decimal8Value(-999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse("-.999999999999999999", 18, 18,
      Decimal8Value(-999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse("1000000000000000000", 18, 0,
      Decimal8Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("01000000000000000000", 18, 0,
      Decimal8Value(0), StringParser::PARSE_OVERFLOW);

  int128_t result = MAX_UNSCALED_DECIMAL16;
  VerifyParse("99999999999999999999999999999999999999",
      38, 0, Decimal16Value(result), StringParser::PARSE_SUCCESS);
  VerifyParse("99999999999999999999999999999999999999e1",
      38, 0, Decimal16Value(result), StringParser::PARSE_OVERFLOW);
  VerifyParse("999999999999999999999999999999999999990e-1",
      38, 0, Decimal16Value(result), StringParser::PARSE_UNDERFLOW);
  VerifyParse("999999999999999999999999999999999.99999",
      38, 5, Decimal16Value(result), StringParser::PARSE_SUCCESS);
  VerifyParse(".99999999999999999999999999999999999999",
      38, 38, Decimal16Value(result), StringParser::PARSE_SUCCESS);
  VerifyParse("-99999999999999999999999999999999999999",
      38, 0, Decimal16Value(-result), StringParser::PARSE_SUCCESS);
  VerifyParse("-999999999999999999999999999999999.99999",
      38, 5, Decimal16Value(-result), StringParser::PARSE_SUCCESS);
  VerifyParse("-.99999999999999999999999999999999999999",
      38, 38, Decimal16Value(-result), StringParser::PARSE_SUCCESS);
  VerifyParse("-.99999999999999999999999999999999999999e1",
      38, 38, Decimal16Value(-result), StringParser::PARSE_OVERFLOW);
  VerifyParse("-.999999999999999999999999999999999999990e-1", 38, 38,
      Decimal16Value(-result / 10), StringParser::PARSE_UNDERFLOW,
      Decimal16Value(-result / 10 - 1), StringParser::PARSE_UNDERFLOW);
  VerifyParse("-.999999999999999999999999999999999999990000000000000000e-20", 38, 38,
      Decimal16Value(-result / DecimalUtil::GetScaleMultiplier<int128_t>(20)),
      StringParser::PARSE_UNDERFLOW,
      Decimal16Value(-result / DecimalUtil::GetScaleMultiplier<int128_t>(20) - 1),
      StringParser::PARSE_UNDERFLOW);
  VerifyParse("100000000000000000000000000000000000000",
      38, 0, Decimal16Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("-100000000000000000000000000000000000000",
      38, 0, Decimal16Value(0), StringParser::PARSE_OVERFLOW);

  // Rounding tests.
  VerifyParse("555.554", 5, 2, Decimal4Value(55555), StringParser::PARSE_UNDERFLOW);
  VerifyParse("555.555", 5, 2,
      Decimal4Value(55555), StringParser::PARSE_UNDERFLOW,
      Decimal4Value(55556), StringParser::PARSE_UNDERFLOW);
  // Too many digits to the left of the dot so we overflow.
  VerifyParse("555.555e1", 5, 2, Decimal4Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("-555.555e1", 5, 2, Decimal4Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("5555.555", 5, 2, Decimal16Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("-555.555", 5, 2,
        Decimal4Value(-55555), StringParser::PARSE_UNDERFLOW,
        Decimal4Value(-55556), StringParser::PARSE_UNDERFLOW);
  VerifyParse("-5555.555", 5, 2, Decimal16Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("5555.555e-1", 5, 2,
          Decimal4Value(55555), StringParser::PARSE_UNDERFLOW,
          Decimal4Value(55556), StringParser::PARSE_UNDERFLOW);
  VerifyParse("55555.555e-1", 5, 2, Decimal16Value(0), StringParser::PARSE_OVERFLOW);
  // Too many digits to the right of the dot and not enough to the left. Rounding via
  // ScaleDownAndRound().
  VerifyParse("5.55444", 5, 2, Decimal4Value(555), StringParser::PARSE_UNDERFLOW);
  VerifyParse("5.55555", 5, 2,
        Decimal4Value(555), StringParser::PARSE_UNDERFLOW,
        Decimal4Value(556), StringParser::PARSE_UNDERFLOW);
  VerifyParse("5.555e-9", 5, 2, Decimal4Value(0), StringParser::PARSE_UNDERFLOW);
  // The number of digits to the left of the dot equals to precision - scale. Rounding
  // by adding 1 if the first truncated digit is greater or equal to 5.
  VerifyParse("555.554", 5, 2, Decimal4Value(55555), StringParser::PARSE_UNDERFLOW);
  VerifyParse("555.555", 5, 2,
      Decimal4Value(55555), StringParser::PARSE_UNDERFLOW,
      Decimal4Value(55556), StringParser::PARSE_UNDERFLOW);
  VerifyParse("5.55554e2", 5, 2, Decimal4Value(55555), StringParser::PARSE_UNDERFLOW);
  VerifyParse("5.55555e2", 5, 2,
      Decimal4Value(55555), StringParser::PARSE_UNDERFLOW,
      Decimal4Value(55556), StringParser::PARSE_UNDERFLOW);
  VerifyParse("55555.4e-2", 5, 2, Decimal4Value(55555), StringParser::PARSE_UNDERFLOW);
  VerifyParse("55555.5e-2", 5, 2,
          Decimal4Value(55555), StringParser::PARSE_UNDERFLOW,
          Decimal4Value(55556), StringParser::PARSE_UNDERFLOW);
  // Rounding causes overflow.
  VerifyParse("999.994", 5, 2, Decimal4Value(99999), StringParser::PARSE_UNDERFLOW);
  VerifyParse("999.995", 5, 2,
        Decimal4Value(99999), StringParser::PARSE_UNDERFLOW,
        Decimal4Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("9.99995e2", 5, 2,
          Decimal4Value(99999), StringParser::PARSE_UNDERFLOW,
          Decimal4Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("99999.5e-2", 5, 2,
            Decimal4Value(99999), StringParser::PARSE_UNDERFLOW,
            Decimal4Value(0), StringParser::PARSE_OVERFLOW);
}

TEST(DecimalTest, Overflow) {
  bool overflow = false;

  Decimal16Value result;
  Decimal16Value d_max(MAX_UNSCALED_DECIMAL16);
  Decimal16Value two(2);
  Decimal16Value one(1);
  Decimal16Value zero(0);

  // Adding same sign
  overflow = false;
  d_max.Add<int128_t>(0, one, 0, 38, 0, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  one.Add<int128_t>(0, d_max, 0, 38, 0, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  d_max.Add<int128_t>(0, d_max, 0, 38, 0, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  result = d_max.Add<int128_t>(0, zero, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value());

  // Subtracting same sign
  overflow = false;
  result = d_max.Subtract<int128_t>(0, one, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  EXPECT_TRUE(result.value() == d_max.value() - 1);
  result = one.Subtract<int128_t>(0, d_max, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  EXPECT_TRUE(result.value() == -(d_max.value() - 1));
  result = d_max.Subtract<int128_t>(0, d_max, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  EXPECT_TRUE(result.value() == 0);
  result = d_max.Subtract<int128_t>(0, zero, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value());

  // Adding different sign
  overflow = false;
  result = d_max.Add<int128_t>(0, -one, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value() - 1);

  overflow = false;
  result = one.Add<int128_t>(0, -d_max, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == -(d_max.value() - 1));

  overflow = false;
  result = d_max.Add<int128_t>(0, -d_max, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == 0);

  overflow = false;
  result = d_max.Add<int128_t>(0, -zero, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value());

  // Subtracting different sign
  overflow = false;
  d_max.Subtract<int128_t>(0, -one, 0, 38, 0, false, &overflow);
  EXPECT_TRUE(overflow);
  one.Subtract<int128_t>(0, -d_max, 0, 38, 0, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  d_max.Subtract<int128_t>(0, -d_max, 0, 38, 0, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  result = d_max.Subtract<int128_t>(0, -zero, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value());

  // Multiply
  overflow = false;
  result = d_max.Multiply<int128_t>(0, one, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value());

  overflow = false;
  result = d_max.Multiply<int128_t>(0, -one, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == -d_max.value());

  overflow = false;
  result = d_max.Multiply<int128_t>(0, two, 0, 38, 0, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  result = d_max.Multiply<int128_t>(0, -two, 0, 38, 0, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  result = d_max.Multiply<int128_t>(0, d_max, 0, 38, 0, false, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  result = d_max.Multiply<int128_t>(0, -d_max, 0, 38, 0, false, &overflow);
  EXPECT_TRUE(overflow);

  // Multiply by 0
  overflow = false;
  result = zero.Multiply<int128_t>(0, one, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == 0);

  overflow = false;
  result = one.Multiply<int128_t>(0, zero, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == 0);

  overflow = false;
  result = zero.Multiply<int128_t>(0, zero, 0, 38, 0, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == 0);

  // Adding any value with scale to (38, 0) will overflow if the most significant
  // digit is set.
  overflow = false;
  result = d_max.Add<int128_t>(0, zero, 1, 38, 1, false, &overflow);
  EXPECT_TRUE(overflow);

  // Add 37 9's (with scale 0)
  Decimal16Value d3(MAX_UNSCALED_DECIMAL16 / 10);
  overflow = false;
  result = d3.Add<int128_t>(0, zero, 1, 38, 1, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(result.value(), MAX_UNSCALED_DECIMAL16 - 9);

  overflow = false;
  result = d3.Add<int128_t>(0, one, 1, 38, 1, false, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(result.value(), MAX_UNSCALED_DECIMAL16 - 8);

  // Mod
  overflow = false;
  bool is_nan;
  result = d3.Mod<int128_t>(0, d3, 20, 38, 20, false, &is_nan, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_FALSE(is_nan);
  EXPECT_EQ(result.value(), 0);

  overflow = false;
  result = d3.Mod<int128_t>(0, two, 0, 38, 0, false, &is_nan, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_FALSE(is_nan);
  EXPECT_EQ(result.value(), MAX_UNSCALED_DECIMAL16 % 2);

  result = d3.Mod<int128_t>(0, zero, 1, 38, 1, false, &is_nan, &overflow);
  EXPECT_TRUE(is_nan);
}

// Overflow cases only need to test with Decimal16Value with max precision. In
// the other cases, the planner should have casted the values to this precision.
// Add/Subtract/Mod cannot overflow the scale. With division, we always handle the case
// where the result scale needs to be adjusted.
TEST(DecimalTest, MultiplyScaleOverflow) {
  bool overflow = false;
  Decimal16Value x(1);
  Decimal16Value y(3);
  int max_scale = 38;

  // x = 0.<37 zeroes>1. y = 0.<37 zeroes>3 The result should be 0.<74 zeroes>3.
  // Since this can't be  represented, the result will truncate to 0.
  Decimal16Value result = x.Multiply<int128_t>(max_scale, y, max_scale, 38, 38, false, &overflow);
  EXPECT_TRUE(result.value() == 0);
  EXPECT_FALSE(overflow);

  int scale_1 = 1;
  int scale_37 = 37;
  // x = 0.<36 zeroes>1, y = 0.3
  // The result should be 0.<37 zeroes>11, which would require scale = 39.
  // The truncated version should 0.<37 zeroes>3.
  result = x.Multiply<int128_t>(scale_37, y, scale_1, 38, 38, false, &overflow);
  EXPECT_TRUE(result.value() == 3);
  EXPECT_FALSE(overflow);
}

// Test that unaligned decimal values are handled correctly.
TEST(DecimalTest, UnalignedValues) {
  // Regression test for IMPALA-7473 that triggered a crash in release builds.
  Decimal16Value aligned(12345);
  uint8_t* unaligned_mem = reinterpret_cast<uint8_t*>(malloc(sizeof(Decimal16Value) + 9));
  memcpy(&unaligned_mem[9], &aligned, sizeof(aligned));
  Decimal16Value* unaligned = reinterpret_cast<Decimal16Value*>(&unaligned_mem[9]);
  // VerifyToString() worked even prior to the bugfix because GCC happened to generate
  // code without aligned load instructions.
  VerifyToString(*unaligned, 28, 2, "123.45");
  // PrintValue() contained different generated code that wasn't safe for unaligned
  // values.
  stringstream ss;
  RawValue::PrintValue(unaligned, ColumnType::CreateDecimalType(28, 2), 0, &ss);
  EXPECT_EQ("123.45", ss.str());
  // Regression test for IMPALA-9781: Verify that operator=() works
  *unaligned = 0;
  __int128_t val = unaligned->value();
  EXPECT_EQ(val, 0);
  free(unaligned_mem);
}

enum Op {
  ADD,
  SUBTRACT,
  MULTIPLY,
  DIVIDE,
  MOD,
};

// Implementation of decimal rules. This is handled in the planner in the normal
// execution paths.
ColumnType GetResultType(const ColumnType& t1, const ColumnType& t2, Op op, bool v2) {
  // TODO: Implement V2 result types
  switch (op) {
    case ADD:
    case SUBTRACT:
      return ColumnType::CreateDecimalType(
          max(t1.scale, t2.scale) +
              max(t1.precision - t1.scale, t2.precision - t2.scale) + 1,
          max(t1.scale, t2.scale));
    case MULTIPLY:
      return ColumnType::CreateDecimalType(
          t1.precision + t2.precision, t1.scale + t2.scale);
    case DIVIDE:
      if (v2) {
        int result_scale =
            max(ColumnType::MIN_ADJUSTED_SCALE, t1.scale + t2.precision + 1);
        int result_precision =t1.precision - t1.scale + t2.scale + result_scale;
        return ColumnType::CreateAdjustedDecimalType(result_precision, result_scale);
      } else {
        return ColumnType::CreateDecimalType(
          min(ColumnType::MAX_PRECISION,
            t1.precision - t1.scale + t2.scale + max(4, t1.scale + t2.precision + 1)),
          min(ColumnType::MAX_PRECISION, max(4, t1.scale + t2.precision + 1)));
      }
    case MOD:
      return ColumnType::CreateDecimalType(
          min(t1.precision - t1.scale, t2.precision - t2.scale) + max(t1.scale, t2.scale),
          max(t1.scale, t2.scale));
    default:
      EXPECT_TRUE(false);
      return ColumnType();
  }
}

TEST(DecimalTest, ResultTypes) {
  ColumnType t1 = ColumnType::CreateDecimalType(38, 10);
  ColumnType t2 = ColumnType::CreateDecimalType(38, 38);
  ColumnType t3 = ColumnType::CreateDecimalType(38, 0);

  auto r1 = GetResultType(t1, t2, DIVIDE, true);
  EXPECT_EQ(r1.precision, 38);
  EXPECT_EQ(r1.scale, 6);

  auto r2 = GetResultType(t1, t3, DIVIDE, true);
  EXPECT_EQ(r2.precision, 38);
  EXPECT_EQ(r2.scale, 10);

  auto r3 = GetResultType(t1, t2, DIVIDE, false);
  EXPECT_EQ(r3.precision, 38);
  EXPECT_EQ(r3.scale, 38);
}

template<typename T>
void VerifyFuzzyEquals(const T& actual, const ColumnType& t,
    double expected, bool overflow, double max_error = MAX_ERROR) {
  double actual_d = actual.ToDouble(t.scale);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(fabs(actual_d - expected) < max_error)
    << actual_d << " != " << expected;
}

TEST(DecimalTest, BasicArithmetic) {
  ColumnType t1 = ColumnType::CreateDecimalType(5, 4);
  ColumnType t2 = ColumnType::CreateDecimalType(8, 3);
  ColumnType t1_plus_2 = GetResultType(t1, t2, ADD, false);
  ColumnType t1_times_2 = GetResultType(t1, t2, MULTIPLY, false);

  Decimal4Value d1(123456789);
  Decimal4Value d2(23456);
  Decimal4Value d3(-23456);
  double d1_double = d1.ToDouble(t1.scale);
  double d2_double = d2.ToDouble(t2.scale);
  double d3_double = d3.ToDouble(t2.scale);

  bool overflow = false;
  // TODO: what's the best way to author a bunch of tests like this?
  VerifyFuzzyEquals(d1.Add<int64_t>(
      t1.scale, d2, t2.scale, t1_plus_2.precision, t1_plus_2.scale, false, &overflow),
      t1_plus_2, d1_double + d2_double, overflow);
  VerifyFuzzyEquals(d1.Add<int64_t>(
      t1.scale, d3, t2.scale, t1_plus_2.precision, t1_plus_2.scale, false, &overflow),
      t1_plus_2, d1_double + d3_double, overflow);
  VerifyFuzzyEquals(d1.Subtract<int64_t>(
      t1.scale, d2, t2.scale, t1_plus_2.precision, t1_plus_2.scale, false, &overflow),
      t1_plus_2, d1_double - d2_double, overflow);
  VerifyFuzzyEquals(d1.Subtract<int64_t>(
      t1.scale, d3, t2.scale, t1_plus_2.precision, t1_plus_2.scale, false, &overflow),
      t1_plus_2, d1_double - d3_double, overflow);
  VerifyFuzzyEquals(d1.Multiply<int128_t>(
      t1.scale, d2, t2.scale, t1_times_2.precision, t1_times_2.scale, false, &overflow),
      t1_times_2, d1_double * d2_double, overflow);
  VerifyFuzzyEquals(d1.Multiply<int64_t>(
      t1.scale, d3, t2.scale, t1_times_2.precision, t1_times_2.scale, false, &overflow),
      t1_times_2, d1_double * d3_double, overflow);
}

TEST(DecimalTest, Divide) {
  // Exhaustively test precision and scale for 4 byte decimals. The logic errors tend
  // to be by powers of 10 so not testing the other decimal types is okay.
  Decimal4Value x(123456789);
  Decimal4Value y(234);
  for (int numerator_p = 1; numerator_p <= 9; ++numerator_p) {
    for (int numerator_s = 0; numerator_s <= numerator_p; ++numerator_s) {
      for (int denominator_p = 1; denominator_p <= 3; ++denominator_p) {
        for (int denominator_s = 0; denominator_s <= denominator_p; ++denominator_s) {
          for (int v2: { 0, 1 }) {
            ColumnType t1 = ColumnType::CreateDecimalType(numerator_p, numerator_s);
            ColumnType t2 = ColumnType::CreateDecimalType(denominator_p, denominator_s);
            ColumnType t3 = GetResultType(t1, t2, DIVIDE, v2);
            bool is_nan = false;
            bool is_overflow = false;
            Decimal8Value r = x.Divide<int64_t>(
                t1.scale, y, t2.scale, t3.precision, t3.scale, true, &is_nan, &is_overflow);
            double approx_x = x.ToDouble(t1.scale);
            double approx_y = y.ToDouble(t2.scale);
            double approx_r = r.ToDouble(t3.scale);
            double expected_r = approx_x / approx_y;

            EXPECT_FALSE(is_nan);
            EXPECT_FALSE(is_overflow);
            if (fabs(approx_r - expected_r) > MAX_ERROR) {
              LOG(ERROR) << approx_r << " " << expected_r;
              LOG(ERROR) << x.ToString(t1) << "/" << y.ToString(t2)
                         << "=" << r.ToString(t3);
              EXPECT_TRUE(false);
            }
          }
        }
      }
    }
  }
  // Divide by 0
  bool is_nan = false;
  bool is_overflow = false;
  Decimal8Value r = x.Divide<int64_t>(0, Decimal4Value(0), 0, 38, 4, true,
      &is_nan, &is_overflow);
  EXPECT_TRUE(is_nan) << "Expected NaN, got: " << r;
  EXPECT_FALSE(is_overflow);

  // In this case, we are dividing large precision decimals meaning the resulting
  // decimal underflows. The resulting type is (38,38).
  Decimal16Value x2(53994500);
  Decimal16Value y2(5399450);
  is_nan = false;
  is_overflow = false;
  x2.Divide<int128_t>(4, y2, 4, 38, 38, true, &is_nan, &is_overflow);
  EXPECT_TRUE(is_overflow);
  EXPECT_FALSE(is_nan);
}

TEST(DecimalTest, DivideLargeScales) {
  ColumnType t1 = ColumnType::CreateDecimalType(38, 8);
  ColumnType t2 = ColumnType::CreateDecimalType(20, 0);
  ColumnType t3 = GetResultType(t1, t2, DIVIDE, false);
  StringParser::ParseResult result;
  const char* data = "319391280635.61476055";
  Decimal16Value x =
      StringParser::StringToDecimal<int128_t>(data, strlen(data), t1, false, &result);
  Decimal16Value y(10000);
  bool is_nan = false;
  bool is_overflow = false;
  Decimal16Value r = x.Divide<int128_t>(t1.scale, y, t2.scale, t3.precision, t3.scale,
      true, &is_nan, &is_overflow);
  VerifyToString(r, t3.precision, t3.scale, "31939128.06356147605500000000000000000");
  EXPECT_FALSE(is_nan);
  EXPECT_FALSE(is_overflow);

  y = -y;
  r = x.Divide<int128_t>(t1.scale, y, t2.scale, t3.precision, t3.scale, true,
      &is_nan, &is_overflow);
  VerifyToString(r, t3.precision, t3.scale, "-31939128.06356147605500000000000000000");
  EXPECT_FALSE(is_nan);
  EXPECT_FALSE(is_overflow);
}

template<typename T>
DecimalValue<T> RandDecimal(int max_precision) {
  T val = 0;
  int precision = rand() % max_precision;
  for (int i = 0; i < precision; ++i) {
    int digit = rand() % 10;
    val = val * 10 + digit;
  }
  return DecimalValue<T>(rand() % 2 == 0 ? val : -val);
}

int DoubleCompare(double x, double y) {
  if (x < y) return -1;
  if (x > y) return 1;
  return 0;
}

// Randomly test decimal operations, comparing the result with a double ground truth.
TEST(DecimalTest, RandTesting) {
  int NUM_ITERS = 1000000;
  int seed = time(0);
  LOG(ERROR) << "Seed: " << seed;
  for (int i = 0; i < NUM_ITERS; ++i) {
    // TODO: double is too imprecise so we can't test with high scales.
    int p1 = rand() % 12 + 1;
    int s1 = rand() % min(4, p1);
    int p2 = rand() % 12 + 1;
    int s2 = rand() % min(4, p2);

    DecimalValue<int64_t> dec1 = RandDecimal<int64_t>(p1);
    DecimalValue<int64_t> dec2 = RandDecimal<int64_t>(p2);
    ColumnType t1 = ColumnType::CreateDecimalType(p1, s1);
    ColumnType t2 = ColumnType::CreateDecimalType(p2, s2);
    double t1_d = dec1.ToDouble(s1);
    double t2_d = dec2.ToDouble(s2);

    ColumnType add_t = GetResultType(t1, t2, ADD, false);

    bool overflow = false;
    VerifyFuzzyEquals(dec1.Add<int64_t>(
        t1.scale, dec2, t2.scale, add_t.precision, add_t.scale, false, &overflow),
        add_t, t1_d + t2_d, overflow);
    VerifyFuzzyEquals(dec1.Subtract<int64_t>(
        t1.scale, dec2, t2.scale, add_t.precision, add_t.scale, false, &overflow),
        add_t, t1_d - t2_d, overflow);
    if (overflow) continue;

#if 0
    // multiply V2 result type not implemented yet
    ColumnType multiply_t = GetResultType(t1, t2, MULTIPLY, true);
    VerifyFuzzyEquals(dec1.Multiply<int64_t>(
        t1.scale, dec2, t2.scale, multiply_t.scale), multiply_t, t1_d * t2_d);
#endif
    // With rounding, we should be able to get much closer to real values
    ColumnType divide_t = GetResultType(t1, t2, DIVIDE, true);
    bool is_nan = false;
    overflow = false;
    auto result = dec1.Divide<int128_t>(t1.scale, dec2, t2.scale, divide_t.precision,
        divide_t.scale, true, &is_nan, &overflow);
    if (!is_nan && !overflow)
      VerifyFuzzyEquals(result, divide_t, t1_d / t2_d, false,
          pow(10, divide_t.precision - divide_t.scale - 6));
    EXPECT_EQ(is_nan, dec2.value() == 0);
    EXPECT_EQ(dec1.Compare(t1.scale, dec2, t2.scale), DoubleCompare(t1_d, t2_d));
    EXPECT_TRUE(dec1.Compare(t1.scale, dec1, t1.scale) == 0);
    EXPECT_TRUE(dec2.Compare(t2.scale, dec2, t2.scale) == 0);
  }
}

TEST(DecimalTest, PrecisionScaleValidation) {
  // Valid precision and scale.
  EXPECT_TRUE(ColumnType::ValidateDecimalParams(1, 0));
  EXPECT_TRUE(ColumnType::ValidateDecimalParams(1, 1));
  EXPECT_TRUE(ColumnType::ValidateDecimalParams(38, 38));
  EXPECT_TRUE(ColumnType::ValidateDecimalParams(38, 0));

  // Out of range precision or scale.
  EXPECT_FALSE(ColumnType::ValidateDecimalParams(3, -1));
  EXPECT_FALSE(ColumnType::ValidateDecimalParams(0, 0));
  EXPECT_FALSE(ColumnType::ValidateDecimalParams(39, 0));
  EXPECT_FALSE(ColumnType::ValidateDecimalParams(38, 39));

  // Incompatible precision and scale.
  EXPECT_FALSE(ColumnType::ValidateDecimalParams(15, 16));
}

template <typename T>
static void TestGetScaleMultiplier(int scale_upper_bound, T overflow_val) {
  T expect = 1;
  for (int scale = 0; scale < scale_upper_bound; scale++) {
    EXPECT_EQ(expect, DecimalUtil::GetScaleMultiplier<T>(scale));
    expect *= 10;
  }
  // test overflow
  EXPECT_EQ(overflow_val, DecimalUtil::GetScaleMultiplier<T>(scale_upper_bound));
}

TEST(DecimalTest, GetScaleMultiplier) {
  TestGetScaleMultiplier<int32_t>(DecimalUtil::INT32_SCALE_UPPER_BOUND, -1);
  TestGetScaleMultiplier<int64_t>(DecimalUtil::INT64_SCALE_UPPER_BOUND, -1);
  TestGetScaleMultiplier<int128_t>(DecimalUtil::INT128_SCALE_UPPER_BOUND, -1);
  TestGetScaleMultiplier<int256_t>(DecimalUtil::INT256_SCALE_UPPER_BOUND, -1);
  TestGetScaleMultiplier<double>(DecimalUtil::INT64_SCALE_UPPER_BOUND, 1E19);
}

}

