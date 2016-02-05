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
#include <limits>
#include <gtest/gtest.h>
#include <boost/cstdint.hpp>
#include <boost/lexical_cast.hpp>
#include "runtime/decimal-value.h"
#include "util/string-parser.h"

#include "common/names.h"

using std::max;
using std::min;

namespace impala {

// Compare decimal result against double.
static const double MAX_ERROR = 0.0001;

template <typename T>
void VerifyEquals(const DecimalValue<T>& t1, const DecimalValue<T>& t2) {
  if (t1 != t2) {
    LOG(ERROR) << t1 << " != " << t2;
    EXPECT_TRUE(false);
  }
}

template <typename T>
void VerifyParse(const string& s, const ColumnType& t,
    const DecimalValue<T>& expected_val, StringParser::ParseResult expected_result) {
  StringParser::ParseResult parse_result;
  DecimalValue<T> val = StringParser::StringToDecimal<T>(
      s.c_str(), s.size(), t, &parse_result);
  EXPECT_EQ(expected_result, parse_result) << "Failed test string: " << s;
  if (expected_result == StringParser::PARSE_SUCCESS ||
      expected_result == StringParser::PARSE_UNDERFLOW) {
    VerifyEquals(expected_val, val);
  }
}

template<typename T>
void VerifyToString(const T& decimal, const ColumnType& t, const string& expected) {
  EXPECT_EQ(decimal.ToString(t), expected);
}

void StringToAllDecimals(const string& s, const ColumnType& t, int32_t val,
    StringParser::ParseResult result) {
  VerifyParse(s, t, Decimal4Value(val), result);
  VerifyParse(s, t, Decimal8Value(val), result);
  VerifyParse(s, t, Decimal16Value(val), result);
}

TEST(IntToDecimal, Basic) {
  Decimal16Value d16;
  bool overflow = false;

  d16 = Decimal16Value::FromInt(ColumnType::CreateDecimalType(27, 18), -25559, &overflow);
  EXPECT_FALSE(overflow);
  VerifyToString(d16, ColumnType::CreateDecimalType(27, 18), "-25559.000000000000000000");

  d16 = Decimal16Value::FromInt(ColumnType::CreateDecimalType(36, 29), 32130, &overflow);
  EXPECT_FALSE(overflow);
  VerifyToString(d16, ColumnType::CreateDecimalType(36, 29),
      "32130.00000000000000000000000000000");

  d16 = Decimal16Value::FromInt(ColumnType::CreateDecimalType(38, 38), 1, &overflow);
  EXPECT_TRUE(overflow);

  // Smaller decimal types can't overflow here since the FE should never generate
  // that.
}

TEST(DoubleToDecimal, Basic) {
  ColumnType t1 = ColumnType::CreateDecimalType(9, 0);
  ColumnType t2 = ColumnType::CreateDecimalType(10, 5);
  ColumnType t3 = ColumnType::CreateDecimalType(10, 10);
  ColumnType t4 = ColumnType::CreateDecimalType(1, 0);
  ColumnType t5 = ColumnType::CreateDecimalType(1, 1);

  Decimal4Value d4;
  Decimal8Value d8;
  Decimal16Value d16;

  bool overflow = false;
  d4 = Decimal4Value::FromDouble(t1, 1.1, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 1);
  VerifyToString(d4, t1, "1");

  d4 = Decimal4Value::FromDouble(t4, 1, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 1);
  VerifyToString(d4, t4, "1");

  d4 = Decimal4Value::FromDouble(t4, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 0);
  VerifyToString(d4, t4, "0");

  d4 = Decimal4Value::FromDouble(t4, -1, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), -1);
  VerifyToString(d4, t4, "-1");

  d4 = Decimal4Value::FromDouble(t5, 0.1, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 1);
  VerifyToString(d4, t5, "0.1");

  d4 = Decimal4Value::FromDouble(t5, 0.0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), 0);
  VerifyToString(d4, t5, "0.0");

  d4 = Decimal4Value::FromDouble(t5, -0.1, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d4.value(), -1);
  VerifyToString(d4, t5, "-0.1");

  overflow = false;
  d8 = Decimal8Value::FromDouble(t2, -100.1, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d8.value(), -10010000);
  VerifyToString(d8, t2, "-100.10000");

  overflow = false;
  d16 = Decimal16Value::FromDouble(t3, -.1, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(d16.value(), -1000000000);
  VerifyToString(d16, t3, "-0.1000000000");

  // Test overflow
  overflow = false;
  Decimal4Value::FromDouble(t1, 999999999.123, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  Decimal4Value::FromDouble(t1, 1234567890.1, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  Decimal8Value::FromDouble(t1, -1234567890.123, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  Decimal8Value::FromDouble(t2, 99999.1234567, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  Decimal8Value::FromDouble(t2, 100000.1, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  Decimal8Value::FromDouble(t2, -123456.123, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  d16 = Decimal16Value::FromDouble(t3, 0.1234, &overflow);
  EXPECT_FALSE(overflow);
  VerifyToString(d16, t3, "0.1234000000");

  overflow = false;
  Decimal16Value::FromDouble(t3, 1.1, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  Decimal16Value::FromDouble(t3, -1.1, &overflow);
  EXPECT_TRUE(overflow);
}

TEST(StringToDecimal, Basic) {
  ColumnType t1 = ColumnType::CreateDecimalType(10, 0);
  ColumnType t2 = ColumnType::CreateDecimalType(10, 2);
  ColumnType t3 = ColumnType::CreateDecimalType(2, 0);
  ColumnType t4 = ColumnType::CreateDecimalType(10, 5);

  StringToAllDecimals("       1234", t1, 1234, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("   ", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals(" 0 0 ", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0 0", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("++0", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("--0", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("..0", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0..", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals(".0.", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0.0.", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0-", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0+", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("X", t1, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("+", t3, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("-", t3, 0, StringParser::PARSE_FAILURE);

  StringToAllDecimals("1234", t1, 1234, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("1234", t2, 123400, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-1234", t2, -123400, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("123", t3, 123, StringParser::PARSE_OVERFLOW);
  StringToAllDecimals("  12  ", t3, 12, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("000", t3, 0, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(" . ", t3, 0, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-.", t3, 0, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("1.", t3, 1, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(".1", t2, 10, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("00012.3", t2, 1230, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-00012.3", t2, -1230, StringParser::PARSE_SUCCESS);

  StringToAllDecimals("123.45", t2, 12345, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(".45", t2, 45, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-.45", t2, -45, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(" 123.4 ", t4, 12340000, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-123.45", t4, -12345000, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-123.456", t2, -12345, StringParser::PARSE_UNDERFLOW);

  StringToAllDecimals("e", t3, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("E", t3, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals("+E", t3, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals(".E", t3, 0, StringParser::PARSE_FAILURE);
  StringToAllDecimals(" 0 e 0 ", t1, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0e 0", t1, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0e1.0", t1, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0e1.", t1, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0e.1", t1, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("0ee0", t1, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("e1", t1, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("1e", t1, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("1.1.0e0", t1, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("1e1.0", t1, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("1..1e1", t1, 100, StringParser::PARSE_FAILURE);
  StringToAllDecimals("1e9999999999999999999", t1, 100, StringParser::PARSE_OVERFLOW);

  StringToAllDecimals(" 1e0 ", t2, 100, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-1e0", t2, -100, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("1e-0", t2, 100, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(" 1e2 ", t2, 10000, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-1e-2", t2, -1, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(".011e3 ", t3, 11, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(".00011e5 ", t3, 11, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("110e-1 ", t3, 11, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("1.10e1 ", t3, 11, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("1.10e3 ", t3, 11, StringParser::PARSE_OVERFLOW);
}

TEST(StringToDecimal, LargeDecimals) {
  ColumnType t1 = ColumnType::CreateDecimalType(1, 0);
  ColumnType t2 = ColumnType::CreateDecimalType(10, 10);
  ColumnType t3 = ColumnType::CreateDecimalType(8, 3);

  StringToAllDecimals("1", t1, 1, StringParser::PARSE_SUCCESS);
  StringToAllDecimals("-1", t1, -1, StringParser::PARSE_SUCCESS);
  StringToAllDecimals(".1", t1, 0, StringParser::PARSE_UNDERFLOW);
  StringToAllDecimals("10", t1, 10, StringParser::PARSE_OVERFLOW);
  StringToAllDecimals("-10", t1, -10, StringParser::PARSE_OVERFLOW);

  VerifyParse(".1234567890", t2,
      Decimal8Value(1234567890L), StringParser::PARSE_SUCCESS);
  VerifyParse("-.1234567890", t2,
      Decimal8Value(-1234567890L), StringParser::PARSE_SUCCESS);
  VerifyParse(".12345678900", t2,
      Decimal8Value(1234567890L), StringParser::PARSE_UNDERFLOW);
  VerifyParse("-.12345678900", t2,
      Decimal8Value(-1234567890L), StringParser::PARSE_UNDERFLOW);
  VerifyParse(".1234567890", t2,
      Decimal16Value(1234567890L), StringParser::PARSE_SUCCESS);
  VerifyParse("-.1234567890", t2,
      Decimal16Value(-1234567890L), StringParser::PARSE_SUCCESS);
  VerifyParse(".12345678900", t2,
      Decimal16Value(1234567890L), StringParser::PARSE_UNDERFLOW);
  VerifyParse("-.12345678900", t2,
      Decimal16Value(-1234567890L), StringParser::PARSE_UNDERFLOW);

  // Up to 8 digits with 5 before the decimal and 3 after.
  VerifyParse("12345.678", t3,
      Decimal8Value(12345678L), StringParser::PARSE_SUCCESS);
  VerifyParse("-12345.678", t3,
      Decimal8Value(-12345678L), StringParser::PARSE_SUCCESS);
  VerifyParse("123456.78", t3,
      Decimal8Value(12345678L), StringParser::PARSE_OVERFLOW);
  VerifyParse("1234.5678", t3,
      Decimal8Value(1234567L), StringParser::PARSE_UNDERFLOW);
  VerifyParse("12345.678", t3,
      Decimal16Value(12345678L), StringParser::PARSE_SUCCESS);
  VerifyParse("-12345.678", t3,
      Decimal16Value(-12345678L), StringParser::PARSE_SUCCESS);
  VerifyParse("123456.78", t3,
      Decimal16Value(12345678L), StringParser::PARSE_OVERFLOW);
  VerifyParse("1234.5678", t3,
      Decimal16Value(1234567L), StringParser::PARSE_UNDERFLOW);

  // Test max unscaled value for each of the decimal types.
  VerifyParse("999999999", ColumnType::CreateDecimalType(9, 0),
      Decimal4Value(999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("99999.9999", ColumnType::CreateDecimalType(9, 4),
      Decimal4Value(999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("0.999999999", ColumnType::CreateDecimalType(9, 9),
      Decimal4Value(999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("-999999999", ColumnType::CreateDecimalType(9, 0),
      Decimal4Value(-999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("-99999.9999", ColumnType::CreateDecimalType(9, 4),
      Decimal4Value(-999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("-0.999999999", ColumnType::CreateDecimalType(9, 9),
      Decimal4Value(-999999999), StringParser::PARSE_SUCCESS);
  VerifyParse("1000000000", ColumnType::CreateDecimalType(9, 0),
      Decimal4Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("-1000000000", ColumnType::CreateDecimalType(9, 0),
      Decimal4Value(0), StringParser::PARSE_OVERFLOW);

  VerifyParse("999999999999999999", ColumnType::CreateDecimalType(18, 0),
      Decimal8Value(999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse("999999.999999999999", ColumnType::CreateDecimalType(18, 12),
      Decimal8Value(999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse(".999999999999999999", ColumnType::CreateDecimalType(18, 18),
      Decimal8Value(999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse("-999999999999999999", ColumnType::CreateDecimalType(18, 0),
      Decimal8Value(-999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse("-999999.999999999999", ColumnType::CreateDecimalType(18, 12),
      Decimal8Value(-999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse("-.999999999999999999", ColumnType::CreateDecimalType(18, 18),
      Decimal8Value(-999999999999999999ll), StringParser::PARSE_SUCCESS);
  VerifyParse("1000000000000000000", ColumnType::CreateDecimalType(18, 0),
      Decimal8Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("01000000000000000000", ColumnType::CreateDecimalType(18, 0),
      Decimal8Value(0), StringParser::PARSE_OVERFLOW);

  int128_t result = DecimalUtil::MAX_UNSCALED_DECIMAL16;
  VerifyParse("99999999999999999999999999999999999999",
      ColumnType::CreateDecimalType(38, 0),
      Decimal16Value(result), StringParser::PARSE_SUCCESS);
  VerifyParse("99999999999999999999999999999999999999e1",
      ColumnType::CreateDecimalType(38, 0),
      Decimal16Value(result), StringParser::PARSE_OVERFLOW);
  VerifyParse("999999999999999999999999999999999999990e-1",
      ColumnType::CreateDecimalType(38, 0),
      Decimal16Value(result), StringParser::PARSE_UNDERFLOW);
  VerifyParse("999999999999999999999999999999999.99999",
      ColumnType::CreateDecimalType(38, 5),
      Decimal16Value(result), StringParser::PARSE_SUCCESS);
  VerifyParse(".99999999999999999999999999999999999999",
      ColumnType::CreateDecimalType(38, 38),
      Decimal16Value(result), StringParser::PARSE_SUCCESS);
  VerifyParse("-99999999999999999999999999999999999999",
      ColumnType::CreateDecimalType(38, 0),
      Decimal16Value(-result), StringParser::PARSE_SUCCESS);
  VerifyParse("-999999999999999999999999999999999.99999",
      ColumnType::CreateDecimalType(38, 5),
      Decimal16Value(-result), StringParser::PARSE_SUCCESS);
  VerifyParse("-.99999999999999999999999999999999999999",
      ColumnType::CreateDecimalType(38, 38),
      Decimal16Value(-result), StringParser::PARSE_SUCCESS);
  VerifyParse("-.99999999999999999999999999999999999999e1",
      ColumnType::CreateDecimalType(38, 38),
      Decimal16Value(-result), StringParser::PARSE_OVERFLOW);
  VerifyParse("-.999999999999999999999999999999999999990e-1",
      ColumnType::CreateDecimalType(38, 38),
      Decimal16Value(-result / 10), StringParser::PARSE_UNDERFLOW);
  VerifyParse("-.999999999999999999999999999999999999990000000000000000e-20",
      ColumnType::CreateDecimalType(38, 38),
      Decimal16Value(-result / DecimalUtil::GetScaleMultiplier<int128_t>(20)),
      StringParser::PARSE_UNDERFLOW);
  VerifyParse("100000000000000000000000000000000000000",
      ColumnType::CreateDecimalType(38, 0),
      Decimal16Value(0), StringParser::PARSE_OVERFLOW);
  VerifyParse("-100000000000000000000000000000000000000",
      ColumnType::CreateDecimalType(38, 0),
      Decimal16Value(0), StringParser::PARSE_OVERFLOW);
}

TEST(DecimalTest, Overflow) {
  bool overflow = false;
  ColumnType t1 = ColumnType::CreateDecimalType(38, 0);

  Decimal16Value result;
  Decimal16Value d_max(DecimalUtil::MAX_UNSCALED_DECIMAL16);
  Decimal16Value two(2);
  Decimal16Value one(1);
  Decimal16Value zero(0);

  // Adding same sign
  overflow = false;
  d_max.Add<int128_t>(t1, one, t1, 38, 0, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  one.Add<int128_t>(t1, d_max, t1, 38, 0, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  d_max.Add<int128_t>(t1, d_max, t1, 38, 0, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  result = d_max.Add<int128_t>(t1, zero, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value());

  // Subtracting same sign
  overflow = false;
  result = d_max.Subtract<int128_t>(t1, one, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  EXPECT_TRUE(result.value() == d_max.value() - 1);
  result = one.Subtract<int128_t>(t1, d_max, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  EXPECT_TRUE(result.value() == -(d_max.value() - 1));
  result = d_max.Subtract<int128_t>(t1, d_max, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);

  overflow = false;
  EXPECT_TRUE(result.value() == 0);
  result = d_max.Subtract<int128_t>(t1, zero, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value());

  // Adding different sign
  overflow = false;
  result = d_max.Add<int128_t>(t1, -one, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value() - 1);

  overflow = false;
  result = one.Add<int128_t>(t1, -d_max, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == -(d_max.value() - 1));

  overflow = false;
  result = d_max.Add<int128_t>(t1, -d_max, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == 0);

  overflow = false;
  result = d_max.Add<int128_t>(t1, -zero, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value());

  // Subtracting different sign
  overflow = false;
  d_max.Subtract<int128_t>(t1, -one, t1, 38, 0, &overflow);
  EXPECT_TRUE(overflow);
  one.Subtract<int128_t>(t1, -d_max, t1, 38, 0, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  d_max.Subtract<int128_t>(t1, -d_max, t1, 38, 0, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  result = d_max.Subtract<int128_t>(t1, -zero, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value());

  // Multiply
  overflow = false;
  result = d_max.Multiply<int128_t>(t1, one, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == d_max.value());

  overflow = false;
  result = d_max.Multiply<int128_t>(t1, -one, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == -d_max.value());

  overflow = false;
  result = d_max.Multiply<int128_t>(t1, two, t1, 38, 0, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  result = d_max.Multiply<int128_t>(t1, -two, t1, 38, 0, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  result = d_max.Multiply<int128_t>(t1, d_max, t1, 38, 0, &overflow);
  EXPECT_TRUE(overflow);

  overflow = false;
  result = d_max.Multiply<int128_t>(t1, -d_max, t1, 38, 0, &overflow);
  EXPECT_TRUE(overflow);

  // Multiply by 0
  overflow = false;
  result = zero.Multiply<int128_t>(t1, one, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == 0);

  overflow = false;
  result = one.Multiply<int128_t>(t1, zero, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == 0);

  overflow = false;
  result = zero.Multiply<int128_t>(t1, zero, t1, 38, 0, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(result.value() == 0);

  // Adding any value with scale to (38, 0) will overflow if the most significant
  // digit is set.
  ColumnType t2 = ColumnType::CreateDecimalType(1, 1);
  overflow = false;
  result = d_max.Add<int128_t>(t1, zero, t2, 38, 1, &overflow);
  EXPECT_TRUE(overflow);

  // Add 37 9's (with scale 0)
  Decimal16Value d3(DecimalUtil::MAX_UNSCALED_DECIMAL16 / 10);
  overflow = false;
  result = d3.Add<int128_t>(t1, zero, t2, 38, 1, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(result.value(), DecimalUtil::MAX_UNSCALED_DECIMAL16 - 9);

  overflow = false;
  result = d3.Add<int128_t>(t1, one, t2, 38, 1, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_EQ(result.value(), DecimalUtil::MAX_UNSCALED_DECIMAL16 - 8);

  // Mod
  overflow = false;
  bool is_nan;
  ColumnType t3 = ColumnType::CreateDecimalType(38, 20);
  result = d3.Mod<int128_t>(t1, d3, t3, 38, 20, &is_nan, &overflow);
  EXPECT_TRUE(overflow);
  EXPECT_FALSE(is_nan);

  overflow = false;
  result = d3.Mod<int128_t>(t1, two, t1, 38, 0, &is_nan, &overflow);
  EXPECT_FALSE(overflow);
  EXPECT_FALSE(is_nan);
  EXPECT_EQ(result.value(), DecimalUtil::MAX_UNSCALED_DECIMAL16 % 2);

  result = d3.Mod<int128_t>(t1, zero, t2, 38, 1, &is_nan, &overflow);
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
  ColumnType max_scale = ColumnType::CreateDecimalType(38, 38);

  // x = 0.<37 zeroes>1. y = 0.<37 zeroes>3 The result should be 0.<74 zeroes>3.
  // Since this can't be  represented, the result will truncate to 0.
  Decimal16Value result = x.Multiply<int128_t>(max_scale, y, max_scale, 38, 38, &overflow);
  EXPECT_TRUE(result.value() == 0);
  EXPECT_FALSE(overflow);

  ColumnType scale_1 = ColumnType::CreateDecimalType(1, 1);
  ColumnType scale_37 = ColumnType::CreateDecimalType(38, 37);
  // x = 0.<36 zeroes>1, y = 0.3
  // The result should be 0.<37 zeroes>11, which would require scale = 39.
  // The truncated version should 0.<37 zeroes>3.
  result = x.Multiply<int128_t>(scale_37, y, scale_1, 38, 38, &overflow);
  EXPECT_TRUE(result.value() == 3);
  EXPECT_FALSE(overflow);
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
ColumnType GetResultType(const ColumnType& t1, const ColumnType& t2, Op op) {
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
      return ColumnType::CreateDecimalType(
          min(38,
            t1.precision - t1.scale + t2.scale + max(4, t1.scale + t2.precision + 1)),
          max(4, t1.scale + t2.precision + 1));
    case MOD:
      return ColumnType::CreateDecimalType(
          min(t1.precision - t1.scale, t2.precision - t2.scale) + max(t1.scale, t2.scale),
          max(t1.scale, t2.scale));
    default:
      EXPECT_TRUE(false);
      return ColumnType();
  }
}

template<typename T>
void VerifyFuzzyEquals(const T& actual, const ColumnType& t,
    double expected, bool overflow) {
  double actual_d = actual.ToDouble(t);
  EXPECT_FALSE(overflow);
  EXPECT_TRUE(fabs(actual_d - expected) < MAX_ERROR)
    << actual_d << " != " << expected;
}

TEST(DecimalArithmetic, Basic) {
  ColumnType t1 = ColumnType::CreateDecimalType(5, 4);
  ColumnType t2 = ColumnType::CreateDecimalType(8, 3);
  ColumnType t1_plus_2 = GetResultType(t1, t2, ADD);
  ColumnType t1_times_2 = GetResultType(t1, t2, MULTIPLY);

  Decimal4Value d1(123456789);
  Decimal4Value d2(23456);
  Decimal4Value d3(-23456);
  double d1_double = d1.ToDouble(t1);
  double d2_double = d2.ToDouble(t2);
  double d3_double = d3.ToDouble(t2);

  bool overflow = false;
  // TODO: what's the best way to author a bunch of tests like this?
  VerifyFuzzyEquals(d1.Add<int64_t>(
      t1, d2, t2, t1_plus_2.precision, t1_plus_2.scale, &overflow),
      t1_plus_2, d1_double + d2_double, overflow);
  VerifyFuzzyEquals(d1.Add<int64_t>(
      t1, d3, t2, t1_plus_2.precision, t1_plus_2.scale, &overflow),
      t1_plus_2, d1_double + d3_double, overflow);
  VerifyFuzzyEquals(d1.Subtract<int64_t>(
      t1, d2, t2, t1_plus_2.precision, t1_plus_2.scale, &overflow),
      t1_plus_2, d1_double - d2_double, overflow);
  VerifyFuzzyEquals(d1.Subtract<int64_t>(
      t1, d3, t2, t1_plus_2.precision, t1_plus_2.scale, &overflow),
      t1_plus_2, d1_double - d3_double, overflow);
  VerifyFuzzyEquals(d1.Multiply<int128_t>(
      t1, d2, t2, t1_times_2.precision, t1_times_2.scale, &overflow),
      t1_times_2, d1_double * d2_double, overflow);
  VerifyFuzzyEquals(d1.Multiply<int64_t>(
      t1, d3, t2, t1_times_2.precision, t1_times_2.scale, &overflow),
      t1_times_2, d1_double * d3_double, overflow);
}

TEST(DecimalArithmetic, Divide) {
  // Exhaustively test precision and scale for 4 byte decimals. The logic errors tend
  // to be by powers of 10 so not testing the other decimal types is okay.
  Decimal4Value x(123456789);
  Decimal4Value y(234);
  for (int numerator_p = 1; numerator_p <= 9; ++numerator_p) {
    for (int numerator_s = 0; numerator_s <= numerator_p; ++numerator_s) {
      for (int denominator_p = 1; denominator_p <= 3; ++denominator_p) {
        for (int denominator_s = 0; denominator_s <= denominator_p; ++denominator_s) {
          ColumnType t1 = ColumnType::CreateDecimalType(numerator_p, numerator_s);
          ColumnType t2 = ColumnType::CreateDecimalType(denominator_p, denominator_s);
          ColumnType t3 = GetResultType(t1, t2, DIVIDE);
          bool is_nan = false;
          bool is_overflow = false;
          Decimal8Value r = x.Divide<int64_t>(
              t1, y, t2, t3.precision, t3.scale, &is_nan, &is_overflow);
          double approx_x = x.ToDouble(t1);
          double approx_y = y.ToDouble(t2);
          double approx_r = r.ToDouble(t3);
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
  // Divide by 0
  bool is_nan = false;
  bool is_overflow = false;
  Decimal8Value r = x.Divide<int64_t>(ColumnType::CreateDecimalType(10, 0),
      Decimal4Value(0), ColumnType::CreateDecimalType(2,0), 38, 4, &is_nan, &is_overflow);
  EXPECT_TRUE(is_nan) << "Expected NaN, got: " << r;
  EXPECT_FALSE(is_overflow);

  // In this case, we are dividing large precision decimals meaning the resulting
  // decimal underflows. The resulting type is (38,38).
  ColumnType t4 = ColumnType::CreateDecimalType(38, 4);
  Decimal16Value x2(53994500);
  Decimal16Value y2(5399450);
  is_nan = false;
  is_overflow = false;
  x2.Divide<int128_t>(t4, y2, t4, 38, 38, &is_nan, &is_overflow);
  EXPECT_TRUE(is_overflow);
  EXPECT_FALSE(is_nan);
}

TEST(DecimalArithmetic, DivideLargeScales) {
  ColumnType t1 = ColumnType::CreateDecimalType(38, 8);
  ColumnType t2 = ColumnType::CreateDecimalType(20, 0);
  ColumnType t3 = GetResultType(t1, t2, DIVIDE);
  StringParser::ParseResult result;
  const char* data = "319391280635.61476055";
  Decimal16Value x =
      StringParser::StringToDecimal<int128_t>(data, strlen(data), t1, &result);
  Decimal16Value y(10000);
  bool is_nan = false;
  bool is_overflow = false;
  Decimal16Value r =
      x.Divide<int128_t>(t1, y, t2, t3.precision, t3.scale, &is_nan, &is_overflow);
  VerifyToString(r, t3, "31939128.06356147605500000000000000000");
  EXPECT_FALSE(is_nan);
  EXPECT_FALSE(is_overflow);

  y = -y;
  r = x.Divide<int128_t>(t1, y, t2, t3.precision, t3.scale, &is_nan, &is_overflow);
  VerifyToString(r, t3, "-31939128.06356147605500000000000000000");
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
TEST(DecimalArithmetic, RandTesting) {
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
    double t1_d = dec1.ToDouble(t1);
    double t2_d = dec2.ToDouble(t2);

    ColumnType add_t = GetResultType(t1, t2, ADD);

    bool overflow = false;
    VerifyFuzzyEquals(dec1.Add<int64_t>(
        t1, dec2, t2, add_t.precision, add_t.scale, &overflow),
        add_t, t1_d + t2_d, overflow);
    VerifyFuzzyEquals(dec1.Subtract<int64_t>(
        t1, dec2, t2, add_t.precision, add_t.scale, &overflow),
        add_t, t1_d - t2_d, overflow);
#if 0
    TODO: doubles are not precise enough for this
    ColumnType multiply_t = GetResultType(t1, t2, MULTIPLY);
    ColumnType divide_t = GetResultType(t1, t2, DIVIDE);
    // double is too imprecise to generate the right result.
    // TODO: compare against the ground truth using the multi precision float library.
    VerifyFuzzyEquals(dec1.Multiply<int64_t>(
        t1, dec2, t2, multiply_t.scale), multiply_t, t1_d * t2_d);
    if (dec2.value() != 0) {
      VerifyFuzzyEquals(dec1.Divide<int64_t>(
          t1, dec2, t2, divide_t.scale), divide_t, t1_d / t2_d);
    }
#endif

    EXPECT_EQ(dec1.Compare(t1.scale, dec2, t2.scale), DoubleCompare(t1_d, t2_d));
    EXPECT_TRUE(dec1.Compare(t1.scale, dec1, t1.scale) == 0);
    EXPECT_TRUE(dec2.Compare(t2.scale, dec2, t2.scale) == 0);
  }
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::DecimalUtil::InitMaxUnscaledDecimal16();
  return RUN_ALL_TESTS();
}
