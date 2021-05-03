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

#include "util/string-parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <cstdint>
#include <cstring>
#include <limits>
#include <boost/lexical_cast.hpp>
#include "testutil/gtest-util.h"

#include "common/names.h"

using std::min;
using std::numeric_limits;

namespace impala {

string space[] = {"", "   ", "\t\t\t", "\n\n\n", "\v\v\v", "\f\f\f", "\r\r\r"};
int space_len = 7;

// Tests conversion of s to integer with and without leading/trailing whitespace
template<typename T>
void TestIntValue(const char* s, T exp_val, StringParser::ParseResult exp_result) {
  for (int i = 0; i < space_len; ++i) {
    for (int j = 0; j < space_len; ++j) {
      // All combinations of leading and/or trailing whitespace.
      string str = space[i] + s + space[j];
      StringParser::ParseResult result;
      T val = StringParser::StringToInt<T>(str.data(), str.length(), &result);
      EXPECT_EQ(exp_val, val) << str;
      EXPECT_EQ(result, exp_result);
    }
  }
}

// Tests conversion of s, given a base, to an integer with and without leading/trailing
// whitespace
template<typename T>
void TestIntValue(
    const char* s, int base, T exp_val, StringParser::ParseResult exp_result) {
  for (int i = 0; i < space_len; ++i) {
    for (int j = 0; j < space_len; ++j) {
      // All combinations of leading and/or trailing whitespace.
      string str = space[i] + s + space[j];
      StringParser::ParseResult result;
      T val = StringParser::StringToInt<T>(str.data(), str.length(), base, &result);
      EXPECT_EQ(exp_val, val) << str;
      EXPECT_EQ(result, exp_result);
    }
  }
}

void TestBoolValue(const char* s, bool exp_val, StringParser::ParseResult exp_result) {
  for (int i = 0; i < space_len; ++i) {
    for (int j = 0; j < space_len; ++j) {
      // All combinations of leading and/or trailing whitespace.
      string str = space[i] + s + space[j];
      StringParser::ParseResult result;
      bool val = StringParser::StringToBool(str.data(), str.length(), &result);
      EXPECT_EQ(exp_val, val) << s;
      EXPECT_EQ(result, exp_result);
    }
  }
}

void TestDateValue(const char* s, DateValue exp_val,
    StringParser::ParseResult exp_result) {
  for (int i = 0; i < space_len; ++i) {
    for (int j = 0; j < space_len; ++j) {
      // All combinations of leading and/or trailing whitespace.
      string str = space[i] + s + space[j];
      StringParser::ParseResult result;
      DateValue val = StringParser::StringToDate(str.data(), str.length(), &result);
      EXPECT_EQ(exp_val, val) << s;
      EXPECT_EQ(result, exp_result);
    }
  }
}

// Compare Impala's float conversion function against strtod.
template<typename T>
void TestFloatValue(const string& s, StringParser::ParseResult exp_result) {
  StringParser::ParseResult result;
  T val = StringParser::StringToFloat<T>(s.data(), s.length(), &result);
  EXPECT_EQ(exp_result, result) << s;

  if (exp_result == StringParser::PARSE_SUCCESS && result == exp_result) {
    T exp_val = strtod(s.c_str(), NULL);
    EXPECT_EQ(exp_val, val) << s;
  }
}

template<typename T>
void TestFloatValueIsNan(const string& s, StringParser::ParseResult exp_result) {
  StringParser::ParseResult result;
  T val = StringParser::StringToFloat<T>(s.data(), s.length(), &result);
  EXPECT_EQ(exp_result, result);

  if (exp_result == StringParser::PARSE_SUCCESS && result == exp_result) {
    EXPECT_TRUE(std::isnan(val));
  }
}

// Tests conversion of s to double and float with +/- prefixing (and no prefix) and with
// and without leading/trailing whitespace
void TestAllFloatVariants(const string& s, StringParser::ParseResult exp_result) {
  string sign[] = {"", "+", "-"};
  for (int i = 0; i < space_len; ++i) {
    for (int j = 0; j < space_len; ++j) {
      for (int k = 0; k < 3; ++k) {
        // All combinations of leading and/or trailing whitespace and +/- sign.
        string str = space[i] + sign[k] + s + space[j];
        TestFloatValue<float>(str, exp_result);
        TestFloatValue<double>(str, exp_result);
      }
    }
  }
}

template<typename T>
void TestFloatBruteForce() {
  T min_val = numeric_limits<T>::min();
  T max_val = numeric_limits<T>::max();

  // Keep multiplying by 2.
  T cur_val = 1.0;
  while (cur_val < max_val) {
    string s = lexical_cast<string>(cur_val);
    TestFloatValue<T>(s, StringParser::PARSE_SUCCESS);
    cur_val *= 2;
  }

  // Keep dividing by 2.
  cur_val = 1.0;
  while (cur_val > min_val) {
    string s = lexical_cast<string>(cur_val);
    TestFloatValue<T>(s, StringParser::PARSE_SUCCESS);
    cur_val /= 2;
  }
}

void TestStringToFloatPreprocess(const char* s, const char* expected) {
  string trailers[] = {"", "4567", "fdfgh"};
  for (int i = 0; i < 3; ++i) {
    string input = s + trailers[i];
    // don't process trailers
    int processing_len = input.length() - trailers[i].length();
    char result[input.length() + 2];
    int res = StringParser::StringToFloatPreprocess(input.data(), processing_len,
      result);
    EXPECT_EQ(strlen(expected), res);
    EXPECT_EQ(string(result), string(expected));
  }
}

TEST(StringToInt, Basic) {
  TestIntValue<int8_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("123", 123, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("12345", 12345, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("12345678", 12345678, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("12345678901234", 12345678901234, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("-10", -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-10", -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("-10", -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-10", -10, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("+1", 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("+1", 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("+1", 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("+1", 1, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("+0", 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-0", 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("+0", 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-0", 0, StringParser::PARSE_SUCCESS);
}

TEST(StringToInt, InvalidLeadingTrailing) {
  // Test that trailing garbage is not allowed.
  TestIntValue<int8_t>("123xyz   ", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("-123xyz   ", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   123xyz   ", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   -12  3xyz ", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("12 3", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("-12 3", 0, StringParser::PARSE_FAILURE);

  // Must have at least one leading valid digit.
  TestIntValue<int8_t>("x123", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   x123", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   -x123", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   x-123", 0, StringParser::PARSE_FAILURE);

  // Test empty string and string with only whitespaces.
  TestIntValue<int8_t>("", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   ", 0, StringParser::PARSE_FAILURE);
}

TEST(StringToInt, Limit) {
  TestIntValue<int8_t>("127", 127, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("-128", -128, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("32767", 32767, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-32768", -32768, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("2147483647", 2147483647, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("-2147483648", -2147483648, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("9223372036854775807", numeric_limits<int64_t>::max(),
      StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-9223372036854775808", numeric_limits<int64_t>::min(),
      StringParser::PARSE_SUCCESS);
}

TEST(StringToInt, Overflow) {
  TestIntValue<int8_t>("128", 127, StringParser::PARSE_OVERFLOW);
  TestIntValue<int8_t>("-129", -128, StringParser::PARSE_OVERFLOW);
  TestIntValue<int16_t>("32768", 32767, StringParser::PARSE_OVERFLOW);
  TestIntValue<int16_t>("-32769", -32768, StringParser::PARSE_OVERFLOW);
  TestIntValue<int32_t>("2147483648", 2147483647, StringParser::PARSE_OVERFLOW);
  TestIntValue<int32_t>("-2147483649", -2147483648, StringParser::PARSE_OVERFLOW);
  TestIntValue<int64_t>("9223372036854775808", 9223372036854775807LL,
      StringParser::PARSE_OVERFLOW);
  TestIntValue<int64_t>("-9223372036854775809", numeric_limits<int64_t>::min(),
      StringParser::PARSE_OVERFLOW);
}

TEST(StringToInt, Int8_Exhaustive) {
  char buffer[5];
  for (int i = -256; i <= 256; ++i) {
    sprintf(buffer, "%d", i);
    int8_t expected = i;
    if (i > 127) {
      expected = 127;
    } else if (i < -128) {
      expected = -128;
    }
    TestIntValue<int8_t>(buffer, expected,
        i == expected ? StringParser::PARSE_SUCCESS : StringParser::PARSE_OVERFLOW);
  }
}

TEST(StringToIntWithBase, Basic) {
  TestIntValue<int8_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("123", 10, 123, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("12345", 10, 12345, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("12345678", 10, 12345678, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("12345678901234", 10, 12345678901234, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("+0", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-0", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("+0", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-0", 10, 0, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("a", 16, 10, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("A", 16, 10, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("b", 20, 11, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("B", 20, 11, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("z", 36, 35, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("f0a", 16, 3850, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("7", 8, 7, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("10", 2, 2, StringParser::PARSE_SUCCESS);
}

TEST(StringToIntWithBase, NonNumericCharacters) {
  // Alphanumeric digits that are not in base are ok
  TestIntValue<int8_t>("123abc   ", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("-123abc   ", 10, -123, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("   123abc   ", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("a123", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("   a123", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("   -a123", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("   a!123", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("   a!123", 10, 0, StringParser::PARSE_SUCCESS);

  // Trailing white space + digits is not ok
  TestIntValue<int8_t>("   -12  3xyz ", 10, 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("12 3", 10, 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("-12 3", 10, 0, StringParser::PARSE_FAILURE);

  // Must have at least one leading valid digit.
  TestIntValue<int8_t>("!123", 0, StringParser::PARSE_FAILURE);

  // Test empty string and string with only whitespaces.
  TestIntValue<int8_t>("", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   ", 0, StringParser::PARSE_FAILURE);
}

TEST(StringToIntWithBase, Limit) {
  TestIntValue<int8_t>("127", 10, 127, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("-128", 10, -128, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("32767", 10, 32767, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-32768", 10, -32768, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("2147483647", 10, 2147483647, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("-2147483648", 10, -2147483648, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("9223372036854775807", 10, numeric_limits<int64_t>::max(),
      StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-9223372036854775808", 10, numeric_limits<int64_t>::min(),
      StringParser::PARSE_SUCCESS);
}

TEST(StringToIntWithBase, Overflow) {
  TestIntValue<int8_t>("128", 10, 127, StringParser::PARSE_OVERFLOW);
  TestIntValue<int8_t>("-129", 10, -128, StringParser::PARSE_OVERFLOW);
  TestIntValue<int16_t>("32768", 10, 32767, StringParser::PARSE_OVERFLOW);
  TestIntValue<int16_t>("-32769", 10, -32768, StringParser::PARSE_OVERFLOW);
  TestIntValue<int32_t>("2147483648", 10, 2147483647, StringParser::PARSE_OVERFLOW);
  TestIntValue<int32_t>("-2147483649", 10, -2147483648, StringParser::PARSE_OVERFLOW);
  TestIntValue<int64_t>("9223372036854775808", 10, 9223372036854775807LL,
      StringParser::PARSE_OVERFLOW);
  TestIntValue<int64_t>("-9223372036854775809", 10, numeric_limits<int64_t>::min(),
      StringParser::PARSE_OVERFLOW);
}

TEST(StringToIntWithBase, Int8_Exhaustive) {
  char buffer[5];
  for (int i = -256; i <= 256; ++i) {
    sprintf(buffer, "%d", i);
    int8_t expected = i;
    if (i > 127) {
      expected = 127;
    } else if (i < -128) {
      expected = -128;
    }
    TestIntValue<int8_t>(buffer, 10, expected,
        i == expected ? StringParser::PARSE_SUCCESS : StringParser::PARSE_OVERFLOW);
  }
}

TEST(StringToFloat, Basic) {
  TestAllFloatVariants("0", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("123", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.456", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(".456", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.0", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.789", StringParser::PARSE_SUCCESS);

  // Scientific notation.
  TestAllFloatVariants("1e10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1E10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1e-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1E-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.456e10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.456E10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.456e-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.456E-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.789e10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.789E10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.789e-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.789E-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.7e-294", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.7E-294", StringParser::PARSE_SUCCESS);

  // Min/max values.
  string float_min = lexical_cast<string>(numeric_limits<float>::min());
  string float_max = lexical_cast<string>(numeric_limits<float>::max());
  TestFloatValue<float>(float_min, StringParser::PARSE_SUCCESS);
  TestFloatValue<float>(float_max, StringParser::PARSE_SUCCESS);
  string double_min = lexical_cast<string>(numeric_limits<double>::min());
  string double_max = lexical_cast<string>(numeric_limits<double>::max());
  TestFloatValue<double>(double_min, StringParser::PARSE_SUCCESS);
  TestFloatValue<double>(double_max, StringParser::PARSE_SUCCESS);

  // Non-finite values
  TestAllFloatVariants("INFinity", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("infinity", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("inFIniTy", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("inf", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("InF", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("INF", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("iNf", StringParser::PARSE_SUCCESS);

  TestFloatValueIsNan<float>("nan", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<double>("nan", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<float>("NaN", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<double>("NaN", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<float>("naN", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<double>("naN", StringParser::PARSE_SUCCESS);

  // Overflow.
  TestFloatValue<float>(float_max + "11111", StringParser::PARSE_OVERFLOW);
  TestFloatValue<double>(double_max + "11111", StringParser::PARSE_OVERFLOW);
  TestFloatValue<float>("-" + float_max + "11111", StringParser::PARSE_OVERFLOW);
  TestFloatValue<double>("-" + double_max + "11111", StringParser::PARSE_OVERFLOW);

  // Precision limits
  // Regression test for IMPALA-1622 (make sure we get correct result with many digits
  // after decimal)
  TestAllFloatVariants("1.12345678912345678912", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.1234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.01234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.01111111111111111111111", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.1234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.01234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(".1234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.01234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(
      "12345678901234567890.1234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(
      "12345678901234567890.01234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.000000000000000000001234", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.000000000000000000001234", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(".000000000000000000001234", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.000000000000000000001234e10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(
      "00000000000000000000.000000000000000000000", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(
      "00000000000000000000.000000000000000000001", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("12345678901234567890123456", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("12345678901234567890123456e10", StringParser::PARSE_SUCCESS);

  // Invalid floats.
  TestAllFloatVariants("x456.789e10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456x.789e10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.x789e10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.789xe10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.789a10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.789ex10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.789e10x", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.789e10   sdfs ", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("1e10   sdfs", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("in", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("in finity", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("na", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("ThisIsANaN", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("n aN", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("nnaN", StringParser::PARSE_FAILURE);

  // IMPALA-3868: Test that multiple dots are not allowed.
  TestAllFloatVariants(".1.2", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("..12", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("12..", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("1.23.", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("1.23.4", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("1234.5678.90", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("12.34.5.6", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("0..e", StringParser::PARSE_FAILURE);

  //Test longer strings (>100)
  string space_100(100, '0');
  TestAllFloatVariants(space_100 + ".7" + space_100, StringParser::PARSE_SUCCESS);

  // Test broken strings with null-character in the middle.
  string s1("in\0f", 4);
  TestAllFloatVariants(s1, StringParser::PARSE_FAILURE);
  string s2("n\0an", 4);
  TestAllFloatVariants(s2, StringParser::PARSE_FAILURE);
  string s3("1\0.2", 4);
  TestAllFloatVariants(s3, StringParser::PARSE_FAILURE);
}

TEST(StringToFloat, InvalidLeadingTrailing) {
  // Test that trailing garbage is not allowed.
  TestFloatValue<double>("123xyz   ", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("-123xyz   ", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   123xyz   ", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   -12  3xyz ", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("12 3", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("-12 3", StringParser::PARSE_FAILURE);

  // Must have at least one leading valid digit.
  TestFloatValue<double>("x123", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   x123", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   -x123", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   x-123", StringParser::PARSE_FAILURE);

  // Test empty string and string with only whitespaces.
  TestFloatValue<double>("", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   ", StringParser::PARSE_FAILURE);

  // IMPALA-1731: Test that leading/trailing garbage is not allowed when parsing inf.
  TestAllFloatVariants("1.23inf", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("1inf", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("iinf", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("1.23inf456", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("info", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("inf123", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("infinity2", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("infinite", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("inf123nan", StringParser::PARSE_FAILURE);

  // IMPALA-1731: Test that leading/trailing garbage is not allowed when parsing NaN.
  TestAllFloatVariants("1.23nan", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("1nan", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("anan", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("1.23nan456", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("nana", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("nan2", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("nan123inf", StringParser::PARSE_FAILURE);
}

TEST(StringToFloat, BruteForce) {
  TestFloatBruteForce<float>();
  TestFloatBruteForce<double>();
}

TEST(StringToFloat, Preprocess) {
  TestStringToFloatPreprocess("00000000000000", "0");
  TestStringToFloatPreprocess("0000000000000234", "234");
  TestStringToFloatPreprocess("00000000000000.1", "0.1");
  TestStringToFloatPreprocess("0000000134.56", "134.56");
  TestStringToFloatPreprocess(".765", "0.765");
  TestStringToFloatPreprocess("000006.4396785e3", "6.4396785e3");
  TestStringToFloatPreprocess(".43256e4", "0.43256e4");
  // Inputs for which function is Identity function.
  TestStringToFloatPreprocess("0.5", "0.5");
  TestStringToFloatPreprocess("123", "123");
  TestStringToFloatPreprocess("12.33432435454", "12.33432435454");
  TestStringToFloatPreprocess("infinity", "infinity");
  TestStringToFloatPreprocess("NaN", "NaN");
  TestStringToFloatPreprocess("6.4396785e3", "6.4396785e3");
  // Invalid input
  TestStringToFloatPreprocess("0NaN", "0NaN");
  TestStringToFloatPreprocess("000000  ^&$23", "0  ^&$23");
  TestStringToFloatPreprocess(".&*(%", "0.&*(%");
  TestStringToFloatPreprocess("rrtrtsgfg", "rrtrtsgfg");
  TestStringToFloatPreprocess("-0000.4", "-0000.4");
  TestStringToFloatPreprocess("+.4675", "+.4675");
}

TEST(StringToBool, Basic) {
  TestBoolValue("true", true, StringParser::PARSE_SUCCESS);
  TestBoolValue("false", false, StringParser::PARSE_SUCCESS);

  TestBoolValue("false xdfsd", false, StringParser::PARSE_FAILURE);
  TestBoolValue("true xdfsd", false, StringParser::PARSE_FAILURE);
  TestBoolValue("ffffalse xdfsd", false, StringParser::PARSE_FAILURE);
  TestBoolValue("tttfalse xdfsd", false, StringParser::PARSE_FAILURE);
}

TEST(StringToDate, Basic) {
  TestDateValue("2018-11-10", DateValue(2018, 11, 10), StringParser::PARSE_SUCCESS);
  TestDateValue("2018-1-10", DateValue(2018, 1, 10), StringParser::PARSE_SUCCESS);
  TestDateValue("2018-11-1", DateValue(2018, 11, 1), StringParser::PARSE_SUCCESS);

  // Test min/max dates.
  TestDateValue("0001-01-01", DateValue(1, 1, 1), StringParser::PARSE_SUCCESS);
  TestDateValue("9999-12-31", DateValue(9999, 12, 31), StringParser::PARSE_SUCCESS);

  // Test less than min and greater than max dates.
  DateValue invalid_date;
  TestDateValue("0000-12-31", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("-0001-12-31", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("10000-01-01", invalid_date, StringParser::PARSE_FAILURE);

  // Test bad formats.
  TestDateValue("2-11-10", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("20-11-10", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("201-11-10", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("02018-11-10", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("2018-100-10", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("2018-10-100", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("2018-10-", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("2018--11", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("2010-01-01 foo", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("2010-01-01 12:22:22", invalid_date, StringParser::PARSE_FAILURE);

  // Test invalid month/day values.
  TestDateValue("2018-00-10", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("2018-13-10", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("2018-01-0", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("2018-01-32", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("2019-02-39", invalid_date, StringParser::PARSE_FAILURE);

  // Leap year tests: 2100-02-29 doesn't exist but 2000-02-29 does.
  TestDateValue("2100-02-29", invalid_date, StringParser::PARSE_FAILURE);
  TestDateValue("2000-02-29", DateValue(2000, 2, 29), StringParser::PARSE_SUCCESS);
}

}

