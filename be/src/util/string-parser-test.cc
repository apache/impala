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
#include <gtest/gtest.h>
#include <boost/cstdint.hpp>
#include "util/string-parser.h"

using namespace std;

namespace impala {

template<typename T>
void TestValue(const char* s, T expected_val, StringParser::ParseResult expected_result) {
  StringParser::ParseResult result;
  T val = StringParser::StringToInt<T>(s, strlen(s), &result);
  EXPECT_EQ(expected_val, val) << s;
  EXPECT_EQ(result, expected_result);
}

TEST(StringToInt, Basic) {
  TestValue<int8_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestValue<int16_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestValue<int32_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestValue<int64_t>("123", 123, StringParser::PARSE_SUCCESS);
  
  TestValue<int8_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestValue<int16_t>("12345", 12345, StringParser::PARSE_SUCCESS);
  TestValue<int32_t>("12345678", 12345678, StringParser::PARSE_SUCCESS);
  TestValue<int64_t>("12345678901234", 12345678901234, StringParser::PARSE_SUCCESS);
  
  TestValue<int8_t>("-10", -10, StringParser::PARSE_SUCCESS);
  TestValue<int16_t>("-10", -10, StringParser::PARSE_SUCCESS);
  TestValue<int32_t>("-10", -10, StringParser::PARSE_SUCCESS);
  TestValue<int64_t>("-10", -10, StringParser::PARSE_SUCCESS);
  
  TestValue<int8_t>("+1", 1, StringParser::PARSE_SUCCESS);
  TestValue<int16_t>("+1", 1, StringParser::PARSE_SUCCESS);
  TestValue<int32_t>("+1", 1, StringParser::PARSE_SUCCESS);
  TestValue<int64_t>("+1", 1, StringParser::PARSE_SUCCESS);
  
  TestValue<int8_t>("+0", 0, StringParser::PARSE_SUCCESS);
  TestValue<int16_t>("-0", 0, StringParser::PARSE_SUCCESS);
  TestValue<int32_t>("+0", 0, StringParser::PARSE_SUCCESS);
  TestValue<int64_t>("-0", 0, StringParser::PARSE_SUCCESS);
}

TEST(StringToInt, Limit) {
  TestValue<int8_t>("127", 127, StringParser::PARSE_SUCCESS);
  TestValue<int8_t>("-128", -128, StringParser::PARSE_SUCCESS);
  TestValue<int16_t>("32767", 32767, StringParser::PARSE_SUCCESS);
  TestValue<int16_t>("-32768", -32768, StringParser::PARSE_SUCCESS);
  TestValue<int32_t>("2147483647", 2147483647, StringParser::PARSE_SUCCESS);
  TestValue<int32_t>("-2147483648", -2147483648, StringParser::PARSE_SUCCESS);
  TestValue<int64_t>("9223372036854775807", numeric_limits<int64_t>::max(), 
      StringParser::PARSE_SUCCESS);
  TestValue<int64_t>("-9223372036854775808", numeric_limits<int64_t>::min(),
      StringParser::PARSE_SUCCESS);
}

TEST(StringToInt, Overflow) {
  TestValue<int8_t>("128", 127, StringParser::PARSE_OVERFLOW);
  TestValue<int8_t>("-129", -128, StringParser::PARSE_OVERFLOW);
  TestValue<int16_t>("32768", 32767, StringParser::PARSE_OVERFLOW);
  TestValue<int16_t>("-32769", -32768, StringParser::PARSE_OVERFLOW);
  TestValue<int32_t>("2147483648", 2147483647, StringParser::PARSE_OVERFLOW);
  TestValue<int32_t>("-2147483649", -2147483648, StringParser::PARSE_OVERFLOW);
  TestValue<int64_t>("9223372036854775808", 9223372036854775807LL, 
      StringParser::PARSE_OVERFLOW);
  TestValue<int64_t>("-9223372036854775809", numeric_limits<int64_t>::min(),
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
    TestValue<int8_t>(buffer, expected, 
        i == expected ? StringParser::PARSE_SUCCESS : StringParser::PARSE_OVERFLOW);
  }
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
