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

#include <string>

#include "runtime/string-value.inline.h"
#include "testutil/gtest-util.h"
#include "util/cpu-info.h"

#include "common/names.h"

namespace impala {

StringValue FromStdString(const string& str) {
  char* ptr = const_cast<char*>(str.c_str());
  int len = str.size();
  return StringValue(ptr, len);
}

TEST(StringValueTest, TestCompare) {
  string empty_str = "";
  string str1_str("\0", 1);
  string str2_str("\0xy", 3);
  string str3_str = "abc";
  string str4_str("abc\0def", 7);
  string str5_str = "abcdef";
  string str6_str = "xyz";
  string str7_str("xyz\0", 4);
  // Include a few long strings so we test the SSE path
  string str8_str("yyyyyyyyyyyyyyyy\0yyyyyyyyyyyyyyyyyy", 35);
  string str9_str("yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy", 34);
  string char0_str("hi", 2);
  string char1_str("hi  ", 4);
  string char2_str(" hi  ", 5);
  string char3_str("12345", 5);
  string char4_str(" ", 1);
  string char5_str("", 0);

  const int NUM_STRINGS = 10;
  const int NUM_CHARS = 6;

  // Must be in lexical order
  StringValue svs[NUM_STRINGS];
  svs[0] = FromStdString(empty_str);
  svs[1] = FromStdString(str1_str);
  svs[2] = FromStdString(str2_str);
  svs[3] = FromStdString(str3_str);
  svs[4] = FromStdString(str4_str);
  svs[5] = FromStdString(str5_str);
  svs[6] = FromStdString(str6_str);
  svs[7] = FromStdString(str7_str);
  svs[8] = FromStdString(str8_str);
  svs[9] = FromStdString(str9_str);

  for (int i = 0; i < NUM_STRINGS; ++i) {
    for (int j = 0; j < NUM_STRINGS; ++j) {
      if (i == j) {
        // Same string
        EXPECT_TRUE(svs[i].Eq(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_FALSE(svs[i].Ne(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_FALSE(svs[i].Lt(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_FALSE(svs[i].Gt(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_TRUE(svs[i].Le(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_TRUE(svs[i].Ge(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_TRUE(svs[i].Compare(svs[j]) == 0) << "i=" << i << " j=" << j;
      } else if (i < j) {
        // svs[i] < svs[j]
        EXPECT_FALSE(svs[i].Eq(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_TRUE(svs[i].Ne(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_TRUE(svs[i].Lt(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_FALSE(svs[i].Gt(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_TRUE(svs[i].Le(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_FALSE(svs[i].Gt(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_TRUE(svs[i].Compare(svs[j]) < 0) << "i=" << i << " j=" << j;
      } else {
        // svs[i] > svs[j]
        EXPECT_FALSE(svs[i].Eq(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_TRUE(svs[i].Ne(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_FALSE(svs[i].Lt(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_TRUE(svs[i].Gt(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_FALSE(svs[i].Le(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_TRUE(svs[i].Gt(svs[j])) << "i=" << i << " j=" << j;
        EXPECT_TRUE(svs[i].Compare(svs[j]) > 0) << "i=" << i << " j=" << j;
      }
    }
  }

  StringValue chars[NUM_CHARS];
  chars[0] = FromStdString(char0_str);
  chars[1] = FromStdString(char1_str);
  chars[2] = FromStdString(char2_str);
  chars[3] = FromStdString(char3_str);
  chars[4] = FromStdString(char4_str);
  chars[5] = FromStdString(char5_str);

  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[0].ptr, 2), 2);
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[1].ptr, 4), 2);
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[2].ptr, 5), 3);
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[3].ptr, 5), 5);
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[4].ptr, 1), 0);
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[5].ptr, 0), 0);

  StringValue::PadWithSpaces(chars[3].ptr, 5, 4);
  EXPECT_EQ(chars[3].ptr[4], ' ');
  EXPECT_EQ(chars[3].ptr[3], '4');
}

}

IMPALA_TEST_MAIN();
