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

void TestCompareImpl(StringValue* svs, int NUM_STRINGS) {
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
}

class StringValueTest : public ::testing::Test {
protected:
    void SmallifySV(StringValue* sv) { sv->Smallify(); }

    void SmallifySVExpect(StringValue* sv, bool expect_to_succeed) {
      if (expect_to_succeed) {
        EXPECT_TRUE(sv->Smallify());
      } else {
        EXPECT_FALSE(sv->Smallify());
      }
    }

    void TestLargestSmallerString(StringValue& sv, const string& expected) {
      EXPECT_EQ(sv.LargestSmallerString(), expected);
      sv.Smallify();
      EXPECT_EQ(sv.LargestSmallerString(), expected);
    }

    void TestLeastLargerString(StringValue& sv, const string& expected) {
      EXPECT_EQ(sv.LeastLargerString(), expected);
      sv.Smallify();
      EXPECT_EQ(sv.LeastLargerString(), expected);
    }
};

TEST_F(StringValueTest, TestCompare) {
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

  const int NUM_STRINGS = 10;

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

  TestCompareImpl(svs, NUM_STRINGS);
  for (int i = 0; i < NUM_STRINGS; ++i) {
    SmallifySV(&svs[i]);
  }
  TestCompareImpl(svs, NUM_STRINGS);
}

void TestUnpaddedCharLength(StringValue* chars) {
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[0].Ptr(), 2), 2);
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[1].Ptr(), 4), 2);
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[2].Ptr(), 5), 3);
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[3].Ptr(), 5), 5);
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[4].Ptr(), 1), 0);
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[5].Ptr(), 0), 0);
  EXPECT_EQ(StringValue::UnpaddedCharLength(chars[6].Ptr(), 20), 17);
}

TEST_F(StringValueTest, TestCharFunctions) {
  string char0_str("hi", 2);
  string char1_str("hi  ", 4);
  string char2_str(" hi  ", 5);
  string char3_str("12345", 5);
  string char4_str(" ", 1);
  string char5_str("", 0);
  string char6_str("   0123456789ABCD   ");

  const int NUM_CHARS = 7;
  StringValue chars[NUM_CHARS];
  chars[0] = FromStdString(char0_str);
  chars[1] = FromStdString(char1_str);
  chars[2] = FromStdString(char2_str);
  chars[3] = FromStdString(char3_str);
  chars[4] = FromStdString(char4_str);
  chars[5] = FromStdString(char5_str);
  chars[6] = FromStdString(char6_str);

  TestUnpaddedCharLength(chars);
  for (int i = 0; i < NUM_CHARS; ++i) {
    SmallifySV(&chars[i]);
  }
  TestUnpaddedCharLength(chars);

  StringValue::PadWithSpaces(chars[3].Ptr(), 5, 4);
  EXPECT_EQ(chars[3].Ptr()[4], ' ');
  EXPECT_EQ(chars[3].Ptr()[3], '4');

  StringValue::PadWithSpaces(chars[6].Ptr(), 20, 10);
  EXPECT_EQ(chars[6].Ptr()[10], ' ');
  EXPECT_EQ(chars[6].Ptr()[9], '6');
}

void TestConvertToUInt64Impl(StringValue* svs) {
  EXPECT_EQ(svs[0].ToUInt64(), 0);
  EXPECT_EQ(svs[1].ToUInt64(), 0x100000000000000);
  EXPECT_EQ(svs[2].ToUInt64(), 0x102000000000000);
  EXPECT_EQ(svs[3].ToUInt64(), 0x102030000000000);

  // extra character(s) does not change the result
  EXPECT_EQ(svs[4].ToUInt64(),   0x102030405060707);
  EXPECT_EQ(svs[5].ToUInt64(), 0x102030405060707);
  EXPECT_EQ(svs[6].ToUInt64(), 0x102030405060707);
}

TEST_F(StringValueTest, TestConvertToUInt64) {
  // Test converting StringValues to uint64_t which utilizes up to first 8 bytes.
  const int NUM_STRINGS = 7;
  string strings[NUM_STRINGS];
  strings[0] = "";
  strings[1] = "\1";
  strings[2] = "\1\2";
  strings[3] = "\1\2\3";
  strings[4] = "\1\2\3\4\5\6\7\7";
  strings[5] = "\1\2\3\4\5\6\7\7\7";
  strings[6] = "\1\2\3\4\5\6\7\7\7\7\7\7\7\7\7";

  // Must be in lexical order
  StringValue svs[NUM_STRINGS];
  for (int i = 0; i < NUM_STRINGS; ++i) {
    svs[i] = FromStdString(strings[i]);
  }

  TestConvertToUInt64Impl(svs);
  for (int i = 0; i < NUM_STRINGS; ++i) {
    SmallifySV(&svs[i]);
  }
  TestConvertToUInt64Impl(svs);
}

// Test finding the largest smaller strings.
TEST_F(StringValueTest, TestLargestSmallerString) {
  string oneKbNullStr(1024, 0x00);
  string a1023NullStr(1023, 0x00);
  EXPECT_EQ(StringValue(oneKbNullStr).LargestSmallerString(), a1023NullStr);

  StringValue asv(const_cast<char*>("\x12\xef"), 2);
  TestLargestSmallerString(asv, "\x12\xee");
  StringValue bsv(const_cast<char*>("\x12\x00"), 2);
  TestLargestSmallerString(bsv, "\x12");

  // "0x00" is the smallest non-empty string.
  string oneNullStr("\00", 1);
  StringValue oneNullStrSv(oneNullStr);
  TestLargestSmallerString(oneNullStrSv, "");

  // The empty string is the absolute smallest string.
  StringValue emptySv(const_cast<char*>(""));
  TestLargestSmallerString(emptySv, "");
}

// Test finding the least larger strings.
TEST_F(StringValueTest, TestLeastLargerString) {
  string nullStr(const_cast<char*>("\x00"), 1);
  StringValue nullStrSv(nullStr);
  TestLeastLargerString(nullStrSv, string("\x01", 1));

  string a10230xFFStr(1023, 0xff);
  string oneKbStr(1023, 0xff);
  oneKbStr.append(1, 0x00);
  StringValue a10230xFFStrSv(a10230xFFStr);
  TestLeastLargerString(a10230xFFStrSv, oneKbStr);

  StringValue asv(const_cast<char*>("\x12\xef"), 2);
  TestLeastLargerString(asv, "\x12\xf0");
  StringValue bsv(const_cast<char*>("\x12\xff"), 2);
  TestLeastLargerString(bsv, "\x13");

  string smallLimit(11, 0xff);
  StringValue smallLimitSv(smallLimit);
  string smallLimitLeastLarger = smallLimit + '\0';
  TestLeastLargerString(smallLimitSv, smallLimitLeastLarger);

  StringValue emptySv(const_cast<char*>(""));
  TestLeastLargerString(emptySv, string("\00", 1));
}

TEST_F(StringValueTest, TestConstructors) {
  // Test that all strings are non-small initially.
  StringValue def_ctor;
  EXPECT_FALSE(def_ctor.IsSmall());

  StringValue copy_ctor(def_ctor);
  EXPECT_FALSE(copy_ctor.IsSmall());
  // Modify 'copy_ctor' to make Clang Tidy happy.
  SmallifySVExpect(&copy_ctor, true);

  StringValue char_ctor(const_cast<char*>("small"));
  EXPECT_FALSE(char_ctor.IsSmall());

  StringValue char_n(const_cast<char*>("small"), 5);
  EXPECT_FALSE(char_n.IsSmall());

  string small_str("small");
  StringValue string_ctor(small_str);
  EXPECT_FALSE(string_ctor.IsSmall());
}

TEST_F(StringValueTest, TestSmallify) {
  StringValue nullstr;
  StringValue empty(const_cast<char*>(""), 0);
  StringValue one_char(const_cast<char*>("a"), 1);
  StringValue limit(const_cast<char*>("0123456789A"), 11);
  StringValue over_the_limit(const_cast<char*>("0123456789AB"), 12);

  StringValue nullstr_clone(nullstr);
  StringValue empty_clone(empty);
  StringValue one_char_clone(one_char);
  StringValue limit_clone(limit);
  StringValue over_the_limit_clone(over_the_limit);

  SmallifySVExpect(&nullstr, true);
  SmallifySVExpect(&empty, true);
  SmallifySVExpect(&one_char, true);
  SmallifySVExpect(&limit, true);
  SmallifySVExpect(&over_the_limit, false);

  EXPECT_EQ(nullstr, nullstr_clone);
  EXPECT_NE(nullstr.Ptr(), nullstr_clone.Ptr());

  EXPECT_EQ(empty, empty_clone);
  EXPECT_NE(empty.Ptr(), empty_clone.Ptr());

  EXPECT_EQ(one_char, one_char_clone);
  EXPECT_NE(one_char.Ptr(), one_char_clone.Ptr());

  EXPECT_EQ(limit, limit_clone);
  EXPECT_NE(limit.Ptr(), limit_clone.Ptr());

  EXPECT_EQ(over_the_limit, over_the_limit_clone);
  EXPECT_EQ(over_the_limit.Ptr(), over_the_limit_clone.Ptr());
}

}

