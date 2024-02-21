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

#include "testutil/gtest-util.h"

#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/test_util.h"
#include "util/string-util.h"

#include "common/names.h"

namespace impala {

enum Truncation {
  DOWN,
  UP
};

void EvalTruncation(const string& original, const string& expected_result,
    int32_t max_length, Truncation boundary) {
  string result;
  if (boundary == DOWN) {
    ASSERT_OK(TruncateDown(original, max_length, &result));
  } else {
    ASSERT_OK(TruncateUp(original, max_length, &result));
  }
  EXPECT_EQ(expected_result, result);
}

TEST(TruncateDownTest, Basic) {
  EvalTruncation("0123456789", "0123456789", 100, DOWN);
  EvalTruncation("0123456789", "0123456789", 10, DOWN);
  EvalTruncation("0123456789", "01234", 5, DOWN);
  EvalTruncation("0123456789", "", 0, DOWN);
  EvalTruncation("", "", 10, DOWN);
  EvalTruncation(string("\0\0\0", 3), string("\0\0", 2), 2, DOWN);
  EvalTruncation("asdfghjkl", "asdf", 4, DOWN);
  char a[] = {'a', CHAR_MAX, CHAR_MIN, 'b', '\0'};
  char b[] = {'a', CHAR_MAX, '\0'};
  EvalTruncation(a, b, 2, DOWN);
}

TEST(TruncateUpTest, Basic) {
  EvalTruncation("0123456789", "0123456789", 100, UP);
  EvalTruncation("abcdefghij", "abcdefghij", 10, UP);
  EvalTruncation("abcdefghij", "abcdefghj", 9, UP);
  EvalTruncation("abcdefghij", "abcdf", 5, UP);

  string max_string(100, 0xFF);
  EvalTruncation(max_string, max_string, 100, UP);

  string normal_plus_max = "abcdef" + max_string;
  EvalTruncation(normal_plus_max, normal_plus_max, 200, UP);
  EvalTruncation(normal_plus_max, "abcdeg", 10, UP);

  string result;
  Status s = TruncateUp(max_string, 10, &result);
  EXPECT_EQ(s.GetDetail(), "TruncateUp() couldn't increase string.\n");

  EvalTruncation("", "", 10, UP);
  EvalTruncation(string("\0\0\0", 3), string("\0\001", 2), 2, UP);
  EvalTruncation("asdfghjkl", "asdg", 4, UP);
  char a[] = {0, (char)0x7F, (char)0xFF, 0};
  char b[] = {0, (char)0x80, 0};
  EvalTruncation(a, b, 2, UP);
}

TEST(CommaSeparatedContainsTest, Basic) {
  // Basic tests with string present.
  EXPECT_TRUE(CommaSeparatedContains("LZO", "LZO"));
  EXPECT_TRUE(CommaSeparatedContains("foo,LZO", "LZO"));
  EXPECT_TRUE(CommaSeparatedContains("LZO,bar", "LZO"));
  EXPECT_TRUE(CommaSeparatedContains("foo,LZO,bar", "LZO"));

  // Handles zero-length entries.
  EXPECT_FALSE(CommaSeparatedContains("", "LZO"));
  EXPECT_FALSE(CommaSeparatedContains(",", "LZO"));
  EXPECT_FALSE(CommaSeparatedContains(",,", "LZO"));
  EXPECT_TRUE(CommaSeparatedContains("foo,LZO,", "LZO"));
  EXPECT_TRUE(CommaSeparatedContains(",foo,LZO,", "LZO"));
  EXPECT_TRUE(CommaSeparatedContains(",foo,,LZO,", "LZO"));

  // Basic tests with string absent.
  EXPECT_FALSE(CommaSeparatedContains("foo,bar", "LZO"));
  EXPECT_FALSE(CommaSeparatedContains("foo", "LZO"));
  EXPECT_FALSE(CommaSeparatedContains("foo,", "LZO"));
  EXPECT_FALSE(CommaSeparatedContains("foo,bar,baz", "LZO"));
  EXPECT_FALSE(CommaSeparatedContains(",foo,LzO,", "LZO"));

  // Pattern is longer than token.
  EXPECT_FALSE(CommaSeparatedContains(",foo,LzO,", "ZZZZZ"));
  // Pattern is longer than string.
  EXPECT_FALSE(CommaSeparatedContains("foo", "ZZZZZ"));

  // Whitespace is included in tokens alone.
  EXPECT_FALSE(CommaSeparatedContains("foo , foo, foo,\nfoo,\tfoo", "foo"));
}

TEST(FindUtf8PosForwardTest, Basic) {
  // Each Chinese character is encoded into 3 bytes in UTF-8.
  EXPECT_EQ(0, FindUtf8PosForward("李小龙", 9, 0));
  EXPECT_EQ(3, FindUtf8PosForward("李小龙", 9, 1));
  EXPECT_EQ(6, FindUtf8PosForward("李小龙", 9, 2));
  EXPECT_EQ(9, FindUtf8PosForward("李小龙", 9, 3));
  EXPECT_EQ(9, FindUtf8PosForward("李小龙", 9, 4));
  EXPECT_EQ(10, FindUtf8PosForward("李小龙Bruce Lee", 18, 4));
  EXPECT_EQ(11, FindUtf8PosForward("李小龙Bruce Lee", 18, 5));
  EXPECT_EQ(18, FindUtf8PosForward("李小龙Bruce Lee", 18, 50));

  // Test with a combination of UTF8 characters in 1, 2, 3 and 4 bytes.
  // 'Б', 'и' and 'ö' are encoded into 2 bytes. '和' is 3 bytes. '🙂' is 4 bytes.
  int byte_lens[] = {
      1, 1, 1, 1, 1, 1, 2, 1, 1, 2, 1, 1, // byte lengths for "hello Бopиc "
      3, 1, 1, 2, 1, 1, 1, 1,             // byte lengths for "和 Jörg! "
      4, 1, 1, 1                          // byte lengths for "🙂 Hi"
  };
  const char test_str[] = "Hello Бopиc 和 Jörg! 🙂 Hi";
  // Ignore tailing '\0'
  int total_byte_len = sizeof(test_str) / sizeof(char) - 1;
  int total_chars = sizeof(byte_lens) / sizeof(int);
  int pos = 0;
  for (int i = 0; i < total_chars; ++i) {
    EXPECT_EQ(pos, FindUtf8PosForward(test_str, total_byte_len, i));
    pos += byte_lens[i];
  }
  EXPECT_EQ(total_byte_len, pos);
  EXPECT_EQ(total_byte_len, FindUtf8PosForward(test_str, total_byte_len, total_chars));

  // \x93 is a non-ascii byte and not the start byte of any UTF-8 characters. It will be
  // treated as a malformed character and counted as one.
  EXPECT_EQ(9, FindUtf8PosForward("李小龙 \x93\x93 ", 13, 3));
  EXPECT_EQ(10, FindUtf8PosForward("李小龙 \x93\x93 ", 13, 4));
  EXPECT_EQ(11, FindUtf8PosForward("李小龙 \x93\x93 ", 13, 5));
  EXPECT_EQ(12, FindUtf8PosForward("李小龙 \x93\x93 ", 13, 6));
  EXPECT_EQ(13, FindUtf8PosForward("李小龙 \x93\x93 ", 13, 7));
  // Here we just need 4 characters, i.e. "李小龙 " in byte length 10. Set 'str_len'
  // to 10 to make sure the remaining bytes won't be counted.
  EXPECT_EQ(9, FindUtf8PosForward("李小龙 \x93\x93 ", 10, 3));
  EXPECT_EQ(10, FindUtf8PosForward("李小龙 \x93\x93 ", 10, 4));
  EXPECT_EQ(10, FindUtf8PosForward("李小龙 \x93\x93 ", 10, 5));
  EXPECT_EQ(10, FindUtf8PosForward("李小龙 \x93\x93 ", 10, 6));

  // More cases on malformed UTF-8.
  // \xc3 is the start byte of a 2-bytes UTF-8 character.
  // Make sure we won't get overflow index in results
  EXPECT_EQ(0, FindUtf8PosForward("李小龙\xc3", 10, 0));
  EXPECT_EQ(3, FindUtf8PosForward("李小龙\xc3", 10, 1));
  EXPECT_EQ(6, FindUtf8PosForward("李小龙\xc3", 10, 2));
  EXPECT_EQ(9, FindUtf8PosForward("李小龙\xc3", 10, 3));
  EXPECT_EQ(10, FindUtf8PosForward("李小龙\xc3", 10, 4));
  // Test cases for \xc3 in the middle, i.e. "李\xc3小龙".
  // In UTF-8, "小" encodes to [\xe5\xb0\x8f], and "龙" encodes to [\xe9\xbe\x99].
  // \xc3 is the start byte of a 2-bytes UTF-8 character. So "\xc3\xe5" is counted as
  // one character. "\xb0" and "\x8f" is treated as two malformed characters. "龙" is
  // still treated as one character since it's not messed up.
  // This may be inconsistent with Hive. We just make sure it won't crash to process,
  // and will deal with this in IMPALA-10761.
  EXPECT_EQ(0, FindUtf8PosForward("李\xc3小龙", 10, 0));
  EXPECT_EQ(3, FindUtf8PosForward("李\xc3小龙", 10, 1));
  EXPECT_EQ(5, FindUtf8PosForward("李\xc3小龙", 10, 2));
  EXPECT_EQ(6, FindUtf8PosForward("李\xc3小龙", 10, 3));
  EXPECT_EQ(7, FindUtf8PosForward("李\xc3小龙", 10, 4));
  EXPECT_EQ(10, FindUtf8PosForward("李\xc3小龙", 10, 5));
}

TEST(FindUtf8PosBackwardTest, Basic) {
  // Each Chinese character is encoded into 3 bytes in UTF-8.
  EXPECT_EQ(6, FindUtf8PosBackward("李小龙", 9, 0));
  EXPECT_EQ(3, FindUtf8PosBackward("李小龙", 9, 1));
  EXPECT_EQ(0, FindUtf8PosBackward("李小龙", 9, 2));
  EXPECT_EQ(-1, FindUtf8PosBackward("李小龙", 9, 17));
  EXPECT_EQ(17, FindUtf8PosBackward("李小龙Bruce Lee", 18, 0));
  EXPECT_EQ(10, FindUtf8PosBackward("李小龙Bruce Lee", 18, 7));
  EXPECT_EQ(9, FindUtf8PosBackward("李小龙Bruce Lee", 18, 8));
  EXPECT_EQ(6, FindUtf8PosBackward("李小龙Bruce Lee", 18, 9));
  EXPECT_EQ(3, FindUtf8PosBackward("李小龙Bruce Lee", 18, 10));
  EXPECT_EQ(0, FindUtf8PosBackward("李小龙Bruce Lee", 18, 11));
  EXPECT_EQ(0, FindUtf8PosBackward("hello李小龙Bruce Lee", 23, 16));
  EXPECT_EQ(-1, FindUtf8PosBackward("hello李小龙Bruce Lee", 23, 50));

  // Test with a combination of UTF8 characters in 1, 2, 3 and 4 bytes.
  // 'Б', 'и' and 'ö' are encoded into 2 bytes. '和' is 3 bytes. '🙂' is 4 bytes.
  int byte_lens[] = {
      1, 1, 1, 1, 1, 1, 2, 1, 1, 2, 1, 1, // byte lengths for "hello Бopиc "
      3, 1, 1, 2, 1, 1, 1, 1,             // byte lengths for "和 Jörg! "
      4, 1, 1, 1                          // byte lengths for "🙂 Hi"
  };
  const char test_str[] = "Hello Бopиc 和 Jörg! 🙂 Hi";
  // Ignore tailing '\0'
  int total_byte_len = sizeof(test_str) / sizeof(char) - 1;
  int total_chars = sizeof(byte_lens) / sizeof(int);
  int pos = total_byte_len;
  for (int i = 0; i < total_chars; ++i) {
    pos -= byte_lens[total_chars - i - 1];
    EXPECT_EQ(pos, FindUtf8PosBackward(test_str, total_byte_len, i));
  }
  EXPECT_EQ(0, pos);
  EXPECT_EQ(-1, FindUtf8PosBackward(test_str, total_byte_len, total_chars));

  // \x93 is a non-ascii byte and not the start byte of any UTF-8 characters. It will be
  // treated as a malformed character and counted as one.
  EXPECT_EQ(12, FindUtf8PosBackward("李小龙 \x93\x93 ", 13, 0));
  EXPECT_EQ(11, FindUtf8PosBackward("李小龙 \x93\x93 ", 13, 1));
  EXPECT_EQ(10, FindUtf8PosBackward("李小龙 \x93\x93 ", 13, 2));
  EXPECT_EQ(9, FindUtf8PosBackward("李小龙 \x93\x93 ", 13, 3));
  EXPECT_EQ(6, FindUtf8PosBackward("李小龙 \x93\x93 ", 13, 4));
  // Here we just need 4 characters, i.e. "李小龙 " in byte length 10. Set 'str_len'
  // to 10 to make sure the remaining bytes won't be counted.
  EXPECT_EQ(9, FindUtf8PosBackward("李小龙 \x93\x93 ", 10, 0));
  EXPECT_EQ(6, FindUtf8PosBackward("李小龙 \x93\x93 ", 10, 1));
  EXPECT_EQ(3, FindUtf8PosBackward("李小龙 \x93\x93 ", 10, 2));
  EXPECT_EQ(0, FindUtf8PosBackward("李小龙 \x93\x93 ", 10, 3));
  EXPECT_EQ(-1, FindUtf8PosBackward("李小龙 \x93\x93 ", 10, 4));
  // Test malformed UTF-8 bytes at the beginning.
  EXPECT_EQ(1, FindUtf8PosBackward("\x93\x93李小龙", 11, 3));
  EXPECT_EQ(0, FindUtf8PosBackward("\x93\x93李小龙", 11, 4));
  EXPECT_EQ(-1, FindUtf8PosBackward("\x93\x93李小龙", 11, 5));

  // More cases on malformed UTF-8.
  // \xc3 is the start byte of a 2-bytes UTF-8 character.
  EXPECT_EQ(9, FindUtf8PosBackward("李小龙\xc3", 10, 0));
  EXPECT_EQ(6, FindUtf8PosBackward("李小龙\xc3", 10, 1));
  EXPECT_EQ(3, FindUtf8PosBackward("李小龙\xc3", 10, 2));
  EXPECT_EQ(0, FindUtf8PosBackward("李小龙\xc3", 10, 3));
  EXPECT_EQ(-1, FindUtf8PosBackward("李小龙\xc3", 10, 4));
  // Test cases for \xc3 in the middle, i.e. "李\xc3小龙".
  EXPECT_EQ(7, FindUtf8PosBackward("李\xc3小龙", 10, 0));
  EXPECT_EQ(4, FindUtf8PosBackward("李\xc3小龙", 10, 1));
  EXPECT_EQ(3, FindUtf8PosBackward("李\xc3小龙", 10, 2));
  EXPECT_EQ(0, FindUtf8PosBackward("李\xc3小龙", 10, 3));
  EXPECT_EQ(-1, FindUtf8PosBackward("李\xc3小龙", 10, 4));
}

static const int MAX_LEN = 100;
static const int NUM_TRIALS = 100;
TEST(RandomFindUtf8PosTest, Basic) {
  // Fuzz test to make sure we won't crash or return overflow index when processing
  // malformed UTF-8 characters. It takes ~5s.
  char buffer[MAX_LEN];
  kudu::Random rng(kudu::SeedRandom());
  for (int i = 0; i < NUM_TRIALS; ++i) {
    RandomString(buffer, MAX_LEN, &rng);
    // Use the random string 100 times.
    for (int j = 0; j < 100; ++j) {
      int len = rng.Uniform(MAX_LEN + 1);
      int index = rng.Uniform(len + 1);
      int pos = FindUtf8PosForward(buffer, len, index);
      EXPECT_GE(pos, -1);
      EXPECT_LE(pos, len);
      pos = FindUtf8PosBackward(buffer, len, index);
      EXPECT_GE(pos, -1);
      EXPECT_LE(pos, len);
    }
  }
}

// StringStreamPopTest: These tests assert the functionality of the StringStreamPop class.

// Assert the most common use case where the last character is popped and a new character
// is written to the stream.
TEST(StringStreamPopTest, NotEmptyPopOnce) {
  StringStreamPop fixture;
  fixture << "this is a tes,";
  fixture.move_back();
  fixture << "t";
  EXPECT_EQ("this is a test", fixture.str());
}

// Asssert where the stream only contains a single character that is popped before another
// character is written to the stream.
TEST(StringStreamPopTest, OneCharPop) {
  StringStreamPop fixture;
  fixture << "t";
  fixture.move_back();
  fixture << "v";
  EXPECT_EQ("v", fixture.str());
}

// Assert where the last two characters of a non-empty stream are popped.
TEST(StringStreamPopTest, NotEmptyPopTwice) {
  StringStreamPop fixture;
  fixture << "this is a second te,,";
  fixture.move_back();
  fixture.move_back();
  fixture << "st";
  EXPECT_EQ("this is a second test", fixture.str());
}

// Assert where an empty stream has it's last (nonexistant) character popped.
TEST(StringStreamPopTest, EmptyPopOnce) {
  StringStreamPop fixture;
  fixture.move_back();
  EXPECT_TRUE(fixture.str().empty());
}

// Assert where an empty stream has it's last (nonexistant) character popped twice.
TEST(StringStreamPopTest, EmptyPopTwice) {
  StringStreamPop fixture;
  fixture.move_back();
  fixture.move_back();
  EXPECT_TRUE(fixture.str().empty());
}

// Assert the move_back functionality does not actually remove the character.
TEST(StringStreamPopTest, PopOnceBeforeAppend) {
  StringStreamPop fixture;
  fixture.move_back();
  fixture << "a";
  fixture.move_back();

  // This assertion is correct because the move_back() function only moves the write
  // pointer, it does not modify the internal buffer.
  EXPECT_EQ("a", fixture.str());
}

// Assert the StringStreamPop class behavior matches the behavior of the stringstream
// class.
TEST(StringStreamPopTest, CompareWithStringstream) {
  StringStreamPop fixture;
  stringstream expected;

  expected << "C++ is" << " an " << "invisible found" << "ation of " << "everything!";
  fixture  << "C++ is" << " an " << "invisible found" << "ation of " << "everything?";
  fixture.move_back();
  fixture << '!';

  EXPECT_EQ(expected.str(), fixture.str());
}

}

