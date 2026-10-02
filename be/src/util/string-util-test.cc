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

// Don't cut in the middle of a character.
TEST(TruncateDownTest, Utf8) {
  // Cut inside a 2, 3 and 4-byte character.
  EvalTruncation("a\xC3\xA9", "a", 2, DOWN);         // aé
  EvalTruncation("a\xE4\xB8\xAD", "a", 2, DOWN);     // a中
  EvalTruncation("a\xE4\xB8\xAD", "a", 3, DOWN);     // a中
  EvalTruncation("a\xF0\x9F\x98\x80", "a", 2, DOWN); // a😀
  EvalTruncation("a\xF0\x9F\x98\x80", "a", 4, DOWN); // a😀

  // Cut on a character boundary.
  EvalTruncation("a\xC3\xA9", "a\xC3\xA9", 3, DOWN);                 // aé
  EvalTruncation("a\xE4\xB8\xAD", "a\xE4\xB8\xAD", 4, DOWN);         // a中
  EvalTruncation("a\xF0\x9F\x98\x80", "a\xF0\x9F\x98\x80", 5, DOWN); // a😀

  // The only character is dropped.
  EvalTruncation("\xE4\xB8\xAD", "", 2, DOWN);       // 中

  // Still invalid after trimming the lead byte, so cut bytes.
  EvalTruncation(string("a\xC3\xC3\xC3", 4),
      string("a\xC3\xC3", 3), 3, DOWN); // aÃÃÃ => aÃÃ
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

TEST(TruncateUpTest, Utf8) {
  // The last whole character is replaced by the next code point.
  EvalTruncation("\xC3\xA9\xC3\xA9", "\xC3\xAA", 3, UP);             // éé => ê
  EvalTruncation("\xE4\xB8\xAD\xE4\xB8\xAD", "\xE4\xB8\xAE", 4, UP); // 中中 => 丮
  EvalTruncation("\xC3\xBF\xC3\xBF", "\xC4\x80", 3, UP);             // ÿÿ => Ā

  // Partial character dropped first.
  EvalTruncation("a\xE4\xB8\xAD\xE4\xB8\xAD", "b", 3, UP); // a中中 => b

  // The next code point can be a byte longer.
  // U+007F (7F) => U+0080 (C2 80)
  EvalTruncation("\x7F\x7F\x7F", "\xC2\x80", 2, UP);
  // U+07FF (DF BF) => U+0800 (E0 A0 80)
  EvalTruncation("\xDF\xBF\xDF\xBF", "\xE0\xA0\x80", 3, UP);
  // U+FFFF (EF BF BF) => U+10000 (F0 90 80 80)
  EvalTruncation("\xEF\xBF\xBF\xEF\xBF\xBF", "\xF0\x90\x80\x80", 4, UP);

  // If it doesn't fit, the character before it goes up instead.
  EvalTruncation("aa\x7Fz", "ab", 3, UP);
  // Surrogates are skipped: U+D7FF => U+E000.
  EvalTruncation("\xED\x9F\xBF\xED\x9F\xBF", "\xEE\x80\x80", 3, UP);
  // Nothing above U+10FFFF, so bytes are increased instead.
  EvalTruncation("\xF4\x8F\xBF\xBF\xF4\x8F\xBF\xBF", "\xF4\x8F\xBF\xC0", 4, UP);
  // The 0xFF tail trims away, leaving a valid "aaa" => "aab".
  EvalTruncation(string("aaa\xFF\xFF", 5), "aab", 4, UP);
  // Nothing valid left after trimming: byte-wise path, trailing 0xFFs dropped.
  EvalTruncation(string("a\xFF\xFF\xFF\xFF", 5), "b", 4, UP);
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

