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

#include "util/string-util.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"

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

}

