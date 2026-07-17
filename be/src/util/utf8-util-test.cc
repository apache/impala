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

#include "util/utf8-util.h"

#include "common/names.h"

namespace impala {

// Helper so tests can pass string literals without computing lengths by hand.
// Uses sizeof(literal) - 1 to exclude the trailing NUL, which also allows
// embedded NUL bytes.
#define EXPECT_VALID_UTF8(literal) \
    EXPECT_TRUE(IsValidUtf8(literal, sizeof(literal) - 1))
#define EXPECT_INVALID_UTF8(literal) \
    EXPECT_FALSE(IsValidUtf8(literal, sizeof(literal) - 1))

TEST(Utf8UtilTest, Empty) {
  // An empty buffer is valid, and null is accepted when len == 0.
  EXPECT_TRUE(IsValidUtf8(nullptr, 0));
  EXPECT_TRUE(IsValidUtf8("", 0));
}

TEST(Utf8UtilTest, Ascii) {
  EXPECT_VALID_UTF8("hello world");
  // Embedded NUL is valid UTF-8.
  EXPECT_TRUE(IsValidUtf8("a\0b", 3));
}

TEST(Utf8UtilTest, ValidMultibyte) {
  EXPECT_VALID_UTF8("\xc3\xa9"); // U+00E9 é (2 bytes)
  EXPECT_VALID_UTF8("\xd0\x91"); // U+0411 Б (2 bytes)
  EXPECT_VALID_UTF8("\xe4\xbd\xa0\xe5\xa5\xbd"); // 你好 (3 bytes each)
  EXPECT_VALID_UTF8("\xf0\x9f\x99\x82"); // U+1F642 🙂 (4 bytes)
  EXPECT_VALID_UTF8("a\xc3\xa9\xe4\xbd\xa0\xf0\x9f\x99\x82z"); // mixed widths
}

TEST(Utf8UtilTest, LoneContinuationByte) {
  EXPECT_INVALID_UTF8("\x80");
  EXPECT_INVALID_UTF8("\xbf");
  EXPECT_INVALID_UTF8("abc\x80xyz"); // invalid byte surrounded by valid ASCII
}

TEST(Utf8UtilTest, TruncatedSequence) {
  EXPECT_INVALID_UTF8("\xc3");         // lead byte of a 2-byte seq, missing cont.
  EXPECT_INVALID_UTF8("\xe4\xbd");     // 2 of 3 bytes
  EXPECT_INVALID_UTF8("\xf0\x9f\x99"); // 3 of 4 bytes
}

TEST(Utf8UtilTest, InvalidContinuation) {
  EXPECT_INVALID_UTF8("\xc3\x28"); // second byte is not a continuation byte
  EXPECT_INVALID_UTF8("\xe4\x28\xa0");
}

TEST(Utf8UtilTest, InvalidBytes) {
  EXPECT_INVALID_UTF8("\xff");
  EXPECT_INVALID_UTF8("\xfe");
  EXPECT_INVALID_UTF8("\xc0\x80"); // overlong encoding of U+0000
}

TEST(Utf8UtilTest, CodepointBoundaries) {
  // U+10FFFF, the largest valid code point: F4 8F BF BF.
  EXPECT_VALID_UTF8("\xf4\x8f\xbf\xbf");
  // One past the maximum (U+110000): F4 90 80 80 is invalid.
  EXPECT_INVALID_UTF8("\xf4\x90\x80\x80");
  // UTF-16 surrogate half U+D800 (ED A0 80) is not valid UTF-8.
  EXPECT_INVALID_UTF8("\xed\xa0\x80");
}

TEST(Utf8UtilTest, FindFirstInvalidPosition) {
  // Valid inputs report -1.
  EXPECT_EQ(-1, FindFirstInvalidUtf8(nullptr, 0));
  EXPECT_EQ(-1, FindFirstInvalidUtf8("hello", 5));
  EXPECT_EQ(-1, FindFirstInvalidUtf8("\xc3\xa9", 2)); // é
  // The offset points at the first invalid byte.
  EXPECT_EQ(0, FindFirstInvalidUtf8("\xff", 1));
  EXPECT_EQ(3, FindFirstInvalidUtf8("abc\x80xyz", 7)); // lone continuation byte
  EXPECT_EQ(2, FindFirstInvalidUtf8("ab\xc3", 3));     // truncated 2-byte sequence
}

} // namespace impala
