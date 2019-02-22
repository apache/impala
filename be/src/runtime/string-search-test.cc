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

#include <gtest/gtest.h>

#include "runtime/string-search.h"

namespace impala {

StringValue StrValFromCString(const char* str, int len) {
  if (len == -1) len = strlen(str);
  return StringValue(const_cast<char*>(str), len);
}

// Test string search for haystack/needle, up to len for each.
// If the length is -1, use the full string length.
// Searching in a substring allows checking whether the search respects the
// length stored in the StringValue or also reads parts of the string which
// should be excluded.
int TestSearch(const char* haystack, const char* needle, int haystack_len = -1,
    int needle_len = -1) {
  StringValue needle_str_val = StrValFromCString(needle, needle_len);
  StringValue haystack_str_val = StrValFromCString(haystack, haystack_len);
  StringSearch search(&needle_str_val);
  return search.Search(&haystack_str_val);
}

// Test reverse string search for haystack/needle, up to len for each.
// If the length is -1, use the full string length.
// Searching in a substring allows checking whether the search respects the
// length stored in the StringValue or also reads parts of the string which
// should be excluded.
int TestRSearch(const char* haystack, const char* needle, int haystack_len = -1,
    int needle_len = -1) {
  StringValue needle_str_val = StrValFromCString(needle, needle_len);
  StringValue haystack_str_val = StrValFromCString(haystack, haystack_len);
  StringSearch search(&needle_str_val);
  return search.RSearch(&haystack_str_val);
}

TEST(StringSearchTest, RegularSearch) {
  // Basic Tests
  EXPECT_EQ(0, TestSearch("abcdabcd", "a"));
  EXPECT_EQ(0, TestSearch("abcdabcd", "ab"));
  EXPECT_EQ(2, TestSearch("abcdabcd", "c"));
  EXPECT_EQ(2, TestSearch("abcdabcd", "cd"));
  EXPECT_EQ(-1, TestSearch("", ""));
  EXPECT_EQ(-1, TestSearch("abc", ""));
  EXPECT_EQ(-1, TestSearch("", "a"));

  // Test single chars
  EXPECT_EQ(0, TestSearch("a", "a"));
  EXPECT_EQ(-1, TestSearch("a", "b"));
  EXPECT_EQ(1, TestSearch("abc", "b"));

  // Test searching in a substring of the haystack
  EXPECT_EQ(0, TestSearch("abcabcd", "abc", 5));
  EXPECT_EQ(-1, TestSearch("abcabcd", "abcd", 5));
  EXPECT_EQ(-1, TestSearch("abcabcd", "abcd", 0));

  // Test searching a substring of the needle in a substring of the haystack.
  EXPECT_EQ(0, TestSearch("abcde", "abaaaaaa", 4, 2));
  EXPECT_EQ(-1, TestSearch("abcde", "abaaaaaa", 4, 3));
  EXPECT_EQ(-1, TestSearch("abcdabaaaaa", "abaaaaaa", 4, 3));
  EXPECT_EQ(-1, TestSearch("abcdabaaaaa", "abaaaaaa", 4, 0));
  EXPECT_EQ(-1, TestSearch("abcdabaaaaa", "abaaaaaa"));

  // Test with needle matching the end of the substring.
  EXPECT_EQ(4, TestSearch("abcde", "e"));
  EXPECT_EQ(3, TestSearch("abcde", "de"));
  EXPECT_EQ(2, TestSearch("abcdede", "cde"));
  EXPECT_EQ(5, TestSearch("abcdacde", "cde"));

  // Test correct skip_ handling, which depends on which char of the needle is
  // the same as the last one.
  EXPECT_EQ(2, TestSearch("ababcacac", "abcacac"));
}

TEST(StringSearchTest, ReverseSearch) {
  // Basic Tests
  EXPECT_EQ(4, TestRSearch("abcdabcd", "a"));
  EXPECT_EQ(4, TestRSearch("abcdabcd", "ab"));
  EXPECT_EQ(6, TestRSearch("abcdabcd", "c"));
  EXPECT_EQ(6, TestRSearch("abcdabcd", "cd"));
  EXPECT_EQ(-1, TestRSearch("", ""));
  EXPECT_EQ(-1, TestRSearch("abc", ""));
  EXPECT_EQ(-1, TestRSearch("", "a"));

  // Test single chars
  EXPECT_EQ(0, TestRSearch("a", "a"));
  EXPECT_EQ(-1, TestRSearch("a", "b"));
  EXPECT_EQ(1, TestRSearch("abc", "b"));

  // Test searching in a substring of the haystack
  EXPECT_EQ(0, TestRSearch("abcabcd", "abc", 5));
  EXPECT_EQ(-1, TestRSearch("abcabcd", "abcd", 5));
  EXPECT_EQ(-1, TestRSearch("abcabcd", "abcd", 0));

  // Test searching a substring of the needle in a substring of the haystack.
  EXPECT_EQ(0, TestRSearch("abcde", "abaaaaaa", 4, 2));
  EXPECT_EQ(-1, TestRSearch("abcde", "abaaaaaa", 4, 3));
  EXPECT_EQ(-1, TestRSearch("abcdabaaaaa", "abaaaaaa", 4, 3));
  EXPECT_EQ(-1, TestRSearch("abcdabaaaaa", "abaaaaaa", 4, 0));
  EXPECT_EQ(-1, TestRSearch("abcdabaaaaa", "abaaaaaa"));

  // Test with needle matching the end of the substring.
  EXPECT_EQ(4, TestRSearch("abcde", "e"));
  EXPECT_EQ(3, TestRSearch("abcde", "de"));
  EXPECT_EQ(2, TestRSearch("abcdede", "cde"));
  EXPECT_EQ(5, TestRSearch("abcdacde", "cde"));

  // Test correct rskip_ handling, which depends on which char of the needle is
  // the same as the first one.
  EXPECT_EQ(0, TestRSearch("cacacbaba", "cacacba"));
}
}
