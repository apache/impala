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

#include "experiments/string-search-sse.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

// Test string search for haystack/needle, up to len for each.
// If the length is -1, use the full string length
// haystack/needle are null terminated
void TestSearch(const char* haystack_orig, const char* needle_orig, int haystack_len = -1,
    int needle_len = -1) {
  string haystack_copy(haystack_orig);
  string needle_copy(needle_orig);
  const char* haystack = haystack_copy.c_str();
  const char* needle = needle_copy.c_str();

  string haystack_buffer, needle_buffer;

  const char* haystack_null_terminated = haystack;
  const char* needle_null_terminated = needle;

  // Make null terminated versions (for libc).
  if (haystack_len != -1) {
    haystack_buffer = string(haystack, haystack_len);
    haystack_null_terminated = haystack_buffer.c_str();
  } else {
    haystack_len = strlen(haystack);
  }

  if (needle_len != -1) {
    needle_buffer = string(needle, needle_len);
    needle_null_terminated = needle_buffer.c_str();
  } else {
    needle_len = strlen(needle);
  }

  // Call libc for correct result
  const char* libc_result = strstr(haystack_null_terminated, needle_null_terminated);
  int libc_offset = (libc_result == NULL) ? -1 : libc_result - haystack_null_terminated;

  StringValue haystack_str_val(const_cast<char*>(haystack), haystack_len);

  // Call our strstr with null terminated needle
  StringSearchSSE needle1(needle_null_terminated);
  int null_offset = needle1.Search(haystack_str_val);
  EXPECT_EQ(null_offset, libc_offset);

  // Call our strstr with non-null terminated needle
  StringValue needle_str_val(const_cast<char*>(needle), needle_len);
  StringSearchSSE needle2(&needle_str_val);
  int not_null_offset = needle2.Search(haystack_str_val);
  EXPECT_EQ(not_null_offset, libc_offset);

  // Ensure haystack/needle are unmodified
  EXPECT_EQ(strlen(needle_null_terminated), needle_len);
  EXPECT_EQ(strlen(haystack_null_terminated), haystack_len);
  EXPECT_EQ(strcmp(haystack, haystack_orig), 0);
  EXPECT_EQ(strcmp(needle, needle_orig), 0);
}

TEST(StringSearchTest, Basic) {
  // Basic Tests
  TestSearch("abcd", "a");
  TestSearch("abcd", "ab");
  TestSearch("abcd", "c");
  TestSearch("abcd", "cd");
  TestSearch("", "");
  TestSearch("abc", "");
  TestSearch("", "a");

  // Test single chars
  TestSearch("a", "a");
  TestSearch("a", "b");
  TestSearch("abc", "b");

  // Haystack is not full string
  TestSearch("abcabcd", "abc", 5);
  TestSearch("abcabcd", "abcd", 5);
  TestSearch("abcabcd", "abcd", 0);

  // Haystack and needle not full len
  TestSearch("abcde", "abaaaaaa", 4, 2);
  TestSearch("abcde", "abaaaaaa", 4, 3);
  TestSearch("abcdabaaaaa", "abaaaaaa", 4, 3);
  TestSearch("abcdabaaaaa", "abaaaaaa", 4, 0);
  TestSearch("abcdabaaaaa", "abaaaaaa", 0, 0);

  // Test last bit, this is the interesting edge case
  TestSearch("abcde", "e");
  TestSearch("abcde", "de");
  TestSearch("abcdede", "cde");
  TestSearch("abcdacde", "cde");
}
}

IMPALA_TEST_MAIN();
