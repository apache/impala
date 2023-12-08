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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include "common/logging.h"
#include "testutil/gtest-util.h"
#include "util/coding-util.h"
#include "util/ubsan.h"

#include "common/names.h"

namespace impala {

// Tests encoding/decoding of input.  If expected_encoded is non-empty, the
// encoded string is validated against it.
void TestUrl(const string& input, const string& expected_encoded, bool hive_compat) {
  string intermediate;
  UrlEncode(input, &intermediate, hive_compat);
  string output;
  if (!expected_encoded.empty()) {
    EXPECT_EQ(intermediate, expected_encoded);
  }
  EXPECT_TRUE(UrlDecode(intermediate, &output, hive_compat));
  EXPECT_EQ(input, output);

  // Convert string to vector and try that also
  vector<uint8_t> input_vector;
  input_vector.resize(input.size());
  Ubsan::MemCpy(input_vector.data(), input.c_str(), input.size());
  string intermediate2;
  UrlEncode(input_vector, &intermediate2, hive_compat);
  EXPECT_EQ(intermediate, intermediate2);
}

void TestBase64(const string& input, const string& expected_encoded) {
  string intermediate;
  Base64Encode(input, &intermediate);
  if (!expected_encoded.empty()) {
    EXPECT_EQ(intermediate, expected_encoded);
  }
  int64_t out_max = 0;
  EXPECT_TRUE(Base64DecodeBufLen(intermediate.c_str(), intermediate.size(), &out_max));
  string output(out_max, '\0');
  unsigned out_len = 0;
  EXPECT_TRUE(Base64Decode(intermediate.c_str(), intermediate.size(),
      out_max, const_cast<char*>(output.c_str()), &out_len));
  output.resize(out_len);
  EXPECT_EQ(input, output);

  // Convert string to vector and try that also
  vector<uint8_t> input_vector;
  input_vector.resize(input.size());
  memcpy(input_vector.data(), input.c_str(), input.size());
  string intermediate2;
  Base64Encode(input_vector, &intermediate2);
  EXPECT_EQ(intermediate, intermediate2);
}

// Test Base64 encoding when the variables in which the calculated maximal output size and
// the actual output size are stored have specific initial values (regression test for
// IMPALA-12986).
void TestBase64EncodeWithInitialValues(int64_t initial_max_value,
    int64_t initial_out_value) {
  const string bytes = "abc\1\2\3";

  int64_t base64_max_len = initial_max_value;
  bool succ = Base64EncodeBufLen(bytes.size(), &base64_max_len);
  EXPECT_TRUE(succ);

  // 'base64_max_len' includes the null terminator.
  string buf(base64_max_len - 1, '\0');
  unsigned base64_len = initial_out_value;
  succ = Base64Encode(bytes.c_str(), bytes.size(), base64_max_len, buf.data(),
      &base64_len);
  EXPECT_TRUE(succ);

  const string expected = "YWJjAQID";
  EXPECT_EQ(expected, buf);
}

// Test URL encoding. Check that the values that are put in are the
// same that come out.
TEST(UrlCodingTest, Basic) {
  string input = "ABCDEFGHIJKLMNOPQRSTUWXYZ1234567890~!@#$%^&*()<>?,./:\";'{}|[]\\_+-=";
  TestUrl(input, "", false);
  TestUrl(input, "", true);
}

TEST(UrlCodingTest, HiveExceptions) {
  TestUrl(" +", " +", true);
}

TEST(UrlCodingTest, BlankString) {
  TestUrl("", "", false);
  TestUrl("", "", true);
}

TEST(UrlCodingTest, PathSeparators) {
  string test_path = "/home/impala/directory/";
  string encoded_test_path = "%2Fhome%2Fimpala%2Fdirectory%2F";
  TestUrl(test_path, encoded_test_path, false);
  TestUrl(test_path, encoded_test_path, true);
  string test = "SpecialCharacters\x01\x02\x03\x04\x05\x06\x07\b\t\n\v\f\r\x0E\x0F\x10"
                "\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F\x7F\"#%'"
                "*/:=?\\{[]^";
  string encoded_test = "SpecialCharacters%01%02%03%04%05%06%07%08%09%0A%0B%0C%0D%0E%0F"
                        "%10%11%12%13%14%15%16%17%18%19%1A%1B%1C%1D%1E%1F%7F%22%23%25"
                        "%27%2A%2F%3A%3D%3F%5C%7B%5B%5D%5E";
  TestUrl(test, encoded_test, false);
  TestUrl(test, encoded_test, true);
}

TEST(Base64Test, Basic) {
  TestBase64("a", "YQ==");
  TestBase64("ab", "YWI=");
  TestBase64("abc", "YWJj");
  TestBase64("abcd", "YWJjZA==");
  TestBase64("abcde", "YWJjZGU=");
  TestBase64("abcdef", "YWJjZGVm");
  TestBase64(string("a\0", 2), "YQA=");
  TestBase64(string("ab\0", 3), "YWIA");
  TestBase64(string("abc\0", 4), "YWJjAA==");
  TestBase64(string("abcd\0", 5), "YWJjZAA=");
  TestBase64(string("abcde\0", 6), "YWJjZGUA");
  TestBase64(string("abcdef\0", 7), "YWJjZGVmAA==");
  TestBase64(string("a\0b", 3), "YQBi");
  TestBase64(string("a\0b\0", 4), "YQBiAA==");
}

TEST(Base64Test, VariousInitialVariableValues) {
  TestBase64EncodeWithInitialValues(0, 0);
  TestBase64EncodeWithInitialValues(5, -10);
  // Test a value that doesn't fit in 32 bits.
  TestBase64EncodeWithInitialValues(5, 88090617260393);
}

TEST(HtmlEscapingTest, Basic) {
  string before = "<html><body>&amp";
  stringstream after;
  EscapeForHtml(before, &after);
  EXPECT_EQ(after.str(), "&lt;html&gt;&lt;body&gt;&amp;amp");
}

}

