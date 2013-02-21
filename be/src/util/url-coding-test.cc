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
#include "util/url-coding.h"
#include "util/logging.h"

using namespace std;

namespace impala {

// Test URL encoding. Check that the values that are put in are the
// same that come out.
TEST(UrlCodingTest, Basic) {
  string input = "ABCDEFGHIJKLMNOPQRSTUWXYZ1234567890~!@#$%^&*()<>?,./:\";'{}|[]\\_+-=";
  string intermediate;
  UrlEncode(input, &intermediate);
  string output;
  EXPECT_TRUE(UrlDecode(intermediate, &output));
  EXPECT_EQ(input, output);
}

TEST(UrlCodingTest, BlankString) {
  string intermediate, output;
  UrlEncode("", &intermediate);
  EXPECT_TRUE(UrlDecode(intermediate, &output));
  EXPECT_EQ("", output);
}

TEST(UrlCodingTest, PathSeparators) {
  string input = "/home/impala/directory/";
  string intermediate;
  string output;
  UrlEncode(input, &intermediate);
  EXPECT_EQ(intermediate, "%2Fhome%2Fimpala%2Fdirectory%2F");
  EXPECT_TRUE(UrlDecode(intermediate, &output));
  EXPECT_EQ(output, input);
}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
