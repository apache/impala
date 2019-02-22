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

#include <cstring>

#include "runtime/string-value.inline.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

// Test to verify that impala::StringCompare() and clib strncmp() give equivalent results.
namespace impala {

// Expect that strncmp() and StringCompare() return the same result.
static void RunTestCase(const string& l, const string& r) {
  int cmp_len = ::min(l.size(), r.size());
  int strncmp_r = strncmp(l.c_str(), r.c_str(), cmp_len);
  int stringcompare_r = StringCompare(l.c_str(), l.size(), r.c_str(), r.size(), cmp_len);
  if (strncmp_r == 0) {
    EXPECT_EQ(stringcompare_r, 0) << l << " " << r;
  } else if (strncmp_r > 0) {
    EXPECT_GT(stringcompare_r, 0) << l << " " << r;
  } else {
    EXPECT_LT(stringcompare_r, 0) << l << " " << r;
  }
}

TEST(StringCompareTest, Basic) {
  const struct Cases {
    string lhs, rhs;
  } cases[] = {{"abc", "abc"},
               {"abc", "abd"},
               {"", ""},
               {"\x01", "\x01"},
               {"\x01", "\x02"},
               {"\x7f", "\x01"},
               {"\x80", "\x80"},
               {"\x80", "\x01"},
               {"\x7F", "\x80"},
               {"\xFF", "\x01"},
               {"\xFF", "\x7F"},
               {"\xE9", "\x32"},
               {"\x32", "\xE5"},
               {"\xE5", "\xE9"}};

  for (const Cases& c : cases) {
    RunTestCase(c.lhs, c.rhs);
    RunTestCase(c.rhs, c.lhs);
  }

  // Induce the SSE 4.2 code path by using longer strings.
  string suffix = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
  for (const Cases& c : cases) {
    RunTestCase(c.lhs + suffix, c.rhs + suffix);
    RunTestCase(c.rhs + suffix, c.lhs + suffix);
  }
}
}

