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

#include "testutil/gtest-util.h"
#include "util/benchmark.h"
#include "util/debug-util.h"

#include "common/names.h"

namespace impala {

TEST(DebugUtil, UniqueID) {
  TUniqueId unique_id;
  unique_id.hi = 0xfeedbeeff00d7777ULL;
  unique_id.lo = 0x2020202020202020ULL;
  std::string str("feedbeeff00d7777:2020202020202020");
  EXPECT_EQ(str, PrintId(unique_id));
  unique_id.lo = 0x20ULL;
  EXPECT_EQ("feedbeeff00d7777:0000000000000020", PrintId(unique_id));
}

TEST(DebugUtil, UniqueIDCompromised) {
  TUniqueId unique_id;
  unique_id.hi = 0xfeedbeeff00d7777ULL;
  unique_id.lo = 0x2020202020202020ULL;
  char out[TUniqueIdBufferSize+1];
  out[TUniqueIdBufferSize] = '\0';

  PrintIdCompromised(unique_id, out);
  EXPECT_EQ(string("feedbeeff00d7777:2020202020202020"), out);

  unique_id.lo = 0x20ULL;
  PrintIdCompromised(unique_id, out);
  EXPECT_EQ(string("feedbeeff00d7777:0000000000000020"), out);
}

string RecursionStack(int level) {
  if (level == 0) return GetStackTrace();
  return RecursionStack(level - 1);
}

TEST(DebugUtil, StackDump) {
  cout << "Stack: " << endl << GetStackTrace() << endl;
  cout << "Stack Recursion: " << endl << RecursionStack(5) << endl;
}

TEST(DebugUtil, QueryIdParsing) {
  TUniqueId id;
  EXPECT_FALSE(ParseId("abcd", &id));
  EXPECT_FALSE(ParseId("abcdabcdabcdabcdabcdabcdabcdabcda", &id));
  EXPECT_FALSE(ParseId("zbcdabcdabcdabcd:abcdabcdabcdabcd", &id));
  EXPECT_FALSE(ParseId("~bcdabcdabcdabcd:abcdabcdabcdabcd", &id));
  EXPECT_FALSE(ParseId("abcdabcdabcdabcd:!bcdabcdabcdabcd", &id));

  EXPECT_TRUE(ParseId("abcdabcdabcdabcd:abcdabcdabcdabcd", &id));
  EXPECT_EQ(id.hi, 0xabcdabcdabcdabcd);
  EXPECT_EQ(id.lo, 0xabcdabcdabcdabcd);

  EXPECT_TRUE(ParseId("abcdabcdabcdabcd:1234abcdabcd5678", &id));
  EXPECT_EQ(id.hi, 0xabcdabcdabcdabcd);
  EXPECT_EQ(id.lo, 0x1234abcdabcd5678);

  EXPECT_TRUE(ParseId("cdabcdabcdabcd:1234abcdabcd5678", &id));
  EXPECT_EQ(id.hi, 0xcdabcdabcdabcd);
  EXPECT_EQ(id.lo, 0x1234abcdabcd5678);

  EXPECT_TRUE(ParseId("cdabcdabcdabcd:abcdabcd5678", &id));
  EXPECT_EQ(id.hi, 0xcdabcdabcdabcd);
  EXPECT_EQ(id.lo, 0xabcdabcd5678);
}

TEST(DebugUtil, PreCDH5QueryIdParsing) {
  TUniqueId id;
  // Pre-CDH5 CM sends query IDs as decimal ints separated by a space.
  EXPECT_TRUE(ParseId("-6067004223159161907 -6067004223159161907", &id));
  EXPECT_EQ(id.hi, 0xabcdabcdabcdabcd);
  EXPECT_EQ(id.lo, 0xabcdabcdabcdabcd);

  // Check components are parsed separately
  EXPECT_TRUE(ParseId("1:2", &id));
  EXPECT_EQ(1, id.hi);
  EXPECT_EQ(2, id.lo);

  // Too many components
  EXPECT_FALSE(
      ParseId("-6067004223159161907 -6067004223159161907 -6067004223159161907", &id));

  // Extra whitespace ok
  EXPECT_TRUE(ParseId("-6067004223159161907  -6067004223159161907", &id));
  EXPECT_EQ(id.hi, 0xabcdabcdabcdabcd);
  EXPECT_EQ(id.lo, 0xabcdabcdabcdabcd);

  // Unsigned representation of 0xffffffffffffffff -- too large to parse
  EXPECT_FALSE(ParseId("18446744073709551615 18446744073709551615", &id));

  // Hex but with a space separator
  EXPECT_FALSE(ParseId("aaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaa", &id));
}

}

