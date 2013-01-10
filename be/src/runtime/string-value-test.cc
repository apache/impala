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

#include <string>
#include <gtest/gtest.h>

#include "runtime/string-value.inline.h"
#include "util/cpu-info.h"

using namespace std;

namespace impala {

StringValue FromStdString(const string& str) {
  char* ptr = const_cast<char*>(str.c_str());
  int len = str.size();
  return StringValue(ptr, len);
}

TEST(StringValueTest, TestCompare) {
  string empty_str = "";
  string str1_str = "abc";
  string str2_str = "abcdef";
  string str3_str = "xyz";

  const int NUM_STRINGS = 4;

  // Must be in lexical order
  StringValue svs[NUM_STRINGS];
  svs[0] = FromStdString(empty_str);
  svs[1] = FromStdString(str1_str);
  svs[2] = FromStdString(str2_str);
  svs[3] = FromStdString(str3_str);

  for (int i = 0; i < NUM_STRINGS; ++i) {
    for (int j = 0; j < NUM_STRINGS; ++j) {
      if (i == j) {
        // Same string
        EXPECT_TRUE(svs[i].Eq(svs[j]));
        EXPECT_FALSE(svs[i].Ne(svs[j]));
        EXPECT_FALSE(svs[i].Lt(svs[j]));
        EXPECT_FALSE(svs[i].Gt(svs[j]));
        EXPECT_TRUE(svs[i].Le(svs[j]));
        EXPECT_TRUE(svs[i].Ge(svs[j]));
        EXPECT_TRUE(svs[i].Compare(svs[j]) == 0);
      } else if (i < j) {
        // svs[i] < svs[j]
        EXPECT_FALSE(svs[i].Eq(svs[j]));
        EXPECT_TRUE(svs[i].Ne(svs[j]));
        EXPECT_TRUE(svs[i].Lt(svs[j]));
        EXPECT_FALSE(svs[i].Gt(svs[j]));
        EXPECT_TRUE(svs[i].Le(svs[j]));
        EXPECT_FALSE(svs[i].Gt(svs[j]));
        EXPECT_TRUE(svs[i].Compare(svs[j]) < 0);
      } else {
        // svs[i] > svs[j]
        EXPECT_FALSE(svs[i].Eq(svs[j]));
        EXPECT_TRUE(svs[i].Ne(svs[j]));
        EXPECT_FALSE(svs[i].Lt(svs[j]));
        EXPECT_TRUE(svs[i].Gt(svs[j]));
        EXPECT_FALSE(svs[i].Le(svs[j]));
        EXPECT_TRUE(svs[i].Gt(svs[j]));
        EXPECT_TRUE(svs[i].Compare(svs[j]) > 0);
      }
    }
  }
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  return RUN_ALL_TESTS();
}

