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
#include <limits.h>
#include <gtest/gtest.h>
#include "exec/hdfs-parquet-scanner.h"

using namespace std;

namespace impala {

void CheckVersionParse(const string& s, const string& expected_application,
    int expected_major, int expected_minor, int expected_patch,
    bool expected_is_internal) {
  HdfsParquetScanner::FileVersion v(s);
  EXPECT_EQ(v.application, expected_application) << "String: " << s;
  EXPECT_EQ(v.version.major, expected_major) << "String: " << s;
  EXPECT_EQ(v.version.minor, expected_minor) << "String: " << s;
  EXPECT_EQ(v.version.patch, expected_patch) << "String: " << s;
  EXPECT_EQ(v.is_impala_internal, expected_is_internal);
}

TEST(ParquetVersionTest, Parsing) {
  CheckVersionParse("impala version 1.0", "impala", 1, 0, 0, false);
  CheckVersionParse("impala VERSION 1.0", "impala", 1, 0, 0, false);
  CheckVersionParse("impala VERSION 1.0 ignored", "impala", 1, 0, 0, false);
  CheckVersionParse("parquet-mr version 2.0", "parquet-mr", 2, 0, 0, false);

  CheckVersionParse("impala version 1.2", "impala", 1, 2, 0, false);
  CheckVersionParse("impala version 1.2.3", "impala", 1, 2, 3, false);
  CheckVersionParse("impala version 1.2.3-cdh4.5", "impala", 1, 2, 3, false);
  CheckVersionParse("impala version 1.2.3.cdh4.5", "impala", 1, 2, 3, false);
  CheckVersionParse("impala version 1.2-cdh4.5", "impala", 1, 2, 0, false);
  CheckVersionParse("impala version 1.2.cdh4.5", "impala", 1, 2, 0, false);
  CheckVersionParse("impala version 1.2 (build xyz)", "impala", 1, 2, 0, false);
  CheckVersionParse("impala version cdh4.5", "impala", 0, 0, 0, false);

  // Test internal versions
  CheckVersionParse("impala version 1.0-internal", "impala", 1, 0, 0, true);
  CheckVersionParse("impala version 1.23-internal", "impala", 1, 23, 0, true);
  CheckVersionParse("impala version 2-inTERnal", "impala", 2, 0, 0, true);
  CheckVersionParse("mr version 1-internal", "mr", 1, 0, 0, false);

  // Test some malformed strings.
  CheckVersionParse("parquet-mr 2.0", "parquet-mr", 0, 0, 0, false);
  CheckVersionParse("impala ve 2.0", "impala", 0, 0, 0, false);
  CheckVersionParse("", "", 0, 0, 0, false);
}

TEST(ParquetVersionTest, Comparisons) {
  HdfsParquetScanner::FileVersion v("foo version 1.2.3");
  EXPECT_TRUE(v.VersionEq(1, 2, 3));
  EXPECT_FALSE(v.VersionEq(1, 2, 4));
  EXPECT_TRUE(v.VersionLt(3, 2, 1));
  EXPECT_TRUE(v.VersionLt(1, 2, 4));
  EXPECT_TRUE(v.VersionLt(2, 0, 0));
  EXPECT_FALSE(v.VersionLt(0, 0, 0));
  EXPECT_FALSE(v.VersionLt(1, 2, 3));
  EXPECT_FALSE(v.VersionLt(1, 2, 2));
  EXPECT_FALSE(v.VersionLt(0, 4, 4));
}

}

int main(int argc, char **argv) {
  impala::CpuInfo::Init();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
