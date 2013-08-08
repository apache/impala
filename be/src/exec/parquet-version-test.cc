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
    const string& expected_version, bool expected_is_internal) {
  HdfsParquetScanner::FileVersion v(s);
  EXPECT_EQ(v.application, expected_application) << "String: " << s;
  EXPECT_EQ(v.version, expected_version) << "String: " << s;
  EXPECT_EQ(v.is_impala_internal, expected_is_internal);
}

TEST(ParquetVersionParseTest, Basic) {
  CheckVersionParse("impala version 1.0", "impala", "1.0", false);
  CheckVersionParse("impala VERSION 1.0", "impala", "1.0", false);
  CheckVersionParse("impala VERSION 1.0 ignored", "impala", "1.0", false);
  CheckVersionParse("parquet-mr version 2.0", "parquet-mr", "2.0", false);

  // Test internal versions
  CheckVersionParse("impala version 1.0-internal", "impala", "1.0", true);
  CheckVersionParse("impala version 1.23-internal", "impala", "1.23", true);
  CheckVersionParse("impala version 2-inTERnal", "impala", "2", true);
  CheckVersionParse("mr version 1-internal", "mr", "1-internal", false);

  // Test some malformed strings. They should leave version/app as empty
  CheckVersionParse("parquet-mr 2.0", "", "", false);
  CheckVersionParse("impala ve 2.0", "", "", false);
}

}

int main(int argc, char **argv) {
  impala::CpuInfo::Init();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
