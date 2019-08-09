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

#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>

#include "testutil/gtest-util.h"
#include "util/mem-info.h"
#include "util/parse-util.h"

#include "common/names.h"

namespace impala {

TEST(ParseMemSpecs, Basic) {
  bool is_percent;
  int64_t bytes;

  int64_t kilobytes = 1024;
  int64_t megabytes = 1024 * kilobytes;
  int64_t gigabytes = 1024 * megabytes;
  int64_t terabytes = 1024 * gigabytes;

  // Initialize 'is_percent' to the opposite of the expected value, to confirm that
  // ParseMemSpec() is actually setting the output parameter.
  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("1", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(1, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("100b", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(100, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("100kb", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(100 * 1024, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("5KB", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(5 * 1024, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("4MB", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(4 * megabytes, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("4m", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(4 * megabytes, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("8gb", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(8 * gigabytes, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("8G", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(8 * gigabytes, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("12Gb", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(12 * gigabytes, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("8T", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(8 * terabytes, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("12tb", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(12 * terabytes, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = false;
  bytes = ParseUtil::ParseMemSpec("13%", &is_percent, MemInfo::physical_mem());
  ASSERT_GT(bytes, 0);
  ASSERT_TRUE(is_percent);

  is_percent = false;
  ASSERT_GT(ParseUtil::ParseMemSpec("17%", &is_percent, MemInfo::physical_mem()), bytes);
  ASSERT_EQ(ParseUtil::ParseMemSpec("17%", &is_percent, 100), 17);
  ASSERT_TRUE(is_percent);

  vector<string> bad_values;
  bad_values.push_back("1gib");
  bad_values.push_back("1%b");
  bad_values.push_back("1b%");
  bad_values.push_back("gb");
  bad_values.push_back("1GMb");
  bad_values.push_back("1b1Mb");
  bad_values.push_back("1kib");
  bad_values.push_back("1Bb");
  bad_values.push_back("1%%");
  bad_values.push_back("1.1");
  bad_values.push_back("1pb");
  bad_values.push_back("1eb");
  stringstream ss;
  ss << UINT64_MAX;
  bad_values.push_back(ss.str());
  bad_values.push_back("%");
  for (vector<string>::iterator it = bad_values.begin(); it != bad_values.end(); it++) {
    bytes = ParseUtil::ParseMemSpec(*it, &is_percent, MemInfo::physical_mem());
    ASSERT_EQ(-1, bytes);
  }

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(0, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("-1", &is_percent, MemInfo::physical_mem());
  ASSERT_EQ(0, bytes);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("-2", &is_percent, MemInfo::physical_mem());
  ASSERT_LT(bytes, 0);
  ASSERT_FALSE(is_percent);

  is_percent = true;
  bytes = ParseUtil::ParseMemSpec("-2%", &is_percent, MemInfo::physical_mem());
  ASSERT_LT(bytes, 0);
  ASSERT_TRUE(is_percent);
}

}

