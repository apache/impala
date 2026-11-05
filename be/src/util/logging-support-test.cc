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

#include <boost/filesystem.hpp>
#include <ctime>
#include <limits>
#include <sstream>

#include "common/logging.h"
#include "testutil/gtest-util.h"
#include "util/filesystem-util.h"
#include "util/logging-support.h"

using namespace impala;
namespace filesystem = boost::filesystem;
using filesystem::path;

void CreateFile(const path& p, time_t last_modified) {
  EXPECT_TRUE(FileSystemUtil::CreateFile(p.string()).ok());
  // We modify the timestamps to guarantee that they are unique.
  filesystem::last_write_time(p, last_modified);
  EXPECT_TRUE(filesystem::exists(p));
}

TEST(LoggingSupport, DeleteOldLogs) {
  path dir = filesystem::unique_path();
  filesystem::create_directories(dir);

  time_t current_time;
  time(&current_time);

  // Start with one file
  path file1 = dir / "impala1";
  CreateFile(file1, current_time - 4);

  string filename_matcher = (filesystem::canonical(dir) / "impala*").string();

  // If there is only one file, it should never be deleted, regardless
  // of the value of max_log_files.
  LoggingSupport::DeleteOldLogs(filename_matcher, 0);
  EXPECT_TRUE(filesystem::exists(file1));
  LoggingSupport::DeleteOldLogs(filename_matcher, 1);
  EXPECT_TRUE(filesystem::exists(file1));
  LoggingSupport::DeleteOldLogs(filename_matcher, 2);
  EXPECT_TRUE(filesystem::exists(file1));

  // Add two more files.
  path file2 = dir / "impala2";
  CreateFile(file2, current_time - 3);

  path file3 = dir / "impala3";
  CreateFile(file3, current_time - 2);

  // max_log_files < 0 shouldn't remove any files
  LoggingSupport::DeleteOldLogs(filename_matcher, -1);
  EXPECT_TRUE(filesystem::exists(file1));
  EXPECT_TRUE(filesystem::exists(file2));
  EXPECT_TRUE(filesystem::exists(file3));

  // The oldest file (file1) shoujld be deleted.
  LoggingSupport::DeleteOldLogs(filename_matcher, 2);
  EXPECT_FALSE(filesystem::exists(file1));
  EXPECT_TRUE(filesystem::exists(file2));
  EXPECT_TRUE(filesystem::exists(file3));

  // Add two more files.
  path file4 = dir / "impala4";
  CreateFile(file4, current_time - 1);

  path file5 = dir / "impala5";
  CreateFile(file5, current_time);

  // The oldest 2 files (file2 and file3) should be deleted.
  LoggingSupport::DeleteOldLogs(filename_matcher, 2);
  EXPECT_FALSE(filesystem::exists(file1));
  EXPECT_FALSE(filesystem::exists(file2));
  EXPECT_FALSE(filesystem::exists(file3));
  EXPECT_TRUE(filesystem::exists(file4));
  EXPECT_TRUE(filesystem::exists(file5));

  filesystem::remove_all(dir);
}

static string Int128ToString(__int128_t val) {
  std::stringstream ss;
  ss << val;
  return ss.str();
}

TEST(LoggingSupport, PrintInt128) {
  const __int128_t int128_max =
      static_cast<__int128_t>(~static_cast<__uint128_t>(0) >> 1);
  const __int128_t int128_min = -int128_max - 1;
  const __int128_t one_billion = 1000000000;

  EXPECT_EQ("0", Int128ToString(0));
  EXPECT_EQ("1", Int128ToString(1));
  EXPECT_EQ("-1", Int128ToString(-1));
  EXPECT_EQ("999999999", Int128ToString(one_billion - 1));
  EXPECT_EQ("1000000000", Int128ToString(one_billion));
  EXPECT_EQ("-1000000000", Int128ToString(-one_billion));
  // Inner base-1-billion parts must be zero-padded.
  EXPECT_EQ("1000000000000000001", Int128ToString(one_billion * one_billion + 1));
  EXPECT_EQ("-9223372036854775808", Int128ToString(std::numeric_limits<int64_t>::min()));
  EXPECT_EQ("170141183460469231731687303715884105727", Int128ToString(int128_max));
  EXPECT_EQ("-170141183460469231731687303715884105727", Int128ToString(-int128_max));
  // Negating the minimum value as a signed __int128_t would overflow.
  EXPECT_EQ("-170141183460469231731687303715884105728", Int128ToString(int128_min));
}
