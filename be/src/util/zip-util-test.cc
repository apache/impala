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

#include "util/zip-util.h"

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include <sys/stat.h>

#include "common/init.h"
#include "common/status.h"
#include "testutil/gtest-util.h"
#include "util/filesystem-util.h"
#include "util/test-info.h"

#include "common/names.h"

namespace filesystem = boost::filesystem;
using filesystem::path;

using namespace impala;

// Tests for util/zip-util.cc.
TEST(ZipUtilTest, Basic) {
  // Create path to a unique directory to hold destination directories.
  path uniq_dir = filesystem::unique_path();
  // Make sure the directory doesn't exist yet.
  EXPECT_FALSE(filesystem::exists(uniq_dir));

  // Extracting files from a valid zip archive should be successful.
  string archive_file1 = Substitute("$0/testdata/tzdb/2017c.zip", getenv("IMPALA_HOME"));
  path dest_dir1 = uniq_dir / "impala.zip.test.1" / "2017c";
  EXPECT_OK(ZipUtil::ExtractFiles(archive_file1, dest_dir1.string()));
  // Verify that some extracted files exist.
  path zone_info_root_dir = dest_dir1 / "zoneinfo";
  EXPECT_TRUE(filesystem::exists(zone_info_root_dir / "UTC"));
  EXPECT_TRUE(filesystem::exists(zone_info_root_dir / "America" / "Indiana" / "Marengo"));

  // Extracting files from a non-existent zip archive should fail.
  string archive_file2 = Substitute("$0/testdata/tzdb/non-existent.zip",
      getenv("IMPALA_HOME"));
  path dest_dir2 = uniq_dir / "impala.zip.test.2";
  EXPECT_ERROR(ZipUtil::ExtractFiles(archive_file2, dest_dir2.string()),
      TErrorCode::GENERAL);
  // Make sure destination directory hasn't been created.
  EXPECT_FALSE(filesystem::exists(dest_dir2));

  // Extracting files from a corrupt zip archive should fail.
  string archive_file3 = Substitute("$0/testdata/tzdb/2017c-corrupt.zip",
      getenv("IMPALA_HOME"));
  path dest_dir3 = uniq_dir / "impala.zip.test.3";
  EXPECT_ERROR(ZipUtil::ExtractFiles(archive_file3, dest_dir3.string()),
      TErrorCode::GENERAL);

  // Extracting files to a non-writable directory should fail.
  string archive_file4 = Substitute("$0/testdata/tzdb/2017c.zip", getenv("IMPALA_HOME"));
  path dest_dir4 = uniq_dir / "impala.zip.test.4";
  EXPECT_TRUE(mkdir(dest_dir4.string().c_str(),
      S_IRUSR | S_IXUSR | S_IRWXG | S_IRWXO) == 0);
  EXPECT_ERROR(ZipUtil::ExtractFiles(archive_file4, dest_dir4.string()),
      TErrorCode::GENERAL);
  // Make sure the archive's root directory hasn't been created.
  EXPECT_FALSE(filesystem::exists(dest_dir4 / "zoneinfo"));

  // Cleanup
  filesystem::remove_all(uniq_dir);
}
