// Copyright 2015 Cloudera Inc.
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

#include <cstdlib>

#include <boost/filesystem.hpp>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>

#include "common/init.h"
#include "runtime/tmp-file-mgr.h"

#include "gen-cpp/Types_types.h"  // for TUniqueId

#include "common/names.h"

namespace impala {

class TmpFileMgrTest : public ::testing::Test {
 protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

/// Regression test for IMPALA-2160. Verify that temporary file manager allocates blocks
/// at the expected file offsets and expands the temporary file to the correct size.
TEST_F(TmpFileMgrTest, TestFileAllocation) {
  // Default configuration should give us one temporary device.
  EXPECT_EQ(1, TmpFileMgr::num_tmp_devices());
  TUniqueId id;
  TmpFileMgr::File *file;
  Status status = TmpFileMgr::GetFile(0, id, &file);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(file != NULL);
  // Apply writes of variable sizes and check space was allocated correctly.
  int64_t write_sizes[] = {
    1, 10, 1024, 4, 1024 * 1024 * 8, 1024 * 1024 * 8, 16, 10
  };
  int num_write_sizes = sizeof(write_sizes)/sizeof(write_sizes[0]);
  int64_t next_offset = 0;
  for (int i = 0; i < num_write_sizes; ++i) {
    int64_t offset;
    status = file->AllocateSpace(write_sizes[i], &offset);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(next_offset, offset);
    next_offset = offset + write_sizes[i];
    EXPECT_EQ(next_offset, boost::filesystem::file_size(file->path()));
  }
  // Check that cleanup is correct.
  status = file->Remove();
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(boost::filesystem::exists(file->path()));
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::TmpFileMgr::Init();
  return RUN_ALL_TESTS();
}
