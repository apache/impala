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

#include <cstdlib>

#include <boost/filesystem.hpp>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>

#include "common/init.h"
#include "runtime/tmp-file-mgr.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "util/filesystem-util.h"
#include "util/metrics.h"

#include "gen-cpp/Types_types.h"  // for TUniqueId

#include "common/names.h"

using boost::filesystem::path;

namespace impala {

class TmpFileMgrTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    metrics_.reset(new MetricGroup("tmp-file-mgr-test"));
  }

  virtual void TearDown() {
    metrics_.reset();
  }

  /// Check that metric values are consistent with TmpFileMgr state.
  void CheckMetrics(TmpFileMgr* tmp_file_mgr) {
    vector<TmpFileMgr::DeviceId> active = tmp_file_mgr->active_tmp_devices();
    IntGauge* active_metric = metrics_->FindMetricForTesting<IntGauge>(
        "tmp-file-mgr.active-scratch-dirs");
    EXPECT_EQ(active.size(), active_metric->value());
    SetMetric<string>* active_set_metric =
        metrics_->FindMetricForTesting<SetMetric<string>>(
        "tmp-file-mgr.active-scratch-dirs.list");
    set<string> active_set = active_set_metric->value();
    EXPECT_EQ(active.size(), active_set.size());
    for (int i = 0; i < active.size(); ++i) {
      string tmp_dir_path = tmp_file_mgr->GetTmpDirPath(active[i]);
      EXPECT_TRUE(active_set.find(tmp_dir_path) != active_set.end());
    }
  }

  void RemoveAndCreateDirs(const vector<string>& dirs) {
    for (const string& dir: dirs) {
      ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(dir));
    }
  }

  scoped_ptr<MetricGroup> metrics_;
};

/// Regression test for IMPALA-2160. Verify that temporary file manager allocates blocks
/// at the expected file offsets and expands the temporary file to the correct size.
TEST_F(TmpFileMgrTest, TestFileAllocation) {
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.Init(metrics_.get()));
  TmpFileMgr::FileGroup file_group(&tmp_file_mgr);
  // Default configuration should give us one temporary device.
  EXPECT_EQ(1, tmp_file_mgr.num_active_tmp_devices());
  vector<TmpFileMgr::DeviceId> tmp_devices = tmp_file_mgr.active_tmp_devices();
  EXPECT_EQ(1, tmp_devices.size());
  TUniqueId id;
  TmpFileMgr::File *file;
  ASSERT_OK(file_group.NewFile(tmp_devices[0], id, &file));
  EXPECT_TRUE(file != NULL);
  // Apply writes of variable sizes and check space was allocated correctly.
  int64_t write_sizes[] = {
    1, 10, 1024, 4, 1024 * 1024 * 8, 1024 * 1024 * 8, 16, 10
  };
  int num_write_sizes = sizeof(write_sizes)/sizeof(write_sizes[0]);
  int64_t next_offset = 0;
  for (int i = 0; i < num_write_sizes; ++i) {
    int64_t offset;
    ASSERT_OK(file->AllocateSpace(write_sizes[i], &offset));
    EXPECT_EQ(next_offset, offset);
    next_offset = offset + write_sizes[i];
    EXPECT_EQ(next_offset, boost::filesystem::file_size(file->path()));
  }
  // Check that cleanup is correct.
  string file_path = file->path();
  file_group.Close();
  EXPECT_FALSE(boost::filesystem::exists(file_path));
  CheckMetrics(&tmp_file_mgr);
}

/// Test that we can do initialization with two directories on same device and
/// that validations prevents duplication of directories.
TEST_F(TmpFileMgrTest, TestOneDirPerDevice) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  tmp_file_mgr.InitCustom(tmp_dirs, true, metrics_.get());
  TmpFileMgr::FileGroup file_group(&tmp_file_mgr);

  // Only the first directory should be used.
  EXPECT_EQ(1, tmp_file_mgr.num_active_tmp_devices());
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.active_tmp_devices();
  EXPECT_EQ(1, devices.size());
  TUniqueId id;
  TmpFileMgr::File *file;
  ASSERT_OK(file_group.NewFile(devices[0], id, &file));
  // Check the prefix is the expected temporary directory.
  EXPECT_EQ(0, file->path().find(tmp_dirs[0]));
  FileSystemUtil::RemovePaths(tmp_dirs);
  file_group.Close();
  CheckMetrics(&tmp_file_mgr);
}

/// Test that we can do custom initialization with two dirs on same device.
TEST_F(TmpFileMgrTest, TestMultiDirsPerDevice) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  tmp_file_mgr.InitCustom(tmp_dirs, false, metrics_.get());
  TmpFileMgr::FileGroup file_group(&tmp_file_mgr);

  // Both directories should be used.
  EXPECT_EQ(2, tmp_file_mgr.num_active_tmp_devices());
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.active_tmp_devices();
  EXPECT_EQ(2, devices.size());
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    EXPECT_EQ(0, tmp_file_mgr.GetTmpDirPath(devices[i]).find(tmp_dirs[i]));
    TUniqueId id;
    TmpFileMgr::File *file;
    ASSERT_OK(file_group.NewFile(devices[i], id, &file));
    // Check the prefix is the expected temporary directory.
    EXPECT_EQ(0, file->path().find(tmp_dirs[i]));
  }
  FileSystemUtil::RemovePaths(tmp_dirs);
  file_group.Close();
  CheckMetrics(&tmp_file_mgr);
}

/// Test that reporting a write error is possible but does not result in
/// blacklisting, which is disabled.
TEST_F(TmpFileMgrTest, TestReportError) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  tmp_file_mgr.InitCustom(tmp_dirs, false, metrics_.get());
  TmpFileMgr::FileGroup file_group(&tmp_file_mgr);

  // Both directories should be used.
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.active_tmp_devices();
  EXPECT_EQ(2, devices.size());
  CheckMetrics(&tmp_file_mgr);

  // Inject an error on one device so that we can validate it is handled correctly.
  TUniqueId id;
  int good_device = 0, bad_device = 1;
  TmpFileMgr::File* bad_file;
  ASSERT_OK(file_group.NewFile(devices[bad_device], id, &bad_file));
  ErrorMsg errmsg(TErrorCode::GENERAL, "A fake error");
  bad_file->ReportIOError(errmsg);

  // Blacklisting is disabled.
  EXPECT_FALSE(bad_file->is_blacklisted());
  // The second device should still be active.
  EXPECT_EQ(2, tmp_file_mgr.num_active_tmp_devices());
  vector<TmpFileMgr::DeviceId> devices_after = tmp_file_mgr.active_tmp_devices();
  EXPECT_EQ(2, devices_after.size());
  CheckMetrics(&tmp_file_mgr);

  // Attempts to expand bad file should succeed.
  int64_t offset;
  ASSERT_OK(bad_file->AllocateSpace(128, &offset));
  // The good device should still be usable.
  TmpFileMgr::File* good_file;
  ASSERT_OK(file_group.NewFile(devices[good_device], id, &good_file));
  EXPECT_TRUE(good_file != NULL);
  ASSERT_OK(good_file->AllocateSpace(128, &offset));
  // Attempts to allocate new files on bad device should succeed.
  ASSERT_OK(file_group.NewFile(devices[bad_device], id, &bad_file));
  FileSystemUtil::RemovePaths(tmp_dirs);
  file_group.Close();
  CheckMetrics(&tmp_file_mgr);
}

TEST_F(TmpFileMgrTest, TestAllocateFails) {
  string tmp_dir("/tmp/tmp-file-mgr-test.1");
  string scratch_subdir = tmp_dir + "/impala-scratch";
  vector<string> tmp_dirs({tmp_dir});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  tmp_file_mgr.InitCustom(tmp_dirs, false, metrics_.get());
  TmpFileMgr::FileGroup file_group(&tmp_file_mgr);

  TUniqueId id;
  TmpFileMgr::File* allocated_file1;
  TmpFileMgr::File* allocated_file2;
  int64_t offset;
  ASSERT_OK(file_group.NewFile(0, id, &allocated_file1));
  ASSERT_OK(file_group.NewFile(0, id, &allocated_file2));
  ASSERT_OK(allocated_file1->AllocateSpace(1, &offset));

  // Make scratch non-writable and test for allocation errors at different stages:
  // new file creation, files with no allocated blocks. files with allocated space.
  chmod(scratch_subdir.c_str(), 0);
  // allocated_file1 already has space allocated.
  EXPECT_FALSE(allocated_file1->AllocateSpace(1, &offset).ok());
  // allocated_file2 has no space allocated.
  EXPECT_FALSE(allocated_file2->AllocateSpace(1, &offset).ok());
  // Creating a new File object can succeed because it is not immediately created on disk.
  TmpFileMgr::File* unallocated_file;
  ASSERT_OK(file_group.NewFile(0, id, &unallocated_file));

  chmod(scratch_subdir.c_str(), S_IRWXU);
  FileSystemUtil::RemovePaths(tmp_dirs);
  file_group.Close();
}

// Test scratch limit is applied correctly to group of files.
TEST_F(TmpFileMgrTest, TestScratchLimit) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  tmp_file_mgr.InitCustom(tmp_dirs, false, metrics_.get());

  const int64_t LIMIT = 100;
  const int64_t FILE1_ALLOC = 25;
  const int64_t FILE2_ALLOC = LIMIT - FILE1_ALLOC;
  TmpFileMgr::FileGroup file_group(&tmp_file_mgr, LIMIT);
  TmpFileMgr::File* file1;
  TmpFileMgr::File* file2;
  TUniqueId id;
  ASSERT_OK(file_group.NewFile(0, id, &file1));
  ASSERT_OK(file_group.NewFile(1, id, &file2));

  // Test individual limit is enforced.
  Status status;
  int64_t offset;
  status = file1->AllocateSpace(LIMIT + 1, &offset);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.code(), TErrorCode::SCRATCH_LIMIT_EXCEEDED);
  ASSERT_OK(file1->AllocateSpace(FILE1_ALLOC, &offset));
  ASSERT_EQ(0, offset);

  // Test aggregate limit is enforced.
  status = file2->AllocateSpace(FILE2_ALLOC + 1, &offset);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.code(), TErrorCode::SCRATCH_LIMIT_EXCEEDED);
  ASSERT_OK(file2->AllocateSpace(FILE2_ALLOC, &offset));
  ASSERT_EQ(0, offset);
  status = file2->AllocateSpace(1, &offset);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.code(), TErrorCode::SCRATCH_LIMIT_EXCEEDED);

  file_group.Close();
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  return RUN_ALL_TESTS();
}
