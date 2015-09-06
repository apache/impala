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
#include "service/fe-support.h"
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
        metrics_->FindMetricForTesting<SetMetric<string> >(
        "tmp-file-mgr.active-scratch-dirs.list");
    set<string> active_set = active_set_metric->value();
    EXPECT_EQ(active.size(), active_set.size());
    for (int i = 0; i < active.size(); ++i) {
      string tmp_dir_path = tmp_file_mgr->GetTmpDirPath(active[i]);
      EXPECT_TRUE(active_set.find(tmp_dir_path) != active_set.end());
    }
  }

  scoped_ptr<MetricGroup> metrics_;
};

/// Regression test for IMPALA-2160. Verify that temporary file manager allocates blocks
/// at the expected file offsets and expands the temporary file to the correct size.
TEST_F(TmpFileMgrTest, TestFileAllocation) {
  TmpFileMgr tmp_file_mgr;
  EXPECT_TRUE(tmp_file_mgr.Init(metrics_.get()).ok());
  // Default configuration should give us one temporary device.
  EXPECT_EQ(1, tmp_file_mgr.num_active_tmp_devices());
  vector<TmpFileMgr::DeviceId> tmp_devices = tmp_file_mgr.active_tmp_devices();
  EXPECT_EQ(1, tmp_devices.size());
  TUniqueId id;
  TmpFileMgr::File *file;
  Status status = tmp_file_mgr.GetFile(tmp_devices[0], id, &file);
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
  CheckMetrics(&tmp_file_mgr);
}

/// Test that we can do initialization with two directories on same device and
/// that validations prevents duplication of directories.
TEST_F(TmpFileMgrTest, TestOneDirPerDevice) {
  vector<string> tmp_dirs;
  tmp_dirs.push_back("/tmp/tmp-file-mgr-test.1");
  tmp_dirs.push_back("/tmp/tmp-file-mgr-test.2");
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    EXPECT_TRUE(FileSystemUtil::CreateDirectory(tmp_dirs[i]).ok());
  }
  TmpFileMgr tmp_file_mgr;
  tmp_file_mgr.InitCustom(tmp_dirs, true, metrics_.get());

  // Only the first directory should be used.
  EXPECT_EQ(1, tmp_file_mgr.num_active_tmp_devices());
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.active_tmp_devices();
  EXPECT_EQ(1, devices.size());
  TUniqueId id;
  TmpFileMgr::File *file;
  EXPECT_TRUE(tmp_file_mgr.GetFile(devices[0], id, &file).ok());
  // Check the prefix is the expected temporary directory.
  EXPECT_EQ(0, file->path().find(tmp_dirs[0]));
  FileSystemUtil::RemovePaths(tmp_dirs);
  CheckMetrics(&tmp_file_mgr);
}

/// Test that we can do custom initialization with two dirs on same device.
TEST_F(TmpFileMgrTest, TestMultiDirsPerDevice) {
  vector<string> tmp_dirs;
  tmp_dirs.push_back("/tmp/tmp-file-mgr-test.1");
  tmp_dirs.push_back("/tmp/tmp-file-mgr-test.2");
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    EXPECT_TRUE(FileSystemUtil::CreateDirectory(tmp_dirs[i]).ok());
  }
  TmpFileMgr tmp_file_mgr;
  tmp_file_mgr.InitCustom(tmp_dirs, false, metrics_.get());

  // Both directories should be used.
  EXPECT_EQ(2, tmp_file_mgr.num_active_tmp_devices());
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.active_tmp_devices();
  EXPECT_EQ(2, devices.size());
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    EXPECT_EQ(0, tmp_file_mgr.GetTmpDirPath(devices[i]).find(tmp_dirs[i]));
    TUniqueId id;
    TmpFileMgr::File *file;
    EXPECT_TRUE(tmp_file_mgr.GetFile(devices[i], id, &file).ok());
    // Check the prefix is the expected temporary directory.
    EXPECT_EQ(0, file->path().find(tmp_dirs[i]));
  }
  FileSystemUtil::RemovePaths(tmp_dirs);
  CheckMetrics(&tmp_file_mgr);
}

/// Test that reporting a write error takes directory out of usage.
/// Disabled because blacklisting was disabled as workaround for IMPALA-2305.
TEST_F(TmpFileMgrTest, DISABLED_TestReportError) {
  vector<string> tmp_dirs;
  tmp_dirs.push_back("/tmp/tmp-file-mgr-test.1");
  tmp_dirs.push_back("/tmp/tmp-file-mgr-test.2");
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    EXPECT_TRUE(FileSystemUtil::CreateDirectory(tmp_dirs[i]).ok());
  }
  TmpFileMgr tmp_file_mgr;
  tmp_file_mgr.InitCustom(tmp_dirs, false, metrics_.get());

  // Both directories should be used.
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.active_tmp_devices();
  EXPECT_EQ(2, devices.size());
  CheckMetrics(&tmp_file_mgr);

  // Inject an error on one device so that we can validate it is handled correctly.
  TUniqueId id;
  int good_device = 0, bad_device = 1;
  TmpFileMgr::File* bad_file;
  EXPECT_TRUE(tmp_file_mgr.GetFile(devices[bad_device], id, &bad_file).ok());
  ErrorMsg errmsg(TErrorCode::GENERAL, "A fake error");
  bad_file->ReportIOError(errmsg);

  // Bad file should be blacklisted.
  EXPECT_TRUE(bad_file->is_blacklisted());
  // The second device should no longer be in use.
  EXPECT_EQ(1, tmp_file_mgr.num_active_tmp_devices());
  vector<TmpFileMgr::DeviceId> devices_after = tmp_file_mgr.active_tmp_devices();
  EXPECT_EQ(1, devices_after.size());
  EXPECT_EQ(devices[good_device], devices_after[0]);
  CheckMetrics(&tmp_file_mgr);

  // Attempts to expand bad file should fail.
  int64_t offset;
  EXPECT_FALSE(bad_file->AllocateSpace(128, &offset).ok());
  EXPECT_TRUE(bad_file->Remove().ok());
  // The good device should still be usable.
  TmpFileMgr::File* good_file;
  EXPECT_TRUE(tmp_file_mgr.GetFile(devices[good_device], id, &good_file).ok());
  EXPECT_TRUE(good_file != NULL);
  EXPECT_TRUE(good_file->AllocateSpace(128, &offset).ok());
  // Attempts to allocate new files on bad device should fail.
  EXPECT_FALSE(tmp_file_mgr.GetFile(devices[bad_device], id, &bad_file).ok());
  FileSystemUtil::RemovePaths(tmp_dirs);
  CheckMetrics(&tmp_file_mgr);
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  return RUN_ALL_TESTS();
}
