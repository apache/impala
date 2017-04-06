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

#include <cstdio>
#include <cstdlib>

#include <boost/filesystem.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/locks.hpp>
#include <gtest/gtest.h>

#include "common/init.h"
#include "runtime/test-env.h"
#include "runtime/tmp-file-mgr-internal.h"
#include "runtime/tmp-file-mgr.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "util/condition-variable.h"
#include "util/filesystem-util.h"
#include "util/metrics.h"

#include "gen-cpp/Types_types.h"  // for TUniqueId

#include "common/names.h"

using boost::filesystem::path;

DECLARE_bool(disk_spill_encryption);
#ifndef NDEBUG
DECLARE_int32(stress_scratch_write_delay_ms);
#endif

namespace impala {

class TmpFileMgrTest : public ::testing::Test {
 public:
  virtual void SetUp() {
    metrics_.reset(new MetricGroup("tmp-file-mgr-test"));
    profile_ = obj_pool_.Add(new RuntimeProfile(&obj_pool_, "tmp-file-mgr-test"));
    test_env_.reset(new TestEnv);
    ASSERT_OK(test_env_->Init());
    cb_counter_ = 0;

    // Reset query options that are modified by tests.
    FLAGS_disk_spill_encryption = false;
#ifndef NDEBUG
    FLAGS_stress_scratch_write_delay_ms = 0;
#endif
  }

  virtual void TearDown() {
    test_env_.reset();
    metrics_.reset();
    obj_pool_.Clear();
  }

  DiskIoMgr* io_mgr() { return test_env_->exec_env()->disk_io_mgr(); }

  /// Check that metric values are consistent with TmpFileMgr state.
  void CheckMetrics(TmpFileMgr* tmp_file_mgr) {
    vector<TmpFileMgr::DeviceId> active = tmp_file_mgr->ActiveTmpDevices();
    IntGauge* active_metric =
        metrics_->FindMetricForTesting<IntGauge>("tmp-file-mgr.active-scratch-dirs");
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

  /// Helper to call the private CreateFiles() method and return
  /// the created files.
  static Status CreateFiles(
      TmpFileMgr::FileGroup* group, vector<TmpFileMgr::File*>* files) {
    // The method expects the lock to be held.
    lock_guard<SpinLock> lock(group->lock_);
    RETURN_IF_ERROR(group->CreateFiles());
    for (unique_ptr<TmpFileMgr::File>& file : group->tmp_files_) {
      files->push_back(file.get());
    }
    return Status::OK();
  }

  /// Helper to call the private TmpFileMgr::NewFile() method.
  static Status NewFile(TmpFileMgr* mgr, TmpFileMgr::FileGroup* group,
      TmpFileMgr::DeviceId device_id, unique_ptr<TmpFileMgr::File>* new_file) {
    return mgr->NewFile(group, device_id, new_file);
  }

  /// Helper to call the private File::AllocateSpace() method.
  static Status FileAllocateSpace(
      TmpFileMgr::File* file, int64_t num_bytes, int64_t* offset) {
    return file->AllocateSpace(num_bytes, offset);
  }

  /// Helper to call the private FileGroup::AllocateSpace() method.
  static Status GroupAllocateSpace(TmpFileMgr::FileGroup* group, int64_t num_bytes,
      TmpFileMgr::File** file, int64_t* offset) {
    return group->AllocateSpace(num_bytes, file, offset);
  }

  /// Helper to set FileGroup::next_allocation_index_.
  static void SetNextAllocationIndex(TmpFileMgr::FileGroup* group, int value) {
    group->next_allocation_index_ = value;
  }

  /// Helper to cancel the FileGroup DiskIoRequestContext.
  static void CancelIoContext(TmpFileMgr::FileGroup* group) {
    group->io_mgr_->CancelContext(group->io_ctx_);
  }

  /// Helper to get the # of bytes allocated by the group. Validates that the sum across
  /// all files equals this total.
  static int64_t BytesAllocated(TmpFileMgr::FileGroup* group) {
    int64_t bytes_allocated = 0;
    for (unique_ptr<TmpFileMgr::File>& file : group->tmp_files_) {
      bytes_allocated += file->bytes_allocated_;
    }
    EXPECT_EQ(bytes_allocated, group->current_bytes_allocated_);
    return bytes_allocated;
  }

  // Write callback, which signals 'cb_cv_' and increments 'cb_counter_'.
  void SignalCallback(Status write_status) {
    {
      lock_guard<mutex> lock(cb_cv_lock_);
      ++cb_counter_;
    }
    cb_cv_.NotifyAll();
  }

  /// Wait until 'cb_counter_' reaches 'val'.
  void WaitForCallbacks(int64_t val) {
    unique_lock<mutex> lock(cb_cv_lock_);
    while (cb_counter_ < val) cb_cv_.Wait(lock);
  }

  ObjectPool obj_pool_;
  scoped_ptr<MetricGroup> metrics_;
  // Owned by 'obj_pool_'.
  RuntimeProfile* profile_;

  /// Used for DiskIoMgr.
  scoped_ptr<TestEnv> test_env_;

  // Variables used by SignalCallback().
  mutex cb_cv_lock_;
  ConditionVariable cb_cv_;
  int64_t cb_counter_;
};

/// Regression test for IMPALA-2160. Verify that temporary file manager allocates blocks
/// at the expected file offsets.
TEST_F(TmpFileMgrTest, TestFileAllocation) {
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.Init(metrics_.get()));
  TUniqueId id;
  TmpFileMgr::FileGroup file_group(
      &tmp_file_mgr, io_mgr(), profile_, id, 1024 * 1024 * 8);

  // Default configuration should give us one temporary device.
  EXPECT_EQ(1, tmp_file_mgr.NumActiveTmpDevices());
  vector<TmpFileMgr::DeviceId> tmp_devices = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(1, tmp_devices.size());
  vector<TmpFileMgr::File*> files;
  ASSERT_OK(CreateFiles(&file_group, &files));
  EXPECT_EQ(1, files.size());
  TmpFileMgr::File* file = files[0];
  // Apply writes of variable sizes and check space was allocated correctly.
  int64_t write_sizes[] = {1, 10, 1024, 4, 1024 * 1024 * 8, 1024 * 1024 * 8, 16, 10};
  int num_write_sizes = sizeof(write_sizes) / sizeof(write_sizes[0]);
  int64_t next_offset = 0;
  for (int i = 0; i < num_write_sizes; ++i) {
    int64_t offset;
    ASSERT_OK(FileAllocateSpace(file, write_sizes[i], &offset));
    EXPECT_EQ(next_offset, offset);
    next_offset = offset + write_sizes[i];
  }
  // Check that cleanup is correct.
  string file_path = file->path();
  EXPECT_FALSE(boost::filesystem::exists(file_path));

  // Check that the file is cleaned up correctly. Need to create file first since
  // tmp file is only allocated on writes.
  EXPECT_OK(FileSystemUtil::CreateFile(file->path()));
  file_group.Close();
  EXPECT_FALSE(boost::filesystem::exists(file->path()));
  CheckMetrics(&tmp_file_mgr);
}

/// Test that we can do initialization with two directories on same device and
/// that validations prevents duplication of directories.
TEST_F(TmpFileMgrTest, TestOneDirPerDevice) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, true, metrics_.get()));
  TUniqueId id;
  TmpFileMgr::FileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);

  // Only the first directory should be used.
  EXPECT_EQ(1, tmp_file_mgr.NumActiveTmpDevices());
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(1, devices.size());
  vector<TmpFileMgr::File*> files;
  ASSERT_OK(CreateFiles(&file_group, &files));
  EXPECT_EQ(1, files.size());
  TmpFileMgr::File* file = files[0];
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
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, metrics_.get()));
  TUniqueId id;
  TmpFileMgr::FileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);

  // Both directories should be used.
  EXPECT_EQ(2, tmp_file_mgr.NumActiveTmpDevices());
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(2, devices.size());

  vector<TmpFileMgr::File*> files;
  ASSERT_OK(CreateFiles(&file_group, &files));
  EXPECT_EQ(2, files.size());
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(0, tmp_file_mgr.GetTmpDirPath(devices[i]).find(tmp_dirs[i]));
    // Check the prefix is the expected temporary directory.
    EXPECT_EQ(0, files[i]->path().find(tmp_dirs[i]));
  }
  FileSystemUtil::RemovePaths(tmp_dirs);
  file_group.Close();
  CheckMetrics(&tmp_file_mgr);
}

/// Test that reporting a write error is possible but does not result in
/// blacklisting the device.
TEST_F(TmpFileMgrTest, TestReportError) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, metrics_.get()));
  TUniqueId id;
  TmpFileMgr::FileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);

  // Both directories should be used.
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(2, devices.size());
  CheckMetrics(&tmp_file_mgr);

  // Inject an error on one device so that we can validate it is handled correctly.
  int good_device = 0, bad_device = 1;
  vector<TmpFileMgr::File*> files;
  ASSERT_OK(CreateFiles(&file_group, &files));
  ASSERT_EQ(2, files.size());
  TmpFileMgr::File* good_file = files[good_device];
  TmpFileMgr::File* bad_file = files[bad_device];
  ErrorMsg errmsg(TErrorCode::GENERAL, "A fake error");
  bad_file->Blacklist(errmsg);

  // File-level blacklisting is enabled but not device-level.
  EXPECT_TRUE(bad_file->is_blacklisted());
  // The bad device should still be active.
  EXPECT_EQ(2, tmp_file_mgr.NumActiveTmpDevices());
  vector<TmpFileMgr::DeviceId> devices_after = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(2, devices_after.size());
  CheckMetrics(&tmp_file_mgr);

  // Attempts to expand bad file should succeed.
  int64_t offset;
  ASSERT_OK(FileAllocateSpace(bad_file, 128, &offset));
  // The good device should still be usable.
  ASSERT_OK(FileAllocateSpace(good_file, 128, &offset));
  // Attempts to allocate new files on bad device should succeed.
  unique_ptr<TmpFileMgr::File> bad_file2;
  ASSERT_OK(NewFile(&tmp_file_mgr, &file_group, bad_device, &bad_file2));
  FileSystemUtil::RemovePaths(tmp_dirs);
  file_group.Close();
  CheckMetrics(&tmp_file_mgr);
}

TEST_F(TmpFileMgrTest, TestAllocateNonWritable) {
  vector<string> tmp_dirs;
  vector<string> scratch_subdirs;
  for (int i = 0; i < 2; ++i) {
    tmp_dirs.push_back(Substitute("/tmp/tmp-file-mgr-test.$0", i));
    scratch_subdirs.push_back(tmp_dirs[i] + "/impala-scratch");
  }
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, metrics_.get()));
  TUniqueId id;
  TmpFileMgr::FileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);

  vector<TmpFileMgr::File*> allocated_files;
  ASSERT_OK(CreateFiles(&file_group, &allocated_files));
  int64_t offset;
  ASSERT_OK(FileAllocateSpace(allocated_files[0], 1, &offset));

  // Make scratch non-writable and test allocation at different stages:
  // new file creation, files with no allocated blocks. files with allocated space.
  // No errors should be encountered during allocation since allocation is purely logical.
  chmod(scratch_subdirs[0].c_str(), 0);
  ASSERT_OK(FileAllocateSpace(allocated_files[0], 1, &offset));
  ASSERT_OK(FileAllocateSpace(allocated_files[1], 1, &offset));

  chmod(scratch_subdirs[0].c_str(), S_IRWXU);
  FileSystemUtil::RemovePaths(tmp_dirs);
  file_group.Close();
}

// Test scratch limit is applied correctly to group of files.
TEST_F(TmpFileMgrTest, TestScratchLimit) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, metrics_.get()));

  const int64_t LIMIT = 128;
  // A power-of-two so that FileGroup allocates exactly this amount of scratch space.
  const int64_t ALLOC_SIZE = 64;
  TUniqueId id;
  TmpFileMgr::FileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id, LIMIT);

  vector<TmpFileMgr::File*> files;
  ASSERT_OK(CreateFiles(&file_group, &files));

  // Test individual limit is enforced.
  Status status;
  int64_t offset;
  TmpFileMgr::File* alloc_file;

  // Alloc from file 1 should succeed.
  SetNextAllocationIndex(&file_group, 0);
  ASSERT_OK(GroupAllocateSpace(&file_group, ALLOC_SIZE, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files[0]); // Should select files round-robin.
  ASSERT_EQ(0, offset);

  // Allocate up to the max.
  ASSERT_OK(GroupAllocateSpace(&file_group, ALLOC_SIZE, &alloc_file, &offset));
  ASSERT_EQ(0, offset);
  ASSERT_EQ(alloc_file, files[1]);

  // Test aggregate limit is enforced.
  status = GroupAllocateSpace(&file_group, 1, &alloc_file, &offset);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.code(), TErrorCode::SCRATCH_LIMIT_EXCEEDED);

  file_group.Close();
}

// Test that scratch file ranges of varying length are recycled as expected.
TEST_F(TmpFileMgrTest, TestScratchRangeRecycling) {
  TUniqueId id;
  TmpFileMgr::FileGroup file_group(test_env_->tmp_file_mgr(), io_mgr(), profile_, id);
  int64_t expected_scratch_bytes_allocated = 0;
  // Test some different allocation sizes.
  for (int alloc_size = 64; alloc_size <= 64 * 1024; alloc_size *= 2) {
    // Generate some data.
    const int BLOCKS = 5;
    vector<vector<uint8_t>> data(BLOCKS);
    for (int i = 0; i < BLOCKS; ++i) {
      data[i].resize(alloc_size);
      std::iota(data[i].begin(), data[i].end(), i);
    }

    DiskIoMgr::WriteRange::WriteDoneCallback callback =
        bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
    vector<unique_ptr<TmpFileMgr::WriteHandle>> handles(BLOCKS);
    // 'file_group' should allocate extra scratch bytes for this 'alloc_size'.
    expected_scratch_bytes_allocated += alloc_size * BLOCKS;
    const int TEST_ITERS = 5;
    // Make sure free space doesn't grow over several iterations.
    for (int i = 0; i < TEST_ITERS; ++i) {
      cb_counter_ = 0;
      for (int j = 0; j < BLOCKS; ++j) {
        ASSERT_OK(file_group.Write(
            MemRange(data[j].data(), alloc_size), callback, &handles[j]));
      }
      WaitForCallbacks(BLOCKS);
      EXPECT_EQ(expected_scratch_bytes_allocated, BytesAllocated(&file_group));

      // Read back and validate.
      for (int j = 0; j < BLOCKS; ++j) {
        vector<uint8_t> tmp(alloc_size);
        ASSERT_OK(file_group.Read(handles[j].get(), MemRange(tmp.data(), alloc_size)));
        EXPECT_EQ(0, memcmp(tmp.data(), data[j].data(), alloc_size));
        file_group.DestroyWriteHandle(move(handles[j]));
      }
      // Check that the space is still in use - it should be recycled by the next
      // iteration.
      EXPECT_EQ(expected_scratch_bytes_allocated, BytesAllocated(&file_group));
    }
  }
  file_group.Close();
  test_env_->TearDownQueries();
}

// Regression test for IMPALA-4748, where hitting the process memory limit caused
// internal invariants of TmpFileMgr to be broken on error path.
TEST_F(TmpFileMgrTest, TestProcessMemLimitExceeded) {
  TUniqueId id;
  TmpFileMgr::FileGroup file_group(test_env_->tmp_file_mgr(), io_mgr(), profile_, id);

  const int DATA_SIZE = 64;
  vector<uint8_t> data(DATA_SIZE);

  // Fake the asynchronous error from the process mem limit by cancelling the io context.
  CancelIoContext(&file_group);

  // After this error, writing via the file group should fail.
  DiskIoMgr::WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
  unique_ptr<TmpFileMgr::WriteHandle> handle;
  Status status = file_group.Write(MemRange(data.data(), DATA_SIZE), callback, &handle);
  EXPECT_EQ(TErrorCode::CANCELLED, status.code());
  file_group.Close();
  test_env_->TearDownQueries();
}

// Regression test for IMPALA-4820 - encrypted data can get written to disk.
TEST_F(TmpFileMgrTest, TestEncryptionDuringCancellation) {
  FLAGS_disk_spill_encryption = true;
  // A delay is required for this to reproduce the issue, since writes are often buffered
  // in memory by the OS. The test should succeed regardless of the delay.
#ifndef NDEBUG
  FLAGS_stress_scratch_write_delay_ms = 1000;
#endif
  TUniqueId id;
  TmpFileMgr::FileGroup file_group(test_env_->tmp_file_mgr(), io_mgr(), profile_, id);

  // Make the data fairly large so that we have a better chance of cancelling while the
  // write is in flight
  const int DATA_SIZE = 8 * 1024 * 1024;
  string data(DATA_SIZE, ' ');
  MemRange data_mem_range(reinterpret_cast<uint8_t*>(&data[0]), DATA_SIZE);

  // Write out a string repeatedly. We don't want to see this written unencypted to disk.
  string plaintext("the quick brown fox jumped over the lazy dog");
  for (int pos = 0; pos + plaintext.size() < DATA_SIZE; pos += plaintext.size()) {
    memcpy(&data[pos], &plaintext[0], plaintext.size());
  }

  // Start a write in flight, which should encrypt the data and write it to disk.
  unique_ptr<TmpFileMgr::WriteHandle> handle;
  DiskIoMgr::WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
  ASSERT_OK(file_group.Write(data_mem_range, callback, &handle));
  string file_path = handle->TmpFilePath();

  // Cancel the write - prior to the IMPALA-4820 fix decryption could race with the write.
  handle->Cancel();
  handle->WaitForWrite();
  ASSERT_OK(file_group.RestoreData(move(handle), data_mem_range));
  WaitForCallbacks(1);

  // Read the data from the scratch file and check that the plaintext isn't present.
  FILE* file = fopen(file_path.c_str(), "r");
  ASSERT_EQ(DATA_SIZE, fread(&data[0], 1, DATA_SIZE, file));
  for (int pos = 0; pos + plaintext.size() < DATA_SIZE; pos += plaintext.size()) {
    ASSERT_NE(0, memcmp(&data[pos], &plaintext[0], plaintext.size()))
        << file_path << "@" << pos;
  }
  fclose(file);
  file_group.Close();
  test_env_->TearDownQueries();
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  return RUN_ALL_TESTS();
}
