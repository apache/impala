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
#include <limits>
#include <mutex>
#include <numeric>

#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <gtest/gtest.h>

#include "common/init.h"
#include "runtime/io/request-context.h"
#include "runtime/test-env.h"
#include "runtime/tmp-file-mgr-internal.h"
#include "runtime/tmp-file-mgr.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "util/bit-util.h"
#include "util/collection-metrics.h"
#include "util/condition-variable.h"
#include "util/cpu-info.h"
#include "util/filesystem-util.h"
#include "util/metrics.h"

#include "gen-cpp/Types_types.h"  // for TUniqueId

#include "common/names.h"

using boost::filesystem::path;
using boost::uuids::random_generator;

DECLARE_bool(disk_spill_encryption);
DECLARE_int64(disk_spill_compression_buffer_limit_bytes);
DECLARE_string(disk_spill_compression_codec);
DECLARE_bool(disk_spill_punch_holes);
#ifndef NDEBUG
DECLARE_int32(stress_scratch_write_delay_ms);
#endif
DECLARE_string(remote_tmp_file_size);
DECLARE_int32(wait_for_spill_buffer_timeout_s);

namespace impala {

using namespace io;

static const int64_t KILOBYTE = 1024L;
static const int64_t MEGABYTE = 1024L * KILOBYTE;
static const int64_t GIGABYTE = 1024L * MEGABYTE;
static const int64_t TERABYTE = 1024L * GIGABYTE;

/// For testing spill to remote.
static const string HDFS_LOCAL_URL = "hdfs://localhost:20500/tmp";
static const string REMOTE_URL = HDFS_LOCAL_URL;
static const string LOCAL_BUFFER_PATH = "/tmp/tmp-file-mgr-test-buffer";

class TmpFileMgrTest : public ::testing::Test {
 public:
  static const int DEFAULT_PRIORITY = numeric_limits<int>::max();
  virtual void SetUp() {
    // Reset query options that are modified by tests.
    FLAGS_disk_spill_encryption = false;
    FLAGS_disk_spill_compression_codec = "";
    FLAGS_disk_spill_punch_holes = false;
#ifndef NDEBUG
    FLAGS_stress_scratch_write_delay_ms = 0;
#endif
    FLAGS_remote_tmp_file_size = "8MB";

    metrics_.reset(new MetricGroup("tmp-file-mgr-test"));
    profile_ = RuntimeProfile::Create(&obj_pool_, "tmp-file-mgr-test");
    test_env_.reset(new TestEnv);
    ASSERT_OK(test_env_->Init());
    cb_counter_ = 0;
  }

  virtual void TearDown() {
    test_env_.reset();
    metrics_.reset();
    obj_pool_.Clear();
  }

  DiskIoMgr* io_mgr() { return test_env_->exec_env()->disk_io_mgr(); }

  /// Helper to create a TmpFileMgr and initialise it with InitCustom(). Adds the mgr to
  /// 'obj_pool_' for automatic cleanup at the end of each test. Fails the test if
  /// InitCustom() fails.
  TmpFileMgr* CreateTmpFileMgr(const string& tmp_dirs_spec, bool expect_ok = true,
      HdfsFsCache::HdfsFsMap* hdfs_conn_map = nullptr) {
    // Allocate a new metrics group for each TmpFileMgr so they don't get confused by
    // the pre-existing metrics (TmpFileMgr assumes it's a singleton in product code).
    MetricGroup* metrics = obj_pool_.Add(new MetricGroup(""));
    TmpFileMgr* mgr = obj_pool_.Add(new TmpFileMgr());
    if (hdfs_conn_map != nullptr) mgr->hdfs_conns_ = *hdfs_conn_map;
    Status status = mgr->InitCustom(tmp_dirs_spec, false, "", false, metrics);
    if (expect_ok) {
      EXPECT_TRUE(status.ok());
    } else {
      EXPECT_FALSE(status.ok());
    }
    return mgr;
  }

  /// Check that metric values are consistent with TmpFileMgr state.
  void CheckMetrics(TmpFileMgr* tmp_file_mgr) {
    vector<TmpFileMgr::DeviceId> active = tmp_file_mgr->ActiveTmpDevices();
    IntGauge* active_metric =
        metrics_->FindMetricForTesting<IntGauge>("tmp-file-mgr.active-scratch-dirs");
    EXPECT_EQ(active.size(), active_metric->GetValue());
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

  /// Check that current scratch space bytes and HWM match the expected values.
  void checkHWMMetrics(int64_t exp_current_value, int64_t exp_hwm_value) {
    AtomicHighWaterMarkGauge* hwm_value =
        metrics_->FindMetricForTesting<AtomicHighWaterMarkGauge>(
            "tmp-file-mgr.scratch-space-bytes-used-high-water-mark");
    IntGauge* current_value = hwm_value->current_value_;
    ASSERT_EQ(current_value->GetValue(), exp_current_value);
    ASSERT_EQ(hwm_value->GetValue(), exp_hwm_value);
  }

  void RemoveAndCreateDirs(const vector<string>& dirs) {
    for (const string& dir: dirs) {
      ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(dir));
    }
  }

  void RemoveDirs(const vector<string>& dirs) {
    ASSERT_OK(FileSystemUtil::RemovePaths(dirs));
  }

  /// Helper to call the private CreateFiles() method and return
  /// the created files.
  static Status CreateFiles(
      TmpFileGroup* group, vector<TmpFile*>* files) {
    // The method expects the lock to be held.
    lock_guard<SpinLock> lock(group->lock_);
    RETURN_IF_ERROR(group->CreateFiles());
    for (unique_ptr<TmpFile>& file : group->tmp_files_) {
      files->push_back(file.get());
    }
    return Status::OK();
  }

  /// Helper to get the private tmp_dirs_ member.
  static const vector<unique_ptr<TmpDir>>& GetTmpDirs(TmpFileMgr* mgr) {
    return mgr->tmp_dirs_;
  }

  /// Helper to get the private tmp_remote_dirs_ pointer.
  static const unique_ptr<TmpDir>& GetTmpRemoteDir(TmpFileMgr* mgr) {
    return mgr->tmp_dirs_remote_;
  }

  /// Helper to call the private TmpFileMgr::NewFile() method.
  static void NewFile(TmpFileMgr* mgr, TmpFileGroup* group,
      TmpFileMgr::DeviceId device_id, unique_ptr<TmpFile>* new_file) {
    mgr->NewFile(group, device_id, new_file);
  }

  /// Helper to call the private File::AllocateSpace() method.
  static void FileAllocateSpace(
      TmpFile* file, int64_t num_bytes, int64_t* offset) {
    file->AllocateSpace(num_bytes, offset);
  }

  /// Helper to call the private FileGroup::AllocateSpace() method.
  static Status GroupAllocateSpace(TmpFileGroup* group, int64_t num_bytes,
      TmpFile** file, int64_t* offset) {
    return group->AllocateSpace(num_bytes, file, offset);
  }

  /// Helper to set FileGroup::next_allocation_index_.
  static void SetNextAllocationIndex(TmpFileGroup* group, int priority, int value) {
    group->next_allocation_index_[priority] = value;
  }

  /// Helper to cancel the FileGroup RequestContext.
  static void CancelIoContext(TmpFileGroup* group) {
    group->io_ctx_->Cancel();
  }

  /// Helper to set an invalid remote path to create an error.
  static void SetInvalidRemotePath(
      TmpFileMgr* tmp_file_mgr, TmpFileGroup* group, int tmp_file_idx) {
    const string& dir = tmp_file_mgr->tmp_dirs_remote_->path();
    int dev_id = 0;
    string invalid_path = "";
    auto tmp_file_shared_ptr = group->tmp_files_remote_[tmp_file_idx];
    auto org_tmp_file = static_cast<TmpFileRemote*>(tmp_file_shared_ptr.get());
    auto local_path = org_tmp_file->local_buffer_path_;
    TmpFileRemote* tmp_file_r =
        new TmpFileRemote(group, dev_id, invalid_path, local_path, false, dir.c_str());
    tmp_file_r->at_capacity_ = org_tmp_file->at_capacity_;
    tmp_file_r->enqueued_ = org_tmp_file->enqueued_;
    tmp_file_r->local_buffer_path_ = org_tmp_file->local_buffer_path_;
    tmp_file_r->file_size_ = org_tmp_file->file_size_;
    tmp_file_r->allocation_offset_ = org_tmp_file->allocation_offset_;
    tmp_file_r->hdfs_conn_ = org_tmp_file->hdfs_conn_;
    tmp_file_r->disk_file_->hdfs_conn_ = org_tmp_file->disk_file_->hdfs_conn_;
    tmp_file_r->disk_buffer_file_.swap(org_tmp_file->disk_buffer_file_);
    // Set buffer returned to avoid DCHECK failure while destruction.
    org_tmp_file->buffer_returned_ = true;
    shared_ptr<TmpFile> tmp_file_remote(move(tmp_file_r));
    group->tmp_files_remote_[tmp_file_idx] = tmp_file_remote;
    group->tmp_files_remote_ptrs_.emplace(tmp_file_remote.get(), tmp_file_remote);
  }

  static void SetWriteRangeContext(WriteRange* write_range, TmpFileGroup* group) {
    write_range->SetRequestContext(group->io_ctx_.get());
  }

  /// Helper to get the # of bytes allocated by the group. Validates that the sum across
  /// all files equals this total.
  static int64_t BytesAllocated(TmpFileGroup* group) {
    int64_t bytes_allocated = 0;
    for (unique_ptr<TmpFile>& file : group->tmp_files_) {
      int64_t allocated = file->allocation_offset_;
      int64_t reclaimed = file->bytes_reclaimed_.Load();
      EXPECT_GE(allocated, reclaimed);
      bytes_allocated += allocated - reclaimed;
    }
    EXPECT_EQ(bytes_allocated, group->current_bytes_allocated_.Load());
    return bytes_allocated;
  }

  /// Helper to validate the [start, end] index range for a given priority for a given
  /// file group.
  static void ValidatePriorityIndexRange(TmpFileGroup* group, int priority, int start,
    int end) {
    auto search = group->tmp_files_index_range_.find(priority);
    EXPECT_TRUE(search != group->tmp_files_index_range_.end());
    EXPECT_EQ(start, search->second.start);
    EXPECT_EQ(end, search->second.end);
  }

  /// Helpers to call WriteHandle methods.
  void Cancel(TmpWriteHandle* handle) { handle->Cancel(); }
  void WaitForWrite(TmpWriteHandle* handle) {
    handle->WaitForWrite();
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

  /// Implementation of TestScratchLimit.
  void TestScratchLimit(bool punch_holes, int64_t alloc_size);

  /// Implementation of TestScratchRangeRecycling.
  void TestScratchRangeRecycling(bool punch_holes);

  /// Implementation of TestDirectoryLimits.
  void TestDirectoryLimits(bool punch_holes);

  /// Implementation of TestBlockVerification(), which is run with different environments.
  void TestBlockVerification();

  /// Implementation of TestCompressBufferManagment
  void TestCompressBufferManagement(bool spill_to_remote, bool wait_upload);

  /// Helper function to do test on TmpFileBufferPool.
  void TestTmpFileBufferPoolHelper(TmpFileMgr& tmp_file_mgr,
      unique_ptr<TmpFileGroup>* tmp_file_group,
      boost::scoped_ptr<io::WriteRange>& write_range,
      WriteRange::WriteDoneCallback& callback);

  /// Check the TmpFileBufferPool after the test case tears down.
  void TestTmpFileBufferPoolTearDown(TmpFileMgr& tmp_file_mgr);

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
  TmpFileGroup file_group(
      &tmp_file_mgr, io_mgr(), profile_, id, 1024 * 1024 * 8);

  // Default configuration should give us one temporary device.
  EXPECT_EQ(1, tmp_file_mgr.NumActiveTmpDevices());
  vector<TmpFileMgr::DeviceId> tmp_devices = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(1, tmp_devices.size());
  vector<TmpFile*> files;
  ASSERT_OK(CreateFiles(&file_group, &files));
  EXPECT_EQ(1, files.size());
  TmpFile* file = files[0];
  // Apply writes of variable sizes and check space was allocated correctly.
  int64_t write_sizes[] = {1, 10, 1024, 4, 1024 * 1024 * 8, 1024 * 1024 * 8, 16, 10};
  int num_write_sizes = sizeof(write_sizes) / sizeof(write_sizes[0]);
  int64_t next_offset = 0;
  for (int i = 0; i < num_write_sizes; ++i) {
    int64_t offset;
    FileAllocateSpace(file, write_sizes[i], &offset);
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
  EXPECT_FALSE(boost::filesystem::exists(file_path));
  CheckMetrics(&tmp_file_mgr);
}

/// Test that we can do initialization with two directories on same device and
/// that validations prevents duplication of directories.
TEST_F(TmpFileMgrTest, TestOneDirPerDevice) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, true, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);

  // Only the first directory should be used.
  EXPECT_EQ(1, tmp_file_mgr.NumActiveTmpDevices());
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(1, devices.size());
  vector<TmpFile*> files;
  ASSERT_OK(CreateFiles(&file_group, &files));
  EXPECT_EQ(1, files.size());
  TmpFile* file = files[0];
  // Check the prefix is the expected temporary directory.
  EXPECT_EQ(0, file->path().find(tmp_dirs[0]));
  ASSERT_OK(FileSystemUtil::RemovePaths(tmp_dirs));
  file_group.Close();
  CheckMetrics(&tmp_file_mgr);
}

/// Test that we can do custom initialization with two dirs on same device.
TEST_F(TmpFileMgrTest, TestMultiDirsPerDevice) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);

  // Both directories should be used.
  EXPECT_EQ(2, tmp_file_mgr.NumActiveTmpDevices());
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(2, devices.size());

  vector<TmpFile*> files;
  ASSERT_OK(CreateFiles(&file_group, &files));
  EXPECT_EQ(2, files.size());
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(0, tmp_file_mgr.GetTmpDirPath(devices[i]).find(tmp_dirs[i]));
    // Check the prefix is the expected temporary directory.
    EXPECT_EQ(0, files[i]->path().find(tmp_dirs[i]));
  }
  ASSERT_OK(FileSystemUtil::RemovePaths(tmp_dirs));
  file_group.Close();
  CheckMetrics(&tmp_file_mgr);
}

/// Test that reporting a write error is possible but does not result in
/// blacklisting the device.
TEST_F(TmpFileMgrTest, TestReportError) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);

  // Both directories should be used.
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(2, devices.size());
  CheckMetrics(&tmp_file_mgr);

  // Inject an error on one device so that we can validate it is handled correctly.
  int good_device = 0, bad_device = 1;
  vector<TmpFile*> files;
  ASSERT_OK(CreateFiles(&file_group, &files));
  ASSERT_EQ(2, files.size());
  TmpFile* good_file = files[good_device];
  TmpFile* bad_file = files[bad_device];
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
  FileAllocateSpace(bad_file, 128, &offset);
  // The good device should still be usable.
  FileAllocateSpace(good_file, 128, &offset);
  // Attempts to allocate new files on bad device should succeed.
  unique_ptr<TmpFile> bad_file2;
  NewFile(&tmp_file_mgr, &file_group, bad_device, &bad_file2);
  ASSERT_OK(FileSystemUtil::RemovePaths(tmp_dirs));
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
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);

  vector<TmpFile*> allocated_files;
  ASSERT_OK(CreateFiles(&file_group, &allocated_files));
  int64_t offset;
  FileAllocateSpace(allocated_files[0], 1, &offset);

  // Make scratch non-writable and test allocation at different stages:
  // new file creation, files with no allocated blocks. files with allocated space.
  // No errors should be encountered during allocation since allocation is purely logical.
  chmod(scratch_subdirs[0].c_str(), 0);
  FileAllocateSpace(allocated_files[0], 1, &offset);
  FileAllocateSpace(allocated_files[1], 1, &offset);

  chmod(scratch_subdirs[0].c_str(), S_IRWXU);
  ASSERT_OK(FileSystemUtil::RemovePaths(tmp_dirs));
  file_group.Close();
}

// Test scratch limit is applied correctly to group of files.
TEST_F(TmpFileMgrTest, TestScratchLimit) {
  TestScratchLimit(false, 128);
}

// Test that the scratch limit logic behaves identically with hole punching.
// The test doesn't recycle ranges so this shouldn't affect behaviour.
TEST_F(TmpFileMgrTest, TestScratchLimitPunchHoles) {
  // With hole punching, allocations are rounded up to the nearest 4kb block,
  // so we need the allocation size to be at least this large for the test.
  TestScratchLimit(true, TmpFileMgr::HOLE_PUNCH_BLOCK_SIZE_BYTES);
}

void TmpFileMgrTest::TestScratchLimit(bool punch_holes, int64_t alloc_size) {
  // Size must bea power-of-two so that FileGroup allocates exactly this amount of
  // scratch space.
  ASSERT_TRUE(BitUtil::IsPowerOf2(alloc_size));
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", punch_holes, metrics_.get()));

  const int64_t limit = alloc_size * 2;
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id, limit);

  vector<TmpFile*> files;
  ASSERT_OK(CreateFiles(&file_group, &files));

  // Test individual limit is enforced.
  Status status;
  int64_t offset;
  TmpFile* alloc_file;

  // Alloc from file 1 should succeed.
  SetNextAllocationIndex(&file_group, DEFAULT_PRIORITY, 0);
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files[0]); // Should select files round-robin.
  ASSERT_EQ(0, offset);

  // Allocate up to the max.
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &alloc_file, &offset));
  ASSERT_EQ(0, offset);
  ASSERT_EQ(alloc_file, files[1]);

  // Test aggregate limit is enforced.
  status = GroupAllocateSpace(&file_group, 1, &alloc_file, &offset);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.code(), TErrorCode::SCRATCH_LIMIT_EXCEEDED);
  ASSERT_NE(string::npos, status.msg().msg().find(GetBackendString()));

  // Check HWM metrics
  checkHWMMetrics(limit, limit);
  file_group.Close();
  checkHWMMetrics(0, limit);
}

// Test that scratch file ranges of varying length are recycled as expected.
TEST_F(TmpFileMgrTest, TestScratchRangeRecycling) {
  TestScratchRangeRecycling(false);
}

// Test that scratch file ranges are not counted as allocated when we punch holes.
TEST_F(TmpFileMgrTest, TestScratchRangeHolePunching) {
  TestScratchRangeRecycling(true);
}

void TmpFileMgrTest::TestScratchRangeRecycling(bool punch_holes) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", punch_holes, metrics_.get()));
  TUniqueId id;

  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);
  int64_t expected_scratch_bytes_allocated = 0;
  // The max value of expected_scratch_bytes_allocated throughout this test.
  int64_t max_scratch_bytes_allocated = 0;
  // Test some different allocation sizes.
  checkHWMMetrics(0, 0);
  // Hole punching only reclaims space in 4kb block sizes.
  int min_alloc_size = punch_holes ? TmpFileMgr::HOLE_PUNCH_BLOCK_SIZE_BYTES : 64;
  for (int alloc_size = min_alloc_size; alloc_size <= 64 * 1024; alloc_size *= 2) {
    // Generate some data.
    const int BLOCKS = 5;
    vector<vector<uint8_t>> data(BLOCKS);
    for (int i = 0; i < BLOCKS; ++i) {
      data[i].resize(alloc_size);
      std::iota(data[i].begin(), data[i].end(), i);
    }

    WriteRange::WriteDoneCallback callback =
        bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
    vector<unique_ptr<TmpWriteHandle>> handles(BLOCKS);
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
      checkHWMMetrics(expected_scratch_bytes_allocated, expected_scratch_bytes_allocated);

      // Read back and validate.
      for (int j = 0; j < BLOCKS; ++j) {
        vector<uint8_t> tmp(alloc_size);
        ASSERT_OK(file_group.Read(handles[j].get(), MemRange(tmp.data(), alloc_size)));
        EXPECT_EQ(0, memcmp(tmp.data(), data[j].data(), alloc_size));
        file_group.DestroyWriteHandle(move(handles[j]));
      }
      // With hole punching, the scratch space is not in use. Without hole punching it
      // is in used, but will be reused by the next iteration.
      int64_t expected_bytes_allocated =
          punch_holes ? 0 : expected_scratch_bytes_allocated;
      EXPECT_EQ(expected_bytes_allocated, BytesAllocated(&file_group));
      checkHWMMetrics(expected_bytes_allocated, expected_scratch_bytes_allocated);
    }
    /// No scratch should be in use for the next iteration if holes were punched.
    max_scratch_bytes_allocated =
        max(max_scratch_bytes_allocated, expected_scratch_bytes_allocated);
    if (punch_holes) expected_scratch_bytes_allocated = 0;
  }
  file_group.Close();
  checkHWMMetrics(0, max_scratch_bytes_allocated);
}

// Regression test for IMPALA-4748, where hitting the process memory limit caused
// internal invariants of TmpFileMgr to be broken on error path.
TEST_F(TmpFileMgrTest, TestProcessMemLimitExceeded) {
  TUniqueId id;
  TmpFileGroup file_group(test_env_->tmp_file_mgr(), io_mgr(), profile_, id);

  const int DATA_SIZE = 64;
  vector<uint8_t> data(DATA_SIZE);

  // Fake the asynchronous error from the process mem limit by cancelling the io context.
  CancelIoContext(&file_group);

  // After this error, writing via the file group should fail.
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
  unique_ptr<TmpWriteHandle> handle;
  Status status = file_group.Write(MemRange(data.data(), DATA_SIZE), callback, &handle);
  EXPECT_EQ(TErrorCode::CANCELLED_INTERNALLY, status.code());
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
  TmpFileGroup file_group(test_env_->tmp_file_mgr(), io_mgr(), profile_, id);

  // Make the data fairly large so that we have a better chance of cancelling while the
  // write is in flight
  const int DATA_SIZE = 8 * 1024 * 1024;
  string data(DATA_SIZE, ' ');
  MemRange data_mem_range(reinterpret_cast<uint8_t*>(&data[0]), DATA_SIZE);

  // Write out a string repeatedly. We don't want to see this written unencypted to disk.
  string plaintext("the quick brown fox jumped over the lazy dog");
  for (int pos = 0; pos + plaintext.size() < DATA_SIZE; pos += plaintext.size()) {
    memcpy(&data[pos], plaintext.data(), plaintext.size());
  }

  // Start a write in flight, which should encrypt the data and write it to disk.
  unique_ptr<TmpWriteHandle> handle;
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
  ASSERT_OK(file_group.Write(data_mem_range, callback, &handle));
  string file_path = handle->TmpFilePath();

  // Cancel the write - prior to the IMPALA-4820 fix decryption could race with the write.
  Cancel(handle.get());
  WaitForWrite(handle.get());
  ASSERT_OK(file_group.RestoreData(move(handle), data_mem_range));
  WaitForCallbacks(1);

  // Read the data from the scratch file and check that the plaintext isn't present.
  FILE* file = fopen(file_path.c_str(), "r");
  ASSERT_EQ(DATA_SIZE, fread(&data[0], 1, DATA_SIZE, file));
  for (int pos = 0; pos + plaintext.size() < DATA_SIZE; pos += plaintext.size()) {
    ASSERT_NE(0, memcmp(&data[pos], plaintext.data(), plaintext.size()))
        << file_path << "@" << pos;
  }
  fclose(file);
  file_group.Close();
  test_env_->TearDownQueries();
}

TEST_F(TmpFileMgrTest, TestBlockVerification) {
  TestBlockVerification();
}

TEST_F(TmpFileMgrTest, TestBlockVerificationGcmDisabled) {
  // Disable AES-GCM to test that errors from non-AES-GCM verification are also correct.
  CpuInfo::TempDisable t1(CpuInfo::PCLMULQDQ);
  TestBlockVerification();
}

TEST_F(TmpFileMgrTest, TestBlockVerificationCompression) {
  FLAGS_disk_spill_compression_codec = "zstd";
  TestBlockVerification();
}

void TmpFileMgrTest::TestBlockVerification() {
  FLAGS_disk_spill_encryption = true;
  TUniqueId id;
  TmpFileGroup file_group(test_env_->tmp_file_mgr(), io_mgr(), profile_, id);
  string data = "the quick brown fox jumped over the lazy dog";
  MemRange data_mem_range(reinterpret_cast<uint8_t*>(&data[0]), data.size());

  // Start a write in flight, which should encrypt the data and write it to disk.
  unique_ptr<TmpWriteHandle> handle;
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
  ASSERT_OK(file_group.Write(data_mem_range, callback, &handle));
  string file_path = handle->TmpFilePath();

  WaitForWrite(handle.get());
  WaitForCallbacks(1);

  // Modify the data in the scratch file and check that a read error occurs.
  LOG(INFO) << "Corrupting " << file_path;
  uint8_t corrupt_byte = data[0] ^ 1;
  FILE* file = fopen(file_path.c_str(), "rb+");
  ASSERT_TRUE(file != nullptr);
  ASSERT_EQ(corrupt_byte, fputc(corrupt_byte, file));
  ASSERT_EQ(0, fclose(file));
  vector<uint8_t> tmp;
  tmp.resize(data.size());
  Status read_status = file_group.Read(handle.get(), MemRange(tmp.data(), tmp.size()));
  LOG(INFO) << read_status.GetDetail();
  EXPECT_EQ(TErrorCode::SCRATCH_READ_VERIFY_FAILED, read_status.code())
      << read_status.GetDetail();

  // Modify the data in memory. Restoring the data should fail.
  LOG(INFO) << "Corrupting data in memory";
  data[0] = corrupt_byte;
  Status restore_status = file_group.RestoreData(move(handle), data_mem_range);
  LOG(INFO) << restore_status.GetDetail();
  EXPECT_EQ(TErrorCode::SCRATCH_READ_VERIFY_FAILED, restore_status.code())
      << restore_status.GetDetail();

  file_group.Close();
  test_env_->TearDownQueries();
}

// Test that the current scratch space bytes and HWM values are proper when different
// FileGroups are used concurrently. This test unit mimics concurrent spilling queries.
TEST_F(TmpFileMgrTest, TestHWMMetric) {
  RuntimeProfile* profile_1 = RuntimeProfile::Create(&obj_pool_, "tmp-file-mgr-test-1");
  RuntimeProfile* profile_2 = RuntimeProfile::Create(&obj_pool_, "tmp-file-mgr-test-2");
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", false, metrics_.get()));

  const int64_t LIMIT = 128;
  // A power-of-two so that FileGroup allocates exactly this amount of scratch space.
  const int64_t ALLOC_SIZE = 64;
  TUniqueId id_1;
  TmpFileGroup file_group_1(&tmp_file_mgr, io_mgr(), profile_1, id_1, LIMIT);
  TUniqueId id_2;
  TmpFileGroup file_group_2(&tmp_file_mgr, io_mgr(), profile_2, id_2, LIMIT);

  vector<TmpFile*> files;
  ASSERT_OK(CreateFiles(&file_group_1, &files));
  ASSERT_OK(CreateFiles(&file_group_2, &files));

  Status status;
  int64_t offset;
  TmpFile* alloc_file;

  // Alloc from file_group_1 and file_group_2 interleaving allocations.
  SetNextAllocationIndex(&file_group_1, DEFAULT_PRIORITY, 0);
  ASSERT_OK(GroupAllocateSpace(&file_group_1, ALLOC_SIZE, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files[0]);
  ASSERT_EQ(0, offset);

  SetNextAllocationIndex(&file_group_2, DEFAULT_PRIORITY, 0);
  ASSERT_OK(GroupAllocateSpace(&file_group_2, ALLOC_SIZE, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files[2]);
  ASSERT_EQ(0, offset);

  ASSERT_OK(GroupAllocateSpace(&file_group_1, ALLOC_SIZE, &alloc_file, &offset));
  ASSERT_EQ(0, offset);
  ASSERT_EQ(alloc_file, files[1]);

  ASSERT_OK(GroupAllocateSpace(&file_group_2, ALLOC_SIZE, &alloc_file, &offset));
  ASSERT_EQ(0, offset);
  ASSERT_EQ(alloc_file, files[3]);

  EXPECT_EQ(LIMIT, BytesAllocated(&file_group_1));
  EXPECT_EQ(LIMIT, BytesAllocated(&file_group_2));

  // Check HWM metrics
  checkHWMMetrics(2 * LIMIT, 2 * LIMIT);
  file_group_1.Close();
  checkHWMMetrics(LIMIT, 2 * LIMIT);
  file_group_2.Close();
  checkHWMMetrics(0, 2 * LIMIT);
}

// Test that usage per directory is tracked correctly and per-directory limits are
// enforced. Sets up several scratch directories, some with limits, and checks
// that the allocations occur in the right directories.
TEST_F(TmpFileMgrTest, TestDirectoryLimits) {
  TestDirectoryLimits(false);
}

// Same, but with hole punching enabled.
TEST_F(TmpFileMgrTest, TestDirectoryLimitsPunchHoles) {
  TestDirectoryLimits(true);
}

void TmpFileMgrTest::TestDirectoryLimits(bool punch_holes) {
  // Use an allocation size where FileGroup allocates exactly this amount of scratch
  // space. The directory limits below are set relative to this size.
  const int64_t ALLOC_SIZE = TmpFileMgr::HOLE_PUNCH_BLOCK_SIZE_BYTES;
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2",
      "/tmp/tmp-file-mgr-test.3"});
  vector<string> tmp_dir_specs({"/tmp/tmp-file-mgr-test.1:4k",
      "/tmp/tmp-file-mgr-test.2:8k", "/tmp/tmp-file-mgr-test.3"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(
      tmp_file_mgr.InitCustom(tmp_dir_specs, false, "", punch_holes, metrics_.get()));

  TmpFileGroup file_group_1(
      &tmp_file_mgr, io_mgr(), RuntimeProfile::Create(&obj_pool_, "p1"), TUniqueId());
  TmpFileGroup file_group_2(
      &tmp_file_mgr, io_mgr(), RuntimeProfile::Create(&obj_pool_, "p2"), TUniqueId());

  vector<TmpFile*> files;
  ASSERT_OK(CreateFiles(&file_group_1, &files));
  ASSERT_OK(CreateFiles(&file_group_2, &files));

  IntGauge* dir1_usage = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.scratch-space-bytes-used.dir-0");
  IntGauge* dir2_usage = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.scratch-space-bytes-used.dir-1");
  IntGauge* dir3_usage = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.scratch-space-bytes-used.dir-2");

  int64_t offset;
  TmpFile* alloc_file;

  // Allocate three times - once per directory. We expect these allocations to go through
  // so we should have one allocation in each directory.
  SetNextAllocationIndex(&file_group_1, DEFAULT_PRIORITY, 0);
  for (int i = 0; i < tmp_dir_specs.size(); ++i) {
    ASSERT_OK(GroupAllocateSpace(&file_group_1, ALLOC_SIZE, &alloc_file, &offset));
  }
  EXPECT_EQ(ALLOC_SIZE, dir1_usage->GetValue());
  EXPECT_EQ(ALLOC_SIZE, dir2_usage->GetValue());
  EXPECT_EQ(ALLOC_SIZE, dir3_usage->GetValue());

  // This time we should hit the limit on the first directory. Do this from a
  // different file group to show that limits are enforced across file groups.
  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(GroupAllocateSpace(&file_group_2, ALLOC_SIZE, &alloc_file, &offset));
  }
  EXPECT_EQ(ALLOC_SIZE, dir1_usage->GetValue());
  EXPECT_EQ(2 * ALLOC_SIZE, dir2_usage->GetValue());
  EXPECT_EQ(2 * ALLOC_SIZE, dir3_usage->GetValue());

  // Now we're at the limits on two directories, all allocations should got to the
  // last directory without a limit.
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(GroupAllocateSpace(&file_group_2, ALLOC_SIZE, &alloc_file, &offset));
  }
  EXPECT_EQ(ALLOC_SIZE, dir1_usage->GetValue());
  EXPECT_EQ(2 * ALLOC_SIZE, dir2_usage->GetValue());
  EXPECT_EQ(102 * ALLOC_SIZE, dir3_usage->GetValue());

  file_group_2.Close();
  // Metrics should be decremented when the file groups delete the underlying files.
  EXPECT_EQ(ALLOC_SIZE, dir1_usage->GetValue());
  EXPECT_EQ(ALLOC_SIZE, dir2_usage->GetValue());
  EXPECT_EQ(ALLOC_SIZE, dir3_usage->GetValue());

  // We should be able to reuse the space freed up.
  ASSERT_OK(GroupAllocateSpace(&file_group_1, ALLOC_SIZE, &alloc_file, &offset));

  EXPECT_EQ(2 * ALLOC_SIZE, dir2_usage->GetValue());
  file_group_1.Close();
  EXPECT_EQ(0, dir1_usage->GetValue());
  EXPECT_EQ(0, dir2_usage->GetValue());
  EXPECT_EQ(0, dir3_usage->GetValue());
}

// Test the case when all per-directory limits are hit. We expect to return a status
// and fail gracefully.
TEST_F(TmpFileMgrTest, TestDirectoryLimitsExhausted) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1", "/tmp/tmp-file-mgr-test.2"});
  vector<string> tmp_dir_specs(
      {"/tmp/tmp-file-mgr-test.1:256kb", "/tmp/tmp-file-mgr-test.2:1mb"});
  const int64_t DIR1_LIMIT = 256L * 1024L;
  const int64_t DIR2_LIMIT = 1024L * 1024L;
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dir_specs, false, "", false, metrics_.get()));

  TmpFileGroup file_group_1(
      &tmp_file_mgr, io_mgr(), RuntimeProfile::Create(&obj_pool_, "p1"), TUniqueId());
  TmpFileGroup file_group_2(
      &tmp_file_mgr, io_mgr(), RuntimeProfile::Create(&obj_pool_, "p2"), TUniqueId());

  vector<TmpFile*> files;
  ASSERT_OK(CreateFiles(&file_group_1, &files));
  ASSERT_OK(CreateFiles(&file_group_2, &files));

  IntGauge* dir1_usage = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.scratch-space-bytes-used.dir-0");
  IntGauge* dir2_usage = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.scratch-space-bytes-used.dir-1");

  // A power-of-two so that FileGroup allocates exactly this amount of scratch space.
  const int64_t ALLOC_SIZE = 512;
  const int64_t MAX_ALLOCATIONS = (DIR1_LIMIT + DIR2_LIMIT) / ALLOC_SIZE;
  int64_t offset;
  TmpFile* alloc_file;

  // Allocate exactly the maximum total capacity of the directories.
  SetNextAllocationIndex(&file_group_1, DEFAULT_PRIORITY, 0);
  for (int i = 0; i < MAX_ALLOCATIONS; ++i) {
    ASSERT_OK(GroupAllocateSpace(&file_group_1, ALLOC_SIZE, &alloc_file, &offset));
  }
  EXPECT_EQ(DIR1_LIMIT, dir1_usage->GetValue());
  EXPECT_EQ(DIR2_LIMIT, dir2_usage->GetValue());
  // The directories are at capacity, so allocations should fail.
  Status err1 = GroupAllocateSpace(&file_group_1, ALLOC_SIZE, &alloc_file, &offset);
  ASSERT_EQ(err1.code(), TErrorCode::SCRATCH_ALLOCATION_FAILED);
  EXPECT_STR_CONTAINS(err1.GetDetail(), "Could not create files in any configured "
      "scratch directories (--scratch_dirs=/tmp/tmp-file-mgr-test.1/impala-scratch,"
      "/tmp/tmp-file-mgr-test.2/impala-scratch)");
  EXPECT_STR_CONTAINS(err1.GetDetail(), "1.25 MB of scratch is currently in use by "
      "this Impala Daemon (1.25 MB by this query)");
  Status err2 = GroupAllocateSpace(&file_group_2, ALLOC_SIZE, &alloc_file, &offset);
  ASSERT_EQ(err2.code(), TErrorCode::SCRATCH_ALLOCATION_FAILED);
  EXPECT_STR_CONTAINS(err2.GetDetail(), "Could not create files in any configured "
      "scratch directories (--scratch_dirs=/tmp/tmp-file-mgr-test.1/impala-scratch,"
      "/tmp/tmp-file-mgr-test.2/impala-scratch)");
  EXPECT_STR_CONTAINS(err2.GetDetail(), "1.25 MB of scratch is currently in use by "
      "this Impala Daemon (0 by this query)");

  // A FileGroup should recover once allocations are released, i.e. it does not
  // permanently block allocating files from the group.
  file_group_1.Close();
  ASSERT_OK(GroupAllocateSpace(&file_group_2, ALLOC_SIZE, &alloc_file, &offset));
  file_group_2.Close();
}

// Test the directory parsing logic, including the various error cases.
TEST_F(TmpFileMgrTest, TestDirectoryLimitParsing) {
  RemoveAndCreateDirs({"/tmp/tmp-file-mgr-test1", "/tmp/tmp-file-mgr-test2",
      "/tmp/tmp-file-mgr-test3", "/tmp/tmp-file-mgr-test4", "/tmp/tmp-file-mgr-test5",
      "/tmp/tmp-file-mgr-test6", "/tmp/tmp-file-mgr-test7"});
  // Configure various directories with valid formats.
  auto& dirs = GetTmpDirs(
      CreateTmpFileMgr("/tmp/tmp-file-mgr-test1:5g:1,"
                       "/tmp/tmp-file-mgr-test2::2,"
                       "/tmp/tmp-file-mgr-test3:1234:3,"
                       "/tmp/tmp-file-mgr-test4:99999999:4,"
                       "/tmp/tmp-file-mgr-test5:200tb:5,"
                       "/tmp/tmp-file-mgr-test6:100MB:6"));
  EXPECT_EQ(6, dirs.size());
  EXPECT_EQ(5 * GIGABYTE, dirs[0]->bytes_limit());
  EXPECT_EQ(numeric_limits<int64_t>::max(), dirs[1]->bytes_limit());
  EXPECT_EQ(1234, dirs[2]->bytes_limit());
  EXPECT_EQ(99999999, dirs[3]->bytes_limit());
  EXPECT_EQ(200 * TERABYTE, dirs[4]->bytes_limit());
  EXPECT_EQ(100 * MEGABYTE, dirs[5]->bytes_limit());

  // Various invalid limit formats result in the directory getting skipped.
  // Include a valid dir on the end to ensure that we don't short-circuit all
  // directories.
  auto& dirs2 = GetTmpDirs(
      CreateTmpFileMgr("/tmp/tmp-file-mgr-test1:foo,/tmp/tmp-file-mgr-test2:?,"
                       "/tmp/tmp-file-mgr-test3:1.2.3.4,/tmp/tmp-file-mgr-test4: ,"
                       "/tmp/tmp-file-mgr-test5:5pb,/tmp/tmp-file-mgr-test6:10%,"
                       "/tmp/tmp-file-mgr-test1:100"));
  EXPECT_EQ(1, dirs2.size());
  EXPECT_EQ("/tmp/tmp-file-mgr-test1/impala-scratch", dirs2[0]->path());
  EXPECT_EQ(100, dirs2[0]->bytes_limit());

  // Various valid ways of specifying "unlimited".
  auto& dirs3 =
      GetTmpDirs(CreateTmpFileMgr("/tmp/tmp-file-mgr-test1:,/tmp/tmp-file-mgr-test2:-1,"
                                  "/tmp/tmp-file-mgr-test3,/tmp/tmp-file-mgr-test4:0"));
  EXPECT_EQ(4, dirs3.size());
  for (const auto& dir : dirs3) {
    EXPECT_EQ(numeric_limits<int64_t>::max(), dir->bytes_limit());
  }

  // Extra colons
  auto& dirs4 = GetTmpDirs(
      CreateTmpFileMgr("/tmp/tmp-file-mgr-test1:1:,/tmp/tmp-file-mgr-test2:10mb::"));
  EXPECT_EQ(1, dirs4.size());

  // Empty strings.
  auto& nodirs = GetTmpDirs(CreateTmpFileMgr(""));
  EXPECT_EQ(0, nodirs.size());
  auto& empty_paths = GetTmpDirs(CreateTmpFileMgr(","));
  EXPECT_EQ(2, empty_paths.size());
}

// Test the scratch directory parsing logic with remote paths.
TEST_F(TmpFileMgrTest, TestDirectoryLimitParsingRemotePath) {
  // Create a local directory for the local buffer.
  RemoveAndCreateDirs({"/tmp/local-buffer-dir", "/tmp/local-buffer-dir1",
      "/tmp/local-buffer-dir2", "/tmp/local-buffer-dir3"});

  // Successful cases for HDFS paths.
  // Two types of paths, one with directory, one without.
  vector<string> hdfs_paths{"hdfs://localhost:20500", "hdfs://localhost:20500/tmp"};
  for (string hdfs_path : hdfs_paths) {
    string full_hdfs_path = hdfs_path + "/impala-scratch";
    auto& dirs1 = GetTmpRemoteDir(CreateTmpFileMgr(hdfs_path + ",/tmp/local-buffer-dir"));
    EXPECT_NE(nullptr, dirs1);

    auto& dirs2 =
        GetTmpRemoteDir(CreateTmpFileMgr(hdfs_path + ":100,/tmp/local-buffer-dir"));
    EXPECT_NE(nullptr, dirs2);
    EXPECT_EQ(full_hdfs_path, dirs2->path());
    EXPECT_EQ(100, dirs2->bytes_limit());

    auto& dirs3 =
        GetTmpRemoteDir(CreateTmpFileMgr(hdfs_path + ":1KB:1,/tmp/local-buffer-dir"));
    EXPECT_NE(nullptr, dirs3);
    EXPECT_EQ(full_hdfs_path, dirs3->path());
    EXPECT_EQ(1024, dirs3->bytes_limit());

    // Multiple local paths with one remote path.
    auto tmp_mgr_4 = CreateTmpFileMgr(hdfs_path
        + ",/tmp/local-buffer-dir1,"
          "/tmp/local-buffer-dir2,/tmp/local-buffer-dir3");
    auto& dirs4_local = GetTmpDirs(tmp_mgr_4);
    auto& dirs4_remote = GetTmpRemoteDir(tmp_mgr_4);
    EXPECT_NE(nullptr, dirs4_remote);
    EXPECT_EQ(full_hdfs_path, dirs4_remote->path());
    EXPECT_EQ(2, dirs4_local.size());
    EXPECT_EQ("/tmp/local-buffer-dir2/impala-scratch", dirs4_local[0]->path());
    EXPECT_EQ("/tmp/local-buffer-dir3/impala-scratch", dirs4_local[1]->path());

    // Fails the parsing due to no port number for the HDFS path.
    auto tmp_mgr_5 = CreateTmpFileMgr("hdfs://localhost/tmp,/tmp/local-buffer-dir");
    auto& dirs5_local = GetTmpDirs(tmp_mgr_5);
    auto& dirs5_remote = GetTmpRemoteDir(tmp_mgr_5);
    EXPECT_EQ(1, dirs5_local.size());
    EXPECT_EQ(nullptr, dirs5_remote);

    // Fails to init due to no local buffer for the remote scratch space.
    // We expect a non-null tmp_mgr, but the init process should fail.
    EXPECT_NE(nullptr, CreateTmpFileMgr(hdfs_path, false));

    // Parse successfully, but the parsed HDFS path is unable to connect.
    // These cases would fail the initialization of TmpFileMgr.
    auto& dirs7 = GetTmpRemoteDir(
        CreateTmpFileMgr("hdfs://localhost:1/tmp::1,/tmp/local-buffer-dir", false));
    EXPECT_EQ(nullptr, dirs7);

    auto& dirs8 = GetTmpRemoteDir(
        CreateTmpFileMgr("hdfs://localhost:/tmp::,/tmp/local-buffer-dir", false));
    EXPECT_EQ(nullptr, dirs8);

    auto& dirs9 = GetTmpRemoteDir(
        CreateTmpFileMgr("hdfs://localhost/tmp::1,/tmp/local-buffer-dir", false));
    EXPECT_EQ(nullptr, dirs9);

    auto& dirs10 = GetTmpRemoteDir(
        CreateTmpFileMgr("hdfs://localhost/tmp:1,/tmp/local-buffer-dir", false));
    EXPECT_EQ(nullptr, dirs10);

    // Multiple remote paths, should support only one.
    auto& dirs11 = GetTmpRemoteDir(CreateTmpFileMgr(hdfs_path
        + ",hdfs://localhost:20501/tmp,"
          "/tmp/local-buffer-dir"));
    EXPECT_NE(nullptr, dirs11);
    EXPECT_EQ(full_hdfs_path, dirs11->path());

    // The order of the buffer and the remote dir should not affect the result.
    auto& dirs12 = GetTmpRemoteDir(CreateTmpFileMgr(
        "/tmp/local-buffer-dir, " + hdfs_path + ",hdfs://localhost:20501/tmp"));
    EXPECT_NE(nullptr, dirs12);
    EXPECT_EQ(full_hdfs_path, dirs12->path());
  }

  // Successful cases for parsing S3 paths.
  // Create a fake s3 connection in order to pass the connection verification.
  HdfsFsCache::HdfsFsMap fake_hdfs_conn_map;
  hdfsFS fake_conn = reinterpret_cast<hdfsFS>(1);
  fake_hdfs_conn_map.insert(make_pair("s3a://fake_host/", fake_conn));
  // Two types of paths, one with directory, one without.
  vector<string> s3_paths{"s3a://fake_host", "s3a://fake_host/dir"};
  for (string s3_path : s3_paths) {
    string full_s3_path = s3_path + "/impala-scratch";
    auto& dirs13 = GetTmpRemoteDir(
        CreateTmpFileMgr("/tmp/local-buffer-dir, " + s3_path, true, &fake_hdfs_conn_map));
    EXPECT_NE(nullptr, dirs13);
    EXPECT_EQ(full_s3_path, dirs13->path());

    auto& dirs14 = GetTmpRemoteDir(CreateTmpFileMgr(
        "/tmp/local-buffer-dir, " + s3_path + ":100", true, &fake_hdfs_conn_map));
    EXPECT_NE(nullptr, dirs14);
    EXPECT_EQ(full_s3_path, dirs14->path());
    EXPECT_EQ(100, dirs14->bytes_limit());

    auto& dirs15 = GetTmpRemoteDir(CreateTmpFileMgr(
        "/tmp/local-buffer-dir, " + s3_path + ":1KB:1", true, &fake_hdfs_conn_map));
    EXPECT_NE(nullptr, dirs15);
    EXPECT_EQ(full_s3_path, dirs15->path());
    EXPECT_EQ(1024, dirs15->bytes_limit());
  }

  // Failure cases for parsing S3 paths, S3 paths don't allow the port number.
  auto& dirs16 = GetTmpRemoteDir(CreateTmpFileMgr(
      "/tmp/local-buffer-dir, s3a://fake_host:1234/for-parsing-test-only:1KB:1", true,
      &fake_hdfs_conn_map));
  EXPECT_EQ(nullptr, dirs16);

  auto& dirs17 = GetTmpRemoteDir(CreateTmpFileMgr(
      "/tmp/local-buffer-dir, s3a://fake_host:1234/for-parsing-test-only:1KB", true,
      &fake_hdfs_conn_map));
  EXPECT_EQ(nullptr, dirs17);

  auto& dirs18 = GetTmpRemoteDir(CreateTmpFileMgr(
      "/tmp/local-buffer-dir, s3a://fake_host:1234/for-parsing-test-only", true,
      &fake_hdfs_conn_map));
  EXPECT_EQ(nullptr, dirs18);
}

// Test compression buffer memory management for reads and writes.
TEST_F(TmpFileMgrTest, TestCompressBufferManagement) {
  FLAGS_disk_spill_encryption = false;
  TestCompressBufferManagement(false, false);
}

TEST_F(TmpFileMgrTest, TestCompressBufferManagementEncrypted) {
  FLAGS_disk_spill_encryption = true;
  TestCompressBufferManagement(false, false);
}

// Test spill to a remote directory with compression or encryption.
TEST_F(TmpFileMgrTest, TestCompressBufferManagementRemote) {
  FLAGS_disk_spill_encryption = false;
  TestCompressBufferManagement(true, false);
}

TEST_F(TmpFileMgrTest, TestCompressBufferManagementEncryptedRemote) {
  FLAGS_disk_spill_encryption = true;
  TestCompressBufferManagement(true, false);
}

// Test spill to a remote directory with compression or encryption, and wait for the
// upload finished.
TEST_F(TmpFileMgrTest, TestCompressBufferManagementRemoteUpload) {
  FLAGS_disk_spill_encryption = false;
  FLAGS_remote_tmp_file_size = "8B";
  TestCompressBufferManagement(true, true);
}

TEST_F(TmpFileMgrTest, TestCompressBufferManagementEncryptedRemoteUpload) {
  FLAGS_disk_spill_encryption = true;
  FLAGS_remote_tmp_file_size = "8B";
  TestCompressBufferManagement(true, true);
}

void TmpFileMgrTest::TestCompressBufferManagement(
    bool spill_to_remote, bool wait_upload) {
  // Data string should be long and redundant enough to be compressible.
  const string DATA = "the quick brown fox jumped over the lazy dog"
                      "the fast red fox leaped over the sleepy dog";
  const string BIG_DATA = DATA + DATA;
  string data = DATA;
  string big_data = BIG_DATA;
  FLAGS_disk_spill_compression_buffer_limit_bytes = 2 * data.size();
  // Limit compression buffers to not quite enough for two compression buffers
  // for 'data' (since compression buffers need to be slightly larger than
  // uncompressed data).
  TmpFileMgr tmp_file_mgr;
  if (spill_to_remote) {
    RemoveAndCreateDirs(vector<string>{LOCAL_BUFFER_PATH});
    ASSERT_OK(tmp_file_mgr.InitCustom(vector<string>{REMOTE_URL, LOCAL_BUFFER_PATH}, true,
        "lz4", true, metrics_.get()));
  } else {
    ASSERT_OK(tmp_file_mgr.InitCustom(
        vector<string>{"/tmp/tmp-file-mgr-test.1"}, true, "lz4", true, metrics_.get()));
  }
  MemTracker* compressed_buffer_tracker = tmp_file_mgr.compressed_buffer_tracker();
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, TUniqueId());
  MemRange data_mem_range(reinterpret_cast<uint8_t*>(&data[0]), data.size());
  MemRange big_data_mem_range(reinterpret_cast<uint8_t*>(&big_data[0]), big_data.size());

  // Start a write in flight, which should encrypt the data and write it to disk.
  unique_ptr<TmpWriteHandle> compressed_handle, uncompressed_handle;
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
  ASSERT_OK(file_group.Write(data_mem_range, callback, &compressed_handle));
  EXPECT_TRUE(compressed_handle->is_compressed());
  int mem_consumption_after_first_write = compressed_buffer_tracker->peak_consumption();
  EXPECT_GT(mem_consumption_after_first_write, 0)
      << "Compressed buffer memory should be consumed for in-flight writes";
  WaitForWrite(compressed_handle.get());
  EXPECT_EQ(0, compressed_buffer_tracker->consumption())
      << "No memory should be consumed when no reads or writes in-flight";

  // Spot-check that bytes written counters were updated correctly.
  EXPECT_GT(file_group.bytes_written_counter_->value(), 0);
  EXPECT_GT(file_group.uncompressed_bytes_written_counter_->value(),
            file_group.bytes_written_counter_->value());

  // This data range is larger than the memory limit and falls back to uncompressed
  // writes.
  ASSERT_OK(file_group.Write(big_data_mem_range, callback, &uncompressed_handle));
  EXPECT_EQ(uncompressed_handle->data_len(), uncompressed_handle->on_disk_len());
  EXPECT_FALSE(uncompressed_handle->is_compressed());
  EXPECT_LE(compressed_buffer_tracker->consumption(), mem_consumption_after_first_write)
      << "Second write should have fallen back to uncompressed writes";

  WaitForWrite(uncompressed_handle.get());
  WaitForCallbacks(2);
  EXPECT_EQ(0, compressed_buffer_tracker->consumption())
      << "No memory should be consumed when no reads or writes in-flight";

  if (spill_to_remote && wait_upload) {
    EXPECT_NE(compressed_handle->file_, uncompressed_handle->file_);
    auto wait_upload_func = [&](TmpFile* tmp_file) {
      // Wait until the file has been uploaded to remote dir.
      // Should be finished in 2 seconds.
      int wait_times = 10;
      while (wait_times-- > 0) {
        if (tmp_file->DiskFile()->GetFileStatus() == io::DiskFileStatus::PERSISTED) break;
        usleep(200 * 1000);
      }
      EXPECT_EQ(tmp_file->DiskFile()->GetFileStatus(), io::DiskFileStatus::PERSISTED);
      // Remove the local buffer to enforce reading from the remote file.
      tmp_file->GetWriteFile()->SetStatus(io::DiskFileStatus::DELETED);
      EXPECT_OK(FileSystemUtil::RemovePaths({tmp_file->GetWriteFile()->path()}));
    };
    wait_upload_func(compressed_handle->file_);
    wait_upload_func(uncompressed_handle->file_);
  }

  vector<uint8_t> tmp(data.size());
  vector<uint8_t> big_tmp(big_data.size());
  // Check behaviour when reading compressed range with available memory.
  // Reading from disk needs to allocate a compression buffer.
  EXPECT_OK(file_group.Read(compressed_handle.get(), MemRange(tmp.data(), tmp.size())));
  EXPECT_EQ(0, memcmp(tmp.data(), DATA.data(), DATA.size()));

  // Check behaviour when reading ranges with not enough available memory.
  compressed_buffer_tracker->Consume(DATA.size() * 2);
  Status status =
      file_group.Read(compressed_handle.get(), MemRange(tmp.data(), tmp.size()));
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::MEM_LIMIT_EXCEEDED);

  // Reading uncompressed range becomes no extra memory
  EXPECT_OK(file_group.Read(
      uncompressed_handle.get(), MemRange(big_tmp.data(), big_tmp.size())));
  EXPECT_EQ(0, memcmp(big_tmp.data(), BIG_DATA.data(), BIG_DATA.size()));

  // Clear buffers to avoid false positives in tests.
  memset(tmp.data(), 0, tmp.size());
  memset(big_tmp.data(), 0, big_tmp.size());

  // Restoring data should work ok with no memory.
  EXPECT_OK(file_group.RestoreData(move(compressed_handle), data_mem_range));
  EXPECT_EQ(0, memcmp(DATA.data(), data.data(), data.size()));
  EXPECT_OK(file_group.RestoreData(move(uncompressed_handle), big_data_mem_range));
  EXPECT_EQ(0, memcmp(BIG_DATA.data(), big_data.data(), big_data.size()));

  compressed_buffer_tracker->Release(DATA.size() * 2);
  file_group.Close();
}

// Test the directory parsing logic, including the various error cases.
TEST_F(TmpFileMgrTest, TestDirectoryPriorityParsing) {
  RemoveAndCreateDirs({"/tmp/tmp-file-mgr-test1", "/tmp/tmp-file-mgr-test2",
      "/tmp/tmp-file-mgr-test3", "/tmp/tmp-file-mgr-test4", "/tmp/tmp-file-mgr-test5",
      "/tmp/tmp-file-mgr-test6", "/tmp/tmp-file-mgr-test7"});
  // Configure various directories with valid formats. The directories are passed in
  // unsorted priority order. The expectation is that the resulting directory lists will
  // be sorted by priority.
  auto& dirs = GetTmpDirs(
      CreateTmpFileMgr("/tmp/tmp-file-mgr-test3:1234:3,/tmp/tmp-file-mgr-test6:100MB:,"
                       "/tmp/tmp-file-mgr-test2::2,/tmp/tmp-file-mgr-test4:99999999:4,"
                       "/tmp/tmp-file-mgr-test5:200tb:5,/tmp/tmp-file-mgr-test1:5g:1"));
  EXPECT_EQ(6, dirs.size());
  EXPECT_EQ(5 * GIGABYTE, dirs[0]->bytes_limit());
  EXPECT_EQ(1, dirs[0]->priority());
  EXPECT_EQ(numeric_limits<int64_t>::max(), dirs[1]->bytes_limit());
  EXPECT_EQ(2, dirs[1]->priority());
  EXPECT_EQ(1234, dirs[2]->bytes_limit());
  EXPECT_EQ(3, dirs[2]->priority());
  EXPECT_EQ(99999999, dirs[3]->bytes_limit());
  EXPECT_EQ(4, dirs[3]->priority());
  EXPECT_EQ(200 * TERABYTE, dirs[4]->bytes_limit());
  EXPECT_EQ(5, dirs[4]->priority());
  EXPECT_EQ(100 * MEGABYTE, dirs[5]->bytes_limit());
  EXPECT_EQ(numeric_limits<int>::max(), dirs[5]->priority());

  // Various invalid limit formats result in the directory getting skipped.
  // Include a valid dir on the end to ensure that we don't short-circuit all
  // directories.
  auto& dirs2 = GetTmpDirs(
      CreateTmpFileMgr("/tmp/tmp-file-mgr-test1::foo,/tmp/tmp-file-mgr-test2::?,"
                       "/tmp/tmp-file-mgr-test3::1.2.3.4,/tmp/tmp-file-mgr-test4:: ,"
                       "/tmp/tmp-file-mgr-test5::p0,/tmp/tmp-file-mgr-test6::10%,"
                       "/tmp/tmp-file-mgr-test1:100:-1"));
  EXPECT_EQ(1, dirs2.size());
  EXPECT_EQ("/tmp/tmp-file-mgr-test1/impala-scratch", dirs2[0]->path());
  EXPECT_EQ(100, dirs2[0]->bytes_limit());
  EXPECT_EQ(-1, dirs2[0]->priority());
}

// Tests that when TmpFileGroup is constructed, the priority based index ranges are
// populated properly.
TEST_F(TmpFileMgrTest, TestPriorityBasedIndexRanges) {
  // Tmp dirs with priority 0, 1 and 2.
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test5::1", "/tmp/tmp-file-mgr-test2::0",
      "/tmp/tmp-file-mgr-test7::2", "/tmp/tmp-file-mgr-test1::0",
      "/tmp/tmp-file-mgr-test6::1", "/tmp/tmp-file-mgr-test3::0",
      "/tmp/tmp-file-mgr-test4::1"});
  // Tmp dirs with priority 0 and 1.
  vector<string> tmp_dirs1({"/tmp/tmp-file-mgr-test5::0", "/tmp/tmp-file-mgr-test2::1",
      "/tmp/tmp-file-mgr-test7::0", "/tmp/tmp-file-mgr-test1::1",
      "/tmp/tmp-file-mgr-test6::0", "/tmp/tmp-file-mgr-test3::1",
      "/tmp/tmp-file-mgr-test4::0"});
  // Tmp dirs with priority 0.
  vector<string> tmp_dirs2({"/tmp/tmp-file-mgr-test1::0", "/tmp/tmp-file-mgr-test2::0",
      "/tmp/tmp-file-mgr-test3::0", "/tmp/tmp-file-mgr-test4::0"});
  TmpFileMgr tmp_file_mgr;
  TmpFileMgr tmp_file_mgr1;
  TmpFileMgr tmp_file_mgr2;
  scoped_ptr<MetricGroup> metrics1(new MetricGroup("tmp-file-mgr-test"));
  scoped_ptr<MetricGroup> metrics2(new MetricGroup("tmp-file-mgr-test"));
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", false, metrics_.get()));
  ASSERT_OK(tmp_file_mgr1.InitCustom(tmp_dirs1, false, "", false, metrics1.get()));
  ASSERT_OK(tmp_file_mgr2.InitCustom(tmp_dirs2, false, "", false, metrics2.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);
  ValidatePriorityIndexRange(&file_group, 0, 0, 2);
  ValidatePriorityIndexRange(&file_group, 1, 3, 5);
  ValidatePriorityIndexRange(&file_group, 2, 6, 6);
  TmpFileGroup file_group1(&tmp_file_mgr1, io_mgr(), profile_, id);
  ValidatePriorityIndexRange(&file_group1, 0, 0, 3);
  ValidatePriorityIndexRange(&file_group1, 1, 4, 6);
  TmpFileGroup file_group2(&tmp_file_mgr2, io_mgr(), profile_, id);
  ValidatePriorityIndexRange(&file_group2, 0, 0, 3);
  file_group.Close();
  file_group1.Close();
  file_group2.Close();
}

// Tests various scenarios with priority based spilling.
TEST_F(TmpFileMgrTest, TestPriorityBasedSpilling) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test1:1K:0", "/tmp/tmp-file-mgr-test2:1K:3",
      "/tmp/tmp-file-mgr-test3:1K:2", "/tmp/tmp-file-mgr-test4:1K:2",
      "/tmp/tmp-file-mgr-test5:1K:2", "/tmp/tmp-file-mgr-test6:1K:1"});
  RemoveAndCreateDirs(tmp_dirs);
  TmpFileMgr tmp_file_mgr;
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", false, metrics_.get()));

  // The allocation size is same as the limit per directory.
  // So each scratch directory can fit exactly one allocation.
  const int alloc_size = 1024;
  const int64_t limit = alloc_size * 6;
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id, limit);
  TUniqueId id1;
  TmpFileGroup file_group1(&tmp_file_mgr, io_mgr(), profile_, id1, limit);
  TUniqueId id2;
  TmpFileGroup file_group2(&tmp_file_mgr, io_mgr(), profile_, id2, limit);

  vector<TmpFile*> files;
  ASSERT_OK(CreateFiles(&file_group, &files));
  vector<TmpFile*> files1;
  ASSERT_OK(CreateFiles(&file_group1, &files1));
  vector<TmpFile*> files2;
  ASSERT_OK(CreateFiles(&file_group2, &files2));

  int64_t offset;
  TmpFile* alloc_file;

  // filegroup should allocate from file at index 0
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files[0]);

  // filegroup1 should allocate from file at index 1
  ASSERT_OK(GroupAllocateSpace(&file_group1, alloc_size, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files1[1]);

  SetNextAllocationIndex(&file_group1, 2, 4);
  // filegroup1 should allocate from file at index 4
  ASSERT_OK(GroupAllocateSpace(&file_group1, alloc_size, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files1[4]);

  // filegroup1 should allocate from file at index 2
  ASSERT_OK(GroupAllocateSpace(&file_group1, alloc_size, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files1[2]);

  // filegroup2 should allocate from file at index 3
  ASSERT_OK(GroupAllocateSpace(&file_group2, alloc_size, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files2[3]);

  // filegroup should allocate from file at index 5
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files[5]);

  // Closing filegroup should result in freeing allocation at index 0 and 5
  file_group.Close();

  // filegroup1 should allocate from file at index 0
  ASSERT_OK(GroupAllocateSpace(&file_group1, alloc_size, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files1[0]);

  // Closing filegroup2 should result in freeing allocation at index 3
  file_group2.Close();

  // filegroup should allocate from file at index 3
  ASSERT_OK(GroupAllocateSpace(&file_group1, alloc_size, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files1[3]);

  // filegroup should allocate from file at index 5
  ASSERT_OK(GroupAllocateSpace(&file_group1, alloc_size, &alloc_file, &offset));
  ASSERT_EQ(alloc_file, files1[5]);

  file_group1.Close();
}

/// Test the case we have one remote dir with one local dir as buffer.
TEST_F(TmpFileMgrTest, TestRemoteOneDir) {
  vector<string> tmp_dirs({LOCAL_BUFFER_PATH});
  TmpFileMgr tmp_file_mgr;
  RemoveAndCreateDirs(tmp_dirs);
  tmp_dirs.push_back(REMOTE_URL);
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, true, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);
  int64_t alloc_size = 1024;

  EXPECT_EQ(1, tmp_file_mgr.NumActiveTmpDevices());
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(1, devices.size());
  TmpFile* file = nullptr;
  int64_t offset = -1;
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &file, &offset));
  ASSERT_TRUE(file != nullptr);
  EXPECT_EQ(0, offset);
  // Check the prefix is the expected temporary directory.
  EXPECT_EQ(0, file->path().find(tmp_dirs[1]));
  file_group.Close();
  CheckMetrics(&tmp_file_mgr);
}

/// Test that reporting a write error is possible but does not result in
/// blacklisting the device.
TEST_F(TmpFileMgrTest, TestRemoteDirReportError) {
  int64_t alloc_size = 1024;
  int64_t alloc_size2 = 512;
  FLAGS_remote_tmp_file_size = "2K";
  vector<string> tmp_dirs({LOCAL_BUFFER_PATH});
  TmpFileMgr tmp_file_mgr;
  RemoveAndCreateDirs(tmp_dirs);
  tmp_dirs.push_back(REMOTE_URL);
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);

  EXPECT_EQ(1, tmp_file_mgr.NumActiveTmpDevices());
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(1, devices.size());
  CheckMetrics(&tmp_file_mgr);

  TmpFile* good_file = nullptr;
  int64_t offset = -1;
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &good_file, &offset));
  ASSERT_TRUE(good_file != nullptr);
  EXPECT_EQ(0, offset);

  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &good_file, &offset));
  ASSERT_TRUE(good_file != nullptr);
  EXPECT_EQ(alloc_size, offset);

  // A new file should be created.
  TmpFile* bad_file = nullptr;
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &bad_file, &offset));
  ASSERT_TRUE(bad_file != nullptr);
  EXPECT_EQ(0, offset);

  // Inject an error on one device so that we can validate it is handled correctly.
  ErrorMsg errmsg(TErrorCode::GENERAL, "A fake error");
  bad_file->Blacklist(errmsg);

  // File-level blacklisting is enabled but not device-level.
  EXPECT_TRUE(bad_file->is_blacklisted());
  // The bad device should still be active.
  EXPECT_EQ(1, tmp_file_mgr.NumActiveTmpDevices());
  vector<TmpFileMgr::DeviceId> devices_after = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(1, devices_after.size());
  CheckMetrics(&tmp_file_mgr);

  // Attempts to expand bad file should change a file.
  TmpFile* bad_file2;
  TmpFile* good_file2;
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size2, &bad_file2, &offset));
  EXPECT_EQ(0, offset);
  EXPECT_NE(bad_file, bad_file2);
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &good_file2, &offset));
  EXPECT_EQ(alloc_size2, offset);
  EXPECT_EQ(bad_file2, good_file2);
  file_group.Close();
  CheckMetrics(&tmp_file_mgr);
}

TEST_F(TmpFileMgrTest, TestRemoteAllocateNonWritable) {
  vector<string> tmp_dirs({LOCAL_BUFFER_PATH});
  TmpFileMgr tmp_file_mgr;
  RemoveAndCreateDirs(tmp_dirs);
  tmp_dirs.push_back(REMOTE_URL);
  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, true, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);
  int64_t alloc_size = 1024;

  EXPECT_EQ(1, tmp_file_mgr.NumActiveTmpDevices());
  vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.ActiveTmpDevices();
  EXPECT_EQ(1, devices.size());
  TmpFile* file = nullptr;
  int64_t offset = -1;
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &file, &offset));
  ASSERT_TRUE(file != nullptr);
  EXPECT_EQ(0, offset);

  // Make local buffer non-writable and test allocation at different stages:
  // new file creation, files with no allocated blocks. files with allocated space.
  // No errors should be encountered during allocation since allocation is purely logical.
  chmod(LOCAL_BUFFER_PATH.c_str(), 0);
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &file, &offset));
  ASSERT_TRUE(file != nullptr);
  EXPECT_EQ(alloc_size, offset);
  chmod(LOCAL_BUFFER_PATH.c_str(), S_IRWXU);
  file_group.Close();
}

TEST_F(TmpFileMgrTest, TestRemoteScratchLimit) {
  int64_t alloc_size = 512;
  int64_t limit = alloc_size * 2;
  FLAGS_remote_tmp_file_size = "1K";
  vector<string> tmp_dirs({LOCAL_BUFFER_PATH});
  TmpFileMgr tmp_file_mgr;
  RemoveAndCreateDirs(tmp_dirs);
  tmp_dirs.push_back(REMOTE_URL);

  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, true, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id, limit);

  // Test individual limit is enforced.
  Status status;
  int64_t offset;
  TmpFile* alloc_file = nullptr;

  // Alloc from file 1 should succeed.
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &alloc_file, &offset));
  ASSERT_TRUE(alloc_file != nullptr);
  ASSERT_EQ(0, offset);

  // Allocate up to the max.
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &alloc_file, &offset));
  ASSERT_TRUE(alloc_file != nullptr);
  ASSERT_EQ(alloc_size, offset);

  // Test aggregate limit is enforced.
  status = GroupAllocateSpace(&file_group, 1, &alloc_file, &offset);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.code(), TErrorCode::SCRATCH_LIMIT_EXCEEDED);
  ASSERT_NE(string::npos, status.msg().msg().find(GetBackendString()));

  // Check HWM metrics
  checkHWMMetrics(limit, limit);
  file_group.Close();
  checkHWMMetrics(0, limit);
}

/// Test writing a single record to the remote dir and read it back from
/// the local buffer and verify the data correctness.
/// There is a testcase to verify the data correctness in DiskIoMgrTest for
/// reading from the remote file.
TEST_F(TmpFileMgrTest, TestRemoteWriteRange) {
  FLAGS_disk_spill_encryption = false;
  vector<string> tmp_dirs({LOCAL_BUFFER_PATH});
  TmpFileMgr tmp_file_mgr;
  RemoveAndCreateDirs(tmp_dirs);
  tmp_dirs.push_back(REMOTE_URL);

  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, true, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);
  string data = "the quick brown fox jumped over the lazy dog";
  MemRange data_mem_range(reinterpret_cast<uint8_t*>(&data[0]), data.size());

  // Start a write in flight, which should write it to disk without encryption.
  unique_ptr<TmpWriteHandle> handle;
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
  ASSERT_OK(file_group.Write(data_mem_range, callback, &handle));

  WaitForWrite(handle.get());
  WaitForCallbacks(1);

  vector<uint8_t> tmp;
  tmp.resize(data.size());
  Status read_status = file_group.Read(handle.get(), MemRange(tmp.data(), tmp.size()));
  EXPECT_TRUE(read_status.ok());
  EXPECT_EQ(tmp.size(), data.size());
  for (int i = 0; i < data.size(); i++) {
    EXPECT_EQ(tmp[i], data[i]);
  }
  file_group.Close();
}

/// Test writing a single record with encryption to a remote dir with local
/// buffer.
TEST_F(TmpFileMgrTest, TestRemoteBlockVerification) {
  FLAGS_disk_spill_encryption = true;
  vector<string> tmp_dirs({LOCAL_BUFFER_PATH});
  TmpFileMgr tmp_file_mgr;
  RemoveAndCreateDirs(tmp_dirs);
  tmp_dirs.push_back(REMOTE_URL);

  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, true, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);
  string data = "the quick brown fox jumped over the lazy dog";
  MemRange data_mem_range(reinterpret_cast<uint8_t*>(&data[0]), data.size());

  // Start a write in flight, which should encrypt the data and write it to disk.
  unique_ptr<TmpWriteHandle> handle;
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
  ASSERT_OK(file_group.Write(data_mem_range, callback, &handle));
  string file_path = handle->TmpFileBufferPath();

  WaitForWrite(handle.get());
  WaitForCallbacks(1);

  // Modify the data in the local buffer file and check that a read error occurs.
  LOG(INFO) << "Corrupting " << file_path;
  uint8_t corrupt_byte = data[0] ^ 1;
  FILE* file = fopen(file_path.c_str(), "rb+");
  ASSERT_TRUE(file != nullptr);
  ASSERT_EQ(corrupt_byte, fputc(corrupt_byte, file));
  ASSERT_EQ(0, fclose(file));
  vector<uint8_t> tmp;
  tmp.resize(data.size());
  Status read_status = file_group.Read(handle.get(), MemRange(tmp.data(), tmp.size()));
  LOG(INFO) << read_status.GetDetail();
  EXPECT_EQ(TErrorCode::SCRATCH_READ_VERIFY_FAILED, read_status.code())
      << read_status.GetDetail();

  // Modify the data in memory. Restoring the data should fail.
  LOG(INFO) << "Corrupting data in memory";
  data[0] = corrupt_byte;
  Status restore_status = file_group.RestoreData(move(handle), data_mem_range);
  LOG(INFO) << restore_status.GetDetail();
  EXPECT_EQ(TErrorCode::SCRATCH_READ_VERIFY_FAILED, restore_status.code())
      << restore_status.GetDetail();

  file_group.Close();
  test_env_->TearDownQueries();
}

/// Test when the total file size reaches the remote directory limits.
TEST_F(TmpFileMgrTest, TestRemoteDirectoryLimits) {
  vector<string> tmp_dirs{{LOCAL_BUFFER_PATH}};
  TmpFileMgr tmp_file_mgr;
  RemoveAndCreateDirs(tmp_dirs);
  tmp_dirs.push_back(REMOTE_URL);
  int64_t alloc_size = 1024;
  int64_t file_size = 1024;
  FLAGS_remote_tmp_file_size = "1K";

  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, true, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group_1(&tmp_file_mgr, io_mgr(), profile_, id);
  TmpFileGroup file_group_2(&tmp_file_mgr, io_mgr(), profile_, id);

  IntGauge* dir_usage = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.scratch-space-bytes-used.dir-0");
  IntGauge* local_buff_usage = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.local-buff-bytes-used.dir-0");

  int64_t offset;
  TmpFile* alloc_file;

  ASSERT_OK(GroupAllocateSpace(&file_group_1, alloc_size, &alloc_file, &offset));
  EXPECT_EQ(file_size, dir_usage->GetValue());
  EXPECT_EQ(file_size, local_buff_usage->GetValue());

  // This time we should hit the limit on the first directory. Do this from a
  // different file group to show that limits are enforced across file groups.
  ASSERT_OK(GroupAllocateSpace(&file_group_2, alloc_size, &alloc_file, &offset));
  EXPECT_EQ(2 * file_size, dir_usage->GetValue());
  EXPECT_EQ(2 * file_size, local_buff_usage->GetValue());

  file_group_2.Close();
  // Metrics should be decremented when the file groups delete the underlying files.
  EXPECT_EQ(file_size, dir_usage->GetValue());
  EXPECT_EQ(file_size, local_buff_usage->GetValue());

  // We should be able to reuse the space freed up.
  ASSERT_OK(GroupAllocateSpace(&file_group_1, alloc_size, &alloc_file, &offset));
  EXPECT_EQ(2 * file_size, dir_usage->GetValue());
  EXPECT_EQ(2 * file_size, local_buff_usage->GetValue());

  file_group_1.Close();
  EXPECT_EQ(0, dir_usage->GetValue());
  EXPECT_EQ(0, local_buff_usage->GetValue());
}

/// Test when both remote and local dir exist, if the directory limits
/// work.
TEST_F(TmpFileMgrTest, TestMixDirectoryLimits) {
  vector<string> tmp_create_dirs{{LOCAL_BUFFER_PATH, "/tmp/tmp-file-mgr-test-local"}};
  vector<string> tmp_dirs{{LOCAL_BUFFER_PATH, "/tmp/tmp-file-mgr-test-local:1024"}};
  TmpFileMgr tmp_file_mgr;
  RemoveAndCreateDirs(tmp_create_dirs);
  tmp_dirs.push_back(REMOTE_URL);
  int64_t alloc_size = 1024;
  int64_t file_size = 1024;
  FLAGS_remote_tmp_file_size = "1K";

  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group_1(&tmp_file_mgr, io_mgr(), profile_, id);
  TmpFileGroup file_group_2(&tmp_file_mgr, io_mgr(), profile_, id);

  IntGauge* dir_usage_local = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.scratch-space-bytes-used.dir-0");
  IntGauge* dir_usage_remote = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.scratch-space-bytes-used.dir-1");
  IntGauge* local_buff_usage = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.local-buff-bytes-used.dir-0");

  ASSERT_TRUE(dir_usage_remote != nullptr);
  ASSERT_TRUE(dir_usage_local != nullptr);
  ASSERT_TRUE(local_buff_usage != nullptr);

  int64_t offset;
  TmpFile* alloc_file;

  // Allocate space from local.
  ASSERT_OK(GroupAllocateSpace(&file_group_1, alloc_size, &alloc_file, &offset));
  EXPECT_EQ(file_size, dir_usage_local->GetValue());
  EXPECT_EQ(0, dir_usage_remote->GetValue());
  EXPECT_EQ(0, local_buff_usage->GetValue());

  // This time we should hit the limit on the local directory. Should allocate
  // from the remote directory.
  ASSERT_OK(GroupAllocateSpace(&file_group_2, alloc_size, &alloc_file, &offset));
  EXPECT_EQ(file_size, dir_usage_local->GetValue());
  EXPECT_EQ(file_size, dir_usage_remote->GetValue());
  EXPECT_EQ(file_size, local_buff_usage->GetValue());

  file_group_2.Close();
  // Metrics of the remote directory should be decremented when the
  // second file groups delete the underlying files.
  EXPECT_EQ(file_size, dir_usage_local->GetValue());
  EXPECT_EQ(0, dir_usage_remote->GetValue());
  EXPECT_EQ(0, local_buff_usage->GetValue());

  // Should allocate from the remote directory again.
  ASSERT_OK(GroupAllocateSpace(&file_group_1, alloc_size, &alloc_file, &offset));
  EXPECT_EQ(file_size, dir_usage_local->GetValue());
  EXPECT_EQ(file_size, dir_usage_remote->GetValue());
  EXPECT_EQ(file_size, local_buff_usage->GetValue());

  file_group_1.Close();
  EXPECT_EQ(0, dir_usage_local->GetValue());
  EXPECT_EQ(0, dir_usage_remote->GetValue());
  EXPECT_EQ(0, local_buff_usage->GetValue());
}

/// Config a value bigger than the max remote file size allowed, should be
/// adjusted to the max size.
TEST_F(TmpFileMgrTest, TestMixTmpFileLimits) {
  vector<string> tmp_create_dirs{{LOCAL_BUFFER_PATH}};
  vector<string> tmp_dirs{{LOCAL_BUFFER_PATH}};
  TmpFileMgr tmp_file_mgr;
  RemoveAndCreateDirs(tmp_create_dirs);
  tmp_dirs.push_back(REMOTE_URL);
  int64_t alloc_size = 1024;
  int64_t file_size = 256 * 1024 * 1024;
  int64_t offset;
  TmpFile* alloc_file;
  FLAGS_remote_tmp_file_size = "512MB";

  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);
  EXPECT_EQ(tmp_file_mgr.GetRemoteTmpFileSize(), file_size);
  IntGauge* dir_usage_local = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.scratch-space-bytes-used.dir-0");
  ASSERT_TRUE(dir_usage_local != nullptr);
  // Allocate space from local.
  ASSERT_OK(GroupAllocateSpace(&file_group, alloc_size, &alloc_file, &offset));
  EXPECT_EQ(file_size, dir_usage_local->GetValue());
  file_group.Close();
}

void TmpFileMgrTest::TestTmpFileBufferPoolHelper(TmpFileMgr& tmp_file_mgr,
    unique_ptr<TmpFileGroup>* tmp_file_group,
    boost::scoped_ptr<io::WriteRange>& write_range,
    WriteRange::WriteDoneCallback& callback) {
  TUniqueId id;
  vector<string> tmp_create_dirs{{LOCAL_BUFFER_PATH}};
  vector<string> tmp_dirs{{Substitute(LOCAL_BUFFER_PATH + ":$0", 2048)}};
  RemoveAndCreateDirs(tmp_create_dirs);
  tmp_dirs.push_back(REMOTE_URL);
  int64_t alloc_size = 1024;
  int64_t file_size = 1024;
  FLAGS_remote_tmp_file_size = "1KB";
  int64_t offset;
  TmpFile *file1, *file2, *file3;
  cb_counter_ = 0;

  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, false, "", false, metrics_.get()));
  unique_ptr<TmpFileGroup> unique_file_group =
      make_unique<TmpFileGroup>(&tmp_file_mgr, io_mgr(), profile_, id);
  auto file_group = unique_file_group.get();
  EXPECT_EQ(tmp_file_mgr.GetRemoteTmpFileSize(), file_size);
  IntGauge* dir_usage_local = metrics_->FindMetricForTesting<IntGauge>(
      "tmp-file-mgr.scratch-space-bytes-used.dir-0");
  ASSERT_TRUE(dir_usage_local != nullptr);
  ASSERT_OK(GroupAllocateSpace(file_group, alloc_size, &file1, &offset));
  EXPECT_EQ(file_size, dir_usage_local->GetValue());
  ASSERT_OK(GroupAllocateSpace(file_group, alloc_size, &file2, &offset));
  EXPECT_NE(file1, file2);
  EXPECT_TRUE(file1->GetWriteFile()->IsSpaceReserved());
  EXPECT_TRUE(file2->GetWriteFile()->IsSpaceReserved());
  ASSERT_OK(GroupAllocateSpace(file_group, alloc_size, &file3, &offset));
  EXPECT_NE(file2, file3);

  write_range.reset(new WriteRange(
      file3->path(), offset, file3->AssignDiskQueue(!file3->is_local()), callback));
  const char* dummy_data = "dummy data";
  write_range->SetData((const uint8_t*)dummy_data, strlen(dummy_data));
  write_range->SetDiskFile(file3->GetWriteFile());
  SetWriteRangeContext(write_range.get(), file_group);
  // The space hasn't been reserved for file3 because the pool size is 2K, and there are
  // two files in use, we use the AsyncWriteRange() function to wait until a file is
  // available to be evicted, then finish the writing of file3.
  EXPECT_FALSE(file3->GetWriteFile()->IsSpaceReserved());
  ASSERT_OK(tmp_file_mgr.AsyncWriteRange(write_range.get(), file3));
  *tmp_file_group = move(unique_file_group);
}

void TmpFileMgrTest::TestTmpFileBufferPoolTearDown(TmpFileMgr& tmp_file_mgr) {
  auto tmp_file_pool = tmp_file_mgr.tmp_dirs_remote_ctrl_.tmp_file_pool_.get();
  ASSERT_TRUE(tmp_file_pool->io_ctx_to_file_set_map_.empty());
  ASSERT_TRUE(tmp_file_pool->write_ranges_to_add_.empty());
  ASSERT_TRUE(tmp_file_pool->write_ranges_.empty());
  ASSERT_TRUE(tmp_file_pool->write_ranges_iterator_.empty());
}

// Test one write is waiting in the pool for the reservation, but file group is closed
// before the reservation is done. The write's callback function should be called.
TEST_F(TmpFileMgrTest, TestTmpFileBufferPoolOneWriteCancel) {
  TmpFileMgr tmp_file_mgr;
  unique_ptr<TmpFileGroup> file_group;
  boost::scoped_ptr<io::WriteRange> write_range;
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
  TestTmpFileBufferPoolHelper(tmp_file_mgr, &file_group, write_range, callback);
  file_group->Close();
  WaitForCallbacks(1);
  TestTmpFileBufferPoolTearDown(tmp_file_mgr);
}

// Test two writes are waiting in the pool for the reservation, but file group is closed
// before the reservation is done. The writes' callback functions should be called.
TEST_F(TmpFileMgrTest, TestTmpFileBufferPoolTwoWritesCancel) {
  TmpFileMgr tmp_file_mgr;
  unique_ptr<TmpFileGroup> file_group;
  boost::scoped_ptr<io::WriteRange> write_range;
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
  TestTmpFileBufferPoolHelper(tmp_file_mgr, &file_group, write_range, callback);
  TmpFile* file4;
  int64_t alloc_size = 1024;
  int64_t offset;
  TmpFileGroup* tmp_file_group = file_group.get();
  ASSERT_OK(GroupAllocateSpace(tmp_file_group, alloc_size, &file4, &offset));
  boost::scoped_ptr<io::WriteRange> write_range2;
  write_range2.reset(new WriteRange(
      file4->path(), offset, file4->AssignDiskQueue(!file4->is_local()), callback));
  const char* dummy_data = "dummy data";
  write_range2->SetData((const uint8_t*)dummy_data, strlen(dummy_data));
  write_range2->SetDiskFile(file4->GetWriteFile());
  SetWriteRangeContext(write_range2.get(), tmp_file_group);
  EXPECT_FALSE(file4->GetWriteFile()->IsSpaceReserved());
  ASSERT_OK(tmp_file_mgr.AsyncWriteRange(write_range2.get(), file4));
  file_group->Close();
  WaitForCallbacks(2);
  TestTmpFileBufferPoolTearDown(tmp_file_mgr);
}

// Test one write is waiting in the pool for the reservation, when a file is returned to
// the buffer pool, the reservation should be done.
TEST_F(TmpFileMgrTest, TestTmpFileBufferPoolOneWriteDone) {
  TmpFileMgr tmp_file_mgr;
  unique_ptr<TmpFileGroup> file_group;
  boost::scoped_ptr<io::WriteRange> write_range;
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&TmpFileMgrTest::SignalCallback), this, _1);
  TestTmpFileBufferPoolHelper(tmp_file_mgr, &file_group, write_range, callback);
  // Return a dummy file to the pool.
  tmp_file_mgr.EnqueueTmpFilesPoolDummyFile();
  WaitForCallbacks(1);
  file_group->Close();
  TestTmpFileBufferPoolTearDown(tmp_file_mgr);
}

/// Test setting a remote fs for the default fs, but should not affect the spilling.
TEST_F(TmpFileMgrTest, TestSpillingWithRemoteDefaultFS) {
  vector<string> tmp_dirs({"/tmp/tmp-file-mgr-test.1"});
  TmpFileMgr tmp_file_mgr;
  RemoveAndCreateDirs(tmp_dirs);
  string org_default_fs = test_env_->exec_env()->default_fs();
  string fake_remote_default_fs = "s3a://fake_s3";
  test_env_->SetDefaultFS(fake_remote_default_fs);

  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, true, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);
  string data = "arbitrary data";
  MemRange data_mem_range(reinterpret_cast<uint8_t*>(&data[0]), data.size());

  unique_ptr<TmpWriteHandle> handle;
  WriteRange::WriteDoneCallback callback = [this](const Status& status) {
    EXPECT_TRUE(status.ok());
    SignalCallback(status);
  };
  ASSERT_OK(file_group.Write(data_mem_range, callback, &handle));
  WaitForWrite(handle.get());
  WaitForCallbacks(1);
  file_group.Close();
  test_env_->SetDefaultFS(org_default_fs);
  test_env_->TearDownQueries();
}

/// Test writing a single record to a remote dir with local buffer, create an IO error
/// to trigger writing failed.
TEST_F(TmpFileMgrTest, TestRemoteRemoveBuffer) {
  vector<string> tmp_dirs({LOCAL_BUFFER_PATH});
  TmpFileMgr tmp_file_mgr;
  RemoveAndCreateDirs(tmp_dirs);
  tmp_dirs.push_back(REMOTE_URL);

  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, true, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);
  string data = "arbitrary data";
  MemRange data_mem_range(reinterpret_cast<uint8_t*>(&data[0]), data.size());

  unique_ptr<TmpWriteHandle> handle;
  WriteRange::WriteDoneCallback callback = [this](const Status& status) {
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsDiskIoError());
    SignalCallback(status);
  };
  // Remove the buffer before the write to create an IO Error.
  vector<string> buffer_dirs({LOCAL_BUFFER_PATH});
  RemoveDirs(buffer_dirs);
  ASSERT_OK(file_group.Write(data_mem_range, callback, &handle));
  WaitForWrite(handle.get());
  WaitForCallbacks(1);
  file_group.Close();
  test_env_->TearDownQueries();
}

/// Test config a non-existent remote path.
TEST_F(TmpFileMgrTest, TestRemoteUploadToNonExistentPath) {
  TmpFileMgr tmp_file_mgr;
  vector<string> tmp_dirs{{LOCAL_BUFFER_PATH}};
  RemoveAndCreateDirs(tmp_dirs);
  // Create a non-existent remote path.
  string random_name = lexical_cast<string>(random_generator()());
  string remote_file_path_to_crt = "s3a://non-existent_" + random_name + "/tmp/test";
  tmp_dirs.push_back(remote_file_path_to_crt);
  // Failed to initialize because unable to get a connection to the non-existent path
  // during verification.
  EXPECT_FALSE(tmp_file_mgr.InitCustom(tmp_dirs, true, "", false, metrics_.get()).ok());
  test_env_->TearDownQueries();
}

/// Test upload failure and lead to query cancelled due to timeout.
TEST_F(TmpFileMgrTest, TestRemoteUploadFailed) {
  TmpFileMgr tmp_file_mgr;
  int64_t buffer_limit = 2048;
  int64_t alloc_size = 512;
  // The file size should be aligned with the FLAGS_remote_tmp_file_size.
  int64_t file_size = 1024;
  int64_t per_file_page_num = file_size / alloc_size;
  // Set the timeout value to the minimum, 1 second. Because otherwise, the testcase
  // needs to wait for the default timeout interval, which is much larger than 1, before
  // the testcase can end and exit.
  FLAGS_wait_for_spill_buffer_timeout_s = 1;
  // The file size is the same as file_size, so each write uses one file.
  FLAGS_remote_tmp_file_size = "1KB";
  vector<string> tmp_create_dirs{{LOCAL_BUFFER_PATH}};
  vector<string> tmp_dirs{{Substitute(LOCAL_BUFFER_PATH + ":$0", buffer_limit)}};
  RemoveAndCreateDirs(tmp_create_dirs);
  tmp_dirs.push_back(HDFS_LOCAL_URL);

  ASSERT_OK(tmp_file_mgr.InitCustom(tmp_dirs, true, "", false, metrics_.get()));
  TUniqueId id;
  TmpFileGroup file_group(&tmp_file_mgr, io_mgr(), profile_, id);
  uint8_t* data = (uint8_t*)malloc(alloc_size);
  ASSERT_TRUE(data != nullptr);
  MemRange data_mem_range(data, alloc_size);

  int write_times = buffer_limit / alloc_size;

  // We set invalid remote paths to the tmp files, so that the upload of
  // the tmp files would fail. Because all of the uploads fail, the query
  // should be cancelled in the end due to timeout (the local buffer is full,
  // the later file waits for the buffer till timeout happens).
  for (int i = 0; i < write_times + 1; i++) {
    // Set the remote paths of current TmpFile to invalid paths to create upload failures.
    if (i != write_times && ((i + 1) % per_file_page_num == 0)) {
      int cur_tmp_file_idx = i / per_file_page_num;
      SetInvalidRemotePath(&tmp_file_mgr, &file_group, cur_tmp_file_idx);
    }
    unique_ptr<TmpWriteHandle> handle;
    WriteRange::WriteDoneCallback callback = [this, i, write_times](
                                                 const Status& status) {
      // The first several pages have been successfully written because the local
      // buffer isn't full. However, because all of the uploads fail, the existing
      // local buffer can't be evicted and would be full eventually. In the end, the
      // query is cancelled because of the timeout.
      if (i < write_times) {
        EXPECT_OK(status);
      } else {
        EXPECT_EQ(status.code(), TErrorCode::CANCELLED_INTERNALLY);
      }
      SignalCallback(status);
    };
    EXPECT_OK(file_group.Write(data_mem_range, callback, &handle));
    WaitForWrite(handle.get());
    WaitForCallbacks(1);
  }

  file_group.Close();
  test_env_->TearDownQueries();
}

} // namespace impala
