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

#include <sched.h>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <sys/stat.h>

#include "testutil/gtest-util.h"
#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/disk-io-mgr-stress.h"
#include "runtime/mem-tracker.h"
#include "runtime/thread-resource-mgr.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/thread.h"

#include "common/names.h"

DECLARE_int32(num_remote_hdfs_io_threads);
DECLARE_int32(num_s3_io_threads);
DECLARE_int32(num_adls_io_threads);

using boost::condition_variable;

const int MIN_BUFFER_SIZE = 512;
const int MAX_BUFFER_SIZE = 1024;
const int LARGE_MEM_LIMIT = 1024 * 1024 * 1024;

namespace impala {

class DiskIoMgrTest : public testing::Test {
 public:

  virtual void SetUp() {}

  virtual void TearDown() {
    pool_.Clear();
  }
  void WriteValidateCallback(int num_writes, DiskIoMgr::WriteRange** written_range,
      DiskIoMgr* io_mgr, DiskIoRequestContext* reader, int32_t* data,
      Status expected_status, const Status& status) {
    if (expected_status.code() == TErrorCode::CANCELLED) {
      EXPECT_TRUE(status.ok() || status.IsCancelled()) << "Error: " << status.GetDetail();
    } else {
      EXPECT_EQ(status.code(), expected_status.code());
    }
    if (status.ok()) {
      DiskIoMgr::ScanRange* scan_range = pool_.Add(new DiskIoMgr::ScanRange());
      scan_range->Reset(NULL, (*written_range)->file(), (*written_range)->len(),
          (*written_range)->offset(), 0, false, DiskIoMgr::BufferOpts::Uncached());
      ValidateSyncRead(io_mgr, reader, scan_range, reinterpret_cast<const char*>(data),
          sizeof(int32_t));
    }

    {
      lock_guard<mutex> l(written_mutex_);
      ++num_ranges_written_;
      if (num_ranges_written_ == num_writes) writes_done_.notify_one();
    }
  }

  void WriteCompleteCallback(int num_writes, const Status& status) {
    EXPECT_OK(status);
    {
      lock_guard<mutex> l(written_mutex_);
      ++num_ranges_written_;
      if (num_ranges_written_ == num_writes) writes_done_.notify_all();
    }
  }

 protected:
  void CreateTempFile(const char* filename, const char* data) {
    FILE* file = fopen(filename, "w");
    EXPECT_TRUE(file != NULL);
    fwrite(data, 1, strlen(data), file);
    fclose(file);
  }

  int CreateTempFile(const char* filename, int file_size) {
    FILE* file = fopen(filename, "w");
    EXPECT_TRUE(file != NULL);
    int success = fclose(file);
    if (success != 0) {
      LOG(ERROR) << "Error closing file " << filename;
      return success;
    }
    return truncate(filename, file_size);
  }

  // Validates that buffer[i] is \0 or expected[i]
  static void ValidateEmptyOrCorrect(const char* expected, const char* buffer, int len) {
    for (int i = 0; i < len; ++i) {
      if (buffer[i] != '\0') {
        EXPECT_EQ(expected[i], buffer[i]) << (int)expected[i] << " != " << (int)buffer[i];
      }
    }
  }

  static void ValidateSyncRead(DiskIoMgr* io_mgr, DiskIoRequestContext* reader,
      DiskIoMgr::ScanRange* range, const char* expected, int expected_len = -1) {
    unique_ptr<DiskIoMgr::BufferDescriptor> buffer;
    ASSERT_OK(io_mgr->Read(reader, range, &buffer));
    ASSERT_TRUE(buffer != NULL);
    EXPECT_EQ(buffer->len(), range->len());
    if (expected_len < 0) expected_len = strlen(expected);
    int cmp = memcmp(buffer->buffer(), expected, expected_len);
    EXPECT_TRUE(cmp == 0);
    io_mgr->ReturnBuffer(move(buffer));
  }

  static void ValidateScanRange(DiskIoMgr* io_mgr, DiskIoMgr::ScanRange* range,
      const char* expected, int expected_len, const Status& expected_status) {
    char result[expected_len + 1];
    memset(result, 0, expected_len + 1);

    while (true) {
      unique_ptr<DiskIoMgr::BufferDescriptor> buffer;
      Status status = range->GetNext(&buffer);
      ASSERT_TRUE(status.ok() || status.code() == expected_status.code());
      if (buffer == NULL || !status.ok()) {
        if (buffer != NULL) io_mgr->ReturnBuffer(move(buffer));
        break;
      }
      ASSERT_LE(buffer->len(), expected_len);
      memcpy(result + range->offset() + buffer->scan_range_offset(),
          buffer->buffer(), buffer->len());
      io_mgr->ReturnBuffer(move(buffer));
    }
    ValidateEmptyOrCorrect(expected, result, expected_len);
  }

  // Continues pulling scan ranges from the io mgr until they are all done.
  // Updates num_ranges_processed with the number of ranges seen by this thread.
  static void ScanRangeThread(DiskIoMgr* io_mgr, DiskIoRequestContext* reader,
      const char* expected_result, int expected_len, const Status& expected_status,
      int max_ranges, AtomicInt32* num_ranges_processed) {
    int num_ranges = 0;
    while (max_ranges == 0 || num_ranges < max_ranges) {
      DiskIoMgr::ScanRange* range;
      Status status = io_mgr->GetNextRange(reader, &range);
      ASSERT_TRUE(status.ok() || status.code() == expected_status.code());
      if (range == NULL) break;
      ValidateScanRange(io_mgr, range, expected_result, expected_len, expected_status);
      num_ranges_processed->Add(1);
      ++num_ranges;
    }
  }

  DiskIoMgr::ScanRange* AllocateRange() {
    return pool_.Add(new DiskIoMgr::ScanRange);
  }

  DiskIoMgr::ScanRange* InitRange(const char* file_path, int offset, int len,
      int disk_id, int64_t mtime, void* meta_data = NULL, bool is_cached = false) {
    DiskIoMgr::ScanRange* range = AllocateRange();
    range->Reset(NULL, file_path, len, offset, disk_id, true,
        DiskIoMgr::BufferOpts(is_cached, mtime), meta_data);
    EXPECT_EQ(mtime, range->mtime());
    return range;
  }

  ObjectPool pool_;

  mutex written_mutex_;
  condition_variable writes_done_;
  int num_ranges_written_;
};

// Test a single writer with multiple disks and threads per disk. Each WriteRange
// writes random 4-byte integers, and upon completion, the written data is validated
// by reading the data back via a separate IoMgr instance. All writes are expected to
// complete successfully.
TEST_F(DiskIoMgrTest, SingleWriter) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  num_ranges_written_ = 0;
  string tmp_file = "/tmp/disk_io_mgr_test.txt";
  int num_ranges = 100;
  int64_t file_size = 1024 * 1024;
  int64_t cur_offset = 0;
  int success = CreateTempFile(tmp_file.c_str(), file_size);
  if (success != 0) {
    LOG(ERROR) << "Error creating temp file " << tmp_file.c_str() << " of size " <<
        file_size;
    EXPECT_TRUE(false);
  }

  scoped_ptr<DiskIoMgr> read_io_mgr(new DiskIoMgr(1, 1, 1, 1, 10));
  MemTracker reader_mem_tracker(LARGE_MEM_LIMIT);
  ASSERT_OK(read_io_mgr->Init(&reader_mem_tracker));
  DiskIoRequestContext* reader;
  read_io_mgr->RegisterContext(&reader, &reader_mem_tracker);
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      pool_.Clear(); // Destroy scan ranges from previous iterations.
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 10);
      ASSERT_OK(io_mgr.Init(&mem_tracker));
      DiskIoRequestContext* writer;
      io_mgr.RegisterContext(&writer, &mem_tracker);
      for (int i = 0; i < num_ranges; ++i) {
        int32_t* data = pool_.Add(new int32_t);
        *data = rand();
        DiskIoMgr::WriteRange** new_range = pool_.Add(new DiskIoMgr::WriteRange*);
        DiskIoMgr::WriteRange::WriteDoneCallback callback =
            bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this, num_ranges,
                new_range, read_io_mgr.get(), reader, data, Status::OK(), _1);
        *new_range = pool_.Add(new DiskIoMgr::WriteRange(tmp_file, cur_offset,
            num_ranges % num_disks, callback));
        (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
        EXPECT_OK(io_mgr.AddWriteRange(writer, *new_range));
        cur_offset += sizeof(int32_t);
      }

      {
        unique_lock<mutex> lock(written_mutex_);
        while (num_ranges_written_ < num_ranges) writes_done_.wait(lock);
      }
      num_ranges_written_ = 0;
      io_mgr.UnregisterContext(writer);
    }
  }

  read_io_mgr->UnregisterContext(reader);
  read_io_mgr.reset();
}

// Perform invalid writes (e.g. file in non-existent directory, negative offset) and
// validate that an error status is returned via the write callback.
TEST_F(DiskIoMgrTest, InvalidWrite) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  num_ranges_written_ = 0;
  string tmp_file = "/non-existent/file.txt";
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init(&mem_tracker));
  DiskIoRequestContext* writer;
  io_mgr.RegisterContext(&writer, NULL);
  int32_t* data = pool_.Add(new int32_t);
  *data = rand();

  // Write to file in non-existent directory.
  DiskIoMgr::WriteRange** new_range = pool_.Add(new DiskIoMgr::WriteRange*);
  DiskIoMgr::WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this, 2, new_range,
          (DiskIoMgr*)NULL, (DiskIoRequestContext*)NULL, data,
          Status(TErrorCode::DISK_IO_ERROR, "Test Failure"), _1);
  *new_range = pool_.Add(new DiskIoMgr::WriteRange(tmp_file, rand(), 0, callback));

  (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
  EXPECT_OK(io_mgr.AddWriteRange(writer, *new_range));

  // Write to a bad location in a file that exists.
  tmp_file = "/tmp/disk_io_mgr_test.txt";
  int success = CreateTempFile(tmp_file.c_str(), 100);
  if (success != 0) {
    LOG(ERROR) << "Error creating temp file " << tmp_file.c_str() << " of size 100";
    EXPECT_TRUE(false);
  }

  new_range = pool_.Add(new DiskIoMgr::WriteRange*);
  callback = bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this, 2,
      new_range, (DiskIoMgr*)NULL, (DiskIoRequestContext*)NULL,
      data, Status(TErrorCode::DISK_IO_ERROR, "Test Failure"), _1);

  *new_range = pool_.Add(new DiskIoMgr::WriteRange(tmp_file, -1, 0, callback));
  (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
  EXPECT_OK(io_mgr.AddWriteRange(writer, *new_range));

  {
    unique_lock<mutex> lock(written_mutex_);
    while (num_ranges_written_ < 2) writes_done_.wait(lock);
  }
  num_ranges_written_ = 0;
  io_mgr.UnregisterContext(writer);
}

// Issue a number of writes, cancel the writer context and issue more writes.
// AddWriteRange() is expected to succeed before the cancel and fail after it.
// The writes themselves may finish with status cancelled or ok.
TEST_F(DiskIoMgrTest, SingleWriterCancel) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  num_ranges_written_ = 0;
  string tmp_file = "/tmp/disk_io_mgr_test.txt";
  int num_ranges = 100;
  int num_ranges_before_cancel = 25;
  int64_t file_size = 1024 * 1024;
  int64_t cur_offset = 0;
  int success = CreateTempFile(tmp_file.c_str(), file_size);
  if (success != 0) {
    LOG(ERROR) << "Error creating temp file " << tmp_file.c_str() << " of size " <<
        file_size;
    EXPECT_TRUE(false);
  }

  scoped_ptr<DiskIoMgr> read_io_mgr(new DiskIoMgr(1, 1, 1, 1, 10));
  MemTracker reader_mem_tracker(LARGE_MEM_LIMIT);
  ASSERT_OK(read_io_mgr->Init(&reader_mem_tracker));
  DiskIoRequestContext* reader;
  read_io_mgr->RegisterContext(&reader, &reader_mem_tracker);
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      pool_.Clear(); // Destroy scan ranges from previous iterations.
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 10);
      ASSERT_OK(io_mgr.Init(&mem_tracker));
      DiskIoRequestContext* writer;
      io_mgr.RegisterContext(&writer, &mem_tracker);
      Status validate_status = Status::OK();
      for (int i = 0; i < num_ranges; ++i) {
        if (i == num_ranges_before_cancel) {
          io_mgr.CancelContext(writer);
          validate_status = Status::CANCELLED;
        }
        int32_t* data = pool_.Add(new int32_t);
        *data = rand();
        DiskIoMgr::WriteRange** new_range = pool_.Add(new DiskIoMgr::WriteRange*);
        DiskIoMgr::WriteRange::WriteDoneCallback callback =
            bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this,
                num_ranges_before_cancel, new_range, read_io_mgr.get(), reader, data,
                Status::CANCELLED, _1);
        *new_range = pool_.Add(new DiskIoMgr::WriteRange(tmp_file, cur_offset,
            num_ranges % num_disks, callback));
        (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
        cur_offset += sizeof(int32_t);
        Status add_status = io_mgr.AddWriteRange(writer, *new_range);
        EXPECT_TRUE(add_status.code() == validate_status.code());
      }

      {
        unique_lock<mutex> lock(written_mutex_);
        while (num_ranges_written_ < num_ranges_before_cancel) writes_done_.wait(lock);
      }
      num_ranges_written_ = 0;
      io_mgr.UnregisterContext(writer);
    }
  }

  read_io_mgr->UnregisterContext(reader);
  read_io_mgr.reset();
}

// Basic test with a single reader, testing multiple threads, disks and a different
// number of buffers.
TEST_F(DiskIoMgrTest, SingleReader) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklm";
  int len = strlen(data);
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int num_read_threads = 1; num_read_threads <= 5; ++num_read_threads) {
        ObjectPool pool;
        LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                  << " num_disk=" << num_disks
                  << " num_read_threads=" << num_read_threads;

        if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
        DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 1);

        ASSERT_OK(io_mgr.Init(&mem_tracker));
        MemTracker reader_mem_tracker;
        DiskIoRequestContext* reader;
        io_mgr.RegisterContext(&reader, &reader_mem_tracker);

        vector<DiskIoMgr::ScanRange*> ranges;
        for (int i = 0; i < len; ++i) {
          int disk_id = i % num_disks;
          ranges.push_back(
              InitRange(tmp_file, 0, len, disk_id, stat_val.st_mtime));
        }
        ASSERT_OK(io_mgr.AddScanRanges(reader, ranges));

        AtomicInt32 num_ranges_processed;
        thread_group threads;
        for (int i = 0; i < num_read_threads; ++i) {
          threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader, data,
              len, Status::OK(), 0, &num_ranges_processed));
        }
        threads.join_all();

        EXPECT_EQ(num_ranges_processed.Load(), ranges.size());
        io_mgr.UnregisterContext(reader);
        EXPECT_EQ(reader_mem_tracker.consumption(), 0);
      }
    }
  }
  EXPECT_EQ(mem_tracker.consumption(), 0);
}

// This test issues adding additional scan ranges while there are some still in flight.
TEST_F(DiskIoMgrTest, AddScanRangeTest) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklm";
  int len = strlen(data);
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      pool_.Clear(); // Destroy scan ranges from previous iterations.
      LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                << " num_disk=" << num_disks;

      if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 1);

      ASSERT_OK(io_mgr.Init(&mem_tracker));
      MemTracker reader_mem_tracker;
      DiskIoRequestContext* reader;
      io_mgr.RegisterContext(&reader, &reader_mem_tracker);

      vector<DiskIoMgr::ScanRange*> ranges_first_half;
      vector<DiskIoMgr::ScanRange*> ranges_second_half;
      for (int i = 0; i < len; ++i) {
        int disk_id = i % num_disks;
        if (i > len / 2) {
          ranges_second_half.push_back(
              InitRange(tmp_file, i, 1, disk_id, stat_val.st_mtime));
        } else {
          ranges_first_half.push_back(
              InitRange(tmp_file, i, 1, disk_id, stat_val.st_mtime));
        }
      }
      AtomicInt32 num_ranges_processed;

      // Issue first half the scan ranges.
      ASSERT_OK(io_mgr.AddScanRanges(reader, ranges_first_half));

      // Read a couple of them
      ScanRangeThread(&io_mgr, reader, data, strlen(data), Status::OK(), 2,
          &num_ranges_processed);

      // Issue second half
      ASSERT_OK(io_mgr.AddScanRanges(reader, ranges_second_half));

      // Start up some threads and then cancel
      thread_group threads;
      for (int i = 0; i < 3; ++i) {
        threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader, data,
            strlen(data), Status::CANCELLED, 0, &num_ranges_processed));
      }

      threads.join_all();
      EXPECT_EQ(num_ranges_processed.Load(), len);
      io_mgr.UnregisterContext(reader);
      EXPECT_EQ(reader_mem_tracker.consumption(), 0);
    }
  }
  EXPECT_EQ(mem_tracker.consumption(), 0);
}

// Test to make sure that sync reads and async reads work together
// Note: this test is constructed so the number of buffers is greater than the
// number of scan ranges.
TEST_F(DiskIoMgrTest, SyncReadTest) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklm";
  int len = strlen(data);
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      pool_.Clear(); // Destroy scan ranges from previous iterations.
      LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                << " num_disk=" << num_disks;

      if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk,
          MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

      ASSERT_OK(io_mgr.Init(&mem_tracker));
      MemTracker reader_mem_tracker;
      DiskIoRequestContext* reader;
      io_mgr.RegisterContext(&reader, &reader_mem_tracker);

      DiskIoMgr::ScanRange* complete_range = InitRange(tmp_file, 0, strlen(data), 0,
          stat_val.st_mtime);

      // Issue some reads before the async ones are issued
      ValidateSyncRead(&io_mgr, reader, complete_range, data);
      ValidateSyncRead(&io_mgr, reader, complete_range, data);

      vector<DiskIoMgr::ScanRange*> ranges;
      for (int i = 0; i < len; ++i) {
        int disk_id = i % num_disks;
        ranges.push_back(InitRange(tmp_file, 0, len, disk_id,
            stat_val.st_mtime));
      }
      ASSERT_OK(io_mgr.AddScanRanges(reader, ranges));

      AtomicInt32 num_ranges_processed;
      thread_group threads;
      for (int i = 0; i < 5; ++i) {
        threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader, data,
            strlen(data), Status::OK(), 0, &num_ranges_processed));
      }

      // Issue some more sync ranges
      for (int i = 0; i < 5; ++i) {
        sched_yield();
        ValidateSyncRead(&io_mgr, reader, complete_range, data);
      }

      threads.join_all();

      ValidateSyncRead(&io_mgr, reader, complete_range, data);
      ValidateSyncRead(&io_mgr, reader, complete_range, data);

      EXPECT_EQ(num_ranges_processed.Load(), ranges.size());
      io_mgr.UnregisterContext(reader);
      EXPECT_EQ(reader_mem_tracker.consumption(), 0);
    }
  }
  EXPECT_EQ(mem_tracker.consumption(), 0);
}

// Tests a single reader cancelling half way through scan ranges.
TEST_F(DiskIoMgrTest, SingleReaderCancel) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklm";
  int len = strlen(data);
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      pool_.Clear(); // Destroy scan ranges from previous iterations.
      LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                << " num_disk=" << num_disks;

      if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 1);

      ASSERT_OK(io_mgr.Init(&mem_tracker));
      MemTracker reader_mem_tracker;
      DiskIoRequestContext* reader;
      io_mgr.RegisterContext(&reader, &reader_mem_tracker);

      vector<DiskIoMgr::ScanRange*> ranges;
      for (int i = 0; i < len; ++i) {
        int disk_id = i % num_disks;
        ranges.push_back(InitRange(tmp_file, 0, len, disk_id, stat_val.st_mtime));
      }
      ASSERT_OK(io_mgr.AddScanRanges(reader, ranges));

      AtomicInt32 num_ranges_processed;
      int num_succesful_ranges = ranges.size() / 2;
      // Read half the ranges
      for (int i = 0; i < num_succesful_ranges; ++i) {
        ScanRangeThread(&io_mgr, reader, data, strlen(data), Status::OK(), 1,
            &num_ranges_processed);
      }
      EXPECT_EQ(num_ranges_processed.Load(), num_succesful_ranges);

      // Start up some threads and then cancel
      thread_group threads;
      for (int i = 0; i < 3; ++i) {
        threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader, data,
            strlen(data), Status::CANCELLED, 0, &num_ranges_processed));
      }

      io_mgr.CancelContext(reader);
      sched_yield();

      threads.join_all();
      EXPECT_TRUE(io_mgr.context_status(reader).IsCancelled());
      io_mgr.UnregisterContext(reader);
      EXPECT_EQ(reader_mem_tracker.consumption(), 0);
    }
  }
  EXPECT_EQ(mem_tracker.consumption(), 0);
}

// Test when the reader goes over the mem limit
TEST_F(DiskIoMgrTest, MemLimits) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklm";
  int len = strlen(data);
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  const int mem_limit_num_buffers = 2;
  // Allocate enough ranges so that the total buffers exceeds the mem limit.
  const int num_ranges = 25;
  {
    MemTracker root_mem_tracker(mem_limit_num_buffers * MAX_BUFFER_SIZE);
    DiskIoMgr io_mgr(1, 1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

    ASSERT_OK(io_mgr.Init(&root_mem_tracker));
    MemTracker reader_mem_tracker(-1, "Reader", &root_mem_tracker);
    DiskIoRequestContext* reader;
    io_mgr.RegisterContext(&reader, &reader_mem_tracker);

    vector<DiskIoMgr::ScanRange*> ranges;
    for (int i = 0; i < num_ranges; ++i) {
      ranges.push_back(InitRange(tmp_file, 0, len, 0, stat_val.st_mtime));
    }
    ASSERT_OK(io_mgr.AddScanRanges(reader, ranges));

    // Don't return buffers to force memory pressure
    vector<unique_ptr<DiskIoMgr::BufferDescriptor>> buffers;

    AtomicInt32 num_ranges_processed;
    ScanRangeThread(&io_mgr, reader, data, strlen(data), Status::MemLimitExceeded(),
        1, &num_ranges_processed);

    char result[strlen(data) + 1];
    // Keep reading new ranges without returning buffers. This forces us
    // to go over the limit eventually.
    while (true) {
      memset(result, 0, strlen(data) + 1);
      DiskIoMgr::ScanRange* range = NULL;
      Status status = io_mgr.GetNextRange(reader, &range);
      ASSERT_TRUE(status.ok() || status.IsMemLimitExceeded());
      if (range == NULL) break;

      while (true) {
        unique_ptr<DiskIoMgr::BufferDescriptor> buffer;
        Status status = range->GetNext(&buffer);
        ASSERT_TRUE(status.ok() || status.IsMemLimitExceeded());
        if (buffer == NULL) break;
        memcpy(result + range->offset() + buffer->scan_range_offset(),
            buffer->buffer(), buffer->len());
        buffers.push_back(move(buffer));
      }
      ValidateEmptyOrCorrect(data, result, strlen(data));
    }

    for (int i = 0; i < buffers.size(); ++i) {
      io_mgr.ReturnBuffer(move(buffers[i]));
    }

    EXPECT_TRUE(io_mgr.context_status(reader).IsMemLimitExceeded());
    io_mgr.UnregisterContext(reader);
    EXPECT_EQ(reader_mem_tracker.consumption(), 0);
  }
}

// Test when some scan ranges are marked as being cached.
// Since these files are not in HDFS, the cached path always fails so this
// only tests the fallback mechanism.
// TODO: we can fake the cached read path without HDFS
TEST_F(DiskIoMgrTest, CachedReads) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklm";
  int len = strlen(data);
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  const int num_disks = 2;
  {
    DiskIoMgr io_mgr(num_disks, 1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

    ASSERT_OK(io_mgr.Init(&mem_tracker));
    MemTracker reader_mem_tracker;
    DiskIoRequestContext* reader;
    io_mgr.RegisterContext(&reader, &reader_mem_tracker);

    DiskIoMgr::ScanRange* complete_range =
        InitRange(tmp_file, 0, strlen(data), 0, stat_val.st_mtime, NULL, true);

    // Issue some reads before the async ones are issued
    ValidateSyncRead(&io_mgr, reader, complete_range, data);
    ValidateSyncRead(&io_mgr, reader, complete_range, data);

    vector<DiskIoMgr::ScanRange*> ranges;
    for (int i = 0; i < len; ++i) {
      int disk_id = i % num_disks;
      ranges.push_back(
          InitRange(tmp_file, 0, len, disk_id, stat_val.st_mtime, NULL, true));
    }
    ASSERT_OK(io_mgr.AddScanRanges(reader, ranges));

    AtomicInt32 num_ranges_processed;
    thread_group threads;
    for (int i = 0; i < 5; ++i) {
      threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader, data,
          strlen(data), Status::OK(), 0, &num_ranges_processed));
    }

    // Issue some more sync ranges
    for (int i = 0; i < 5; ++i) {
      sched_yield();
      ValidateSyncRead(&io_mgr, reader, complete_range, data);
    }

    threads.join_all();

    ValidateSyncRead(&io_mgr, reader, complete_range, data);
    ValidateSyncRead(&io_mgr, reader, complete_range, data);

    EXPECT_EQ(num_ranges_processed.Load(), ranges.size());
    io_mgr.UnregisterContext(reader);
    EXPECT_EQ(reader_mem_tracker.consumption(), 0);
  }
  EXPECT_EQ(mem_tracker.consumption(), 0);
}

TEST_F(DiskIoMgrTest, MultipleReaderWriter) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const int ITERATIONS = 1;
  const char* data = "abcdefghijklmnopqrstuvwxyz";
  const int num_contexts = 5;
  const int file_size = 4 * 1024;
  const int num_writes_queued = 5;
  const int num_reads_queued = 5;

  string file_name = "/tmp/disk_io_mgr_test.txt";
  int success = CreateTempFile(file_name.c_str(), file_size);
  if (success != 0) {
    LOG(ERROR) << "Error creating temp file " << file_name.c_str() << " of size " <<
        file_size;
    ASSERT_TRUE(false);
  }

  // Get mtime for file
  struct stat stat_val;
  stat(file_name.c_str(), &stat_val);

  int64_t iters = 0;
  vector<DiskIoRequestContext*> contexts(num_contexts);
  Status status;
  for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
    for (int threads_per_disk = 1; threads_per_disk <= 5; ++threads_per_disk) {
      for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
        DiskIoMgr io_mgr(num_disks, threads_per_disk, threads_per_disk, MIN_BUFFER_SIZE,
            MAX_BUFFER_SIZE);
        ASSERT_OK(io_mgr.Init(&mem_tracker));
        for (int file_index = 0; file_index < num_contexts; ++file_index) {
          io_mgr.RegisterContext(&contexts[file_index], &mem_tracker);
        }
        pool_.Clear();
        int read_offset = 0;
        int write_offset = 0;
        while (read_offset < file_size) {
          for (int context_index = 0; context_index < num_contexts; ++context_index) {
            if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
            AtomicInt32 num_ranges_processed;
            thread_group threads;
            vector<DiskIoMgr::ScanRange*> ranges;
            int num_scan_ranges = min<int>(num_reads_queued, write_offset - read_offset);
            for (int i = 0; i < num_scan_ranges; ++i) {
              ranges.push_back(InitRange(file_name.c_str(), read_offset, 1,
                  i % num_disks, stat_val.st_mtime));
              threads.add_thread(new thread(ScanRangeThread, &io_mgr,
                  contexts[context_index],
                  reinterpret_cast<const char*>(data + (read_offset % strlen(data))), 1,
                  Status::OK(), num_scan_ranges, &num_ranges_processed));
              ++read_offset;
            }

            num_ranges_written_ = 0;
            int num_write_ranges = min<int>(num_writes_queued, file_size - write_offset);
            for (int i = 0; i < num_write_ranges; ++i) {
              DiskIoMgr::WriteRange::WriteDoneCallback callback =
                  bind(mem_fn(&DiskIoMgrTest::WriteCompleteCallback),
                      this, num_write_ranges, _1);
              DiskIoMgr::WriteRange* new_range = pool_.Add(
                  new DiskIoMgr::WriteRange(file_name,
                      write_offset, i % num_disks, callback));
              new_range->SetData(reinterpret_cast<const uint8_t*>
                  (data + (write_offset % strlen(data))), 1);
              status = io_mgr.AddWriteRange(contexts[context_index], new_range);
              ++write_offset;
            }

            {
              unique_lock<mutex> lock(written_mutex_);
              while (num_ranges_written_ < num_write_ranges) writes_done_.wait(lock);
            }

            threads.join_all();
          } // for (int context_index
        } // while (read_offset < file_size)


        for (int file_index = 0; file_index < num_contexts; ++file_index) {
          io_mgr.UnregisterContext(contexts[file_index]);
        }
      } // for (int num_disks
    } // for (int threads_per_disk
  } // for (int iteration
}

// This test will test multiple concurrent reads each reading a different file.
TEST_F(DiskIoMgrTest, MultipleReader) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const int NUM_READERS = 5;
  const int DATA_LEN = 50;
  const int ITERATIONS = 25;
  const int NUM_THREADS_PER_READER = 3;

  vector<string> file_names;
  vector<int64_t> mtimes;
  vector<string> data;
  vector<DiskIoRequestContext*> readers;
  vector<char*> results;

  file_names.resize(NUM_READERS);
  readers.resize(NUM_READERS);
  mtimes.resize(NUM_READERS);
  data.resize(NUM_READERS);
  results.resize(NUM_READERS);

  // Initialize data for each reader.  The data will be
  // 'abcd...' for reader one, 'bcde...' for reader two (wrapping around at 'z')
  for (int i = 0; i < NUM_READERS; ++i) {
    char buf[DATA_LEN];
    for (int j = 0; j < DATA_LEN; ++j) {
      int c = (j + i) % 26;
      buf[j] = 'a' + c;
    }
    data[i] = string(buf, DATA_LEN);

    stringstream ss;
    ss << "/tmp/disk_io_mgr_test" << i << ".txt";
    file_names[i] = ss.str();
    CreateTempFile(ss.str().c_str(), data[i].c_str());

    // Get mtime for file
    struct stat stat_val;
    stat(file_names[i].c_str(), &stat_val);
    mtimes[i] = stat_val.st_mtime;

    results[i] = new char[DATA_LEN + 1];
    memset(results[i], 0, DATA_LEN + 1);
  }

  // This exercises concurrency, run the test multiple times
  int64_t iters = 0;
  for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
    for (int threads_per_disk = 1; threads_per_disk <= 5; ++threads_per_disk) {
      for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
        pool_.Clear(); // Destroy scan ranges from previous iterations.
        LOG(INFO) << "Starting test with num_threads_per_disk=" << threads_per_disk
                  << " num_disk=" << num_disks;
        if (++iters % 2500 == 0) LOG(ERROR) << "Starting iteration " << iters;

        DiskIoMgr io_mgr(num_disks, threads_per_disk, threads_per_disk, MIN_BUFFER_SIZE,
            MAX_BUFFER_SIZE);
        EXPECT_OK(io_mgr.Init(&mem_tracker));

        for (int i = 0; i < NUM_READERS; ++i) {
          io_mgr.RegisterContext(&readers[i], &mem_tracker);

          vector<DiskIoMgr::ScanRange*> ranges;
          for (int j = 0; j < DATA_LEN; ++j) {
            int disk_id = j % num_disks;
            ranges.push_back(
                InitRange(file_names[i].c_str(), j, 1, disk_id, mtimes[i]));
          }
          ASSERT_OK(io_mgr.AddScanRanges(readers[i], ranges));
        }

        AtomicInt32 num_ranges_processed;
        thread_group threads;
        for (int i = 0; i < NUM_READERS; ++i) {
          for (int j = 0; j < NUM_THREADS_PER_READER; ++j) {
            threads.add_thread(new thread(ScanRangeThread, &io_mgr, readers[i],
                data[i].c_str(), data[i].size(), Status::OK(), 0,
                &num_ranges_processed));
          }
        }
        threads.join_all();
        EXPECT_EQ(num_ranges_processed.Load(), DATA_LEN * NUM_READERS);
        for (int i = 0; i < NUM_READERS; ++i) {
          io_mgr.UnregisterContext(readers[i]);
        }
      }
    }
  }
  EXPECT_EQ(mem_tracker.consumption(), 0);
}

// Stress test for multiple clients with cancellation
// TODO: the stress app should be expanded to include sync reads and adding scan
// ranges in the middle.
TEST_F(DiskIoMgrTest, StressTest) {
  // Run the test with 5 disks, 5 threads per disk, 10 clients and with cancellation
  DiskIoMgrStress test(5, 5, 10, true);
  test.Run(2); // In seconds
}

TEST_F(DiskIoMgrTest, Buffers) {
  // Test default min/max buffer size
  int min_buffer_size = 1024;
  int max_buffer_size = 8 * 1024 * 1024; // 8 MB
  MemTracker root_mem_tracker(max_buffer_size * 2);

  DiskIoMgr io_mgr(1, 1, 1, min_buffer_size, max_buffer_size);
  ASSERT_OK(io_mgr.Init(&root_mem_tracker));
  ASSERT_EQ(root_mem_tracker.consumption(), 0);

  MemTracker reader_mem_tracker(-1, "Reader", &root_mem_tracker);
  DiskIoRequestContext* reader;
  io_mgr.RegisterContext(&reader, &reader_mem_tracker);

  DiskIoMgr::ScanRange* dummy_range = InitRange("dummy", 0, 0, 0, 0);

  // buffer length should be rounded up to min buffer size
  int64_t buffer_len = 1;
  unique_ptr<DiskIoMgr::BufferDescriptor> buffer_desc;
  buffer_desc = io_mgr.GetFreeBuffer(reader, dummy_range, buffer_len);
  EXPECT_TRUE(buffer_desc->buffer() != NULL);
  EXPECT_EQ(min_buffer_size, buffer_desc->buffer_len());
  EXPECT_EQ(1, io_mgr.num_allocated_buffers_.Load());
  io_mgr.FreeBufferMemory(buffer_desc.get());
  io_mgr.ReturnBuffer(move(buffer_desc));
  EXPECT_EQ(min_buffer_size, root_mem_tracker.consumption());

  // reuse buffer
  buffer_len = min_buffer_size;
  buffer_desc = io_mgr.GetFreeBuffer(reader, dummy_range, buffer_len);
  EXPECT_TRUE(buffer_desc->buffer() != NULL);
  EXPECT_EQ(min_buffer_size, buffer_desc->buffer_len());
  EXPECT_EQ(1, io_mgr.num_allocated_buffers_.Load());
  io_mgr.FreeBufferMemory(buffer_desc.get());
  io_mgr.ReturnBuffer(move(buffer_desc));
  EXPECT_EQ(min_buffer_size, root_mem_tracker.consumption());

  // bump up to next buffer size
  buffer_len = min_buffer_size + 1;
  buffer_desc = io_mgr.GetFreeBuffer(reader, dummy_range, buffer_len);
  EXPECT_TRUE(buffer_desc->buffer() != NULL);
  EXPECT_EQ(min_buffer_size * 2, buffer_desc->buffer_len());
  EXPECT_EQ(2, io_mgr.num_allocated_buffers_.Load());
  EXPECT_EQ(min_buffer_size * 3, root_mem_tracker.consumption());

  // gc unused buffer
  io_mgr.GcIoBuffers();
  EXPECT_EQ(1, io_mgr.num_allocated_buffers_.Load());
  EXPECT_EQ(min_buffer_size * 2, root_mem_tracker.consumption());

  io_mgr.FreeBufferMemory(buffer_desc.get());
  io_mgr.ReturnBuffer(move(buffer_desc));

  // max buffer size
  buffer_len = max_buffer_size;
  buffer_desc = io_mgr.GetFreeBuffer(reader, dummy_range, buffer_len);
  EXPECT_TRUE(buffer_desc->buffer() != NULL);
  EXPECT_EQ(max_buffer_size, buffer_desc->buffer_len());
  EXPECT_EQ(2, io_mgr.num_allocated_buffers_.Load());
  io_mgr.FreeBufferMemory(buffer_desc.get());
  io_mgr.ReturnBuffer(move(buffer_desc));
  EXPECT_EQ(min_buffer_size * 2 + max_buffer_size, root_mem_tracker.consumption());

  // gc buffers
  io_mgr.GcIoBuffers();
  EXPECT_EQ(io_mgr.num_allocated_buffers_.Load(), 0);
  EXPECT_EQ(root_mem_tracker.consumption(), 0);
  io_mgr.UnregisterContext(reader);
}

// IMPALA-2366: handle partial read where range goes past end of file.
TEST_F(DiskIoMgrTest, PartialRead) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  int len = strlen(data);
  int read_len = len + 1000; // Read past end of file.
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  scoped_ptr<DiskIoMgr> io_mgr(new DiskIoMgr(1, 1, 1, read_len, read_len));

  ASSERT_OK(io_mgr->Init(&mem_tracker));
  MemTracker reader_mem_tracker;
  DiskIoRequestContext* reader;
  io_mgr->RegisterContext(&reader, &reader_mem_tracker);

  // We should not read past the end of file.
  DiskIoMgr::ScanRange* range = InitRange(tmp_file, 0, read_len, 0, stat_val.st_mtime);
  unique_ptr<DiskIoMgr::BufferDescriptor> buffer;
  ASSERT_OK(io_mgr->Read(reader, range, &buffer));
  ASSERT_TRUE(buffer->eosr());
  ASSERT_EQ(len, buffer->len());
  ASSERT_TRUE(memcmp(buffer->buffer(), data, len) == 0);
  io_mgr->ReturnBuffer(move(buffer));

  io_mgr->UnregisterContext(reader);
  pool_.Clear();
  io_mgr.reset();
  EXPECT_EQ(reader_mem_tracker.consumption(), 0);
  EXPECT_EQ(mem_tracker.consumption(), 0);
}

// Test reading into a client-allocated buffer.
TEST_F(DiskIoMgrTest, ReadIntoClientBuffer) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  int len = strlen(data);
  int read_len = 4; // Make buffer size smaller than client-provided buffer.
  CreateTempFile(tmp_file, data);

  scoped_ptr<DiskIoMgr> io_mgr(new DiskIoMgr(1, 1, 1, read_len, read_len));

  ASSERT_OK(io_mgr->Init(&mem_tracker));
  // Reader doesn't need to provide mem tracker if it's providing buffers.
  MemTracker* reader_mem_tracker = NULL;
  DiskIoRequestContext* reader;
  io_mgr->RegisterContext(&reader, reader_mem_tracker);

  for (int buffer_len : vector<int>({len - 1, len, len + 1})) {
    vector<uint8_t> client_buffer(buffer_len);
    int scan_len = min(len, buffer_len);
    DiskIoMgr::ScanRange* range = AllocateRange();
    range->Reset(NULL, tmp_file, scan_len, 0, 0, true,
        DiskIoMgr::BufferOpts::ReadInto(client_buffer.data(), buffer_len));
    ASSERT_OK(io_mgr->AddScanRange(reader, range, true));

    unique_ptr<DiskIoMgr::BufferDescriptor> io_buffer;
    ASSERT_OK(range->GetNext(&io_buffer));
    ASSERT_TRUE(io_buffer->eosr());
    ASSERT_EQ(scan_len, io_buffer->len());
    ASSERT_EQ(client_buffer.data(), io_buffer->buffer());
    ASSERT_EQ(memcmp(io_buffer->buffer(), data, scan_len), 0);

    // DiskIoMgr should not have allocated memory.
    EXPECT_EQ(mem_tracker.consumption(), 0);
    io_mgr->ReturnBuffer(move(io_buffer));
  }

  io_mgr->UnregisterContext(reader);
  pool_.Clear();
  io_mgr.reset();
  EXPECT_EQ(mem_tracker.consumption(), 0);
}

// Test reading into a client-allocated buffer where the read fails.
TEST_F(DiskIoMgrTest, ReadIntoClientBufferError) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const char* tmp_file = "/file/that/does/not/exist";
  const int SCAN_LEN = 128;

  scoped_ptr<DiskIoMgr> io_mgr(new DiskIoMgr(1, 1, 1, SCAN_LEN, SCAN_LEN));

  ASSERT_OK(io_mgr->Init(&mem_tracker));
  // Reader doesn't need to provide mem tracker if it's providing buffers.
  MemTracker* reader_mem_tracker = NULL;
  DiskIoRequestContext* reader;
  vector<uint8_t> client_buffer(SCAN_LEN);
  for (int i = 0; i < 1000; ++i) {
    io_mgr->RegisterContext(&reader, reader_mem_tracker);
    DiskIoMgr::ScanRange* range = AllocateRange();
    range->Reset(NULL, tmp_file, SCAN_LEN, 0, 0, true,
        DiskIoMgr::BufferOpts::ReadInto(client_buffer.data(), SCAN_LEN));
    ASSERT_OK(io_mgr->AddScanRange(reader, range, true));

    /// Also test the cancellation path. Run multiple iterations since it is racy whether
    /// the read fails before the cancellation.
    if (i >= 1) io_mgr->CancelContext(reader);

    unique_ptr<DiskIoMgr::BufferDescriptor> io_buffer;
    ASSERT_FALSE(range->GetNext(&io_buffer).ok());

    // DiskIoMgr should not have allocated memory.
    EXPECT_EQ(mem_tracker.consumption(), 0);

    io_mgr->UnregisterContext(reader);
  }

  pool_.Clear();
  io_mgr.reset();
  EXPECT_EQ(mem_tracker.consumption(), 0);
}

// Test to verify configuration parameters for number of I/O threads per disk.
TEST_F(DiskIoMgrTest, VerifyNumThreadsParameter) {
  const int num_io_threads_for_remote_disks = FLAGS_num_remote_hdfs_io_threads
      + FLAGS_num_s3_io_threads + FLAGS_num_adls_io_threads;

  // Verify num_io_threads_per_rotational_disk and num_io_threads_per_solid_state_disk.
  // Since we do not have control over which disk is used, we check for either type
  // (rotational/solid state)
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const int num_io_threads_per_rotational_or_ssd = 2;
  DiskIoMgr io_mgr(1, num_io_threads_per_rotational_or_ssd,
      num_io_threads_per_rotational_or_ssd, 1, 10);
  ASSERT_OK(io_mgr.Init(&mem_tracker));
  const int num_io_threads = io_mgr.disk_thread_group_.Size();
  ASSERT_TRUE(num_io_threads ==
      num_io_threads_per_rotational_or_ssd + num_io_threads_for_remote_disks);
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  return RUN_ALL_TESTS();
}
