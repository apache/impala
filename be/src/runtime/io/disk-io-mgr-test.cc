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

#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/io/disk-io-mgr-stress.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/request-context.h"
#include "runtime/test-env.h"
#include "runtime/thread-resource-mgr.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"
#include "util/condition-variable.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/thread.h"

#include "common/names.h"

using std::mt19937;
using std::uniform_int_distribution;
using std::uniform_real_distribution;

DECLARE_int64(min_buffer_size);
DECLARE_int32(num_remote_hdfs_io_threads);
DECLARE_int32(num_s3_io_threads);
DECLARE_int32(num_adls_io_threads);

const int MIN_BUFFER_SIZE = 128;
const int MAX_BUFFER_SIZE = 1024;
const int64_t LARGE_RESERVATION_LIMIT = 4L * 1024L * 1024L * 1024L;
const int64_t LARGE_INITIAL_RESERVATION = 128L * 1024L * 1024L;
const int64_t BUFFER_POOL_CAPACITY = LARGE_RESERVATION_LIMIT;

namespace impala {
namespace io {

class DiskIoMgrTest : public testing::Test {
 public:

  virtual void SetUp() {
    test_env_.reset(new TestEnv);
    // Tests try to allocate arbitrarily small buffers. Ensure Buffer Pool allows it.
    test_env_->SetBufferPoolArgs(1, BUFFER_POOL_CAPACITY);
    ASSERT_OK(test_env_->Init());
    RandTestUtil::SeedRng("DISK_IO_MGR_TEST_SEED", &rng_);
  }

  virtual void TearDown() {
    root_reservation_.Close();
    pool_.Clear();
    test_env_.reset();
  }

  /// Initialises 'root_reservation_'. The reservation is automatically closed in
  /// TearDown().
  void InitRootReservation(int64_t reservation_limit) {
    root_reservation_.InitRootTracker(
        RuntimeProfile::Create(&pool_, "root"), reservation_limit);
  }

  /// Initialise 'client' with the given reservation limit. The client reservation is a
  /// child of 'root_reservation_'.
  void RegisterBufferPoolClient(int64_t reservation_limit, int64_t initial_reservation,
      BufferPool::ClientHandle* client) {
    ASSERT_OK(buffer_pool()->RegisterClient("", nullptr, &root_reservation_, nullptr,
        reservation_limit, RuntimeProfile::Create(&pool_, ""), client));
    if (initial_reservation > 0) {
      ASSERT_TRUE(client->IncreaseReservation(initial_reservation));
    }
  }

  void WriteValidateCallback(int num_writes, WriteRange** written_range,
      DiskIoMgr* io_mgr, RequestContext* reader, BufferPool::ClientHandle* client,
      int32_t* data, Status expected_status, const Status& status) {
    if (expected_status.code() == TErrorCode::CANCELLED) {
      EXPECT_TRUE(status.ok() || status.IsCancelled()) << "Error: " << status.GetDetail();
    } else {
      EXPECT_EQ(status.code(), expected_status.code());
    }
    if (status.ok()) {
      ScanRange* scan_range = pool_.Add(new ScanRange());
      scan_range->Reset(nullptr, (*written_range)->file(), (*written_range)->len(),
          (*written_range)->offset(), 0, false, BufferOpts::Uncached());
      ValidateSyncRead(io_mgr, reader, client, scan_range,
          reinterpret_cast<const char*>(data), sizeof(int32_t));
    }

    {
      lock_guard<mutex> l(written_mutex_);
      ++num_ranges_written_;
      if (num_ranges_written_ == num_writes) writes_done_.NotifyOne();
    }
  }

  void WriteCompleteCallback(int num_writes, const Status& status) {
    EXPECT_OK(status);
    {
      lock_guard<mutex> l(written_mutex_);
      ++num_ranges_written_;
      if (num_ranges_written_ == num_writes) writes_done_.NotifyAll();
    }
  }

 protected:
  void CreateTempFile(const char* filename, const char* data) {
    CreateTempFile(filename, data, strlen(data));
  }

  void CreateTempFile(const char* filename, const char* data, int64_t data_bytes) {
    FILE* file = fopen(filename, "w");
    EXPECT_TRUE(file != nullptr);
    fwrite(data, 1, data_bytes, file);
    fclose(file);
  }

  int CreateTempFile(const char* filename, int file_size) {
    FILE* file = fopen(filename, "w");
    EXPECT_TRUE(file != nullptr);
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

  static void ValidateSyncRead(DiskIoMgr* io_mgr, RequestContext* reader,
      BufferPool::ClientHandle* client, ScanRange* range, const char* expected,
      int expected_len = -1) {
    unique_ptr<BufferDescriptor> buffer;
    bool needs_buffers;
    ASSERT_OK(io_mgr->StartScanRange(reader, range, &needs_buffers));
    if (needs_buffers) {
      ASSERT_OK(io_mgr->AllocateBuffersForRange(
          reader, client, range, io_mgr->max_buffer_size()));
    }
    ASSERT_OK(range->GetNext(&buffer));
    ASSERT_TRUE(buffer != nullptr);
    EXPECT_EQ(buffer->len(), range->len());
    if (expected_len < 0) expected_len = strlen(expected);
    int cmp = memcmp(buffer->buffer(), expected, expected_len);
    EXPECT_TRUE(cmp == 0);
    range->ReturnBuffer(move(buffer));
  }

  static void ValidateScanRange(DiskIoMgr* io_mgr, ScanRange* range,
      const char* expected, int expected_len, const Status& expected_status) {
    char result[expected_len + 1];
    memset(result, 0, expected_len + 1);

    while (true) {
      unique_ptr<BufferDescriptor> buffer;
      Status status = range->GetNext(&buffer);
      ASSERT_TRUE(status.ok() || status.code() == expected_status.code());
      if (buffer == nullptr || !status.ok()) {
        if (buffer != nullptr) range->ReturnBuffer(move(buffer));
        break;
      }
      ASSERT_LE(buffer->len(), expected_len);
      memcpy(result + range->offset() + buffer->scan_range_offset(),
          buffer->buffer(), buffer->len());
      range->ReturnBuffer(move(buffer));
    }
    ValidateEmptyOrCorrect(expected, result, expected_len);
  }

  // Continues pulling scan ranges from the io mgr until they are all done.
  // Updates num_ranges_processed with the number of ranges seen by this thread.
  static void ScanRangeThread(DiskIoMgr* io_mgr, RequestContext* reader,
      BufferPool::ClientHandle* client, const char* expected_result, int expected_len,
      const Status& expected_status, int max_ranges, AtomicInt32* num_ranges_processed) {
    int num_ranges = 0;
    while (max_ranges == 0 || num_ranges < max_ranges) {
      ScanRange* range;
      bool needs_buffers;
      Status status = io_mgr->GetNextUnstartedRange(reader, &range, &needs_buffers);
      ASSERT_TRUE(status.ok() || status.code() == expected_status.code());
      if (range == nullptr) break;
      if (needs_buffers) {
        ASSERT_OK(io_mgr->AllocateBuffersForRange(
            reader, client, range, io_mgr->max_buffer_size() * 3));
      }
      ValidateScanRange(io_mgr, range, expected_result, expected_len, expected_status);
      num_ranges_processed->Add(1);
      ++num_ranges;
    }
  }

  ScanRange* InitRange(ObjectPool* pool, const char* file_path, int offset, int len,
      int disk_id, int64_t mtime, void* meta_data = nullptr, bool is_cached = false) {
    ScanRange* range = pool->Add(new ScanRange);
    range->Reset(nullptr, file_path, len, offset, disk_id, true,
        BufferOpts(is_cached, mtime), meta_data);
    EXPECT_EQ(mtime, range->mtime());
    return range;
  }

  /// Convenience function to get a reference to the buffer pool.
  BufferPool* buffer_pool() const { return ExecEnv::GetInstance()->buffer_pool(); }

  boost::scoped_ptr<TestEnv> test_env_;

  /// Per-test random number generator. Seeded before every test.
  mt19937 rng_;

  ObjectPool pool_;

  ReservationTracker root_reservation_;

  mutex written_mutex_;
  ConditionVariable writes_done_;
  int num_ranges_written_;
};

// Test a single writer with multiple disks and threads per disk. Each WriteRange
// writes random 4-byte integers, and upon completion, the written data is validated
// by reading the data back via a separate IoMgr instance. All writes are expected to
// complete successfully.
TEST_F(DiskIoMgrTest, SingleWriter) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
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
  BufferPool::ClientHandle read_client;
  RegisterBufferPoolClient(
      LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
  ASSERT_OK(read_io_mgr->Init());
  unique_ptr<RequestContext> reader = read_io_mgr->RegisterContext();
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      // Pool for temporary objects from this iteration only.
      ObjectPool tmp_pool;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 10);
      ASSERT_OK(io_mgr.Init());
      unique_ptr<RequestContext> writer = io_mgr.RegisterContext();
      for (int i = 0; i < num_ranges; ++i) {
        int32_t* data = tmp_pool.Add(new int32_t);
        *data = rand();
        WriteRange** new_range = tmp_pool.Add(new WriteRange*);
        WriteRange::WriteDoneCallback callback = bind(
            mem_fn(&DiskIoMgrTest::WriteValidateCallback), this, num_ranges, new_range,
            read_io_mgr.get(), reader.get(), &read_client, data, Status::OK(), _1);
        *new_range = tmp_pool.Add(
            new WriteRange(tmp_file, cur_offset, num_ranges % num_disks, callback));
        (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
        EXPECT_OK(io_mgr.AddWriteRange(writer.get(), *new_range));
        cur_offset += sizeof(int32_t);
      }

      {
        unique_lock<mutex> lock(written_mutex_);
        while (num_ranges_written_ < num_ranges) writes_done_.Wait(lock);
      }
      num_ranges_written_ = 0;
      io_mgr.UnregisterContext(writer.get());
    }
  }

  read_io_mgr->UnregisterContext(reader.get());
  buffer_pool()->DeregisterClient(&read_client);
}

// Perform invalid writes (e.g. file in non-existent directory, negative offset) and
// validate that an error status is returned via the write callback.
TEST_F(DiskIoMgrTest, InvalidWrite) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  num_ranges_written_ = 0;
  string tmp_file = "/non-existent/file.txt";
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  unique_ptr<RequestContext> writer = io_mgr.RegisterContext();
  int32_t* data = pool_.Add(new int32_t);
  *data = rand();

  // Write to file in non-existent directory.
  WriteRange** new_range = pool_.Add(new WriteRange*);
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this, 2, new_range, nullptr,
          nullptr, nullptr, data, Status(TErrorCode::DISK_IO_ERROR, "Test Failure"), _1);
  *new_range = pool_.Add(new WriteRange(tmp_file, rand(), 0, callback));

  (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
  EXPECT_OK(io_mgr.AddWriteRange(writer.get(), *new_range));

  // Write to a bad location in a file that exists.
  tmp_file = "/tmp/disk_io_mgr_test.txt";
  int success = CreateTempFile(tmp_file.c_str(), 100);
  if (success != 0) {
    LOG(ERROR) << "Error creating temp file " << tmp_file.c_str() << " of size 100";
    EXPECT_TRUE(false);
  }

  new_range = pool_.Add(new WriteRange*);
  callback = bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this, 2, new_range,
      nullptr, nullptr, nullptr, data,
      Status(TErrorCode::DISK_IO_ERROR, "Test Failure"), _1);

  *new_range = pool_.Add(new WriteRange(tmp_file, -1, 0, callback));
  (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
  EXPECT_OK(io_mgr.AddWriteRange(writer.get(), *new_range));

  {
    unique_lock<mutex> lock(written_mutex_);
    while (num_ranges_written_ < 2) writes_done_.Wait(lock);
  }
  num_ranges_written_ = 0;
  io_mgr.UnregisterContext(writer.get());
}

// Issue a number of writes, cancel the writer context and issue more writes.
// AddWriteRange() is expected to succeed before the cancel and fail after it.
// The writes themselves may finish with status cancelled or ok.
TEST_F(DiskIoMgrTest, SingleWriterCancel) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
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
  BufferPool::ClientHandle read_client;
  RegisterBufferPoolClient(
      LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
  ASSERT_OK(read_io_mgr->Init());
  unique_ptr<RequestContext> reader = read_io_mgr->RegisterContext();
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      // Pool for temporary objects from this iteration only.
      ObjectPool tmp_pool;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 10);
      ASSERT_OK(io_mgr.Init());
      unique_ptr<RequestContext> writer = io_mgr.RegisterContext();
      Status validate_status = Status::OK();
      for (int i = 0; i < num_ranges; ++i) {
        if (i == num_ranges_before_cancel) {
          writer->Cancel();
          validate_status = Status::CANCELLED;
        }
        int32_t* data = tmp_pool.Add(new int32_t);
        *data = rand();
        WriteRange** new_range = tmp_pool.Add(new WriteRange*);
        WriteRange::WriteDoneCallback callback =
            bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this,
                num_ranges_before_cancel, new_range, read_io_mgr.get(), reader.get(),
                &read_client, data, Status::CANCELLED, _1);
        *new_range = tmp_pool.Add(
            new WriteRange(tmp_file, cur_offset, num_ranges % num_disks, callback));
        (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
        cur_offset += sizeof(int32_t);
        Status add_status = io_mgr.AddWriteRange(writer.get(), *new_range);
        EXPECT_TRUE(add_status.code() == validate_status.code());
      }

      {
        unique_lock<mutex> lock(written_mutex_);
        while (num_ranges_written_ < num_ranges_before_cancel) writes_done_.Wait(lock);
      }
      num_ranges_written_ = 0;
      io_mgr.UnregisterContext(writer.get());
    }
  }

  read_io_mgr->UnregisterContext(reader.get());
  buffer_pool()->DeregisterClient(&read_client);
}

// Basic test with a single reader, testing multiple threads, disks and a different
// number of buffers.
TEST_F(DiskIoMgrTest, SingleReader) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
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
        ObjectPool tmp_pool;
        LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                  << " num_disk=" << num_disks
                  << " num_read_threads=" << num_read_threads;

        if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
        DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 1);

        ASSERT_OK(io_mgr.Init());
        BufferPool::ClientHandle read_client;
        RegisterBufferPoolClient(
            LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
        unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

        vector<ScanRange*> ranges;
        for (int i = 0; i < len; ++i) {
          int disk_id = i % num_disks;
          ranges.push_back(InitRange(&tmp_pool, tmp_file, 0, len, disk_id, stat_val.st_mtime));
        }
        ASSERT_OK(io_mgr.AddScanRanges(reader.get(), ranges));

        AtomicInt32 num_ranges_processed;
        thread_group threads;
        for (int i = 0; i < num_read_threads; ++i) {
          threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader.get(),
              &read_client, data, len, Status::OK(), 0, &num_ranges_processed));
        }
        threads.join_all();

        EXPECT_EQ(num_ranges_processed.Load(), ranges.size());
        io_mgr.UnregisterContext(reader.get());
        EXPECT_EQ(read_client.GetUsedReservation(), 0);
        buffer_pool()->DeregisterClient(&read_client);
      }
    }
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// This test issues adding additional scan ranges while there are some still in flight.
TEST_F(DiskIoMgrTest, AddScanRangeTest) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
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
      // Pool for temporary objects from this iteration only.
      ObjectPool tmp_pool;
      LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                << " num_disk=" << num_disks;

      if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 1);

      ASSERT_OK(io_mgr.Init());
      BufferPool::ClientHandle read_client;
      RegisterBufferPoolClient(
          LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
      unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

      vector<ScanRange*> ranges_first_half;
      vector<ScanRange*> ranges_second_half;
      for (int i = 0; i < len; ++i) {
        int disk_id = i % num_disks;
        if (i > len / 2) {
          ranges_second_half.push_back(
              InitRange(&tmp_pool, tmp_file, i, 1, disk_id, stat_val.st_mtime));
        } else {
          ranges_first_half.push_back(
              InitRange(&tmp_pool, tmp_file, i, 1, disk_id, stat_val.st_mtime));
        }
      }
      AtomicInt32 num_ranges_processed;

      // Issue first half the scan ranges.
      ASSERT_OK(io_mgr.AddScanRanges(reader.get(), ranges_first_half));

      // Read a couple of them
      ScanRangeThread(&io_mgr, reader.get(), &read_client, data, strlen(data),
          Status::OK(), 2, &num_ranges_processed);

      // Issue second half
      ASSERT_OK(io_mgr.AddScanRanges(reader.get(), ranges_second_half));

      // Start up some threads and then cancel
      thread_group threads;
      for (int i = 0; i < 3; ++i) {
        threads.add_thread(
            new thread(ScanRangeThread, &io_mgr, reader.get(), &read_client, data,
                strlen(data), Status::CANCELLED, 0, &num_ranges_processed));
      }

      threads.join_all();
      EXPECT_EQ(num_ranges_processed.Load(), len);
      io_mgr.UnregisterContext(reader.get());
      EXPECT_EQ(read_client.GetUsedReservation(), 0);
      buffer_pool()->DeregisterClient(&read_client);
    }
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Test to make sure that sync reads and async reads work together
// Note: this test is constructed so the number of buffers is greater than the
// number of scan ranges.
TEST_F(DiskIoMgrTest, SyncReadTest) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
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
      // Pool for temporary objects from this iteration only.
      ObjectPool tmp_pool;
      LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                << " num_disk=" << num_disks;

      if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk,
          MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

      ASSERT_OK(io_mgr.Init());
      BufferPool::ClientHandle read_client;
      RegisterBufferPoolClient(
          LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
      unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

      ScanRange* complete_range =
          InitRange(&tmp_pool, tmp_file, 0, strlen(data), 0, stat_val.st_mtime);

      // Issue some reads before the async ones are issued
      ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);
      ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);
      vector<ScanRange*> ranges;
      for (int i = 0; i < len; ++i) {
        int disk_id = i % num_disks;
        ranges.push_back(
            InitRange(&tmp_pool, tmp_file, 0, len, disk_id, stat_val.st_mtime));
      }
      ASSERT_OK(io_mgr.AddScanRanges(reader.get(), ranges));

      AtomicInt32 num_ranges_processed;
      thread_group threads;
      for (int i = 0; i < 5; ++i) {
        threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader.get(),
            &read_client, data, strlen(data), Status::OK(), 0, &num_ranges_processed));
      }

      // Issue some more sync ranges
      for (int i = 0; i < 5; ++i) {
        sched_yield();
        ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);
      }

      threads.join_all();

      ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);
      ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);

      EXPECT_EQ(num_ranges_processed.Load(), ranges.size());
      io_mgr.UnregisterContext(reader.get());
      EXPECT_EQ(read_client.GetUsedReservation(), 0);
      buffer_pool()->DeregisterClient(&read_client);
    }
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Tests a single reader cancelling half way through scan ranges.
TEST_F(DiskIoMgrTest, SingleReaderCancel) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
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
      // Pool for temporary objects from this iteration only.
      ObjectPool tmp_pool;
      LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                << " num_disk=" << num_disks;

      if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 1);

      ASSERT_OK(io_mgr.Init());
      BufferPool::ClientHandle read_client;
      RegisterBufferPoolClient(
          LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
      unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

      vector<ScanRange*> ranges;
      for (int i = 0; i < len; ++i) {
        int disk_id = i % num_disks;
        ranges.push_back(
            InitRange(&tmp_pool, tmp_file, 0, len, disk_id, stat_val.st_mtime));
      }
      ASSERT_OK(io_mgr.AddScanRanges(reader.get(), ranges));

      AtomicInt32 num_ranges_processed;
      int num_succesful_ranges = ranges.size() / 2;
      // Read half the ranges
      for (int i = 0; i < num_succesful_ranges; ++i) {
        ScanRangeThread(&io_mgr, reader.get(), &read_client, data, strlen(data),
            Status::OK(), 1, &num_ranges_processed);
      }
      EXPECT_EQ(num_ranges_processed.Load(), num_succesful_ranges);

      // Start up some threads and then cancel
      thread_group threads;
      for (int i = 0; i < 3; ++i) {
        threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader.get(),
            &read_client, data, strlen(data), Status::CANCELLED, 0,
            &num_ranges_processed));
      }

      reader->Cancel();
      sched_yield();

      threads.join_all();
      EXPECT_TRUE(reader->IsCancelled());
      io_mgr.UnregisterContext(reader.get());
      EXPECT_EQ(read_client.GetUsedReservation(), 0);
      buffer_pool()->DeregisterClient(&read_client);
    }
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Test readers running with different amounts of memory and getting blocked on scan
// ranges that have run out of buffers.
TEST_F(DiskIoMgrTest, MemScarcity) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  // File is 2.5 max buffers so that we can scan file without returning buffers
  // when we get the max reservation below.
  const int64_t DATA_BYTES = MAX_BUFFER_SIZE * 5 / 2;
  char data[DATA_BYTES];
  for (int i = 0; i < DATA_BYTES; ++i) {
    data[i] = uniform_int_distribution<uint8_t>(0, 255)(rng_);
  }
  CreateTempFile(tmp_file, data, DATA_BYTES);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  const int RESERVATION_LIMIT_NUM_BUFFERS = 20;
  const int64_t RESERVATION_LIMIT = RESERVATION_LIMIT_NUM_BUFFERS * MAX_BUFFER_SIZE;
  InitRootReservation(RESERVATION_LIMIT);

  thread_group threads;
  // Allocate enough ranges so that the total buffers exceeds the limit.
  const int num_ranges = 25;
  {
    DiskIoMgr io_mgr(1, 1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

    ASSERT_OK(io_mgr.Init());
    BufferPool::ClientHandle read_client;
    RegisterBufferPoolClient(RESERVATION_LIMIT, RESERVATION_LIMIT, &read_client);
    unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

    vector<ScanRange*> ranges;
    for (int i = 0; i < num_ranges; ++i) {
      ranges.push_back(InitRange(&pool_, tmp_file, 0, DATA_BYTES, 0, stat_val.st_mtime));
    }
    ASSERT_OK(io_mgr.AddScanRanges(reader.get(), ranges));
    // Keep starting new ranges without returning buffers until we run out of
    // reservation.
    while (read_client.GetUnusedReservation() >= MIN_BUFFER_SIZE) {
      ScanRange* range = nullptr;
      bool needs_buffers;
      ASSERT_OK(io_mgr.GetNextUnstartedRange(reader.get(), &range, &needs_buffers));
      if (range == nullptr) break;
      ASSERT_TRUE(needs_buffers);
      // Pick a random amount of memory to reserve.
      int64_t max_bytes_to_alloc = uniform_int_distribution<int64_t>(MIN_BUFFER_SIZE,
          min<int64_t>(read_client.GetUnusedReservation(), MAX_BUFFER_SIZE * 3))(rng_);
      ASSERT_OK(io_mgr.AllocateBuffersForRange(
          reader.get(), &read_client, range, max_bytes_to_alloc));
      // Start a thread fetching from the range. The thread will either finish the
      // range or be cancelled.
      threads.add_thread(new thread([&data, DATA_BYTES, range] {
        // Don't return buffers to force memory pressure.
        vector<unique_ptr<BufferDescriptor>> buffers;
        int64_t data_offset = 0;
        Status status;
        while (true) {
          unique_ptr<BufferDescriptor> buffer;
          status = range->GetNext(&buffer);
          ASSERT_TRUE(status.ok() || status.IsCancelled()) << status.GetDetail();
          if (status.IsCancelled() || buffer == nullptr) break;
          EXPECT_EQ(0, memcmp(data + data_offset, buffer->buffer(), buffer->len()));
          data_offset += buffer->len();
          buffers.emplace_back(move(buffer));
        }
        if (status.ok()) ASSERT_EQ(DATA_BYTES, data_offset);
        for (auto& buffer : buffers) range->ReturnBuffer(move(buffer));
      }));
      // Let the thread start running before starting the next.
      SleepForMs(10);
    }
    // Let the threads run for a bit then cancel everything.
    SleepForMs(500);
    reader->Cancel();
    // Wait until the threads have returned their buffers before unregistering.
    threads.join_all();
    io_mgr.UnregisterContext(reader.get());
    EXPECT_EQ(read_client.GetUsedReservation(), 0);
    buffer_pool()->DeregisterClient(&read_client);
  }
}

// Test when some scan ranges are marked as being cached.
// Since these files are not in HDFS, the cached path always fails so this
// only tests the fallback mechanism.
// TODO: we can fake the cached read path without HDFS
TEST_F(DiskIoMgrTest, CachedReads) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
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

    ASSERT_OK(io_mgr.Init());
    BufferPool::ClientHandle read_client;
    RegisterBufferPoolClient(
        LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
    unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

    ScanRange* complete_range =
        InitRange(&pool_, tmp_file, 0, strlen(data), 0, stat_val.st_mtime, nullptr, true);

    // Issue some reads before the async ones are issued
    ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);
    ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);

    vector<ScanRange*> ranges;
    for (int i = 0; i < len; ++i) {
      int disk_id = i % num_disks;
      ranges.push_back(
          InitRange(&pool_, tmp_file, 0, len, disk_id, stat_val.st_mtime, nullptr, true));
    }
    ASSERT_OK(io_mgr.AddScanRanges(reader.get(), ranges));

    AtomicInt32 num_ranges_processed;
    thread_group threads;
    for (int i = 0; i < 5; ++i) {
      threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader.get(), &read_client,
          data, strlen(data), Status::OK(), 0, &num_ranges_processed));
    }

    // Issue some more sync ranges
    for (int i = 0; i < 5; ++i) {
      sched_yield();
      ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);
    }

    threads.join_all();

    ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);
    ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);

    EXPECT_EQ(num_ranges_processed.Load(), ranges.size());
    io_mgr.UnregisterContext(reader.get());
    EXPECT_EQ(read_client.GetUsedReservation(), 0);
    buffer_pool()->DeregisterClient(&read_client);
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

TEST_F(DiskIoMgrTest, MultipleReaderWriter) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
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
  vector<unique_ptr<RequestContext>> contexts(num_contexts);
  unique_ptr<BufferPool::ClientHandle[]> clients(
      new BufferPool::ClientHandle[num_contexts]);
  Status status;
  for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
    for (int threads_per_disk = 1; threads_per_disk <= 5; ++threads_per_disk) {
      for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
        // Pool for temporary objects from this iteration only.
        ObjectPool tmp_pool;
        DiskIoMgr io_mgr(num_disks, threads_per_disk, threads_per_disk, MIN_BUFFER_SIZE,
            MAX_BUFFER_SIZE);
        ASSERT_OK(io_mgr.Init());
        for (int file_index = 0; file_index < num_contexts; ++file_index) {
          RegisterBufferPoolClient(
              LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &clients[file_index]);
          contexts[file_index] = io_mgr.RegisterContext();
        }
        int read_offset = 0;
        int write_offset = 0;
        while (read_offset < file_size) {
          for (int context_index = 0; context_index < num_contexts; ++context_index) {
            if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
            AtomicInt32 num_ranges_processed;
            thread_group threads;
            vector<ScanRange*> ranges;
            int num_scan_ranges = min<int>(num_reads_queued, write_offset - read_offset);
            for (int i = 0; i < num_scan_ranges; ++i) {
              ranges.push_back(InitRange(
                  &tmp_pool, file_name.c_str(), read_offset, 1, i % num_disks, stat_val.st_mtime));
              threads.add_thread(new thread(ScanRangeThread, &io_mgr,
                  contexts[context_index].get(), &clients[context_index],
                  reinterpret_cast<const char*>(data + (read_offset % strlen(data))),
                  1, Status::OK(), num_scan_ranges, &num_ranges_processed));
              ++read_offset;
            }

            num_ranges_written_ = 0;
            int num_write_ranges = min<int>(num_writes_queued, file_size - write_offset);
            for (int i = 0; i < num_write_ranges; ++i) {
              WriteRange::WriteDoneCallback callback =
                  bind(mem_fn(&DiskIoMgrTest::WriteCompleteCallback),
                      this, num_write_ranges, _1);
              WriteRange* new_range = tmp_pool.Add(new WriteRange(
                  file_name, write_offset, i % num_disks, callback));
              new_range->SetData(
                  reinterpret_cast<const uint8_t*>(data + (write_offset % strlen(data))),
                  1);
              status = io_mgr.AddWriteRange(contexts[context_index].get(), new_range);
              ++write_offset;
            }

            {
              unique_lock<mutex> lock(written_mutex_);
              while (num_ranges_written_ < num_write_ranges) writes_done_.Wait(lock);
            }

            threads.join_all();
          } // for (int context_index
        } // while (read_offset < file_size)
        for (int file_index = 0; file_index < num_contexts; ++file_index) {
          io_mgr.UnregisterContext(contexts[file_index].get());
          buffer_pool()->DeregisterClient(&clients[file_index]);
        }
      } // for (int num_disks
    } // for (int threads_per_disk
  } // for (int iteration
}

// This test will test multiple concurrent reads each reading a different file.
TEST_F(DiskIoMgrTest, MultipleReader) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const int NUM_READERS = 5;
  const int DATA_LEN = 50;
  const int ITERATIONS = 25;
  const int NUM_THREADS_PER_READER = 3;

  vector<string> file_names(NUM_READERS);
  vector<int64_t> mtimes(NUM_READERS);
  vector<string> data(NUM_READERS);
  unique_ptr<BufferPool::ClientHandle[]> clients(
      new BufferPool::ClientHandle[NUM_READERS]);
  vector<unique_ptr<RequestContext>> readers(NUM_READERS);
  vector<char*> results(NUM_READERS);

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
        // Pool for temporary objects from this iteration only.
        ObjectPool tmp_pool;
        LOG(INFO) << "Starting test with num_threads_per_disk=" << threads_per_disk
                  << " num_disk=" << num_disks;
        if (++iters % 2500 == 0) LOG(ERROR) << "Starting iteration " << iters;

        DiskIoMgr io_mgr(num_disks, threads_per_disk, threads_per_disk, MIN_BUFFER_SIZE,
            MAX_BUFFER_SIZE);
        ASSERT_OK(io_mgr.Init());

        for (int i = 0; i < NUM_READERS; ++i) {
          RegisterBufferPoolClient(
              LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &clients[i]);
          readers[i] = io_mgr.RegisterContext();

          vector<ScanRange*> ranges;
          for (int j = 0; j < DATA_LEN; ++j) {
            int disk_id = j % num_disks;
            ranges.push_back(InitRange(&tmp_pool, file_names[i].c_str(), j, 1, disk_id, mtimes[i]));
          }
          ASSERT_OK(io_mgr.AddScanRanges(readers[i].get(), ranges));
        }

        AtomicInt32 num_ranges_processed;
        thread_group threads;
        for (int i = 0; i < NUM_READERS; ++i) {
          for (int j = 0; j < NUM_THREADS_PER_READER; ++j) {
            threads.add_thread(new thread(ScanRangeThread, &io_mgr, readers[i].get(),
                &clients[i], data[i].c_str(), data[i].size(), Status::OK(), 0,
                &num_ranges_processed));
          }
        }
        threads.join_all();
        EXPECT_EQ(num_ranges_processed.Load(), DATA_LEN * NUM_READERS);
        for (int i = 0; i < NUM_READERS; ++i) {
          io_mgr.UnregisterContext(readers[i].get());
          buffer_pool()->DeregisterClient(&clients[i]);
        }
      }
    }
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Stress test for multiple clients with cancellation
// TODO: the stress app should be expanded to include sync reads and adding scan
// ranges in the middle.
TEST_F(DiskIoMgrTest, StressTest) {
  // Run the test with 5 disks, 5 threads per disk, 10 clients and with cancellation
  DiskIoMgrStress test(5, 5, 10, true);
  test.Run(2); // In seconds
}

// IMPALA-2366: handle partial read where range goes past end of file.
TEST_F(DiskIoMgrTest, PartialRead) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  int len = strlen(data);
  int read_len = len + 1000; // Read past end of file.
  // Test with various buffer sizes to exercise different code paths, e.g.
  // * the truncated data ends exactly on a buffer boundary
  // * the data is split between many buffers
  // * the data fits in one buffer
  const int64_t MIN_BUFFER_SIZE = 2;
  vector<int64_t> max_buffer_sizes{4, 16, 32, 128, 1024, 4096};
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  for (int64_t max_buffer_size : max_buffer_sizes) {
    DiskIoMgr io_mgr(1, 1, 1, MIN_BUFFER_SIZE, max_buffer_size);
    ASSERT_OK(io_mgr.Init());
    unique_ptr<RequestContext> reader;
    reader = io_mgr.RegisterContext();

    BufferPool::ClientHandle read_client;
    RegisterBufferPoolClient(
        LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);

    // We should not read past the end of file.
    ScanRange* range = InitRange(&pool_, tmp_file, 0, read_len, 0, stat_val.st_mtime);
    unique_ptr<BufferDescriptor> buffer;
    bool needs_buffers;
    ASSERT_OK(io_mgr.StartScanRange(reader.get(), range, &needs_buffers));
    if (needs_buffers) {
      ASSERT_OK(io_mgr.AllocateBuffersForRange(
          reader.get(), &read_client, range, 3 * max_buffer_size));
    }

    int64_t bytes_read = 0;
    bool eosr = false;
    do {
      ASSERT_OK(range->GetNext(&buffer));
      ASSERT_GE(buffer->buffer_len(), MIN_BUFFER_SIZE);
      ASSERT_LE(buffer->buffer_len(), max_buffer_size);
      ASSERT_LE(buffer->len(), len - bytes_read);
      ASSERT_TRUE(memcmp(buffer->buffer(), data + bytes_read, buffer->len()) == 0);
      bytes_read += buffer->len();
      eosr = buffer->eosr();
      // Should see eosr if we've read past the end of the file. If the data is an exact
      // multiple of the max buffer size then we may read to the end of the file without
      // noticing that it is eosr. Eosr will be returned on the next read in that case.
      ASSERT_TRUE(bytes_read < len || buffer->eosr()
          || (buffer->len() == max_buffer_size && len % max_buffer_size == 0))
          << "max_buffer_size " << max_buffer_size << " bytes_read " << bytes_read
          << "len " << len << " buffer->len() " << buffer->len()
          << " buffer->buffer_len() " << buffer->buffer_len();
      ASSERT_TRUE(buffer->len() > 0 || buffer->eosr());
      range->ReturnBuffer(move(buffer));
    } while (!eosr);

    io_mgr.UnregisterContext(reader.get());
    EXPECT_EQ(read_client.GetUsedReservation(), 0);
    buffer_pool()->DeregisterClient(&read_client);
  }
}

// Test zero-length scan range.
TEST_F(DiskIoMgrTest, ZeroLengthScanRange) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  const int64_t MIN_BUFFER_SIZE = 2;
  const int64_t MAX_BUFFER_SIZE = 1024;
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  DiskIoMgr io_mgr(1, 1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

  ASSERT_OK(io_mgr.Init());
  unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

  ScanRange* range = InitRange(&pool_, tmp_file, 0, 0, 0, stat_val.st_mtime);
  bool needs_buffers;
  Status status = io_mgr.StartScanRange(reader.get(), range, &needs_buffers);
  ASSERT_EQ(TErrorCode::DISK_IO_ERROR, status.code());

  status = io_mgr.AddScanRanges(reader.get(), vector<ScanRange*>({range}));
  ASSERT_EQ(TErrorCode::DISK_IO_ERROR, status.code());

  io_mgr.UnregisterContext(reader.get());
}

// Test what happens if don't call AllocateBuffersForRange() after trying to start a
// range.
TEST_F(DiskIoMgrTest, SkipAllocateBuffers) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  int len = strlen(data);
  const int64_t MIN_BUFFER_SIZE = 2;
  const int64_t MAX_BUFFER_SIZE = 1024;
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  DiskIoMgr io_mgr(1, 1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

  ASSERT_OK(io_mgr.Init());
  MemTracker reader_mem_tracker;
  unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

  // We should not read past the end of file.
  vector<ScanRange*> ranges;
  for (int i = 0; i < 4; ++i) {
    ranges.push_back(InitRange(&pool_, tmp_file, 0, len, 0, stat_val.st_mtime));
  }
  bool needs_buffers;
  // Test StartScanRange().
  ASSERT_OK(io_mgr.StartScanRange(reader.get(), ranges[0], &needs_buffers));
  EXPECT_TRUE(needs_buffers);
  ASSERT_OK(io_mgr.StartScanRange(reader.get(), ranges[1], &needs_buffers));
  EXPECT_TRUE(needs_buffers);

  // Test AddScanRanges()/GetNextUnstartedRange().
  ASSERT_OK(
      io_mgr.AddScanRanges(reader.get(), vector<ScanRange*>({ranges[2], ranges[3]})));

  // Cancel two directly, cancel the other two indirectly via the context.
  ranges[0]->Cancel(Status::CANCELLED);
  ranges[2]->Cancel(Status::CANCELLED);
  reader->Cancel();

  io_mgr.UnregisterContext(reader.get());
}

// Test reading into a client-allocated buffer.
TEST_F(DiskIoMgrTest, ReadIntoClientBuffer) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  int len = strlen(data);
  int read_len = 4; // Make buffer size smaller than client-provided buffer.
  CreateTempFile(tmp_file, data);

  scoped_ptr<DiskIoMgr> io_mgr(new DiskIoMgr(1, 1, 1, read_len, read_len));

  ASSERT_OK(io_mgr->Init());
  // Reader doesn't need to provide client if it's providing buffers.
  unique_ptr<RequestContext> reader = io_mgr->RegisterContext();

  for (int buffer_len : vector<int>({len - 1, len, len + 1})) {
    vector<uint8_t> client_buffer(buffer_len);
    int scan_len = min(len, buffer_len);
    ScanRange* range = pool_.Add(new ScanRange);
    range->Reset(nullptr, tmp_file, scan_len, 0, 0, true,
        BufferOpts::ReadInto(client_buffer.data(), buffer_len));
    bool needs_buffers;
    ASSERT_OK(io_mgr->StartScanRange(reader.get(), range, &needs_buffers));
    ASSERT_FALSE(needs_buffers);

    unique_ptr<BufferDescriptor> io_buffer;
    ASSERT_OK(range->GetNext(&io_buffer));
    ASSERT_TRUE(io_buffer->eosr());
    ASSERT_EQ(scan_len, io_buffer->len());
    ASSERT_EQ(client_buffer.data(), io_buffer->buffer());
    ASSERT_EQ(memcmp(io_buffer->buffer(), data, scan_len), 0);

    // DiskIoMgr should not have allocated memory.
    EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
    range->ReturnBuffer(move(io_buffer));
  }

  io_mgr->UnregisterContext(reader.get());
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Test reading into a client-allocated buffer where the read fails.
TEST_F(DiskIoMgrTest, ReadIntoClientBufferError) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/file/that/does/not/exist";
  const int SCAN_LEN = 128;

  scoped_ptr<DiskIoMgr> io_mgr(new DiskIoMgr(1, 1, 1, SCAN_LEN, SCAN_LEN));

  ASSERT_OK(io_mgr->Init());
  vector<uint8_t> client_buffer(SCAN_LEN);
  for (int i = 0; i < 1000; ++i) {
    // Reader doesn't need to provide mem tracker if it's providing buffers.
    BufferPool::ClientHandle read_client;
    RegisterBufferPoolClient(
        LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
    unique_ptr<RequestContext> reader = io_mgr->RegisterContext();
    ScanRange* range = pool_.Add(new ScanRange);
    range->Reset(nullptr, tmp_file, SCAN_LEN, 0, 0, true,
        BufferOpts::ReadInto(client_buffer.data(), SCAN_LEN));
    bool needs_buffers;
    ASSERT_OK(io_mgr->StartScanRange(reader.get(), range, &needs_buffers));
    ASSERT_FALSE(needs_buffers);

    /// Also test the cancellation path. Run multiple iterations since it is racy whether
    /// the read fails before the cancellation.
    if (i >= 1) reader->Cancel();

    unique_ptr<BufferDescriptor> io_buffer;
    ASSERT_FALSE(range->GetNext(&io_buffer).ok());

    // DiskIoMgr should not have allocated memory.
    EXPECT_EQ(read_client.GetUsedReservation(), 0);

    io_mgr->UnregisterContext(reader.get());
    EXPECT_EQ(read_client.GetUsedReservation(), 0);
    buffer_pool()->DeregisterClient(&read_client);
  }

  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Test to verify configuration parameters for number of I/O threads per disk.
TEST_F(DiskIoMgrTest, VerifyNumThreadsParameter) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const int num_io_threads_for_remote_disks = FLAGS_num_remote_hdfs_io_threads
      + FLAGS_num_s3_io_threads + FLAGS_num_adls_io_threads;

  // Verify num_io_threads_per_rotational_disk and num_io_threads_per_solid_state_disk.
  // Since we do not have control over which disk is used, we check for either type
  // (rotational/solid state)
  const int num_io_threads_per_rotational_or_ssd = 2;
  DiskIoMgr io_mgr(1, num_io_threads_per_rotational_or_ssd,
      num_io_threads_per_rotational_or_ssd, 1, 10);
  ASSERT_OK(io_mgr.Init());
  const int num_io_threads = io_mgr.disk_thread_group_.Size();
  ASSERT_TRUE(num_io_threads ==
      num_io_threads_per_rotational_or_ssd + num_io_threads_for_remote_disks);
}

// Test to verify that the correct buffer sizes are chosen given different
// of scan range lengths and max_bytes values.
TEST_F(DiskIoMgrTest, BufferSizeSelection) {
  DiskIoMgr io_mgr(1, 1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);
  ASSERT_OK(io_mgr.Init());

  // Scan range doesn't fit in max_bytes - allocate as many max-sized buffers as possible.
  EXPECT_EQ(vector<int64_t>(3, MAX_BUFFER_SIZE),
      io_mgr.ChooseBufferSizes(10 * MAX_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(10 * MAX_BUFFER_SIZE, MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>(4, MAX_BUFFER_SIZE),
      io_mgr.ChooseBufferSizes(10 * MAX_BUFFER_SIZE, 4 * MAX_BUFFER_SIZE));

  // Scan range fits in max_bytes - allocate as many max-sized buffers as possible, then
  // a smaller buffer to fit the remainder.
  EXPECT_EQ(vector<int64_t>(2, MAX_BUFFER_SIZE),
      io_mgr.ChooseBufferSizes(2 * MAX_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE, MAX_BUFFER_SIZE, MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(2 * MAX_BUFFER_SIZE + 1, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE, MAX_BUFFER_SIZE, 2 * MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(
        2 * MAX_BUFFER_SIZE + MIN_BUFFER_SIZE + 1, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE, MAX_BUFFER_SIZE, 2 * MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(
        2 * MAX_BUFFER_SIZE + 2 * MIN_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE));

  // Scan range is smaller than max buffer size - allocate a single buffer that fits
  // the range.
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE - 1, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE / 2}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE - 1, MAX_BUFFER_SIZE / 2));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE / 2}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE / 2 - 1, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE / 2}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE / 2- 1, MAX_BUFFER_SIZE / 2));
  EXPECT_EQ(vector<int64_t>({MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(MIN_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(MIN_BUFFER_SIZE, MIN_BUFFER_SIZE));

  // Scan range is smaller than max buffer size and max bytes is smaller still -
  // should allocate a single smaller buffer.
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE / 4}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE / 2, MAX_BUFFER_SIZE / 2 - 1));

  // Non power-of-two size > max buffer size.
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE, MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE + 7, 3 * MAX_BUFFER_SIZE));
  // Non power-of-two size < min buffer size.
  EXPECT_EQ(vector<int64_t>({MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(MIN_BUFFER_SIZE - 7, 3 * MAX_BUFFER_SIZE));
}
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  return RUN_ALL_TESTS();
}
