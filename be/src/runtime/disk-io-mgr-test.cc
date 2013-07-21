// Copyright 2012 Cloudera Inc.
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

#include <sched.h>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>

#include <gtest/gtest.h>

#include "codegen/llvm-codegen.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/disk-io-mgr-stress.h"
#include "runtime/mem-tracker.h"
#include "runtime/thread-resource-mgr.h"
#include "util/cpu-info.h"
#include "util/thread.h"

using namespace std;
using namespace boost;

const int BUFFER_SIZE = 1024;
const int LARGE_MEM_LIMIT = 1024 * 1024 * 1024;

namespace impala {

class DiskIoMgrTest : public testing::Test {
 protected:
  void CreateTempFile(const char* filename, const char* data) {
    FILE* file = fopen(filename, "w");
    EXPECT_TRUE(file != NULL);
    fwrite(data, 1, strlen(data), file);
    fclose(file);
  }

  // Validates that buffer[i] is \0 or expected[i]
  static void ValidateEmptyOrCorrect(const char* expected, const char* buffer, int len) {
    for (int i = 0; i < len; ++i) {
      if (buffer[i] != '\0') {
        EXPECT_EQ(expected[i], buffer[i]) << (int)expected[i] << " != " << (int)buffer[i];
      }
    }
  }

  static void ValidateSyncRead(DiskIoMgr* io_mgr, DiskIoMgr::ReaderContext* reader,
      DiskIoMgr::ScanRange* range, const char* expected) {
    DiskIoMgr::BufferDescriptor* buffer;
    Status status = io_mgr->Read(reader, range, &buffer);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(buffer != NULL);
    EXPECT_EQ(buffer->len(), range->len());
    EXPECT_TRUE(strncmp(buffer->buffer(), expected, strlen(expected)) == 0);
    buffer->Return();
  }

  static void ValidateScanRange(DiskIoMgr::ScanRange* range, const char* expected,
      const Status& expected_status) {
    char result[strlen(expected) + 1];
    memset(result, 0, strlen(expected) + 1);

    while (true) {
      DiskIoMgr::BufferDescriptor* buffer = NULL;
      Status status = range->GetNext(&buffer);
      ASSERT_TRUE(status.ok() || status.code() == expected_status.code());
      if (buffer == NULL || !status.ok()) {
        if (buffer != NULL) buffer->Return();
        break;
      }
      memcpy(result + range->offset() + buffer->scan_range_offset(),
          buffer->buffer(), buffer->len());
      buffer->Return();
    }
    ValidateEmptyOrCorrect(expected, result, strlen(expected));
  }

  // Continues pulling scan ranges from the io mgr until they are all done.
  // Updates num_ranges_processed with the number of ranges seen by this thread.
  static void ScanRangeThread(DiskIoMgr* io_mgr, DiskIoMgr::ReaderContext* reader,
      const char* expected_result, const Status& expected_status,
      int max_ranges, AtomicInt<int>* num_ranges_processed) {
    int num_ranges = 0;
    while (max_ranges == 0 || num_ranges < max_ranges) {
      DiskIoMgr::ScanRange* range;
      Status status = io_mgr->GetNextRange(reader, &range);
      ASSERT_TRUE(status.ok() || status.code() == expected_status.code());
      if (range == NULL) break;
      ValidateScanRange(range, expected_result, expected_status);
      ++(*num_ranges_processed);
      ++num_ranges;
    }
  }

  DiskIoMgr::ScanRange* InitRange(int num_buffers, const char* file_path, int offset,
      int len, int disk_id, void* meta_data = NULL) {
    DiskIoMgr::ScanRange* range = pool_->Add(new DiskIoMgr::ScanRange(num_buffers));
    range->Reset(file_path, len, offset, disk_id, meta_data);
    return range;
  }

  scoped_ptr<ObjectPool> pool_;
};

// Basic test with a single reader, testing multiple threads, disks and a different
// number of buffers.
TEST_F(DiskIoMgrTest, SingleReader) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklm";
  int len = strlen(data);
  CreateTempFile(tmp_file, data);

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int num_buffers = 1; num_buffers <= 5; ++num_buffers) {
        for (int num_read_threads = 1; num_read_threads <= 5; ++num_read_threads) {
          pool_.reset(new ObjectPool);
          LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                    << " num_disk=" << num_disks << " num_buffers=" << num_buffers
                    << " num_read_threads=" << num_read_threads;

          if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
          DiskIoMgr io_mgr(num_disks, num_threads_per_disk, 1);

          Status status = io_mgr.Init(&mem_tracker);
          ASSERT_TRUE(status.ok());
          MemTracker reader_mem_tracker;
          DiskIoMgr::ReaderContext* reader;
          status = io_mgr.RegisterReader(NULL, &reader, &reader_mem_tracker);
          ASSERT_TRUE(status.ok());

          vector<DiskIoMgr::ScanRange*> ranges;
          for (int i = 0; i < len; ++i) {
            int disk_id = i % num_disks;
            ranges.push_back(InitRange(num_buffers, tmp_file, 0, len, disk_id));
          }
          status = io_mgr.AddScanRanges(reader, ranges);
          ASSERT_TRUE(status.ok());

          AtomicInt<int> num_ranges_processed;
          thread_group threads;
          for (int i = 0; i < num_read_threads; ++i) {
            threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader, data,
                Status::OK, 0, &num_ranges_processed));
          }
          threads.join_all();

          EXPECT_EQ(num_ranges_processed, ranges.size());
          io_mgr.UnregisterReader(reader);
          EXPECT_EQ(reader_mem_tracker.consumption(), 0);
        }
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

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int num_buffers = 1; num_buffers <= 5; ++num_buffers) {
        pool_.reset(new ObjectPool);
        LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                  << " num_disk=" << num_disks << " num_buffers=" << num_buffers;

        if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
        DiskIoMgr io_mgr(num_disks, num_threads_per_disk, 1);

        Status status = io_mgr.Init(&mem_tracker);
        ASSERT_TRUE(status.ok());
        MemTracker reader_mem_tracker;
        DiskIoMgr::ReaderContext* reader;
        status = io_mgr.RegisterReader(NULL, &reader, &reader_mem_tracker);
        ASSERT_TRUE(status.ok());

        vector<DiskIoMgr::ScanRange*> ranges_first_half;
        vector<DiskIoMgr::ScanRange*> ranges_second_half;
        for (int i = 0; i < len; ++i) {
          int disk_id = i % num_disks;
          if (i > len / 2) {
            ranges_second_half.push_back(
                InitRange(num_buffers, tmp_file, i, 1, disk_id));
          } else {
            ranges_first_half.push_back(InitRange(num_buffers, tmp_file, i, 1, disk_id));
          }
        }
        AtomicInt<int> num_ranges_processed;

        // Issue first half the scan ranges.
        status = io_mgr.AddScanRanges(reader, ranges_first_half);
        ASSERT_TRUE(status.ok());

        // Read a couple of them
        ScanRangeThread(&io_mgr, reader, data, Status::OK, 2, &num_ranges_processed);

        // Issue second half
        status = io_mgr.AddScanRanges(reader, ranges_second_half);
        ASSERT_TRUE(status.ok());

        // Start up some threads and then cancel
        thread_group threads;
        for (int i = 0; i < 3; ++i) {
          threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader, data,
              Status::CANCELLED, 0, &num_ranges_processed));
        }

        threads.join_all();
        EXPECT_EQ(num_ranges_processed, len);
        io_mgr.UnregisterReader(reader);
        EXPECT_EQ(reader_mem_tracker.consumption(), 0);
      }
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

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int num_buffers = 1; num_buffers <= 5; ++num_buffers) {
        pool_.reset(new ObjectPool);
        LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                  << " num_disk=" << num_disks << " num_buffers=" << num_buffers;

        if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
        DiskIoMgr io_mgr(num_disks, num_threads_per_disk, BUFFER_SIZE);

        Status status = io_mgr.Init(&mem_tracker);
        ASSERT_TRUE(status.ok());
        MemTracker reader_mem_tracker;
        DiskIoMgr::ReaderContext* reader;
        status = io_mgr.RegisterReader(NULL, &reader, &reader_mem_tracker);
        ASSERT_TRUE(status.ok());

        DiskIoMgr::ScanRange* complete_range = InitRange(1, tmp_file, 0, strlen(data), 0);

        // Issue some reads before the async ones are issued
        ValidateSyncRead(&io_mgr, reader, complete_range, data);
        ValidateSyncRead(&io_mgr, reader, complete_range, data);

        vector<DiskIoMgr::ScanRange*> ranges;
        for (int i = 0; i < len; ++i) {
          int disk_id = i % num_disks;
          ranges.push_back(InitRange(num_buffers, tmp_file, 0, len, disk_id));
        }
        status = io_mgr.AddScanRanges(reader, ranges);
        ASSERT_TRUE(status.ok());

        AtomicInt<int> num_ranges_processed;
        thread_group threads;
        for (int i = 0; i < 5; ++i) {
          threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader, data,
              Status::OK, 0, &num_ranges_processed));
        }

        // Issue some more sync ranges
        for (int i = 0; i < 5; ++i) {
          sched_yield();
          ValidateSyncRead(&io_mgr, reader, complete_range, data);
        }

        threads.join_all();

        ValidateSyncRead(&io_mgr, reader, complete_range, data);
        ValidateSyncRead(&io_mgr, reader, complete_range, data);

        EXPECT_EQ(num_ranges_processed, ranges.size());
        io_mgr.UnregisterReader(reader);
        EXPECT_EQ(reader_mem_tracker.consumption(), 0);
      }
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

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int num_buffers = 1; num_buffers <= 5; ++num_buffers) {
        pool_.reset(new ObjectPool);
        LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                  << " num_disk=" << num_disks << " num_buffers=" << num_buffers;

        if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
        DiskIoMgr io_mgr(num_disks, num_threads_per_disk, 1);

        Status status = io_mgr.Init(&mem_tracker);
        ASSERT_TRUE(status.ok());
        MemTracker reader_mem_tracker;
        DiskIoMgr::ReaderContext* reader;
        status = io_mgr.RegisterReader(NULL, &reader, &reader_mem_tracker);
        ASSERT_TRUE(status.ok());

        vector<DiskIoMgr::ScanRange*> ranges;
        for (int i = 0; i < len; ++i) {
          int disk_id = i % num_disks;
          ranges.push_back(InitRange(num_buffers, tmp_file, 0, len, disk_id));
        }
        status = io_mgr.AddScanRanges(reader, ranges);
        ASSERT_TRUE(status.ok());

        AtomicInt<int> num_ranges_processed;
        int num_succesful_ranges = ranges.size() / 2;
        // Read half the ranges
        for (int i = 0; i < num_succesful_ranges; ++i) {
          ScanRangeThread(&io_mgr, reader, data, Status::OK, 1, &num_ranges_processed);
        }
        EXPECT_EQ(num_ranges_processed, num_succesful_ranges);

        // Start up some threads and then cancel
        thread_group threads;
        for (int i = 0; i < 3; ++i) {
          threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader, data,
              Status::CANCELLED, 0, &num_ranges_processed));
        }

        io_mgr.CancelReader(reader);
        sched_yield();

        threads.join_all();
        EXPECT_TRUE(io_mgr.reader_status(reader).IsCancelled());
        io_mgr.UnregisterReader(reader);
        EXPECT_EQ(reader_mem_tracker.consumption(), 0);
      }
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

  const int num_buffers = 25;
  // Give the reader more buffers than the limit
  const int mem_limit_num_buffers = 2;

  int64_t iters = 0;
  {
    pool_.reset(new ObjectPool);
    if (++iters % 1000 == 0) LOG(ERROR) << "Starting iteration " << iters;

    MemTracker mem_tracker(mem_limit_num_buffers * BUFFER_SIZE);
    DiskIoMgr io_mgr(1, 1, BUFFER_SIZE);

    Status status = io_mgr.Init(&mem_tracker);
    ASSERT_TRUE(status.ok());
    MemTracker reader_mem_tracker;
    DiskIoMgr::ReaderContext* reader;
    status = io_mgr.RegisterReader(NULL, &reader, &reader_mem_tracker);
    ASSERT_TRUE(status.ok());

    vector<DiskIoMgr::ScanRange*> ranges;
    for (int i = 0; i < num_buffers; ++i) {
      ranges.push_back(InitRange(num_buffers, tmp_file, 0, len, 0));
    }
    status = io_mgr.AddScanRanges(reader, ranges);
    ASSERT_TRUE(status.ok());

    // Don't return buffers to force memory pressure
    vector<DiskIoMgr::BufferDescriptor*> buffers;

    AtomicInt<int> num_ranges_processed;
    ScanRangeThread(&io_mgr, reader, data, Status::MEM_LIMIT_EXCEEDED,
        1, &num_ranges_processed);

    char result[strlen(data) + 1];
    // Keep reading new ranges without returning buffers. This forces us
    // to go over the limit eventually.
    while (true) {
      memset(result, 0, strlen(data) + 1);
      DiskIoMgr::ScanRange* range = NULL;
      status = io_mgr.GetNextRange(reader, &range);
      ASSERT_TRUE(status.ok() || status.IsMemLimitExceeded());
      if (range == NULL) break;

      while (true) {
        DiskIoMgr::BufferDescriptor* buffer = NULL;
        Status status = range->GetNext(&buffer);
        ASSERT_TRUE(status.ok() || status.IsMemLimitExceeded());
        if (buffer == NULL) break;
        memcpy(result + range->offset() + buffer->scan_range_offset(),
            buffer->buffer(), buffer->len());
        buffers.push_back(buffer);
      }
      ValidateEmptyOrCorrect(data, result, strlen(data));
    }

    for (int i = 0; i < buffers.size(); ++i) {
      buffers[i]->Return();
    }

    EXPECT_TRUE(io_mgr.reader_status(reader).IsMemLimitExceeded());
    io_mgr.UnregisterReader(reader);
    EXPECT_EQ(reader_mem_tracker.consumption(), 0);
  }
}

// This test will test multiple concurrent reads each reading a different file.
TEST_F(DiskIoMgrTest, MultipleReader) {
  MemTracker mem_tracker(LARGE_MEM_LIMIT);
  const int NUM_READERS = 5;
  const int DATA_LEN = 50;
  const int ITERATIONS = 25;
  const int NUM_THREADS_PER_READER = 3;

  vector<string> file_names;
  vector<string> data;
  vector<DiskIoMgr::ReaderContext*> readers;
  vector<char*> results;

  file_names.resize(NUM_READERS);
  readers.resize(NUM_READERS);
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

    results[i] = new char[DATA_LEN + 1];
    memset(results[i], 0, DATA_LEN + 1);
  }

  // This exercises concurrency, run the test multiple times
  int64_t iters = 0;
  for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
    for (int threads_per_disk = 1; threads_per_disk <= 5; ++threads_per_disk) {
      for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
        for (int num_buffers = 1; num_buffers <= 5; ++num_buffers) {
          pool_.reset(new ObjectPool);
          LOG(INFO) << "Starting test with num_threads_per_disk=" << threads_per_disk
                    << " num_disk=" << num_disks << " num_buffers=" << num_buffers;
          if (++iters % 2500 == 0) LOG(ERROR) << "Starting iteration " << iters;

          DiskIoMgr io_mgr(num_disks, threads_per_disk, BUFFER_SIZE);
          Status status = io_mgr.Init(&mem_tracker);
          ASSERT_TRUE(status.ok());

          for (int i = 0; i < NUM_READERS; ++i) {
            status = io_mgr.RegisterReader(NULL, &readers[i], NULL);
            ASSERT_TRUE(status.ok());

            vector<DiskIoMgr::ScanRange*> ranges;
            for (int j = 0; j < DATA_LEN; ++j) {
              int disk_id = j % num_disks;
              ranges.push_back(
                  InitRange(num_buffers,file_names[i].c_str(), j, 1, disk_id));
            }
            status = io_mgr.AddScanRanges(readers[i], ranges);
            ASSERT_TRUE(status.ok());
          }

          AtomicInt<int> num_ranges_processed;
          thread_group threads;
          for (int i = 0; i < NUM_READERS; ++i) {
            for (int j = 0; j < NUM_THREADS_PER_READER; ++j) {
              threads.add_thread(new thread(ScanRangeThread, &io_mgr, readers[i],
                  data[i].c_str(), Status::OK, 0, &num_ranges_processed));
            }
          }
          threads.join_all();
          EXPECT_EQ(num_ranges_processed, DATA_LEN * NUM_READERS);
          for (int i = 0; i < NUM_READERS; ++i) {
            io_mgr.UnregisterReader(readers[i]);
          }
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

}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  impala::InitThreading();
  return RUN_ALL_TESTS();
}
