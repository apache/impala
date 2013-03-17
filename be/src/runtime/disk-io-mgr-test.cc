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

#include <boost/thread/thread.hpp>

#include <gtest/gtest.h>

#include "codegen/llvm-codegen.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/disk-io-mgr-stress.h"
#include "util/cpu-info.h"

using namespace std;
using namespace boost;

const int BUFFER_SIZE = 1024;

namespace impala {

class DiskIoMgrTest : public testing::Test {
 protected:
  void CreateTempFile(const char* filename, const char* data) {
    FILE* file = fopen(filename, "w");
    EXPECT_TRUE(file != NULL);
    fwrite(data, 1, strlen(data), file);
    fclose(file);
  }

  static void ValidateRead(DiskIoMgr* io_mgr, DiskIoMgr::ReaderContext* reader, 
      const char* expected) {
    int len = strlen(expected);
  
    Status status;
    DiskIoMgr::BufferDescriptor* buffer;
    bool eos = true;

    for (int i = 0; i < len; ++i) {
      status = io_mgr->GetNext(reader, &buffer, &eos);
      EXPECT_TRUE(status.ok());
      ASSERT_TRUE(buffer != NULL);
      ASSERT_TRUE(buffer->buffer() != NULL);
      EXPECT_TRUE(buffer->eosr());
      EXPECT_EQ(eos, i == len -1);
      ASSERT_EQ(buffer->len(), 1);
      EXPECT_EQ(buffer->buffer()[0], expected[buffer->scan_range()->offset()]);
      buffer->Return();
    }
  
    EXPECT_TRUE(eos);
  }

  static void ValidateSyncRead(DiskIoMgr* io_mgr, DiskIoMgr::ReaderContext* reader,
      DiskIoMgr::ScanRange* range, const char* expected) {
    DiskIoMgr::BufferDescriptor* buffer;
    Status status = io_mgr->Read(NULL, range, &buffer);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(buffer != NULL);
    EXPECT_EQ(buffer->len(), range->len());
    EXPECT_TRUE(strncmp(buffer->buffer(), expected, strlen(expected)) == 0);
    buffer->Return();
  }

  DiskIoMgr::ScanRange* InitRange(const char* file_path, int offset, 
      int len, int disk_id, void* meta_data = NULL) {
    DiskIoMgr::ScanRange* range = pool_.Add(new DiskIoMgr::ScanRange());
    range->Reset(file_path, len, offset, disk_id, meta_data);
    return range;
  }

  ObjectPool pool_;
};

// Basic test with a single reader, testing multiple threads, disks and a different
// number of buffers.
TEST_F(DiskIoMgrTest, SingleReader) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghjijklm";
  CreateTempFile(tmp_file, data);

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int num_buffers = 1; num_buffers <= 11; num_buffers += 2) {
        LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                  << " num_disk=" << num_disks << " num_buffers=" << num_buffers;
        
        if (++iters % 10000 == 0) LOG(ERROR) << "Starting iteration " << iters;

        DiskIoMgr io_mgr(num_disks, num_threads_per_disk, BUFFER_SIZE);
        Status status = io_mgr.Init();
        ASSERT_TRUE(status.ok());
        DiskIoMgr::ReaderContext* reader;
        status = io_mgr.RegisterReader(NULL, num_buffers, 0, &reader);
        ASSERT_TRUE(status.ok());

        vector<DiskIoMgr::ScanRange*> ranges;
        for (int i = 0; i < strlen(data); ++i) {
          int disk_id = i % num_disks;
          ranges.push_back(InitRange(tmp_file, i, 1, disk_id));
        }
      
        status = io_mgr.AddScanRanges(reader, ranges);
        ASSERT_TRUE(status.ok());

        ValidateRead(&io_mgr, reader, data);

        io_mgr.UnregisterReader(reader);
      }
    }
  }
}

// Tests a single reader cancelling half way through scan ranges.  
TEST_F(DiskIoMgrTest, SingleReaderCancel) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghjijklm";
  CreateTempFile(tmp_file, data);
          
  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int num_buffers = 1; num_buffers <= 11; num_buffers += 2) {
        LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                  << " num_disk=" << num_disks << " num_buffers=" << num_buffers;
        if (++iters % 10000 == 0) LOG(ERROR) << "Starting iteration " << iters;

        DiskIoMgr io_mgr(num_disks, num_threads_per_disk, BUFFER_SIZE);
        DiskIoMgr::ReaderContext* reader;
        Status status = io_mgr.Init();
        ASSERT_TRUE(status.ok());
        status = io_mgr.RegisterReader(NULL, num_buffers, 0, &reader);
        ASSERT_TRUE(status.ok());

        vector<DiskIoMgr::ScanRange*> ranges;
        for (int i = 0; i < strlen(data); ++i) {
          int disk_id = i % num_disks;
          ranges.push_back(InitRange(tmp_file, i, 1, disk_id));
        }
        io_mgr.AddScanRanges(reader, ranges);

        DiskIoMgr::BufferDescriptor* buffer;
        bool eos;
        status = io_mgr.GetNext(reader, &buffer, &eos);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(buffer != NULL);
        EXPECT_FALSE(eos);
        buffer->Return();

        // At this point there should be some number of buffers read and multiple
        // scan ranges queued
        io_mgr.CancelReader(reader);
        status = io_mgr.GetNext(reader, &buffer, &eos);
        EXPECT_TRUE(status.IsCancelled());
        EXPECT_TRUE(buffer == NULL);

        io_mgr.UnregisterReader(reader);
        // The io_mgr destructor asserts that everything is cleaned up.
      }
    }
  }
}

// This test issues adding additional scan ranges while there are some still in flight.
TEST_F(DiskIoMgrTest, AddScanRangeTest) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklm";
  CreateTempFile(tmp_file, data);
  
  char result[strlen(data)];

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int num_buffers = 1; num_buffers <= 11; num_buffers += 2) {
        LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                  << " num_disk=" << num_disks << " num_buffers=" << num_buffers;
        if (++iters % 10000 == 0) LOG(ERROR) << "Starting iteration " << iters;
        
        DiskIoMgr io_mgr(num_disks, num_threads_per_disk, BUFFER_SIZE);
        Status status = io_mgr.Init();
        ASSERT_TRUE(status.ok());
        DiskIoMgr::ReaderContext* reader;
        status = io_mgr.RegisterReader(NULL, num_buffers, 0, &reader);
        ASSERT_TRUE(status.ok());
          
        memset(result, 0, strlen(data));
          
        vector<DiskIoMgr::ScanRange*> ranges_first_half;
        vector<DiskIoMgr::ScanRange*> ranges_second_half;
        for (int i = 0; i < strlen(data); ++i) {
          int disk_id = i % num_disks;
          if (i > strlen(data) / 2) {
            ranges_second_half.push_back(InitRange(tmp_file, i, 1, disk_id));
          } else {
            ranges_first_half.push_back(InitRange(tmp_file, i, 1, disk_id));
          }
        }
        // Issue first half the scan ranges.
        io_mgr.AddScanRanges(reader, ranges_first_half);
        
        // Call get next a couple of times 
        for (int i = 0; i < 2; ++i) {
          DiskIoMgr::BufferDescriptor* buffer;
          bool eos;
          status = io_mgr.GetNext(reader, &buffer, &eos);
          ASSERT_TRUE(status.ok());
          ASSERT_TRUE(buffer != NULL);
          EXPECT_FALSE(eos);
          EXPECT_TRUE(buffer->eosr());
          result[buffer->scan_range()->offset()] = buffer->buffer()[0];
          EXPECT_EQ(buffer->len(), 1);
          buffer->Return();
        }

        // Issue second half
        io_mgr.AddScanRanges(reader, ranges_second_half);

        // Read all the scan ranges and validate the result.
        bool eos = false;
        do {
          DiskIoMgr::BufferDescriptor* buffer;
          status = io_mgr.GetNext(reader, &buffer, &eos);
          ASSERT_TRUE(status.ok());
          ASSERT_TRUE(buffer != NULL);
          EXPECT_TRUE(buffer->eosr());
          result[buffer->scan_range()->offset()] = buffer->buffer()[0];
          EXPECT_EQ(buffer->len(), 1);
          buffer->Return();
        } while (!eos);

        EXPECT_TRUE(strncmp(data, result, strlen(data)) == 0);

        io_mgr.UnregisterReader(reader);
      }
    }
  }
}

// Test to make sure that sync reads and async reads work together
// Note: this test is constructed so the number of buffers is greater than the
// number of scan ranges.
TEST_F(DiskIoMgrTest, SyncReadTest) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcde";
  CreateTempFile(tmp_file, data);
          
  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int num_buffers = 1; num_buffers <= 11; num_buffers += 2) {
        LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                  << " num_disk=" << num_disks << " num_buffers=" << num_buffers;
        if (++iters % 10000 == 0) LOG(ERROR) << "Starting iteration " << iters;
        
        DiskIoMgr io_mgr(num_disks, num_threads_per_disk, BUFFER_SIZE);
        Status status = io_mgr.Init();
        ASSERT_TRUE(status.ok());
        DiskIoMgr::ReaderContext* reader;
        status = io_mgr.RegisterReader(NULL, num_buffers, 0, &reader);
        ASSERT_TRUE(status.ok());
          
        DiskIoMgr::ScanRange* complete_range = InitRange(tmp_file, 0, strlen(data), 0);

        // Issue some reads before the async ones are issued
        ValidateSyncRead(&io_mgr, reader, complete_range, data);
        ValidateSyncRead(&io_mgr, reader, complete_range, data);

        vector<DiskIoMgr::ScanRange*> ranges;
        for (int i = 0; i < strlen(data); ++i) {
          int disk_id = i % num_disks;
          ranges.push_back(InitRange(tmp_file, i, 1, disk_id));
        }
        io_mgr.AddScanRanges(reader, ranges);

        // A bunch of scan ranges are added.  Issue sync reads and make sure they complete
        ValidateSyncRead(&io_mgr, reader, complete_range, data);
        ValidateSyncRead(&io_mgr, reader, complete_range, data);

        // Call get next a couple of times 
        char result[strlen(data)];
        memset(result, 0, strlen(data));

        int bytes_read = 0;
        for (; bytes_read < 2; ++bytes_read) {
          DiskIoMgr::BufferDescriptor* buffer;
          bool eos;
          status = io_mgr.GetNext(reader, &buffer, &eos);
          ASSERT_TRUE(status.ok());
          ASSERT_TRUE(buffer != NULL);
          EXPECT_FALSE(eos);
          EXPECT_TRUE(buffer->eosr());
          result[buffer->scan_range()->offset()] = buffer->buffer()[0];
          EXPECT_EQ(buffer->len(), 1);
          buffer->Return();
        }

        // Issue two more sync read
        ValidateSyncRead(&io_mgr, reader, complete_range, data);
        ValidateSyncRead(&io_mgr, reader, complete_range, data);

        // Finish up the async reads
        for (; bytes_read < strlen(data); ++bytes_read) {
          DiskIoMgr::BufferDescriptor* buffer;
          bool eos;
          status = io_mgr.GetNext(reader, &buffer, &eos);
          ASSERT_TRUE(status.ok());
          ASSERT_TRUE(buffer != NULL);
          EXPECT_EQ(eos, bytes_read == strlen(data) -1);
          EXPECT_TRUE(buffer->eosr());
          result[buffer->scan_range()->offset()] = buffer->buffer()[0];
          EXPECT_EQ(buffer->len(), 1);
          buffer->Return();
        }

        // Validate async read result
        EXPECT_TRUE(strncmp(data, result, strlen(data)) == 0);

        // One additional buffer could have been allocated for the sync read
        io_mgr.UnregisterReader(reader);
      }
    }
  }
}

// This test will test multiple concurrent reads each reading a different file.
TEST_F(DiskIoMgrTest, MultipleReader) {
  const int NUM_THREADS = 5;
  const int DATA_LEN = 50;
  const int ITERATIONS = 25;

  vector<string> file_names;
  vector<string> data;
  vector<DiskIoMgr::ReaderContext*> readers;
  file_names.resize(NUM_THREADS);
  readers.resize(NUM_THREADS);
  data.resize(NUM_THREADS);

  // Initialize data for each thread.  The data will be 
  // 'abcd...' for thread one, 'bcde...' for thread two (wrapping around at 'z')
  for (int i = 0; i < NUM_THREADS; ++i) {
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
  }

  // This exercises concurrency, run the test multiple times
  int64_t iters = 0;
  for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
    for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
      for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
        for (int num_buffers = 1; num_buffers <= 11; num_buffers += 2) {
          LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                    << " num_disk=" << num_disks << " num_buffers=" << num_buffers;
          if (++iters % 2500 == 0) LOG(ERROR) << "Starting iteration " << iters;
        
          DiskIoMgr io_mgr(num_disks, num_threads_per_disk, BUFFER_SIZE);
          Status status = io_mgr.Init();
          ASSERT_TRUE(status.ok());

          for (int i = 0; i < NUM_THREADS; ++i) {
            status = io_mgr.RegisterReader(NULL, num_buffers, 0, &readers[i]);
            ASSERT_TRUE(status.ok());
          
            vector<DiskIoMgr::ScanRange*> ranges;
            for (int j = 0; j < DATA_LEN; ++j) {
              int disk_id = j % num_disks;
              ranges.push_back(InitRange(file_names[i].c_str(), j, 1, disk_id));
            }
            status = io_mgr.AddScanRanges(readers[i], ranges);
            ASSERT_TRUE(status.ok());
          }

          thread_group threads;
          for (int i = 0; i < NUM_THREADS; ++i) {
            threads.add_thread(new thread(&DiskIoMgrTest::ValidateRead, &io_mgr, 
                readers[i], data[i].c_str()));
          }
          threads.join_all();

          for (int i = 0; i < NUM_THREADS; ++i) {
            io_mgr.UnregisterReader(readers[i]);
          }
        }
      }
    }
  }
}

// This test is going to dynamically adjust the number of buffers assigned to
// a reader.
TEST_F(DiskIoMgrTest, UpdateBufferQuotaTest) {
  int MAX_BUFFERS = 7;
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklmnopqrstuvwxyz";
  CreateTempFile(tmp_file, data);

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int adding = 0; adding < 1; ++adding) {
        LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                  << " num_disk=" << num_disks << " adding=" << adding;
        if (++iters % 1000 == 0) LOG(ERROR) << "Starting iteration " << iters;

        DiskIoMgr io_mgr(num_disks, num_threads_per_disk, 1);
        Status status = io_mgr.Init();
        ASSERT_TRUE(status.ok());
        
        // Start with one buffer and work our way up or the max buffers and go down.
        int num_buffers = adding ? 1 : MAX_BUFFERS;
        DiskIoMgr::ReaderContext* reader;
        status = io_mgr.RegisterReader(NULL, num_buffers, 0, &reader);
        ASSERT_TRUE(status.ok());
          
        vector<DiskIoMgr::ScanRange*> ranges;
        for (int i = 0; i < strlen(data); ++i) {
          int disk_id = i % num_disks;
          ranges.push_back(InitRange(tmp_file, i, 1, disk_id));
        }
        status = io_mgr.AddScanRanges(reader, ranges);
        ASSERT_TRUE(status.ok());

        bool eos = false;
        while (!eos) {
          // Should be able to read once for each buffer
          list<DiskIoMgr::BufferDescriptor*> buffers;
          DiskIoMgr::BufferDescriptor* buffer;
          for (int i = 0; i < num_buffers; ++i) {
            status = io_mgr.GetNext(reader, &buffer, &eos);
            EXPECT_TRUE(status.ok());
            ASSERT_TRUE(buffer != NULL);
            ASSERT_TRUE(buffer->buffer() != NULL);
            EXPECT_TRUE(buffer->eosr());
            ASSERT_EQ(buffer->len(), 1);
            EXPECT_EQ(buffer->buffer()[0], data[buffer->scan_range()->offset()]);
            buffers.push_back(buffer);
            if (eos) break;
          }
          num_buffers += (adding ? 1 : -1);
          num_buffers = ::min(num_buffers, MAX_BUFFERS);
          num_buffers = ::max(num_buffers, 1);
          bool ok = io_mgr.SetMaxIoBuffers(reader, num_buffers);
          EXPECT_TRUE(ok);

          for (list<DiskIoMgr::BufferDescriptor*>::iterator it = buffers.begin();
              it != buffers.end(); ++it) {
            (*it)->Return();
          }
        }
        io_mgr.UnregisterReader(reader);
      }
    }
  }
}

// This tests exercises the grouped scan range functionality.
TEST_F(DiskIoMgrTest, ScanRangeGroupTest) {
  int64_t return_buffer_idx = 0;
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklmnopqrstuvwxyz";
  CreateTempFile(tmp_file, data);

  int data_len = strlen(data);
  char result[data_len];

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int group_size = 1; group_size < 5; ++group_size) {
        // There are two interesting num_buffer settings.  
        // 1) The number of buffers is less than the group.  The IO mgr needs to increase 
        // the number to at least the group size.
        // 2) The number is more, in which case the IO mgr can buffer some ranges in a 
        // group more.
        for (int num_buffers = 1; num_buffers < 2; num_buffers += group_size) {
          LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                    << " num_disk=" << num_disks << " num_buffers=" << num_buffers
                    << " group_size=" << group_size;
          if (++iters % 1000 == 0) LOG(ERROR) << "Starting iteration " << iters;

          memset(result, 0, strlen(data));
          DiskIoMgr io_mgr(num_disks, num_threads_per_disk, 1);
          Status status = io_mgr.Init();
          ASSERT_TRUE(status.ok());

          DiskIoMgr::ReaderContext* reader;
          status = io_mgr.RegisterReader(NULL, num_buffers, 0, &reader);
          ASSERT_TRUE(status.ok());
        
          // Make one range that is the entire file for each group
          DiskIoMgr::ScanRangeGroup group;
          for (int i = 0; i < group_size; ++i) {
            int disk_id = i % num_disks;
            group.ranges.push_back(
                InitRange(tmp_file, 0, data_len, disk_id, reinterpret_cast<void*>(i)));
          }

          // Create a vector of list of buffers.  There is a list per column.
          vector<list<DiskIoMgr::BufferDescriptor*> > buffers;
          buffers.resize(group_size);
        
          vector<DiskIoMgr::ScanRangeGroup*> groups;
          groups.push_back(&group);
          status = io_mgr.AddScanRangeGroups(reader, groups);
          ASSERT_TRUE(status.ok());

          // Until all the ranges are read, call TryGetNext until all the buffers are
          // exhausted.  Then return one buffer and repeat.  Whenever TryGetNext
          // fails to return a buffer, we verify that at least one buffer has been
          // returned for each scan range.  This guarantees the grouping functionality
          // is correct.
          bool try_can_fail = false;
          while (true) {
            bool eos;
            DiskIoMgr::BufferDescriptor* buffer;
            status = io_mgr.TryGetNext(reader, &buffer, &eos);
            ASSERT_TRUE(status.ok());

            if (buffer != NULL) {
              try_can_fail = true;
              EXPECT_EQ(buffer->len(), 1);
              result[buffer->scan_range_offset()] = buffer->buffer()[0];
              long group_idx = reinterpret_cast<long>(buffer->scan_range()->meta_data());
              ASSERT_GE(group_idx, 0);
              ASSERT_LT(group_idx, group_size);
              buffers[group_idx].push_back(buffer);

              if (eos) {
                EXPECT_TRUE(strncmp(data, result, strlen(data)) == 0);
                // Clean up.  Return all buffers.
                for (int i = 0; i < buffers.size(); ++i) {
                  int num_buffers = buffers[i].size();
                  EXPECT_GT(num_buffers, 0);
                  for (int j = 0; j < num_buffers; ++j) {
                    DiskIoMgr::BufferDescriptor* return_buffer = buffers[i].front();
                    buffers[i].pop_front();
                    if (return_buffer != NULL) return_buffer->Return();
                  }
                }
                break;
              }
            } else {
              EXPECT_TRUE(try_can_fail);
              // All buffers are exhausted - verify that there is at least one buffer
              // per group queued.
              for (int i = 0; i < buffers.size(); ++i) {
                EXPECT_GT(buffers[i].size(), 0);
              }

              // Return a "random" buffer
              int return_idx = return_buffer_idx % group_size;
              ++return_buffer_idx;

              for (int i = 0; i < buffers.size(); ++i) {
                int idx = (return_idx + i) % group_size;
                list<DiskIoMgr::BufferDescriptor*>& l = buffers[idx];
                DiskIoMgr::BufferDescriptor* return_buffer = l.front();
                // This scan range is all done, return the next one.
                if (return_buffer == NULL) continue;
                l.pop_front();
                if (return_buffer->eosr()) {
                  // Push a NULL as a sentinel that this range is done.
                  l.push_back(NULL);
                }
                // Return the buffer which should trigger the IO mgr to read the 
                // next thing.
                return_buffer->Return();
                try_can_fail = false;
              }
            }
          }
        
          // End of test case for these configs
          io_mgr.UnregisterReader(reader);
        }
      }
    }
  }
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
  return RUN_ALL_TESTS();
}

