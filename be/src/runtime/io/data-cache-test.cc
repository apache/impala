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

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <gflags/gflags.h>
#include <rapidjson/document.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <sys/sysmacros.h>
#include <sys/types.h>

#include "gutil/strings/join.h"
#include "gutil/strings/util.h"
#include "runtime/io/data-cache.h"
#include "runtime/io/data-cache-trace.h"
#include "runtime/io/request-ranges.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "testutil/scoped-flag-setter.h"
#include "util/counting-barrier.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"
#include "util/simple-logger.h"
#include "util/thread.h"

#include "common/names.h"

#define BASE_CACHE_DIR     "/tmp"
#define FNAME              ("foobar")
#define NUM_TEST_DIRS      (16)
#define NUM_THREADS        (18)
#define MTIME              (12345)
#define TEMP_BUFFER_SIZE   (4096)
#define TEST_BUFFER_SIZE   (8192)
#define NUM_CACHE_ENTRIES  (1024)
#define DEFAULT_CACHE_SIZE (NUM_CACHE_ENTRIES * TEMP_BUFFER_SIZE)

// The NUM_CACHE_ENTRIES_NO_EVICT says how many entries can be inserted into the cache
// and queried without any evictions. This is smaller than NUM_CACHE_ENTRIES to
// provide a bit of flexibility for cache implementations like LIRS that have multiple
// segments. The segments may not split cleanly along entries of size TEMP_BUFFER_SIZE,
// so it can evict something when adding NUM_CACHE_ENTRIES.
#define NUM_CACHE_ENTRIES_NO_EVICT (NUM_CACHE_ENTRIES - 1)

DECLARE_bool(cache_force_single_shard);
DECLARE_bool(data_cache_anonymize_trace);
DECLARE_bool(data_cache_enable_tracing);
DECLARE_int64(data_cache_file_max_size_bytes);
DECLARE_int32(data_cache_max_opened_files);
DECLARE_int32(data_cache_write_concurrency);
DECLARE_string(data_cache_eviction_policy);
DECLARE_string(data_cache_trace_dir);
DECLARE_int32(max_data_cache_trace_file_size);
DECLARE_int32(data_cache_trace_percentage);
DECLARE_int32(data_cache_num_async_write_threads);
DECLARE_string(data_cache_async_write_buffer_limit);
DECLARE_bool(data_cache_keep_across_restarts);

namespace impala {
namespace io {

using boost::filesystem::path;

struct DataCacheTestParam {
  DataCacheTestParam(const std::string& eviction_policy, int32 num_write_threads)
    : eviction_policy(eviction_policy),
      num_write_threads(num_write_threads) { }
  std::string eviction_policy;
  int32 num_write_threads;
};

class DataCacheBaseTest : public testing::Test {
 public:
  const uint8_t* test_buffer() {
    return reinterpret_cast<const uint8_t*>(test_buffer_);
  }

  const std::vector<string>& data_cache_dirs() {
    return data_cache_dirs_;
  }

  const string& data_cache_trace_dir() {
    return data_cache_trace_dir_;
  }

  /// A function for testing, we need to make sure that all the store tasks have completed
  /// before any lookup when running test case in async write mode.
  static void WaitForAsyncWrite(const DataCache& cache) {
    while (cache.current_buffer_size_.Load() != 0) {
      usleep(500);
    }
  }

  //
  // Use multiple threads to insert and read back a set of ranges from test_buffer().
  // Depending on the setting, the working set may or may not fit in the cache.
  //
  // 'cache_size'              : cache size in byte
  // 'max_start_offset'        : maximum offset in test_buffer() to start copying from
  //                             into the cache for a given entry
  // 'use_per_thread_filename' : If true, the filename used when inserting into the cache
  //                             will be prefixed with the thread name, which is unique
  //                             per thread; If false, the prefix for filenames is empty
  // 'expect_misses'           : If true, expect there will be cache misses when reading
  //                             from the cache
  // 'test_readonly'           : Only for testing read-only, if true, set data cache
  //                             read-only after create all threads, and check number of
  //                             writes later
  //
  void MultiThreadedReadWrite(DataCache* cache, int64_t max_start_offset,
      bool use_per_thread_filename, bool expect_misses, double* write_time_ms,
      bool test_readonly = false) {
    // Barrier to synchronize all threads so no thread will start probing the cache until
    // all insertions are done.
    CountingBarrier barrier(NUM_THREADS);

    vector<unique_ptr<Thread>> threads;
    int num_misses[NUM_THREADS];
    int64_t write_times_us[NUM_THREADS];
    int64_t num_writes = 0;
    for (int i = 0; i < NUM_THREADS; ++i) {
      unique_ptr<Thread> thread;
      num_misses[i] = 0;
      string thread_name = Substitute("thread-$0", i);
      ASSERT_OK(Thread::Create("data-cache-test", thread_name,
          boost::bind(&DataCacheBaseTest::ThreadFn, this,
             use_per_thread_filename ? thread_name : "", cache, max_start_offset,
             &barrier, &num_misses[i], &write_times_us[i]), &thread));
      threads.emplace_back(std::move(thread));
      // If testing read only, set it read only after starting half of the threads.
      if (i == (NUM_THREADS / 2) && test_readonly) {
        // Set read-only before the writes are complete, and record the total write count
        // of the cache after read-only set.
        num_writes = cache->SetDataCacheReadOnly();
      }
    }

    int cache_misses = 0;
    int64_t avg_write_time_us = 0;
    for (int i = 0; i < NUM_THREADS; ++i) {
      threads[i]->Join();
      cache_misses += num_misses[i];
      avg_write_time_us += write_times_us[i];
    }
    *write_time_ms = static_cast<double>(avg_write_time_us / NUM_THREADS) / 1000;
    if (expect_misses) {
      ASSERT_GT(cache_misses, 0);
    } else {
      ASSERT_EQ(0, cache_misses);
    }

    if (test_readonly) {
      // Revoke read-only and check the total write count, which should not have increased
      // due to read-only.
      ASSERT_EQ(num_writes, cache->RevokeDataCacheReadOnly());
      return;
    }

    // Verify the backing files don't exceed size limits.
    ASSERT_OK(cache->CloseFilesAndVerifySizes());
  }

  void MockDisk(std::string name, bool is_rotational) {
    mock_fs_ = "/tmp" / boost::filesystem::unique_path();
    ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(mock_fs_.string()));
    boost::filesystem::path proc_fs = mock_fs_ / "proc";
    ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(proc_fs.string()));
    boost::filesystem::path sys_fs = mock_fs_ / "sys";
    boost::filesystem::path dev_queue = sys_fs / "block" / name / "queue";
    ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(dev_queue.string()));

    {
      struct stat s;
      stat(BASE_CACHE_DIR, &s);
      std::ofstream partitions((proc_fs / "partitions").string());
      partitions << "major minor #blocks name\n";
      partitions << major(s.st_dev) << " " << minor(s.st_dev) << " 1 " << name << "\n";

      std::ofstream rotational((dev_queue / "rotational").string());
      rotational << (is_rotational ? "1" : "0");
    }

    DiskInfo::Init(proc_fs.string(), sys_fs.string());
  }

 protected:
  DataCacheBaseTest() {
    // Create a buffer of random characters.
    for (int i = 0; i < TEST_BUFFER_SIZE; ++i) {
      test_buffer_[i] = '!' + (rand() % 93);
    }
  }

  // Create a bunch of test directories in which the data cache will reside.
  void SetupWithParameters(DataCacheTestParam param) {
    test_env_.reset(new TestEnv());
    flag_saver_.reset(new google::FlagSaver());
    ASSERT_OK(test_env_->Init());
    for (int i = 0; i < NUM_TEST_DIRS; ++i) {
      const string& path = Substitute("$0/data-cache-test.$1", BASE_CACHE_DIR, i);
      ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(path));
      data_cache_dirs_.push_back(path);
    }
    data_cache_trace_dir_ = ("/tmp" / boost::filesystem::unique_path()).string();
    ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(data_cache_trace_dir_));

    // Force a single shard to avoid imbalance between shards in the Kudu LRU cache which
    // may lead to unintended eviction. This allows the capacity of the cache is utilized
    // to the limit for the purpose of this test.
    FLAGS_cache_force_single_shard = true;

    // Allows full write concurrency for the multi-threaded tests.
    FLAGS_data_cache_write_concurrency = NUM_THREADS;

    // This is parameterized to allow testing LRU and LIRS
    FLAGS_data_cache_eviction_policy = param.eviction_policy;

    // This is parameterized to allow testing asynchronous write.
    FLAGS_data_cache_num_async_write_threads = param.num_write_threads;
    FLAGS_data_cache_async_write_buffer_limit = "1G";
  }

  // Delete all the test directories created.
  virtual void TearDown() {
    // Make sure the cache's destructor removes all backing files, except for
    // potentially the trace file.
    for (const string& dir_path : data_cache_dirs_) {
      vector<string> entries;
      ASSERT_OK(FileSystemUtil::Directory::GetEntryNames(dir_path, &entries));
      ASSERT_EQ(0, entries.size());
    }
    ASSERT_OK(FileSystemUtil::RemovePaths(data_cache_dirs_));
    boost::filesystem::remove_all(path(data_cache_trace_dir_));
    flag_saver_.reset();
    test_env_.reset();

    // Reset DiskInfo in case it was mocked.
    if (!mock_fs_.empty()) {
      boost::filesystem::remove_all(mock_fs_);
      mock_fs_.clear();
    }
    DiskInfo::Init();
  }

 private:
  std::unique_ptr<TestEnv> test_env_;
  char test_buffer_[TEST_BUFFER_SIZE];
  vector<string> data_cache_dirs_;
  string data_cache_trace_dir_;
  boost::filesystem::path mock_fs_;

  // Saved configuration flags for restoring the values at the end of the test.
  std::unique_ptr<google::FlagSaver> flag_saver_;

  // The function is invoked by each thread in the multi-threaded tests below.
  // It inserts chunks of size TEMP_BUFFER_SIZE from different offsets at range
  // [0, 'max_test_offset') from 'test_buffer_'. Afterwards, it tries reading back
  // all the inserted chunks.
  //
  // 'fname_prefix' is the prefix added to the default filename when inserting into
  // the cache. 'max_test_offset' and 'fname_prefix' implicitly control the combination
  // of cache keys which indirectly control the cache footprint. 'num_misses' records
  // the number of cache misses.
  void ThreadFn(const string& fname_prefix, DataCache* cache, int64_t max_start_offset,
      CountingBarrier* store_barrier, int* num_misses, int64_t* write_time_us) {
    const string& custom_fname = Substitute("$0file", fname_prefix);
    vector<int64_t> offsets;
    for (int64_t offset = 0; offset < max_start_offset; ++offset) {
      offsets.push_back(offset);
    }
    random_shuffle(offsets.begin(), offsets.end());
    int64_t write_start_time = UnixMicros();
    for (int64_t offset : offsets) {
      cache->Store(custom_fname, MTIME, offset, test_buffer() + offset,
          TEMP_BUFFER_SIZE);
    }
    *write_time_us = UnixMicros() - write_start_time;
    // Wait until all threads have finished inserting. Since different threads may be
    // inserting the same cache key and collide, only one thread which wins the race will
    // insert the cache entry. Make sure other threads which lose out on the race will
    // wait for the insertion to complete first before proceeding.
    store_barrier->Notify();
    store_barrier->Wait();

    // Before continue, we should wait for all asynchronous store tasks (if there are)
    // finished.
    WaitForAsyncWrite(*cache);
    for (int64_t offset : offsets) {
      uint8_t buffer[TEMP_BUFFER_SIZE];
      memset(buffer, 0, TEMP_BUFFER_SIZE);
      int64_t bytes_read =
          cache->Lookup(custom_fname, MTIME, offset, TEMP_BUFFER_SIZE, buffer);
      if (bytes_read == TEMP_BUFFER_SIZE) {
        ASSERT_EQ(0, memcmp(buffer, test_buffer() + offset, TEMP_BUFFER_SIZE));
      } else {
        ASSERT_EQ(bytes_read, 0);
        ++(*num_misses);
      }
    }
  }
};

class DataCacheTest :
    public DataCacheBaseTest,
    public ::testing::WithParamInterface<DataCacheTestParam> {
 public:
  DataCacheTest()
      : DataCacheBaseTest() {
  }

  void SetUp() override {
    const auto& param = GetParam();
    SetupWithParameters(param);
  }
};

INSTANTIATE_TEST_SUITE_P(DataCacheTestTypes, DataCacheTest,::testing::Values(
    DataCacheTestParam("LRU", 0),
    DataCacheTestParam("LRU", NUM_THREADS),
    DataCacheTestParam("LIRS", 0),
    DataCacheTestParam("LIRS", NUM_THREADS)));

// This test exercises the basic insertion and lookup paths by inserting a known set of
// offsets which fit in the cache entirely. Also tries reading entries which are never
// inserted into the cache.
TEST_P(DataCacheTest, TestBasics) {
  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());

  // Temporary buffer for holding results read from the cache.
  uint8_t buffer[TEMP_BUFFER_SIZE];
  // Read and then insert a range of offsets. Expected all misses in the first iteration
  // and all hits in the second iteration.
  for (int i = 0; i < 2; ++i) {
    for (int64_t offset = 0; offset < NUM_CACHE_ENTRIES_NO_EVICT; ++offset) {
      int expected_bytes = i * TEMP_BUFFER_SIZE;
      memset(buffer, 0, TEMP_BUFFER_SIZE);
      ASSERT_EQ(expected_bytes,
          cache.Lookup(FNAME, MTIME, offset, TEMP_BUFFER_SIZE, buffer)) << offset;
      if (i == 0) {
        ASSERT_TRUE(cache.Store(FNAME, MTIME, offset, test_buffer() + offset,
            TEMP_BUFFER_SIZE));
      } else {
        ASSERT_EQ(0, memcmp(test_buffer() + offset, buffer, TEMP_BUFFER_SIZE));
      }
    }
    // Before continue, we should wait for all asynchronous store tasks (if there are)
    // finished.
    WaitForAsyncWrite(cache);
  }

  // Read the same range inserted previously but with a different filename.
  for (int64_t offset = NUM_CACHE_ENTRIES_NO_EVICT; offset < TEST_BUFFER_SIZE;
       ++offset) {
    const string& alt_fname = "random";
    ASSERT_EQ(0, cache.Lookup(alt_fname, MTIME, offset, TEMP_BUFFER_SIZE, buffer));
  }

  // Read the same range inserted previously but with a different mtime.
  for (int64_t offset = NUM_CACHE_ENTRIES_NO_EVICT; offset < TEST_BUFFER_SIZE;
       ++offset) {
    int64_t alt_mtime = 67890;
    ASSERT_EQ(0, cache.Lookup(FNAME, alt_mtime, offset, TEMP_BUFFER_SIZE, buffer));
  }

  // Read a range of offsets which should miss in the cache.
  for (int64_t offset = NUM_CACHE_ENTRIES_NO_EVICT; offset < TEST_BUFFER_SIZE;
       ++offset) {
    ASSERT_EQ(0, cache.Lookup(FNAME, MTIME, offset, TEMP_BUFFER_SIZE, buffer));
  }

  // Read the same same range inserted previously. They should still all be in the cache.
  for (int64_t offset = 0; offset < NUM_CACHE_ENTRIES_NO_EVICT; ++offset) {
    memset(buffer, 0, TEMP_BUFFER_SIZE);
    ASSERT_EQ(TEMP_BUFFER_SIZE,
        cache.Lookup(FNAME, MTIME, offset, TEMP_BUFFER_SIZE + 10, buffer));
    ASSERT_EQ(0, memcmp(test_buffer() + offset, buffer, TEMP_BUFFER_SIZE));
    ASSERT_EQ(TEMP_BUFFER_SIZE - 10,
        cache.Lookup(FNAME, MTIME, offset, TEMP_BUFFER_SIZE - 10, buffer));
    ASSERT_EQ(0, memcmp(test_buffer() + offset, buffer, TEMP_BUFFER_SIZE - 10));
  }

  // Insert with the same key but different length.
  uint8_t buffer2[TEMP_BUFFER_SIZE + 10];
  const int64_t larger_entry_size = TEMP_BUFFER_SIZE + 10;
  ASSERT_TRUE(cache.Store(FNAME, MTIME, 0, test_buffer(), larger_entry_size));
  WaitForAsyncWrite(cache);
  memset(buffer2, 0, larger_entry_size);
  ASSERT_EQ(larger_entry_size,
      cache.Lookup(FNAME, MTIME, 0, larger_entry_size, buffer2));
  ASSERT_EQ(0, memcmp(test_buffer(), buffer2, larger_entry_size));

  // Check that an insertion larger than the cache size will fail.
  ASSERT_FALSE(cache.Store(FNAME, MTIME, 0, test_buffer(), cache_size * 2));

  // Test with uncacheable 'mtime' to make sure the entry is not stored.
  ASSERT_FALSE(cache.Store(FNAME, ScanRange::INVALID_MTIME, 0, test_buffer(),
      TEMP_BUFFER_SIZE));
  ASSERT_EQ(0, cache.Lookup(FNAME, ScanRange::INVALID_MTIME, 0, TEMP_BUFFER_SIZE,
      buffer));

  // Test with bad 'mtime' to make sure the entry is not stored.
  ASSERT_FALSE(cache.Store(FNAME, -1000, 0, test_buffer(), TEMP_BUFFER_SIZE));
  ASSERT_EQ(0, cache.Lookup(FNAME, -1000, 0, TEMP_BUFFER_SIZE, buffer));

  // Test with bad 'offset' to make sure the entry is not stored.
  ASSERT_FALSE(cache.Store(FNAME, MTIME, -2000, test_buffer(), TEMP_BUFFER_SIZE));
  ASSERT_EQ(0, cache.Lookup(FNAME, MTIME, -2000, TEMP_BUFFER_SIZE, buffer));

  // Test with bad 'buffer_len' to make sure the entry is not stored.
  ASSERT_FALSE(cache.Store(FNAME, MTIME, 0, test_buffer(), -5000));
  ASSERT_EQ(0, cache.Lookup(FNAME, MTIME, 0, -5000, buffer));
}

TEST_P(DataCacheTest, NonCanonicalPath) {
  DataCache cache("/noncanonical/../file/./system/path:1M",
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_ERROR_MSG(cache.Init(), "/noncanonical/../file/./system/path is not a canonical"
      " path");
}

TEST_P(DataCacheTest, NonExistantPath) {
  DataCache cache("/invalid/file/system/path:1M",
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_ERROR_MSG(cache.Init(), "Encountered exception while verifying existence of "
      "directory path /invalid/file/system/path: No such file or directory");
}

// Tests backing file rotation by setting FLAGS_data_cache_file_max_size_bytes to be 1/4
// of the cache size. This forces rotation of backing files.
TEST_P(DataCacheTest, RotateFiles) {
  // Set the maximum size of backing files to be 1/4 of the cache size.
  FLAGS_data_cache_file_max_size_bytes = DEFAULT_CACHE_SIZE / 4;
  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());

  // Read and then insert a range of offsets. Expected all misses in the first iteration
  // and all hits in the second iteration.
  for (int i = 0; i < 2; ++i) {
    for (int64_t offset = 0; offset < NUM_CACHE_ENTRIES_NO_EVICT; ++offset) {
      int expected_bytes = i * TEMP_BUFFER_SIZE;
      uint8_t buffer[TEMP_BUFFER_SIZE];
      memset(buffer, 0, TEMP_BUFFER_SIZE);
      ASSERT_EQ(expected_bytes,
          cache.Lookup(FNAME, MTIME, offset, TEMP_BUFFER_SIZE, buffer)) << offset;
      if (i == 0) {
        ASSERT_TRUE(cache.Store(FNAME, MTIME, offset, test_buffer() + offset,
            TEMP_BUFFER_SIZE));
      } else {
        ASSERT_EQ(0, memcmp(test_buffer() + offset, buffer, TEMP_BUFFER_SIZE));
      }
    }
    WaitForAsyncWrite(cache);
  }

  // Make sure the cache's destructor removes all backing files.
  vector<string> entries;
  ASSERT_OK(FileSystemUtil::Directory::GetEntryNames(data_cache_dirs()[0], &entries, -1,
      FileSystemUtil::Directory::EntryType::DIR_ENTRY_REG));
  ASSERT_EQ(4, entries.size());

  // Verify the backing files don't exceed size limits.
  ASSERT_OK(cache.CloseFilesAndVerifySizes());
}

// Tests backing file rotation by setting --data_cache_file_max_size_bytes to be 1/4
// of the cache size. This forces rotation of backing files. Also sets
// --data_cache_max_opened_files to 1 so that only one underlying
// file is allowed. This exercises the lazy deletion path.
TEST_P(DataCacheTest, RotateAndDeleteFiles) {
  // Set the maximum size of backing files to be 1/4 of the cache size.
  FLAGS_data_cache_file_max_size_bytes = DEFAULT_CACHE_SIZE / 4;
  // Force to allow one backing file.
  FLAGS_data_cache_max_opened_files = 1;

  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());

  // Read and insert a working set the same size of the cache. Expected all misses
  // in all iterations as the backing file is only 1/4 of the cache capacity.
  uint8_t buffer[TEMP_BUFFER_SIZE];
  for (int i = 0; i < 4; ++i) {
    for (int64_t offset = 0; offset < 1024; ++offset) {
      memset(buffer, 0, TEMP_BUFFER_SIZE);
      ASSERT_EQ(0, cache.Lookup(FNAME, MTIME, offset, TEMP_BUFFER_SIZE, buffer));
      ASSERT_TRUE(cache.Store(FNAME, MTIME, offset, test_buffer() + offset,
          TEMP_BUFFER_SIZE));
      WaitForAsyncWrite(cache);
    }
  }

  // Verifies that the last part of the working set which fits in a single backing file
  // is still in the cache.
  int64_t in_cache_offset =
      1024 - FLAGS_data_cache_file_max_size_bytes / TEMP_BUFFER_SIZE;
  int64_t num_entries_hit = 0;
  for (int64_t offset = in_cache_offset ; offset < 1024; ++offset) {
    memset(buffer, 0, TEMP_BUFFER_SIZE);
    int64_t lookup_size = cache.Lookup(FNAME, MTIME, offset,
        TEMP_BUFFER_SIZE, buffer);
    if (lookup_size != 0) {
      ++num_entries_hit;
      EXPECT_EQ(TEMP_BUFFER_SIZE, lookup_size);
      EXPECT_EQ(0, memcmp(buffer, test_buffer() + offset, TEMP_BUFFER_SIZE));
    }
  }
  EXPECT_GE(num_entries_hit, NUM_CACHE_ENTRIES / 4 - 1);

  // Make sure only one backing file exists. Allow for 10 seconds latency for the
  // file deleter thread to run.
  int num_entries = 0;
  const int num_wait_secs = 10;
  for (int i = 0; i <= num_wait_secs; ++i) {
    vector<string> entries;
    ASSERT_OK(FileSystemUtil::Directory::GetEntryNames(data_cache_dirs()[0], &entries, -1,
        FileSystemUtil::Directory::EntryType::DIR_ENTRY_REG));
    num_entries = entries.size();
    if (num_entries == 1) break;
    // Flag a failure on the 11-th time around this loop.
    ASSERT_LT(i, num_wait_secs);
    sleep(1);
  }
}

// Tests eviction in the cache by inserting a large entry which evicts all existing
// entries in the cache.
TEST_P(DataCacheTest, LRUEviction) {
  // This test is specific to LRU
  if (FLAGS_data_cache_eviction_policy != "LRU") return;
  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());

  // Read and then insert range chunks of size TEMP_BUFFER_SIZE from the test buffer.
  // Expected all misses in both iterations due to LRU eviction.
  uint8_t buffer[TEMP_BUFFER_SIZE];
  int64_t offset = 0;
  for (int i = 0; i < 2; ++i) {
    for (offset = 0; offset < 1028; ++offset) {
      ASSERT_EQ(0, cache.Lookup(FNAME, MTIME, offset, TEMP_BUFFER_SIZE, buffer));
      ASSERT_TRUE(cache.Store(FNAME, MTIME, offset, test_buffer() + offset,
          TEMP_BUFFER_SIZE));
      WaitForAsyncWrite(cache);
    }
  }
  // Verifies that the cache is full.
  int hit_count = 0;
  for (offset = 0; offset < 1028; ++offset) {
    int64_t bytes_read = cache.Lookup(FNAME, MTIME, offset, TEMP_BUFFER_SIZE, buffer);
    DCHECK(bytes_read == 0 || bytes_read == TEMP_BUFFER_SIZE);
    if (bytes_read == TEMP_BUFFER_SIZE) ++hit_count;
  }
  ASSERT_EQ(1024, hit_count);

  // Create a buffer which has the same size as the cache and insert it into the cache.
  // This should evict all the existing entries in the cache.
  unique_ptr<uint8_t[]> large_buffer(new uint8_t[DEFAULT_CACHE_SIZE]);
  for (offset = 0; offset < cache_size; offset += TEST_BUFFER_SIZE) {
    memcpy(large_buffer.get() + offset, test_buffer(), TEST_BUFFER_SIZE);
  }
  const string& alt_fname = "random";
  offset = 0;
  ASSERT_TRUE(cache.Store(alt_fname, MTIME, offset, large_buffer.get(), cache_size));
  WaitForAsyncWrite(cache);

  // Verifies that all previous entries are all evicted.
  for (offset = 0; offset < 1028; ++offset) {
    ASSERT_EQ(0, cache.Lookup(FNAME, MTIME, offset, TEMP_BUFFER_SIZE, buffer));
  }
  // The large buffer should still be in the cache.
  unique_ptr<uint8_t[]> temp_buffer(new uint8_t[DEFAULT_CACHE_SIZE]);
  offset = 0;
  ASSERT_EQ(cache_size,
      cache.Lookup(alt_fname, MTIME, offset, cache_size, temp_buffer.get()));
  ASSERT_EQ(0, memcmp(temp_buffer.get(), large_buffer.get(), cache_size));

  // Verify the backing files don't exceed size limits.
  ASSERT_OK(cache.CloseFilesAndVerifySizes());
}

// Tests insertion and lookup with the cache with multiple threads.
// Inserts a working set which will fit in the cache. Despite potential
// collision during insertion, all entries in the working set should be found.
TEST_P(DataCacheTest, MultiThreadedNoMisses) {
  int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());

  int64_t max_start_offset = NUM_CACHE_ENTRIES_NO_EVICT;
  bool use_per_thread_filename = false;
  bool expect_misses = false;
  double wait_time_ms;
  MultiThreadedReadWrite(&cache, max_start_offset, use_per_thread_filename,
      expect_misses, &wait_time_ms);
  LOG(INFO) << "MultiThreadedNoMisses, "
            << FLAGS_data_cache_eviction_policy << ", "
            << (FLAGS_data_cache_num_async_write_threads > 0 ? "Async" : "Sync")
            << ", write_time_ms: " << wait_time_ms;
}

// Inserts a working set which is known to be larger than the cache's capacity.
// Expect some cache misses in lookups.
TEST_P(DataCacheTest, MultiThreadedWithMisses) {
  int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());

  int64_t max_start_offset = 1024;
  bool use_per_thread_filename = true;
  bool expect_misses = true;
  double wait_time_ms;
  MultiThreadedReadWrite(&cache, max_start_offset, use_per_thread_filename,
      expect_misses, &wait_time_ms);
  LOG(INFO) << "MultiThreadedWithMisses, "
            << FLAGS_data_cache_eviction_policy << ", "
            << (FLAGS_data_cache_num_async_write_threads > 0 ? "Async" : "Sync")
            << ", write_time_ms: " << wait_time_ms;
}

// Test insertion and lookup with a cache configured with multiple partitions.
TEST_P(DataCacheTest, MultiPartitions) {
  StringPiece delimiter(",");
  string cache_base = JoinStrings(data_cache_dirs(), delimiter);
  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", cache_base, std::to_string(cache_size)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());

  int64_t max_start_offset = 512;
  bool use_per_thread_filename = false;
  bool expect_misses = false;
  double wait_time_ms;
  MultiThreadedReadWrite(&cache, max_start_offset, use_per_thread_filename,
      expect_misses, &wait_time_ms);
  LOG(INFO) << "MultiPartitions, "
            << FLAGS_data_cache_eviction_policy << ", "
            << (FLAGS_data_cache_num_async_write_threads > 0 ? "Async" : "Sync")
            << ", write_time_ms: " << wait_time_ms;
}

// Tests insertion of a working set whose size is 1/8 of the total memory size.
// This likely exceeds the size of the page cache and forces write back of dirty pages in
// the page cache to the backing files and also read from the backing files during lookup.
TEST_P(DataCacheTest, LargeFootprint) {
  struct sysinfo info;
  ASSERT_EQ(0, sysinfo(&info));
  ASSERT_GT(info.totalram, 0);
  // Set sufficient buffer size limits to prevent async store failures.
  FLAGS_data_cache_async_write_buffer_limit = std::to_string(info.totalram);
  DataCache cache(
      Substitute("$0:$1", data_cache_dirs()[0], std::to_string(info.totalram)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());

  const int64_t footprint = info.totalram / 8;
  for (int64_t i = 0; i < footprint / TEST_BUFFER_SIZE; ++i) {
    int64_t offset = i * TEST_BUFFER_SIZE;
    ASSERT_TRUE(cache.Store(FNAME, MTIME, offset, test_buffer(), TEST_BUFFER_SIZE));
  }
  WaitForAsyncWrite(cache);
  uint8_t buffer[TEST_BUFFER_SIZE];
  for (int64_t i = 0; i < footprint / TEST_BUFFER_SIZE; ++i) {
    int64_t offset = i * TEST_BUFFER_SIZE;
    memset(buffer, 0, TEST_BUFFER_SIZE);
    ASSERT_EQ(TEST_BUFFER_SIZE,
        cache.Lookup(FNAME, MTIME, offset, TEST_BUFFER_SIZE, buffer));
    ASSERT_EQ(0, memcmp(buffer, test_buffer(), TEST_BUFFER_SIZE));
  }
}

TEST_P(DataCacheTest, AccessTraceAnonymization) {
  FLAGS_data_cache_enable_tracing = true;
  FLAGS_data_cache_trace_dir = data_cache_trace_dir();
  // Use a small trace file size so that the trace is split over multiple files
  FLAGS_max_data_cache_trace_file_size = 10000;
  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  int64_t max_start_offset = 1024;
  uint64_t expected_total_events = NUM_THREADS * max_start_offset * 2;
  for (bool anon : { false, true }) {
    SCOPED_TRACE(anon);
    FLAGS_data_cache_anonymize_trace = anon;
    // Remove files between iterations to avoid crosstalk between the anonymized
    // iteration and non-anonymized iteration
    ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(FLAGS_data_cache_trace_dir));
    {
      DataCache cache(Substitute(
          "$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
          FLAGS_data_cache_num_async_write_threads);
      ASSERT_OK(cache.Init());

      bool use_per_thread_filename = true;
      bool expect_misses = true;
      double wait_time_ms;
      MultiThreadedReadWrite(&cache, max_start_offset, use_per_thread_filename,
          expect_misses, &wait_time_ms);
      LOG(INFO) << "AccessTraceAnonymization, "
                << FLAGS_data_cache_eviction_policy << ", "
                << (FLAGS_data_cache_num_async_write_threads > 0 ? "Async" : "Sync")
                << ", write_time_ms: " << wait_time_ms;
    }

    // Part 1: Read the trace files and ensure that the JSON entries are valid
    // TraceEvent entries
    vector<string> trace_files;
    string trace_dir = Substitute("$0/partition-0/", FLAGS_data_cache_trace_dir);
    ASSERT_OK(SimpleLogger::GetLogFiles(trace_dir, trace::TRACE_FILE_PREFIX,
        &trace_files));

    for (const string& filename : trace_files) {
      trace::TraceFileIterator trace_file_iter(filename);
      ASSERT_OK(trace_file_iter.Init());
      while (true) {
        trace::TraceEvent trace_event;
        bool done = false;
        ASSERT_OK(trace_file_iter.GetNextEvent(&trace_event, &done));
        if (done) break;
        if (anon) {
          // We expect anonymized filenames to be 22-character fingerprints:
          // - 128 bit fingerprint = 16 bytes
          // - 16 * 4/3 (base64 encoding) = 21.3333
          // - Round up to 22.
          EXPECT_EQ(22, trace_event.filename.size()) << trace_event.filename;
        } else {
          EXPECT_TRUE(MatchPattern(trace_event.filename, "thread-*file"));
        }
      }
    }

    // Part 2: Create a replayer and verify that it can successfully replay from that
    // directory.
    trace::TraceReplayer replayer(Substitute("/this_directory_doesnt_matter:$0",
        std::to_string(cache_size)));
    EXPECT_OK(replayer.Init());
    EXPECT_OK(replayer.ReplayDirectory(trace_dir));
    trace::CacheHitStatistics replay_stats = replayer.GetReplayStatistics();
    int64_t total_events = replay_stats.hits + replay_stats.partial_hits +
      replay_stats.misses + replay_stats.stores + replay_stats.failed_stores;
    EXPECT_EQ(total_events, expected_total_events);
  }
}

TEST_P(DataCacheTest, AccessTraceSubsetPercentage) {
  FLAGS_data_cache_enable_tracing = true;
  FLAGS_data_cache_trace_dir = data_cache_trace_dir();
  // Use a small trace file size so that the trace is split over multiple files
  FLAGS_max_data_cache_trace_file_size = 10000;
  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  int64_t max_start_offset = 1024;
  // Each thread does a store and a lookup for each offset
  uint64_t expected_total_events = NUM_THREADS * max_start_offset * 2;
  for (int trace_percentage : {100, 50, 20, 10, 5, 1}) {
    SCOPED_TRACE(trace_percentage);
    FLAGS_data_cache_trace_percentage = trace_percentage;
    // Remove files between iterations to avoid crosstalk between the anonymized
    // iteration and non-anonymized iteration
    ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(FLAGS_data_cache_trace_dir));
    {
      DataCache cache(Substitute(
          "$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
          FLAGS_data_cache_num_async_write_threads);
      ASSERT_OK(cache.Init());

      bool use_per_thread_filename = true;
      bool expect_misses = true;
      double wait_time_ms;
      MultiThreadedReadWrite(&cache, max_start_offset, use_per_thread_filename,
          expect_misses, &wait_time_ms);
      LOG(INFO) << "AccessTraceSubsetPercentage, "
                << FLAGS_data_cache_eviction_policy << ", "
                << (FLAGS_data_cache_num_async_write_threads > 0 ? "Async" : "Sync")
                << ", write_time_ms: " << wait_time_ms;
    }

    // Replay the trace and record the counts
    string trace_dir = Substitute("$0/partition-0/", FLAGS_data_cache_trace_dir);
    trace::TraceReplayer replayer(Substitute("/this_directory_doesnt_matter:$0",
        std::to_string(cache_size)));
    EXPECT_OK(replayer.Init());
    EXPECT_OK(replayer.ReplayDirectory(trace_dir));

    // The trace percentage is enforced when generating the trace, so verify that
    // the number of events in the original trace is within +/-1% of the expected value.
    // This is deterministic, because the actual cache entries accessed are always
    // the same (even if the order is not).
    trace::CacheHitStatistics original_trace_stats =
        replayer.GetOriginalTraceStatistics();
    int64_t num_original_trace_events = original_trace_stats.hits +
      original_trace_stats.partial_hits + original_trace_stats.misses +
      original_trace_stats.stores + original_trace_stats.failed_stores;
    EXPECT_LE(100 * num_original_trace_events,
              (expected_total_events * (trace_percentage + 1)));
    EXPECT_GE(100 * num_original_trace_events,
              (expected_total_events * (trace_percentage - 1)));
    LOG(INFO) << "Trace percentage " << trace_percentage << " num events: "
              << num_original_trace_events;

    // The replay should have the same number of events.
    trace::CacheHitStatistics replay_stats = replayer.GetReplayStatistics();
    int64_t num_replay_trace_events = replay_stats.hits + replay_stats.partial_hits +
      replay_stats.misses + replay_stats.stores + replay_stats.failed_stores;
    EXPECT_EQ(num_original_trace_events, num_replay_trace_events);
  }
}

TEST_P(DataCacheTest, RotationalDisk) {
  // Set to default to test disk detection and mock disk access.
  FLAGS_data_cache_write_concurrency = 0;
  MockDisk("sda", /*is_rotational*/ true);

  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());
  ASSERT_EQ(1, cache.partitions_[0]->data_cache_write_concurrency_);
}

TEST_P(DataCacheTest, NonRotationalDisk) {
  // Set to default to test disk detection and mock disk access.
  FLAGS_data_cache_write_concurrency = 0;
  MockDisk("nvme0n1", /*is_rotational*/ false);

  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());
  ASSERT_EQ(8, cache.partitions_[0]->data_cache_write_concurrency_);
}

TEST_P(DataCacheTest, InvalidDisk) {
  // Set to default to test disk detection and mock disk access.
  FLAGS_data_cache_write_concurrency = 0;
  MockDisk("won't parse", /*is_rotational*/ false);

  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());
  ASSERT_EQ(1, cache.partitions_[0]->data_cache_write_concurrency_);
}

TEST_P(DataCacheTest, OutOfWriteBufferLimit) {
  // This test is specific to asynchronous write
  if (FLAGS_data_cache_num_async_write_threads == 0) return;

  // Setting a lower buffer size limit will fail to insert data larger than this value at
  // once.
  FLAGS_data_cache_async_write_buffer_limit = Substitute("$0", TEMP_BUFFER_SIZE << 1);

  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", data_cache_dirs()[0], std::to_string(cache_size)),
      FLAGS_data_cache_num_async_write_threads);
  ASSERT_OK(cache.Init());

  // Continuously inserting a large amount of entries, there will be a certain number of
  // store tasks fail due to the write buffer limit.
  int32_t count = 0;
  for (int64_t offset = 0; offset < NUM_CACHE_ENTRIES_NO_EVICT; ++offset) {
    if (cache.Store(FNAME, MTIME, offset, test_buffer() + offset, TEMP_BUFFER_SIZE)) {
      ++count;
    }
  }
  EXPECT_LT(count, NUM_CACHE_ENTRIES_NO_EVICT);
}

TEST_P(DataCacheTest, SetReadOnly) {
  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  DataCache cache(Substitute("$0:$1", data_cache_dirs()[0], std::to_string(cache_size)));
  ASSERT_OK(cache.Init());

  int64_t max_start_offset = NUM_CACHE_ENTRIES_NO_EVICT;
  bool use_per_thread_filename = false;
  // Set read-only for the first multi-threaded read-write test, which should expect there
  // will be cache misses when reading from the cache.
  bool expect_misses = true;
  bool test_readonly = true;
  double wait_time_ms = 0;
  MultiThreadedReadWrite(&cache, max_start_offset, use_per_thread_filename,
      expect_misses, &wait_time_ms, test_readonly);
  LOG(INFO) << "SetReadOnly, "
          << FLAGS_data_cache_eviction_policy << ", "
          << (FLAGS_data_cache_num_async_write_threads > 0 ? "Async" : "Sync")
          << ", write_time_ms: " << wait_time_ms;

  // For the second test, do not set read-only, and no cache misses should occur.
  expect_misses = false;
  test_readonly = false;
  MultiThreadedReadWrite(&cache, max_start_offset, use_per_thread_filename,
      expect_misses, &wait_time_ms, test_readonly);
  LOG(INFO) << "SetReadOnly, "
        << FLAGS_data_cache_eviction_policy << ", "
        << (FLAGS_data_cache_num_async_write_threads > 0 ? "Async" : "Sync")
        << ", write_time_ms: " << wait_time_ms;
}

TEST_P(DataCacheTest, DumpAndLoad) {
  FLAGS_data_cache_keep_across_restarts = true;

  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  unique_ptr<DataCache> cache = make_unique<DataCache>(Substitute("$0:$1",
      data_cache_dirs()[0], std::to_string(cache_size)));
  ASSERT_OK(cache->Init());

  int64_t max_start_offset = NUM_CACHE_ENTRIES_NO_EVICT;
  for (int64_t offset = 0; offset < max_start_offset; ++offset) {
    ASSERT_TRUE(cache->Store(FNAME, MTIME, offset, test_buffer() + offset,
        TEMP_BUFFER_SIZE));
  }
  WaitForAsyncWrite(*cache);

  // After completing the normal write, dump the cached data and then create a new data
  // cache to load the dump file to initialize.
  ASSERT_OK(cache->Dump());
  cache.reset(new DataCache(Substitute("$0:$1", data_cache_dirs()[0],
      std::to_string(cache_size))));
  ASSERT_OK(cache->Init());

  // There is no need to write again, just lookup all entries written before the dump one
  // by one, should all hit the cache.
  uint8_t buffer[TEMP_BUFFER_SIZE];
  for (int64_t offset = 0; offset < max_start_offset; ++offset) {
    memset(buffer, 0, TEMP_BUFFER_SIZE);
    int64_t bytes_read =
        cache->Lookup(FNAME, MTIME, offset, TEMP_BUFFER_SIZE, buffer);
    ASSERT_EQ(TEMP_BUFFER_SIZE, bytes_read);
    ASSERT_EQ(0, memcmp(buffer, test_buffer() + offset, TEMP_BUFFER_SIZE));
  }
}

TEST_P(DataCacheTest, ChangeConfBeforeLoad) {
  FLAGS_data_cache_keep_across_restarts = true;

  const int64_t cache_size = DEFAULT_CACHE_SIZE;
  unique_ptr<DataCache> cache = make_unique<DataCache>(Substitute("$0:$1",
      data_cache_dirs()[0], std::to_string(cache_size)));
  ASSERT_OK(cache->Init());

  int64_t max_start_offset = NUM_CACHE_ENTRIES_NO_EVICT;
  for (int64_t offset = 0; offset < max_start_offset; ++offset) {
    ASSERT_TRUE(cache->Store(FNAME, MTIME, offset, test_buffer() + offset,
        TEMP_BUFFER_SIZE));
  }
  WaitForAsyncWrite(*cache);

  // After completing the normal write, dump the cached data and change some configs, then
  // create a new data cache to load the dump file to initialize.
  ASSERT_OK(cache->Dump());
  const int64_t new_cache_size = DEFAULT_CACHE_SIZE / 2;
  FLAGS_data_cache_eviction_policy =
      FLAGS_data_cache_eviction_policy == "LRU" ? "LIRS" : "LRU";
  cache.reset(new DataCache(Substitute("$0:$1", data_cache_dirs()[0],
      std::to_string(new_cache_size))));
  ASSERT_OK(cache->Init());

  // Since we have reduced the data cache size and the previous writes have already
  // reached the capacity limit, some data must have been discarded during the load
  // process due to the new capacity limit. Therefore, there should be some misses.
  int num_misses = 0;
  uint8_t buffer[TEMP_BUFFER_SIZE];
  for (int64_t offset = 0; offset < max_start_offset; ++offset) {
    memset(buffer, 0, TEMP_BUFFER_SIZE);
    int64_t bytes_read =
        cache->Lookup(FNAME, MTIME, offset, TEMP_BUFFER_SIZE, buffer);
    if (bytes_read == TEMP_BUFFER_SIZE) {
      ASSERT_EQ(0, memcmp(buffer, test_buffer() + offset, TEMP_BUFFER_SIZE));
    } else {
      ASSERT_EQ(bytes_read, 0);
      ++num_misses;
    }
  }

  EXPECT_GT(num_misses, 0);
  EXPECT_LT(num_misses, NUM_CACHE_ENTRIES_NO_EVICT);
}

} // namespace io
} // namespace impala

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  int rand_seed = time(NULL);
  LOG(INFO) << "rand_seed: " << rand_seed;
  srand(rand_seed);
  // Skip if the platform is affected by KUDU-1508.
  if (!impala::FileSystemUtil::CheckForBuggyExtFS(BASE_CACHE_DIR).ok()) {
    LOG(WARNING) << "Skipping data-cache-test due to KUDU-1508.";
    return 0;
  }
  return RUN_ALL_TESTS();
}
