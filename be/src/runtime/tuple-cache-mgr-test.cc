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

#include <future>
#include <boost/filesystem.hpp>

#include "gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "runtime/tuple-cache-mgr.h"
#include "testutil/gtest-util.h"
#include "util/filesystem-util.h"
#include "util/time.h"

#include "common/names.h"

namespace filesystem = boost::filesystem;
using std::async;
using std::future;
using std::launch;

DECLARE_bool(cache_force_single_shard);

namespace impala {

class TupleCacheMgrTest : public ::testing::Test {
public:

  void SetUp() override {
    cache_dir_ = ("/tmp" / boost::filesystem::unique_path()).string();
    ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(cache_dir_));
  }

  void TearDown() override {
    ASSERT_OK(FileSystemUtil::RemovePaths({cache_dir_}));
  }

  TupleCacheMgr GetCache(const string& cache_dir, const string& capacity = "1MB",
      string eviction_policy = "LRU", uint8_t debug_pos = TupleCacheMgr::NO_FILES,
      uint32_t sync_pool_size = 0, uint32_t sync_pool_queue_depth = 1000,
      string outstanding_write_limit_str = "1GB",
      uint32_t outstanding_write_chunk_bytes = 0) {
    string cache_config;
    if (!cache_dir.empty()) {
      cache_config = Substitute("$0:$1", cache_dir, capacity);
    }
    return TupleCacheMgr{move(cache_config), move(eviction_policy), &metrics_, debug_pos,
        sync_pool_size, sync_pool_queue_depth, move(outstanding_write_limit_str),
        outstanding_write_chunk_bytes};
  }

  TupleCacheMgr GetCache() {
    return GetCache(GetCacheDir());
  }

  TupleCacheMgr GetFailAllocateCache() {
    return GetCache(GetCacheDir(), "1MB", "LRU",
        TupleCacheMgr::FAIL_ALLOCATE | TupleCacheMgr::NO_FILES);
  }

  TupleCacheMgr GetFailInsertCache() {
    return GetCache(GetCacheDir(), "1MB", "LRU",
        TupleCacheMgr::FAIL_INSERT | TupleCacheMgr::NO_FILES);
  }

  std::string GetCacheDir() const { return cache_dir_; }

 private:
  std::string cache_dir_;
  MetricGroup metrics_{"tuple-cache-test"};
};

TEST_F(TupleCacheMgrTest, Disabled) {
  TupleCacheMgr cache = GetCache("");
  ASSERT_OK(cache.Init());
  TupleCacheMgr::UniqueHandle handle = cache.Lookup("a_key");
  EXPECT_FALSE(cache.IsAvailableForRead(handle));
  EXPECT_FALSE(cache.IsAvailableForWrite(handle));
}

TEST_F(TupleCacheMgrTest, TestMiss) {
  TupleCacheMgr cache = GetCache();
  ASSERT_OK(cache.Init());
  TupleCacheMgr::UniqueHandle handle = cache.Lookup("a_key");
  EXPECT_FALSE(cache.IsAvailableForRead(handle));
  EXPECT_FALSE(cache.IsAvailableForWrite(handle));
}

TEST_F(TupleCacheMgrTest, TestMissAcquire) {
  TupleCacheMgr cache = GetCache();
  ASSERT_OK(cache.Init());
  TupleCacheMgr::UniqueHandle handle = cache.Lookup("a_key", true);
  EXPECT_FALSE(cache.IsAvailableForRead(handle));
  EXPECT_TRUE(cache.IsAvailableForWrite(handle));
}

TEST_F(TupleCacheMgrTest, TestFailAllocate) {
  TupleCacheMgr cache = GetFailAllocateCache();
  ASSERT_OK(cache.Init());
  TupleCacheMgr::UniqueHandle handle = cache.Lookup("a_key", true);
  EXPECT_FALSE(cache.IsAvailableForRead(handle));
  EXPECT_FALSE(cache.IsAvailableForWrite(handle));
}

TEST_F(TupleCacheMgrTest, TestFailInsert) {
  TupleCacheMgr cache = GetFailInsertCache();
  ASSERT_OK(cache.Init());
  TupleCacheMgr::UniqueHandle handle = cache.Lookup("a_key", true);
  EXPECT_FALSE(cache.IsAvailableForRead(handle));
  EXPECT_FALSE(cache.IsAvailableForWrite(handle));
}

TEST_F(TupleCacheMgrTest, TestHit) {
  TupleCacheMgr cache = GetCache();
  ASSERT_OK(cache.Init());
  TupleCacheMgr::UniqueHandle handle = cache.Lookup("a_key", true);
  EXPECT_TRUE(cache.IsAvailableForWrite(handle));
  cache.CompleteWrite(move(handle), 100);

  handle = cache.Lookup("a_key", true);
  EXPECT_TRUE(cache.IsAvailableForRead(handle));
  EXPECT_FALSE(cache.IsAvailableForWrite(handle));
  std::string expected_loc =
    (filesystem::path(GetCacheDir()) / "tuple-cache-a_key").string();
  std::string actual_loc = cache.GetPath(handle);
  EXPECT_EQ(expected_loc, actual_loc);
}

TEST_F(TupleCacheMgrTest, TestTombstone) {
  // Create and immediately tombstone an entry.
  TupleCacheMgr cache = GetCache();
  ASSERT_OK(cache.Init());
  TupleCacheMgr::UniqueHandle handle = cache.Lookup("tombstone_key", true);
  EXPECT_FALSE(cache.IsAvailableForRead(handle));
  EXPECT_TRUE(cache.IsAvailableForWrite(handle));
  cache.AbortWrite(move(handle), true);

  // Subsequent lookups should find a tombstone.
  handle = cache.Lookup("tombstone_key", true);
  EXPECT_FALSE(cache.IsAvailableForRead(handle));
  EXPECT_FALSE(cache.IsAvailableForWrite(handle));
}

TEST_F(TupleCacheMgrTest, TestConcurrentWrite) {
  TupleCacheMgr cache = GetCache();
  ASSERT_OK(cache.Init());
  // Attempt to IsAvailableForWrite many times concurrently. Successes will be returned.
  vector<future<TupleCacheMgr::UniqueHandle>> results;
  results.reserve(100);
  for (int i = 0; i < 100; ++i) {
    results.emplace_back(async(launch::async, [&cache]() {
      TupleCacheMgr::UniqueHandle handle = cache.Lookup("concurrent_key", true);
      EXPECT_FALSE(cache.IsAvailableForRead(handle));
      if (cache.IsAvailableForWrite(handle)) {
        return handle;
      }
      return TupleCacheMgr::UniqueHandle{nullptr};
    }));
  }

  // Wait for all threads to complete so we don't abort and allow another success.
  for (auto& result : results) {
    result.wait();
  }

  int successes = 0;
  for (auto& result : results) {
    if (TupleCacheMgr::UniqueHandle handle = result.get(); handle) {
      ++successes;
      cache.AbortWrite(move(handle), false);
    }
  }
  // Only one Acquire should succeed.
  EXPECT_EQ(1, successes);
}

TEST_F(TupleCacheMgrTest, TestConcurrentTombstone) {
  TupleCacheMgr cache = GetCache();
  ASSERT_OK(cache.Init());
  // Attempt to IsAvailableForWrite many times concurrently. Returns successes.
  vector<future<bool>> results;
  results.reserve(100);
  for (int i = 0; i < 100; ++i) {
    results.emplace_back(async(launch::async, [&cache]() {
      TupleCacheMgr::UniqueHandle handle = cache.Lookup("concurrent_key", true);
      EXPECT_FALSE(cache.IsAvailableForRead(handle));
      if (cache.IsAvailableForWrite(handle)) {
        // Immediately tombstone for other threads.
        cache.AbortWrite(move(handle), true);
        return true;
      }
      return false;
    }));
  }

  int successes = 0;
  for (auto& result : results) {
    result.wait();
    if (result.get()) ++successes;
  }
  // Only one Acquire should succeed.
  EXPECT_EQ(1, successes);
}

TEST_F(TupleCacheMgrTest, TestConcurrentAbort) {
  TupleCacheMgr cache = GetCache();
  ASSERT_OK(cache.Init());
  // Attempt to IsAvailableForWrite many times concurrently. Returns successes.
  vector<future<bool>> results;
  results.reserve(100);
  for (int i = 0; i < 100; ++i) {
    results.emplace_back(async(launch::async, [&cache]() {
      TupleCacheMgr::UniqueHandle handle = cache.Lookup("concurrent_key", true);
      EXPECT_FALSE(cache.IsAvailableForRead(handle));
      if (cache.IsAvailableForWrite(handle)) {
        // Immediately abort for other threads.
        cache.AbortWrite(move(handle), false);
        return true;
      }
      return false;
    }));
  }

  int successes = 0;
  for (auto& result : results) {
    result.wait();
    if (result.get()) ++successes;
  }
  // Multiple Acquires should succeed. This is somewhat probabilistic, but odds of
  // every other thread completing between the first IsAvailableForWrite/AbortWrite
  // seem very low.
  EXPECT_GT(successes, 1);
}

TEST_F(TupleCacheMgrTest, TestConcurrentComplete) {
  TupleCacheMgr cache = GetCache();
  ASSERT_OK(cache.Init());
  // Attempt to write many times concurrently. Returns successful reads of that write.
  vector<future<bool>> results;
  results.reserve(100);
  for (int i = 0; i < 100; ++i) {
    results.emplace_back(async(launch::async, [&cache]() {
      TupleCacheMgr::UniqueHandle handle = cache.Lookup("concurrent_key", true);
      if (cache.IsAvailableForRead(handle)) {
        return true;
      }
      if (cache.IsAvailableForWrite(handle)) {
        // Immediately complete for other threads.
        cache.CompleteWrite(move(handle), 10);
      }
      return false;
    }));
  }

  int successes = 0;
  for (auto& result : results) {
    result.wait();
    if (result.get()) ++successes;
  }
  // At least one Read should succeed. This is somewhat probabilistic, but odds of
  // every other thread completing between the first IsAvailableForWrite/CompleteWrite
  // seem very low.
  EXPECT_GT(successes, 0);
}

TEST_F(TupleCacheMgrTest, TestConcurrentEviction) {
  FLAGS_cache_force_single_shard = true;
  TupleCacheMgr cache = GetCache(GetCacheDir(), "1KB");
  ASSERT_OK(cache.Init());
  // Add many entries concurrently.
  vector<future<void>> results;
  results.reserve(100);
  for (int i = 0; i < 100; ++i) {
    results.emplace_back(async(launch::async, [&cache, i]() {
      TupleCacheMgr::UniqueHandle handle =
          cache.Lookup(Substitute("concurrent_key$0", i), true);
      EXPECT_FALSE(cache.IsAvailableForRead(handle));
      EXPECT_TRUE(cache.IsAvailableForWrite(handle));
      // Keep the 1st key in the cache.
      cache.Lookup("concurrent_key0");
      cache.CompleteWrite(move(handle), 20+i/2);
    }));
  }

  for (auto& result : results) {
    result.wait();
  }
  // The 1st key should still be in cache, 2nd should have been evicted.
  TupleCacheMgr::UniqueHandle handle0 = cache.Lookup("concurrent_key0");
  EXPECT_TRUE(cache.IsAvailableForRead(handle0));
  TupleCacheMgr::UniqueHandle handle1 = cache.Lookup("concurrent_key1");
  EXPECT_FALSE(cache.IsAvailableForRead(handle1));
}

TEST_F(TupleCacheMgrTest, TestMaxSize) {
  FLAGS_cache_force_single_shard = true;
  TupleCacheMgr cache = GetCache(GetCacheDir(), "1KB");
  ASSERT_OK(cache.Init());
  EXPECT_EQ(1024, cache.MaxSize());
}

TEST_F(TupleCacheMgrTest, TestRequestWriteSize) {
  FLAGS_cache_force_single_shard = true;
  TupleCacheMgr cache = GetCache(GetCacheDir(), "1KB");
  ASSERT_OK(cache.Init());

  // Write 5 entries of 200 bytes each
  for (int i = 0; i < 5; ++i) {
    TupleCacheMgr::UniqueHandle handle = cache.Lookup(Substitute("a_key_$0", i), true);
    EXPECT_TRUE(cache.IsAvailableForWrite(handle));
    cache.CompleteWrite(move(handle), 200);
  }

  TupleCacheMgr::UniqueHandle handle = cache.Lookup("update_entry_then_abort", true);
  EXPECT_TRUE(cache.IsAvailableForWrite(handle));
  // Update to 200 bytes. This should evict one entry.
  Status status = cache.RequestWriteSize(&handle, 200);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_entries_evicted_->GetValue(), 1);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 200);

  // Update to 900. This will evict all the others
  status = cache.RequestWriteSize(&handle, 900);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_entries_evicted_->GetValue(), 5);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 900);

  // Update to MaxSize(). This will succeed.
  status = cache.RequestWriteSize(&handle, cache.MaxSize());
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_entries_evicted_->GetValue(), 5);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), cache.MaxSize());

  // Try to update to MaxSize() + 1. This will fail.
  status = cache.RequestWriteSize(&handle, cache.MaxSize() + 1);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::TUPLE_CACHE_ENTRY_SIZE_LIMIT_EXCEEDED);
  EXPECT_EQ(cache.tuple_cache_entries_evicted_->GetValue(), 5);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), cache.MaxSize());

  // Need to test the three state transitions out of IN_PROGRESS
  // Path #1: AbortWrite without tombstone
  cache.AbortWrite(move(handle), /* tombstone */ false);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 0);

  // Path #2: AbortWrite with tombstone
  handle = cache.Lookup("update_entry_then_tombstone", true);
  EXPECT_TRUE(cache.IsAvailableForWrite(handle));
  status = cache.RequestWriteSize(&handle, 900);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 900);
  cache.AbortWrite(move(handle), /* tombstone */ true);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 0);

  // Path #3: CompleteWrite
  handle = cache.Lookup("update_entry_then_complete", true);
  EXPECT_TRUE(cache.IsAvailableForWrite(handle));
  status = cache.RequestWriteSize(&handle, 900);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 900);
  cache.CompleteWrite(move(handle), 900);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 0);
}

TEST_F(TupleCacheMgrTest, TestOutstandingWriteLimit) {
  FLAGS_cache_force_single_shard = true;
  // Set up a cache with an outstanding write limit of 1KB
  TupleCacheMgr cache = GetCache(GetCacheDir(), "1KB", "LRU", 0, 0, 0, "1KB");
  ASSERT_OK(cache.Init());

  // Open two handles
  TupleCacheMgr::UniqueHandle handle1 = cache.Lookup("outstanding_write_limit_1", true);
  EXPECT_TRUE(cache.IsAvailableForWrite(handle1));
  TupleCacheMgr::UniqueHandle handle2 = cache.Lookup("outstanding_write_limit_2", true);
  EXPECT_TRUE(cache.IsAvailableForWrite(handle2));

  // UpdateWrite size to 512 bytes for each, so it is equal to the limit and succeeds.
  Status status = cache.RequestWriteSize(&handle1, 512);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 512);

  status = cache.RequestWriteSize(&handle2, 512);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 1024);

  // Going one byte past the limit should fail
  // This does not set exceeded_max_size
  status = cache.RequestWriteSize(&handle1, 513);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::TUPLE_CACHE_OUTSTANDING_WRITE_LIMIT_EXCEEDED);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 1024);

  // Clean up
  cache.AbortWrite(move(handle1), /* tombstone */ false);
  cache.AbortWrite(move(handle2), /* tombstone */ false);
}

TEST_F(TupleCacheMgrTest, TestOutstandingWriteLimitConcurrent) {
  FLAGS_cache_force_single_shard = true;
  // Set up a cache with a low outstanding write limit of 1KB to make it easy to hit
  // the limit.
  TupleCacheMgr cache = GetCache(GetCacheDir(), "100KB", "LRU", 0, 0, 0, "1KB");
  ASSERT_OK(cache.Init());

  // This attempts to do 100 512-byte writes to the cache with 64 byte request chunks.
  // The cache is big enough to fit all of the writes, so the only reason they should
  // fail is when they hit the outstanding write limit.
  vector<future<bool>> results;
  results.reserve(100);
  for (int i = 0; i < 100; ++i) {
    results.emplace_back(async(launch::async, [&cache, i]() {
      TupleCacheMgr::UniqueHandle handle = cache.Lookup(Substitute("write$0", i), true);
      EXPECT_TRUE(cache.IsAvailableForWrite(handle));
      // Write in 64 byte chunks, 8 chunks = 512 bytes
      for (int num_chunks = 1; num_chunks <= 8; ++num_chunks) {
        Status status = cache.RequestWriteSize(&handle, num_chunks * 64);
        if (!status.ok()) {
          cache.AbortWrite(move(handle), /* tombstone */ false);
          return false;
        }
      }
      cache.CompleteWrite(move(handle), 512);
      return true;
    }));
  }

  // Wait for all threads to complete and count the number of failures
  uint32_t num_failures = 0;
  for (auto& result : results) {
    result.wait();
    if (!result.get()) num_failures++;
  }

  // This test case has race conditions. We expect the failures to line up with the
  // number of backpressure halted. We expect at least one thread to succeed.
  // There are scenarios where all the threads can succeed, so this doesn't require
  // num_failures > 0.
  EXPECT_EQ(cache.tuple_cache_backpressure_halted_->GetValue(), num_failures);
  EXPECT_LT(num_failures, 100);
}

TEST_F(TupleCacheMgrTest, TestOutstandingWriteChunkSize) {
  FLAGS_cache_force_single_shard = true;
  uint32_t chunk_size = 250;
  // Set up a cache with an outstanding write limit of 1KB and a chunk size of 250
  // The chunk size is specifically not a clean divisor of 1KB.
  TupleCacheMgr cache =
    GetCache(GetCacheDir(), "1KB", "LRU", 0, 0, 0, "2KB", chunk_size);
  ASSERT_OK(cache.Init());

  TupleCacheMgr::UniqueHandle handle = cache.Lookup("outstanding_chunk_then_abort", true);
  EXPECT_TRUE(cache.IsAvailableForWrite(handle));

  // Update write size to 1, but this is counted as the chunk size
  Status status = cache.RequestWriteSize(&handle, 1);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), chunk_size);

  // Request write size to be equal to the chunk size. This doesn't change the outstanding
  // write bytes.
  status = cache.RequestWriteSize(&handle, chunk_size);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), chunk_size);

  // Request write size to be one above the chunk size. This grabs a second chunk.
  status = cache.RequestWriteSize(&handle, chunk_size + 1);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 2 * chunk_size);

  // The chunk size avoids conflicts with the MaxSize(). This requests a size that
  // would round to larger than MaxSize (the chunk size is not a clean divisor of the
  // cache size), but it does not result in an error. Instead, it reserves MaxSize().
  status = cache.RequestWriteSize(&handle,
      ((cache.MaxSize() / chunk_size) * chunk_size) + 1);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), cache.MaxSize());

  // Request size can go all the way to MaxSize() even with chunk size.
  status = cache.RequestWriteSize(&handle, cache.MaxSize());
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), cache.MaxSize());

  // Need to test the three state transitions out of IN_PROGRESS
  // Path #1: AbortWrite without tombstone
  cache.AbortWrite(move(handle), /* tombstone */ false);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 0);

  // Path #2: AbortWrite with tombstone
  handle = cache.Lookup("outstanding_chunk_then_tombstone", true);
  EXPECT_TRUE(cache.IsAvailableForWrite(handle));
  status = cache.RequestWriteSize(&handle, chunk_size + 1);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 2 * chunk_size);
  cache.AbortWrite(move(handle), /* tombstone */ true);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 0);

  // Path #3: CompleteWrite
  handle = cache.Lookup("outstanding_chunk_then_complete", true);
  EXPECT_TRUE(cache.IsAvailableForWrite(handle));
  status = cache.RequestWriteSize(&handle, chunk_size + 1);
  EXPECT_OK(status);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 2 * chunk_size);
  cache.CompleteWrite(move(handle), chunk_size + 1);
  EXPECT_EQ(cache.tuple_cache_outstanding_writes_bytes_->GetValue(), 0);
}

TEST_F(TupleCacheMgrTest, TestSyncToDisk) {
  // Need the debug_pos to be zero so that DebugPos::NO_FILES is not set.
  TupleCacheMgr cache =
      GetCache(GetCacheDir(), "1KB", "LRU", /* debug_pos */ 0, /* sync_pool_size */ 10);
  ASSERT_OK(cache.Init());

  // Error case: If there is no file, then the thread doing sync will get an error
  // when trying to open the file. This causes the entry to be evicted.
  TupleCacheMgr::UniqueHandle handle = cache.Lookup("key_without_file", true);
  EXPECT_TRUE(cache.IsAvailableForWrite(handle));
  cache.CompleteWrite(move(handle), 100);
  // Sleep a bit to let the thread pool process the entry
  SleepForMs(100);
  handle = cache.Lookup("key_without_file", false);
  EXPECT_FALSE(cache.IsAvailableForRead(handle));

  // Success case: If there is a file that can be synced to disk, everything behaves
  // normally.
  handle = cache.Lookup("key_with_file", true);
  std::string file_path = cache.GetPath(handle);
  std::unique_ptr<kudu::WritableFile> cache_file;
  kudu::Status s = kudu::Env::Default()->NewWritableFile(file_path, &cache_file);
  EXPECT_TRUE(s.ok());
  std::string data("data");
  cache_file->Append(Slice(data));
  cache.CompleteWrite(move(handle), 100);
  // Sleep a bit to let the thread pool process the entry
  SleepForMs(100);
  handle = cache.Lookup("key_with_file", false);
  EXPECT_TRUE(cache.IsAvailableForRead(handle));
}

TEST_F(TupleCacheMgrTest, TestDroppedSyncs) {
  // Need the debug_pos to be zero so that DebugPos::NO_FILES is not set.
  // We set a small sync_pool_size (1) and the bare minimum sync_pool_queue_depth (1)
  // to force some syncs to be dropped.
  FLAGS_cache_force_single_shard = true;
  TupleCacheMgr cache = GetCache(GetCacheDir(), "10KB", "LRU", /* debug_pos */ 0,
      /* sync_pool_size */ 1, /* sync_pool_queue_depth */ 1);
  ASSERT_OK(cache.Init());

  // Attempt to write entries to the cache concurrently to stress the sync pool.
  // This uses many writers, but the writes are small and can all fit into the
  // cache. The only reason something would fail to write to the cache is if the
  // sync pool gets overwhelmed.
  vector<future<bool>> results;
  results.reserve(100);
  for (int i = 0; i < 100; ++i) {
    results.emplace_back(async(launch::async, [&cache, i]() {
      TupleCacheMgr::UniqueHandle handle = cache.Lookup(Substitute("write$0", i), true);
      EXPECT_TRUE(cache.IsAvailableForWrite(handle));
      std::string file_path = cache.GetPath(handle);
      std::unique_ptr<kudu::WritableFile> cache_file;
      kudu::Status s = kudu::Env::Default()->NewWritableFile(file_path, &cache_file);
      EXPECT_TRUE(s.ok());
      std::string data("data");
      cache_file->Append(Slice(data));
      cache.CompleteWrite(move(handle), 100);
      // CompleteWrite doesn't return status, so we can only tell if the sync failed
      // by looking up the entry.
      handle = cache.Lookup(Substitute("write$0", i), false);
      return cache.IsAvailableForRead(handle);
    }));
  }

  // Wait for all threads to complete and count the number of failures
  uint32_t num_failures = 0;
  for (auto& result : results) {
    result.wait();
    if (!result.get()) num_failures++;
  }
  // The sync pool should get overwhelmed and the number of dropped syncs should match
  // the number of failures.
  EXPECT_GT(cache.tuple_cache_dropped_sync_->GetValue(), 0);
  EXPECT_EQ(cache.tuple_cache_dropped_sync_->GetValue(), num_failures);
}

} // namespace impala
