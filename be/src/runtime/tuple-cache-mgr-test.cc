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
#include "runtime/tuple-cache-mgr.h"
#include "testutil/gtest-util.h"
#include "util/filesystem-util.h"

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

  TupleCacheMgr GetCache(string cache_dir, string capacity = "1MB",
    string eviction_policy = "LRU", uint8_t debug_pos = 0) {
    string cache_config;
    if (!cache_dir.empty()) {
      cache_config = Substitute("$0:$1", cache_dir, capacity);
    }
    return TupleCacheMgr{cache_config, eviction_policy, &metrics_, debug_pos};
  }

  TupleCacheMgr GetCache() {
    return GetCache(GetCacheDir());
  }

  TupleCacheMgr GetFailAllocateCache() {
    return GetCache(GetCacheDir(), "1MB", "LRU", TupleCacheMgr::FAIL_ALLOCATE);
  }

  TupleCacheMgr GetFailInsertCache() {
    return GetCache(GetCacheDir(), "1MB", "LRU", TupleCacheMgr::FAIL_INSERT);
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

} // namespace impala
