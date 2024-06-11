// Some portions Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/cache/cache.h"
#include "util/cache/cache-test.h"

#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/slice.h"

using std::make_tuple;
using std::tuple;
using strings::Substitute;

namespace impala {

// The Invalidate operation is currently only supported by the RLCacheShard
// implementation.
class CacheInvalidationTest :
    public CacheBaseTest,
    public ::testing::WithParamInterface<tuple<Cache::EvictionPolicy, ShardingPolicy>> {
 public:
  CacheInvalidationTest()
      : CacheBaseTest(16 * 1024 * 1024) {
  }

  void SetUp() override {
    const auto& param = GetParam();
    SetupWithParameters(std::get<0>(param),
                        std::get<1>(param));
  }
};

INSTANTIATE_TEST_SUITE_P(
    CacheTypes, CacheInvalidationTest,
    ::testing::Values(
        make_tuple(Cache::EvictionPolicy::FIFO,
                   ShardingPolicy::MultiShard),
        make_tuple(Cache::EvictionPolicy::FIFO,
                   ShardingPolicy::SingleShard),
        make_tuple(Cache::EvictionPolicy::LRU,
                   ShardingPolicy::MultiShard),
        make_tuple(Cache::EvictionPolicy::LRU,
                   ShardingPolicy::SingleShard)));

TEST_P(CacheInvalidationTest, InvalidateAllEntries) {
  constexpr const int kEntriesNum = 1024;
  // This scenarios assumes no evictions are done at the cache capacity.
  ASSERT_LE(kEntriesNum, cache_size());

  // Running invalidation on empty cache should yield no invalidated entries.
  ASSERT_EQ(0, cache_->Invalidate({}));
  for (auto i = 0; i < kEntriesNum; ++i) {
    Insert(i, i);
  }
  // Remove a few entries from the cache (sparse pattern of keys).
  constexpr const int kSparseKeys[] = {1, 100, 101, 500, 501, 512, 999, 1001};
  for (const auto key : kSparseKeys) {
    Erase(key);
  }
  ASSERT_EQ(ARRAYSIZE(kSparseKeys), evicted_keys_.size());

  // All inserted entries, except for the removed one, should be invalidated.
  ASSERT_EQ(kEntriesNum - ARRAYSIZE(kSparseKeys), cache_->Invalidate({}));
  // In the end, no entries should be left in the cache.
  ASSERT_EQ(kEntriesNum, evicted_keys_.size());
}

TEST_P(CacheInvalidationTest, InvalidateNoEntries) {
  constexpr const int kEntriesNum = 10;
  // This scenarios assumes no evictions are done at the cache capacity.
  ASSERT_LE(kEntriesNum, cache_size());

  const Cache::ValidityFunc func = [](Slice /* key */, Slice /* value */) {
    return true;
  };
  // Running invalidation on empty cache should yield no invalidated entries.
  ASSERT_EQ(0, cache_->Invalidate({ func }));

  for (auto i = 0; i < kEntriesNum; ++i) {
    Insert(i, i);
  }

  // No entries should be invalidated since the validity function considers
  // all entries valid.
  ASSERT_EQ(0, cache_->Invalidate({ func }));
  ASSERT_TRUE(evicted_keys_.empty());
}

TEST_P(CacheInvalidationTest, InvalidateNoEntriesNoAdvanceIterationFunctor) {
  constexpr const int kEntriesNum = 256;
  // This scenarios assumes no evictions are done at the cache capacity.
  ASSERT_LE(kEntriesNum, cache_size());

  const Cache::InvalidationControl ctl = {
    Cache::kInvalidateAllEntriesFunc,
    [](size_t /* valid_entries_count */, size_t /* invalid_entries_count */) {
      // Never advance over the item list.
      return false;
    }
  };

  // Running invalidation on empty cache should yield no invalidated entries.
  ASSERT_EQ(0, cache_->Invalidate(ctl));

  for (auto i = 0; i < kEntriesNum; ++i) {
    Insert(i, i);
  }

  // No entries should be invalidated since the iteration functor doesn't
  // advance over the list of entries, even if every entry is declared invalid.
  ASSERT_EQ(0, cache_->Invalidate(ctl));
  // In the end, all entries should be in the cache.
  ASSERT_EQ(0, evicted_keys_.size());
}

TEST_P(CacheInvalidationTest, InvalidateOddKeyEntries) {
  constexpr const int kEntriesNum = 64;
  // This scenarios assumes no evictions are done at the cache capacity.
  ASSERT_LE(kEntriesNum, cache_size());

  const Cache::ValidityFunc func = [](Slice key, Slice /* value */) {
    return DecodeInt(key) % 2 == 0;
  };
  // Running invalidation on empty cache should yield no invalidated entries.
  ASSERT_EQ(0, cache_->Invalidate({ func }));

  for (auto i = 0; i < kEntriesNum; ++i) {
    Insert(i, i);
  }
  ASSERT_EQ(kEntriesNum / 2, cache_->Invalidate({ func }));
  ASSERT_EQ(kEntriesNum / 2, evicted_keys_.size());
  for (auto i = 0; i < kEntriesNum; ++i) {
    if (i % 2 == 0) {
      ASSERT_EQ(i,  Lookup(i));
    } else {
      ASSERT_EQ(-1,  Lookup(i));
    }
  }
}

// This class is dedicated for scenarios specific for FIFOCache.
// The scenarios use a single-shard cache for simpler logic.
class FIFOCacheTest : public CacheBaseTest {
 public:
  FIFOCacheTest()
      : CacheBaseTest(10 * 1024) {
  }

  void SetUp() override {
    SetupWithParameters(Cache::EvictionPolicy::FIFO,
                        ShardingPolicy::SingleShard);
  }
};

// Verify how the eviction behavior of a FIFO cache.
TEST_F(FIFOCacheTest, EvictionPolicy) {
  static constexpr int kNumElems = 20;
  const int size_per_elem = cache_size() / kNumElems;
  // First data chunk: fill the cache up to the capacity.
  int idx = 0;
  do {
    Insert(idx, idx, size_per_elem);
    // Keep looking up the very first entry: this is to make sure lookups
    // do not affect the recency criteria of the eviction policy for FIFO cache.
    Lookup(0);
    ++idx;
  } while (evicted_keys_.empty());
  ASSERT_GT(idx, 1);

  // Make sure the earliest inserted entry was evicted.
  ASSERT_EQ(-1, Lookup(0));

  // Verify that the 'empirical' capacity matches the expected capacity
  // (it's a single-shard cache).
  const int capacity = idx - 1;
  ASSERT_EQ(kNumElems, capacity);

  // Second data chunk: add (capacity / 2) more elements.
  for (int i = 1; i < capacity / 2; ++i) {
    // Earlier inserted elements should be gone one-by-one as new elements are
    // inserted, and lookups should not affect the recency criteria of the FIFO
    // eviction policy.
    ASSERT_EQ(i, Lookup(i));
    Insert(capacity + i, capacity + i, size_per_elem);
    ASSERT_EQ(capacity + i, Lookup(capacity + i));
    ASSERT_EQ(-1, Lookup(i));
  }
  ASSERT_EQ(capacity / 2, evicted_keys_.size());

  // Early inserted elements from the first chunk should be evicted
  // to accommodate the elements from the second chunk.
  for (int i = 0; i < capacity / 2; ++i) {
    SCOPED_TRACE(Substitute("early inserted elements: index $0", i));
    ASSERT_EQ(-1, Lookup(i));
  }
  // The later inserted elements from the first chunk should be still
  // in the cache.
  for (int i = capacity / 2; i < capacity; ++i) {
    SCOPED_TRACE(Substitute("late inserted elements: index $0", i));
    ASSERT_EQ(i, Lookup(i));
  }
}

TEST_F(FIFOCacheTest, UpdateEvictionPolicy) {
  static constexpr int kNumElems = 20;
  const int size_per_elem = cache_size() / kNumElems;
  // First data chunk: fill the cache up to the capacity.
  int idx = 0;
  do {
    Insert(idx, idx);
    UpdateCharge(idx, size_per_elem);
    // Keep looking up the very first entry: this is to make sure lookups
    // do not affect the recency criteria of the eviction policy for FIFO cache.
    Lookup(0);
    ++idx;
  } while (evicted_keys_.empty());
  ASSERT_GT(idx, 1);

  // Make sure the earliest inserted entry was evicted.
  ASSERT_EQ(-1, Lookup(0));

  // Verify that the 'empirical' capacity matches the expected capacity
  // (it's a single-shard cache).
  const int capacity = idx - 1;
  ASSERT_EQ(kNumElems, capacity);

  // Second data chunk: add (capacity / 2) more elements.
  for (int i = 1; i < capacity / 2; ++i) {
    // Earlier inserted elements should be gone one-by-one as new elements are
    // inserted, and lookups should not affect the recency criteria of the FIFO
    // eviction policy.
    ASSERT_EQ(i, Lookup(i));
    Insert(capacity + i, capacity + i);
    UpdateCharge(capacity + i, size_per_elem);
    ASSERT_EQ(capacity + i, Lookup(capacity + i));
    ASSERT_EQ(-1, Lookup(i));
  }
  ASSERT_EQ(capacity / 2, evicted_keys_.size());

  // Early inserted elements from the first chunk should be evicted
  // to accommodate the elements from the second chunk.
  for (int i = 0; i < capacity / 2; ++i) {
    SCOPED_TRACE(Substitute("early inserted elements: index $0", i));
    ASSERT_EQ(-1, Lookup(i));
  }
  // The later inserted elements from the first chunk should be still
  // in the cache.
  for (int i = capacity / 2; i < capacity; ++i) {
    SCOPED_TRACE(Substitute("late inserted elements: index $0", i));
    ASSERT_EQ(i, Lookup(i));
  }
}

class LRUCacheTest :
    public CacheBaseTest,
    public ::testing::WithParamInterface<ShardingPolicy> {
 public:
  LRUCacheTest()
      : CacheBaseTest(16 * 1024 * 1024) {
  }

  void SetUp() override {
    const auto& param = GetParam();
    SetupWithParameters(Cache::EvictionPolicy::LRU,
                        param);
  }
};

INSTANTIATE_TEST_SUITE_P(
    CacheTypes, LRUCacheTest,
    ::testing::Values(ShardingPolicy::MultiShard,
                      ShardingPolicy::SingleShard));

TEST_P(LRUCacheTest, EvictionPolicy) {
  static constexpr int kNumElems = 1000;
  const int size_per_elem = cache_size() / kNumElems;

  Insert(100, 101);
  Insert(200, 201);

  // Loop adding and looking up new entries, but repeatedly accessing key 100.
  // This frequently-used entry should not be evicted. It also accesses key 200,
  // but the lookup uses NO_UPDATE, so this key is not preserved.
  for (int i = 0; i < kNumElems + 1000; ++i) {
    Insert(1000+i, 2000+i, size_per_elem);
    ASSERT_EQ(2000+i, Lookup(1000+i));
    ASSERT_EQ(101, Lookup(100));
    int entry200val = Lookup(200, Cache::NO_UPDATE);
    ASSERT_TRUE(entry200val == -1 || entry200val == 201);
  }
  ASSERT_EQ(101, Lookup(100));
  // Since '200' was accessed using NO_UPDATE in the loop above, it should have
  // been evicted.
  ASSERT_EQ(-1, Lookup(200));
}

TEST_P(LRUCacheTest, UpdateEvictionPolicy) {
  static constexpr int kNumElems = 1000;
  const int size_per_elem = cache_size() / kNumElems;

  Insert(100, 101);
  Insert(200, 201);

  // Loop adding and looking up new entries, but repeatedly accessing key 100.
  // This frequently-used entry should not be evicted. It also accesses key 200,
  // but the lookup uses NO_UPDATE, so this key is not preserved.
  for (int i = 0; i < kNumElems + 1000; ++i) {
    Insert(1000+i, 2000+i);
    UpdateCharge(1000+i, size_per_elem);
    ASSERT_EQ(2000+i, Lookup(1000+i));
    ASSERT_EQ(101, Lookup(100));
    int entry200val = Lookup(200, Cache::NO_UPDATE);
    ASSERT_TRUE(entry200val == -1 || entry200val == 201);
  }
  ASSERT_EQ(101, Lookup(100));
  // Since '200' was accessed using NO_UPDATE in the loop above, it should have
  // been evicted.
  ASSERT_EQ(-1, Lookup(200));
}

}  // namespace impala
