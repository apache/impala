// Some portions Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/cache/cache.h"
#include "util/cache/cache-test.h"

#include <memory>
#include <tuple>
#include <utility>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/mem_tracker.h"
#include "testutil/gtest-util.h"

DECLARE_bool(cache_force_single_shard);

DECLARE_double(cache_memtracker_approximation_ratio);

using std::make_tuple;
using std::tuple;

namespace impala {

void CacheBaseTest::SetupWithParameters(Cache::EvictionPolicy eviction_policy,
    ShardingPolicy sharding_policy) {
  // Disable approximate tracking of cache memory since we make specific
  // assertions on the MemTracker in this test.
  FLAGS_cache_memtracker_approximation_ratio = 0;

  // Using single shard makes the logic of scenarios simple for capacity-
  // and eviction-related behavior.
  FLAGS_cache_force_single_shard =
    (sharding_policy == ShardingPolicy::SingleShard);

  switch (eviction_policy) {
    case Cache::EvictionPolicy::FIFO:
      cache_.reset(NewCache(Cache::EvictionPolicy::FIFO, cache_size(), "cache_test"));
      kudu::MemTracker::FindTracker("cache_test-sharded_fifo_cache", &mem_tracker_);
      break;
    case Cache::EvictionPolicy::LRU:
      cache_.reset(NewCache(Cache::EvictionPolicy::LRU, cache_size(), "cache_test"));
      kudu::MemTracker::FindTracker("cache_test-sharded_lru_cache", &mem_tracker_);
      break;
    case Cache::EvictionPolicy::LIRS:
      cache_.reset(NewCache(Cache::EvictionPolicy::LIRS, cache_size(), "cache_test"));
      kudu::MemTracker::FindTracker("cache_test-sharded_lirs_cache", &mem_tracker_);
      break;
    default:
      FAIL() << "unrecognized cache eviction policy";
  }
  ASSERT_TRUE(mem_tracker_.get());
  Status status = cache_->Init();
  ASSERT_OK(status);
}

class CacheTest :
    public CacheBaseTest,
    public ::testing::WithParamInterface<tuple<Cache::EvictionPolicy, ShardingPolicy>> {
 public:
  CacheTest()
      : CacheBaseTest(16 * 1024 * 1024) {
  }

  void SetUp() override {
    const auto& param = GetParam();
    SetupWithParameters(std::get<0>(param),
                        std::get<1>(param));
  }
};

INSTANTIATE_TEST_SUITE_P(
    CacheTypes, CacheTest,
    ::testing::Values(
        make_tuple(Cache::EvictionPolicy::FIFO,
                   ShardingPolicy::MultiShard),
        make_tuple(Cache::EvictionPolicy::FIFO,
                   ShardingPolicy::SingleShard),
        make_tuple(Cache::EvictionPolicy::LRU,
                   ShardingPolicy::MultiShard),
        make_tuple(Cache::EvictionPolicy::LRU,
                   ShardingPolicy::SingleShard),
        make_tuple(Cache::EvictionPolicy::LIRS,
                   ShardingPolicy::SingleShard)));

TEST_P(CacheTest, TrackMemory) {
  if (mem_tracker_) {
    Insert(100, 100, 1);
    ASSERT_EQ(1, mem_tracker_->consumption());
    UpdateCharge(100, 2);
    ASSERT_EQ(2, mem_tracker_->consumption());
    Erase(100);
    ASSERT_EQ(0, mem_tracker_->consumption());
    ASSERT_EQ(2, mem_tracker_->peak_consumption());
  }
}

TEST_P(CacheTest, HitAndMiss) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1,  Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  ASSERT_EQ(1, evicted_keys_.size());
  ASSERT_EQ(100, evicted_keys_[0]);
  ASSERT_EQ(101, evicted_values_[0]);
}

TEST_P(CacheTest, Erase) {
  Erase(200);
  ASSERT_EQ(0, evicted_keys_.size());

  Insert(100, 101);
  Insert(200, 201);
  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, evicted_keys_.size());
  ASSERT_EQ(100, evicted_keys_[0]);
  ASSERT_EQ(101, evicted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, evicted_keys_.size());
}

TEST_P(CacheTest, EntriesArePinned) {
  Insert(100, 101);
  auto h1 = cache_->Lookup(EncodeInt(100));
  ASSERT_EQ(101, DecodeInt(cache_->Value(h1)));

  Insert(100, 102);
  auto h2 = cache_->Lookup(EncodeInt(100));
  ASSERT_EQ(102, DecodeInt(cache_->Value(h2)));
  ASSERT_EQ(0, evicted_keys_.size());

  h1.reset();
  ASSERT_EQ(1, evicted_keys_.size());
  ASSERT_EQ(100, evicted_keys_[0]);
  ASSERT_EQ(101, evicted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(1, evicted_keys_.size());

  h2.reset();
  ASSERT_EQ(2, evicted_keys_.size());
  ASSERT_EQ(100, evicted_keys_[1]);
  ASSERT_EQ(102, evicted_values_[1]);
}

TEST_P(CacheTest, UpdateChargeCausesEviction) {
  // Canary entries that could be evicted. Insert 1000 entries so
  // that each shard would have some entries.
  for (int i = 0; i < 1000; ++i) {
    Insert(1000 + i, 1000 + i);
  }

  Insert(100, 100);
  auto h1 = cache_->Lookup(EncodeInt(100));
  ASSERT_NE(h1, nullptr);

  // Updating the charge for the cache entry evicts something
  cache_->UpdateCharge(h1, cache_->MaxCharge());
  ASSERT_GT(evicted_keys_.size(), 0);
  ASSERT_GT(evicted_values_.size(), 0);
}

TEST_P(CacheTest, UpdateChargeForErased) {
  // Insert 1000 entries so that each shard would have some entries.
  for (int i = 0; i < 1000; ++i) {
    Insert(1000 + i, 1000 + i);
  }

  Insert(100, 100);
  auto h1 = cache_->Lookup(EncodeInt(100));
  ASSERT_NE(h1, nullptr);

  Erase(100);

  // Updating the charge for the erased element doesn't do anything,
  // because the entry has been erased.
  cache_->UpdateCharge(h1, cache_->MaxCharge());

  ASSERT_EQ(evicted_keys_.size(), 0);
  ASSERT_EQ(evicted_values_.size(), 0);
}

// Add a bunch of light and heavy entries and then count the combined
// size of items still in the cache, which must be approximately the
// same as the total capacity.
TEST_P(CacheTest, HeavyEntries) {
  const int kLight = cache_size() / 1000;
  const int kHeavy = cache_size() / 100;
  int added = 0;
  int index = 0;
  while (added < 2 * cache_size()) {
    const int weight = (index & 1) ? kLight : kHeavy;
    Insert(index, 1000+index, weight);
    added += weight;
    ++index;
  }

  int cached_weight = 0;
  for (int i = 0; i < index; ++i) {
    const int weight = (i & 1 ? kLight : kHeavy);
    int r = Lookup(i);
    if (r >= 0) {
      cached_weight += weight;
      ASSERT_EQ(1000+i, r);
    }
  }
  ASSERT_LE(cached_weight, cache_size() + cache_size() / 10);
}

TEST_P(CacheTest, UpdateHeavyEntries) {
  const int kLight = cache_size() / 1000;
  const int kHeavy = cache_size() / 100;
  int added = 0;
  int index = 0;
  while (added < 2 * cache_size()) {
    Insert(index, 1000+index, kLight);
    const int weight = (index & 1) ? kLight : kHeavy;
    UpdateCharge(index, weight);
    added += weight;
    ++index;
  }

  int cached_weight = 0;
  for (int i = 0; i < index; ++i) {
    const int weight = (i & 1 ? kLight : kHeavy);
    int r = Lookup(i);
    if (r >= 0) {
      cached_weight += weight;
      ASSERT_EQ(1000+i, r);
    }
  }
  ASSERT_LE(cached_weight, cache_size() + cache_size() / 10);
  // FIFO won't use space very efficiently with alternating light/heavy entries.
  ASSERT_GE(cached_weight, cache_size() / 2);
}

TEST_P(CacheTest, MaxCharge) {
  // The default size of the cache is smaller than INTMAXVAL, so the max
  // charge will be related to the cache size.
  int max_charge = cache_->MaxCharge();
  if (FLAGS_cache_force_single_shard) {
    ASSERT_EQ(max_charge, cache_size());
  } else {
    // Split across multiple shards (# of CPUs), but we don't specifically
    // know how many.
    ASSERT_LE(max_charge, cache_size());
  }
}

}  // namespace impala

