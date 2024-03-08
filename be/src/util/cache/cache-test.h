// Some portions Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/cache/cache.h"

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/util/coding.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/slice.h"

namespace impala {

// Conversions between numeric keys/values and the types expected by Cache.
static std::string EncodeInt(int k) {
  kudu::faststring result;
  kudu::PutFixed32(&result, k);
  return result.ToString();
}
static int DecodeInt(const Slice& k) {
  CHECK_EQ(4, k.size());
  return kudu::DecodeFixed32(k.data());
}

// Cache sharding policy affects the composition of the cache. Some test
// scenarios assume cache is single-sharded to keep the logic simpler.
enum class ShardingPolicy {
  MultiShard,
  SingleShard,
};

// CacheBaseTest provides a base test class for testing a variety of different cache
// eviction policies (LRU, LIRS, FIFO). It wraps the common functions to simplify the
// calls for tests and to use integer keys and values, so that tests can avoid using
// Slice types directly. It also maintains a record of all evictions that take place,
// both the keys and the values.
// NOTE: The structures are not synchronized, so this is only useful for single-threaded
// tests.
class CacheBaseTest : public ::testing::Test,
                      public Cache::EvictionCallback {
 public:
  explicit CacheBaseTest(size_t cache_size)
      : cache_size_(cache_size) {
  }

  size_t cache_size() const {
    return cache_size_;
  }

  // Implementation of the EvictionCallback interface. This maintains a record of all
  // evictions, keys and values.
  void EvictedEntry(Slice key, Slice val) override {
    evicted_keys_.push_back(DecodeInt(key));
    evicted_values_.push_back(DecodeInt(val));
  }

  // Lookup the key. Return the value on success or -1 on failure.
  int Lookup(int key, Cache::LookupBehavior behavior = Cache::NORMAL) {
    auto handle(cache_->Lookup(EncodeInt(key), behavior));
    return handle ? DecodeInt(cache_->Value(handle)) : -1;
  }

  void UpdateCharge(int key, int charge) {
    auto handle(cache_->Lookup(EncodeInt(key), Cache::NO_UPDATE));
    cache_->UpdateCharge(handle, charge);
  }

  // Insert a key with the give value and charge. Return whether the insert was
  // successful.
  bool Insert(int key, int value, int charge = 1) {
    std::string key_str = EncodeInt(key);
    std::string val_str = EncodeInt(value);
    auto handle(cache_->Allocate(key_str, val_str.size(), charge));
    // This can be null if Allocate rejects something with this charge.
    if (handle == nullptr) return false;
    memcpy(cache_->MutableValue(&handle), val_str.data(), val_str.size());
    auto inserted_handle(cache_->Insert(std::move(handle), this));
    // Insert can fail and return nullptr
    if (inserted_handle == nullptr) return false;
    return true;
  }

  void Erase(int key) {
    cache_->Erase(EncodeInt(key));
  }

 protected:
  // This initializes the cache with the specified 'eviction_policy' and
  // 'sharding_policy'. Child classes frequently call this from the SetUp() method.
  // For example, there are shared tests that apply to all eviction algorithms. This
  // function is used during SetUp() to allow those shared tests to be parameterized to
  // use various combinations of eviction policy and sharding policy.
  void SetupWithParameters(Cache::EvictionPolicy eviction_policy,
      ShardingPolicy sharding_policy);

  const size_t cache_size_;
  // Record of all evicted keys/values. When an entry is evicted, its key is put in
  // evicted_keys_ and its value is put in evicted_values_. These are inserted in
  // the order they are evicted (i.e. index 0 happened before index 1).
  std::vector<int> evicted_keys_;
  std::vector<int> evicted_values_;
  std::shared_ptr<kudu::MemTracker> mem_tracker_;
  std::unique_ptr<Cache> cache_;
};

} // namespace impala
