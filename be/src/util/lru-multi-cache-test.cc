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

#include "util/lru-multi-cache.inline.h"

#include "testutil/gtest-util.h"

#include <memory>

struct CollidingKey {
  CollidingKey(const std::string& s) : s(s) {}
  CollidingKey(const char* s) : s(s) {}
  std::string s;
};

bool operator==(const CollidingKey& k1, const CollidingKey& k2) {
  return k1.s == k2.s;
}

template <>
struct std::hash<CollidingKey> {
  size_t operator()(const CollidingKey& k) const noexcept { return 0; }
};

namespace impala {

struct TestType {
  // Testing EmplaceAndGet with perfect forwarding
  explicit TestType(int i, float) : i(i) {}
  int i;

  // Making sure nothing is being copied or moved
  TestType(const TestType&) = delete;
  TestType(TestType&&) = delete;

  TestType& operator=(const TestType&) = delete;
  TestType& operator=(const TestType&&) = delete;
};

// Notation for implied state of inner data structure:
// 108 -> 107 -> .... -> 102 | (19) (18) (109)
// LRU list has 108 107 106 105 104 103 102 in this order, 7 elements available
// 19, 18 and 109 are currently in use
// 7 + 3 = 10 total elements in cache

TEST(LruMultiCache, BasicTests) {
  LruMultiCache<std::string, TestType> cache(100);

  const size_t num_of_parallel_keys = 5;
  std::string keys[num_of_parallel_keys] = {"a", "b", "c", "d", "e"};
  int value_bases[num_of_parallel_keys] = {1, 10, 100, 1000, 10000};

  for (size_t i = 0; i < 5; i++) {
    std::string& key = keys[i];
    int& value_base = value_bases[i];

    // {}
    ASSERT_EQ(nullptr, cache.Get(key).Get());

    auto value = cache.EmplaceAndGet(key, value_base, 1.0f);

    ASSERT_EQ(nullptr, cache.Get(key).Get());

    value.Release();

    // 1
    value = cache.Get(key);

    ASSERT_NE(nullptr, value.Get());

    value.Release();

    auto value2 = cache.EmplaceAndGet(key, value_base + 1, 1.0f);

    value = cache.Get(key);

    ASSERT_EQ(value_base, value.Get()->i);

    value.Release();
    value2.Release();
    // 2 -> 1
  }

  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(10, cache.NumberOfAvailableObjects());

  for (size_t i = 0; i < 5; i++) {
    std::string& key = keys[i];
    int& value_base = value_bases[i];

    // 2 -> 1
    auto value2 = cache.Get(key);

    ASSERT_EQ(value_base + 1, value2.Get()->i);

    auto value = cache.Get(key);

    ASSERT_EQ(value_base, value.Get()->i);

    value2.Release();
    value.Release();

    // 1 -> 2

    value = cache.Get(key);

    ASSERT_EQ(value_base, value.Get()->i);

    cache.Rehash();

    value2 = cache.Get(key);

    ASSERT_EQ(value_base + 1, value2.Get()->i);

    value.Release();
    value2.Release();
    // 2 -> 1

    cache.Rehash();
  }
}

TEST(LruMultiCache, EvictionTests) {
  const size_t cache_capacity = 10;
  LruMultiCache<std::string, TestType> cache(cache_capacity);
  std::string key = "a";

  for (size_t i = 0; i < cache_capacity; i++) {
    cache.EmplaceAndGet(key, i, 1.0f).Release();
    ASSERT_EQ(i + 1, cache.Size());
    ASSERT_EQ(i + 1, cache.NumberOfAvailableObjects());
  }

  // 9 -> 8 .... -> 0
  auto value = cache.Get(key);

  ASSERT_EQ(9, value.Get()->i);
  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(9, cache.NumberOfAvailableObjects());

  value.Release();

  for (size_t i = 0; i < cache_capacity; i++) {
    cache.EmplaceAndGet(key, 10 + i, 1.0f).Release();
    ASSERT_EQ(10, cache.Size());
    ASSERT_EQ(10, cache.NumberOfAvailableObjects());
  }

  // 19 -> 18 .... -> 10
  value = cache.Get(key);
  ASSERT_EQ(19, value.Get()->i);

  // 18 .... -> 10 | (19)
  auto value2 = cache.Get(key);
  ASSERT_EQ(18, value2.Get()->i);

  // 17 -> 16 .... -> 10 | (19) (18)
  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(8, cache.NumberOfAvailableObjects());

  cache.Rehash();

  for (size_t i = 0; i < cache_capacity; i++) {
    cache.EmplaceAndGet(key, 100 + i, 1.0f).Release();
    ASSERT_EQ(10, cache.Size());
    ASSERT_EQ(8, cache.NumberOfAvailableObjects());
  }

  // 109 -> 108 .... -> 102 | (19) (18)
  auto value3 = cache.Get(key);
  ASSERT_EQ(109, value3.Get()->i);

  // 108 .... -> 102 | (19) (18) (109)
  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(7, cache.NumberOfAvailableObjects());

  value2.Release();

  // 18 -> 108 .... -> 102 | (19) (109)
  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(8, cache.NumberOfAvailableObjects());

  auto value4 = cache.Get(key);
  ASSERT_EQ(18, value4.Get()->i);

  // 108 .... -> 102 | (19) (109) (18)
  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(7, cache.NumberOfAvailableObjects());

  value.Release();
  value3.Release();
  value4.Release();

  for (size_t i = 0; i < cache_capacity; i++) {
    cache.EmplaceAndGet(key, 1000 + i, 1.0f).Release();
    ASSERT_EQ(10, cache.Size());
    ASSERT_EQ(10, cache.NumberOfAvailableObjects());
  }

  // 1009 .... -> 1000

  std::vector<LruMultiCache<std::string, TestType>::Accessor> values;

  for (size_t i = 0; i < cache_capacity; i++) {
    values.push_back(cache.Get(key));
    ASSERT_EQ(1000 + (cache_capacity - 1 - i), values[i].Get()->i);
  }

  // {} | (1009) ... (1000)
  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(0, cache.NumberOfAvailableObjects());

  value = cache.EmplaceAndGet(key, 10000, 1.0f);
  ASSERT_EQ(10000, value.Get()->i);

  // {} | (10001) (1009) ... (1000)
  ASSERT_EQ(11, cache.Size());
  ASSERT_EQ(0, cache.NumberOfAvailableObjects());

  value2 = cache.EmplaceAndGet(key, 10001, 1.0f);
  ASSERT_EQ(10001, value2.Get()->i);

  // {} | (10001) (10000) (1009) ... (1000)
  ASSERT_EQ(12, cache.Size());
  ASSERT_EQ(0, cache.NumberOfAvailableObjects());

  values[0].Release();
  values[1].Release();

  // {} | (10001) (10000) (1007) ... (1000)
  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(0, cache.NumberOfAvailableObjects());

  for (size_t i = 2; i < cache_capacity; i++) {
    values[i].Release();
  }

  // 1000 -> ... -> 1007 | (10001) (10000)
  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(8, cache.NumberOfAvailableObjects());

  value3 = cache.Get(key);
  ASSERT_EQ(1000, value3.Get()->i);

  // 1001 -> ... -> 1007 | (10001) (10000) (1000)
  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(7, cache.NumberOfAvailableObjects());

  value2.Release();

  // 10001 -> 1001 -> ... -> 1007 | (10000) (1000)
  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(8, cache.NumberOfAvailableObjects());

  value4 = cache.Get(key);
  ASSERT_EQ(10001, value4.Get()->i);

  // 1001 -> ... -> 1007 | (10001) (10000) (1000)
  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(7, cache.NumberOfAvailableObjects());
}

TEST(LruMultiCache, AutoRelease) {
  const size_t cache_capacity = 10;
  LruMultiCache<std::string, TestType> cache(cache_capacity);
  std::string key = "a";

  for (size_t i = 0; i < cache_capacity; i++) {
    { auto accessor = cache.EmplaceAndGet(key, i, 1.0f); }
    ASSERT_EQ(i + 1, cache.Size());
    ASSERT_EQ(i + 1, cache.NumberOfAvailableObjects());
  }

  // 9 -> 8 .... -> 0
  {
    auto value = cache.Get(key);

    ASSERT_EQ(9, value.Get()->i);
    ASSERT_EQ(10, cache.Size());
    ASSERT_EQ(9, cache.NumberOfAvailableObjects());
  }

  for (size_t i = 0; i < cache_capacity; i++) {
    { auto accessor = cache.EmplaceAndGet(key, 10 + i, 1.0f); }
    ASSERT_EQ(10, cache.Size());
    ASSERT_EQ(10, cache.NumberOfAvailableObjects());
  }
}

TEST(LruMultiCache, Destroy) {
  const size_t cache_capacity = 10;
  LruMultiCache<std::string, TestType> cache(cache_capacity);
  std::string key = "a";

  for (size_t i = 0; i < cache_capacity; i++) {
    { auto accessor = cache.EmplaceAndGet(key, i, 1.0f); }
    ASSERT_EQ(i + 1, cache.Size());
    ASSERT_EQ(i + 1, cache.NumberOfAvailableObjects());
  }

  // 9 -> 8 .... -> 0
  {
    auto value = cache.Get(key);

    ASSERT_EQ(9, value.Get()->i);
    ASSERT_EQ(10, cache.Size());
    ASSERT_EQ(9, cache.NumberOfAvailableObjects());

    // 8 -> .... -> 0
    value.Destroy();
    ASSERT_EQ(9, cache.Size());
    ASSERT_EQ(9, cache.NumberOfAvailableObjects());
  }

  ASSERT_EQ(9, cache.Size());
  ASSERT_EQ(9, cache.NumberOfAvailableObjects());

  for (size_t i = 0; i < cache_capacity; i++) {
    { auto accessor = cache.EmplaceAndGet(key, 10 + i, 1.0f); }
    ASSERT_EQ(10, cache.Size());
    ASSERT_EQ(10, cache.NumberOfAvailableObjects());
  }
}

TEST(LruMultiCache, RemoveEmptyList) {
  const size_t cache_capacity = 10;
  LruMultiCache<std::string, TestType> cache(cache_capacity);
  std::string key = "a";

  ASSERT_EQ(0, cache.NumberOfKeys());

  auto accessor = cache.EmplaceAndGet(key, 0, 1.0f);
  ASSERT_EQ(1, cache.NumberOfKeys());

  // Last element is destroyed in "a" list
  accessor.Destroy();
  ASSERT_EQ(0, cache.NumberOfKeys());

  // Removed by eviction

  for (size_t i = 0; i < cache_capacity; i++) {
    cache.EmplaceAndGet(key, i, 1.0f).Release();
    ASSERT_EQ(i + 1, cache.Size());
    ASSERT_EQ(i + 1, cache.NumberOfAvailableObjects());
  }

  // All in "a" list
  ASSERT_EQ(1, cache.NumberOfKeys());

  std::string key2 = "b";

  for (size_t i = 0; i < (cache_capacity - 1); i++) {
    cache.EmplaceAndGet(key2, 10 + i, 1.0f).Release();
    ASSERT_EQ(10, cache.Size());
    ASSERT_EQ(10, cache.NumberOfAvailableObjects());

    // 9-i in "a" list, i+1 in "b" list
    ASSERT_EQ(2, cache.NumberOfKeys());
  }

  cache.EmplaceAndGet(key2, 19, 1.0f).Release();

  // 10 in "b" list, "a" is removed
  ASSERT_EQ(1, cache.NumberOfKeys());
}

TEST(LruMultiCache, HashCollision) {
  const size_t cache_capacity = 10;
  LruMultiCache<CollidingKey, TestType> cache(cache_capacity);

  const size_t num_of_parallel_keys = 5;
  CollidingKey keys[num_of_parallel_keys] = {"a", "b", "c", "d", "e"};
  int value_bases[num_of_parallel_keys] = {1, 10, 100, 1000, 10000};

  for (size_t i = 0; i < 5; i++) {
    CollidingKey& key = keys[i];
    int& value_base = value_bases[i];

    // {}
    ASSERT_EQ(nullptr, cache.Get(key).Get());

    auto value = cache.EmplaceAndGet(key, value_base, 1.0f);

    ASSERT_EQ(nullptr, cache.Get(key).Get());

    value.Release();

    // 1
    value = cache.Get(key);

    ASSERT_NE(nullptr, value.Get());

    value.Release();

    auto value2 = cache.EmplaceAndGet(key, value_base + 1, 1.0f);

    value = cache.Get(key);

    ASSERT_EQ(value_base, value.Get()->i);

    value.Release();
    value2.Release();
    // 2 -> 1
  }

  ASSERT_EQ(10, cache.Size());
  ASSERT_EQ(10, cache.NumberOfAvailableObjects());

  for (size_t i = 0; i < 5; i++) {
    CollidingKey& key = keys[i];
    int& value_base = value_bases[i];

    // 2 -> 1
    auto value2 = cache.Get(key);

    ASSERT_EQ(value_base + 1, value2.Get()->i);

    auto value = cache.Get(key);

    ASSERT_EQ(value_base, value.Get()->i);

    value2.Release();
    value.Release();

    // 1 -> 2

    value = cache.Get(key);

    ASSERT_EQ(value_base, value.Get()->i);

    cache.Rehash();

    value2 = cache.Get(key);

    ASSERT_EQ(value_base + 1, value2.Get()->i);

    value.Release();
    value2.Release();
    // 2 -> 1

    cache.Rehash();
  }
}

} // namespace impala