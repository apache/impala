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

#include <string>
#include <gtest/gtest.h>

#include "runtime/free-pool.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"

#include "common/names.h"

namespace impala {

TEST(FreePoolTest, Basic) {
  MemTracker tracker;
  MemPool mem_pool(&tracker);
  FreePool pool(&mem_pool);

  // Start off with some corner cases.
  uint8_t* p1 = pool.Allocate(0);
  ASSERT_TRUE(p1 != NULL);
  pool.Free(p1);
  pool.Free(NULL);

  EXPECT_EQ(mem_pool.total_allocated_bytes(), 0);
  p1 = pool.Allocate(1);
  *p1 = 111; // Scribble something to make sure it doesn't mess up the list.
  ASSERT_TRUE(p1 != NULL);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 16);
  pool.Free(p1);

  // Allocating this should return p again.
  for (int i = 0; i < 10; ++i) {
    uint8_t* p2 = pool.Allocate(1);
    *p2 = 111;
    ASSERT_EQ(p1, p2);
    EXPECT_EQ(mem_pool.total_allocated_bytes(), 16);
    pool.Free(p2);
  }

  uint8_t* p2 = pool.Allocate(1);
  *p2 = 111;
  // p3 will cause a new allocation.
  uint8_t* p3 = pool.Allocate(1);
  *p3 = 111;
  EXPECT_TRUE(p1 == p2);
  EXPECT_TRUE(p1 != p3);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 32);
  pool.Free(p2);
  pool.Free(p3);

  // We know have 2 1 byte allocations. Make a two byte allocation.
  uint8_t* p4 = pool.Allocate(2);
  memset(p4, 2, 123);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 48);
  EXPECT_TRUE(p4 != p1);
  EXPECT_TRUE(p4 != p2);
  EXPECT_TRUE(p4 != p3);
  pool.Free(p4);

  // Everything's freed. Try grabbing the ones that have been allocated.
  p1 = pool.Allocate(1);
  *p1 = 123;
  p2 = pool.Allocate(1);
  *p2 = 123;
  p3 = pool.Allocate(2);
  memset(p3, 123, 2);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 48);

  // Make another 1 byte allocation.
  p4 = pool.Allocate(1);
  *p4 = 1;
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 64);

  mem_pool.FreeAll();
}

// In this test we make two allocations at increasing sizes and then we
// free both to prime the pool. Then, for a few iterations, we make the same allocations
// as we did to prime the pool in a random order. We validate that the returned
// allocation is one of the two original and the pool did not increase in size.
TEST(FreePoolTest, Loop) {
  MemTracker tracker;
  MemPool mem_pool(&tracker);
  FreePool pool(&mem_pool);

  map<int, pair<uint8_t*, uint8_t*> > primed_allocations;
  vector<int> allocation_sizes;

  int64_t expected_pool_size = 0;

  // Pick a non-power of 2 to exercise more code.
  for (int size = 3; size < 1024 * 1024 * 1024; size *= 3) {
    uint8_t* p1 = pool.Allocate(size);
    uint8_t* p2 = pool.Allocate(size);
    EXPECT_TRUE(p1 != NULL);
    EXPECT_TRUE(p2 != NULL);
    EXPECT_TRUE(p1 != p2);
    memset(p1, 1, size); // Scribble the expected value (used below).
    memset(p2, 1, size);
    primed_allocations[size] = make_pair(p1, p2);
    pool.Free(p1);
    pool.Free(p2);
    allocation_sizes.push_back(size);
  }
  expected_pool_size = mem_pool.total_allocated_bytes();

  for (int iters = 1; iters <= 5; ++iters) {
    std::random_shuffle(allocation_sizes.begin(), allocation_sizes.end());
    for (int i = 0; i < allocation_sizes.size(); ++i) {
      uint8_t* p1 = pool.Allocate(allocation_sizes[i]);
      uint8_t* p2 = pool.Allocate(allocation_sizes[i]);
      EXPECT_TRUE(p1 != p2);
      EXPECT_TRUE(p1 == primed_allocations[allocation_sizes[i]].first ||
                  p1 == primed_allocations[allocation_sizes[i]].second);
      EXPECT_TRUE(p2 == primed_allocations[allocation_sizes[i]].first ||
                  p2 == primed_allocations[allocation_sizes[i]].second);
      EXPECT_EQ(*p1, iters);
      EXPECT_EQ(*p2, iters);
      ++(*p1);
      ++(*p2);
      pool.Free(p1);
      pool.Free(p2);
    }
    EXPECT_EQ(mem_pool.total_allocated_bytes(), expected_pool_size);
  }

  mem_pool.FreeAll();
}

TEST(FreePoolTest, ReAlloc) {
  MemTracker tracker;
  MemPool mem_pool(&tracker);
  FreePool pool(&mem_pool);

  uint8_t* ptr = pool.Reallocate(NULL, 0);
  ptr = pool.Reallocate(ptr, 0);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 0);

  ptr = pool.Reallocate(ptr, 600);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 1024 + 8);
  uint8_t* ptr2 = pool.Reallocate(ptr, 200);
  EXPECT_TRUE(ptr == ptr2);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 1024 + 8);

  uint8_t* ptr3 = pool.Reallocate(ptr, 2000);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 1024 + 8 + 2048 + 8);
  EXPECT_TRUE(ptr2 != ptr3);

  // The original 600 allocation should be there.
  ptr = pool.Allocate(600);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 1024 + 8 + 2048 + 8);

  mem_pool.FreeAll();
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

