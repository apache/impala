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

#include <string>

#include "runtime/free-pool.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "testutil/gtest-util.h"

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
    EXPECT_EQ(p1, p2);
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

  // We know have 2 1 byte allocations, which were rounded up to 8 bytes. Make an 8
  // byte allocation, which can reuse one of the existing ones.
  uint8_t* p4 = pool.Allocate(2);
  memset(p4, 123, 2);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 32);
  EXPECT_TRUE(p4 == p1 || p4 == p2 || p4 == p3);
  pool.Free(p4);

  // Make a 9 byte allocation, which requires a new allocation.
  uint8_t* p5 = pool.Allocate(9);
  memset(p5, 123, 9);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 56);
  pool.Free(p5);
  EXPECT_TRUE(p5 != p1);
  EXPECT_TRUE(p5 != p2);
  EXPECT_TRUE(p5 != p3);

  // Everything's freed. Try grabbing the ones that have been allocated.
  p1 = pool.Allocate(1);
  *p1 = 123;
  p2 = pool.Allocate(1);
  *p2 = 123;
  p5 = pool.Allocate(9);
  memset(p5, 123, 9);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 56);

  // Make another 1 byte allocation.
  p4 = pool.Allocate(1);
  *p4 = 1;
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 72);

  mem_pool.FreeAll();

  // Try making allocations larger than 1GB.
  uint8_t* p6 = pool.Allocate(1LL << 32);
  EXPECT_TRUE(p6 != NULL);
  for (int64_t i = 0; i < (1LL << 32); i += (1 << 29)) {
    *(p6 + i) = i;
  }
  EXPECT_EQ(mem_pool.total_allocated_bytes(), (1LL << 32) + 8);

  // Test zero-byte allocation.
  p6 = pool.Allocate(0);
  EXPECT_TRUE(p6 != NULL);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), (1LL << 32) + 8);
  pool.Free(p6);

  mem_pool.FreeAll();
}

#ifdef ADDRESS_SANITIZER

// The following tests confirm that ASAN catches invalid memory accesses thanks to the
// FreePool manually (un)poisoning memory.
TEST(FreePoolTest, OutOfBoundsAccess) {
  MemTracker tracker;
  MemPool mem_pool(&tracker);
  FreePool pool(&mem_pool);

  uint8_t* ptr = pool.Allocate(5);
  ptr[4] = 'O';
  ASSERT_DEATH({ptr[5] = 'X';}, "use-after-poison");
  mem_pool.FreeAll();
}

TEST(FreePoolTest, UseAfterFree) {
  MemTracker tracker;
  MemPool mem_pool(&tracker);
  FreePool pool(&mem_pool);

  uint8_t* ptr = pool.Allocate(5);
  ptr[4] = 'O';
  pool.Free(ptr);
  ASSERT_DEATH({ptr[0] = 'X';}, "use-after-poison");
  mem_pool.FreeAll();
}

TEST(FreePoolTest, ReallocPoison) {
  MemTracker tracker;
  MemPool mem_pool(&tracker);
  FreePool pool(&mem_pool);

  uint8_t* ptr = pool.Allocate(32);
  ptr[30] = 'O';
  ptr = pool.Reallocate(ptr, 16);
  ASSERT_DEATH({ptr[30] = 'X';}, "use-after-poison");
  mem_pool.FreeAll();
}

#endif


// In this test we make two allocations at increasing sizes and then we
// free both to prime the pool. Then, for a few iterations, we make the same allocations
// as we did to prime the pool in a random order. We validate that the returned
// allocation is one of the two original and the pool did not increase in size.
TEST(FreePoolTest, Loop) {
  MemTracker tracker;
  MemPool mem_pool(&tracker);
  FreePool pool(&mem_pool);

  map<int64_t, pair<uint8_t*, uint8_t*>> primed_allocations;
  vector<int64_t> allocation_sizes;

  int64_t expected_pool_size = 0;

  // Pick a non-power of 2 to exercise more code.
  for (int64_t size = 5; size < 6LL * 1024 * 1024 * 1024; size *= 5) {
    uint8_t* p1 = pool.Allocate(size);
    uint8_t* p2 = pool.Allocate(size);
    EXPECT_TRUE(p1 != NULL);
    EXPECT_TRUE(p2 != NULL);
    EXPECT_TRUE(p1 != p2);
    // Scribble the expected value (used below).
    *p1 = 1;
    *p2 = 1;
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

  // Try allocation larger than 1GB.
  uint8_t* ptr4 = pool.Reallocate(ptr3, 1LL << 32);
  EXPECT_TRUE(ptr3 != ptr4);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 1024 + 8 + 2048 + 8 + (1LL << 32) + 8);

  // Shrink the allocation.
  uint8_t* ptr5 = pool.Reallocate(ptr4, 1024);
  EXPECT_TRUE(ptr4 == ptr5);
  EXPECT_EQ(mem_pool.total_allocated_bytes(), 1024 + 8 + 2048 + 8 + (1LL << 32) + 8);
  pool.Free(ptr5);

  mem_pool.FreeAll();
}

}

