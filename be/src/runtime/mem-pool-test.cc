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

#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "testutil/gtest-util.h"
#include "util/bit-util.h"

#include "common/names.h"

// Maximum allocation size which exceeds 32-bit.
#define LARGE_ALLOC_SIZE (1LL << 32)

namespace impala {

// Utility class to call private functions on MemPool.
class MemPoolTest {
 public:
  static bool CheckIntegrity(MemPool* pool, bool current_chunk_empty) {
    return pool->CheckIntegrity(current_chunk_empty);
  }

  static const int INITIAL_CHUNK_SIZE = MemPool::INITIAL_CHUNK_SIZE;
  static const int MAX_CHUNK_SIZE = MemPool::MAX_CHUNK_SIZE;
};

const int MemPoolTest::INITIAL_CHUNK_SIZE;
const int MemPoolTest::MAX_CHUNK_SIZE;

TEST(MemPoolTest, Basic) {
  MemTracker tracker;
  MemPool p(&tracker);
  MemPool p2(&tracker);
  MemPool p3(&tracker);

  uint8_t* ptr = NULL;

  for (int iter = 0; iter < 2; ++iter) {
    // Allocate 768 25 + 7 byte allocations.
    for (int i = 0; i < 768; ++i) {
      ptr = p.Allocate(25);
      EXPECT_TRUE(ptr != NULL);
      EXPECT_EQ(0, reinterpret_cast<uintptr_t>(ptr) % MemPool::DEFAULT_ALIGNMENT)
          << "Allocation should be aligned";

      // Allocate the padding to get 32 bytes
      ptr = p.TryAllocateAligned(7, 1);
      EXPECT_TRUE(ptr != NULL);
    }
    // We allocated 24K from 28K of chunks (4, 8, 16)
    EXPECT_EQ(24 * 1024, p.total_allocated_bytes());
    EXPECT_EQ(28 * 1024, p.GetTotalChunkSizes());

    // we're passing on the first two chunks, containing 12K of data; we're left with
    // one chunk of 16K containing 12K of data
    p2.AcquireData(&p, true);
    EXPECT_EQ(12 * 1024, p.total_allocated_bytes());
    EXPECT_EQ(16 * 1024, p.GetTotalChunkSizes());

    // we allocate 8K, for which there isn't enough room in the current chunk,
    // so another one is allocated (32K)
    p.Allocate(8 * 1024);
    EXPECT_EQ((16 + 32) * 1024, p.GetTotalChunkSizes());

    // we allocate 65K, which doesn't fit into the current chunk or the default
    // size of the next allocated chunk (64K)
    p.Allocate(65 * 1024);
    EXPECT_EQ((12 + 8 + 65) * 1024, p.total_allocated_bytes());
    EXPECT_EQ((16 + 32 + 65) * 1024, p.GetTotalChunkSizes());

    // Clear() resets allocated data, but doesn't remove any chunks
    p.Clear();
    EXPECT_EQ(0, p.total_allocated_bytes());
    EXPECT_EQ((16 + 32 + 65) * 1024, p.GetTotalChunkSizes());

    // next allocation reuses existing chunks
    p.Allocate(1024);
    EXPECT_EQ(1024, p.total_allocated_bytes());
    EXPECT_EQ((16 + 32 + 65) * 1024, p.GetTotalChunkSizes());

    // ... unless it doesn't fit into any available chunk
    p.Allocate(120 * 1024);
    EXPECT_EQ((1 + 120) * 1024, p.total_allocated_bytes());
    EXPECT_EQ((130 + 16 + 32 + 65) * 1024, p.GetTotalChunkSizes());

    // ... Try another chunk that fits into an existing chunk
    p.Allocate(33 * 1024);
    EXPECT_EQ((1 + 120 + 33) * 1024, p.total_allocated_bytes());
    EXPECT_EQ((130 + 16 + 32 + 65) * 1024, p.GetTotalChunkSizes());

    // we're releasing 3 chunks, which get added to p2
    p2.AcquireData(&p, false);
    EXPECT_EQ(0, p.total_allocated_bytes());
    EXPECT_EQ(0, p.GetTotalChunkSizes());

    p3.AcquireData(&p2, true);  // we're keeping the 65k chunk
    EXPECT_EQ(33 * 1024, p2.total_allocated_bytes());
    EXPECT_EQ(65 * 1024, p2.GetTotalChunkSizes());

    p.FreeAll();
    p2.FreeAll();
    p3.FreeAll();
  }

  // Test zero byte allocation.
  ptr = p.Allocate(0);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(0, p.GetTotalChunkSizes());
}

// Test that we can keep an allocated chunk and a free chunk.
// This case verifies that when chunks are acquired by another memory pool the
// remaining chunks are consistent if there were more than one used chunk and some
// free chunks.
TEST(MemPoolTest, Keep) {
  MemTracker tracker;
  MemPool p(&tracker);
  p.Allocate(4*1024);
  p.Allocate(8*1024);
  p.Allocate(16*1024);
  EXPECT_EQ((4 + 8 + 16) * 1024, p.total_allocated_bytes());
  EXPECT_EQ((4 + 8 + 16) * 1024, p.GetTotalChunkSizes());
  p.Clear();
  EXPECT_EQ(0, p.total_allocated_bytes());
  EXPECT_EQ((4 + 8 + 16) * 1024, p.GetTotalChunkSizes());
  p.Allocate(1*1024);
  p.Allocate(4*1024);
  EXPECT_EQ((1 + 4) * 1024, p.total_allocated_bytes());
  EXPECT_EQ((4 + 8 + 16) * 1024, p.GetTotalChunkSizes());
  MemPool p2(&tracker);
  p2.AcquireData(&p, true);
  EXPECT_EQ(4 * 1024, p.total_allocated_bytes());
  EXPECT_EQ((8 + 16) * 1024, p.GetTotalChunkSizes());
  EXPECT_EQ(1 * 1024, p2.total_allocated_bytes());
  EXPECT_EQ(4 * 1024, p2.GetTotalChunkSizes());

  p.FreeAll();
  p2.FreeAll();
}

#ifdef ADDRESS_SANITIZER

/// These tests confirm that ASAN will catch use-after-return errors (even though the
/// memory is still valid, just cached by the MemPool).
TEST(MemPoolTest, UseAfterClear) {
  MemTracker tracker;
  MemPool p(&tracker);
  uint8_t* ptr = p.Allocate(1024);
  ptr[10] = 'A';
  p.Clear();
  ASSERT_DEATH({ptr[10] = 'B';}, "use-after-poison");
  p.FreeAll();
}

TEST(MemPoolTest, UseAfterPartialReturn) {
  MemTracker tracker;
  MemPool p(&tracker);
  uint8_t* ptr = p.Allocate(1024);
  ptr[10] = 'A';
  p.ReturnPartialAllocation(512);
  ptr[511] = 'B';
  ASSERT_DEATH({ptr[512] = 'X';}, "use-after-poison");
  p.FreeAll();
}

#endif

// Tests that we can return partial allocations.
TEST(MemPoolTest, ReturnPartial) {
  MemTracker tracker;
  MemPool p(&tracker);
  uint8_t* ptr = p.Allocate(1024);
  EXPECT_EQ(1024, p.total_allocated_bytes());
  memset(ptr, 0, 1024);
  p.ReturnPartialAllocation(1024);

  uint8_t* ptr2 = p.Allocate(1024);
  EXPECT_EQ(1024, p.total_allocated_bytes());
  EXPECT_TRUE(ptr == ptr2);
  p.ReturnPartialAllocation(1016);

  ptr2 = p.Allocate(1016);
  EXPECT_EQ(1024, p.total_allocated_bytes());
  EXPECT_TRUE(ptr2 == ptr + 8);
  p.ReturnPartialAllocation(512);
  memset(ptr2, 1, 1016 - 512);

  uint8_t* ptr3 = p.Allocate(512);
  EXPECT_EQ(1024, p.total_allocated_bytes());
  EXPECT_TRUE(ptr3 == ptr + 512);
  memset(ptr3, 2, 512);

  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(0, ptr[i]);
  }
  for (int i = 8; i < 512; ++i) {
    EXPECT_EQ(1, ptr[i]);
  }
  for (int i = 512; i < 1024; ++i) {
    EXPECT_EQ(2, ptr[i]);
  }

  // Try ReturnPartialAllocations() with 64-bit values.
  uint8_t* ptr4 = p.Allocate(LARGE_ALLOC_SIZE + 512);
  EXPECT_EQ(1024 + LARGE_ALLOC_SIZE + 512, p.total_allocated_bytes());
  memset(ptr4, 3, 512 * 2);
  p.ReturnPartialAllocation(LARGE_ALLOC_SIZE);
  uint8_t* ptr5 = p.Allocate(512);
  EXPECT_TRUE(ptr5 == ptr4 + 512);
  memset(ptr5, 4, 512);

  for (int i = 0; i < 512; ++i) {
    EXPECT_EQ(3, ptr4[i]);
  }
  for (int i = 512; i < 512 * 2; ++i) {
    EXPECT_EQ(4, ptr4[i]);
  }

  p.FreeAll();
}

TEST(MemPoolTest, Limits) {
  MemTracker limit3(4 * MemPoolTest::INITIAL_CHUNK_SIZE);
  MemTracker limit1(2 * MemPoolTest::INITIAL_CHUNK_SIZE, "", &limit3);
  MemTracker limit2(3 * MemPoolTest::INITIAL_CHUNK_SIZE, "", &limit3);

  MemPool* p1 = new MemPool(&limit1);
  EXPECT_FALSE(limit1.AnyLimitExceeded(MemLimit::HARD));

  MemPool* p2 = new MemPool(&limit2);
  EXPECT_FALSE(limit2.AnyLimitExceeded(MemLimit::HARD));

  // p1 exceeds a non-shared limit
  p1->Allocate(MemPoolTest::INITIAL_CHUNK_SIZE);
  EXPECT_FALSE(limit1.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE, limit1.consumption());
  EXPECT_FALSE(limit3.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE, limit3.consumption());

  p1->Allocate(MemPoolTest::INITIAL_CHUNK_SIZE);
  EXPECT_TRUE(limit1.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE * 3, limit1.consumption());
  EXPECT_FALSE(limit3.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE * 3, limit3.consumption());

  // p2 exceeds a shared limit
  p2->Allocate(MemPoolTest::INITIAL_CHUNK_SIZE);
  EXPECT_FALSE(limit2.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE, limit2.consumption());
  EXPECT_FALSE(limit3.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE * 4, limit3.consumption());

  p2->Allocate(1);
  EXPECT_FALSE(limit2.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE * 3, limit2.consumption());
  EXPECT_TRUE(limit3.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE * 6, limit3.consumption());

  // deleting pools reduces consumption
  p1->FreeAll();
  delete p1;
  EXPECT_EQ(0, limit1.consumption());
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE * 3, limit2.consumption());
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE * 3, limit3.consumption());

  // Allocate another chunk
  p2->FreeAll();
  EXPECT_FALSE(limit2.LimitExceeded(MemLimit::HARD));
  uint8_t* result = p2->TryAllocate(MemPoolTest::INITIAL_CHUNK_SIZE);
  ASSERT_TRUE(result != NULL);
  ASSERT_TRUE(MemPoolTest::CheckIntegrity(p2, false));

  // Try To allocate over the limit, this should fail.
  result = p2->TryAllocate(MemPoolTest::INITIAL_CHUNK_SIZE * 4);
  ASSERT_TRUE(result == NULL);
  ASSERT_TRUE(MemPoolTest::CheckIntegrity(p2, false));

  // Try To allocate 20 bytes, this should succeed. TryAllocate() should leave the
  // pool in a functional state.
  result = p2->TryAllocate(20);
  ASSERT_TRUE(result != NULL);
  ASSERT_TRUE(MemPoolTest::CheckIntegrity(p2, false));

  p2->FreeAll();
  delete p2;
}

TEST(MemPoolTest, MaxAllocation) {
  int64_t int_max_rounded = BitUtil::RoundUp(LARGE_ALLOC_SIZE, 8);

  // Allocate a single LARGE_ALLOC_SIZE chunk
  MemTracker tracker;
  MemPool p1(&tracker);
  uint8_t* ptr = p1.Allocate(LARGE_ALLOC_SIZE);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(int_max_rounded, p1.GetTotalChunkSizes());
  EXPECT_EQ(int_max_rounded, p1.total_allocated_bytes());
  p1.FreeAll();

  // Allocate a small chunk (INITIAL_CHUNK_SIZE) followed by an LARGE_ALLOC_SIZE chunk
  MemPool p2(&tracker);
  p2.Allocate(8);
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE, p2.GetTotalChunkSizes());
  EXPECT_EQ(8, p2.total_allocated_bytes());
  ptr = p2.Allocate(LARGE_ALLOC_SIZE);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(MemPoolTest::INITIAL_CHUNK_SIZE + int_max_rounded,
      p2.GetTotalChunkSizes());
  EXPECT_EQ(8LL + int_max_rounded, p2.total_allocated_bytes());
  p2.FreeAll();

  // Allocate three LARGE_ALLOC_SIZE chunks followed by a small chunk
  // followed by another LARGE_ALLOC_SIZE chunk.
  MemPool p3(&tracker);
  p3.Allocate(LARGE_ALLOC_SIZE);
  // Allocates new int_max_rounded chunk
  ptr = p3.Allocate(LARGE_ALLOC_SIZE);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(int_max_rounded * 2, p3.GetTotalChunkSizes());
  EXPECT_EQ(int_max_rounded * 2, p3.total_allocated_bytes());
  // Allocates new int_max_rounded chunk
  ptr = p3.Allocate(LARGE_ALLOC_SIZE);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(int_max_rounded * 3, p3.GetTotalChunkSizes());
  EXPECT_EQ(int_max_rounded * 3, p3.total_allocated_bytes());

  // Allocates new MAX_CHUNK_SIZE chunk
#if !defined (ADDRESS_SANITIZER) || (__clang_major__ >= 3 && __clang_minor__ >= 7)
  ptr = p3.Allocate(8);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(int_max_rounded * 3 + MemPoolTest::MAX_CHUNK_SIZE, p3.GetTotalChunkSizes());
  EXPECT_EQ(int_max_rounded * 3 + 8, p3.total_allocated_bytes());
  // Allocates new int_max_rounded chunk
  ptr = p3.Allocate(LARGE_ALLOC_SIZE);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(int_max_rounded * 4 + MemPoolTest::MAX_CHUNK_SIZE, p3.GetTotalChunkSizes());
  EXPECT_EQ(int_max_rounded * 4 + 8, p3.total_allocated_bytes());
#endif
  p3.FreeAll();

}

// Test for IMPALA-2742: chunk size can grow unbounded when calling AcquireData with
// MemPools with keep_current = true.
TEST(MemPoolTest, GrowthWithTransfer) {
  MemTracker tracker;
  MemPool src(&tracker);
  MemPool dst(&tracker);
  const int64_t total_to_alloc = 100L * 1024L * 1024L * 1024L;
  // Choose size that will straddle chunk boundaries to exercise boundary cases.
  const int alloc_size = 100 * 1024 + 12345;
  // We should have at most one chunk allocated at a time.
  const int max_consumption = MemPoolTest::MAX_CHUNK_SIZE;

  for (int i = 0; i < total_to_alloc / alloc_size; ++i) {
    uint8_t* mem = src.Allocate(alloc_size);
    ASSERT_TRUE(mem != NULL);

    dst.AcquireData(&src, true);
    dst.FreeAll();

    // Check that the chunk size limits memory consumption.
    ASSERT_LE(src.GetTotalChunkSizes(), max_consumption);
    ASSERT_EQ(0, dst.GetTotalChunkSizes());
    ASSERT_LE(tracker.consumption(), max_consumption);
  }

  dst.AcquireData(&src, false);
  dst.FreeAll();
}

// Test that the MemPool overhead is bounded when we make allocations of
// INITIAL_CHUNK_SIZE.
TEST(MemPoolTest, MemoryOverhead) {
  MemTracker tracker;
  MemPool p(&tracker);
  const int alloc_size = MemPoolTest::INITIAL_CHUNK_SIZE;
  const int num_allocs = 1000;
  int64_t total_allocated = 0;

  for (int i = 0; i < num_allocs; ++i) {
    uint8_t* mem = p.Allocate(alloc_size);
    ASSERT_TRUE(mem != NULL);
    total_allocated += alloc_size;

    int64_t wasted_memory = p.GetTotalChunkSizes() - total_allocated;
    // The initial chunk fits evenly into MAX_CHUNK_SIZE, so should have at most
    // one empty chunk at the end.
    EXPECT_LE(wasted_memory, MemPoolTest::MAX_CHUNK_SIZE);
    // The chunk doubling algorithm should not allocate chunks larger than the total
    // amount of memory already allocated.
    EXPECT_LE(wasted_memory, total_allocated);
  }

  p.FreeAll();
}

// Test that the MemPool overhead is bounded when we make alternating large and small
// allocations.
TEST(MemPoolTest, FragmentationOverhead) {
  MemTracker tracker;
  MemPool p(&tracker);
  const int num_allocs = 100;
  int64_t total_allocated = 0;

  for (int i = 0; i < num_allocs; ++i) {
    int alloc_size = i % 2 == 0 ? 1 : MemPoolTest::MAX_CHUNK_SIZE;
    uint8_t* mem = p.Allocate(alloc_size);
    ASSERT_TRUE(mem != NULL);
    total_allocated += alloc_size;

    int64_t wasted_memory = p.GetTotalChunkSizes() - total_allocated;
    // Fragmentation should not waste more than half of each completed chunk.
    EXPECT_LE(wasted_memory, total_allocated + MemPoolTest::MAX_CHUNK_SIZE);
  }

  p.FreeAll();
}

/// Test interaction between returning a full allocation and transferring chunks.
TEST(MemPoolTest, ReturnAllocationThenTransfer) {
  MemTracker tracker;
  MemPool src(&tracker);
  MemPool dst(&tracker);

  const int64_t alloc_size = 1L * 1024 * 1024;
  uint8_t* mem = src.Allocate(alloc_size);
  ASSERT_TRUE(mem != NULL);

  src.ReturnPartialAllocation(alloc_size);
  ASSERT_TRUE(MemPoolTest::CheckIntegrity(&src, true));

  // IMPALA-3946: this would hit a DCHECK because the current chunk was empty.
  dst.AcquireData(&src, true);
  ASSERT_EQ(0, dst.GetTotalChunkSizes()); // Src chunk shouldn't be transferred.
  ASSERT_TRUE(MemPoolTest::CheckIntegrity(&dst, true));
  ASSERT_TRUE(MemPoolTest::CheckIntegrity(&src, true));
  src.FreeAll();
  dst.FreeAll();
}

/// Test that making a large allocation that doesn't fit in the current chunk after
/// returning a full allocation works correctly.
TEST(MemPoolTest, ReturnAllocationThenLargeAllocation) {
  MemTracker tracker;
  MemPool src(&tracker);

  const int64_t alloc_size = 1L * 1024 * 1024;
  uint8_t* mem = src.Allocate(alloc_size);
  ASSERT_TRUE(mem != NULL);

  src.ReturnPartialAllocation(alloc_size);
  ASSERT_TRUE(MemPoolTest::CheckIntegrity(&src, true));

  // IMPALA-3946: chunks ended in invalid order because the first chunk was empty.
  // This resulted in the the integrity check failing.
  mem = src.Allocate(alloc_size + 1);
  ASSERT_TRUE(mem != NULL);
  ASSERT_TRUE(MemPoolTest::CheckIntegrity(&src, false));
  src.FreeAll();
}

/// Test that making a large allocation that fails after returning a full allocation works
/// correctly.
TEST(MemPoolTest, ReturnAllocationThenFailedAllocation) {
  const int64_t alloc_size = 1L * 1024 * 1024;
  const int64_t mem_limit = alloc_size * 10;
  MemTracker tracker(mem_limit);
  MemPool src(&tracker);

  uint8_t* mem;
  // Allocate two chunks then clear them so they are empty.
  for (int i = 0; i < 2; ++i) {
    mem = src.Allocate(alloc_size);
    ASSERT_TRUE(mem != NULL);
  }
  src.Clear();

  // IMPALA-3946: chunk index wasn't reset correctly to before the empty chunks when an
  // allocation failed. This resulted in the the integrity check failing.
  mem = src.TryAllocate(mem_limit);
  ASSERT_TRUE(mem == NULL);
  ASSERT_TRUE(MemPoolTest::CheckIntegrity(&src, false));
  src.FreeAll();
}

TEST(MemPoolTest, TryAllocateAligned) {
  MemTracker tracker(-1);
  MemPool pool(&tracker);
  int alignment = 1;
  int64_t size = 1;
  constexpr int NUM_ALLOCATIONS = 100000;
  constexpr int MAX_ALLOCATION_SIZE = 113;

  for (int i = 0; i < NUM_ALLOCATIONS; ++i) {
    uint8_t* ptr = pool.TryAllocateAligned(size, alignment);
    ASSERT_TRUE(ptr != NULL) << "size=" << size << " alignment=" << alignment;
    ASSERT_EQ(0, reinterpret_cast<uintptr_t>(ptr) % alignment)
        << "size=" << size << " alignment=" << alignment;
    alignment = alignment == alignof(std::max_align_t) ? 1 : alignment * 2;
    size = (size + 1) % MAX_ALLOCATION_SIZE;
  }

  pool.FreeAll();
}
}

