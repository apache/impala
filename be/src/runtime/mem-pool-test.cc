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

#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"

#include "common/names.h"

namespace impala {

TEST(MemPoolTest, Basic) {
  MemTracker tracker;
  MemPool p(&tracker);
  MemPool p2(&tracker);
  MemPool p3(&tracker);

  for (int iter = 0; iter < 2; ++iter) {
    // allocate a total of 24K in 32-byte pieces (for which we only request 25 bytes)
    for (int i = 0; i < 768; ++i) {
      // pads to 32 bytes
      p.Allocate(25);
    }
    // we handed back 24K
    EXPECT_EQ(p.total_allocated_bytes(), 24 * 1024);
    // .. and allocated 28K of chunks (4, 8, 16)
    EXPECT_EQ(p.GetTotalChunkSizes(), 28 * 1024);

    // we're passing on the first two chunks, containing 12K of data; we're left with
    // one chunk of 16K containing 12K of data
    p2.AcquireData(&p, true);
    EXPECT_EQ(p.total_allocated_bytes(), 12 * 1024);
    EXPECT_EQ(p.GetTotalChunkSizes(), 16 * 1024);

    // we allocate 8K, for which there isn't enough room in the current chunk,
    // so another one is allocated (32K)
    p.Allocate(8 * 1024);
    EXPECT_EQ(p.GetTotalChunkSizes(), (16 + 32) * 1024);

    // we allocate 65K, which doesn't fit into the current chunk or the default
    // size of the next allocated chunk (64K)
    p.Allocate(65 * 1024);
    EXPECT_EQ(p.total_allocated_bytes(), (12 + 8 + 65) * 1024);
    if (iter == 0) {
      EXPECT_EQ(p.peak_allocated_bytes(), (12 + 8 + 65) * 1024);
    } else {
      EXPECT_EQ(p.peak_allocated_bytes(), (1 + 120 + 33) * 1024);
    }
    EXPECT_EQ(p.GetTotalChunkSizes(), (16 + 32 + 65) * 1024);

    // Clear() resets allocated data, but doesn't remove any chunks
    p.Clear();
    EXPECT_EQ(p.total_allocated_bytes(), 0);
    if (iter == 0) {
      EXPECT_EQ(p.peak_allocated_bytes(), (12 + 8 + 65) * 1024);
    } else {
      EXPECT_EQ(p.peak_allocated_bytes(), (1 + 120 + 33) * 1024);
    }
    EXPECT_EQ(p.GetTotalChunkSizes(), (16 + 32 + 65) * 1024);

    // next allocation reuses existing chunks
    p.Allocate(1024);
    EXPECT_EQ(p.total_allocated_bytes(), 1024);
    if (iter == 0) {
      EXPECT_EQ(p.peak_allocated_bytes(), (12 + 8 + 65) * 1024);
    } else {
      EXPECT_EQ(p.peak_allocated_bytes(), (1 + 120 + 33) * 1024);
    }
    EXPECT_EQ(p.GetTotalChunkSizes(), (16 + 32 + 65) * 1024);

    // ... unless it doesn't fit into any available chunk
    p.Allocate(120 * 1024);
    EXPECT_EQ(p.total_allocated_bytes(), (1 + 120) * 1024);
    if (iter == 0) {
      EXPECT_EQ(p.peak_allocated_bytes(), (1 + 120) * 1024);
    } else {
      EXPECT_EQ(p.peak_allocated_bytes(), (1 + 120 + 33) * 1024);
    }
    EXPECT_EQ(p.GetTotalChunkSizes(), (130 + 16 + 32 + 65) * 1024);

    // ... Try another chunk that fits into an existing chunk
    p.Allocate(33 * 1024);
    EXPECT_EQ(p.total_allocated_bytes(), (1 + 120 + 33) * 1024);
    EXPECT_EQ(p.GetTotalChunkSizes(), (130 + 16 + 32 + 65) * 1024);

    // we're releasing 3 chunks, which get added to p2
    p2.AcquireData(&p, false);
    EXPECT_EQ(p.total_allocated_bytes(), 0);
    EXPECT_EQ(p.peak_allocated_bytes(), (1 + 120 + 33) * 1024);
    EXPECT_EQ(p.GetTotalChunkSizes(), 0);

    p3.AcquireData(&p2, true);  // we're keeping the 65k chunk
    EXPECT_EQ(p2.total_allocated_bytes(), 33 * 1024);
    EXPECT_EQ(p2.GetTotalChunkSizes(), 65 * 1024);

    p.FreeAll();
    p2.FreeAll();
    p3.FreeAll();
  }
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
  EXPECT_EQ(p.total_allocated_bytes(), (4 + 8 + 16) * 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), (4 + 8 + 16) * 1024);
  p.Clear();
  EXPECT_EQ(p.total_allocated_bytes(), 0);
  EXPECT_EQ(p.GetTotalChunkSizes(), (4 + 8 + 16) * 1024);
  p.Allocate(1*1024);
  p.Allocate(4*1024);
  EXPECT_EQ(p.total_allocated_bytes(), (1 + 4) * 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), (4 + 8 + 16) * 1024);
  MemPool p2(&tracker);
  p2.AcquireData(&p, true);
  EXPECT_EQ(p.total_allocated_bytes(), 4 * 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), (8 + 16) * 1024);
  EXPECT_EQ(p2.total_allocated_bytes(), 1 * 1024);
  EXPECT_EQ(p2.GetTotalChunkSizes(), 4 * 1024);

  p.FreeAll();
  p2.FreeAll();
}

// Tests that we can return partial allocations.
TEST(MemPoolTest, ReturnPartial) {
  MemTracker tracker;
  MemPool p(&tracker);
  uint8_t* ptr = p.Allocate(1024);
  EXPECT_EQ(p.total_allocated_bytes(), 1024);
  memset(ptr, 0, 1024);
  p.ReturnPartialAllocation(1024);

  uint8_t* ptr2 = p.Allocate(1024);
  EXPECT_EQ(p.total_allocated_bytes(), 1024);
  EXPECT_TRUE(ptr == ptr2);
  p.ReturnPartialAllocation(1016);

  ptr2 = p.Allocate(1016);
  EXPECT_EQ(p.total_allocated_bytes(), 1024);
  EXPECT_TRUE(ptr2 == ptr + 8);
  p.ReturnPartialAllocation(512);
  memset(ptr2, 1, 1016 - 512);

  uint8_t* ptr3 = p.Allocate(512);
  EXPECT_EQ(p.total_allocated_bytes(), 1024);
  EXPECT_TRUE(ptr3 == ptr + 512);
  memset(ptr3, 2, 512);

  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(ptr[i], 0);
  }
  for (int i = 8; i < 512; ++i) {
    EXPECT_EQ(ptr[i], 1);
  }
  for (int i = 512; i < 1024; ++i) {
    EXPECT_EQ(ptr[i], 2);
  }

  p.FreeAll();
}

// Utility class to call private functions on MemPool.
class MemPoolTest {
 public:
  static bool CheckIntegrity(MemPool* pool, bool current_chunk_empty) {
    return pool->CheckIntegrity(current_chunk_empty);
  }
};

TEST(MemPoolTest, Limits) {
  MemTracker limit3(320);
  MemTracker limit1(160, -1, "", &limit3);
  MemTracker limit2(240, -1, "", &limit3);

  MemPool* p1 = new MemPool(&limit1, 80);
  EXPECT_FALSE(limit1.AnyLimitExceeded());

  MemPool* p2 = new MemPool(&limit2, 80);
  EXPECT_FALSE(limit2.AnyLimitExceeded());

  // p1 exceeds a non-shared limit
  p1->Allocate(80);
  EXPECT_FALSE(limit1.LimitExceeded());
  EXPECT_EQ(limit1.consumption(), 80);
  EXPECT_FALSE(limit3.LimitExceeded());
  EXPECT_EQ(limit3.consumption(), 80);

  p1->Allocate(88);
  EXPECT_TRUE(limit1.LimitExceeded());
  EXPECT_EQ(limit1.consumption(), 168);
  EXPECT_FALSE(limit3.LimitExceeded());
  EXPECT_EQ(limit3.consumption(), 168);

  // p2 exceeds a shared limit
  p2->Allocate(80);
  EXPECT_FALSE(limit2.LimitExceeded());
  EXPECT_EQ(limit2.consumption(), 80);
  EXPECT_FALSE(limit3.LimitExceeded());
  EXPECT_EQ(limit3.consumption(), 248);

  p2->Allocate(80);
  EXPECT_FALSE(limit2.LimitExceeded());
  EXPECT_EQ(limit2.consumption(), 160);
  EXPECT_TRUE(limit3.LimitExceeded());
  EXPECT_EQ(limit3.consumption(), 328);

  // deleting pools reduces consumption
  p1->FreeAll();
  delete p1;
  EXPECT_EQ(limit1.consumption(), 0);
  EXPECT_EQ(limit2.consumption(), 160);
  EXPECT_EQ(limit3.consumption(), 160);

  // Allocate 160 bytes from 240 byte limit.
  p2->FreeAll();
  EXPECT_FALSE(limit2.LimitExceeded());
  uint8_t* result = p2->TryAllocate(160);
  DCHECK(result != NULL);
  DCHECK(MemPoolTest::CheckIntegrity(p2, false));

  // Try To allocate another 160 bytes, this should fail.
  result = p2->TryAllocate(160);
  DCHECK(result == NULL);
  DCHECK(MemPoolTest::CheckIntegrity(p2, false));

  // Try To allocate 20 bytes, this should succeed. TryAllocate() should leave the
  // pool in a functional state..
  result = p2->TryAllocate(20);
  DCHECK(result != NULL);
  DCHECK(MemPoolTest::CheckIntegrity(p2, false));


  p2->FreeAll();
  delete p2;
}

TEST(MemPoolTest, MaxAllocation) {
  int64_t int_max_rounded = BitUtil::RoundUp(INT_MAX, 8);

  // Allocate a single INT_MAX chunk
  MemTracker tracker;
  MemPool p1(&tracker);
  uint8_t* ptr = p1.Allocate(INT_MAX);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(p1.GetTotalChunkSizes(), int_max_rounded);
  EXPECT_EQ(p1.total_allocated_bytes(), int_max_rounded);
  p1.FreeAll();

  // Allocate a small chunk (DEFAULT_INITIAL_CHUNK_SIZE) followed by an INT_MAX chunk
  MemPool p2(&tracker);
  p2.Allocate(8);
  EXPECT_EQ(p2.GetTotalChunkSizes(), 4096);
  EXPECT_EQ(p2.total_allocated_bytes(), 8);
  ptr = p2.Allocate(INT_MAX);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(p2.GetTotalChunkSizes(), 4096LL + int_max_rounded);
  EXPECT_EQ(p2.total_allocated_bytes(), 8LL + int_max_rounded);
  p2.FreeAll();

  // Allocate three INT_MAX chunks followed by a small chunk followed by another INT_MAX
  // chunk
  MemPool p3(&tracker);
  p3.Allocate(INT_MAX);
  // Allocates new int_max_rounded * 2 chunk
  ptr = p3.Allocate(INT_MAX);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(p3.GetTotalChunkSizes(), int_max_rounded * 3);
  EXPECT_EQ(p3.total_allocated_bytes(), int_max_rounded * 2);
  // Uses existing int_max_rounded * 2 chunk
  ptr = p3.Allocate(INT_MAX);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(p3.GetTotalChunkSizes(), int_max_rounded * 3);
  EXPECT_EQ(p3.total_allocated_bytes(), int_max_rounded * 3);

  // ASAN can't allocate a 4GB chunk for some reason (IMPALA-2461), disable this part of
  // the test.
#ifndef ADDRESS_SANITIZER

  // Allocates a new int_max_rounded * 4 chunk
  ptr = p3.Allocate(8);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(p3.GetTotalChunkSizes(), int_max_rounded * 7);
  EXPECT_EQ(p3.total_allocated_bytes(), int_max_rounded * 3 + 8);
  // Uses existing int_max_rounded * 4 chunk
  ptr = p3.Allocate(INT_MAX);
  EXPECT_TRUE(ptr != NULL);
  EXPECT_EQ(p3.GetTotalChunkSizes(), int_max_rounded * 7);
  EXPECT_EQ(p3.total_allocated_bytes(), int_max_rounded * 4 + 8);

#endif

  p3.FreeAll();
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
