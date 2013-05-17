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
#include "runtime/mem-limit.h"

using namespace std;

namespace impala {

TEST(MemPoolTest, Basic) {
  MemPool p(NULL);
  // allocate a total of 24K in 32-byte pieces (for which we only request 25 bytes)
  for (int i = 0; i < 768; ++i) {
    // pads to 32 bytes
    p.Allocate(25);
  }
  // we handed back 24K
  EXPECT_EQ(p.total_allocated_bytes(), 24 * 1024);
  // .. and allocated 28K of chunks (4, 8, 16)
  EXPECT_EQ(p.GetTotalChunkSizes(), 28 * 1024);

  MemPool p2(NULL);
  // we're passing on the first two chunks, containing 12K of data; we're left with one
  // chunk of 16K containing 12K of data
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
  EXPECT_EQ(p.peak_allocated_bytes(), (12 + 8 + 65) * 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), (16 + 32 + 65) * 1024);

  // Clear() resets allocated data, but doesn't remove any chunks
  p.Clear();
  EXPECT_EQ(p.total_allocated_bytes(), 0);
  EXPECT_EQ(p.peak_allocated_bytes(), (12 + 8 + 65) * 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), (16 + 32 + 65) * 1024);

  // next allocation reuses existing chunks
  p.Allocate(1024);
  EXPECT_EQ(p.total_allocated_bytes(), 1024);
  EXPECT_EQ(p.peak_allocated_bytes(), (12 + 8 + 65) * 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), (16 + 32 + 65) * 1024);

  // ... unless it doesn't fit into any available chunk
  p.Allocate(120 * 1024);
  EXPECT_EQ(p.total_allocated_bytes(), (1 + 120) * 1024);
  EXPECT_EQ(p.peak_allocated_bytes(), (1 + 120) * 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), (130 + 16 + 32 + 65) * 1024);

  // ... Try another chunk that fits into an existing chunk
  p.Allocate(33 * 1024);
  EXPECT_EQ(p.total_allocated_bytes(), (1 + 120 + 33) * 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), (130 + 16 + 32 + 65) * 1024);

  // we're releasing 3 chunks, which get added to p2
  p2.AcquireData(&p, false);
  EXPECT_EQ(p.total_allocated_bytes(), 0);
  EXPECT_EQ(p.peak_allocated_bytes(), (1 + 120 + 33) * 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), 32 * 1024);

  MemPool p3(NULL);
  p3.AcquireData(&p2, true);  // we're keeping the 65k chunk
  EXPECT_EQ(p2.total_allocated_bytes(), 33 * 1024);
  EXPECT_EQ(p2.GetTotalChunkSizes(), 65 * 1024);
}

TEST(MemPoolTest, Offsets) {
  MemPool p(NULL);
  uint8_t* data[1024];
  int offset = 0;
  // test GetCurrentOffset()
  for (int i = 0; i < 1024; ++i) {
    EXPECT_EQ(offset, p.GetCurrentOffset());
    data[i] = p.Allocate(8);
    offset += 8;
  }

  // test GetOffset()
  offset = 0;
  for (int i = 0; i < 1024; ++i) {
    EXPECT_EQ(offset, p.GetOffset(data[i]));
    offset += 8;
  }

  // test GetDataPtr
  offset = 0;
  for (int i = 0; i < 1024; ++i) {
    EXPECT_EQ(data[i], p.GetDataPtr(offset));
    offset += 8;
  }
}

// Test that we can keep an allocated chunk and a free chunk.
// This case verifies that when chunks are acquired by another memory pool the
// remaining chunks are consistent if there were more than one used chunk and some
// free chunks.
TEST(MemPoolTest, Keep) {
  MemPool p(NULL);
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
  MemPool p2(NULL);
  p2.AcquireData(&p, true);
  EXPECT_EQ(p.total_allocated_bytes(), 4 * 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), (8 + 16) * 1024);
  EXPECT_EQ(p2.total_allocated_bytes(), 1 * 1024);
  EXPECT_EQ(p2.GetTotalChunkSizes(), 4 * 1024);
}

TEST(MemPoolTest, Limits) {
  MemLimit limit1(160);
  MemLimit limit2(240);
  MemLimit limit3(320);

  vector<MemLimit*> limits;
  limits.push_back(&limit1);
  limits.push_back(&limit3);
  MemPool* p1 = new MemPool(&limits, 80);
  EXPECT_FALSE(p1->exceeded_limit());

  limits.clear();
  limits.push_back(&limit2);
  limits.push_back(&limit3);
  MemPool* p2 = new MemPool(&limits, 80);
  EXPECT_FALSE(p2->exceeded_limit());

  // p1 exceeds a non-shared limit
  p1->Allocate(80);
  EXPECT_FALSE(p1->exceeded_limit());
  EXPECT_FALSE(limit1.LimitExceeded());
  EXPECT_EQ(limit1.consumption(), 80);
  EXPECT_FALSE(limit3.LimitExceeded());
  EXPECT_EQ(limit3.consumption(), 80);

  p1->Allocate(88);
  EXPECT_TRUE(p1->exceeded_limit());
  EXPECT_TRUE(limit1.LimitExceeded());
  EXPECT_EQ(limit1.consumption(), 168);
  EXPECT_FALSE(limit3.LimitExceeded());
  EXPECT_EQ(limit3.consumption(), 168);

  // p2 exceeds a shared limit
  p2->Allocate(80);
  EXPECT_FALSE(p2->exceeded_limit());
  EXPECT_FALSE(limit2.LimitExceeded());
  EXPECT_EQ(limit2.consumption(), 80);
  EXPECT_FALSE(limit3.LimitExceeded());
  EXPECT_EQ(limit3.consumption(), 248);

  p2->Allocate(80);
  EXPECT_TRUE(p2->exceeded_limit());
  EXPECT_FALSE(limit2.LimitExceeded());
  EXPECT_EQ(limit2.consumption(), 160);
  EXPECT_TRUE(limit3.LimitExceeded());
  EXPECT_EQ(limit3.consumption(), 328);

  // deleting pools reduces consumption
  delete p1;
  EXPECT_EQ(limit1.consumption(), 0);
  EXPECT_EQ(limit2.consumption(), 160);
  EXPECT_EQ(limit3.consumption(), 160);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

