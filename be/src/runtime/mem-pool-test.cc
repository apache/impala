// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>
#include <gtest/gtest.h>

#include "runtime/mem-pool.h"


using namespace std;

namespace impala {

TEST(MemPoolTest, Basic) {
  MemPool p;
  // allocate a total of 24K in 32-byte pieces (for which we only request 25 bytes)
  for (int i = 0; i < 768; ++i) {
    // pads to 32 bytes
    p.Allocate(25);
  }
  // we handed back 24K
  EXPECT_EQ(p.total_allocated_bytes(), 24 * 1024);
  // .. and allocated 28K of chunks (4, 8, 16)
  EXPECT_EQ(p.GetTotalChunkSizes(), 28 * 1024);

  MemPool p2;
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
  EXPECT_EQ(p.GetTotalChunkSizes(), (16 + 32 + 65) * 1024);
  EXPECT_EQ(p.total_allocated_bytes(), (12 + 8 + 65) * 1024);

  // Clear() resets allocated data, but doesn't remove any chunks
  p.Clear();
  EXPECT_EQ(p.total_allocated_bytes(), 0);
  EXPECT_EQ(p.GetTotalChunkSizes(), (16 + 32 + 65) * 1024);

  // next allocation reuses existing chunks
  p.Allocate(1024);
  EXPECT_EQ(p.total_allocated_bytes(), 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), (16 + 32 + 65) * 1024);

  // ... unless it doesn't fit into the next available chunk
  p.Allocate(33 * 1024);
  EXPECT_EQ(p.total_allocated_bytes(), 34 * 1024);
  EXPECT_EQ(p.GetTotalChunkSizes(), (33 + 16 + 32 + 65) * 1024);

  // we're releasing the first 2 chunks, which get added to p2
  p2.AcquireData(&p, false);
  EXPECT_EQ(p.total_allocated_bytes(), 0);
  EXPECT_EQ(p.GetTotalChunkSizes(), (32 + 65) * 1024);

  MemPool p3;
  p3.AcquireData(&p2, true);  // we're keeping the last 33K chunk
  EXPECT_EQ(p2.total_allocated_bytes(), 33 * 1024);
  EXPECT_EQ(p2.GetTotalChunkSizes(), 33 * 1024);
}

TEST(MemPoolTest, Offsets) {
  MemPool p;
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

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

