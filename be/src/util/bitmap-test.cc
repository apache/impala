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

#include <stdlib.h>
#include <limits.h>

#include "testutil/gtest-util.h"
#include "util/bitmap.h"

#include "common/names.h"

namespace impala {

void CreateRandomBitmap(Bitmap* bitmap) {
  for (int64_t i = 0; i < bitmap->num_bits(); ++i) {
    bitmap->Set(i, rand() % 2 == 0);
  }
}

// Returns true if all the bits in the bitmap are equal to 'value'.
bool CheckAll(const Bitmap& bitmap, const bool value) {
  for (int64_t i = 0; i < bitmap.num_bits(); ++i) {
    if (bitmap.Get(i) != value) return false;
  }
  return true;
}

TEST(Bitmap, SetupTest) {
  Bitmap bm1(0);
  EXPECT_EQ(bm1.num_bits(), 0);
  Bitmap bm2(1);
  EXPECT_EQ(bm2.num_bits(), 1);
  Bitmap bm3(63);
  EXPECT_EQ(bm3.num_bits(), 63);
  Bitmap bm4(64);
  EXPECT_EQ(bm4.num_bits(), 64);
}

TEST(Bitmap, SetAllTest) {
  Bitmap bm(1024);
  EXPECT_EQ(bm.num_bits(), 1024);

  CreateRandomBitmap(&bm);
  bm.SetAllBits(false);
  EXPECT_TRUE(CheckAll(bm, false));
  bm.SetAllBits(true);
  EXPECT_TRUE(CheckAll(bm, true));

  CreateRandomBitmap(&bm);
  bm.SetAllBits(true);
  EXPECT_TRUE(CheckAll(bm, true));
  bm.SetAllBits(false);
  EXPECT_TRUE(CheckAll(bm, false));
}

// Regression test for IMPALA-2155.
TEST(Bitmap, SetGetTest) {
  Bitmap bm(1024);
  bm.SetAllBits(false);
  // Go over different words (1, 2, 4, 8) and set the same index alternatively
  // to 0 and 1.
  for (int64_t bit_idx = 0; bit_idx < 63; ++bit_idx) {
    for (int64_t i = 0; i < 4; ++i) {
      bm.Set((1 << (6 + i)) + bit_idx, (i + bit_idx) % 2 == 0);
    }
  }
  for (int64_t bit_idx = 0; bit_idx < 63; ++bit_idx) {
    for (int64_t i = 0; i < 4; ++i) {
      EXPECT_EQ(bm.Get((1 << (6 + i)) + bit_idx), (i + bit_idx) % 2 == 0);
    }
  }
}

/// Regression test for IMPALA-2307.
TEST(Bitmap, OverflowTest) {
  Bitmap bm(64);
  bm.SetAllBits(false);
  int64_t bit_idx = 45;
  int64_t ovr_idx = 13;

  bm.Set(bit_idx, true);
  EXPECT_FALSE(bm.Get(ovr_idx));
  EXPECT_TRUE(bm.Get(bit_idx));

  bm.SetAllBits(false);
  bm.Set(ovr_idx, true);
  EXPECT_FALSE(bm.Get(bit_idx));
  EXPECT_TRUE(bm.Get(ovr_idx));

  bm.SetAllBits(false);
  bm.Set(ovr_idx, true);
  bm.Set(bit_idx, false);
  EXPECT_TRUE(bm.Get(ovr_idx));
  EXPECT_FALSE(bm.Get(bit_idx));
}

/// Test that bitmap memory usage calculation is correct.
TEST(Bitmap, MemUsage) {
  // 70 bits requires two int64s, for 16 bytes.
  EXPECT_EQ(16, Bitmap::MemUsage(70));
  Bitmap bm(70);
  EXPECT_EQ(16, bm.MemUsage());
}

}
