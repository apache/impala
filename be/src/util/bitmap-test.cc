// Copyright 2015 Cloudera Inc.
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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <limits.h>

#include <boost/utility.hpp>
#include <gtest/gtest.h>
#include "util/bitmap.h"
#include "util/cpu-info.h"

#include "common/names.h"

namespace impala {

void CreateRandomBitmap(Bitmap* bitmap) {
  for (int64_t i = 0; i < bitmap->size(); ++i) {
    bitmap->Set<false>(i, rand() % 2 == 0);
  }
}

// Returns true if all the bits in the bitmap are equal to 'value'.
bool CheckAll(const Bitmap& bitmap, const bool value) {
  for (int64_t i = 0; i < bitmap.size(); ++i) {
    if (bitmap.Get<false>(i) != value) return false;
  }
  return true;
}

TEST(Bitmap, SetupTest) {
  Bitmap bm1(0);
  EXPECT_EQ(bm1.size(), 0);
  Bitmap bm2(1);
  EXPECT_EQ(bm2.size(), 1);
  Bitmap bm3(63);
  EXPECT_EQ(bm3.size(), 63);
  Bitmap bm4(64);
  EXPECT_EQ(bm4.size(), 64);
}

TEST(Bitmap, SetAllTest) {
  Bitmap bm(1024);
  EXPECT_EQ(bm.size(), 1024);

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

TEST(Bitmap, AndTest) {
  Bitmap bm(1024);
  CreateRandomBitmap(&bm);
  Bitmap bm_zeros(1024);
  bm_zeros.SetAllBits(false);
  bm.And(&bm_zeros);
  EXPECT_TRUE(CheckAll(bm, false));
}

TEST(Bitmap, OrTest) {
  Bitmap bm(1024);
  CreateRandomBitmap(&bm);
  Bitmap bm_ones(1024);
  bm_ones.SetAllBits(true);
  bm.Or(&bm_ones);
  EXPECT_TRUE(CheckAll(bm, true));
}

// Regression test for IMPALA-2155.
TEST(Bitmap, SetGetTest) {
  Bitmap bm(1024);
  bm.SetAllBits(false);
  // Go over different words (1, 2, 4, 8) and set the same index alternatively
  // to 0 and 1.
  for (int64_t bit_idx = 0; bit_idx < 63; ++bit_idx) {
    for (int64_t i = 0; i < 4; ++i) {
      bm.Set<false>((1 << (6 + i)) + bit_idx, (i + bit_idx) % 2 == 0);
    }
  }
  for (int64_t bit_idx = 0; bit_idx < 63; ++bit_idx) {
    for (int64_t i = 0; i < 4; ++i) {
      EXPECT_EQ(bm.Get<false>((1 << (6 + i)) + bit_idx), (i + bit_idx) % 2 == 0);
    }
  }
}

TEST(Bitmap, SetGetModTest) {
  Bitmap bm(256);
  bm.SetAllBits(false);
  for (int64_t bit_idx = 0; bit_idx < 1024; ++bit_idx) {
    bm.Set<true>(bit_idx, true);
    EXPECT_TRUE(bm.Get<true>(bit_idx));
    bm.Set<true>(bit_idx, false);
    EXPECT_FALSE(bm.Get<true>(bit_idx));
  }

  bm.SetAllBits(false);
  EXPECT_TRUE(CheckAll(bm, false));
  for (int64_t bit_idx = 0; bit_idx < 1024; ++bit_idx) {
    bm.Set<true>(bit_idx, bit_idx % 2 == 0);
  }
  for (int64_t bit_idx = 0; bit_idx < 1024; ++bit_idx) {
    EXPECT_EQ(bm.Get<true>(bit_idx), bit_idx % 2 == 0);
  }
}

/// Regression test for IMPALA-2307.
TEST(Bitmap, OverflowTest) {
  Bitmap bm(64);
  bm.SetAllBits(false);
  int64_t bit_idx = 45;
  int64_t ovr_idx = 13;

  bm.Set<false>(bit_idx, true);
  EXPECT_FALSE(bm.Get<false>(ovr_idx));
  EXPECT_TRUE(bm.Get<false>(bit_idx));

  bm.SetAllBits(false);
  bm.Set<false>(ovr_idx, true);
  EXPECT_FALSE(bm.Get<false>(bit_idx));
  EXPECT_TRUE(bm.Get<false>(ovr_idx));

  bm.SetAllBits(false);
  bm.Set<false>(ovr_idx, true);
  bm.Set<false>(bit_idx, false);
  EXPECT_TRUE(bm.Get<false>(ovr_idx));
  EXPECT_FALSE(bm.Get<false>(bit_idx));

  bm.SetAllBits(false);
  bm.Set<true>(bit_idx, true);
  EXPECT_FALSE(bm.Get<true>(ovr_idx));
  EXPECT_TRUE(bm.Get<true>(bit_idx));

  bm.SetAllBits(false);
  bm.Set<true>(ovr_idx, true);
  EXPECT_FALSE(bm.Get<true>(bit_idx));
  EXPECT_TRUE(bm.Get<true>(ovr_idx));

  bm.SetAllBits(false);
  bm.Set<true>(ovr_idx, true);
  bm.Set<true>(bit_idx, false);
  EXPECT_TRUE(bm.Get<true>(ovr_idx));
  EXPECT_FALSE(bm.Get<true>(bit_idx));
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  return RUN_ALL_TESTS();
}
