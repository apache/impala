// Copyright 2016 Cloudera Inc.
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

#include "util/bloom-filter.h"

#include <algorithm>
#include <set>
#include <vector>

#include <gtest/gtest.h>

using namespace std;

namespace {
// Make a random uint32_t, avoiding the absent high bit and the low-entropy low bits
// produced by rand().
uint64_t MakeRand() {
  uint32_t result = (rand() >> 8) & 0xffff;
  result <<= 16;
  result |= (rand() >> 8) & 0xffff;
  return result;
}

}  // namespace
namespace impala {

// We can construct (and destruct) Bloom filters with different spaces.
TEST(BloomFilter, Constructor) {
  for (int i = 0; i < 30; ++i) {
    BloomFilter bf(i, nullptr, nullptr);
  }
}

// We can Insert() hashes into a Bloom filter with different spaces.
TEST(BloomFilter, Insert) {
  srand(0);
  for (int i = 13; i < 17; ++i) {
    BloomFilter bf(i, nullptr, nullptr);
    for (int k = 0; k < (1 << 15); ++k) {
      bf.Insert(MakeRand());
    }
  }
}

// After Insert()ing someting into a Bloom filter, it can be found again immediately.
TEST(BloomFilter, Find) {
  srand(0);
  for (int i = 13; i < 17; ++i) {
    BloomFilter bf(i, nullptr, nullptr);
    for (int k = 0; k < (1 << 15); ++k) {
      const uint64_t to_insert = MakeRand();
      bf.Insert(to_insert);
      EXPECT_TRUE(bf.Find(to_insert));
    }
  }
}

// After Insert()ing someting into a Bloom filter, it can be found again much later.
TEST(BloomFilter, CumulativeFind) {
  srand(0);
  for (int i = 5; i < 11; ++i) {
    std::vector<uint32_t> inserted;
    BloomFilter bf(i, nullptr, nullptr);
    for (int k = 0; k < (1 << 10); ++k) {
      const uint32_t to_insert = MakeRand();
      inserted.push_back(to_insert);
      bf.Insert(to_insert);
      for (int n = 0; n < inserted.size(); ++n) {
        EXPECT_TRUE(bf.Find(inserted[n]));
      }
    }
  }
}

// The empirical false positives we find when looking for random items is with a constant
// factor of the false positive probability the Bloom filter was constructed for.
TEST(BloomFilter, FindInvalid) {
  srand(0);
  static const int find_limit = 1 << 20;
  set<uint32_t> to_find;
  while (to_find.size() < find_limit) {
    to_find.insert(MakeRand());
  }
  static const int max_log_ndv = 19;
  set<uint32_t> to_insert;
  while (to_insert.size() < (1ull << max_log_ndv)) {
    to_insert.insert(MakeRand());
  }
  vector<uint32_t> shuffled_insert(to_insert.begin(), to_insert.end());
  random_shuffle(shuffled_insert.begin(), shuffled_insert.end());
  for (int log_ndv = 12; log_ndv < max_log_ndv; ++log_ndv) {
    for (int log_fpp = 4; log_fpp < 15; ++log_fpp) {
      double fpp = 1.0 / (1 << log_fpp);
      const size_t ndv = 1 << log_ndv;
      const int log_heap_space = BloomFilter::MinLogSpace(ndv, fpp);
      BloomFilter bf(log_heap_space, nullptr, nullptr);
      // Fill up a BF with exactly as much ndv as we planned for it:
      for (size_t i = 0; i < ndv; ++i) {
        bf.Insert(shuffled_insert[i]);
      }
      int found = 0;
      // Now we sample from the set of possible hashes, looking for hits.
      for (const auto& i : to_find) {
        found += bf.Find(i);
      }
      EXPECT_LE(found, 3 * find_limit * fpp)
          << "Too many false positives with -log2(fpp) = " << log_fpp;
      // Because the space is rounded up to a power of 2, we might actually get a lower
      // fpp than the one passed to MinLogSpace().
      const double expected_fpp = BloomFilter::FalsePositiveProb(ndv, log_heap_space);
      EXPECT_GE(found, 0.33 * find_limit * expected_fpp)
          << "Too few false positives with -log2(fpp) = " << log_fpp;
    }
  }
}

// Test that MaxNdv() and MinLogSpace() are dual
TEST(BloomFilter, MinSpaceMaxNdv) {
  for (double fpp = 0.25; fpp >= 1.0 / (1ull << 63); fpp /= 2) {
    for (int given_log_space = 8; given_log_space < 30; ++given_log_space) {
      const size_t derived_ndv = BloomFilter::MaxNdv(given_log_space, fpp);
      int derived_log_space = BloomFilter::MinLogSpace(derived_ndv, fpp);

      EXPECT_EQ(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;

      // If we lower the fpp, we need more space; if we raise it we need less.
      derived_log_space = BloomFilter::MinLogSpace(derived_ndv, fpp / 2);
      EXPECT_GE(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;
      derived_log_space = BloomFilter::MinLogSpace(derived_ndv, fpp * 2);
      EXPECT_LE(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;
    }
    for (size_t given_ndv = 1000; given_ndv < 1000 * 1000; given_ndv *= 3) {
      const int derived_log_space = BloomFilter::MinLogSpace(given_ndv, fpp);
      const size_t derived_ndv = BloomFilter::MaxNdv(derived_log_space, fpp);

      // The max ndv is close to, but larger than, then ndv we asked for
      EXPECT_LE(given_ndv, derived_ndv) << "fpp: " << fpp
                                        << " derived_log_space: " << derived_log_space;
      EXPECT_GE(given_ndv * 2, derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;

      // Changing the fpp changes the ndv capacity in the expected direction.
      size_t new_derived_ndv = BloomFilter::MaxNdv(derived_log_space, fpp / 2);
      EXPECT_GE(derived_ndv, new_derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;
      new_derived_ndv = BloomFilter::MaxNdv(derived_log_space, fpp * 2);
      EXPECT_LE(derived_ndv, new_derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;
    }
  }
}

// Check that MinLogSpace() and FalsePositiveProb() are dual
TEST(BloomFilter, MinSpaceForFpp) {
  for (size_t ndv = 10000; ndv < 100 * 1000 * 1000; ndv *= 1.01) {
    for (double fpp = 0.1; fpp > pow(2, -20); fpp *= 0.99) {
      // When contructing a Bloom filter, we can request a particular fpp by calling
      // MinLogSpace().
      const int min_log_space = BloomFilter::MinLogSpace(ndv, fpp);
      // However, at the resulting ndv and space, the expected fpp might be lower than
      // the one that was requested.
      double expected_fpp = BloomFilter::FalsePositiveProb(ndv, min_log_space);
      EXPECT_LE(expected_fpp, fpp);
      // The fpp we get might be much lower than the one we asked for. However, if the
      // space were just one size smaller, the fpp we get would be larger than the one we
      // asked for.
      expected_fpp = BloomFilter::FalsePositiveProb(ndv, min_log_space - 1);
      EXPECT_GE(expected_fpp, fpp);
      // Therefore, the return value of MinLogSpace() is actually the minimum
      // log space at which we can guarantee the requested fpp.
    }
  }
}

}  // namespace impala

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
