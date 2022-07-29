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

#include <cmath>
#include <unordered_set>
#include <vector>

#include "common/names.h"
#include "kudu/util/random.h"
#include "testutil/gtest-util.h"
#include "util/parquet-bloom-filter.h"

namespace impala {

TEST(ParquetBloomFilter, OptimalByteSize) {
  // Check that results are consistent with kudu::BlockBloomFilter::MinLogSpace(ndv, fpp).
  std::vector<size_t> ndvs = {1, 100, 500, 1000, 10000, 100000, 1024*1024};
  std::vector<double> fpps = {0.01, 0.001};

  for (uint64_t ndv : ndvs) {
    for (double fpp : fpps) {
      const int expected_log_bytes = ParquetBloomFilter::MinLogSpace(ndv, fpp);
      const int expected_bytes = std::pow(2, expected_log_bytes);
      const int actual_bytes = ParquetBloomFilter::OptimalByteSize(ndv, fpp);

      if (expected_bytes < ParquetBloomFilter::MIN_BYTES) {
        EXPECT_EQ(actual_bytes, ParquetBloomFilter::MIN_BYTES)
            << "NDV: " << ndv << ", FPP: " << fpp << ".";
      } else if (expected_bytes > ParquetBloomFilter::MAX_BYTES) {
        EXPECT_EQ(actual_bytes, ParquetBloomFilter::MAX_BYTES)
            << "NDV: " << ndv << ", FPP: " << fpp << ".";
      } else {
        EXPECT_EQ(actual_bytes, expected_bytes)
            << "NDV: " << ndv << ", FPP: " << fpp << ".";
      }
    }
  }
}

TEST(ParquetBloomFilter, OptimalByteSizeEdgeCase) {
  const int space = ParquetBloomFilter::OptimalByteSize(1, 0.75);
  EXPECT_GE(space, ParquetBloomFilter::MIN_BYTES)
      << "OptimalByteSize should always be >= ParquetBloomFilter::MIN_BYTES";
}

// A ParquetBloomFilter with storage.
struct BloomWrapper {
  std::unique_ptr<ParquetBloomFilter> bloom;
  std::unique_ptr<vector<uint8_t>> storage;
};

BloomWrapper CreateBloomFilter(int ndv, double fpp) {
  const int storage_size = ParquetBloomFilter::OptimalByteSize(ndv, fpp);
  BloomWrapper res;
  res.bloom = std::make_unique<ParquetBloomFilter>();
  res.storage = std::make_unique<vector<uint8_t>>(storage_size, 0);
  Status status = res.bloom->Init(res.storage->data(), storage_size, true);
  EXPECT_TRUE(status.ok()) << status.GetDetail();
  return res;
}

// Make a random uint64_t, avoiding the absent high bit and the low-entropy low bits
// produced by rand().
uint64_t MakeRand() {
  uint32_t high = (rand() >> 8) & 0xffff;
  high <<= 16;
  high |= (rand() >> 8) & 0xffff;

  uint32_t low = (rand() >> 8) & 0xffff;
  low <<= 16;
  low |= (rand() >> 8) & 0xffff;

  uint64_t result = high;
  result <<= 32;
  result |= low;
  return result;
}

// We can Insert() hashes into a Bloom filter with different spaces.
TEST(ParquetBloomFilter, Insert) {
  srand(0);
  for (int ndv = 100; ndv <= 100000; ndv *= 10) {
    BloomWrapper wrapper = CreateBloomFilter(ndv, 0.01);
    for (int i = 0; i < ndv; ++i) {
      wrapper.bloom->Insert(MakeRand());
    }
  }
}

// After Insert()ing something into a Bloom filter, it can be found again immediately.
TEST(ParquetBloomFilter, Find) {
  srand(0);
  for (int ndv = 100; ndv <= 100000; ndv *= 10) {
    BloomWrapper wrapper = CreateBloomFilter(ndv, 0.01);
    for (int i = 0; i < ndv; ++i) {
      const uint64_t to_insert = MakeRand();
      wrapper.bloom->Insert(to_insert);
      EXPECT_TRUE(wrapper.bloom->Find(to_insert));
    }
  }
}

TEST(ParquetBloomFilter, HashAndFind) {
  srand(0);
  for (int ndv = 100; ndv <= 100000; ndv *= 10) {
    BloomWrapper wrapper = CreateBloomFilter(ndv, 0.01);
    for (int i = 0; i < ndv; ++i) {
      const uint64_t to_insert = MakeRand();
      wrapper.bloom->HashAndInsert(reinterpret_cast<const uint8_t*>(&to_insert),
          sizeof(uint64_t));
      EXPECT_TRUE(wrapper.bloom->HashAndFind(reinterpret_cast<const uint8_t*>(&to_insert),
              sizeof(uint64_t)));
    }
  }
}


// After Insert()ing something into a Bloom filter, it can be found again much later.
TEST(ParquetBloomFilter, CumulativeFind) {
  srand(0);
  for (int ndv = 100; ndv <= 1000; ndv *= 10) {
    std::vector<uint64_t> inserted;
    BloomWrapper wrapper = CreateBloomFilter(ndv, 0.01);
    for (int i = 0; i < ndv; ++i) {
      const uint64_t to_insert = MakeRand();
      inserted.push_back(to_insert);
      wrapper.bloom->Insert(to_insert);
      for (int n = 0; n < inserted.size(); ++n) {
        EXPECT_TRUE(wrapper.bloom->Find(inserted[n]));
      }
    }
  }
}

// The empirical false positives we find when looking for random items is with a constant
// factor of the false positive probability the Bloom filter was constructed for.
TEST(ParquetBloomFilter, FindInvalid) {
  // We use a deterministic pseudorandom number generator with a set seed. The reason is
  // that with a run-dependent seed, there will always be inputs that can fail. That's a
  // potential argument for this to be a benchmark rather than a test, although the
  // measured quantity would be not time but deviation from predicted fpp.
  kudu::Random rgen(867 + 5309);
  static const int find_limit = 1 << 22;
  std::unordered_set<uint64_t> to_find;
  while (to_find.size() < find_limit) {
    to_find.insert(rgen.Next64());
  }
  static const int max_log_ndv = 19;
  std::unordered_set<uint64_t> to_insert;
  while (to_insert.size() < (1ull << max_log_ndv)) {
    const uint64_t candidate = rgen.Next64();
    if (to_find.find(candidate) == to_find.end()) {
      to_insert.insert(candidate);
    }
  }
  // NOTE: Even though this was generated with a deterministic pseudorandom number
  // generator, the order of items in the vector is dependent on the implementation
  // of unordered_set, which can change with different libstdc++ versions. Since
  // the tests below use the first X entries, changes in the order also change the
  // test.
  vector<uint64_t> shuffled_insert(to_insert.begin(), to_insert.end());
  for (int log_ndv = 12; log_ndv < max_log_ndv; ++log_ndv) {
    for (int log_fpp = 4; log_fpp < 12; ++log_fpp) {
      double fpp = 1.0 / (1 << log_fpp);
      const size_t ndv = 1 << log_ndv;
      BloomWrapper wrapper = CreateBloomFilter(ndv, fpp);
      ParquetBloomFilter* bloom = wrapper.bloom.get();

      // Fill up a BF with exactly as much ndv as we planned for it:
      for (size_t i = 0; i < ndv; ++i) {
        bloom->Insert(shuffled_insert[i]);
      }
      int found = 0;
      // Now we sample from the set of possible hashes, looking for hits.
      for (const uint64_t hash_to_find : to_find) {
        bool elem_found = bloom->Find(hash_to_find);
        found += elem_found;
      }
      EXPECT_LE(found, find_limit * fpp * 2)
          << "Too many false positives with -log2(fpp) = " << log_fpp
          << " and ndv = " << ndv;

      // Because the space is rounded up to a power of 2, we might actually get a lower
      // fpp than the one passed to MinLogSpace().
      const int log_space = log2(wrapper.storage->size());
      const double expected_fpp =
          ParquetBloomFilter::FalsePositiveProb(ndv, log_space);
      // Fudge factors are present because filter characteristics are true in the limit,
      // and will deviate for small samples.
      EXPECT_GE(found, find_limit * expected_fpp * 0.75)
          << "Too few false positives with -log2(fpp) = " << log_fpp
          << " expected_fpp = " << expected_fpp;
      EXPECT_LE(found, find_limit * expected_fpp * 1.25)
          << "Too many false positives with -log2(fpp) = " << log_fpp
          << " expected_fpp = " << expected_fpp;
    }
  }
}

} // namespace impala
