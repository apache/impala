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

#include "util/bloom-filter.h"

#include <algorithm>
#include <unordered_set>
#include <vector>

#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/mem-tracker.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"

using namespace std;

namespace {

using namespace impala;

// Make a random uint64_t, avoiding the absent high bit and the low-entropy low bits
// produced by rand().
uint64_t MakeRand() {
  uint32_t result = (rand() >> 8) & 0xffff;
  result <<= 16;
  result |= (rand() >> 8) & 0xffff;
  return result;
}

// BfInsert() and BfFind() are like BloomFilter::{Insert,Find}, except they randomly
// disable AVX2 instructions half of the time. These are used for testing that AVX2
// machines and non-AVX2 machines produce compatible BloomFilters.

void BfInsert(BloomFilter& bf, uint32_t h) {
  if (MakeRand() & 0x1) {
    bf.Insert(h);
  } else {
    CpuInfo::TempDisable t1(CpuInfo::AVX2);
    bf.Insert(h);
  }
}

bool BfFind(BloomFilter& bf, uint32_t h) {
  if (MakeRand() & 0x1) {
    return bf.Find(h);
  } else {
    CpuInfo::TempDisable t1(CpuInfo::AVX2);
    return bf.Find(h);
  }
}

// Computes union of 'x' and 'y'. Computes twice with AVX enabled and disabled and
// verifies both produce the same result. 'success' is set to true if both union
// computations returned the same result and set to false otherwise.
TBloomFilter BfUnion(const BloomFilter& x, const BloomFilter& y, bool* success) {
  TBloomFilter thrift_x, thrift_y;
  BloomFilter::ToThrift(&x, &thrift_x);
  BloomFilter::ToThrift(&y, &thrift_y);
  BloomFilter::Or(thrift_x, &thrift_y);
  {
    CpuInfo::TempDisable t1(CpuInfo::AVX);
    CpuInfo::TempDisable t2(CpuInfo::AVX2);
    TBloomFilter thrift_x2, thrift_y2;
    BloomFilter::ToThrift(&x, &thrift_x2);
    BloomFilter::ToThrift(&y, &thrift_y2);
    BloomFilter::Or(thrift_x2, &thrift_y2);
    *success = thrift_y.directory == thrift_y2.directory;
  }
  return thrift_y;
}

}  // namespace

namespace impala {

// Test that MaxNdv() and MinLogSpace() are dual
TEST(BloomFilter, MinSpaceMaxNdv) {
  for (int log2fpp = -2; log2fpp >= -63; --log2fpp) {
    const double fpp = pow(2, log2fpp);
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

TEST(BloomFilter, MinSpaceEdgeCase) {
  int min_space = BloomFilter::MinLogSpace(1, 0.75);
  EXPECT_GE(min_space, 0) << "LogSpace should always be >= 0";
}

// Check that MinLogSpace() and FalsePositiveProb() are dual
TEST(BloomFilter, MinSpaceForFpp) {
  for (size_t ndv = 10000; ndv < 100 * 1000 * 1000; ndv *= 1.01) {
    for (double fpp = 0.1; fpp > pow(2, -20); fpp *= 0.99) { // NOLINT: loop on double
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

class BloomFilterTest : public testing::Test {
 protected:
  /// Temporary runtime environment for the BloomFilters.
  unique_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_;

  ObjectPool pool_;
  unique_ptr<MemTracker> tracker_;
  unique_ptr<BufferPool::ClientHandle> buffer_pool_client_;
  vector<BloomFilter*> bloom_filters_;

  virtual void SetUp() {
    int64_t min_page_size = 64; // Min filter size that we allocate in our tests.
    int64_t buffer_bytes_limit = 4L * 1024 * 1024 * 1024;
    test_env_.reset(new TestEnv());
    test_env_->SetBufferPoolArgs(min_page_size, buffer_bytes_limit);
    ASSERT_OK(test_env_->Init());
    ASSERT_OK(test_env_->CreateQueryState(0, nullptr, &runtime_state_));
    buffer_pool_client_.reset(new BufferPool::ClientHandle);
    tracker_.reset(new MemTracker(-1, "client", runtime_state_->instance_mem_tracker()));
    BufferPool* buffer_pool = test_env_->exec_env()->buffer_pool();
    ASSERT_OK(buffer_pool->RegisterClient("", nullptr,
        runtime_state_->instance_buffer_reservation(), tracker_.get(),
        std::numeric_limits<int64>::max(), runtime_state_->runtime_profile(),
        buffer_pool_client_.get()));
  }

  virtual void TearDown() {
    for (BloomFilter* filter : bloom_filters_) filter->Close();
    bloom_filters_.clear();
    runtime_state_ = nullptr;
    pool_.Clear();
    test_env_->exec_env()->buffer_pool()->DeregisterClient(buffer_pool_client_.get());
    buffer_pool_client_.reset();
    tracker_.reset();
    test_env_.reset();
  }

  BloomFilter* CreateBloomFilter(int log_bufferpool_space) {
    int64_t filter_size = BloomFilter::GetExpectedMemoryUsed(log_bufferpool_space);
    EXPECT_TRUE(buffer_pool_client_->IncreaseReservation(filter_size));
    BloomFilter* bloom_filter = pool_.Add(new BloomFilter(buffer_pool_client_.get()));
    EXPECT_OK(bloom_filter->Init(log_bufferpool_space));
    bloom_filters_.push_back(bloom_filter);
    EXPECT_NE(bloom_filter->GetBufferPoolSpaceUsed(), -1);
    return bloom_filter;
  }

  BloomFilter* CreateBloomFilter(TBloomFilter t_filter) {
    int64_t filter_size =
        BloomFilter::GetExpectedMemoryUsed(t_filter.log_bufferpool_space);
    EXPECT_TRUE(buffer_pool_client_->IncreaseReservation(filter_size));
    BloomFilter* bloom_filter = pool_.Add(new BloomFilter(buffer_pool_client_.get()));
    EXPECT_OK(bloom_filter->Init(t_filter));
    bloom_filters_.push_back(bloom_filter);
    EXPECT_NE(bloom_filter->GetBufferPoolSpaceUsed(), -1);
    return bloom_filter;
  }
};

// We can construct (and destruct) Bloom filters with different spaces.
TEST_F(BloomFilterTest, Constructor) {
  for (int i = 1; i < 30; ++i) {
    CreateBloomFilter(i);
  }
}

// We can Insert() hashes into a Bloom filter with different spaces.
TEST_F(BloomFilterTest, Insert) {
  srand(0);
  for (int i = 13; i < 17; ++i) {
    BloomFilter* bf = CreateBloomFilter(i);
    for (int k = 0; k < (1 << 15); ++k) {
      BfInsert(*bf, MakeRand());
    }
  }
}

// After Insert()ing something into a Bloom filter, it can be found again immediately.
TEST_F(BloomFilterTest, Find) {
  srand(0);
  for (int i = 13; i < 17; ++i) {
    BloomFilter* bf = CreateBloomFilter(i);
    for (int k = 0; k < (1 << 15); ++k) {
      const uint64_t to_insert = MakeRand();
      BfInsert(*bf, to_insert);
      EXPECT_TRUE(BfFind(*bf, to_insert));
    }
  }
}

// After Insert()ing something into a Bloom filter, it can be found again much later.
TEST_F(BloomFilterTest, CumulativeFind) {
  srand(0);
  for (int i = 5; i < 11; ++i) {
    std::vector<uint32_t> inserted;
    BloomFilter* bf = CreateBloomFilter(i);
    for (int k = 0; k < (1 << 10); ++k) {
      const uint32_t to_insert = MakeRand();
      inserted.push_back(to_insert);
      BfInsert(*bf, to_insert);
      for (int n = 0; n < inserted.size(); ++n) {
        EXPECT_TRUE(BfFind(*bf, inserted[n]));
      }
    }
  }
}

// The empirical false positives we find when looking for random items is with a constant
// factor of the false positive probability the Bloom filter was constructed for.
TEST_F(BloomFilterTest, FindInvalid) {
  srand(0);
  static const int find_limit = 1 << 20;
  unordered_set<uint32_t> to_find;
  while (to_find.size() < find_limit) {
    to_find.insert(MakeRand());
  }
  static const int max_log_ndv = 19;
  unordered_set<uint32_t> to_insert;
  while (to_insert.size() < (1ull << max_log_ndv)) {
    const auto candidate = MakeRand();
    if (to_find.find(candidate) == to_find.end()) {
      to_insert.insert(candidate);
    }
  }
  vector<uint32_t> shuffled_insert(to_insert.begin(), to_insert.end());
  for (int log_ndv = 12; log_ndv < max_log_ndv; ++log_ndv) {
    for (int log_fpp = 4; log_fpp < 15; ++log_fpp) {
      double fpp = 1.0 / (1 << log_fpp);
      const size_t ndv = 1 << log_ndv;
      const int log_bufferpool_space = BloomFilter::MinLogSpace(ndv, fpp);
      BloomFilter* bf = CreateBloomFilter(log_bufferpool_space);
      // Fill up a BF with exactly as much ndv as we planned for it:
      for (size_t i = 0; i < ndv; ++i) {
        BfInsert(*bf, shuffled_insert[i]);
      }
      int found = 0;
      // Now we sample from the set of possible hashes, looking for hits.
      for (const auto& i : to_find) {
        found += BfFind(*bf, i);
      }
      EXPECT_LE(found, find_limit * fpp * 2)
          << "Too many false positives with -log2(fpp) = " << log_fpp;
      // Because the space is rounded up to a power of 2, we might actually get a lower
      // fpp than the one passed to MinLogSpace().
      const double expected_fpp =
          BloomFilter::FalsePositiveProb(ndv, log_bufferpool_space);
      EXPECT_GE(found, find_limit * expected_fpp)
          << "Too few false positives with -log2(fpp) = " << log_fpp;
      EXPECT_LE(found, find_limit * expected_fpp * 8)
          << "Too many false positives with -log2(fpp) = " << log_fpp;
    }
  }
}

TEST_F(BloomFilterTest, Thrift) {
  BloomFilter* bf = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  for (int i = 0; i < 10; ++i) BfInsert(*bf, i);
  // Check no unexpected new false positives.
  unordered_set<int> missing_ints;
  for (int i = 11; i < 100; ++i) {
    if (!BfFind(*bf, i)) missing_ints.insert(i);
  }

  TBloomFilter to_thrift;
  BloomFilter::ToThrift(bf, &to_thrift);
  EXPECT_EQ(to_thrift.always_true, false);

  BloomFilter* from_thrift = CreateBloomFilter(to_thrift);
  for (int i = 0; i < 10; ++i) ASSERT_TRUE(BfFind(*from_thrift, i));
  for (int missing: missing_ints) ASSERT_FALSE(BfFind(*from_thrift, missing));

  BloomFilter::ToThrift(NULL, &to_thrift);
  EXPECT_EQ(to_thrift.always_true, true);
}

TEST_F(BloomFilterTest, ThriftOr) {
  BloomFilter* bf1 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  BloomFilter* bf2 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));

  for (int i = 60; i < 80; ++i) BfInsert(*bf2, i);
  for (int i = 0; i < 10; ++i) BfInsert(*bf1, i);

  bool success;
  BloomFilter *bf3 = CreateBloomFilter(BfUnion(*bf1, *bf2, &success));
  ASSERT_TRUE(success) << "SIMD BloomFilter::Union error";
  for (int i = 0; i < 10; ++i) ASSERT_TRUE(BfFind(*bf3, i)) << i;
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(*bf3, i)) << i;

  // Insert another value to aggregated BloomFilter.
  for (int i = 11; i < 50; ++i) BfInsert(*bf3, i);

  // Apply TBloomFilter back to BloomFilter and verify if aggregation was correct.
  BloomFilter *bf4 = CreateBloomFilter(BfUnion(*bf1, *bf3, &success));
  ASSERT_TRUE(success) << "SIMD BloomFilter::Union error";
  for (int i = 11; i < 50; ++i) ASSERT_TRUE(BfFind(*bf4, i)) << i;
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(*bf4, i)) << i;
  ASSERT_FALSE(BfFind(*bf4, 81));
}

}  // namespace impala

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  return RUN_ALL_TESTS();
}
