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

#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/random.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/mem-tracker.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"

#include "gen-cpp/data_stream_service.pb.h"

// This flag is used in Kudu to temporarily disable AVX2 support for testing purpose.
DECLARE_bool(disable_blockbloomfilter_avx2);

using namespace std;

using namespace impala;
using kudu::rpc::RpcController;

namespace bloom_filter_test_util {

// Make a random uint32_t, avoiding the absent high bit and the low-entropy low bits
// produced by rand().
uint32_t MakeRand() {
  uint32_t result = (rand() >> 8) & 0xffff;
  result <<= 16;
  result |= (rand() >> 8) & 0xffff;
  return result;
}

// BfInsert() and BfFind() call BloomFilter::{Insert,Find} respectively.

void BfInsert(BloomFilter& bf, uint32_t h) {
  bf.Insert(h);
}

bool BfFind(BloomFilter& bf, uint32_t h) {
  return bf.Find(h);
}

// Computes union of 'x' and 'y'. Computes twice with AVX enabled and disabled and
// verifies both produce the same result. 'success' is set to true if both union
// computations returned the same result and set to false otherwise.
void BfUnion(BloomFilter& x, BloomFilter& y, int64_t directory_size, bool* success,
    BloomFilterPB* protobuf, std::string* directory) {
  BloomFilterPB protobuf_x, protobuf_y;
  RpcController controller_x;
  RpcController controller_y;
  BloomFilter::ToProtobuf(&x, &controller_x, &protobuf_x);
  BloomFilter::ToProtobuf(&y, &controller_y, &protobuf_y);

  FLAGS_disable_blockbloomfilter_avx2 = false;
  string directory_x(
      reinterpret_cast<const char*>(x.GetBlockBloomFilter()->directory().data()),
      directory_size);
  string directory_y(
      reinterpret_cast<const char*>(y.GetBlockBloomFilter()->directory().data()),
      directory_size);

  BloomFilter::Or(protobuf_x, reinterpret_cast<const uint8_t*>(directory_x.data()),
      &protobuf_y, reinterpret_cast<uint8_t*>(const_cast<char*>(directory_y.data())),
      directory_size);

  {
    FLAGS_disable_blockbloomfilter_avx2 = true;
    BloomFilterPB protobuf_x2, protobuf_y2;
    RpcController controller_x2;
    RpcController controller_y2;
    BloomFilter::ToProtobuf(&x, &controller_x2, &protobuf_x2);
    BloomFilter::ToProtobuf(&y, &controller_y2, &protobuf_y2);

    string directory_x2(
        reinterpret_cast<const char*>(x.GetBlockBloomFilter()->directory().data()),
        directory_size);
    string directory_y2(
        reinterpret_cast<const char*>(y.GetBlockBloomFilter()->directory().data()),
        directory_size);

    BloomFilter::Or(protobuf_x2, reinterpret_cast<const uint8_t*>(directory_x2.data()),
        &protobuf_y2, reinterpret_cast<uint8_t*>(const_cast<char*>(directory_y2.data())),
        directory_size);

    *success = directory_y.compare(directory_y2) == 0;
  }

  *protobuf = protobuf_y;
  *directory = directory_y;
}

} // namespace bloom_filter_test_util

using namespace bloom_filter_test_util;

namespace impala {

// Test that MaxNdv() and MinLogSpace() are dual
TEST(BloomFilter, MinSpaceMaxNdv) {
  for (int log2fpp = -2; log2fpp >= -30; --log2fpp) {
    const double fpp = pow(2, log2fpp);
    for (int given_log_space = 8; given_log_space < 30; ++given_log_space) {
      const size_t derived_ndv = BloomFilter::MaxNdv(given_log_space, fpp);
      // If NO values can be added without exceeding fpp, then the space needed is
      // trivially zero. This becomes a useless test; skip to the next iteration.
      if (0 == derived_ndv) continue;
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
  for (size_t ndv = 10000; ndv < 100 * 1000 * 1000; ndv *= 1.1) {
    for (double fpp = 0.1; fpp > pow(2, -20); fpp *= 0.9) { // NOLINT: loop on double
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
    // Randomly disable AVX2 instructions half of the time. These are used for testing
    // that AVX2 machines and non-AVX2 machines produce compatible BloomFilters.
    FLAGS_disable_blockbloomfilter_avx2 = (MakeRand() & 0x1) == 0;
    BloomFilter* bloom_filter = pool_.Add(new BloomFilter(buffer_pool_client_.get()));
    EXPECT_OK(bloom_filter->Init(log_bufferpool_space, 0));
    bloom_filters_.push_back(bloom_filter);
    EXPECT_NE(bloom_filter->GetBufferPoolSpaceUsed(), -1);
    return bloom_filter;
  }

  BloomFilter* CreateBloomFilter(BloomFilterPB filter_pb, const std::string& directory) {
    int64_t filter_size =
        BloomFilter::GetExpectedMemoryUsed(filter_pb.log_bufferpool_space());
    EXPECT_TRUE(buffer_pool_client_->IncreaseReservation(filter_size));
    // Randomly disable AVX2 instructions half of the time. These are used for testing
    // that AVX2 machines and non-AVX2 machines produce compatible BloomFilters.
    FLAGS_disable_blockbloomfilter_avx2 = (MakeRand() & 0x1) == 0;
    BloomFilter* bloom_filter = pool_.Add(new BloomFilter(buffer_pool_client_.get()));

    EXPECT_OK(bloom_filter->Init(filter_pb,
        reinterpret_cast<const uint8_t*>(directory.data()), directory.size(), 0));

    bloom_filters_.push_back(bloom_filter);
    EXPECT_NE(bloom_filter->GetBufferPoolSpaceUsed(), -1);
    return bloom_filter;
  }
};

// We can construct (and destruct) Bloom filters with different spaces.
TEST_F(BloomFilterTest, Constructor) {
  // The minimum log_bufferpool_space size is 5 for bloom filter, which is defined
  // as BlockBloomFilter.kLogBucketWordBits in kudu/util/block_bloom_filter.h,
  // and is checked in function BlockBloomFilter.GetExpectedMemoryUsed().
  for (int i = 5; i < 30; ++i) {
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
      const uint32_t to_insert = MakeRand();
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
  // We use a deterministic pseudorandom number generator with a set seed. The reason is
  // that with a run-dependent seed, there will always be inputs that can fail. That's a
  // potential argument for this to be a benchmark rather than a test, although the
  // measured quantity would be not time but deviation from predicted fpp.
  ::kudu::Random rgen(867 + 5309);
  static const int find_limit = 1 << 22;
  unordered_set<uint32_t> to_find;
  while (to_find.size() < find_limit) {
    to_find.insert(rgen.Next64());
  }
  static const int max_log_ndv = 19;
  unordered_set<uint32_t> to_insert;
  while (to_insert.size() < (1ull << max_log_ndv)) {
    const auto candidate = rgen.Next64();
    if (to_find.find(candidate) == to_find.end()) {
      to_insert.insert(candidate);
    }
  }
  vector<uint32_t> shuffled_insert(to_insert.begin(), to_insert.end());
  for (int log_ndv = 12; log_ndv < max_log_ndv; ++log_ndv) {
    for (int log_fpp = 4; log_fpp < 12; ++log_fpp) {
      double fpp = 1.0 / (1 << log_fpp);
      const size_t ndv = 1 << log_ndv;
      const int log_heap_space = BloomFilter::MinLogSpace(ndv, fpp);
      BloomFilter* bf = CreateBloomFilter(log_heap_space);
      // Fill up a BF with exactly as much ndv as we planned for it:
      for (size_t i = 0; i < ndv; ++i) {
        bf->Insert( shuffled_insert[i]);
      }
      int found = 0;
      // Now we sample from the set of possible hashes, looking for hits.
      for (const auto& i : to_find) {
        found += bf->Find(i);
      }
      EXPECT_LE(found, find_limit * fpp * 2)
          << "Too many false positives with -log2(fpp) = " << log_fpp
          << " and log_ndv = " << log_ndv << " and log_heap_space = " << log_heap_space;
      // Because the space is rounded up to a power of 2, we might actually get a lower
      // fpp than the one passed to MinLogSpace().
      const double expected_fpp = BloomFilter::FalsePositiveProb(ndv, log_heap_space);
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

TEST_F(BloomFilterTest, Protobuf) {
  BloomFilter* bf = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  for (int i = 0; i < 10; ++i) BfInsert(*bf, i);
  // Check no unexpected new false positives.
  unordered_set<int> missing_ints;
  for (int i = 11; i < 100; ++i) {
    if (!BfFind(*bf, i)) missing_ints.insert(i);
  }

  BloomFilterPB to_protobuf;

  RpcController controller;
  BloomFilter::ToProtobuf(bf, &controller, &to_protobuf);

  EXPECT_EQ(to_protobuf.always_true(), false);

  std::string directory(
      reinterpret_cast<const char*>(bf->GetBlockBloomFilter()->directory().data()),
      BloomFilter::GetExpectedMemoryUsed(BloomFilter::MinLogSpace(100, 0.01)));

  BloomFilter* from_protobuf = CreateBloomFilter(to_protobuf, directory);

  for (int i = 0; i < 10; ++i) ASSERT_TRUE(BfFind(*from_protobuf, i));
  for (int missing : missing_ints) ASSERT_FALSE(BfFind(*from_protobuf, missing));

  RpcController controller_2;
  BloomFilter::ToProtobuf(nullptr, &controller_2, &to_protobuf);
  EXPECT_EQ(to_protobuf.always_true(), true);
}

// Basic test for the Or() operation on BloomFilter objects.
// ThriftOr() tests the low-level implementation more exhaustively.
TEST_F(BloomFilterTest, Or) {
  BloomFilter* bf1 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  BloomFilter* bf2 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));

  for (int i = 60; i < 80; ++i) BfInsert(*bf2, i);
  for (int i = 0; i < 10; ++i) BfInsert(*bf1, i);

  bf1->Or(*bf2);
  for (int i = 0; i < 10; ++i) ASSERT_TRUE(BfFind(*bf1, i)) << i;
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(*bf1, i)) << i;

  // Insert another value to aggregated BloomFilter.
  for (int i = 11; i < 50; ++i) BfInsert(*bf1, i);

  for (int i = 11; i < 50; ++i) ASSERT_TRUE(BfFind(*bf1, i));
  ASSERT_FALSE(BfFind(*bf1, 81));

  // Check that AlwaysFalse() is updated correctly.
  BloomFilter* bf3 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  BloomFilter* always_false = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  bf3->Or(*always_false);
  EXPECT_TRUE(bf3->AlwaysFalse());
  bf3->Or(*bf2);
  EXPECT_FALSE(bf3->AlwaysFalse());
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(*bf1, i)) << i;
  ASSERT_FALSE(BfFind(*bf1, 81));
}

TEST_F(BloomFilterTest, ProtobufOr) {
  BloomFilter* bf1 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  BloomFilter* bf2 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));

  for (int i = 60; i < 80; ++i) BfInsert(*bf2, i);
  for (int i = 0; i < 10; ++i) BfInsert(*bf1, i);

  bool success;
  BloomFilterPB protobuf;
  std::string directory;
  int64_t directory_size =
      BloomFilter::GetExpectedMemoryUsed(BloomFilter::MinLogSpace(100, 0.01));

  BfUnion(*bf1, *bf2, directory_size, &success, &protobuf, &directory);

  BloomFilter* bf3 = CreateBloomFilter(protobuf, directory);

  ASSERT_TRUE(success) << "SIMD BloomFilter::Union error";
  for (int i = 0; i < 10; ++i) ASSERT_TRUE(BfFind(*bf3, i)) << i;
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(*bf3, i)) << i;

  // Insert another value to aggregated BloomFilter.
  for (int i = 11; i < 50; ++i) BfInsert(*bf3, i);

  // Apply BloomFilterPB back to BloomFilter and verify if aggregation was correct.
  BfUnion(*bf1, *bf3, directory_size, &success, &protobuf, &directory);
  BloomFilter* bf4 = CreateBloomFilter(protobuf, directory);

  ASSERT_TRUE(success) << "SIMD BloomFilter::Union error";
  for (int i = 11; i < 50; ++i) ASSERT_TRUE(BfFind(*bf4, i)) << i;
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(*bf4, i)) << i;
  ASSERT_FALSE(BfFind(*bf4, 81));
}

}  // namespace impala

