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

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include "common/object-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/bufferpool/suballocator.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/death-test-util.h"
#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"
#include "util/bit-util.h"

#include "common/names.h"

using std::lognormal_distribution;
using std::mt19937;
using std::shuffle;
using std::uniform_int_distribution;

namespace impala {

class SuballocatorTest : public ::testing::Test {
 public:
  virtual void SetUp() override {
    test_env_.reset(new TestEnv);
    test_env_->DisableBufferPool();
    ASSERT_OK(test_env_->Init());
    RandTestUtil::SeedRng("SUBALLOCATOR_TEST_SEED", &rng_);
    profile_ = RuntimeProfile::Create(&obj_pool_, "test profile");
  }

  virtual void TearDown() override {
    for (unique_ptr<BufferPool::ClientHandle>& client : clients_) {
      buffer_pool_->DeregisterClient(client.get());
    }
    clients_.clear();
    buffer_pool_.reset();
    global_reservation_.Close();
    obj_pool_.Clear();
  }

  /// The minimum buffer size used in most tests. Chosen so that the buffer is split
  /// at least several ways.
  const static int64_t TEST_BUFFER_LEN = Suballocator::MIN_ALLOCATION_BYTES * 16;

 protected:
  /// Initialize 'buffer_pool_' and 'global_reservation_' with a limit of 'total_mem'
  /// bytes of buffers of minimum length 'min_buffer_len'.
  void InitPool(int64_t min_buffer_len, int total_mem) {
    global_reservation_.InitRootTracker(nullptr, total_mem);
    buffer_pool_.reset(
        new BufferPool(test_env_->metrics(), min_buffer_len, total_mem, 0));
  }

  /// Register a client with 'buffer_pool_'. The client is automatically deregistered
  /// and freed at the end of the test.
  void RegisterClient(
      ReservationTracker* parent_reservation, BufferPool::ClientHandle** client) {
    clients_.push_back(make_unique<BufferPool::ClientHandle>());
    *client = clients_.back().get();
    ASSERT_OK(buffer_pool_->RegisterClient("test client", NULL, parent_reservation, NULL,
        numeric_limits<int64_t>::max(), profile_, *client));
  }

  /// Assert that the memory for all of the suballocations is writable and disjoint by
  /// writing a distinct value to each suballocation and reading it back. Only works for
  /// suballocations at least 8 bytes in size.
  void AssertMemoryValid(const vector<unique_ptr<Suballocation>>& allocs);

  /// Free all the suballocations and clear the vector.
  static void FreeAllocations(
      Suballocator* allocator, vector<unique_ptr<Suballocation>>* allocs) {
    for (auto& alloc : *allocs) allocator->Free(move(alloc));
    allocs->clear();
  }

  static void ExpectReservationUnused(BufferPool::ClientHandle* client) {
    EXPECT_EQ(client->GetUsedReservation(), 0) << client->DebugString();
  }

  BufferPool* buffer_pool() { return buffer_pool_.get(); }

  /// Pool for objects with per-test lifetime. Cleared after every test.
  ObjectPool obj_pool_;

  /// The top-level global reservation. Initialized in InitPool() and closed after every
  /// test.
  ReservationTracker global_reservation_;

  /// The buffer pool. Initialized in InitPool() and reset after every test.
  scoped_ptr<BufferPool> buffer_pool_;

  /// Clients for the buffer pool. Deregistered and freed after every test.
  vector<unique_ptr<BufferPool::ClientHandle>> clients_;

  boost::scoped_ptr<TestEnv> test_env_;

  /// Global profile - recreated for every test.
  RuntimeProfile* profile_;

  /// Per-test random number generator. Seeded before every test.
  mt19937 rng_;
};

const int64_t SuballocatorTest::TEST_BUFFER_LEN;

/// Basic test to make sure that we can make multiple suballocations of the same size
/// while using the expected number of buffers.
TEST_F(SuballocatorTest, SameSizeAllocations) {
  const int64_t TOTAL_MEM = TEST_BUFFER_LEN * 100;
  InitPool(TEST_BUFFER_LEN, TOTAL_MEM);
  BufferPool::ClientHandle* client;
  RegisterClient(&global_reservation_, &client);
  Suballocator allocator(buffer_pool(), client, TEST_BUFFER_LEN);
  vector<unique_ptr<Suballocation>> allocs;

  // Make suballocations smaller than the buffer size.
  const int64_t ALLOC_SIZE = TEST_BUFFER_LEN / 4;
  int64_t allocated_mem = 0;
  while (allocated_mem < TOTAL_MEM) {
    allocs.emplace_back();
    ASSERT_OK(allocator.Allocate(ALLOC_SIZE, &allocs.back()));
    ASSERT_TRUE(allocs.back() != nullptr) << ALLOC_SIZE << " " << allocated_mem << " "
                                          << global_reservation_.DebugString();
    allocated_mem += ALLOC_SIZE;
  }

  // Attempts to allocate more memory should fail gracefully.
  const int64_t MAX_ALLOC_SIZE = 1L << 24;
  for (int alloc_size = 1; alloc_size <= MAX_ALLOC_SIZE; alloc_size *= 2) {
    unique_ptr<Suballocation> failed_alloc;
    ASSERT_OK(allocator.Allocate(alloc_size, &failed_alloc));
    ASSERT_TRUE(failed_alloc == nullptr) << alloc_size << " " << allocated_mem << " "
                                         << global_reservation_.DebugString();
  }
  AssertMemoryValid(allocs);

  // Check that reservation usage matches the amount allocated.
  EXPECT_EQ(client->GetUsedReservation(), allocated_mem)
      << global_reservation_.DebugString();
  FreeAllocations(&allocator, &allocs);
  ExpectReservationUnused(client);
}

/// Check behaviour of zero-length allocation.
TEST_F(SuballocatorTest, ZeroLengthAllocation) {
  const int64_t TOTAL_MEM = TEST_BUFFER_LEN * 100;
  InitPool(TEST_BUFFER_LEN, TOTAL_MEM);
  BufferPool::ClientHandle* client;
  RegisterClient(&global_reservation_, &client);
  Suballocator allocator(buffer_pool(), client, TEST_BUFFER_LEN);
  unique_ptr<Suballocation> alloc;

  // Zero-length allocations are allowed and rounded up to the minimum size.
  ASSERT_OK(allocator.Allocate(0, &alloc));
  ASSERT_TRUE(alloc != nullptr) << global_reservation_.DebugString();
  EXPECT_EQ(alloc->len(), Suballocator::MIN_ALLOCATION_BYTES);
  allocator.Free(move(alloc));
  ExpectReservationUnused(client);
}

/// Check behaviour of out-of-range allocation.
TEST_F(SuballocatorTest, OutOfRangeAllocations) {
  const int64_t TOTAL_MEM = TEST_BUFFER_LEN * 100;
  InitPool(TEST_BUFFER_LEN, TOTAL_MEM);
  BufferPool::ClientHandle* client;
  RegisterClient(&global_reservation_, &client);
  Suballocator allocator(buffer_pool(), client, TEST_BUFFER_LEN);
  unique_ptr<Suballocation> alloc;

  // Negative allocations are not allowed and cause a DCHECK.
  IMPALA_ASSERT_DEBUG_DEATH(allocator.Allocate(-1, &alloc), "");

  // Too-large allocations fail gracefully.
  ASSERT_FALSE(allocator.Allocate(Suballocator::MAX_ALLOCATION_BYTES + 1, &alloc).ok())
      << global_reservation_.DebugString();
  ExpectReservationUnused(client);
}

/// Basic test to make sure that non-power-of-two suballocations are handled as expected
/// by rounding up.
TEST_F(SuballocatorTest, NonPowerOfTwoAllocations) {
  const int64_t TOTAL_MEM = TEST_BUFFER_LEN * 128;
  InitPool(TEST_BUFFER_LEN, TOTAL_MEM);
  BufferPool::ClientHandle* client;
  RegisterClient(&global_reservation_, &client);
  Suballocator allocator(buffer_pool(), client, TEST_BUFFER_LEN);

  vector<int64_t> alloc_sizes;
  // Multiply by 7 to get some unusual sizes.
  for (int64_t alloc_size = 7; BitUtil::RoundUpToPowerOfTwo(alloc_size) <= TOTAL_MEM;
       alloc_size *= 7) {
    alloc_sizes.push_back(alloc_size);
  }
  // Test edge cases around power-of-two-sizes.
  for (int64_t power_of_two = 2; power_of_two <= TOTAL_MEM; power_of_two *= 2) {
    alloc_sizes.push_back(power_of_two - 1);
    if (power_of_two != TOTAL_MEM) alloc_sizes.push_back(power_of_two + 1);
  }
  for (int64_t alloc_size : alloc_sizes) {
    unique_ptr<Suballocation> alloc;
    ASSERT_OK(allocator.Allocate(alloc_size, &alloc));
    ASSERT_TRUE(alloc != nullptr) << alloc_size << " "
                                  << global_reservation_.DebugString();

    // Check that it was rounded up to a power-of-two.
    EXPECT_EQ(alloc->len(), max(Suballocator::MIN_ALLOCATION_BYTES,
                                BitUtil::RoundUpToPowerOfTwo(alloc_size)));
    EXPECT_EQ(max(TEST_BUFFER_LEN, alloc->len()), client->GetUsedReservation())
        << global_reservation_.DebugString();
    memset(alloc->data(), 0, alloc->len()); // Check memory is writable.

    allocator.Free(move(alloc));
  }
  ExpectReservationUnused(client);
}

/// Test that simulates hash table's patterns of doubling suballocations and validates
/// that memory does not become fragmented.
TEST_F(SuballocatorTest, DoublingAllocations) {
  const int64_t TOTAL_MEM = TEST_BUFFER_LEN * 100;
  InitPool(TEST_BUFFER_LEN, TOTAL_MEM);
  BufferPool::ClientHandle* client;
  RegisterClient(&global_reservation_, &client);
  Suballocator allocator(buffer_pool(), client, TEST_BUFFER_LEN);

  const int NUM_ALLOCS = 16;
  vector<unique_ptr<Suballocation>> allocs(NUM_ALLOCS);

  // Start with suballocations smaller than the page.
  for (int64_t curr_alloc_size = TEST_BUFFER_LEN / 8;
       curr_alloc_size * NUM_ALLOCS < TOTAL_MEM; curr_alloc_size *= 2) {
    // Randomise the order of suballocations so that coalescing happens in different ways.
    shuffle(allocs.begin(), allocs.end(), rng_);
    for (unique_ptr<Suballocation>& alloc : allocs) {
      unique_ptr<Suballocation> old_alloc = move(alloc);
      ASSERT_OK(allocator.Allocate(curr_alloc_size, &alloc));
      if (old_alloc != nullptr) allocator.Free(move(old_alloc));
    }

    AssertMemoryValid(allocs);

    // Test that the memory isn't fragmented more than expected. In the worst case, the
    // suballocations should require an extra page.
    //
    // If curr_alloc_size is at least the buffer size, there is no fragmentation because
    // all previous suballocations are coalesced and freed, and all new suballocations
    // are backed by a newly-allocated buffer.
    //
    // If curr_alloc_size is less than the buffer size, we lose at most a buffer to
    // fragmentation because previous suballocations are incrementally freed in a way
    // such that they can always be coalesced and reused. At least N/2 out of N of the
    // Free() calls in an iteration result in the free memory being coalesced. This is
    // because either the buddy is freed earlier or later, and the coalescing must happen
    // either in the current Free() call or a later Free() call. Therefore at least
    // N/2 - 1 out of N Allocate() calls follow a Free() call that coalesced memory
    // and can therefore alway recycle a coalesced suballocation instead of allocating
    // additional buffers.
    //
    // In the worst case we end up with two buffers with gaps: one buffer carried over
    // from the previous iteration with a single curr_alloc_size gap (if the last Free()
    // coalesced two buddies of curr_alloc_size / 2) and one buffer with only
    // 'curr_alloc_size' bytes in use (if an Allocate() call couldn't recycle memory and
    // had to allocate a new buffer).
    EXPECT_LE(client->GetUsedReservation(),
        TEST_BUFFER_LEN + max(TEST_BUFFER_LEN, curr_alloc_size * NUM_ALLOCS));
  }
  // Check that reservation usage behaves as expected.
  FreeAllocations(&allocator, &allocs);
  ExpectReservationUnused(client);
}

/// Do some randomised testing of the allocator. Simulate some interesting patterns with
/// a mix of long and short runs of suballocations of variable size. Try to ensure that we
/// spend some time with the allocator near its upper limit, where most suballocations
/// will fail, and also in other parts of its range.
TEST_F(SuballocatorTest, RandomAllocations) {
  const int64_t TOTAL_MEM = TEST_BUFFER_LEN * 1000;
  InitPool(TEST_BUFFER_LEN, TOTAL_MEM);
  BufferPool::ClientHandle* client;
  RegisterClient(&global_reservation_, &client);
  Suballocator allocator(buffer_pool(), client, TEST_BUFFER_LEN);

  vector<unique_ptr<Suballocation>> allocs;
  int64_t allocated_mem = 0;
  for (int iter = 0; iter < 1000; ++iter) {
    // We want to make runs of suballocations and frees. Use lognormal distribution so
    // that runs are mostly short, but there are some long runs mixed in.
    int num_allocs = max(1, static_cast<int>(lognormal_distribution<double>(3, 1)(rng_)));
    const bool alloc = uniform_int_distribution<int>(0, 1)(rng_);
    if (alloc) {
      const int64_t remaining_mem_per_alloc = (TOTAL_MEM - allocated_mem) / num_allocs;
      // Fraction is ~0.12 on average but sometimes ranges above 1.0 so that we'll hit the
      // max reservation and suballocations will fail.
      double fraction_to_alloc = lognormal_distribution<double>(2, 1)(rng_) / 100.;
      int64_t alloc_size = max(8L, BitUtil::RoundUpToPowerOfTwo(static_cast<int64_t>(
                                       fraction_to_alloc * remaining_mem_per_alloc)));
      for (int i = 0; i < num_allocs; ++i) {
        unique_ptr<Suballocation> alloc;
        ASSERT_OK(allocator.Allocate(alloc_size, &alloc));
        if (alloc != nullptr) {
          EXPECT_EQ(alloc->len(), max(alloc_size, Suballocator::MIN_ALLOCATION_BYTES));
          allocated_mem += alloc->len();
          allocs.push_back(move(alloc));
        } else {
          LOG(INFO) << "Failed to alloc " << alloc_size << " consumed " << allocated_mem
                    << "/" << TOTAL_MEM;
        }
      }
    } else {
      // Free a random subset of suballocations.
      num_allocs = min<int>(num_allocs, allocs.size());
      shuffle(allocs.end() - num_allocs, allocs.end(), rng_);
      for (int i = 0; i < num_allocs; ++i) {
        allocated_mem -= allocs.back()->len();
        allocator.Free(move(allocs.back()));
        allocs.pop_back();
      }
    }
    // Occasionally check that the suballocations are valid.
    if (iter % 50 == 0) AssertMemoryValid(allocs);
  }
  // Check that memory is released when suballocations are freed.
  FreeAllocations(&allocator, &allocs);
  ExpectReservationUnused(client);
}

void SuballocatorTest::AssertMemoryValid(
    const vector<unique_ptr<Suballocation>>& allocs) {
  for (int64_t i = 0; i < allocs.size(); ++i) {
    const unique_ptr<Suballocation>& alloc = allocs[i];
    ASSERT_GE(alloc->len(), 8);
    // Memory should be 8-byte aligned.
    ASSERT_EQ(0, reinterpret_cast<uint64_t>(alloc->data()) % 8) << alloc->data();
    for (int64_t offset = 0; offset < alloc->len(); offset += 8) {
      *reinterpret_cast<int64_t*>(alloc->data() + offset) = i;
    }
  }
  for (int64_t i = 0; i < allocs.size(); ++i) {
    const unique_ptr<Suballocation>& alloc = allocs[i];
    for (int64_t offset = 0; offset < alloc->len(); offset += 8) {
      ASSERT_EQ(*reinterpret_cast<int64_t*>(alloc->data() + offset), i)
          << i << " " << alloc->data() << " " << offset;
    }
  }
}
}
