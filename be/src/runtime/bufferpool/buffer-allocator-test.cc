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

#include <vector>

#include "common/object-pool.h"
#include "runtime/bufferpool/buffer-allocator.h"
#include "runtime/bufferpool/buffer-pool-internal.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/system-allocator.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/cpu-util.h"
#include "testutil/gtest-util.h"
#include "util/cpu-info.h"
#include "util/metrics.h"

#include "common/names.h"

DECLARE_bool(mmap_buffers);
DECLARE_bool(madvise_huge_pages);

namespace impala {

using BufferAllocator = BufferPool::BufferAllocator;
using BufferHandle = BufferPool::BufferHandle;

class BufferAllocatorTest : public ::testing::Test {
 public:
  virtual void SetUp() {
    test_env_.reset(new TestEnv);
    test_env_->DisableBufferPool();
    ASSERT_OK(test_env_->Init());
    MetricGroup* dummy_metrics = obj_pool_.Add(new MetricGroup("test"));
    dummy_pool_ = obj_pool_.Add(new BufferPool(dummy_metrics, 1, 0, 0));
    dummy_reservation_.InitRootTracker(nullptr, 0);
    ASSERT_OK(dummy_pool_->RegisterClient("", nullptr, &dummy_reservation_, nullptr, 0,
        RuntimeProfile::Create(&obj_pool_, ""), &dummy_client_));
  }

  virtual void TearDown() {
    dummy_pool_->DeregisterClient(&dummy_client_);
    dummy_reservation_.Close();
    obj_pool_.Clear();
    CpuTestUtil::ResetAffinity(); // Some tests modify affinity.
  }

  int GetFreeListSize(BufferAllocator* allocator, int core, int64_t len) {
    return allocator->GetFreeListSize(core, len);
  }

  /// The minimum buffer size used in most tests.
  const static int64_t TEST_BUFFER_LEN = 1024;

  boost::scoped_ptr<TestEnv> test_env_;

  ObjectPool obj_pool_;

  /// Need a dummy pool and client to pass around. We bypass the reservation mechanisms
  /// in these tests so they don't need to be properly initialised.
  BufferPool* dummy_pool_;
  BufferPool::ClientHandle dummy_client_;
  ReservationTracker dummy_reservation_;
};

#ifdef ADDRESS_SANITIZER

// Confirm that ASAN will catch use-after-free errors, even if the BufferAllocator caches
// returned memory.
TEST_F(BufferAllocatorTest, Poisoning) {
  BufferAllocator allocator(dummy_pool_, test_env_->metrics(), TEST_BUFFER_LEN,
      2 * TEST_BUFFER_LEN, 2 * TEST_BUFFER_LEN);
  BufferHandle buffer;
  ASSERT_OK(allocator.Allocate(&dummy_client_, TEST_BUFFER_LEN, &buffer));
  uint8_t* data = buffer.data();
  allocator.Free(move(buffer));

  // Should trigger a ASAN failure.
  ASSERT_DEATH({data[10] = 0;}, "use-after-poison");
}

#endif

// Functional test that makes sure the free lists cache as many buffers as expected.
TEST_F(BufferAllocatorTest, FreeListSizes) {
  // Run on core 0 to ensure that we always go to the same free list.
  const int CORE = 0;
  CpuTestUtil::PinToCore(CORE);

  const int NUM_BUFFERS = 512;
  const int64_t TOTAL_BYTES = NUM_BUFFERS * TEST_BUFFER_LEN;

  BufferAllocator allocator(
      dummy_pool_, test_env_->metrics(), TEST_BUFFER_LEN, TOTAL_BYTES, TOTAL_BYTES);

  // Allocate a bunch of buffers - all free list checks should miss.
  vector<BufferHandle> buffers(NUM_BUFFERS);
  for (int i = 0; i < NUM_BUFFERS; ++i) {
    ASSERT_OK(allocator.Allocate(&dummy_client_, TEST_BUFFER_LEN, &buffers[i]));
  }

  // Add back the allocated buffers - all should be added to the list.
  for (BufferHandle& buffer : buffers) allocator.Free(move(buffer));
  ASSERT_EQ(NUM_BUFFERS, GetFreeListSize(&allocator, CORE, TEST_BUFFER_LEN));

  // We should be able to get back the buffers from this list.
  for (int i = 0; i < NUM_BUFFERS; ++i) {
    ASSERT_OK(allocator.Allocate(&dummy_client_, TEST_BUFFER_LEN, &buffers[i]));
    ASSERT_TRUE(buffers[i].is_open());
  }
  ASSERT_EQ(0, GetFreeListSize(&allocator, CORE, TEST_BUFFER_LEN));

  // Add back the buffers.
  for (BufferHandle& buffer : buffers) allocator.Free(move(buffer));
  ASSERT_EQ(NUM_BUFFERS, GetFreeListSize(&allocator, CORE, TEST_BUFFER_LEN));

  // Test DebugString().
  LOG(INFO) << allocator.DebugString();

  // Periodic maintenance should shrink the list's size each time after the first two
  // calls, since the low water mark is the current size.
  int maintenance_calls = 0;
  while (GetFreeListSize(&allocator, CORE, TEST_BUFFER_LEN) > 0) {
    int prev_size = GetFreeListSize(&allocator, CORE, TEST_BUFFER_LEN);
    allocator.Maintenance();
    int new_size = GetFreeListSize(&allocator, CORE, TEST_BUFFER_LEN);
    if (maintenance_calls == 0) {
      // The low water mark should be zero until we've called Maintenance() once.
      EXPECT_EQ(prev_size, new_size);
    } else {
      // The low water mark will be the current size, so half the buffers should be freed.
      EXPECT_EQ(prev_size == 1 ? 0 : prev_size - prev_size / 2, new_size);
    }
    // Check that the allocator reports the correct numbers.
    EXPECT_EQ(new_size, allocator.GetNumFreeBuffers());
    EXPECT_EQ(new_size * TEST_BUFFER_LEN, allocator.GetFreeBufferBytes());
    ++maintenance_calls;
  }

  // Also exercise ReleaseMemory() - it should clear out the list entirely.
  for (int i = 0; i < NUM_BUFFERS; ++i) {
    ASSERT_OK(allocator.Allocate(&dummy_client_, TEST_BUFFER_LEN, &buffers[i]));
  }
  for (BufferHandle& buffer : buffers) allocator.Free(move(buffer));
  allocator.ReleaseMemory(TOTAL_BYTES);
  ASSERT_EQ(0, GetFreeListSize(&allocator, CORE, TEST_BUFFER_LEN));
}

class SystemAllocatorTest : public ::testing::Test {
 public:
  virtual void SetUp() {}

  virtual void TearDown() {}

  static const int64_t MIN_BUFFER_LEN = 4 * 1024;
  static const int64_t MAX_BUFFER_LEN = 1024 * 1024 * 1024;
};

/// Basic test that checks that we can allocate buffers of the expected power-of-two
/// sizes.
TEST_F(SystemAllocatorTest, BasicPowersOfTwo) {
  SystemAllocator allocator(MIN_BUFFER_LEN);

  // Iterate a few times to make sure we can reallocate.
  for (int iter = 0; iter < 5; ++iter) {
    // Allocate buffers of a mix of sizes.
    vector<BufferHandle> buffers;
    for (int alloc_iter = 0; alloc_iter < 2; ++alloc_iter) {
      for (int64_t len = MIN_BUFFER_LEN; len <= MAX_BUFFER_LEN; len *= 2) {
        BufferHandle buffer;
        ASSERT_OK(allocator.Allocate(len, &buffer));
        ASSERT_TRUE(buffer.is_open());
        // Write a few bytes to the buffer to check it's valid memory.
        buffer.data()[0] = 0;
        buffer.data()[buffer.len() / 2] = 0;
        buffer.data()[buffer.len() - 1] = 0;
        buffers.push_back(move(buffer));
      }
    }

    // Free all the buffers.
    for (BufferHandle& buffer : buffers) allocator.Free(move(buffer));
  }
}

/// Make an absurdly large allocation to test the failure path.
TEST_F(SystemAllocatorTest, LargeAllocFailure) {
  SystemAllocator allocator(MIN_BUFFER_LEN);
  BufferHandle buffer;
  Status status = allocator.Allocate(1LL << 48, &buffer);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.msg().error(), TErrorCode::BUFFER_ALLOCATION_FAILED);
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  int result = 0;
  for (bool mmap : {false, true}) {
    for (bool madvise : {false, true}) {
      std::cerr << "+==================================================" << std::endl
                << "| Running tests with mmap=" << mmap << " madvise=" << madvise
                << std::endl
                << "+==================================================" << std::endl;
      FLAGS_mmap_buffers = mmap;
      FLAGS_madvise_huge_pages = madvise;
      if (RUN_ALL_TESTS() != 0) result = 1;
    }
  }
  return result;
}
