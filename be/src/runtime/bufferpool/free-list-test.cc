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

#include <cstdlib>
#include <random>

#include "common/object-pool.h"
#include "runtime/bufferpool/free-list.h"
#include "runtime/bufferpool/system-allocator.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"

#include "common/names.h"

namespace impala {

class FreeListTest : public ::testing::Test {
 protected:
  virtual void SetUp() override {
    test_env_.reset(new TestEnv);
    ASSERT_OK(test_env_->Init());
    allocator_ = obj_pool_.Add(new SystemAllocator(MIN_BUFFER_LEN));
    RandTestUtil::SeedRng("FREE_LIST_TEST_SEED", &rng_);
  }

  virtual void TearDown() override {
    allocator_ = nullptr;
    obj_pool_.Clear();
  }

  void AllocateBuffers(
      int num_buffers, int64_t buffer_len, vector<BufferHandle>* buffers) {
    for (int i = 0; i < num_buffers; ++i) {
      BufferHandle buffer;
      ASSERT_OK(allocator_->Allocate(buffer_len, &buffer));
      buffers->push_back(move(buffer));
    }
  }

  vector<const void*> GetSortedAddrs(const vector<BufferHandle>& buffers) {
    vector<const void*> addrs;
    addrs.reserve(buffers.size());
    for (const BufferHandle& buffer : buffers) addrs.push_back(buffer.data());
    std::sort(addrs.begin(), addrs.end());
    return addrs;
  }

  void AddFreeBuffers(FreeList* list, vector<BufferHandle>* buffers) {
    for (BufferHandle& buffer : *buffers) {
      list->AddFreeBuffer(move(buffer));
    }
    buffers->clear();
  }

  void FreeBuffers(vector<BufferHandle>&& buffers) {
    for (BufferHandle& buffer : buffers) {
      allocator_->Free(move(buffer));
    }
  }

  const static int MIN_BUFFER_LEN = 1024;

  boost::scoped_ptr<TestEnv> test_env_;

  /// Per-test random number generator. Seeded before every test.
  std::mt19937 rng_;

  /// Pool for objects with per-test lifetime. Cleared after every test.
  ObjectPool obj_pool_;

  /// The buffer allocator, owned by 'obj_pool_'.
  SystemAllocator* allocator_;
};

const int FreeListTest::MIN_BUFFER_LEN;

// Functional test for a small free list.
TEST_F(FreeListTest, SmallList) {
  const int LIST_SIZE = 2;
  FreeList small_list;

  // PopFreeBuffer() on empty list returns false.
  BufferHandle buffer;
  ASSERT_FALSE(small_list.PopFreeBuffer(&buffer));
  ASSERT_FALSE(small_list.PopFreeBuffer(&buffer));

  // Add various numbers of buffers to the free list and check that they're
  // either freed or returned in the order expected.
  for (int num_buffers = 0; num_buffers <= LIST_SIZE + 2; ++num_buffers) {
    for (int attempt = 0; attempt < 10; ++attempt) {
      LOG(INFO) << "num_buffers " << num_buffers << " attempt " << attempt;
      vector<BufferHandle> buffers;
      AllocateBuffers(num_buffers, MIN_BUFFER_LEN, &buffers);

      // Keep track of the addresses so we can validate the buffer order.
      const vector<const void*> addrs = GetSortedAddrs(buffers);

      // Try shuffling to make sure we don't always just add in ascending order.
      std::shuffle(buffers.begin(), buffers.end(), rng_);
      AddFreeBuffers(&small_list, &buffers);
      // Shrink list down to LIST_SIZE.
      FreeBuffers(
          small_list.GetBuffersToFree(max<int64_t>(0, small_list.Size() - LIST_SIZE)));

      // The LIST_SIZE buffers with the lowest address should be retained, and the
      // remaining buffers should have been freed.
      for (int i = 0; i < min(num_buffers, LIST_SIZE); ++i) {
        ASSERT_TRUE(small_list.PopFreeBuffer(&buffer)) << i;
        ASSERT_EQ(addrs[i], buffer.data()) << i;
        buffers.push_back(move(buffer));
      }
      ASSERT_FALSE(small_list.PopFreeBuffer(&buffer));
      FreeBuffers(move(buffers));
    }
  }
}

// Functional test that makes sure the free lists return buffers in ascending order
TEST_F(FreeListTest, ReturnOrder) {
  const int LIST_SIZE = 100;
  FreeList list;
  for (int num_buffers = 50; num_buffers <= 400; num_buffers *= 2) {
    for (int attempt = 0; attempt < 5; ++attempt) {
      LOG(INFO) << "num_buffers " << num_buffers << " attempt " << attempt;
      vector<BufferHandle> buffers;
      AllocateBuffers(num_buffers, MIN_BUFFER_LEN, &buffers);

      // Keep track of the addresses so we can validate the buffer order.
      const vector<const void*> addrs = GetSortedAddrs(buffers);

      // Try shuffling to make sure we don't always just add in ascending order.
      std::shuffle(buffers.begin(), buffers.end(), rng_);
      AddFreeBuffers(&list, &buffers);

      // Free buffers. Only the buffers with the high addresses should be freed.
      FreeBuffers(list.GetBuffersToFree(max<int64_t>(0, list.Size() - LIST_SIZE)));

      // Validate that the buffers with lowest addresses are returned in ascending order.
      BufferHandle buffer;
      for (int i = 0; i < min(LIST_SIZE, num_buffers); ++i) {
        ASSERT_TRUE(list.PopFreeBuffer(&buffer));
        ASSERT_EQ(addrs[i], buffer.data()) << i;
        allocator_->Free(move(buffer));
      }
      ASSERT_FALSE(list.PopFreeBuffer(&buffer));
    }
  }
}
}
