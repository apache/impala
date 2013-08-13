// Copyright 2012 Cloudera Inc.
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

#include "blocked-vector.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "runtime/mem-pool.h"
#include "block-mem-pool.h"
#include "sort-util.h"

using namespace std;

namespace impala {

TEST(BlockedVectorTest, TestBasic) {
  int element_size = sizeof(int64_t);
  BufferPool buffer_pool(10, element_size * 1024);
  ObjectPool obj_pool;
  BlockMemPool block_mem_pool(&obj_pool, &buffer_pool);
  BlockedVector<int64_t> vector(&block_mem_pool, element_size);

  int64_t* data = (int64_t*) malloc(element_size * 2);
  *(data+0) = 0x1337;
  *(data+1) = 0xca7;

  ASSERT_EQ(0, vector.size());
  vector.Insert(data + 1); // insert second element first
  ASSERT_EQ(1, vector.size());
  vector.Insert(data + 0);
  ASSERT_EQ(2, vector.size());

  ASSERT_EQ(0xca7,  *vector[0]);
  ASSERT_EQ(0x1337, *vector[1]);

  free(data);
}

struct TestElement {
  int data[0];
};

TEST(BlockedVectorTest, TestAllocation) {
  int element_size = sizeof(int) * 4;
  BufferPool buffer_pool(1024, 1024 * element_size);
  ObjectPool obj_pool;
  BlockMemPool block_mem_pool(&obj_pool, &buffer_pool);
  BlockedVector<TestElement> vector(&block_mem_pool, element_size);
  ASSERT_EQ(0, vector.bytes_allocated());
  ASSERT_EQ(0, vector.size());

  TestElement* element = (TestElement*) malloc(element_size);
  for (int i = 0; i < 1024; ++i) {
    element->data[3] = i;
    vector.Insert(element);
    ASSERT_EQ(1024*element_size, vector.bytes_allocated());
    ASSERT_EQ(i+1, vector.size());
  }

  // The 1025th element should cause us to allocate a new block.
  element->data[3] = 1024;
  vector.Insert(element);
  ASSERT_EQ((1024+1024)*element_size, vector.bytes_allocated());
  ASSERT_EQ(1025, vector.size());

  for (int i = 1; i < 1024; ++i) {
    element->data[3] = 1024+i;
    vector.Insert(element);
    ASSERT_EQ((1024+1024)*element_size, vector.bytes_allocated());
    ASSERT_EQ(1025+i, vector.size());
  }

  element->data[3] = 1024+1024;
  vector.Insert(element);
  ASSERT_EQ((1024+1024+1024)*element_size, vector.bytes_allocated());

  // One last check for the data
  ASSERT_EQ(1024+1024+1, vector.size());
  for (int i = 0; i < vector.size(); ++i) {
    ASSERT_EQ(i, vector[i]->data[3]);
  }

  free(element);
}

TEST(BlockedVectorTest, TestIterator) {
  int element_size = sizeof(uint32_t);
  BufferPool buffer_pool(10, element_size * 1024);
  ObjectPool obj_pool;
  BlockMemPool block_mem_pool(&obj_pool, &buffer_pool);
  BlockedVector<uint32_t> vector(&block_mem_pool, element_size);

  uint32_t element;
  for (int i = 0; i < 1025; ++i) {
    element = i;
    vector.Insert(&element);
  }

  // Test iterating forwards...
  uint32_t i;
  BlockedVector<uint32_t>::Iterator it = vector.Begin();
  for (i = 0; it != vector.End(); ++it, ++i) {
    uint32_t* value = *it;
    ASSERT_EQ(i, *value);
  }

  // ... and backwards
  it = vector.End();
  for (i = vector.size() - 1; it != vector.Begin(); --i) {
    --it;
    uint32_t* value = *it;
    ASSERT_EQ(i, *value);
  }

  // Test basic operators
  it = vector.Begin();
  ASSERT_EQ(**(it+512), 512);
  ASSERT_EQ(**(it+1024), 1024); // next block
  ASSERT_EQ(**((it+1024)-512), 512);
  ASSERT_TRUE(it == it);
  ASSERT_TRUE((it+512) == ((it+1024)-512));
  ASSERT_TRUE(it <= it && it >= it);
  ASSERT_TRUE(vector.Begin() != vector.End());
  ASSERT_TRUE(vector.Begin() < vector.End());
  ASSERT_TRUE(vector.End() > vector.Begin());
  ASSERT_EQ(vector.End() - vector.Begin(), 1025);
  ASSERT_EQ(vector.Begin() - vector.Begin(), 0);

  // Test mutation operators
  ++it;
  ASSERT_EQ(it - vector.Begin(), 1);
  element = -1;
  it.SetValue(&element);
  ASSERT_EQ(**it, -1);
  ASSERT_EQ(*vector[1], -1);
  BlockedVector<uint32_t>::Iterator last = vector.End() - 1;
  void* swap_buffer = malloc(sizeof(uint32_t));
  it.Swap(last, swap_buffer);
  ASSERT_EQ(**it, 1024);
  ASSERT_EQ(*vector[1], 1024);
  ASSERT_EQ(**last, -1);
  ASSERT_EQ(*vector[1024], -1);
}

// Ensure we're SortUtil-compliant.
TEST(BlockedVectorTest, TestSort) {
  int element_size = sizeof(uint8_t);
  BufferPool buffer_pool(10, element_size * 1024);
  ObjectPool obj_pool;
  BlockMemPool block_mem_pool(&obj_pool, &buffer_pool);
  BlockedVector<uint8_t> vector(&block_mem_pool, element_size);

  uint8_t x;
  for (int i = 0; i < 1024; ++i) {
    x = rand() % 128;
    vector.Insert(&x);
  }

  SortUtil<uint8_t>::SortNormalized(vector.Begin(), vector.End(),
                                    element_size, 0, element_size);
  uint8_t* last = NULL;
  for (BlockedVector<uint8_t>::Iterator bv = vector.Begin(); bv != vector.End(); ++bv) {
    uint8_t* value = *bv;
    if (last != NULL) {
      ASSERT_GE(*value, *last);
    }
    last = value;
  }
}

}


