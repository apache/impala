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

#include <string>
#include <gtest/gtest.h>

#include "runtime/free-list.h"
#include "runtime/mem-pool.h"


using namespace std;

namespace impala {

TEST(FreeListTest, Basic) {
  MemPool pool;
  FreeList list;

  int allocated_size;
  uint8_t* free_list_mem = list.Allocate(FreeList::MinSize(), &allocated_size);
  EXPECT_EQ(NULL, free_list_mem);
  EXPECT_EQ(allocated_size, 0);
  
  uint8_t* mem = pool.Allocate(FreeList::MinSize());
  EXPECT_TRUE(mem != NULL);

  list.Add(mem, FreeList::MinSize()); 
  free_list_mem = list.Allocate(FreeList::MinSize(), &allocated_size);
  EXPECT_EQ(mem, free_list_mem);
  EXPECT_EQ(allocated_size, FreeList::MinSize());

  free_list_mem = list.Allocate(FreeList::MinSize(), &allocated_size);
  EXPECT_EQ(NULL, free_list_mem);
  EXPECT_EQ(allocated_size, 0);

  // Make 3 allocations and add them to the free list.
  // Get them all back from the free list, scribbling to the 
  // returned memory in between.
  // Attempt a 4th allocation from the free list and make sure
  // we get NULL.
  // Repeat with the same memory blocks.
  uint8_t* free_list_mem1, *free_list_mem2, *free_list_mem3;

  mem = pool.Allocate(FreeList::MinSize());
  list.Add(mem, FreeList::MinSize());
  mem = pool.Allocate(FreeList::MinSize());
  list.Add(mem, FreeList::MinSize());
  mem = pool.Allocate(FreeList::MinSize());
  list.Add(mem, FreeList::MinSize());

  free_list_mem1 = list.Allocate(FreeList::MinSize(), &allocated_size);
  EXPECT_TRUE(free_list_mem1 != NULL);
  EXPECT_EQ(allocated_size, FreeList::MinSize());
  bzero(free_list_mem1, FreeList::MinSize());

  free_list_mem2 = list.Allocate(FreeList::MinSize(), &allocated_size);
  EXPECT_TRUE(free_list_mem2 != NULL);
  EXPECT_EQ(allocated_size, FreeList::MinSize());
  bzero(free_list_mem2, FreeList::MinSize());
  
  free_list_mem3 = list.Allocate(FreeList::MinSize(), &allocated_size);
  EXPECT_TRUE(free_list_mem3 != NULL);
  EXPECT_EQ(allocated_size, FreeList::MinSize());
  bzero(free_list_mem3, FreeList::MinSize());

  free_list_mem = list.Allocate(FreeList::MinSize(), &allocated_size);
  EXPECT_EQ(NULL, free_list_mem);
  EXPECT_EQ(allocated_size, 0);

  list.Add(free_list_mem1, FreeList::MinSize());
  list.Add(free_list_mem2, FreeList::MinSize());
  list.Add(free_list_mem3, FreeList::MinSize());

  free_list_mem1 = list.Allocate(FreeList::MinSize(), &allocated_size);
  EXPECT_TRUE(free_list_mem1 != NULL);
  EXPECT_EQ(allocated_size, FreeList::MinSize());
  bzero(free_list_mem1, FreeList::MinSize());

  free_list_mem2 = list.Allocate(FreeList::MinSize(), &allocated_size);
  EXPECT_TRUE(free_list_mem2 != NULL);
  EXPECT_EQ(allocated_size, FreeList::MinSize());
  bzero(free_list_mem2, FreeList::MinSize());
  
  free_list_mem3 = list.Allocate(FreeList::MinSize(), &allocated_size);
  EXPECT_TRUE(free_list_mem3 != NULL);
  EXPECT_EQ(allocated_size, FreeList::MinSize());
  bzero(free_list_mem3, FreeList::MinSize());
  
  free_list_mem = list.Allocate(FreeList::MinSize(), &allocated_size);
  EXPECT_EQ(NULL, free_list_mem);
  EXPECT_EQ(allocated_size, 0);


  // Try some allocations with different sizes
  int size1 = FreeList::MinSize();
  int size2 = FreeList::MinSize() * 2;
  int size4 = FreeList::MinSize() * 4;

  uint8_t* mem1 = pool.Allocate(size1);
  uint8_t* mem2 = pool.Allocate(size2);
  uint8_t* mem4 = pool.Allocate(size4);

  list.Add(mem2, size2);
  free_list_mem = list.Allocate(size4, &allocated_size);
  EXPECT_EQ(NULL, free_list_mem);
  EXPECT_EQ(allocated_size, 0);

  free_list_mem = list.Allocate(size1, &allocated_size);
  EXPECT_TRUE(free_list_mem != NULL);
  EXPECT_EQ(allocated_size, size2);
  bzero(free_list_mem, size1);

  free_list_mem = list.Allocate(size1, &allocated_size);
  EXPECT_EQ(NULL, free_list_mem);
  EXPECT_EQ(allocated_size, 0);

  list.Add(mem2, size2);
  list.Add(mem4, size4);
  list.Add(mem1, size1);

  free_list_mem = list.Allocate(size4, &allocated_size);
  EXPECT_EQ(mem4, free_list_mem);
  EXPECT_EQ(allocated_size, size4);
  bzero(free_list_mem, size4);

  free_list_mem = list.Allocate(size2, &allocated_size);
  EXPECT_EQ(mem2, free_list_mem);
  EXPECT_EQ(allocated_size, size2);
  bzero(free_list_mem, size2);
  
  free_list_mem = list.Allocate(size1, &allocated_size);
  EXPECT_EQ(mem1, free_list_mem);
  EXPECT_EQ(allocated_size, size1);
  bzero(free_list_mem, size1);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

