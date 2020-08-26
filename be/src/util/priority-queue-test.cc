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


#include <glog/logging.h>
#include <gtest/gtest.h>
#include "common/logging.h"
#include "common/compiler-util.h"
#include "util/priority-queue.h"

namespace impala {

class IntComparator {
  public:
    bool ALWAYS_INLINE Less(const int& x, const int& y) const { return x < y; }
};

// Basic Push and Pop operations on total_num_values integers.
void TestInt(int total_num_values) {

  IntComparator int_comparator;
  PriorityQueue<int, IntComparator> queue(int_comparator);
  int values[total_num_values];

  // Setup a total of total_num_values distinct integers in arrayvalues[].
  for (int i = 0; i < total_num_values; i++) {
    values[i] = i;
  }
  // Randomize all elements in values[].
  srand(19);
  for (int i = 0; i < total_num_values - 1; i++) {
    // find an element to swap with the current element at i.
    int index = i + rand() % (total_num_values - i - 1) + 1;
    //
    ASSERT_GE(index, i + 1);
    ASSERT_LE(index, total_num_values - 1);
    // swap elements at i and index
    int t = values[i];
    values[i] = values[index];
    values[index] = t;
  }
  // Push all elements in values[] into the heap.
  for (int i = 0; i < total_num_values; i++) {
    queue.Push(values[i]);
  }
  // Checkout the heap size and allocation capacity.
  ASSERT_EQ(queue.Size(), total_num_values);
  // Pop each element out. Since this is a max-heap, we expect to see
  // elements poped in descending order.
  int v = -1;
  for (int i = 0; i < total_num_values; i++) {
    v = queue.Pop();
    ASSERT_EQ(v, total_num_values - i - 1);
    ASSERT_EQ(queue.Size(), total_num_values - i - 1);
  }
}

TEST(PriorityQueueTest, TestBasic) {
  TestInt(100);
  TestInt(1000);
  TestInt(5000);
}

template class PriorityQueue<int, IntComparator>;
template class PriorityQueueIterator<int, IntComparator>;

}

