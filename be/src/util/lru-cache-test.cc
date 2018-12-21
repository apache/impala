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

#include "lru-cache.h"

#include <boost/thread.hpp>
#include <glog/logging.h>

#include "common/logging.h"
#include "testutil/gtest-util.h"
#include "util/test-info.h"

#include "common/names.h"

using boost::thread;
using boost::thread_group;

using namespace impala;

TEST(FifoMultimap, Basic) {
  FifoMultimap<int, int> c(3);
  int result;
  ASSERT_EQ(3, c.capacity());
  c.Put(0, 1);
  c.Put(0, 2);
  ASSERT_EQ(2, c.size());
  ASSERT_FALSE(c.Pop(99, &result));
  c.Pop(0, &result);
  c.Pop(0, &result);
  ASSERT_FALSE(c.Pop(0, &result));
}

TEST(FifoMultimap, Invalid) {
  FifoMultimap<int, int> c(0);
  int result;
  ASSERT_EQ(0, c.capacity());
  c.Put(0, 1);
  ASSERT_EQ(0, c.size());
  ASSERT_FALSE(c.Pop(99, &result));
}

TEST(FifoMultimap, Evict) {
  FifoMultimap<int, int> c(3);
  int result;
  c.Put(0, 1);
  c.Put(1, 2);
  c.Put(2, 3);
  ASSERT_EQ(3, c.size());
  c.Put(3, 4);
  ASSERT_EQ(3, c.size());
  ASSERT_FALSE(c.Pop(0, &result));
  ASSERT_TRUE(c.Pop(1, &result));
  ASSERT_EQ(2, result);
  ASSERT_FALSE(c.Pop(1, &result));
  ASSERT_EQ(2, c.size());
}

TEST(FifoMultimap, Large) {
  FifoMultimap<int, int> c(1000);
  int result;
  for (int i = 0; i < 1000; ++i) {
    c.Put(i, i);
  }
  ASSERT_EQ(1000, c.size());
  for (int i = 0; i < 2000; ++i) {
    int key = rand();
    c.Pop(key % 1000, &result);
    ASSERT_EQ(999, c.size());
    c.Put(key % 1000, i);
    ASSERT_EQ(1000, c.size());
  }
}

static int del_sum = 0;
void TestDeleter(int* v) { del_sum += *v; }

TEST(FifoMultimap, Deleter) {
  FifoMultimap<int, int> c(10, TestDeleter);
  for (int i = 1; i <= 1000; ++i) {
    c.Put(i, i);
  }
  int n = 1000 - c.capacity();
  // All values from 1 to n have been evicted, so the sum of all values must match del_sum
  ASSERT_EQ((n * (n + 1)) / 2, del_sum);
}

static int del_count = 0;
void TestCountDeleter(int* v) { ++del_count; }

void ParallelPut(const int start, const int end, FifoMultimap<int, int>* shelf) {
  int val;
  for (int i = start; i < end; ++i) {
    if (i % 3 == 0) {
      if (shelf->Pop(i, &val)) {
        shelf->Put(i, i);
      }
    }
    shelf->Put(i, i);
  }
}

TEST(FifoMultimap, ParallelEviction) {
  del_sum = 0;
  FifoMultimap<int, int> c(10, TestCountDeleter);

  thread_group group;
  size_t count = 100000;
  size_t thread_count = 10;
  size_t part = count / thread_count;
  for (int i = 0; i < thread_count; ++i) {
    group.add_thread(new thread(ParallelPut, 0, part, &c));
  }
  group.join_all();
  ASSERT_EQ(10, c.size());
  ASSERT_EQ(count - c.capacity(), del_count);
}

TEST(FifoMultimap, PopShouldPopMostRecent) {
  FifoMultimap<int, int> c(3);
  int result;
  c.Put(1, 1);
  c.Put(1, 2);
  c.Put(1, 3);
  ASSERT_TRUE(c.Pop(1, &result));
  ASSERT_EQ(3, result);
  c.Put(1, 3);
  c.Put(1, 4);
  ASSERT_EQ(3, c.size());
  ASSERT_TRUE(c.Pop(1, &result));
  ASSERT_EQ(4, result);
  ASSERT_EQ(2, c.size());
  ASSERT_TRUE(c.Pop(1, &result));
  ASSERT_EQ(3, result);
  ASSERT_EQ(1, c.size());
  ASSERT_TRUE(c.Pop(1, &result));
  ASSERT_EQ(2, result);
  ASSERT_EQ(0, c.size());
}

