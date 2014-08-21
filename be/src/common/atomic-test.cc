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
#include <boost/thread.hpp>

#include "common/atomic.h"

using namespace boost;
using namespace std;

namespace impala {

// Simple test to make sure there is no obvious error in the usage of the
// __sync* operations.  This is not intended to test the thread safety.
TEST(AtomicTest, Basic) {
  AtomicInt<int> i1;
  EXPECT_EQ(i1, 0);
  i1 = 10;
  EXPECT_EQ(i1, 10);
  i1 += 5;
  EXPECT_EQ(i1, 15);
  i1 -= 25;
  EXPECT_EQ(i1, -10);
  ++i1;
  EXPECT_EQ(i1, -9);
  --i1;
  EXPECT_EQ(i1, -10);
  i1 = 100;
  EXPECT_EQ(i1, 100);

  i1.UpdateMax(50);
  EXPECT_EQ(i1, 100);
  i1.UpdateMax(150);
  EXPECT_EQ(i1, 150);

  i1.UpdateMin(200);
  EXPECT_EQ(i1, 150);
  i1.UpdateMin(-200);
  EXPECT_EQ(i1, -200);

  bool success = i1.CompareAndSwap(-200, 50);
  EXPECT_EQ(i1, 50);
  EXPECT_EQ(success, true);
  success = i1.CompareAndSwap(50, 100);
  EXPECT_EQ(i1, 100);
  EXPECT_EQ(success, true);

  success = i1.CompareAndSwap(-200, 50);
  EXPECT_EQ(i1, 100);
  EXPECT_EQ(success, false);
  success = i1.CompareAndSwap(50, 200);
  EXPECT_EQ(i1, 100);
  EXPECT_EQ(success, false);

  int retval = i1.CompareAndSwapVal(100, 200);
  EXPECT_EQ(i1, 200);
  EXPECT_EQ(retval, 100);
  retval = i1.CompareAndSwapVal(200, 250);
  EXPECT_EQ(i1, 250);
  EXPECT_EQ(retval, 200);

  retval = i1.CompareAndSwapVal(100, 200);
  EXPECT_EQ(i1, 250);
  EXPECT_EQ(retval, 250);
  retval = i1.CompareAndSwapVal(-200, 50);
  EXPECT_EQ(i1, 250);
  EXPECT_EQ(retval, 250);

  retval = i1.Swap(300);
  EXPECT_EQ(i1, 300);
  EXPECT_EQ(retval, 250);
  retval = i1.Swap(350);
  EXPECT_EQ(i1, 350);
  EXPECT_EQ(retval, 300);
}

TEST(AtomicTest, TestAndSet) {
  AtomicInt<int> i1;
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(i + 1, i1.UpdateAndFetch(1));
  }

  i1 = 0;

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(i, i1.FetchAndUpdate(1));
  }
}

// Basic multi-threaded testing
typedef function<void (int64_t, int64_t , AtomicInt<int>*)> Fn;

void IncrementThread(int64_t id, int64_t n, AtomicInt<int>* ai) {
  for (int64_t i = 0; i < n * id; ++i) {
    ++*ai;
  }
}

void DecrementThread(int64_t id, int64_t n, AtomicInt<int>* ai) {
  for (int64_t i = 0; i < n * id; ++i) {
    --*ai;
  }
}

TEST(AtomicTest, MultipleThreadsIncDec) {
  thread_group increments, decrements;
  vector<int> ops;
  ops.push_back(1000);
  ops.push_back(10000);
  vector<int> num_threads;
  num_threads.push_back(4);
  num_threads.push_back(8);
  num_threads.push_back(16);

  for (vector<int>::iterator thrit = num_threads.begin(); thrit != num_threads.end();
       ++thrit) {
    for (vector<int>::iterator opit = ops.begin(); opit != ops.end(); ++opit) {
      AtomicInt<int> ai = 0;
      for (int i = 0; i < *thrit; ++i) {
        increments.add_thread( new thread(IncrementThread, i, *opit, &ai));
        decrements.add_thread( new thread(DecrementThread, i, *opit, &ai));
      }
      increments.join_all();
      decrements.join_all();
      EXPECT_EQ(ai, 0);
    }
  }
}

void CASIncrementThread(int64_t id, int64_t n, AtomicInt<int>* ai) {
  int oldval = 0;
  int newval = 0;
  bool success = false;
  for (int64_t i = 0; i < n * id; ++i) {
    success = false;
    while ( !success ) {
      oldval = ai->Read();
      newval = oldval + 1;
      success = ai->CompareAndSwap(oldval, newval);
    }
  }
}

void CASDecrementThread(int64_t id, int64_t n, AtomicInt<int>* ai) {
  int oldval = 0;
  int newval = 0;
  bool success = false;
  for (int64_t i = 0; i < n * id; ++i) {
    success = false;
    while ( !success ) {
      oldval = ai->Read();
      newval = oldval - 1;
      success = ai->CompareAndSwap(oldval, newval);
    }
  }
}

TEST(AtomicTest, MultipleThreadsCASIncDec) {
  thread_group increments, decrements;
  vector<int> ops;
  ops.push_back(10);
  ops.push_back(10000);
  vector<int> num_threads;
  num_threads.push_back(4);
  num_threads.push_back(8);
  num_threads.push_back(16);

  for (vector<int>::iterator thrit = num_threads.begin(); thrit != num_threads.end();
       ++thrit) {
    for (vector<int>::iterator opit = ops.begin(); opit != ops.end(); ++opit) {
      AtomicInt<int> ai = 0;
      for (int i = 0; i < *thrit; ++i) {
        increments.add_thread( new thread(CASIncrementThread, i, *opit, &ai));
        decrements.add_thread( new thread(CASDecrementThread, i, *opit, &ai));
      }
      increments.join_all();
      decrements.join_all();
      EXPECT_EQ(ai, 0);
    }
  }
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
