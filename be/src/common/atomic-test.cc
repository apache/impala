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

#include <string>
#include <boost/thread/thread.hpp>

#include "common/atomic.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

using namespace internal; // Testing AtomicInt<> directly.

// Simple test to make sure there is no obvious error in the operations.  This is not
// intended to test the thread safety.
template<typename T>
static void TestBasic() {
  AtomicInt<T> i1;
  EXPECT_EQ(i1.Load(), 0);
  i1.Store(10);
  EXPECT_EQ(i1.Load(), 10);
  i1.Add(5);
  EXPECT_EQ(i1.Load(), 15);
  i1.Add(-25);
  EXPECT_EQ(i1.Load(), -10);
  i1.Store(100);
  EXPECT_EQ(i1.Load(), 100);

  bool success = i1.CompareAndSwap(100, 50);
  EXPECT_EQ(i1.Load(), 50);
  EXPECT_EQ(success, true);
  success = i1.CompareAndSwap(50, 100);
  EXPECT_EQ(i1.Load(), 100);
  EXPECT_EQ(success, true);

  success = i1.CompareAndSwap(-200, 50);
  EXPECT_EQ(i1.Load(), 100);
  EXPECT_EQ(success, false);
  success = i1.CompareAndSwap(50, 200);
  EXPECT_EQ(i1.Load(), 100);
  EXPECT_EQ(success, false);
}

TEST(AtomicTest, Basic) {
  TestBasic<int32_t>();
}

TEST(AtomicTest, Basic64) {
  TestBasic<int64_t>();
}

// Basic multi-threaded testing

template<typename T>
static void IncrementThread(int64_t id, int64_t n, AtomicInt<T>* ai) {
  for (int64_t i = 0; i < n * id; ++i) {
    ai->Add(1);
  }
}

template<typename T>
static void DecrementThread(int64_t id, int64_t n, AtomicInt<T>* ai) {
  for (int64_t i = 0; i < n * id; ++i) {
    ai->Add(-1);
  }
}

template<typename T>
static void TestMultipleThreadsIncDec() {
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
      AtomicInt<T> ai(0);
      for (int i = 0; i < *thrit; ++i) {
        increments.add_thread(new thread(IncrementThread<T>, i, *opit, &ai));
        decrements.add_thread(new thread(DecrementThread<T>, i, *opit, &ai));
      }
      increments.join_all();
      decrements.join_all();
      EXPECT_EQ(ai.Load(), 0);
    }
  }
}

TEST(AtomicTest, MultipleThreadsIncDec) {
  TestMultipleThreadsIncDec<int32_t>();
}

TEST(AtomicTest, MultipleThreadsIncDec64) {
  TestMultipleThreadsIncDec<int64_t>();
}

template<typename T>
static void CASIncrementThread(int64_t id, int64_t n, AtomicInt<T>* ai) {
  T oldval = 0;
  T newval = 0;
  bool success = false;
  for (int64_t i = 0; i < n * id; ++i) {
    success = false;
    while (!success) {
      oldval = ai->Load();
      newval = oldval + 1;
      success = ai->CompareAndSwap(oldval, newval);
    }
  }
}

template<typename T>
static void CASDecrementThread(int64_t id, int64_t n, AtomicInt<T>* ai) {
  T oldval = 0;
  T newval = 0;
  bool success = false;
  for (int64_t i = 0; i < n * id; ++i) {
    success = false;
    while (!success) {
      oldval = ai->Load();
      newval = oldval - 1;
      success = ai->CompareAndSwap(oldval, newval);
    }
  }
}

template<typename T>
static void TestMultipleThreadsCASIncDec() {
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
      AtomicInt<T> ai(0);
      for (int i = 0; i < *thrit; ++i) {
        increments.add_thread(new thread(CASIncrementThread<T>, i, *opit, &ai));
        decrements.add_thread(new thread(CASDecrementThread<T>, i, *opit, &ai));
      }
      increments.join_all();
      decrements.join_all();
      EXPECT_EQ(ai.Load(), 0);
    }
  }
}

TEST(AtomicTest, MultipleThreadsCASIncDec) {
  TestMultipleThreadsCASIncDec<int32_t>();
}

TEST(AtomicTest, MultipleThreadsCASIncDec64) {
  TestMultipleThreadsCASIncDec<int64_t>();
}

template<typename T>
static void AcquireReleaseThreadA(T id, T other_id, int iters, AtomicInt<T>* control,
    T* payload) {
  const int DELAY = 100;
  for (int it = 0; it < iters; ++it) {
    // Phase 1
    // (A-1) This store is not allowed to reorder with the below "store release".
    *payload = it;
    control->Store(other_id);

    // Phase 2
    while (control->Load() != id) ;
    // (A-2) This store is not allowed to reorder with the above "load acquire".
    *payload = -it;

    // Give thread B a chance to get started on Phase 1.
    for (int i = 0; i < DELAY; ++i) {
      AtomicUtil::CompilerBarrier();
    }
  }
}

template<typename T>
static void AcquireReleaseThreadB(T id, T other_id, int iters, AtomicInt<T>* control,
    T* payload) {
  const int DELAY = 100;
  for (int it = 0; it < iters; ++it) {
    T p;
    // Phase 1
    // Wait until ThreadA signaled that payload was updated.
    while (control->Load() != id);
    // (B-1) This load is not allowed to reorder with the above "load acquire".
    p = *payload;
    // Verify ordering for both (A-1) and (B-1).
    EXPECT_EQ(it, p);

    // Phase 2
    // Payload should not change until we give ThreadA the go-ahead.
    for (int i = 0; i < DELAY; ++i) {
      AtomicUtil::CompilerBarrier();
      // (B-2) This load is not allowed to reorder with the below "store release".
      p = *payload;
    }
    control->Store(other_id);
    // Verify ordering for both (A-2) and (B-2).
    EXPECT_EQ(it, p);
  }
}

// Test "acquire" and "release" memory ordering semantics. There are two threads, A and
// B, and each execute phase 1 and phase 2 in lockstep to verify the various
// combinations of memory accesses.
template<typename T>
static void TestAcquireReleaseLoadStore() {
  const int ITERS = 1000000;
  AtomicInt<T> control(-1);
  T payload = -1;
  thread t_a(AcquireReleaseThreadA<T>, 0, 1, ITERS, &control, &payload);
  thread t_b(AcquireReleaseThreadB<T>, 1, 0, ITERS, &control, &payload);
  t_a.join();
  t_b.join();
}

TEST(AtomicTest, MultipleTreadsAcquireReleaseLoadStoreInt) {
  TestAcquireReleaseLoadStore<int32_t>();
}

TEST(AtomicTest, MultipleTreadsAcquireReleaseLoadStoreInt64) {
  TestAcquireReleaseLoadStore<int64_t>();
}

}
