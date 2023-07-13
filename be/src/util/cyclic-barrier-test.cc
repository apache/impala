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

#include <boost/thread/thread.hpp>

#include "common/atomic.h"
#include "testutil/death-test-util.h"
#include "testutil/gtest-util.h"
#include "util/cyclic-barrier.h"
#include "util/time.h"

#include "common/names.h"

namespace impala {

// Basic test implementation that has a group of 'num_threads' threads join the
// same barrier 'num_iters' times. There is no overlap between the iterations i.e.
// each group of threads terminates before the next group starts.
void BasicTest(int num_threads, int num_iters) {
  CyclicBarrier barrier(num_threads);
  int counter = 0;
  for (int i = 0; i < num_iters; ++i) {
    thread_group threads;
    for (int j = 0; j < num_threads; ++j) {
      threads.add_thread(new thread([&]() {
        // Add some randomness to test so that threads don't always join in predictable
        // order.
        SleepForMs(rand() % 5);
        EXPECT_OK(barrier.Wait([&counter]() {
          ++counter;
          return Status::OK();
        }));
      }));
    }
    threads.join_all();
    // Counter should have been incremented by last arriving threads.
    EXPECT_EQ(i + 1, counter);
  }
}

// Test one iteration of the barrier with varying thread counts.
TEST(CyclicBarrierTest, BasicOneIter) {
  BasicTest(1, 1);
  BasicTest(2, 1);
  BasicTest(4, 1);
  BasicTest(50, 1);
}

// Test many iterations of the barrier.
TEST(CyclicBarrierTest, BasicManyIters) {
  BasicTest(8, 2);
  BasicTest(8, 4);
  BasicTest(8, 20);
  BasicTest(8, 100);
}

// Test where each thread calls Wait() in a tight loop, to exercise cases where calls to
// Wait() from different cycles can overlap. E.g. if there are two threads and a single
// processor, the following timing is possible:
// 1. Thread 1 runs, calls Wait() and blocks.
// 2. Thread 2 runs, calls Wait(), and notifies thread 1.
// 3. Thread 2 continues running, returns from Wait(), then calls Wait() again and blocks.
// 4. Thread 2 wakes up, notices that all threads had joined the barrier, and returns
//    from Wait().
// At step 3, thread 1 and 2 are both in Wait() but are in different cycles, hence the
// overlapping of the cycles.
void OverlapTest(int num_threads, int num_iters) {
  CyclicBarrier barrier(num_threads);
  int counter = 0;
  thread_group threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.add_thread(new thread([&]() {
      for (int j = 0; j < num_iters; ++j) {
        EXPECT_OK(barrier.Wait([&counter]() {
          ++counter;
          return Status::OK();
        }));
      }
    }));
  }
  threads.join_all();
  // Counter should have been incremented by last arriving threads.
  EXPECT_EQ(num_iters, counter);
}

// Test many iterations of the barrier.
TEST(CyclicBarrierTest, Overlap) {
  OverlapTest(2, 100);
  OverlapTest(8, 100);
  OverlapTest(100, 100);
}

// Test that cancellation functions as expected.
TEST(CyclicBarrierTest, Cancellation) {
  const int NUM_THREADS = 8;
  int counter = 0;
  AtomicInt32 waits_complete{0};
  CyclicBarrier barrier(NUM_THREADS);
  thread_group threads;
  // All threads except one should join the barrier, then get cancelled.
  for (int i = 0; i < NUM_THREADS - 1; ++i) {
    threads.add_thread(new thread([&barrier, &waits_complete, &counter, i]() {
      // Add some jitter so that not all threads will be waiting when cancelled.
      if (i % 2 == 0) SleepForMs(rand() % 5);
      Status status = barrier.Wait([&counter]() {
        ++counter;
        return Status::OK();
      });
      EXPECT_FALSE(status.ok());
      EXPECT_EQ(status.code(), TErrorCode::CANCELLED);
      waits_complete.Add(1);
    }));
  }
  SleepForMs(1); // Give other threads a chance to start before cancelling.
  barrier.Cancel(Status::CANCELLED);
  threads.join_all();
  EXPECT_EQ(0, counter) << "The callback should not have run.";
  EXPECT_EQ(NUM_THREADS - 1, waits_complete.Load()) << "Threads should have returned.";

  // Subsequent calls to Wait() return immediately.
  for (int i = 0; i < NUM_THREADS; ++i) {
    Status status = barrier.Wait([&counter]() {
      ++counter;
      return Status::OK();
    });
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), TErrorCode::CANCELLED);
    EXPECT_EQ(0, counter) << "The callback should not have run.";
  }

  // First status is not overwritten by later Cancel() calls.
  barrier.Cancel(Status("Different status"));
  Status status = barrier.Wait([&counter]() {
    ++counter;
    return Status::OK();
  });
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::CANCELLED);

  // Invalid to cancel with Status::OK();
  IMPALA_ASSERT_DEBUG_DEATH(barrier.Cancel(Status::OK()), "");
}

// Test that unregister functions as expected when the last call is an Unregister
TEST(CyclicBarrierTest, UnregisterLast) {
  const int NUM_THREADS = 8;
  const int NUM_OF_UNREGISTERING_THREADS = 6;
  int counter = 0;
  AtomicInt32 waits_complete{0};
  AtomicInt32 unregisters_complete{0};
  CyclicBarrier barrier(NUM_THREADS + NUM_OF_UNREGISTERING_THREADS);
  thread_group threads;
  thread_group unregistering_threads;
  // All threads should join the barrier, waiting.
  for (int i = 0; i < NUM_THREADS; ++i) {
    threads.add_thread(new thread([&barrier, &waits_complete, &counter]() {
      Status status = barrier.Wait([&counter]() {
        ++counter;
        return Status::OK();
      });
      EXPECT_TRUE(status.ok());
      waits_complete.Add(1);
    }));
  }
  SleepForMs(10); // Give other threads a chance to start before unregistering
  EXPECT_EQ(0, counter) << "The callback should not have run.";

  // All threads but one unregisters, the others are still waiting in the barrier
  for (int i = 0; i < NUM_OF_UNREGISTERING_THREADS - 1; ++i) {
    unregistering_threads.add_thread(new thread([&barrier, &unregisters_complete]() {
      barrier.Unregister();
      unregisters_complete.Add(1);
    }));
  }
  unregistering_threads.join_all();

  EXPECT_EQ(0, counter) << "The callback should not have run.";
  EXPECT_EQ(0, waits_complete.Load()) << "Threads should not have returned.";
  EXPECT_EQ(NUM_OF_UNREGISTERING_THREADS - 1, unregisters_complete.Load())
      << "Unregisters should have returned.";

  barrier.Unregister();

  threads.join_all();

  EXPECT_EQ(1, counter) << "Counter should have been incremented by the woken up thread "
                           "after the last Unregister.";
  EXPECT_EQ(NUM_THREADS, waits_complete.Load()) << "Threads should have returned.";
}

// Test that unregister functions as expected when the last call is a Wait.
TEST(CyclicBarrierTest, UnregisterNotLast) {
  const int NUM_THREADS = 8;
  const int NUM_OF_UNREGISTERING_THREADS = 6;
  int counter = 0;
  AtomicInt32 waits_complete{0};
  AtomicInt32 unregisters_complete{0};
  CyclicBarrier barrier(NUM_THREADS + NUM_OF_UNREGISTERING_THREADS);
  thread_group threads;
  thread_group unregistering_threads;
  // All threads but one should join the barrier, waiting.
  for (int i = 0; i < NUM_THREADS - 1; ++i) {
    threads.add_thread(new thread([&barrier, &waits_complete, &counter]() {
      Status status = barrier.Wait([&counter]() {
        ++counter;
        return Status::OK();
      });
      EXPECT_TRUE(status.ok());
      waits_complete.Add(1);
    }));
  }
  SleepForMs(10); // Give other threads a chance to start before unregistering
  EXPECT_EQ(0, counter) << "The callback should not have run.";

  // All threads unregister, the others are still waiting in the barrier for the last one
  for (int i = 0; i < NUM_OF_UNREGISTERING_THREADS; ++i) {
    unregistering_threads.add_thread(new thread([&barrier, &unregisters_complete]() {
      barrier.Unregister();
      unregisters_complete.Add(1);
    }));
  }
  unregistering_threads.join_all();

  EXPECT_EQ(0, counter) << "The callback should not have run.";
  EXPECT_EQ(0, waits_complete.Load()) << "Threads should not have returned.";
  EXPECT_EQ(NUM_OF_UNREGISTERING_THREADS, unregisters_complete.Load())
      << "Unregisters should have returned.";

  threads.add_thread(new thread([&barrier, &waits_complete, &counter]() {
    Status status = barrier.Wait([&counter]() {
      ++counter;
      return Status::OK();
    });
    EXPECT_TRUE(status.ok());
    waits_complete.Add(1);
  }));

  threads.join_all();

  EXPECT_EQ(1, counter) << "Counter should have been incremented by the last thread.";
  EXPECT_EQ(NUM_THREADS, waits_complete.Load()) << "Threads should have returned.";
}

// Passing an empty/null function to Wait() is not supported.
TEST(CyclicBarrierTest, NullFunction) {
  CyclicBarrier barrier(1);
  typedef Status (*fn_ptr_t)();
  IMPALA_ASSERT_DEBUG_DEATH(barrier.Wait(static_cast<fn_ptr_t>(nullptr)), "");
}
} // namespace impala
