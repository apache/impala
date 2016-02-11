// Copyright 2013 Cloudera Inc.
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

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include "common/logging.h"
#include "util/thread-pool.h"

#include "common/names.h"

namespace impala {

const int NUM_THREADS = 5;
int thread_counters[NUM_THREADS];

// Per-thread mutex to ensure visibility of counters after thread pool terminates
mutex thread_mutexes[NUM_THREADS];

void Count(int thread_id, const int& i) {
  lock_guard<mutex> l(thread_mutexes[thread_id]);
  thread_counters[thread_id] += i;
}

TEST(ThreadPoolTest, BasicTest) {
  const int OFFERED_RANGE = 10000;
  for (int i = 0; i < NUM_THREADS; ++i) {
    thread_counters[i] = 0;
  }

  ThreadPool<int> thread_pool("thread-pool", "worker", 5, 250, Count);
  for (int i = 0; i <= OFFERED_RANGE; ++i) {
    ASSERT_TRUE(thread_pool.Offer(i));
  }

  thread_pool.DrainAndShutdown();

  // Check that Offer() after Shutdown() will return false
  ASSERT_FALSE(thread_pool.Offer(-1));
  EXPECT_EQ(0, thread_pool.GetQueueSize());

  int expected_count = (OFFERED_RANGE * (OFFERED_RANGE + 1)) / 2;
  int count = 0;
  for (int i = 0; i < NUM_THREADS; ++i) {
    lock_guard<mutex> l(thread_mutexes[i]);
    LOG(INFO) << "Counter " << i << ": " << thread_counters[i];
    count += thread_counters[i];
  }

  EXPECT_EQ(expected_count, count);
}

}

int main(int argc, char** argv) {
  impala::InitGoogleLoggingSafe(argv[0]);
  impala::InitThreading();
  impala::OsInfo::Init();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
