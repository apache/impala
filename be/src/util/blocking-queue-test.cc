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

#include "util/blocking-queue.h"

#include "common/names.h"

namespace impala {

TEST(BlockingQueueTest, TestBasic) {
  int32_t i;
  BlockingQueue<int32_t> test_queue(5);
  ASSERT_TRUE(test_queue.BlockingPut(1));
  ASSERT_TRUE(test_queue.BlockingPut(2));
  ASSERT_TRUE(test_queue.BlockingPut(3));
  ASSERT_TRUE(test_queue.BlockingGet(&i));
  ASSERT_EQ(1, i);
  ASSERT_TRUE(test_queue.BlockingGet(&i));
  ASSERT_EQ(2, i);
  ASSERT_TRUE(test_queue.BlockingGet(&i));
  ASSERT_EQ(3, i);
}

TEST(BlockingQueueTest, TestGetFromShutdownQueue) {
  int64_t i;
  BlockingQueue<int64_t> test_queue(2);
  ASSERT_TRUE(test_queue.BlockingPut(123));
  test_queue.Shutdown();
  ASSERT_FALSE(test_queue.BlockingPut(456));
  ASSERT_TRUE(test_queue.BlockingGet(&i));
  ASSERT_EQ(123, i);
  ASSERT_FALSE(test_queue.BlockingGet(&i));
}

class MultiThreadTest {
 public:
  MultiThreadTest()
    : iterations_(10000),
      nthreads_(5),
      queue_(iterations_*nthreads_/10),
      num_inserters_(nthreads_) {
  }

  void InserterThread(int arg) {
    for (int i = 0; i < iterations_; ++i) {
      queue_.BlockingPut(arg);
    }

    {
      lock_guard<mutex> guard(lock_);
      if (--num_inserters_ == 0) {
        queue_.Shutdown();
      }
    }
  }

  void RemoverThread() {
    for (int i = 0; i < iterations_; ++i) {
      int32_t arg;
      bool got = queue_.BlockingGet(&arg);
      if (!got) arg = -1;

      {
        lock_guard<mutex> guard(lock_);
        gotten_[arg] = gotten_[arg] + 1;
      }
    }
  }

  void Run() {
    for (int i = 0; i < nthreads_; ++i) {
      threads_.push_back(shared_ptr<thread>(
          new thread(bind(&MultiThreadTest::InserterThread, this, i))));
      threads_.push_back(shared_ptr<thread>(
          new thread(bind(&MultiThreadTest::RemoverThread, this))));
    }
    // We add an extra thread to ensure that there aren't enough elements in
    // the queue to go around.  This way, we test removal after Shutdown.
    threads_.push_back(shared_ptr<thread>(
            new thread(bind(
              &MultiThreadTest::RemoverThread, this))));
    for (int i = 0; i < threads_.size(); ++i) {
      threads_[i]->join();
    }

    // Let's check to make sure we got what we should have.
    lock_guard<mutex> guard(lock_);
    for (int i = 0; i < nthreads_; ++i) {
      ASSERT_EQ(iterations_, gotten_[i]);
    }
    // And there were nthreads_ * (iterations_ + 1)  elements removed, but only
    // nthreads_ * iterations_ elements added.  So some removers hit the shutdown
    // case.
    ASSERT_EQ(iterations_, gotten_[-1]);
  }

 private:
  typedef vector<shared_ptr<thread> > ThreadVector;

  int iterations_;
  int nthreads_;
  BlockingQueue<int32_t> queue_;
  // Lock for gotten_ and num_inserters_.
  mutex lock_;
  // Map from inserter thread id to number of consumed elements from that id.
  // Ultimately, this should map each thread id to insertions_ elements.
  // Additionally, if the BlockingGet returns false, this increments id=-1.
  map<int32_t, int> gotten_;
  // All inserter and remover threads.
  ThreadVector threads_;
  // Number of inserters which haven't yet finished inserting.
  int num_inserters_;
};

TEST(BlockingQueueTest, TestMultipleThreads) {
  MultiThreadTest test;
  test.Run();
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::OsInfo::Init();
  return RUN_ALL_TESTS();
}
