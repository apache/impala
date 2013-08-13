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

#include "buffer-pool.h"
#include "disk-structs.h"
#include "disk-writer.h"

using namespace boost;
using namespace std;

namespace impala {

TEST(BufferPoolTest, TestBasic) {
  BufferPool pool(4, 1024);
  BufferPool::BufferDescriptor* buf = pool.GetBuffer();
  buf->buffer[0] = 'Q';
  BufferPool::BufferDescriptor* buf2 = pool.GetBuffer();
  ASSERT_NE(buf->buffer[0], buf2->buffer[0]);
  pool.GetBuffer();
  pool.GetBuffer();
  buf->Return();
  BufferPool::BufferDescriptor* buf3 = pool.GetBuffer();
  ASSERT_EQ(buf->buffer[0], buf3->buffer[0]);
}

void GetBufferAndTest(BufferPool* pool) {
  BufferPool::BufferDescriptor* buf = pool->GetBuffer();
  ASSERT_EQ('Q', buf->buffer[0]);
}

TEST(BufferPoolTest, TestThreading) {
  BufferPool pool(4, 1024);
  BufferPool::BufferDescriptor* buf = pool.GetBuffer();
  pool.GetBuffer();
  pool.GetBuffer();
  pool.GetBuffer();
  thread* t = new thread(&GetBufferAndTest, &pool);
  buf->buffer[0] = 'Q';
  buf->Return();
  t->join();
  delete t;
}

// Test the behavior of the BufferPool's reservation system.
class ReservationTest {
 public:
  ReservationTest()
    : pool_(4, 1024),
      num_buffers_freed_(0),
      expect_full_(false),
      buffer_to_free_(NULL) {
  }

  void Run() {
    function<bool ()> callback(bind(&ReservationTest::NoFreeBufferCallback, this));
    pool_.SetTryFreeBufferCallback(callback);

    vector<BufferPool::BufferDescriptor*> no_context_buffers;
    AddBuffer(no_context_buffers, NULL);
    AddBuffer(no_context_buffers, NULL);
    AddBuffer(no_context_buffers, NULL);
    AddBuffer(no_context_buffers, NULL);
    AddBufferExpectFull(no_context_buffers, NULL, PopBack(no_context_buffers));

    // Reserve will cause the Pool to free up 1 buffer for context1.
    BufferPool::ReservationContext* context1;
    ReserveExpectFull(&context1, 1, PopBack(no_context_buffers));
    vector<BufferPool::BufferDescriptor*> context1_buffers;
    // Our freed buffers look like this:
    // <no_context: 3, context1: 0/1> : <free: 1, reserved: 1>
    AddBuffer(context1_buffers, context1);
    PopBack(context1_buffers)->Return();
    // <no_context: 3, context1: 0/1> : <free: 1, reserved: 1>
    // no_context can't obtain the reserved buffer
    AddBufferExpectFull(no_context_buffers, NULL, PopBack(no_context_buffers));
    PopBack(no_context_buffers)->Return();
    AddBuffer(no_context_buffers, NULL);
    // <no_context: 3, context1: 0/1> : <free: 1, reserved: 1>
    AddBufferExpectFull(no_context_buffers, NULL, PopBack(no_context_buffers));
    AddBuffer(context1_buffers, context1);
    // <no_context: 3, context1: 1/1> : <free: 0, reserved: 0>
    PopBack(no_context_buffers)->Return();
    AddBuffer(no_context_buffers, NULL);
    PopBack(no_context_buffers)->Return();
    AddBuffer(context1_buffers, context1);
    // <no_context: 2, context1: 2/1> : <free: 0, reserved: 0>

    // Reserve will cause the Pool to free up 2 buffers for context2.
    BufferPool::ReservationContext* context2;
    PopBack(no_context_buffers)->Return();
    ReserveExpectFull(&context2, 2, PopBack(no_context_buffers));
    vector<BufferPool::BufferDescriptor*> context2_buffers;
    // <no_context: 0, context1: 2/1, context2: 0/2> : <free: 2, reserved: 2>
    AddBufferExpectFull(context1_buffers, context1, PopBack(context1_buffers));
    AddBufferExpectFull(no_context_buffers, NULL, PopBack(context1_buffers));
    // <no_context: 1, context1: 1/1, context2: 0/2> : <free: 2, reserved: 2>
    PopBack(context1_buffers)->Return();
    PopBack(no_context_buffers)->Return();
    // <no_context: 0, context1: 0/1, context2: 0/2> : <free: 4, reserved: 3>
    AddBuffer(context2_buffers, context2);
    AddBuffer(context1_buffers, context1);
    AddBuffer(no_context_buffers, NULL);
    AddBufferExpectFull(context1_buffers, context1, PopBack(context1_buffers));
    AddBuffer(context2_buffers, context2);
    // <no_context: 1, context1: 1/1, context2: 2/2> : <free: 0, reserved: 0>
    PopBack(context1_buffers)->Return();
    // <no_context: 1, context1: 0/1, context2: 2/2> : <free: 1, reserved: 1>
    AddBufferExpectFull(context2_buffers, context2, PopBack(no_context_buffers));
    // <no_context: 0, context1: 0/1, context2: 3/2> : <free: 1, reserved: 1>

    // Cancel reservation
    pool_.CancelReservation(context1);
    // <no_context: 0, context1: 0, context2: 3/2> : <free: 1, reserved: 0>
    AddBuffer(no_context_buffers, NULL);
    // <no_context: 1, context1: 0, context2: 3/2> : <free: 0, reserved: 0>

    pool_.CancelReservation(context2);
    PopBack(context2_buffers)->Return();
    AddBuffer(no_context_buffers, NULL);
    // <no_context: 2, context1: 0, context2: 2> : <free: 0, reserved: 0>
    PopBack(context2_buffers)->Return();
    AddBuffer(context1_buffers, NULL);
    // <no_context: 2, context1: 1, context2: 1> : <free: 0, reserved: 0>
    AddBufferExpectFull(no_context_buffers, NULL, PopBack(context2_buffers));
    AddBufferExpectFull(no_context_buffers, NULL, PopBack(context1_buffers));
    // <no_context: 4, context1: 0, context2: 0> : <free: 0, reserved: 0>

    DCHECK(context1_buffers.empty());
    DCHECK(context2_buffers.empty());
    DCHECK_EQ(no_context_buffers.size(), 4);
    AddBufferExpectFull(no_context_buffers, NULL, PopBack(no_context_buffers));
    // <no_context: 4, context1: 0, context2: 0> : <free: 0, reserved: 0>

    PopBack(no_context_buffers)->Return();
    PopBack(no_context_buffers)->Return();
    PopBack(no_context_buffers)->Return();
    PopBack(no_context_buffers)->Return();
    // <no_context: 0, context1: 0, context2: 0> : <free: 4, reserved: 0>
  }

  // Called by the BufferPool when it runs out of freed buffers.
  // If we fail to return a buffer, we would deadlock.
  bool NoFreeBufferCallback() {
    DCHECK(expect_full_);
    DCHECK(buffer_to_free_ != NULL);
    ++num_buffers_freed_;
    buffer_to_free_->Return();
    buffer_to_free_ = NULL;
    return true;
  }

  // Tries to get a Buffer from the BufferPool and appends it to the given vector.
  void AddBuffer(vector<BufferPool::BufferDescriptor*>& buffers,
      BufferPool::ReservationContext* context) {
    expect_full_ = false;
    int num_free_before = num_buffers_freed_;
    buffers.push_back(pool_.GetBuffer(context));
    ASSERT_EQ(num_buffers_freed_, num_free_before);
  }

  // Tries to get a Buffer from the BufferPool, expects the BufferPool to be full,
  // and so releases the given Buffer to avoid deadlock.
  void AddBufferExpectFull(vector<BufferPool::BufferDescriptor*>& buffers,
      BufferPool::ReservationContext* context,
      BufferPool::BufferDescriptor* buffer_to_free) {
    DCHECK(buffer_to_free != NULL);
    expect_full_ = true;
    buffer_to_free_ = buffer_to_free;
    int num_free_before = num_buffers_freed_;
    buffers.push_back(pool_.GetBuffer(context));
    ASSERT_EQ(num_buffers_freed_, num_free_before + 1);
    expect_full_ = false;
  }

  // Initiates a reservation that requires a single buffer to be freed.
  void ReserveExpectFull(BufferPool::ReservationContext** context,
      int num_buffers, BufferPool::BufferDescriptor* buffer_to_free) {
    expect_full_ = true;
    buffer_to_free_ = buffer_to_free;
    int num_free_before = num_buffers_freed_;
    pool_.Reserve(num_buffers, context);
    ASSERT_EQ(num_buffers_freed_, num_free_before + 1);
    expect_full_ = false;
  }

  BufferPool::BufferDescriptor* PopBack(vector<BufferPool::BufferDescriptor*>& buffers) {
    DCHECK(!buffers.empty());
    BufferPool::BufferDescriptor* first = buffers.back();
    buffers.erase(buffers.end() - 1);
    return first;
  }

 private:
  BufferPool pool_;
  int num_buffers_freed_;
  bool expect_full_;
  BufferPool::BufferDescriptor* buffer_to_free_;
};

TEST(BufferPoolTest, TestReservation) {
  ReservationTest test;
  test.Run();
}

// Relies on the fact that the buffers are recycled, so data persists.
class MultiThreadTest {
 public:
  MultiThreadTest()
    : iterations_(100),
      nthreads_(20),
      nbuffers_(5),
      pool_(nbuffers_, 1024) {
  }

  void WorkerThread(int thread_num) {
    for (int i = 0; i < iterations_; ++i) {
      BufferPool::BufferDescriptor* buf = pool_.GetBuffer();
      int32_t& cur_val = *(thread_num + reinterpret_cast<int32_t*>(buf->buffer));
      ++cur_val;
      buf->Return();
    }
  }

  void Run() {
    // Zero out the buffers.
    vector<BufferPool::BufferDescriptor*> buffers;
    for (int i = 0; i < nbuffers_; ++i) {
      BufferPool::BufferDescriptor* buf = pool_.GetBuffer();
      memset(buf->buffer, 0, nthreads_ * sizeof(int32_t));
      buffers.push_back(buf);

      buf->buffer[nthreads_ * sizeof(int32_t)] = '\n';
    }
    for (int i = 0; i < nbuffers_; ++i) {
      buffers[i]->Return();
    }

    for (int i = 0; i < nthreads_; ++i) {
      threads_.push_back(shared_ptr<thread>(
          new thread(bind(&MultiThreadTest::WorkerThread, this, i))));
    }

    for (int i = 0; i < threads_.size(); ++i) {
      threads_[i]->join();
    }

    // Ensure all worker threads incremented their value 'iterations_' times
    vector<int> thread_sums(nthreads_, 0);
    for (int i = 0; i < nbuffers_; ++i) {
      BufferPool::BufferDescriptor* buf = pool_.GetBuffer();
      for (int j = 0; j < nthreads_; ++j) {
        int32_t val = *(j + reinterpret_cast<int32_t*>(buf->buffer));
        thread_sums[j] += val;
      }
    }

    for (int i = 0; i < nthreads_; ++i) {
      ASSERT_EQ(iterations_, thread_sums[i]);
    }
  }

 private:
  typedef vector<shared_ptr<thread> > ThreadVector;

  int iterations_;
  int nthreads_;
  int nbuffers_;
  BufferPool pool_;
  // Lock for gotten_ and num_inserters_.
  mutex lock_;
  // Map from inserter thread id to number of consumed elements from that id.
  // Ultimately, this should map each thread id to insertions_ elements.
  // Additionally, if the BlockingGet returns false, this increments id=-1.
  map<int32_t, int> gotten_;
  // All inserter and remover threads.
  ThreadVector threads_;
};

TEST(BlockingQueueTest, TestMultipleThreads) {
  MultiThreadTest test;
  test.Run();
}

}
