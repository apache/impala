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
#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/macros.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"

namespace kudu {
namespace rpc {

// Return values for ServiceQueue::Put()
enum QueueStatus {
  QUEUE_SUCCESS = 0,
  QUEUE_SHUTDOWN = 1,
  QUEUE_FULL = 2
};

// Blocking queue used for passing inbound RPC calls to the service handler pool.
// Calls are dequeued in 'earliest-deadline first' order. The queue also maintains a
// bounded number of calls. If the queue is about to overflow, then one call
// with the deadline later than the deadline of the call being added is evicted.
//
// In order to improve concurrent throughput, this class uses a LIFO design:
// Each consumer thread has its own lock and condition variable. If a
// consumer arrives and there is no work available in the queue, it will not
// wait on the queue lock, but rather push its own 'ConsumerState' object
// to the 'waiting_consumers_' stack. When work arrives, if there are waiting
// consumers, the top consumer is popped from the stack and woken up.
//
// This design has a few advantages over the basic BlockingQueue:
// - the worker who was most recently busy is the one which will be selected for
//   new work. This gives an opportunity for the worker to be scheduled again
//   without going to sleep, and also keeps CPU cache and allocator caches hot.
// - in the common case that there are enough workers to fully service the incoming
//   work rate, the queue implementation itself is never used. Thus, we can
//   have a priority queue without paying extra for it in the common case.
//
// NOTE: because of the use of thread-local consumer records, once a consumer
// thread accesses one LifoServiceQueue, it becomes "bound" to that queue and
// must never access any other instance.
class LifoServiceQueue final {
 public:
  explicit LifoServiceQueue(size_t max_size);
  ~LifoServiceQueue();

  size_t max_size() const {
    return max_queue_size_;
  }

  // Get an element from the queue. Returns false if the queue is shut down.
  bool BlockingGet(std::unique_ptr<InboundCall>* out);

  // Add a new call to the queue.
  // Returns:
  // - QUEUE_SHUTDOWN if Shutdown() has already been called.
  // - QUEUE_FULL if the queue is full and 'call' has a later deadline than most
  //   of the RPCs already in the queue.
  // - QUEUE_SUCCESS if 'call' was enqueued.
  //
  // In the case of a 'QUEUE_SUCCESS' response, the new element may have bumped
  // another call out of the queue. In that case, *evicted will be set to the
  // call that was bumped.
  QueueStatus Put(InboundCall* call, InboundCall** evicted);

  // Shut down the queue.
  // When a blocking queue is shut down, no more elements can be added to it,
  // and Put() will return QUEUE_SHUTDOWN.
  // Existing elements will drain out of it, and then BlockingGet will start
  // returning false.
  void Shutdown();

  std::string ToString() const;

 private:
  FRIEND_TEST(TestServiceQueue, LifoServiceQueuePerf);

  // Comparison function which orders calls by their deadlines.
  static bool DeadlineGreater(const InboundCall* a,
                              const InboundCall* b) {
    auto time_a = a->GetClientDeadline();
    auto time_b = b->GetClientDeadline();
    if (time_a == time_b) {
      // If two calls have the same deadline (most likely because neither one specified
      // one) then we should order them by arrival order.
      time_a = a->GetTimeReceived();
      time_b = b->GetTimeReceived();
    }
    return time_a > time_b;
  }

  // Struct functor wrapper for DeadlineGreater.
  static const struct DeadlineGreaterStruct {
    bool operator()(const InboundCall* a, const InboundCall* b) const noexcept {
      return DeadlineGreater(a, b);
    }
  } kMinHeapCompare;

  // The thread-local record corresponding to a single consumer thread.
  // Threads push this record onto the waiting_consumers_ stack when
  // they are awaiting work. Producers pop the top waiting consumer and
  // post work using Post().
  class ConsumerState final {
   public:
    explicit ConsumerState(LifoServiceQueue* queue) :
        cond_(&lock_),
        call_(nullptr),
        should_wake_(false),
        bound_queue_(queue) {
    }

    void Post(InboundCall* call) {
      std::lock_guard l(lock_);
      DCHECK(!call_);
      call_ = call;
      should_wake_ = true;
      cond_.Signal();
    }

    InboundCall* Wait() {
      std::lock_guard l(lock_);
      while (!should_wake_) {
        cond_.Wait();
      }
      should_wake_ = false;
      InboundCall* ret = call_;
      call_ = nullptr;
      return ret;
    }

    void DCheckBoundInstance(LifoServiceQueue* q) {
      DCHECK_EQ(q, bound_queue_);
    }

   private:
    Mutex lock_;
    ConditionVariable cond_;
    InboundCall* call_;
    bool should_wake_;

    // For the purpose of assertions, tracks the LifoServiceQueue instance that
    // this consumer is reading from.
    LifoServiceQueue* bound_queue_;
  };

  // Return an estimate of the current queue length.
  size_t estimated_queue_length() const {
    ANNOTATE_IGNORE_READS_BEGIN();
    // Size of a vector is a simple field access so this is safe.
    auto ret = queue_.size();
    ANNOTATE_IGNORE_READS_END();
    return ret;
  }

  // Return an estimate of the number of idle threads currently awaiting work.
  size_t estimated_idle_worker_count() const {
    ANNOTATE_IGNORE_READS_BEGIN();
    // Size of a vector is a simple field access so this is safe.
    auto ret = waiting_consumers_.size();
    ANNOTATE_IGNORE_READS_END();
    return ret;
  }

  static thread_local ConsumerState* tl_consumer_;

  const size_t max_queue_size_;
  mutable simple_spinlock lock_;
  bool shutdown_;

  // Stack of consumer threads which are currently waiting for work.
  std::vector<ConsumerState*> waiting_consumers_;

  // The container backing the queue. This is operated as a min priority queue.
  // Items are only added into the queue when there aren't any consumers
  // available for a "direct hand-off".
  std::vector<InboundCall*> queue_;

  // The total set of consumers who have ever accessed this queue.
  // This container is necessary to maintain proper lifecycle and ownership
  // of the corresponding ConsumerState objects while their raw pointers
  // are used elsewhere in this class (e.g., in 'waiting_consumers_').
  std::vector<std::unique_ptr<ConsumerState>> consumers_;

  DISALLOW_COPY_AND_ASSIGN(LifoServiceQueue);
};

} // namespace rpc
} // namespace kudu
