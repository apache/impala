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

#include "kudu/rpc/service_queue.h"

#include <algorithm>
#include <mutex>
#include <ostream>

#include "kudu/gutil/port.h"

using std::lock_guard;
using std::pop_heap;
using std::push_heap;
using std::string;
using std::unique_lock;
using std::unique_ptr;

namespace kudu {
namespace rpc {

thread_local LifoServiceQueue::ConsumerState* LifoServiceQueue::tl_consumer_ = nullptr;

LifoServiceQueue::LifoServiceQueue(size_t max_size)
    : max_queue_size_(max_size),
      shutdown_(false) {
  DCHECK_GT(max_queue_size_, 0);
  // Reserving all the required memory upfront, no re-allocations are necessary
  // during the lifecycle of the instance of this class.
  queue_.reserve(max_queue_size_);
}

LifoServiceQueue::~LifoServiceQueue() {
  DCHECK(queue_.empty())
      << "ServiceQueue holds bare pointers at destruction time";
}

bool LifoServiceQueue::BlockingGet(unique_ptr<InboundCall>* out) {
  auto* consumer = tl_consumer_;
  if (PREDICT_FALSE(!consumer)) {
    consumer = tl_consumer_ = new ConsumerState(this);
    lock_guard l(lock_);
    consumers_.emplace_back(consumer);
  }

  while (true) {
    {
      lock_guard l(lock_);
      if (!queue_.empty()) {
        out->reset(queue_.front());
        pop_heap(queue_.begin(), queue_.end(), kMinHeapCompare);
        queue_.pop_back();
        return true;
      }
      if (PREDICT_FALSE(shutdown_)) {
        return false;
      }
#if DCHECK_IS_ON()
      consumer->DCheckBoundInstance(this);
#endif
      waiting_consumers_.push_back(consumer);
    }
    InboundCall* call = consumer->Wait();
    if (call != nullptr) {
      out->reset(call);
      return true;
    }
    // if call == nullptr, this means we are shutting down the queue.
    // Loop back around and re-check 'shutdown_'.
  }
}

QueueStatus LifoServiceQueue::Put(InboundCall* call, InboundCall** evicted) {
  unique_lock l(lock_);
  if (PREDICT_FALSE(shutdown_)) {
    return QUEUE_SHUTDOWN;
  }

  DCHECK(waiting_consumers_.empty() || queue_.empty());

  // fast path
  if (queue_.empty() && !waiting_consumers_.empty()) {
    auto* consumer = waiting_consumers_.back();
    waiting_consumers_.pop_back();
    // Notifying the condition and waking up the consumer thread takes time,
    // so put it out of spinlock scope.
    l.unlock();
    consumer->Post(call);
    return QUEUE_SUCCESS;
  }

  if  (queue_.size() < max_queue_size_) {
    queue_.push_back(call);
    push_heap(queue_.begin(), queue_.end(), kMinHeapCompare);
    return QUEUE_SUCCESS;
  }
  DCHECK_EQ(queue_.size(), max_queue_size_);

  // If the deadline of the new call is not earlier than the deadline of the
  // call in the very end of the array backing the min heap, reject it with
  // QUEUE_FULL status.
  auto* back = queue_.back();
  if (!DeadlineGreater(back, call)) {
    return QUEUE_FULL;
  }

  // Otherwise, replace the call in the very end of the container with the new
  // one. The replaced call isn't guaranteed to be have the latest deadline
  // among all the elements in the queue because it's a binary heap,
  // not a sorted sequence. However, usually it's the latest one because most
  // of the times the elements are added into the queue timestamped by their
  // arrival time, and it increases monotonically. Removing the last element
  // of the array representing a binary heap leaves the rest of the array still
  // a binary heap, of one element smaller size, but with all the invariants
  // of a binary heap preserved.
  *evicted = back;
  queue_.back() = call;
  push_heap(queue_.begin(), queue_.end(), kMinHeapCompare);
  return QUEUE_SUCCESS;
}

void LifoServiceQueue::Shutdown() {
  lock_guard l(lock_);
  shutdown_ = true;

  // Post a nullptr to wake up any consumers which are waiting.
  for (auto* cs : waiting_consumers_) {
    cs->Post(nullptr);
  }
  waiting_consumers_.clear();
}

string LifoServiceQueue::ToString() const {
  string ret;

  // It's necessary to take a lock while accessing InboundCall pointers
  // in the queue. There isn't a guarantee the underlying memory stays valid
  // and not freed otherwise.
  lock_guard l(lock_);
  decltype(queue_) tmp(queue_);
  while (!tmp.empty()) {
    ret += tmp.front()->ToString() + "\n";
    pop_heap(tmp.begin(), tmp.end(), kMinHeapCompare);
    tmp.pop_back();
  }

  return ret;
}

} // namespace rpc
} // namespace kudu
