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


#ifndef IMPALA_UTIL_BLOCKING_QUEUE_H
#define IMPALA_UTIL_BLOCKING_QUEUE_H

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <deque>
#include <unistd.h>

#include "util/stopwatch.h"

namespace impala {

/// Fixed capacity FIFO queue, where both BlockingGet and BlockingPut operations block
/// if the queue is empty or full, respectively.

/// TODO: Add some double-buffering so that readers do not block writers and vice versa.
/// Or, implement a mostly lock-free blocking queue.
template <typename T>
class BlockingQueue {
 public:
  BlockingQueue(size_t max_elements)
    : shutdown_(false),
      max_elements_(max_elements),
      total_get_wait_time_(0),
      total_put_wait_time_(0) {
  }

  /// Get an element from the queue, waiting indefinitely for one to become available.
  /// Returns false if we were shut down prior to getting the element, and there
  /// are no more elements available.
  bool BlockingGet(T* out) {
    MonotonicStopWatch timer;
    boost::unique_lock<boost::mutex> unique_lock(lock_);

    while (true) {
      if (!list_.empty()) {
        *out = list_.front();
        list_.pop_front();
        total_get_wait_time_ += timer.ElapsedTime();
        unique_lock.unlock();
        put_cv_.notify_one();
        return true;
      }
      if (shutdown_) return false;

      timer.Start();
      get_cv_.wait(unique_lock);
      timer.Stop();
    }
  }

  /// Puts an element into the queue, waiting indefinitely until there is space.
  /// If the queue is shut down, returns false.
  bool BlockingPut(const T& val) {
    MonotonicStopWatch timer;
    boost::unique_lock<boost::mutex> unique_lock(lock_);

    while (list_.size() >= max_elements_ && !shutdown_) {
      timer.Start();
      put_cv_.wait(unique_lock);
      timer.Stop();
    }
    total_put_wait_time_ += timer.ElapsedTime();
    if (shutdown_) return false;

    DCHECK_LT(list_.size(), max_elements_);
    list_.push_back(val);
    unique_lock.unlock();
    get_cv_.notify_one();
    return true;
  }

  /// Shut down the queue. Wakes up all threads waiting on BlockingGet or BlockingPut.
  void Shutdown() {
    {
      boost::lock_guard<boost::mutex> guard(lock_);
      shutdown_ = true;
    }

    get_cv_.notify_all();
    put_cv_.notify_all();
  }

  uint32_t GetSize() const {
    boost::unique_lock<boost::mutex> l(lock_);
    return list_.size();
  }

  /// Returns the total amount of time threads have blocked in BlockingGet.
  uint64_t total_get_wait_time() const {
    boost::lock_guard<boost::mutex> guard(lock_);
    return total_get_wait_time_;
  }

  /// Returns the total amount of time threads have blocked in BlockingPut.
  uint64_t total_put_wait_time() const {
    boost::lock_guard<boost::mutex> guard(lock_);
    return total_put_wait_time_;
  }

 private:
  bool shutdown_;
  const int max_elements_;
  boost::condition_variable get_cv_;   // 'get' callers wait on this
  boost::condition_variable put_cv_;   // 'put' callers wait on this
  /// lock_ guards access to list_, total_get_wait_time, and total_put_wait_time
  mutable boost::mutex lock_;
  std::deque<T> list_;
  uint64_t total_get_wait_time_;
  uint64_t total_put_wait_time_;
};

}

#endif
