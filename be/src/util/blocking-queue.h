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


#ifndef IMPALA_UTIL_BLOCKING_QUEUE_H
#define IMPALA_UTIL_BLOCKING_QUEUE_H

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <deque>
#include <memory>
#include <unistd.h>

#include "common/atomic.h"
#include "common/compiler-util.h"
#include "util/aligned-new.h"
#include "util/condition-variable.h"
#include "util/stopwatch.h"
#include "util/time.h"

namespace impala {

/// Fixed capacity FIFO queue, where both BlockingGet() and BlockingPut() operations block
/// if the queue is empty or full, respectively.
///
/// FIFO is made up of a 'get_list_' that BlockingGet() consumes from and a 'put_list_'
/// that BlockingPut() enqueues into. They are protected by 'get_lock_' and 'put_lock_'
/// respectively. If both locks need to be held at the same time, 'get_lock_' must be
/// held before 'put_lock_'. When the 'get_list_' is empty, the caller of BlockingGet()
/// will atomically swap the 'put_list_' with 'get_list_'. The swapping happens with both
/// the 'get_lock_' and 'put_lock_' held.
template <typename T>
class BlockingQueue : public CacheLineAligned {
 public:
  BlockingQueue(size_t max_elements)
    : shutdown_(false),
      max_elements_(max_elements),
      total_put_wait_time_(0),
      get_list_size_(0),
      total_get_wait_time_(0) {
    DCHECK_GT(max_elements_, 0);
    // Make sure class members commonly used in BlockingPut() don't alias with class
    // members used in BlockingGet(). 'pad_' is the point of division.
    DCHECK_NE(offsetof(BlockingQueue, pad_) / 64,
        offsetof(BlockingQueue, get_lock_) / 64);
  }

  /// Gets an element from the queue, waiting indefinitely for one to become available.
  /// Returns false if we were shut down prior to getting the element, and there
  /// are no more elements available.
  bool BlockingGet(T* out) {
    boost::unique_lock<boost::mutex> read_lock(get_lock_);

    if (UNLIKELY(get_list_.empty())) {
      MonotonicStopWatch timer;
      // Block off writers while swapping 'get_list_' with 'put_list_'.
      boost::unique_lock<boost::mutex> write_lock(put_lock_);
      while (put_list_.empty()) {
        DCHECK(get_list_.empty());
        if (UNLIKELY(shutdown_)) return false;
        // Note that it's intentional to signal the writer while holding 'put_lock_' to
        // avoid the race in which the writer may be signalled between when it checks
        // the queue size and when it calls Wait() in BlockingGet(). NotifyAll() is not
        // used here to avoid thundering herd which leads to contention (e.g. InitTuple()
        // in scanner).
        put_cv_.NotifyOne();
        // Sleep with 'get_lock_' held to block off other readers which cannot
        // make progress anyway.
        timer.Start();
        get_cv_.Wait(write_lock);
        timer.Stop();
      }
      DCHECK(!put_list_.empty());
      put_list_.swap(get_list_);
      get_list_size_.Store(get_list_.size());
      write_lock.unlock();
      total_get_wait_time_ += timer.ElapsedTime();
    }

    DCHECK(!get_list_.empty());
    *out = std::move(get_list_.front());
    get_list_.pop_front();
    get_list_size_.Store(get_list_.size());
    read_lock.unlock();
    // Note that there is a race with any writer if NotifyOne() is called between when
    // a writer checks the queue size and when it calls put_cv_.Wait(). If this race
    // occurs, a writer can stay blocked even if the queue is not full until the next
    // BlockingGet(). The race is benign correctness wise as BlockingGet() will always
    // notify a writer with 'put_lock_' held when both lists are empty.
    put_cv_.NotifyOne();
    return true;
  }

  /// Puts an element into the queue, waiting indefinitely until there is space. Rvalues
  /// are moved into the queue, lvalues are copied. If the queue is shut down, returns
  /// false. V is a type that is compatible with T; that is, objects of type V can be
  /// inserted into the queue.
  template <typename V>
  bool BlockingPut(V&& val) {
    MonotonicStopWatch timer;
    boost::unique_lock<boost::mutex> write_lock(put_lock_);

    while (SizeLocked(write_lock) >= max_elements_ && !shutdown_) {
      timer.Start();
      put_cv_.Wait(write_lock);
      timer.Stop();
    }
    total_put_wait_time_ += timer.ElapsedTime();
    if (UNLIKELY(shutdown_)) return false;

    DCHECK_LT(put_list_.size(), max_elements_);
    Put(std::forward<V>(val));
    write_lock.unlock();
    get_cv_.NotifyOne();
    return true;
  }

  /// Puts an element into the queue, waiting until 'timeout_micros' elapses, if there is
  /// no space. If the queue is shut down, or if the timeout elapsed without being able to
  /// put the element, returns false. Rvalues are moved into the queue, lvalues are
  /// copied. V is a type that is compatible with T; that is, objects of type V can be
  /// inserted into the queue.
  template <typename V>
  bool BlockingPutWithTimeout(V&& val, int64_t timeout_micros) {
    MonotonicStopWatch timer;
    boost::unique_lock<boost::mutex> write_lock(put_lock_);
    boost::system_time wtime = boost::get_system_time() +
        boost::posix_time::microseconds(timeout_micros);
    const struct timespec timeout = boost::detail::to_timespec(wtime);
    bool notified = true;
    while (SizeLocked(write_lock) >= max_elements_ && !shutdown_ && notified) {
      timer.Start();
      // Wait until we're notified or until the timeout expires.
      notified = put_cv_.TimedWait(write_lock, &timeout);
      timer.Stop();
    }
    total_put_wait_time_ += timer.ElapsedTime();
    // If the list is still full or if the the queue has been shut down, return false.
    // NOTE: We don't check 'notified' here as it appears that pthread condition variables
    // have a weird behavior in which they can return ETIMEDOUT from timed_wait even if
    // another thread did in fact signal
    if (SizeLocked(write_lock) >= max_elements_ || shutdown_) return false;
    DCHECK_LT(put_list_.size(), max_elements_);
    Put(std::forward<V>(val));
    write_lock.unlock();
    get_cv_.NotifyOne();
    return true;
  }

  /// Shut down the queue. Wakes up all threads waiting on BlockingGet or BlockingPut.
  void Shutdown() {
    {
      // No need to hold 'get_lock_' here. BlockingGet() may sleep with 'get_lock_' so
      // it may delay the caller here if the lock is acquired.
      boost::lock_guard<boost::mutex> write_lock(put_lock_);
      shutdown_ = true;
    }

    get_cv_.NotifyAll();
    put_cv_.NotifyAll();
  }

  uint32_t Size() const {
    boost::unique_lock<boost::mutex> write_lock(put_lock_);
    return SizeLocked(write_lock);
  }

  int64_t total_get_wait_time() const {
    // Hold lock to make sure the value read is consistent (i.e. no torn read).
    boost::lock_guard<boost::mutex> read_lock(get_lock_);
    return total_get_wait_time_;
  }

  int64_t total_put_wait_time() const {
    // Hold lock to make sure the value read is consistent (i.e. no torn read).
    boost::lock_guard<boost::mutex> write_lock(put_lock_);
    return total_put_wait_time_;
  }

 private:

  uint32_t ALWAYS_INLINE SizeLocked(const boost::unique_lock<boost::mutex>& lock) const {
    // The size of 'get_list_' is read racily to avoid getting 'get_lock_' in write path.
    DCHECK(lock.mutex() == &put_lock_ && lock.owns_lock());
    return get_list_size_.Load() + put_list_.size();
  }

  /// Overloads for inserting an item into the list, depending on whether it should be
  /// moved or copied.
  void Put(const T& val) { put_list_.push_back(val); }
  void Put(T&& val) { put_list_.emplace_back(std::move(val)); }

  /// True if the BlockingQueue is being shut down. Guarded by 'put_lock_'.
  bool shutdown_;

  /// Maximum total number of elements in 'get_list_' + 'put_list_'.
  const int max_elements_;

  /// Guards against concurrent access to 'put_list_'.
  /// Please see comments at the beginning of the file for lock ordering.
  mutable boost::mutex put_lock_;

  /// The queue for items enqueued by BlockingPut(). Guarded by 'put_lock_'.
  std::deque<T> put_list_;

  /// BlockingPut()/BlockingPutWithTimeout() wait on this.
  ConditionVariable put_cv_;

  /// Total amount of time threads blocked in BlockingPut(). Guarded by 'put_lock_'.
  int64_t total_put_wait_time_;

  /// Padding to avoid data structures used in BlockingGet() to share cache lines
  /// with data structures used in BlockingPut().
  int64_t pad_;

  /// Guards against concurrent access to 'get_list_'.
  mutable boost::mutex get_lock_;

  /// The queue of items to be consumed by BlockingGet(). Guarded by 'get_lock_'.
  std::deque<T> get_list_;

  /// The size of 'get_list_'. Read without lock held so explicitly use an AtomicInt32
  /// to make sure readers will read a consistent value on all CPU architectures.
  AtomicInt32 get_list_size_;

  /// BlockingGet() waits on this.
  ConditionVariable get_cv_;

  /// Total amount of time a thread blocked in BlockingGet(). Guarded by 'get_lock_'.
  /// Note that a caller of BlockingGet() may sleep with 'get_lock_' held and this
  /// variable doesn't include the time which other threads block waiting for 'get_lock_'.
  int64_t total_get_wait_time_;

};

}

#endif
