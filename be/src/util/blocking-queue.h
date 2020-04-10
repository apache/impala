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

#include <unistd.h>
#include <deque>
#include <memory>
#include <mutex>
#include <boost/scoped_ptr.hpp>

#include "common/atomic.h"
#include "common/compiler-util.h"
#include "util/aligned-new.h"
#include "util/condition-variable.h"
#include "util/runtime-profile.h"
#include "util/stopwatch.h"
#include "util/time.h"
#include "gutil/port.h"

namespace impala {

/// Default functor that always returns 0 bytes. This disables the byte limit
/// functionality for the queue.
template <typename T>
struct ByteLimitDisabledFn {
  int64_t operator()(const T& item) {
    return 0;
  }
};

/// Fixed capacity FIFO queue, where both BlockingGet() and BlockingPut() operations block
/// if the queue is empty or full, respectively.
///
/// The queue always has a hard maximum capacity of elements. It also has an optional
/// limit on the bytes enqueued. This limit is a soft limit - one element can always be
/// enqueued regardless of the size in bytes. In order to use the bytes limit, the queue
/// must be instantiated with a functor that returns the size in bytes of an enqueued
/// item. The functor is invoked multiple times and must always return the same value for
/// the same item.
///
/// FIFO is made up of a 'get_list_' that BlockingGet() consumes from and a 'put_list_'
/// that BlockingPut() enqueues into. They are protected by 'get_lock_' and 'put_lock_'
/// respectively. If both locks need to be held at the same time, 'get_lock_' must be
/// held before 'put_lock_'. When the 'get_list_' is empty, the caller of BlockingGet()
/// will atomically swap the 'put_list_' with 'get_list_'. The swapping happens with both
/// the 'get_lock_' and 'put_lock_' held.
///
/// The queue supports two optional RuntimeProfile::Counters. One to track the amount
/// of time spent blocking in BlockingGet() and the other to track the amount of time
/// spent in BlockingPut().
template <typename T, typename ElemBytesFn = ByteLimitDisabledFn<T>>
class BlockingQueue : public CacheLineAligned {
 public:
  BlockingQueue(size_t max_elements, int64_t max_bytes = -1,
      RuntimeProfile::Counter* get_wait_timer = nullptr,
      RuntimeProfile::Counter* put_wait_timer = nullptr)
    : shutdown_(false),
      max_elements_(max_elements),
      put_wait_timer_(put_wait_timer),
      get_list_size_(0),
      get_wait_timer_(get_wait_timer),
      max_bytes_(max_bytes) {
    DCHECK(max_bytes == -1 || max_bytes > 0) << max_bytes;
    DCHECK_GT(max_elements_, 0);
    // Make sure class members commonly used in BlockingPut() don't alias with class
    // members used in BlockingGet(). 'put_bytes_enqueued_' is the point of division.
    DCHECK_NE(offsetof(BlockingQueue, put_bytes_enqueued_) / 64,
        offsetof(BlockingQueue, get_lock_) / 64);
  }

  /// Gets an element from the queue, waiting indefinitely for one to become available.
  /// Returns false if we were shut down prior to getting the element, and there
  /// are no more elements available.
  bool BlockingGet(T* out) {
    std::unique_lock<std::mutex> read_lock(get_lock_);

    if (UNLIKELY(get_list_.empty())) {
      MonotonicStopWatch timer;
      // Block off writers while swapping 'get_list_' with 'put_list_'.
      std::unique_lock<std::mutex> write_lock(put_lock_);
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
        if (get_wait_timer_ != nullptr) timer.Start();
        get_cv_.Wait(write_lock);
        if (get_wait_timer_ != nullptr) timer.Stop();
      }
      DCHECK(!put_list_.empty());
      put_list_.swap(get_list_);
      get_list_size_.Store(get_list_.size());
      write_lock.unlock();
      if (get_wait_timer_ != nullptr) get_wait_timer_->Add(timer.ElapsedTime());
    }

    DCHECK(!get_list_.empty());
    *out = std::move(get_list_.front());
    get_list_.pop_front();
    get_list_size_.Store(get_list_.size());
    read_lock.unlock();
    int64_t val_bytes = ElemBytesFn()(*out);
    DCHECK_GE(val_bytes, 0);
    get_bytes_dequeued_.Add(val_bytes);
    // Note that there is a race with any writer if NotifyOne() is called between when
    // a writer checks the queue size and when it calls put_cv_.Wait(). If this race
    // occurs, a writer can stay blocked even if the queue is not full until the next
    // BlockingGet(). The race is benign correctness wise as BlockingGet() will always
    // notify a writer with 'put_lock_' held when both lists are empty.
    //
    // Relatedly, if multiple writers hit the bytes limit of the queue and queue elements
    // vary in size, we may not immediately unblock all writers. E.g. if two writers are
    // waiting to enqueue elements of N bytes and we dequeue an element of 2N bytes, we
    // could wake up both writers but actually only wake up one. This is also benign
    // correctness-wise because we will continue to make progress.
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
    int64_t val_bytes = ElemBytesFn()(val);
    DCHECK_GE(val_bytes, 0);
    std::unique_lock<std::mutex> write_lock(put_lock_);
    while (!HasCapacityInternal(write_lock, val_bytes) && !shutdown_) {
      if (put_wait_timer_ != nullptr) timer.Start();
      put_cv_.Wait(write_lock);
      if (put_wait_timer_ != nullptr) timer.Stop();
    }
    if (put_wait_timer_ != nullptr) put_wait_timer_->Add(timer.ElapsedTime());
    if (UNLIKELY(shutdown_)) return false;

    DCHECK_LT(put_list_.size(), max_elements_);
    put_bytes_enqueued_ += val_bytes;
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
    int64_t val_bytes = ElemBytesFn()(val);
    DCHECK_GE(val_bytes, 0);
    std::unique_lock<std::mutex> write_lock(put_lock_);
    timespec abs_time;
    TimeFromNowMicros(timeout_micros, &abs_time);
    bool notified = true;
    while (!HasCapacityInternal(write_lock, val_bytes) && !shutdown_ && notified) {
      if (put_wait_timer_ != nullptr) timer.Start();
      // Wait until we're notified or until the timeout expires.
      notified = put_cv_.WaitUntil(write_lock, abs_time);
      if (put_wait_timer_ != nullptr) timer.Stop();
    }
    if (put_wait_timer_ != nullptr) put_wait_timer_->Add(timer.ElapsedTime());
    // If the list is still full or if the the queue has been shut down, return false.
    // NOTE: We don't check 'notified' here as it appears that pthread condition variables
    // have a weird behavior in which they can return ETIMEDOUT from timed_wait even if
    // another thread did in fact signal
    if (!HasCapacityInternal(write_lock, val_bytes)) return false;
    DCHECK_LT(put_list_.size(), max_elements_);
    put_bytes_enqueued_ += val_bytes;
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
      std::lock_guard<std::mutex> write_lock(put_lock_);
      shutdown_ = true;
    }

    get_cv_.NotifyAll();
    put_cv_.NotifyAll();
  }

  uint32_t Size() const {
    std::unique_lock<std::mutex> write_lock(put_lock_);
    return SizeLocked(write_lock);
  }

  bool AtCapacity() const {
    std::unique_lock<std::mutex> write_lock(put_lock_);
    return SizeLocked(write_lock) >= max_elements_;
  }

 private:
  uint32_t ALWAYS_INLINE SizeLocked(const std::unique_lock<std::mutex>& lock) const {
    // The size of 'get_list_' is read racily to avoid getting 'get_lock_' in write path.
    DCHECK(lock.mutex() == &put_lock_ && lock.owns_lock());
    return get_list_size_.Load() + put_list_.size();
  }

  /// Return true if the queue has capacity to add one more element with size 'val_bytes'.
  /// Caller must hold 'put_lock_' via 'lock'.
  bool HasCapacityInternal(const std::unique_lock<std::mutex>& lock, int64_t val_bytes) {
    DCHECK(lock.mutex() == &put_lock_ && lock.owns_lock());
    uint32_t size = SizeLocked(lock);
    if (size >= max_elements_) return false;
    if (val_bytes == 0 || max_bytes_ == -1 || size == 0) return true;

    // At this point we can enqueue the item if there is sufficient bytes capacity.
    if (put_bytes_enqueued_ + val_bytes <= max_bytes_) return true;

    // No bytes capacity left - swap over dequeued bytes to account for elements the
    // consumer has dequeued. All decrementers of 'get_bytes_dequeued_' hold 'put_lock_'
    // races with other decrementers are impossible.
    int64_t dequeued = get_bytes_dequeued_.Swap(0);
    put_bytes_enqueued_ -= dequeued;
    return put_bytes_enqueued_ + val_bytes <= max_bytes_;
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
  mutable std::mutex put_lock_;

  /// The queue for items enqueued by BlockingPut(). Guarded by 'put_lock_'.
  std::deque<T> put_list_;

  /// BlockingPut()/BlockingPutWithTimeout() wait on this.
  ConditionVariable put_cv_;

  /// Total amount of time threads blocked in BlockingPut(). Guarded by 'put_lock_'.
  RuntimeProfile::Counter* put_wait_timer_ = nullptr;

  /// Running counter for bytes enqueued, incremented through the producer thread.
  /// Decremented by transferring value from 'get_bytes_dequeued_'.
  /// Guarded by 'put_lock_'
  int64_t put_bytes_enqueued_ = 0;

#ifdef __aarch64__
  /// Add padding to keep cache line aligned on aarch64 platform.
  char padding[CACHELINE_SIZE - (sizeof(bool) + sizeof(int) + sizeof(std::mutex) +
      sizeof(std::deque<T>) + sizeof(ConditionVariable) + sizeof(uintptr_t)
      + sizeof(int64_t)) % CACHELINE_SIZE];

#endif

  /// Guards against concurrent access to 'get_list_'.
  mutable std::mutex get_lock_;

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
  RuntimeProfile::Counter* get_wait_timer_ = nullptr;

  /// Running count of bytes dequeued. Decremented from 'put_bytes_enqueued_' when it
  /// exceeds the queue capacity. Kept separate from 'put_bytes_enqueued_' so that
  /// producers and consumers are not updating the same cache line for every put and get.
  /// Decrementers must hold 'put_lock_'.
  AtomicInt64 get_bytes_dequeued_{0};

  /// Soft limit on total bytes in queue. -1 if no limit.
  const int64_t max_bytes_;
};
}
