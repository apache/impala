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

#ifndef IMPALA_UTIL_COUNTING_BARRIER_H
#define IMPALA_UTIL_COUNTING_BARRIER_H

#include "common/atomic.h"
#include "util/promise.h"

namespace impala {

/// Allows clients to wait for the arrival of a fixed number of notifications after which
/// they are returned a value of type 'T' and allowed to continue.
template <typename T>
class TypedCountingBarrier {
 public:
  /// Initialises the TypedCountingBarrier with `count` pending notifications.
  TypedCountingBarrier(int32_t count) : count_(count) { DCHECK_GT(count, 0); }

  /// Sends one notification, decrementing the number of pending notifications by one.
  /// Returns the remaining pending notifications.
  /// If this is the final notifier, it unblocks Wait() with the returned value as
  /// 'promise_value'.
  /// TODO: some callers will decrement this below 0. In those cases only the notification
  /// that brings the count to 0 has an effect.
  int32_t Notify(const T& promise_value) {
    int32_t result = count_.Add(-1);
    if (result == 0) discard_result(promise_.Set(promise_value));
    return result;
  }

  /// Sets the number of pending notifications to 0 and unblocks Wait() with the returned
  /// value as 'promise_value'.
  void NotifyRemaining(const T& promise_value) {
    while (true) {
      int32_t value = count_.Load();
      if (value <= 0) return;  // count_ can legitimately drop below 0
      if (count_.CompareAndSwap(value, 0)) {
        discard_result(promise_.Set(promise_value));
        return;
      }
    }
  }

  /// Blocks until all notifications are received. Returns the value set by
  /// Notify() or NotifyRemaining().
  const T& Wait() { return promise_.Get(); }

  /// Blocks until all notifications are received, or until 'timeout_ms' passes, in which
  /// case '*timed_out' will be true. If '*timed_out' is false, then returns the value set
  /// by Notify() or NotifyRemaining().
  const T& Wait(int64_t timeout_ms, bool* timed_out) {
    return promise_.Get(timeout_ms, timed_out);
  }

  int32_t pending() const { return count_.Load(); }

 private:
  /// Used to signal waiters when all notifications are received.
  Promise<T> promise_;

  /// The number of pending notifications remaining.
  AtomicInt32 count_;

  DISALLOW_COPY_AND_ASSIGN(TypedCountingBarrier);
};

/// Wrapper around TypedCountingBarrier<T> which allows clients to wait for the arrival
/// of a fixed number of notifications after which they are allowed to continue.
class CountingBarrier {
 public:
  /// Initialises the CountingBarrier with `count` pending notifications.
  CountingBarrier(int32_t count) : barrier_(count) { DCHECK_GT(count, 0); }

  /// Sends one notification, decrementing the number of pending notifications by one.
  /// Returns the remaining pending notifications.
  int32_t Notify() { return barrier_.Notify(true); }

  /// Sets the number of pending notifications to 0 and unblocks Wait().
  void NotifyRemaining() { barrier_.NotifyRemaining(true); }

  /// Blocks until all notifications are received.
  void Wait() { discard_result(barrier_.Wait()); }

  /// Blocks until all notifications are received, or until 'timeout_ms' passes, in which
  /// case '*timed_out' will be true.
  void Wait(int64_t timeout_ms, bool* timed_out) {
    discard_result(barrier_.Wait(timeout_ms, timed_out));
  }

  int32_t pending() const { return barrier_.pending(); }

 private:
  TypedCountingBarrier<bool> barrier_;

  DISALLOW_COPY_AND_ASSIGN(CountingBarrier);
};

/// Helper class to always notify a CountingBarrier on scope exit, so users don't have to
/// worry about notifying on every possible path out of a scope.
class NotifyBarrierOnExit {
 public:
  NotifyBarrierOnExit(CountingBarrier* b) : barrier(b) {
    DCHECK(b != NULL);
  }

  ~NotifyBarrierOnExit() { discard_result(barrier->Notify()); }

 private:
  CountingBarrier* barrier;
};

}

#endif
