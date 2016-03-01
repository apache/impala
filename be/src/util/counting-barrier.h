// Copyright 2015 Cloudera Inc.
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

#ifndef IMPALA_UTIL_COUNTING_BARRIER_H
#define IMPALA_UTIL_COUNTING_BARRIER_H

namespace impala {

/// Allows clients to wait for the arrival of a fixed number of notifications before they
/// are allowed to continue.
class CountingBarrier {
 public:
  /// Initialises the CountingBarrier with `count` pending notifications.
  CountingBarrier(int32_t count) : count_(count) {
    DCHECK_GT(count, 0);
  }

  /// Sends one notification, decrementing the number of pending notifications by one.
  void Notify() {
    if (count_.UpdateAndFetch(-1) == 0) promise_.Set(true);
  }

  /// Blocks until all notifications are received.
  void Wait() { promise_.Get(); }

  /// Blocks until all notifications are received, or until 'timeout_ms' passes, in which
  /// case '*timed_out' will be true.
  void Wait(int64_t timeout_ms, bool* timed_out) { promise_.Get(timeout_ms, timed_out); }

 private:
  /// Used to signal waiters when all notifications are received.
  Promise<bool> promise_;

  /// The number of pending notifications remaining.
  AtomicInt<int32_t> count_;

  DISALLOW_COPY_AND_ASSIGN(CountingBarrier);
};

/// Helper class to always notify a CountingBarrier on scope exit, so users don't have to
/// worry about notifying on every possible path out of a scope.
class NotifyBarrierOnExit {
 public:
  NotifyBarrierOnExit(CountingBarrier* b) : barrier(b) {
    DCHECK(b != NULL);
  }

  ~NotifyBarrierOnExit() {
    barrier->Notify();
  }

 private:
  CountingBarrier* barrier;
};

}

#endif
