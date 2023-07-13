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

#include <cstdint>
#include <mutex>

#include "common/logging.h"
#include "common/status.h"
#include "util/condition-variable.h"

namespace impala {

/// Synchronization barrier that is used when a fixed number of threads need to repeatedly
/// synchronize, e.g. to proceed to the next phase of an algorithm. At each phase, waits
/// until all threads have called Wait() on the barrier, then unblocks all threads.
/// The last thread to arrive at the barrier executes a function before waking the other
/// threads.
class CyclicBarrier {
 public:
  CyclicBarrier(int num_threads);

  /// Waits until all threads have joined the barrier. Then the last thread executes 'fn'
  /// and once that is completed, all threads return. Note that 'fn' executes serially,
  /// so can be used to implement a serial phase of a parallel algorithm.
  ///
  /// 'fn' must return a Status object and have no arguments. If the call to 'fn' returns
  /// an error status, the barrier will be cancelled with that status.
  ///
  /// Returns OK if all threads joined the barrier or an error status if cancelled.
  template <typename F>
  Status Wait(const F& fn) {
    Status fn_status;
    {
      std::unique_lock<std::mutex> l(lock_);
      RETURN_IF_ERROR(cancel_status_);
      ++num_waiting_threads_;
      DCHECK_LE(num_waiting_threads_, num_threads_);
      if (num_waiting_threads_ < num_threads_) {
        // Wait for the last thread to wake us up.
        int64_t start_cycle = cycle_num_;
        while (cancel_status_.ok() && cycle_num_ == start_cycle
            && num_waiting_threads_ < num_threads_) {
          barrier_cv_.Wait(l);
        }
        if (!cancel_status_.ok() || cycle_num_ > start_cycle) {
          return cancel_status_;
        }
      }
      // This is the last thread or a woken up thread by the last unregister and barrier
      // isn't cancelled. We can proceed by resetting state for the next cycle.
      fn_status = fn();
      if (fn_status.ok()) {
        num_waiting_threads_ = 0;
        ++cycle_num_;
      } else {
        cancel_status_ = fn_status;
      }
    }
    barrier_cv_.NotifyAll();
    return fn_status;
  }

  // Cancels the barrier. All blocked and future calls to cancel will return immediately
  // with an error status. Cancel() can be called multiple times. In that case, the 'err'
  // value from the first call is used.
  // 'err' must be a non-OK status.
  void Cancel(const Status& err);

  // Unregisters one thread from the synchronization, wakes up one waiting thread to
  // execute 'fn' of Wait() if the unregistering thread was the last one.
  void Unregister();

 private:
  // The number of threads participating in synchronization.
  int num_threads_;

  // Protects below members.
  std::mutex lock_;

  // Condition variable that is signalled (with NotifyAll) when all threads join the
  // barrier, or the barrier is cancelled.
  ConditionVariable barrier_cv_;

  // The number of barrier synchronizations that have occurred. Incremented each time that
  // a barrier synchronization completes. The synchronisation algorithm uses this to
  // determine whether the previous cycle was complete (calls to Wait() from different
  // cycles can overlap if a thread calls Wait() for the next cycle before all threads
  // have returned from Wait() for the previous cycle). The algorithm depends on the cycle
  // number not being reused until all threads from previous cycles have returned from
  // Wait().
  //
  // Use unsigned integer so that overflow is extremely unlikely, but also has defined
  // behaviour.
  uint64_t cycle_num_ = 0;

  // Number of threads that are currently waiting for the barrier.
  int num_waiting_threads_ = 0;

  // Error status if the barrier was cancelled.
  Status cancel_status_;
};

} // namespace impala
