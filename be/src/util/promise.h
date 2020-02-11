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

#ifndef IMPALA_UTIL_PROMISE_H
#define IMPALA_UTIL_PROMISE_H

#include <algorithm>
#include <mutex>

#include "util/condition-variable.h"
#include "util/time.h"
#include "common/atomic.h"
#include "common/logging.h"

namespace impala {

/// Is used as a template parameter for Promise class and represents the mode that a
/// Promise object works in. The values SINGLE_PRODUCER and MULTIPLE_PRODUCER are used
/// based on whether multiple calls to Promise::Set() should be invalid or not
/// respectively.
enum class PromiseMode { SINGLE_PRODUCER, MULTIPLE_PRODUCER };

/// A stripped-down replacement for boost::promise which, to the best of our knowledge,
/// actually works. Provides single assignment semantics for storing a value that can be
/// later retrieved asynchronously. Common usage pattern for different PromiseMode modes:
/// SINGLE_PRODUCER: A single producer provides a single value by calling Set(..)
/// MULTIPLE_PRODUCER: Multiple producers call Set(..) and can check if it was
/// successfully set or was already set by another producer by looking at the returned
/// value.
///
/// One or more consumers can retrieve the value through calling Get(..). They must be
/// consistent in their use of Get(), i.e., for a particular promise all consumers should
/// either have a timeout or not.

template <typename T, PromiseMode mode = PromiseMode::SINGLE_PRODUCER>
class Promise {
 public:
  Promise() : val_is_set_(false) { }

  /// Copies val into this promise if the val has not already been set, and notifies any
  /// consumers blocked in Get(). Returns the val that is set or the val that was already
  /// set before this call.
  /// It is invalid to call Set() twice if the PromiseMode is SINGLE_PRODUCER.
  T Set(const T& val) {
    std::unique_lock<std::mutex> l(val_lock_);
    if (val_is_set_) {
      DCHECK_ENUM_EQ(mode, PromiseMode::MULTIPLE_PRODUCER)
          << "Called Set(..) twice on the same Promise in SINGLE_PRODUCER mode";
      return val_;
    }
    val_ = val;
    val_is_set_ = true;

    /// Note: this must be called with 'val_lock_' taken. There are places where
    /// we use this object with this pattern:
    /// {
    ///   Promise p;
    ///   ...
    ///   p.get();
    /// }
    /// < promise object gets destroyed >
    /// Calling NotifyAll() with the val_lock_ guarantees that the thread calling
    /// Set() is done and the promise is safe to delete.
    val_set_cond_.NotifyAll();
    return val_;
  }

  /// Blocks until a value is set, and then returns a reference to that value. Once Get()
  /// returns, the returned value will not change, since Set(..) may not be called twice.
  const T& Get() {
    std::unique_lock<std::mutex> l(val_lock_);
    while (!val_is_set_) {
      val_set_cond_.Wait(l);
    }
    return val_;
  }

  /// Blocks until a value is set or the given timeout was reached.
  /// Returns a reference to that value which is invalid if the timeout was reached.
  /// Once Get() returns and *timed_out is false, the returned value will not
  /// change, since Set(..) may not be called twice.
  /// timeout_millis: The max wall-clock time in milliseconds to wait (must be > 0).
  /// timed_out: Indicates whether Get() returned due to timeout. Must be non-NULL.
  const T& Get(int64_t timeout_millis, bool* timed_out) {
    DCHECK_GT(timeout_millis, 0);
    int64_t timeout_micros = timeout_millis * MICROS_PER_MILLI;
    DCHECK(timed_out != NULL);
    std::unique_lock<std::mutex> l(val_lock_);
    int64_t start;
    int64_t now;
    now = start = MonotonicMicros();
    while (!val_is_set_ && (now - start) < timeout_micros) {
      int64_t wait_time_micros = std::max<int64_t>(1, timeout_micros - (now - start));
      val_set_cond_.WaitFor(l, wait_time_micros);
      now = MonotonicMicros();
    }
    *timed_out = !val_is_set_;
    return val_;
  }

  /// Returns whether the value is set.
  bool IsSet() {
    std::lock_guard<std::mutex> l(val_lock_);
    return val_is_set_;
  }

 private:
  /// These variables deal with coordination between consumer and producer, and protect
  /// access to val_;
  ConditionVariable val_set_cond_;
  bool val_is_set_;
  std::mutex val_lock_;

  /// The actual value transferred from producer to consumer
  T val_;
};

}

#endif
