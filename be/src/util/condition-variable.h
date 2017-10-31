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

#ifndef IMPALA_UTIL_CONDITION_VARIABLE_H
#define IMPALA_UTIL_CONDITION_VARIABLE_H

#include <boost/thread/pthread/timespec.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread_time.hpp>
#include <pthread.h>
#include <unistd.h>

namespace impala {

/// Simple wrapper around POSIX pthread condition variable. This has lower overhead than
/// boost's implementation as it doesn't implement boost thread interruption.
class ConditionVariable {
 public:
  ConditionVariable() { pthread_cond_init(&cv_, NULL); }

  ~ConditionVariable() { pthread_cond_destroy(&cv_); }

  /// Wait indefinitely on the condition variable until it's notified.
  void Wait(boost::unique_lock<boost::mutex>& lock) {
    DCHECK(lock.owns_lock());
    pthread_mutex_t* mutex = lock.mutex()->native_handle();
    pthread_cond_wait(&cv_, mutex);
  }

  /// Wait until the condition variable is notified or 'timeout' has passed.
  /// Returns true if the condition variable is notified before the absolute timeout
  /// specified in 'timeout' has passed. Returns false otherwise.
  bool WaitUntil(boost::unique_lock<boost::mutex>& lock,
      const timespec& abs_time) {
    DCHECK(lock.owns_lock());
    pthread_mutex_t* mutex = lock.mutex()->native_handle();
    return pthread_cond_timedwait(&cv_, mutex, &abs_time) == 0;
  }

  /// Wait until the condition variable is notified or 'abs_time' has passed.
  /// Returns true if the condition variable is notified before the absolute timeout
  /// specified in 'abs_time' has passed. Returns false otherwise.
  bool WaitUntil(boost::unique_lock<boost::mutex>& lock,
      const boost::system_time& abs_time) {
    return WaitUntil(lock, to_timespec(abs_time));
  }

  /// Wait until the condition variable is notified or have waited for the time
  /// specified in 'wait_duration'.
  /// Returns true if the condition variable is notified in time.
  /// Returns false otherwise.
  template <typename duration_type>
  bool WaitFor(boost::unique_lock<boost::mutex>& lock,
      const duration_type& wait_duration) {
    return WaitUntil(lock, to_timespec(boost::get_system_time() + wait_duration));
  }

  /// Notify a single waiter on this condition variable.
  void NotifyOne() { pthread_cond_signal(&cv_); }

  /// Notify all waiters on this condition variable.
  void NotifyAll() { pthread_cond_broadcast(&cv_); }

 private:
  pthread_cond_t cv_;

};

}
#endif
