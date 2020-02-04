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

#include <util/os-info.h>
#include <util/spinlock.h>
#include <util/time.h>
#include <time.h>

namespace impala {

#define SCOPED_STOP_WATCH(c) \
  ScopedStopWatch<MonotonicStopWatch> \
  MACRO_CONCAT(STOP_WATCH, __COUNTER__)(c)

#define CONDITIONAL_SCOPED_STOP_WATCH(c, enabled) \
  ScopedStopWatch<MonotonicStopWatch> \
  MACRO_CONCAT(STOP_WATCH, __COUNTER__)(c, enabled)

#define SCOPED_CONCURRENT_STOP_WATCH(c) \
  ScopedStopWatch<ConcurrentStopWatch> \
  MACRO_CONCAT(CONCURRENT_STOP_WATCH, __COUNTER__)(c)

#define CONDITIONAL_SCOPED_CONCURRENT_STOP_WATCH(c, enabled) \
  ScopedStopWatch<ConcurrentStopWatch> \
  MACRO_CONCAT(CONCURRENT_STOP_WATCH, __COUNTER__)(c, enabled)

/// Utility class to measure time.  This is measured using the cpu tick counter which
/// is very low overhead but can be inaccurate if the thread is switched away.  This
/// is useful for measuring cpu time at the row batch level (too much overhead at the
/// row granularity).
class StopWatch {
 public:
  StopWatch() {
    total_time_ = 0;
    running_ = false;
  }

  void Start() {
    if (!running_) {
      start_ = Rdtsc();
      running_ = true;
    }
  }

  void Stop() {
    total_time_ += RunningTime();
    running_ = false;
  }

  /// Returns total time in cpu ticks for which the stopwatch was running, including
  /// the time since Start() was called, if it is currently running.
  uint64_t ElapsedTime() const {
    return total_time_ + RunningTime();
  }

#if defined(__aarch64__)
  static uint64_t Rdtsc() {
    uint64_t virtual_timer_value;
    asm volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
    return virtual_timer_value;
  }
#else
  static uint64_t Rdtsc() {
    uint32_t lo, hi;
    __asm__ __volatile__ (
      "xorl %%eax,%%eax \n        cpuid"
      ::: "%rax", "%rbx", "%rcx", "%rdx");
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return (uint64_t)hi << 32 | lo;
  }
#endif

 private:
  /// Returns total time in cpu ticks since Start() was called. If not running, returns 0.
  uint64_t RunningTime() const {
    return running_ ? Rdtsc() - start_ : 0;
  }

  uint64_t start_, total_time_;
  bool running_;
};

/// Stop watch for reporting elapsed time in nanosec based on clock_gettime (Linux) or
/// MonotonicNanos (Apple).  It is not affected by cpu frequency changes and it is not
/// affected by user setting the system clock.  A monotonic clock represents monotonic
/// time since some unspecified starting point.  It is good for computing elapsed time.
///
/// The time values are in nanoseconds.  For most machine configurations, the clock
/// resolution will be 1 nanosecond.  We fall back to low resolution in configurations
/// where the clock is expensive.  For those machine configurations (notably EC2), the
/// clock resolution will be that of the system jiffy, which is between 1 and 10
/// milliseconds.
class MonotonicStopWatch {
 public:
  MonotonicStopWatch() : start_(0), total_time_(0),
                         time_ceiling_(0), running_(false) {
  }

  void Start() {
    if (!running_) {
      start_ = Now();
      running_ = true;
    }
  }

  void Stop() {
    total_time_ += RunningTime();
    running_ = false;
  }

  /// Set the time ceiling of the stop watch to Now(). The stop watch won't run past the
  /// ceiling.
  void SetTimeCeiling() { time_ceiling_ = Now(); }

  /// Restarts the timer. Returns the elapsed time until this point.
  uint64_t Reset() {
    uint64_t ret = RunningTime();
    if (running_) {
      start_ = Now();
      time_ceiling_ = 0;
    }
    return ret;
  }

  /// Returns total time in nanoseconds for which the stopwatch was running, including
  /// the time since Start() was called, if it is currently running.
  uint64_t ElapsedTime() const {
    return total_time_ + RunningTime();
  }

  /// Returns an representation of the current time in nanoseconds. It can be used to
  /// measure time durations by repeatedly calling this function and comparing the result.
  /// While this function returns nanoseconds, its resolution may be as large as
  /// milliseconds, depending on OsInfo::fast_clock().
  static inline int64_t Now() {
#if defined(__APPLE__)
    // Apple does not support clock_gettime.
    return MonotonicNanos();
#else
    // Use a fast but low-resolution clock if the high-resolution clock is slow because
    // Now() can be called frequently (IMPALA-2407).
    timespec ts;
    clock_gettime(OsInfo::fast_clock(), &ts);
    return ts.tv_sec * NANOS_PER_SEC + ts.tv_nsec;
#endif
  }

 private:
  /// Start epoch value.
  uint64_t start_;

  /// Total elapsed time in nanoseconds.
  uint64_t total_time_;

  /// Upper bound of the running time as a epoch value. If the value is larger than 0,
  /// the stopwatch interprets this as a time ceiling is set.
  uint64_t time_ceiling_;

  /// True if stopwatch is running.
  bool running_;

  /// Returns the time since Start() was called, if running. Otherwise return 0.
  /// If time_ceiling_ is set, the stop watch won't run pass the ceiling.
  uint64_t RunningTime() const {
    if (!running_) return 0;
    uint64_t end = Now();
    if (time_ceiling_ > 0) {
      if (time_ceiling_ < start_) return 0;
      if (time_ceiling_ < end) end = time_ceiling_;
    }
    // IMPALA-7963: On some buggy kernels, time can go backwards slightly. Log a warning
    // and return 0 in this case.
    if (end < start_) {
      LOG(INFO) << "WARNING: time went backwards from " << start_ << " to " << end;
      return 0;
    }
    return end - start_;
  }
};

/// Utility class to measure multiple threads concurrent wall time.
/// If a thread is already running, the following thread won't reset the stop watch.
/// The stop watch is stopped only when all threads finish their work.
class ConcurrentStopWatch {
 public:
  ConcurrentStopWatch() : busy_threads_(0), last_lap_start_(0) {}

  void Start() {
    std::lock_guard<SpinLock> l(thread_counter_lock_);
    if (busy_threads_ == 0) {
      msw_.Start();
    }
    ++busy_threads_;
  }

  void Stop() {
    std::lock_guard<SpinLock> l(thread_counter_lock_);
    DCHECK_GT(busy_threads_, 0);
    --busy_threads_;
    if (busy_threads_ == 0) {
      msw_.Stop();
    }
  }

  /// Returns delta wall time since last time LapTime() is called.
  uint64_t LapTime() {
    std::lock_guard<SpinLock> l(thread_counter_lock_);
    uint64_t now = msw_.ElapsedTime();
    uint64_t lap_duration = now - last_lap_start_;
    last_lap_start_ = now;
    return lap_duration;
  }

  uint64_t TotalRunningTime() const {
    std::lock_guard<SpinLock> l(thread_counter_lock_);
    return msw_.ElapsedTime();
  }

  /// Set the time ceiling of the stop watch to Now(). The stop watch won't run past the
  /// ceiling.
  void SetTimeCeiling() {
    std::lock_guard<SpinLock> l(thread_counter_lock_);
    msw_.SetTimeCeiling();
  }

 private:
  MonotonicStopWatch msw_;

  /// Lock with busy_threads_.
  mutable SpinLock thread_counter_lock_;

  /// Track how many threads are currently busy.
  int busy_threads_;

  /// Track when the last time LapTime() is called so we can calculate lap time.
  uint64_t last_lap_start_;
};

/// Utility class that starts the stop watch in the constructor and stops the watch when
/// the object goes out of scope. If the optional argument 'enabled' is false, the
/// stopwatch is not updated.
/// 'T' must implement the StopWatch interface "Start", "Stop".
template<class T>
class ScopedStopWatch {
 public:
  ScopedStopWatch(T* sw, bool enabled = true) :
    sw_(sw), enabled_(enabled) {
    DCHECK(sw != NULL);
    if (enabled_) sw_->Start();
  }

  ~ScopedStopWatch() {
    if (enabled_) sw_->Stop();
  }

 private:
  T* sw_;
  bool enabled_;
};
}
