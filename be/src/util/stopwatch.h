// Copyright 2012 Cloudera Inc.
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


#ifndef IMPALA_UTIL_STOPWATCH_H
#define IMPALA_UTIL_STOPWATCH_H

#include <boost/cstdint.hpp>
#include <util/time.h>
#include <time.h>

namespace impala {

#define SCOPED_STOP_WATCH(c) \
  ScopedStopWatch<MonotonicStopWatch> \
  MACRO_CONCAT(STOP_WATCH, __COUNTER__)(c)


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
    if (running_) {
      total_time_ += Rdtsc() - start_;
      running_ = false;
    }
  }

  /// Returns time in cpu ticks.
  uint64_t ElapsedTime() const {
    return running_ ? Rdtsc() - start_ : total_time_;
  }

  /// Returns the total time accumulated
  uint64_t TotalElapsedTime() const {
    return total_time_ + (running_ ? Rdtsc() - start_ : 0);
  }

  static uint64_t Rdtsc() {
    uint32_t lo, hi;
    __asm__ __volatile__ (
      "xorl %%eax,%%eax \n        cpuid"
      ::: "%rax", "%rbx", "%rcx", "%rdx");
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return (uint64_t)hi << 32 | lo;
  }

 private:
  uint64_t start_, total_time_;
  bool running_;
};

/// Stop watch for reporting elapsed time in nanosec based on MonotonicNanos.
/// It is as fast as Rdtsc.
/// It is also accurate because it not affected by cpu frequency changes and
/// it is not affected by user setting the system clock.
/// A monotonic clock represents monotonic time since some unspecified starting point.
/// It is good for computing elapsed time.
class MonotonicStopWatch {
 public:
  MonotonicStopWatch() : start_(0), total_time_(0),
                         time_ceiling_(0), running_(false) {
  }

  void Start() {
    if (!running_) {
      start_ = MonotonicNanos();
      running_ = true;
    }
  }

  void Stop() {
    if (running_) {
      total_time_ += ElapsedTime();
      running_ = false;
    }
  }

  /// Set the time ceiling of the stop watch. The stop watch won't run past the ceiling.
  void SetTimeCeiling(const uint64_t ceiling) { time_ceiling_ = ceiling; }

  /// Restarts the timer. Returns the elapsed time until this point.
  uint64_t Reset() {
    uint64_t ret = ElapsedTime();
    if (running_) {
      start_ = MonotonicNanos();
      time_ceiling_ = 0;
    }
    return ret;
  }

  /// Returns time in nanosecond.
  uint64_t ElapsedTime() const {
    if (running_) return RunningTime();
    return total_time_;
  }

  /// Returns the total time accumulated
  uint64_t TotalElapsedTime() const {
    if (running_) return total_time_ + RunningTime();
    return total_time_;
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

  /// Returns the time since start.
  /// If time_ceiling_ is set, the stop watch won't run pass the ceiling.
  uint64_t RunningTime() const {
    uint64_t end = MonotonicNanos();
    if (time_ceiling_ > 0) {
      if (time_ceiling_ < start_) return 0;
      if (time_ceiling_ < end) end = time_ceiling_;
    }
    return end - start_;
  }
};

/// Utility class that starts the stop watch in the constructor and stops the watch when
/// the object goes out of scope.
/// 'T' must implement the StopWatch interface "Start", "Stop".
template<class T>
class ScopedStopWatch {
 public:
  ScopedStopWatch(T* sw) :
    sw_(sw) {
    DCHECK(sw != NULL);
    sw_->Start();
  }

  ~ScopedStopWatch() {
    sw_->Stop();
  }

 private:
  T* sw_;
};

}

#endif
