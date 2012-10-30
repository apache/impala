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

namespace impala {

// Utility class to measure time.  This is measured using the cpu tick counter which
// is very low overhead but can be inaccurate if the thread is switched away.  This
// is useful for measuring cpu time at the row batch level (too much overhead at the
// row granularity).
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

  // Returns time in cpu ticks.
  uint64_t ElapsedTime() const {
    return running_ ? Rdtsc() - start_ : total_time_;
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

// Stop watch for reporting elapsed time based on wall block.  This is heavyweight
// but accurate.
class WallClockStopWatch {
 public:
  WallClockStopWatch() {
    total_time_ = 0;
    running_ = false;
  }

  void Start() {
    if (!running_) {
      start_ = GetTime();
      running_ = true;
    }
  }
  
  void Stop() {
    if (running_) {
      total_time_ += GetTime() - start_;
      running_ = false;
    }
  }

  // Returns time in ms.
  uint64_t ElapsedTime() {
    return running_ ? GetTime() - start_ : total_time_;
  }

 private:
  // Returns time (since epoch) in ms.
  static uint64_t GetTime();

  uint64_t start_, total_time_;
  bool running_;
};

}

#endif

