// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_STOPWATCH_H
#define IMPALA_UTIL_STOPWATCH_H

#include <boost/cstdint.hpp>

namespace impala {

// Utility class to measure time.
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

  uint64_t Ticks() const {
    return running_ ? Rdtsc() - start_ : total_time_;
  }

  // This instruction (existed since Pentiums) returns the clock counter
  // on the chip.  It is not perfectly accurate because of skew between
  // cores and cores running slower for power savings.  Nevertheless,
  // it is extremely low overhead and probably accurate enough for us.
  uint64_t Rdtsc() const {
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

}

#endif

