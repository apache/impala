// Copyright (c) 2011 Cloudera, Inc.  All right reserved.

#ifndef IMPALA_UTIL_CPU_INFO_H
#define IMPALA_UTIL_CPU_INFO_H

#include <string>
#include <boost/cstdint.hpp>

#include "common/logging.h"

namespace impala {

// CpuInfo is an interface to query for cpu information at runtime.  The caller can
// ask for the sizes of the caches and what hardware features are supported.
// On Linux, this information is pulled from a couple of sys files (/proc/cpuinfo and 
// /sys/devices)
class CpuInfo {
 public:
  static const int64_t SSE3    = (1 << 1);
  static const int64_t SSE4_1  = (1 << 2);
  static const int64_t SSE4_2  = (1 << 3);

  // Cache enums for L1 (data), L2 and L3 
  enum CacheLevel {
    L1_CACHE = 0,
    L2_CACHE = 1,
    L3_CACHE = 2,
  };

  // Initialize CpuInfo.
  static void Init();

  // Returns all the flags for this cpu
  static int64_t hardware_flags() {
    DCHECK(initialized_);
    return hardware_flags_;
  }
  
  // Returns whether of not the cpu supports this flag
  inline static bool IsSupported(long flag) {
    DCHECK(initialized_);
    return (hardware_flags_ & flag) != 0;
  }

  // Toggle a hardware feature on and off.  It is not valid to turn on a feature
  // that the underlying hardware cannot support. This is useful for testing.
  static void EnableFeature(long flag, bool enable);

  // Returns the size of the cache in KB at this cache level
  static long CacheSize(CacheLevel level) {
    DCHECK(initialized_);
    return cache_sizes_[level];
  }

  // Returns the number of cpu cycles per millisecond
  static int64_t cycles_per_ms() {
    DCHECK(initialized_);
    return cycles_per_ms_;
  }

  static std::string DebugString();

 private:
  static bool initialized_;
  static int64_t hardware_flags_;
  static int64_t original_hardware_flags_;
  static long cache_sizes_[L3_CACHE + 1];
  static int64_t cycles_per_ms_;
};
}
#endif

