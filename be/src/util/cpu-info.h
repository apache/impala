// Copyright (c) 2011 Cloudera, Inc.  All right reserved.

#ifndef IMPALA_UTIL_CPU_INFO_H
#define IMPALA_UTIL_CPU_INFO_H

#include <string>
#include <boost/cstdint.hpp>

namespace impala {

// CpuInfo is an interface to query for cpu information at runtime.  The caller can
// ask for the sizes of the caches and what hardware features are supported.  This
// class is a singleton. 
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

  // Singleton interface
  static CpuInfo* Instance() { 
    if (instance_ == NULL) {
      instance_ = new CpuInfo();
      instance_->Init();
    }
    return instance_;
  }

  // Returns all the flags for this cpu
  int64_t hardware_flags() const { return hardware_flags_; }
  
  // Returns whether of not the cpu supports this flag
  bool IsSupported(long flag) const { return (hardware_flags_ & flag) != 0; }

  // Returns the size of the cache in KB at this cache level
  long CacheSize(CacheLevel level) const { return cache_sizes_[level]; }

 private:
  static CpuInfo* instance_;

  CpuInfo();
  void Init();

  int64_t hardware_flags_;
  long cache_sizes_[L3_CACHE + 1];
};

// Prints the cpu information
std::ostream& operator<<(std::ostream& os, const CpuInfo& info);

}

#endif

