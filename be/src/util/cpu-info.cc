// Copyright (c) 2011 Cloudera, Inc.  All right reserved.

#include "util/cpu-info.h"
#include "util/debug-util.h"

#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

using namespace boost;
using namespace std;

namespace impala {

bool CpuInfo::initialized_ = false;
int64_t CpuInfo::hardware_flags_ = 0;
int64_t CpuInfo::original_hardware_flags_;
long CpuInfo::cache_sizes_[L3_CACHE + 1];
int64_t CpuInfo::cycles_per_ms_;

static struct {
  string name;
  int64_t flag;
} flag_mappings[] =
{
  { "ssse3",  CpuInfo::SSE3 },
  { "sse4_1", CpuInfo::SSE4_1 },
  { "sse4_2", CpuInfo::SSE4_2 },
};
static const long num_flags = sizeof(flag_mappings) / sizeof(flag_mappings[0]);

// Helper function to parse for hardware flags.  
// values contains a list of space-seperated flags.  check to see if the flags we
// care about are present.
// Returns a bitmap of flags.
int64_t ParseCPUFlags(const string& values) {
  int64_t flags = 0;
  for (int i = 0; i < num_flags; ++i) {
    if (contains(values, flag_mappings[i].name)) {
      flags |= flag_mappings[i].flag;
    }
  }
  return flags;
}

void CpuInfo::Init() {
  string line;
  string name;
  string value;
  
  float max_mhz = 0;

  memset(&cache_sizes_, 0, sizeof(cache_sizes_));

  // Read from /proc/cpuinfo
  ifstream cpuinfo("/proc/cpuinfo", ios::in);
  while (cpuinfo) {
    getline(cpuinfo, line);
    size_t colon = line.find(':');
    if (colon != string::npos) {
      name = line.substr(0, colon - 1);
      value = line.substr(colon + 1, string::npos);
      trim(name);
      if (name.compare("flags") == 0) {
        trim(value);
        hardware_flags_ |= ParseCPUFlags(value);
      } else if (name.compare("cpu MHz") == 0) {
        trim(value);
        // Every core will report a different speed.  We'll take the max, assuming
        // that when impala is running, the core will not be in a lower power state.
        // TODO: is there a more robust way to do this, such as
        // Window's QueryPerformanceFrequency()
        float mhz = atof(value.c_str());
        max_mhz = max(mhz, max_mhz);
      }
    }
  }
  if (cpuinfo.is_open()) cpuinfo.close();

  // Call sysconf to query for the cache sizes
  cache_sizes_[0] = sysconf(_SC_LEVEL1_DCACHE_SIZE);
  cache_sizes_[1] = sysconf(_SC_LEVEL2_CACHE_SIZE);
  cache_sizes_[2] = sysconf(_SC_LEVEL3_CACHE_SIZE);

  if (max_mhz != 0) {
    cycles_per_ms_ = max_mhz * 1000;
  } else {
    cycles_per_ms_ = 1000000;
  }
  original_hardware_flags_ = hardware_flags_;
  initialized_ = true;

  LOG(INFO) << DebugString();
}

void CpuInfo::EnableFeature(long flag, bool enable) {
  DCHECK(initialized_);
  if (!enable) {
    hardware_flags_ &= ~flag;
  } else {
    // Can't turn something on that can't be supported
    DCHECK((original_hardware_flags_ & flag) != 0);
    hardware_flags_ |= flag;
  }
}

string CpuInfo::DebugString() {
  DCHECK(initialized_);
  stringstream stream;
  int64_t L1 = CacheSize(L1_CACHE);
  int64_t L2 = CacheSize(L2_CACHE);
  int64_t L3 = CacheSize(L3_CACHE);
  stream << "Cpu Info:" << endl;
  stream << "  L1 Cache: " << PrettyPrinter::Print(L1, TCounterType::BYTES) << endl;
  stream << "  L2 Cache: " << PrettyPrinter::Print(L2, TCounterType::BYTES) << endl;
  stream << "  L3 Cache: " << PrettyPrinter::Print(L3, TCounterType::BYTES) << endl;
  stream << "  Hardware Supports:" << endl;
  for (int i = 0; i < num_flags; ++i) {
    if (IsSupported(flag_mappings[i].flag)) {
      stream << "    " << flag_mappings[i].name << endl;
    }
  }
  return stream.str();
}

}

