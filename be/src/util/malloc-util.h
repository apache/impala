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

#include <string>

#include <glog/logging.h>

#include "common/status.h"
#include "util/metrics-fwd.h"

namespace impala {
  class MallocUtil {
  public:
    // Gets the MallocUtil singleton. Always a non-nullptr.
    static MallocUtil* GetInstance();

    // Initialize the settings for the malloc implementation. This should be called once
    // at startup. Since backend tests may call this repeatedly, any additional calls
    // are ignored.
    virtual Status Init() = 0;

    // Gets a human readable description of malloc state / statistics
    virtual std::string GetTextDescription() const = 0;

    // The name of the malloc implementation
    virtual std::string GetName() const = 0;

    // Add malloc implementation specific metrics in the provided metrics group.
    virtual Status RegisterMemoryMetrics(MetricGroup* metrics) = 0;

    // Get the metric for used memory. This can only be called after initialization.
    // This must be non-null for all malloc implementations.
    virtual IntGauge* GetUsedBytesMetric(bool include_overhead) const = 0;

    // Get the metric for the malloc implementation's overhead. This can only be
    // called after initialization. This will return null if the malloc
    // implementation does not have an overhead metric.
    virtual IntGauge* GetOverheadBytesMetric() const = 0;

    // Get information about whether the malloc's implementation is compatible with
    // madvising huge pages. If a malloc implementation holds on to memory and can
    // break it up into smaller pieces, then it must return MADVISE_INCOMPATIBLE.
    // An implementation that will either immediately free a huge page should return
    // MADVISE_COMPATIBLE. This can only be called after initialization.
    enum class HugePageSupport {
      MADVISE_COMPATIBLE,
      MADVISE_INCOMPATIBLE
    };
    virtual HugePageSupport GetHugePageSupport() const = 0;
    friend std::ostream& operator<<(std::ostream& os, const HugePageSupport& h);

    // Profiler interfaces
    // This will need to evolve as other malloc implementations are added. Right now,
    // this only needs to handle Gperftools TCMalloc and the sanitizers. To simplify
    // implementations that don't support profiling, these default to returning false
    // and DCHECKing if any are used.
    //
    // Currently, all the different types of profiling return a binary structure for
    // use with the pprof utility. These are the current types of profiling:
    // Heap profiling: profile all allocations for a period of time. The call pattern
    //    is HeapProfilerStart(), wait for some samples to be collected,
    //    GetHeapProfile(), then HeapProfilerStop().
    // Heap growth stacks: return stacks for allocations that caused the address space
    //    to grow.
    // CPU profiling: send a signal and sample stacks. The call pattern is
    //    CPUProfileStart(), wait for some samples to be collected, then
    //    CPUProfileStop(). This writes the profile to a temporary file that can be
    //    read back.

    virtual bool SupportsHeapProfiling() const { return false; }
    virtual void HeapProfilerStart(const std::string& tmp_prof_file_name) {
      DCHECK(false);
    }
    virtual void HeapProfilerStop() { DCHECK(false); }
    // Returns a binary heap profile. The returned pointer is a '\0'-terminated
    // string allocated using malloc() and should be free()-ed as soon as the
    // caller does not need it anymore.
    virtual char* GetHeapProfile() {
      DCHECK(false);
      return nullptr;
    }

    virtual bool SupportsHeapGrowthStacks() const {
      return false;
    }
    virtual void GetHeapGrowthStacks(std::string* heap_growth_stacks) {
      DCHECK(false);
    }

    virtual bool SupportsCPUProfiling() const { return false; }
    virtual void CPUProfilerStart(const std::string& tmp_prof_file_name) {
      DCHECK(false);
    }
    virtual void CPUProfilerStop() {
      DCHECK(false);
    }
  protected:
    // Restrict access to the constructor / destructor to protect the singleton.
    MallocUtil() = default;
    ~MallocUtil() = default;
  private:
    DISALLOW_COPY_AND_ASSIGN(MallocUtil);
  };
} // namespace impala
