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

#include <algorithm>
#include <memory>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>

#include "gutil/strings/substitute.h"
#include "util/malloc-util.h"
#include "util/metrics.h"
#include "util/parse-util.h"
#include "util/thread.h"
#include "util/time.h"

DECLARE_int64(tcmalloc_max_total_thread_cache_bytes);
DECLARE_bool(tcmalloc_aggressive_memory_decommit);
DECLARE_string(tcmalloc_max_free_bytes);
DECLARE_int64(tcmalloc_garbage_collection_chunk_size);

using strings::Substitute;

namespace impala {

/// Specialised metric which exposes numeric properties from tcmalloc.
/// These metrics line up with the text description used for /memz:
/// MALLOC:     3388499040 ( 3231.5 MiB) Bytes in use by application (#1)
/// MALLOC: +            0 (    0.0 MiB) Bytes in page heap freelist (#2)
/// MALLOC: +     98309992 (   93.8 MiB) Bytes in central cache freelist (#3)
/// MALLOC: +      4746496 (    4.5 MiB) Bytes in transfer cache freelist (#4)
/// MALLOC: +    177297208 (  169.1 MiB) Bytes in thread cache freelists (#5)
/// MALLOC: +     14942208 (   14.2 MiB) Bytes in malloc metadata
/// MALLOC:   ------------
/// MALLOC: =   3683794944 ( 3513.1 MiB) Actual memory used (physical + swap) (#6)
/// MALLOC: +    149757952 (  142.8 MiB) Bytes released to OS (aka unmapped) (#7)
/// MALLOC:   ------------
/// MALLOC: =   3833552896 ( 3656.0 MiB) Virtual address space used (#8)
class TcmallocMetric : public IntGauge {
 public:
  /// #1: Number of bytes allocated by tcmalloc, currently used by the application.
  static TcmallocMetric* BYTES_IN_USE;

  /// #2: Number of bytes reserved and still mapped by tcmalloc that are not allocated
  /// to the application.
  static TcmallocMetric* PAGEHEAP_FREE_BYTES;

  /// #3: Number of free bytes in the central cache assigned to size classes.
  static TcmallocMetric* CENTRAL_CACHE_FREE_BYTES;

  /// #4: Number of free bytes waiting to be transferred between central and thread
  /// caches.
  static TcmallocMetric* TRANSFER_CACHE_FREE_BYTES;

  /// #5: Number of bytes used across all thread caches.
  static TcmallocMetric* CURRENT_TOTAL_THREAD_CACHE_BYTES;

  /// #6: Number of physical bytes in use (including any tcmalloc caches and metadata)
  static TcmallocMetric* PHYSICAL_BYTES_RESERVED;

  /// #7: Number of bytes once reserved by tcmalloc, but released back to the operating
  /// system so that their use incurs a pagefault. Contributes to the total amount of
  /// virtual address space used, but not to the physical memory usage.
  static TcmallocMetric* PAGEHEAP_UNMAPPED_BYTES;

  /// #8: Derived metric computing the amount of virtual memory (in bytes) used by the
  /// process, including all different types of tcmalloc memory including unmapped
  /// virtual memory.
  class TotalBytesReservedMetric : public IntGauge {
  public:
    TotalBytesReservedMetric(const TMetricDef& def) : IntGauge(def, 0) { }

    virtual int64_t GetValue() override {
      return PHYSICAL_BYTES_RESERVED->GetValue() + PAGEHEAP_UNMAPPED_BYTES->GetValue();
    }
  };
  static TotalBytesReservedMetric* TOTAL_BYTES_RESERVED;

  /// Derived metric computing the amount of memory (in bytes) used by tcmalloc for
  /// overhead (thread caches, unused memory in page heap, etc).
  class OverheadBytesMetric : public IntGauge {
  public:
    OverheadBytesMetric(const TMetricDef& def) : IntGauge(def, 0) { }

    int64_t GetValue() override {
      return PHYSICAL_BYTES_RESERVED->GetValue() - BYTES_IN_USE->GetValue();
    }
  };

  static OverheadBytesMetric* OVERHEAD_BYTES;

  static TcmallocMetric* CreateAndRegister(MetricGroup* metrics, const std::string& key,
      const std::string& tcmalloc_var) {
    return metrics->RegisterMetric(
        new TcmallocMetric(MetricDefs::Get(key), tcmalloc_var));
  }

  int64_t GetValue() override {
    int64_t retval = 0;
    MallocExtension::instance()->GetNumericProperty(tcmalloc_var_.c_str(),
      reinterpret_cast<size_t*>(&retval));
    return retval;
  }

 private:
  /// Name of the tcmalloc property this metric should fetch.
  const std::string tcmalloc_var_;

  TcmallocMetric(const TMetricDef& def, const std::string& tcmalloc_var)
    : IntGauge(def, 0), tcmalloc_var_(tcmalloc_var) { }
};

TcmallocMetric* TcmallocMetric::BYTES_IN_USE = nullptr;
TcmallocMetric* TcmallocMetric::PAGEHEAP_FREE_BYTES = nullptr;
TcmallocMetric* TcmallocMetric::CENTRAL_CACHE_FREE_BYTES = nullptr;
TcmallocMetric* TcmallocMetric::TRANSFER_CACHE_FREE_BYTES = nullptr;
TcmallocMetric* TcmallocMetric::CURRENT_TOTAL_THREAD_CACHE_BYTES = nullptr;
TcmallocMetric* TcmallocMetric::PHYSICAL_BYTES_RESERVED = nullptr;
TcmallocMetric* TcmallocMetric::PAGEHEAP_UNMAPPED_BYTES = nullptr;
TcmallocMetric::TotalBytesReservedMetric* TcmallocMetric::TOTAL_BYTES_RESERVED = nullptr;
TcmallocMetric::OverheadBytesMetric* TcmallocMetric::OVERHEAD_BYTES = nullptr;

class GperftoolsMallocUtil : public MallocUtil {
 public:
  Status Init(int64_t process_mem_limit) override {
    // Some backend tests call this multiple times
    if (initialized_) return Status::OK();

    if (FLAGS_tcmalloc_aggressive_memory_decommit) {
      // By default tcmalloc does not use aggressive decommit. Set it if it is enabled.
      MallocExtension::instance()->SetNumericProperty(
          "tcmalloc.aggressive_memory_decommit", 1);
    }

    const static char* TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES =
      "tcmalloc.max_total_thread_cache_bytes";
    // Change the total TCMalloc thread cache size if necessary.
    if (FLAGS_tcmalloc_max_total_thread_cache_bytes > 0 &&
        !MallocExtension::instance()->SetNumericProperty(
            TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES,
            FLAGS_tcmalloc_max_total_thread_cache_bytes)) {
      return Status(Substitute("Failed to change {0}",
          TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES));
    }
    // Read the value back from tcmalloc to verify it matches what we set.
    size_t actual_max_total_thread_cache_bytes = 0;
    bool retval = MallocExtension::instance()->GetNumericProperty(
      TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES,
      &actual_max_total_thread_cache_bytes);
    if (!retval) {
      return Status(Substitute("Could not retrieve value of {0}.",
          TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES));
    }
    if (actual_max_total_thread_cache_bytes !=
        FLAGS_tcmalloc_max_total_thread_cache_bytes) {
      LOG(WARNING) << "Set " << TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES << " to "
                   << FLAGS_tcmalloc_max_total_thread_cache_bytes << " bytes but "
                   << "actually using " << actual_max_total_thread_cache_bytes
                   << " bytes.";
    }

    // Start the background garbage collector thread if aggressive decommit
    // is disabled.
    if (!FLAGS_tcmalloc_aggressive_memory_decommit) {
      // Determine the maximum overhead
      bool is_percent;
      max_overhead_ = ParseUtil::ParseMemSpec(FLAGS_tcmalloc_max_free_bytes,
                                                      &is_percent, process_mem_limit);
      if (max_overhead_ <= 0) {
        if (process_mem_limit <= 0) {
          // If the process_mem_limit is not specified, then this cannot accept a
          // percentage value.
          return Status(Substitute("Invalid --tcmalloc_max_free_bytes value, must be a "
              "positive bytes value: $0", FLAGS_tcmalloc_max_free_bytes));
        } else {
          return Status(Substitute("Invalid --tcmalloc_max_free_bytes value, must be a "
              "positive bytes value or percentage: $0", FLAGS_tcmalloc_max_free_bytes));
        }
      }
      LOG(INFO) << "TCMalloc max overhead = " << max_overhead_;

      RETURN_IF_ERROR(Thread::Create("malloc-util", "gc_thread",
          [this]() { this->GarbageCollectorThread(); }, &gc_thread_));
    }
    initialized_ = true;
    return Status::OK();
  }

  void ReleaseMemoryToSystem(int64_t bytes_to_free) override {
    // If aggressive memory decommit is enabled, tcmalloc is not holding on to large
    // amounts of memory, so there is no need to manually release it.
    if (FLAGS_tcmalloc_aggressive_memory_decommit) return;

    int64_t extra = bytes_to_free;
    while (extra > 0) {
      // Tcmalloc holds the page heap lock while releasing the memory, so release in
      // chunks to avoid holding the lock for an extended period of time. This does
      // not call sched_yield() between calls, because the kernel already gave us a
      // time slice and sched_yield() is often a no-op in that case.
      int64_t amount_to_release =
        std::min(FLAGS_tcmalloc_garbage_collection_chunk_size, extra);
      MallocExtension::instance()->ReleaseToSystem(amount_to_release);
      extra -= amount_to_release;
    }
  }

  std::string GetTextDescription() const override {
    char buf[2048];
    MallocExtension::instance()->GetStats(buf, 2048);
    return std::string(buf);
  }

  std::string GetName() const override { return "TCMalloc"; }

  Status RegisterMemoryMetrics(MetricGroup* metrics) override {
    DCHECK(initialized_);
    MetricGroup* tcmalloc_metrics = metrics->GetOrCreateChildGroup("tcmalloc");

    /// These metrics line up with the text description used for /memz:
    /// MALLOC:     3388499040 ( 3231.5 MiB) Bytes in use by application (#1)
    /// MALLOC: +            0 (    0.0 MiB) Bytes in page heap freelist (#2)
    /// MALLOC: +     98309992 (   93.8 MiB) Bytes in central cache freelist (#3)
    /// MALLOC: +      4746496 (    4.5 MiB) Bytes in transfer cache freelist (#4)
    /// MALLOC: +    177297208 (  169.1 MiB) Bytes in thread cache freelists (#5)
    /// MALLOC: +     14942208 (   14.2 MiB) Bytes in malloc metadata
    /// MALLOC:   ------------
    /// MALLOC: =   3683794944 ( 3513.1 MiB) Actual memory used (physical + swap) (#6)
    /// MALLOC: +    149757952 (  142.8 MiB) Bytes released to OS (aka unmapped) (#7)
    /// MALLOC:   ------------
    /// MALLOC: =   3833552896 ( 3656.0 MiB) Virtual address space used (#8)

    /// #1
    TcmallocMetric::BYTES_IN_USE = TcmallocMetric::CreateAndRegister(
        tcmalloc_metrics, "tcmalloc.bytes-in-use", "generic.current_allocated_bytes");

    /// #2
    TcmallocMetric::PAGEHEAP_FREE_BYTES = TcmallocMetric::CreateAndRegister(
        tcmalloc_metrics, "tcmalloc.pageheap-free-bytes", "tcmalloc.pageheap_free_bytes");

    /// #3
    TcmallocMetric::CENTRAL_CACHE_FREE_BYTES = TcmallocMetric::CreateAndRegister(
        tcmalloc_metrics, "tcmalloc.central-cache-free-bytes",
        "tcmalloc.central_cache_free_bytes");

    /// #4
    TcmallocMetric::TRANSFER_CACHE_FREE_BYTES = TcmallocMetric::CreateAndRegister(
        tcmalloc_metrics, "tcmalloc.transfer-cache-free-bytes",
        "tcmalloc.transfer_cache_free_bytes");

    /// #5
    TcmallocMetric::CURRENT_TOTAL_THREAD_CACHE_BYTES = TcmallocMetric::CreateAndRegister(
        tcmalloc_metrics, "tcmalloc.current-total-thread-cache-bytes",
        "tcmalloc.current_total_thread_cache_bytes");

    /// #6
    TcmallocMetric::PHYSICAL_BYTES_RESERVED = TcmallocMetric::CreateAndRegister(
        tcmalloc_metrics, "tcmalloc.physical-bytes-reserved",
        "generic.total_physical_bytes");

    /// #7
    TcmallocMetric::PAGEHEAP_UNMAPPED_BYTES =
        TcmallocMetric::CreateAndRegister(tcmalloc_metrics,
            "tcmalloc.pageheap-unmapped-bytes", "tcmalloc.pageheap_unmapped_bytes");

    /// #8
    TcmallocMetric::TOTAL_BYTES_RESERVED =
        tcmalloc_metrics->RegisterMetric(new TcmallocMetric::TotalBytesReservedMetric(
            MetricDefs::Get("tcmalloc.total-bytes-reserved")));

    /// Additional overhead metric (#6 - #1)
    TcmallocMetric::OVERHEAD_BYTES =
        tcmalloc_metrics->RegisterMetric(new TcmallocMetric::OverheadBytesMetric(
            MetricDefs::Get("tcmalloc.overhead-bytes")));

    return Status::OK();
  }

  IntGauge* GetUsedBytesMetric(bool include_overhead) const override {
    DCHECK(initialized_);
    if (include_overhead) {
      DCHECK(TcmallocMetric::PHYSICAL_BYTES_RESERVED != nullptr);
      return TcmallocMetric::PHYSICAL_BYTES_RESERVED;
    } else {
      DCHECK(TcmallocMetric::BYTES_IN_USE != nullptr);
      return TcmallocMetric::BYTES_IN_USE;
    }
  }

  IntGauge* GetOverheadBytesMetric() const override {
    DCHECK(initialized_);
    DCHECK(TcmallocMetric::OVERHEAD_BYTES != nullptr);
    return TcmallocMetric::OVERHEAD_BYTES;
  }

  HugePageSupport GetHugePageSupport() const override {
    DCHECK(initialized_);
    // As an extra precaution, read the actual value from tcmalloc rather than simply
    // reading the startup parameter.
    size_t aggressive_decommit_enabled;
    MallocExtension::instance()->GetNumericProperty(
        "tcmalloc.aggressive_memory_decommit", &aggressive_decommit_enabled);
    if (FLAGS_tcmalloc_aggressive_memory_decommit) {
      DCHECK(aggressive_decommit_enabled);
    } else {
      DCHECK(!aggressive_decommit_enabled);
    }
    if (aggressive_decommit_enabled) {
      // With aggressive decommit, large allocations are immediately freed,
      // so it is compatible with using madvise to get huge pages.
      return HugePageSupport::MADVISE_COMPATIBLE;
    } else {
      return HugePageSupport::MADVISE_INCOMPATIBLE;
    }
  }

  bool SupportsHeapProfiling() const override { return true; }
  void HeapProfilerStart(const std::string& tmp_prof_file_name) override {
    ::HeapProfilerStart(tmp_prof_file_name.c_str());
  }

  void HeapProfilerStop() override {
    ::HeapProfilerStop();
  }

  char* GetHeapProfile() override {
    return ::GetHeapProfile();
  }

  bool SupportsHeapGrowthStacks() const override { return true; }
  void GetHeapGrowthStacks(std::string* heap_growth_stacks) override {
    MallocExtension::instance()->GetHeapGrowthStacks(heap_growth_stacks);
  }

  bool SupportsCPUProfiling() const override { return true; }
  void CPUProfilerStart(const std::string& tmp_prof_file_name) override {
    ::ProfilerStart(tmp_prof_file_name.c_str());
  }
  void CPUProfilerStop() override {
    ::ProfilerStop();
  }

private:
  int64_t max_overhead_;
  std::unique_ptr<Thread> gc_thread_;
  bool initialized_ = false;

  // When using TCMalloc with aggressive decommit off, TCMalloc accumulates memory
  // indefinitely unless we manually garbage collect it. TCMalloc releases memory
  // every N deletes, where N is based on the TCMALLOC_RELEASE_RATE property.
  // When TCMalloc decides to release memory, it removes a single span from the
  // page heap. This means that there are certain allocation patterns that can lead
  // to continuous accumulation of memory. One example is continually resizing a
  // vector, which results in many allocations. Even after the vector goes out of
  // scope, it will not release all the memory unless there are enough other deletions
  // occuring in the system. Impala must have these types of memory patterns, as
  // our experience is that even high TCMALLOC_RELEASE_RATE settings do not bound
  // memory use.
  //
  // This background thread frees memory periodically to keep TCMalloc's memory overhead
  // limited. To smooth out the release, this keeps track of the last N samples and
  // frees memory based on the minimum sample. This avoids releasing memory that will
  // be reused quickly.
  static constexpr int GARBAGE_COLLECTOR_HISTORY_SIZE = 10;
  [[noreturn]] void GarbageCollectorThread() {
    DCHECK(!FLAGS_tcmalloc_aggressive_memory_decommit);
    // Initialize the history to all zeros
    std::vector<int64_t> bytes_overhead_history(GARBAGE_COLLECTOR_HISTORY_SIZE, 0);
    int cur_history_index = 0;
    while (true) {
      // Number of bytes in the 'NORMAL' free list (i.e. reserved by tcmalloc but
      // not in use).
      if (TcmallocMetric::PAGEHEAP_FREE_BYTES != nullptr) {
        bytes_overhead_history[cur_history_index] =
            TcmallocMetric::PAGEHEAP_FREE_BYTES->GetValue();
        cur_history_index++;
        if (cur_history_index == bytes_overhead_history.size()) {
          cur_history_index = 0;
        }
      }

      // Free based on the minimum overhead in the history. This avoids releasing memory
      // that will be reused very quickly. However, the max_overhead is usually a fairly
      // substantial amount of memory, so it is ok to aggressively enforce the limit.
      int64_t min_bytes_overhead = *std::min_element(bytes_overhead_history.cbegin(),
          bytes_overhead_history.cend());
      if (min_bytes_overhead > max_overhead_) {
        ReleaseMemoryToSystem(min_bytes_overhead - max_overhead_);
      }
      SleepForMs(1000);
    }
  }
};

} // namespace impala
