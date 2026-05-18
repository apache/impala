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

#include <memory>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <tcmalloc/malloc_extension.h>

#include "util/malloc-util.h"
#include "util/metrics.h"
#include "util/thread.h"

DECLARE_int64(tcmalloc_garbage_collection_chunk_size);
DECLARE_int32(googletcmalloc_max_per_cpu_cache_size);
DECLARE_uint64(googletcmalloc_memory_release_rate_bytes_per_sec);

namespace impala {

/// Specialised metric which exposes numeric properties from tcmalloc.
class GoogleTcmallocMetric : public IntGauge {
 public:
  // To help explain the stats, here is some descriptive text output from
  // tcmalloc:
  //
  // MALLOC:     2474426384 ( 2359.8 MiB) Bytes in use by application (#1)
  // MALLOC: +    150306816 (  143.3 MiB) Bytes in page heap freelist (#2)
  // MALLOC: +     75811440 (   72.3 MiB) Bytes in central cache freelist (#3)
  // MALLOC: +     20860912 (   19.9 MiB) Bytes in per-CPU cache freelist (#4)
  // MALLOC: +            0 (    0.0 MiB) Bytes in Sharded cache freelist
  // MALLOC: +      5032672 (    4.8 MiB) Bytes in transfer cache freelist (#5)
  // MALLOC: +        47792 (    0.0 MiB) Bytes in thread cache freelists (#6)
  // MALLOC: +     13975886 (   13.3 MiB) Bytes in malloc metadata
  // MALLOC: +      1773024 (    1.7 MiB) Bytes in malloc metadata Arena unallocated
  // MALLOC: +       903424 (    0.9 MiB) Bytes in malloc metadata Arena unavailable
  // MALLOC:   ------------
  // MALLOC: =   2743138350 ( 2616.1 MiB) Actual memory used (physical + swap) (#7**)
  // MALLOC: +   1098194944 ( 1047.3 MiB) Bytes released to OS (aka unmapped) (#8)
  // MALLOC:   ------------
  // MALLOC: =   3841333294 ( 3663.4 MiB) Virtual address space used (#9**)

  /// #1: Number of bytes allocated by tcmalloc, currently used by the application.
  static GoogleTcmallocMetric* BYTES_IN_USE;

  /// #2: Number of bytes reserved and still mapped by tcmalloc that are not allocated
  /// to the application.
  static GoogleTcmallocMetric* PAGEHEAP_FREE_BYTES;

  /// #3: Number of free bytes in the central cache assigned to size classes.
  static GoogleTcmallocMetric* CENTRAL_CACHE_FREE_BYTES;

  /// #4: Number of bytes used across all CPU caches
  static GoogleTcmallocMetric* CPU_CACHE_FREE_BYTES;

  /// #5: Number of free bytes waiting to be transferred between central and CPU caches.
  static GoogleTcmallocMetric* TRANSFER_CACHE_FREE_BYTES;

  /// #6: Number of bytes used across all thread caches
  static GoogleTcmallocMetric* CURRENT_TOTAL_THREAD_CACHE_BYTES;

  /// #7: Number of bytes of physical memory used by the process, including that actually
  /// in use and free bytes reserved by tcmalloc. This includes malloc metadata.
  /// **This is different. MallocExtension::GetNumericProperty() uses a different
  //  calculation to avoid a potentially expensive OS call.
  static GoogleTcmallocMetric* PHYSICAL_BYTES_RESERVED;

  /// #8: Number of bytes once reserved by tcmalloc, but released back to the operating
  /// system so that their use incurs a pagefault. Contributes to the total amount of
  /// virtual address space used, but not to the physical memory usage.
  static GoogleTcmallocMetric* PAGEHEAP_UNMAPPED_BYTES;

  /// #9: Number of bytes of system memory reserved by tcmalloc, including that in use by
  /// the application. This includes unmapped virtual memory and metadata.
  /// **This is different. MallocExtension::GetNumericProperty() uses a different
  //  calculation to avoid a potentially expensive OS call.
  static GoogleTcmallocMetric* TOTAL_BYTES_RESERVED;

  /// Derived metric computing the amount of memory (in bytes) used by tcmalloc for
  /// overhead (thread caches, unused memory in page heap, malloc metadata, etc).
  class OverheadBytesMetric : public IntGauge {
  public:
    OverheadBytesMetric(const TMetricDef& def) : IntGauge(def, 0) { }

    int64_t GetValue() override {
      return PHYSICAL_BYTES_RESERVED->GetValue() - BYTES_IN_USE->GetValue();
    }
  };

  static OverheadBytesMetric* OVERHEAD_BYTES;

  static GoogleTcmallocMetric* CreateAndRegister(MetricGroup* metrics,
      const std::string& key, const std::string& tcmalloc_var) {
    return metrics->RegisterMetric(new GoogleTcmallocMetric(MetricDefs::Get(key),
        tcmalloc_var));
  }

  int64_t GetValue() override {
    int64_t retval = 0;
    std::optional<size_t> retopt =
        tcmalloc::MallocExtension::GetNumericProperty(tcmalloc_var_.c_str());
    DCHECK(retopt.has_value());
    if (retopt.has_value()) retval = retopt.value();
    return retval;
  }

 private:
  /// Name of the tcmalloc property this metric should fetch.
  const std::string tcmalloc_var_;

  GoogleTcmallocMetric(const TMetricDef& def, const std::string& tcmalloc_var)
    : IntGauge(def, 0), tcmalloc_var_(tcmalloc_var) { }
};

GoogleTcmallocMetric* GoogleTcmallocMetric::BYTES_IN_USE = nullptr;
GoogleTcmallocMetric* GoogleTcmallocMetric::PAGEHEAP_FREE_BYTES = nullptr;
GoogleTcmallocMetric* GoogleTcmallocMetric::CENTRAL_CACHE_FREE_BYTES = nullptr;
GoogleTcmallocMetric* GoogleTcmallocMetric::CPU_CACHE_FREE_BYTES = nullptr;
GoogleTcmallocMetric* GoogleTcmallocMetric::TRANSFER_CACHE_FREE_BYTES = nullptr;
GoogleTcmallocMetric* GoogleTcmallocMetric::CURRENT_TOTAL_THREAD_CACHE_BYTES = nullptr;
GoogleTcmallocMetric* GoogleTcmallocMetric::PHYSICAL_BYTES_RESERVED = nullptr;
GoogleTcmallocMetric* GoogleTcmallocMetric::PAGEHEAP_UNMAPPED_BYTES = nullptr;
GoogleTcmallocMetric* GoogleTcmallocMetric::TOTAL_BYTES_RESERVED = nullptr;
GoogleTcmallocMetric::OverheadBytesMetric* GoogleTcmallocMetric::OVERHEAD_BYTES = nullptr;

class GoogleTcmallocMallocUtil : public MallocUtil {
 public:
  Status Init(int64_t process_mem_limit) override {
    if (initialized_) return Status::OK();

    // Verify that it is using the per-CPU caches. For now, error out if it isn't,
    // as that indicates a misconfiguration.
    if (!tcmalloc::MallocExtension::PerCpuCachesActive()) {
      return Status("Google TCMalloc is not using the per-CPU cache mode.");
    }

    // Set the per-CPU cache size
    tcmalloc::MallocExtension::SetMaxPerCpuCacheSize(
        FLAGS_googletcmalloc_max_per_cpu_cache_size);
    // Get the value back from tcmalloc and make sure it was set.
    int32_t cpu_cache_size = tcmalloc::MallocExtension::GetMaxPerCpuCacheSize();
    DCHECK_EQ(cpu_cache_size, FLAGS_googletcmalloc_max_per_cpu_cache_size);

    // Set the memory release rate
    tcmalloc::MallocExtension::SetBackgroundReleaseRate(
        static_cast<tcmalloc::MallocExtension::BytesPerSecond>(
            FLAGS_googletcmalloc_memory_release_rate_bytes_per_sec));
    // Get the value back from tcmalloc and make sure it was set.
    tcmalloc::MallocExtension::BytesPerSecond release_rate =
        tcmalloc::MallocExtension::GetBackgroundReleaseRate();
    DCHECK_EQ(static_cast<size_t>(release_rate),
        FLAGS_googletcmalloc_memory_release_rate_bytes_per_sec);

    // Start the background maintenance thread that frees memory
    RETURN_IF_ERROR(Thread::Create("malloc-util", "gc_thread",
        [this]() { this->GarbageCollectorThread(); }, &gc_thread_));

    initialized_ = true;
    return Status::OK();
  }

  void ReleaseMemoryToSystem(int64_t bytes_to_free) override {
    int64_t extra = bytes_to_free;
    while (extra > 0) {
      // Tcmalloc holds the page heap lock while releasing the memory, so release in
      // chunks to avoid holding the lock for an extended period of time. This does
      // not call sched_yield() between calls, because the kernel already gave us a
      // time slice and sched_yield() is often a no-op in that case.
      int64_t amount_to_release =
        std::min(FLAGS_tcmalloc_garbage_collection_chunk_size, extra);
      tcmalloc::MallocExtension::ReleaseMemoryToSystem(amount_to_release);
      extra -= amount_to_release;
    }
  }

  std::string GetTextDescription() const override {
    // The google tcmalloc GetStats() output is ridiculously verbose.
    // Limit ourselves to the summary at the start of the string by
    // chopping off everything after. Ideally, google tcmalloc would have
    // a mode to return less output.
    std::string s = tcmalloc::MallocExtension::GetStats();
    // This string comes at the end of the summary. Everything after this
    // is extremely detailed.
    size_t end_of_summary = s.find("Call ReleaseMemoryToSystem()");
    DCHECK_NE(end_of_summary, std::string::npos);
    return std::string(s, 0, end_of_summary);
  }

  std::string GetName() const override { return "Google TCMalloc"; }

  Status RegisterMemoryMetrics(MetricGroup* metrics) override {
    DCHECK(initialized_);
    MetricGroup* tcmalloc_metrics = metrics->GetOrCreateChildGroup("tcmalloc");
    // To help explain the stats, here is some descriptive text output from
    // tcmalloc:
    //
    // MALLOC:     2474426384 ( 2359.8 MiB) Bytes in use by application (#1)
    // MALLOC: +    150306816 (  143.3 MiB) Bytes in page heap freelist (#2)
    // MALLOC: +     75811440 (   72.3 MiB) Bytes in central cache freelist (#3)
    // MALLOC: +     20860912 (   19.9 MiB) Bytes in per-CPU cache freelist (#4)
    // MALLOC: +            0 (    0.0 MiB) Bytes in Sharded cache freelist
    // MALLOC: +      5032672 (    4.8 MiB) Bytes in transfer cache freelist (#5)
    // MALLOC: +        47792 (    0.0 MiB) Bytes in thread cache freelists (#6)
    // MALLOC: +     13975886 (   13.3 MiB) Bytes in malloc metadata
    // MALLOC: +      1773024 (    1.7 MiB) Bytes in malloc metadata Arena unallocated
    // MALLOC: +       903424 (    0.9 MiB) Bytes in malloc metadata Arena unavailable
    // MALLOC:   ------------
    // MALLOC: =   2743138350 ( 2616.1 MiB) Actual memory used (physical + swap) (#7**)
    // MALLOC: +   1098194944 ( 1047.3 MiB) Bytes released to OS (aka unmapped) (#8)
    // MALLOC:   ------------
    // MALLOC: =   3841333294 ( 3663.4 MiB) Virtual address space used (#9**)
    // #1
    GoogleTcmallocMetric::BYTES_IN_USE = GoogleTcmallocMetric::CreateAndRegister(
        tcmalloc_metrics, "tcmalloc.bytes-in-use", "generic.current_allocated_bytes");

    // #2
    GoogleTcmallocMetric::PAGEHEAP_FREE_BYTES = GoogleTcmallocMetric::CreateAndRegister(
        tcmalloc_metrics, "tcmalloc.pageheap-free-bytes",
        "tcmalloc.pageheap_free_bytes");

    // #3
    GoogleTcmallocMetric::CENTRAL_CACHE_FREE_BYTES =
        GoogleTcmallocMetric::CreateAndRegister(tcmalloc_metrics,
            "tcmalloc.central-cache-free-bytes", "tcmalloc.central_cache_free");

    // #4
    GoogleTcmallocMetric::CPU_CACHE_FREE_BYTES =
        GoogleTcmallocMetric::CreateAndRegister(tcmalloc_metrics,
            "tcmalloc.cpu-cache-free-bytes", "tcmalloc.cpu_free");

    // #5
    GoogleTcmallocMetric::TRANSFER_CACHE_FREE_BYTES =
        GoogleTcmallocMetric::CreateAndRegister(tcmalloc_metrics,
            "tcmalloc.transfer-cache-free-bytes", "tcmalloc.transfer_cache_free");

    // #6
    GoogleTcmallocMetric::CURRENT_TOTAL_THREAD_CACHE_BYTES =
      GoogleTcmallocMetric::CreateAndRegister(tcmalloc_metrics,
          "tcmalloc.current-total-thread-cache-bytes",
          "tcmalloc.current_total_thread_cache_bytes");

    // #7** This is different. MallocExtension::GetNumericProperty() uses a different
    //      calculation to avoid a potentially expensive OS call.
    GoogleTcmallocMetric::PHYSICAL_BYTES_RESERVED =
        GoogleTcmallocMetric::CreateAndRegister(tcmalloc_metrics,
            "tcmalloc.physical-bytes-reserved", "generic.physical_memory_used");

    // #8
    GoogleTcmallocMetric::PAGEHEAP_UNMAPPED_BYTES =
        GoogleTcmallocMetric::CreateAndRegister(tcmalloc_metrics,
            "tcmalloc.pageheap-unmapped-bytes", "tcmalloc.pageheap_unmapped_bytes");

    // #9** This is different. MallocExtension::GetNumericProperty() uses a different
    //      calculation to avoid a potentially expensive OS call.
    GoogleTcmallocMetric::TOTAL_BYTES_RESERVED =
        GoogleTcmallocMetric::CreateAndRegister(tcmalloc_metrics,
            "tcmalloc.total-bytes-reserved", "generic.virtual_memory_used");


    // Additional overhead metric (#7 - #1). This is higher than what the /memz output
    // would show, because #7 is calculated differently through the
    // MallocExtension::GetNumericProperty() API.
    GoogleTcmallocMetric::OVERHEAD_BYTES =
        tcmalloc_metrics->RegisterMetric(new GoogleTcmallocMetric::OverheadBytesMetric(
            MetricDefs::Get("tcmalloc.overhead-bytes")));
    return Status::OK();
  }

  IntGauge* GetUsedBytesMetric(bool include_overhead) const override {
    DCHECK(initialized_);
    if (include_overhead) {
      DCHECK(GoogleTcmallocMetric::PHYSICAL_BYTES_RESERVED != nullptr);
      return GoogleTcmallocMetric::PHYSICAL_BYTES_RESERVED;
    } else {
      DCHECK(GoogleTcmallocMetric::BYTES_IN_USE != nullptr);
      return GoogleTcmallocMetric::BYTES_IN_USE;
    }
  }

  IntGauge* GetOverheadBytesMetric() const override {
    DCHECK(initialized_);
    DCHECK(GoogleTcmallocMetric::OVERHEAD_BYTES != nullptr);
    return GoogleTcmallocMetric::OVERHEAD_BYTES;
  }

  HugePageSupport GetHugePageSupport() const override {
    DCHECK(initialized_);
    return HugePageSupport::MADVISE_UNNECESSARY;
  }

private:
  std::unique_ptr<Thread> gc_thread_;
  bool initialized_ = false;

  [[noreturn]] void GarbageCollectorThread() {
    // Background process actions better be enabled (this is sensitive to how google
    // tcmalloc gets built).
    DCHECK(tcmalloc::MallocExtension::GetBackgroundProcessActionsEnabled());
    DCHECK(tcmalloc::MallocExtension::NeedsProcessBackgroundActions());
    // Never returns:
    tcmalloc::MallocExtension::ProcessBackgroundActions();
    DCHECK(false) << "TCMalloc ProcessBackgroundActions() should not return.";
  }
};

} // namespace impala
