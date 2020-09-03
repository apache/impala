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

#include "util/metrics.h"

#include <boost/thread/shared_mutex.hpp>
#include <gperftools/malloc_extension.h>
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
#include <sanitizer/allocator_interface.h>
#endif

#include "gen-cpp/Frontend_types.h"

namespace impala {

class BufferPool;
class MemTracker;
class ReservationTracker;
class Thread;

/// Each names one of the fields in TJvmMemoryPool.
enum JvmMemoryMetricType {
  MAX,
  INIT,
  COMMITTED,
  CURRENT,
  PEAK_MAX,
  PEAK_INIT,
  PEAK_COMMITTED,
  PEAK_CURRENT
};

/// Memory metrics including TCMalloc and BufferPool memory.
class AggregateMemoryMetrics {
 public:
  /// The sum of Tcmalloc TOTAL_BYTES_RESERVED and BufferPool SYSTEM_ALLOCATED.
  /// Approximates the total amount of physical memory consumed by the backend (i.e. not
  /// including JVM memory), which is either in use by queries or cached by the BufferPool
  /// or the malloc implementation.
  /// TODO: IMPALA-691 - consider changing this to include JVM memory.
  static SumGauge* TOTAL_USED;

  /// The total number of virtual memory regions for the process.
  /// The value must be refreshed by calling Refresh().
  static IntGauge* NUM_MAPS;

  /// The total size of virtual memory regions for the process.
  /// The value must be refreshed by calling Refresh().
  static IntGauge* MAPPED_BYTES;

  /// The total RSS of all virtual memory regions for the process.
  /// The value must be refreshed by calling Refresh().
  static IntGauge* RSS;

  /// The total RSS of all virtual memory regions for the process.
  /// The value must be refreshed by calling Refresh().
  static IntGauge* ANON_HUGE_PAGE_BYTES;

  /// The string reporting the /enabled setting for transparent huge pages.
  /// The value must be refreshed by calling Refresh().
  static StringProperty* THP_ENABLED;

  /// The string reporting the /defrag setting for transparent huge pages.
  /// The value must be refreshed by calling Refresh().
  static StringProperty* THP_DEFRAG;

  /// The string reporting the khugepaged/defrag setting for transparent huge pages.
  /// The value must be refreshed by calling Refresh().
  static StringProperty* THP_KHUGEPAGED_DEFRAG;

  /// Refreshes values of any of the aggregate metrics that require refreshing.
  static void Refresh();
};

/// Specialised metric which exposes numeric properties from tcmalloc.
class TcmallocMetric : public IntGauge {
 public:
  /// Number of bytes allocated by tcmalloc, currently used by the application.
  static TcmallocMetric* BYTES_IN_USE;

  /// Number of bytes of system memory reserved by tcmalloc, including that in use by the
  /// application. Does not include what tcmalloc accounts for as 'malloc metadata' in
  /// /memz. That is, this is memory reserved by tcmalloc that the application can use.
  /// Includes unmapped virtual memory.
  static TcmallocMetric* TOTAL_BYTES_RESERVED;

  /// Number of bytes reserved and still mapped by tcmalloc that are not allocated to the
  /// application.
  static TcmallocMetric* PAGEHEAP_FREE_BYTES;

  /// Number of bytes once reserved by tcmalloc, but released back to the operating system
  /// so that their use incurs a pagefault. Contributes to the total amount of virtual
  /// address space used, but not to the physical memory usage.
  static TcmallocMetric* PAGEHEAP_UNMAPPED_BYTES;

  /// Derived metric computing the amount of physical memory (in bytes) used by the
  /// process, including that actually in use and free bytes reserved by tcmalloc. Does not
  /// include the tcmalloc metadata.
  class PhysicalBytesMetric : public IntGauge {
   public:
    PhysicalBytesMetric(const TMetricDef& def) : IntGauge(def, 0) { }

    virtual int64_t GetValue() override {
      return TOTAL_BYTES_RESERVED->GetValue() - PAGEHEAP_UNMAPPED_BYTES->GetValue();
    }
  };

  static PhysicalBytesMetric* PHYSICAL_BYTES_RESERVED;

  static TcmallocMetric* CreateAndRegister(MetricGroup* metrics, const std::string& key,
      const std::string& tcmalloc_var);

  virtual int64_t GetValue() override {
    int64_t retval = 0;
#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
    MallocExtension::instance()->GetNumericProperty(tcmalloc_var_.c_str(),
        reinterpret_cast<size_t*>(&retval));
#endif
    return retval;
  }

 private:
  /// Name of the tcmalloc property this metric should fetch.
  const std::string tcmalloc_var_;

  TcmallocMetric(const TMetricDef& def, const std::string& tcmalloc_var)
    : IntGauge(def, 0), tcmalloc_var_(tcmalloc_var) { }
};

/// Alternative to TCMallocMetric if we're running under a sanitizer that replaces
/// malloc(), e.g. address or thread sanitizer.
class SanitizerMallocMetric : public IntGauge {
 public:
  SanitizerMallocMetric(const TMetricDef& def) : IntGauge(def, 0) {}

  static SanitizerMallocMetric* BYTES_ALLOCATED;

  virtual int64_t GetValue() override {
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
    return __sanitizer_get_current_allocated_bytes();
#else
    return 0;
#endif

  }
};

// A singleton for caching the gathering of JVM Metrics for 1 second, to amortize
// the cost of getting all the JVM metrics.
//
// Clients should get the singleton via GetInstance() and call the
// Get* methods. Internally, the Get* methods are synchronized with
// lock_.
class JvmMetricCache {
  public:
    /// Retrieves a metric for a given pool.
    long GetPoolMetric(const std::string& mempool_name, JvmMemoryMetricType type);
    /// Retrieves a single counter metric from the response.
    long GetCounterMetric(int64_t(*accessor)(const TGetJvmMemoryMetricsResponse&));
    /// Returns all pool names.
    vector<string> GetPoolNames();
    /// Returns singleton instance.
    static JvmMetricCache* GetInstance();

  private:
    /// Updates metrics if over CACHE_PERIOD_MILLIS has elapsed. Thread safe.
    void GrabMetricsIfNecessary();

    /// A shared lock that allows low-overhead reads of last_fetch_ and last_response_.
    /// The write lock is only acquired when the CACHE_PERIOD_MILLIS timeout expires and
    /// last_response_ and last_fetch_ need to be updated.
    boost::shared_mutex lock_;

    /// Time when metrics were last fetched, using MonotonicMillis().
    /// Protected by lock_.
    int64_t last_fetch_ = 0;

    /// Last available metrics.
    /// Protected by lock_.
    TGetJvmMemoryMetricsResponse last_response_;

    static const int64_t CACHE_PERIOD_MILLIS = 1000;
    JvmMetricCache() { }
    DISALLOW_COPY_AND_ASSIGN(JvmMetricCache);
};

/// A JvmMemoryMetric corresponds to one value drawn from one 'memory pool' in the JVM. A
/// memory pool is an area of memory assigned for one particular aspect of memory
/// management. For example Hotspot has pools for the permanent generation, the old
/// generation, survivor space, code cache and permanently tenured objects.
class JvmMemoryMetric : public IntGauge {
 public:
  /// Adds a "jvm" child group to 'parent' and registers many Jvm memory metrics: one
  /// for every member of JvmMemoryMetricType for each pool (usually ~5 pools plus a
  /// synthetic 'total' pool).
  /// Idempotent but not thread-safe - can be safely called multiple times from the same
  /// thread.
  static void InitMetrics(MetricGroup* parent);

  /// Searches through jvm_metrics_response_ for a matching memory pool and pulls out the
  /// right value from that structure according to metric_type_.
  virtual int64_t GetValue() override;

  // Expose the total memory metrics that may be counted against process memory limit.
  // Initialised by InitMetrics().

  // The jvm.heap.max-usage-bytes metric.
  static JvmMemoryMetric* HEAP_MAX_USAGE;

  // The jvm.non-heap.committed-usage-bytes metric.
  static JvmMemoryMetric* NON_HEAP_COMMITTED;

 private:
  static JvmMemoryMetric* CreateAndRegister(MetricGroup* metrics, const std::string& key,
      const std::string& pool_name, JvmMemoryMetricType type);

  /// Private constructor to ensure only InitMetrics() can create JvmMemoryMetrics.
  JvmMemoryMetric(
      const TMetricDef& def, const std::string& mempool_name, JvmMemoryMetricType type);

  /// The name of the memory pool, defined by the Jvm.
  std::string mempool_name_;

  /// Each metric corresponds to one value; this tells us which value from the memory pool
  /// that is.
  JvmMemoryMetricType metric_type_;

  /// Set the first time that InitMetrics() is called.
  static bool initialized_;
};

// A counter that represents metrics about JVM Memory. It acesses the underlying
// data via JniUtil::GetJvmMemoryMetrics() via JvmMetricCache.
class JvmMemoryCounterMetric : public IntCounter {
  public:
    virtual int64_t GetValue() override;

    static JvmMemoryCounterMetric* GC_COUNT;
    static JvmMemoryCounterMetric* GC_TIME_MILLIS;
    static JvmMemoryCounterMetric* GC_NUM_WARN_THRESHOLD_EXCEEDED;
    static JvmMemoryCounterMetric* GC_NUM_INFO_THRESHOLD_EXCEEDED;
    static JvmMemoryCounterMetric* GC_TOTAL_EXTRA_SLEEP_TIME_MILLIS;

  private:
    friend class JvmMemoryMetric;
    static JvmMemoryCounterMetric* CreateAndRegister(MetricGroup* metrics,
        const string& key,
        int64_t(*accessor)(const TGetJvmMemoryMetricsResponse&));
    /// Private constructor; used via CreateAndRegister
    JvmMemoryCounterMetric(const TMetricDef& def,
      int64_t(*accessor)(const TGetJvmMemoryMetricsResponse&));

    int64_t(*accessor_)(const TGetJvmMemoryMetricsResponse&);
};

/// Metric that reports information about the buffer pool.
class BufferPoolMetric : public IntGauge {
 public:
  static Status InitMetrics(MetricGroup* metrics, ReservationTracker* global_reservations,
      BufferPool* buffer_pool) WARN_UNUSED_RESULT;

  /// Global metrics, initialized by InitMetrics().
  static BufferPoolMetric* LIMIT;
  static BufferPoolMetric* SYSTEM_ALLOCATED;
  static BufferPoolMetric* RESERVED;
  static BufferPoolMetric* UNUSED_RESERVATION_BYTES;
  static BufferPoolMetric* NUM_FREE_BUFFERS;
  static BufferPoolMetric* FREE_BUFFER_BYTES;
  static BufferPoolMetric* CLEAN_PAGES_LIMIT;
  static BufferPoolMetric* NUM_CLEAN_PAGES;
  static BufferPoolMetric* CLEAN_PAGE_BYTES;

  virtual int64_t GetValue() override;

 private:
  friend class ReservationTrackerTest;

  enum class BufferPoolMetricType {
    LIMIT, // Limit on memory allocated to buffers.
    // Total amount of buffer memory allocated from the system. Always <= LIMIT.
    SYSTEM_ALLOCATED,
    // Total of all buffer reservations. May be < SYSTEM_ALLOCATED if not all reservations
    // are fulfilled, or > SYSTEM_ALLOCATED because of additional memory cached by
    // BufferPool. Always <= LIMIT.
    RESERVED,
    // Total bytes of reservations that have not been used to allocate buffers from the
    // pool.
    UNUSED_RESERVATION_BYTES,
    NUM_FREE_BUFFERS, // Total number of free buffers in BufferPool.
    FREE_BUFFER_BYTES, // Total bytes of free buffers in BufferPool.
    CLEAN_PAGES_LIMIT, // Limit on number of clean pages in BufferPool.
    NUM_CLEAN_PAGES, // Total number of clean pages in BufferPool.
    CLEAN_PAGE_BYTES, // Total bytes of clean pages in BufferPool.
  };

  BufferPoolMetric(const TMetricDef& def, BufferPoolMetricType type,
      ReservationTracker* global_reservations, BufferPool* buffer_pool);

  BufferPoolMetricType type_;
  ReservationTracker* global_reservations_;
  BufferPool* buffer_pool_;
};

/// Metric that reports information about a MemTracker.
class MemTrackerMetric : public IntGauge {
 public:
  // Creates two new metrics tracking the current and peak usages of 'mem_tracker' in
  // the metrics group 'metrics'. The caller must make sure that 'mem_tracker' is not
  // destructed before 'metrics'.
  static void CreateMetrics(MetricGroup* metrics, MemTracker* mem_tracker,
      const std::string& name);

  virtual int64_t GetValue() override;

 private:
  enum class MemTrackerMetricType {
    CURRENT, // Current usage of the MemTracker
    PEAK, // Peak usage of the MemTracker
  };

  MemTrackerMetric(const TMetricDef& def, MemTrackerMetricType type,
      MemTracker* mem_tracker);

  const MemTrackerMetricType type_;
  const MemTracker* mem_tracker_;
};

/// Registers common tcmalloc memory metrics. If 'register_jvm_metrics' is true, the JVM
/// memory metrics are also registered. If 'global_reservations' and 'buffer_pool' are
/// not NULL, also register buffer pool metrics.
Status RegisterMemoryMetrics(MetricGroup* metrics, bool register_jvm_metrics,
    ReservationTracker* global_reservations, BufferPool* buffer_pool);
}
