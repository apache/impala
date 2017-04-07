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

#ifndef IMPALA_UTIL_MEM_METRICS_H
#define IMPALA_UTIL_MEM_METRICS_H

#include "util/metrics.h"

#include <boost/bind.hpp>
#include <boost/thread/mutex.hpp>
#include <gperftools/malloc_extension.h>
#ifdef ADDRESS_SANITIZER
#include <sanitizer/allocator_interface.h>
#endif

#include "gen-cpp/Frontend_types.h"
#include "util/debug-util.h"

namespace impala {

class BufferPool;
class ReservationTracker;
class Thread;

/// Memory metrics including TCMalloc and BufferPool memory.
class AggregateMemoryMetric {
 public:
  /// The sum of Tcmalloc TOTAL_BYTES_RESERVED and BufferPool SYSTEM_ALLOCATED.
  /// Approximates the total amount of physical memory consumed by the backend (i.e. not
  /// including JVM memory), which is either in use by queries or cached by the BufferPool
  /// or TcMalloc. NULL when running under ASAN.
  /// TODO: IMPALA-691 - consider changing this to include JVM memory.
  static SumGauge<uint64_t>* TOTAL_USED;
};

/// Specialised metric which exposes numeric properties from tcmalloc.
class TcmallocMetric : public UIntGauge {
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
  class PhysicalBytesMetric : public UIntGauge {
   public:
    PhysicalBytesMetric(const TMetricDef& def) : UIntGauge(def, 0) { }

   private:
    virtual void CalculateValue() {
      value_ = TOTAL_BYTES_RESERVED->value() - PAGEHEAP_UNMAPPED_BYTES->value();
    }
  };

  static PhysicalBytesMetric* PHYSICAL_BYTES_RESERVED;

  static TcmallocMetric* CreateAndRegister(MetricGroup* metrics, const std::string& key,
      const std::string& tcmalloc_var);

 private:
  /// Name of the tcmalloc property this metric should fetch.
  const std::string tcmalloc_var_;

  TcmallocMetric(const TMetricDef& def, const std::string& tcmalloc_var)
      : UIntGauge(def, 0), tcmalloc_var_(tcmalloc_var) { }

  virtual void CalculateValue() {
#ifndef ADDRESS_SANITIZER
    DCHECK_EQ(sizeof(size_t), sizeof(value_));
    MallocExtension::instance()->GetNumericProperty(tcmalloc_var_.c_str(),
        reinterpret_cast<size_t*>(&value_));
#endif
  }
};

/// Alternative to TCMallocMetric if we're running under Address Sanitizer, which
/// does not provide the same metrics.
class AsanMallocMetric : public UIntGauge {
 public:
  AsanMallocMetric(const TMetricDef& def) : UIntGauge(def, 0) {}
  static AsanMallocMetric* BYTES_ALLOCATED;
 private:
  virtual void CalculateValue() override {
#ifdef ADDRESS_SANITIZER
    value_ = __sanitizer_get_current_allocated_bytes();
#endif
  }
};

/// A JvmMetric corresponds to one value drawn from one 'memory pool' in the JVM. A memory
/// pool is an area of memory assigned for one particular aspect of memory management. For
/// example Hotspot has pools for the permanent generation, the old generation, survivor
/// space, code cache and permanently tenured objects.
class JvmMetric : public IntGauge {
 public:
  /// Registers many Jvm memory metrics: one for every member of JvmMetricType for each
  /// pool (usually ~5 pools plus a synthetic 'total' pool).
  static Status InitMetrics(MetricGroup* metrics);

 protected:
  /// Searches through jvm_metrics_response_ for a matching memory pool and pulls out the
  /// right value from that structure according to metric_type_.
  virtual void CalculateValue();

 private:
  /// Each names one of the fields in TJvmMemoryPool.
  enum JvmMetricType {
    MAX,
    INIT,
    COMMITTED,
    CURRENT,
    PEAK_MAX,
    PEAK_INIT,
    PEAK_COMMITTED,
    PEAK_CURRENT
  };

  static JvmMetric* CreateAndRegister(MetricGroup* metrics, const std::string& key,
      const std::string& pool_name, JvmMetric::JvmMetricType type);

  /// Private constructor to ensure only InitMetrics() can create JvmMetrics.
  JvmMetric(const TMetricDef& def, const std::string& mempool_name, JvmMetricType type);

  /// The name of the memory pool, defined by the Jvm.
  std::string mempool_name_;

  /// Each metric corresponds to one value; this tells us which value from the memory pool
  /// that is.
  JvmMetricType metric_type_;
};

/// Metric that reports information about the buffer pool.
class BufferPoolMetric : public UIntGauge {
 public:
  static Status InitMetrics(MetricGroup* metrics, ReservationTracker* global_reservations,
      BufferPool* buffer_pool);

  /// Global metrics, initialized by CreateAndRegisterMetrics().
  static BufferPoolMetric* LIMIT;
  static BufferPoolMetric* SYSTEM_ALLOCATED;
  static BufferPoolMetric* RESERVED;
  static BufferPoolMetric* NUM_FREE_BUFFERS;
  static BufferPoolMetric* FREE_BUFFER_BYTES;
  static BufferPoolMetric* NUM_CLEAN_PAGES;
  static BufferPoolMetric* CLEAN_PAGE_BYTES;

 protected:
  virtual void CalculateValue();

 private:
  enum class BufferPoolMetricType {
    LIMIT, // Limit on memory allocated to buffers.
    // Total amount of buffer memory allocated from the system. Always <= LIMIT.
    SYSTEM_ALLOCATED,
    // Total of all buffer reservations. May be < SYSTEM_ALLOCATED if not all reservations
    // are fulfilled, or > SYSTEM_ALLOCATED because of additional memory cached by
    // BufferPool. Always <= LIMIT.
    RESERVED,
    NUM_FREE_BUFFERS, // Total number of free buffers in BufferPool.
    FREE_BUFFER_BYTES, // Total bytes of free buffers in BufferPool.
    NUM_CLEAN_PAGES, // Total number of clean pages in BufferPool.
    CLEAN_PAGE_BYTES, // Total bytes of clean pages in BufferPool.
  };

  BufferPoolMetric(const TMetricDef& def, BufferPoolMetricType type,
      ReservationTracker* global_reservations, BufferPool* buffer_pool);

  BufferPoolMetricType type_;
  ReservationTracker* global_reservations_;
  BufferPool* buffer_pool_;
};

/// Registers common tcmalloc memory metrics. If 'register_jvm_metrics' is true, the JVM
/// memory metrics are also registered. If 'global_reservations' and 'buffer_pool' are
/// not NULL, also register buffer pool metrics.
Status RegisterMemoryMetrics(MetricGroup* metrics, bool register_jvm_metrics,
    ReservationTracker* global_reservations, BufferPool* buffer_pool);
}

#endif
