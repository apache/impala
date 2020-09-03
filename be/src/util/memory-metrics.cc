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

#include "util/memory-metrics.h"

#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/mem-tracker.h"
#include "util/jni-util.h"
#include "util/mem-info.h"
#include "util/process-state-info.h"
#include "util/time.h"

using boost::algorithm::to_lower;
using boost::shared_lock;
using boost::shared_mutex;
using boost::unique_lock;

using namespace impala;
using namespace strings;

DECLARE_bool(mem_limit_includes_jvm);
DECLARE_bool(mmap_buffers);
DEFINE_bool_hidden(enable_extended_memory_metrics, false,
    "(Experimental) enable extended memory metrics, including those that can be "
    "expensive to compute. This was introduced as a workaround for poor /proc/*/smaps "
    "performance in certain Linux kernel versions - see IMPALA-7239.");

SumGauge* AggregateMemoryMetrics::TOTAL_USED = nullptr;
IntGauge* AggregateMemoryMetrics::NUM_MAPS = nullptr;
IntGauge* AggregateMemoryMetrics::MAPPED_BYTES = nullptr;
IntGauge* AggregateMemoryMetrics::RSS = nullptr;
IntGauge* AggregateMemoryMetrics::ANON_HUGE_PAGE_BYTES = nullptr;
StringProperty* AggregateMemoryMetrics::THP_ENABLED = nullptr;
StringProperty* AggregateMemoryMetrics::THP_DEFRAG = nullptr;
StringProperty* AggregateMemoryMetrics::THP_KHUGEPAGED_DEFRAG = nullptr;

TcmallocMetric* TcmallocMetric::BYTES_IN_USE = nullptr;
TcmallocMetric* TcmallocMetric::PAGEHEAP_FREE_BYTES = nullptr;
TcmallocMetric* TcmallocMetric::TOTAL_BYTES_RESERVED = nullptr;
TcmallocMetric* TcmallocMetric::PAGEHEAP_UNMAPPED_BYTES = nullptr;
TcmallocMetric::PhysicalBytesMetric* TcmallocMetric::PHYSICAL_BYTES_RESERVED = nullptr;

SanitizerMallocMetric* SanitizerMallocMetric::BYTES_ALLOCATED = nullptr;

bool JvmMemoryMetric::initialized_ = false;
JvmMemoryMetric* JvmMemoryMetric::HEAP_MAX_USAGE = nullptr;
JvmMemoryMetric* JvmMemoryMetric::NON_HEAP_COMMITTED = nullptr;

JvmMemoryCounterMetric* JvmMemoryCounterMetric::GC_COUNT = nullptr;
JvmMemoryCounterMetric* JvmMemoryCounterMetric::GC_TIME_MILLIS = nullptr;
JvmMemoryCounterMetric* JvmMemoryCounterMetric::GC_NUM_WARN_THRESHOLD_EXCEEDED = nullptr;
JvmMemoryCounterMetric* JvmMemoryCounterMetric::GC_NUM_INFO_THRESHOLD_EXCEEDED = nullptr;
JvmMemoryCounterMetric* JvmMemoryCounterMetric::GC_TOTAL_EXTRA_SLEEP_TIME_MILLIS =
    nullptr;

BufferPoolMetric* BufferPoolMetric::LIMIT = nullptr;
BufferPoolMetric* BufferPoolMetric::SYSTEM_ALLOCATED = nullptr;
BufferPoolMetric* BufferPoolMetric::RESERVED = nullptr;
BufferPoolMetric* BufferPoolMetric::UNUSED_RESERVATION_BYTES = nullptr;
BufferPoolMetric* BufferPoolMetric::NUM_FREE_BUFFERS = nullptr;
BufferPoolMetric* BufferPoolMetric::FREE_BUFFER_BYTES = nullptr;
BufferPoolMetric* BufferPoolMetric::CLEAN_PAGES_LIMIT = nullptr;
BufferPoolMetric* BufferPoolMetric::NUM_CLEAN_PAGES = nullptr;
BufferPoolMetric* BufferPoolMetric::CLEAN_PAGE_BYTES = nullptr;

TcmallocMetric* TcmallocMetric::CreateAndRegister(
    MetricGroup* metrics, const string& key, const string& tcmalloc_var) {
  return metrics->RegisterMetric(new TcmallocMetric(MetricDefs::Get(key), tcmalloc_var));
}

Status impala::RegisterMemoryMetrics(MetricGroup* metrics, bool register_jvm_metrics,
    ReservationTracker* global_reservations, BufferPool* buffer_pool) {
  if (global_reservations != nullptr) {
    DCHECK(buffer_pool != nullptr);
    RETURN_IF_ERROR(BufferPoolMetric::InitMetrics(
        metrics->GetOrCreateChildGroup("buffer-pool"), global_reservations, buffer_pool));
  }

  // Add compound metrics that track totals across malloc and the buffer pool.
  // total-used should track the total physical memory in use.
  vector<IntGauge*> used_metrics;
  if (FLAGS_mmap_buffers && global_reservations != nullptr) {
    // If we mmap() buffers, the buffers are not allocated via malloc. Ensure they are
    // properly tracked.
    used_metrics.push_back(BufferPoolMetric::SYSTEM_ALLOCATED);
  }
  if (register_jvm_metrics) {
    JvmMemoryMetric::InitMetrics(metrics);
    if (FLAGS_mem_limit_includes_jvm) {
      used_metrics.push_back(JvmMemoryMetric::HEAP_MAX_USAGE);
      used_metrics.push_back(JvmMemoryMetric::NON_HEAP_COMMITTED);
    }
  }
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
  SanitizerMallocMetric::BYTES_ALLOCATED = metrics->RegisterMetric(
      new SanitizerMallocMetric(MetricDefs::Get("sanitizer-total-bytes-allocated")));
  used_metrics.push_back(SanitizerMallocMetric::BYTES_ALLOCATED);
#else
  MetricGroup* tcmalloc_metrics = metrics->GetOrCreateChildGroup("tcmalloc");
  // We rely on TCMalloc for our global memory metrics, so skip setting them up
  // if we're not using TCMalloc.
  TcmallocMetric::BYTES_IN_USE = TcmallocMetric::CreateAndRegister(
      tcmalloc_metrics, "tcmalloc.bytes-in-use", "generic.current_allocated_bytes");

  TcmallocMetric::TOTAL_BYTES_RESERVED = TcmallocMetric::CreateAndRegister(
      tcmalloc_metrics, "tcmalloc.total-bytes-reserved", "generic.heap_size");

  TcmallocMetric::PAGEHEAP_FREE_BYTES = TcmallocMetric::CreateAndRegister(
      tcmalloc_metrics, "tcmalloc.pageheap-free-bytes", "tcmalloc.pageheap_free_bytes");

  TcmallocMetric::PAGEHEAP_UNMAPPED_BYTES =
      TcmallocMetric::CreateAndRegister(tcmalloc_metrics,
          "tcmalloc.pageheap-unmapped-bytes", "tcmalloc.pageheap_unmapped_bytes");

  TcmallocMetric::PHYSICAL_BYTES_RESERVED =
      tcmalloc_metrics->RegisterMetric(new TcmallocMetric::PhysicalBytesMetric(
          MetricDefs::Get("tcmalloc.physical-bytes-reserved")));

  used_metrics.push_back(TcmallocMetric::PHYSICAL_BYTES_RESERVED);
#endif
  MetricGroup* aggregate_metrics = metrics->GetOrCreateChildGroup("memory");
  AggregateMemoryMetrics::TOTAL_USED = aggregate_metrics->RegisterMetric(
      new SumGauge(MetricDefs::Get("memory.total-used"), used_metrics));

  if (FLAGS_enable_extended_memory_metrics && MemInfo::HaveSmaps()) {
    AggregateMemoryMetrics::NUM_MAPS = aggregate_metrics->AddGauge("memory.num-maps", 0U);
    AggregateMemoryMetrics::ANON_HUGE_PAGE_BYTES =
        aggregate_metrics->AddGauge("memory.anon-huge-page-bytes", 0U);
  }
  AggregateMemoryMetrics::MAPPED_BYTES =
      aggregate_metrics->AddGauge("memory.mapped-bytes", 0U);
  AggregateMemoryMetrics::RSS = aggregate_metrics->AddGauge("memory.rss", 0U);
  ThpConfig thp_config = MemInfo::ParseThpConfig();
  AggregateMemoryMetrics::THP_ENABLED =
      aggregate_metrics->AddProperty("memory.thp.enabled", thp_config.enabled);
  AggregateMemoryMetrics::THP_DEFRAG =
      aggregate_metrics->AddProperty("memory.thp.defrag", thp_config.defrag);
  AggregateMemoryMetrics::THP_KHUGEPAGED_DEFRAG = aggregate_metrics->AddProperty(
      "memory.thp.khugepaged-defrag", thp_config.khugepaged_defrag);
  AggregateMemoryMetrics::Refresh();
  return Status::OK();
}

void AggregateMemoryMetrics::Refresh() {
  if (NUM_MAPS != nullptr) {
    // Only call ParseSmaps() if the metrics were created.
    MappedMemInfo map_info = MemInfo::ParseSmaps();
    NUM_MAPS->SetValue(map_info.num_maps);
    ANON_HUGE_PAGE_BYTES->SetValue(map_info.anon_huge_pages_kb * 1024);
  }
  ProcessStateInfo proc_state(false);
  MAPPED_BYTES->SetValue(proc_state.GetVmSize());
  RSS->SetValue(proc_state.GetRss());

  ThpConfig thp_config = MemInfo::ParseThpConfig();
  THP_ENABLED->SetValue(thp_config.enabled);
  THP_DEFRAG->SetValue(thp_config.defrag);
  THP_KHUGEPAGED_DEFRAG->SetValue(thp_config.khugepaged_defrag);
}

JvmMemoryMetric* JvmMemoryMetric::CreateAndRegister(
    MetricGroup* metrics, const string& key, const string& pool_name,
    JvmMemoryMetricType type) {
  string pool_name_for_key = pool_name;
  to_lower(pool_name_for_key);
  replace(pool_name_for_key.begin(), pool_name_for_key.end(), ' ', '-');
  return metrics->RegisterMetric(
      new JvmMemoryMetric(MetricDefs::Get(key, pool_name_for_key), pool_name, type));
}

JvmMemoryMetric::JvmMemoryMetric(const TMetricDef& def, const string& mempool_name,
    JvmMemoryMetricType type) : IntGauge(def, 0) {
  mempool_name_ = mempool_name;
  metric_type_ = type;
}

int64_t JvmMemoryMetric::GetValue() {
  return JvmMetricCache::GetInstance()->GetPoolMetric(mempool_name_, metric_type_);
}

void JvmMemoryMetric::InitMetrics(MetricGroup* parent) {
  if (initialized_) return;
  MetricGroup* metrics = parent->GetOrCreateChildGroup("jvm");
  vector<string> names = JvmMetricCache::GetInstance()->GetPoolNames();
  for (const string& name : names) {
    JvmMemoryMetric* pool_max_usage =
        JvmMemoryMetric::CreateAndRegister(metrics, "jvm.$0.max-usage-bytes", name, MAX);
    if (name == "heap") HEAP_MAX_USAGE = pool_max_usage;
    JvmMemoryMetric::CreateAndRegister(
        metrics, "jvm.$0.current-usage-bytes", name, CURRENT);
    JvmMemoryMetric* pool_committed = JvmMemoryMetric::CreateAndRegister(
        metrics, "jvm.$0.committed-usage-bytes", name, COMMITTED);
    if (name == "non-heap") NON_HEAP_COMMITTED = pool_committed;
    JvmMemoryMetric::CreateAndRegister(metrics, "jvm.$0.init-usage-bytes", name, INIT);
    JvmMemoryMetric::CreateAndRegister(
        metrics, "jvm.$0.peak-max-usage-bytes", name, PEAK_MAX);
    JvmMemoryMetric::CreateAndRegister(
        metrics, "jvm.$0.peak-current-usage-bytes", name, PEAK_CURRENT);
    JvmMemoryMetric::CreateAndRegister(
        metrics, "jvm.$0.peak-committed-usage-bytes", name, PEAK_COMMITTED);
    JvmMemoryMetric::CreateAndRegister(
        metrics, "jvm.$0.peak-init-usage-bytes", name, PEAK_INIT);
  }

  JvmMemoryCounterMetric::GC_TIME_MILLIS =
      JvmMemoryCounterMetric::CreateAndRegister(metrics,
          "jvm.gc_time_millis",
          [](const TGetJvmMemoryMetricsResponse& r) {
          return r.gc_time_millis;
          });
  JvmMemoryCounterMetric::GC_NUM_INFO_THRESHOLD_EXCEEDED =
      JvmMemoryCounterMetric::CreateAndRegister(metrics,
          "jvm.gc_num_info_threshold_exceeded",
          [](const TGetJvmMemoryMetricsResponse& r) {
            return r.gc_num_info_threshold_exceeded;
          });
  JvmMemoryCounterMetric::GC_NUM_WARN_THRESHOLD_EXCEEDED =
      JvmMemoryCounterMetric::CreateAndRegister(metrics,
          "jvm.gc_num_warn_threshold_exceeded",
          [](const TGetJvmMemoryMetricsResponse& r) {
            return r.gc_num_warn_threshold_exceeded;
          });
  JvmMemoryCounterMetric::GC_COUNT =
    JvmMemoryCounterMetric::CreateAndRegister(metrics,
          "jvm.gc_count",
          [](const TGetJvmMemoryMetricsResponse& r) {
            return r.gc_count;
          });
  JvmMemoryCounterMetric::GC_TOTAL_EXTRA_SLEEP_TIME_MILLIS =
      JvmMemoryCounterMetric::CreateAndRegister(metrics,
          "jvm.gc_total_extra_sleep_time_millis",
          [](const TGetJvmMemoryMetricsResponse& r) {
            return r.gc_total_extra_sleep_time_millis;
          });
  initialized_ = true;
}

JvmMemoryCounterMetric* JvmMemoryCounterMetric::CreateAndRegister(
    MetricGroup* metrics, const string& key,
    int64_t(*accessor)(const TGetJvmMemoryMetricsResponse&)) {
  return metrics->RegisterMetric(
      new JvmMemoryCounterMetric(MetricDefs::Get(key), accessor));
}

JvmMemoryCounterMetric::JvmMemoryCounterMetric(const TMetricDef& def,
    int64_t(*accessor)(const TGetJvmMemoryMetricsResponse&))
    : IntCounter(def, 0) {
  accessor_ = accessor;
}

int64_t JvmMemoryCounterMetric::GetValue() {
  return JvmMetricCache::GetInstance()->GetCounterMetric(accessor_);
}

JvmMetricCache* JvmMetricCache::GetInstance() {
  static JvmMetricCache instance;
  return &instance;
}

void JvmMetricCache::GrabMetricsIfNecessary() {
  // Acquire the read lock and check if the cache timeout has expired. Acquiring the read
  // lock should be cheap, so optimize for the fast path where the timeout has not been
  // hit.
  {
    shared_lock<shared_mutex> shared_l(lock_);
    int64_t now = MonotonicMillis();
    if (now - last_fetch_ < CACHE_PERIOD_MILLIS) return;
  }

  // If the timeout has been hit, then refresh 'last_response_'. Since the read lock is
  // released before the write lock is acquired, it is possible another thread
  // already refreshed 'last_response_' so check the value of 'last_fetch_' again.
  unique_lock<shared_mutex> unique_l(lock_);
  int64_t now = MonotonicMillis();
  if (now - last_fetch_ < CACHE_PERIOD_MILLIS) return;
  Status status = JniUtil::GetJvmMemoryMetrics(&last_response_);
  if (!status.ok()) {
    VLOG_QUERY << "Couldn't retrieve JVM Memory Metrics: " << status.GetDetail();
    return;
  }
  last_fetch_ = now;
  return;
}

int64_t JvmMetricCache::GetCounterMetric(
    int64_t(*accessor)(const TGetJvmMemoryMetricsResponse&)) {
  GrabMetricsIfNecessary();
  shared_lock<shared_mutex> shared_l(lock_);
  return accessor(last_response_);
}

int64_t JvmMetricCache::GetPoolMetric(const std::string& mempool_name,
    JvmMemoryMetricType type) {
  GrabMetricsIfNecessary();

  shared_lock<shared_mutex> shared_l(lock_);
  for (const TJvmMemoryPool& pool : last_response_.memory_pools) {
    if (pool.name == mempool_name) {
      switch (type) {
        case MAX:
          return pool.max;
        case INIT:
          return pool.init;
        case CURRENT:
          return pool.used;
        case COMMITTED:
          return pool.committed;
        case PEAK_MAX:
          return pool.peak_max;
        case PEAK_INIT:
          return pool.peak_init;
        case PEAK_CURRENT:
          return pool.peak_used;
        case PEAK_COMMITTED:
          return pool.peak_committed;
        default:
          DCHECK(false) << "Unknown JvmMemoryMetricType: " << type;
      }
    }
  }
  DCHECK(false) << "Could not find pool: " << mempool_name;
  return 0;
}

vector<string> JvmMetricCache::GetPoolNames() {
  GrabMetricsIfNecessary();
  vector<string> names;
  shared_lock<shared_mutex> shared_l(lock_);
  for (const TJvmMemoryPool& usage: last_response_.memory_pools) {
    names.push_back(usage.name);
  }
  return names;
}

Status BufferPoolMetric::InitMetrics(MetricGroup* metrics,
    ReservationTracker* global_reservations, BufferPool* buffer_pool) {
  LIMIT = metrics->RegisterMetric(
      new BufferPoolMetric(MetricDefs::Get("buffer-pool.limit"),
          BufferPoolMetricType::LIMIT, global_reservations, buffer_pool));
  SYSTEM_ALLOCATED = metrics->RegisterMetric(
      new BufferPoolMetric(MetricDefs::Get("buffer-pool.system-allocated"),
          BufferPoolMetricType::SYSTEM_ALLOCATED, global_reservations, buffer_pool));
  RESERVED = metrics->RegisterMetric(
      new BufferPoolMetric(MetricDefs::Get("buffer-pool.reserved"),
          BufferPoolMetricType::RESERVED, global_reservations, buffer_pool));
  UNUSED_RESERVATION_BYTES = metrics->RegisterMetric(
      new BufferPoolMetric(MetricDefs::Get("buffer-pool.unused-reservation-bytes"),
          BufferPoolMetricType::UNUSED_RESERVATION_BYTES, global_reservations,
          buffer_pool));
  NUM_FREE_BUFFERS = metrics->RegisterMetric(
      new BufferPoolMetric(MetricDefs::Get("buffer-pool.free-buffers"),
          BufferPoolMetricType::NUM_FREE_BUFFERS, global_reservations, buffer_pool));
  FREE_BUFFER_BYTES = metrics->RegisterMetric(
      new BufferPoolMetric(MetricDefs::Get("buffer-pool.free-buffer-bytes"),
          BufferPoolMetricType::FREE_BUFFER_BYTES, global_reservations, buffer_pool));
  CLEAN_PAGES_LIMIT = metrics->RegisterMetric(
      new BufferPoolMetric(MetricDefs::Get("buffer-pool.clean-pages-limit"),
          BufferPoolMetricType::CLEAN_PAGES_LIMIT, global_reservations, buffer_pool));
  NUM_CLEAN_PAGES = metrics->RegisterMetric(
      new BufferPoolMetric(MetricDefs::Get("buffer-pool.clean-pages"),
          BufferPoolMetricType::NUM_CLEAN_PAGES, global_reservations, buffer_pool));
  CLEAN_PAGE_BYTES = metrics->RegisterMetric(
      new BufferPoolMetric(MetricDefs::Get("buffer-pool.clean-page-bytes"),
          BufferPoolMetricType::CLEAN_PAGE_BYTES, global_reservations, buffer_pool));
  return Status::OK();
}

BufferPoolMetric::BufferPoolMetric(const TMetricDef& def, BufferPoolMetricType type,
    ReservationTracker* global_reservations, BufferPool* buffer_pool)
  : IntGauge(def, 0),
    type_(type),
    global_reservations_(global_reservations),
    buffer_pool_(buffer_pool) {}

int64_t BufferPoolMetric::GetValue() {
  // IMPALA-6362: we have to be careful that none of the below calls to ReservationTracker
  // methods acquire ReservationTracker::lock_ to avoid a potential circular dependency
  // with MemTracker::child_trackers_lock_, which may be held when refreshing MemTracker
  // consumption.
  switch (type_) {
    case BufferPoolMetricType::LIMIT:
      return buffer_pool_->GetSystemBytesLimit();
    case BufferPoolMetricType::SYSTEM_ALLOCATED:
      return buffer_pool_->GetSystemBytesAllocated();
    case BufferPoolMetricType::RESERVED:
      return global_reservations_->GetReservation();
    case BufferPoolMetricType::UNUSED_RESERVATION_BYTES: {
      // Estimate the unused reservation based on other aggregate values, defined as
      // the total bytes of reservation where there is no corresponding buffer in use
      // by a client. Buffers are either in-use, free buffers, or attached to clean pages.
      int64_t total_used_reservation = buffer_pool_->GetSystemBytesAllocated()
          - buffer_pool_->GetFreeBufferBytes()
          - buffer_pool_->GetCleanPageBytes();
      return global_reservations_->GetReservation() - total_used_reservation;
    }
    case BufferPoolMetricType::NUM_FREE_BUFFERS:
      return buffer_pool_->GetNumFreeBuffers();
    case BufferPoolMetricType::FREE_BUFFER_BYTES:
      return buffer_pool_->GetFreeBufferBytes();
    case BufferPoolMetricType::CLEAN_PAGES_LIMIT:
      return buffer_pool_->GetCleanPageBytesLimit();
    case BufferPoolMetricType::NUM_CLEAN_PAGES:
      return buffer_pool_->GetNumCleanPages();
    case BufferPoolMetricType::CLEAN_PAGE_BYTES:
      return buffer_pool_->GetCleanPageBytes();
    default:
      DCHECK(false) << "Unknown BufferPoolMetricType: " << static_cast<int>(type_);
  }
  return 0;
}

void MemTrackerMetric::CreateMetrics(MetricGroup* metrics, MemTracker* mem_tracker,
    const string& name) {
  metrics->RegisterMetric(
      new MemTrackerMetric(MetricDefs::Get("mem-tracker.$0.current_usage_bytes", name),
      MemTrackerMetricType::CURRENT, mem_tracker));
  metrics->RegisterMetric(
      new MemTrackerMetric(MetricDefs::Get("mem-tracker.$0.peak_usage_bytes", name),
      MemTrackerMetricType::PEAK, mem_tracker));
}

MemTrackerMetric::MemTrackerMetric(const TMetricDef& def, MemTrackerMetricType type,
    MemTracker* mem_tracker)
  : IntGauge(def, 0),
    type_(type),
    mem_tracker_(mem_tracker) {}

int64_t MemTrackerMetric::GetValue() {
  switch (type_) {
    case MemTrackerMetricType::CURRENT:
      return mem_tracker_->consumption();
    case MemTrackerMetricType::PEAK:
      return mem_tracker_->peak_consumption();
    default:
      DCHECK(false) << "Unknown MemTrackerMetricType: " << static_cast<int>(type_);
  }
  return 0;
}
