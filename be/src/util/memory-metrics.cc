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
#include "util/jni-util.h"
#include "util/mem-info.h"
#include "util/time.h"

using boost::algorithm::to_lower;
using namespace impala;
using namespace strings;

DECLARE_bool(mmap_buffers);

SumGauge<int64_t>* AggregateMemoryMetrics::TOTAL_USED = nullptr;
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
      new SumGauge<int64_t>(MetricDefs::Get("memory.total-used"), used_metrics));
  if (register_jvm_metrics) {
    RETURN_IF_ERROR(JvmMetric::InitMetrics(metrics->GetOrCreateChildGroup("jvm")));
  }

  if (MemInfo::HaveSmaps()) {
    AggregateMemoryMetrics::NUM_MAPS =
        aggregate_metrics->AddGauge<int64_t>("memory.num-maps", 0U);
    AggregateMemoryMetrics::MAPPED_BYTES =
        aggregate_metrics->AddGauge<int64_t>("memory.mapped-bytes", 0U);
    AggregateMemoryMetrics::RSS = aggregate_metrics->AddGauge<int64_t>("memory.rss", 0U);
    AggregateMemoryMetrics::ANON_HUGE_PAGE_BYTES =
        aggregate_metrics->AddGauge<int64_t>("memory.anon-huge-page-bytes", 0U);
  }
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
    NUM_MAPS->set_value(map_info.num_maps);
    MAPPED_BYTES->set_value(map_info.size_kb * 1024);
    RSS->set_value(map_info.rss_kb * 1024);
    ANON_HUGE_PAGE_BYTES->set_value(map_info.anon_huge_pages_kb * 1024);
  }

  ThpConfig thp_config = MemInfo::ParseThpConfig();
  THP_ENABLED->set_value(thp_config.enabled);
  THP_DEFRAG->set_value(thp_config.defrag);
  THP_KHUGEPAGED_DEFRAG->set_value(thp_config.khugepaged_defrag);
}

JvmMetric* JvmMetric::CreateAndRegister(MetricGroup* metrics, const string& key,
    const string& pool_name, JvmMetric::JvmMetricType type) {
  string pool_name_for_key = pool_name;
  to_lower(pool_name_for_key);
  replace(pool_name_for_key.begin(), pool_name_for_key.end(), ' ', '-');
  return metrics->RegisterMetric(new JvmMetric(MetricDefs::Get(key, pool_name_for_key),
      pool_name, type));
}

JvmMetric::JvmMetric(const TMetricDef& def, const string& mempool_name,
    JvmMetricType type) : IntGauge(def, 0) {
  mempool_name_ = mempool_name;
  metric_type_ = type;
}

Status JvmMetric::InitMetrics(MetricGroup* metrics) {
  DCHECK(metrics != nullptr);
  TGetJvmMetricsRequest request;
  request.get_all = true;
  TGetJvmMetricsResponse response;
  RETURN_IF_ERROR(JniUtil::GetJvmMetrics(request, &response));
  for (const TJvmMemoryPool& usage: response.memory_pools) {
    JvmMetric::CreateAndRegister(metrics, "jvm.$0.max-usage-bytes", usage.name, MAX);
    JvmMetric::CreateAndRegister(metrics, "jvm.$0.current-usage-bytes", usage.name,
        CURRENT);
    JvmMetric::CreateAndRegister(metrics, "jvm.$0.committed-usage-bytes", usage.name,
        COMMITTED);
    JvmMetric::CreateAndRegister(metrics, "jvm.$0.init-usage-bytes", usage.name, INIT);
    JvmMetric::CreateAndRegister(metrics, "jvm.$0.peak-max-usage-bytes", usage.name,
        PEAK_MAX);
    JvmMetric::CreateAndRegister(metrics, "jvm.$0.peak-current-usage-bytes", usage.name,
        PEAK_CURRENT);
    JvmMetric::CreateAndRegister(metrics, "jvm.$0.peak-committed-usage-bytes", usage.name,
        PEAK_COMMITTED);
    JvmMetric::CreateAndRegister(metrics, "jvm.$0.peak-init-usage-bytes", usage.name,
        PEAK_INIT);
  }

  return Status::OK();
}

void JvmMetric::CalculateValue() {
  TGetJvmMetricsRequest request;
  request.get_all = false;
  request.__set_memory_pool(mempool_name_);
  TGetJvmMetricsResponse response;
  if (!JniUtil::GetJvmMetrics(request, &response).ok()) return;
  if (response.memory_pools.size() != 1) return;
  TJvmMemoryPool& pool = response.memory_pools[0];
  DCHECK(pool.name == mempool_name_);
  switch (metric_type_) {
    case MAX: value_ = pool.max;
      return;
    case INIT: value_ = pool.init;
      return;
    case CURRENT: value_ = pool.used;
      return;
    case COMMITTED: value_ = pool.committed;
      return;
    case PEAK_MAX: value_ = pool.peak_max;
      return;
    case PEAK_INIT: value_ = pool.peak_init;
      return;
    case PEAK_CURRENT: value_ = pool.peak_used;
      return;
    case PEAK_COMMITTED: value_ = pool.peak_committed;
      return;
    default:
      DCHECK(false) << "Unknown JvmMetricType: " << metric_type_;
  }
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

void BufferPoolMetric::CalculateValue() {
  switch (type_) {
    case BufferPoolMetricType::LIMIT:
      value_ = buffer_pool_->GetSystemBytesLimit();
      break;
    case BufferPoolMetricType::SYSTEM_ALLOCATED:
      value_ = buffer_pool_->GetSystemBytesAllocated();
      break;
    case BufferPoolMetricType::RESERVED:
      value_ = global_reservations_->GetReservation();
      break;
    case BufferPoolMetricType::UNUSED_RESERVATION_BYTES: {
      // Estimate the unused reservation based on other aggregate values, defined as
      // the total bytes of reservation where there is no corresponding buffer in use
      // by a client. Buffers are either in-use, free buffers, or attached to clean pages.
      int64_t total_used_reservation = buffer_pool_->GetSystemBytesAllocated()
        - buffer_pool_->GetFreeBufferBytes()
        - buffer_pool_->GetCleanPageBytes();
      value_ = global_reservations_->GetReservation() - total_used_reservation;
      break;
    }
    case BufferPoolMetricType::NUM_FREE_BUFFERS:
      value_ = buffer_pool_->GetNumFreeBuffers();
      break;
    case BufferPoolMetricType::FREE_BUFFER_BYTES:
      value_ = buffer_pool_->GetFreeBufferBytes();
      break;
    case BufferPoolMetricType::CLEAN_PAGES_LIMIT:
      value_ = buffer_pool_->GetCleanPageBytesLimit();
      break;
    case BufferPoolMetricType::NUM_CLEAN_PAGES:
      value_ = buffer_pool_->GetNumCleanPages();
      break;
    case BufferPoolMetricType::CLEAN_PAGE_BYTES:
      value_ = buffer_pool_->GetCleanPageBytes();
      break;
    default:
      DCHECK(false) << "Unknown BufferPoolMetricType: " << static_cast<int>(type_);
  }
}
