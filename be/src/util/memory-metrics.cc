// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "util/memory-metrics.h"

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <gutil/strings/substitute.h>

#include "util/jni-util.h"
#include "util/time.h"

using namespace boost;
using namespace boost::algorithm;
using namespace impala;
using namespace strings;

TcmallocMetric* TcmallocMetric::BYTES_IN_USE = NULL;
TcmallocMetric* TcmallocMetric::PAGEHEAP_FREE_BYTES = NULL;
TcmallocMetric* TcmallocMetric::TOTAL_BYTES_RESERVED = NULL;
TcmallocMetric* TcmallocMetric::PAGEHEAP_UNMAPPED_BYTES = NULL;
TcmallocMetric::PhysicalBytesMetric* TcmallocMetric::PHYSICAL_BYTES_RESERVED = NULL;

Status impala::RegisterMemoryMetrics(Metrics* metrics, bool register_jvm_metrics) {
#ifndef ADDRESS_SANITIZER
  TcmallocMetric::BYTES_IN_USE = metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.bytes-in-use", "generic.current_allocated_bytes"));

  TcmallocMetric::TOTAL_BYTES_RESERVED = metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.total-bytes-reserved", "generic.heap_size"));

  TcmallocMetric::PAGEHEAP_FREE_BYTES = metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.pageheap-free-bytes", "tcmalloc.pageheap_free_bytes"));

  TcmallocMetric::PAGEHEAP_UNMAPPED_BYTES = metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.pageheap-unmapped-bytes", "tcmalloc.pageheap_unmapped_bytes"));

  TcmallocMetric::PHYSICAL_BYTES_RESERVED = metrics->RegisterMetric(
      new TcmallocMetric::PhysicalBytesMetric("tcmalloc.physical-bytes-reserved"));
#endif

  if (register_jvm_metrics) RETURN_IF_ERROR(JvmMetric::InitMetrics(metrics));
  return Status::OK;
}

JvmMetric::JvmMetric(const string& key, const string& mempool_name, JvmMetricType type)
    : Metrics::PrimitiveMetric<uint64_t>(key, 0) {
  mempool_name_ = mempool_name;
  metric_type_ = type;
}

Status JvmMetric::InitMetrics(Metrics* metrics) {
  DCHECK(metrics != NULL);
  TGetJvmMetricsRequest request;
  request.get_all = true;
  TGetJvmMetricsResponse response;
  RETURN_IF_ERROR(JniUtil::GetJvmMetrics(request, &response));
  BOOST_FOREACH(const TJvmMemoryPool& usage, response.memory_pools) {
    string name = usage.name;
    to_lower(name);
    replace(name.begin(), name.end(), ' ', '-');
    metrics->RegisterMetric(
        new JvmMetric(Substitute("jvm.$0.max-usage-bytes", name), usage.name, MAX));
    metrics->RegisterMetric(
        new JvmMetric(Substitute("jvm.$0.current-usage-bytes", name),
            usage.name, CURRENT));
    metrics->RegisterMetric(
        new JvmMetric(Substitute("jvm.$0.committed-usage-bytes", name), usage.name,
            COMMITTED));
    metrics->RegisterMetric(
        new JvmMetric(Substitute("jvm.$0.init-usage-bytes", name), usage.name, INIT));
    metrics->RegisterMetric(
        new JvmMetric(Substitute("jvm.$0.peak-max-usage-bytes", name), usage.name,
            PEAK_MAX));
    metrics->RegisterMetric(
        new JvmMetric(Substitute("jvm.$0.peak-current-usage-bytes", name),
            usage.name, PEAK_CURRENT));
    metrics->RegisterMetric(
        new JvmMetric(Substitute("jvm.$0.peak-committed-usage-bytes", name), usage.name,
            PEAK_COMMITTED));
    metrics->RegisterMetric(
        new JvmMetric(Substitute("jvm.$0.peak-init-usage-bytes", name), usage.name,
            PEAK_INIT));
  }

  return Status::OK;
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
    default: DCHECK(false) << "Unknown JvmMetricType: " << metric_type_;
  }
}
