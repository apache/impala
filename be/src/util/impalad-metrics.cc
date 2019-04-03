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

#include "util/impalad-metrics.h"

#include "util/debug-util.h"
#include "util/histogram-metric.h"

#include "common/names.h"

DECLARE_bool(use_local_catalog);

namespace impala {

// Naming convention: Components should be separated by '.' and words should
// be separated by '-'.
const char* ImpaladMetricKeys::IMPALA_SERVER_VERSION =
    "impala-server.version";
const char* ImpaladMetricKeys::IMPALA_SERVER_READY = "impala-server.ready";
const char* ImpaladMetricKeys::IMPALA_SERVER_NUM_QUERIES = "impala-server.num-queries";
const char* ImpaladMetricKeys::NUM_QUERIES_REGISTERED =
    "impala-server.num-queries-registered";
const char* ImpaladMetricKeys::BACKEND_NUM_QUERIES_EXECUTED =
    "impala-server.backend-num-queries-executed";
const char* ImpaladMetricKeys::BACKEND_NUM_QUERIES_EXECUTING =
    "impala-server.backend-num-queries-executing";
const char* ImpaladMetricKeys::IMPALA_SERVER_NUM_FRAGMENTS =
    "impala-server.num-fragments";
const char* ImpaladMetricKeys::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT =
    "impala-server.num-fragments-in-flight";
const char* ImpaladMetricKeys::TOTAL_SCAN_RANGES_PROCESSED =
    "impala-server.scan-ranges.total";
const char* ImpaladMetricKeys::NUM_SCAN_RANGES_MISSING_VOLUME_ID =
    "impala-server.scan-ranges.num-missing-volume-id";
const char* ImpaladMetricKeys::IO_MGR_NUM_OPEN_FILES =
    "impala-server.io-mgr.num-open-files";
const char* ImpaladMetricKeys::IO_MGR_BYTES_READ =
    "impala-server.io-mgr.bytes-read";
const char* ImpaladMetricKeys::IO_MGR_LOCAL_BYTES_READ =
    "impala-server.io-mgr.local-bytes-read";
const char* ImpaladMetricKeys::IO_MGR_SHORT_CIRCUIT_BYTES_READ =
    "impala-server.io-mgr.short-circuit-bytes-read";
const char* ImpaladMetricKeys::IO_MGR_CACHED_BYTES_READ =
    "impala-server.io-mgr.cached-bytes-read";
const char* ImpaladMetricKeys::IO_MGR_REMOTE_DATA_CACHE_HIT_BYTES =
    "impala-server.io-mgr.remote-data-cache-hit-bytes";
const char* ImpaladMetricKeys::IO_MGR_REMOTE_DATA_CACHE_MISS_BYTES =
    "impala-server.io-mgr.remote-data-cache-miss-bytes";
const char* ImpaladMetricKeys::IO_MGR_REMOTE_DATA_CACHE_TOTAL_BYTES =
    "impala-server.io-mgr.remote-data-cache-total-bytes";
const char* ImpaladMetricKeys::IO_MGR_REMOTE_DATA_CACHE_DROPPED_BYTES =
    "impala-server.io-mgr.remote-data-cache-dropped-bytes";
const char* ImpaladMetricKeys::IO_MGR_BYTES_WRITTEN =
    "impala-server.io-mgr.bytes-written";
const char* ImpaladMetricKeys::IO_MGR_NUM_CACHED_FILE_HANDLES =
    "impala-server.io.mgr.num-cached-file-handles";
const char* ImpaladMetricKeys::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING =
    "impala-server.io.mgr.num-file-handles-outstanding";
const char* ImpaladMetricKeys::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO =
    "impala-server.io.mgr.cached-file-handles-hit-ratio";
const char* ImpaladMetricKeys::IO_MGR_CACHED_FILE_HANDLES_HIT_COUNT =
    "impala-server.io.mgr.cached-file-handles-hit-count";
const char* ImpaladMetricKeys::IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT =
    "impala-server.io.mgr.cached-file-handles-miss-count";
const char* ImpaladMetricKeys::IO_MGR_CACHED_FILE_HANDLES_REOPENED =
    "impala-server.io.mgr.cached-file-handles-reopened";
const char* ImpaladMetricKeys::CATALOG_NUM_DBS =
    "catalog.num-databases";
const char* ImpaladMetricKeys::CATALOG_NUM_TABLES =
    "catalog.num-tables";
const char* ImpaladMetricKeys::CATALOG_VERSION = "catalog.curr-version";
const char* ImpaladMetricKeys::CATALOG_TOPIC_VERSION = "catalog.curr-topic";
const char* ImpaladMetricKeys::CATALOG_SERVICE_ID = "catalog.curr-serviceid";
const char* ImpaladMetricKeys::CATALOG_READY = "catalog.ready";
const char* ImpaladMetricKeys::CATALOG_CACHE_AVG_LOAD_TIME =
    "catalog.cache.average-load-time";
const char* ImpaladMetricKeys::CATALOG_CACHE_EVICTION_COUNT =
    "catalog.cache.eviction-count";
const char* ImpaladMetricKeys::CATALOG_CACHE_HIT_COUNT = "catalog.cache.hit-count";
const char* ImpaladMetricKeys::CATALOG_CACHE_HIT_RATE ="catalog.cache.hit-rate";
const char* ImpaladMetricKeys::CATALOG_CACHE_LOAD_COUNT = "catalog.cache.load-count";
const char* ImpaladMetricKeys::CATALOG_CACHE_LOAD_EXCEPTION_COUNT =
    "catalog.cache.load-exception-count";
const char* ImpaladMetricKeys::CATALOG_CACHE_LOAD_EXCEPTION_RATE =
    "catalog.cache.load-exception-rate";
const char* ImpaladMetricKeys::CATALOG_CACHE_LOAD_SUCCESS_COUNT =
    "catalog.cache.load-success-count";
const char* ImpaladMetricKeys::CATALOG_CACHE_MISS_COUNT =
    "catalog.cache.miss-count";
const char* ImpaladMetricKeys::CATALOG_CACHE_MISS_RATE =
    "catalog.cache.miss-rate";
const char* ImpaladMetricKeys::CATALOG_CACHE_REQUEST_COUNT =
    "catalog.cache.request-count";
const char* ImpaladMetricKeys::CATALOG_CACHE_TOTAL_LOAD_TIME =
    "catalog.cache.total-load-time";
const char* ImpaladMetricKeys::NUM_FILES_OPEN_FOR_INSERT =
    "impala-server.num-files-open-for-insert";
const char* ImpaladMetricKeys::IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS =
    "impala-server.num-open-hiveserver2-sessions";
const char* ImpaladMetricKeys::IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS =
    "impala-server.num-open-beeswax-sessions";
const char* ImpaladMetricKeys::NUM_SESSIONS_EXPIRED =
    "impala-server.num-sessions-expired";
const char* ImpaladMetricKeys::NUM_QUERIES_EXPIRED =
    "impala-server.num-queries-expired";
const char* ImpaladMetricKeys::NUM_QUERIES_SPILLED =
    "impala-server.num-queries-spilled";
const char* ImpaladMetricKeys::RESULTSET_CACHE_TOTAL_NUM_ROWS =
    "impala-server.resultset-cache.total-num-rows";
const char* ImpaladMetricKeys::RESULTSET_CACHE_TOTAL_BYTES =
    "impala-server.resultset-cache.total-bytes";
const char* ImpaladMetricKeys::QUERY_DURATIONS =
    "impala-server.query-durations-ms";
const char* ImpaladMetricKeys::DDL_DURATIONS =
    "impala-server.ddl-durations-ms";
const char* ImpaladMetricKeys::HEDGED_READ_OPS =
    "impala-server.hedged-read-ops";
const char* ImpaladMetricKeys::HEDGED_READ_OPS_WIN =
    "impala-server.hedged-read-ops-win";

// These are created by impala-server during startup.
// =======
// Counters
IntCounter* ImpaladMetrics::BACKEND_NUM_QUERIES_EXECUTED = NULL;
IntGauge* ImpaladMetrics::BACKEND_NUM_QUERIES_EXECUTING = NULL;
IntCounter* ImpaladMetrics::IMPALA_SERVER_NUM_QUERIES = NULL;
IntCounter* ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS = NULL;
IntGauge* ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT = NULL;
IntCounter* ImpaladMetrics::NUM_QUERIES_EXPIRED = NULL;
IntCounter* ImpaladMetrics::NUM_QUERIES_SPILLED = NULL;
IntCounter* ImpaladMetrics::NUM_RANGES_MISSING_VOLUME_ID = NULL;
IntCounter* ImpaladMetrics::NUM_RANGES_PROCESSED = NULL;
IntCounter* ImpaladMetrics::NUM_SESSIONS_EXPIRED = NULL;
IntCounter* ImpaladMetrics::IO_MGR_BYTES_READ = NULL;
IntCounter* ImpaladMetrics::IO_MGR_LOCAL_BYTES_READ = NULL;
IntCounter* ImpaladMetrics::IO_MGR_SHORT_CIRCUIT_BYTES_READ = NULL;
IntCounter* ImpaladMetrics::IO_MGR_CACHED_BYTES_READ = NULL;
IntCounter* ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_HIT_BYTES = NULL;
IntCounter* ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_MISS_BYTES = NULL;
IntCounter* ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_DROPPED_BYTES = NULL;
IntCounter* ImpaladMetrics::IO_MGR_BYTES_WRITTEN = NULL;
IntCounter* ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_REOPENED = NULL;
IntCounter* ImpaladMetrics::HEDGED_READ_OPS = NULL;
IntCounter* ImpaladMetrics::HEDGED_READ_OPS_WIN = NULL;
IntCounter* ImpaladMetrics::CATALOG_CACHE_EVICTION_COUNT = NULL;
IntCounter* ImpaladMetrics::CATALOG_CACHE_HIT_COUNT = NULL;
IntCounter* ImpaladMetrics::CATALOG_CACHE_LOAD_COUNT = NULL;
IntCounter* ImpaladMetrics::CATALOG_CACHE_LOAD_EXCEPTION_COUNT = NULL;
IntCounter* ImpaladMetrics::CATALOG_CACHE_LOAD_SUCCESS_COUNT = NULL;
IntCounter* ImpaladMetrics::CATALOG_CACHE_MISS_COUNT = NULL;
IntCounter* ImpaladMetrics::CATALOG_CACHE_REQUEST_COUNT = NULL;
IntCounter* ImpaladMetrics::CATALOG_CACHE_TOTAL_LOAD_TIME = NULL;

// Gauges
IntGauge* ImpaladMetrics::CATALOG_NUM_DBS = NULL;
IntGauge* ImpaladMetrics::CATALOG_NUM_TABLES = NULL;
IntGauge* ImpaladMetrics::CATALOG_VERSION = NULL;
IntGauge* ImpaladMetrics::CATALOG_TOPIC_VERSION = NULL;
IntGauge* ImpaladMetrics::IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS = NULL;
IntGauge* ImpaladMetrics::IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS = NULL;
IntGauge* ImpaladMetrics::IO_MGR_NUM_BUFFERS = NULL;
IntGauge* ImpaladMetrics::IO_MGR_NUM_OPEN_FILES = NULL;
IntGauge* ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS = NULL;
IntGauge* ImpaladMetrics::IO_MGR_NUM_CACHED_FILE_HANDLES = NULL;
IntGauge* ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING = NULL;
IntGauge* ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_COUNT = NULL;
IntGauge* ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT = NULL;
IntGauge* ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_TOTAL_BYTES = NULL;
IntGauge* ImpaladMetrics::NUM_FILES_OPEN_FOR_INSERT = NULL;
IntGauge* ImpaladMetrics::NUM_QUERIES_REGISTERED = NULL;
IntGauge* ImpaladMetrics::RESULTSET_CACHE_TOTAL_NUM_ROWS = NULL;
IntGauge* ImpaladMetrics::RESULTSET_CACHE_TOTAL_BYTES = NULL;
DoubleGauge* ImpaladMetrics::CATALOG_CACHE_AVG_LOAD_TIME = NULL;
DoubleGauge* ImpaladMetrics::CATALOG_CACHE_HIT_RATE = NULL;
DoubleGauge* ImpaladMetrics::CATALOG_CACHE_LOAD_EXCEPTION_RATE = NULL;
DoubleGauge* ImpaladMetrics::CATALOG_CACHE_MISS_RATE = NULL;

// Properties
BooleanProperty* ImpaladMetrics::CATALOG_READY = NULL;
BooleanProperty* ImpaladMetrics::IMPALA_SERVER_READY = NULL;
StringProperty* ImpaladMetrics::IMPALA_SERVER_VERSION = NULL;
StringProperty* ImpaladMetrics::CATALOG_SERVICE_ID = NULL;

// Histograms
HistogramMetric* ImpaladMetrics::QUERY_DURATIONS = NULL;
HistogramMetric* ImpaladMetrics::DDL_DURATIONS = NULL;

// Other
StatsMetric<uint64_t, StatsType::MEAN>*
ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO = NULL;

void ImpaladMetrics::InitCatalogMetrics(MetricGroup* m) {
  // Initialize catalog metrics
  MetricGroup* catalog_metrics = m->GetOrCreateChildGroup("catalog");
  CATALOG_NUM_DBS = catalog_metrics->AddGauge(ImpaladMetricKeys::CATALOG_NUM_DBS, 0);
  CATALOG_NUM_TABLES =
      catalog_metrics->AddGauge(ImpaladMetricKeys::CATALOG_NUM_TABLES, 0);
  CATALOG_VERSION = catalog_metrics->AddGauge(ImpaladMetricKeys::CATALOG_VERSION, 0);
  CATALOG_TOPIC_VERSION =
      catalog_metrics->AddGauge(ImpaladMetricKeys::CATALOG_TOPIC_VERSION, 0);
  CATALOG_SERVICE_ID =
      catalog_metrics->AddProperty<string>(ImpaladMetricKeys::CATALOG_SERVICE_ID, "");
  CATALOG_READY =
      catalog_metrics->AddProperty<bool>(ImpaladMetricKeys::CATALOG_READY, false);
  // CatalogdMetaProvider cache metrics. Valid only when --use_local_catalog is set.
  if (FLAGS_use_local_catalog) {
    CATALOG_CACHE_AVG_LOAD_TIME = catalog_metrics->AddDoubleGauge(
        ImpaladMetricKeys::CATALOG_CACHE_AVG_LOAD_TIME, 0);
    CATALOG_CACHE_EVICTION_COUNT =
        catalog_metrics->AddCounter(ImpaladMetricKeys::CATALOG_CACHE_EVICTION_COUNT, 0);
    CATALOG_CACHE_HIT_COUNT =
        catalog_metrics->AddCounter(ImpaladMetricKeys::CATALOG_CACHE_HIT_COUNT, 0);
    CATALOG_CACHE_HIT_RATE =
        catalog_metrics->AddDoubleGauge(ImpaladMetricKeys::CATALOG_CACHE_HIT_RATE, 0);
    CATALOG_CACHE_LOAD_COUNT =
        catalog_metrics->AddCounter(ImpaladMetricKeys::CATALOG_CACHE_LOAD_COUNT, 0);
    CATALOG_CACHE_LOAD_EXCEPTION_COUNT = catalog_metrics->AddCounter(
        ImpaladMetricKeys::CATALOG_CACHE_LOAD_EXCEPTION_COUNT, 0);
    CATALOG_CACHE_LOAD_EXCEPTION_RATE = catalog_metrics->AddDoubleGauge(
        ImpaladMetricKeys::CATALOG_CACHE_LOAD_EXCEPTION_RATE, 0);
    CATALOG_CACHE_LOAD_SUCCESS_COUNT = catalog_metrics->AddCounter(
        ImpaladMetricKeys::CATALOG_CACHE_LOAD_SUCCESS_COUNT, 0);
    CATALOG_CACHE_MISS_COUNT =
        catalog_metrics->AddCounter(ImpaladMetricKeys::CATALOG_CACHE_MISS_COUNT, 0);
    CATALOG_CACHE_MISS_RATE =
        catalog_metrics->AddDoubleGauge(ImpaladMetricKeys::CATALOG_CACHE_MISS_RATE, 0);
    CATALOG_CACHE_REQUEST_COUNT =
        catalog_metrics->AddCounter(ImpaladMetricKeys::CATALOG_CACHE_REQUEST_COUNT, 0);
    CATALOG_CACHE_TOTAL_LOAD_TIME =
        catalog_metrics->AddCounter(ImpaladMetricKeys::CATALOG_CACHE_TOTAL_LOAD_TIME, 0);
  }
}

void ImpaladMetrics::CreateMetrics(MetricGroup* m) {
  // Initialize impalad metrics
  IMPALA_SERVER_VERSION = m->AddProperty<string>(
      ImpaladMetricKeys::IMPALA_SERVER_VERSION, GetVersionString(true));
  IMPALA_SERVER_READY = m->AddProperty<bool>(
      ImpaladMetricKeys::IMPALA_SERVER_READY, false);

  IMPALA_SERVER_NUM_QUERIES = m->AddCounter(
      ImpaladMetricKeys::IMPALA_SERVER_NUM_QUERIES, 0);
  NUM_QUERIES_REGISTERED = m->AddGauge(
      ImpaladMetricKeys::NUM_QUERIES_REGISTERED, 0);
  NUM_QUERIES_EXPIRED = m->AddCounter(
      ImpaladMetricKeys::NUM_QUERIES_EXPIRED, 0);
  NUM_QUERIES_SPILLED = m->AddCounter(
      ImpaladMetricKeys::NUM_QUERIES_SPILLED, 0);
  BACKEND_NUM_QUERIES_EXECUTED = m->AddCounter(
      ImpaladMetricKeys::BACKEND_NUM_QUERIES_EXECUTED, 0);
  BACKEND_NUM_QUERIES_EXECUTING = m->AddGauge(
      ImpaladMetricKeys::BACKEND_NUM_QUERIES_EXECUTING, 0);
  IMPALA_SERVER_NUM_FRAGMENTS = m->AddCounter(
      ImpaladMetricKeys::IMPALA_SERVER_NUM_FRAGMENTS, 0);
  IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT = m->AddGauge(
      ImpaladMetricKeys::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT, 0);
  IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS = m->AddGauge(
      ImpaladMetricKeys::IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS, 0);
  IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS = m->AddGauge(
      ImpaladMetricKeys::IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS, 0);
  NUM_SESSIONS_EXPIRED = m->AddCounter(
      ImpaladMetricKeys::NUM_SESSIONS_EXPIRED, 0);
  RESULTSET_CACHE_TOTAL_NUM_ROWS = m->AddGauge(
      ImpaladMetricKeys::RESULTSET_CACHE_TOTAL_NUM_ROWS, 0);
  RESULTSET_CACHE_TOTAL_BYTES = m->AddGauge(
      ImpaladMetricKeys::RESULTSET_CACHE_TOTAL_BYTES, 0);

  // Initialize scan node metrics
  NUM_RANGES_PROCESSED = m->AddCounter(
      ImpaladMetricKeys::TOTAL_SCAN_RANGES_PROCESSED, 0);
  NUM_RANGES_MISSING_VOLUME_ID = m->AddCounter(
      ImpaladMetricKeys::NUM_SCAN_RANGES_MISSING_VOLUME_ID, 0);

  // Initialize insert metrics
  NUM_FILES_OPEN_FOR_INSERT = m->AddGauge(
      ImpaladMetricKeys::NUM_FILES_OPEN_FOR_INSERT, 0);

  // Initialize IO mgr metrics
  IO_MGR_NUM_OPEN_FILES = m->AddGauge(
      ImpaladMetricKeys::IO_MGR_NUM_OPEN_FILES, 0);
  IO_MGR_NUM_CACHED_FILE_HANDLES = m->AddGauge(
      ImpaladMetricKeys::IO_MGR_NUM_CACHED_FILE_HANDLES, 0);
  IO_MGR_NUM_FILE_HANDLES_OUTSTANDING = m->AddGauge(
      ImpaladMetricKeys::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING, 0);

  IO_MGR_CACHED_FILE_HANDLES_HIT_COUNT = m->AddGauge(
      ImpaladMetricKeys::IO_MGR_CACHED_FILE_HANDLES_HIT_COUNT, 0);

  IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT = m->AddGauge(
      ImpaladMetricKeys::IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT, 0);

  IO_MGR_CACHED_FILE_HANDLES_REOPENED = m->AddCounter(
      ImpaladMetricKeys::IO_MGR_CACHED_FILE_HANDLES_REOPENED, 0);

  IO_MGR_BYTES_READ = m->AddCounter(ImpaladMetricKeys::IO_MGR_BYTES_READ, 0);
  IO_MGR_LOCAL_BYTES_READ = m->AddCounter(
      ImpaladMetricKeys::IO_MGR_LOCAL_BYTES_READ, 0);
  IO_MGR_CACHED_BYTES_READ = m->AddCounter(
      ImpaladMetricKeys::IO_MGR_CACHED_BYTES_READ, 0);
  IO_MGR_SHORT_CIRCUIT_BYTES_READ = m->AddCounter(
      ImpaladMetricKeys::IO_MGR_SHORT_CIRCUIT_BYTES_READ, 0);
  IO_MGR_BYTES_WRITTEN = m->AddCounter(
      ImpaladMetricKeys::IO_MGR_BYTES_WRITTEN, 0);

  IO_MGR_REMOTE_DATA_CACHE_HIT_BYTES = m->AddCounter(
      ImpaladMetricKeys::IO_MGR_REMOTE_DATA_CACHE_HIT_BYTES, 0);
  IO_MGR_REMOTE_DATA_CACHE_MISS_BYTES = m->AddCounter(
      ImpaladMetricKeys::IO_MGR_REMOTE_DATA_CACHE_MISS_BYTES, 0);
  IO_MGR_REMOTE_DATA_CACHE_TOTAL_BYTES = m->AddGauge(
      ImpaladMetricKeys::IO_MGR_REMOTE_DATA_CACHE_TOTAL_BYTES, 0);
  IO_MGR_REMOTE_DATA_CACHE_DROPPED_BYTES = m->AddCounter(
      ImpaladMetricKeys::IO_MGR_REMOTE_DATA_CACHE_DROPPED_BYTES, 0);

  IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO =
      StatsMetric<uint64_t, StatsType::MEAN>::CreateAndRegister(m,
      ImpaladMetricKeys::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO);

  InitCatalogMetrics(m);

  // Maximum duration to be tracked by the query durations metric. No particular reasoning
  // behind five hours, except to say that there's some threshold beyond which queries
  // just become "long running", and at that point the distribution of their run times
  // isn't so interesting.
  const int FIVE_HOURS_IN_MS = 60 * 60 * 1000 * 5;
  QUERY_DURATIONS = m->RegisterMetric(new HistogramMetric(
      MetricDefs::Get(ImpaladMetricKeys::QUERY_DURATIONS), FIVE_HOURS_IN_MS, 3));
  DDL_DURATIONS = m->RegisterMetric(new HistogramMetric(
      MetricDefs::Get(ImpaladMetricKeys::DDL_DURATIONS), FIVE_HOURS_IN_MS, 3));

  // Initialize Hedged read metrics
  HEDGED_READ_OPS = m->AddCounter(ImpaladMetricKeys::HEDGED_READ_OPS, 0);
  HEDGED_READ_OPS_WIN = m->AddCounter(ImpaladMetricKeys::HEDGED_READ_OPS_WIN, 0);

}

}
