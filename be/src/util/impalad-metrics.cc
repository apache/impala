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

namespace impala {

// Naming convention: Components should be separated by '.' and words should
// be separated by '-'.
const char* ImpaladMetricKeys::IMPALA_SERVER_VERSION =
    "impala-server.version";
const char* ImpaladMetricKeys::IMPALA_SERVER_READY = "impala-server.ready";
const char* ImpaladMetricKeys::IMPALA_SERVER_NUM_QUERIES = "impala-server.num-queries";
const char* ImpaladMetricKeys::NUM_QUERIES_REGISTERED =
    "impala-server.num-queries-registered";
const char* ImpaladMetricKeys::IMPALA_SERVER_NUM_FRAGMENTS =
    "impala-server.num-fragments";
const char* ImpaladMetricKeys::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT =
    "impala-server.num-fragments-in-flight";
const char* ImpaladMetricKeys::TOTAL_SCAN_RANGES_PROCESSED =
    "impala-server.scan-ranges.total";
const char* ImpaladMetricKeys::NUM_SCAN_RANGES_MISSING_VOLUME_ID =
    "impala-server.scan-ranges.num-missing-volume-id";
const char* ImpaladMetricKeys::MEM_POOL_TOTAL_BYTES =
    "impala-server.mem-pool.total-bytes";
const char* ImpaladMetricKeys::HASH_TABLE_TOTAL_BYTES =
    "impala-server.hash-table.total-bytes";
const char* ImpaladMetricKeys::IO_MGR_NUM_OPEN_FILES =
    "impala-server.io-mgr.num-open-files";
const char* ImpaladMetricKeys::IO_MGR_NUM_BUFFERS =
    "impala-server.io-mgr.num-buffers";
const char* ImpaladMetricKeys::IO_MGR_TOTAL_BYTES =
    "impala-server.io-mgr.total-bytes";
const char* ImpaladMetricKeys::IO_MGR_NUM_UNUSED_BUFFERS =
    "impala-server.io-mgr.num-unused-buffers";
const char* ImpaladMetricKeys::IO_MGR_BYTES_READ =
    "impala-server.io-mgr.bytes-read";
const char* ImpaladMetricKeys::IO_MGR_LOCAL_BYTES_READ =
    "impala-server.io-mgr.local-bytes-read";
const char* ImpaladMetricKeys::IO_MGR_SHORT_CIRCUIT_BYTES_READ =
    "impala-server.io-mgr.short-circuit-bytes-read";
const char* ImpaladMetricKeys::IO_MGR_CACHED_BYTES_READ =
    "impala-server.io-mgr.cached-bytes-read";
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
const char* ImpaladMetricKeys::CATALOG_READY =
    "catalog.ready";
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
IntGauge* ImpaladMetrics::HASH_TABLE_TOTAL_BYTES = NULL;
IntCounter* ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS = NULL;
IntGauge* ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT = NULL;
IntCounter* ImpaladMetrics::IMPALA_SERVER_NUM_QUERIES = NULL;
IntCounter* ImpaladMetrics::NUM_QUERIES_EXPIRED = NULL;
IntCounter* ImpaladMetrics::NUM_QUERIES_SPILLED = NULL;
IntCounter* ImpaladMetrics::NUM_RANGES_MISSING_VOLUME_ID = NULL;
IntCounter* ImpaladMetrics::NUM_RANGES_PROCESSED = NULL;
IntCounter* ImpaladMetrics::NUM_SESSIONS_EXPIRED = NULL;
IntCounter* ImpaladMetrics::IO_MGR_BYTES_READ = NULL;
IntCounter* ImpaladMetrics::IO_MGR_LOCAL_BYTES_READ = NULL;
IntCounter* ImpaladMetrics::IO_MGR_SHORT_CIRCUIT_BYTES_READ = NULL;
IntCounter* ImpaladMetrics::IO_MGR_CACHED_BYTES_READ = NULL;
IntCounter* ImpaladMetrics::IO_MGR_BYTES_WRITTEN = NULL;
IntCounter* ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_REOPENED = NULL;
IntCounter* ImpaladMetrics::HEDGED_READ_OPS = NULL;
IntCounter* ImpaladMetrics::HEDGED_READ_OPS_WIN = NULL;

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
IntGauge* ImpaladMetrics::IO_MGR_TOTAL_BYTES = NULL;
IntGauge* ImpaladMetrics::MEM_POOL_TOTAL_BYTES = NULL;
IntGauge* ImpaladMetrics::NUM_FILES_OPEN_FOR_INSERT = NULL;
IntGauge* ImpaladMetrics::NUM_QUERIES_REGISTERED = NULL;
IntGauge* ImpaladMetrics::RESULTSET_CACHE_TOTAL_NUM_ROWS = NULL;
IntGauge* ImpaladMetrics::RESULTSET_CACHE_TOTAL_BYTES = NULL;

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
  IMPALA_SERVER_NUM_FRAGMENTS = m->AddCounter(
      ImpaladMetricKeys::IMPALA_SERVER_NUM_FRAGMENTS, 0);
  IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT = m->AddGauge(
      ImpaladMetricKeys::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT, 0L);
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

  // Initialize memory usage metrics
  MEM_POOL_TOTAL_BYTES = m->AddGauge(
      ImpaladMetricKeys::MEM_POOL_TOTAL_BYTES, 0);
  HASH_TABLE_TOTAL_BYTES = m->AddGauge(
      ImpaladMetricKeys::HASH_TABLE_TOTAL_BYTES, 0);

  // Initialize insert metrics
  NUM_FILES_OPEN_FOR_INSERT = m->AddGauge(
      ImpaladMetricKeys::NUM_FILES_OPEN_FOR_INSERT, 0);

  // Initialize IO mgr metrics
  IO_MGR_NUM_OPEN_FILES = m->AddGauge(ImpaladMetricKeys::IO_MGR_NUM_OPEN_FILES, 0);
  IO_MGR_NUM_BUFFERS = m->AddGauge(ImpaladMetricKeys::IO_MGR_NUM_BUFFERS, 0);
  IO_MGR_TOTAL_BYTES = m->AddGauge(ImpaladMetricKeys::IO_MGR_TOTAL_BYTES, 0);
  IO_MGR_NUM_UNUSED_BUFFERS = m->AddGauge(
      ImpaladMetricKeys::IO_MGR_NUM_UNUSED_BUFFERS, 0);
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

  IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO =
      StatsMetric<uint64_t, StatsType::MEAN>::CreateAndRegister(m,
      ImpaladMetricKeys::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO);

  // Initialize catalog metrics
  CATALOG_NUM_DBS = m->AddGauge(ImpaladMetricKeys::CATALOG_NUM_DBS, 0);
  CATALOG_NUM_TABLES = m->AddGauge(ImpaladMetricKeys::CATALOG_NUM_TABLES, 0);
  CATALOG_VERSION = m->AddGauge(ImpaladMetricKeys::CATALOG_VERSION, 0);
  CATALOG_TOPIC_VERSION = m->AddGauge(ImpaladMetricKeys::CATALOG_TOPIC_VERSION, 0);
  CATALOG_SERVICE_ID = m->AddProperty<string>(ImpaladMetricKeys::CATALOG_SERVICE_ID, "");
  CATALOG_READY = m->AddProperty<bool>(ImpaladMetricKeys::CATALOG_READY, false);

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
