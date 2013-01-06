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

#include "util/impalad-metrics.h"

namespace impala {

const char* ImpaladMetricKeys::IMPALA_SERVER_NUM_QUERIES = 
    "impala-server.num.queries";
const char* ImpaladMetricKeys::IMPALA_SERVER_NUM_FRAGMENTS = 
    "impala-server.num.fragments";
const char* ImpaladMetricKeys::TOTAL_SCAN_RANGES_PROCESSED = 
    "impala-server.scan.ranges.total";
const char* ImpaladMetricKeys::NUM_SCAN_RANGES_MISSING_VOLUME_ID = 
    "impala-server.scan_ranges.num-missing-volume-id";
const char* ImpaladMetricKeys::MEM_POOL_TOTAL_BYTES = 
    "impala-server.mem.pool.total-bytes";
const char* ImpaladMetricKeys::IO_MGR_NUM_OPEN_FILES = 
    "impala-server.io.mgr.num.open-files";
const char* ImpaladMetricKeys::IO_MGR_NUM_BUFFERS = 
    "impala-server.io.mgr.num.buffers";

// These are created by impala-server during startup.
Metrics::IntMetric* ImpaladMetrics::IMPALA_SERVER_NUM_QUERIES = NULL;
Metrics::IntMetric* ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS = NULL;
Metrics::IntMetric* ImpaladMetrics::NUM_RANGES_PROCESSED = NULL;
Metrics::IntMetric* ImpaladMetrics::NUM_RANGES_MISSING_VOLUME_ID = NULL;
Metrics::IntMetric* ImpaladMetrics::MEM_POOL_TOTAL_BYTES = NULL;
Metrics::IntMetric* ImpaladMetrics::IO_MGR_NUM_OPEN_FILES = NULL;
Metrics::IntMetric* ImpaladMetrics::IO_MGR_NUM_BUFFERS = NULL;

void ImpaladMetrics::CreateMetrics(Metrics* m) {
  // Initialize impalad metrics
  IMPALA_SERVER_NUM_QUERIES = m->CreateAndRegisterPrimitiveMetric(
      ImpaladMetricKeys::IMPALA_SERVER_NUM_QUERIES, 0L);
  IMPALA_SERVER_NUM_FRAGMENTS = m->CreateAndRegisterPrimitiveMetric(
      ImpaladMetricKeys::IMPALA_SERVER_NUM_FRAGMENTS, 0L);

  // Initialize scan node metrics
  NUM_RANGES_PROCESSED = m->CreateAndRegisterPrimitiveMetric(
      ImpaladMetricKeys::TOTAL_SCAN_RANGES_PROCESSED, 0L);
  NUM_RANGES_MISSING_VOLUME_ID = m->CreateAndRegisterPrimitiveMetric(
      ImpaladMetricKeys::NUM_SCAN_RANGES_MISSING_VOLUME_ID, 0L);

  // Initialize memory usage metrics
  MEM_POOL_TOTAL_BYTES = m->CreateAndRegisterPrimitiveMetric(
      ImpaladMetricKeys::MEM_POOL_TOTAL_BYTES, 0L);

  // Initialize IO mgr metrics
  IO_MGR_NUM_OPEN_FILES = m->CreateAndRegisterPrimitiveMetric(
      ImpaladMetricKeys::IO_MGR_NUM_OPEN_FILES, 0L);
  IO_MGR_NUM_BUFFERS = m->CreateAndRegisterPrimitiveMetric(
      ImpaladMetricKeys::IO_MGR_NUM_BUFFERS, 0L);
}

}

