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


#ifndef IMPALA_UTIL_IMPALAD_METRICS_H
#define IMPALA_UTIL_IMPALAD_METRICS_H

#include "util/metrics.h"

namespace impala {

// Contains the keys (strings) for impala metrics.
class ImpaladMetricKeys {
 public:
  // Local time that the server started
  static const char* IMPALA_SERVER_START_TIME;

  // Full version string of the Impala server
  static const char* IMPALA_SERVER_VERSION;

  // True if Impala has finished initialisation
  static const char* IMPALA_SERVER_READY;

  // The last last time someone issued 'refresh' or an insert query was run from
  // this node.
  static const char* IMPALA_SERVER_LAST_REFRESH_TIME;

  // Number of queries executed by this server, including failed and cancelled
  // queries
  static const char* IMPALA_SERVER_NUM_QUERIES;

  // Number of fragments executed by this server, including failed and cancelled
  // queries
  static const char* IMPALA_SERVER_NUM_FRAGMENTS;

  // Number of open HiveServer2 sessions
  static const char* IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS;

  // Number of open Beeswax sessions
  static const char* IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS;

  // Number of scan ranges processed
  static const char* TOTAL_SCAN_RANGES_PROCESSED;

  // Number of scan ranges with missing volume id metadata
  static const char* NUM_SCAN_RANGES_MISSING_VOLUME_ID;

  // Number of bytes currently in use across all mem pools
  static const char* MEM_POOL_TOTAL_BYTES;

  // Number of bytes currently in use across all hash tables
  static const char* HASH_TABLE_TOTAL_BYTES;

  // Number of files currently opened by the io mgr
  static const char* IO_MGR_NUM_OPEN_FILES;

  // Number of IO buffers allocated by the io mgr
  static const char* IO_MGR_NUM_BUFFERS;

  // Number of IO buffers that are currently unused (and can be GC'ed)
  static const char* IO_MGR_NUM_UNUSED_BUFFERS;

  // Number of DBs in the catalog
  static const char* CATALOG_NUM_DBS;

  // Number of tables in the catalog
  static const char* CATALOG_NUM_TABLES;

  // Number of files open for insert
  static const char* NUM_FILES_OPEN_FOR_INSERT;
};

// Global impalad-wide metrics.  This is useful for objects that want to update metrics
// without having to do frequent metrics lookups.
// These get created by impala-server from the Metrics object in ExecEnv right when the
// ImpaladServer starts up.
class ImpaladMetrics {
 public:
  static Metrics::StringMetric* IMPALA_SERVER_START_TIME;
  static Metrics::StringMetric* IMPALA_SERVER_VERSION;
  static Metrics::BooleanMetric* IMPALA_SERVER_READY;
  static Metrics::StringMetric* IMPALA_SERVER_LAST_REFRESH_TIME;
  static Metrics::IntMetric* IMPALA_SERVER_NUM_QUERIES;
  static Metrics::IntMetric* IMPALA_SERVER_NUM_FRAGMENTS;
  static Metrics::IntMetric* IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS;
  static Metrics::IntMetric* IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS;
  static Metrics::IntMetric* NUM_RANGES_PROCESSED;
  static Metrics::IntMetric* NUM_RANGES_MISSING_VOLUME_ID;
  static Metrics::IntMetric* MEM_POOL_TOTAL_BYTES;
  static Metrics::IntMetric* HASH_TABLE_TOTAL_BYTES;
  static Metrics::IntMetric* IO_MGR_NUM_OPEN_FILES;
  static Metrics::IntMetric* IO_MGR_NUM_BUFFERS;
  static Metrics::IntMetric* IO_MGR_NUM_UNUSED_BUFFERS;
  static Metrics::IntMetric* CATALOG_NUM_DBS;
  static Metrics::IntMetric* CATALOG_NUM_TABLES;
  static Metrics::IntMetric* NUM_FILES_OPEN_FOR_INSERT;

  // Creates and initializes all metrics above in 'm'.
  static void CreateMetrics(Metrics* m);
};


};

#endif
