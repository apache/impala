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

  // Number of bytes used by IO buffers (used and unused).
  static const char* IO_MGR_TOTAL_BYTES;

  // Number of IO buffers that are currently unused (and can be GC'ed)
  static const char* IO_MGR_NUM_UNUSED_BUFFERS;

  // Total number of bytes read by the io mgr
  static const char* IO_MGR_BYTES_READ;

  // Total number of local bytes read by the io mgr
  static const char* IO_MGR_LOCAL_BYTES_READ;

  // Total number of short-circuit bytes read by the io mgr
  static const char* IO_MGR_SHORT_CIRCUIT_BYTES_READ;

  // Total number of cached bytes read by the io mgr
  static const char* IO_MGR_CACHED_BYTES_READ;

  // Total number of bytes written to disk by the io mgr (for spilling)
  static const char* IO_MGR_BYTES_WRITTEN;

  // Number of DBs in the catalog
  static const char* CATALOG_NUM_DBS;

  // Number of tables in the catalog
  static const char* CATALOG_NUM_TABLES;

  // True if the impalad catalog is ready (has received a valid catalog-update topic
  // entry from the state store). Reset to false while recovering from an invalid
  // catalog state, such as detecting a catalog-update topic entry originating from
  // a catalog server with an unexpected ID.
  static const char* CATALOG_READY;

  // Number of files open for insert
  static const char* NUM_FILES_OPEN_FOR_INSERT;

  // Number of sessions expired due to inactivity
  static const char* NUM_SESSIONS_EXPIRED;

  // Number of queries expired due to inactivity
  static const char* NUM_QUERIES_EXPIRED;

  // Number of queries that spilled.
  static const char* NUM_QUERIES_SPILLED;

  // Total number of rows cached to support HS2 FETCH_FIRST.
  static const char* RESULTSET_CACHE_TOTAL_NUM_ROWS;

  // Total bytes consumed for rows cached to support HS2 FETCH_FIRST.
  static const char* RESULTSET_CACHE_TOTAL_BYTES;
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
  static Metrics::BytesMetric* MEM_POOL_TOTAL_BYTES;
  static Metrics::BytesMetric* HASH_TABLE_TOTAL_BYTES;
  static Metrics::IntMetric* IO_MGR_NUM_OPEN_FILES;
  static Metrics::IntMetric* IO_MGR_NUM_BUFFERS;
  static Metrics::IntMetric* IO_MGR_TOTAL_BYTES;
  static Metrics::IntMetric* IO_MGR_NUM_UNUSED_BUFFERS;
  static Metrics::BytesMetric* IO_MGR_BYTES_READ;
  static Metrics::BytesMetric* IO_MGR_LOCAL_BYTES_READ;
  static Metrics::BytesMetric* IO_MGR_SHORT_CIRCUIT_BYTES_READ;
  static Metrics::BytesMetric* IO_MGR_CACHED_BYTES_READ;
  static Metrics::BytesMetric* IO_MGR_BYTES_WRITTEN;
  static Metrics::IntMetric* CATALOG_NUM_DBS;
  static Metrics::IntMetric* CATALOG_NUM_TABLES;
  static Metrics::BooleanMetric* CATALOG_READY;
  static Metrics::IntMetric* NUM_FILES_OPEN_FOR_INSERT;
  static Metrics::IntMetric* NUM_SESSIONS_EXPIRED;
  static Metrics::IntMetric* NUM_QUERIES_EXPIRED;
  static Metrics::IntMetric* NUM_QUERIES_SPILLED;
  static Metrics::IntMetric* RESULTSET_CACHE_TOTAL_NUM_ROWS;
  static Metrics::BytesMetric* RESULTSET_CACHE_TOTAL_BYTES;

  // Creates and initializes all metrics above in 'm'.
  static void CreateMetrics(Metrics* m);
};


};

#endif
