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


#ifndef IMPALA_UTIL_IMPALAD_METRICS_H
#define IMPALA_UTIL_IMPALAD_METRICS_H

#include "util/metrics.h"
#include "util/collection-metrics.h"

namespace impala {

class HistogramMetric;

/// Contains the keys (strings) for impala metrics.
class ImpaladMetricKeys {
 public:
  /// Full version string of the Impala server
  static const char* IMPALA_SERVER_VERSION;

  /// True if Impala has finished initialisation
  static const char* IMPALA_SERVER_READY;

  /// Number of queries executed by this server, including failed and cancelled
  /// queries
  static const char* IMPALA_SERVER_NUM_QUERIES;

  /// Number of fragments executed by this server, including failed and cancelled
  /// queries
  static const char* IMPALA_SERVER_NUM_FRAGMENTS;

  /// Number of fragments currently running on this server.
  static const char* IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT;

  /// Number of open HiveServer2 sessions
  static const char* IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS;

  /// Number of open Beeswax sessions
  static const char* IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS;

  /// Number of scan ranges processed
  static const char* TOTAL_SCAN_RANGES_PROCESSED;

  /// Number of scan ranges with missing volume id metadata
  static const char* NUM_SCAN_RANGES_MISSING_VOLUME_ID;

  /// Number of bytes currently in use across all mem pools
  static const char* MEM_POOL_TOTAL_BYTES;

  /// Number of bytes currently in use across all hash tables
  static const char* HASH_TABLE_TOTAL_BYTES;

  /// Number of files currently opened by the io mgr
  static const char* IO_MGR_NUM_OPEN_FILES;

  /// Number of IO buffers allocated by the io mgr
  static const char* IO_MGR_NUM_BUFFERS;

  /// Number of bytes used by IO buffers (used and unused).
  static const char* IO_MGR_TOTAL_BYTES;

  /// Number of IO buffers that are currently unused (and can be GC'ed)
  static const char* IO_MGR_NUM_UNUSED_BUFFERS;

  /// Total number of bytes read by the io mgr
  static const char* IO_MGR_BYTES_READ;

  /// Total number of local bytes read by the io mgr
  static const char* IO_MGR_LOCAL_BYTES_READ;

  /// Total number of short-circuit bytes read by the io mgr
  static const char* IO_MGR_SHORT_CIRCUIT_BYTES_READ;

  /// Total number of cached bytes read by the io mgr
  static const char* IO_MGR_CACHED_BYTES_READ;

  /// Total number of bytes written to disk by the io mgr (for spilling)
  static const char* IO_MGR_BYTES_WRITTEN;

  /// Number of unbuffered file handles cached by the io mgr
  static const char* IO_MGR_NUM_CACHED_FILE_HANDLES;

  /// Number of file handles that are open and not cached
  static const char* IO_MGR_NUM_FILE_HANDLES_OUTSTANDING;

  /// Hit ratio for the cached HDFS file handles
  static const char* IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO;

  /// Number of cache hits for cached HDFS file handles
  static const char* IO_MGR_CACHED_FILE_HANDLES_HIT_COUNT;

  /// Number of cache misses for cached HDFS file handles
  static const char* IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT;

  /// Number of cached file handles that hit an error and were reopened
  static const char* IO_MGR_CACHED_FILE_HANDLES_REOPENED;

  /// Number of DBs in the catalog
  static const char* CATALOG_NUM_DBS;

  /// Current version of catalog with impalad.
  static const char* CATALOG_VERSION;

  /// Catalog topic version  with impalad.
  static const char* CATALOG_TOPIC_VERSION;

  /// ServiceID of Catalog with impalad.
  static const char* CATALOG_SERVICE_ID;

  /// Number of tables in the catalog
  static const char* CATALOG_NUM_TABLES;

  /// True if the impalad catalog is ready (has received a valid catalog-update topic
  /// entry from the state store). Reset to false while recovering from an invalid
  /// catalog state, such as detecting a catalog-update topic entry originating from
  /// a catalog server with an unexpected ID.
  static const char* CATALOG_READY;

  /// Number of files open for insert
  static const char* NUM_FILES_OPEN_FOR_INSERT;

  /// Number of sessions expired due to inactivity
  static const char* NUM_SESSIONS_EXPIRED;

  /// Number of queries expired due to inactivity
  static const char* NUM_QUERIES_EXPIRED;

  /// Number of queries currently registered on this server, i.e. that have been
  /// registered but are not yet unregistered.
  static const char* NUM_QUERIES_REGISTERED;

  /// Number of queries that spilled.
  static const char* NUM_QUERIES_SPILLED;

  /// Total number of rows cached to support HS2 FETCH_FIRST.
  static const char* RESULTSET_CACHE_TOTAL_NUM_ROWS;

  /// Total bytes consumed for rows cached to support HS2 FETCH_FIRST.
  static const char* RESULTSET_CACHE_TOTAL_BYTES;

  /// Distribution of execution times for queries and DDL statements, in ms.
  static const char* QUERY_DURATIONS;
  static const char* DDL_DURATIONS;

  /// Total number of attempted hedged reads operations.
  static const char* HEDGED_READ_OPS;

  /// Total number of hedged reads operations that won
  /// (i.e. returned faster than original read).
  static const char* HEDGED_READ_OPS_WIN;

};

/// Global impalad-wide metrics.  This is useful for objects that want to update metrics
/// without having to do frequent metrics lookups.
/// These get created by impala-server from the Metrics object in ExecEnv right when the
/// ImpaladServer starts up.
class ImpaladMetrics {
 public:
  // Counters
  static IntGauge* HASH_TABLE_TOTAL_BYTES;
  static IntCounter* IMPALA_SERVER_NUM_FRAGMENTS;
  static IntGauge* IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT;
  static IntCounter* IMPALA_SERVER_NUM_QUERIES;
  static IntCounter* NUM_QUERIES_EXPIRED;
  static IntCounter* NUM_QUERIES_SPILLED;
  static IntCounter* NUM_RANGES_MISSING_VOLUME_ID;
  static IntCounter* NUM_RANGES_PROCESSED;
  static IntCounter* NUM_SESSIONS_EXPIRED;
  static IntCounter* IO_MGR_BYTES_READ;
  static IntCounter* IO_MGR_LOCAL_BYTES_READ;
  static IntCounter* IO_MGR_CACHED_BYTES_READ;
  static IntCounter* IO_MGR_SHORT_CIRCUIT_BYTES_READ;
  static IntCounter* IO_MGR_BYTES_WRITTEN;
  static IntCounter* IO_MGR_CACHED_FILE_HANDLES_REOPENED;
  static IntCounter* HEDGED_READ_OPS;
  static IntCounter* HEDGED_READ_OPS_WIN;

  // Gauges
  static IntGauge* CATALOG_NUM_DBS;
  static IntGauge* CATALOG_NUM_TABLES;
  static IntGauge* CATALOG_VERSION;
  static IntGauge* CATALOG_TOPIC_VERSION;
  static IntGauge* IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS;
  static IntGauge* IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS;
  static IntGauge* IO_MGR_NUM_BUFFERS;
  static IntGauge* IO_MGR_NUM_OPEN_FILES;
  static IntGauge* IO_MGR_NUM_UNUSED_BUFFERS;
  static IntGauge* IO_MGR_NUM_CACHED_FILE_HANDLES;
  static IntGauge* IO_MGR_NUM_FILE_HANDLES_OUTSTANDING;
  static IntGauge* IO_MGR_CACHED_FILE_HANDLES_HIT_COUNT;
  static IntGauge* IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT;
  static IntGauge* IO_MGR_TOTAL_BYTES;
  static IntGauge* MEM_POOL_TOTAL_BYTES;
  static IntGauge* NUM_FILES_OPEN_FOR_INSERT;
  static IntGauge* NUM_QUERIES_REGISTERED;
  static IntGauge* RESULTSET_CACHE_TOTAL_NUM_ROWS;
  static IntGauge* RESULTSET_CACHE_TOTAL_BYTES;
  // Properties
  static BooleanProperty* CATALOG_READY;
  static BooleanProperty* IMPALA_SERVER_READY;
  static StringProperty* IMPALA_SERVER_VERSION;
  static StringProperty* CATALOG_SERVICE_ID;
  // Histograms
  static HistogramMetric* QUERY_DURATIONS;
  static HistogramMetric* DDL_DURATIONS;

  // Other
  static StatsMetric<uint64_t, StatsType::MEAN>* IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO;

  // Creates and initializes all metrics above in 'm'.
  static void CreateMetrics(MetricGroup* m);
};


};

#endif
