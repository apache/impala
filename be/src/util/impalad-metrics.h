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

#include "util/metrics-fwd.h"

namespace impala {

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

  /// Number of queries that started executing on this backend.
  static const char* BACKEND_NUM_QUERIES_EXECUTED;

  /// Number of queries currently executing on this backend.
  static const char* BACKEND_NUM_QUERIES_EXECUTING;

  /// Number of open HiveServer2 sessions
  static const char* IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS;

  /// Number of open Beeswax sessions
  static const char* IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS;

  /// Number of scan ranges processed
  static const char* TOTAL_SCAN_RANGES_PROCESSED;

  /// Number of scan ranges with missing volume id metadata
  static const char* NUM_SCAN_RANGES_MISSING_VOLUME_ID;

  /// Number of files currently opened by the io mgr
  static const char* IO_MGR_NUM_OPEN_FILES;

  /// Total number of bytes read by the io mgr
  static const char* IO_MGR_BYTES_READ;

  /// Total number of local bytes read by the io mgr
  static const char* IO_MGR_LOCAL_BYTES_READ;

  /// Total number of short-circuit bytes read by the io mgr
  static const char* IO_MGR_SHORT_CIRCUIT_BYTES_READ;

  /// Total number of cached bytes read by the io mgr
  static const char* IO_MGR_CACHED_BYTES_READ;

  /// Total number of encrypted bytes read by the io mgr
  static const char* IO_MGR_ENCRYPTED_BYTES_READ;

  /// Total number of erasure-coded bytes read by the io mgr
  static const char* IO_MGR_ERASURE_CODED_BYTES_READ;

  /// Total number of bytes read from the remote data cache.
  static const char* IO_MGR_REMOTE_DATA_CACHE_HIT_BYTES;

  /// Total number of cache hits for the remote data cache.
  static const char* IO_MGR_REMOTE_DATA_CACHE_HIT_COUNT;

  /// Total number of bytes missing from the remote data cache.
  static const char* IO_MGR_REMOTE_DATA_CACHE_MISS_BYTES;

  /// Total number of cache misses for the remote data cache.
  static const char* IO_MGR_REMOTE_DATA_CACHE_MISS_COUNT;

  /// Current byte size of the remote data cache.
  static const char* IO_MGR_REMOTE_DATA_CACHE_TOTAL_BYTES;

  /// Current number of entries in the remote data cache.
  static const char* IO_MGR_REMOTE_DATA_CACHE_NUM_ENTRIES;

  /// Total number of writes for the remote data cache.
  static const char* IO_MGR_REMOTE_DATA_CACHE_NUM_WRITES;

  /// Total number of bytes not inserted into the remote data cache due to
  /// concurrency limit.
  static const char* IO_MGR_REMOTE_DATA_CACHE_DROPPED_BYTES;

  /// Total number of entries not inserted into the remote data cache due to
  /// concurrency limit.
  static const char* IO_MGR_REMOTE_DATA_CACHE_DROPPED_ENTRIES;

  /// Total number of entries evicted immediately from the remote data cache.
  static const char* IO_MGR_REMOTE_DATA_CACHE_INSTANT_EVICTIONS;

  /// Total number of bytes async writes outstanding in the remote data cache.
  static const char* IO_MGR_REMOTE_DATA_CACHE_ASYNC_WRITES_OUTSTANDING_BYTES;

  /// Total number of async writes submitted in the remote data cache.
  static const char* IO_MGR_REMOTE_DATA_CACHE_NUM_ASYNC_WRITES_SUBMITTED;

  /// Total number of bytes not inserted in the remote data cache due to async writes
  /// buffer size limit.
  static const char* IO_MGR_REMOTE_DATA_CACHE_ASYNC_WRITES_DROPPED_BYTES;

  /// Total number of entries not inserted in the remote data cache due to async writes
  /// buffer size limit.
  static const char* IO_MGR_REMOTE_DATA_CACHE_ASYNC_WRITES_DROPPED_ENTRIES;

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

  /// Lower bound of catalog object version in local catalog cache. All catalog objects
  /// have catalog version larger than this.
  static const char* CATALOG_OBJECT_VERSION_LOWER_BOUND;

  /// Catalog topic version  with impalad.
  static const char* CATALOG_TOPIC_VERSION;

  /// ServiceID of Catalog with impalad.
  static const char* CATALOG_SERVICE_ID;

  // Address of active catalogd.
  static const char* ACTIVE_CATALOGD_ADDRESS;

  /// Number of tables in the catalog
  static const char* CATALOG_NUM_TABLES;

  /// True if the impalad catalog is ready (has received a valid catalog-update topic
  /// entry from the state store). Reset to false while recovering from an invalid
  /// catalog state, such as detecting a catalog-update topic entry originating from
  /// a catalog server with an unexpected ID.
  static const char* CATALOG_READY;

  /// Average time spent loading new values into the Impalad Catalog Cache.
  static const char* CATALOG_CACHE_AVG_LOAD_TIME;

  /// Total number of evictions from the Impalad Catalog Cache. Does not include manual
  /// cache invalidate calls.
  static const char* CATALOG_CACHE_EVICTION_COUNT;

  /// Total number of Impalad Catalog cache hits.
  static const char* CATALOG_CACHE_HIT_COUNT;

  /// Ratio of Impalad Catalog cache requests that were hits. Accounts for all the
  /// requests since the process boot time.
  static const char* CATALOG_CACHE_HIT_RATE;

  /// Total requests to Impalad Catalog cache requests that loaded new values.
  static const char* CATALOG_CACHE_LOAD_COUNT;

  /// Total requests to Impalad Catalog cache requests that threw exceptions loading
  /// new values.
  static const char* CATALOG_CACHE_LOAD_EXCEPTION_COUNT;

  /// Ratio of Impalad Catalog cache requests that threw exceptions loading new values.
  /// Accounts for all the requests since the process boot time.
  static const char* CATALOG_CACHE_LOAD_EXCEPTION_RATE;

  /// Number of Impalad Catalog cache requests that successfully loaded new values.
  static const char* CATALOG_CACHE_LOAD_SUCCESS_COUNT;

  /// Number of Impalad Catalog cache requests that returned uncached values.
  static const char* CATALOG_CACHE_MISS_COUNT;

  /// Ratio of Impalad Catalog cache requests that were misses. Accounts for all the
  /// requests since the process boot time.
  static const char* CATALOG_CACHE_MISS_RATE;

  /// Total number of Impalad Catalog cache requests.
  static const char* CATALOG_CACHE_REQUEST_COUNT;

  /// Total time spent in Impalad Catalog cache loading new values.
  static const char* CATALOG_CACHE_TOTAL_LOAD_TIME;

  /// Median size of Impalad Catalog cache entries.
  static const char* CATALOG_CACHE_ENTRY_MEDIAN_SIZE;

  /// 99th percentile size of Impalad Catalog cache entries.
  static const char* CATALOG_CACHE_ENTRY_99TH_SIZE;

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

  /// Total number of times the FAIL debug action is hit. The counter is only created if
  /// --debug_actions is set.
  static const char* DEBUG_ACTION_NUM_FAIL;

  // Estimated total size of query logs that are currently retained in memory.
  // Associated metric is modified in ImpalaServer::ArchiveQuery and must hold
  // ImpalaServer::query_log_lock_ on modification.
  static const char* QUERY_LOG_EST_TOTAL_BYTES;

  /// The number of completed queries queued up and waiting to be written to the query
  /// log table.
  static const char* COMPLETED_QUERIES_QUEUED;

  /// The number of completed queries successfully written to the query log table.
  static const char* COMPLETED_QUERIES_WRITTEN;

  /// The number of completed queries that failed to be written to the query log
  /// table.
  static const char* COMPLETED_QUERIES_FAIL;

  /// Number of writes to the query log table that happened at the regularly scheduled
  /// time.
  static const char* COMPLETED_QUERIES_SCHEDULED_WRITES;

  /// Number of writes to the query log table that happened because the max queued
  /// completed queries records was reached.
  static const char* COMPLETED_QUERIES_MAX_RECORDS_WRITES;

  /// Time spent writing completed queries to the query log table.
  static const char* COMPLETED_QUERIES_WRITE_DURATIONS;
};

/// Global impalad-wide metrics.  This is useful for objects that want to update metrics
/// without having to do frequent metrics lookups.
/// These get created by impala-server from the Metrics object in ExecEnv right when the
/// ImpaladServer starts up.
class ImpaladMetrics {
 public:
  // Counters
  static IntCounter* BACKEND_NUM_QUERIES_EXECUTED;
  /// BACKEND_NUM_QUERIES_EXECUTING is used to determine when the backend has quiesced
  /// and can be safely shut down without causing query failures. See IMPALA-7931 for
  /// an example of a race that can occur if this is decremented before a query is
  /// truly finished.
  static IntGauge* BACKEND_NUM_QUERIES_EXECUTING;
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
  static IntCounter* IO_MGR_ENCRYPTED_BYTES_READ;
  static IntCounter* IO_MGR_ERASURE_CODED_BYTES_READ;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_HIT_BYTES;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_HIT_COUNT;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_MISS_BYTES;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_MISS_COUNT;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_NUM_WRITES;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_DROPPED_BYTES;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_DROPPED_ENTRIES;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_INSTANT_EVICTIONS;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_ASYNC_WRITES_OUTSTANDING_BYTES;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_NUM_ASYNC_WRITES_SUBMITTED;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_ASYNC_WRITES_DROPPED_BYTES;
  static IntCounter* IO_MGR_REMOTE_DATA_CACHE_ASYNC_WRITES_DROPPED_ENTRIES;
  static IntCounter* IO_MGR_SHORT_CIRCUIT_BYTES_READ;
  static IntCounter* IO_MGR_BYTES_WRITTEN;
  static IntCounter* IO_MGR_CACHED_FILE_HANDLES_REOPENED;
  static IntCounter* HEDGED_READ_OPS;
  static IntCounter* HEDGED_READ_OPS_WIN;
  static IntCounter* CATALOG_CACHE_EVICTION_COUNT;
  static IntCounter* CATALOG_CACHE_HIT_COUNT;
  static IntCounter* CATALOG_CACHE_LOAD_COUNT;
  static IntCounter* CATALOG_CACHE_LOAD_EXCEPTION_COUNT;
  static IntCounter* CATALOG_CACHE_LOAD_SUCCESS_COUNT;
  static IntCounter* CATALOG_CACHE_MISS_COUNT;
  static IntCounter* CATALOG_CACHE_REQUEST_COUNT;
  static IntCounter* CATALOG_CACHE_TOTAL_LOAD_TIME;
  static IntCounter* DEBUG_ACTION_NUM_FAIL;
  static IntCounter* COMPLETED_QUERIES_WRITTEN;
  static IntCounter* COMPLETED_QUERIES_FAIL;
  static IntCounter* COMPLETED_QUERIES_SCHEDULED_WRITES;
  static IntCounter* COMPLETED_QUERIES_MAX_RECORDS_WRITES;

  // Gauges
  static IntGauge* CATALOG_NUM_DBS;
  static IntGauge* CATALOG_NUM_TABLES;
  static IntGauge* CATALOG_VERSION;
  static IntGauge* CATALOG_OBJECT_VERSION_LOWER_BOUND;
  static IntGauge* CATALOG_TOPIC_VERSION;
  static DoubleGauge* CATALOG_CACHE_AVG_LOAD_TIME;
  static DoubleGauge* CATALOG_CACHE_HIT_RATE;
  static DoubleGauge* CATALOG_CACHE_LOAD_EXCEPTION_RATE;
  static DoubleGauge* CATALOG_CACHE_MISS_RATE;
  static DoubleGauge* CATALOG_CACHE_ENTRY_MEDIAN_SIZE;
  static DoubleGauge* CATALOG_CACHE_ENTRY_99TH_SIZE;
  static IntGauge* IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS;
  static IntGauge* IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS;
  static MetricGroup* IO_MGR_METRICS;
  static IntGauge* IO_MGR_NUM_BUFFERS;
  static IntGauge* IO_MGR_NUM_OPEN_FILES;
  static IntGauge* IO_MGR_NUM_UNUSED_BUFFERS;
  static IntGauge* IO_MGR_NUM_CACHED_FILE_HANDLES;
  static IntGauge* IO_MGR_NUM_FILE_HANDLES_OUTSTANDING;
  static IntGauge* IO_MGR_CACHED_FILE_HANDLES_HIT_COUNT;
  static IntGauge* IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT;
  static IntGauge* IO_MGR_REMOTE_DATA_CACHE_TOTAL_BYTES;
  static IntGauge* IO_MGR_REMOTE_DATA_CACHE_NUM_ENTRIES;
  static IntGauge* NUM_FILES_OPEN_FOR_INSERT;
  static IntGauge* NUM_QUERIES_REGISTERED;
  static IntGauge* RESULTSET_CACHE_TOTAL_NUM_ROWS;
  static IntGauge* RESULTSET_CACHE_TOTAL_BYTES;
  static IntGauge* QUERY_LOG_EST_TOTAL_BYTES;
  static IntGauge* COMPLETED_QUERIES_QUEUED;

  // Properties
  static BooleanProperty* CATALOG_READY;
  static BooleanProperty* IMPALA_SERVER_READY;
  static StringProperty* IMPALA_SERVER_VERSION;
  static StringProperty* CATALOG_SERVICE_ID;
  static StringProperty* ACTIVE_CATALOGD_ADDRESS;
  // Histograms
  static HistogramMetric* QUERY_DURATIONS;
  static HistogramMetric* DDL_DURATIONS;
  static HistogramMetric* COMPLETED_QUERIES_WRITE_DURATIONS;

  // Other
  static StatsMetric<uint64_t, StatsType::MEAN>* IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO;

  // Creates and initializes all metrics above in 'm'.
  static void CreateMetrics(MetricGroup* m);

 private:
  // Initializes the metrics for this coordinator's metadata catalog.
  static void InitCatalogMetrics(MetricGroup* m);
};


};

#endif
