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

#ifndef IMPALA_SERVICE_QUERY_OPTIONS_H
#define IMPALA_SERVICE_QUERY_OPTIONS_H

#include <string>
#include <map>
#include <unordered_map>
#include <bitset>

#include "common/status.h"

/// Utility methods to process per-query options

namespace impala {

class TQueryOptions;

// Maps query option names to option levels used for displaying the query
// options via SET and SET ALL
typedef std::unordered_map<string, beeswax::TQueryOptionLevel::type>
    QueryOptionLevels;

// Macro to help generate functions that use or manipulate query options.
// If the DCHECK is hit then handle the missing query option below and update
// the DCHECK.
#define QUERY_OPTS_TABLE\
  DCHECK_EQ(_TImpalaQueryOptions_VALUES_TO_NAMES.size(),\
      TImpalaQueryOptions::EXEC_TIME_LIMIT_S + 1);\
  REMOVED_QUERY_OPT_FN(abort_on_default_limit_exceeded, ABORT_ON_DEFAULT_LIMIT_EXCEEDED)\
  QUERY_OPT_FN(abort_on_error, ABORT_ON_ERROR, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(allow_unsupported_formats, ALLOW_UNSUPPORTED_FORMATS,\
      TQueryOptionLevel::DEPRECATED)\
  QUERY_OPT_FN(batch_size, BATCH_SIZE, TQueryOptionLevel::DEVELOPMENT)\
  QUERY_OPT_FN(debug_action, DEBUG_ACTION, TQueryOptionLevel::DEVELOPMENT)\
  REMOVED_QUERY_OPT_FN(default_order_by_limit, DEFAULT_ORDER_BY_LIMIT)\
  REMOVED_QUERY_OPT_FN(disable_cached_reads, DISABLE_CACHED_READS)\
  QUERY_OPT_FN(disable_outermost_topn, DISABLE_OUTERMOST_TOPN,\
      TQueryOptionLevel::DEVELOPMENT)\
  QUERY_OPT_FN(disable_codegen, DISABLE_CODEGEN, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(explain_level, EXPLAIN_LEVEL, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(hbase_cache_blocks, HBASE_CACHE_BLOCKS, TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(hbase_caching, HBASE_CACHING, TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(max_errors, MAX_ERRORS, TQueryOptionLevel::ADVANCED)\
  REMOVED_QUERY_OPT_FN(max_io_buffers, MAX_IO_BUFFERS)\
  QUERY_OPT_FN(max_scan_range_length, MAX_SCAN_RANGE_LENGTH,\
      TQueryOptionLevel::DEVELOPMENT)\
  QUERY_OPT_FN(mem_limit, MEM_LIMIT, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(num_nodes, NUM_NODES, TQueryOptionLevel::DEVELOPMENT)\
  QUERY_OPT_FN(num_scanner_threads, NUM_SCANNER_THREADS, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(compression_codec, COMPRESSION_CODEC, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(parquet_file_size, PARQUET_FILE_SIZE, TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(request_pool, REQUEST_POOL, TQueryOptionLevel::REGULAR)\
  REMOVED_QUERY_OPT_FN(reservation_request_timeout, RESERVATION_REQUEST_TIMEOUT)\
  QUERY_OPT_FN(sync_ddl, SYNC_DDL, TQueryOptionLevel::REGULAR)\
  REMOVED_QUERY_OPT_FN(v_cpu_cores, V_CPU_CORES)\
  REMOVED_QUERY_OPT_FN(rm_initial_mem, RM_INITIAL_MEM)\
  QUERY_OPT_FN(query_timeout_s, QUERY_TIMEOUT_S, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(buffer_pool_limit, BUFFER_POOL_LIMIT, TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(appx_count_distinct, APPX_COUNT_DISTINCT, TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(disable_unsafe_spills, DISABLE_UNSAFE_SPILLS, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(seq_compression_mode, SEQ_COMPRESSION_MODE, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(exec_single_node_rows_threshold, EXEC_SINGLE_NODE_ROWS_THRESHOLD,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(optimize_partition_key_scans, OPTIMIZE_PARTITION_KEY_SCANS,\
      TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(replica_preference, REPLICA_PREFERENCE, TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(schedule_random_replica, SCHEDULE_RANDOM_REPLICA,\
      TQueryOptionLevel::ADVANCED)\
  REMOVED_QUERY_OPT_FN(scan_node_codegen_threshold, SCAN_NODE_CODEGEN_THRESHOLD)\
  QUERY_OPT_FN(disable_streaming_preaggregations, DISABLE_STREAMING_PREAGGREGATIONS,\
      TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(runtime_filter_mode, RUNTIME_FILTER_MODE, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(runtime_bloom_filter_size, RUNTIME_BLOOM_FILTER_SIZE,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(runtime_filter_wait_time_ms, RUNTIME_FILTER_WAIT_TIME_MS,\
      TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(disable_row_runtime_filtering, DISABLE_ROW_RUNTIME_FILTERING,\
      TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(max_num_runtime_filters, MAX_NUM_RUNTIME_FILTERS,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(parquet_annotate_strings_utf8, PARQUET_ANNOTATE_STRINGS_UTF8,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(parquet_fallback_schema_resolution, PARQUET_FALLBACK_SCHEMA_RESOLUTION,\
      TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(mt_dop, MT_DOP, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(s3_skip_insert_staging, S3_SKIP_INSERT_STAGING,\
      TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(runtime_filter_min_size, RUNTIME_FILTER_MIN_SIZE,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(runtime_filter_max_size, RUNTIME_FILTER_MAX_SIZE,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(prefetch_mode, PREFETCH_MODE, TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(strict_mode, STRICT_MODE, TQueryOptionLevel::DEVELOPMENT)\
  QUERY_OPT_FN(scratch_limit, SCRATCH_LIMIT, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(enable_expr_rewrites, ENABLE_EXPR_REWRITES, TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(decimal_v2, DECIMAL_V2, TQueryOptionLevel::DEVELOPMENT)\
  QUERY_OPT_FN(parquet_dictionary_filtering, PARQUET_DICTIONARY_FILTERING,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(parquet_array_resolution, PARQUET_ARRAY_RESOLUTION,\
      TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(parquet_read_statistics, PARQUET_READ_STATISTICS,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(default_join_distribution_mode, DEFAULT_JOIN_DISTRIBUTION_MODE,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(disable_codegen_rows_threshold, DISABLE_CODEGEN_ROWS_THRESHOLD,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(default_spillable_buffer_size, DEFAULT_SPILLABLE_BUFFER_SIZE,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(min_spillable_buffer_size, MIN_SPILLABLE_BUFFER_SIZE,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(max_row_size, MAX_ROW_SIZE, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(idle_session_timeout, IDLE_SESSION_TIMEOUT, TQueryOptionLevel::REGULAR)\
  QUERY_OPT_FN(compute_stats_min_sample_size, COMPUTE_STATS_MIN_SAMPLE_SIZE,\
      TQueryOptionLevel::ADVANCED)\
  QUERY_OPT_FN(exec_time_limit_s, EXEC_TIME_LIMIT_S, TQueryOptionLevel::REGULAR)\
  ;

/// Enforce practical limits on some query options to avoid undesired query state.
  static const int64_t SPILLABLE_BUFFER_LIMIT = 1LL << 40; // 1 TB
  static const int64_t ROW_SIZE_LIMIT = 1LL << 40; // 1 TB

/// Converts a TQueryOptions struct into a map of key, value pairs.  Options that
/// aren't set and lack defaults in common/thrift/ImpalaInternalService.thrift are
/// mapped to the empty string.
void TQueryOptionsToMap(const TQueryOptions& query_options,
    std::map<std::string, std::string>* configuration);

/// Returns a comma-delimted string of the contents of query_options. The output does not
/// contain key-value pairs where the value matches the default value specified in the
/// TQueryOptions definition (regardless of whether or not it was explicitly or
/// implicitly set to the default value).
std::string DebugQueryOptions(const TQueryOptions& query_options);

/// Bitmask for the values of TQueryOptions.
/// TODO: Find a way to set the size based on the number of fields.
typedef std::bitset<64> QueryOptionsMask;

/// Updates the query options in dst from those in src where the query option is set
/// (i.e. src->__isset.PROPERTY is true) and the corresponding bit in mask is set. If
/// mask has no set bits, no options are set. If all bits are set, then all options
/// that were set on src are copied to dst.
void OverlayQueryOptions(const TQueryOptions& src, const QueryOptionsMask& mask,
    TQueryOptions* dst);

/// Set the key/value pair in TQueryOptions. It will override existing setting in
/// query_options. The bit corresponding to query option 'key' in set_query_options_mask
/// is set. An empty string value will reset the key to its default value.
Status SetQueryOption(const std::string& key, const std::string& value,
    TQueryOptions* query_options, QueryOptionsMask* set_query_options_mask);

/// Parse a "," separated key=value pair of query options and set it in 'query_options'.
/// If the same query option is specified more than once, the last one wins. The
/// set_query_options_mask bitmask is updated to reflect the query options which were
/// set. Returns an error status containing an error detail for any invalid options (e.g.
/// bad format or invalid query option), but all valid query options are still handled.
Status ParseQueryOptions(const std::string& options, TQueryOptions* query_options,
    QueryOptionsMask* set_query_options_mask);

/// Based on the query option levels provided to QUERY_OPT_FN macro this function
/// populates the received QueryOptionLevels map with (option name -> option level)
/// entries.
void PopulateQueryOptionLevels(QueryOptionLevels* query_option_levels);

}

#endif
