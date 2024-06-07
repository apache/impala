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

std::ostream& operator<<(std::ostream& out,
    const std::set<impala::TRuntimeFilterType::type>& filter_types);

std::ostream& operator<<(std::ostream& out, const std::set<int32_t>& filter_ids);

// Maps query option names to option levels used for displaying the query
// options via SET and SET ALL
typedef std::unordered_map<string, beeswax::TQueryOptionLevel::type>
    QueryOptionLevels;

// Macro to help generate functions that use or manipulate query options.
// If the DCHECK is hit then handle the missing query option below and update
// the DCHECK.
// Specifically, the DCHECK will make sure that the number of elements in
// the map _TImpalaQueryOptions_VALUES_TO_NAMES automatically generated in
// ImpalaService_types.cpp is equal to the largest integer associated with an
// option in the enum TImpalaQueryOptions (defined in ImpalaService.thrift)
// plus one. Thus, the second argument to the DCHECK has to be updated every
// time we add or remove a query option to/from the enum TImpalaQueryOptions.
#define QUERY_OPTS_TABLE                                                                 \
  DCHECK_EQ(_TImpalaQueryOptions_VALUES_TO_NAMES.size(),                                 \
      TImpalaQueryOptions::WRITE_KUDU_UTC_TIMESTAMPS + 1);                               \
  REMOVED_QUERY_OPT_FN(abort_on_default_limit_exceeded, ABORT_ON_DEFAULT_LIMIT_EXCEEDED) \
  QUERY_OPT_FN(abort_on_error, ABORT_ON_ERROR, TQueryOptionLevel::REGULAR)               \
  REMOVED_QUERY_OPT_FN(allow_unsupported_formats, ALLOW_UNSUPPORTED_FORMATS)             \
  QUERY_OPT_FN(batch_size, BATCH_SIZE, TQueryOptionLevel::DEVELOPMENT)                   \
  QUERY_OPT_FN(debug_action, DEBUG_ACTION, TQueryOptionLevel::DEVELOPMENT)               \
  REMOVED_QUERY_OPT_FN(default_order_by_limit, DEFAULT_ORDER_BY_LIMIT)                   \
  REMOVED_QUERY_OPT_FN(disable_cached_reads, DISABLE_CACHED_READS)                       \
  QUERY_OPT_FN(                                                                          \
      disable_outermost_topn, DISABLE_OUTERMOST_TOPN, TQueryOptionLevel::DEVELOPMENT)    \
  QUERY_OPT_FN(disable_codegen, DISABLE_CODEGEN, TQueryOptionLevel::REGULAR)             \
  QUERY_OPT_FN(explain_level, EXPLAIN_LEVEL, TQueryOptionLevel::REGULAR)                 \
  QUERY_OPT_FN(hbase_cache_blocks, HBASE_CACHE_BLOCKS, TQueryOptionLevel::ADVANCED)      \
  QUERY_OPT_FN(hbase_caching, HBASE_CACHING, TQueryOptionLevel::ADVANCED)                \
  QUERY_OPT_FN(max_errors, MAX_ERRORS, TQueryOptionLevel::ADVANCED)                      \
  REMOVED_QUERY_OPT_FN(max_io_buffers, MAX_IO_BUFFERS)                                   \
  QUERY_OPT_FN(                                                                          \
      max_scan_range_length, MAX_SCAN_RANGE_LENGTH, TQueryOptionLevel::DEVELOPMENT)      \
  QUERY_OPT_FN(mem_limit, MEM_LIMIT, TQueryOptionLevel::REGULAR)                         \
  QUERY_OPT_FN(num_nodes, NUM_NODES, TQueryOptionLevel::DEVELOPMENT)                     \
  QUERY_OPT_FN(num_scanner_threads, NUM_SCANNER_THREADS, TQueryOptionLevel::REGULAR)     \
  QUERY_OPT_FN(compression_codec, COMPRESSION_CODEC, TQueryOptionLevel::REGULAR)         \
  QUERY_OPT_FN(parquet_file_size, PARQUET_FILE_SIZE, TQueryOptionLevel::ADVANCED)        \
  QUERY_OPT_FN(request_pool, REQUEST_POOL, TQueryOptionLevel::REGULAR)                   \
  REMOVED_QUERY_OPT_FN(reservation_request_timeout, RESERVATION_REQUEST_TIMEOUT)         \
  QUERY_OPT_FN(sync_ddl, SYNC_DDL, TQueryOptionLevel::REGULAR)                           \
  REMOVED_QUERY_OPT_FN(v_cpu_cores, V_CPU_CORES)                                         \
  REMOVED_QUERY_OPT_FN(rm_initial_mem, RM_INITIAL_MEM)                                   \
  QUERY_OPT_FN(query_timeout_s, QUERY_TIMEOUT_S, TQueryOptionLevel::REGULAR)             \
  QUERY_OPT_FN(buffer_pool_limit, BUFFER_POOL_LIMIT, TQueryOptionLevel::ADVANCED)        \
  QUERY_OPT_FN(appx_count_distinct, APPX_COUNT_DISTINCT, TQueryOptionLevel::ADVANCED)    \
  QUERY_OPT_FN(disable_unsafe_spills, DISABLE_UNSAFE_SPILLS, TQueryOptionLevel::REGULAR) \
  REMOVED_QUERY_OPT_FN(seq_compression_mode, SEQ_COMPRESSION_MODE)                       \
  QUERY_OPT_FN(exec_single_node_rows_threshold, EXEC_SINGLE_NODE_ROWS_THRESHOLD,         \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(optimize_partition_key_scans, OPTIMIZE_PARTITION_KEY_SCANS,               \
      TQueryOptionLevel::REGULAR)                                                        \
  QUERY_OPT_FN(replica_preference, REPLICA_PREFERENCE, TQueryOptionLevel::ADVANCED)      \
  QUERY_OPT_FN(                                                                          \
      schedule_random_replica, SCHEDULE_RANDOM_REPLICA, TQueryOptionLevel::ADVANCED)     \
  REMOVED_QUERY_OPT_FN(scan_node_codegen_threshold, SCAN_NODE_CODEGEN_THRESHOLD)         \
  QUERY_OPT_FN(disable_streaming_preaggregations, DISABLE_STREAMING_PREAGGREGATIONS,     \
      TQueryOptionLevel::REGULAR)                                                        \
  QUERY_OPT_FN(runtime_filter_mode, RUNTIME_FILTER_MODE, TQueryOptionLevel::REGULAR)     \
  QUERY_OPT_FN(                                                                          \
      runtime_bloom_filter_size, RUNTIME_BLOOM_FILTER_SIZE, TQueryOptionLevel::ADVANCED) \
  QUERY_OPT_FN(runtime_filter_wait_time_ms, RUNTIME_FILTER_WAIT_TIME_MS,                 \
      TQueryOptionLevel::REGULAR)                                                        \
  QUERY_OPT_FN(disable_row_runtime_filtering, DISABLE_ROW_RUNTIME_FILTERING,             \
      TQueryOptionLevel::REGULAR)                                                        \
  QUERY_OPT_FN(                                                                          \
      max_num_runtime_filters, MAX_NUM_RUNTIME_FILTERS, TQueryOptionLevel::ADVANCED)     \
  QUERY_OPT_FN(parquet_annotate_strings_utf8, PARQUET_ANNOTATE_STRINGS_UTF8,             \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(parquet_fallback_schema_resolution, PARQUET_FALLBACK_SCHEMA_RESOLUTION,   \
      TQueryOptionLevel::REGULAR)                                                        \
  QUERY_OPT_FN(mt_dop, MT_DOP, TQueryOptionLevel::REGULAR)                               \
  QUERY_OPT_FN(                                                                          \
      s3_skip_insert_staging, S3_SKIP_INSERT_STAGING, TQueryOptionLevel::REGULAR)        \
  QUERY_OPT_FN(                                                                          \
      runtime_filter_min_size, RUNTIME_FILTER_MIN_SIZE, TQueryOptionLevel::ADVANCED)     \
  QUERY_OPT_FN(                                                                          \
      runtime_filter_max_size, RUNTIME_FILTER_MAX_SIZE, TQueryOptionLevel::ADVANCED)     \
  QUERY_OPT_FN(prefetch_mode, PREFETCH_MODE, TQueryOptionLevel::ADVANCED)                \
  QUERY_OPT_FN(strict_mode, STRICT_MODE, TQueryOptionLevel::DEVELOPMENT)                 \
  QUERY_OPT_FN(scratch_limit, SCRATCH_LIMIT, TQueryOptionLevel::REGULAR)                 \
  QUERY_OPT_FN(enable_expr_rewrites, ENABLE_EXPR_REWRITES, TQueryOptionLevel::ADVANCED)  \
  QUERY_OPT_FN(enable_cnf_rewrites, ENABLE_CNF_REWRITES, TQueryOptionLevel::ADVANCED)    \
  QUERY_OPT_FN(decimal_v2, DECIMAL_V2, TQueryOptionLevel::DEVELOPMENT)                   \
  QUERY_OPT_FN(parquet_dictionary_filtering, PARQUET_DICTIONARY_FILTERING,               \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(                                                                          \
      parquet_array_resolution, PARQUET_ARRAY_RESOLUTION, TQueryOptionLevel::REGULAR)    \
  QUERY_OPT_FN(                                                                          \
      parquet_read_statistics, PARQUET_READ_STATISTICS, TQueryOptionLevel::ADVANCED)     \
  QUERY_OPT_FN(default_join_distribution_mode, DEFAULT_JOIN_DISTRIBUTION_MODE,           \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(disable_codegen_rows_threshold, DISABLE_CODEGEN_ROWS_THRESHOLD,           \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(default_spillable_buffer_size, DEFAULT_SPILLABLE_BUFFER_SIZE,             \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(                                                                          \
      min_spillable_buffer_size, MIN_SPILLABLE_BUFFER_SIZE, TQueryOptionLevel::ADVANCED) \
  QUERY_OPT_FN(max_row_size, MAX_ROW_SIZE, TQueryOptionLevel::REGULAR)                   \
  QUERY_OPT_FN(idle_session_timeout, IDLE_SESSION_TIMEOUT, TQueryOptionLevel::REGULAR)   \
  QUERY_OPT_FN(compute_stats_min_sample_size, COMPUTE_STATS_MIN_SAMPLE_SIZE,             \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(exec_time_limit_s, EXEC_TIME_LIMIT_S, TQueryOptionLevel::REGULAR)         \
  QUERY_OPT_FN(                                                                          \
      shuffle_distinct_exprs, SHUFFLE_DISTINCT_EXPRS, TQueryOptionLevel::ADVANCED)       \
  QUERY_OPT_FN(max_mem_estimate_for_admission, MAX_MEM_ESTIMATE_FOR_ADMISSION,           \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(                                                                          \
      thread_reservation_limit, THREAD_RESERVATION_LIMIT, TQueryOptionLevel::REGULAR)    \
  QUERY_OPT_FN(thread_reservation_aggregate_limit, THREAD_RESERVATION_AGGREGATE_LIMIT,   \
      TQueryOptionLevel::REGULAR)                                                        \
  QUERY_OPT_FN(kudu_read_mode, KUDU_READ_MODE, TQueryOptionLevel::ADVANCED)              \
  QUERY_OPT_FN(allow_erasure_coded_files, ALLOW_ERASURE_CODED_FILES,                     \
      TQueryOptionLevel::DEVELOPMENT)                                                    \
  QUERY_OPT_FN(timezone, TIMEZONE, TQueryOptionLevel::REGULAR)                           \
  QUERY_OPT_FN(scan_bytes_limit, SCAN_BYTES_LIMIT, TQueryOptionLevel::ADVANCED)          \
  QUERY_OPT_FN(cpu_limit_s, CPU_LIMIT_S, TQueryOptionLevel::DEVELOPMENT)                 \
  QUERY_OPT_FN(topn_bytes_limit, TOPN_BYTES_LIMIT, TQueryOptionLevel::ADVANCED)          \
  QUERY_OPT_FN(client_identifier, CLIENT_IDENTIFIER, TQueryOptionLevel::ADVANCED)        \
  QUERY_OPT_FN(resource_trace_ratio, RESOURCE_TRACE_RATIO, TQueryOptionLevel::ADVANCED)  \
  QUERY_OPT_FN(num_remote_executor_candidates, NUM_REMOTE_EXECUTOR_CANDIDATES,           \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(                                                                          \
      num_rows_produced_limit, NUM_ROWS_PRODUCED_LIMIT, TQueryOptionLevel::ADVANCED)     \
  QUERY_OPT_FN(                                                                          \
      planner_testcase_mode, PLANNER_TESTCASE_MODE, TQueryOptionLevel::DEVELOPMENT)      \
  QUERY_OPT_FN(default_file_format, DEFAULT_FILE_FORMAT, TQueryOptionLevel::REGULAR)     \
  QUERY_OPT_FN(                                                                          \
      parquet_timestamp_type, PARQUET_TIMESTAMP_TYPE, TQueryOptionLevel::DEVELOPMENT)    \
  QUERY_OPT_FN(                                                                          \
      parquet_read_page_index, PARQUET_READ_PAGE_INDEX, TQueryOptionLevel::ADVANCED)     \
  QUERY_OPT_FN(                                                                          \
      parquet_write_page_index, PARQUET_WRITE_PAGE_INDEX, TQueryOptionLevel::ADVANCED)   \
  QUERY_OPT_FN(parquet_page_row_count_limit, PARQUET_PAGE_ROW_COUNT_LIMIT,               \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(disable_hdfs_num_rows_estimate, DISABLE_HDFS_NUM_ROWS_ESTIMATE,           \
      TQueryOptionLevel::REGULAR)                                                        \
  QUERY_OPT_FN(default_hints_insert_statement, DEFAULT_HINTS_INSERT_STATEMENT,           \
      TQueryOptionLevel::REGULAR)                                                        \
  QUERY_OPT_FN(spool_query_results, SPOOL_QUERY_RESULTS, TQueryOptionLevel::DEVELOPMENT) \
  QUERY_OPT_FN(default_transactional_type, DEFAULT_TRANSACTIONAL_TYPE,                   \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(statement_expression_limit, STATEMENT_EXPRESSION_LIMIT,                   \
      TQueryOptionLevel::REGULAR)                                                        \
  QUERY_OPT_FN(max_statement_length_bytes, MAX_STATEMENT_LENGTH_BYTES,                   \
      TQueryOptionLevel::REGULAR)                                                        \
  QUERY_OPT_FN(disable_data_cache, DISABLE_DATA_CACHE, TQueryOptionLevel::ADVANCED)      \
  QUERY_OPT_FN(                                                                          \
      max_result_spooling_mem, MAX_RESULT_SPOOLING_MEM, TQueryOptionLevel::DEVELOPMENT)  \
  QUERY_OPT_FN(max_spilled_result_spooling_mem, MAX_SPILLED_RESULT_SPOOLING_MEM,         \
      TQueryOptionLevel::DEVELOPMENT)                                                    \
  QUERY_OPT_FN(disable_hbase_num_rows_estimate, DISABLE_HBASE_NUM_ROWS_ESTIMATE,         \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(                                                                          \
      fetch_rows_timeout_ms, FETCH_ROWS_TIMEOUT_MS, TQueryOptionLevel::ADVANCED)         \
  QUERY_OPT_FN(now_string, NOW_STRING, TQueryOptionLevel::DEVELOPMENT)                   \
  QUERY_OPT_FN(parquet_object_store_split_size, PARQUET_OBJECT_STORE_SPLIT_SIZE,         \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(mem_limit_executors, MEM_LIMIT_EXECUTORS, TQueryOptionLevel::ADVANCED)    \
  QUERY_OPT_FN(                                                                          \
      broadcast_bytes_limit, BROADCAST_BYTES_LIMIT, TQueryOptionLevel::ADVANCED)         \
  QUERY_OPT_FN(preagg_bytes_limit, PREAGG_BYTES_LIMIT, TQueryOptionLevel::ADVANCED)      \
  QUERY_OPT_FN(max_cnf_exprs, MAX_CNF_EXPRS, TQueryOptionLevel::ADVANCED)                \
  QUERY_OPT_FN(kudu_snapshot_read_timestamp_micros, KUDU_SNAPSHOT_READ_TIMESTAMP_MICROS, \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(retry_failed_queries, RETRY_FAILED_QUERIES, TQueryOptionLevel::REGULAR)   \
  QUERY_OPT_FN(enabled_runtime_filter_types, ENABLED_RUNTIME_FILTER_TYPES,               \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(async_codegen, ASYNC_CODEGEN, TQueryOptionLevel::DEVELOPMENT)             \
  QUERY_OPT_FN(enable_distinct_semi_join_optimization,                                   \
      ENABLE_DISTINCT_SEMI_JOIN_OPTIMIZATION, TQueryOptionLevel::ADVANCED)               \
  QUERY_OPT_FN(sort_run_bytes_limit, SORT_RUN_BYTES_LIMIT, TQueryOptionLevel::ADVANCED)  \
  QUERY_OPT_FN(max_fs_writers, MAX_FS_WRITERS, TQueryOptionLevel::ADVANCED)              \
  QUERY_OPT_FN(refresh_updated_hms_partitions, REFRESH_UPDATED_HMS_PARTITIONS,           \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(spool_all_results_for_retries, SPOOL_ALL_RESULTS_FOR_RETRIES,             \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(                                                                          \
      runtime_filter_error_rate, RUNTIME_FILTER_ERROR_RATE, TQueryOptionLevel::ADVANCED) \
  QUERY_OPT_FN(use_local_tz_for_unix_timestamp_conversions,                              \
      USE_LOCAL_TZ_FOR_UNIX_TIMESTAMP_CONVERSIONS, TQueryOptionLevel::ADVANCED)          \
  QUERY_OPT_FN(convert_legacy_hive_parquet_utc_timestamps,                               \
      CONVERT_LEGACY_HIVE_PARQUET_UTC_TIMESTAMPS, TQueryOptionLevel::ADVANCED)           \
  QUERY_OPT_FN(enable_outer_join_to_inner_transformation,                                \
      ENABLE_OUTER_JOIN_TO_INNER_TRANSFORMATION, TQueryOptionLevel::ADVANCED)            \
  QUERY_OPT_FN(targeted_kudu_scan_range_length, TARGETED_KUDU_SCAN_RANGE_LENGTH,         \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(report_skew_limit, REPORT_SKEW_LIMIT, TQueryOptionLevel::ADVANCED)        \
  QUERY_OPT_FN(optimize_simple_limit, OPTIMIZE_SIMPLE_LIMIT, TQueryOptionLevel::REGULAR) \
  QUERY_OPT_FN(use_dop_for_costing, USE_DOP_FOR_COSTING, TQueryOptionLevel::ADVANCED)    \
  QUERY_OPT_FN(broadcast_to_partition_factor, BROADCAST_TO_PARTITION_FACTOR,             \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(                                                                          \
      join_rows_produced_limit, JOIN_ROWS_PRODUCED_LIMIT, TQueryOptionLevel::ADVANCED)   \
  QUERY_OPT_FN(utf8_mode, UTF8_MODE, TQueryOptionLevel::DEVELOPMENT)                     \
  QUERY_OPT_FN(analytic_rank_pushdown_threshold, ANALYTIC_RANK_PUSHDOWN_THRESHOLD,       \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(                                                                          \
      minmax_filter_threshold, MINMAX_FILTER_THRESHOLD, TQueryOptionLevel::ADVANCED)     \
  QUERY_OPT_FN(                                                                          \
      minmax_filtering_level, MINMAX_FILTERING_LEVEL, TQueryOptionLevel::ADVANCED)       \
  QUERY_OPT_FN(compute_column_minmax_stats, COMPUTE_COLUMN_MINMAX_STATS,                 \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(                                                                          \
      show_column_minmax_stats, SHOW_COLUMN_MINMAX_STATS, TQueryOptionLevel::ADVANCED)   \
  QUERY_OPT_FN(default_ndv_scale, DEFAULT_NDV_SCALE, TQueryOptionLevel::ADVANCED)        \
  QUERY_OPT_FN(                                                                          \
      kudu_replica_selection, KUDU_REPLICA_SELECTION, TQueryOptionLevel::ADVANCED)       \
  QUERY_OPT_FN(                                                                          \
      delete_stats_in_truncate, DELETE_STATS_IN_TRUNCATE, TQueryOptionLevel::ADVANCED)   \
  QUERY_OPT_FN(                                                                          \
      parquet_bloom_filtering, PARQUET_BLOOM_FILTERING, TQueryOptionLevel::ADVANCED)     \
  QUERY_OPT_FN(minmax_filter_sorted_columns, MINMAX_FILTER_SORTED_COLUMNS,               \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(minmax_filter_fast_code_path, MINMAX_FILTER_FAST_CODE_PATH,               \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(                                                                          \
      enable_kudu_transaction, ENABLE_KUDU_TRANSACTION, TQueryOptionLevel::DEVELOPMENT)  \
  QUERY_OPT_FN(minmax_filter_partition_columns, MINMAX_FILTER_PARTITION_COLUMNS,         \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(parquet_bloom_filter_write, PARQUET_BLOOM_FILTER_WRITE,                   \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(orc_read_statistics, ORC_READ_STATISTICS, TQueryOptionLevel::ADVANCED)    \
  QUERY_OPT_FN(enable_async_ddl_execution, ENABLE_ASYNC_DDL_EXECUTION,                   \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(enable_async_load_data_execution, ENABLE_ASYNC_LOAD_DATA_EXECUTION,       \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(parquet_late_materialization_threshold,                                   \
      PARQUET_LATE_MATERIALIZATION_THRESHOLD, TQueryOptionLevel::ADVANCED)               \
  QUERY_OPT_FN(parquet_dictionary_runtime_filter_entry_limit,                            \
      PARQUET_DICTIONARY_RUNTIME_FILTER_ENTRY_LIMIT, TQueryOptionLevel::ADVANCED)        \
  QUERY_OPT_FN(abort_java_udf_on_exception, ABORT_JAVA_UDF_ON_EXCEPTION,                 \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(orc_async_read, ORC_ASYNC_READ, TQueryOptionLevel::ADVANCED)              \
  QUERY_OPT_FN(runtime_in_list_filter_entry_limit, RUNTIME_IN_LIST_FILTER_ENTRY_LIMIT,   \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(enable_replan, ENABLE_REPLAN, TQueryOptionLevel::ADVANCED)                \
  QUERY_OPT_FN(test_replan, TEST_REPLAN, TQueryOptionLevel::ADVANCED)                    \
  QUERY_OPT_FN(lock_max_wait_time_s, LOCK_MAX_WAIT_TIME_S, TQueryOptionLevel::REGULAR)   \
  QUERY_OPT_FN(orc_schema_resolution, ORC_SCHEMA_RESOLUTION, TQueryOptionLevel::REGULAR) \
  QUERY_OPT_FN(expand_complex_types, EXPAND_COMPLEX_TYPES, TQueryOptionLevel::REGULAR)   \
  QUERY_OPT_FN(                                                                          \
      fallback_db_for_functions, FALLBACK_DB_FOR_FUNCTIONS, TQueryOptionLevel::ADVANCED) \
  QUERY_OPT_FN(                                                                          \
      disable_codegen_cache, DISABLE_CODEGEN_CACHE, TQueryOptionLevel::ADVANCED)         \
  QUERY_OPT_FN(codegen_cache_mode, CODEGEN_CACHE_MODE, TQueryOptionLevel::DEVELOPMENT)   \
  QUERY_OPT_FN(stringify_map_keys, STRINGIFY_MAP_KEYS, TQueryOptionLevel::ADVANCED)      \
  QUERY_OPT_FN(enable_trivial_query_for_admission, ENABLE_TRIVIAL_QUERY_FOR_ADMISSION,   \
      TQueryOptionLevel::REGULAR)                                                        \
  QUERY_OPT_FN(                                                                          \
      compute_processing_cost, COMPUTE_PROCESSING_COST, TQueryOptionLevel::ADVANCED)     \
  QUERY_OPT_FN(processing_cost_min_threads, PROCESSING_COST_MIN_THREADS,                 \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(join_selectivity_correlation_factor, JOIN_SELECTIVITY_CORRELATION_FACTOR, \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(max_fragment_instances_per_node, MAX_FRAGMENT_INSTANCES_PER_NODE,         \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(max_sort_run_size, MAX_SORT_RUN_SIZE, TQueryOptionLevel::DEVELOPMENT)     \
  QUERY_OPT_FN(allow_unsafe_casts, ALLOW_UNSAFE_CASTS, TQueryOptionLevel::DEVELOPMENT)   \
  QUERY_OPT_FN(num_threads_for_table_migration, NUM_THREADS_FOR_TABLE_MIGRATION,         \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(disable_optimized_iceberg_v2_read, DISABLE_OPTIMIZED_ICEBERG_V2_READ,     \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(values_stmt_avoid_lossy_char_padding,                                     \
      VALUES_STMT_AVOID_LOSSY_CHAR_PADDING, TQueryOptionLevel::REGULAR)                  \
  QUERY_OPT_FN(                                                                          \
      large_agg_mem_threshold, LARGE_AGG_MEM_THRESHOLD, TQueryOptionLevel::ADVANCED)     \
  QUERY_OPT_FN(agg_mem_correlation_factor, AGG_MEM_CORRELATION_FACTOR,                   \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(mem_limit_coordinators, MEM_LIMIT_COORDINATORS,                           \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(iceberg_predicate_pushdown_subsetting,                                    \
      ICEBERG_PREDICATE_PUSHDOWN_SUBSETTING, TQueryOptionLevel::DEVELOPMENT)             \
  QUERY_OPT_FN(hdfs_scanner_non_reserved_bytes, HDFS_SCANNER_NON_RESERVED_BYTES,         \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(codegen_opt_level, CODEGEN_OPT_LEVEL, TQueryOptionLevel::ADVANCED)        \
  QUERY_OPT_FN(kudu_table_reserve_seconds, KUDU_TABLE_RESERVE_SECONDS,                   \
      TQueryOptionLevel::ADVANCED)                                                       \
  QUERY_OPT_FN(convert_kudu_utc_timestamps,                                              \
      CONVERT_KUDU_UTC_TIMESTAMPS, TQueryOptionLevel::ADVANCED)                          \
  QUERY_OPT_FN(disable_kudu_local_timestamp_bloom_filter,                                \
      DISABLE_KUDU_LOCAL_TIMESTAMP_BLOOM_FILTER, TQueryOptionLevel::ADVANCED)            \
  QUERY_OPT_FN(runtime_filter_cardinality_reduction_scale,                               \
      RUNTIME_FILTER_CARDINALITY_REDUCTION_SCALE, TQueryOptionLevel::DEVELOPMENT)        \
  QUERY_OPT_FN(max_num_filters_aggregated_per_host, MAX_NUM_FILTERS_AGGREGATED_PER_HOST, \
      TQueryOptionLevel::DEVELOPMENT)                                                    \
  QUERY_OPT_FN(query_cpu_count_divisor,                                                  \
      QUERY_CPU_COUNT_DIVISOR, TQueryOptionLevel::ADVANCED)                              \
  QUERY_OPT_FN(enable_tuple_cache, ENABLE_TUPLE_CACHE, TQueryOptionLevel::ADVANCED)      \
  QUERY_OPT_FN(iceberg_disable_count_star_optimization,                                  \
      ICEBERG_DISABLE_COUNT_STAR_OPTIMIZATION, TQueryOptionLevel::ADVANCED)              \
  QUERY_OPT_FN(runtime_filter_ids_to_skip,                                               \
      RUNTIME_FILTER_IDS_TO_SKIP, TQueryOptionLevel::DEVELOPMENT)                        \
  QUERY_OPT_FN(slot_count_strategy, SLOT_COUNT_STRATEGY, TQueryOptionLevel::ADVANCED)    \
  QUERY_OPT_FN(clean_dbcp_ds_cache, CLEAN_DBCP_DS_CACHE, TQueryOptionLevel::ADVANCED)    \
  QUERY_OPT_FN(use_null_slots_cache, USE_NULL_SLOTS_CACHE, TQueryOptionLevel::ADVANCED)  \
  QUERY_OPT_FN(write_kudu_utc_timestamps,                                                \
      WRITE_KUDU_UTC_TIMESTAMPS, TQueryOptionLevel::ADVANCED)                            \
  ;

/// Enforce practical limits on some query options to avoid undesired query state.
static const int64_t SPILLABLE_BUFFER_LIMIT = 1LL << 40; // 1 TB
static const int64_t ROW_SIZE_LIMIT = 1LL << 40; // 1 TB

/// Limits on the query size are intended to be large. Prevent them from being set
/// to small values (which can prevent clients from executing anything).
static const int32_t MIN_STATEMENT_EXPRESSION_LIMIT = 1 << 10; // 1024
static const int32_t MIN_MAX_STATEMENT_LENGTH_BYTES = 1 << 10; // 1 KB

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
typedef std::bitset<192> QueryOptionsMask;

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

/// Set the key/value pair in TQueryOptions. It will override existing setting in
/// query_options. The bit corresponding to query option 'key' in set_query_options_mask
/// is set. An empty string value will reset the key to its default value.
Status SetQueryOption(TImpalaQueryOptions::type option, const std::string& value,
    TQueryOptions* query_options, QueryOptionsMask* set_query_options_mask);

/// Validates the query options after they have all been set. Returns a Status indicating
/// the results of running the validation rules. The majority of the query options
/// validation is done in SetQueryOption. However, more complex validations rules (e.g.
/// validating that one config is greater than another config) are run here.
Status ValidateQueryOptions(TQueryOptions* query_options);

/// Parse a "," separated key=value pair of query options and set it in 'query_options'.
/// If the same query option is specified more than once, the last one wins. The
/// set_query_options_mask bitmask is updated to reflect the query options which were
/// set. Double quote can be used to wrap a query option value that has "," char in it.
/// Returns an error status containing an error detail for any invalid options (e.g.
/// bad format or invalid query option), but all valid query options are still handled.
Status ParseQueryOptions(const std::string& options, TQueryOptions* query_options,
    QueryOptionsMask* set_query_options_mask);

/// Based on the query option levels provided to QUERY_OPT_FN macro this function
/// populates the received QueryOptionLevels map with (option name -> option level)
/// entries.
void PopulateQueryOptionLevels(QueryOptionLevels* query_option_levels);

/// Reset all query options to its default value if they are not equal to default value.
/// The bit corresponding to query option 'key' in set_query_options_mask is unset.
Status ResetAllQueryOptions(
    TQueryOptions* query_options, QueryOptionsMask* set_query_options_mask);
}

#endif
