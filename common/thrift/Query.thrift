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

namespace cpp impala
namespace java org.apache.impala.thrift

include "Types.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "Descriptors.thrift"
include "Results.thrift"
include "CatalogObjects.thrift"
include "LineageGraph.thrift"

// Enum for schema resolution strategies. A schema resolution strategy
// determines how columns/fields are looked up in the data files.
enum TSchemaResolutionStrategy {
  // Resolve columns based on position. This assumes that the HMS
  // table schema and the file schema are in sync.
  POSITION = 0
  // Resolve columns by names.
  NAME = 1
  // Valid for Iceberg tables. This resolves columns by using the
  // Iceberg field ids.
  FIELD_ID = 2
}

// A table's Hive ACID type.
enum TTransactionalType {
  NONE,
  INSERT_ONLY
}

// Consistency level options for Kudu scans.
enum TKuduReadMode {
  DEFAULT = 0
  READ_LATEST = 1
  READ_AT_SNAPSHOT = 2
}

enum TKuduReplicaSelection {
  LEADER_ONLY = 0
  CLOSEST_REPLICA = 1
}

enum TJoinDistributionMode {
  BROADCAST = 0
  SHUFFLE = 1
  DIRECTED = 2
}

// The order of the enum values needs to be kept in sync with
// ParquetMetadataUtils::ORDERED_ARRAY_ENCODINGS in parquet-metadata-utils.cc.
enum TParquetArrayResolution {
  THREE_LEVEL = 0
  TWO_LEVEL = 1
  TWO_LEVEL_THEN_THREE_LEVEL = 2
}

// Physical type and unit used when writing timestamps in Parquet.
enum TParquetTimestampType {
  INT96_NANOS,
  INT64_MILLIS,
  INT64_MICROS,
  INT64_NANOS
}

// The options for a minmax filter to take fast code path.
enum TMinmaxFilterFastCodePathMode {
  OFF=0,
  ON=1,
  VERIFICATION=2
}

// The options for CodeGen Cache.
// The debug options allow more logs, the value equal to the mode plus 256.
enum TCodeGenCacheMode {
  NORMAL = 0
  OPTIMAL = 1
  NORMAL_DEBUG = 256
  OPTIMAL_DEBUG = 257
}

// Options for when to write Parquet Bloom filters for supported types.
enum TParquetBloomFilterWrite {
  // Never write Parquet Bloom filters.
  NEVER,

  // Write Parquet Bloom filters if specified in the table properties AND the row group
  // is not fully dictionary encoded.
  IF_NO_DICT,

  // Always write Parquet Bloom filters if specified in the table properties,
  // even if the row group is fully dictionary encoded.
  ALWAYS
}

enum TCodeGenOptLevel {
  O0,
  O1,
  Os,
  O2,
  O3
}

// Option to decide how to compute slots_to_use for a query.
// See Scheduler::ComputeBackendExecParams.
enum TSlotCountStrategy {
  // Compute slots to use for each backend based on the max number of instances of any
  // fragment on that backend. This is the default and only strategy available if
  // COMPUTE_PROCESSING_COST option is disabled. See IMPALA-8998.
  LARGEST_FRAGMENT = 0,

  // Compute slots to use for each backend based on CpuAsk counter from Planner.
  // The CpuAsk is the largest sum of fragments instances subset that can run in-parallel
  // without waiting for each other. This strategy relies on blocking operator analysis
  // that is only available if COMPUTE_PROCESSING_COST option is enabled, and will
  // schedule more or equal admission control slots than the LARGEST_FRAGMENT strategy.
  // The scheduler will silently ignore this choice and fallback to LARGEST_FRAGMENT if
  // COMPUTE_PROCESSING_COST is disabled.
  PLANNER_CPU_ASK = 1
}

// constants for TQueryOptions.num_nodes
const i32 NUM_NODES_ALL = 0
const i32 NUM_NODES_ALL_RACKS = -1
// constant used as upperbound for TQueryOptions.processing_cost_min_threads and
// TQueryOptions.max_fragment_instances_per_node
const i32 MAX_FRAGMENT_INSTANCES_PER_NODE = 128
// Conservative minimum size of hash table for low-cardinality aggregations.
const i64 MIN_HASH_TBL_MEM = 10485760  // 10MB

// Query options that correspond to ImpalaService.ImpalaQueryOptions, with their
// respective defaults. Query options can be set in the following ways:
//
// 1) Process-wide defaults (via the impalad arg --default_query_options)
// 2) Resource pool defaults (via resource pool configuration)
// 3) Session settings (via the SET command or the HS2 OpenSession RPC)
// 4) HS2/Beeswax configuration 'overlay' in the request metadata
//
// (1) and (2) are set by administrators and provide the default query options for a
// session, in that order, so options set in (2) override those in (1). The user
// can specify query options with (3) to override the preceding layers; these
// overrides are stored in SessionState. Finally, the client can pass a config
// 'overlay' (4) in the request metadata which overrides everything else.
//
// Session options (level 3, above) can be set by the user with SET <key>=<value>
// or in the OpenSession RPC. They can be unset with SET <key>="". When unset,
// it's unset in that level, and the values as specified by the defaults,
// and levels 1 and 2 above take hold.
//
// Because of the ambiguity between null and the empty string here, string-typed
// options where the empty string is a valid value can cause problems as follows:
// * If their default is not the empty string, a user can't set it to the
//   empty string with SET.
// * Even if their default is the empty string, they may be set to something
//   else via process defaults or resource pool defaults, and the user
//   may not be able to override them back to the empty string.
struct TQueryOptions {
  1: optional bool abort_on_error = 0
  2: optional i32 max_errors = 100
  3: optional bool disable_codegen = 0
  4: optional i32 batch_size = 0
  5: optional i32 num_nodes = NUM_NODES_ALL
  6: optional i64 max_scan_range_length = 0
  7: optional i32 num_scanner_threads = 0
  11: optional string debug_action = ""
  12: optional i64 mem_limit = 0
  14: optional CatalogObjects.TCompressionCodec compression_codec
  15: optional i32 hbase_caching = 0
  16: optional bool hbase_cache_blocks = 0
  17: optional i64 parquet_file_size = 0
  18: optional Types.TExplainLevel explain_level = 1
  19: optional bool sync_ddl = 0

  // Request pool this request should be submitted to. If not set
  // the pool is determined based on the user.
  20: optional string request_pool

  // test hook to disable topn on the outermost select block.
  24: optional bool disable_outermost_topn = 0

  // Time, in s, before a query will be timed out if it is inactive. May not exceed
  // --idle_query_timeout if that flag > 0. If 0, falls back to --idle_query_timeout.
  26: optional i32 query_timeout_s = 0

  // test hook to cap max memory for spilling operators (to force them to spill).
  27: optional i64 buffer_pool_limit

  // If true, transforms all count(distinct) aggregations into NDV()
  28: optional bool appx_count_distinct = 0

  // If true, allows Impala to internally disable spilling for potentially
  // disastrous query plans. Impala will excercise this option if a query
  // has no plan hints, and at least one table is missing relevant stats.
  29: optional bool disable_unsafe_spills = 0

  // If the number of rows that are processed for a single query is below the
  // threshold, it will be executed on the coordinator only with codegen disabled
  31: optional i32 exec_single_node_rows_threshold = 100

  // If true, use the table's metadata to produce the partition columns instead of table
  // scans whenever possible. This option is opt-in by default as this optimization may
  // produce different results than the scan based approach in some edge cases.
  32: optional bool optimize_partition_key_scans = 0

  // Specify the prefered locality level of replicas during scan scheduling.
  // Replicas with an equal or better locality will be preferred.
  33: optional PlanNodes.TReplicaPreference replica_preference =
      PlanNodes.TReplicaPreference.CACHE_LOCAL

  // Configure whether scan ranges with local replicas will be assigned by starting from
  // the same replica for every query or by starting with a new, pseudo-random replica for
  // subsequent queries. The default is to start with the same replica for every query.
  34: optional bool schedule_random_replica = 0

  // If true, the planner will not generate plans with streaming preaggregations.
  36: optional bool disable_streaming_preaggregations = 0

  // If true, runtime filter propagation is enabled
  37: optional Types.TRuntimeFilterMode runtime_filter_mode = 2

  // Size in bytes of Bloom Filters used for runtime filters. Actual size of filter will
  // be rounded up to the nearest power of two.
  38: optional i32 runtime_bloom_filter_size = 1048576

  // Time in ms to wait until runtime filters are delivered. Note that the wait time for
  // a runtime filter is with respect to the start of processing the query in the given
  // executor instead of the beginning of the Open phase of a scan node. If 0, the
  // default defined by the startup flag of the same name is used.
  39: optional i32 runtime_filter_wait_time_ms = 0

  // If true, per-row runtime filtering is disabled
  40: optional bool disable_row_runtime_filtering = false

  // Maximum number of bloom runtime filters allowed per query
  41: optional i32 max_num_runtime_filters = 10

  // If true, use UTF-8 annotation for string columns. Note that char and varchar columns
  // always use the annotation.
  //
  // This is disabled by default in order to preserve the existing behavior of legacy
  // workloads. In addition, Impala strings are not necessarily UTF8-encoded.
  42: optional bool parquet_annotate_strings_utf8 = false

  // Determines how to resolve Parquet files' schemas in the absence of field IDs (which
  // is always, since fields IDs are NYI). Valid values are "position" (default) and
  // "name".
  43: optional TSchemaResolutionStrategy parquet_fallback_schema_resolution = 0

  // Multi-threaded execution: degree of parallelism (= number of active threads) per
  // query per backend.
  // > 0: multi-threaded execution mode, with given dop
  // 0: single-threaded execution mode
  // unset: may be set automatically to > 0 in createExecRequest(), otherwise same as 0
  44: optional i32 mt_dop

  // If true, INSERT writes to S3 go directly to their final location rather than being
  // copied there by the coordinator. We cannot do this for INSERT OVERWRITES because for
  // those queries, the coordinator deletes all files in the final location before copying
  // the files there.
  45: optional bool s3_skip_insert_staging = true

  // Minimum runtime bloom filter size, in bytes
  46: optional i32 runtime_filter_min_size = 1048576

  // Maximum runtime bloom filter size, in bytes
  47: optional i32 runtime_filter_max_size = 16777216

  // Prefetching behavior during hash tables' building and probing.
  48: optional Types.TPrefetchMode prefetch_mode = Types.TPrefetchMode.HT_BUCKET

  // Additional strict handling of invalid data parsing and type conversions.
  49: optional bool strict_mode = false

  // A limit on the amount of scratch directory space that can be used;
  50: optional i64 scratch_limit = -1

  // Indicates whether the FE should rewrite Exprs for optimization purposes.
  // It's sometimes useful to disable rewrites for testing, e.g., expr-test.cc.
  51: optional bool enable_expr_rewrites = true

  // Indicates whether to use the new decimal semantics.
  52: optional bool decimal_v2 = true

  // Indicates whether to use dictionary filtering for Parquet files
  53: optional bool parquet_dictionary_filtering = true

  // Policy for resolving nested array fields in Parquet files.
  54: optional TParquetArrayResolution parquet_array_resolution =
    TParquetArrayResolution.THREE_LEVEL

  // Indicates whether to read statistics from Parquet files and use them during query
  // processing. This includes skipping data based on the statistics and computing query
  // results like "select min()".
  55: optional bool parquet_read_statistics = true

  // Join distribution mode that is used when the join inputs have an unknown
  // cardinality, e.g., because of missing table statistics.
  56: optional TJoinDistributionMode default_join_distribution_mode =
    TJoinDistributionMode.BROADCAST

  // If the number of rows processed per node is below the threshold codegen will be
  // automatically disabled by the planner.
  57: optional i32 disable_codegen_rows_threshold = 50000

  // The default spillable buffer size in bytes, which may be overridden by the planner.
  // Defaults to 2MB.
  58: optional i64 default_spillable_buffer_size = 2097152;

  // The minimum spillable buffer to use. The planner will not choose a size smaller than
  // this. Defaults to 64KB.
  59: optional i64 min_spillable_buffer_size = 65536;

  // The maximum size of row that the query will reserve memory to process. Processing
  // rows larger than this may result in a query failure. Defaults to 512KB, e.g.
  // enough for a row with 15 32KB strings or many smaller columns.
  //
  // Different operators handle this option in different ways. E.g. some simply increase
  // the size of all their buffers to fit this row size, whereas others may use more
  // sophisticated strategies - e.g. reserving a small number of buffers large enough to
  // fit maximum-sized rows.
  60: optional i64 max_row_size = 524288;

  // The time, in seconds, that a session may be idle for before it is closed (and all
  // running queries cancelled) by Impala. If 0, idle sessions never expire.
  // The default session timeout is set by the command line flag of the same name.
  61: optional i32 idle_session_timeout;

  // Minimum number of bytes that will be scanned in COMPUTE STATS TABLESAMPLE,
  // regardless of the user-supplied sampling percent. Default value: 1GB
  62: optional i64 compute_stats_min_sample_size = 1073741824;

  // Time limit, in s, before a query will be timed out after it starts executing. Does
  // not include time spent in planning, scheduling or admission control. A value of 0
  // means no time limit.
  63: optional i32 exec_time_limit_s = 0;

  // When a query has both grouping and distinct exprs, impala can optionally include the
  // distinct exprs in the hash exchange of the first aggregation phase to spread the data
  // among more nodes. However, this plan requires another hash exchange on the grouping
  // exprs in the second phase which is not required when omitting the distinct exprs in
  // the first phase. Shuffling by both is better if the grouping exprs have low NDVs.
  64: optional bool shuffle_distinct_exprs = true;

  // See comment in ImpalaService.thrift.
  65: optional i64 max_mem_estimate_for_admission = 0;

  // See comment in ImpalaService.thrift.
  // The default values is set fairly high based on empirical data - queries with up to
  // this number of reserved threads have run successfully as part of production
  // workloads but with very degraded performance.
  66: optional i32 thread_reservation_limit = 3000;

  // See comment in ImpalaService.thrift.
  67: optional i32 thread_reservation_aggregate_limit = 0;

  // See comment in ImpalaService.thrift.
  68: optional TKuduReadMode kudu_read_mode = TKuduReadMode.DEFAULT;

  // Allow reading of erasure coded files in HDFS.
  69: optional bool allow_erasure_coded_files = true;

  // See comment in ImpalaService.thrift.
  70: optional string timezone = ""

  // See comment in ImpalaService.thrift.
  71: optional i64 scan_bytes_limit = 0;

  // See comment in ImpalaService.thrift.
  72: optional i64 cpu_limit_s = 0;

  // See comment in ImpalaService.thrift
  // The default value is set to 512MB based on empirical data
  73: optional i64 topn_bytes_limit = 536870912;

  // See comment in ImpalaService.thrift
  74: optional string client_identifier;

  75: optional double resource_trace_ratio = 1;

  // See comment in ImpalaService.thrift.
  // The default value is set to 3 as this is the default value of HDFS replicas.
  76: optional i32 num_remote_executor_candidates = 3;

  // See comment in ImpalaService.thrift.
  77: optional i64 num_rows_produced_limit = 0;

  // See comment in ImpalaService.thrift
  78: optional bool planner_testcase_mode = false;

  // See comment in ImpalaService.thrift.
  79: optional CatalogObjects.THdfsFileFormat default_file_format =
      CatalogObjects.THdfsFileFormat.TEXT;

  // See comment in ImpalaService.thrift.
  80: optional TParquetTimestampType parquet_timestamp_type =
      TParquetTimestampType.INT96_NANOS;

  // See comment in ImpalaService.thrift.
  81: optional bool parquet_read_page_index = true;

  // See comment in ImpalaService.thrift.
  82: optional bool parquet_write_page_index = true;

  // See comment in ImpalaService.thrift.
  83: optional i32 parquet_page_row_count_limit;

  // Disable the attempt to compute an estimated number of rows in an
  // hdfs table.
  84: optional bool disable_hdfs_num_rows_estimate = false;

  // See comment in ImpalaService.thrift.
  85: optional string default_hints_insert_statement;

  // See comment in ImpalaService.thrift
  86: optional bool spool_query_results = true;

  // See comment in ImpalaService.thrift
  87: optional TTransactionalType default_transactional_type = TTransactionalType.NONE;

  // See comment in ImpalaService.thrift.
  // The default of 250,000 is set to a high value to avoid impacting existing users, but
  // testing indicates a statement with this number of expressions can run.
  88: optional i32 statement_expression_limit = 250000

  // See comment in ImpalaService.thrift
  // The default is set to 16MB. It is likely that a statement of this size would exceed
  // the statement expression limit. Setting a limit on the total statement size avoids
  // the cost of parsing and analyzing the statement, which is required to enforce the
  // statement expression limit.
  89: optional i32 max_statement_length_bytes = 16777216

  // If true, skip using the data cache for this query session.
  90: optional bool disable_data_cache = false;

  // See comment in ImpalaService.thrift
  91: optional i64 max_result_spooling_mem = 104857600;

  // See comment in ImpalaService.thrift
  92: optional i64 max_spilled_result_spooling_mem = 1073741824;

  // See comment in ImpalaService.thrift
  93: optional bool disable_hbase_num_rows_estimate = false;

  // See comment in ImpalaService.thrift
  94: optional i64 fetch_rows_timeout_ms = 10000;

  // For testing purposes
  95: optional string now_string = "";

  // See comment in ImpalaService.thrift
  96: optional i64 parquet_object_store_split_size = 268435456;

  // See comment in ImpalaService.thrift
  97: optional i64 mem_limit_executors = 0;

  // See comment in ImpalaService.thrift
  // The default value is set to 32 GB
  98: optional i64 broadcast_bytes_limit = 34359738368;

  // See comment in ImpalaService.thrift
  99: optional i64 preagg_bytes_limit = -1;

  // See comment in ImpalaService.thrift
  100: optional bool enable_cnf_rewrites = true;

  // See comment in ImpalaService.thrift
  101: optional i32 max_cnf_exprs = 200;

  // See comment in ImpalaService.thrift
  102: optional i64 kudu_snapshot_read_timestamp_micros = 0;

  // See comment in ImpalaService.thrift
  103: optional bool retry_failed_queries = false;

  // See comment in ImpalaService.thrift
  104: optional set<PlanNodes.TRuntimeFilterType> enabled_runtime_filter_types =
      [PlanNodes.TRuntimeFilterType.BLOOM, PlanNodes.TRuntimeFilterType.MIN_MAX];

  // See comment in ImpalaService.thrift
  105: optional bool async_codegen = false;

  // See comment in ImpalaService.thrift
  106: optional bool enable_distinct_semi_join_optimization = true;

  // See comment in ImpalaService.thrift
  107: optional i64 sort_run_bytes_limit = -1;

  // See comment in ImpalaService.thrift
  108: optional i32 max_fs_writers = 0;

  // See comment in ImpalaService.thrift
  109: optional bool refresh_updated_hms_partitions = false;

  // See comment in ImpalaService.thrift
  110: optional bool spool_all_results_for_retries = true;

  // See comment in ImpalaService.thrift
  111: optional double runtime_filter_error_rate;

  // See comment in ImpalaService.thrift
  112: optional bool use_local_tz_for_unix_timestamp_conversions = false;

  // See comment in ImpalaService.thrift
  113: optional bool convert_legacy_hive_parquet_utc_timestamps = false;

  // See comment in ImpalaService.thrift
  114: optional bool enable_outer_join_to_inner_transformation = false;

  // Initialized with -1 to indicate it is unspecified.
  // See comment in ImpalaService.thrift
  115: optional i64 targeted_kudu_scan_range_length = -1;

  // See comment in ImpalaService.thrift
  116: optional double report_skew_limit = 1.0;

  // See comment in ImpalaService.thrift
  117: optional bool optimize_simple_limit = false;

  // See comment in ImpalaService.thrift
  118: optional bool use_dop_for_costing = true;

  // See comment in ImpalaService.thrift
  119: optional double broadcast_to_partition_factor = 1.0;

  // See comment in ImpalaService.thrift
  120: optional i64 join_rows_produced_limit = 0;

  // See comment in ImpalaService.thrift
  121: optional bool utf8_mode = false;

  // See comment in ImpalaService.thrift
  122: optional i64 analytic_rank_pushdown_threshold = 1000;

  // See comment in ImpalaService.thrift
  123: optional double minmax_filter_threshold = 0.0;

  // See comment in ImpalaService.thrift
  124: optional PlanNodes.TMinmaxFilteringLevel minmax_filtering_level =
      PlanNodes.TMinmaxFilteringLevel.ROW_GROUP;

  // See comment in ImpalaService.thrift
  125: optional bool compute_column_minmax_stats = false;

  // See comment in ImpalaService.thrift
  126: optional bool show_column_minmax_stats = false;

  // Default NDV scale
  127: optional i32 default_ndv_scale = 2;

  // See comment in ImpalaService.thrift
  128: optional TKuduReplicaSelection kudu_replica_selection =
      TKuduReplicaSelection.CLOSEST_REPLICA;

  // See comment in ImpalaService.thrift
  129: optional bool delete_stats_in_truncate = true;

  // See comment in ImpalaService.thrift
  130: optional bool parquet_bloom_filtering = true;

  // See comment in ImpalaService.thrift
  131: optional bool minmax_filter_sorted_columns = true;

  // See comment in ImpalaService.thrift
  132: optional TMinmaxFilterFastCodePathMode minmax_filter_fast_code_path =
      TMinmaxFilterFastCodePathMode.ON;

  // See comment in ImpalaService.thrift
  133: optional bool enable_kudu_transaction = false;

  // See comment in ImpalaService.thrift
  134: optional bool minmax_filter_partition_columns = true;

  // See comment in ImpalaService.thrift
  135: optional TParquetBloomFilterWrite parquet_bloom_filter_write =
      TParquetBloomFilterWrite.IF_NO_DICT;

  // Indicates whether to use ORC's search argument to push down predicates.
  136: optional bool orc_read_statistics = true;

  // Allow ddl exec request to run in a separate thread
  137: optional bool enable_async_ddl_execution = true;

  // Allow load data exec request to run in a separate thread
  138: optional bool enable_async_load_data_execution = true;

  // Number of minimum consecutive rows when filtered out, will avoid materialization
  // of columns in parquet. Set it to -1 to turn off late materialization feature.
  139: optional i32 parquet_late_materialization_threshold = 20;

  // Max entries in the dictionary before skipping runtime filter evaluation for row
  // groups. If a dictionary has many entries, then runtime filter evaluation is more
  // likely to give false positive results, which means that the row groups won't be
  // rejected. Set it to 0 to disable runtime filter dictionary filtering, above 0 will
  // enable runtime filtering on the row group. For example, 2 means that runtime filter
  // will be evaluated when the dictionary size is smaller or equal to 2.
  140: optional i32 parquet_dictionary_runtime_filter_entry_limit = 1024;

  // Abort the Java UDF if an exception is thrown. Default is that only a
  // warning will be logged if the Java UDF throws an exception.
  141: optional bool abort_java_udf_on_exception = false;

  // Indicates whether to use ORC's async read.
  142: optional bool orc_async_read = true;

  // See comment in ImpalaService.thrift
  143: optional i32 runtime_in_list_filter_entry_limit = 1024;

  // Indicates whether to enable auto-scaling which is a process to generate a suitable
  // plan among different-sized executor group sets. The returned plan satisfies the
  // resource requirement imposed on the executor group set. Default is to enable.
  144: optional bool enable_replan = true;

  // Set to true to programmatically treat the default executor group as a two-executor
  // groups in FE as follows.
  //   1. regular: <num_nodes> nodes with 64MB of per-host estimated memory threshold
  //   2. large:   <num_nodes> nodes with 8PB of per-host estimated memory threshold
  145: optional bool test_replan = false;

  // See comment in ImpalaService.thrift
  146: optional i32 lock_max_wait_time_s = 300

  // See comment in ImpalaService.thrift
  147: optional TSchemaResolutionStrategy orc_schema_resolution = 0;

  // See comment in ImpalaService.thrift
  148: optional bool expand_complex_types = false;

  149: optional string fallback_db_for_functions;

  // See comment in ImpalaService.thrift
  150: optional bool disable_codegen_cache = false;

  151: optional TCodeGenCacheMode codegen_cache_mode = TCodeGenCacheMode.NORMAL;

  // See comment in ImpalaService.thrift
  152: optional bool stringify_map_keys = false;

  // See comment in ImpalaService.thrift
  153: optional bool enable_trivial_query_for_admission = true;

  // See comment in ImpalaService.thrift
  154: optional bool compute_processing_cost = false;

  // See comment in ImpalaService.thrift
  155: optional i32 processing_cost_min_threads = 1;

  // See comment in ImpalaService.thrift
  156: optional double join_selectivity_correlation_factor = 0.0;
  // See comment in ImpalaService.thrift
  157: optional i32 max_fragment_instances_per_node = MAX_FRAGMENT_INSTANCES_PER_NODE

  // Configures the in-memory sort algorithm used in the sorter.
  // See comment in ImpalaService.thrift
  158: optional i32 max_sort_run_size = 0;

  // See comment in ImpalaService.thrift
  159: optional bool allow_unsafe_casts = false;

  // See comment in ImpalaService.thrift
  160: optional i32 num_threads_for_table_migration = 1;

  // See comment in ImpalaService.thrift
  161: optional bool disable_optimized_iceberg_v2_read = false;

  // See comment in ImpalaService.thrift
  162: optional bool values_stmt_avoid_lossy_char_padding = false;

  // See comment in ImpalaService.thrift
  163: optional i64 large_agg_mem_threshold = 536870912  // 512MB

  // See comment in ImpalaService.thrift
  164: optional double agg_mem_correlation_factor = 0.5

  // See comment in ImpalaService.thrift
  165: optional i64 mem_limit_coordinators = 0;

  // See comment in ImpalaService.thrift
  166: optional bool iceberg_predicate_pushdown_subsetting = true;

  // See comment in ImpalaService.thrift
  167: optional i64 hdfs_scanner_non_reserved_bytes = -1

  // See comment in ImpalaService.thrift
  168: optional TCodeGenOptLevel codegen_opt_level = TCodeGenOptLevel.O2

  // See comment in ImpalaService.thrift
  169: optional i32 kudu_table_reserve_seconds = 0;

  // See comment in ImpalaService.thrift
  170: optional bool convert_kudu_utc_timestamps = false;

  // See comment in ImpalaService.thrift
  171: optional bool disable_kudu_local_timestamp_bloom_filter = true;

  // See comment in ImpalaService.thrift
  172: optional double runtime_filter_cardinality_reduction_scale = 1.0

  // See comment in ImpalaService.thrift
  173: optional i32 max_num_filters_aggregated_per_host = -1

  // See comment in ImpalaService.thrift
  174: optional double query_cpu_count_divisor

  // See comment in ImpalaService.thrift
  175: optional bool enable_tuple_cache = false;

  // See comment in ImpalaService.thrift
  176: optional bool iceberg_disable_count_star_optimization = false;

  // See comment in ImpalaService.thrift
  177: optional set<i32> runtime_filter_ids_to_skip

  // See comment in ImpalaService.thrift
  178: optional TSlotCountStrategy slot_count_strategy =
    TSlotCountStrategy.LARGEST_FRAGMENT

  179: optional bool clean_dbcp_ds_cache = true;

  // See comment in ImpalaService.thrift
  180: optional bool use_null_slots_cache = true;

  // See comment in ImpalaService.thrift
  181: optional bool write_kudu_utc_timestamps = false;
}

// Impala currently has three types of sessions: Beeswax, HiveServer2 and external
// frontend. External frontend is a variation of HiveServer2 to support external
// planning.
enum TSessionType {
  BEESWAX = 0
  HIVESERVER2 = 1
  EXTERNAL_FRONTEND = 2
}

// Client request including stmt to execute and query options.
struct TClientRequest {
  // SQL stmt to be executed
  1: required string stmt

  // query options
  2: required TQueryOptions query_options

  // Redacted SQL stmt
  3: optional string redacted_stmt

  // Indicates if an HS2 metadata operation code was provided in the client request
  4: optional bool hs2_metadata_op
}

// Per-client session state
struct TSessionState {
  // A unique identifier for this session
  3: required Types.TUniqueId session_id

  // Session Type (Beeswax or HiveServer2)
  5: required TSessionType session_type

  // The default database for the session
  1: required string database

  // The user to whom this session belongs
  2: required string connected_user

  // If set, the user we are delegating for the current session
  6: optional string delegated_user;

  // Client network address
  4: required Types.TNetworkAddress network_address

  // If set, the latest Kudu timestamp observed within this session.
  7: optional i64 kudu_latest_observed_ts;
}

// Context of this query, including the client request, session state and
// global query parameters needed for consistent expr evaluation (e.g., now()).
//
// TODO: Separate into FE/BE initialized vars.
struct TQueryCtx {
  // Client request containing stmt to execute and query options.
  1: required TClientRequest client_request

  // A globally unique id assigned to the entire query in the BE.
  // The bottom 4 bytes are 0 (for details see be/src/util/uid-util.h).
  2: required Types.TUniqueId query_id

  // Session state including user.
  3: required TSessionState session

  // String containing a timestamp (in local timezone) set as the query submission time.
  4: required string now_string

  // Process ID of the impalad to which the user is connected.
  5: required i32 pid

  // The coordinator's hostname.
  // TODO: determine whether we can get this somehow via the Thrift rpc mechanism.
  6: optional string coord_hostname

  // The initiating coordinator's address of its KRPC based ImpalaInternalService.
  7: optional Types.TNetworkAddress coord_ip_address

  // List of tables missing relevant table and/or column stats. Used for
  // populating query-profile fields consumed by CM as well as warning messages.
  8: optional list<CatalogObjects.TTableName> tables_missing_stats

  // Internal flag to disable spilling. Used as a guard against potentially
  // disastrous query plans. The rationale is that cancelling queries, e.g.,
  // with a huge join build is preferable over spilling "forever".
  9: optional bool disable_spilling

  // Set if this is a child query (e.g. a child of a COMPUTE STATS request)
  10: optional Types.TUniqueId parent_query_id

  // List of tables suspected to have corrupt stats
  11: optional list<CatalogObjects.TTableName> tables_with_corrupt_stats

  // The snapshot timestamp as of which to execute the query
  // When the backing storage engine supports snapshot timestamps (such as Kudu) this
  // allows to select a snapshot timestamp on which to perform the scan, making sure that
  // results returned from multiple scan nodes are consistent.
  // This defaults to -1 when no timestamp is specified.
  12: optional i64 snapshot_timestamp = -1;

  // Optional for frontend tests.
  // The descriptor table can be included in one of two forms:
  //  - TDescriptorTable - standard Thrift object
  //  - TDescriptorTableSerialized - binary blob with a serialized TDescriptorTable
  // Normal end-to-end query execution uses the serialized form to avoid copying a large
  // number of objects when sending RPCs. For this case, desc_tbl_serialized is set and
  // desc_tbl_testonly is not set. See IMPALA-8732.
  // Frontend tests cannot use the serialized form, because some frontend tests deal with
  // incomplete structures (e.g. THdfsTable without the required nullPartitionKeyValue
  // field) that cannot be serialized. In this case, desc_tbl_testonly is set and
  // desc_tbl_serialized is not set. See Frontend.PlanCtx.serializeDescTbl_.
  13: optional Descriptors.TDescriptorTable desc_tbl_testonly

  // Milliseconds since UNIX epoch at the start of query execution.
  14: required i64 start_unix_millis

  // Hint to disable codegen. Set by planner for single-node optimization or by the
  // backend in NativeEvalExprsWithoutRow() in FESupport. This flag is only advisory to
  // avoid the overhead of codegen and can be ignored if codegen is needed functionally.
  15: optional bool disable_codegen_hint = false;

  // List of tables with scan ranges that map to blocks with missing disk IDs.
  16: optional list<CatalogObjects.TTableName> tables_missing_diskids

  // The resolved admission control pool to which this request will be submitted. May be
  // unset for statements that aren't subjected to admission control (e.g. USE, SET).
  17: optional string request_pool

  // String containing a timestamp (in UTC) set as the query submission time. It
  // represents the same point in time as now_string
  18: required string utc_timestamp_string

  // String containing name of the local timezone.
  // It is guaranteed to be a valid timezone on the coordinator (but not necessarily on
  // the executor, since in theory the executor could have a different timezone db).
  // TODO(Csaba): adding timezone as a query option made this property redundant. It
  //   still has an effect if TimezoneDatabase::LocalZoneName() cannot find the
  //   system's local timezone and falls back to UTC. This logic will be removed in
  //   IMPALA-7359, which will make this member completely obsolete.
  19: required string local_time_zone

  // Disables the code that estimates HBase scan cardinality from key ranges.
  // When disabled, scan cardinality is estimated from HMS table row count
  // stats and key column predicate selectivity. Generally only disabled
  // for testing.
  20: optional bool disable_hbase_num_rows_estimate = false;

  // Flag to enable tracing of resource usage consumption for all fragment instances of a
  // query. Set in ImpalaServer::PrepareQueryContext().
  21: required bool trace_resource_usage = false

  // Taken from the flags of the same name. The coordinator uses these to decide how long
  // to wait for a report before cancelling a backend, so we want to ensure that the
  // coordinator and executors for a given query always agree this value.
  22: optional i32 status_report_interval_ms

  23: optional i32 status_report_max_retry_s

  24: optional Descriptors.TDescriptorTableSerialized desc_tbl_serialized

  // Stores the transaction id if the query is transactional. This is only used for HIVE
  // ACID transaction.
  25: optional i64 transaction_id

  // If mt_dop was overridden by admission control's max mt_dop setting, then this
  // is set to the original value. If mt_dop was not overridden, then this is not set.
  26: optional i32 overridden_mt_dop_value

  // The initiating coordinator's backend_id.
  27: optional Types.TUniqueId coord_backend_id

  // True if the new runtime profile format added by IMPALA-9382 should be generated
  // by this query.
  28: optional bool gen_aggregated_profile

  // True if the query is transactional for Kudu table.
  29: required bool is_kudu_transactional = false

  // True if the query can be optimized for Iceberg V2 table.
  30: required bool optimize_count_star_for_iceberg_v2 = false
}


// Execution parameters for a single plan; component of TQueryExecRequest
struct TPlanExecInfo {
  // fragments[i] may consume the output of fragments[j > i];
  // fragments[0] is the root fragment and also the coordinator fragment, if
  // it is unpartitioned.
  1: required list<Planner.TPlanFragment> fragments

  // A map from scan node ids to a scan range specification.
  // The node ids refer to scan nodes in fragments[].plan
  2: optional map<Types.TPlanNodeId, Planner.TScanRangeSpec>
      per_node_scan_ranges
}

struct TIcebergDmlFinalizeParams {
  // Type of the Iceberg operation
  1: required Types.TIcebergOperation operation

  // Stores the Iceberg spec id of the partition spec used for this DML operation.
  2: optional i32 spec_id;

  // Stores the Iceberg snapshot id of the target table for this DML operation.
  3: optional i64 initial_snapshot_id;
}

// Metadata required to finalize a query - that is, to clean up after the query is done.
// Only relevant for DML statements.
struct TFinalizeParams {
  // True if the INSERT query was OVERWRITE, rather than INTO
  1: required bool is_overwrite

  // The base directory in hdfs of the table targeted by this INSERT
  2: required string hdfs_base_dir

  // The target table name
  3: required string table_name

  // The target table database
  4: required string table_db

  // The full path in HDFS of a directory under which temporary files may be written
  // during an INSERT. For a query with id a:b, files are written to <staging_dir>/.a_b/,
  // and that entire directory is removed after the INSERT completes.
  5: optional string staging_dir

  // Identifier for the target table in the query-wide descriptor table (see
  // TDescriptorTable and TTableDescriptor).
  6: optional i64 table_id;

  // Stores the ACID transaction id of the target table for transactional INSERTs.
  7: optional i64 transaction_id;

  // Stores the ACID write id of the target table for transactional INSERTs.
  8: optional i64 write_id;

  // Stores params for Iceberg operation
  9: optional TIcebergDmlFinalizeParams iceberg_params;
}

// Result of call to ImpalaPlanService/JniFrontend.CreateQueryRequest()
struct TQueryExecRequest {
  // exec info for all plans; the first one materializes the query result, and subsequent
  // plans materialize the build sides of joins. Each plan appears before its
  // dependencies in the list.
  1: optional list<TPlanExecInfo> plan_exec_info

  // Metadata of the query result set (only for select)
  2: optional Results.TResultSetMetadata result_set_metadata

  // Set if the query needs finalization after it executes
  3: optional TFinalizeParams finalize_params

  4: required TQueryCtx query_ctx

  // The same as the output of 'explain <query>'
  5: optional string query_plan

  // The statement type governs when the coordinator can judge a query to be finished.
  // DML queries are complete after Wait(), SELECTs may not be. Generally matches
  // the stmt_type of the parent TExecRequest, but in some cases (such as CREATE TABLE
  // AS SELECT), these may differ.
  6: required Types.TStmtType stmt_type

  // List of replica hosts.  Used by the host_idx field of TScanRangeLocation.
  7: required list<Types.TNetworkAddress> host_list

  // Column lineage graph
  8: optional LineageGraph.TLineageGraph lineage_graph

  // Estimated per-host peak memory consumption in bytes. Used by admission control.
  // TODO: Remove when AC doesn't rely on this any more.
  9: optional i64 per_host_mem_estimate

  // Maximum possible (in the case all fragments are scheduled on all hosts with
  // max DOP) minimum memory reservation required per host, in bytes.
  10: optional i64 max_per_host_min_mem_reservation;

  // Maximum possible (in the case all fragments are scheduled on all hosts with
  // max DOP) required threads per host, i.e. the number of threads that this query
  // needs to execute successfully. Does not include "optional" threads.
  11: optional i64 max_per_host_thread_reservation;

  // Estimated coordinator's memory consumption in bytes assuming that the coordinator
  // fragment will run on a dedicated coordinator. Set by the planner and used by
  // admission control.
  12: optional i64 dedicated_coord_mem_estimate;

  // Indicate whether the request is a trivial query. Used by admission control.
  13: optional bool is_trivial_query

  // CPU core count required to run the query. Used by Frontend to do executor group-set
  // assignment for the query. Should either be unset or set with positive value.
  14: optional i32 cores_required

  // Estimated per-host memory. The planner generates this value which may or may not be
  // overridden to come up with a final per-host memory estimate.
  15: optional i64 planner_per_host_mem_estimate;

  // Used for system tables that need to run on all nodes.
  16: optional bool include_all_coordinators

  // Maximum admission control slot to use per executor backend.
  // Only set if COMPUTE_PROCESSING_COST option is True.
  17: optional i32 max_slot_per_executor

  // The unbounded version of cores_required. Used by Frontend to do executor group-set
  // assignment for the query. Should either be unset or set with positive value.
  18: optional i32 cores_required_unbounded
}

