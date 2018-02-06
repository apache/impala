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

//
// This file contains the details of the protocol between coordinators and backends.

namespace cpp impala
namespace java org.apache.impala.thrift

include "Status.thrift"
include "ErrorCodes.thrift"
include "Types.thrift"
include "Exprs.thrift"
include "CatalogObjects.thrift"
include "Descriptors.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "DataSinks.thrift"
include "Results.thrift"
include "RuntimeProfile.thrift"
include "ImpalaService.thrift"
include "Data.thrift"

// constants for TQueryOptions.num_nodes
const i32 NUM_NODES_ALL = 0
const i32 NUM_NODES_ALL_RACKS = -1

// constants for TPlanNodeId
const i32 INVALID_PLAN_NODE_ID = -1

// Constant default partition ID, must be < 0 to avoid collisions
const i64 DEFAULT_PARTITION_ID = -1;

enum TParquetFallbackSchemaResolution {
  POSITION,
  NAME
}

// The order of the enum values needs to be kept in sync with
// ParquetMetadataUtils::ORDERED_ARRAY_ENCODINGS in parquet-metadata-utils.cc.
enum TParquetArrayResolution {
  THREE_LEVEL,
  TWO_LEVEL,
  TWO_LEVEL_THEN_THREE_LEVEL
}

enum TJoinDistributionMode {
  BROADCAST,
  SHUFFLE
}

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
  9: optional bool allow_unsupported_formats = 0
  11: optional string debug_action = ""
  12: optional i64 mem_limit = 0
  14: optional CatalogObjects.THdfsCompression compression_codec
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

  // Mode for compression; RECORD, or BLOCK
  // This field only applies for certain file types and is ignored
  // by all other file types.
  30: optional CatalogObjects.THdfsSeqCompressionMode seq_compression_mode

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

  // Time in ms to wait until runtime filters are delivered. If 0, the default defined
  // by the startup flag of the same name is used.
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
  43: optional TParquetFallbackSchemaResolution parquet_fallback_schema_resolution = 0

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
}

// Impala currently has two types of sessions: Beeswax and HiveServer2
enum TSessionType {
  BEESWAX,
  HIVESERVER2
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

// Client request including stmt to execute and query options.
struct TClientRequest {
  // SQL stmt to be executed
  1: required string stmt

  // query options
  2: required TQueryOptions query_options

  // Redacted SQL stmt
  3: optional string redacted_stmt
}

// Debug options: perform some action in a particular phase of a particular node
// TODO: find a better name
struct TDebugOptions {
  // The plan node that this action should be applied to. If -1 it is applied to all plan
  // nodes.
  1: optional Types.TPlanNodeId node_id
  2: optional PlanNodes.TExecNodePhase phase
  3: optional PlanNodes.TDebugAction action
  // Optional parameter that goes along with the action.
  4: optional string action_param
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

  // The initiating coordinator's address of its thrift based ImpalaInternalService.
  // TODO: determine whether we can get this somehow via the Thrift rpc mechanism.
  6: optional Types.TNetworkAddress coord_address

  // List of tables missing relevant table and/or column stats. Used for
  // populating query-profile fields consumed by CM as well as warning messages.
  7: optional list<CatalogObjects.TTableName> tables_missing_stats

  // Internal flag to disable spilling. Used as a guard against potentially
  // disastrous query plans. The rationale is that cancelling queries, e.g.,
  // with a huge join build is preferable over spilling "forever".
  8: optional bool disable_spilling

  // Set if this is a child query (e.g. a child of a COMPUTE STATS request)
  9: optional Types.TUniqueId parent_query_id

  // List of tables suspected to have corrupt stats
  10: optional list<CatalogObjects.TTableName> tables_with_corrupt_stats

  // The snapshot timestamp as of which to execute the query
  // When the backing storage engine supports snapshot timestamps (such as Kudu) this
  // allows to select a snapshot timestamp on which to perform the scan, making sure that
  // results returned from multiple scan nodes are consistent.
  // This defaults to -1 when no timestamp is specified.
  11: optional i64 snapshot_timestamp = -1;

  // Optional for frontend tests.
  12: optional Descriptors.TDescriptorTable desc_tbl

  // Milliseconds since UNIX epoch at the start of query execution.
  13: required i64 start_unix_millis

  // Hint to disable codegen. Set by planner for single-node optimization or by the
  // backend in NativeEvalExprsWithoutRow() in FESupport. This flag is only advisory to
  // avoid the overhead of codegen and can be ignored if codegen is needed functionally.
  14: optional bool disable_codegen_hint = false;

  // List of tables with scan ranges that map to blocks with missing disk IDs.
  15: optional list<CatalogObjects.TTableName> tables_missing_diskids

  // The pool to which this request has been submitted. Used to update pool statistics
  // for admission control.
  16: optional string request_pool

  // String containing a timestamp (in UTC) set as the query submission time. It
  // represents the same point in time as now_string
  17: required string utc_timestamp_string
}

// Specification of one output destination of a plan fragment
struct TPlanFragmentDestination {
  // the globally unique fragment instance id
  1: required Types.TUniqueId fragment_instance_id

  // IP address + port of the thrift based ImpalaInteralService on the destination
  2: required Types.TNetworkAddress server

  // IP address + port of the KRPC based ImpalaInternalService on the destination
  3: optional Types.TNetworkAddress krpc_server
}

// Context to collect information, which is shared among all instances of that plan
// fragment.
struct TPlanFragmentCtx {
  1: required Planner.TPlanFragment fragment

  // Output destinations, one per output partition.
  // The partitioning of the output is specified by
  // TPlanFragment.output_sink.output_partition.
  // The number of output partitions is destinations.size().
  2: list<TPlanFragmentDestination> destinations
}

// A scan range plus the parameters needed to execute that scan.
struct TScanRangeParams {
  1: required PlanNodes.TScanRange scan_range
  2: optional i32 volume_id = -1
  3: optional bool is_cached = false
  4: optional bool is_remote
}

// Execution parameters of a single fragment instance.
struct TPlanFragmentInstanceCtx {
  // TPlanFragment.idx
  1: required Types.TFragmentIdx fragment_idx

  // The globally unique fragment instance id.
  // Format: query id + query-wide fragment instance index
  // The query-wide fragment instance index enumerates all fragment instances of a
  // particular query. It starts at 0, so that the query id and the id of the first
  // fragment instance are identical.
  // If there is a coordinator instance, it is the first one, with index 0.
  // Range: [0, TExecQueryFInstancesParams.fragment_instance_ctxs.size()-1]
  2: required Types.TUniqueId fragment_instance_id

  // Index of this fragment instance across all instances of its parent fragment
  // (TPlanFragment with idx = TPlanFragmentInstanceCtx.fragment_idx).
  // Range: [0, <# of instances of parent fragment> - 1]
  3: required i32 per_fragment_instance_idx

  // Initial scan ranges for each scan node in TPlanFragment.plan_tree
  4: required map<Types.TPlanNodeId, list<TScanRangeParams>> per_node_scan_ranges

  // Number of senders for ExchangeNodes contained in TPlanFragment.plan_tree;
  // needed to create a DataStreamRecvr
  // TODO for per-query exec rpc: move these to TPlanFragmentCtx
  5: required map<Types.TPlanNodeId, i32> per_exch_num_senders

  // Id of this instance in its role as a sender.
  6: optional i32 sender_id

  7: optional TDebugOptions debug_options
}


// Service Protocol Details

enum ImpalaInternalServiceVersion {
  V1
}

// The following contains the per-rpc structs for the parameters and the result.

// ExecQueryFInstances

struct TExecQueryFInstancesParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // this backend's index into Coordinator::backend_states_,
  // needed for subsequent rpcs to the coordinator
  // required in V1
  2: optional i32 coord_state_idx

  // required in V1
  3: optional TQueryCtx query_ctx

  // required in V1
  4: optional list<TPlanFragmentCtx> fragment_ctxs

  // the order corresponds to the order of fragments in fragment_ctxs
  // required in V1
  5: optional list<TPlanFragmentInstanceCtx> fragment_instance_ctxs

  // The minimum query-wide buffer reservation size (in bytes) required for the backend
  // executing the instances in fragment_instance_ctxs. This is the peak minimum
  // reservation that may be required by the concurrently-executing operators at any
  // point in query execution. It may be less than the initial reservation total claims
  // (below) if execution of some operators never overlaps, which allows reuse of
  // reservations. required in V1
  6: optional i64 min_reservation_bytes

  // Total of the initial buffer reservations that we expect to be claimed on this
  // backend for all fragment instances in fragment_instance_ctxs. I.e. the sum over all
  // operators in all fragment instances that execute on this backend. This is used for
  // an optimization in InitialReservation. Measured in bytes. required in V1
  7: optional i64 initial_reservation_total_claims
}

struct TExecQueryFInstancesResult {
  // required in V1
  1: optional Status.TStatus status
}


// ReportExecStatus

struct TParquetInsertStats {
  // For each column, the on disk byte size
  1: required map<string, i64> per_column_size
}

struct TKuduDmlStats {
  // The number of reported per-row errors, i.e. this many rows were not modified.
  // Note that this aggregate is less useful than a breakdown of the number of errors by
  // error type, e.g. number of rows with duplicate key conflicts, number of rows
  // violating nullability constraints, etc., but it isn't possible yet to differentiate
  // all error types in the KuduTableSink yet.
  1: optional i64 num_row_errors
}

// Per partition DML stats
// TODO: this should include the table stats that we update the metastore with.
// TODO: Refactor to reflect usage by other DML statements.
struct TInsertStats {
  1: required i64 bytes_written
  2: optional TParquetInsertStats parquet_stats
  3: optional TKuduDmlStats kudu_stats
}

const string ROOT_PARTITION_KEY = ''

// Per-partition statistics and metadata resulting from DML statements.
// TODO: Refactor to reflect usage by other DML statements.
struct TInsertPartitionStatus {
  // The id of the partition written to (may be -1 if the partition is created by this
  // query). See THdfsTable.partitions.
  1: optional i64 id

  // The number of rows modified in this partition
  2: optional i64 num_modified_rows

  // Detailed statistics gathered by table writers for this partition
  3: optional TInsertStats stats

  // Fully qualified URI to the base directory for this partition.
  4: required string partition_base_dir

  // The latest observed Kudu timestamp reported by the local KuduSession.
  // This value is an unsigned int64.
  5: optional i64 kudu_latest_observed_ts
}

// The results of a DML statement, sent to the coordinator as part of
// TReportExecStatusParams
// TODO: Refactor to reflect usage by other DML statements.
struct TInsertExecStatus {
  // A map from temporary absolute file path to final absolute destination. The
  // coordinator performs these updates after the query completes.
  1: required map<string, string> files_to_move;

  // Per-partition details, used in finalization and reporting.
  // The keys represent partitions to create, coded as k1=v1/k2=v2/k3=v3..., with the
  // root's key in an unpartitioned table being ROOT_PARTITION_KEY.
  // The target table name is recorded in the corresponding TQueryExecRequest
  2: optional map<string, TInsertPartitionStatus> per_partition_status
}

// Error message exchange format
struct TErrorLogEntry {

  // Number of error messages reported using the above identifier
  1: i32 count = 0

  // Sample messages from the above error code
  2: list<string> messages
}

// Represents the states that a fragment instance goes through during its execution. The
// current state gets sent back to the coordinator and will be presented to users through
// the debug webpages.
enum TFInstanceExecState {
  WAITING_FOR_EXEC,
  WAITING_FOR_CODEGEN,
  WAITING_FOR_PREPARE,
  WAITING_FOR_OPEN,
  WAITING_FOR_FIRST_BATCH,
  FIRST_BATCH_PRODUCED,
  PRODUCING_DATA,
  LAST_BATCH_SENT,
  FINISHED
}

struct TFragmentInstanceExecStatus {
  // required in V1
  1: optional Types.TUniqueId fragment_instance_id

  // Status of fragment execution; any error status means it's done.
  // required in V1
  2: optional Status.TStatus status

  // If true, fragment finished executing.
  // required in V1
  3: optional bool done

  // cumulative profile
  // required in V1
  4: optional RuntimeProfile.TRuntimeProfileTree profile

  // The current state of this fragment instance's execution.
  // required in V1
  5: optional TFInstanceExecState current_state
}

struct TReportExecStatusParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId query_id

  // same as TExecQueryFInstancesParams.coord_state_idx
  // required in V1
  3: optional i32 coord_state_idx

  4: list<TFragmentInstanceExecStatus> instance_exec_status

  // Cumulative structural changes made by the table sink of any instance
  // included in instance_exec_status
  // optional in V1
  5: optional TInsertExecStatus insert_exec_status;

  // New errors that have not been reported to the coordinator by any of the
  // instances included in instance_exec_status
  6: optional map<ErrorCodes.TErrorCode, TErrorLogEntry> error_log;

  // Cumulative status for this backend. A backend can have an error from a specific
  // fragment instance, or it can have a general error that is independent of any
  // individual fragment. If reporting a single error, this status is always set to
  // the error being reported. If reporting multiple errors, the status is set by the
  // following rules:
  // 1. A general error takes precedence over any fragment instance error.
  // 2. Any fragment instance error takes precedence over any cancelled status.
  // 3. If multiple fragments have errors, prefer the error that comes first in the
  // 'instance_exec_status' list.
  // This status is only OK if all fragment instances included are OK.
  7: optional Status.TStatus status;
}

struct TReportExecStatusResult {
  // required in V1
  1: optional Status.TStatus status
}


// CancelQueryFInstances

struct TCancelQueryFInstancesParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId query_id
}

struct TCancelQueryFInstancesResult {
  // required in V1
  1: optional Status.TStatus status
}


// TransmitData

struct TTransmitDataParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId dest_fragment_instance_id

  // Id of this fragment in its role as a sender.
  3: optional i32 sender_id

  // required in V1
  4: optional Types.TPlanNodeId dest_node_id

  // optional in V1
  5: optional Results.TRowBatch row_batch

  // if set to true, indicates that no more row batches will be sent
  // for this dest_node_id
  6: optional bool eos
}

struct TTransmitDataResult {
  // required in V1
  1: optional Status.TStatus status
}

// Parameters for RequestPoolService.resolveRequestPool()
// TODO: why is this here?
struct TResolveRequestPoolParams {
  // User to resolve to a pool via the allocation placement policy and
  // authorize for pool access.
  1: required string user

  // Pool name specified by the user. The allocation placement policy may
  // return a different pool.
  2: required string requested_pool
}

// Returned by RequestPoolService.resolveRequestPool()
struct TResolveRequestPoolResult {
  // Actual pool to use, as determined by the pool allocation policy. Not set
  // if no pool was resolved.
  1: optional string resolved_pool

  // True if the user has access to submit requests to the resolved_pool. Not set
  // if no pool was resolved.
  2: optional bool has_access

  3: optional Status.TStatus status
}

// Parameters for RequestPoolService.getPoolConfig()
// TODO: why is this here?
struct TPoolConfigParams {
  // Pool name
  1: required string pool
}

// Returned by RequestPoolService.getPoolConfig()
struct TPoolConfig {
  // Maximum number of placed requests before incoming requests are queued.
  // A value of 0 effectively disables the pool. -1 indicates no limit.
  1: required i64 max_requests

  // Maximum number of queued requests before incoming requests are rejected.
  // Any non-positive number (<= 0) disables queuing, i.e. requests are rejected instead
  // of queued.
  2: required i64 max_queued

  // Maximum memory resources of the pool in bytes.
  // A value of 0 effectively disables the pool. -1 indicates no limit.
  3: required i64 max_mem_resources

  // Maximum amount of time (in milliseconds) that a request will wait to be admitted
  // before timing out. Optional, if not set then the process default (set via gflags) is
  // used.
  4: optional i64 queue_timeout_ms;

  // Default query options that are applied to requests mapped to this pool.
  5: required string default_query_options;
}

struct TBloomFilter {
  // Log_2 of the bufferpool space required for this filter.
  // See BloomFilter::BloomFilter() for details.
  1: required i32 log_bufferpool_space

  // List of buckets representing the Bloom Filter contents, laid out contiguously in one
  // string for efficiency of (de)serialisation. See BloomFilter::Bucket and
  // BloomFilter::directory_.
  2: binary directory

  // If always_true or always_false is true, 'directory' and 'log_bufferpool_space' are
  // not meaningful.
  3: required bool always_true
  4: required bool always_false
}

struct TMinMaxFilter {
  // If true, filter allows all elements to pass and 'min'/'max' will not be set.
  1: required bool always_true

  // If true, filter doesn't allow any elements to pass and 'min'/'max' will not be set.
  2: required bool always_false

  3: optional Data.TColumnValue min
  4: optional Data.TColumnValue max
}

// UpdateFilter

struct TUpdateFilterParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // Filter ID, unique within a query.
  // required in V1
  2: optional i32 filter_id

  // Query that this filter is for.
  // required in V1
  3: optional Types.TUniqueId query_id

  // required in V1
  4: optional TBloomFilter bloom_filter

  5: optional TMinMaxFilter min_max_filter
}

struct TUpdateFilterResult {
}


// PublishFilter

struct TPublishFilterParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // Filter ID to update
  // required in V1
  2: optional i32 filter_id

  // required in V1
  3: optional Types.TUniqueId dst_query_id

  // Index of fragment to receive this filter
  // required in V1
  4: optional Types.TFragmentIdx dst_fragment_idx

  // Actual bloom_filter payload
  // required in V1
  5: optional TBloomFilter bloom_filter

  6: optional TMinMaxFilter min_max_filter
}

struct TPublishFilterResult {
}


service ImpalaInternalService {
  // Called by coord to start asynchronous execution of a query's fragment instances in
  // backend.
  // Returns as soon as all incoming data streams have been set up.
  TExecQueryFInstancesResult ExecQueryFInstances(1:TExecQueryFInstancesParams params);

  // Periodically called by backend to report status of fragment instance execution
  // back to coord; also called when execution is finished, for whatever reason.
  TReportExecStatusResult ReportExecStatus(1:TReportExecStatusParams params);

  // Called by coord to cancel execution of a single query's fragment instances, which
  // the coordinator initiated with a prior call to ExecQueryFInstances.
  // Cancellation is asynchronous.
  TCancelQueryFInstancesResult CancelQueryFInstances(
      1:TCancelQueryFInstancesParams params);

  // Called by sender to transmit single row batch. Returns error indication
  // if params.fragmentId or params.destNodeId are unknown or if data couldn't be read.
  TTransmitDataResult TransmitData(1:TTransmitDataParams params);

  // Called by fragment instances that produce local runtime filters to deliver them to
  // the coordinator for aggregation and broadcast.
  TUpdateFilterResult UpdateFilter(1:TUpdateFilterParams params);

  // Called by the coordinator to deliver global runtime filters to fragments for
  // application at plan nodes.
  TPublishFilterResult PublishFilter(1:TPublishFilterParams params);
}
