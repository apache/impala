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

//
// This file contains the details of the protocol between coordinators and backends.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Status.thrift"
include "Types.thrift"
include "Exprs.thrift"
include "CatalogObjects.thrift"
include "Descriptors.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "DataSinks.thrift"
include "Data.thrift"
include "RuntimeProfile.thrift"
include "ImpalaService.thrift"
include "Llama.thrift"

// constants for TQueryOptions.num_nodes
const i32 NUM_NODES_ALL = 0
const i32 NUM_NODES_ALL_RACKS = -1

// constants for TPlanNodeId
const i32 INVALID_PLAN_NODE_ID = -1

// Constant default partition ID, must be < 0 to avoid collisions
const i64 DEFAULT_PARTITION_ID = -1;

// Query options that correspond to ImpalaService.ImpalaQueryOptions,
// with their respective defaults
struct TQueryOptions {
  1: optional bool abort_on_error = 0
  2: optional i32 max_errors = 0
  3: optional bool disable_codegen = 0
  4: optional i32 batch_size = 0
  5: optional i32 num_nodes = NUM_NODES_ALL
  6: optional i64 max_scan_range_length = 0
  7: optional i32 num_scanner_threads = 0

  8: optional i32 max_io_buffers = 0              // Deprecated in 1.1
  9: optional bool allow_unsupported_formats = 0
  10: optional i64 default_order_by_limit = -1
  11: optional string debug_action = ""
  12: optional i64 mem_limit = 0
  13: optional bool abort_on_default_limit_exceeded = 0
  14: optional CatalogObjects.THdfsCompression parquet_compression_codec =
      CatalogObjects.THdfsCompression.SNAPPY
  15: optional i32 hbase_caching = 0
  16: optional bool hbase_cache_blocks = 0
  17: optional i64 parquet_file_size = 0
  18: optional Types.TExplainLevel explain_level
  19: optional bool sync_ddl = 0

  // Request pool this request should be submitted to. If not set
  // the pool is determined based on the user.
  20: optional string request_pool

  // Per-host virtual CPU cores required for query (only relevant with RM).
  21: optional i16 v_cpu_cores

  // Max time in milliseconds the resource broker should wait for
  // a resource request to be granted by Llama/Yarn (only relevant with RM).
  22: optional i64 reservation_request_timeout

  // Disables taking advantage of HDFS caching. This has two parts:
  // 1. disable preferring to schedule to cached replicas
  // 2. disable the cached read path.
  23: optional bool disable_cached_reads = 0
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

  // If set, the user we are impersonating for the current session
  6: optional string impersonated_user;

  // Client network address
  4: required Types.TNetworkAddress network_address
}

// Client request including stmt to execute and query options.
struct TClientRequest {
  // SQL stmt to be executed
  1: required string stmt

  // query options
  2: required TQueryOptions query_options
}

// Context of this query, including the client request, session state and
// global query parameters needed for consistent expr evaluation (e.g., now()).
struct TQueryContext {
  // Client request containing stmt to execute and query options.
  1: required TClientRequest request

  // Session state including user.
  2: required TSessionState session

  // String containing a timestamp set as the query submission time.
  3: required string now_string

  // Process ID of the impalad to which the user is connected.
  4: required i32 pid

  // List of tables missing relevant table and/or column stats. Used for
  // populating query-profile fields consumed by CM as well as warning messages.
  5: optional list<CatalogObjects.TTableName> tables_missing_stats
}

// A scan range plus the parameters needed to execute that scan.
struct TScanRangeParams {
  1: required PlanNodes.TScanRange scan_range
  2: optional i32 volume_id = -1
  3: optional bool is_cached = false
}

// Specification of one output destination of a plan fragment
struct TPlanFragmentDestination {
  // the globally unique fragment instance id
  1: required Types.TUniqueId fragment_instance_id

  // ... which is being executed on this server
  2: required Types.TNetworkAddress server
}

// Parameters for a single execution instance of a particular TPlanFragment
// TODO: for range partitioning, we also need to specify the range boundaries
struct TPlanFragmentExecParams {
  // a globally unique id assigned to the entire query
  1: required Types.TUniqueId query_id

  // a globally unique id assigned to this particular execution instance of
  // a TPlanFragment
  2: required Types.TUniqueId fragment_instance_id

  // initial scan ranges for each scan node in TPlanFragment.plan_tree
  3: required map<Types.TPlanNodeId, list<TScanRangeParams>> per_node_scan_ranges

  // number of senders for ExchangeNodes contained in TPlanFragment.plan_tree;
  // needed to create a DataStreamRecvr
  4: required map<Types.TPlanNodeId, i32> per_exch_num_senders

  // Output destinations, one per output partition.
  // The partitioning of the output is specified by
  // TPlanFragment.output_sink.output_partition.
  // The number of output partitions is destinations.size().
  5: list<TPlanFragmentDestination> destinations

  // Debug options: perform some action in a particular phase of a particular node
  6: optional Types.TPlanNodeId debug_node_id
  7: optional PlanNodes.TExecNodePhase debug_phase
  8: optional PlanNodes.TDebugAction debug_action

  // The pool to which this request has been submitted. Used to update pool statistics
  // for admission control.
  9: optional string request_pool;
}

// Service Protocol Details

enum ImpalaInternalServiceVersion {
  V1
}


// ExecPlanFragment

struct TExecPlanFragmentParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // required in V1
  2: optional Planner.TPlanFragment fragment

  // required in V1
  3: optional Descriptors.TDescriptorTable desc_tbl

  // required in V1
  4: optional TPlanFragmentExecParams params

  // Initiating coordinator.
  // TODO: determine whether we can get this somehow via the Thrift rpc mechanism.
  // required in V1
  5: optional Types.TNetworkAddress coord

  // backend number assigned by coord to identify backend
  // required in V1
  6: optional i32 backend_num

  // Context of this query, including query options, session state and
  // global query parameters needed for consistent expr evaluation (e.g., now()).
  // required in V1
  7: optional TQueryContext query_ctxt

  // Resource reservation to run this plan fragment in.
  8: optional Llama.TAllocatedResource reserved_resource

  // Address of local node manager (used for expanding resource allocations)
  9: optional Types.TNetworkAddress local_resource_address
}

struct TExecPlanFragmentResult {
  // required in V1
  1: optional Status.TStatus status
}

// ReportExecStatus
struct TParquetInsertStats {
  // For each column, the on disk byte size
  1: required map<string, i64> per_column_size
}

// Per partition insert stats
// TODO: this should include the table stats that we update the metastore with.
struct TInsertStats {
  1: required i64 bytes_written
  2: optional TParquetInsertStats parquet_stats
}

// The results of an INSERT query, sent to the coordinator as part of
// TReportExecStatusParams
struct TInsertExecStatus {
  // Number of rows appended by an INSERT, per-partition.
  // The keys represent partitions to create, coded as k1=v1/k2=v2/k3=v3..., with the
  // root in an unpartitioned table being the empty string.
  // The target table name is recorded in the corresponding TQueryExecRequest
  1: optional map<string, i64> num_appended_rows

  // A map from temporary absolute file path to final absolute destination. The
  // coordinator performs these updates after the query completes.
  2: required map<string, string> files_to_move;

  // Stats from running the insert, per-partition. The keys are coded the
  // same way as with num_appended_rows.
  3: optional map<string, TInsertStats> insert_stats
}

struct TReportExecStatusParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId query_id

  // passed into ExecPlanFragment() as TExecPlanFragmentParams.backend_num
  // required in V1
  3: optional i32 backend_num

  // required in V1
  4: optional Types.TUniqueId fragment_instance_id

  // Status of fragment execution; any error status means it's done.
  // required in V1
  5: optional Status.TStatus status

  // If true, fragment finished executing.
  // required in V1
  6: optional bool done

  // cumulative profile
  // required in V1
  7: optional RuntimeProfile.TRuntimeProfileTree profile

  // Cumulative structural changes made by a table sink
  // optional in V1
  8: optional TInsertExecStatus insert_exec_status;

  // New errors that have not been reported to the coordinator
  // optional in V1
  9: optional list<string> error_log
}

struct TReportExecStatusResult {
  // required in V1
  1: optional Status.TStatus status
}


// CancelPlanFragment

struct TCancelPlanFragmentParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId fragment_instance_id
}

struct TCancelPlanFragmentResult {
  // required in V1
  1: optional Status.TStatus status
}


// TransmitData

struct TTransmitDataParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId dest_fragment_instance_id

  // for debugging purposes; currently ignored
  //3: optional Types.TUniqueId src_fragment_instance_id

  // required in V1
  4: optional Types.TPlanNodeId dest_node_id

  // required in V1
  5: optional Data.TRowBatch row_batch

  // if set to true, indicates that no more row batches will be sent
  // for this dest_node_id
  6: optional bool eos
}

struct TTransmitDataResult {
  // required in V1
  1: optional Status.TStatus status
}

// Parameters for RequestPoolService.resolveRequestPool()
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
  // Actual pool to use, as determined by the pool allocation policy.
  1: required string resolved_pool

  // True if the user has access to submit requests to the resolved_pool.
  2: required bool has_access
}

// Parameters for RequestPoolService.getPoolConfig()
struct TPoolConfigParams {
  // Pool name
  1: required string pool
}

// Returned by RequestPoolService.getPoolConfig()
struct TPoolConfigResult {
  // Maximum number of placed requests before incoming requests are queued.
  1: required i64 max_requests

  // Maximum number of queued requests before incoming requests are rejected.
  2: required i64 max_queued

  // Memory limit of the pool before incoming requests are queued.
  // -1 indicates no limit.
  3: required i64 mem_limit
}

service ImpalaInternalService {
  // Called by coord to start asynchronous execution of plan fragment in backend.
  // Returns as soon as all incoming data streams have been set up.
  TExecPlanFragmentResult ExecPlanFragment(1:TExecPlanFragmentParams params);

  // Periodically called by backend to report status of plan fragment execution
  // back to coord; also called when execution is finished, for whatever reason.
  TReportExecStatusResult ReportExecStatus(1:TReportExecStatusParams params);

  // Called by coord to cancel execution of a single plan fragment, which this
  // coordinator initiated with a prior call to ExecPlanFragment.
  // Cancellation is asynchronous.
  TCancelPlanFragmentResult CancelPlanFragment(1:TCancelPlanFragmentParams params);

  // Called by sender to transmit single row batch. Returns error indication
  // if params.fragmentId or params.destNodeId are unknown or if data couldn't be read.
  TTransmitDataResult TransmitData(1:TTransmitDataParams params);
}
