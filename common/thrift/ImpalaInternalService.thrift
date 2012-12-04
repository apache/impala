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
include "Descriptors.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "DataSinks.thrift"
include "Data.thrift"
include "RuntimeProfile.thrift"
include "ImpalaService.thrift"

// constants for TQueryOptions.num_nodes
const i32 NUM_NODES_ALL = 0
const i32 NUM_NODES_ALL_RACKS = -1

// constants for TPlanNodeId
const i32 INVALID_PLAN_NODE_ID = -1

// Constant default partition ID, must be < 0 to avoid collisions
const i64 DEFAULT_PARTITION_ID = -1;

// Query options that correspond to ImpalaService.ImpalaQueryOptions
// TODO: make all of these optional, otherwise it will be impossible to
// retire options and do rolling upgrades between releases
struct TQueryOptions {
  1: required bool abort_on_error = 0
  2: required i32 max_errors = 0
  3: required bool disable_codegen = 0
  4: required i32 batch_size = 0
  
  // return_as_ascii is not listed in ImpalaService.ImpalaQueryOptions because Beeswax
  // should only return ascii. This option is only for internal testing.
  // if true, return query results in ASCII format (TColumnValue.stringVal),
  // otherwise return results in their native format (each TColumnValue
  // uses the field corresponding to the column's native type). 
  // TODO: this is not used in the backend at all. Remove this option.
  5: required bool return_as_ascii = 1
  
  6: required i32 num_nodes = NUM_NODES_ALL
  7: required i64 max_scan_range_length = 0
  8: required i32 num_scanner_threads = 0
  9: required i32 max_io_buffers = 0
  10: required bool allow_unsupported_formats = 0
  11: optional i64 default_order_by_limit = -1
}

// A scan range plus the parameters needed to execute that scan.
struct TScanRangeParams {
  1: required PlanNodes.TScanRange scan_range
  2: optional i32 volume_id = -1
}

// Specification of one output destination of a plan fragment
struct TPlanFragmentDestination {
  // the globally unique fragment instance id
  1: required Types.TUniqueId fragment_instance_id

  // ... which is being executed on this server
  2: required Types.THostPort server
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
}

// Global query parameters assigned by the coordinator.
struct TQueryGlobals {
  // String containing a timestamp set as the current time.
  1: required string now_string
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
  5: optional Types.THostPort coord

  // backend number assigned by coord to identify backend
  // required in V1
  6: optional i32 backend_num

  // Global query parameters assigned by coordinator.
  // required in V1
  7: optional TQueryGlobals query_globals
  
  // options for the query
  // required in V1
  8: optional TQueryOptions query_options
}

struct TExecPlanFragmentResult {
  // required in V1
  1: optional Status.TStatus status
}


// ReportExecStatus

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
