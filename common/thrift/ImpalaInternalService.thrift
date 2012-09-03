// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
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
include "JavaConstants.thrift"

// Query options that correspond to ImpalaService.ImpalaQueryOptions
struct TQueryOptions {
  1: required bool abort_on_error = 1
  2: required i32 max_errors = 0
  3: required bool disable_codegen = 0
  4: required i32 batch_size = 0
  
  // return_as_ascii is not listed in ImpalaService.ImpalaQueryOptions because Beeswax
  // should only return ascii. This option is only for internal testing.
  // if true, return query results in ASCII format (TColumnValue.stringVal),
  // otherwise return results in their native format (each TColumnValue
  // uses the field corresponding to the column's native type). 
  5: required bool return_as_ascii = 1
  
  6: required i32 num_nodes = JavaConstants.NUM_NODES_ALL
  7: required i64 max_scan_range_length = 0
  8: required i32 file_buffer_size = 0
  9: required i32 num_scanner_threads = 0
  10: required i32 max_io_buffers = 0
}

// Parameters for the execution of a plan fragment on a particular node.
struct TPlanExecParams {
  // scan ranges for each of the scan nodes
  1: list<PlanNodes.TScanRange> scan_ranges

  // id of fragment that receives the output of this fragment
  2: optional Types.TUniqueId dest_fragment_id

  // (host, port) pairs of output destinations, one per output partition
  3: list<Types.THostPort> destinations
}

// A scan range plus the parameters needed to execute that scan.
struct TScanRangeParams {
  1: required PlanNodes.TScanRange2 scan_range
  2: required i32 volume_id
}

// Specification of one output destination of a plan fragment
struct TPlanFragmentDestination {
  // The unique fragment id
  1: required Types.TUniqueId fragment_id

  // ... which is being executed on this server
  2: required Types.THostPort server
}

// Parameters for the execution of a plan fragment on a particular node.
// TODO: remove TPlanExecParams once transition to NewPlanner is complete
// TODO: for range partitioning, we also need to specify the range boundaries
struct TPlanFragmentExecParams {
  // initial scan ranges for each scan node in TPlanFragment.plan_tree
  1: map<Types.TPlanNodeId, list<TScanRangeParams>> per_node_scan_ranges

  // Output destinations, one per output partition.
  // The partitioning of the output is specified by
  // TPlanFragment.output_sink.output_partitioning.
  // The number of output partitions is destinations.size().
  2: list<TPlanFragmentDestination> destinations
}

// Global query parameters assigned by the coordinator.
struct TQueryGlobals {
  // String containing a timestamp set as the current time.
  1: required string now_string
}

// TPlanExecRequest encapsulates info needed to execute a particular
// plan fragment, including how to produce and how to partition its output.
// It leaves out node-specific parameters (see TPlanExecParams).
struct TPlanExecRequest {
  // Globally unique id for each fragment. Assigned by coordinator.
  1: required Types.TUniqueId fragment_id

  // same as TQueryExecRequest.query_id
  2: required Types.TUniqueId query_id

  // no plan or descriptor table: query without From clause
  3: optional PlanNodes.TPlan plan_fragment
  4: optional Descriptors.TDescriptorTable desc_tbl

  // exprs that produce values for slots of output tuple (one expr per slot)
  5: list<Exprs.TExpr> output_exprs
  
  // Specifies the destination of this plan fragment's output rows.
  // For example, the destination could be a stream sink which forwards 
  // the data to a remote plan fragment, or a sink which writes to a table (for
  // insert stmts).
  6: optional DataSinks.TDataSink data_sink
  
  // Global query parameters assigned by coordinator.
  7: required TQueryGlobals query_globals
  
  // query options for the query
  8: required TQueryOptions query_options
}

// Service Protocol Details

enum ImpalaInternalServiceVersion {
  V1
}


// ExecPlanFragment

struct TExecPlanFragmentParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // required in V1
  2: optional TPlanExecRequest request

  // required in V1
  3: optional TPlanExecParams params

  // Initiating coordinator.
  // TODO: determine whether we can get this somehow via the Thrift rpc mechanism.
  // required in V1
  4: optional Types.THostPort coord

  // backend number assigned by coord to identify backend
  // required in V1
  5: optional i32 backend_num
}

// TODO: remove TExecPlanFragmentParams once migration to NewPlanner is complete
struct TExecPlanFragmentParams2 {
  1: required ImpalaInternalServiceVersion protocol_version

  // Globally unique id for each fragment. Assigned by coordinator.
  // Required in V1.
  2: optional Types.TUniqueId fragment_id

  // required in V1
  3: optional Planner.TPlanFragment fragment

  // required in V1
  4: optional Descriptors.TDescriptorTable desc_tbl

  // required in V1
  5: optional TPlanFragmentExecParams params

  // Initiating coordinator.
  // TODO: determine whether we can get this somehow via the Thrift rpc mechanism.
  // required in V1
  6: optional Types.THostPort coord

  // backend number assigned by coord to identify backend
  // required in V1
  7: optional i32 backend_num

  // Global query parameters assigned by coordinator.
  // required in V1
  8: optional TQueryGlobals query_globals
  
  // options for the query
  // required in V1
  9: optional TQueryOptions query_options
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
  4: optional Types.TUniqueId fragment_id

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
}

struct TReportExecStatusResult {
  // required in V1
  1: optional Status.TStatus status
}


// CancelPlanFragment

struct TCancelPlanFragmentParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId fragment_id
}

struct TCancelPlanFragmentResult {
  // required in V1
  1: optional Status.TStatus status
}


// TransmitData

struct TTransmitDataParams {
  1: required ImpalaInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId dest_fragment_id

  // for debugging purposes; currently ignored
  //3: optional Types.TUniqueId src_fragment_id

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
