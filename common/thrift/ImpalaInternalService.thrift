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

// TQueryExecRequest encapsulates everything needed to execute all plan fragments
// for a single query. 
// If only a single plan fragment is present, it is executed by the coordinator itself.
// TODO: should this move elsewhere?
struct TQueryExecRequest {
  // Globally unique id for this query. Assigned by the planner.
  1: required Types.TUniqueId query_id

  // True if the coordinator should execute a fragment, located in fragment_requests[0]
  2: required bool has_coordinator_fragment;

  // one request per plan fragment;
  // fragmentRequests[i] may consume the output of fragmentRequests[j > i];
  // fragmentRequests[0] will contain the coordinator fragment if one exists
  3: list<TPlanExecRequest> fragment_requests

  // list of host/port pairs that serve the data for the plan fragments
  // If has_coordinator_fragment == true then:
  //   data_locations.size() == fragment_requests.size() - 1, and fragment_requests[i+1]
  //   is executed on data_locations[i], since (fragment_requests[0] is the coordinator
  //   fragment, which is executed by the coordinator itself) 
  // else: 
  //   data_locations.size() == fragment_requests.size(), and fragment_requests[i]
  // is executed on data_locations[i]
  4: list<list<Types.THostPort>> data_locations

  // node-specific request parameters;
  // nodeRequestParams[i][j] is the parameter for fragmentRequests[i] executed on 
  // execNodes[i][j]
  5: list<list<TPlanExecParams>> node_request_params

  // for debugging purposes (to allow backend to log the query string)
  6: optional string sql_stmt;

  // If this query INSERTs data, it is convenient to have to table name and db available
  // when updating the metastore. 
  7: optional string insert_table_name;
  8: optional string insert_table_db;
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

struct TExecPlanFragmentResult {
  // required in V1
  1: optional Status.TStatus status
}


// ReportExecStatus

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

  // list of partitions that need to be created after an INSERT.
  // Target table name is recorded in TQueryExecRequest.
  // optional in V1
  8: optional list<string> partitions_to_create
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
