// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"
include "Exprs.thrift"
include "Descriptors.thrift"
include "PlanNodes.thrift"
include "DataSinks.thrift"
include "Data.thrift"
include "RuntimeProfile.thrift"

// Parameters for the execution of a plan fragment on a particular node.
struct TPlanExecParams {
  // scan ranges for each of the scan nodes
  1: list<PlanNodes.TScanRange> scanRanges

  // (host, port) pairs of output destinations, one per output partition
  2: list<Types.THostPort> destinations

  // global execution flags
  3: required bool abortOnError
  4: required i32 maxErrors
  5: required bool disableCodegen

  // 0 means use default
  6: required i32 batchSize
}

// TPlanExecRequest encapsulates info needed to execute a particular
// plan fragment, including how to produce and how to partition its output.
// It leaves out node-specific parameters (see TPlanExecParams).
struct TPlanExecRequest {
  // same as TQueryExecRequest.queryId
  1: required Types.TUniqueId queryId

  // no plan or descriptor table: query without From clause
  2: optional PlanNodes.TPlan planFragment
  3: optional Descriptors.TDescriptorTable descTbl

  // id of output tuple produced by this plan fragment
  // TODO: do we need this?
  //3: required Types.TTupleId outputTupleId

  // exprs that produce values for slots of output tuple (one expr per slot)
  4: list<Exprs.TExpr> outputExprs
  
  // Specifies the destination of this plan fragment's output rows.
  // For example, the destination could be a stream sink which forwards 
  // the data to a remote plan fragment, 
  // or a sink which writes to a table (for insert stmts).
  5: optional DataSinks.TDataSink dataSink
}

// TExecPlanFragmentResult encapsulates the result of running a plan fragment.  It contains
// the status result and performance counters.
struct TExecPlanFragmentResult {
  1: required Types.TStatus status
  2: required RuntimeProfile.TRuntimeProfileTree profiles
}

// TQueryExecRequest encapsulates everything needed to execute all plan fragments
// for a single query. It is passed to the backend's coordinator module via a call
// to the native function NativeBackend.ExecQuery() (ie, it is not used as part of any
// Thrift rpc).
// If only a single plan fragment is present, it is executed by the coordinator itself.
struct TQueryExecRequest {
  1: required Types.TUniqueId queryId

  // one request per plan fragment;
  // fragmentRequests[i] may consume the output of fragmentRequests[j > i];
  // fragmentRequests[0] contains the coordinator fragment
  2: list<TPlanExecRequest> fragmentRequests

  // list of host/port that serve the data for the plan fragments
  // dataLocations.size() == fragmentRequests.size() - 1, and fragmentRequests[i+1] is
  // executed on dataLocations[i] (fragmentRequests[0] is the coordinator fragment, which
  // is executed by the coordinator itself)	
  3: list<list<Types.THostPort>> dataLocations

  // node-specific request parameters;
  // nodeRequestParams[i][j] is the parameter for fragmentRequests[i] executed on 
  // execNodes[i-1][j]
  4: list<list<TPlanExecParams>> nodeRequestParams

  // if true, return result in ascii, otherwise return in binary format
  // (see TColumnValue)
  5: required bool asAscii

  // if true, abort execution on the first error
  6: required bool abortOnError

  // maximum # of errors to be reported
  7: required i32 maxErrors

  // if true, BE will disable llvm codegen
  8: required bool disableCodegen

  // for debugging: set batch size used by backend internally;
  // a size of 0 indicates backend default
  9: required i32 batchSize

  // for debugging purposes (to allow backend to log the query string)
  10: optional string sqlStmt;
}

service ImpalaBackendService {
  // Synchronous execution of plan fragment. Returns completion status.
  TExecPlanFragmentResult ExecPlanFragment(
      1:TPlanExecRequest request, 2:TPlanExecParams params);

  // Transmit single row batch. Returns error indication if queryId or destNodeId
  // are unknown or if data couldn't be read.
  Types.TStatus TransmitData(
      1:Types.TUniqueId queryId, 2:Types.TPlanNodeId destNodeId,
      3:Data.TRowBatch rowBatch);

  // Close channel between this receiver and one of the data stream senders,
  // indicating that no more data will be transmitted by this sender.
  // TODO: do we need this or is eos enough?
  Types.TStatus CloseChannel(1:Types.TUniqueId queryId, 2:Types.TPlanNodeId destNodeId);
}
