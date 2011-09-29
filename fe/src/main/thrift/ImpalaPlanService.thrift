// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"
include "Descriptors.thrift"
include "Exprs.thrift"
include "PlanNodes.thrift"

// The plan is optional, because a select stmt without a from clause
// doesn't need a plan or descriptors.
struct TExecutePlanRequest {
  1: optional PlanNodes.TPlan plan
  2: optional Descriptors.TDescriptorTable descTbl
  3: list<Exprs.TExpr> selectListExprs

  // if true, return result in ascii, otherwise return in binary format
  // (see TColumnValue)
  4: required bool as_ascii

  // if true, abort execution on the first error
  5: required bool abort_on_error

  // maximum # of errors to be reported
  6: required i32 max_errors

  // for debugging: set batch size used by backend internally;
  // a size of 0 indicates backend default
  7: required i32 batch_size
}

exception TException {
  1: string msg;
}

// We're running the Impala frontend as a service from which the backend
// test driver can get plans to run.
service ImpalaPlanService {
  TExecutePlanRequest GetExecRequest(1:string query) throws (1:TException e);

  void ShutdownServer();
}
