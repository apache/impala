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
