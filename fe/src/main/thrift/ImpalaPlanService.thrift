// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"
include "Descriptors.thrift"
include "Exprs.thrift"
include "PlanNodes.thrift"
include "LocalExecutor.thrift"

exception TAnalysisException {
  1: string msg;
}

// We're running the Impala frontend as a service from which the backend
// test driver can get plans to run.
service ImpalaPlanService {
  LocalExecutor.TExecutePlanRequest GetExecRequest(1:string query)
    throws (1:TAnalysisException e);

  void ShutdownServer();
}
