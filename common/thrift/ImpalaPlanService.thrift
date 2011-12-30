// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "ImpalaBackendService.thrift"

exception TException {
  1: string msg;
}

// We're running the Impala frontend as a service from which the backend
// test driver can get plans to run.
service ImpalaPlanService {
  ImpalaBackendService.TQueryExecRequest
      GetExecRequest(1:string query, 2:i32 numNodes) throws (1:TException e);

  void ShutdownServer();
}
