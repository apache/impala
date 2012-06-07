// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Frontend.thrift"

exception TImpalaPlanServiceException {
  1: string msg;
}

// We're running the Impala frontend as a service from which the backend
// test driver can get plans to run.
service ImpalaPlanService {
  Frontend.TQueryRequestResult GetQueryRequestResult(1:string query, 2:i32 numNodes)
      throws (1:TImpalaPlanServiceException e);

  // Force planservice to reload table metadata, in case it has changed due to e.g. an 
  // insert
  void RefreshMetadata();

  string GetExplainString(1:string query, 2:i32 numNodes)
      throws (1:TImpalaPlanServiceException e);

  void ShutdownServer();
}
