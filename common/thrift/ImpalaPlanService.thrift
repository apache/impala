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
  Frontend.TExecRequest CreateExecRequest(
      1:Frontend.TClientRequest clientRequest) throws (1:TImpalaPlanServiceException e);

  // Force planservice to reload table metadata, in case it has changed due to e.g. an 
  // insert
  void RefreshMetadata();

  string GetExplainString(1:Frontend.TClientRequest queryRequest)
      throws (1:TImpalaPlanServiceException e);

  // Makes changes to the metastore - usually creates partitions as a result of an INSERT
  // statement
  void UpdateMetastore(1:Frontend.TCatalogUpdate update) 
      throws (1:TImpalaPlanServiceException e);

  void ShutdownServer();
}
