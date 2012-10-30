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
