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


#ifndef IMPALA_EXEC_DDL_EXECUTOR_H
#define IMPALA_EXEC_DDL_EXECUTOR_H

#include <boost/scoped_ptr.hpp>
#include "gen-cpp/Frontend_types.h"

namespace impala {

class ExecEnv;
class RowBatch;
class Status;
class ImpalaServer;

// The DdlExecutor is responsible for executing statements that modify or query table
// metadata explicitly. These include SHOW and DESCRIBE statements, HiveServer2 metadata
// operations and may in the future include CREATE and ALTER.
// One DdlExecutor is typically created per query statement. 
// Rows are returned in result_set.
// All rows are available to be read after Exec() returns.
class DdlExecutor {
 public:
  DdlExecutor(ImpalaServer* impala_server);

  // Runs a DDL query to completion. Once Exec() returns, all rows are available in
  // result_set().
  Status Exec(TDdlExecRequest* exec_request);

  // Runs a metadata operation to completion. This will assign a unique request
  // id which can be retrieved from request_id(). Once Exec() returns, all rows are
  // available in result_set() and the result set schema can be retrieved from
  // result_set_metadata().
  Status Exec(const TMetadataOpRequest& exec_request);

  // Returns the list of rows retrieved in Exec().
  const std::vector<TResultRow>& result_set() { return result_set_; }

  // Returns the unique request id. Only available if using Exec(TMetadataOpRequest)
  const TUniqueId& request_id() { return request_id_; }

  // Returns the metadata of the result set. Only available if using
  // Exec(TMetadataOpRequest)
  const TResultSetMetadata& result_set_metadata() { return result_set_metadata_; }

 private:
  // The list of all materialized rows after Exec() has been called; empty before that.
  std::vector<TResultRow> result_set_;

  // Unique Id of the medata operation. Only available if using Exec(TMetadataOpRequest)
  TUniqueId request_id_;

  // Schema of result_set_. Only available if using Exec(TMetadataOpRequest)
  TResultSetMetadata result_set_metadata_;

  // Used to execute catalog queries to the Frontend via JNI. Not owned here.
  ImpalaServer* impala_server_;
};

}

#endif
