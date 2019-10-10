// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_TESTUTIL_IMPALAD_QUERY_EXECUTOR_H
#define IMPALA_TESTUTIL_IMPALAD_QUERY_EXECUTOR_H

#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "rpc/thrift-client.h"
#include "common/status.h"
#include "runtime/runtime-state.h"
#include "runtime/types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaService_types.h"

namespace impala {

class RowBatch;

/// Query execution against running impalad process.
class ImpaladQueryExecutor {
 public:
  ImpaladQueryExecutor(const std::string& hostname, uint32_t port);
  ~ImpaladQueryExecutor();

  Status Setup();

  /// Start running query. Call this prior to FetchResult().
  /// If 'col_types' is non-NULL, returns the types of the select list items.
  Status Exec(const std::string& query_string,
      std::vector<Apache::Hadoop::Hive::FieldSchema>* col_types);

  /// Return the explain plan for the query
  Status Explain(const std::string& query_string, std::string* explain_plan);

  /// Returns result batch in 'batch'. The returned rows are the output rows of
  /// the execution tree. In other words, they do *not* reflect the query's
  /// select list exprs, ie, don't call this if the query
  /// doesn't have a FROM clause, this function will not return any result rows for
  /// that case.
  /// Sets 'batch' to NULL if no more data. Batch is owned by ImpaladQueryExecutor
  /// and must not be deallocated.
  Status FetchResult(RowBatch** batch);

  /// Return single row as comma-separated list of values.
  /// Indicates end-of-stream by setting 'row' to the empty string.
  /// Returns OK if successful, otherwise error.
  Status FetchResult(std::string* row);

  /// Return single row as vector of raw values.
  /// Indicates end-of-stream by returning empty 'row'.
  /// Returns OK if successful, otherwise error.
  Status FetchResult(std::vector<void*>* row);

  /// Returns the error log lines in executor_'s runtime state as a string joined
  /// with '\n'.
  std::string ErrorString() const;

  /// Returns a string representation of the file_errors_.
  std::string FileErrors() const;

  /// Returns the counters for the entire query
  RuntimeProfile* query_profile();

  bool eos() { return eos_; }

  /// Add a query option, preserving the existing set of query options.
  void PushExecOption(const std::string& exec_option) {
    exec_options_.push_back(exec_option);
  }

  /// Remove the last query option that was added by pushExecOption().
  void PopExecOption() { exec_options_.pop_back(); }

  /// Remove all query options previously added by pushExecOption().
  void ClearExecOptions() { exec_options_.clear(); }

 private:
  /// fe service-related
  boost::scoped_ptr<ThriftClient<ImpalaServiceClient>> client_;

  /// Execution options
  std::vector<std::string> exec_options_;

  /// Beeswax query handle and result
  beeswax::QueryHandle beeswax_handle_;
  beeswax::Results query_results_;
  beeswax::QueryExplanation query_explanation_;

  bool query_in_progress_;
  int current_row_;
  bool eos_;
  std::string hostname_;
  uint32_t port_;

  /// call beeswax.close() for current query, if one in progress
  Status Close();
};

}

#endif
