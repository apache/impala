// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_TESTUTIL_IMPALAD_QUERY_EXECUTOR_H
#define IMPALA_TESTUTIL_IMPALAD_QUERY_EXECUTOR_H

#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include "testutil/query-executor-if.h"
#include "util/thrift-client.h"
#include "common/status.h"
#include "exec/exec-stats.h"
#include "runtime/primitive-type.h"
#include "runtime/runtime-state.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaService_types.h"

namespace impala {

// Query execution against running impalad process.
class ImpaladQueryExecutor : public QueryExecutorIf {
 public:
  ImpaladQueryExecutor();
  ~ImpaladQueryExecutor();

  virtual Status Setup();

  // Start running query. Call this prior to FetchResult().
  // If 'col_types' is non-NULL, returns the types of the select list items.
  virtual Status Exec(
      const std::string& query_string, std::vector<PrimitiveType>* col_types);

  // Return the explain plan for the query
  virtual Status Explain(const std::string& query_string, std::string* explain_plan);

  // Returns result batch in 'batch'. The returned rows are the output rows of
  // the execution tree. In other words, they do *not* reflect the query's
  // select list exprs, ie, don't call this if the query
  // doesn't have a FROM clause, this function will not return any result rows for
  // that case.
  // Sets 'batch' to NULL if no more data. Batch is owned by ImpaladQueryExecutor
  // and must not be deallocated.
  virtual Status FetchResult(RowBatch** batch);

  // Return single row as comma-separated list of values.
  // Indicates end-of-stream by setting 'row' to the empty string.
  // Returns OK if successful, otherwise error.
  virtual Status FetchResult(std::string* row);

  // Return single row as vector of raw values.
  // Indicates end-of-stream by returning empty 'row'.
  // Returns OK if successful, otherwise error.
  virtual Status FetchResult(std::vector<void*>* row);

  // Returns the error log lines in executor_'s runtime state as a string joined
  // with '\n'.
  virtual std::string ErrorString() const;

  // Returns a string representation of the file_errors_.
  virtual std::string FileErrors() const;

  // Returns the counters for the entire query
  virtual RuntimeProfile* query_profile();

  virtual bool eos() { return eos_; }

  virtual ExecStats* exec_stats() { return &exec_stats_; }

  void setExecOptions(const std::vector<std::string>& exec_options) {
    exec_options_ = exec_options;
  }

 private:
  // fe service-related
  boost::scoped_ptr<ThriftClient<ImpalaServiceClient, IMPALA_SERVER> > client_;

  // Execution options
  std::vector<std::string> exec_options_;

  // Beeswax query handle and result
  beeswax::QueryHandle query_handle_;
  beeswax::Results query_results_;
  beeswax::QueryExplanation query_explanation_;

  bool query_in_progress_;
  int current_row_;
  bool eos_;
  ExecStats exec_stats_;  // not set right now

  // call beeswax.close() for current query, if one in progress
  Status Close();
};

}

#endif
