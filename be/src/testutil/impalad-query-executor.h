// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_TESTUTIL_IMPALAD_QUERY_EXECUTOR_H
#define IMPALA_TESTUTIL_IMPALAD_QUERY_EXECUTOR_H

#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include "testutil/query-executor-if.h"
#include "common/status.h"
#include "exec/exec-stats.h"
#include "runtime/primitive-type.h"
#include "runtime/runtime-state.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaService_types.h"

namespace apache { namespace thrift { namespace transport { class TTransport; } } }
namespace apache { namespace thrift { namespace protocol { class TProtocol; } } }

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
      const std::string& query, std::vector<PrimitiveType>* col_types);

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

 private:
  // fe service-related
  boost::shared_ptr<apache::thrift::transport::TTransport> socket_;
  boost::shared_ptr<apache::thrift::transport::TTransport> transport_;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> protocol_;
  boost::scoped_ptr<ImpalaServiceClient> client_;

  TUniqueId query_id_;
  TQueryResult query_result_;
  int current_row_;
  bool eos_;
  ExecStats exec_stats_;  // not set right now
};

}

#endif
