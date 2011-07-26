// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_TESTUTIL_QUERY_EXECUTOR_H
#define IMPALA_TESTUTIL_QUERY_EXECUTOR_H

#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include "common/status.h"
#include "runtime/runtime-state.h"

namespace apache { namespace thrift { namespace transport { class TTransport; } } }
namespace apache { namespace thrift { namespace protocol { class TProtocol; } } }

namespace impala {

class Expr;
class ImpalaPlanServiceClient;
class ObjectPool;
class PlanExecutor;
class RowBatch;

class QueryExecutor {
 public:
  QueryExecutor();
  ~QueryExecutor();

  Status Setup();

  // Start running query. Call this prior to FetchResult().
  Status Exec(const std::string& query);

  // Return single row as comma-separated list of values.
  // Indicates end-of-stream by setting 'row' to the empty string.
  // Returns OK if successful, otherwise error.
  Status FetchResult(std::string* row);

 private:
  boost::shared_ptr<apache::thrift::transport::TTransport> socket_;
  boost::shared_ptr<apache::thrift::transport::TTransport> transport_;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> protocol_;
  boost::scoped_ptr<ImpalaPlanServiceClient> client_;
  bool started_server_;
  boost::scoped_ptr<ObjectPool> pool_;
  boost::scoped_ptr<PlanExecutor> executor_;
  std::vector<Expr*> select_list_exprs_;
  boost::scoped_ptr<RowBatch> row_batch_;
  int next_row_;  // to return from row batch
  int num_rows_;  // total # of rows returned for current query
  bool eos_;  // if true, no more rows for current query

  // End execution of the currently-running query.
  Status EndQuery();

  // Stop plan service, if we started one.
  void Shutdown();
};

}

#endif
