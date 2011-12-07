// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_TESTUTIL_QUERY_EXECUTOR_H
#define IMPALA_TESTUTIL_QUERY_EXECUTOR_H

#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include "common/status.h"
#include "runtime/primitive-type.h"
#include "runtime/runtime-state.h"

namespace apache { namespace thrift { namespace transport { class TTransport; } } }
namespace apache { namespace thrift { namespace protocol { class TProtocol; } } }

namespace impala {

class Expr;
class ImpalaPlanServiceClient;
class ObjectPool;
class PlanExecutor;
class RowBatch;
class RowDescriptor;
class TRowBatch;

class QueryExecutor {
 public:
  QueryExecutor();
  ~QueryExecutor();

  Status Setup();

  // Start running query. Call this prior to FetchResult().
  // If 'col_types' is non-NULL, returns the types of the select list items.
  Status Exec(
      const std::string& query, std::vector<PrimitiveType>* col_types);

  // Return single row as comma-separated list of values.
  // Indicates end-of-stream by setting 'row' to the empty string.
  // Returns OK if successful, otherwise error.
  Status FetchResult(std::string* row);

  // Return result batch (which reflects plan obtained from query; it does
  // *not* reflect the query's select list exprs) in 'batch'; set 'batch' to
  // NULL if no more data. Batch is owned by QueryExecutor and must not be deallocated.
  Status FetchResult(RowBatch** batch);

  // Return single row as vector of raw values.
  // Indicates end-of-stream by returning empty 'row'.
  // Returns OK if successful, otherwise error.
  Status FetchResult(std::vector<void*>* row);

  RuntimeState* runtime_state();

  const RowDescriptor& row_desc() const { return *row_desc_; }

  // Returns the error log lines in executor_'s runtime state as a string joined with '\n'.
  std::string ErrorString() const;

  // Returns a string representation of the file_errors_.
  std::string FileErrors() const;

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
  boost::scoped_ptr<TRowBatch> thrift_row_batch_;
  const RowDescriptor* row_desc_;  // owned by plan root, which resides in pool_
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
