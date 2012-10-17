// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_TESTUTIL_QUERY_EXECUTOR_IF_H
#define IMPALA_TESTUTIL_QUERY_EXECUTOR_IF_H

#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include "common/status.h"
#include "runtime/primitive-type.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile.h"

namespace impala {

class ExecStats;
class RowBatch;

// Query execution interface for tests.
// Results are returned either as the row batches (FetchResult(RowBatch**)) or as
// individual rows (FetchResult(string*)/FetchResult(vector<void*>*)).
// Do *not* mix calls to the two types of FetchResult().
class QueryExecutorIf {
 public:
  QueryExecutorIf();
  virtual ~QueryExecutorIf();

  virtual Status Setup() = 0;

  // Start running query. Call this prior to FetchResult().
  // If 'col_types' is non-NULL, returns the types of the select list items.
  virtual Status Exec(const std::string& query,
      std::vector<Apache::Hadoop::Hive::FieldSchema>* col_types) = 0;

  // Return the explain plan for the query
  virtual Status Explain(const std::string& query, std::string* explain_plan) = 0;

  // Returns result batch in 'batch'. The returned rows are the output rows of
  // the execution tree. In other words, they do *not* reflect the query's
  // select list exprs, ie, don't call this if the query
  // doesn't have a FROM clause, this function will not return any result rows for
  // that case.
  // Sets 'batch' to NULL if no more data. Batch is owned by QueryExecutor
  // and must not be deallocated.
  virtual Status FetchResult(RowBatch** batch) = 0;

  // Return single row as comma-separated list of values.
  // Indicates end-of-stream by setting 'row' to the empty string.
  // Returns OK if successful, otherwise error.
  virtual Status FetchResult(std::string* row) = 0;

  // Return single row as vector of raw values.
  // Indicates end-of-stream by returning empty 'row'.
  // Returns OK if successful, otherwise error.
  virtual Status FetchResult(std::vector<void*>* row) = 0;

  // Returns the error log lines in executor_'s runtime state as a string joined
  // with '\n'.
  virtual std::string ErrorString() const = 0;

  // Returns a string representation of the file_errors_.
  virtual std::string FileErrors() const = 0;

  // Returns the counters for the entire query
  virtual RuntimeProfile* query_profile() = 0;

  // Return true if no more rows are being returned.
  virtual bool eos() = 0;

  // Return ExecState for executed stmt.
  virtual ExecStats* exec_stats() = 0;
};

}

#endif
