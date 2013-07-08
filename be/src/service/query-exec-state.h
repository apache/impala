// Copyright 2013 Cloudera Inc.
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

#ifndef IMPALA_SERVICE_QUERY_EXEC_STATE_H
#define IMPALA_SERVICE_QUERY_EXEC_STATE_H

#include "common/status.h"
#include "exec/catalog-op-executor.h"
#include "util/runtime-profile.h"
#include "runtime/timestamp-value.h"
#include "gen-cpp/Frontend_types.h"
#include "service/impala-server.h"

#include <boost/thread.hpp>
#include <boost/unordered_set.hpp>
#include <vector>

namespace impala {

class ExecEnv;
class Coordinator;
class RuntimeState;
class RowBatch;
class Expr;
class TupleRow;
class Frontend;

// Execution state of a query. This captures everything necessary
// to convert row batches received by the coordinator into results
// we can return to the client. It also captures all state required for
// servicing query-related requests from the client.
// Thread safety: this class is generally not thread-safe, callers need to
// synchronize access explicitly via lock().
// To avoid deadlocks, the caller must *not* acquire query_exec_state_map_lock_
// while holding the exec state's lock.
// TODO: Consider renaming to RequestExecState for consistency.
class ImpalaServer::QueryExecState {
 public:
  QueryExecState(ExecEnv* exec_env, Frontend* frontend,
                 ImpalaServer* server,
                 boost::shared_ptr<ImpalaServer::SessionState> session,
                 const TSessionState& query_session_state,
                 const std::string& sql_stmt);

  ~QueryExecState() {
  }

  // Initiates execution of a exec_request.
  // Non-blocking.
  // Must *not* be called with lock_ held.
  Status Exec(TExecRequest* exec_request);

  // Execute a HiveServer2 metadata operation
  // TODO: This is likely a superset of GetTableNames/GetDbNames. Coalesce these different
  // code paths.
  Status Exec(const TMetadataOpRequest& exec_request);

  // Call this to ensure that rows are ready when calling FetchRows().
  // Must be preceded by call to Exec().
  Status Wait();

  // Return at most max_rows from the current batch. If the entire current batch has
  // been returned, fetch another batch first.
  // Caller needs to hold fetch_rows_lock_ and lock_.
  // Caller should verify that EOS has not be reached before calling.
  // Always calls coord()->Wait() prior to getting a batch.
  // Also updates query_state_/status_ in case of error.
  Status FetchRows(const int32_t max_rows, QueryResultSet* fetched_rows);

  // Update query state if the requested state isn't already obsolete.
  // Takes lock_.
  void UpdateQueryState(beeswax::QueryState::type query_state);

  // Update the query status and the "Query Status" summary profile string.
  // If current status is already != ok, no update is made (we preserve the first error)
  // If called with a non-ok argument, the expectation is that the query will be aborted
  // quickly.
  // Returns the status argument (so we can write
  // RETURN_IF_ERROR(UpdateQueryStatus(SomeOperation())).
  // Does not take lock_, but requires it: caller must ensure lock_
  // is taken before calling UpdateQueryStatus
  Status UpdateQueryStatus(const Status& status);

  // Sets state to EXCEPTION and cancels coordinator with the given cause.
  // Caller needs to hold lock_.
  // Does nothing if the query has reached EOS.
  void Cancel(const Status* cause = NULL);

  // This is called when the query is done (finished, cancelled, or failed).
  // Takes lock_: callers must not hold lock() before calling.
  void Done();

  ImpalaServer::SessionState* parent_session() const { return parent_session_.get(); }
  const std::string& user() const { return parent_session_->user; }
  TSessionType::type session_type() const { return query_session_state_.session_type; }
  const TUniqueId& session_id() const { return query_session_state_.session_id; }
  const std::string& default_db() const { return query_session_state_.database; }
  bool eos() const { return eos_; }
  Coordinator* coord() const { return coord_.get(); }
  int num_rows_fetched() const { return num_rows_fetched_; }
  bool returns_result_set() { return !result_metadata_.columnDescs.empty(); }
  const TResultSetMetadata* result_metadata() { return &result_metadata_; }
  const TUniqueId& query_id() const { return query_id_; }
  const TExecRequest& exec_request() const { return exec_request_; }
  TStmtType::type stmt_type() const { return exec_request_.stmt_type; }
  TCatalogOpType::type catalog_op_type() const {
    return exec_request_.catalog_op_request.op_type;
  }
  TDdlType::type ddl_type() const {
    return exec_request_.catalog_op_request.ddl_params.ddl_type;
  }
  boost::mutex* lock() { return &lock_; }
  boost::mutex* fetch_rows_lock() { return &fetch_rows_lock_; }
  const beeswax::QueryState::type query_state() const { return query_state_; }
  void set_query_state(beeswax::QueryState::type state) { query_state_ = state; }
  const Status& query_status() const { return query_status_; }
  void set_result_metadata(const TResultSetMetadata& md) { result_metadata_ = md; }
  const RuntimeProfile& profile() const { return profile_; }
  const TimestampValue& start_time() const { return start_time_; }
  const TimestampValue& end_time() const { return end_time_; }
  const std::string& sql_stmt() const { return sql_stmt_; }

  RuntimeProfile::EventSequence* query_events() { return query_events_; }

 private:
  TUniqueId query_id_;
  const std::string sql_stmt_;

  // Ensures single-threaded execution of FetchRows(). Callers of FetchRows() are
  // responsible for acquiring this lock. To avoid deadlocks, callers must not hold lock_
  // while acquiring this lock (since FetchRows() will release and re-acquire lock_ during
  // its execution).
  boost::mutex fetch_rows_lock_;

  boost::mutex lock_;  // protects all following fields
  ExecEnv* exec_env_;

  // Session that this query is from
  boost::shared_ptr<SessionState> parent_session_;

  // Snapshot of state in session_ that is not constant (and can change from
  // QueryExecState to QueryExecState).
  const TSessionState query_session_state_;

  // not set for ddl queries, or queries with "limit 0"
  boost::scoped_ptr<Coordinator> coord_;

 // Runs statements that query or modify the catalog via the CatalogService.
 boost::scoped_ptr<CatalogOpExecutor> catalog_op_executor_;

  // Result set used for requests that return results and are not QUERY
  // statements. For example, EXPLAIN, LOAD, and SHOW use this.
  boost::scoped_ptr<std::vector<TResultRow> > request_result_set_;

  // local runtime_state_ in case we don't have a coord_
  boost::scoped_ptr<RuntimeState> local_runtime_state_;
  ObjectPool profile_pool_;

  // The QueryExecState builds three separate profiles.
  // * profile_ is the top-level profile which houses the other
  //   profiles, plus the query timeline
  // * summary_profile_ contains mostly static information about the
  //   query, including the query statement, the plan and the user who submitted it.
  // * server_profile_ tracks time spent inside the ImpalaServer,
  //   but not inside fragment execution, i.e. the time taken to
  //   register and set-up the query and for rows to be fetched.
  //
  // There's a fourth profile which is not built here (but is a
  // child of profile_); the execution profile which tracks the
  // actual fragment execution.
  RuntimeProfile profile_;
  RuntimeProfile server_profile_;
  RuntimeProfile summary_profile_;
  RuntimeProfile::Counter* row_materialization_timer_;

  // Tracks how long we are idle waiting for a client to fetch rows.
  RuntimeProfile::Counter* client_wait_timer_;
  // Timer to track idle time for the above counter.
  MonotonicStopWatch client_wait_sw_;

  RuntimeProfile::EventSequence* query_events_;
  std::vector<Expr*> output_exprs_;
  bool eos_;  // if true, there are no more rows to return
  beeswax::QueryState::type query_state_;
  Status query_status_;
  TExecRequest exec_request_;

  TResultSetMetadata result_metadata_; // metadata for select query
  RowBatch* current_batch_; // the current row batch; only applicable if coord is set
  int current_batch_row_; // number of rows fetched within the current batch
  int num_rows_fetched_; // number of rows fetched by client for the entire query

  // To get access to UpdateMetastore, LOAD, and DDL methods. Not owned.
  Frontend* frontend_;

  // The parent ImpalaServer; called to wait until the the impalad has processed a
  // catalog update request. Not owned.
  ImpalaServer* parent_server_;

  // Start/end time of the query
  TimestampValue start_time_, end_time_;

  // Executes a local catalog operation (an operation that does not need to execute
  // against the catalog service). Includes USE, SHOW, DESCRIBE, and EXPLAIN statements.
  Status ExecLocalCatalogOp(const TCatalogOpRequest& catalog_op);

  // Core logic of initiating a query or dml execution request.
  // Initiates execution of plan fragments, if there are any, and sets
  // up the output exprs for subsequent calls to FetchRows().
  // Also sets up profile and pre-execution counters.
  // Non-blocking.
  Status ExecQueryOrDmlRequest(const TQueryExecRequest& query_exec_request);

  // Executes a LOAD DATA
  Status ExecLoadDataRequest();

  // Core logic of FetchRows(). Does not update query_state_/status_.
  // Caller needs to hold fetch_rows_lock_ and lock_.
  Status FetchRowsInternal(const int32_t max_rows, QueryResultSet* fetched_rows);

  // Fetch the next row batch and store the results in current_batch_. Only called for
  // non-DDL / DML queries. current_batch_ is set to NULL if execution is complete or the
  // query was cancelled.
  // Caller needs to hold fetch_rows_lock_ and lock_. Blocks, during which time lock_ is
  // released.
  Status FetchNextBatch();

  // Evaluates 'output_exprs_' against 'row' and output the evaluated row in
  // 'result'. The values' scales (# of digits after decimal) are stored in 'scales'.
  // result and scales must have been resized to the number of columns before call.
  Status GetRowValue(TupleRow* row, std::vector<void*>* result, std::vector<int>* scales);

  // Gather and publish all required updates to the metastore
  Status UpdateMetastore();

  // Copies results into request_result_set_
  void SetResultSet(const std::vector<std::string>& results);

  // Sets the result set for a CREATE TABLE AS SELECT statement. The results will not be
  // ready until all BEs complete execution. This can be called as part of Wait(),
  // at which point results will be avilable.
  void SetCreateTableAsSelectResultSet();
};

}
#endif
