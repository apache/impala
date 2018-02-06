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

#ifndef IMPALA_SERVICE_CLIENT_REQUEST_STATE_H
#define IMPALA_SERVICE_CLIENT_REQUEST_STATE_H

#include "common/status.h"
#include "exec/catalog-op-executor.h"
#include "runtime/timestamp-value.h"
#include "scheduling/query-schedule.h"
#include "service/child-query.h"
#include "service/impala-server.h"
#include "util/auth-util.h"
#include "util/condition-variable.h"
#include "util/runtime-profile.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Frontend_types.h"

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
class ClientRequestStateCleaner;

/// Execution state of the client-facing side a query. This captures everything
/// necessary to convert row batches received by the coordinator into results
/// we can return to the client. It also captures all state required for
/// servicing query-related requests from the client.
/// Thread safety: this class is generally not thread-safe, callers need to
/// synchronize access explicitly via lock(). See the ImpalaServer class comment for
/// the required lock acquisition order.
///
/// TODO: Compute stats is the only stmt that requires child queries. Once the
/// CatalogService performs background stats gathering the concept of child queries
/// will likely become obsolete. Remove all child-query related code from this class.
class ClientRequestState {
 public:
  ClientRequestState(const TQueryCtx& query_ctx, ExecEnv* exec_env, Frontend* frontend,
      ImpalaServer* server, std::shared_ptr<ImpalaServer::SessionState> session);

  ~ClientRequestState();

  /// Initiates execution of a exec_request.
  /// Non-blocking.
  /// Must *not* be called with lock_ held.
  Status Exec(TExecRequest* exec_request) WARN_UNUSED_RESULT;

  /// Execute a HiveServer2 metadata operation
  /// TODO: This is likely a superset of GetTableNames/GetDbs. Coalesce these different
  /// code paths.
  Status Exec(const TMetadataOpRequest& exec_request) WARN_UNUSED_RESULT;

  /// Call this to ensure that rows are ready when calling FetchRows(). Updates the
  /// query_status_, and advances query_state_ to FINISHED or EXCEPTION. Must be preceded
  /// by call to Exec(). Waits for all child queries to complete. Takes lock_.
  void Wait();

  /// Calls Wait() asynchronously in a thread and returns immediately.
  Status WaitAsync();

  /// BlockOnWait() may be called after WaitAsync() has been called in order to wait
  /// for the asynchronous thread (wait_thread_) to complete. It is safe to call this
  /// from multiple threads (all threads will block until wait_thread_ has completed)
  /// and multiple times (non-blocking once wait_thread_ has completed). Do not call
  /// while holding lock_.
  void BlockOnWait();

  /// Return at most max_rows from the current batch. If the entire current batch has
  /// been returned, fetch another batch first.
  /// Caller needs to hold fetch_rows_lock_ and lock_.
  /// Caller should verify that EOS has not be reached before calling.
  /// Must be preceeded by call to Wait() (or WaitAsync()/BlockOnWait()).
  /// Also updates query_state_/status_ in case of error.
  Status FetchRows(const int32_t max_rows, QueryResultSet* fetched_rows)
      WARN_UNUSED_RESULT;

  /// Resets the state of this query such that the next fetch() returns results from the
  /// beginning of the query result set (by using the using result_cache_).
  /// It is valid to call this function for any type of statement that returns a result
  /// set, including queries, show stmts, compute stats, etc.
  /// Returns a recoverable error status if the restart is not possible, ok() otherwise.
  /// The error is recoverable to allow clients to resume fetching.
  /// The caller must hold fetch_rows_lock_ and lock_.
  Status RestartFetch() WARN_UNUSED_RESULT;

  /// Update query state if the requested state isn't already obsolete. This is only for
  /// non-error states - if the query encounters an error the query status needs to be set
  /// with information about the error so UpdateQueryStatus must be used instead.
  /// Takes lock_.
  void UpdateNonErrorQueryState(beeswax::QueryState::type query_state);

  /// Update the query status and the "Query Status" summary profile string.
  /// If current status is already != ok, no update is made (we preserve the first error)
  /// If called with a non-ok argument, the expectation is that the query will be aborted
  /// quickly.
  /// Returns the status argument (so we can write
  /// RETURN_IF_ERROR(UpdateQueryStatus(SomeOperation())).
  /// Does not take lock_, but requires it: caller must ensure lock_
  /// is taken before calling UpdateQueryStatus
  Status UpdateQueryStatus(const Status& status) WARN_UNUSED_RESULT;

  /// Cancels the child queries and the coordinator with the given cause.
  /// If cause is NULL, assume this was deliberately cancelled by the user.
  /// Otherwise, sets state to EXCEPTION.
  /// Does nothing if the query has reached EOS or already cancelled.
  ///
  /// Only returns an error if 'check_inflight' is true and the query is not yet
  /// in-flight. Otherwise, proceed and return Status::OK() even if the query isn't
  /// in-flight (for cleaning up after an error on the query issuing path).
  Status Cancel(bool check_inflight, const Status* cause) WARN_UNUSED_RESULT;

  /// This is called when the query is done (finished, cancelled, or failed).
  /// Takes lock_: callers must not hold lock() before calling.
  void Done();

  /// Sets the API-specific (Beeswax, HS2) result cache and its size bound.
  /// The given cache is owned by this query exec state, even if an error is returned.
  /// Returns a non-ok status if max_size exceeds the per-impalad allowed maximum.
  Status SetResultCache(QueryResultSet* cache, int64_t max_size) WARN_UNUSED_RESULT;

  ImpalaServer::SessionState* session() const { return session_.get(); }

  /// Queries are run and authorized on behalf of the effective_user.
  const std::string& effective_user() const {
      return GetEffectiveUser(query_ctx_.session);
  }
  const std::string& connected_user() const { return query_ctx_.session.connected_user; }
  bool user_has_profile_access() const { return user_has_profile_access_; }
  const std::string& do_as_user() const { return query_ctx_.session.delegated_user; }
  TSessionType::type session_type() const { return query_ctx_.session.session_type; }
  const TUniqueId& session_id() const { return query_ctx_.session.session_id; }
  const std::string& default_db() const { return query_ctx_.session.database; }
  bool eos() const { return eos_; }
  Coordinator* coord() const { return coord_.get(); }
  QuerySchedule* schedule() { return schedule_.get(); }

  /// Resource pool associated with this query, or an empty string if the schedule has not
  /// been created and had the pool set yet, or this StmtType doesn't go through admission
  /// control.
  std::string request_pool() const {
    return schedule_ == nullptr ? "" : schedule_->request_pool();
  }
  int num_rows_fetched() const { return num_rows_fetched_; }
  void set_fetched_rows() { fetched_rows_ = true; }
  bool fetched_rows() const { return fetched_rows_; }
  bool returns_result_set() { return !result_metadata_.columns.empty(); }
  const TResultSetMetadata* result_metadata() { return &result_metadata_; }
  const TUniqueId& query_id() const { return query_ctx_.query_id; }
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
  beeswax::QueryState::type query_state() const { return query_state_; }
  const Status& query_status() const { return query_status_; }
  void set_result_metadata(const TResultSetMetadata& md) { result_metadata_ = md; }
  void set_user_profile_access(bool user_has_profile_access) {
    user_has_profile_access_ = user_has_profile_access;
  }
  const RuntimeProfile* profile() const { return profile_; }
  const RuntimeProfile* summary_profile() const { return summary_profile_; }
  int64_t start_time_us() const { return start_time_us_; }
  int64_t end_time_us() const { return end_time_us_; }
  const std::string& sql_stmt() const { return query_ctx_.client_request.stmt; }
  const TQueryOptions& query_options() const {
    return query_ctx_.client_request.query_options;
  }
  /// Returns 0:0 if this is a root query
  TUniqueId parent_query_id() const { return query_ctx_.parent_query_id; }

  const std::vector<std::string>& GetAnalysisWarnings() const {
    return exec_request_.analysis_warnings;
  }

  inline int64_t last_active_ms() const {
    boost::lock_guard<boost::mutex> l(expiration_data_lock_);
    return last_active_time_ms_;
  }

  /// Returns true if Impala is actively processing this query.
  inline bool is_active() const {
    boost::lock_guard<boost::mutex> l(expiration_data_lock_);
    return ref_count_ > 0;
  }

  bool is_expired() const {
    boost::lock_guard<boost::mutex> l(expiration_data_lock_);
    return is_expired_;
  }

  void set_expired() {
    boost::lock_guard<boost::mutex> l(expiration_data_lock_);
    is_expired_ = true;
  }

  RuntimeProfile::EventSequence* query_events() const { return query_events_; }
  RuntimeProfile* summary_profile() { return summary_profile_; }

 private:
  const TQueryCtx query_ctx_;

  /// Ensures single-threaded execution of FetchRows(). Callers of FetchRows() are
  /// responsible for acquiring this lock. To avoid deadlocks, callers must not hold lock_
  /// while acquiring this lock (since FetchRows() will release and re-acquire lock_ during
  /// its execution).
  /// See "Locking" in the class comment for lock acquisition order.
  boost::mutex fetch_rows_lock_;

  /// Protects last_active_time_ms_, ref_count_ and is_expired_. Only held during short
  /// function calls - no other locks should be acquired while holding this lock.
  mutable boost::mutex expiration_data_lock_;

  /// Stores the last time that the query was actively doing work, in Unix milliseconds.
  int64_t last_active_time_ms_;

  /// ref_count_ > 0 if Impala is currently performing work on this query's behalf. Every
  /// time a client instructs Impala to do work on behalf of this query, the ref count is
  /// increased, and decreased once that work is completed.
  uint32_t ref_count_ = 0;

  /// True if the query expired by timing out.
  bool is_expired_ = false;

  /// Executor for any child queries (e.g. compute stats subqueries). Always non-NULL.
  const boost::scoped_ptr<ChildQueryExecutor> child_query_executor_;

  /// Protects all following fields. Acquirers should be careful not to hold it for too
  /// long, e.g. during RPCs because this lock is required to make progress on various
  /// ImpalaServer requests. If held for too long it can block progress of client
  /// requests for this query, e.g. query status and cancellation. Furthermore, until
  /// IMPALA-3882 is fixed, it can indirectly block progress on all other queries.
  /// See "Locking" in the class comment for lock acquisition order.
  boost::mutex lock_;

  /// TODO: remove and use ExecEnv::GetInstance() instead
  ExecEnv* exec_env_;

  /// Thread for asynchronously running Wait().
  std::unique_ptr<Thread> wait_thread_;

  /// Condition variable to make BlockOnWait() thread-safe. One thread joins
  /// wait_thread_, and all other threads block on this cv. Used with lock_.
  ConditionVariable block_on_wait_cv_;

  /// Used in conjunction with block_on_wait_cv_ to make BlockOnWait() thread-safe.
  bool is_block_on_wait_joining_ = false;

  /// Session that this query is from
  std::shared_ptr<ImpalaServer::SessionState> session_;

  /// Resource assignment determined by scheduler. Owned by obj_pool_.
  boost::scoped_ptr<QuerySchedule> schedule_;

  /// Not set for ddl queries.
  boost::scoped_ptr<Coordinator> coord_;

  /// Runs statements that query or modify the catalog via the CatalogService.
  boost::scoped_ptr<CatalogOpExecutor> catalog_op_executor_;

  /// Result set used for requests that return results and are not QUERY
  /// statements. For example, EXPLAIN, LOAD, and SHOW use this.
  boost::scoped_ptr<std::vector<TResultRow>> request_result_set_;

  /// Cache of the first result_cache_max_size_ query results to allow clients to restart
  /// fetching from the beginning of the result set. This cache is appended to in
  /// FetchInternal(), and set to NULL if its bound is exceeded. If the bound is exceeded,
  /// then clients cannot restart fetching because some results have been lost since the
  /// last fetch. Only set if result_cache_max_size_ > 0.
  boost::scoped_ptr<QueryResultSet> result_cache_;

  /// Max size of the result_cache_ in number of rows. A value <= 0 means no caching.
  int64_t result_cache_max_size_ = -1;

  ObjectPool profile_pool_;

  /// The ClientRequestState builds three separate profiles.
  /// * profile_ is the top-level profile which houses the other
  ///   profiles, plus the query timeline
  /// * summary_profile_ contains mostly static information about the
  ///   query, including the query statement, the plan and the user who submitted it.
  /// * server_profile_ tracks time spent inside the ImpalaServer,
  ///   but not inside fragment execution, i.e. the time taken to
  ///   register and set-up the query and for rows to be fetched.
  ///
  /// There's a fourth profile which is not built here (but is a
  /// child of profile_); the execution profile which tracks the
  /// actual fragment execution.
  ///
  /// Redaction: Only the following info strings in the profile are redacted as they
  /// are expected to contain sensitive information like schema/column references etc.
  /// Other fields are left unredacted.
  /// - Query Statement
  /// - Query Plan
  /// - Query Status
  /// - Error logs
  RuntimeProfile* const profile_;
  RuntimeProfile* const server_profile_;
  RuntimeProfile* const summary_profile_;
  RuntimeProfile::Counter* row_materialization_timer_;

  /// Tracks how long we are idle waiting for a client to fetch rows.
  RuntimeProfile::Counter* client_wait_timer_;
  /// Timer to track idle time for the above counter.
  MonotonicStopWatch client_wait_sw_;

  RuntimeProfile::EventSequence* query_events_;

  bool is_cancelled_ = false; // if true, Cancel() was called.
  bool eos_ = false;  // if true, there are no more rows to return
  /// We enforce the invariant that query_status_ is not OK iff query_state_ is EXCEPTION,
  /// given that lock_ is held. query_state_ should only be updated using
  /// UpdateQueryState(), to ensure that the query profile is also updated.
  beeswax::QueryState::type query_state_ = beeswax::QueryState::CREATED;
  Status query_status_;
  TExecRequest exec_request_;
  /// If true, effective_user() has access to the runtime profile and execution
  /// summary.
  bool user_has_profile_access_ = true;
  TResultSetMetadata result_metadata_; // metadata for select query
  RowBatch* current_batch_ = nullptr; // the current row batch; only applicable if coord is set
  int current_batch_row_ = 0 ; // number of rows fetched within the current batch
  int num_rows_fetched_ = 0; // number of rows fetched by client for the entire query

  /// True if a fetch was attempted by a client, regardless of whether a result set
  /// (or error) was returned to the client.
  bool fetched_rows_ = false;

  /// To get access to UpdateCatalog, LOAD, and DDL methods. Not owned.
  Frontend* frontend_;

  /// The parent ImpalaServer; called to wait until the the impalad has processed a
  /// catalog update request. Not owned.
  ImpalaServer* parent_server_;

  /// Start/end time of the query, in Unix microseconds.
  /// end_time_us_ is initialized to 0, which is used to indicate that the query is not
  /// yet done. It is assinged the final value in
  /// ClientRequestState::Done().
  int64_t start_time_us_, end_time_us_ = 0;

  /// Executes a local catalog operation (an operation that does not need to execute
  /// against the catalog service). Includes USE, SHOW, DESCRIBE, and EXPLAIN statements.
  Status ExecLocalCatalogOp(const TCatalogOpRequest& catalog_op) WARN_UNUSED_RESULT;

  /// Updates last_active_time_ms_ and ref_count_ to reflect that query is currently not
  /// doing any work. Takes expiration_data_lock_.
  void MarkInactive();

  /// Updates last_active_time_ms_ and ref_count_ to reflect that query is currently being
  /// actively processed. Takes expiration_data_lock_.
  void MarkActive();

  /// Core logic of initiating a query or dml execution request.
  /// Initiates execution of plan fragments, if there are any, and sets
  /// up the output exprs for subsequent calls to FetchRows().
  /// 'coord_' is only valid after this method is called, and may be invalid if it
  /// returns an error.
  /// Also sets up profile and pre-execution counters.
  /// Non-blocking.
  Status ExecQueryOrDmlRequest(const TQueryExecRequest& query_exec_request)
      WARN_UNUSED_RESULT;

  /// Core logic of executing a ddl statement. May internally initiate execution of
  /// queries (e.g., compute stats) or dml (e.g., create table as select)
  Status ExecDdlRequest() WARN_UNUSED_RESULT;

  /// Executes a LOAD DATA
  Status ExecLoadDataRequest() WARN_UNUSED_RESULT;

  /// Core logic of Wait(). Does not update query_state_/status_.
  Status WaitInternal() WARN_UNUSED_RESULT;

  /// Core logic of FetchRows(). Does not update query_state_/status_.
  /// Caller needs to hold fetch_rows_lock_ and lock_.
  Status FetchRowsInternal(const int32_t max_rows, QueryResultSet* fetched_rows)
      WARN_UNUSED_RESULT;

  /// Gather and publish all required updates to the metastore
  Status UpdateCatalog() WARN_UNUSED_RESULT;

  /// Copies results into request_result_set_
  /// TODO: Have the FE return list<Data.TResultRow> so that this isn't necessary
  void SetResultSet(const TDdlExecResponse* ddl_resp);
  void SetResultSet(const std::vector<std::string>& results);
  void SetResultSet(const std::vector<std::string>& col1,
      const std::vector<std::string>& col2);
  void SetResultSet(const vector<string>& col1,
      const vector<string>& col2, const vector<string>& col3);
  void SetResultSet(const std::vector<std::string>& col1,
      const std::vector<std::string>& col2, const std::vector<std::string>& col3,
      const std::vector<std::string>& col4);

  /// Sets the result set for a CREATE TABLE AS SELECT statement. The results will not be
  /// ready until all BEs complete execution. This can be called as part of Wait(),
  /// at which point results will be avilable.
  void SetCreateTableAsSelectResultSet();

  /// Updates the metastore's table and column statistics based on the child-query results
  /// of a compute stats command.
  /// TODO: Unify the various ways that the Metastore is updated for DDL/DML.
  /// For example, INSERT queries update partition metadata in UpdateCatalog() using a
  /// TUpdateCatalogRequest, whereas our DDL uses a TCatalogOpRequest for very similar
  /// purposes. Perhaps INSERT should use a TCatalogOpRequest as well.
  Status UpdateTableAndColumnStats(const std::vector<ChildQuery*>& child_queries)
      WARN_UNUSED_RESULT;

  /// Sets result_cache_ to NULL and updates its associated metrics and mem consumption.
  /// This function is a no-op if the cache has already been cleared.
  void ClearResultCache();

  /// Update the query state and the "Query State" summary profile string.
  /// Does not take lock_, but requires it: caller must ensure lock_
  /// is taken before calling UpdateQueryState.
  void UpdateQueryState(beeswax::QueryState::type query_state);

  /// Gets the query options, their values and levels and populates the result set
  /// with them. It covers the subset of options for 'SET' and all of them for
  /// 'SET ALL'
  void PopulateResultForSet(bool is_set_all);
};

}
#endif
