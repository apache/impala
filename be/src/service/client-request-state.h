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

#pragma once

#include "common/atomic.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exec/catalog-op-executor.h"
#include "rpc/rpc-trace.h"
#include "service/child-query.h"
#include "service/impala-server.h"
#include "service/query-result-set.h"
#include "runtime/hdfs-fs-cache.h"
#include "util/condition-variable.h"
#include "util/runtime-profile.h"
#include "util/runtime-profile-counters.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/ImpalaHiveServer2Service.h"

#include <boost/unordered_set.hpp>
#include <vector>

namespace impala {

class AdmissionControlClient;
class ClientRequestStateCleaner;
class Coordinator;
class Expr;
class Frontend;
class ReportExecStatusRequestPB;
class RowBatch;
class RuntimeState;
class Thread;
class TRuntimeProfileTree;
class TupleRow;
class QuerySchedulePB;

/// Execution state of the client-facing side of a query. This captures everything
/// necessary to convert row batches received by the coordinator into results
/// we can return to the client. It also captures all state required for
/// servicing query-related requests from the client.
/// Thread safety: this class is generally not thread-safe, callers need to
/// synchronize access explicitly via lock(). See the ImpalaServer class comment for
/// the required lock acquisition order.
///
/// State Machine:
/// ExecState represents all possible states and UpdateNonErrorExecState /
/// UpdateQueryStatus defines all possible state transitions. ExecState starts out in the
/// INITIALIZED state and eventually transition to either the FINISHED or ERROR state. Any
/// state can transition to the ERROR state (if an error is hit or a timeout occurs). The
/// possible state transitions differ for a query depending on its type (e.g. depending on
/// the TStmtType). Successful QUERY / DML queries transition from INITIALIZED to PENDING
/// to RUNNING to FINISHED whereas DDL queries skip the PENDING phase.
///
/// Retry State:
/// The retry state is only used when the query corresponding to this ClientRequestState
/// is retried. Otherwise, the default state is NOT_RETRIED. MarkAsRetrying and
/// MarkAsRetried set the state to RETRYING and RETRIED, respectively. Queries can only
/// transition from NOT_RETRIED to RETRYING and then finally to RETRIED.
///
/// TODO: Compute stats is the only stmt that requires child queries. Once the
/// CatalogService performs background stats gathering the concept of child queries
/// will likely become obsolete. Remove all child-query related code from this class.
class ClientRequestState {
 public:
  ClientRequestState(const TQueryCtx& query_ctx, Frontend* frontend, ImpalaServer* server,
      std::shared_ptr<ImpalaServer::SessionState> session, QueryDriver* query_driver);

  ~ClientRequestState();

  enum class ExecState { INITIALIZED, PENDING, RUNNING, FINISHED, ERROR };

  enum class RetryState { RETRYING, RETRIED, NOT_RETRIED };

  /// Initialize TExecRequest. Prior to this, exec_request() returns a place-holder.
  void SetExecRequest(const TExecRequest* exec_request) {
    DCHECK(exec_request_.Load()->stmt_type == TStmtType::UNKNOWN);
    exec_request_.Store(exec_request);
  }

  /// Sets the profile that is produced by the frontend. The frontend creates the
  /// profile during planning and returns it to the backend via TExecRequest,
  /// which then sets the frontend profile.
  void SetFrontendProfile(const TExecRequest& exec_request);

  /// Sets the coordinator time that the plan request was submitted at so that
  /// the backend timeline starts where the frontend timeline ends
  void SetRemoteSubmitTime(int64_t remote_submit_time);

  /// Based on query type, this either initiates execution of this ClientRequestState's
  /// TExecRequest or submits the query to the Admission controller for asynchronous
  /// admission control. When this returns the operation state is either RUNNING_STATE or
  /// PENDING_STATE.
  /// Non-blocking.
  /// Must *not* be called with lock_ held.
  Status Exec() WARN_UNUSED_RESULT;

  /// Execute a HiveServer2 metadata operation
  /// TODO: This is likely a superset of GetTableNames/GetDbs. Coalesce these different
  /// code paths.
  Status Exec(const TMetadataOpRequest& exec_request) WARN_UNUSED_RESULT;

  /// Call this to ensure that rows are ready when calling FetchRows(). Updates the
  /// query_status_, and advances operation_state_ to FINISHED or EXCEPTION. Must be
  /// preceded by call to Exec(). Waits for all child queries to complete. Takes lock_.
  /// Since this is a blocking operation, it is invoked from a separate thread
  /// (WaitAsync()) and all the callers that intend to block until Wait() completes
  /// are expected to use BlockOnWait().
  ///
  /// Logs the query events once the query reaches the FINISHED state. Threads blocked
  /// on Wait() are signalled *before* logging the events so that they can resume their
  /// execution (like fetching the results) and are not blocked until the event logging
  /// is complete.
  void Wait();

  /// Calls Wait() asynchronously in a thread and returns immediately.
  Status WaitAsync();

  /// BlockOnWait() may be called after WaitAsync() has been called in order to wait
  /// for the asynchronous thread (wait_thread_) to signal block_on_wait_cv_. It is
  /// thread-safe and all the caller threads will block until wait_thread_ has
  /// completed) and multiple times (non-blocking once wait_thread_ has completed).
  /// Do not call while holding lock_. 'timeout' is the amount of time (in microseconds)
  /// that the thread waits for WaitAsync() to complete before returning. If WaitAsync()
  /// completed within the timeout, this method returns true, false otherwise. A value of
  /// 0 causes this method to wait indefinitely. 'block_on_wait_time_us_' is the amount of
  /// time the client spent (in microseconds) waiting in BlockOnWait().
  bool BlockOnWait(int64_t timeout_us, int64_t* block_on_wait_time_us);

  /// Return at most max_rows from the current batch. If the entire current batch has
  /// been returned, fetch another batch first.
  /// Caller needs to hold fetch_rows_lock_ and lock_.
  /// Caller should verify that EOS has not be reached before calling.
  /// Must be preceeded by call to Wait() (or WaitAsync()/BlockOnWait()).
  /// Also updates operation_state_/query_status_ in case of error.
  /// 'block_on_wait_time_us' is the amount of time spent waiting in BlockOnWait(). It
  /// should be 0 if BlockOnWait() was never called.
  Status FetchRows(const int32_t max_rows, QueryResultSet* fetched_rows,
      int64_t block_on_wait_time_us) WARN_UNUSED_RESULT;

  /// Resets the state of this query such that the next fetch() returns results from the
  /// beginning of the query result set (by using the using result_cache_).
  /// It is valid to call this function for any type of statement that returns a result
  /// set, including queries, show stmts, compute stats, etc.
  /// Returns a recoverable error status if the restart is not possible, ok() otherwise.
  /// The error is recoverable to allow clients to resume fetching.
  /// The caller must hold fetch_rows_lock_ and lock_.
  Status RestartFetch() WARN_UNUSED_RESULT;

  /// Update operation state if the requested state isn't already obsolete. This is
  /// only for non-error states (PENDING, RUNNING and FINISHED) - if the query encounters
  /// an error the query status needs to be set with information about the error so
  /// UpdateQueryStatus() must be used instead. If an invalid state transition is
  /// attempted, this method either DCHECKs or skips the state update. Takes lock_.
  void UpdateNonErrorExecState(ExecState exec_state);

  /// Update the query status and the "Query Status" summary profile string.
  /// If current status is already != ok, no update is made (we preserve the first error)
  /// If called with a non-ok argument, the expectation is that the query will be aborted
  /// quickly.
  /// If 'log_error' is true, log the error when query status become non-ok. This is used
  /// when running DDLs in async threads. The async thread should take care of the logging
  /// since the main thread only checks whether the thread creation succeeds.
  /// E.g. ExecDdlRequest() returns the query status in sync mode, but returns the thread
  /// creation status in async mode. So LOG_AND_RETURN_IF_ERROR(ExecDdlRequest()) doesn't
  /// log the query failure in async mode.
  /// Returns the status argument (so we can write
  /// RETURN_IF_ERROR(UpdateQueryStatus(SomeOperation())).
  /// Does not take lock_, but requires it: caller must ensure lock_
  /// is taken before calling UpdateQueryStatus
  Status UpdateQueryStatus(const Status& status, bool log_error=false)
      WARN_UNUSED_RESULT;

  /// Cancels the child queries and the coordinator with the given cause.
  /// If cause is NULL, it assumes this was deliberately cancelled by the user while in
  /// FINISHED state. Otherwise, sets state to ERROR (TODO: IMPALA-1262: use CANCELLED).
  /// Does nothing if the query has reached EOS or already cancelled. If
  /// 'wait_until_finalized' is true and another thread is cancelling 'coord_', block
  /// until cancellaton of 'coord_' finishes and it is finalized.
  /// 'wait_until_finalized' should only used by the single thread finalizing the query,
  /// to avoid many threads piling up waiting for query cancellation.
  void Cancel(const Status* cause, bool wait_until_finalized=false);

  /// This is called when the query is done (finished, cancelled, or failed). This runs
  /// synchronously within the last client RPC and does any work that is required before
  /// the query is finished from the client's point of view, including cancelling the
  /// query with 'cause'. Takes lock_: callers must not hold lock() before calling.
  void Finalize(const Status* cause);

  /// Sets the API-specific (Beeswax, HS2) result cache and its size bound.
  /// The given cache is owned by this client request state, even if an error is returned.
  /// Returns a non-ok status if max_size exceeds the per-impalad allowed maximum.
  Status SetResultCache(QueryResultSet* cache, int64_t max_size) WARN_UNUSED_RESULT;

  /// Wrappers around Coordinator::UpdateBackendExecStatus() and
  /// Coordinator::UpdateFilter() respectively for the coordinator object associated with
  /// 'this' ClientRequestState object. It ensures that these updates are applied to the
  /// coordinator even before it becomes accessible through GetCoordinator(). These
  /// methods should be used instead of calling them directly using the coordinator
  /// object.
  Status UpdateBackendExecStatus(const ReportExecStatusRequestPB& request,
      const TRuntimeProfileForest& thrift_profiles) WARN_UNUSED_RESULT;
  void UpdateFilter(const UpdateFilterParamsPB& params, kudu::rpc::RpcContext* context);

  /// Populate DML stats in 'dml_result' if this request succeeded.
  /// Sets 'query_status' to the overall query status.
  /// Return true if the result was set, otherwise return false.
  /// Caller must not hold 'lock()'.
  bool GetDmlStats(TDmlResult* dml_result, Status* query_status);

  /// Blocks until this query has been retried. Waits until the ExecState has transitioned
  /// to RETRIED (e.g. once MarkAsRetried() has been called). Can only be called if the
  /// current state is either RETRIED or RETRYING. Takes lock_.
  void WaitUntilRetried();

  /// Converts the given ExecState to a string representation.
  static std::string ExecStateToString(ExecState state);

  /// Converts the given RetryState to a string representation.
  static std::string RetryStateToString(RetryState state);

  /// Returns the session for this query.
  std::shared_ptr<ImpalaServer::SessionState> session() const { return session_; }

  /// Queries are run and authorized on behalf of the effective_user.
  const std::string& effective_user() const;
  const std::string& connected_user() const { return query_ctx_.session.connected_user; }
  bool user_has_profile_access() const { return user_has_profile_access_.Load(); }
  const std::string& do_as_user() const { return query_ctx_.session.delegated_user; }
  TSessionType::type session_type() const { return query_ctx_.session.session_type; }
  const TUniqueId& session_id() const { return query_ctx_.session.session_id; }
  const std::string& default_db() const { return query_ctx_.session.database; }
  const std::string& redacted_sql() const {
    return
      query_ctx_.client_request.__isset.redacted_stmt ?
      query_ctx_.client_request.redacted_stmt : query_ctx_.client_request.stmt;
  }
  bool eos() const { return eos_.Load(); }
  const QuerySchedulePB* schedule() const { return schedule_.get(); }

  /// Returns the Coordinator for 'QUERY' and 'DML' requests once Coordinator::Exec()
  /// completes successfully. Otherwise returns null.
  Coordinator* GetCoordinator() const {
    return coord_exec_called_.Load() ? coord_.get() : nullptr;
  }

  /// Resource pool associated with this query, or an empty string if the schedule has not
  /// been created and had the pool set yet, or this StmtType doesn't go through admission
  /// control.
  /// Admission control resource pool associated with this query.
  std::string request_pool() const {
    if (is_planning_done_.load()) {
      const TExecRequest& exec_req = exec_request();
      if (exec_req.query_exec_request.query_ctx.__isset.request_pool) {
        // If the request pool has been set by Planner, return the request pool selected
        // by Planner.
        return exec_req.query_exec_request.query_ctx.request_pool;
      }
    }
    return query_ctx_.__isset.request_pool ? query_ctx_.request_pool : "";
  }

  int num_rows_fetched() const { return num_rows_fetched_; }
  void set_fetched_rows() { fetched_rows_ = true; }
  bool fetched_rows() const { return fetched_rows_; }
  bool returns_result_set() { return !result_metadata_.columns.empty(); }
  const TResultSetMetadata* result_metadata() const { return &result_metadata_; }
  const TUniqueId& query_id() const { return query_ctx_.query_id; }

  /// Return values from counters. See explanation on declaration of
  /// num_rows_fetched_counter_, row_materialization_rate_, and row_materialization_timer_
  /// for details.
  int64_t num_rows_fetched_counter() const;
  int64_t row_materialization_rate() const;
  int64_t row_materialization_timer() const;

  /// Returns the TExecRequest for the query associated with this ClientRequestState.
  /// Contents are a place-holder until GetExecRequest(TQueryCtx) initializes the
  /// TExecRequest.
  const TExecRequest& exec_request() const {
    DCHECK(exec_request_.Load() != nullptr);
    return *exec_request_.Load();
  }
  TStmtType::type stmt_type() const { return exec_request().stmt_type; }
  TCatalogOpType::type catalog_op_type() const {
    return exec_request().catalog_op_request.op_type;
  }
  TDdlType::type ddl_type() const {
    return exec_request().catalog_op_request.ddl_params.ddl_type;
  }
  std::mutex* lock() { return &lock_; }
  std::mutex* fetch_rows_lock() { return &fetch_rows_lock_; }

  /// ExecState is stored using an AtomicEnum, so reads do not require holding lock_.
  ExecState exec_state() const { return exec_state_.Load(); }

  /// RetryState is stored using an AtomicEnum, so reads do not require holding lock_.
  RetryState retry_state() const { return retry_state_.Load(); }

  /// Translate exec_state_ to a TOperationState. Returns the current TOperationState.
  apache::hive::service::cli::thrift::TOperationState::type TOperationState() const;

  /// Translate exec_state_ to a beeswax::QueryState. Returns the current
  /// beeswax::QueryState.
  beeswax::QueryState::type BeeswaxQueryState() const;

  const Status& query_status() const { return query_status_; }
  void set_result_metadata(const TResultSetMetadata& md) { result_metadata_ = md; }
  void set_user_profile_access(bool user_has_profile_access) {
    user_has_profile_access_.Store(user_has_profile_access);
  }
  const RuntimeProfile* profile() const { return profile_; }
  const RuntimeProfile* summary_profile() const { return summary_profile_; }
  const RuntimeProfile* frontend_profile() const { return frontend_profile_; }
  int64_t start_time_us() const { return start_time_us_; }
  int64_t end_time_us() const { return end_time_us_.Load(); }
  const std::string& sql_stmt() const { return query_ctx_.client_request.stmt; }
  const TQueryOptions& query_options() const {
    return query_ctx_.client_request.query_options;
  }
  bool hs2_metadata_op() const { return query_ctx_.client_request.hs2_metadata_op; }
  /// Returns 0:0 if this is a root query
  TUniqueId parent_query_id() const { return query_ctx_.parent_query_id; }
  const vector<TTableName>& tables() const { return exec_request().tables; }

  const std::vector<std::string>& GetAnalysisWarnings() const {
    return exec_request().analysis_warnings;
  }

  inline int64_t last_active_ms() const {
    std::lock_guard<std::mutex> l(expiration_data_lock_);
    return last_active_time_ms_;
  }

  /// Returns true if Impala is actively processing this query.
  inline bool is_active() const {
    std::lock_guard<std::mutex> l(expiration_data_lock_);
    return ref_count_ > 0;
  }

  inline bool is_inflight() const {
    // If the query is in 'inflight_queries' it means that the query has actually started
    // executing. It is ok if the query is removed from 'inflight_queries' later, so we
    // can release the session lock before returning.
    std::lock_guard<std::mutex> session_lock(session_->lock);
    return session_->inflight_queries.find(query_id())
        != session_->inflight_queries.end();
  }

  bool is_expired() const {
    std::lock_guard<std::mutex> l(expiration_data_lock_);
    return is_expired_;
  }

  void set_expired() {
    std::lock_guard<std::mutex> l(expiration_data_lock_);
    is_expired_ = true;
  }

  RuntimeProfile::EventSequence* query_events() const { return query_events_; }
  RuntimeProfile* summary_profile() { return summary_profile_; }

  /// Returns nullptr when catalog_op_type is not DDL.
  const TDdlExecResponse* ddl_exec_response() const {
    return catalog_op_executor_->ddl_exec_response();
  }

  /// Returns the FETCH_ROWS_TIMEOUT_MS value for this query (converted to microseconds).
  int64_t fetch_rows_timeout_us() const { return fetch_rows_timeout_us_; }

  /// Returns the max size of the result_cache_ in number of rows.
  int64_t result_cache_max_size() const { return result_cache_max_size_; }

  /// Returns duration of the query is waiting in the queue.
  int64_t wait_time_ms() const {
    // wait_start_time_ms_ is zero iff the query is not require queuing.
    if (wait_start_time_ms_ == 0) return 0;
    // wait_end_time_ms_ might still be zero if the query is not yet dequeue.
    // Use the current Unix time in that case. Note that the duration can be
    // negative if a system clock reset happened after the query was initiated.
    int64_t wait_end_time_ms = wait_end_time_ms_ > 0LL ?
        wait_end_time_ms_ : MonotonicMillis();
    return wait_end_time_ms - wait_start_time_ms_;
  }

  /// Returns the time taken for the client to fetch all rows.
  int64_t client_fetch_wait_time_ns() const {
    return client_wait_timer_->value();
  }

  /// Sets the RetryState to RETRYING. Updates the runtime profile with the retry status
  /// and cause. Must be called while 'lock_' is held. Sets the query_status_. Future
  /// calls to UpdateQueryStatus will not have any effect. This is necessary to prevent
  /// any future calls to UpdateQueryStatus from updating the ExecState to ERROR. The
  /// ExecState should not be set to ERROR when a query is being retried in order to
  /// prevent any error statuses from being exposed to the client.
  void MarkAsRetrying(const Status& status);

  /// Sets the RetryState to RETRIED and wakes up any threads waiting for the query to be
  /// RETRIED. 'retried_id' is the query id of the newly retried query (e.g. not the id
  /// of the "original" query that was submitted by the user, but the id of the new query
  /// that was created because the "original" query failed and had to be retried).
  void MarkAsRetried(const TUniqueId& retried_id);

  /// Returns true if this ClientRequestState was created as a retry of a previously
  /// failed query, false otherwise. This is different from WasRetried() which tracks
  /// if this ClientRequestState was retried (retries are done in a new
  /// ClientRequestState).
  bool IsRetriedQuery() const { return original_id_ != nullptr; }

  /// Only called if this is a "retried" query - e.g. it was created as a result of
  /// retrying a failed query. It sets the 'original_id_' field, which is the query id of
  /// the original query attempt that failed. The original query id is added to the
  /// runtime profile as well.
  void SetOriginalId(const TUniqueId& original_id);

  /// Returns true if this ClientRequestState has already been retried, e.g. the
  /// RetryState is either RETRYING or RETRIED.
  bool WasRetried() const {
    RetryState retry_state = retry_state_.Load();
    return retry_state == RetryState::RETRYING || retry_state == RetryState::RETRIED;
  }

  /// Can only be called if this query is the result of retrying a previously failed
  /// query. Returns the query id of the original query.
  const TUniqueId& original_id() const {
    DCHECK(original_id_ != nullptr);
    return *original_id_;
  }

  /// Can only be called if this query was retried. Returns the query id of the retried
  /// query.
  const TUniqueId& retried_id() const {
    DCHECK(retried_id_ != nullptr);
    return *retried_id_;
  }

  bool IsSetRetriedId() const {
    return retried_id_ != nullptr;
  }

  /// Returns the QueryDriver that owns this ClientRequestState.
  QueryDriver* parent_driver() const { return parent_driver_; }

  AdmissionControlClient* admission_control_client() const {
    return admission_control_client_.get();
  }

  /// Returns true if results cacheing is enabled, false otherwise.
  bool IsResultCacheingEnabled() const { return result_cache_max_size_ >= 0; }

  /// Add an executor address to the set of blacklisted executor addresses. The scheduler
  /// will not schedule any query fragment instance on the given executor.
  /// Note: The given address is added to the set of blacklisted addresses only if the
  /// request has not been retried, e.g retry_state_ is not equal RETRYING nor RETRIED.
  void AddBlacklistedExecutorAddress(const NetworkAddressPB& addr);

  /// Return blacklisted executor addresses for the query
  /// This function should be called after the request is retried, no more executor
  /// address will be added to the set of blacklisted addresses so there is no data race.
  std::unordered_set<NetworkAddressPB>& GetBlacklistedExecutorAddresses() {
    return blacklisted_executor_addresses_;
  }

  /// Set a set of blacklisted executor addresses. The scheduler will not schedule any
  /// query fragment instance on the given set of executors.
  /// This function is called before the retried request is executed so don't need
  /// mutex protection.
  void SetBlacklistedExecutorAddresses(
      std::unordered_set<NetworkAddressPB>& executor_addresses);

  /// Mark planning as done for this request.
  /// This function should be called after QueryDriver::RunFrontendPlanner() is
  /// returned without error.
  void SetPlanningDone() { is_planning_done_.store(true); }

  // Register an RPC context with the query so that RPC metrics can be
  // accumulated at the query level and shown in the query profile
  void RegisterRPC();
  // Unregister any RPCs that have been released by their RPC thread and
  // accumulate their metrics into the profile timers
  void UnRegisterCompletedRPCs();
  // Unregister any remaining RPCs without recording timing in this query.
  // RPC threads will usually have completed by this point.
  void UnRegisterRemainingRPCs();
  // Copy pending RPCs for a retried query
  void CopyRPCs(ClientRequestState& from_request);

  // Update the profile counter 'GetInFlightProfileTimeStats' which is the summary stats
  // of time spent in dumping the profile before the query is archived.
  void UpdateGetInFlightProfileTime(int64_t elapsed_time_ns) {
    get_inflight_profile_time_stats_->UpdateCounter(elapsed_time_ns);
  }
  // Update the profile counter 'ClientFetchLockWaitTimer' which is the cumulative time
  // that client fetch requests waiting for locks.
  void AddClientFetchLockWaitTime(int64_t lock_wait_time_ns) {
    client_fetch_lock_wait_timer_->Add(lock_wait_time_ns);
  }
 protected:
  /// Updates the end_time_us_ of this query if it isn't set. The end time is determined
  /// when this function is called for the first time, calling it multiple times does not
  /// change the end time.
  void UpdateEndTime();

 private:
  /// The coordinator is a friend class because it needs to be able to call
  /// UpdateEndTime() when a query's admission control resources are released.
  friend class Coordinator;

  const TQueryCtx query_ctx_;

  /// Ensures single-threaded execution of FetchRows(). Callers of FetchRows() are
  /// responsible for acquiring this lock. To avoid deadlocks, callers must not hold lock_
  /// while acquiring this lock (since FetchRows() will release and re-acquire lock_ during
  /// its execution).
  /// See "Locking" in the class comment for lock acquisition order.
  std::mutex fetch_rows_lock_;

  /// Protects last_active_time_ms_, ref_count_ and is_expired_. Only held during short
  /// function calls - no other locks should be acquired while holding this lock.
  mutable std::mutex expiration_data_lock_;

  /// Stores the last time that the query was actively doing work, in Unix milliseconds.
  int64_t last_active_time_ms_;

  /// ref_count_ > 0 if Impala is currently performing work on this query's behalf. Every
  /// time a client instructs Impala to do work on behalf of this query, the ref count is
  /// increased, and decreased once that work is completed.
  uint32_t ref_count_ = 0;

  /// True if the query expired by timing out.
  bool is_expired_ = false;

  /// True if there was a transaction and it got committed or aborted.
  bool transaction_closed_ = false;

  /// Indicates whether the planning is done for the request.
  std::atomic_bool is_planning_done_{false};

  /// Executor for any child queries (e.g. compute stats subqueries). Always non-NULL.
  const boost::scoped_ptr<ChildQueryExecutor> child_query_executor_;

  /// Protects all following fields. Acquirers should be careful not to hold it for too
  /// long, e.g. during RPCs because this lock is required to make progress on various
  /// ImpalaServer requests. If held for too long it can block progress of client
  /// requests for this query, e.g. query status and cancellation. Furthermore, until
  /// IMPALA-3882 is fixed, it can indirectly block progress on all other queries.
  /// See "Locking" in the class comment for lock acquisition order.
  std::mutex lock_;

  /// Thread for asynchronously running Wait().
  std::unique_ptr<Thread> wait_thread_;

  /// Condition variable used to signal the threads that are blocked on Wait() to finish.
  /// Callers are expected to use BlockOnWait() for Wait() to finish.
  ConditionVariable block_on_wait_cv_;

  /// Wait condition used in conjunction with block_on_wait_cv_.
  bool is_wait_done_ = false;

  /// Session that this query is from
  std::shared_ptr<ImpalaServer::SessionState> session_;

  /// Resource assignment determined by scheduler.
  std::unique_ptr<QuerySchedulePB> schedule_;

  /// Thread for asynchronously running the admission control code-path and starting
  /// execution in the following cases:
  /// 1. exec_request().stmt_type == QUERY or DML.
  /// 2. CTAS query for creating a table that does not exist.
  /// 3. DDLs that are not the following:
  ///    a. Local catalog operations requiring no catalog service access;
  ///    b. Compute Stats.
  std::unique_ptr<Thread> async_exec_thread_;

  /// Set by the async-exec-thread after successful admission. Accessed through
  /// GetCoordinator() since most Coordinator methods can be called only after
  /// Coordinator::Exec() returns success. The only exceptions to this are
  /// FinishExecQueryOrDmlRequest(), which initializes it, calls Exec() on it and sets
  /// 'coord_exec_called_' to true, and UpdateBackendExecStatus() and UpdateFilter(),
  /// which can be called before Coordinator::Exec() returns.
  boost::scoped_ptr<Coordinator> coord_;

  /// Used by GetCoordinator() to check if 'coord_' can be made accessible. Set to true by
  /// the async-exec-thread only after Exec() has been successfully called on 'coord_'.
  AtomicBool coord_exec_called_;

  /// Set of executor addresses, which allow caller to tell the scheduler to not schedule
  /// the query on this set of executors.
  std::unordered_set<NetworkAddressPB> blacklisted_executor_addresses_;

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
  /// * frontend_profile_ is the profile emitted by the frontend
  ///   during planning. Added to summary_profile_ so as to avoid
  ///   breaking other tools that depend on the profile_ layout.
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
  RuntimeProfile* const frontend_profile_;
  RuntimeProfile* const server_profile_;
  RuntimeProfile* const summary_profile_;

  /// Tracks the time spent materializing rows and converting them into a QueryResultSet.
  /// The QueryResultSet format used depends on the client, for Beeswax clients an ASCII
  /// representation is used, whereas for HS2 clients (using TCLIService.thrift) rows are
  /// converted into a TRowSet. Materializing rows includes evaluating any yet unevaluated
  /// expressions using ScalarExprEvaluators.
  RuntimeProfile::Counter* row_materialization_timer_;

  /// Tracks the rate that rows are materialized.
  RuntimeProfile::Counter* row_materialization_rate_;

  /// Tracks how long we are idle waiting for a client to fetch rows.
  /// Keep summary statistics to get a sense for the number of fetch calls and
  /// the typical round-trip time.
  RuntimeProfile::Counter* client_wait_timer_;
  RuntimeProfile::SummaryStatsCounter* client_wait_time_stats_;
  /// Timer to track idle time for the above counter.
  MonotonicStopWatch client_wait_sw_;
  int64_t last_client_wait_time_ = 0;

  // Tracks time spent in dumping the profile before the query is archived.
  RuntimeProfile::SummaryStatsCounter* get_inflight_profile_time_stats_;
  // Tracks time client fetch requests waiting for locks.
  RuntimeProfile::Counter* client_fetch_lock_wait_timer_;

  // Tracks time spent by client calls reading RPC arguments
  RuntimeProfile::Counter* rpc_read_timer_;
  // Tracks time spent by client calls writing RPC results
  RuntimeProfile::Counter* rpc_write_timer_;
  // Tracks number of client RPCs
  RuntimeProfile::Counter* rpc_count_;
  // Contexts for RPC calls that have not completed and had their stats collected
  std::set<RpcEventHandler::InvocationContext*> pending_rpcs_;
  // Enables tracking of RPCs in pending_rpcs_. Also used to turn off tracking once
  // Finalize() has been called.
  bool track_rpcs_ = true;

  RuntimeProfile::EventSequence* query_events_;

  bool is_cancelled_ = false; // if true, Cancel() was called.
  AtomicBool eos_;  // if true, there are no more rows to return

  /// We enforce the invariant that query_status_ is not OK iff exec_state_ is ERROR,
  /// given that lock_ is held. exec_state_ should only be updated using
  /// UpdateExecState(), to ensure that the query profile is also updated.
  AtomicEnum<ExecState> exec_state_{ExecState::INITIALIZED};

  /// The current RetryState of the query.
  AtomicEnum<RetryState> retry_state_{RetryState::NOT_RETRIED};

  /// The current status of the query tracked by this ClientRequestState. Updated by
  /// UpdateQueryStatus(Status) or MarkAsRetrying(Status).
  Status query_status_;

  /// Default TExecRequest to return until SetExecRequest is called.
  static const TExecRequest unknown_exec_request_;

  /// The TExecRequest for the query tracked by this ClientRequestState. The TExecRequest
  /// is initialized once it's finalized. It's owned by the parent QueryDriver.
  AtomicPtr<const TExecRequest> exec_request_{&unknown_exec_request_};

  /// If true, effective_user() has access to the runtime profile and execution
  /// summary.
  AtomicBool user_has_profile_access_{true};

  TResultSetMetadata result_metadata_; // metadata for select query
  int num_rows_fetched_ = 0; // number of rows fetched by client for the entire query

  /// The total number of rows fetched for this query, the counter is not reset if the
  /// fetch is restarted. It does not include any rows read from the results cache. It
  /// only counts the number of materialized rows and it is used for deriving the
  /// row_materialization_rate_. It is not set for non QUERY statements such as EXPLAIN,
  /// SHOW, etc.
  RuntimeProfile::Counter* num_rows_fetched_counter_ = nullptr;

  /// Similar to the 'num_rows_fetched_counter_' except it tracks the number of rows read
  /// from the results cache. The counter is not reset if the fetch is restarted and it
  /// is not set for non QUERY statements.
  RuntimeProfile::Counter* num_rows_fetched_from_cache_counter_ = nullptr;

  /// True if a fetch was attempted by a client, regardless of whether a result set
  /// (or error) was returned to the client.
  bool fetched_rows_ = false;

  /// To get access to UpdateCatalog, LOAD, and DDL methods. Not owned.
  Frontend* frontend_;

  /// The parent ImpalaServer; called to wait until the impalad has processed a catalog
  /// update request. Not owned.
  ImpalaServer* parent_server_;

  /// Start/end time of the query, in Unix microseconds.
  int64_t start_time_us_;
  /// end_time_us_ is initialized to 0, which is used to indicate that the query is not
  /// yet done. It is assigned the final value in ClientRequestState::Finalize() or when
  /// the coordinator releases its admission control resources.
  AtomicInt64 end_time_us_{0};

  /// Timeout, in microseconds, when waiting for rows to become available. Derived from
  /// the query option FETCH_ROWS_TIMEOUT_MS.
  const int64_t fetch_rows_timeout_us_;

  /// Start/end time of the query queuing, in Unix milliseconds.
  /// Initialized to 0, indicating that queries do not need to be queued.
  int64_t wait_start_time_ms_ = 0;
  int64_t wait_end_time_ms_ = 0;

  /// If this ClientRequestState was created as a retry of a previously failed query, the
  /// original_id_ is set to the query id of the original query that failed. The
  /// "original" query is the query that was submitted by the user that failed and had to
  /// be retried.
  std::unique_ptr<const TUniqueId> original_id_ = nullptr;

  /// Query id of the retried query. The retried query is the new query that is run
  /// whenever the original query fails with a retryable error. See 'original_id_' for
  /// an explanation of what the "original" query is.
  std::unique_ptr<const TUniqueId> retried_id_ = nullptr;

  /// Condition variable used to signal any threads that are waiting until the query has
  /// been retried.
  ConditionVariable block_until_retried_cv_;

  /// The QueryDriver that owns this ClientRequestState. The reference is set in the
  /// constructor. It always outlives the ClientRequestState.
  QueryDriver* parent_driver_;

  /// Manages interactions with the AdmissionController, which may be either local or
  /// remote.
  std::unique_ptr<AdmissionControlClient> admission_control_client_;

  /// Executes a local catalog operation (an operation that does not need to execute
  /// against the catalog service). Includes USE, SHOW, DESCRIBE, and EXPLAIN statements.
  Status ExecLocalCatalogOp(const TCatalogOpRequest& catalog_op) WARN_UNUSED_RESULT;

  /// Updates last_active_time_ms_ and ref_count_ to reflect that query is currently not
  /// doing any work. Takes expiration_data_lock_.
  void MarkInactive();

  /// Updates last_active_time_ms_ and ref_count_ to reflect that query is currently being
  /// actively processed. Takes expiration_data_lock_.
  void MarkActive();

  /// Sets up profile and pre-execution counters, creates the query schedule, and calls
  /// FinishExecQueryOrDmlRequest() which contains the core logic of executing a QUERY or
  /// DML execution request. When 'async' is true, spawn a thread to run
  /// FinishExecQueryOrDmlRequest(). Otherwise, the method runs in the same thread as the
  /// caller.
  /// Non-blocking.
  Status ExecQueryOrDmlRequest(const TQueryExecRequest& query_exec_request, bool async)
      WARN_UNUSED_RESULT;

  /// Submits the exec request to the admission controller and on successful admission,
  /// starts up the coordinator execution, makes it accessible by setting
  /// 'coord_exec_called_' to true and advances operation_state_ to RUNNING. Handles
  /// async cancellation of queries and cleans up state if needed.
  void FinishExecQueryOrDmlRequest();

  /// Core logic of executing a ddl statement. May internally initiate execution of
  /// queries (e.g., compute stats) or dml (e.g., create table as select)
  Status ExecDdlRequest() WARN_UNUSED_RESULT;

  /// A helper function to execute certain ddl requests synchronously.
  Status ExecDdlRequestImplSync() WARN_UNUSED_RESULT;

  /// A helper function to execute certain ddl requests optionally
  /// asynchronously. 'exec_in_worker_thread' indicates whether the execution of
  /// DDL is done in a worker thread or not. This flag is set to true when the
  /// execution of the DDL request runs in 'async_exec_thread_', and set to false
  /// otherwise.
  void ExecDdlRequestImpl(bool exec_in_worker_thread);

  /// Decide whether to call ExecDdlRequestImpl() or
  /// ExecDdlRequestImplSync() for ExecDdlRequest(). Return true to call
  /// ExecDdlRequestImpl() and false to call ExecDdlRequestImplSync().
  bool ShouldRunExecDdlAsync();

  /// The logic of executing a load data statement which runs ExecLoadDataRequestImpl()
  /// in async_exec_thread_ by default.
  Status ExecLoadDataRequest() WARN_UNUSED_RESULT;

  /// Core logic of executing a load data statement.
  void ExecLoadDataRequestImpl(bool exec_in_worker_thread);

  /// Executes LOAD DATA related sub-queries for Iceberg tables.
  void ExecLoadIcebergDataRequestImpl(TLoadDataResp response);

  /// Executes a shut down request.
  Status ExecShutdownRequest() WARN_UNUSED_RESULT;

  /// Core logic of Wait(). Does not update operation_state_/query_status_.
  Status WaitInternal() WARN_UNUSED_RESULT;

  /// Core logic of FetchRows(). Does not update operation_state_/query_status_.
  /// Caller needs to hold fetch_rows_lock_ and lock_. 'block_on_wait_time_us_' is the
  /// amount of time the client spent (in microseconds) waiting in BlockOnWait().
  Status FetchRowsInternal(const int32_t max_rows, QueryResultSet* fetched_rows,
      int64_t block_on_wait_time_us) WARN_UNUSED_RESULT;

  /// Gather and publish all required updates to the metastore.
  /// For transactional queries:
  /// If everything goes well the Hive transaction is committed by the Catalogd,
  /// but the Kudu transaction is committed by this function.
  /// If an error occurs the transaction gets aborted by this function. Either way
  /// the transaction will be closed when this function returns.
  Status UpdateCatalog() WARN_UNUSED_RESULT;

  /// Fills 'cat_ice_op' based on 'finalize_params'. Returns false if there is no
  /// need to update the Catalog (no records changed), returns true otherwise.
  bool CreateIcebergCatalogOps(const TFinalizeParams& finalize_params,
      TIcebergOperationParam* cat_ice_op);

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
  /// at which point results will be available.
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

  /// Update the operation state and the "Query State" summary profile string.
  /// Does not take lock_, but requires it: caller must ensure lock_ is taken before
  /// calling UpdateExecState.
  void UpdateExecState(ExecState exec_state);

  /// Gets the query options, their levels and the values for this client request
  /// and populates the result set with them. It covers the subset of options for
  /// 'SET' and all of them for 'SET ALL'
  void PopulateResultForSet(bool is_set_all);

  /// Returns the Hive transaction id for this client request. 'InTransaction()' must be
  /// true when invoked.
  int64_t GetTransactionId() const;

  /// Returns true if there is an open Hive transaction for this client request.
  bool InTransaction() const;

  /// Aborts the Hive transaction of this client request.
  void AbortTransaction();

  /// Invoke this function when the Hive transaction is committed or aborted.
  void ClearTransactionState();

  /// Returns true if there is an open Kudu transaction for this client request.
  bool InKuduTransaction() const;

  /// Aborts the Kudu transaction of this client request.
  void AbortKuduTransaction();

  /// Commits the Kudu transaction of this client request.
  Status CommitKuduTransaction();

  /// helper that logs the audit record for this query id. Takes the query_status
  /// as input parameter so that it operates on the same status polled in the
  /// beginning of LogQueryEvents().
  Status LogAuditRecord(const Status& query_status) WARN_UNUSED_RESULT;

  /// Helper that logs the lineage record for this query id.
  Status LogLineageRecord() WARN_UNUSED_RESULT;

  /// Logs audit and column lineage events. Expects that Wait() has already finished.
  /// Grabs lock_ for polling the query_status(). Hence do not call it under lock_.
  void LogQueryEvents();

  /// Helper function to get common header
  TCatalogServiceRequestHeader GetCatalogServiceRequestHeader();

  /// The logic of executing a MIGRATE TABLE statement.
  Status ExecMigrateRequest() WARN_UNUSED_RESULT;

  /// Core logic of executing a MIGRATE TABLE statement.
  void ExecMigrateRequestImpl();

  /// Used when running into an error during table migration to extend 'status' with some
  /// hints about how to reset the original table name. 'params' holds the SQL query
  /// string the user should run.
  void AddTableResetHints(const TConvertTableRequest& params, Status* status) const;

  /// Adds the catalog execution timeline returned from catalog RPCs into the profile.
  void AddCatalogTimeline();
};
}
