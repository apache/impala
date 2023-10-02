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

#include "common/status.h"
#include "service/impala-server.h"

#include "common/names.h"

namespace impala {

class ClientRequestState;
class ExecEnv;
class TExecRequest;

/// References to a ClientRequestState should be done via a QueryHandle. A
/// ClientRequestState requires a QueryDriver to be in scope, since the QueryDriver
/// owns the ClientRequestState. The QueryHandle is similar to a unique_ptr in that it
/// wraps a pointer to a given object (in this case ClientRequestState) and overloads
/// the '->' and '*' operators. The ClientRequestState is accessed using the '->' and '*'
/// operators. This ensures that whenever a ClientRequestState is referenced, a valid
/// shared pointer to a QueryDriver is always in scope.
struct QueryHandle {
  /// Sets the QueryDriver and ClientRequestState for this handle.
  void SetHandle(
      std::shared_ptr<QueryDriver> query_driver, ClientRequestState* request_state) {
    SetQueryDriver(move(query_driver));
    SetClientRequestState(request_state);
  }

  const std::shared_ptr<QueryDriver>& query_driver() const { return query_driver_; }

  inline ClientRequestState* operator->() const {
    DCHECK(request_state_ != nullptr);
    return request_state_;
  }

  inline ClientRequestState& operator*() const {
    DCHECK(request_state_ != nullptr);
    return *request_state_;
  }

 private:
  friend class QueryDriver;

  /// Sets the QueryDriver for this handle.
  void SetQueryDriver(std::shared_ptr<QueryDriver> query_driver) {
    query_driver_ = std::move(query_driver);
  }

  /// Sets the ClientRequestState for this handle.
  void SetClientRequestState(ClientRequestState* request_state) {
    request_state_ = request_state;
  }

  std::shared_ptr<QueryDriver> query_driver_;
  ClientRequestState* request_state_ = nullptr;
};

/// The QueryDriver owns the ClientRequestStates for a query. A single query can map
/// to multiple ClientRequestStates if the query is retried multiple times. The
/// QueryDriver sits between the ImpalaServer and ClientRequestState. Currently, it
/// mainly handles query retries.
///
/// ImpalaServer vs. QueryDriver vs. ClientRequestState vs. Coordinator:
///
/// All of these classes work together to run a client-submitted query end to end. A
/// client submits a request to the ImpalaServer to run a query. The ImpalaServer
/// creates a QueryDriver and ClientRequestState to run the query. The
/// ClientRequestState creates a Coordinator that is responsible for coordinating the
/// execution of all query fragments and for tracking the lifecyle of the fragments. A
/// fetch results request flows from the ImpalaServer to the ClientRequestState and then
/// to the Coordinator, which fetches the results from the fragment running on the
/// coordinator process.
///
/// Query Retries:
///
/// Query retries are driven by the 'TryQueryRetry' and 'RetryQueryFromThread' methods.
/// Each query retry creates a new ClientRequestState. In other words, a single
/// ClientRequestState corresponds to a single attempt of a query. Thus, a QueryDriver can
/// own multiple ClientRequestStates, one for each query attempt.
///
/// Retries are done asynchronously in a separate thread. If the query cannot be retried
/// for any reason, the original query is unregistered and moved to the ERROR state. The
/// steps required to launch a retry of a query are very similar to the steps necessary
/// to start the original attempt of the query, except parsing, planning, optimizing,
/// etc. are all skipped. This is done by cacheing the TExecRequest from the original
/// query and re-using it for all query retries.
///
/// At a high level, retrying a query requires performing the following steps:
///   * Cancelling the original query
///   * Creating a new ClientRequestState with a unique query id and a copy of the
///     TExecRequest from the original query
///   * Registering the new query (ImpalaServer::RegisterQuery)
///   * Launching the new query (ClientRequestState::Exec)
///   * Closing and unregistering the original query
///
/// *Transparent* Query Retries:
/// A key feature of query retries is that they should be "transparent" from the client
/// perspective. No client code modifications should be necessary to support query
/// retries. The QueryDriver makes query retries "transparent" by introducing the
/// concept of an "active" ClientRequestState. The "active" ClientRequestState refers
/// to the currently running attempt to run the query, and is accessible by the method
/// GetActiveClientRequestState(). Callers (e.g. ImpalaServer) can use this method to get
/// a reference to the most recent attempt of the query.
///
/// Disclaimer: While query retries are designed to be *transparent* there are scenarios
/// where clients may see some unexpected behavior when a query is retried.
///
/// When a query is retried, the retry is modeled as a brand new query with a new query
/// id, which will be distinct from the query id of the originally submitted query that
/// ultimately failed. So users might see multiple query ids in play during the lifecycle
/// of a single query submission.
///
/// Since a query retry is a brand new query, that query has its own runtime profile as
/// well; the runtime profiles of the failed and retried queries will be linked together.
/// Users need to be aware that there can be multiple runtime profiles associated with a
/// single query submission.
///
/// Thread Safety:
///
/// Only GetClientRequestState(query_id) and GetActiveClientRequestState() are thread
/// safe. They are protected by an internal SpinLock.
class QueryDriver {
 public:
  QueryDriver(ImpalaServer* parent_server);
  ~QueryDriver();

  /// Creates the TExecRequest for this query. The TExecRequest is owned by the
  /// QueryDriver. The TExecRequest is created by calling into Frontend.java,
  /// specifically, the Frontend#createExecRequest(PlanCtx) method. When creating the
  /// TExecRequest, the Frontend runs the parser, analyzer, authorization code, planner,
  /// optimizer, etc. The TQueryCtx is created by the ImpalaServer and contains the full
  /// query string (TQueryCtx::TClientRequest::stmt).
  Status RunFrontendPlanner(const TQueryCtx& query_ctx) WARN_UNUSED_RESULT;

  /// Similar to RunFrontendPlanner but takes TExecRequest from an external planner
  Status SetExternalPlan(const TQueryCtx& query_ctx, TExecRequest exec_request);

  /// Returns the ClientRequestState corresponding to the given query id.
  ClientRequestState* GetClientRequestState(const TUniqueId& query_id);

  /// Returns the active ClientRequestState for the query. If a query is retried, this
  /// method returns the most recent attempt of the query.
  ClientRequestState* GetActiveClientRequestState();

  /// Retry the query if query retries are enabled (they are enabled / disabled using the
  /// query option 'retry_failed_queries') and if the query can be retried. Queries can
  /// only be retried if (1) no rows have already been fetched for the query, (2) the
  /// query has not already been retried (when a query fails, only a single retry of that
  /// query attempt should be run), and (3) the max number of retries has not been
  /// exceeded (currently the limit is just one retry). Queries should only be retried if
  /// there has been a cluster membership change. So either a node is blacklisted or a
  /// statestore update removes a node from the cluster membership. The retry is done
  /// asynchronously by a dedicated thread. 'error' is the reason why the query failed. If
  /// the attempt to retry the query failed, additional details might be added to the
  /// status. If 'was_retried' is not nullptr it is set to true if the query was actually
  /// retried, false otherwise. This method is idempotent, it can safely be called
  /// multiple times, however, only the first call to the method will trigger a retry.
  /// This method will set the query_status_ of the given client_request_state.
  void TryQueryRetry(ClientRequestState* client_request_state, Status* error,
      bool* was_retried = nullptr);

  /// Finalize this QueryDriver. Must be called before Unregister(...) is called.
  /// This indicates that the query should no longer be considered registered from the
  /// client's point of view. Returns an INVALID_QUERY_HANDLE error if finalization
  /// already started. After this method has been called, finalized() will return true.
  /// If 'check_inflight' is true and the query is not yet inflight, Finalize will error.
  /// 'cause' is passed to ClientRequestState::Finalize(Status).
  Status Finalize(QueryHandle* query_handle, bool check_inflight, const Status* cause);

  /// Delete this query from the given QueryDriverMap.
  Status Unregister(ImpalaServer::QueryDriverMap* query_driver_map) WARN_UNUSED_RESULT;

  /// True if Finalize() was called while the query was inflight.
  bool finalized() const { return finalized_.Load(); }

  /// Functions to set/get whether or not the query managed by this class should be
  /// recorded in the query log table.
  void IncludeInQueryLog(const bool include) noexcept;
  bool IncludedInQueryLog() const noexcept;

  /// Creates a new QueryDriver instance using the given ImpalaServer. Creates the
  /// ClientRequestState for the given 'query_ctx' and 'session_state'. Sets the given
  /// QueryHandle's QueryDriver.
  static void CreateNewDriver(ImpalaServer* impala_server, QueryHandle* query_handle,
      const TQueryCtx& query_ctx,
      std::shared_ptr<ImpalaServer::SessionState> session_state);

 private:
  /// Helper method to process query retries, called by the 'retry_query_thread_'.
  /// 'error' is the reason why the query failed. The failed query is cancelled, and then
  /// a new ClientRequestState is created for the retried query. The new
  /// ClientRequestState copies the TExecRequest from the failed query in order to avoid
  /// query compilation and planning again. Once the new query is registered and launched,
  /// the failed query is unregistered. 'query_driver' is a shared_ptr to 'this'
  /// QueryDriver. The pointer is necessary to ensure that 'this' QueryDriver is not
  /// deleted while the thread is running.
  void RetryQueryFromThread(
      const Status& error, std::shared_ptr<QueryDriver> query_driver);

  /// Creates the initial ClientRequestState for the given TQueryCtx. Should only be
  /// called once by the ImpalaServer. Additional ClientRequestStates are created by
  /// CreateRetriedClientRequestState, although they are only created if the query
  /// is retried.
  void CreateClientRequestState(const TQueryCtx& query_ctx,
      std::shared_ptr<ImpalaServer::SessionState> session_state,
      QueryHandle* query_handle);

  /// Helper method for RetryQueryFromThread. Creates the retry client request state (the
  /// new attempt of the query) based on the original request state. Uses the TExecRequest
  /// from the original request state to create the retry request state. Creates a new
  /// query id for the retry request state.
  void CreateRetriedClientRequestState(ClientRequestState* request_state,
      std::unique_ptr<ClientRequestState>* retry_request_state,
      std::shared_ptr<ImpalaServer::SessionState>* session);

  /// Does the work of RunFrontendPlanner so we can also use it to dump the planner
  /// result from SetExternalPlan to dump_exec_request_path without redundant work.
  /// Set use_request to false to skip saving the TExecRequest produced in exec_request_.
  Status DoFrontendPlanning(const TQueryCtx& query_ctx,
      bool use_request = true) WARN_UNUSED_RESULT;

  /// Helper method for handling failures when retrying a query. 'status' is the reason
  /// why the retry failed and is expected to be in the error state. Additional details
  /// are added to the 'status'. Once the 'status' has been updated, it is set as the
  /// 'query status' of the given 'request_state'. Finally, the 'request_state' is
  /// unregistered from 'parent_server_', since the retry failed.
  void HandleRetryFailure(Status* status, string* error_msg,
      ClientRequestState* request_state, const TUniqueId& retry_query_id);

  /// ImpalaServer that owns this QueryDriver.
  ImpalaServer* parent_server_;

  /// Protects 'client_request_state_' and 'retried_client_request_state_'.
  SpinLock client_request_state_lock_;

  /// The ClientRequestState for the query. Set in 'CreateClientRequestState'. Owned by
  /// the QueryDriver.
  std::unique_ptr<ClientRequestState> client_request_state_;

  /// The ClientRequestState for the retried query. Set in 'RetryQueryFromThread'. Only
  /// set if the query is retried. Owned by the QueryDriver.
  std::unique_ptr<ClientRequestState> retried_client_request_state_;

  /// The TExecRequest for the query. Created in 'CreateClientRequestState' and loaded in
  /// 'RunFrontendPlanner'. Not thread safe.
  std::unique_ptr<const TExecRequest> exec_request_;

  /// The TExecRequest for the retried query. Created and initialized in
  /// 'CreateRetriedClientRequestState'.
  std::unique_ptr<const TExecRequest> retry_exec_request_;

  /// Thread to process query retry requests. Done in a separate thread to avoid blocking
  /// control service RPC threads.
  std::unique_ptr<Thread> retry_query_thread_;

  /// The retry query id that has been registered. 0 if no retry or the retry fails before
  /// registering the retry query id. Used to delete the retry query id in the
  /// query_driver_map.
  TUniqueId registered_retry_query_id_;

  /// True if a thread has called Finalize() and the query is inflight. Threads calling
  /// Finalize() do a compare-and-swap on this so that only one thread can proceed.
  AtomicBool finalized_{false};

  /// True if this query should be recorded in the query log table.
  /// Default: `true`
  bool include_in_query_log_ = true;
};
}
