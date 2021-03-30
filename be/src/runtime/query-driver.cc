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

#include <thrift/protocol/TDebugProtocol.h>

#include "runtime/exec-env.h"
#include "runtime/query-driver.h"
#include "service/client-request-state.h"
#include "service/frontend.h"
#include "service/impala-server.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

QueryDriver::QueryDriver(ImpalaServer* parent_server) : parent_server_(parent_server) {}

QueryDriver::~QueryDriver() {
  DCHECK(finalized_.Load()) << "Finalize() must have been called";
}

void QueryDriver::CreateClientRequestState(const TQueryCtx& query_ctx,
    shared_ptr<ImpalaServer::SessionState> session_state, QueryHandle* query_handle) {
  DCHECK(exec_request_ == nullptr);
  DCHECK(client_request_state_ == nullptr);
  ExecEnv* exec_env = ExecEnv::GetInstance();
  lock_guard<SpinLock> l(client_request_state_lock_);
  exec_request_ = make_unique<TExecRequest>();
  client_request_state_ =
      make_unique<ClientRequestState>(query_ctx, exec_env->frontend(), parent_server_,
          session_state, exec_request_.get(), query_handle->query_driver().get());
  DCHECK(query_handle != nullptr);
  (*query_handle).SetClientRequestState(client_request_state_.get());
}

Status QueryDriver::RunFrontendPlanner(const TQueryCtx& query_ctx) {
  // Takes the TQueryCtx and calls into the frontend to initialize the TExecRequest for
  // this query.
  DCHECK(client_request_state_ != nullptr);
  DCHECK(exec_request_ != nullptr);
  RETURN_IF_ERROR(client_request_state_->UpdateQueryStatus(
      ExecEnv::GetInstance()->frontend()->GetExecRequest(
          query_ctx, exec_request_.get())));
  return Status::OK();
}

ClientRequestState* QueryDriver::GetActiveClientRequestState() {
  lock_guard<SpinLock> l(client_request_state_lock_);
  if (retried_client_request_state_ != nullptr) {
    return retried_client_request_state_.get();
  }
  DCHECK(client_request_state_ != nullptr);
  return client_request_state_.get();
}

ClientRequestState* QueryDriver::GetClientRequestState(const TUniqueId& query_id) {
  lock_guard<SpinLock> l(client_request_state_lock_);
  if (retried_client_request_state_ != nullptr
      && retried_client_request_state_->query_id() == query_id) {
    return retried_client_request_state_.get();
  }
  DCHECK(client_request_state_ != nullptr);
  DCHECK(client_request_state_->query_id() == query_id);
  return client_request_state_.get();
}

void QueryDriver::TryQueryRetry(
    ClientRequestState* client_request_state, Status* error, bool* was_retried) {
  DCHECK(error != nullptr);
  if (was_retried != nullptr) *was_retried = false;

  // Get the most recent query attempt, and retry it.
  const TUniqueId& query_id = client_request_state->query_id();
  DCHECK(client_request_state->schedule() != nullptr);

  if (client_request_state->schedule()->query_options().retry_failed_queries) {
    lock_guard<mutex> l(*client_request_state->lock());

    // Queries can only be retried if no rows for the query have been fetched
    // (IMPALA-9225).
    if (client_request_state->fetched_rows()) {
      string err_msg = Substitute("Skipping retry of query_id=$0 because the client has "
                                  "already fetched some rows",
          PrintId(query_id));
      VLOG_QUERY << err_msg;
      error->AddDetail(err_msg);
      return;
    }

    // If a retry for the failed query has already been scheduled, don't retry it
    // again.
    if (client_request_state->WasRetried()) {
      return;
    }

    // Queries can only be retried once (IMPALA-9200).
    if (client_request_state->IsRetriedQuery()) {
      VLOG_QUERY << Substitute(
          "Skipping retry of query_id=$0 because it has already been retried",
          PrintId(query_id));
      // If query retries are enabled, but the max number of retries has been hit,
      // include the number of retries in the error message.
      error->AddDetail("Max retry limit was hit. Query was retried 1 time(s).");
      return;
    }

    const TUniqueId& query_id = client_request_state_->query_id();

    // Triggering a retry from the INITIALIZED phase is possible: the
    // cancellation thread pool can kill a query while it is in the INITIALIZATION phase.
    ClientRequestState::ExecState exec_state = client_request_state_->exec_state();
    DCHECK(exec_state == ClientRequestState::ExecState::INITIALIZED
        || exec_state == ClientRequestState::ExecState::PENDING
        || exec_state == ClientRequestState::ExecState::RUNNING)
        << Substitute(
            "Illegal state: $0", ClientRequestState::ExecStateToString(exec_state));

    // If a retry has already been scheduled for this query, do not schedule another one.
    DCHECK(client_request_state_->retry_state()
        == ClientRequestState::RetryState::NOT_RETRIED)
        << Substitute("Cannot retry a that has already been retried query_id = $0",
            PrintId(query_id));

    // Update the state and then schedule the retry asynchronously.
    client_request_state_->MarkAsRetrying(*error);

    // Another reference to this QueryDriver (via the shared_ptr) needs to be created and
    // passed to the thread so that a valid shared_ptr exists while the thread is running.
    // Otherwise it is possible that the user cancels the query and this QueryDriver gets
    // deleted by the shared_ptr.
    shared_ptr<QueryDriver> query_driver = parent_server_->GetQueryDriver(query_id);

    // Launch the query retry in a separate thread, 'was_retried' is set to true
    // if the query retry was successfully launched.
    Status status = Thread::Create("impala-server",
        Substitute("query-retry-thread-$0", PrintId(query_id)),
        &QueryDriver::RetryQueryFromThread, this, *error, query_driver,
        &retry_query_thread_);

    if (!status.ok()) {
      LOG(ERROR) << Substitute(
          "Unable to schedule a retry of query $0 due to thread creation error $1",
          PrintId(query_id), status.GetDetail());
    } else if (was_retried != nullptr) {
      *was_retried = true;
    }
  }
}

void QueryDriver::RetryQueryFromThread(
    const Status& error, shared_ptr<QueryDriver> query_driver) {
  // This method does not require holding the ClientRequestState::lock_ for the original
  // query. This ensures that the client can still interact (e.g. poll the state) of the
  // original query while the new query is being created. This is necessary as it might
  // take a non-trivial amount of time to setup and start running the new query.

  DCHECK(query_driver.get() == this);
  const TUniqueId& query_id = client_request_state_->query_id();
  VLOG_QUERY << Substitute(
      "Retrying query $0 with error message $1", PrintId(query_id), error.GetDetail());

  // There should be no retried client request state.
  ClientRequestState* request_state;
  {
    lock_guard<SpinLock> l(client_request_state_lock_);
    DCHECK(retried_client_request_state_ == nullptr);
    DCHECK(client_request_state_ != nullptr);
    request_state = client_request_state_.get();
  }
  DCHECK(request_state->retry_state() == ClientRequestState::RetryState::RETRYING)
      << Substitute("query=$0 unexpected state $1", PrintId(request_state->query_id()),
          request_state->ExecStateToString(request_state->exec_state()));

  shared_ptr<ImpalaServer::SessionState> session = request_state->session();

  // Cancel the query. 'check_inflight' should be false because (1) a retry can be
  // triggered when the query is in the INITIALIZED state, and (2) the user could have
  // already cancelled the query.
  Status status = request_state->Cancel(false, nullptr);
  if (!status.ok()) {
    status.AddDetail(Substitute("Failed to retry query $0", PrintId(query_id)));
    discard_result(request_state->UpdateQueryStatus(status));
    return;
  }

  unique_ptr<ClientRequestState> retry_request_state = nullptr;
  CreateRetriedClientRequestState(request_state, &retry_request_state, &session);
  DCHECK(retry_request_state != nullptr);

  const TUniqueId& retry_query_id = retry_request_state->query_id();
  VLOG_QUERY << Substitute("Retrying query $0 with new query id $1", PrintId(query_id),
      PrintId(retry_query_id));

  // The steps below mimic what is done when a query is first launched. See
  // ImpalaServer::ExecuteStatement.

  // Mark the session as active. This is necessary because a ScopedSessionState may not
  // actively be opened at this time. A reference to the session (SessionState::ref_count)
  // is necessary when calling ImpalaServer::RegisterQuery with the session. Furthermore,
  // a reference to the session is necessary to ensure that the session does not get
  // expired while the retry is running.
  parent_server_->MarkSessionActive(session);

  // A QueryHandle instance is required for a few of the methods called below.
  QueryHandle retry_query_handle;
  retry_query_handle.SetHandle(query_driver, retry_request_state.get());

  // Register the new query.
  status = parent_server_->RegisterQuery(retry_query_id, session, &retry_query_handle);
  if (!status.ok()) {
    string error_msg = Substitute(
        "RegisterQuery for new query with id $0 failed", PrintId(retry_query_id));
    HandleRetryFailure(&status, &error_msg, request_state, retry_query_id);
    return;
  }

  // Run the new query.
  status = retry_request_state->Exec();
  if (!status.ok()) {
    string error_msg =
        Substitute("Exec for new query with id $0 failed", PrintId(retry_query_id));
    HandleRetryFailure(&status, &error_msg, request_state, retry_query_id);
    return;
  }

  status = retry_request_state->WaitAsync();
  if (!status.ok()) {
    string error_msg =
        Substitute("WaitAsync for new query with id $0 failed", PrintId(retry_query_id));
    HandleRetryFailure(&status, &error_msg, request_state, retry_query_id);
    return;
  }

  // Optionally enable result caching on the ClientRequestState. The
  // 'result_cache_max_size' value was already validated in ImpalaHs2Server, so it does
  // not need to be validated again.
  if (request_state->IsResultCacheingEnabled()) {
    status = parent_server_->SetupResultsCacheing(
        retry_query_handle, session, request_state->result_cache_max_size());
    if (!status.ok()) {
      string error_msg = Substitute(
          "Setting up results cacheing for query $0 failed", PrintId(retry_query_id));
      HandleRetryFailure(&status, &error_msg, request_state, retry_query_id);
      return;
    }
  }

  // Mark the new query as "in flight".
  status = parent_server_->SetQueryInflight(session, retry_query_handle);
  if (!status.ok()) {
    string error_msg = Substitute(
        "SetQueryInFlight for new query with id $0 failed", PrintId(retry_query_id));
    HandleRetryFailure(&status, &error_msg, request_state, retry_query_id);
    return;
  }

  // 'client_request_state_' points to the original query and
  // 'retried_client_request_state_' points to the retried query.
  {
    lock_guard<SpinLock> l(client_request_state_lock_);
    // Before exposing the new query, check if the original query was unregistered while
    // the new query was being created. If it was, then abort the new query.
    if (parent_server_->GetQueryDriver(query_id) == nullptr) {
      string error_msg = Substitute("Query was unregistered");
      HandleRetryFailure(&status, &error_msg, request_state, retry_query_id);
      return;
    }
    retried_client_request_state_ = move(retry_request_state);
  }

  // Mark the original query as successfully retried.
  request_state->MarkAsRetried(retry_query_id);
  VLOG_QUERY << Substitute("Retried query $0 with new query id $1", PrintId(query_id),
      PrintId(retry_query_id));

  // Close the original query.
  QueryHandle query_handle;
  query_handle.SetHandle(query_driver, request_state);
  parent_server_->CloseClientRequestState(query_handle);
  parent_server_->MarkSessionInactive(session);
}

void QueryDriver::CreateRetriedClientRequestState(ClientRequestState* request_state,
    unique_ptr<ClientRequestState>* retry_request_state,
    shared_ptr<ImpalaServer::SessionState>* session) {
  parent_server_->PrepareQueryContext(&exec_request_->query_exec_request.query_ctx);

  // Create the ClientRequestState for the new query.
  const TQueryCtx& query_ctx = exec_request_->query_exec_request.query_ctx;
  ExecEnv* exec_env = ExecEnv::GetInstance();
  *retry_request_state = make_unique<ClientRequestState>(query_ctx, exec_env->frontend(),
      parent_server_, *session, exec_request_.get(), request_state->parent_driver());
  (*retry_request_state)->SetOriginalId(request_state->query_id());
  (*retry_request_state)
      ->set_user_profile_access(
          (*retry_request_state)->exec_request().user_has_profile_access);
  if ((*retry_request_state)->exec_request().__isset.result_set_metadata) {
    (*retry_request_state)
        ->set_result_metadata((*retry_request_state)->exec_request().result_set_metadata);
  }
}

void QueryDriver::HandleRetryFailure(Status* status, string* error_msg,
    ClientRequestState* request_state, const TUniqueId& retry_query_id) {
  status->AddDetail(
      Substitute("Failed to retry query $0", PrintId(request_state->query_id())));
  status->AddDetail(*error_msg);
  discard_result(request_state->UpdateQueryStatus(*status));
  parent_server_->UnregisterQueryDiscardResult(retry_query_id, false, status);
}

Status QueryDriver::Finalize(
    QueryHandle* query_handle, bool check_inflight, const Status* cause) {
  if (!finalized_.CompareAndSwap(false, true)) {
    // Return error as-if the query was already unregistered, so that it appears to the
    // client as-if unregistration already happened. We don't need a distinct
    // client-visible error for this case.
    lock_guard<SpinLock> l(client_request_state_lock_);
    return Status::Expected(
        TErrorCode::INVALID_QUERY_HANDLE, PrintId(client_request_state_->query_id()));
  }
  RETURN_IF_ERROR((*query_handle)->Finalize(check_inflight, cause));
  return Status::OK();
}

Status QueryDriver::Unregister(QueryDriverMap* query_driver_map) {
  DCHECK(finalized_.Load());
  const TUniqueId* query_id = nullptr;
  const TUniqueId* retry_query_id = nullptr;
  {
    // In order to preserve a consistent lock ordering, client_request_state_lock_ is
    // released before DeleteQueryDriver is called, as DeleteQueryDriver requires taking
    // a ScopedShardedMapRef (a sharded map lock). Methods in ImpalaServer (such as
    // UnresponsiveBackendThread) require taking a ScopedShardedMapRef and then calling
    // Get*ClientRequestState methods. So in order to define a consistent lock ordering
    // (e.g. acquire ScopedShardedMapRef before client_request_state_lock_)
    // client_request_state_lock_ is released before calling DeleteQueryDriver.
    lock_guard<SpinLock> l(client_request_state_lock_);
    query_id = &client_request_state_->query_id();
    if (retried_client_request_state_ != nullptr) {
      retry_query_id = &retried_client_request_state_->query_id();
    }
  }
  RETURN_IF_ERROR(query_driver_map->DeleteQueryDriver(*query_id));
  if (retry_query_id != nullptr) {
    RETURN_IF_ERROR(query_driver_map->DeleteQueryDriver(*retry_query_id));
  }
  return Status::OK();
}

void QueryDriver::CreateNewDriver(ImpalaServer* impala_server, QueryHandle* query_handle,
    const TQueryCtx& query_ctx, shared_ptr<ImpalaServer::SessionState> session_state) {
  query_handle->query_driver_ = std::make_shared<QueryDriver>(impala_server);
  query_handle->query_driver_->CreateClientRequestState(
      query_ctx, session_state, query_handle);
}
}
