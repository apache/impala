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

#include "service/child-query.h"
#include "service/impala-server.inline.h"
#include "service/client-request-state.h"
#include "service/query-options.h"
#include "util/debug-util.h"

#include "common/names.h"

using namespace impala;
using namespace apache::hive::service::cli::thrift;

namespace impala {

const string ChildQuery::PARENT_QUERY_OPT = "impala.parent_query_id";

// To detect cancellation of the parent query this function checks IsCancelled() before
// any HS2 "RPC" into the impala server. It is important not to hold any locks (in
// particular the parent query's lock_) while invoking HS2 functions to avoid deadlock.
Status ChildQuery::ExecAndFetch() {
  const TUniqueId& session_id = parent_request_state_->session_id();
  const TUniqueId& session_secret = parent_request_state_->session()->secret;
  VLOG_QUERY << "Executing child query: " << query_ << " in session "
             << PrintId(session_id);

  // Create HS2 request and response structs.
  TExecuteStatementResp exec_stmt_resp;
  TExecuteStatementReq exec_stmt_req;
  ImpalaServer::TUniqueIdToTHandleIdentifier(session_id, session_secret,
      &exec_stmt_req.sessionHandle.sessionId);
  exec_stmt_req.__set_statement(query_);
  SetQueryOptions(&exec_stmt_req);
  exec_stmt_req.confOverlay[PARENT_QUERY_OPT] =
      PrintId(parent_request_state_->query_id());

  // Starting executing of the child query and setting is_running are not made atomic
  // because holding a lock while calling into the parent_server_ may result in deadlock.
  // Cancellation is checked immediately after setting is_running_ below.
  // The order of the following three steps is important:
  // 1. Start query execution before setting is_running_ to ensure that
  //    a concurrent Cancel() initiated by the parent is a no-op.
  // 2. Set the hs2_handle_ before is_running_ to ensure there is a proper handle
  //    for Cancel() to use.
  // 3. Set is_running_ to true. Once is_running_ is set, the child query
  //    can be cancelled via Cancel().
  RETURN_IF_ERROR(IsCancelled());
  parent_server_->ExecuteStatement(exec_stmt_resp, exec_stmt_req);
  hs2_handle_ = exec_stmt_resp.operationHandle;
  {
    lock_guard<mutex> l(lock_);
    is_running_ = true;
  }
  RETURN_IF_ERROR(Status(exec_stmt_resp.status));

  TUniqueId query_id;
  TUniqueId secret_unused;
  // Add the query id to the profile, in case we exit with an error before we get the
  // full profile below. If we get an error here, just ignore it and continue.
  if (ImpalaServer::THandleIdentifierToTUniqueId(
          hs2_handle_.operationId, &query_id, &secret_unused)
          .ok()) {
    profile_->set_name(Substitute("$0 (id=$1)", profile_->name(), PrintId(query_id)));
  }

  TGetResultSetMetadataReq meta_req;
  meta_req.operationHandle = exec_stmt_resp.operationHandle;
  RETURN_IF_ERROR(IsCancelled());
  parent_server_->GetResultSetMetadata(meta_resp_, meta_req);
  RETURN_IF_ERROR(Status(meta_resp_.status));

  // Fetch all results.
  TFetchResultsReq fetch_req;
  fetch_req.operationHandle = exec_stmt_resp.operationHandle;
  fetch_req.maxRows = 1024;
  Status status;
  do {
    RETURN_IF_ERROR(IsCancelled());
    parent_server_->FetchResults(fetch_resp_, fetch_req);
    status = Status(fetch_resp_.status);
  } while (status.ok() && fetch_resp_.hasMoreRows);
  RETURN_IF_ERROR(IsCancelled());

  TCloseOperationResp close_resp;
  TCloseOperationReq close_req;
  close_req.operationHandle = exec_stmt_resp.operationHandle;
  parent_server_->CloseOperation(close_resp, close_req);
  {
    lock_guard<mutex> l(lock_);
    is_running_ = false;
  }
  RETURN_IF_ERROR(IsCancelled());

  // Get the runtime profile and add it to 'profile_'.
  TGetRuntimeProfileResp get_profile_resp;
  TGetRuntimeProfileReq get_profile_req;
  get_profile_req.operationHandle = exec_stmt_resp.operationHandle;
  get_profile_req.format = TRuntimeProfileFormat::THRIFT;
  ImpalaServer::TUniqueIdToTHandleIdentifier(
      session_id, session_id, &get_profile_req.sessionHandle.sessionId);
  parent_server_->GetRuntimeProfile(get_profile_resp, get_profile_req);
  if (Status(get_profile_resp.status).ok()) {
    RuntimeProfile* runtime_profile =
        RuntimeProfile::CreateFromThrift(profile_pool_, get_profile_resp.thrift_profile);
    if (runtime_profile != nullptr) profile_->AddChild(runtime_profile);
  }

  // Don't overwrite error from fetch. A failed fetch unregisters the query and we want to
  // preserve the original error status (e.g., CANCELLED).
  if (status.ok()) status = Status(close_resp.status);
  return status;
}

template <typename T>
void PrintQueryOptionValue (const T& option, stringstream& val) {
  val << option;
}

void PrintQueryOptionValue(const impala::TCompressionCodec& compression_codec,
    stringstream& val) {
  if (compression_codec.codec != THdfsCompression::ZSTD) {
    val << compression_codec.codec;
  } else {
    val << compression_codec.codec << ":" << compression_codec.compression_level;
  }
}

void PrintQueryOptionValue(const set<impala::TRuntimeFilterType::type>& filter_types,
    stringstream& val) {
  val << filter_types;
}

void PrintQueryOptionValue(const std::set<int32_t>& filter_ids, stringstream& val) {
  val << filter_ids;
}

void ChildQuery::SetQueryOptions(TExecuteStatementReq* exec_stmt_req) {
  map<string, string> conf;
  const TQueryOptions& parent_options =
      parent_request_state_->exec_request().query_options;
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)\
  if (parent_options.__isset.NAME) {\
    stringstream val;\
    PrintQueryOptionValue(parent_options.NAME, val);\
    conf[#ENUM] = val.str();\
  }
#define REMOVED_QUERY_OPT_FN(NAME, ENUM)
  QUERY_OPTS_TABLE
#undef QUERY_OPT_FN
#undef REMOVED_QUERY_OPT_FN
  // Ignore debug actions on child queries because they may cause deadlock.
  map<string, string>::iterator it = conf.find("DEBUG_ACTION");
  if (it != conf.end()) conf.erase(it);

  if (parent_request_state_->exec_request().request_pool_set_by_frontend) {
    // Remove REQUEST_POOL if this option was set by Frontend auto-scaling.
    it = conf.find("REQUEST_POOL");
    if (it != conf.end()) conf.erase(it);
  }
  exec_stmt_req->__set_confOverlay(conf);
}

void ChildQuery::Cancel() {
  // Do not hold lock_ while calling into parent_server_ to avoid deadlock.
  {
    lock_guard<mutex> l(lock_);
    is_cancelled_ = true;
    if (!is_running_) return;
    is_running_ = false;
  }
  TUniqueId session_id;
  TUniqueId secret_unused;
  // Ignore return statuses because they are not actionable.
  Status status = ImpalaServer::THandleIdentifierToTUniqueId(hs2_handle_.operationId,
      &session_id, &secret_unused);
  if (status.ok()) {
    VLOG_QUERY << "Cancelling and closing child query with operation id: " <<
        PrintId(session_id);
  } else {
    VLOG_QUERY << "Cancelling and closing child query. Failed to get query id: " <<
        status;
  }
  TCancelOperationResp cancel_resp;
  TCancelOperationReq cancel_req;
  cancel_req.operationHandle = hs2_handle_;
  parent_server_->CancelOperation(cancel_resp, cancel_req);
  TCloseOperationResp close_resp;
  TCloseOperationReq close_req;
  close_req.operationHandle = hs2_handle_;
  parent_server_->CloseOperation(close_resp, close_req);
}

Status ChildQuery::IsCancelled() {
  lock_guard<mutex> l(lock_);
  if (!is_cancelled_) return Status::OK();
  return Status::CANCELLED;
}

ChildQueryExecutor::ChildQueryExecutor() : is_cancelled_(false), is_running_(false) {}
ChildQueryExecutor::~ChildQueryExecutor() {
  DCHECK(!is_running_);
}

Status ChildQueryExecutor::ExecAsync(vector<ChildQuery>&& child_queries) {
  DCHECK(!child_queries.empty());
  lock_guard<SpinLock> lock(lock_);
  DCHECK(child_queries_.empty());
  DCHECK(child_queries_thread_.get() == NULL);
  if (is_cancelled_) return Status::OK();
  child_queries_ = move(child_queries);
  RETURN_IF_ERROR(Thread::Create("query-exec-state", "async child queries",
      bind(&ChildQueryExecutor::ExecChildQueries, this), &child_queries_thread_));
  is_running_ = true;
  return Status::OK();
}

void ChildQueryExecutor::ExecChildQueries() {
  for (ChildQuery& child_query : child_queries_) {
    // Execute without holding 'lock_'.
    Status status = child_query.ExecAndFetch();
    if (!status.ok()) {
      lock_guard<SpinLock> lock(lock_);
      child_queries_status_ = status;
      break;
    }
  }

  {
    lock_guard<SpinLock> lock(lock_);
    is_running_ = false;
  }
}

Status ChildQueryExecutor::WaitForAll(vector<ChildQuery*>* completed_queries) {
  // Safe to read without lock since we don't call this concurrently with ExecAsync().
  if (child_queries_thread_ == NULL) {
    DCHECK(!is_running_);
    return Status::OK();
  }
  child_queries_thread_->Join();

  // Safe to read below fields without 'lock_' because they are immutable after the
  // thread finishes.
  RETURN_IF_ERROR(child_queries_status_);
  for (ChildQuery& child_query : child_queries_) {
    completed_queries->push_back(&child_query);
  }
  return Status::OK();
}

void ChildQueryExecutor::Cancel() {
  {
    lock_guard<SpinLock> l(lock_);
    // Prevent more child queries from starting. After this critical section,
    // 'child_queries_' will not be modified.
    is_cancelled_ = true;
    if (!is_running_) return;
    DCHECK_EQ(child_queries_thread_ == NULL, child_queries_.empty());
  }

  // Cancel child queries without holding 'lock_'.
  // Safe because 'child_queries_' and 'child_queries_thread_' are immutable after
  // cancellation.
  for (ChildQuery& child_query : child_queries_) {
    child_query.Cancel();
  }
  child_queries_thread_->Join();
}
}
