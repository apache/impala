// Copyright 2012 Cloudera Inc.
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

#include "service/child-query.h"
#include "service/impala-server.inline.h"
#include "service/query-exec-state.h"
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
  const TUniqueId& session_id = parent_exec_state_->session_id();
  VLOG_QUERY << "Executing child query: " << query_ << " in session "
             << PrintId(session_id);

  // Create HS2 request and response structs.
  Status status;
  TExecuteStatementResp exec_stmt_resp;
  TExecuteStatementReq exec_stmt_req;
  ImpalaServer::TUniqueIdToTHandleIdentifier(session_id, session_id,
      &exec_stmt_req.sessionHandle.sessionId);
  exec_stmt_req.__set_statement(query_);
  SetQueryOptions(parent_exec_state_->exec_request().query_options, &exec_stmt_req);
  exec_stmt_req.confOverlay[PARENT_QUERY_OPT] = PrintId(parent_exec_state_->query_id());

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
  status = exec_stmt_resp.status;
  RETURN_IF_ERROR(status);

  TGetResultSetMetadataReq meta_req;
  meta_req.operationHandle = exec_stmt_resp.operationHandle;
  RETURN_IF_ERROR(IsCancelled());
  parent_server_->GetResultSetMetadata(meta_resp_, meta_req);
  status = meta_resp_.status;
  RETURN_IF_ERROR(status);

  // Fetch all results.
  TFetchResultsReq fetch_req;
  fetch_req.operationHandle = exec_stmt_resp.operationHandle;
  fetch_req.maxRows = 1024;
  do {
    RETURN_IF_ERROR(IsCancelled());
    parent_server_->FetchResults(fetch_resp_, fetch_req);
    status = fetch_resp_.status;
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

  // Don't overwrite error from fetch. A failed fetch unregisters the query and we want to
  // preserve the original error status (e.g., CANCELLED).
  if (status.ok()) status = close_resp.status;
  return status;
}

void ChildQuery::SetQueryOptions(const TQueryOptions& parent_options,
    TExecuteStatementReq* exec_stmt_req) {
  map<string, string> conf;
#define QUERY_OPT_FN(NAME, ENUM)\
  if (parent_options.__isset.NAME) {\
    stringstream val;\
    val << parent_options.NAME;\
    conf[#ENUM] = val.str();\
  }
  QUERY_OPTS_TABLE
#undef QUERY_OPT_FN
  // Ignore debug actions on child queries because they may cause deadlock.
  map<string, string>::iterator it = conf.find("DEBUG_ACTION");
  if (it != conf.end()) conf.erase(it);
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
  VLOG_QUERY << "Cancelling and closing child query with operation id: "
             << hs2_handle_.operationId.guid;
  // Ignore return statuses because they are not actionable.
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

}
