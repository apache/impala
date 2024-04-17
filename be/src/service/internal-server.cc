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

#include <memory>
#include <mutex>

#include "common/status.h"
#include "gen-cpp/ErrorCodes_types.h"
#include "gen-cpp/Query_types.h"
#include "gen-cpp/Types_types.h"
#include "rpc/thrift-server.h"
#include "runtime/query-driver.h"
#include "service/client-request-state.h"
#include "service/impala-server.h"
#include "util/debug-util.h"
#include "util/uid-util.h"

using namespace std;

namespace impala {

Status ImpalaServer::OpenSession(const string& user_name, TUniqueId& new_session_id,
    const QueryOptionMap& query_opts) {
  shared_ptr<ThriftServer::ConnectionContext> conn_ctx =
      make_shared<ThriftServer::ConnectionContext>();
  conn_ctx->connection_id = RandomUniqueID();
  conn_ctx->server_name = ImpalaServer::INTERNAL_SERVER_NAME;
  conn_ctx->username = user_name;
  conn_ctx->network_address.hostname = "in-memory.localhost";

  ConnectionStart(*conn_ctx.get());

  {
    lock_guard<mutex> l(connection_to_sessions_map_lock_);
    new_session_id = *connection_to_sessions_map_[conn_ctx->connection_id].cbegin();
  }

  {
    lock_guard<mutex> l(internal_server_connections_lock_);
    internal_server_connections_.insert(make_pair(new_session_id, conn_ctx));
  }

  shared_ptr<ImpalaServer::SessionState> session_state;
  {
    lock_guard<mutex> l(session_state_map_lock_);
    session_state = session_state_map_[new_session_id];
  }

  for (const auto& iter : query_opts) {
    RETURN_IF_ERROR(SetQueryOption(iter.first, iter.second,
        &session_state->set_query_options, &session_state->set_query_options_mask));
  }

  MarkSessionActive(session_state);

  return Status::OK();
} // ImpalaServer::OpenSession

bool ImpalaServer::CloseSession(const TUniqueId& session_id) {
  {
    lock_guard<mutex> l(session_state_map_lock_);

    auto iter = session_state_map_.find(session_id);
    if (iter == session_state_map_.end()) {
      return false;
    }

    MarkSessionInactive(iter->second);
  }

  {
    lock_guard<mutex> l(internal_server_connections_lock_, adopt_lock);
    internal_server_connections_lock_.lock();

    const auto iter = internal_server_connections_.find(session_id);
    if (iter != internal_server_connections_.end()) {
      internal_server_connections_lock_.unlock();
      ConnectionEnd(*iter->second.get());
      internal_server_connections_lock_.lock();
      internal_server_connections_.erase(iter);
    }
  }

  return true;
} // ImpalaServer::CloseSession

Status ImpalaServer::ExecuteIgnoreResults(const string& user_name, const string& sql,
    const QueryOptionMap& query_opts, const bool persist_in_db, TUniqueId* query_id) {
  TUniqueId session_id;
  TUniqueId internal_query_id;
  Status result;

  result = SubmitAndWait(user_name, sql, session_id, internal_query_id, query_opts,
      persist_in_db);

  if (query_id != nullptr) {
    *query_id = internal_query_id;
  }

  if (!UUIDEmpty(internal_query_id)) {
    CloseQuery(internal_query_id);
  }

  CloseSession(session_id);

  return result;
} //ImpalaServer::ExecuteIgnoreResults

Status ImpalaServer::ExecuteAndFetchAllText(const std::string& user_name,
    const std::string& sql, query_results& results, results_columns* columns,
    TUniqueId* query_id){
  TUniqueId session_id;
  TUniqueId internal_query_id;
  Status result;

  result = SubmitAndWait(user_name, sql, session_id, internal_query_id);

  if (query_id != nullptr) {
    *query_id = internal_query_id;
  }

  if (result.ok()) {
    result = FetchAllRows(internal_query_id, results, columns);
  }

  if (!UUIDEmpty(internal_query_id)) {
    CloseQuery(internal_query_id);
  }

  CloseSession(session_id);

  return result;
} // ImpalaServer::ExecuteAndFetchAllText

Status ImpalaServer::SubmitAndWait(const string& user_name, const string& sql,
    TUniqueId& new_session_id, TUniqueId& new_query_id, const QueryOptionMap& query_opts,
    const bool persist_in_db) {

  RETURN_IF_ERROR(OpenSession(user_name, new_session_id, query_opts));
  RETURN_IF_ERROR(SubmitQuery(sql, new_session_id, new_query_id, persist_in_db));

  return WaitForResults(new_query_id);
} // ImpalaServer::SubmitAndWait

Status ImpalaServer::WaitForResults(TUniqueId& query_id) {
  QueryHandle query_handle;
  RETURN_IF_ERROR(GetActiveQueryHandle(query_id, &query_handle));

  RETURN_IF_ERROR(query_handle->WaitAsync());

  int64_t block_wait_time;
  bool timed_out;
  RETURN_IF_ERROR(WaitForResults(query_handle->query_id(), &query_handle,
      &block_wait_time, &timed_out));
  query_id = query_handle->query_id();

  if (timed_out) {
    return Status::Expected("query timed out waiting for results");
  }

  return Status::OK();
} // ImpalaServer::WaitForResults

Status ImpalaServer::SubmitQuery(const string& sql, const TUniqueId& session_id,
    TUniqueId& new_query_id, const bool persist_in_db) {

  // build a query context
  TQueryCtx query_context;
  query_context.client_request.stmt = sql;

  // locate the previously opened session
  shared_ptr<SessionState> session_state;
  {
    lock_guard<mutex> l(session_state_map_lock_);

    const auto iter = session_state_map_.find(session_id);
    if (iter == session_state_map_.end()) {
      return Status::Expected(TErrorCode::GENERAL, PrintId(session_id));
    }

    session_state = iter->second;
  }

  session_state->ToThrift(session_state->session_id, &query_context.session);

  query_context.client_request.query_options = session_state->QueryOptions();
  AddPoolConfiguration(&query_context, ~session_state->set_query_options_mask);

  QueryHandle query_handle;
  RETURN_IF_ERROR(Execute(&query_context, session_state, &query_handle, nullptr,
      persist_in_db));
  new_query_id = query_handle->query_id();

  return SetQueryInflight(session_state, query_handle);
} // ImpalaServer::SubmitQuery

Status ImpalaServer::FetchAllRows(const TUniqueId& query_id, query_results& results,
    results_columns* columns) {
  QueryResultSet* result_set;
  const TResultSetMetadata* results_metadata;
  vector<string> row_set;

  QueryHandle query_handle;
  RETURN_IF_ERROR(GetActiveQueryHandle(query_id, &query_handle));

  {
    lock_guard<mutex> l1(*query_handle->fetch_rows_lock());
    lock_guard<mutex> l2(*query_handle->lock());

    if (query_handle->num_rows_fetched() == 0) {
      query_handle->set_fetched_rows();
    }

    results_metadata = query_handle->result_metadata();

    // populate column vector if provided by the user
    if (columns != nullptr) {
      for (int i=0; i<results_metadata->columns.size(); i++) {
        // TODO: As of today, the ODBC driver does not support boolean and timestamp data
        // type but it should. This is tracked by ODBC-189. We should verify that our
        // boolean and timestamp type are correctly recognized when ODBC-189 is closed.
        // TODO: Handle complex types.
        const TColumnType& type = results_metadata->columns[i].columnType;
        columns->emplace_back(make_pair(results_metadata->columns[i].columnName,
            ColumnTypeToBeeswaxTypeString(type)));
      }
    }

    result_set = QueryResultSet::CreateAsciiQueryResultSet(
        *results_metadata, &row_set, true);
  }

  int64_t block_wait_time = 30000000;
  while (!query_handle->eos()) {
    lock_guard<mutex> l1(*query_handle->fetch_rows_lock());
    lock_guard<mutex> l2(*query_handle->lock());

    row_set.clear();

    RETURN_IF_ERROR(query_handle->FetchRows(10, result_set, block_wait_time));
    results->insert(results->cend(), row_set.cbegin(), row_set.cend());
  }

  return Status::OK();
} // ImpalaServer::FetchAllRows

void ImpalaServer::CloseQuery(const TUniqueId& query_id) {
  QueryHandle query_handle;
  Status found = GetActiveQueryHandle(query_id, &query_handle);

  if (!found.ok()) {
    return;
  }

  UnregisterQueryDiscardResult(query_handle->query_id(), false);
} // ImpalaServer::CloseQuery

void ImpalaServer::GetConnectionContextList(
    ThriftServer::ConnectionContextList* connection_contexts) {
  lock_guard<mutex> l(internal_server_connections_lock_);

  for(auto iter = internal_server_connections_.cbegin();
      iter != internal_server_connections_.cend(); iter++) {
    connection_contexts->push_back(iter->second);
  }
} // ImpalaServer::GetConnectionContextList

} // namespace impala
