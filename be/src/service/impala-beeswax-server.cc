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

#include "service/impala-server.h"

#include "common/logging.h"
#include "gen-cpp/Frontend_types.h"
#include "rpc/thrift-util.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "runtime/raw-value.inline.h"
#include "runtime/timestamp-value.h"
#include "runtime/query-driver.h"
#include "service/client-request-state.h"
#include "service/frontend.h"
#include "service/query-options.h"
#include "service/query-result-set.h"
#include "util/auth-util.h"
#include "util/debug-util.h"
#include "util/webserver.h"
#include "util/runtime-profile.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace apache::thrift;
using namespace apache::hive::service::cli::thrift;
using namespace beeswax;

#define RAISE_IF_ERROR(stmt, ex_type)                           \
  do {                                                          \
    const Status& _status = (stmt);                          \
    if (UNLIKELY(!_status.ok())) {                           \
      RaiseBeeswaxException(_status.GetDetail(), ex_type);   \
    }                                                           \
  } while (false)

DECLARE_bool(ping_expose_webserver_url);
DECLARE_string(anonymous_user_name);

namespace impala {

void ImpalaServer::query(beeswax::QueryHandle& beeswax_handle, const Query& query) {
  VLOG_QUERY << "query(): query=" << query.query;
  RAISE_IF_ERROR(CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  RAISE_IF_ERROR(
      session_handle.WithBeeswaxSession(ThriftServer::GetThreadConnectionId(), &session),
      SQLSTATE_GENERAL_ERROR);
  TQueryCtx query_ctx;
  // raise general error for request conversion error;
  RAISE_IF_ERROR(QueryToTQueryContext(query, &query_ctx), SQLSTATE_GENERAL_ERROR);

  // raise Syntax error or access violation; it's likely to be syntax/analysis error
  // TODO: that may not be true; fix this
  QueryHandle query_handle;
  RAISE_IF_ERROR(Execute(&query_ctx, session, &query_handle, nullptr),
      SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);

  // start thread to wait for results to become available, which will allow
  // us to advance query state to FINISHED or EXCEPTION
  Status status = query_handle->WaitAsync();
  if (!status.ok()) {
    discard_result(UnregisterQuery(query_handle->query_id(), false, &status));
    RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }
  // Once the query is running do a final check for session closure and add it to the
  // set of in-flight queries.
  status = SetQueryInflight(session, query_handle);
  if (!status.ok()) {
    discard_result(UnregisterQuery(query_handle->query_id(), false, &status));
    RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }
  TUniqueIdToBeeswaxHandle(query_handle->query_id(), &beeswax_handle);
}

void ImpalaServer::executeAndWait(beeswax::QueryHandle& beeswax_handle,
    const Query& query, const LogContextId& client_ctx) {
  VLOG_QUERY << "executeAndWait(): query=" << query.query;
  RAISE_IF_ERROR(CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  RAISE_IF_ERROR(
      session_handle.WithBeeswaxSession(ThriftServer::GetThreadConnectionId(), &session),
      SQLSTATE_GENERAL_ERROR);
  TQueryCtx query_ctx;
  // raise general error for request conversion error;
  RAISE_IF_ERROR(QueryToTQueryContext(query, &query_ctx), SQLSTATE_GENERAL_ERROR);

  DCHECK(session != nullptr);  // The session should exist.
  {
    // The session is created when the client connects. Depending on the underlying
    // transport, the username may be known at that time. If the username hasn't been set
    // yet, set it now.
    lock_guard<mutex> l(session->lock);
    if (session->connected_user.empty()) {
      session->connected_user = query.hadoop_user.empty() ?
          FLAGS_anonymous_user_name : query.hadoop_user;
    }
  }

  // raise Syntax error or access violation; it's likely to be syntax/analysis error
  // TODO: that may not be true; fix this
  QueryHandle query_handle;
  RAISE_IF_ERROR(Execute(&query_ctx, session, &query_handle, nullptr),
      SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);

  // Once the query is running do a final check for session closure and add it to the
  // set of in-flight queries.
  Status status = SetQueryInflight(session, query_handle);
  if (!status.ok()) {
    discard_result(UnregisterQuery(query_handle->query_id(), false, &status));
    RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }
  // block until results are ready
  query_handle->Wait();
  {
    lock_guard<mutex> l(*query_handle->lock());
    status = query_handle->query_status();
  }
  if (!status.ok()) {
    discard_result(UnregisterQuery(query_handle->query_id(), false, &status));
    RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }

  TUniqueIdToBeeswaxHandle(query_handle->query_id(), &beeswax_handle);

  // If the input log context id is an empty string, then create a new number and
  // set it to _return. Otherwise, set _return with the input log context
  beeswax_handle.log_context = client_ctx.empty() ? beeswax_handle.id : client_ctx;
}

void ImpalaServer::explain(QueryExplanation& query_explanation, const Query& query) {
  // Translate Beeswax Query to Impala's QueryRequest and then set the explain plan bool
  // before shipping to FE
  VLOG_QUERY << "explain(): query=" << query.query;
  RAISE_IF_ERROR(CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);

  TQueryCtx query_ctx;
  RAISE_IF_ERROR(QueryToTQueryContext(query, &query_ctx), SQLSTATE_GENERAL_ERROR);

  RAISE_IF_ERROR(
      exec_env_->frontend()->GetExplainPlan(query_ctx, &query_explanation.textual),
      SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);
  query_explanation.__isset.textual = true;
  VLOG_QUERY << "explain():\nstmt=" << query_ctx.client_request.stmt
             << "\nplan: " << query_explanation.textual;
}

void ImpalaServer::fetch(Results& query_results,
    const beeswax::QueryHandle& beeswax_handle, const bool start_over,
    const int32_t fetch_size) {
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(
        ThriftServer::GetThreadConnectionId(), &session), SQLSTATE_GENERAL_ERROR);

  if (start_over) {
    // We can't start over. Raise "Optional feature not implemented"
    RaiseBeeswaxException(
        "Does not support start over", SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED);
  }

  TUniqueId query_id;
  BeeswaxHandleToTUniqueId(beeswax_handle, &query_id);
  VLOG_ROW << "fetch(): query_id=" << PrintId(query_id) << " fetch_size=" << fetch_size;

  QueryHandle query_handle;
  RAISE_IF_ERROR(GetActiveQueryHandle(query_id, &query_handle), SQLSTATE_GENERAL_ERROR);

  // Validate that query can be accessed by user.
  RAISE_IF_ERROR(CheckClientRequestSession(session.get(),
                     query_handle->effective_user(), query_id),
      SQLSTATE_GENERAL_ERROR);
  Status status =
      FetchInternal(query_id, start_over, fetch_size, &query_results);
  VLOG_ROW << "fetch result: #results=" << query_results.data.size()
           << " has_more=" << (query_results.has_more ? "true" : "false");
  if (!status.ok()) {
    discard_result(UnregisterQuery(query_id, false, &status));
    RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }
}

string ImpalaServer::ColumnTypeToBeeswaxTypeString(const TColumnType& type) {
  if (type.types.size() == 1) {
    DCHECK_EQ(TTypeNodeType::SCALAR, type.types[0].type);
    DCHECK(type.types[0].__isset.scalar_type);
    return TypeToOdbcString(type);
  } else if (type.types[0].type == TTypeNodeType::ARRAY
      || type.types[0].type == TTypeNodeType::MAP
      || type.types[0].type == TTypeNodeType::STRUCT) {
    DCHECK_GT(type.types.size(), 1);
    // TODO (IMPALA-11041): consider returning the real type
    return "string";
  } else {
    DCHECK(false);
    return "";
  }
}

// TODO: Handle struct types.
void ImpalaServer::get_results_metadata(ResultsMetadata& results_metadata,
    const beeswax::QueryHandle& beeswax_handle) {
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(
      ThriftServer::GetThreadConnectionId(), &session), SQLSTATE_GENERAL_ERROR);

  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  BeeswaxHandleToTUniqueId(beeswax_handle, &query_id);
  VLOG_QUERY << "get_results_metadata(): query_id=" << PrintId(query_id);

  QueryHandle query_handle;
  RAISE_IF_ERROR(GetActiveQueryHandle(query_id, &query_handle), SQLSTATE_GENERAL_ERROR);

  // Validate that query can be accessed by user.
  RAISE_IF_ERROR(CheckClientRequestSession(session.get(), query_handle->effective_user(),
      query_id), SQLSTATE_GENERAL_ERROR);

  {
    lock_guard<mutex> l(*query_handle->lock());

    // Convert TResultSetMetadata to Beeswax.ResultsMetadata
    const TResultSetMetadata* result_set_md = query_handle->result_metadata();
    results_metadata.__isset.schema = true;
    results_metadata.schema.__isset.fieldSchemas = true;
    results_metadata.schema.fieldSchemas.resize(result_set_md->columns.size());
    for (int i = 0; i < results_metadata.schema.fieldSchemas.size(); ++i) {
      const TColumnType& type = result_set_md->columns[i].columnType;
      results_metadata.schema.fieldSchemas[i].__set_type(
          ColumnTypeToBeeswaxTypeString(type));
      // Fill column name
      results_metadata.schema.fieldSchemas[i].__set_name(
          result_set_md->columns[i].columnName);
    }
  }

  // ODBC-187 - ODBC can only take "\t" as the delimiter and ignores whatever is set here.
  results_metadata.__set_delim("\t");

  // results_metadata.table_dir and in_tablename are not applicable.
}

void ImpalaServer::close(const beeswax::QueryHandle& beeswax_handle) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  TUniqueId query_id;
  BeeswaxHandleToTUniqueId(beeswax_handle, &query_id);

  // Impala-shell and administrative tools can call this from a different connection,
  // e.g. to allow an admin to force-terminate queries. We should allow the operation to
  // proceed without validating the session/query relation so that workflows don't
  // get broken. In future we could check that the users match OR that the user has
  // admin priviliges on the server.

  VLOG_QUERY << "close(): query_id=" << PrintId(query_id);
  // TODO: do we need to raise an exception if the query state is EXCEPTION?
  // TODO: use timeout to get rid of unwanted query_handle.
  RAISE_IF_ERROR(UnregisterQuery(query_id, true), SQLSTATE_GENERAL_ERROR);
}

beeswax::QueryState::type ImpalaServer::get_state(
    const beeswax::QueryHandle& beeswax_handle) {
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(
      ThriftServer::GetThreadConnectionId(), &session), SQLSTATE_GENERAL_ERROR);
  TUniqueId query_id;
  BeeswaxHandleToTUniqueId(beeswax_handle, &query_id);
  VLOG_ROW << "get_state(): query_id=" << PrintId(query_id);

  QueryHandle query_handle;
  Status status = GetActiveQueryHandle(query_id, &query_handle);
  // GetActiveQueryHandle may return the query's status from being cancelled. If not an
  // invalid query handle, we can assume that error statuses reflect a query in the
  // EXCEPTION state.
  if (!status.ok() && status.code() != TErrorCode::INVALID_QUERY_HANDLE) {
    return beeswax::QueryState::EXCEPTION;
  }
  RAISE_IF_ERROR(status, SQLSTATE_GENERAL_ERROR);

  // Validate that query can be accessed by user.
  RAISE_IF_ERROR(CheckClientRequestSession(session.get(), query_handle->effective_user(),
      query_id), SQLSTATE_GENERAL_ERROR);
  // Take the lock to ensure that if the client sees a query_state == EXCEPTION, it is
  // guaranteed to see the error query_status.
  lock_guard<mutex> l(*query_handle->lock());
  beeswax::QueryState::type query_state = query_handle->BeeswaxQueryState();
  DCHECK_EQ(query_state == beeswax::QueryState::EXCEPTION
          || query_handle->retry_state() == ClientRequestState::RetryState::RETRYING
          || query_handle->retry_state() == ClientRequestState::RetryState::RETRIED,
      !query_handle->query_status().ok());
  return query_state;
}

void ImpalaServer::echo(string& echo_string, const string& input_string) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  echo_string = input_string;
}

void ImpalaServer::clean(const LogContextId& log_context) {
}

void ImpalaServer::get_log(string& log, const LogContextId& context) {
  shared_ptr<SessionState> session;
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(
      ThriftServer::GetThreadConnectionId(), &session), SQLSTATE_GENERAL_ERROR);
  // LogContextId is the same as QueryHandle.id
  beeswax::QueryHandle beeswax_handle;
  beeswax_handle.__set_id(context);
  TUniqueId query_id;
  BeeswaxHandleToTUniqueId(beeswax_handle, &query_id);

  QueryHandle query_handle;
  RAISE_IF_ERROR(GetActiveQueryHandle(query_id, &query_handle), SQLSTATE_GENERAL_ERROR);

  // Validate that query can be accessed by user.
  RAISE_IF_ERROR(
      CheckClientRequestSession(session.get(), query_handle->effective_user(), query_id),
      SQLSTATE_GENERAL_ERROR);
  stringstream error_log_ss;

  if (query_handle->IsRetriedQuery()) {
    QueryHandle original_query_handle;
    Status status = GetQueryHandle(query_id, &original_query_handle);
    if (UNLIKELY(!status.ok())) {
      VLOG(1) << "Error in get_log, could not get query handle: " << status.GetDetail();
      RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
    }
    DCHECK(!original_query_handle->query_status().ok());
    error_log_ss << Substitute(GET_LOG_QUERY_RETRY_INFO_FORMAT,
        original_query_handle->query_status().GetDetail(),
        PrintId(query_handle->query_id()));
  }
  {
    // Take the lock to ensure that if the client sees a exec_state == ERROR, it is
    // guaranteed to see the error query_status.
    lock_guard<mutex> l(*query_handle->lock());
    DCHECK_EQ(query_handle->exec_state() == ClientRequestState::ExecState::ERROR,
        !query_handle->query_status().ok());
    // If the query status is !ok, include the status error message at the top of the log.
    if (!query_handle->query_status().ok()) {
      error_log_ss << query_handle->query_status().GetDetail() << "\n";
    }
  }

  // Add warnings from analysis
  for (const string& warning : query_handle->GetAnalysisWarnings()) {
    error_log_ss << warning << "\n";
  }

  // Add warnings from execution
  if (query_handle->GetCoordinator() != nullptr) {
    const std::string coord_errors = query_handle->GetCoordinator()->GetErrorLog();
    if (!coord_errors.empty()) error_log_ss << coord_errors << "\n";
  }
  log = error_log_ss.str();
  VLOG_RPC << "get_log(): query_id=" << PrintId(query_id) << ", log=" << log;
}

void ImpalaServer::get_default_configuration(vector<ConfigVariable>& configurations,
    const bool include_hadoop) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  configurations.insert(configurations.end(), default_configs_.begin(),
      default_configs_.end());
}

void ImpalaServer::dump_config(string& config) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  config = "";
}

void ImpalaServer::Cancel(impala::TStatus& tstatus,
    const beeswax::QueryHandle& beeswax_handle) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  BeeswaxHandleToTUniqueId(beeswax_handle, &query_id);

  // Impala-shell and administrative tools can call this from a different connection,
  // e.g. to allow an admin to force-terminate queries. We should allow the operation to
  // proceed without validating the session/query relation so that workflows don't
  // get broken. In future we could check that the users match OR that the user has
  // admin priviliges on the server.
  RAISE_IF_ERROR(CancelInternal(query_id), SQLSTATE_GENERAL_ERROR);
  tstatus.status_code = TErrorCode::OK;
}

void ImpalaServer::CloseInsert(TDmlResult& dml_result,
    const beeswax::QueryHandle& beeswax_handle) {
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(
      ThriftServer::GetThreadConnectionId(), &session), SQLSTATE_GENERAL_ERROR);
  TUniqueId query_id;
  BeeswaxHandleToTUniqueId(beeswax_handle, &query_id);
  VLOG_QUERY << "CloseInsert(): query_id=" << PrintId(query_id);

  // CloseInsertInternal() will validates that 'session' has access to 'query_id'.
  Status status = CloseInsertInternal(session.get(), query_id, &dml_result);
  if (!status.ok()) {
    RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }
}

// Gets the runtime profile string for the given query handle and stores the result in
// the profile_output parameter. Raises a BeeswaxException if there are any errors
// getting the profile, such as no matching queries found.
void ImpalaServer::GetRuntimeProfile(
    string& profile_output, const beeswax::QueryHandle& beeswax_handle) {
  ScopedSessionState session_handle(this);
  const TUniqueId& session_id = ThriftServer::GetThreadConnectionId();
  stringstream ss;
  shared_ptr<SessionState> session;
  RAISE_IF_ERROR(
      session_handle.WithBeeswaxSession(session_id, &session), SQLSTATE_GENERAL_ERROR);
  if (session == NULL) {
    ss << Substitute("Invalid session id: $0", PrintId(session_id));
    RaiseBeeswaxException(ss.str(), SQLSTATE_GENERAL_ERROR);
  }
  TUniqueId query_id;
  BeeswaxHandleToTUniqueId(beeswax_handle, &query_id);

  VLOG_RPC << "GetRuntimeProfile(): query_id=" << PrintId(query_id);

  // If the query was retried, fetch the profile for the most recent attempt of the query
  // The original query profile should still be accessible via the web ui.
  QueryHandle query_handle;
  Status status = GetActiveQueryHandle(query_id, &query_handle);
  if (LIKELY(status.ok())) {
    query_id = query_handle->query_id();
  }

  // GetRuntimeProfile() will validate that the user has access to 'query_id'.
  RuntimeProfileOutput profile;
  profile.string_output = &ss;
  status = GetRuntimeProfileOutput(query_id, GetEffectiveUser(*session),
      TRuntimeProfileFormat::STRING, &profile);
  if (!status.ok()) {
    ss << "GetRuntimeProfile error: " << status.GetDetail();
    RaiseBeeswaxException(ss.str(), SQLSTATE_GENERAL_ERROR);
  }
  profile_output = ss.str();
}

void ImpalaServer::GetExecSummary(impala::TExecSummary& result,
      const beeswax::QueryHandle& beeswax_handle) {
  ScopedSessionState session_handle(this);
  const TUniqueId& session_id = ThriftServer::GetThreadConnectionId();
  shared_ptr<SessionState> session;
  RAISE_IF_ERROR(
      session_handle.WithBeeswaxSession(session_id, &session), SQLSTATE_GENERAL_ERROR);
  if (session == NULL) {
    stringstream ss;
    ss << Substitute("Invalid session id: $0", PrintId(session_id));
    RaiseBeeswaxException(ss.str(), SQLSTATE_GENERAL_ERROR);
  }
  TUniqueId query_id;
  BeeswaxHandleToTUniqueId(beeswax_handle, &query_id);
  VLOG_RPC << "GetExecSummary(): query_id=" << PrintId(query_id);
  // GetExecSummary() will validate that the user has access to 'query_id'.
  Status status = GetExecSummary(query_id, GetEffectiveUser(*session), &result);
  if (!status.ok()) RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
}

void ImpalaServer::PingImpalaService(TPingImpalaServiceResp& return_val) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithBeeswaxSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);

  VLOG_RPC << "PingImpalaService()";
  return_val.version = GetVersionString(true);
  if (ExecEnv::GetInstance()->get_enable_webserver() && FLAGS_ping_expose_webserver_url) {
    return_val.webserver_address = ExecEnv::GetInstance()->webserver()->url();
  } else {
    return_val.webserver_address = "";
  }
  VLOG_RPC << "PingImpalaService(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::ResetCatalog(impala::TStatus& status) {
  Status::DEPRECATED_RPC.ToThrift(&status);
}

void ImpalaServer::ResetTable(impala::TStatus& status, const TResetTableReq& request) {
  Status::DEPRECATED_RPC.ToThrift(&status);
}

Status ImpalaServer::QueryToTQueryContext(const Query& query,
    TQueryCtx* query_ctx) {
  query_ctx->client_request.stmt = query.query;
  VLOG_QUERY << "query: " << ThriftDebugString(query);
  QueryOptionsMask set_query_options_mask;
  {
    shared_ptr<SessionState> session;
    const TUniqueId& session_id = ThriftServer::GetThreadConnectionId();
    // OK to skip secret validation since 'session_id' comes from connection
    // and is trusted.
    RETURN_IF_ERROR(GetSessionState(session_id, SecretArg::SkipSecretCheck(), &session,
        /* mark_active= */ false));
    DCHECK(session != nullptr);
    {
      // The session is created when the client connects. Depending on the underlying
      // transport, the username may be known at that time. If the username hasn't been
      // set yet, set it now.
      lock_guard<mutex> l(session->lock);
      if (session->connected_user.empty()) {
        session->connected_user = query.hadoop_user.empty() ?
            FLAGS_anonymous_user_name : query.hadoop_user;
      }
      query_ctx->client_request.query_options = session->QueryOptions();
      set_query_options_mask = session->set_query_options_mask;
    }
    session->ToThrift(session_id, &query_ctx->session);
  }

  // Override default query options with Query.Configuration
  if (query.__isset.configuration) {
    TQueryOptions overlay;
    QueryOptionsMask overlay_mask;
    for (const string& option: query.configuration) {
      RETURN_IF_ERROR(ParseQueryOptions(option, &overlay, &overlay_mask));
    }
    OverlayQueryOptions(overlay, overlay_mask, &query_ctx->client_request.query_options);
    RETURN_IF_ERROR(ValidateQueryOptions(&overlay));
    set_query_options_mask |= overlay_mask;
  }

  // Only query options not set in the session or confOverlay can be overridden by the
  // pool options.
  AddPoolConfiguration(query_ctx, ~set_query_options_mask);
  VLOG_QUERY << "TClientRequest.queryOptions: "
             << ThriftDebugString(query_ctx->client_request.query_options);
  return Status::OK();
}

inline void ImpalaServer::TUniqueIdToBeeswaxHandle(
    const TUniqueId& query_id, beeswax::QueryHandle* beeswax_handle) {
  string query_id_str = PrintId(query_id);
  beeswax_handle->__set_id(query_id_str);
  beeswax_handle->__set_log_context(query_id_str);
}

inline void ImpalaServer::BeeswaxHandleToTUniqueId(
    const beeswax::QueryHandle& beeswax_handle, TUniqueId* query_id) {
  ParseId(beeswax_handle.id, query_id);
}

[[noreturn]] void ImpalaServer::RaiseBeeswaxException(
    const string& msg, const char* sql_state) {
  BeeswaxException exc;
  exc.__set_message(msg);
  exc.__set_SQLState(sql_state);
  throw exc;
}

Status ImpalaServer::FetchInternal(TUniqueId query_id, const bool start_over,
    const int32_t fetch_size, beeswax::Results* query_results) {
  bool timed_out = false;
  int64_t block_on_wait_time_us = 0;
  QueryHandle query_handle;
  RETURN_IF_ERROR(
      WaitForResults(query_id, &query_handle, &block_on_wait_time_us, &timed_out));
  if (timed_out) {
    query_results->__set_ready(false);
    query_results->__set_has_more(true);
    query_results->__isset.columns = false;
    query_results->__isset.data = false;
    return Status::OK();
  }

  int64_t start_time_ns = MonotonicNanos();
  lock_guard<mutex> frl(*query_handle->fetch_rows_lock());
  lock_guard<mutex> l(*query_handle->lock());
  int64_t lock_wait_time_ns = MonotonicNanos() - start_time_ns;
  query_handle->AddClientFetchLockWaitTime(lock_wait_time_ns);

  if (query_handle->num_rows_fetched() == 0) {
    query_handle->set_fetched_rows();
  }

  // Check for cancellation or an error.
  RETURN_IF_ERROR(query_handle->query_status());

  // ODBC-190: set Beeswax's Results.columns to work around bug ODBC-190;
  // TODO: remove the block of code when ODBC-190 is resolved.
  const TResultSetMetadata* result_metadata = query_handle->result_metadata();
  query_results->columns.resize(result_metadata->columns.size());
  for (int i = 0; i < result_metadata->columns.size(); ++i) {
    // TODO: As of today, the ODBC driver does not support boolean and timestamp data
    // type but it should. This is tracked by ODBC-189. We should verify that our
    // boolean and timestamp type are correctly recognized when ODBC-189 is closed.
    // TODO: Handle complex types.
    const TColumnType& type = result_metadata->columns[i].columnType;
    query_results->columns[i] = ColumnTypeToBeeswaxTypeString(type);
  }
  query_results->__isset.columns = true;

  // Results are always ready because we're blocking.
  query_results->__set_ready(true);
  // It's likely that ODBC doesn't care about start_row, but Hue needs it. For Hue,
  // start_row starts from zero, not one.
  query_results->__set_start_row(query_handle->num_rows_fetched());

  Status fetch_rows_status;
  query_results->data.clear();
  if (!query_handle->eos()) {
    scoped_ptr<QueryResultSet> result_set(QueryResultSet::CreateAsciiQueryResultSet(
        *query_handle->result_metadata(), &query_results->data,
        query_handle->query_options().stringify_map_keys));
    fetch_rows_status =
        query_handle->FetchRows(fetch_size, result_set.get(), block_on_wait_time_us);
  }
  query_results->__set_has_more(!query_handle->eos());
  query_results->__isset.data = true;

  return fetch_rows_status;
}

Status ImpalaServer::CloseInsertInternal(
    SessionState* session, const TUniqueId& query_id, TDmlResult* dml_result) {
  QueryHandle query_handle;
  RAISE_IF_ERROR(GetActiveQueryHandle(query_id, &query_handle), SQLSTATE_GENERAL_ERROR);
  RETURN_IF_ERROR(
      CheckClientRequestSession(session, query_handle->effective_user(), query_id));

  Status query_status;
  query_handle->GetDmlStats(dml_result, &query_status);
  RETURN_IF_ERROR(UnregisterQuery(query_id, true));
  return query_status;
}
}
