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
#include "service/impala-server.inline.h"

#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/unordered_set.hpp>
#include <jni.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <gtest/gtest.h>
#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "common/version.h"
#include "rpc/thrift-util.h"
#include "runtime/raw-value.h"
#include "runtime/exec-env.h"
#include "service/hs2-util.h"
#include "service/client-request-state.h"
#include "service/query-options.h"
#include "service/query-result-set.h"
#include "util/auth-util.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/runtime-profile-counters.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::adopt_lock_t;
using boost::algorithm::join;
using boost::algorithm::iequals;
using boost::uuids::uuid;
using namespace apache::hive::service::cli::thrift;
using namespace apache::hive::service::cli;
using namespace apache::thrift;
using namespace beeswax; // Converting QueryState
using namespace strings;

const TProtocolVersion::type MAX_SUPPORTED_HS2_VERSION =
    TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V6;

// HiveServer2 error returning macro
#define HS2_RETURN_ERROR(return_val, error_msg, error_state) \
  do { \
    return_val.status.__set_statusCode(thrift::TStatusCode::ERROR_STATUS); \
    return_val.status.__set_errorMessage(error_msg); \
    return_val.status.__set_sqlState(error_state); \
    return; \
  } while (false)

#define HS2_RETURN_IF_ERROR(return_val, status, error_state) \
  do { \
    if (UNLIKELY(!status.ok())) { \
      HS2_RETURN_ERROR(return_val, status.GetDetail(), error_state); \
      return; \
    } \
  } while (false)

DECLARE_string(hostname);
DECLARE_int32(webserver_port);
DECLARE_int32(idle_session_timeout);

namespace impala {

const string IMPALA_RESULT_CACHING_OPT = "impala.resultset.cache.size";

// Helper function to translate between Beeswax and HiveServer2 type
static TOperationState::type QueryStateToTOperationState(
    const beeswax::QueryState::type& query_state) {
  switch (query_state) {
    case beeswax::QueryState::CREATED: return TOperationState::INITIALIZED_STATE;
    case beeswax::QueryState::RUNNING: return TOperationState::RUNNING_STATE;
    case beeswax::QueryState::FINISHED: return TOperationState::FINISHED_STATE;
    case beeswax::QueryState::EXCEPTION: return TOperationState::ERROR_STATE;
    default: return TOperationState::UKNOWN_STATE;
  }
}

void ImpalaServer::ExecuteMetadataOp(const THandleIdentifier& session_handle,
    TMetadataOpRequest* request, TOperationHandle* handle, thrift::TStatus* status) {
  TUniqueId session_id;
  TUniqueId secret;
  Status unique_id_status =
      THandleIdentifierToTUniqueId(session_handle, &session_id, &secret);
  if (!unique_id_status.ok()) {
    status->__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(unique_id_status.GetDetail());
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }
  ScopedSessionState scoped_session(this);
  shared_ptr<SessionState> session;
  Status get_session_status = scoped_session.WithSession(session_id, &session);
  if (!get_session_status.ok()) {
    status->__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(get_session_status.GetDetail());
    // TODO: (here and elsewhere) - differentiate between invalid session ID and timeout
    // when setting the error code.
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }

  if (session == NULL) {
    status->__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(Substitute("Invalid session id: $0",
        PrintId(session_id)));
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }
  TQueryCtx query_ctx;
  PrepareQueryContext(&query_ctx);
  session->ToThrift(session_id, &query_ctx.session);
  request->__set_session(query_ctx.session);

  shared_ptr<ClientRequestState> request_state;
  // There is no user-supplied query text available because this metadata operation comes
  // from an RPC. As a best effort, we use the type of the operation.
  map<int, const char*>::const_iterator query_text_it =
      _TMetadataOpcode_VALUES_TO_NAMES.find(request->opcode);
  const string& query_text = query_text_it == _TMetadataOpcode_VALUES_TO_NAMES.end() ?
      "N/A" : query_text_it->second;
  query_ctx.client_request.stmt = query_text;
  request_state.reset(new ClientRequestState(query_ctx, exec_env_,
      exec_env_->frontend(), this, session));
  Status register_status = RegisterQuery(session, request_state);
  if (!register_status.ok()) {
    status->__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(register_status.GetDetail());
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }

  Status exec_status = request_state->Exec(*request);
  if (!exec_status.ok()) {
    discard_result(UnregisterQuery(request_state->query_id(), false, &exec_status));
    status->__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(exec_status.GetDetail());
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }

  request_state->UpdateNonErrorQueryState(beeswax::QueryState::FINISHED);

  Status inflight_status = SetQueryInflight(session, request_state);
  if (!inflight_status.ok()) {
    discard_result(UnregisterQuery(request_state->query_id(), false, &inflight_status));
    status->__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(inflight_status.GetDetail());
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }
  handle->__set_hasResultSet(true);
  // TODO: create secret for operationId
  TUniqueId operation_id = request_state->query_id();
  TUniqueIdToTHandleIdentifier(operation_id, operation_id, &(handle->operationId));
  status->__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

Status ImpalaServer::FetchInternal(const TUniqueId& query_id, int32_t fetch_size,
    bool fetch_first, TFetchResultsResp* fetch_results) {
  shared_ptr<ClientRequestState> request_state = GetClientRequestState(query_id);
  if (UNLIKELY(request_state == nullptr)) {
    string err_msg = Substitute("Invalid query handle: $0", PrintId(query_id));
    VLOG(1) << err_msg;
    return Status::Expected(err_msg);
  }

  // FetchResults doesn't have an associated session handle, so we presume that this
  // request should keep alive the same session that orignated the query.
  ScopedSessionState session_handle(this);
  const TUniqueId session_id = request_state->session_id();
  shared_ptr<SessionState> session;
  RETURN_IF_ERROR(session_handle.WithSession(session_id, &session));

  // Make sure ClientRequestState::Wait() has completed before fetching rows. Wait()
  // ensures that rows are ready to be fetched (e.g., Wait() opens
  // ClientRequestState::output_exprs_, which are evaluated in
  // ClientRequestState::FetchRows() below).
  request_state->BlockOnWait();

  lock_guard<mutex> frl(*request_state->fetch_rows_lock());
  lock_guard<mutex> l(*request_state->lock());

  // Check for cancellation or an error.
  RETURN_IF_ERROR(request_state->query_status());

  if (request_state->num_rows_fetched() == 0) {
    request_state->query_events()->MarkEvent("First row fetched");
    request_state->set_fetched_rows();
  }

  if (fetch_first) RETURN_IF_ERROR(request_state->RestartFetch());

  fetch_results->results.__set_startRowOffset(request_state->num_rows_fetched());

  // Child queries should always return their results in row-major format, rather than
  // inheriting the parent session's setting.
  bool is_child_query = request_state->parent_query_id() != TUniqueId();
  TProtocolVersion::type version = is_child_query ?
      TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V1 : session->hs2_version;
  scoped_ptr<QueryResultSet> result_set(QueryResultSet::CreateHS2ResultSet(
      version, *(request_state->result_metadata()), &(fetch_results->results)));
  RETURN_IF_ERROR(request_state->FetchRows(fetch_size, result_set.get()));
  fetch_results->__isset.results = true;
  fetch_results->__set_hasMoreRows(!request_state->eos());
  return Status::OK();
}

Status ImpalaServer::TExecuteStatementReqToTQueryContext(
    const TExecuteStatementReq execute_request, TQueryCtx* query_ctx) {
  query_ctx->client_request.stmt = execute_request.statement;
  VLOG_QUERY << "TExecuteStatementReq: " << ThriftDebugString(execute_request);
  QueryOptionsMask set_query_options_mask;
  {
    shared_ptr<SessionState> session_state;
    TUniqueId session_id;
    TUniqueId secret;
    RETURN_IF_ERROR(THandleIdentifierToTUniqueId(execute_request.sessionHandle.sessionId,
        &session_id, &secret));

    RETURN_IF_ERROR(GetSessionState(session_id, &session_state));
    session_state->ToThrift(session_id, &query_ctx->session);
    lock_guard<mutex> l(session_state->lock);
    query_ctx->client_request.query_options = session_state->QueryOptions();
    set_query_options_mask = session_state->set_query_options_mask;
  }

  if (execute_request.__isset.confOverlay) {
    TQueryOptions overlay;
    QueryOptionsMask overlay_mask;
    map<string, string>::const_iterator conf_itr = execute_request.confOverlay.begin();
    for (; conf_itr != execute_request.confOverlay.end(); ++conf_itr) {
      if (conf_itr->first == IMPALA_RESULT_CACHING_OPT) continue;
      if (conf_itr->first == ChildQuery::PARENT_QUERY_OPT) {
        if (ParseId(conf_itr->second, &query_ctx->parent_query_id)) {
          query_ctx->__isset.parent_query_id = true;
        }
        continue;
      }
      RETURN_IF_ERROR(SetQueryOption(conf_itr->first, conf_itr->second,
          &overlay, &overlay_mask));
    }
    OverlayQueryOptions(overlay, overlay_mask, &query_ctx->client_request.query_options);
    set_query_options_mask |= overlay_mask;
  }
  // Only query options not set in the session or confOverlay can be overridden by the
  // pool options.
  AddPoolQueryOptions(query_ctx, ~set_query_options_mask);
  VLOG_QUERY << "TClientRequest.queryOptions: "
             << ThriftDebugString(query_ctx->client_request.query_options);
  return Status::OK();
}

// HiveServer2 API
void ImpalaServer::OpenSession(TOpenSessionResp& return_val,
    const TOpenSessionReq& request) {
  // DO NOT log this Thrift struct in its entirety, in case a bad client sets the
  // password.
  VLOG_QUERY << "OpenSession(): username=" << request.username;

  // Generate session ID and the secret
  TUniqueId session_id;
  {
    lock_guard<mutex> l(uuid_lock_);
    uuid secret = uuid_generator_();
    uuid session_uuid = uuid_generator_();
    return_val.sessionHandle.sessionId.guid.assign(
        session_uuid.begin(), session_uuid.end());
    return_val.sessionHandle.sessionId.secret.assign(secret.begin(), secret.end());
    DCHECK_EQ(return_val.sessionHandle.sessionId.guid.size(), 16);
    DCHECK_EQ(return_val.sessionHandle.sessionId.secret.size(), 16);
    return_val.__isset.sessionHandle = true;
    UUIDToTUniqueId(session_uuid, &session_id);
  }
  // create a session state: initialize start time, session type, database and default
  // query options.
  // TODO: put secret in session state map and check it
  // TODO: Fix duplication of code between here and ConnectionStart().
  shared_ptr<SessionState> state = make_shared<SessionState>(this);
  state->closed = false;
  state->start_time_ms = UnixMillis();
  state->session_type = TSessionType::HIVESERVER2;
  state->network_address = ThriftServer::GetThreadConnectionContext()->network_address;
  state->last_accessed_ms = UnixMillis();
  state->hs2_version = min(MAX_SUPPORTED_HS2_VERSION, request.client_protocol);
  state->kudu_latest_observed_ts = 0;

  // If the username was set by a lower-level transport, use it.
  const ThriftServer::Username& username =
      ThriftServer::GetThreadConnectionContext()->username;
  if (!username.empty()) {
    state->connected_user = username;
  } else {
    state->connected_user = request.username;
  }

  // Process the supplied configuration map.
  state->database = "default";
  state->server_default_query_options = &default_query_options_;
  state->session_timeout = FLAGS_idle_session_timeout;
  if (request.__isset.configuration) {
    typedef map<string, string> ConfigurationMap;
    for (const ConfigurationMap::value_type& v: request.configuration) {
      if (iequals(v.first, "impala.doas.user")) {
        // If the current user is a valid proxy user, he/she can optionally perform
        // authorization requests on behalf of another user. This is done by setting
        // the 'impala.doas.user' Hive Server 2 configuration property.
        state->do_as_user = v.second;
        Status status = AuthorizeProxyUser(state->connected_user, state->do_as_user);
        HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);
      } else if (iequals(v.first, "use:database")) {
        state->database = v.second;
      } else {
        // Normal configuration key. Use it to set session default query options.
        // Ignore failure (failures will be logged in SetQueryOption()).
        Status status = SetQueryOption(v.first, v.second, &state->set_query_options,
            &state->set_query_options_mask);
        if (status.ok() && iequals(v.first, "idle_session_timeout")) {
          state->session_timeout = state->set_query_options.idle_session_timeout;
          VLOG_QUERY << "OpenSession(): idle_session_timeout="
                     << PrettyPrinter::Print(state->session_timeout, TUnit::TIME_S);
        }
      }
    }
  }
  RegisterSessionTimeout(state->session_timeout);
  TQueryOptionsToMap(state->QueryOptions(), &return_val.configuration);

  // OpenSession() should return the coordinator's HTTP server address.
  const string& http_addr = lexical_cast<string>(
      MakeNetworkAddress(FLAGS_hostname, FLAGS_webserver_port));
  return_val.configuration.insert(make_pair("http_addr", http_addr));

  // Put the session state in session_state_map_
  {
    lock_guard<mutex> l(session_state_map_lock_);
    session_state_map_.insert(make_pair(session_id, state));
  }

  {
    lock_guard<mutex> l(connection_to_sessions_map_lock_);
    const TUniqueId& connection_id = ThriftServer::GetThreadConnectionId();
    connection_to_sessions_map_[connection_id].push_back(session_id);
  }

  ImpaladMetrics::IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS->Increment(1);

  return_val.__isset.configuration = true;
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
  return_val.serverProtocolVersion = state->hs2_version;
}

void ImpalaServer::CloseSession(TCloseSessionResp& return_val,
    const TCloseSessionReq& request) {
  VLOG_QUERY << "CloseSession(): request=" << ThriftDebugString(request);

  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.sessionHandle.sessionId, &session_id, &secret), SQLSTATE_GENERAL_ERROR);
  HS2_RETURN_IF_ERROR(return_val,
      CloseSessionInternal(session_id, false), SQLSTATE_GENERAL_ERROR);
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::GetInfo(TGetInfoResp& return_val,
    const TGetInfoReq& request) {
  VLOG_QUERY << "GetInfo(): request=" << ThriftDebugString(request);

  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.sessionHandle.sessionId, &session_id, &secret), SQLSTATE_GENERAL_ERROR);
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id, &session),
      SQLSTATE_GENERAL_ERROR);

  switch (request.infoType) {
    case TGetInfoType::CLI_SERVER_NAME:
    case TGetInfoType::CLI_DBMS_NAME:
      return_val.infoValue.__set_stringValue("Impala");
      break;
    case TGetInfoType::CLI_DBMS_VER:
      return_val.infoValue.__set_stringValue(GetDaemonBuildVersion());
      break;
    default:
      HS2_RETURN_ERROR(return_val, "Unsupported operation",
          SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED);
  }
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::ExecuteStatement(TExecuteStatementResp& return_val,
    const TExecuteStatementReq& request) {
  VLOG_QUERY << "ExecuteStatement(): request=" << ThriftDebugString(request);
  // We ignore the runAsync flag here: Impala's queries will always run asynchronously,
  // and will block on fetch. To the client, this looks like Hive's synchronous mode; the
  // difference is that rows are not available when ExecuteStatement() returns.
  TQueryCtx query_ctx;
  Status status = TExecuteStatementReqToTQueryContext(request, &query_ctx);
  HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);

  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.sessionHandle.sessionId, &session_id, &secret), SQLSTATE_GENERAL_ERROR);
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id, &session),
      SQLSTATE_GENERAL_ERROR);
  if (session == NULL) {
    string err_msg = Substitute("Invalid session id: $0", PrintId(session_id));
    VLOG(1) << err_msg;
    HS2_RETURN_IF_ERROR(return_val, Status::Expected(err_msg), SQLSTATE_GENERAL_ERROR);
  }

  // Optionally enable result caching to allow restarting fetches.
  int64_t cache_num_rows = -1;
  if (request.__isset.confOverlay) {
    map<string, string>::const_iterator iter =
        request.confOverlay.find(IMPALA_RESULT_CACHING_OPT);
    if (iter != request.confOverlay.end()) {
      StringParser::ParseResult parse_result;
      cache_num_rows = StringParser::StringToInt<int64_t>(
          iter->second.c_str(), iter->second.size(), &parse_result);
      if (parse_result != StringParser::PARSE_SUCCESS) {
        HS2_RETURN_IF_ERROR(
            return_val, Status::Expected(Substitute("Invalid value '$0' for '$1' option.",
                iter->second, IMPALA_RESULT_CACHING_OPT)), SQLSTATE_GENERAL_ERROR);
      }
    }
  }

  shared_ptr<ClientRequestState> request_state;
  status = Execute(&query_ctx, session, &request_state);
  HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);

  // Optionally enable result caching on the ClientRequestState.
  if (cache_num_rows > 0) {
    status = request_state->SetResultCache(
        QueryResultSet::CreateHS2ResultSet(
            session->hs2_version, *request_state->result_metadata(), nullptr),
        cache_num_rows);
    if (!status.ok()) goto return_error;
  }
  request_state->UpdateNonErrorQueryState(beeswax::QueryState::RUNNING);
  // Start thread to wait for results to become available.
  status = request_state->WaitAsync();
  if (!status.ok()) goto return_error;
  // Once the query is running do a final check for session closure and add it to the
  // set of in-flight queries.
  status = SetQueryInflight(session, request_state);
  if (!status.ok()) goto return_error;
  return_val.__isset.operationHandle = true;
  return_val.operationHandle.__set_operationType(TOperationType::EXECUTE_STATEMENT);
  return_val.operationHandle.__set_hasResultSet(request_state->returns_result_set());
  // TODO: create secret for operationId and store the secret in request_state
  TUniqueIdToTHandleIdentifier(request_state->query_id(), request_state->query_id(),
                               &return_val.operationHandle.operationId);
  return_val.status.__set_statusCode(
      apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);

  VLOG_QUERY << "ExecuteStatement(): return_val=" << ThriftDebugString(return_val);
  return;

 return_error:
  discard_result(UnregisterQuery(request_state->query_id(), false, &status));
  HS2_RETURN_ERROR(return_val, status.GetDetail(), SQLSTATE_GENERAL_ERROR);
}

void ImpalaServer::GetTypeInfo(TGetTypeInfoResp& return_val,
    const TGetTypeInfoReq& request) {
  VLOG_QUERY << "GetTypeInfo(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_TYPE_INFO);
  req.__set_get_type_info_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_TYPE_INFO);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetTypeInfo(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetCatalogs(TGetCatalogsResp& return_val,
    const TGetCatalogsReq& request) {
  VLOG_QUERY << "GetCatalogs(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_CATALOGS);
  req.__set_get_catalogs_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_CATALOGS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetCatalogs(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetSchemas(TGetSchemasResp& return_val,
    const TGetSchemasReq& request) {
  VLOG_QUERY << "GetSchemas(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_SCHEMAS);
  req.__set_get_schemas_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_SCHEMAS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetSchemas(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetTables(TGetTablesResp& return_val,
    const TGetTablesReq& request) {
  VLOG_QUERY << "GetTables(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_TABLES);
  req.__set_get_tables_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_TABLES);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetTables(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetTableTypes(TGetTableTypesResp& return_val,
    const TGetTableTypesReq& request) {
  VLOG_QUERY << "GetTableTypes(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_TABLE_TYPES);
  req.__set_get_table_types_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_TABLE_TYPES);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetTableTypes(): return_val=" << ThriftDebugString(return_val);

}

void ImpalaServer::GetColumns(TGetColumnsResp& return_val,
    const TGetColumnsReq& request) {
  VLOG_QUERY << "GetColumns(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_COLUMNS);
  req.__set_get_columns_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_COLUMNS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetColumns(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetFunctions(TGetFunctionsResp& return_val,
    const TGetFunctionsReq& request) {
  VLOG_QUERY << "GetFunctions(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_FUNCTIONS);
  req.__set_get_functions_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_FUNCTIONS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetFunctions(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetOperationStatus(TGetOperationStatusResp& return_val,
    const TGetOperationStatusReq& request) {
  if (request.operationHandle.operationId.guid.size() == 0) {
    // An empty operation handle identifier means no execution and no result for this
    // query (USE <database>).
    VLOG_ROW << "GetOperationStatus(): guid size 0";
    return_val.operationState = TOperationState::FINISHED_STATE;
    return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
    return;
  }

  // TODO: check secret
  TUniqueId query_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);
  VLOG_ROW << "GetOperationStatus(): query_id=" << PrintId(query_id);

  shared_ptr<ClientRequestState> request_state = GetClientRequestState(query_id);
  if (UNLIKELY(request_state.get() == nullptr)) {
    // No handle was found
    HS2_RETURN_ERROR(return_val,
      Substitute("Invalid query handle: $0", PrintId(query_id)), SQLSTATE_GENERAL_ERROR);
  }

  ScopedSessionState session_handle(this);
  const TUniqueId session_id = request_state->session_id();
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id, &session),
      SQLSTATE_GENERAL_ERROR);

  {
    lock_guard<mutex> l(*request_state->lock());
    TOperationState::type operation_state = QueryStateToTOperationState(
        request_state->query_state());
    return_val.__set_operationState(operation_state);
    if (operation_state == TOperationState::ERROR_STATE) {
      DCHECK(!request_state->query_status().ok());
      return_val.__set_errorMessage(request_state->query_status().GetDetail());
      return_val.__set_sqlState(SQLSTATE_GENERAL_ERROR);
    } else {
      DCHECK(request_state->query_status().ok());
    }
  }
}

void ImpalaServer::CancelOperation(TCancelOperationResp& return_val,
    const TCancelOperationReq& request) {
  TUniqueId query_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);
  VLOG_QUERY << "CancelOperation(): query_id=" << PrintId(query_id);

  shared_ptr<ClientRequestState> request_state = GetClientRequestState(query_id);
  if (UNLIKELY(request_state.get() == nullptr)) {
    // No handle was found
    HS2_RETURN_ERROR(return_val,
      Substitute("Invalid query handle: $0", PrintId(query_id)), SQLSTATE_GENERAL_ERROR);
  }
  ScopedSessionState session_handle(this);
  const TUniqueId session_id = request_state->session_id();
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id),
      SQLSTATE_GENERAL_ERROR);
  HS2_RETURN_IF_ERROR(return_val, CancelInternal(query_id, true), SQLSTATE_GENERAL_ERROR);
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::CloseOperation(TCloseOperationResp& return_val,
    const TCloseOperationReq& request) {
  TUniqueId query_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);
  VLOG_QUERY << "CloseOperation(): query_id=" << PrintId(query_id);

  shared_ptr<ClientRequestState> request_state = GetClientRequestState(query_id);
  if (UNLIKELY(request_state.get() == nullptr)) {
    // No handle was found
    HS2_RETURN_ERROR(return_val,
      Substitute("Invalid query handle: $0", PrintId(query_id)), SQLSTATE_GENERAL_ERROR);
  }
  ScopedSessionState session_handle(this);
  const TUniqueId session_id = request_state->session_id();
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id),
      SQLSTATE_GENERAL_ERROR);
  // TODO: use timeout to get rid of unwanted request_state.
  HS2_RETURN_IF_ERROR(return_val, UnregisterQuery(query_id, true),
      SQLSTATE_GENERAL_ERROR);
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::GetResultSetMetadata(TGetResultSetMetadataResp& return_val,
    const TGetResultSetMetadataReq& request) {
  // Convert Operation id to TUniqueId and get the query exec state.
  // TODO: check secret
  TUniqueId query_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);
  VLOG_QUERY << "GetResultSetMetadata(): query_id=" << PrintId(query_id);

  shared_ptr<ClientRequestState> request_state = GetClientRequestState(query_id);
  if (UNLIKELY(request_state.get() == nullptr)) {
    VLOG_QUERY << "GetResultSetMetadata(): invalid query handle";
    // No handle was found
    HS2_RETURN_ERROR(return_val,
      Substitute("Invalid query handle: $0", PrintId(query_id)), SQLSTATE_GENERAL_ERROR);
  }
  ScopedSessionState session_handle(this);
  const TUniqueId session_id = request_state->session_id();
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id),
      SQLSTATE_GENERAL_ERROR);
  {
    lock_guard<mutex> l(*request_state->lock());

    // Convert TResultSetMetadata to TGetResultSetMetadataResp
    const TResultSetMetadata* result_set_md = request_state->result_metadata();
    DCHECK(result_set_md != NULL);
    if (result_set_md->columns.size() > 0) {
      return_val.__isset.schema = true;
      return_val.schema.columns.resize(result_set_md->columns.size());
      for (int i = 0; i < result_set_md->columns.size(); ++i) {
        return_val.schema.columns[i].__set_columnName(
            result_set_md->columns[i].columnName);
        return_val.schema.columns[i].position = i;
        return_val.schema.columns[i].typeDesc.types.resize(1);
        ColumnType t = ColumnType::FromThrift(result_set_md->columns[i].columnType);
        return_val.schema.columns[i].typeDesc.types[0] = t.ToHs2Type();
      }
    }
  }

  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
  VLOG_QUERY << "GetResultSetMetadata(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::FetchResults(TFetchResultsResp& return_val,
    const TFetchResultsReq& request) {
  if (request.orientation != TFetchOrientation::FETCH_NEXT
      && request.orientation != TFetchOrientation::FETCH_FIRST) {
    HS2_RETURN_ERROR(return_val, "Unsupported operation",
        SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED);
  }
  bool fetch_first = request.orientation == TFetchOrientation::FETCH_FIRST;

  // Convert Operation id to TUniqueId and get the query exec state.
  // TODO: check secret
  TUniqueId query_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);
  VLOG_ROW << "FetchResults(): query_id=" << PrintId(query_id)
           << " fetch_size=" << request.maxRows;

  // FetchInternal takes care of extending the session
  Status status = FetchInternal(query_id, request.maxRows, fetch_first, &return_val);
  VLOG_ROW << "FetchResults(): #results=" << return_val.results.rows.size()
           << " has_more=" << (return_val.hasMoreRows ? "true" : "false");
  if (!status.ok()) {
    // Only unregister the query if the underlying error is unrecoverable.
    // Clients are expected to understand that a failed FETCH_FIRST is recoverable,
    // and hence, the query must eventually be closed by the client.
    // It is important to ensure FETCH_NEXT does not return recoverable errors to
    // preserve compatibility with clients written against Impala versions < 1.3.
    if (status.IsRecoverableError()) {
      DCHECK(fetch_first);
    } else {
      discard_result(UnregisterQuery(query_id, false, &status));
    }
    HS2_RETURN_ERROR(return_val, status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::GetLog(TGetLogResp& return_val, const TGetLogReq& request) {
  TUniqueId query_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);

  shared_ptr<ClientRequestState> request_state = GetClientRequestState(query_id);
  if (UNLIKELY(request_state.get() == nullptr)) {
    // No handle was found
    HS2_RETURN_ERROR(return_val,
      Substitute("Invalid query handle: $0", PrintId(query_id)), SQLSTATE_GENERAL_ERROR);
  }

  // GetLog doesn't have an associated session handle, so we presume that this request
  // should keep alive the same session that orignated the query.
  ScopedSessionState session_handle(this);
  const TUniqueId session_id = request_state->session_id();
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id),
                      SQLSTATE_GENERAL_ERROR);

  stringstream ss;
  if (request_state->coord() != NULL) {
    // Report progress
    ss << request_state->coord()->progress().ToString() << "\n";
  }
  // Report analysis errors
  ss << join(request_state->GetAnalysisWarnings(), "\n");
  if (request_state->coord() != NULL) {
    // Report execution errors
    ss << request_state->coord()->GetErrorLog();
  }
  return_val.log = ss.str();
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::GetExecSummary(TGetExecSummaryResp& return_val,
    const TGetExecSummaryReq& request) {
  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.sessionHandle.sessionId, &session_id, &secret), SQLSTATE_GENERAL_ERROR);
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id, &session),
      SQLSTATE_GENERAL_ERROR);
  if (session == NULL) {
    HS2_RETURN_ERROR(return_val, Substitute("Invalid session id: $0",
        PrintId(session_id)), SQLSTATE_GENERAL_ERROR);
  }

  TUniqueId query_id;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);

  TExecSummary summary;
  Status status = GetExecSummary(query_id, GetEffectiveUser(*session), &summary);
  HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);
  return_val.__set_summary(summary);
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::GetRuntimeProfile(TGetRuntimeProfileResp& return_val,
    const TGetRuntimeProfileReq& request) {
  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.sessionHandle.sessionId, &session_id, &secret), SQLSTATE_GENERAL_ERROR);
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id, &session),
      SQLSTATE_GENERAL_ERROR);
  if (session == NULL) {
    HS2_RETURN_ERROR(return_val, Substitute("Invalid session id: $0",
        PrintId(session_id)), SQLSTATE_GENERAL_ERROR);
  }

  TUniqueId query_id;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);

  stringstream ss;
  HS2_RETURN_IF_ERROR(return_val, GetRuntimeProfileStr(query_id,
      GetEffectiveUser(*session), false, &ss), SQLSTATE_GENERAL_ERROR);
  return_val.__set_profile(ss.str());
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::GetDelegationToken(TGetDelegationTokenResp& return_val,
    const TGetDelegationTokenReq& req) {
  return_val.status.__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
  return_val.status.__set_errorMessage("Not implemented");
}

void ImpalaServer::CancelDelegationToken(TCancelDelegationTokenResp& return_val,
    const TCancelDelegationTokenReq& req) {
  return_val.status.__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
  return_val.status.__set_errorMessage("Not implemented");
}

void ImpalaServer::RenewDelegationToken(TRenewDelegationTokenResp& return_val,
    const TRenewDelegationTokenReq& req) {
  return_val.status.__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
  return_val.status.__set_errorMessage("Not implemented");
}


}
