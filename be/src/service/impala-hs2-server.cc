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

#include "common/thread-debug-info.h"
#include "service/impala-server.h"
#include "service/impala-server.inline.h"

#include <algorithm>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/bind.hpp>
#include <boost/unordered_set.hpp>
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gtest/gtest.h>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "common/version.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "runtime/raw-value.h"
#include "runtime/query-driver.h"
#include "scheduling/admission-controller.h"
#include "service/client-request-state.h"
#include "service/hs2-util.h"
#include "service/query-options.h"
#include "service/query-result-set.h"
#include "util/auth-util.h"
#include "util/backend-gflag-util.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"
#include "util/stopwatch.h"
#include "util/string-parser.h"
#include "util/thrift-debug-util.h"
#include "util/uid-util.h"

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
    return_val.status.__set_errorMessage((error_msg)); \
    return_val.status.__set_sqlState((error_state)); \
    return; \
  } while (false)

#define HS2_RETURN_IF_ERROR(return_val, status, error_state) \
  do { \
    const Status& _status = (status); \
    if (UNLIKELY(!_status.ok())) { \
      HS2_RETURN_ERROR(return_val, _status.GetDetail(), (error_state)); \
      return; \
    } \
  } while (false)

DECLARE_bool(enable_ldap_auth);
DECLARE_string(hostname);
DECLARE_int32(webserver_port);
DECLARE_int32(idle_session_timeout);
DECLARE_int32(disconnected_session_timeout);
DECLARE_int32(max_hs2_sessions_per_user);
DECLARE_bool(ping_expose_webserver_url);
DECLARE_string(anonymous_user_name);

namespace impala {

const string IMPALA_RESULT_CACHING_OPT = "impala.resultset.cache.size";

const string SET_HIVECONF_PREFIX = "set:hiveconf:";

const int SET_HIVECONF_PREFIX_LEN = SET_HIVECONF_PREFIX.length();

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
  Status get_session_status =
      scoped_session.WithSession(session_id, SecretArg::Session(secret), &session);
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
  ScopedThreadContext tdi_context(GetThreadDebugInfo(), query_ctx.query_id);
  session->ToThrift(session_id, &query_ctx.session);
  request->__set_session(query_ctx.session);

  // There is no user-supplied query text available because this metadata operation comes
  // from an RPC. As a best effort, we use the type of the operation.
  map<int, const char*>::const_iterator query_text_it =
      _TMetadataOpcode_VALUES_TO_NAMES.find(request->opcode);
  if (query_text_it == _TMetadataOpcode_VALUES_TO_NAMES.end()) {
    query_ctx.client_request.stmt = "N/A";
  } else {
    query_ctx.client_request.stmt = query_text_it->second;
    query_ctx.client_request.__set_hs2_metadata_op(true);
  }

  QueryHandle query_handle;
  QueryDriver::CreateNewDriver(this, &query_handle, query_ctx, session);
  Status register_status = RegisterQuery(query_ctx.query_id, session, &query_handle);
  if (!register_status.ok()) {
    status->__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(register_status.GetDetail());
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }

  Status exec_status = query_handle->Exec(*request);
  if (!exec_status.ok()) {
    discard_result(UnregisterQuery(query_handle->query_id(), false, &exec_status));
    status->__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(exec_status.GetDetail());
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }

  query_handle->UpdateNonErrorExecState(ClientRequestState::ExecState::FINISHED);

  Status inflight_status = SetQueryInflight(session, query_handle);
  if (!inflight_status.ok()) {
    discard_result(UnregisterQuery(query_handle->query_id(), false, &inflight_status));
    status->__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(inflight_status.GetDetail());
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }
  handle->__set_hasResultSet(true);
  // Secret is inherited from session.
  TUniqueId operation_id = query_handle->query_id();
  TUniqueIdToTHandleIdentifier(operation_id, secret, &(handle->operationId));
  status->__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

Status ImpalaServer::FetchInternal(TUniqueId query_id, SessionState* session,
    int32_t fetch_size, bool fetch_first, TFetchResultsResp* fetch_results,
    int32_t* num_results) {
  bool timed_out = false;
  int64_t block_on_wait_time_us = 0;
  QueryHandle query_handle;
  RETURN_IF_ERROR(
      WaitForResults(query_id, &query_handle, &block_on_wait_time_us, &timed_out));
  if (timed_out) {
    fetch_results->status.__set_statusCode(thrift::TStatusCode::STILL_EXECUTING_STATUS);
    fetch_results->__set_hasMoreRows(true);
    fetch_results->__isset.results = false;
    return Status::OK();
  }

  int64_t start_time_ns = MonotonicNanos();
  lock_guard<mutex> frl(*query_handle->fetch_rows_lock());
  lock_guard<mutex> l(*query_handle->lock());
  int64_t lock_wait_time_ns = MonotonicNanos() - start_time_ns;
  query_handle->AddClientFetchLockWaitTime(lock_wait_time_ns);

  // Check for cancellation or an error.
  RETURN_IF_ERROR(query_handle->query_status());

  if (query_handle->num_rows_fetched() == 0) {
    query_handle->set_fetched_rows();
  }

  if (fetch_first) RETURN_IF_ERROR(query_handle->RestartFetch());

  fetch_results->results.__set_startRowOffset(query_handle->num_rows_fetched());

  // Child queries should always return their results in row-major format, rather than
  // inheriting the parent session's setting.
  bool is_child_query = query_handle->parent_query_id() != TUniqueId();
  TProtocolVersion::type version = is_child_query ?
      TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V1 : session->hs2_version;

  // In the first fetch, expect 0 results to avoid reserving unnecessarily large result
  // vectors for small queries. If there are more fetches (so there are more rows than
  // num_rows_fetched), then expect subsequent fetches to be fully filled.
  int expected_result_count = query_handle->num_rows_fetched() == 0 ? 0
      : fetch_size;

  scoped_ptr<QueryResultSet> result_set(QueryResultSet::CreateHS2ResultSet(
      version, *(query_handle->result_metadata()), &(fetch_results->results),
      query_handle->query_options().stringify_map_keys, expected_result_count));
  RETURN_IF_ERROR(
      query_handle->FetchRows(fetch_size, result_set.get(), block_on_wait_time_us));
  *num_results = result_set->size();
  fetch_results->__isset.results = true;
  fetch_results->__set_hasMoreRows(!query_handle->eos());
  return Status::OK();
}

Status ImpalaServer::TExecuteStatementReqToTQueryContext(
    const TExecuteStatementReq execute_request, TQueryCtx* query_ctx) {
  query_ctx->client_request.stmt = execute_request.statement;
  VLOG_QUERY << "TExecuteStatementReq: " << RedactedDebugString(execute_request);
  QueryOptionsMask set_query_options_mask;
  {
    shared_ptr<SessionState> session_state;
    TUniqueId session_id;
    TUniqueId secret;
    RETURN_IF_ERROR(THandleIdentifierToTUniqueId(execute_request.sessionHandle.sessionId,
        &session_id, &secret));

    RETURN_IF_ERROR(
        GetSessionState(session_id, SecretArg::Session(secret), &session_state));
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
  AddPoolConfiguration(query_ctx, ~set_query_options_mask);
  VLOG_QUERY << "TClientRequest.queryOptions: "
             << ThriftDebugString(query_ctx->client_request.query_options);
  return Status::OK();
}

// HiveServer2 API
void ImpalaServer::OpenSession(TOpenSessionResp& return_val,
    const TOpenSessionReq& request) {
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  // Generate session ID and the secret
  uuid secret_uuid;
  uuid session_uuid;
  {
    lock_guard<mutex> l(uuid_lock_);
    secret_uuid = crypto_uuid_generator_();
    session_uuid = crypto_uuid_generator_();
  }
  return_val.sessionHandle.sessionId.guid.assign(
      session_uuid.begin(), session_uuid.end());
  return_val.sessionHandle.sessionId.secret.assign(
      secret_uuid.begin(), secret_uuid.end());
  DCHECK_EQ(return_val.sessionHandle.sessionId.guid.size(), 16);
  DCHECK_EQ(return_val.sessionHandle.sessionId.secret.size(), 16);
  return_val.__isset.sessionHandle = true;
  TUniqueId session_id, secret;
  UUIDToTUniqueId(session_uuid, &session_id);
  UUIDToTUniqueId(secret_uuid, &secret);

  // DO NOT log this Thrift struct in its entirety, in case a bad client sets the
  // password.
  VLOG_QUERY << "Opening session: " << PrintId(session_id)
             << " request username: " << request.username;

  // create a session state: initialize start time, session type, database and default
  // query options.
  // TODO: Fix duplication of code between here and ConnectionStart().
  shared_ptr<SessionState> state =
      std::make_shared<SessionState>(this, session_id, secret);
  state->closed = false;
  state->start_time_ms = UnixMillis();
  const ThriftServer::ConnectionContext* connection_context =
    ThriftServer::GetThreadConnectionContext();
  if (connection_context->server_name == EXTERNAL_FRONTEND_SERVER_NAME) {
    state->session_type = TSessionType::EXTERNAL_FRONTEND;
  } else {
    state->session_type = TSessionType::HIVESERVER2;
  }
  state->network_address = ThriftServer::GetThreadConnectionContext()->network_address;
  state->last_accessed_ms = UnixMillis();
  // request.client_protocol is not guaranteed to be a valid TProtocolVersion::type, so
  // loading it can cause undefined behavior. Instead, we copy it to a value of the
  // "underlying type" of the enum, then copy it back to state->hs_version only once we
  // have clamped it to be at most MAX_SUPPORTED_HS2_VERSION.
  std::underlying_type_t<decltype(request.client_protocol)> protocol_integer;
  memcpy(&protocol_integer, &request.client_protocol, sizeof(request.client_protocol));
  state->hs2_version = static_cast<TProtocolVersion::type>(
      min<decltype(protocol_integer)>(MAX_SUPPORTED_HS2_VERSION, protocol_integer));
  state->kudu_latest_observed_ts = 0;

  // If the username was set by a lower-level transport, use it.
  if (!connection_context->username.empty()) {
    state->connected_user = connection_context->username;
    if (!connection_context->do_as_user.empty()) {
      state->do_as_user = connection_context->do_as_user;
      Status status = AuthorizeProxyUser(state->connected_user, state->do_as_user);
      HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);
    }
  } else if (!request.username.empty()) {
    state->connected_user = request.username;
  } else {
    state->connected_user = FLAGS_anonymous_user_name;
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
        // Note: The 'impala.doas.user' configuration overrides the user specified
        // in the 'doAs' request parameter, which can be specified for hs2-http transport.
        state->do_as_user = v.second;
        Status status = AuthorizeProxyUser(state->connected_user, state->do_as_user);
        HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);
      } else if (iequals(v.first, "use:database")) {
        state->database = v.second;
      } else {
        string key = v.first, value = v.second;
        // Hive JDBC will add a prefix to the configuration, see IMPALA-11992
        if (key.rfind(SET_HIVECONF_PREFIX, 0) != string::npos) {
          key = key.substr(SET_HIVECONF_PREFIX_LEN);
        }
        // Normal configuration key. Use it to set session default query options.
        // Ignore failure (failures will be logged in SetQueryOption()).
        Status status = SetQueryOption(key, value, &state->set_query_options,
            &state->set_query_options_mask);
        if (status.ok() && iequals(key, "idle_session_timeout")) {
          state->session_timeout = state->set_query_options.idle_session_timeout;
          VLOG_QUERY << "OpenSession(): session: " << PrintId(session_id)
                     << " idle_session_timeout="
                     << PrettyPrinter::Print(state->session_timeout, TUnit::TIME_S);
        }
      }
    }
  }

  // If the connected user is an authorized proxy user, we were not able to check the LDAP
  // filters when the connection was created because we didn't know what the effective
  // user would be yet, so check now.
  // When no 'doAs' user is specified and Kerberos authentication is enabled, then the
  // connected user is a Kerberos user principal and that will certainly not pass the
  // LDAP checks, so in this case the LDAP filters will be checked with the short username
  // derived from the Kerberos user principal.
  if (FLAGS_enable_ldap_auth && IsAuthorizedProxyUser(state->connected_user)) {
    string username_for_ldap_filters = GetEffectiveUser(*state);
    if (state->do_as_user.empty() &&
        !connection_context->kerberos_user_principal.empty()) {

      string short_username = GetShortUsernameFromKerberosPrincipal(
        connection_context->kerberos_user_principal);
      VLOG(1) << "No doAs user is specified and Kerberos authentication is enabled, "
              << "the authenticated user principal is \""
              << connection_context->kerberos_user_principal << "\". "
              << "LDAP filters will be checked using the short username \""
              << short_username << "\", which is extracted from the user principal.";
      username_for_ldap_filters = short_username;
    }

    bool success =
      AuthManager::GetInstance()->GetLdap()->LdapCheckFilters(username_for_ldap_filters);
    if (!success) {
      HS2_RETURN_ERROR(return_val, "User is not authorized.", SQLSTATE_GENERAL_ERROR);
    }
  }

  Status status = IncrementAndCheckSessionCount(state->connected_user);
  HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);

  // At this point all checks are complete, store session status information.

  RegisterSessionTimeout(state->session_timeout);
  TQueryOptionsToMap(state->QueryOptions(), &return_val.configuration);

  // OpenSession() should return the coordinator's HTTP server address.
  const string& http_addr = TNetworkAddressToString(MakeNetworkAddress(
      FLAGS_hostname, FLAGS_webserver_port));
  return_val.configuration.insert(make_pair("http_addr", http_addr));

  {
    lock_guard<mutex> l(connection_to_sessions_map_lock_);
    const TUniqueId& connection_id = ThriftServer::GetThreadConnectionId();
    connection_to_sessions_map_[connection_id].insert(session_id);
    state->connections.insert(connection_id);
  }

  // Put the session state in session_state_map_
  {
    lock_guard<mutex> l(session_state_map_lock_);
    session_state_map_.insert(make_pair(session_id, state));
  }

  ImpaladMetrics::IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS->Increment(1);

  return_val.__isset.configuration = true;
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
  return_val.serverProtocolVersion = state->hs2_version;
  VLOG_QUERY << "Opened session: " << PrintId(session_id)
             << ", effective username: " << GetEffectiveUser(*state)
             << ", client address: "
             << "<" << TNetworkAddressToString(state->network_address) << ">.";
}

void ImpalaServer::DecrementCount(
    std::map<std::string, int64>& loads, const std::string& key) {
  // Check if key is present as dereferencing the map will insert it.
  // FIXME C++20: use contains().
  if (!loads.count(key)) {
    string msg = Substitute("Missing key $0 when decrementing count", key);
    LOG(WARNING) << msg;
    DCHECK(false) << msg;
    return;
  }
  int64& current_value = loads[key];
  if (current_value == 1) {
    // Remove the entry from the map if the current_value will go to zero.
    loads.erase(key);
    return;
  }
  if (current_value < 1) {
    // Don't allow decrement below zero.
    string msg = Substitute("Attempt to decrement below zero with key $0 ", key);
    LOG(WARNING) << msg;
    return;
  }
  loads[key]--;
}

void ImpalaServer::DecrementSessionCount(const string& user_name) {
  if (FLAGS_max_hs2_sessions_per_user > 0) {
    lock_guard<mutex> l(per_user_session_count_lock_);
    DecrementCount(per_user_session_count_map_, user_name);
  }
}

Status ImpalaServer::IncrementAndCheckSessionCount(const string& user_name) {
  if (FLAGS_max_hs2_sessions_per_user > 0) {
    lock_guard<mutex> l(per_user_session_count_lock_);
    // Only check user limit if there is already a session for the user.
    if (per_user_session_count_map_.count(user_name)) {
      int64 load = per_user_session_count_map_[user_name];
      if (load >= FLAGS_max_hs2_sessions_per_user) {
        const string& err_msg =
            Substitute("Number of sessions for user $0 exceeds coordinator limit $1",
                user_name, FLAGS_max_hs2_sessions_per_user);
        VLOG(1) << err_msg;
        return Status::Expected(err_msg);
      }
    }
    per_user_session_count_map_[user_name]++;
  }
  return Status::OK();
}

void ImpalaServer::CloseSession(TCloseSessionResp& return_val,
    const TCloseSessionReq& request) {
  VLOG_QUERY << "CloseSession(): request=" << RedactedDebugString(request);

  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.sessionHandle.sessionId, &session_id, &secret), SQLSTATE_GENERAL_ERROR);
  HS2_RETURN_IF_ERROR(return_val,
      CloseSessionInternal(
          session_id, SecretArg::Session(secret), /* ignore_if_absent= */ false),
      SQLSTATE_GENERAL_ERROR);
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::GetInfo(TGetInfoResp& return_val,
    const TGetInfoReq& request) {
  VLOG_QUERY << "GetInfo(): request=" << RedactedDebugString(request);
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.sessionHandle.sessionId, &session_id, &secret), SQLSTATE_GENERAL_ERROR);
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(session_id, SecretArg::Session(secret), &session),
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
      return_val.status.__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
      return_val.status.__set_errorMessage(("Unsupported operation"));
      return_val.status.__set_sqlState((SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED));
      // 'infoValue' is a required field of TGetInfoResp
      return_val.infoValue.__set_stringValue("");
      return;
  }
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::ExecuteStatementCommon(TExecuteStatementResp& return_val,
    const TExecuteStatementReq& request, const TExecRequest* external_exec_request) {
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);
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
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(session_id, SecretArg::Session(secret), &session),
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

  QueryHandle query_handle;
  status = Execute(&query_ctx, session, &query_handle, external_exec_request);
  HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);

  // Start thread to wait for results to become available.
  status = query_handle->WaitAsync();
  if (!status.ok()) goto return_error;

  // Optionally enable result caching on the ClientRequestState.
  status = SetupResultsCacheing(query_handle, session, cache_num_rows);
  if (!status.ok()) goto return_error;

  // Once the query is running do a final check for session closure and add it to the
  // set of in-flight queries.
  status = SetQueryInflight(session, query_handle);
  if (!status.ok()) goto return_error;
  return_val.__isset.operationHandle = true;
  return_val.operationHandle.__set_operationType(TOperationType::EXECUTE_STATEMENT);
  return_val.operationHandle.__set_hasResultSet(query_handle->returns_result_set());
  // Secret is inherited from session.
  TUniqueIdToTHandleIdentifier(query_handle->query_id(), secret,
                               &return_val.operationHandle.operationId);
  return_val.status.__set_statusCode(
      apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);

  VLOG_QUERY << "ExecuteStatement(): return_val=" << RedactedDebugString(return_val);
  return;

 return_error:
  discard_result(UnregisterQuery(query_handle->query_id(), false, &status));
  HS2_RETURN_ERROR(return_val, status.GetDetail(), SQLSTATE_GENERAL_ERROR);
}

Status ImpalaServer::SetupResultsCacheing(const QueryHandle& query_handle,
    shared_ptr<SessionState> session, int64_t cache_num_rows) {
  // Optionally enable result caching on the ClientRequestState.
  if (cache_num_rows > 0) {
    const TResultSetMetadata* result_set_md = query_handle->result_metadata();
    QueryResultSet* result_set =
        QueryResultSet::CreateHS2ResultSet(session->hs2_version, *result_set_md, nullptr,
            query_handle->query_options().stringify_map_keys, 0);
    RETURN_IF_ERROR(query_handle->SetResultCache(result_set, cache_num_rows));
  }
  return Status::OK();
}

void ImpalaServer::ExecuteStatement(TExecuteStatementResp& return_val,
    const TExecuteStatementReq& request) {
  VLOG_QUERY << "ExecuteStatement(): request=" << RedactedDebugString(request);
  ExecuteStatementCommon(return_val, request);
}

void ImpalaServer::ExecutePlannedStatement(
      TExecuteStatementResp& return_val,
      const TExecutePlannedStatementReq& request) {
  VLOG_QUERY << "ExecutePlannedStatement(): request=" << RedactedDebugString(request);
  VLOG(3) << "ExecutePlannedStatement(): plan=" << ThriftDebugString(request.plan);
  const ThriftServer::ConnectionContext* connection_context =
      ThriftServer::GetThreadConnectionContext();
  // This RPC is only supported on the external frontend service and should only be
  // exposed to trusted clients since it executes provided query plans directly with
  // no authorization checks (they are assumed to have been done by the trusted client)
  if (connection_context->server_name != EXTERNAL_FRONTEND_SERVER_NAME) {
    HS2_RETURN_ERROR(return_val, "Unsupported operation",
        SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED);
  }
  ExecuteStatementCommon(return_val, request.statementReq, &request.plan);
}

void ImpalaServer::InitQueryContext(
      TInitQueryContextResp& return_val) {
  VLOG_QUERY << "InitQueryContext(()";
  const ThriftServer::ConnectionContext* connection_context =
      ThriftServer::GetThreadConnectionContext();
  if (connection_context->server_name != EXTERNAL_FRONTEND_SERVER_NAME) {
    HS2_RETURN_ERROR(return_val, "Unsupported operation",
        SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED);
  }
  PrepareQueryContext(&return_val.query_ctx);
}


void ImpalaServer::GetTypeInfo(TGetTypeInfoResp& return_val,
    const TGetTypeInfoReq& request) {
  VLOG_QUERY << "GetTypeInfo(): request=" << RedactedDebugString(request);
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_TYPE_INFO);
  req.__set_get_type_info_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_TYPE_INFO);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetTypeInfo(): return_val=" << RedactedDebugString(return_val);
}

void ImpalaServer::GetCatalogs(TGetCatalogsResp& return_val,
    const TGetCatalogsReq& request) {
  VLOG_QUERY << "GetCatalogs(): request=" << RedactedDebugString(request);
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_CATALOGS);
  req.__set_get_catalogs_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_CATALOGS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetCatalogs(): return_val=" << RedactedDebugString(return_val);
}

void ImpalaServer::GetSchemas(TGetSchemasResp& return_val,
    const TGetSchemasReq& request) {
  VLOG_QUERY << "GetSchemas(): request=" << RedactedDebugString(request);
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_SCHEMAS);
  req.__set_get_schemas_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_SCHEMAS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetSchemas(): return_val=" << RedactedDebugString(return_val);
}

void ImpalaServer::GetTables(TGetTablesResp& return_val,
    const TGetTablesReq& request) {
  VLOG_QUERY << "GetTables(): request=" << RedactedDebugString(request);
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_TABLES);
  req.__set_get_tables_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_TABLES);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetTables(): return_val=" << RedactedDebugString(return_val);
}

void ImpalaServer::GetTableTypes(TGetTableTypesResp& return_val,
    const TGetTableTypesReq& request) {
  VLOG_QUERY << "GetTableTypes(): request=" << RedactedDebugString(request);
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_TABLE_TYPES);
  req.__set_get_table_types_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_TABLE_TYPES);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetTableTypes(): return_val=" << RedactedDebugString(return_val);

}

void ImpalaServer::GetColumns(TGetColumnsResp& return_val,
    const TGetColumnsReq& request) {
  VLOG_QUERY << "GetColumns(): request=" << RedactedDebugString(request);
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_COLUMNS);
  req.__set_get_columns_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_COLUMNS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetColumns(): return_val=" << RedactedDebugString(return_val);
}

void ImpalaServer::GetFunctions(TGetFunctionsResp& return_val,
    const TGetFunctionsReq& request) {
  VLOG_QUERY << "GetFunctions(): request=" << RedactedDebugString(request);
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_FUNCTIONS);
  req.__set_get_functions_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_FUNCTIONS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetFunctions(): return_val=" << RedactedDebugString(return_val);
}

void ImpalaServer::GetPrimaryKeys(TGetPrimaryKeysResp& return_val,
    const TGetPrimaryKeysReq& request) {
  VLOG_QUERY << "GetPrimaryKeys(): request=" << RedactedDebugString(request);
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_PRIMARY_KEYS);
  req.__set_get_primary_keys_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetPrimaryKeys(): return_val=" << RedactedDebugString(return_val);
}

void ImpalaServer::GetCrossReference(TGetCrossReferenceResp& return_val,
    const TGetCrossReferenceReq& request) {
  VLOG_QUERY << "GetCrossReference(): request=" << RedactedDebugString(request);
  HS2_RETURN_IF_ERROR(return_val, CheckNotShuttingDown(), SQLSTATE_GENERAL_ERROR);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_CROSS_REFERENCE);
  req.__set_get_cross_reference_req(request);

  TOperationHandle handle;
  thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetCrossReference(): return_val=" << RedactedDebugString(return_val);
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

  // Secret is inherited from session.
  TUniqueId query_id;
  TUniqueId op_secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &op_secret),
      SQLSTATE_GENERAL_ERROR);
  VLOG_ROW << "GetOperationStatus(): query_id=" << PrintId(query_id);

  QueryHandle query_handle;
  HS2_RETURN_IF_ERROR(
      return_val, GetActiveQueryHandle(query_id, &query_handle), SQLSTATE_GENERAL_ERROR);

  ScopedSessionState session_handle(this);
  const TUniqueId session_id = query_handle->session_id();
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(
          session_id, SecretArg::Operation(op_secret, query_id), &session),
      SQLSTATE_GENERAL_ERROR);

  {
    lock_guard<mutex> l(*query_handle->lock());
    TOperationState::type operation_state = query_handle->TOperationState();
    return_val.__set_operationState(operation_state);
    if (operation_state == TOperationState::ERROR_STATE) {
      DCHECK(!query_handle->query_status().ok());
      return_val.__set_errorMessage(query_handle->query_status().GetDetail());
      return_val.__set_sqlState(SQLSTATE_GENERAL_ERROR);
    } else {
      ClientRequestState::RetryState retry_state = query_handle->retry_state();
      if (retry_state != ClientRequestState::RetryState::RETRYING
          && retry_state != ClientRequestState::RetryState::RETRIED) {
        DCHECK(query_handle->query_status().ok());
      }
    }
  }
}

void ImpalaServer::CancelOperation(TCancelOperationResp& return_val,
    const TCancelOperationReq& request) {
  TUniqueId query_id;
  TUniqueId op_secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &op_secret),
      SQLSTATE_GENERAL_ERROR);
  VLOG_QUERY << "CancelOperation(): query_id=" << PrintId(query_id);

  QueryHandle query_handle;
  HS2_RETURN_IF_ERROR(
      return_val, GetActiveQueryHandle(query_id, &query_handle), SQLSTATE_GENERAL_ERROR);

  ScopedSessionState session_handle(this);
  const TUniqueId session_id = query_handle->session_id();
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(session_id, SecretArg::Operation(op_secret, query_id)),
      SQLSTATE_GENERAL_ERROR);
  HS2_RETURN_IF_ERROR(return_val, CancelInternal(query_id), SQLSTATE_GENERAL_ERROR);
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::CloseOperation(TCloseOperationResp& return_val,
    const TCloseOperationReq& request) {
  TCloseImpalaOperationReq request2;
  request2.operationHandle = request.operationHandle;
  TCloseImpalaOperationResp tmp_resp;
  CloseImpalaOperation(tmp_resp, request2);
  return_val.status = tmp_resp.status;
}

void ImpalaServer::CloseImpalaOperation(TCloseImpalaOperationResp& return_val,
    const TCloseImpalaOperationReq& request) {
  TUniqueId query_id;
  TUniqueId op_secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &op_secret),
      SQLSTATE_GENERAL_ERROR);
  VLOG_QUERY << "CloseOperation(): query_id=" << PrintId(query_id);

  QueryHandle query_handle;
  HS2_RETURN_IF_ERROR(
      return_val, GetActiveQueryHandle(query_id, &query_handle), SQLSTATE_GENERAL_ERROR);

  ScopedSessionState session_handle(this);
  const TUniqueId session_id = query_handle->session_id();
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(session_id, SecretArg::Operation(op_secret, query_id)),
      SQLSTATE_GENERAL_ERROR);
  if (query_handle->stmt_type() == TStmtType::DML) {
    Status query_status;
    if (query_handle->GetDmlStats(&return_val.dml_result, &query_status)) {
      return_val.__isset.dml_result = true;
    }
  }

  // TODO: use timeout to get rid of unwanted query_handle.
  HS2_RETURN_IF_ERROR(return_val, UnregisterQuery(query_id, true),
      SQLSTATE_GENERAL_ERROR);
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::GetResultSetMetadata(TGetResultSetMetadataResp& return_val,
    const TGetResultSetMetadataReq& request) {
  // Convert Operation id to TUniqueId and get the query exec state.
  TUniqueId query_id;
  TUniqueId op_secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &op_secret),
      SQLSTATE_GENERAL_ERROR);
  VLOG_QUERY << "GetResultSetMetadata(): query_id=" << PrintId(query_id);

  QueryHandle query_handle;
  HS2_RETURN_IF_ERROR(
      return_val, GetActiveQueryHandle(query_id, &query_handle), SQLSTATE_GENERAL_ERROR);

  ScopedSessionState session_handle(this);
  const TUniqueId session_id = query_handle->session_id();
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(session_id, SecretArg::Operation(op_secret, query_id)),
      SQLSTATE_GENERAL_ERROR);
  {
    lock_guard<mutex> l(*query_handle->lock());

    // Convert TResultSetMetadata to TGetResultSetMetadataResp
    const TResultSetMetadata* result_set_md = query_handle->result_metadata();
    DCHECK(result_set_md != NULL);
    if (result_set_md->columns.size() > 0) {
      return_val.__isset.schema = true;
      return_val.schema.columns.resize(result_set_md->columns.size());
      for (int i = 0; i < result_set_md->columns.size(); ++i) {
        return_val.schema.columns[i].__set_columnName(
            result_set_md->columns[i].columnName);
        return_val.schema.columns[i].position = i;
        return_val.schema.columns[i].typeDesc.types.resize(1);
        return_val.schema.columns[i].typeDesc.types[0] =
            ColumnToHs2Type(result_set_md->columns[i].columnType);
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
  TUniqueId query_id;
  TUniqueId op_secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &op_secret),
      SQLSTATE_GENERAL_ERROR);
  VLOG_ROW << "FetchResults(): query_id=" << PrintId(query_id)
           << " fetch_size=" << request.maxRows;


  QueryHandle query_handle;
  HS2_RETURN_IF_ERROR(
      return_val, GetActiveQueryHandle(query_id, &query_handle), SQLSTATE_GENERAL_ERROR);

  // Validate the secret and keep the session that originated the query alive.
  ScopedSessionState session_handle(this);
  const TUniqueId session_id = query_handle->session_id();
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(
          session_id, SecretArg::Operation(op_secret, query_id), &session),
      SQLSTATE_GENERAL_ERROR);

  int32_t num_results = 0;
  Status status = FetchInternal(query_id, session.get(), request.maxRows,
      fetch_first, &return_val, &num_results);

  VLOG_ROW << "FetchResults(): query_id=" << PrintId(query_id)
           << " #results=" << num_results
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
    return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
  }
}

void ImpalaServer::GetLog(TGetLogResp& return_val, const TGetLogReq& request) {
  TUniqueId query_id;
  TUniqueId op_secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &op_secret),
      SQLSTATE_GENERAL_ERROR);


  QueryHandle query_handle;
  HS2_RETURN_IF_ERROR(
      return_val, GetActiveQueryHandle(query_id, &query_handle), SQLSTATE_GENERAL_ERROR);

  // GetLog doesn't have an associated session handle, so we presume that this request
  // should keep alive the same session that orignated the query.
  ScopedSessionState session_handle(this);
  const TUniqueId session_id = query_handle->session_id();
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(session_id, SecretArg::Operation(op_secret, query_id)),
      SQLSTATE_GENERAL_ERROR);

  stringstream ss;
  Coordinator* coord = query_handle->GetCoordinator();
  if (coord != nullptr) {
    // Report progress
    ss << coord->scan_progress().ToString() << "\n";
  }
  // Report the query status, if the query failed or has been retried.
  if (query_handle->IsRetriedQuery()) {
    QueryHandle original_query_handle;
    Status status = GetQueryHandle(query_id, &original_query_handle);
    if (UNLIKELY(!status.ok())) {
      VLOG(1) << "Error in GetLog, could not get query handle: " << status.GetDetail();
      HS2_RETURN_ERROR(return_val, status.GetDetail(), SQLSTATE_GENERAL_ERROR);
      return;
    }
    DCHECK(!original_query_handle->query_status().ok());
    ss << Substitute(GET_LOG_QUERY_RETRY_INFO_FORMAT,
        original_query_handle->query_status().GetDetail(),
        PrintId(query_handle->query_id()));
  }
  {
    // Take the lock to ensure that if the client sees a query_state == EXCEPTION, it is
    // guaranteed to see the error query_status.
    lock_guard<mutex> l(*query_handle->lock());
    Status query_status = query_handle->query_status();
    DCHECK_EQ(query_handle->exec_state() == ClientRequestState::ExecState::ERROR,
        !query_status.ok());
    // If the query status is !ok, include the status error message at the top of the log.
    if (!query_status.ok()) ss << query_status.GetDetail();
  }

  // Report analysis errors
  ss << join(query_handle->GetAnalysisWarnings(), "\n");
  // Report queuing reason if the admission controller queued the query.
  const string* admission_result = query_handle->summary_profile()->GetInfoString(
      AdmissionController::PROFILE_INFO_KEY_ADMISSION_RESULT);
  if (admission_result != nullptr) {
    if (*admission_result == AdmissionController::PROFILE_INFO_VAL_QUEUED) {
      ss << AdmissionController::PROFILE_INFO_KEY_ADMISSION_RESULT << " : "
         << *admission_result << "\n";
      const string* queued_reason = query_handle->summary_profile()->GetInfoString(
          AdmissionController::PROFILE_INFO_KEY_LAST_QUEUED_REASON);
      if (queued_reason != nullptr) {
        ss << AdmissionController::PROFILE_INFO_KEY_LAST_QUEUED_REASON << " : "
           << *queued_reason << "\n";
      }
    }
  }
  if (coord != nullptr) {
    // Report execution errors
    ss << coord->GetErrorLog();
  }
  return_val.log = ss.str();
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
  VLOG_RPC << "GetLog(): query_id=" << PrintId(query_id) << ", log=" << return_val.log;
}

void ImpalaServer::GetExecSummary(TGetExecSummaryResp& return_val,
    const TGetExecSummaryReq& request) {
  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.sessionHandle.sessionId, &session_id, &secret),
      SQLSTATE_GENERAL_ERROR);
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(
          session_id, SecretArg::Session(secret), &session),
      SQLSTATE_GENERAL_ERROR);
  if (session == NULL) {
    HS2_RETURN_ERROR(return_val, Substitute("Invalid session id: $0",
        PrintId(session_id)), SQLSTATE_GENERAL_ERROR);
  }

  TUniqueId query_id, op_secret;
  HS2_RETURN_IF_ERROR(return_val,
      THandleIdentifierToTUniqueId(
          request.operationHandle.operationId, &query_id, &op_secret),
      SQLSTATE_GENERAL_ERROR);

  TExecSummary summary;
  TExecSummary original_summary;
  bool was_retried = false;
  Status status = GetExecSummary(query_id, GetEffectiveUser(*session), &summary,
      &original_summary, &was_retried);
  HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);
  if (request.include_query_attempts && was_retried) {
    return_val.failed_summaries.emplace_back(original_summary);
    return_val.__isset.failed_summaries = true;
  }
  return_val.__set_summary(summary);
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

// Add the given Thrift profile to the list of failed thrift profiles for the given
// TGetRuntimeProfileResp.
void SetFailedProfile(
    TRuntimeProfileTree* thrift_profile, TGetRuntimeProfileResp& return_val) {
  DCHECK(thrift_profile != nullptr);
  vector<TRuntimeProfileTree> failed_thrift_profiles;
  failed_thrift_profiles.emplace_back(*thrift_profile);
  return_val.__set_failed_thrift_profiles(failed_thrift_profiles);
}

// Add the given string profile to the list of failed profiles for the given
// TGetRuntimeProfileResp.
void SetFailedProfile(stringstream* ss, TGetRuntimeProfileResp& return_val) {
  DCHECK(ss != nullptr);
  vector<string> original_profiles;
  original_profiles.emplace_back(ss->str());
  return_val.__set_failed_profiles(original_profiles);
}

void ImpalaServer::SetProfile(TGetRuntimeProfileResp& get_profile_resp,
    TRuntimeProfileFormat::type profile_format, const RuntimeProfileOutput& profile,
    bool set_failed_profile) {
  if (profile_format == TRuntimeProfileFormat::THRIFT) {
    if (set_failed_profile) {
      SetFailedProfile(profile.thrift_output, get_profile_resp);
    } else {
      get_profile_resp.__set_thrift_profile(*profile.thrift_output);
    }
  } else if (profile_format == TRuntimeProfileFormat::JSON) {
    DCHECK(profile.json_output != nullptr);
    RuntimeProfile::JsonProfileToString(
        *profile.json_output, /*pretty=*/false, profile.string_output);
    if (set_failed_profile) {
      SetFailedProfile(profile.string_output, get_profile_resp);
    } else {
      get_profile_resp.__set_profile(profile.string_output->str());
    }
  } else {
    DCHECK(profile_format == TRuntimeProfileFormat::STRING
        || profile_format == TRuntimeProfileFormat::BASE64);
    if (set_failed_profile) {
      SetFailedProfile(profile.string_output, get_profile_resp);
    } else {
      get_profile_resp.__set_profile(profile.string_output->str());
    }
  }
}

void ImpalaServer::GetRuntimeProfile(
    TGetRuntimeProfileResp& return_val, const TGetRuntimeProfileReq& request) {
  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.sessionHandle.sessionId, &session_id, &secret), SQLSTATE_GENERAL_ERROR);
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(
          session_id, SecretArg::Session(secret), &session),
      SQLSTATE_GENERAL_ERROR);
  if (session == NULL) {
    HS2_RETURN_ERROR(return_val, Substitute("Invalid session id: $0",
        PrintId(session_id)), SQLSTATE_GENERAL_ERROR);
  }

  TUniqueId query_id, op_secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &op_secret),
      SQLSTATE_GENERAL_ERROR);

  VLOG_RPC << "GetRuntimeProfile(): query_id=" << PrintId(query_id);

  // Set the RuntimeProfileOutput for the retried query (e.g. the second attempt of a
  // query). If the query has been retried this will be the retried profile. If the
  // query has not been retried then all entries will be nullptr.
  RuntimeProfileOutput retried_profile;
  stringstream retried_ss;
  TRuntimeProfileTree retried_thrift_profile;
  rapidjson::Document retried_json_profile(rapidjson::kObjectType);
  retried_profile.string_output = &retried_ss;
  retried_profile.thrift_output = &retried_thrift_profile;
  retried_profile.json_output = &retried_json_profile;

  // Set the RuntimeProfileOutput for the original query (e.g. the first attempt of a
  // query). If the query has been retried this will be the original profile (the
  // profile of the original query attempt). If the query has not been retried then
  // this will be the normal runtime profile.
  RuntimeProfileOutput profile;
  stringstream ss;
  TRuntimeProfileTree thrift_profile;
  rapidjson::Document json_profile(rapidjson::kObjectType);
  profile.string_output = &ss;
  profile.thrift_output = &thrift_profile;
  profile.json_output = &json_profile;

  // Get the runtime profiles and load them into the given RuntimeProfileOutputs.
  bool was_retried = false;
  HS2_RETURN_IF_ERROR(return_val,
      GetRuntimeProfileOutput(query_id, GetEffectiveUser(*session), request.format,
                          &profile, &retried_profile, &was_retried),
      SQLSTATE_GENERAL_ERROR);

  // Set the Thrift response in TGetRuntimeProfileResp. If the query was retried, then
  // the retried profile should be returned. If not, the regular profile should be
  // returned.
  SetProfile(return_val, request.format, was_retried ? retried_profile : profile);

  // If 'request.include_query_attempts' include the retried profile in the Thrift
  // response.
  if (was_retried && request.include_query_attempts) {
    SetProfile(return_val, request.format, profile, true);
  }
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::GetDelegationToken(TGetDelegationTokenResp& return_val,
    const TGetDelegationTokenReq& req) {
  return_val.status.__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
  return_val.status.__set_errorMessage("Not implemented");
}

void ImpalaServer::GetBackendConfig(TGetBackendConfigResp& return_val,
    const TGetBackendConfigReq& req) {
  VLOG_QUERY << "GetBackendConfig(): req=" << RedactedDebugString(req);
  const ThriftServer::ConnectionContext* connection_context =
      ThriftServer::GetThreadConnectionContext();
  if (connection_context->server_name != EXTERNAL_FRONTEND_SERVER_NAME) {
    HS2_RETURN_ERROR(return_val, "Unsupported operation",
        SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED);
  }
  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      req.sessionHandle.sessionId, &session_id, &secret), SQLSTATE_GENERAL_ERROR);
  HS2_RETURN_IF_ERROR(return_val,
      PopulateThriftBackendGflags(return_val.backend_config),
      SQLSTATE_GENERAL_ERROR);

  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
  VLOG_RPC << "GetBackendConfig(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetExecutorMembership(
    TGetExecutorMembershipResp& return_val, const TGetExecutorMembershipReq& req) {
  VLOG_QUERY << "GetExecutorMembership(): req=" << RedactedDebugString(req);
  const ThriftServer::ConnectionContext* connection_context =
      ThriftServer::GetThreadConnectionContext();
  if (connection_context->server_name != EXTERNAL_FRONTEND_SERVER_NAME) {
    HS2_RETURN_ERROR(
        return_val, "Unsupported operation", SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED);
  }
  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val,
      THandleIdentifierToTUniqueId(req.sessionHandle.sessionId, &session_id, &secret),
      SQLSTATE_GENERAL_ERROR);
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(session_id, SecretArg::Session(secret), &session),
      SQLSTATE_GENERAL_ERROR);
  if (session == NULL) {
    HS2_RETURN_ERROR(return_val,
        Substitute("Invalid session id: $0", PrintId(session_id)),
        SQLSTATE_GENERAL_ERROR);
  }

  ClusterMembershipMgr* cluster_membership_mgr =
      DCHECK_NOTNULL(ExecEnv::GetInstance()->cluster_membership_mgr());
  ClusterMembershipMgr::SnapshotPtr membership_snapshot =
      cluster_membership_mgr->GetSnapshot();
  DCHECK_NOTNULL(membership_snapshot.get());

  // Populate an instance of TUpdateExecutorMembershipRequest
  // with the field values retrieved from membership_snapshot
  PopulateExecutorMembershipRequest(membership_snapshot,
      cluster_membership_mgr->GetExpectedExecGroupSets(), return_val.executor_membership);

  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
  VLOG_RPC << "GetExecutorMembership(): return_val=" << ThriftDebugString(return_val);
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

void ImpalaServer::AddSessionToConnection(
    const TUniqueId& session_id, SessionState* session) {
  const TUniqueId& connection_id = ThriftServer::GetThreadConnectionId();
  {
    lock_guard<mutex> l(connection_to_sessions_map_lock_);
    connection_to_sessions_map_[connection_id].insert(session_id);
  }

  std::lock_guard<std::mutex> session_lock(session->lock);
  if (session->connections.empty()) {
    // This session was previously disconnected but now has an associated
    // connection. It should no longer be considered for the disconnected timeout.
    UnregisterSessionTimeout(FLAGS_disconnected_session_timeout);
  }
  session->connections.insert(connection_id);
}

void ImpalaServer::PingImpalaHS2Service(TPingImpalaHS2ServiceResp& return_val,
    const TPingImpalaHS2ServiceReq& req) {
  VLOG_QUERY << "PingImpalaHS2Service(): request=" << RedactedDebugString(req);
  TUniqueId session_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val,
      THandleIdentifierToTUniqueId(req.sessionHandle.sessionId, &session_id, &secret),
      SQLSTATE_GENERAL_ERROR);
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  HS2_RETURN_IF_ERROR(return_val,
      session_handle.WithSession(session_id, SecretArg::Session(secret), &session),
      SQLSTATE_GENERAL_ERROR);
  if (session == NULL) {
    HS2_RETURN_ERROR(return_val,
        Substitute("Invalid session id: $0", PrintId(session_id)),
        SQLSTATE_GENERAL_ERROR);
  }

  return_val.__set_version(GetVersionString(true));
  if (ExecEnv::GetInstance()->get_enable_webserver() && FLAGS_ping_expose_webserver_url) {
    return_val.__set_webserver_address(ExecEnv::GetInstance()->webserver()->url());
  } else {
    return_val.__set_webserver_address("");
  }
  return_val.__set_timestamp(MonotonicStopWatch::Now());
  VLOG_RPC << "PingImpalaHS2Service(): return_val=" << ThriftDebugString(return_val);
}
}
