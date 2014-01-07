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

#include "service/impala-server.h"

#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/unordered_set.hpp>
#include <jni.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <gtest/gtest.h>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>
#include <google/heap-profiler.h>
#include <google/malloc_extension.h>

#include "common/logging.h"
#include "common/version.h"
#include "exprs/expr.h"
#include "runtime/raw-value.h"
#include "service/query-exec-state.h"
#include "util/debug-util.h"
#include "rpc/thrift-util.h"
#include "util/impalad-metrics.h"

using namespace std;
using namespace boost;
using namespace boost::algorithm;
using namespace boost::uuids;
using namespace apache::thrift;
using namespace apache::hive::service::cli::thrift;
using namespace beeswax; // Converting QueryState

// HiveServer2 error returning macro
#define HS2_RETURN_ERROR(return_val, error_msg, error_state) \
  do { \
    return_val.status.__set_statusCode( \
        apache::hive::service::cli::thrift::TStatusCode::ERROR_STATUS); \
    return_val.status.__set_errorMessage(error_msg); \
    return_val.status.__set_sqlState(error_state); \
    return; \
  } while (false)

#define HS2_RETURN_IF_ERROR(return_val, status, error_state) \
  do { \
    if (UNLIKELY(!status.ok())) { \
      HS2_RETURN_ERROR(return_val, status.GetErrorMsg(), error_state); \
      return; \
    } \
  } while (false)

namespace impala {

// TRow result set for HiveServer2
class ImpalaServer::TRowQueryResultSet : public ImpalaServer::QueryResultSet {
 public:
  // Rows are added into rowset.
  TRowQueryResultSet(const TResultSetMetadata& metadata, TRowSet* rowset)
    : metadata_(metadata), result_set_(rowset) { }

  virtual ~TRowQueryResultSet() {}

  // Convert expr value to HS2 TRow and store it in TRowSet.
  virtual Status AddOneRow(const vector<void*>& col_values, const vector<int>& scales) {
    int num_col = col_values.size();
    DCHECK_EQ(num_col, metadata_.columns.size());
    result_set_->rows.push_back(TRow());
    TRow& trow = result_set_->rows.back();
    trow.colVals.resize(num_col);
    for (int i = 0; i < num_col; ++i) {
      ImpalaServer::ExprValueToHiveServer2TColumnValue(col_values[i],
          metadata_.columns[i].columnType, &(trow.colVals[i]));
    }
    return Status::OK;
  }

  // Convert TResultRow to HS2 TRow and store it in TRowSet.
  virtual Status AddOneRow(const TResultRow& row) {
    int num_col = row.colVals.size();
    DCHECK_EQ(num_col, metadata_.columns.size());
    result_set_->rows.push_back(TRow());
    TRow& trow = result_set_->rows.back();
    trow.colVals.resize(num_col);
    for (int i = 0; i < num_col; ++i) {
      ImpalaServer::TColumnValueToHiveServer2TColumnValue(row.colVals[i],
          metadata_.columns[i].columnType, &(trow.colVals[i]));
    }
    return Status::OK;
  }

 private:
  // Metadata of the result set
  const TResultSetMetadata& metadata_;

  // Points to the TRowSet to be filled. Not owned here.
  TRowSet* result_set_;
};

void ImpalaServer::ExecuteMetadataOp(const THandleIdentifier& session_handle,
    TMetadataOpRequest* request, TOperationHandle* handle,
    apache::hive::service::cli::thrift::TStatus* status) {
  TUniqueId session_id;
  TUniqueId secret;
  Status unique_id_status =
      THandleIdentifierToTUniqueId(session_handle, &session_id, &secret);
  if (!unique_id_status.ok()) {
    status->__set_statusCode(
        apache::hive::service::cli::thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(unique_id_status.GetErrorMsg());
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }
  ScopedSessionState scoped_session(this);
  shared_ptr<SessionState> session;
  Status get_session_status = scoped_session.WithSession(session_id, &session);
  if (!get_session_status.ok()) {
    status->__set_statusCode(
        apache::hive::service::cli::thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(get_session_status.GetErrorMsg());
    // TODO: (here and elsewhere) - differentiate between invalid session ID and timeout
    // when setting the error code.
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }

  if (session == NULL) {
    status->__set_statusCode(
        apache::hive::service::cli::thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage("Invalid session ID");
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }
  TQueryContext query_ctxt;
  session->ToThrift(session_id, &query_ctxt.session);
  request->__set_session(query_ctxt.session);

  shared_ptr<QueryExecState> exec_state;
  // There is no user-supplied query text available because this metadata operation comes
  // from an RPC. As a best effort, we use the type of the operation.
  map<int, const char*>::const_iterator query_text_it =
      _TMetadataOpcode_VALUES_TO_NAMES.find(request->opcode);
  const string& query_text = query_text_it == _TMetadataOpcode_VALUES_TO_NAMES.end() ?
      "N/A" : query_text_it->second;
  query_ctxt.request.stmt = query_text;
  exec_state.reset(new QueryExecState(query_ctxt, exec_env_,
      frontend_.get(), this, session));
  Status register_status = RegisterQuery(session, exec_state);
  if (!register_status.ok()) {
    status->__set_statusCode(
        apache::hive::service::cli::thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(register_status.GetErrorMsg());
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }

  // start execution of metadata first;
  PrepareQueryContext(&query_ctxt);
  Status exec_status = exec_state->Exec(*request);
  if (!exec_status.ok()) {
    status->__set_statusCode(
        apache::hive::service::cli::thrift::TStatusCode::ERROR_STATUS);
    status->__set_errorMessage(exec_status.GetErrorMsg());
    status->__set_sqlState(SQLSTATE_GENERAL_ERROR);
    return;
  }

  exec_state->UpdateQueryState(QueryState::FINISHED);

  handle->__set_hasResultSet(true);
  // TODO: create secret for operationId
  TUniqueId operation_id = exec_state->query_id();
  TUniqueIdToTHandleIdentifier(operation_id, operation_id, &(handle->operationId));
  status->__set_statusCode(
      apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);
}

Status ImpalaServer::FetchInternal(const TUniqueId& query_id, int32_t fetch_size,
    apache::hive::service::cli::thrift::TFetchResultsResp* fetch_results) {
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state == NULL) return Status("Invalid query handle");

  // FetchResults doesn't have an associated session handle, so we presume that this
  // request should keep alive the same session that orignated the query.
  ScopedSessionState session_handle(this);
  const TUniqueId session_id = exec_state->session_id();
  RETURN_IF_ERROR(session_handle.WithSession(session_id));

  lock_guard<mutex> frl(*exec_state->fetch_rows_lock());
  lock_guard<mutex> l(*exec_state->lock());

  if (exec_state->query_state() == QueryState::EXCEPTION) {
    // we got cancelled or saw an error; either way, return now
    return exec_state->query_status();
  }

  if (exec_state->num_rows_fetched() == 0) {
    exec_state->query_events()->MarkEvent("First row fetched");
  }

  fetch_results->results.__set_startRowOffset(exec_state->num_rows_fetched());
  TRowQueryResultSet result_set(*(exec_state->result_metadata()),
      &(fetch_results->results));
  RETURN_IF_ERROR(exec_state->FetchRows(fetch_size, &result_set));
  fetch_results->__isset.results = true;
  fetch_results->__set_hasMoreRows(!exec_state->eos());
  return Status::OK;
}

Status ImpalaServer::TExecuteStatementReqToTQueryContext(
    const TExecuteStatementReq execute_request, TQueryContext* query_ctxt) {
  query_ctxt->request.stmt = execute_request.statement;
  VLOG_QUERY << "TExecuteStatementReq: " << ThriftDebugString(execute_request);
  {
    shared_ptr<SessionState> session_state;
    TUniqueId session_id;
    TUniqueId secret;
    RETURN_IF_ERROR(THandleIdentifierToTUniqueId(execute_request.sessionHandle.sessionId,
        &session_id, &secret));

    RETURN_IF_ERROR(GetSessionState(session_id, &session_state));
    session_state->ToThrift(session_id, &query_ctxt->session);
    lock_guard<mutex> l(session_state->lock);
    query_ctxt->request.query_options = session_state->default_query_options;
  }

  if (execute_request.__isset.confOverlay) {
    map<string, string>::const_iterator conf_itr = execute_request.confOverlay.begin();
    for (; conf_itr != execute_request.confOverlay.end(); ++conf_itr) {
      RETURN_IF_ERROR(SetQueryOptions(conf_itr->first, conf_itr->second,
          &query_ctxt->request.query_options));
    }
    VLOG_QUERY << "TClientRequest.queryOptions: "
               << ThriftDebugString(query_ctxt->request.query_options);
  }
  return Status::OK;
}

// HiveServer2 API
void ImpalaServer::OpenSession(TOpenSessionResp& return_val,
    const TOpenSessionReq& request) {
  VLOG_QUERY << "OpenSession(): request=" << ThriftDebugString(request);

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
  shared_ptr<SessionState> state(new SessionState());
  state->closed = false;
  state->start_time = TimestampValue::local_time();
  state->session_type = TSessionType::HIVESERVER2;
  state->network_address = ThriftServer::GetThreadConnectionContext()->network_address;

  // If the username was set by a lower-level transport, use it.
  const ThriftServer::Username& username =
      ThriftServer::GetThreadConnectionContext()->username;
  if (!username.empty()) {
    state->user = username;
  } else {
    state->user = request.username;
  }

  // TODO: request.configuration might specify database.
  state->database = "default";

  // Convert request.configuration to session default query options.
  state->default_query_options = default_query_options_;
  if (request.__isset.configuration) {
    map<string, string>::const_iterator conf_itr = request.configuration.begin();
    for (; conf_itr != request.configuration.end(); ++conf_itr) {
      // If the current user is a valid proxy user, he/she can optionally perform
      // authorization requests on behalf of another user. This is done by setting the
      // 'impala.doas.user' Hive Server 2 configuration property.
      if (conf_itr->first == "impala.doas.user") {
        state->do_as_user = conf_itr->second;
        Status status = AuthorizeProxyUser(state->user, state->do_as_user);
        HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);
        continue;
      }
      Status status = SetQueryOptions(conf_itr->first, conf_itr->second,
          &state->default_query_options);
      HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);
    }
  }
  TQueryOptionsToMap(state->default_query_options, &return_val.configuration);

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

  ImpaladMetrics::IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS->Increment(1L);

  return_val.__isset.configuration = true;
  return_val.status.__set_statusCode(
      apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);
  return_val.serverProtocolVersion = TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V1;
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
  return_val.status.__set_statusCode(
      apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);
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
      return_val.infoValue.__set_stringValue(IMPALA_BUILD_VERSION);
      break;
    default:
      HS2_RETURN_ERROR(return_val, "Unsupported operation",
          SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED);
  }
  return_val.status.__set_statusCode(
      apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::ExecuteStatement(TExecuteStatementResp& return_val,
    const TExecuteStatementReq& request) {
  VLOG_QUERY << "ExecuteStatement(): request=" << ThriftDebugString(request);

  TQueryContext query_ctxt;
  Status status = TExecuteStatementReqToTQueryContext(request, &query_ctxt);
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
    HS2_RETURN_IF_ERROR(
        return_val, Status("Invalid session ID"), SQLSTATE_GENERAL_ERROR);
  }

  shared_ptr<QueryExecState> exec_state;
  status = Execute(&query_ctxt, session, &exec_state);
  HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);

  return_val.__isset.operationHandle = true;
  return_val.operationHandle.__set_operationType(TOperationType::EXECUTE_STATEMENT);

  exec_state->UpdateQueryState(QueryState::RUNNING);
  return_val.operationHandle.__set_hasResultSet(exec_state->returns_result_set());
  // TODO: create secret for operationId and store the secret in exec_state
  TUniqueIdToTHandleIdentifier(exec_state->query_id(), exec_state->query_id(),
                               &return_val.operationHandle.operationId);

  // start thread to wait for results to become available, which will allow
  // us to advance query state to FINISHED or EXCEPTION
  Thread wait_thread(
      "impala-server", "wait-thread", &ImpalaServer::Wait, this, exec_state);

  return_val.status.__set_statusCode(
      apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);

  VLOG_QUERY << "ExecuteStatement(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetTypeInfo(TGetTypeInfoResp& return_val,
    const TGetTypeInfoReq& request) {
  VLOG_QUERY << "GetTypeInfo(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_TYPE_INFO);
  req.__set_get_type_info_req(request);

  TOperationHandle handle;
  apache::hive::service::cli::thrift::TStatus status;
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
  apache::hive::service::cli::thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_CATALOGS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetCatalogs(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetSchemas(
    TGetSchemasResp& return_val,
    const TGetSchemasReq& request) {
  VLOG_QUERY << "GetSchemas(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_SCHEMAS);
  req.__set_get_schemas_req(request);

  TOperationHandle handle;
  apache::hive::service::cli::thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_SCHEMAS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetSchemas(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetTables(
    TGetTablesResp& return_val,
    const TGetTablesReq& request) {
  VLOG_QUERY << "GetTables(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_TABLES);
  req.__set_get_tables_req(request);

  TOperationHandle handle;
  apache::hive::service::cli::thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_TABLES);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetTables(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetTableTypes(
    TGetTableTypesResp& return_val,
    const TGetTableTypesReq& request) {
  VLOG_QUERY << "GetTableTypes(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_TABLE_TYPES);
  req.__set_get_table_types_req(request);

  TOperationHandle handle;
  apache::hive::service::cli::thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_TABLE_TYPES);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetTableTypes(): return_val=" << ThriftDebugString(return_val);

}

void ImpalaServer::GetColumns(
    TGetColumnsResp& return_val,
    const TGetColumnsReq& request) {
  VLOG_QUERY << "GetColumns(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_COLUMNS);
  req.__set_get_columns_req(request);

  TOperationHandle handle;
  apache::hive::service::cli::thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_COLUMNS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetColumns(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetFunctions(
    TGetFunctionsResp& return_val,
    const TGetFunctionsReq& request) {
  VLOG_QUERY << "GetFunctions(): request=" << ThriftDebugString(request);

  TMetadataOpRequest req;
  req.__set_opcode(TMetadataOpcode::GET_FUNCTIONS);
  req.__set_get_functions_req(request);

  TOperationHandle handle;
  apache::hive::service::cli::thrift::TStatus status;
  ExecuteMetadataOp(request.sessionHandle.sessionId, &req, &handle, &status);
  handle.__set_operationType(TOperationType::GET_FUNCTIONS);
  return_val.__set_operationHandle(handle);
  return_val.__set_status(status);

  VLOG_QUERY << "GetFunctions(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::GetOperationStatus(
    TGetOperationStatusResp& return_val,
    const TGetOperationStatusReq& request) {
  if (request.operationHandle.operationId.guid.size() == 0) {
    // An empty operation handle identifier means no execution and no result for this
    // query (USE <database>).
    VLOG_ROW << "GetOperationStatus(): guid size 0";
    return_val.operationState = TOperationState::FINISHED_STATE;
    return_val.status.__set_statusCode(
        apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);
    return;
  }

  // TODO: check secret
  TUniqueId query_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);
  VLOG_ROW << "GetOperationStatus(): query_id=" << PrintId(query_id);

  lock_guard<mutex> l(query_exec_state_map_lock_);
  QueryExecStateMap::iterator entry = query_exec_state_map_.find(query_id);
  if (entry != query_exec_state_map_.end()) {
    QueryState::type query_state = entry->second->query_state();
    TOperationState::type operation_state = QueryStateToTOperationState(query_state);
    return_val.__set_operationState(operation_state);
    return;
  }

  // No handle was found
  HS2_RETURN_ERROR(return_val, "Invalid query handle", SQLSTATE_GENERAL_ERROR);
}

void ImpalaServer::CancelOperation(TCancelOperationResp& return_val,
    const TCancelOperationReq& request) {
  TUniqueId query_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);
  VLOG_QUERY << "CancelOperation(): query_id=" << PrintId(query_id);

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state.get() == NULL) {
    // No handle was found
    HS2_RETURN_ERROR(return_val, "Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }
  ScopedSessionState session_handle(this);
  const TUniqueId session_id = exec_state->session_id();
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id),
      SQLSTATE_GENERAL_ERROR);

  Status status = CancelInternal(query_id);
  HS2_RETURN_IF_ERROR(return_val, status, SQLSTATE_GENERAL_ERROR);
  return_val.status.__set_statusCode(
    apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::CloseOperation(TCloseOperationResp& return_val,
    const TCloseOperationReq& request) {
  TUniqueId query_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);
  VLOG_QUERY << "CloseOperation(): query_id=" << PrintId(query_id);

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state.get() == NULL) {
    // No handle was found
    HS2_RETURN_ERROR(return_val, "Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }
  ScopedSessionState session_handle(this);
  const TUniqueId session_id = exec_state->session_id();
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id),
      SQLSTATE_GENERAL_ERROR);

  // TODO: use timeout to get rid of unwanted exec_state.
  if (!UnregisterQuery(query_id)) {
    // No handle was found
    HS2_RETURN_ERROR(return_val, "Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }
  return_val.status.__set_statusCode(
    apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);
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

  // Look up the session ID (which takes session_state_map_lock_) before taking the query
  // exec state lock.
  TUniqueId session_id;
  if (!GetSessionIdForQuery(query_id, &session_id)) {
    HS2_RETURN_ERROR(return_val, "Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }
  ScopedSessionState session_handle(this);
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id),
      SQLSTATE_GENERAL_ERROR);

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
  if (exec_state.get() == NULL) {
    VLOG_QUERY << "GetResultSetMetadata(): invalid query handle";
    // No handle was found
    HS2_RETURN_ERROR(return_val, "Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }
  {
    // make sure we release the lock on exec_state if we see any error
    lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());

    // Convert TResultSetMetadata to TGetResultSetMetadataResp
    const TResultSetMetadata* result_set_md = exec_state->result_metadata();
    DCHECK(result_set_md != NULL);
    if (result_set_md->columns.size() > 0) {
      return_val.__isset.schema = true;
      return_val.schema.columns.resize(result_set_md->columns.size());
      for (int i = 0; i < result_set_md->columns.size(); ++i) {
        return_val.schema.columns[i].__set_columnName(
            result_set_md->columns[i].columnName);
        return_val.schema.columns[i].position = i;
        return_val.schema.columns[i].typeDesc.types.resize(1);
        return_val.schema.columns[i].typeDesc.types[0].__isset.primitiveEntry = true;
        TPrimitiveType::type col_type = result_set_md->columns[i].columnType;
        return_val.schema.columns[i].typeDesc.types[0].primitiveEntry.__set_type(
            TypeToHiveServer2Type(ThriftToType(col_type)));
      }
    }
  }

  return_val.status.__set_statusCode(
      apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);
  VLOG_QUERY << "GetResultSetMetadata(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::FetchResults(TFetchResultsResp& return_val,
    const TFetchResultsReq& request) {
  if (request.orientation != TFetchOrientation::FETCH_NEXT) {
    // We can't do anythng other than FETCH_NEXT
    HS2_RETURN_ERROR(return_val, "Unsupported operation",
        SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED);
  }

  // Convert Operation id to TUniqueId and get the query exec state.
  // TODO: check secret
  TUniqueId query_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);
  VLOG_ROW << "FetchResults(): query_id=" << PrintId(query_id)
           << " fetch_size=" << request.maxRows;

  // FetchInternal takes care of extending the session
  Status status = FetchInternal(query_id, request.maxRows, &return_val);
  VLOG_ROW << "FetchResults(): #results=" << return_val.results.rows.size()
           << " has_more=" << (return_val.hasMoreRows ? "true" : "false");
  if (!status.ok()) {
    UnregisterQuery(query_id);
    HS2_RETURN_ERROR(return_val, status.GetErrorMsg(), SQLSTATE_GENERAL_ERROR);
  }
  return_val.status.__set_statusCode(
      apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);
}

void ImpalaServer::GetLog(TGetLogResp& return_val, const TGetLogReq& request) {
  TUniqueId query_id;
  TUniqueId secret;
  HS2_RETURN_IF_ERROR(return_val, THandleIdentifierToTUniqueId(
      request.operationHandle.operationId, &query_id, &secret), SQLSTATE_GENERAL_ERROR);

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state.get() == NULL) {
    // No handle was found
    HS2_RETURN_ERROR(return_val, "Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }

  // GetLog doesn't have an associated session handle, so we presume that this request
  // should keep alive the same session that orignated the query.
  ScopedSessionState session_handle(this);
  const TUniqueId session_id = exec_state->session_id();
  HS2_RETURN_IF_ERROR(return_val, session_handle.WithSession(session_id),
      SQLSTATE_GENERAL_ERROR);

  if (exec_state->coord() != NULL) {
    // Report progress and all errors
    // Hue parses the progress string to do progress indication.
    stringstream ss;
    ss << exec_state->coord()->progress().ToString() << "\n";
    ss << exec_state->coord()->GetErrorLog();
    return_val.log = ss.str();
    return_val.status.__set_statusCode(
        apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS);
  }
}

inline Status ImpalaServer::THandleIdentifierToTUniqueId(
    const apache::hive::service::cli::thrift::THandleIdentifier& handle,
    TUniqueId* unique_id, TUniqueId* secret) {
  if (handle.guid.length() != 16 || handle.secret.length() != 16) {
    stringstream ss;
    ss << "Malformed THandleIdentifier (guid size: " << handle.guid.length()
       << ", expected 16, secret size: " << handle.secret.length() << ", expected 16)";
    return Status(ss.str());
  }
  memcpy(&(unique_id->hi), handle.guid.c_str(), 8);
  memcpy(&(unique_id->lo), handle.guid.c_str() + 8, 8);
  memcpy(&(secret->hi), handle.secret.c_str(), 8);
  memcpy(&(secret->lo), handle.secret.c_str() + 8, 8);

  return Status::OK;
}

inline void ImpalaServer::TUniqueIdToTHandleIdentifier(
    const TUniqueId& unique_id, const TUniqueId& secret,
    apache::hive::service::cli::thrift::THandleIdentifier* handle) {
  char uuid[16];
  memcpy((void*)uuid, &unique_id.hi, 8);
  memcpy((void*)(uuid + 8), &unique_id.lo, 8);
  handle->guid.assign(uuid, 16);
  memcpy((void*)uuid, &secret.hi, 8);
  memcpy((void*)(uuid + 8), &secret.lo, 8);
  handle->secret.assign(uuid, 16);
}

void ImpalaServer::TColumnValueToHiveServer2TColumnValue(const TColumnValue& col_val,
    const TPrimitiveType::type& type,
    apache::hive::service::cli::thrift::TColumnValue* hs2_col_val) {
  switch (type) {
    case TPrimitiveType::BOOLEAN:
      hs2_col_val->__isset.boolVal = true;
      hs2_col_val->boolVal.value = col_val.boolVal;
      hs2_col_val->boolVal.__isset.value = col_val.__isset.boolVal;
      break;
    case TPrimitiveType::TINYINT:
      hs2_col_val->__isset.byteVal = true;
      hs2_col_val->byteVal.value = col_val.intVal;
      hs2_col_val->byteVal.__isset.value = col_val.__isset.intVal;
      break;
    case TPrimitiveType::SMALLINT:
      hs2_col_val->__isset.i16Val = true;
      hs2_col_val->i16Val.value = col_val.intVal;
      hs2_col_val->i16Val.__isset.value = col_val.__isset.intVal;
      break;
    case TPrimitiveType::INT:
      hs2_col_val->__isset.i32Val = true;
      hs2_col_val->i32Val.value = col_val.intVal;
      hs2_col_val->i32Val.__isset.value = col_val.__isset.intVal;
      break;
    case TPrimitiveType::BIGINT:
      hs2_col_val->__isset.i64Val = true;
      hs2_col_val->i64Val.value = col_val.longVal;
      hs2_col_val->i64Val.__isset.value = col_val.__isset.longVal;
      break;
    case TPrimitiveType::FLOAT:
    case TPrimitiveType::DOUBLE:
      hs2_col_val->__isset.doubleVal = true;
      hs2_col_val->doubleVal.value = col_val.doubleVal;
      hs2_col_val->doubleVal.__isset.value = col_val.__isset.doubleVal;
      break;
    case TPrimitiveType::STRING:
    case TPrimitiveType::TIMESTAMP:
      // HiveServer2 requires timestamp to be presented as string.
      hs2_col_val->__isset.stringVal = true;
      hs2_col_val->stringVal.__isset.value = col_val.__isset.stringVal;
      if (col_val.__isset.stringVal) {
        hs2_col_val->stringVal.value = col_val.stringVal;
      }
      break;
    default:
      DCHECK(false) << "bad type: " << TypeToString(ThriftToType(type));
      break;
  }
}

void ImpalaServer::ExprValueToHiveServer2TColumnValue(const void* value,
    const TPrimitiveType::type& type,
    apache::hive::service::cli::thrift::TColumnValue* hs2_col_val) {
  bool not_null = (value != NULL);
  switch (type) {
    case TPrimitiveType::NULL_TYPE:
      // Set NULLs in the boolVal.
      hs2_col_val->__isset.boolVal = true;
      hs2_col_val->boolVal.__isset.value = false;
      break;
    case TPrimitiveType::BOOLEAN:
      hs2_col_val->__isset.boolVal = true;
      if (not_null) hs2_col_val->boolVal.value = *reinterpret_cast<const bool*>(value);
      hs2_col_val->boolVal.__isset.value = not_null;
      break;
    case TPrimitiveType::TINYINT:
      hs2_col_val->__isset.byteVal = true;
      if (not_null) hs2_col_val->byteVal.value = *reinterpret_cast<const int8_t*>(value);
      hs2_col_val->byteVal.__isset.value = not_null;
      break;
    case TPrimitiveType::SMALLINT:
      hs2_col_val->__isset.i16Val = true;
      if (not_null) hs2_col_val->i16Val.value = *reinterpret_cast<const int16_t*>(value);
      hs2_col_val->i16Val.__isset.value = not_null;
      break;
    case TPrimitiveType::INT:
      hs2_col_val->__isset.i32Val = true;
      if (not_null) hs2_col_val->i32Val.value = *reinterpret_cast<const int32_t*>(value);
      hs2_col_val->i32Val.__isset.value = not_null;
      break;
    case TPrimitiveType::BIGINT:
      hs2_col_val->__isset.i64Val = true;
      if (not_null) hs2_col_val->i64Val.value = *reinterpret_cast<const int64_t*>(value);
      hs2_col_val->i64Val.__isset.value = not_null;
      break;
    case TPrimitiveType::FLOAT:
      hs2_col_val->__isset.doubleVal = true;
      if (not_null) hs2_col_val->doubleVal.value = *reinterpret_cast<const float*>(value);
      hs2_col_val->doubleVal.__isset.value = not_null;
      break;
    case TPrimitiveType::DOUBLE:
      hs2_col_val->__isset.doubleVal = true;
      if (not_null) {
        hs2_col_val->doubleVal.value = *reinterpret_cast<const double*>(value);
      }
      hs2_col_val->doubleVal.__isset.value = not_null;
      break;
    case TPrimitiveType::STRING:
      hs2_col_val->__isset.stringVal = true;
      hs2_col_val->stringVal.__isset.value = not_null;
      if (not_null) {
        const StringValue* string_val = reinterpret_cast<const StringValue*>(value);
        hs2_col_val->stringVal.value.assign(static_cast<char*>(string_val->ptr),
            string_val->len);
      }
      break;
    case TPrimitiveType::TIMESTAMP:
      // HiveServer2 requires timestamp to be presented as string.
      hs2_col_val->__isset.stringVal = true;
      hs2_col_val->stringVal.__isset.value = not_null;
      if (not_null) {
        RawValue::PrintValue(value, TYPE_TIMESTAMP, -1, &(hs2_col_val->stringVal.value));
      }
      break;
    default:
      DCHECK(false) << "bad type: " << TypeToString(ThriftToType(type));
      break;
  }
}

TOperationState::type ImpalaServer::QueryStateToTOperationState(
    const QueryState::type& query_state) {
  switch (query_state) {
    case QueryState::CREATED: return TOperationState::INITIALIZED_STATE;
    case QueryState::RUNNING: return TOperationState::RUNNING_STATE;
    case QueryState::FINISHED: return TOperationState::FINISHED_STATE;
    case QueryState::EXCEPTION: return TOperationState::ERROR_STATE;
    default: return TOperationState::UKNOWN_STATE;
  }
}

}
