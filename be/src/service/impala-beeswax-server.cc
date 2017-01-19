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

#include <boost/algorithm/string/join.hpp>

#include "common/logging.h"
#include "gen-cpp/Frontend_types.h"
#include "rpc/thrift-util.h"
#include "runtime/exec-env.h"
#include "runtime/raw-value.inline.h"
#include "runtime/timestamp-value.h"
#include "service/query-exec-state.h"
#include "service/query-options.h"
#include "service/query-result-set.h"
#include "util/impalad-metrics.h"
#include "util/webserver.h"
#include "util/runtime-profile.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using boost::adopt_lock_t;
using boost::algorithm::join;
using namespace apache::thrift;
using namespace apache::hive::service::cli::thrift;
using namespace beeswax;

#define RAISE_IF_ERROR(stmt, ex_type)                           \
  do {                                                          \
    Status __status__ = (stmt);                                 \
    if (UNLIKELY(!__status__.ok())) {                           \
      RaiseBeeswaxException(__status__.GetDetail(), ex_type);   \
    }                                                           \
  } while (false)

namespace impala {

void ImpalaServer::query(QueryHandle& query_handle, const Query& query) {
  VLOG_QUERY << "query(): query=" << query.query;
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  RAISE_IF_ERROR(
      session_handle.WithSession(ThriftServer::GetThreadConnectionId(), &session),
      SQLSTATE_GENERAL_ERROR);
  TQueryCtx query_ctx;
  // raise general error for request conversion error;
  RAISE_IF_ERROR(QueryToTQueryContext(query, &query_ctx), SQLSTATE_GENERAL_ERROR);

  // raise Syntax error or access violation; it's likely to be syntax/analysis error
  // TODO: that may not be true; fix this
  shared_ptr<QueryExecState> exec_state;
  RAISE_IF_ERROR(Execute(&query_ctx, session, &exec_state),
      SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);

  exec_state->UpdateNonErrorQueryState(beeswax::QueryState::RUNNING);
  // start thread to wait for results to become available, which will allow
  // us to advance query state to FINISHED or EXCEPTION
  exec_state->WaitAsync();
  // Once the query is running do a final check for session closure and add it to the
  // set of in-flight queries.
  Status status = SetQueryInflight(session, exec_state);
  if (!status.ok()) {
    UnregisterQuery(exec_state->query_id(), false, &status);
    RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }
  TUniqueIdToQueryHandle(exec_state->query_id(), &query_handle);
}

void ImpalaServer::executeAndWait(QueryHandle& query_handle, const Query& query,
    const LogContextId& client_ctx) {
  VLOG_QUERY << "executeAndWait(): query=" << query.query;
  ScopedSessionState session_handle(this);
  shared_ptr<SessionState> session;
  RAISE_IF_ERROR(
      session_handle.WithSession(ThriftServer::GetThreadConnectionId(), &session),
      SQLSTATE_GENERAL_ERROR);
  TQueryCtx query_ctx;
  // raise general error for request conversion error;
  RAISE_IF_ERROR(QueryToTQueryContext(query, &query_ctx), SQLSTATE_GENERAL_ERROR);

  shared_ptr<QueryExecState> exec_state;
  DCHECK(session != NULL);  // The session should exist.
  {
    // The session is created when the client connects. Depending on the underlying
    // transport, the username may be known at that time. If the username hasn't been set
    // yet, set it now.
    lock_guard<mutex> l(session->lock);
    if (session->connected_user.empty()) session->connected_user = query.hadoop_user;
  }

  // raise Syntax error or access violation; it's likely to be syntax/analysis error
  // TODO: that may not be true; fix this
  RAISE_IF_ERROR(Execute(&query_ctx, session, &exec_state),
      SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);

  exec_state->UpdateNonErrorQueryState(beeswax::QueryState::RUNNING);
  // Once the query is running do a final check for session closure and add it to the
  // set of in-flight queries.
  Status status = SetQueryInflight(session, exec_state);
  if (!status.ok()) {
    UnregisterQuery(exec_state->query_id(), false, &status);
    RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }
  // block until results are ready
  exec_state->Wait();
  status = exec_state->query_status();
  if (!status.ok()) {
    UnregisterQuery(exec_state->query_id(), false, &status);
    RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }

  exec_state->UpdateNonErrorQueryState(beeswax::QueryState::FINISHED);
  TUniqueIdToQueryHandle(exec_state->query_id(), &query_handle);

  // If the input log context id is an empty string, then create a new number and
  // set it to _return. Otherwise, set _return with the input log context
  query_handle.log_context = client_ctx.empty() ? query_handle.id : client_ctx;
}

void ImpalaServer::explain(QueryExplanation& query_explanation, const Query& query) {
  // Translate Beeswax Query to Impala's QueryRequest and then set the explain plan bool
  // before shipping to FE
  VLOG_QUERY << "explain(): query=" << query.query;
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
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

void ImpalaServer::fetch(Results& query_results, const QueryHandle& query_handle,
    const bool start_over, const int32_t fetch_size) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);

  if (start_over) {
    // We can't start over. Raise "Optional feature not implemented"
    RaiseBeeswaxException(
        "Does not support start over", SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED);
  }

  TUniqueId query_id;
  QueryHandleToTUniqueId(query_handle, &query_id);
  VLOG_ROW << "fetch(): query_id=" << PrintId(query_id) << " fetch_size=" << fetch_size;

  Status status = FetchInternal(query_id, start_over, fetch_size, &query_results);
  VLOG_ROW << "fetch result: #results=" << query_results.data.size()
           << " has_more=" << (query_results.has_more ? "true" : "false");
  if (!status.ok()) {
    UnregisterQuery(query_id, false, &status);
    RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }
}

// TODO: Handle complex types.
void ImpalaServer::get_results_metadata(ResultsMetadata& results_metadata,
    const QueryHandle& handle) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);

  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_QUERY << "get_results_metadata(): query_id=" << PrintId(query_id);
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
  if (UNLIKELY(exec_state.get() == nullptr)) {
    RaiseBeeswaxException(Substitute("Invalid query handle: $0", PrintId(query_id)),
      SQLSTATE_GENERAL_ERROR);
  }

  {
    // make sure we release the lock on exec_state if we see any error
    lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());

    // Convert TResultSetMetadata to Beeswax.ResultsMetadata
    const TResultSetMetadata* result_set_md = exec_state->result_metadata();
    results_metadata.__isset.schema = true;
    results_metadata.schema.__isset.fieldSchemas = true;
    results_metadata.schema.fieldSchemas.resize(result_set_md->columns.size());
    for (int i = 0; i < results_metadata.schema.fieldSchemas.size(); ++i) {
      const TColumnType& type = result_set_md->columns[i].columnType;
      DCHECK_EQ(1, type.types.size());
      DCHECK_EQ(TTypeNodeType::SCALAR, type.types[0].type);
      DCHECK(type.types[0].__isset.scalar_type);
      TPrimitiveType::type col_type = type.types[0].scalar_type.type;
      results_metadata.schema.fieldSchemas[i].__set_type(
          TypeToOdbcString(ThriftToType(col_type)));

      // Fill column name
      results_metadata.schema.fieldSchemas[i].__set_name(
          result_set_md->columns[i].columnName);
    }
  }

  // ODBC-187 - ODBC can only take "\t" as the delimiter and ignores whatever is set here.
  results_metadata.__set_delim("\t");

  // results_metadata.table_dir and in_tablename are not applicable.
}

void ImpalaServer::close(const QueryHandle& handle) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_QUERY << "close(): query_id=" << PrintId(query_id);
  // TODO: do we need to raise an exception if the query state is EXCEPTION?
  // TODO: use timeout to get rid of unwanted exec_state.
  RAISE_IF_ERROR(UnregisterQuery(query_id, true), SQLSTATE_GENERAL_ERROR);
}

beeswax::QueryState::type ImpalaServer::get_state(const QueryHandle& handle) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_ROW << "get_state(): query_id=" << PrintId(query_id);

  lock_guard<mutex> l(query_exec_state_map_lock_);
  QueryExecStateMap::iterator entry = query_exec_state_map_.find(query_id);
  if (entry != query_exec_state_map_.end()) {
    return entry->second->query_state();
  } else {
    VLOG_QUERY << "ImpalaServer::get_state invalid handle";
    RaiseBeeswaxException(Substitute("Invalid query handle: $0", PrintId(query_id)),
      SQLSTATE_GENERAL_ERROR);
  }
  // dummy to keep compiler happy
  return beeswax::QueryState::FINISHED;
}

void ImpalaServer::echo(string& echo_string, const string& input_string) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  echo_string = input_string;
}

void ImpalaServer::clean(const LogContextId& log_context) {
}

void ImpalaServer::get_log(string& log, const LogContextId& context) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  // LogContextId is the same as QueryHandle.id
  QueryHandle handle;
  handle.__set_id(context);
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state.get() == NULL) {
    stringstream str;
    str << "unknown query id: " << query_id;
    LOG(ERROR) << str.str();
    return;
  }
  stringstream error_log_ss;
  // If the query status is !ok, include the status error message at the top of the log.
  if (!exec_state->query_status().ok()) {
    error_log_ss << exec_state->query_status().GetDetail() << "\n";
  }

  // Add warnings from analysis
  error_log_ss << join(exec_state->GetAnalysisWarnings(), "\n");

  // Add warnings from execution
  if (exec_state->coord() != NULL) {
    if (!exec_state->query_status().ok()) error_log_ss << "\n\n";
    error_log_ss << exec_state->coord()->GetErrorLog();
  }
  log = error_log_ss.str();
}

void ImpalaServer::get_default_configuration(vector<ConfigVariable> &configurations,
    const bool include_hadoop) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  configurations.insert(configurations.end(), default_configs_.begin(),
      default_configs_.end());
}

void ImpalaServer::dump_config(string& config) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  config = "";
}

void ImpalaServer::Cancel(impala::TStatus& tstatus,
    const beeswax::QueryHandle& query_handle) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  QueryHandleToTUniqueId(query_handle, &query_id);
  RAISE_IF_ERROR(CancelInternal(query_id, true), SQLSTATE_GENERAL_ERROR);
  tstatus.status_code = TErrorCode::OK;
}

void ImpalaServer::CloseInsert(TInsertResult& insert_result,
    const QueryHandle& query_handle) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  TUniqueId query_id;
  QueryHandleToTUniqueId(query_handle, &query_id);
  VLOG_QUERY << "CloseInsert(): query_id=" << PrintId(query_id);

  Status status = CloseInsertInternal(query_id, &insert_result);
  if (!status.ok()) {
    RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
  }
}

// Gets the runtime profile string for the given query handle and stores the result in
// the profile_output parameter. Raises a BeeswaxException if there are any errors
// getting the profile, such as no matching queries found.
void ImpalaServer::GetRuntimeProfile(string& profile_output, const QueryHandle& handle) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);

  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_RPC << "GetRuntimeProfile(): query_id=" << PrintId(query_id);
  stringstream ss;
  Status status = GetRuntimeProfileStr(query_id, false, &ss);
  if (!status.ok()) {
    ss << "GetRuntimeProfile error: " << status.GetDetail();
    RaiseBeeswaxException(ss.str(), SQLSTATE_GENERAL_ERROR);
  }
  profile_output = ss.str();
}

void ImpalaServer::GetExecSummary(impala::TExecSummary& result,
      const beeswax::QueryHandle& handle) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_RPC << "GetExecSummary(): query_id=" << PrintId(query_id);
  Status status = GetExecSummary(query_id, &result);
  if (!status.ok()) RaiseBeeswaxException(status.GetDetail(), SQLSTATE_GENERAL_ERROR);
}

void ImpalaServer::PingImpalaService(TPingImpalaServiceResp& return_val) {
  ScopedSessionState session_handle(this);
  RAISE_IF_ERROR(session_handle.WithSession(ThriftServer::GetThreadConnectionId()),
      SQLSTATE_GENERAL_ERROR);

  VLOG_RPC << "PingImpalaService()";
  return_val.version = GetVersionString(true);
  return_val.webserver_address = ExecEnv::GetInstance()->webserver()->Url();
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
    RETURN_IF_ERROR(GetSessionState(session_id, &session));
    DCHECK(session != NULL);
    {
      // The session is created when the client connects. Depending on the underlying
      // transport, the username may be known at that time. If the username hasn't been
      // set yet, set it now.
      lock_guard<mutex> l(session->lock);
      if (session->connected_user.empty()) session->connected_user = query.hadoop_user;
      query_ctx->client_request.query_options = session->default_query_options;
      set_query_options_mask = session->set_query_options_mask;
    }
    session->ToThrift(session_id, &query_ctx->session);
  }

  // Override default query options with Query.Configuration
  if (query.__isset.configuration) {
    for (const string& option: query.configuration) {
      RETURN_IF_ERROR(ParseQueryOptions(option, &query_ctx->client_request.query_options,
          &set_query_options_mask));
    }
  }

  // Only query options not set in the session or confOverlay can be overridden by the
  // pool options.
  AddPoolQueryOptions(query_ctx, ~set_query_options_mask);
  VLOG_QUERY << "TClientRequest.queryOptions: "
             << ThriftDebugString(query_ctx->client_request.query_options);
  return Status::OK();
}

inline void ImpalaServer::TUniqueIdToQueryHandle(const TUniqueId& query_id,
    QueryHandle* handle) {
  string query_id_str = PrintId(query_id);
  handle->__set_id(query_id_str);
  handle->__set_log_context(query_id_str);
}

inline void ImpalaServer::QueryHandleToTUniqueId(const QueryHandle& handle,
    TUniqueId* query_id) {
  ParseId(handle.id, query_id);
}

[[noreturn]] void ImpalaServer::RaiseBeeswaxException(
    const string& msg, const char* sql_state) {
  BeeswaxException exc;
  exc.__set_message(msg);
  exc.__set_SQLState(sql_state);
  throw exc;
}

Status ImpalaServer::FetchInternal(const TUniqueId& query_id,
    const bool start_over, const int32_t fetch_size, beeswax::Results* query_results) {
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (UNLIKELY(exec_state == nullptr)) {
    return Status(Substitute("Invalid query handle: $0", PrintId(query_id)));
  }

  // Make sure QueryExecState::Wait() has completed before fetching rows. Wait() ensures
  // that rows are ready to be fetched (e.g., Wait() opens QueryExecState::output_exprs_,
  // which are evaluated in QueryExecState::FetchRows() below).
  exec_state->BlockOnWait();

  lock_guard<mutex> frl(*exec_state->fetch_rows_lock());
  lock_guard<mutex> l(*exec_state->lock());

  if (exec_state->num_rows_fetched() == 0) {
    exec_state->query_events()->MarkEvent("First row fetched");
    exec_state->set_fetched_rows();
  }

  // Check for cancellation or an error.
  RETURN_IF_ERROR(exec_state->query_status());

  // ODBC-190: set Beeswax's Results.columns to work around bug ODBC-190;
  // TODO: remove the block of code when ODBC-190 is resolved.
  const TResultSetMetadata* result_metadata = exec_state->result_metadata();
  query_results->columns.resize(result_metadata->columns.size());
  for (int i = 0; i < result_metadata->columns.size(); ++i) {
    // TODO: As of today, the ODBC driver does not support boolean and timestamp data
    // type but it should. This is tracked by ODBC-189. We should verify that our
    // boolean and timestamp type are correctly recognized when ODBC-189 is closed.
    // TODO: Handle complex types.
    const TColumnType& type = result_metadata->columns[i].columnType;
    DCHECK_EQ(1, type.types.size());
    DCHECK_EQ(TTypeNodeType::SCALAR, type.types[0].type);
    DCHECK(type.types[0].__isset.scalar_type);
    TPrimitiveType::type col_type = type.types[0].scalar_type.type;
    query_results->columns[i] = TypeToOdbcString(ThriftToType(col_type));
  }
  query_results->__isset.columns = true;

  // Results are always ready because we're blocking.
  query_results->__set_ready(true);
  // It's likely that ODBC doesn't care about start_row, but Hue needs it. For Hue,
  // start_row starts from zero, not one.
  query_results->__set_start_row(exec_state->num_rows_fetched());

  Status fetch_rows_status;
  query_results->data.clear();
  if (!exec_state->eos()) {
    scoped_ptr<QueryResultSet> result_set(QueryResultSet::CreateAsciiQueryResultSet(
        *exec_state->result_metadata(), &query_results->data));
    fetch_rows_status = exec_state->FetchRows(fetch_size, result_set.get());
  }
  query_results->__set_has_more(!exec_state->eos());
  query_results->__isset.data = true;

  return fetch_rows_status;
}

Status ImpalaServer::CloseInsertInternal(const TUniqueId& query_id,
    TInsertResult* insert_result) {
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
  if (UNLIKELY(exec_state == nullptr)) {
    return Status(Substitute("Invalid query handle: $0", PrintId(query_id)));
  }

  Status query_status;
  {
    lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());
    query_status = exec_state->query_status();
    if (query_status.ok()) {
      // Coord may be NULL for a SELECT with LIMIT 0.
      // Note that when IMPALA-87 is fixed (INSERT without FROM clause) we might
      // need to revisit this, since that might lead us to insert a row without a
      // coordinator, depending on how we choose to drive the table sink.
      int64_t num_row_errors = 0;
      bool has_kudu_stats = false;
      if (exec_state->coord() != NULL) {
        for (const PartitionStatusMap::value_type& v:
             exec_state->coord()->per_partition_status()) {
          const pair<string, TInsertPartitionStatus> partition_status = v;
          insert_result->rows_modified[partition_status.first] =
              partition_status.second.num_modified_rows;

          if (partition_status.second.__isset.stats &&
              partition_status.second.stats.__isset.kudu_stats) {
            has_kudu_stats = true;
          }
          num_row_errors += partition_status.second.stats.kudu_stats.num_row_errors;
        }
        if (has_kudu_stats) insert_result->__set_num_row_errors(num_row_errors);
      }
    }
  }
  RETURN_IF_ERROR(UnregisterQuery(query_id, true));
  return query_status;
}

}
