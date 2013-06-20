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

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/version.h"
#include "exec/ddl-executor.h"
#include "exec/exec-node.h"
#include "exec/hdfs-table-sink.h"
#include "exec/scan-node.h"
#include "exprs/expr.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/client-cache.h"
#include "runtime/descriptors.h"
#include "runtime/data-stream-sender.h"
#include "runtime/row-batch.h"
#include "runtime/plan-fragment-executor.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/exec-env.h"
#include "runtime/raw-value.h"
#include "runtime/timestamp-value.h"
#include "service/query-exec-state.h"
#include "statestore/simple-scheduler.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/string-parser.h"
#include "util/thrift-util.h"
#include "util/thrift-server.h"
#include "util/jni-util.h"
#include "util/webserver.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/Frontend_types.h"

using namespace std;
using namespace boost;
using namespace boost::algorithm;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;
using namespace apache::hive::service::cli::thrift;
using namespace beeswax;

namespace impala {

// Ascii result set for Beeswax.
// Beeswax returns rows in ascii, using "\t" as column delimiter.
class ImpalaServer::AsciiQueryResultSet : public ImpalaServer::QueryResultSet {
 public:
  // Rows are added into rowset.
  AsciiQueryResultSet(const TResultSetMetadata& metadata, vector<string>* rowset)
    : metadata_(metadata), result_set_(rowset) {
  }

  virtual ~AsciiQueryResultSet() {}

  // Convert expr values (col_values) to ASCII using "\t" as column delimiter and store
  // it in this result set.
  virtual Status AddOneRow(const vector<void*>& col_values, const vector<int>& scales) {
    int num_col = col_values.size();
    DCHECK_EQ(num_col, metadata_.columnDescs.size());
    stringstream out_stream;
    out_stream.precision(ASCII_PRECISION);
    for (int i = 0; i < num_col; ++i) {
      // ODBC-187 - ODBC can only take "\t" as the delimiter
      out_stream << (i > 0 ? "\t" : "");
      RawValue::PrintValue(col_values[i],
          ThriftToType(metadata_.columnDescs[i].columnType), scales[i], &out_stream);
    }
    result_set_->push_back(out_stream.str());
    return Status::OK;
  }

  // Convert TResultRow to ASCII using "\t" as column delimiter and store it in this
  // result set.
  virtual Status AddOneRow(const TResultRow& row) {
    int num_col = row.colVals.size();
    DCHECK_EQ(num_col, metadata_.columnDescs.size());
    stringstream out_stream;
    out_stream.precision(ASCII_PRECISION);
    for (int i = 0; i < num_col; ++i) {
      // ODBC-187 - ODBC can only take "\t" as the delimiter
      out_stream << (i > 0 ? "\t" : "");
      out_stream << row.colVals[i];
    }
    result_set_->push_back(out_stream.str());
    return Status::OK;
  }

 private:
  // Metadata of the result set
  const TResultSetMetadata& metadata_;

  // Points to the result set to be filled. Not owned here.
  vector<string>* result_set_;
};

void ImpalaServer::query(QueryHandle& query_handle, const Query& query) {
  VLOG_QUERY << "query(): query=" << query.query;
  TClientRequest query_request;
  Status status = QueryToTClientRequest(query, &query_request);
  if (!status.ok()) {
    // raise general error for request conversion error;
    RaiseBeeswaxException(status.GetErrorMsg(), SQLSTATE_GENERAL_ERROR);
  }

  shared_ptr<QueryExecState> exec_state;
  shared_ptr<SessionState> session;
  GetSessionState(ThriftServer::GetThreadSessionKey(), &session);
  DCHECK(session != NULL);  // We made these keys...
  {
    // The session is created when the client connects. Depending on the underlying
    // transport, the username may be known at that time. If the username hasn't been set
    // yet, set it now.
    lock_guard<mutex> l(session->lock);
    if (session->user.empty()) session->user = query.hadoop_user;
    query_request.sessionState.user = session->user;
  }
  status = Execute(query_request, session, query_request.sessionState, &exec_state);

  if (!status.ok()) {
    // raise Syntax error or access violation;
    // it's likely to be syntax/analysis error
    // TODO: that may not be true; fix this
    RaiseBeeswaxException(
        status.GetErrorMsg(), SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);
  }

  exec_state->UpdateQueryState(QueryState::RUNNING);
  TUniqueIdToQueryHandle(exec_state->query_id(), &query_handle);

  // start thread to wait for results to become available, which will allow
  // us to advance query state to FINISHED or EXCEPTION
  thread wait_thread(&ImpalaServer::Wait, this, exec_state);
}

void ImpalaServer::executeAndWait(QueryHandle& query_handle, const Query& query,
    const LogContextId& client_ctx) {
  VLOG_QUERY << "executeAndWait(): query=" << query.query;
  TClientRequest query_request;
  Status status = QueryToTClientRequest(query, &query_request);
  if (!status.ok()) {
    // raise general error for request conversion error;
    RaiseBeeswaxException(status.GetErrorMsg(), SQLSTATE_GENERAL_ERROR);
  }

  shared_ptr<QueryExecState> exec_state;
  shared_ptr<SessionState> session;
  GetSessionState(ThriftServer::GetThreadSessionKey(), &session);
  DCHECK(session != NULL);  // We made these keys...
  {
    // The session is created when the client connects. Depending on the underlying
    // transport, the username may be known at that time. If the username hasn't been set
    // yet, set it now.
    lock_guard<mutex> l(session->lock);
    if (session->user.empty()) session->user = query.hadoop_user;
  }
  status = Execute(query_request, session, query_request.sessionState, &exec_state);

  if (!status.ok()) {
    // raise Syntax error or access violation;
    // it's likely to be syntax/analysis error
    // TODO: that may not be true; fix this
    RaiseBeeswaxException(
        status.GetErrorMsg(), SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);
  }

  exec_state->UpdateQueryState(QueryState::RUNNING);
  // block until results are ready
  status = exec_state->Wait();
  if (!status.ok()) {
    UnregisterQuery(exec_state->query_id());
    RaiseBeeswaxException(status.GetErrorMsg(), SQLSTATE_GENERAL_ERROR);
  }

  exec_state->UpdateQueryState(QueryState::FINISHED);
  TUniqueIdToQueryHandle(exec_state->query_id(), &query_handle);

  // If the input log context id is an empty string, then create a new number and
  // set it to _return. Otherwise, set _return with the input log context
  query_handle.log_context = client_ctx.empty() ? query_handle.id : client_ctx;
}

void ImpalaServer::explain(QueryExplanation& query_explanation, const Query& query) {
  // Translate Beeswax Query to Impala's QueryRequest and then set the explain plan bool
  // before shipping to FE
  VLOG_QUERY << "explain(): query=" << query.query;
  TClientRequest query_request;
  Status status = QueryToTClientRequest(query, &query_request);
  if (!status.ok()) {
    // raise general error for request conversion error;
    RaiseBeeswaxException(status.GetErrorMsg(), SQLSTATE_GENERAL_ERROR);
  }

  status = GetExplainPlan(query_request, &query_explanation.textual);
  if (!status.ok()) {
    // raise Syntax error or access violation; this is the closest.
    RaiseBeeswaxException(
        status.GetErrorMsg(), SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);
  }
  query_explanation.__isset.textual = true;
  VLOG_QUERY << "explain():\nstmt=" << query_request.stmt
             << "\nplan: " << query_explanation.textual;
}

void ImpalaServer::fetch(Results& query_results, const QueryHandle& query_handle,
    const bool start_over, const int32_t fetch_size) {
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
    UnregisterQuery(query_id);
    RaiseBeeswaxException(status.GetErrorMsg(), SQLSTATE_GENERAL_ERROR);
  }
}

void ImpalaServer::get_results_metadata(ResultsMetadata& results_metadata,
    const QueryHandle& handle) {
  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_QUERY << "get_results_metadata(): query_id=" << PrintId(query_id);
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
  if (exec_state.get() == NULL) {
    RaiseBeeswaxException("Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }

  {
    // make sure we release the lock on exec_state if we see any error
    lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());

    // Convert TResultSetMetadata to Beeswax.ResultsMetadata
    const TResultSetMetadata* result_set_md = exec_state->result_metadata();
    results_metadata.__isset.schema = true;
    results_metadata.schema.__isset.fieldSchemas = true;
    results_metadata.schema.fieldSchemas.resize(result_set_md->columnDescs.size());
    for (int i = 0; i < results_metadata.schema.fieldSchemas.size(); ++i) {
      TPrimitiveType::type col_type = result_set_md->columnDescs[i].columnType;
      results_metadata.schema.fieldSchemas[i].__set_type(
          TypeToOdbcString(ThriftToType(col_type)));

      // Fill column name
      results_metadata.schema.fieldSchemas[i].__set_name(
          result_set_md->columnDescs[i].columnName);
    }
  }

  // ODBC-187 - ODBC can only take "\t" as the delimiter and ignores whatever is set here.
  results_metadata.__set_delim("\t");

  // results_metadata.table_dir and in_tablename are not applicable.
}

void ImpalaServer::close(const QueryHandle& handle) {
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_QUERY << "close(): query_id=" << PrintId(query_id);

  // TODO: do we need to raise an exception if the query state is
  // EXCEPTION?
  // TODO: use timeout to get rid of unwanted exec_state.
  if (!UnregisterQuery(query_id)) {
    RaiseBeeswaxException("Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }
}

QueryState::type ImpalaServer::get_state(const QueryHandle& handle) {
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_ROW << "get_state(): query_id=" << PrintId(query_id);

  lock_guard<mutex> l(query_exec_state_map_lock_);
  QueryExecStateMap::iterator entry = query_exec_state_map_.find(query_id);
  if (entry != query_exec_state_map_.end()) {
    return entry->second->query_state();
  } else {
    VLOG_QUERY << "ImpalaServer::get_state invalid handle";
    RaiseBeeswaxException("Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }
  // dummy to keep compiler happy
  return QueryState::FINISHED;
}

void ImpalaServer::echo(string& echo_string, const string& input_string) {
  echo_string = input_string;
}

void ImpalaServer::clean(const LogContextId& log_context) {
}

void ImpalaServer::get_log(string& log, const LogContextId& context) {
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
  if (exec_state->coord() != NULL) {
    log = exec_state->coord()->GetErrorLog();
  }
}

void ImpalaServer::get_default_configuration(vector<ConfigVariable> &configurations,
    const bool include_hadoop) {
  configurations.insert(configurations.end(), default_configs_.begin(),
      default_configs_.end());
}

void ImpalaServer::dump_config(string& config) {
  config = "";
}

void ImpalaServer::Cancel(impala::TStatus& tstatus,
    const beeswax::QueryHandle& query_handle) {
  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  QueryHandleToTUniqueId(query_handle, &query_id);
  Status status = CancelInternal(query_id);
  if (status.ok()) {
    tstatus.status_code = TStatusCode::OK;
  } else {
    RaiseBeeswaxException(status.GetErrorMsg(), SQLSTATE_GENERAL_ERROR);
  }
}

void ImpalaServer::CloseInsert(TInsertResult& insert_result,
    const QueryHandle& query_handle) {
  TUniqueId query_id;
  QueryHandleToTUniqueId(query_handle, &query_id);
  VLOG_QUERY << "CloseInsert(): query_id=" << PrintId(query_id);

  Status status = CloseInsertInternal(query_id, &insert_result);
  if (!status.ok()) {
    RaiseBeeswaxException(status.GetErrorMsg(), SQLSTATE_GENERAL_ERROR);
  }
}

// Gets the runtime profile string for the given query handle and stores the result in
// the profile_output parameter. Raises a BeeswaxException if there are any errors
// getting the profile, such as no matching queries found.
void ImpalaServer::GetRuntimeProfile(string& profile_output, const QueryHandle& handle) {
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_RPC << "GetRuntimeProfile(): query_id=" << PrintId(query_id);
  stringstream ss;
  Status status = GetRuntimeProfileStr(query_id, false, &ss);
  if (!status.ok()) {
    ss << "GetRuntimeProfile error: " << status.GetErrorMsg();
    RaiseBeeswaxException(ss.str(), SQLSTATE_GENERAL_ERROR);
  }
  profile_output = ss.str();
}

void ImpalaServer::PingImpalaService(TPingImpalaServiceResp& return_val) {
  VLOG_RPC << "PingImpalaService()";
  return_val.version = GetVersionString(true);
  VLOG_RPC << "PingImpalaService(): return_val=" << ThriftDebugString(return_val);
}

void ImpalaServer::ResetCatalog(impala::TStatus& status) {
  ResetCatalogInternal().ToThrift(&status);
}

void ImpalaServer::ResetTable(impala::TStatus& status, const TResetTableReq& request) {
  ResetTableInternal(request.db_name, request.table_name).ToThrift(&status);
}

void ImpalaServer::SessionStart(const ThriftServer::SessionContext& session_context) {
  const ThriftServer::SessionKey& session_key = session_context.session_key;
  shared_ptr<SessionState> state;
  state.reset(new SessionState);
  state->closed = false;
  state->start_time = TimestampValue::local_time();
  state->database = "default";
  state->session_type = BEESWAX;
  // If the username was set by a lower-level transport, use it.
  if (!session_context.username.empty()) {
    state->user = session_context.username;
  }

  lock_guard<mutex> l(session_state_map_lock_);
  bool success = session_state_map_.insert(make_pair(session_key, state)).second;
  // The session should not have already existed.
  DCHECK(success);
}

void ImpalaServer::SessionEnd(const ThriftServer::SessionContext& session_context) {
  CloseSessionInternal(session_context.session_key);
}

Status ImpalaServer::QueryToTClientRequest(const Query& query,
    TClientRequest* request) {
  request->queryOptions = default_query_options_;
  request->stmt = query.query;
  VLOG_QUERY << "query: " << ThriftDebugString(query);
  {
    shared_ptr<SessionState> session;
    RETURN_IF_ERROR(GetSessionState(ThriftServer::GetThreadSessionKey(), &session));
    session->ToThrift(&request->sessionState);
  }

  // Override default query options with Query.Configuration
  if (query.__isset.configuration) {
    BOOST_FOREACH(const string& option, query.configuration) {
      RETURN_IF_ERROR(ParseQueryOptions(option, &request->queryOptions));
    }
    VLOG_QUERY << "TClientRequest.queryOptions: "
               << ThriftDebugString(request->queryOptions);
  }
  return Status::OK;
}

inline void ImpalaServer::TUniqueIdToQueryHandle(const TUniqueId& query_id,
    QueryHandle* handle) {
  stringstream stringstream;
  stringstream << query_id.hi << " " << query_id.lo;
  handle->__set_id(stringstream.str());
  handle->__set_log_context(stringstream.str());
}

inline void ImpalaServer::QueryHandleToTUniqueId(const QueryHandle& handle,
    TUniqueId* query_id) {
  char_separator<char> sep(" ");
  tokenizer< char_separator<char> > tokens(handle.id, sep);
  int i = 0;
  BOOST_FOREACH(const string& t, tokens) {
    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
    int64_t id = StringParser::StringToInt<int64_t>(
        t.c_str(), t.length(), &parse_result);
    if (i == 0) {
      query_id->hi = id;
    } else {
      query_id->lo = id;
    }
    ++i;
  }
}

void ImpalaServer::RaiseBeeswaxException(const string& msg, const char* sql_state) {
  BeeswaxException exc;
  exc.__set_message(msg);
  exc.__set_SQLState(sql_state);
  throw exc;
}

Status ImpalaServer::FetchInternal(const TUniqueId& query_id,
    const bool start_over, const int32_t fetch_size, beeswax::Results* query_results) {
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
  if (exec_state == NULL) return Status("Invalid query handle");

  // make sure we release the lock on exec_state if we see any error
  lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());

  if (exec_state->num_rows_fetched() == 0) {
    exec_state->query_events()->MarkEvent("First row fetched");
  }

  if (exec_state->query_state() == QueryState::EXCEPTION) {
    // we got cancelled or saw an error; either way, return now
    return exec_state->query_status();
  }

  // ODBC-190: set Beeswax's Results.columns to work around bug ODBC-190;
  // TODO: remove the block of code when ODBC-190 is resolved.
  const TResultSetMetadata* result_metadata = exec_state->result_metadata();
  query_results->columns.resize(result_metadata->columnDescs.size());
  for (int i = 0; i < result_metadata->columnDescs.size(); ++i) {
    // TODO: As of today, the ODBC driver does not support boolean and timestamp data
    // type but it should. This is tracked by ODBC-189. We should verify that our
    // boolean and timestamp type are correctly recognized when ODBC-189 is closed.
    TPrimitiveType::type col_type = result_metadata->columnDescs[i].columnType;
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
    AsciiQueryResultSet result_set(*(exec_state->result_metadata()),
        &(query_results->data));
    fetch_rows_status = exec_state->FetchRows(fetch_size, &result_set);
  }
  query_results->__set_has_more(!exec_state->eos());
  query_results->__isset.data = true;

  return fetch_rows_status;
}

Status ImpalaServer::CloseInsertInternal(const TUniqueId& query_id,
    TInsertResult* insert_result) {
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
  if (exec_state == NULL) return Status("Invalid query handle");
  Status query_status;
  {
    lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());
    query_status = exec_state->query_status();
    if (query_status.ok()) {
      // Coord may be NULL for a SELECT with LIMIT 0.
      // Note that when IMPALA-87 is fixed (INSERT without FROM clause) we might
      // need to revisit this, since that might lead us to insert a row without a
      // coordinator, depending on how we choose to drive the table sink.
      if (exec_state->coord() != NULL) {
        insert_result->__set_rows_appended(exec_state->coord()->partition_row_counts());
      }
    }
  }

  if (!UnregisterQuery(query_id)) {
    stringstream ss;
    ss << "Failed to unregister query ID '" << ThriftDebugString(query_id) << "'";
    return Status(ss.str());
  }
  return query_status;
}

}
