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

#include "service/workload-management.h"

#include <chrono>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gutil/strings/strcat.h>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "common/status.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Query_types.h"
#include "gen-cpp/Types_types.h"
#include "runtime/query-driver.h"
#include "service/client-request-state.h"
#include "service/impala-server.h"
#include "service/internal-server.h"
#include "service/query-state-record.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/string-util.h"
#include "util/thread.h"
#include "util/ticker.h"

using namespace impala;
using namespace impala::workload_management;
using namespace std;

DECLARE_bool(enable_workload_mgmt);
DECLARE_string(query_log_table_name);
DECLARE_string(query_log_table_location);
DECLARE_int32(query_log_write_interval_s);
DECLARE_int32(query_log_write_timeout_s);
DECLARE_int32(query_log_max_queued);
DECLARE_string(workload_mgmt_user);
DECLARE_int32(query_log_max_sql_length);
DECLARE_int32(query_log_max_plan_length);
DECLARE_int32(query_log_shutdown_timeout_s);
DECLARE_string(cluster_id);
DECLARE_int32(query_log_max_insert_attempts);
DECLARE_string(query_log_request_pool);
DECLARE_string(query_log_table_props);

namespace impala {

/// Name of the database where all workload management tables will be stored.
static const string DB = "sys";

/// Default query options that will be provided on all queries that insert rows into the
/// completed queries table. See the initialization code in the
/// ImpalaServer::CompletedQueriesThread function for details on which options are set.
static TQueryOptions insert_query_opts;

/// Non-values portion of the sql DML to insert records into the completed queries table.
/// Generates the first portion of the DML that inserts records into the completed queries
/// table. This portion of the statement is constant and thus is only generated once.
static string _insert_dml;

/// Determine if the maximum number of queued completed queries has been exceeded.
///
/// Return:
///   `true`  There is a max limit on the number of queued completed queries and that
///           limit has been exceeded.
///   `false` Either there is no max number of queued completed queries or there is a
///           limit that has not been exceeded.
static inline bool MaxRecordsExceeded(size_t record_count) noexcept {
  return FLAGS_query_log_max_queued > 0 && record_count > FLAGS_query_log_max_queued;
} // function MaxRecordsExceeded

/// Sets up the completed queries database and table by generating and executing the
/// necessary DML statements.
static const Status SetupDbTable(InternalServer* server, const string& table_name) {
  insert_query_opts.__set_sync_ddl(true);

  RETURN_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      StrCat("create database if not exists ", DB, " comment "
      "'System database for Impala introspection'"), insert_query_opts, false));

  StringStreamPop create_table_sql;
  create_table_sql << "CREATE TABLE IF NOT EXISTS " << table_name << "(";

  for (const auto& field : FIELD_DEFINITIONS) {
    create_table_sql << field.db_column_name << " " << field.db_column_type << ",";
  }
  create_table_sql.move_back();

  create_table_sql << ") PARTITIONED BY SPEC(identity(cluster_id), HOUR(start_time_utc)) "
      << "STORED AS iceberg ";

  if (!FLAGS_query_log_table_location.empty()) {
    create_table_sql << "LOCATION '" << FLAGS_query_log_table_location << "' ";
  }

  create_table_sql << "TBLPROPERTIES ('schema_version'='1.0.0','format-version'='2'";

  if (!FLAGS_query_log_table_props.empty()) {
    create_table_sql << "," << FLAGS_query_log_table_props;
  }

  create_table_sql << ")";

  RETURN_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      create_table_sql.str(), insert_query_opts, false));

  insert_query_opts.__set_sync_ddl(false);

  LOG(INFO) << "Completed query log initialization. storage_type=\""
      << FLAGS_enable_workload_mgmt  << "\" write_interval=\"" <<
      FLAGS_query_log_write_interval_s << "s\"";

  return Status::OK();
} // function SetupDbTable

/// Iterates through the list of field in `FIELDS_PARSERS` executing each parser for the
/// given `QueryStateExpanded` object. This function builds the `FieldParserContext`
/// object that is passed to each parser.
///
/// Parameters:
///   `rec` - `QueryStateExpanded` object, an insert sql statement will be generated to
///           insert a row into the completed queries table representing the query in
///           this object.
///
/// Return:
///   `string` - Contains the insert sql statement.
static const string QueryStateToSql(const QueryStateExpanded* rec) noexcept {
  DCHECK(rec != nullptr);
  StringStreamPop sql;
  FieldParserContext ctx(rec, FLAGS_cluster_id, sql);

  sql << "(";

  for (const auto& field : FIELD_DEFINITIONS) {
    field.parser(ctx);
    sql << ",";
  }

  sql.move_back();
  sql << ")";

  return sql.str();
} // function QueryStateToSql

Status ImpalaServer::InitWorkloadManagement() {
  if (FLAGS_enable_workload_mgmt) {
    return Thread::Create("impala-server", "completed-queries",
      bind<void>(&ImpalaServer::CompletedQueriesThread, this),
      &completed_queries_thread_);
  }

  return Status::OK();
} // ImpalaServer::InitWorkloadManagement

void ImpalaServer::ShutdownWorkloadManagement() {
  unique_lock<mutex> l(completed_queries_threadstate_mu_);
  // If the completed queries thread is not yet running, then we don't need to give it a
  // chance to flush the in-memory queue to the completed queries table.
  if (completed_queries_thread_state_ == RUNNING) {
    completed_queries_thread_state_ = SHUTTING_DOWN;
    completed_queries_cv_.notify_all();
    completed_queries_shutdown_cv_.wait_for(l,
        chrono::seconds(FLAGS_query_log_shutdown_timeout_s),
        [this]{ return completed_queries_thread_state_ == SHUTDOWN; });
  }
} // ImpalaServer::ShutdownWorkloadManagement

void ImpalaServer::EnqueueCompletedQuery(const QueryHandle& query_handle,
    const shared_ptr<QueryStateRecord> qs_rec) {

  // Do not enqueue queries that are not written to the table or if workload management is
  // not enabled.
  if (query_handle->stmt_type() == TStmtType::SET
      || !query_handle.query_driver()->IncludedInQueryLog()
      || !FLAGS_enable_workload_mgmt){
    return;  // Note: early return
  }

  // Do not enqueue use and show ddl queries. This check is separate because combining it
  // with the previous check resulted in very confusing code that had duplication.
  if (query_handle->stmt_type() == TStmtType::DDL) {
    switch (query_handle->catalog_op_type()) {
      case TCatalogOpType::SHOW_TABLES:
      case TCatalogOpType::SHOW_DBS:
      case TCatalogOpType::SHOW_STATS:
      case TCatalogOpType::USE:
      case TCatalogOpType::SHOW_FUNCTIONS:
      case TCatalogOpType::SHOW_CREATE_TABLE:
      case TCatalogOpType::SHOW_DATA_SRCS:
      case TCatalogOpType::SHOW_ROLES:
      case TCatalogOpType::SHOW_GRANT_PRINCIPAL:
      case TCatalogOpType::SHOW_FILES:
      case TCatalogOpType::SHOW_CREATE_FUNCTION:
      case TCatalogOpType::SHOW_VIEWS:
      case TCatalogOpType::DESCRIBE_TABLE:
      case TCatalogOpType::DESCRIBE_DB:
      case TCatalogOpType::DESCRIBE_HISTORY:
        return;  // Note: early return
      case TCatalogOpType::RESET_METADATA:
      case TCatalogOpType::DDL:
        break;
      default:
        LOG(FATAL) << "unknown ddl type: " << to_string(query_handle->catalog_op_type());
    }
  }

  shared_ptr<QueryStateExpanded> exp_rec = make_shared<QueryStateExpanded>(*query_handle,
      move(qs_rec));

  {
    lock_guard<mutex> l(completed_queries_lock_);
    completed_queries_.emplace_back(CompletedQuery(move(exp_rec)));
    ImpaladMetrics::COMPLETED_QUERIES_QUEUED->Increment(1L);

    if (MaxRecordsExceeded(completed_queries_.size())) {
      completed_queries_cv_.notify_all();
    }
  }
} // ImpalaServer::EnqueueCompletedQuery

void ImpalaServer::CompletedQueriesThread() {
  {
    lock_guard<mutex> l(completed_queries_threadstate_mu_);
    completed_queries_thread_state_ = INITIALIZING;
  }

  // Setup default query options.
  insert_query_opts.__set_timezone("UTC");
  insert_query_opts.__set_query_timeout_s((FLAGS_query_log_write_timeout_s < 1 ?
      FLAGS_query_log_write_interval_s : FLAGS_query_log_write_timeout_s));
  if (!FLAGS_query_log_request_pool.empty()) {
    insert_query_opts.__set_request_pool(FLAGS_query_log_request_pool);
  }

  // Fully qualified table name based on startup flags.
  const string table_name = StrCat(DB, ".", FLAGS_query_log_table_name);

  // Non-values portion of the completed queries insert dml. Does not change across
  // queries.
  StringStreamPop fields;
  fields << "INSERT INTO " << table_name << "(";
  for (const auto& field : FIELD_DEFINITIONS) {
    fields << field.db_column_name << ",";
  }
  fields.move_back();
  fields << ") VALUES ";
  _insert_dml = fields.str();

  // The initialization code only works when run in a separate thread for reasons unknown.
  ABORT_IF_ERROR(SetupDbTable(internal_server_.get(), table_name));

  {
    lock_guard<mutex> l(completed_queries_threadstate_mu_);
    // This condition will evaluate to false only if a clean shutdown was initiated while
    // the previous function was running.
    if (LIKELY(completed_queries_thread_state_ == INITIALIZING)) {
      completed_queries_thread_state_ = RUNNING;
    } else {
      return; // Note: early return
    }

    completed_queries_ticker_ = make_unique<TickerSecondsBool>(
        FLAGS_query_log_write_interval_s, completed_queries_cv_, completed_queries_lock_);
    ABORT_IF_ERROR(completed_queries_ticker_->Start("impala-server",
        "completed-queries-ticker"));
  }

  while (true) {
    // Exit this thread if a shutdown was initiated.
    {
      lock_guard<mutex> l(completed_queries_threadstate_mu_);

      DCHECK(completed_queries_thread_state_ != SHUTDOWN);

      if (UNLIKELY(completed_queries_thread_state_ == SHUTTING_DOWN)) {
        completed_queries_thread_state_ = SHUTDOWN;
        completed_queries_shutdown_cv_.notify_all();
        return; // Note: early return
      }
    }

    // Sleep this thread until it is time to process queued completed queries. During the
    // wait, the completed_queries_lock_ is only locked while calling the lambda function
    // predicate. After waking up, the completed_queries_lock_ will be locked.
    unique_lock<mutex> l(completed_queries_lock_);
    completed_queries_cv_.wait(l,
        [this]{
          lock_guard<mutex> l2(completed_queries_threadstate_mu_);
          // To guard against spurious wakeups, this predicate ensures there are completed
          // queries queued up before waking up the thread.
          return (completed_queries_ticker_->WakeupGuard()()
              && !completed_queries_.empty())
              || MaxRecordsExceeded(completed_queries_.size())
              || UNLIKELY(completed_queries_thread_state_ == SHUTTING_DOWN);
        });
    completed_queries_ticker_->ResetWakeupGuard();

    // transfer all currently queued completed queries to another list for processing
    // so that the completed queries queue is not blocked while creating and executing the
    // DML to insert into the query log table
    if (!completed_queries_.empty()) {
      if (MaxRecordsExceeded(completed_queries_.size())) {
        ImpaladMetrics::COMPLETED_QUERIES_MAX_RECORDS_WRITES->Increment(1L);
      } else {
        ImpaladMetrics::COMPLETED_QUERIES_SCHEDULED_WRITES->Increment(1L);
      }

      // Copy all completed queries to a temporary list so that inserts to the
      // completed_queries list are not blocked while generating and running an insert
      // SQL statement for the completed queries.
      list<CompletedQuery> queries_to_insert;
      queries_to_insert.splice(queries_to_insert.cend(), completed_queries_);
      completed_queries_lock_.unlock();

      string sql;
      uint32_t max_row_size = 0;

      for (auto iter = queries_to_insert.begin(); iter != queries_to_insert.end();
          iter++) {
        if (iter->insert_attempts_count >= FLAGS_query_log_max_insert_attempts) {
          LOG(ERROR) << "could not write completed query table=\"" << table_name <<
              "\" query_id=\"" << PrintId(iter->query->base_state->id) << "\"";
          iter = queries_to_insert.erase(iter);
          ImpaladMetrics::COMPLETED_QUERIES_QUEUED->Increment(-1);
          continue;
        }

        // Increment the count of attempts to insert this query into the completed
        // queries table.
        iter->insert_attempts_count += 1;

        const string row = QueryStateToSql(iter->query.get());
        if (row.size() > max_row_size) {
          max_row_size = row.size();
        }

        StrAppend(&sql, move(row), ",");
      }

      DCHECK(ImpaladMetrics::COMPLETED_QUERIES_QUEUED->GetValue() >=
          queries_to_insert.size());

      // In the case where queries_to_insert only contains records that have exceeded
      // the max insert attempts, sql will be empty.
      if (LIKELY(!sql.empty())) {
        // Remove the last comma and determine the final sql statement length.
        sql.pop_back();
        const size_t final_sql_len = _insert_dml.size() + sql.size();

        TUniqueId tmp_query_id;

        // Build query options to ensure the query is not rejected.
        TQueryOptions opts = insert_query_opts;

        if (UNLIKELY(final_sql_len > numeric_limits<int32_t>::max())) {
          LOG(ERROR) << "Completed queries table insert sql statement of length '" <<
              final_sql_len << "' was longer than the maximum of '" <<
              numeric_limits<int32_t>::max() << "', skipping";
          continue; // NOTE: early loop continuation
        }

        opts.__set_max_statement_length_bytes(final_sql_len < 1024 ? 1024 :
            final_sql_len);
        opts.__set_max_row_size(max_row_size);

        // Execute the insert dml.
        const Status ret_status = internal_server_->ExecuteIgnoreResults(
            FLAGS_workload_mgmt_user, StrCat(_insert_dml, sql), opts, false,
            &tmp_query_id);

        if (ret_status.ok()) {
          LOG(INFO) << "wrote completed queries table=\"" << table_name << "\" "
              "record_count=\"" << queries_to_insert.size() << "\"";
          ImpaladMetrics::COMPLETED_QUERIES_QUEUED->Increment(
              queries_to_insert.size() * -1);
          DCHECK(ImpaladMetrics::COMPLETED_QUERIES_QUEUED->GetValue() >= 0);
          ImpaladMetrics::COMPLETED_QUERIES_WRITTEN->Increment(
              queries_to_insert.size());
        } else {
          LOG(WARNING) << "failed to write completed queries table=\"" << table_name <<
              "\" record_count=\"" << queries_to_insert.size() << "\"";
          LOG(WARNING) << ret_status.GetDetail();
          ImpaladMetrics::COMPLETED_QUERIES_FAIL->Increment(queries_to_insert.size());
          completed_queries_lock_.lock();
          completed_queries_.splice(
              completed_queries_.cend(), queries_to_insert);
          completed_queries_lock_.unlock();
        }
      }
    }
  }
} // ImpalaServer::CompletedQueriesThread

} // namespace impala
