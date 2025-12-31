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

#include "service/workload-management-worker.h"

#include <chrono>
#include <condition_variable>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

#include <boost/algorithm/string/join.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gutil/strings/strcat.h>
#include <gutil/strings/substitute.h>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "common/status.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/Query_types.h"
#include "gen-cpp/SystemTables_types.h"
#include "gen-cpp/Types_types.h"
#include "kudu/util/version_util.h"
#include "runtime/exec-env.h"
#include "runtime/query-driver.h"
#include "service/client-request-state.h"
#include "service/impala-server.h"
#include "service/internal-server.h"
#include "service/query-options.h"
#include "service/query-state-record.h"
#include "util/debug-util.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/pretty-printer.h"
#include "util/sql-util.h"
#include "util/stopwatch.h"
#include "util/string-util.h"
#include "workload_mgmt/workload-management.h"

using namespace impala;
using namespace impala::workloadmgmt;
using namespace std;
using kudu::Version;

DECLARE_bool(enable_workload_mgmt);
DECLARE_int32(query_log_write_interval_s);
DECLARE_int32(query_log_dml_exec_timeout_s);
DECLARE_int32(query_log_max_insert_attempts);
DECLARE_int32(query_log_max_queued);
DECLARE_int32(query_log_shutdown_timeout_s);
DECLARE_string(debug_actions);
DECLARE_string(cluster_id);
DECLARE_string(query_log_request_pool);
DECLARE_int32(query_log_write_timeout_s);
DECLARE_string(workload_mgmt_user);
DECLARE_int32(query_log_expression_limit);

namespace impala {
namespace workloadmgmt {

/// Constant declaring how to convert from micro and nano seconds to milliseconds.
static constexpr double MICROS_TO_MILLIS = 1000;
static constexpr double NANOS_TO_MILLIS = 1000000;

/// SQL column type for duration columns that store a millisecond value.
static const string MILLIS_DECIMAL_TYPE = strings::Substitute(
    "DECIMAL($0,$1)", DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE);

/// Helper function to add a decimal value to the stream that is building the completed
/// queries insert DML.
///
/// Parameters:
///   `ctx`     The field parse context object.
///   `val`     A value to write in the completed queries sql stream.
///   `factor`  The provided val input will be divided by this value.
static void _write_decimal(FieldParserContext& ctx, int64_t val, double factor) {
  ctx.sql << "CAST(" << val / factor << " AS " << MILLIS_DECIMAL_TYPE << ")";
}

/// Helper function to add the timestamp for a single event from the events timeline into
/// the stream that is building the completed queries insert DML.
///
/// Parameters:
///   `ctx`  The field parse context object.
///   `target_event` The element from the QueryEvent enum that represents the event being
///                  inserted into the sql statement. The corresponding event value will
///                  be retrieved from the map of query events.
static void _write_event(FieldParserContext& ctx, QueryEvent target_event) {
  const auto& event = ctx.record->events.find(target_event);
  DCHECK(event != ctx.record->events.end());
  _write_decimal(ctx, event->second, NANOS_TO_MILLIS);
}

const std::array<FieldParser, NumQueryTableColumns> FIELD_PARSERS = {{
    // Schema Version 1.0.0 Columns
    // cluster id
    {[](FieldParserContext& ctx) { ctx.sql << "'" << ctx.cluster_id << "'"; }},

    // query id
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << PrintId(ctx.record->base_state->id) << "'";
    }},

    // session_id
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << PrintId(ctx.record->session_id) << "'";
    }},

    // session type
    {[](FieldParserContext& ctx) { ctx.sql << "'" << ctx.record->session_type << "'"; }},

    // hiveserver2 protocol version
    {[](FieldParserContext& ctx) {
      ctx.sql << "'";
      if (ctx.record->session_type == TSessionType::HIVESERVER2) {
        ctx.sql << ctx.record->hiveserver2_protocol_version_formatted();
      }
      ctx.sql << "'";
    }},

    // db user
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << ctx.record->base_state->effective_user << "'";
    }},

    // db user connection
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << ctx.record->db_user_connection << "'";
    }},

    // db name
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << ctx.record->base_state->default_db << "'";
    }},

    // impala coordinator
    {[](FieldParserContext& ctx) {
      ctx.sql << "'"
              << TNetworkAddressToString(
                     ExecEnv::GetInstance()->configured_backend_address())
              << "'";
    }},

    // query status
    {[](FieldParserContext& ctx) {
      ctx.sql << "'";
      if (ctx.record->base_state->query_status.ok()) {
        ctx.sql << "OK";
      } else {
        ctx.sql << EscapeSql(ctx.record->base_state->query_status.msg().msg());
      }
      ctx.sql << "'";
    }},

    // query state
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << ctx.record->base_state->query_state << "'";
    }},

    // impala query end state
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << ctx.record->impala_query_end_state << "'";
    }},

    // query type
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << ctx.record->base_state->stmt_type << "'";
    }},

    // network address
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << TNetworkAddressToString(ctx.record->client_address) << "'";
    }},

    // start time utc
    {[](FieldParserContext& ctx) {
      ctx.sql << "UNIX_MICROS_TO_UTC_TIMESTAMP(" << ctx.record->base_state->start_time_us
              << ")";
    }},

    // total time ms
    {[](FieldParserContext& ctx) {
      _write_decimal(ctx,
          (ctx.record->base_state->end_time_us - ctx.record->base_state->start_time_us),
          MICROS_TO_MILLIS);
    }},

    // query opts config
    {[](FieldParserContext& ctx) {
      const string opts_str = DebugQueryOptions(ctx.record->query_options);
      ctx.sql << "'" << EscapeSql(opts_str) << "'";
    }},

    // resource pool
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << EscapeSql(ctx.record->base_state->resource_pool) << "'";
    }},

    // per host mem estimate
    {[](FieldParserContext& ctx) { ctx.sql << ctx.record->per_host_mem_estimate; }},

    // dedicated coord mem estimate
    {[](FieldParserContext& ctx) {
      ctx.sql << ctx.record->dedicated_coord_mem_estimate;
    }},

    // per host fragment instances
    {[](FieldParserContext& ctx) {
      ctx.sql << "'";

      if (!ctx.record->per_host_state.empty()) {
        for (const auto& iter : ctx.record->per_host_state) {
          ctx.sql << TNetworkAddressToString(iter.first) << "="
                  << iter.second.fragment_instance_count << ',';
        }
        ctx.sql.move_back();
      }

      ctx.sql << "'";
    }},

    // backends count
    {[](FieldParserContext& ctx) {
      if (ctx.record->per_host_state.empty()) {
        ctx.sql << 0;
      } else {
        ctx.sql << ctx.record->per_host_state.size();
      }
    }},

    // admission result
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << ctx.record->admission_result << "'";
    }},

    // cluster memory admitted
    {[](FieldParserContext& ctx) { ctx.sql << ctx.record->base_state->cluster_mem_est; }},

    // executor group
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << ctx.record->executor_group << "'";
    }},

    // executor groups
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << EscapeSql(ctx.record->executor_groups) << "'";
    }},

    // exec summary
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << EscapeSql(ctx.record->exec_summary) << "'";
    }},

    // num rows fetched
    {[](FieldParserContext& ctx) {
      ctx.sql << ctx.record->base_state->num_rows_fetched;
    }},

    // row materialization rows per sec
    {[](FieldParserContext& ctx) { ctx.sql << ctx.record->row_materialization_rate; }},

    // row materialization time ms
    {[](FieldParserContext& ctx) {
      _write_decimal(ctx, ctx.record->row_materialization_time, NANOS_TO_MILLIS);
    }},

    // compressed bytes spilled
    {[](FieldParserContext& ctx) { ctx.sql << ctx.record->compressed_bytes_spilled; }},

    // event planning finished
    {[](FieldParserContext& ctx) { _write_event(ctx, PLANNING_FINISHED); }},

    // event submit for admission
    {[](FieldParserContext& ctx) { _write_event(ctx, SUBMIT_FOR_ADMISSION); }},

    // event completed admission
    {[](FieldParserContext& ctx) { _write_event(ctx, COMPLETED_ADMISSION); }},

    // event all backends started
    {[](FieldParserContext& ctx) { _write_event(ctx, ALL_BACKENDS_STARTED); }},

    // event rows available
    {[](FieldParserContext& ctx) { _write_event(ctx, ROWS_AVAILABLE); }},

    // event first row fetched
    {[](FieldParserContext& ctx) { _write_event(ctx, FIRST_ROW_FETCHED); }},

    // event last row fetched
    {[](FieldParserContext& ctx) { _write_event(ctx, LAST_ROW_FETCHED); }},

    // event unregister query
    {[](FieldParserContext& ctx) { _write_event(ctx, UNREGISTER_QUERY); }},

    // read io wait total ms
    {[](FieldParserContext& ctx) {
      _write_decimal(ctx, ctx.record->read_io_wait_time_total, NANOS_TO_MILLIS);
    }},

    // read io wait mean ms
    {[](FieldParserContext& ctx) {
      _write_decimal(ctx, ctx.record->read_io_wait_time_mean, NANOS_TO_MILLIS);
    }},

    // bytes read cache total
    {[](FieldParserContext& ctx) { ctx.sql << ctx.record->bytes_read_cache_total; }},

    // bytes read total
    {[](FieldParserContext& ctx) { ctx.sql << ctx.record->bytes_read_total; }},

    // pernode peak mem min
    {[](FieldParserContext& ctx) {
      auto min_elem = min_element(ctx.record->per_host_state.cbegin(),
          ctx.record->per_host_state.cend(), PerHostPeakMemoryComparator);

      if (LIKELY(min_elem != ctx.record->per_host_state.cend())) {
        ctx.sql << min_elem->second.peak_memory_usage;
      } else {
        ctx.sql << 0;
      }
    }},

    // pernode peak mem max
    {[](FieldParserContext& ctx) {
      auto max_elem = max_element(ctx.record->per_host_state.cbegin(),
          ctx.record->per_host_state.cend(), PerHostPeakMemoryComparator);

      if (UNLIKELY(max_elem == ctx.record->per_host_state.cend())) {
        ctx.sql << 0;
      } else {
        ctx.sql << max_elem->second.peak_memory_usage;
      }
    }},

    // pernode peak mem mean
    {[](FieldParserContext& ctx) {
      int64_t calc_mean = 0;

      if (LIKELY(!ctx.record->per_host_state.empty())) {
        for (const auto& host : ctx.record->per_host_state) {
          calc_mean += host.second.peak_memory_usage;
        }

        calc_mean = calc_mean / ctx.record->per_host_state.size();
      }

      ctx.sql << calc_mean;
    }},

    // sql
    {[](FieldParserContext& ctx) {
      ctx.sql << "'"
              << EscapeSql(ctx.record->redacted_sql, FLAGS_query_log_max_sql_length)
              << "'";
    }},

    // plan
    {[](FieldParserContext& ctx) {
      ctx.sql << "'"
              << EscapeSql(ctx.record->base_state->plan, FLAGS_query_log_max_plan_length)
              << "'";
    }},

    // tables queried
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << PrintTableList(ctx.record->tables) << "'";
    }},

    // Schema Version 1.1.0 Columns
    // select columns
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << boost::algorithm::join(ctx.record->select_columns, ",") << "'";
    }},

    // where columns
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << boost::algorithm::join(ctx.record->where_columns, ",") << "'";
    }},

    // join columns
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << boost::algorithm::join(ctx.record->join_columns, ",") << "'";
    }},

    // aggregate columns
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << boost::algorithm::join(ctx.record->aggregate_columns, ",") << "'";
    }},

    // orderby columns
    {[](FieldParserContext& ctx) {
      ctx.sql << "'" << boost::algorithm::join(ctx.record->orderby_columns, ",") << "'";
    }},

    //  coordinator slots
    {[](FieldParserContext& ctx) {
      ctx.sql << ctx.record->base_state->coordinator_slots;
    }},

    // executor slots
    {[](FieldParserContext& ctx) {
      ctx.sql << ctx.record->base_state->executor_slots;
    }}}}; // FIELD_PARSERS constant array

} // namespace workloadmgmt

/// Queue of completed queries and the mutex to synchronize access to it.
static list<CompletedQuery> _completed_queries;
static mutex _completed_queries_mu;

/// Coordinate periodic execution of the completed queries queue processing thread.
static condition_variable _completed_queries_cv;

/// Coordinate shutdown of the completed queries queue processing thread.
static condition_variable _completed_queries_shutdown_cv;

/// Determine if the maximum number of queued completed queries has been exceeded.
///
/// Return:
///   `true`  There is a max limit on the number of queued completed queries and that
///           limit has been exceeded.
///   `false` Either there is no max number of queued completed queries or there is a
///           limit that has not been exceeded.
static inline bool _maxRecordsExceeded(size_t record_count) noexcept {
  return FLAGS_query_log_max_queued > 0 && record_count > FLAGS_query_log_max_queued;
} // function _maxRecordsExceeded

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
static const string _queryStateToSql(
    const QueryStateExpanded* rec, const Version& target_schema_version) noexcept {
  DCHECK(rec != nullptr);
  StringStreamPop sql;
  FieldParserContext ctx(rec, FLAGS_cluster_id, sql);

  sql << "\n(";

  for (const auto& field : FIELD_DEFINITIONS) {
    if (field.second.Include(target_schema_version)) {
      FieldParser parser = FIELD_PARSERS[field.first];
      parser(ctx);
      sql << ",";
    }
  }

  sql.move_back();
  sql << ")";

  return sql.str();
} // function _queryStateToSql

size_t ImpalaServer::NumLiveQueries() {
  size_t live_queries = query_driver_map_.Count();
  lock_guard<mutex> l(_completed_queries_mu);
  return live_queries + _completed_queries.size();
}

void ImpalaServer::ShutdownWorkloadManagement() {
  unique_lock<mutex> l(workload_mgmt_state_mu_);

  // Handle the situation where this function runs before the workload management process
  // has been started and thus workload_management_thread_ holds a nullptr.
  if (workload_mgmt_state_ == WorkloadManagementState::NOT_STARTED) {
    workload_mgmt_state_ = WorkloadManagementState::SHUTDOWN;
    return;
  }

  DCHECK_NE(nullptr, workload_management_thread_.get());

  // If the completed queries thread is not yet running, then we don't need to give it a
  // chance to flush the in-memory queue to the completed queries table.
  if (workload_mgmt_state_ == WorkloadManagementState::RUNNING) {
    workload_mgmt_state_ = WorkloadManagementState::SHUTTING_DOWN;
    LOG(INFO) << "Workload management is shutting down";

    // Wake up the completed queries processing thread so it can drain the in-memory
    // completed queries queue.
    _completed_queries_cv.notify_all();

    // Wait for the completed queries processing thread to drain the in-memory queue. If
    // the timeout expires before the queue is drained, the thread will be detached
    // and shutdown will continue.
    _completed_queries_shutdown_cv.wait_for(l,
        chrono::seconds(FLAGS_query_log_shutdown_timeout_s),
        [this] { return workload_mgmt_state_ == WorkloadManagementState::SHUTDOWN; });
  }

  switch (workload_mgmt_state_) {
    case WorkloadManagementState::SHUTDOWN:
      // Safe to join the thread here because the workload managmenent processing loop
      // sets the thread state to ThreadState::SHUTDOWN immediately before it returns.
      LOG(INFO) << "Workload management shutdown successful";
      workload_management_thread_->Join();
      break;
    default:
      // The shutdown timeout expired without the completed queries queue draining.
      LOG(INFO) << "Workload management shutdown timed out. Up to '"
                << ImpaladMetrics::COMPLETED_QUERIES_QUEUED->GetValue()
                << "' queries may have "
                << "been lost";
      workload_management_thread_->Detach();
      break;
  }
} // function ImpalaServer::ShutdownWorkloadManagement

void ImpalaServer::EnqueueCompletedQuery(
    const QueryHandle& query_handle, shared_ptr<QueryStateRecord> qs_rec) {
  // Do not enqueue queries that are not written to the table or if workload management is
  // not enabled.
  if (query_handle->stmt_type() == TStmtType::SET
      || !query_handle.query_driver()->IncludedInQueryLog()
      || !FLAGS_enable_workload_mgmt) {
    return; // Note: early return
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
      case TCatalogOpType::SHOW_METADATA_TABLES:
      case TCatalogOpType::DESCRIBE_TABLE:
      case TCatalogOpType::DESCRIBE_DB:
      case TCatalogOpType::DESCRIBE_HISTORY:
        VLOG_QUERY << "skipping enqueue of completed query '"
                   << PrintId(query_handle->query_id()) << "' with type '"
                   << query_handle->stmt_type() << "' and catalog op type '"
                   << query_handle->catalog_op_type() << "'";
        return; // Note: early return, query will not be added to the queue
      case TCatalogOpType::RESET_METADATA:
      case TCatalogOpType::DDL:
        // No-op, continue execution of this function.
        break;
      default:
        LOG(WARNING) << "unknown ddl type: "
                     << to_string(query_handle->catalog_op_type());
        DCHECK(false);
        return; // Note: early return
    }
  } else if (query_handle->stmt_type() == TStmtType::UNKNOWN) {
    if (query_handle->hs2_metadata_op()) {
      VLOG_QUERY << "skipping enqueue of completed query '"
                 << PrintId(query_handle->query_id()) << "' with type '"
                 << query_handle->stmt_type() << "'";
      return; // Note: early return, query will not be added to the queue
    }
  }

  shared_ptr<QueryStateExpanded> exp_rec =
      make_shared<QueryStateExpanded>(*query_handle, move(qs_rec));

  {
    lock_guard<mutex> l(_completed_queries_mu);
    _completed_queries.emplace_back(CompletedQuery(move(exp_rec)));
    ImpaladMetrics::COMPLETED_QUERIES_QUEUED->Increment(1L);

    if (_maxRecordsExceeded(_completed_queries.size())) {
      _completed_queries_cv.notify_all();
    }
  }

  VLOG_QUERY << "enqueued completed '" << query_handle->stmt_type() << "' query '"
             << PrintId(query_handle->query_id()) << "'";
} // ImpalaServer::EnqueueCompletedQuery

static string _dmlPrefix(const string& table_name, const Version& target_schema_version) {
  StringStreamPop fields;
  fields << "INSERT INTO " << table_name << "(";
  for (const auto& field : FIELD_DEFINITIONS) {
    if (field.second.Include(target_schema_version)) {
      fields << to_string(field.first) << ",";
    }
  }
  fields.move_back();
  fields << ") VALUES ";
  return fields.str();
} // function _dmlPrefix

// Returns true if workload management is shutting down. If it is, this function updates
// the workload management state to SHUTDOWN.
bool ImpalaServer::IsWorkloadManagementShuttingDown() {
  lock_guard<mutex> l(workload_mgmt_state_mu_);

  DCHECK(workload_mgmt_state_ != WorkloadManagementState::SHUTDOWN);

  if (UNLIKELY(workload_mgmt_state_ == WorkloadManagementState::SHUTTING_DOWN)) {
    workload_mgmt_state_ = WorkloadManagementState::SHUTDOWN;
    return true;
  }

  return false;
} // function ImpalaServer::IsWorkloadManagementShuttingDown

void ImpalaServer::WorkloadManagementWorker(const Version& target_schema_version) {
  {
    lock_guard<mutex> l(workload_mgmt_state_mu_);
    workload_mgmt_state_ = WorkloadManagementState::STARTING;
  }

  {
    lock_guard<mutex> l(workload_mgmt_state_mu_);
    workload_mgmt_state_ = WorkloadManagementState::STARTED;
  }

  {
    lock_guard<mutex> l(workload_mgmt_state_mu_);
    // This condition will evaluate to false only if a clean shutdown was initiated while
    // the previous function was running.
    if (LIKELY(workload_mgmt_state_ == WorkloadManagementState::STARTED)) {
      workload_mgmt_state_ = WorkloadManagementState::RUNNING;
    } else {
      LOG(INFO) << "Not starting workload management processing thread because "
                << "coordinator shutdown was initiated.";
      return; // Note: early return
    }
  }

  // Non-values portion of the sql DML to insert records into the completed queries
  // tables. This portion of the statement is constant and thus is only generated once.
  const string log_table_name = QueryLogTableName(true);
  const string insert_dml_prefix = _dmlPrefix(log_table_name, target_schema_version);
  VLOG(2) << "Workload Management insert sql prefix: " << insert_dml_prefix;

  // Setup default query options that will be provided on all queries that insert rows
  // into the completed queries table.
  InternalServer::QueryOptionMap insert_query_opts;

  insert_query_opts[TImpalaQueryOptions::TIMEZONE] = "UTC";
  insert_query_opts[TImpalaQueryOptions::QUERY_TIMEOUT_S] = std::to_string(
      FLAGS_query_log_write_timeout_s < 1 ? FLAGS_query_log_write_interval_s :
                                            FLAGS_query_log_write_timeout_s);
  if (!FLAGS_query_log_request_pool.empty()) {
    insert_query_opts[TImpalaQueryOptions::REQUEST_POOL] = FLAGS_query_log_request_pool;
  }
  insert_query_opts[TImpalaQueryOptions::FETCH_ROWS_TIMEOUT_MS] = "0";
  insert_query_opts[TImpalaQueryOptions::EXEC_TIME_LIMIT_S] =
      std::to_string(FLAGS_query_log_dml_exec_timeout_s);
  if (!FLAGS_debug_actions.empty()) {
    insert_query_opts[TImpalaQueryOptions::DEBUG_ACTION] = FLAGS_debug_actions;
  }
  // Hide analyzed query since it can be prohibitively long.
  insert_query_opts[TImpalaQueryOptions::HIDE_ANALYZED_QUERY] = "true";

  while (true) {
    // Exit this thread if a shutdown was initiated.
    if (IsWorkloadManagementShuttingDown()) {
      _completed_queries_shutdown_cv.notify_all();
      return; // Note: early return
    }

    // Sleep this thread until it is time to process queued completed queries or the
    // _completed_queries_cv condition variable is notified either on shutdown (from the
    // ShutdownWorkloadManagement() function) or when the max number of queued queries is
    // exceeded (from the EnqueueCompletedQuery() function).
    const chrono::seconds write_interval_s(FLAGS_query_log_write_interval_s);
    chrono::time_point<std::chrono::steady_clock> next_wake_time;
    list<CompletedQuery> queries_to_insert;
    MonotonicStopWatch timer;
    {
      next_wake_time = chrono::steady_clock::now() + write_interval_s;
      unique_lock<mutex> l(_completed_queries_mu);
      _completed_queries_cv.wait_until(l, next_wake_time, [&next_wake_time, this] {
        return chrono::steady_clock::now() >= next_wake_time
            || _maxRecordsExceeded(_completed_queries.size())
            || UNLIKELY(workload_mgmt_state_ == WorkloadManagementState::SHUTTING_DOWN);
      });

      DebugActionNoFail(FLAGS_debug_actions, "WM_SHUTDOWN_DELAY");

      if (_completed_queries.empty()) continue;

      if (_maxRecordsExceeded(_completed_queries.size())) {
        ImpaladMetrics::COMPLETED_QUERIES_MAX_RECORDS_WRITES->Increment(1L);
      } else {
        ImpaladMetrics::COMPLETED_QUERIES_SCHEDULED_WRITES->Increment(1L);
      }

      timer.Start();

      // Copy all completed queries to a temporary list so that inserts to the
      // completed_queries list are not blocked while generating and running an insert
      // SQL statement for the completed queries.
      queries_to_insert.splice(queries_to_insert.cend(), _completed_queries);
    }

    string sql;
    uint32_t max_row_size = 0;

    for (auto iter = queries_to_insert.begin(); iter != queries_to_insert.end(); iter++) {
      if (iter->insert_attempts_count >= FLAGS_query_log_max_insert_attempts) {
        LOG(ERROR) << "could not write completed query table=\"" << log_table_name
                   << "\" query_id=\"" << PrintId(iter->query->base_state->id) << "\"";
        iter = queries_to_insert.erase(iter);
        ImpaladMetrics::COMPLETED_QUERIES_QUEUED->Increment(-1);
        continue;
      }

      // Increment the count of attempts to insert this query into the completed
      // queries table.
      iter->insert_attempts_count += 1;

      const string row = _queryStateToSql(iter->query.get(), target_schema_version);
      if (row.size() > max_row_size) {
        max_row_size = row.size();
      }

      StrAppend(&sql, move(row), ",");
      VLOG(2) << "added query '" << iter->query->base_state->id
              << "' to insert sql. Insert attempt '"
              << iter->insert_attempts_count << "'.";
    }

    DCHECK(
        ImpaladMetrics::COMPLETED_QUERIES_QUEUED->GetValue() >= queries_to_insert.size());

    // In the case where queries_to_insert only contains records that have exceeded
    // the max insert attempts, sql will be empty.
    if (UNLIKELY(sql.empty())) continue;

    // Remove the last comma and determine the final sql statement length.
    sql.pop_back();
    const size_t final_sql_len = insert_dml_prefix.size() + sql.size();

    uint64_t gather_time = timer.Reset();
    TUniqueId tmp_query_id;

    // Build query options to ensure the query is not rejected.
    InternalServer::QueryOptionMap opts = insert_query_opts;

    if (UNLIKELY(final_sql_len > numeric_limits<int32_t>::max())) {
      LOG(ERROR) << "Completed queries table insert sql statement of length '"
                 << final_sql_len << "' was longer than the maximum of '"
                 << numeric_limits<int32_t>::max() << "', skipping";
      continue; // NOTE: early loop continuation
    }

    // Set max_statement_length_bytes based on actual query, and at least the minimum.
    opts[TImpalaQueryOptions::MAX_STATEMENT_LENGTH_BYTES] =
        std::to_string(max<size_t>(MIN_MAX_STATEMENT_LENGTH_BYTES, final_sql_len));
    // Set statement_expression_limit based on the startup flag. The flag validation
    // ensures this value is numeric and falls within an acceptable range.
    if (FLAGS_query_log_expression_limit >= 0) {
      opts[TImpalaQueryOptions::STATEMENT_EXPRESSION_LIMIT] =
          std::to_string(FLAGS_query_log_expression_limit);
    }
    opts[TImpalaQueryOptions::MAX_ROW_SIZE] = std::to_string(max_row_size);

    // Execute the insert dml.
    const Status ret_status = ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
        StrCat(insert_dml_prefix, sql), opts, false, &tmp_query_id);

    uint64_t exec_time = timer.ElapsedTime();
    ImpaladMetrics::COMPLETED_QUERIES_WRITE_DURATIONS->Update(gather_time + exec_time);

    stringstream log_msg_props;
    log_msg_props
        << " table=\"" << log_table_name << "\""
        << " record_count=\"" << queries_to_insert.size() << "\""
        << " bytes=\"" << PrettyPrinter::PrintBytes(sql.size()) << "\""
        << " gather_time=\"" << PrettyPrinter::Print(gather_time, TUnit::TIME_NS) << "\""
        << " exec_time=\"" << PrettyPrinter::Print(exec_time, TUnit::TIME_NS) << "\""
        << " query_id=\"" << PrintId(tmp_query_id) << "\"";

    if (ret_status.ok()) {
      LOG(INFO) << "wrote completed queries" << log_msg_props.rdbuf();
      ImpaladMetrics::COMPLETED_QUERIES_QUEUED->Increment(queries_to_insert.size() * -1);
      DCHECK(ImpaladMetrics::COMPLETED_QUERIES_QUEUED->GetValue() >= 0);
      ImpaladMetrics::COMPLETED_QUERIES_WRITTEN->Increment(queries_to_insert.size());
    } else {
      log_msg_props << " msg=\"" << ret_status.msg().msg() << "\"";
      LOG(WARNING) << "failed to write completed queries" << log_msg_props.rdbuf();
      LOG(WARNING) << ret_status.GetDetail();
      ImpaladMetrics::COMPLETED_QUERIES_FAIL->Increment(queries_to_insert.size());
      {
        lock_guard<mutex> l(_completed_queries_mu);
        _completed_queries.splice(_completed_queries.cend(), queries_to_insert);
      }
    }
  }
} // function ImpalaServer::WorkloadManagementWorker

vector<shared_ptr<QueryStateExpanded>> ImpalaServer::GetCompletedQueries() {
  lock_guard<mutex> l(_completed_queries_mu);
  vector<shared_ptr<QueryStateExpanded>> results;
  results.reserve(_completed_queries.size());
  for (const auto& r : _completed_queries) {
    results.emplace_back(r.query);
  }
  return results;
} // function ImpalaServer::GetCompletedQueries

} // namespace impala
