// Copyright 2013 Cloudera Inc.
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

#include "service/query-exec-state.h"

#include <limits>
#include <gutil/strings/substitute.h>

#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "service/impala-server.h"
#include "service/frontend.h"
#include "service/query-options.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/time.h"

#include "gen-cpp/CatalogService.h"
#include "gen-cpp/CatalogService_types.h"

#include <thrift/Thrift.h>

#include "common/names.h"

using boost::algorithm::join;
using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift;
using namespace beeswax;
using namespace strings;

DECLARE_int32(catalog_service_port);
DECLARE_string(catalog_service_host);
DECLARE_bool(enable_rm);
DECLARE_int64(max_result_cache_size);

namespace impala {

// Keys into the info string map of the runtime profile referring to specific
// items used by CM for monitoring purposes.
static const string PER_HOST_MEM_KEY = "Estimated Per-Host Mem";
static const string PER_HOST_VCORES_KEY = "Estimated Per-Host VCores";
static const string TABLES_MISSING_STATS_KEY = "Tables Missing Stats";
static const string TABLES_WITH_CORRUPT_STATS_KEY = "Tables With Corrupt Table Stats";

ImpalaServer::QueryExecState::QueryExecState(
    const TQueryCtx& query_ctx, ExecEnv* exec_env, Frontend* frontend,
    ImpalaServer* server, shared_ptr<SessionState> session)
  : query_ctx_(query_ctx),
    last_active_time_(numeric_limits<int64_t>::max()),
    ref_count_(0L),
    exec_env_(exec_env),
    session_(session),
    schedule_(NULL),
    coord_(NULL),
    result_cache_max_size_(-1),
    profile_(&profile_pool_, "Query"),  // assign name w/ id after planning
    server_profile_(&profile_pool_, "ImpalaServer"),
    summary_profile_(&profile_pool_, "Summary"),
    eos_(false),
    query_state_(beeswax::QueryState::CREATED),
    current_batch_(NULL),
    current_batch_row_(0),
    num_rows_fetched_(0),
    fetched_rows_(false),
    frontend_(frontend),
    parent_server_(server),
    start_time_(TimestampValue::LocalTime()) {
  row_materialization_timer_ = ADD_TIMER(&server_profile_, "RowMaterializationTimer");
  client_wait_timer_ = ADD_TIMER(&server_profile_, "ClientFetchWaitTimer");
  query_events_ = summary_profile_.AddEventSequence("Query Timeline");
  query_events_->Start();
  profile_.AddChild(&summary_profile_);

  profile_.set_name("Query (id=" + PrintId(query_id()) + ")");
  summary_profile_.AddInfoString("Session ID", PrintId(session_id()));
  summary_profile_.AddInfoString("Session Type", PrintTSessionType(session_type()));
  if (session_type() == TSessionType::HIVESERVER2) {
    summary_profile_.AddInfoString("HiveServer2 Protocol Version",
        Substitute("V$0", 1 + session->hs2_version));
  }
  summary_profile_.AddInfoString("Start Time", start_time().DebugString());
  summary_profile_.AddInfoString("End Time", "");
  summary_profile_.AddInfoString("Query Type", "N/A");
  summary_profile_.AddInfoString("Query State", PrintQueryState(query_state_));
  summary_profile_.AddInfoString("Query Status", "OK");
  summary_profile_.AddInfoString("Impala Version", GetVersionString(/* compact */ true));
  summary_profile_.AddInfoString("User", effective_user());
  summary_profile_.AddInfoString("Connected User", connected_user());
  summary_profile_.AddInfoString("Delegated User", do_as_user());
  summary_profile_.AddInfoString("Network Address",
      lexical_cast<string>(session_->network_address));
  summary_profile_.AddInfoString("Default Db", default_db());
  summary_profile_.AddInfoString("Sql Statement", query_ctx_.request.stmt);
  summary_profile_.AddInfoString("Coordinator",
      TNetworkAddressToString(exec_env->backend_address()));
}

ImpalaServer::QueryExecState::~QueryExecState() {
  DCHECK(wait_thread_.get() == NULL) << "BlockOnWait() needs to be called!";
}

Status ImpalaServer::QueryExecState::SetResultCache(QueryResultSet* cache,
    int64_t max_size) {
  lock_guard<mutex> l(lock_);
  DCHECK(result_cache_ == NULL);
  result_cache_.reset(cache);
  if (max_size > FLAGS_max_result_cache_size) {
    return Status(
        Substitute("Requested result-cache size of $0 exceeds Impala's maximum of $1.",
            max_size, FLAGS_max_result_cache_size));
  }
  result_cache_max_size_ = max_size;
  return Status::OK();
}

Status ImpalaServer::QueryExecState::Exec(TExecRequest* exec_request) {
  MarkActive();
  exec_request_ = *exec_request;

  profile_.AddChild(&server_profile_);
  summary_profile_.AddInfoString("Query Type", PrintTStmtType(stmt_type()));
  summary_profile_.AddInfoString("Query State", PrintQueryState(query_state_));

  switch (exec_request->stmt_type) {
    case TStmtType::QUERY:
    case TStmtType::DML:
      DCHECK(exec_request_.__isset.query_exec_request);
      return ExecQueryOrDmlRequest(exec_request_.query_exec_request);
    case TStmtType::EXPLAIN: {
      request_result_set_.reset(new vector<TResultRow>(
          exec_request_.explain_result.results));
      return Status::OK();
    }
    case TStmtType::DDL: {
      DCHECK(exec_request_.__isset.catalog_op_request);
      return ExecDdlRequest();
    }
    case TStmtType::LOAD: {
      DCHECK(exec_request_.__isset.load_data_request);
      TLoadDataResp response;
      RETURN_IF_ERROR(
          frontend_->LoadData(exec_request_.load_data_request, &response));
      request_result_set_.reset(new vector<TResultRow>);
      request_result_set_->push_back(response.load_summary);

      // Now refresh the table metadata.
      TCatalogOpRequest reset_req;
      reset_req.__set_op_type(TCatalogOpType::RESET_METADATA);
      reset_req.__set_reset_metadata_params(TResetMetadataRequest());
      reset_req.reset_metadata_params.__set_header(TCatalogServiceRequestHeader());
      reset_req.reset_metadata_params.__set_is_refresh(true);
      reset_req.reset_metadata_params.__set_table_name(
          exec_request_.load_data_request.table_name);
      catalog_op_executor_.reset(
          new CatalogOpExecutor(exec_env_, frontend_, &server_profile_));
      RETURN_IF_ERROR(catalog_op_executor_->Exec(reset_req));
      RETURN_IF_ERROR(parent_server_->ProcessCatalogUpdateResult(
          *catalog_op_executor_->update_catalog_result(),
          exec_request_.query_options.sync_ddl));
      return Status::OK();
    }
    case TStmtType::SET: {
      DCHECK(exec_request_.__isset.set_query_option_request);
      lock_guard<mutex> l(session_->lock);
      if (exec_request_.set_query_option_request.__isset.key) {
        // "SET key=value" updates the session query options.
        DCHECK(exec_request_.set_query_option_request.__isset.value);
        RETURN_IF_ERROR(SetQueryOption(
            exec_request_.set_query_option_request.key,
            exec_request_.set_query_option_request.value,
            &session_->default_query_options));
      } else {
        // "SET" returns a table of all query options.
        map<string, string> config;
        TQueryOptionsToMap(
            session_->default_query_options, &config);
        vector<string> keys, values;
        map<string, string>::const_iterator itr = config.begin();
        for (; itr != config.end(); ++itr) {
          keys.push_back(itr->first);
          values.push_back(itr->second);
        }
        SetResultSet(keys, values);
      }
      return Status::OK();
    }
    default:
      stringstream errmsg;
      errmsg << "Unknown  exec request stmt type: " << exec_request_.stmt_type;
      return Status(errmsg.str());
  }
}

Status ImpalaServer::QueryExecState::ExecLocalCatalogOp(
    const TCatalogOpRequest& catalog_op) {
  switch (catalog_op.op_type) {
    case TCatalogOpType::USE: {
      lock_guard<mutex> l(session_->lock);
      session_->database = exec_request_.catalog_op_request.use_db_params.db;
      return Status::OK();
    }
    case TCatalogOpType::SHOW_TABLES: {
      const TShowTablesParams* params = &catalog_op.show_tables_params;
      // A NULL pattern means match all tables. However, Thrift string types can't
      // be NULL in C++, so we have to test if it's set rather than just blindly
      // using the value.
      const string* table_name =
          params->__isset.show_pattern ? &(params->show_pattern) : NULL;
      TGetTablesResult table_names;
      RETURN_IF_ERROR(frontend_->GetTableNames(params->db, table_name,
          &query_ctx_.session, &table_names));
      SetResultSet(table_names.tables);
      return Status::OK();
    }
    case TCatalogOpType::SHOW_DBS: {
      const TShowDbsParams* params = &catalog_op.show_dbs_params;
      TGetDbsResult db_names;
      const string* db_pattern =
          params->__isset.show_pattern ? (&params->show_pattern) : NULL;
      RETURN_IF_ERROR(
          frontend_->GetDbNames(db_pattern, &query_ctx_.session, &db_names));
      SetResultSet(db_names.dbs);
      return Status::OK();
    }
    case TCatalogOpType::SHOW_DATA_SRCS: {
      const TShowDataSrcsParams* params = &catalog_op.show_data_srcs_params;
      TGetDataSrcsResult result;
      const string* pattern =
          params->__isset.show_pattern ? (&params->show_pattern) : NULL;
      RETURN_IF_ERROR(
          frontend_->GetDataSrcMetadata(pattern, &result));
      SetResultSet(result.data_src_names, result.locations, result.class_names,
          result.api_versions);
      return Status::OK();
    }
    case TCatalogOpType::SHOW_STATS: {
      const TShowStatsParams& params = catalog_op.show_stats_params;
      TResultSet response;
      RETURN_IF_ERROR(frontend_->GetStats(params, &response));
      // Set the result set and its schema from the response.
      request_result_set_.reset(new vector<TResultRow>(response.rows));
      result_metadata_ = response.schema;
      return Status::OK();
    }
    case TCatalogOpType::SHOW_FUNCTIONS: {
      const TShowFunctionsParams* params = &catalog_op.show_fns_params;
      TGetFunctionsResult functions;
      const string* fn_pattern =
          params->__isset.show_pattern ? (&params->show_pattern) : NULL;
      RETURN_IF_ERROR(frontend_->GetFunctions(
          params->category, params->db, fn_pattern, &query_ctx_.session, &functions));
      SetResultSet(functions.fn_ret_types, functions.fn_signatures);
      return Status::OK();
    }
    case TCatalogOpType::SHOW_ROLES: {
      const TShowRolesParams& params = catalog_op.show_roles_params;
      if (params.is_admin_op) {
        // Verify the user has privileges to perform this operation by checking against
        // the Sentry Service (via the Catalog Server).
        catalog_op_executor_.reset(new CatalogOpExecutor(exec_env_, frontend_,
            &server_profile_));

        TSentryAdminCheckRequest req;
        req.__set_header(TCatalogServiceRequestHeader());
        req.header.__set_requesting_user(effective_user());
        RETURN_IF_ERROR(catalog_op_executor_->SentryAdminCheck(req));
      }

      // If we have made it here, the user has privileges to execute this operation.
      // Return the results.
      TShowRolesResult result;
      RETURN_IF_ERROR(frontend_->ShowRoles(params, &result));
      SetResultSet(result.role_names);
      return Status::OK();
    }
    case TCatalogOpType::SHOW_GRANT_ROLE: {
      const TShowGrantRoleParams& params = catalog_op.show_grant_role_params;
      if (params.is_admin_op) {
        // Verify the user has privileges to perform this operation by checking against
        // the Sentry Service (via the Catalog Server).
        catalog_op_executor_.reset(new CatalogOpExecutor(exec_env_, frontend_,
            &server_profile_));

        TSentryAdminCheckRequest req;
        req.__set_header(TCatalogServiceRequestHeader());
        req.header.__set_requesting_user(effective_user());
        RETURN_IF_ERROR(catalog_op_executor_->SentryAdminCheck(req));
      }

      TResultSet response;
      RETURN_IF_ERROR(frontend_->GetRolePrivileges(params, &response));
      // Set the result set and its schema from the response.
      request_result_set_.reset(new vector<TResultRow>(response.rows));
      result_metadata_ = response.schema;
      return Status::OK();
    }
    case TCatalogOpType::DESCRIBE: {
      TDescribeTableResult response;
      RETURN_IF_ERROR(frontend_->DescribeTable(catalog_op.describe_table_params,
          &response));
      // Set the result set
      request_result_set_.reset(new vector<TResultRow>(response.results));
      return Status::OK();
    }
    case TCatalogOpType::SHOW_CREATE_TABLE: {
      string response;
      RETURN_IF_ERROR(frontend_->ShowCreateTable(catalog_op.show_create_table_params,
          &response));
      SetResultSet(vector<string>(1, response));
      return Status::OK();
    }
    case TCatalogOpType::SHOW_CREATE_FUNCTION: {
      string response;
      RETURN_IF_ERROR(frontend_->ShowCreateFunction(catalog_op.show_create_function_params,
          &response));
      SetResultSet(vector<string>(1, response));
      return Status::OK();
    }
    case TCatalogOpType::SHOW_FILES: {
      TResultSet response;
      RETURN_IF_ERROR(frontend_->GetTableFiles(catalog_op.show_files_params, &response));
      // Set the result set and its schema from the response.
      request_result_set_.reset(new vector<TResultRow>(response.rows));
      result_metadata_ = response.schema;
      return Status::OK();
    }
    default: {
      stringstream ss;
      ss << "Unexpected TCatalogOpType: " << catalog_op.op_type;
      return Status(ss.str());
    }
  }
}

Status ImpalaServer::QueryExecState::ExecQueryOrDmlRequest(
    const TQueryExecRequest& query_exec_request) {
  // we always need at least one plan fragment
  DCHECK_GT(query_exec_request.fragments.size(), 0);

  if (query_exec_request.__isset.query_plan) {
    stringstream plan_ss;
    // Add some delimiters to make it clearer where the plan
    // begins and the profile ends
    plan_ss << "\n----------------\n"
            << query_exec_request.query_plan
            << "----------------";
    summary_profile_.AddInfoString("Plan", plan_ss.str());
  }
  // Add info strings consumed by CM: Estimated mem/vcores and tables missing stats.
  if (query_exec_request.__isset.per_host_mem_req) {
    stringstream ss;
    ss << query_exec_request.per_host_mem_req;
    summary_profile_.AddInfoString(PER_HOST_MEM_KEY, ss.str());
  }
  if (query_exec_request.__isset.per_host_vcores) {
    stringstream ss;
    ss << query_exec_request.per_host_vcores;
    summary_profile_.AddInfoString(PER_HOST_VCORES_KEY, ss.str());
  }
  if (!query_exec_request.query_ctx.__isset.parent_query_id &&
      query_exec_request.query_ctx.__isset.tables_missing_stats &&
      !query_exec_request.query_ctx.tables_missing_stats.empty()) {
    stringstream ss;
    const vector<TTableName>& tbls = query_exec_request.query_ctx.tables_missing_stats;
    for (int i = 0; i < tbls.size(); ++i) {
      if (i != 0) ss << ",";
      ss << tbls[i].db_name << "." << tbls[i].table_name;
    }
    summary_profile_.AddInfoString(TABLES_MISSING_STATS_KEY, ss.str());
  }

  if (!query_exec_request.query_ctx.__isset.parent_query_id &&
      query_exec_request.query_ctx.__isset.tables_with_corrupt_stats &&
      !query_exec_request.query_ctx.tables_with_corrupt_stats.empty()) {
    stringstream ss;
    const vector<TTableName>& tbls =
        query_exec_request.query_ctx.tables_with_corrupt_stats;
    for (int i = 0; i < tbls.size(); ++i) {
      if (i != 0) ss << ",";
      ss << tbls[i].db_name << "." << tbls[i].table_name;
    }
    summary_profile_.AddInfoString(TABLES_WITH_CORRUPT_STATS_KEY, ss.str());
  }

  // If desc_tbl is not set, query has SELECT with no FROM. In that
  // case, the query can only have a single fragment, and that fragment needs to be
  // executed by the coordinator. This check confirms that.
  // If desc_tbl is set, the query may or may not have a coordinator fragment.
  bool has_coordinator_fragment =
      query_exec_request.fragments[0].partition.type == TPartitionType::UNPARTITIONED;
  DCHECK(has_coordinator_fragment || query_exec_request.__isset.desc_tbl);

  if (FLAGS_enable_rm) {
    DCHECK(exec_env_->resource_broker() != NULL);
  }
  schedule_.reset(new QuerySchedule(query_id(), query_exec_request,
      exec_request_.query_options, effective_user(), &summary_profile_, query_events_));
  coord_.reset(new Coordinator(exec_env_, query_events_));
  Status status = exec_env_->scheduler()->Schedule(coord_.get(), schedule_.get());
  summary_profile_.AddInfoString("Request Pool", schedule_->request_pool());
  if (FLAGS_enable_rm) {
    if (status.ok()) {
      stringstream reservation_request_ss;
      reservation_request_ss << schedule_->reservation_request();
      summary_profile_.AddInfoString("Resource reservation request",
          reservation_request_ss.str());
    }
  }

  {
    lock_guard<mutex> l(lock_);
    RETURN_IF_ERROR(UpdateQueryStatus(status));
  }

  if (FLAGS_enable_rm && schedule_->HasReservation()) {
    // Add the granted reservation to the query profile.
    stringstream reservation_ss;
    reservation_ss << *schedule_->reservation();
    summary_profile_.AddInfoString("Granted resource reservation", reservation_ss.str());
    query_events_->MarkEvent("Resources reserved");
  }
  status = coord_->Exec(*schedule_, &output_expr_ctxs_);
  {
    lock_guard<mutex> l(lock_);
    RETURN_IF_ERROR(UpdateQueryStatus(status));
  }

  profile_.AddChild(coord_->query_profile());
  return Status::OK();
}

Status ImpalaServer::QueryExecState::ExecDdlRequest() {
  string op_type = catalog_op_type() == TCatalogOpType::DDL ?
      PrintTDdlType(ddl_type()) : PrintTCatalogOpType(catalog_op_type());
  summary_profile_.AddInfoString("DDL Type", op_type);

  if (catalog_op_type() != TCatalogOpType::DDL &&
      catalog_op_type() != TCatalogOpType::RESET_METADATA) {
    Status status = ExecLocalCatalogOp(exec_request_.catalog_op_request);
    lock_guard<mutex> l(lock_);
    return UpdateQueryStatus(status);
  }

  if (ddl_type() == TDdlType::COMPUTE_STATS) {
    TComputeStatsParams& compute_stats_params =
        exec_request_.catalog_op_request.ddl_params.compute_stats_params;
    // Add child queries for computing table and column stats.
    if (compute_stats_params.__isset.tbl_stats_query) {
      child_queries_.push_back(
          ChildQuery(compute_stats_params.tbl_stats_query, this, parent_server_));
    }
    if (compute_stats_params.__isset.col_stats_query) {
      child_queries_.push_back(
          ChildQuery(compute_stats_params.col_stats_query, this, parent_server_));
    }
    if (child_queries_.size() > 0) ExecChildQueriesAsync();
    return Status::OK();
  }

  catalog_op_executor_.reset(new CatalogOpExecutor(exec_env_, frontend_,
      &server_profile_));
  Status status = catalog_op_executor_->Exec(exec_request_.catalog_op_request);
  {
    lock_guard<mutex> l(lock_);
    RETURN_IF_ERROR(UpdateQueryStatus(status));
  }

  // If this is a CTAS request, there will usually be more work to do
  // after executing the CREATE TABLE statement (the INSERT portion of the operation).
  // The exception is if the user specified IF NOT EXISTS and the table already
  // existed, in which case we do not execute the INSERT.
  if (catalog_op_type() == TCatalogOpType::DDL &&
      ddl_type() == TDdlType::CREATE_TABLE_AS_SELECT &&
      !catalog_op_executor_->ddl_exec_response()->new_table_created) {
    DCHECK(exec_request_.catalog_op_request.
        ddl_params.create_table_params.if_not_exists);
    return Status::OK();
  }

  // Add newly created table to catalog cache.
  RETURN_IF_ERROR(parent_server_->ProcessCatalogUpdateResult(
      *catalog_op_executor_->update_catalog_result(),
      exec_request_.query_options.sync_ddl));

  if (catalog_op_type() == TCatalogOpType::DDL &&
      ddl_type() == TDdlType::CREATE_TABLE_AS_SELECT) {
    // At this point, the remainder of the CTAS request executes
    // like a normal DML request. As with other DML requests, it will
    // wait for another catalog update if any partitions were altered as a result
    // of the operation.
    DCHECK(exec_request_.__isset.query_exec_request);
    RETURN_IF_ERROR(ExecQueryOrDmlRequest(exec_request_.query_exec_request));
  }
  return Status::OK();
}

void ImpalaServer::QueryExecState::Done() {
  MarkActive();
  // Make sure we join on wait_thread_ before we finish (and especially before this object
  // is destroyed).
  BlockOnWait();
  unique_lock<mutex> l(lock_);
  end_time_ = TimestampValue::LocalTime();
  summary_profile_.AddInfoString("End Time", end_time().DebugString());
  summary_profile_.AddInfoString("Query State", PrintQueryState(query_state_));
  query_events_->MarkEvent("Unregister query");

  if (coord_.get() != NULL) {
    Expr::Close(output_expr_ctxs_, coord_->runtime_state());
    // Release any reserved resources.
    Status status = exec_env_->scheduler()->Release(schedule_.get());
    if (!status.ok()) {
      LOG(WARNING) << "Failed to release resources of query " << schedule_->query_id()
            << " because of error: " << status.GetDetail();
    }
  }

  // Update result set cache metrics, and update mem limit accounting.
  ClearResultCache();
}

Status ImpalaServer::QueryExecState::Exec(const TMetadataOpRequest& exec_request) {
  TResultSet metadata_op_result;
  // Like the other Exec(), fill out as much profile information as we're able to.
  summary_profile_.AddInfoString("Query Type", PrintTStmtType(TStmtType::DDL));
  summary_profile_.AddInfoString("Query State", PrintQueryState(query_state_));
  RETURN_IF_ERROR(frontend_->ExecHiveServer2MetadataOp(exec_request,
      &metadata_op_result));
  result_metadata_ = metadata_op_result.schema;
  request_result_set_.reset(new vector<TResultRow>(metadata_op_result.rows));
  return Status::OK();
}

void ImpalaServer::QueryExecState::WaitAsync() {
  wait_thread_.reset(new Thread(
      "query-exec-state", "wait-thread", &ImpalaServer::QueryExecState::Wait, this));
}

void ImpalaServer::QueryExecState::BlockOnWait() {
  if (wait_thread_.get() != NULL) {
    wait_thread_->Join();
    wait_thread_.reset();
  }
}

void ImpalaServer::QueryExecState::Wait() {
  // block until results are ready
  Status status = WaitInternal();
  {
    lock_guard<mutex> l(lock_);
    if (returns_result_set()) {
      query_events()->MarkEvent("Rows available");
    } else {
      query_events()->MarkEvent("Request finished");
    }
    UpdateQueryStatus(status);
  }
  if (status.ok()) {
    UpdateQueryState(QueryState::FINISHED);
  }
}

Status ImpalaServer::QueryExecState::WaitInternal() {
  // Explain requests have already populated the result set. Nothing to do here.
  if (exec_request_.stmt_type == TStmtType::EXPLAIN) {
    MarkInactive();
    return Status::OK();
  }

  RETURN_IF_ERROR(WaitForChildQueries());
  if (coord_.get() != NULL) {
    RETURN_IF_ERROR(coord_->Wait());
    RETURN_IF_ERROR(Expr::Open(output_expr_ctxs_, coord_->runtime_state()));
    RETURN_IF_ERROR(UpdateCatalog());
  }

  if (catalog_op_type() == TCatalogOpType::DDL &&
      ddl_type() == TDdlType::COMPUTE_STATS && child_queries_.size() > 0) {
    RETURN_IF_ERROR(UpdateTableAndColumnStats());
  }

  if (!returns_result_set()) {
    // Queries that do not return a result are finished at this point. This includes
    // DML operations and a subset of the DDL operations.
    eos_ = true;
  } else if (catalog_op_type() == TCatalogOpType::DDL &&
      ddl_type() == TDdlType::CREATE_TABLE_AS_SELECT) {
    SetCreateTableAsSelectResultSet();
  }
  // Rows are available now (for SELECT statement), so start the 'wait' timer that tracks
  // how long Impala waits for the client to fetch rows. For other statements, track the
  // time until a Close() is received.
  MarkInactive();
  return Status::OK();
}

Status ImpalaServer::QueryExecState::FetchRows(const int32_t max_rows,
    QueryResultSet* fetched_rows) {
  // Pause the wait timer, since the client has instructed us to do work on its behalf.
  MarkActive();

  // ImpalaServer::FetchInternal has already taken our lock_
  UpdateQueryStatus(FetchRowsInternal(max_rows, fetched_rows));

  MarkInactive();
  return query_status_;
}

Status ImpalaServer::QueryExecState::RestartFetch() {
  // No result caching for this query. Restart is invalid.
  if (result_cache_max_size_ <= 0) {
    return Status(ErrorMsg(TErrorCode::RECOVERABLE_ERROR,
        "Restarting of fetch requires enabling of query result caching."));
  }
  // The cache overflowed on a previous fetch.
  if (result_cache_.get() == NULL) {
    stringstream ss;
    ss << "The query result cache exceeded its limit of " << result_cache_max_size_
       << " rows. Restarting the fetch is not possible.";
    return Status(ErrorMsg(TErrorCode::RECOVERABLE_ERROR, ss.str()));
  }
  // Reset fetch state to start over.
  eos_ = false;
  num_rows_fetched_ = 0;
  return Status::OK();
}

void ImpalaServer::QueryExecState::UpdateQueryState(QueryState::type query_state) {
  lock_guard<mutex> l(lock_);
  if (query_state_ < query_state) query_state_ = query_state;
}

Status ImpalaServer::QueryExecState::UpdateQueryStatus(const Status& status) {
  // Preserve the first non-ok status
  if (!status.ok() && query_status_.ok()) {
    query_state_ = QueryState::EXCEPTION;
    query_status_ = status;
    summary_profile_.AddInfoString("Query Status", query_status_.GetDetail());
  }

  return status;
}

Status ImpalaServer::QueryExecState::FetchRowsInternal(const int32_t max_rows,
    QueryResultSet* fetched_rows) {
  DCHECK(query_state_ != QueryState::EXCEPTION);

  if (eos_) return Status::OK();

  if (request_result_set_ != NULL) {
    query_state_ = QueryState::FINISHED;
    int num_rows = 0;
    const vector<TResultRow>& all_rows = (*(request_result_set_.get()));
    // max_rows <= 0 means no limit
    while ((num_rows < max_rows || max_rows <= 0)
        && num_rows_fetched_ < all_rows.size()) {
      fetched_rows->AddOneRow(all_rows[num_rows_fetched_]);
      ++num_rows_fetched_;
      ++num_rows;
    }
    eos_ = (num_rows_fetched_ == all_rows.size());
    return Status::OK();
  }

  int32_t num_rows_fetched_from_cache = 0;
  if (result_cache_max_size_ > 0 && result_cache_ != NULL) {
    // Satisfy the fetch from the result cache if possible.
    int cache_fetch_size = (max_rows <= 0) ? result_cache_->size() : max_rows;
    num_rows_fetched_from_cache =
        fetched_rows->AddRows(result_cache_.get(), num_rows_fetched_, cache_fetch_size);
    num_rows_fetched_ += num_rows_fetched_from_cache;
    if (num_rows_fetched_from_cache >= max_rows) return Status::OK();
  }

  // List of expr values to hold evaluated rows from the query
  vector<void*> result_row;
  result_row.resize(output_expr_ctxs_.size());

  // List of scales for floating point values in result_row
  vector<int> scales;
  scales.resize(result_row.size());

  if (coord_ == NULL) {
    // Query with LIMIT 0.
    query_state_ = QueryState::FINISHED;
    eos_ = true;
    return Status::OK();
  }

  query_state_ = QueryState::FINISHED;  // results will be ready after this call
  // Fetch the next batch if we've returned the current batch entirely
  if (current_batch_ == NULL || current_batch_row_ >= current_batch_->num_rows()) {
    RETURN_IF_ERROR(FetchNextBatch());
  }
  if (current_batch_ == NULL) return Status::OK();

  // Maximum number of rows to be fetched from the coord.
  int32_t max_coord_rows = max_rows;
  if (max_rows > 0) {
    DCHECK_LE(num_rows_fetched_from_cache, max_rows);
    max_coord_rows = max_rows - num_rows_fetched_from_cache;
  }
  {
    SCOPED_TIMER(row_materialization_timer_);
    // Convert the available rows, limited by max_coord_rows
    int available = current_batch_->num_rows() - current_batch_row_;
    int fetched_count = available;
    // max_coord_rows <= 0 means no limit
    if (max_coord_rows > 0 && max_coord_rows < available) fetched_count = max_coord_rows;
    for (int i = 0; i < fetched_count; ++i) {
      TupleRow* row = current_batch_->GetRow(current_batch_row_);
      RETURN_IF_ERROR(GetRowValue(row, &result_row, &scales));
      RETURN_IF_ERROR(fetched_rows->AddOneRow(result_row, scales));
      ++num_rows_fetched_;
      ++current_batch_row_;
    }
  }
  ExprContext::FreeLocalAllocations(output_expr_ctxs_);
  // Check if there was an error evaluating a row value.
  RETURN_IF_ERROR(coord_->runtime_state()->CheckQueryState());

  // Update the result cache if necessary.
  if (result_cache_max_size_ > 0 && result_cache_.get() != NULL) {
    int rows_fetched_from_coord = fetched_rows->size() - num_rows_fetched_from_cache;
    if (result_cache_->size() + rows_fetched_from_coord > result_cache_max_size_) {
      // Set the cache to NULL to indicate that adding the rows fetched from the coord
      // would exceed the bound of the cache, and therefore, RestartFetch() should fail.
      ClearResultCache();
      return Status::OK();
    }

    // We guess the size of the cache after adding fetched_rows by looking at the size of
    // fetched_rows itself, and using this estimate to confirm that the memtracker will
    // allow us to use this much extra memory. In fact, this might be an overestimate, as
    // the size of two result sets combined into one is not always the size of both result
    // sets added together (the best example is the null bitset for each column: it might
    // have only one entry in each result set, and as a result consume two bytes, but when
    // the result sets are combined, only one byte is needed). Therefore after we add the
    // new result set into the cache, we need to fix up the memory consumption to the
    // actual levels to ensure we don't 'leak' bytes that we aren't using.
    int64_t before = result_cache_->ByteSize();

    // Upper-bound on memory required to add fetched_rows to the cache.
    int64_t delta_bytes =
        fetched_rows->ByteSize(num_rows_fetched_from_cache, fetched_rows->size());
    MemTracker* query_mem_tracker = coord_->query_mem_tracker();
    // Count the cached rows towards the mem limit.
    if (!query_mem_tracker->TryConsume(delta_bytes)) {
      return coord_->runtime_state()->SetMemLimitExceeded(
          query_mem_tracker, delta_bytes);
    }
    // Append all rows fetched from the coordinator into the cache.
    int num_rows_added = result_cache_->AddRows(
        fetched_rows, num_rows_fetched_from_cache, fetched_rows->size());

    int64_t after = result_cache_->ByteSize();

    // Confirm that this was not an underestimate of the memory required.
    DCHECK_GE(before + delta_bytes, after)
        << "Combined result sets consume more memory than both individually "
        << Substitute("(before: $0, delta_bytes: $1, after: $2)",
            before, delta_bytes, after);

    // Fix up the tracked values
    if (before + delta_bytes > after) {
      query_mem_tracker->Release(before + delta_bytes - after);
      delta_bytes = after - before;
    }

    // Update result set cache metrics.
    ImpaladMetrics::RESULTSET_CACHE_TOTAL_NUM_ROWS->Increment(num_rows_added);
    ImpaladMetrics::RESULTSET_CACHE_TOTAL_BYTES->Increment(delta_bytes);
  }

  return Status::OK();
}

Status ImpalaServer::QueryExecState::GetRowValue(TupleRow* row, vector<void*>* result,
                                                 vector<int>* scales) {
  DCHECK(result->size() >= output_expr_ctxs_.size());
  for (int i = 0; i < output_expr_ctxs_.size(); ++i) {
    (*result)[i] = output_expr_ctxs_[i]->GetValue(row);
    (*scales)[i] = output_expr_ctxs_[i]->root()->output_scale();
  }
  return Status::OK();
}

void ImpalaServer::QueryExecState::Cancel(const Status* cause) {
  // Cancel and close child queries before cancelling parent.
  BOOST_FOREACH(ChildQuery& child_query, child_queries_) {
    child_query.Cancel();
  }

  // If the query is completed or cancelled, no need to cancel.
  if (eos_ || query_state_ == QueryState::EXCEPTION) return;

  if (cause != NULL) {
    UpdateQueryStatus(*cause);
    query_events_->MarkEvent("Cancelled");
    query_state_ = QueryState::EXCEPTION;
  }
  if (coord_.get() != NULL) coord_->Cancel(cause);
}

Status ImpalaServer::QueryExecState::UpdateCatalog() {
  if (!exec_request().__isset.query_exec_request ||
      exec_request().query_exec_request.stmt_type != TStmtType::DML) {
    return Status::OK();
  }

  query_events_->MarkEvent("DML data written");
  SCOPED_TIMER(ADD_TIMER(&server_profile_, "MetastoreUpdateTimer"));

  TQueryExecRequest query_exec_request = exec_request().query_exec_request;
  if (query_exec_request.__isset.finalize_params) {
    const TFinalizeParams& finalize_params = query_exec_request.finalize_params;
    TUpdateCatalogRequest catalog_update;
    catalog_update.__set_header(TCatalogServiceRequestHeader());
    catalog_update.header.__set_requesting_user(effective_user());
    if (!coord()->PrepareCatalogUpdate(&catalog_update)) {
      VLOG_QUERY << "No partitions altered, not updating metastore (query id: "
                 << query_id() << ")";
    } else {
      // TODO: We track partitions written to, not created, which means
      // that we do more work than is necessary, because written-to
      // partitions don't always require a metastore change.
      VLOG_QUERY << "Updating metastore with " << catalog_update.created_partitions.size()
                 << " altered partitions ("
                 << join (catalog_update.created_partitions, ", ") << ")";

      catalog_update.target_table = finalize_params.table_name;
      catalog_update.db_name = finalize_params.table_db;

      Status cnxn_status;
      const TNetworkAddress& address =
          MakeNetworkAddress(FLAGS_catalog_service_host, FLAGS_catalog_service_port);
      CatalogServiceConnection client(
          exec_env_->catalogd_client_cache(), address, &cnxn_status);
      RETURN_IF_ERROR(cnxn_status);

      VLOG_QUERY << "Executing FinalizeDml() using CatalogService";
      TUpdateCatalogResponse resp;
      RETURN_IF_ERROR(
          client.DoRpc(&CatalogServiceClient::UpdateCatalog, catalog_update, &resp));

      Status status(resp.result.status);
      if (!status.ok()) LOG(ERROR) << "ERROR Finalizing DML: " << status.GetDetail();
      RETURN_IF_ERROR(status);
      RETURN_IF_ERROR(parent_server_->ProcessCatalogUpdateResult(resp.result,
          exec_request_.query_options.sync_ddl));
    }
  }
  query_events_->MarkEvent("DML Metastore update finished");
  return Status::OK();
}

Status ImpalaServer::QueryExecState::FetchNextBatch() {
  DCHECK(!eos_);
  DCHECK(coord_.get() != NULL);

  // Temporarily release lock so calls to Cancel() are not blocked.  fetch_rows_lock_
  // ensures that we do not call coord_->GetNext() multiple times concurrently.
  lock_.unlock();
  Status status = coord_->GetNext(&current_batch_, coord_->runtime_state());
  lock_.lock();
  if (!status.ok()) return status;

  // Check if query status has changed during GetNext() call
  if (!query_status_.ok()) {
    current_batch_ = NULL;
    return query_status_;
  }

  current_batch_row_ = 0;
  eos_ = current_batch_ == NULL;
  return Status::OK();
}

void ImpalaServer::QueryExecState::SetResultSet(const vector<string>& results) {
  request_result_set_.reset(new vector<TResultRow>);
  request_result_set_->resize(results.size());
  for (int i = 0; i < results.size(); ++i) {
    (*request_result_set_.get())[i].__isset.colVals = true;
    (*request_result_set_.get())[i].colVals.resize(1);
    (*request_result_set_.get())[i].colVals[0].__set_string_val(results[i]);
  }
}

void ImpalaServer::QueryExecState::SetResultSet(const vector<string>& col1,
    const vector<string>& col2) {
  DCHECK_EQ(col1.size(), col2.size());

  request_result_set_.reset(new vector<TResultRow>);
  request_result_set_->resize(col1.size());
  for (int i = 0; i < col1.size(); ++i) {
    (*request_result_set_.get())[i].__isset.colVals = true;
    (*request_result_set_.get())[i].colVals.resize(2);
    (*request_result_set_.get())[i].colVals[0].__set_string_val(col1[i]);
    (*request_result_set_.get())[i].colVals[1].__set_string_val(col2[i]);
  }
}

void ImpalaServer::QueryExecState::SetResultSet(const vector<string>& col1,
    const vector<string>& col2, const vector<string>& col3, const vector<string>& col4) {
  DCHECK_EQ(col1.size(), col2.size());
  DCHECK_EQ(col1.size(), col3.size());
  DCHECK_EQ(col1.size(), col4.size());

  request_result_set_.reset(new vector<TResultRow>);
  request_result_set_->resize(col1.size());
  for (int i = 0; i < col1.size(); ++i) {
    (*request_result_set_.get())[i].__isset.colVals = true;
    (*request_result_set_.get())[i].colVals.resize(4);
    (*request_result_set_.get())[i].colVals[0].__set_string_val(col1[i]);
    (*request_result_set_.get())[i].colVals[1].__set_string_val(col2[i]);
    (*request_result_set_.get())[i].colVals[2].__set_string_val(col3[i]);
    (*request_result_set_.get())[i].colVals[3].__set_string_val(col4[i]);
  }
}

void ImpalaServer::QueryExecState::SetCreateTableAsSelectResultSet() {
  DCHECK(ddl_type() == TDdlType::CREATE_TABLE_AS_SELECT);
  int64_t total_num_rows_inserted = 0;
  // There will only be rows inserted in the case a new table was created as part of this
  // operation.
  if (catalog_op_executor_->ddl_exec_response()->new_table_created) {
    DCHECK(coord_.get());
    BOOST_FOREACH(
        const PartitionStatusMap::value_type& p, coord_->per_partition_status()) {
      total_num_rows_inserted += p.second.num_appended_rows;
    }
  }
  const string& summary_msg = Substitute("Inserted $0 row(s)", total_num_rows_inserted);
  VLOG_QUERY << summary_msg;
  vector<string> results(1, summary_msg);
  SetResultSet(results);
}

void ImpalaServer::QueryExecState::MarkInactive() {
  client_wait_sw_.Start();
  lock_guard<mutex> l(expiration_data_lock_);
  last_active_time_ = UnixMillis();
  DCHECK(ref_count_ > 0) << "Invalid MarkInactive()";
  --ref_count_;
}

void ImpalaServer::QueryExecState::MarkActive() {
  client_wait_sw_.Stop();
  int64_t elapsed_time = client_wait_sw_.ElapsedTime();
  client_wait_timer_->Set(elapsed_time);
  lock_guard<mutex> l(expiration_data_lock_);
  last_active_time_ = UnixMillis();
  ++ref_count_;
}

Status ImpalaServer::QueryExecState::UpdateTableAndColumnStats() {
  DCHECK_GE(child_queries_.size(), 1);
  DCHECK_LE(child_queries_.size(), 2);
  catalog_op_executor_.reset(
      new CatalogOpExecutor(exec_env_, frontend_, &server_profile_));

  // If there was no column stats query, pass in empty thrift structures to
  // ExecComputeStats(). Otherwise pass in the column stats result.
  TTableSchema col_stats_schema;
  TRowSet col_stats_data;
  if (child_queries_.size() > 1) {
    col_stats_schema = child_queries_[1].result_schema();
    col_stats_data = child_queries_[1].result_data();
  }

  Status status = catalog_op_executor_->ExecComputeStats(
      exec_request_.catalog_op_request.ddl_params.compute_stats_params,
      child_queries_[0].result_schema(),
      child_queries_[0].result_data(),
      col_stats_schema,
      col_stats_data);
  {
    lock_guard<mutex> l(lock_);
    RETURN_IF_ERROR(UpdateQueryStatus(status));
  }
  RETURN_IF_ERROR(parent_server_->ProcessCatalogUpdateResult(
      *catalog_op_executor_->update_catalog_result(),
      exec_request_.query_options.sync_ddl));

  // Set the results to be reported to the client.
  const TDdlExecResponse* ddl_resp = catalog_op_executor_->ddl_exec_response();
  if (ddl_resp != NULL && ddl_resp->__isset.result_set) {
    result_metadata_ = ddl_resp->result_set.schema;
    request_result_set_.reset(new vector<TResultRow>);
    request_result_set_->assign(
        ddl_resp->result_set.rows.begin(), ddl_resp->result_set.rows.end());
  }

  query_events_->MarkEvent("Metastore update finished");
  return Status::OK();
}

void ImpalaServer::QueryExecState::ExecChildQueriesAsync() {
  DCHECK(child_queries_thread_.get() == NULL);
  child_queries_thread_.reset(new Thread("query-exec-state", "async child queries",
      bind(&ImpalaServer::QueryExecState::ExecChildQueries, this)));
}

void ImpalaServer::QueryExecState::ExecChildQueries() {
  for (int i = 0; i < child_queries_.size(); ++i) {
    if (!child_queries_status_.ok()) return;
    child_queries_status_ = child_queries_[i].ExecAndFetch();
  }
}

Status ImpalaServer::QueryExecState::WaitForChildQueries() {
  if (child_queries_thread_.get() == NULL) return Status::OK();
  child_queries_thread_->Join();
  {
    lock_guard<mutex> l(lock_);
    RETURN_IF_ERROR(query_status_);
    RETURN_IF_ERROR(UpdateQueryStatus(child_queries_status_));
  }
  query_events_->MarkEvent("Child queries finished");
  return Status::OK();
}

void ImpalaServer::QueryExecState::ClearResultCache() {
  if (result_cache_ == NULL) return;
  // Update result set cache metrics and mem limit accounting.
  ImpaladMetrics::RESULTSET_CACHE_TOTAL_NUM_ROWS->Increment(-result_cache_->size());
  int64_t total_bytes = result_cache_->ByteSize();
  ImpaladMetrics::RESULTSET_CACHE_TOTAL_BYTES->Increment(-total_bytes);
  if (coord_ != NULL) {
    DCHECK(coord_->query_mem_tracker() != NULL);
    coord_->query_mem_tracker()->Release(total_bytes);
  }
  result_cache_.reset(NULL);
}

}
