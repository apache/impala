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

#include "service/client-request-state.h"

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <limits>
#include <gutil/strings/substitute.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "catalog/catalog-service-client-wrapper.h"
#include "common/status.h"
#include "exprs/timezone_db.h"
#include "kudu/rpc/rpc_controller.h"
#include "rpc/rpc-mgr.inline.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-driver.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "scheduling/admission-control-client.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/scheduler.h"
#include "service/frontend.h"
#include "service/impala-server.h"
#include "service/query-options.h"
#include "service/query-result-set.h"
#include "util/auth-util.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/lineage-util.h"
#include "util/pretty-printer.h"
#include "util/redactor.h"
#include "util/runtime-profile-counters.h"
#include "util/time.h"
#include "util/uid-util.h"

#include "gen-cpp/CatalogService_types.h"
#include "gen-cpp/control_service.pb.h"
#include "gen-cpp/control_service.proxy.h"

#include <thrift/Thrift.h>

#include "common/names.h"
#include "control-service.h"

using boost::algorithm::iequals;
using boost::algorithm::join;
using boost::algorithm::replace_all_copy;
using kudu::rpc::RpcController;
using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift;
using namespace beeswax;
using namespace strings;

DECLARE_bool(abort_on_failed_audit_event);
DECLARE_bool(abort_on_failed_lineage_event);
DECLARE_int32(krpc_port);
DECLARE_int64(max_result_cache_size);
DECLARE_bool(use_local_catalog);

namespace impala {

PROFILE_DEFINE_TIMER(ClientFetchLockWaitTimer, UNSTABLE,
    "Cumulative time client fetch requests waiting for locks.");
PROFILE_DEFINE_SUMMARY_STATS_TIMER(GetInFlightProfileTimeStats, UNSTABLE,
    "Summary stats of the time dumping profiles when the query is still in-flight.");

// Keys into the info string map of the runtime profile referring to specific
// items used by CM for monitoring purposes.
static const string PER_HOST_MEM_KEY = "Estimated Per-Host Mem";
static const string TABLES_MISSING_STATS_KEY = "Tables Missing Stats";
static const string TABLES_WITH_CORRUPT_STATS_KEY = "Tables With Corrupt Table Stats";
static const string TABLES_WITH_MISSING_DISK_IDS_KEY = "Tables With Missing Disk Ids";

static const string QUERY_STATUS_KEY = "Query Status";
static const string RETRY_STATUS_KEY = "Retry Status";

const TExecRequest ClientRequestState::unknown_exec_request_;

ClientRequestState::ClientRequestState(const TQueryCtx& query_ctx, Frontend* frontend,
    ImpalaServer* server, shared_ptr<ImpalaServer::SessionState> session,
    QueryDriver* query_driver)
  : query_ctx_(query_ctx),
    last_active_time_ms_(numeric_limits<int64_t>::max()),
    child_query_executor_(new ChildQueryExecutor),
    session_(session),
    coord_exec_called_(false),
    // Profile is assigned name w/ id after planning
    profile_(RuntimeProfile::Create(&profile_pool_, "Query", false)),
    frontend_profile_(RuntimeProfile::Create(&profile_pool_, "Frontend", false)),
    server_profile_(RuntimeProfile::Create(&profile_pool_, "ImpalaServer", false)),
    summary_profile_(RuntimeProfile::Create(&profile_pool_, "Summary", false)),
    frontend_(frontend),
    parent_server_(server),
    start_time_us_(UnixMicros()),
    fetch_rows_timeout_us_(MICROS_PER_MILLI * query_options().fetch_rows_timeout_ms),
    parent_driver_(query_driver) {
  bool is_external_fe = session_type() == TSessionType::EXTERNAL_FRONTEND;
  // "Impala Backend Timeline" was specifically chosen to exploit the lexicographical
  // ordering defined by the underlying std::map holding the timelines displayed in
  // the web UI. This helps ensure that "Frontend Timeline" is displayed before
  // "Impala Backend Timeline".
  query_events_ = summary_profile_->AddEventSequence(
      is_external_fe ? "Impala Backend Timeline" : "Query Timeline");
  query_events_->Start();
  profile_->AddChild(summary_profile_);

#ifndef NDEBUG
  profile_->AddInfoString("DEBUG MODE WARNING", "Query profile created while running a "
      "DEBUG build of Impala. Use RELEASE builds to measure query performance.");
#endif
  row_materialization_timer_ = ADD_TIMER(server_profile_, "RowMaterializationTimer");
  num_rows_fetched_counter_ = ADD_COUNTER(server_profile_, "NumRowsFetched", TUnit::UNIT);
  row_materialization_rate_ =
      server_profile_->AddDerivedCounter("RowMaterializationRate", TUnit::UNIT_PER_SECOND,
          bind<int64_t>(&RuntimeProfile::UnitsPerSecond, num_rows_fetched_counter_,
                                             row_materialization_timer_));
  num_rows_fetched_from_cache_counter_ =
      ADD_COUNTER(server_profile_, "NumRowsFetchedFromCache", TUnit::UNIT);
  client_wait_timer_ = ADD_TIMER(server_profile_, "ClientFetchWaitTimer");
  client_wait_time_stats_ =
      ADD_SUMMARY_STATS_TIMER(server_profile_, "ClientFetchWaitTimeStats");
  rpc_read_timer_ = ADD_TIMER(server_profile_, "RPCReadTimer");
  rpc_write_timer_ = ADD_TIMER(server_profile_, "RPCWriteTimer");
  rpc_count_ = ADD_COUNTER(server_profile_, "RPCCount", TUnit::UNIT);
  get_inflight_profile_time_stats_ =
      PROFILE_GetInFlightProfileTimeStats.Instantiate(server_profile_);
  client_fetch_lock_wait_timer_ =
      PROFILE_ClientFetchLockWaitTimer.Instantiate(server_profile_);

  profile_->set_name("Query (id=" + PrintId(query_id()) + ")");
  summary_profile_->AddInfoString("Session ID", PrintId(session_id()));
  summary_profile_->AddInfoString("Session Type", PrintValue(session_type()));
  if (session_type() == TSessionType::HIVESERVER2 ||
      session_type() == TSessionType::EXTERNAL_FRONTEND) {
    summary_profile_->AddInfoString("HiveServer2 Protocol Version",
        Substitute("V$0", 1 + session->hs2_version));
  }
  // Certain API clients expect Start Time and End Time to be date-time strings
  // of nanosecond precision, so we explicitly specify the precision here.
  summary_profile_->AddInfoString("Start Time", ToStringFromUnixMicros(start_time_us(),
      TimePrecision::Nanosecond));
  summary_profile_->AddInfoString("End Time", "");
  summary_profile_->AddInfoString("Duration", "");
  summary_profile_->AddInfoString("Query Type", "N/A");
  summary_profile_->AddInfoString("Query State", PrintValue(BeeswaxQueryState()));
  summary_profile_->AddInfoString(
      "Impala Query State", ExecStateToString(exec_state()));
  summary_profile_->AddInfoString("Query Status", "OK");
  summary_profile_->AddInfoString("Impala Version", GetVersionString(/* compact */ true));
  summary_profile_->AddInfoString("User", effective_user());
  summary_profile_->AddInfoString("Connected User", connected_user());
  summary_profile_->AddInfoString("Delegated User", do_as_user());
  summary_profile_->AddInfoString("Network Address",
      TNetworkAddressToString(session_->network_address));
  summary_profile_->AddInfoString("Default Db", default_db());
  summary_profile_->AddInfoStringRedacted(
      "Sql Statement", query_ctx_.client_request.stmt);
  summary_profile_->AddInfoString("Coordinator",
      TNetworkAddressToString(ExecEnv::GetInstance()->configured_backend_address()));

  summary_profile_->AddChild(frontend_profile_);

  AdmissionControlClient::Create(query_ctx_, &admission_control_client_);
}

ClientRequestState::~ClientRequestState() {
  DCHECK(wait_thread_.get() == NULL) << "Finalize() needs to be called!";
  DCHECK(!track_rpcs_);  // Should get set to false in Finalize()
  DCHECK(pending_rpcs_.empty()); // Should get cleared in Finalize()
  UnRegisterRemainingRPCs(); // Avoid memory leaks if Finalize() didn't get called
}

Status ClientRequestState::SetResultCache(QueryResultSet* cache,
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

void ClientRequestState::SetRemoteSubmitTime(int64_t remote_submit_time) {
  query_events_->Start(remote_submit_time);
}

void ClientRequestState::SetFrontendProfile(const TExecRequest& exec_request) {
  // Should we defer creating and adding the child until here? probably.
  TRuntimeProfileTree prof_tree;
  prof_tree.nodes.emplace_back(std::move(exec_request.profile));
  for (auto& child : exec_request.profile_children) {
    prof_tree.nodes.emplace_back(std::move(child));
  }
  prof_tree.nodes.at(0).num_children = prof_tree.nodes.size() - 1;
  frontend_profile_->Update(prof_tree, false);
}

void ClientRequestState::AddBlacklistedExecutorAddress(const NetworkAddressPB& addr) {
  lock_guard<mutex> l(lock_);
  if (!WasRetried()) blacklisted_executor_addresses_.emplace(addr);
}

void ClientRequestState::SetBlacklistedExecutorAddresses(
    std::unordered_set<NetworkAddressPB>& executor_addresses) {
  DCHECK(blacklisted_executor_addresses_.empty());
  if (!executor_addresses.empty()) {
    blacklisted_executor_addresses_.insert(
        executor_addresses.begin(), executor_addresses.end());
  }
}

Status ClientRequestState::Exec() {
  MarkActive();

  const TExecRequest& exec_req = exec_request();
  profile_->AddChild(server_profile_);
  summary_profile_->AddInfoString("Query Type", PrintValue(stmt_type()));
  summary_profile_->AddInfoString("Query Options (set by configuration)",
      DebugQueryOptions(query_ctx_.client_request.query_options));
  summary_profile_->AddInfoString("Query Options (set by configuration and planner)",
      DebugQueryOptions(exec_req.query_options));
  if (!exec_req.tables.empty()) {
    summary_profile_->AddInfoString("Tables Queried", PrintTableList(exec_req.tables));
  }
  if (query_ctx_.__isset.overridden_mt_dop_value) {
    DCHECK(query_ctx_.client_request.query_options.__isset.mt_dop);
    summary_profile_->AddInfoString("MT_DOP limited by admission control",
        Substitute("Requested MT_DOP=$0 reduced to MT_DOP=$1",
            query_ctx_.overridden_mt_dop_value,
            query_ctx_.client_request.query_options.mt_dop));
  }

  switch (exec_req.stmt_type) {
    case TStmtType::QUERY:
    case TStmtType::DML:
      DCHECK(exec_req.__isset.query_exec_request);
      RETURN_IF_ERROR(
          ExecQueryOrDmlRequest(exec_req.query_exec_request, true /*async*/));
      break;
    case TStmtType::EXPLAIN: {
      request_result_set_.reset(new vector<TResultRow>(
          exec_req.explain_result.results));
      break;
    }
    case TStmtType::TESTCASE: {
      DCHECK(exec_req.__isset.testcase_data_path);
      SetResultSet(vector<string>(1, exec_req.testcase_data_path));
      break;
    }
    case TStmtType::DDL: {
      DCHECK(exec_req.__isset.catalog_op_request);
      LOG_AND_RETURN_IF_ERROR(ExecDdlRequest());
      break;
    }
    case TStmtType::LOAD: {
      DCHECK(exec_req.__isset.load_data_request);
      LOG_AND_RETURN_IF_ERROR(ExecLoadDataRequest());
      break;
    }
    case TStmtType::SET: {
      DCHECK(exec_req.__isset.set_query_option_request);
      lock_guard<mutex> l(session_->lock);
      if (exec_req.set_query_option_request.__isset.key) {
        // "SET key=value" updates the session query options.
        DCHECK(exec_req.set_query_option_request.__isset.value);
        const auto& key = exec_req.set_query_option_request.key;
        const auto& value = exec_req.set_query_option_request.value;
        RETURN_IF_ERROR(SetQueryOption(key, value, &session_->set_query_options,
              &session_->set_query_options_mask));
        SetResultSet({}, {}, {});
        if (iequals(key, "idle_session_timeout")) {
          // IMPALA-2248: Session timeout is set as a query option
          session_->last_accessed_ms = UnixMillis(); // do not expire session immediately
          session_->UpdateTimeout();
          VLOG_QUERY << "ClientRequestState::Exec() SET: idle_session_timeout="
                     << PrettyPrinter::Print(session_->session_timeout, TUnit::TIME_S);
        }
      } else if (exec_req.set_query_option_request.__isset.query_option_type
          && exec_req.set_query_option_request.query_option_type
              == TQueryOptionType::UNSET_ALL) {
        // "UNSET ALL"
        RETURN_IF_ERROR(ResetAllQueryOptions(
            &session_->set_query_options, &session_->set_query_options_mask));
        SetResultSet({}, {}, {});
      } else {
        // "SET" or "SET ALL"
        bool is_set_all =
            exec_req.set_query_option_request.__isset.query_option_type
            && exec_req.set_query_option_request.query_option_type
                == TQueryOptionType::SET_ALL;
        PopulateResultForSet(is_set_all);
      }
      break;
    }
    case TStmtType::ADMIN_FN:
      DCHECK(exec_req.admin_request.type == TAdminRequestType::SHUTDOWN);
      RETURN_IF_ERROR(ExecShutdownRequest());
      break;
    case TStmtType::CONVERT:
      DCHECK(exec_req.__isset.convert_table_request);
      LOG_AND_RETURN_IF_ERROR(ExecMigrateRequest());
      break;
    case TStmtType::UNKNOWN:
      DCHECK(false);
      return Status("Exec request uninitialized during execution");
    default:
      return Status(Substitute("Unknown exec request stmt type: $0", exec_req.stmt_type));
  }

  if (async_exec_thread_.get() == nullptr) {
    UpdateNonErrorExecState(ExecState::RUNNING);
  }
  return Status::OK();
}

void ClientRequestState::PopulateResultForSet(bool is_set_all) {
  map<string, string> config;
  TQueryOptionsToMap(query_options(), &config);
  vector<string> keys, values, levels;
  map<string, string>::const_iterator itr = config.begin();
  for (; itr != config.end(); ++itr) {
    const auto opt_level_id =
        parent_server_->query_option_levels_[itr->first];
    if (!is_set_all && (opt_level_id == TQueryOptionLevel::DEVELOPMENT ||
                        opt_level_id == TQueryOptionLevel::DEPRECATED ||
                        opt_level_id == TQueryOptionLevel::REMOVED)) {
      continue;
    }
    keys.push_back(itr->first);
    values.push_back(itr->second);
    const auto opt_level = _TQueryOptionLevel_VALUES_TO_NAMES.find(opt_level_id);
    DCHECK(opt_level !=_TQueryOptionLevel_VALUES_TO_NAMES.end());
    levels.push_back(opt_level->second);
  }
  SetResultSet(keys, values, levels);
}

Status ClientRequestState::ExecLocalCatalogOp(
    const TCatalogOpRequest& catalog_op) {
  switch (catalog_op.op_type) {
    case TCatalogOpType::USE: {
      lock_guard<mutex> l(session_->lock);
      session_->database = exec_request().catalog_op_request.use_db_params.db;
      return Status::OK();
    }
    case TCatalogOpType::SHOW_TABLES:
    case TCatalogOpType::SHOW_VIEWS: {
      const TShowTablesParams* params = &catalog_op.show_tables_params;
      // A NULL pattern means match all tables of the specified table types. However,
      // Thrift string types can't be NULL in C++, so we have to test if it's set rather
      // than just blindly using the value.
      const string* table_name_pattern =
          params->__isset.show_pattern ? &(params->show_pattern) : nullptr;
      TGetTablesResult table_names;
      const set<TImpalaTableType::type>& table_types = params->table_types;
      RETURN_IF_ERROR(frontend_->GetTableNames(params->db, table_name_pattern,
          &query_ctx_.session, table_types, &table_names));
      SetResultSet(table_names.tables);
      return Status::OK();
    }
    case TCatalogOpType::SHOW_METADATA_TABLES: {
      const TShowTablesParams* params = &catalog_op.show_tables_params;
      // A NULL pattern means match all tables of the specified table types. However,
      // Thrift string types can't be NULL in C++, so we have to test if it's set rather
      // than just blindly using the value.
      const string* metadata_table_name_pattern =
          params->__isset.show_pattern ? &(params->show_pattern) : nullptr;
      DCHECK(params->__isset.tbl);
      const string& table_name = params->tbl;
      TGetTablesResult table_names;
      RETURN_IF_ERROR(frontend_->GetMetadataTableNames(params->db, table_name,
          metadata_table_name_pattern, &query_ctx_.session, &table_names));
      SetResultSet(table_names.tables);
      return Status::OK();
    }
    case TCatalogOpType::SHOW_DBS: {
      const TShowDbsParams* params = &catalog_op.show_dbs_params;
      TGetDbsResult dbs;
      const string* db_pattern =
          params->__isset.show_pattern ? (&params->show_pattern) : NULL;
      RETURN_IF_ERROR(
          frontend_->GetDbs(db_pattern, &query_ctx_.session, &dbs));
      vector<string> names, comments;
      names.reserve(dbs.dbs.size());
      comments.reserve(dbs.dbs.size());
      for (const TDatabase& db: dbs.dbs) {
        names.push_back(db.db_name);
        comments.push_back(db.metastore_db.description);
      }
      SetResultSet(names, comments);
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
      SetResultSet(functions.fn_ret_types, functions.fn_signatures,
          functions.fn_binary_types, functions.fn_persistence);
      return Status::OK();
    }
    case TCatalogOpType::SHOW_ROLES: {
      const TShowRolesParams& params = catalog_op.show_roles_params;
      // If we have made it here, the user has privileges to execute this operation.
      // Return the results.
      TShowRolesResult result;
      RETURN_IF_ERROR(frontend_->ShowRoles(params, &result));
      SetResultSet(result.role_names);
      return Status::OK();
    }
    case TCatalogOpType::SHOW_GRANT_PRINCIPAL: {
      const TShowGrantPrincipalParams& params = catalog_op.show_grant_principal_params;
      TResultSet response;
      RETURN_IF_ERROR(frontend_->GetPrincipalPrivileges(params, &response));
      // Set the result set and its schema from the response.
      request_result_set_.reset(new vector<TResultRow>(response.rows));
      result_metadata_ = response.schema;
      return Status::OK();
    }
    case TCatalogOpType::DESCRIBE_HISTORY: {
      // This operation is supported for Iceberg tables only.
      const TDescribeHistoryParams& params = catalog_op.describe_history_params;
      TGetTableHistoryResult result;
      RETURN_IF_ERROR(frontend_->GetTableHistory(params, &result));

      request_result_set_.reset(new vector<TResultRow>);
      request_result_set_->resize(result.result.size());
      for (int i = 0; i < result.result.size(); ++i) {
        const TGetTableHistoryResultItem item = result.result[i];
        TResultRow &result_row = (*request_result_set_.get())[i];
        result_row.__isset.colVals = true;
        result_row.colVals.resize(4);
        const Timezone* local_tz = TimezoneDatabase::FindTimezone(
            query_options().timezone);
        TimestampValue tv = TimestampValue::FromUnixTimeMicros(
            item.creation_time * 1000, local_tz);
        result_row.colVals[0].__set_string_val(tv.ToString());
        result_row.colVals[1].__set_string_val(std::to_string(item.snapshot_id));
        result_row.colVals[2].__set_string_val(
            (item.__isset.parent_id) ? std::to_string(item.parent_id) : "NULL");
        result_row.colVals[3].__set_string_val(
            (item.is_current_ancestor) ? "TRUE" : "FALSE");
      }
      return Status::OK();
    }
    case TCatalogOpType::DESCRIBE_DB: {
      TDescribeResult response;
      RETURN_IF_ERROR(frontend_->DescribeDb(catalog_op.describe_db_params,
          &response));
      // Set the result set
      request_result_set_.reset(new vector<TResultRow>(response.results));
      return Status::OK();
    }
    case TCatalogOpType::DESCRIBE_TABLE: {
      TDescribeResult response;
      const TDescribeTableParams& params = catalog_op.describe_table_params;
      RETURN_IF_ERROR(frontend_->DescribeTable(params, query_ctx_.session, &response));
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

Status ClientRequestState::ExecQueryOrDmlRequest(
    const TQueryExecRequest& query_exec_request, bool isAsync) {
  // we always need at least one plan fragment
  DCHECK(query_exec_request.plan_exec_info.size() > 0);

  if (query_exec_request.__isset.query_plan) {
    stringstream plan_ss;
    // Add some delimiters to make it clearer where the plan
    // begins and the profile ends
    plan_ss << "\n----------------\n"
            << query_exec_request.query_plan
            << "----------------";
    summary_profile_->AddInfoStringRedacted("Plan", plan_ss.str());
  }
  // Add info strings consumed by CM: Estimated mem and tables missing stats.
  if (query_exec_request.__isset.per_host_mem_estimate) {
    stringstream ss;
    ss << query_exec_request.per_host_mem_estimate;
    summary_profile_->AddInfoString(PER_HOST_MEM_KEY, ss.str());
  }
  if (!query_exec_request.query_ctx.__isset.parent_query_id &&
      query_exec_request.query_ctx.__isset.tables_missing_stats &&
      !query_exec_request.query_ctx.tables_missing_stats.empty()) {
    summary_profile_->AddInfoString(TABLES_MISSING_STATS_KEY,
        PrintTableList(query_exec_request.query_ctx.tables_missing_stats));
  }

  if (!query_exec_request.query_ctx.__isset.parent_query_id &&
      query_exec_request.query_ctx.__isset.tables_with_corrupt_stats &&
      !query_exec_request.query_ctx.tables_with_corrupt_stats.empty()) {
    summary_profile_->AddInfoString(TABLES_WITH_CORRUPT_STATS_KEY,
        PrintTableList(query_exec_request.query_ctx.tables_with_corrupt_stats));
  }

  if (query_exec_request.query_ctx.__isset.tables_missing_diskids &&
      !query_exec_request.query_ctx.tables_missing_diskids.empty()) {
    summary_profile_->AddInfoString(TABLES_WITH_MISSING_DISK_IDS_KEY,
        PrintTableList(query_exec_request.query_ctx.tables_missing_diskids));
  }

  {
    lock_guard<mutex> l(lock_);
    // Don't start executing the query if Cancel() was called concurrently with Exec().
    if (is_cancelled_) return Status::CANCELLED;
  }
  if (isAsync) {
    // Don't transition to PENDING inside the FinishExecQueryOrDmlRequest thread because
    // the query should be in the PENDING state before the Exec RPC returns.
    UpdateNonErrorExecState(ExecState::PENDING);
    RETURN_IF_ERROR(Thread::Create("query-exec-state", "async-exec-thread",
        &ClientRequestState::FinishExecQueryOrDmlRequest, this, &async_exec_thread_,
        true));
  } else {
    // Update query_status_ as necessary.
    FinishExecQueryOrDmlRequest();
    return query_status_;
  }
  return Status::OK();
}

void ClientRequestState::FinishExecQueryOrDmlRequest() {
  const TExecRequest& exec_req = exec_request();
  DCHECK(exec_req.__isset.query_exec_request);
  UniqueIdPB query_id_pb;
  TUniqueIdToUniqueIdPB(query_id(), &query_id_pb);
  Status admit_status = admission_control_client_->SubmitForAdmission(
      {query_id_pb, ExecEnv::GetInstance()->backend_id(),
          exec_req.query_exec_request, exec_req.query_options,
          summary_profile_, blacklisted_executor_addresses_},
      query_events_, &schedule_, &wait_start_time_ms_, &wait_end_time_ms_);
  {
    lock_guard<mutex> l(lock_);
    if (!UpdateQueryStatus(admit_status).ok()) return;
  }
  DCHECK(schedule_.get() != nullptr);
  // Note that we don't need to check for cancellation between admission and query
  // startup. The query was not cancelled right before being admitted and the window here
  // is small enough to not require special handling. Instead we start the query and then
  // cancel it through the check below if necessary.
  DebugActionNoFail(exec_req.query_options, "CRS_BEFORE_COORD_STARTS");
  // Register the query with the server to support cancellation. This happens after
  // admission because now the set of executors is fixed and an executor failure will
  // cause a query failure.
  parent_server_->RegisterQueryLocations(schedule_->backend_exec_params(), query_id());
  coord_.reset(new Coordinator(this, exec_req, *schedule_.get(), query_events_));
  Status exec_status = coord_->Exec();

  DebugActionNoFail(exec_req.query_options, "CRS_AFTER_COORD_STARTS");

  // Make coordinator profile visible, even upon failure.
  if (coord_->query_profile() != nullptr) profile_->AddChild(coord_->query_profile());

  bool cancelled = false;
  Status cancellation_status;
  {
    lock_guard<mutex> l(lock_);
    if (!UpdateQueryStatus(exec_status).ok()) return;
    // Coordinator::Exec() finished successfully - it is safe to concurrently access
    // 'coord_'. This thread needs to cancel the coordinator if cancellation occurred
    // *before* 'coord_' was accessible to other threads. Once the lock is dropped, any
    // future calls to Cancel() are responsible for calling Coordinator::Cancel(), so
    // while holding the lock we need to both perform a check for cancellation and make
    // the coord_ visible.
    coord_exec_called_.Store(true);
    cancelled = is_cancelled_;
    if (cancelled) {
      VLOG_QUERY << "Cancelled right after starting the coordinator query id="
                 << PrintId(query_id());
      discard_result(UpdateQueryStatus(Status::CANCELLED));
    }
  }

  if (cancelled) {
    coord_->Cancel();
    return;
  }
  UpdateNonErrorExecState(ExecState::RUNNING);
}

Status ClientRequestState::ExecDdlRequestImplSync() {

  if (catalog_op_type() != TCatalogOpType::DDL &&
      catalog_op_type() != TCatalogOpType::RESET_METADATA) {
    Status status = ExecLocalCatalogOp(exec_request().catalog_op_request);
    lock_guard<mutex> l(lock_);
    return UpdateQueryStatus(status);
  }

  if (ddl_type() == TDdlType::COMPUTE_STATS) {
    const TComputeStatsParams& compute_stats_params =
        exec_request().catalog_op_request.ddl_params.compute_stats_params;
    RuntimeProfile* child_profile =
        RuntimeProfile::Create(&profile_pool_, "Child Queries");
    profile_->AddChild(child_profile);
    // Add child queries for computing table and column stats.
    vector<ChildQuery> child_queries;
    if (compute_stats_params.__isset.tbl_stats_query) {
      RuntimeProfile* profile =
          RuntimeProfile::Create(&profile_pool_, "Table Stats Query");
      child_profile->AddChild(profile);
      child_queries.emplace_back(compute_stats_params.tbl_stats_query, this,
          parent_server_, profile, &profile_pool_);
    }
    if (compute_stats_params.__isset.col_stats_query) {
      RuntimeProfile* profile =
          RuntimeProfile::Create(&profile_pool_, "Column Stats Query");
      child_profile->AddChild(profile);
      child_queries.emplace_back(compute_stats_params.col_stats_query, this,
          parent_server_, profile, &profile_pool_);
    }

    if (child_queries.size() > 0) {
      RETURN_IF_ERROR(child_query_executor_->ExecAsync(move(child_queries)));
    } else {
      SetResultSet({"No partitions selected for incremental stats update."});
    }
    return Status::OK();
  }

  DCHECK(false) << "Not handled sync exec ddl request.";
  return Status::OK();
}

void ClientRequestState::ExecDdlRequestImpl(bool exec_in_worker_thread) {
  bool is_CTAS = (catalog_op_type() == TCatalogOpType::DDL
      && ddl_type() == TDdlType::CREATE_TABLE_AS_SELECT);
  const TExecRequest& exec_req = exec_request();

  catalog_op_executor_.reset(
      new CatalogOpExecutor(ExecEnv::GetInstance(), frontend_, server_profile_));

  // Indirectly check if running in thread async_exec_thread_.
  if (exec_in_worker_thread) {
    VLOG_QUERY << "Running in worker thread";
    DCHECK(exec_state() == ExecState::PENDING);

    // 1. For any non-CTAS DDLs, transition to RUNNING
    // 2. For CTAS DDLs, transition to RUNNING during FinishExecQueryOrDmlRequest()
    //    called by ExecQueryOrDmlRequest().
    if (!is_CTAS) UpdateNonErrorExecState(ExecState::RUNNING);
  }

  // Optionally wait with a debug action before Exec() below.
  DebugActionNoFail(exec_req.query_options, "CRS_DELAY_BEFORE_CATALOG_OP_EXEC");

  Status status = catalog_op_executor_->Exec(exec_req.catalog_op_request);
  query_events_->MarkEvent("CatalogDdlRequest finished");
  AddCatalogTimeline();
  {
    lock_guard<mutex> l(lock_);
    RETURN_VOID_IF_ERROR(UpdateQueryStatus(status, exec_in_worker_thread));
  }

  // If this is a CTAS request, there will usually be more work to do
  // after executing the CREATE TABLE statement (the INSERT portion of the operation).
  // The exception is if the user specified IF NOT EXISTS and the table already
  // existed, in which case we do not execute the INSERT.
  if (catalog_op_type() == TCatalogOpType::DDL &&
      ddl_type() == TDdlType::CREATE_TABLE_AS_SELECT &&
      !catalog_op_executor_->ddl_exec_response()->new_table_created) {
    DCHECK(exec_req.catalog_op_request.
        ddl_params.create_table_params.if_not_exists);
    return;
  }

  // Add newly created table to catalog cache.
  status = parent_server_->ProcessCatalogUpdateResult(
      *catalog_op_executor_->update_catalog_result(),
      exec_req.query_options.sync_ddl, query_options(), query_events_);
  {
    lock_guard<mutex> l(lock_);
    RETURN_VOID_IF_ERROR(UpdateQueryStatus(status, exec_in_worker_thread));
  }

  if (is_CTAS) {
    // At this point, the remainder of the CTAS request executes
    // like a normal DML request. As with other DML requests, it will
    // wait for another catalog update if any partitions were altered as a result
    // of the operation.
    DCHECK(exec_req.__isset.query_exec_request);
    RETURN_VOID_IF_ERROR(
        ExecQueryOrDmlRequest(exec_req.query_exec_request, !exec_in_worker_thread));
  }

  // Set the results to be reported to the client. Do this under lock to avoid races
  // with ImpalaServer::GetResultSetMetadata().
  {
    lock_guard<mutex> l(lock_);
    SetResultSet(catalog_op_executor_->ddl_exec_response());
  }
}

bool ClientRequestState::ShouldRunExecDdlAsync() {
  // Local catalog op DDL will run synchronously.
  if (catalog_op_type() != TCatalogOpType::DDL
      && catalog_op_type() != TCatalogOpType::RESET_METADATA) {
    return false;
  }

  // The exec DDL part of compute stats will run synchronously.
  if (ddl_type() == TDdlType::COMPUTE_STATS) return false;

  return true;
}

Status ClientRequestState::ExecDdlRequest() {
  string op_type = catalog_op_type() == TCatalogOpType::DDL ?
      PrintValue(ddl_type()) : PrintValue(catalog_op_type());
  bool async_ddl = ShouldRunExecDdlAsync();
  bool async_ddl_enabled = exec_request().query_options.enable_async_ddl_execution;
  string exec_mode = (async_ddl && async_ddl_enabled) ? "asynchronous" : "synchronous";

  summary_profile_->AddInfoString("DDL Type", op_type);
  summary_profile_->AddInfoString("DDL execution mode", exec_mode);
  VLOG_QUERY << "DDL exec mode=" << exec_mode;

  if (!async_ddl) return ExecDdlRequestImplSync();

  if (async_ddl_enabled) {
    // Transition the exec state out of INITIALIZED to PENDING to make available the
    // runtime profile for the DDL. Later on in ExecDdlRequestImpl(), the state
    // further transitions to RUNNING.
    UpdateNonErrorExecState(ExecState::PENDING);
    return Thread::Create("impala-server", "async_exec_thread_",
        &ClientRequestState::ExecDdlRequestImpl, this, true /*exec in a worker thread*/,
        &async_exec_thread_);
  } else {
    ExecDdlRequestImpl(false /*exec in the same thread as the caller*/);
    return query_status_;
  }
}

void ClientRequestState::ExecLoadDataRequestImpl(bool exec_in_worker_thread) {
  const TExecRequest& exec_req = exec_request();
  if (exec_in_worker_thread) {
    VLOG_QUERY << "Running in worker thread";
    DCHECK(exec_state() == ExecState::PENDING);
    UpdateNonErrorExecState(ExecState::RUNNING);
  }
  DebugActionNoFail(
      exec_req.query_options, "CRS_DELAY_BEFORE_LOAD_DATA");

  TLoadDataResp response;
  Status status = frontend_->LoadData(exec_req.load_data_request, &response);
  if (exec_req.load_data_request.iceberg_tbl) {
    ExecLoadIcebergDataRequestImpl(response);
  }
  {
    lock_guard<mutex> l(lock_);
    RETURN_VOID_IF_ERROR(UpdateQueryStatus(status, exec_in_worker_thread));
  }

  request_result_set_.reset(new vector<TResultRow>);
  request_result_set_->push_back(response.load_summary);

  // We use TUpdateCatalogRequest to refresh the table metadata so that it will
  // fire an insert event just like an insert statement.
  TUpdatedPartition updatedPartition;
  updatedPartition.files.insert(updatedPartition.files.end(),
    response.loaded_files.begin(), response.loaded_files.end());
  TUpdateCatalogRequest catalog_update;
  // The partition_name is an empty string for unpartitioned tables.
  catalog_update.updated_partitions[response.partition_name] = updatedPartition;

  catalog_update.__set_sync_ddl(exec_req.query_options.sync_ddl);
  catalog_update.__set_header(GetCatalogServiceRequestHeader());
  catalog_update.target_table = exec_req.load_data_request.table_name.table_name;
  catalog_update.db_name = exec_req.load_data_request.table_name.db_name;
  catalog_update.is_overwrite = exec_req.load_data_request.overwrite;

  CatalogServiceConnection client(ExecEnv::GetInstance()->catalogd_client_cache(),
      *ExecEnv::GetInstance()->GetCatalogdAddress().get(), &status);
  {
    lock_guard<mutex> l(lock_);
    RETURN_VOID_IF_ERROR(UpdateQueryStatus(status, exec_in_worker_thread));
  }

  TUpdateCatalogResponse resp;
  status = client.DoRpc(
      &CatalogServiceClientWrapper::UpdateCatalog, catalog_update, &resp);
  query_events_->MarkEvent("UpdateCatalog finished");
  if (resp.__isset.profile) {
    for (const TEventSequence& catalog_timeline : resp.profile.event_sequences) {
      summary_profile_->AddEventSequence(catalog_timeline.name, catalog_timeline);
    }
  }
  {
    lock_guard<mutex> l(lock_);
    RETURN_VOID_IF_ERROR(UpdateQueryStatus(status, exec_in_worker_thread));
  }

  status = parent_server_->ProcessCatalogUpdateResult(
      resp.result,
      exec_req.query_options.sync_ddl, query_options(), query_events_);
  {
    lock_guard<mutex> l(lock_);
    RETURN_VOID_IF_ERROR(UpdateQueryStatus(status, exec_in_worker_thread));
  }
}

void ClientRequestState::ExecLoadIcebergDataRequestImpl(TLoadDataResp response) {
  TLoadDataReq load_data_req = exec_request().load_data_request;
  RuntimeProfile* child_profile =
      RuntimeProfile::Create(&profile_pool_, "Child Queries");
  profile_->AddChild(child_profile);
  // Add child queries for computing table and column stats.
  vector<ChildQuery> child_queries;
  // Prepare CREATE
  RuntimeProfile* create_profile =
      RuntimeProfile::Create(&profile_pool_, "Create table query");
  child_profile->AddChild(create_profile);
  child_queries.emplace_back(response.create_tmp_tbl_query, this, parent_server_,
      create_profile, &profile_pool_);
  // Prepare INSERT
  RuntimeProfile* insert_profile =
      RuntimeProfile::Create(&profile_pool_, "Insert query");
  child_profile->AddChild(insert_profile);
  child_queries.emplace_back(load_data_req.insert_into_dst_tbl_query, this,
      parent_server_, insert_profile, &profile_pool_);
  // Prepare DROP
  RuntimeProfile* drop_profile =
      RuntimeProfile::Create(&profile_pool_, "Drop table query");
  child_profile->AddChild(drop_profile);
  child_queries.emplace_back(load_data_req.drop_tmp_tbl_query, this,
      parent_server_, drop_profile, &profile_pool_);
  // Execute queries
  RETURN_VOID_IF_ERROR(child_query_executor_->ExecAsync(move(child_queries)));
  vector<ChildQuery*>* completed_queries = new vector<ChildQuery*>();
  Status query_status = child_query_executor_->WaitForAll(completed_queries);
  if (query_status.ok()) {
    const char* path = response.create_location.c_str();
    string delete_err = "Load was succesful, but failed to remove staging data under '"
        + response.create_location + "', HDFS error: ";
    hdfsFS hdfs_conn;
    Status hdfs_ret = HdfsFsCache::instance()->GetConnection(path, &hdfs_conn);
    if (!hdfs_ret.ok()) {
      lock_guard<mutex> l(lock_);
      RETURN_VOID_IF_ERROR(UpdateQueryStatus(Status(delete_err + hdfs_ret.GetDetail())));
    }
    if (hdfsDelete(hdfs_conn, path, 1)) {
      lock_guard<mutex> l(lock_);
      RETURN_VOID_IF_ERROR(UpdateQueryStatus(Status(delete_err + strerror(errno))));
    }
  } else {
    const char* dst_path = load_data_req.source_path.c_str();
    hdfsFS hdfs_dst_conn;
    string revert_err = "Failed to load data and failed to revert data movement, "
        "please check source and staging directory under '" + response.create_location
        + "', Query error: " + query_status.GetDetail() + " HDFS error: ";
    Status hdfs_ret = HdfsFsCache::instance()->GetConnection(dst_path, &hdfs_dst_conn);
    if (!hdfs_ret.ok()) {
      lock_guard<mutex> l(lock_);
      RETURN_VOID_IF_ERROR(UpdateQueryStatus(Status(revert_err + hdfs_ret.GetDetail())));
    }
    for (const string& src_path : response.loaded_files) {
      hdfsFS hdfs_src_conn;
      hdfs_ret = HdfsFsCache::instance()->GetConnection(dst_path, &hdfs_src_conn);
      if (!hdfs_ret.ok()) {
        lock_guard<mutex> l(lock_);
        RETURN_VOID_IF_ERROR(UpdateQueryStatus(Status(revert_err
            + hdfs_ret.GetDetail())));
      }
      if (hdfsMove(hdfs_src_conn, src_path.c_str(), hdfs_dst_conn, dst_path)) {
        lock_guard<mutex> l(lock_);
        RETURN_VOID_IF_ERROR(UpdateQueryStatus(Status(revert_err + strerror(errno))));
      }
    }
  }
}


Status ClientRequestState::ExecLoadDataRequest() {
  if (exec_request().query_options.enable_async_load_data_execution) {
    // Transition the exec state out of INITIALIZED to PENDING to make available the
    // runtime profile for the DDL.
    UpdateNonErrorExecState(ExecState::PENDING);
    return Thread::Create("impala-server", "async_exec_thread_",
        &ClientRequestState::ExecLoadDataRequestImpl, this, true, &async_exec_thread_);
  }

  // sync exection
  ExecLoadDataRequestImpl(false /* not use a worker thread */);
  return query_status_;
}

Status ClientRequestState::ExecShutdownRequest() {
  const TShutdownParams& request = exec_request().admin_request.shutdown_params;
  bool backend_port_specified = request.__isset.backend && request.backend.port != 0;
  int port = backend_port_specified ? request.backend.port : FLAGS_krpc_port;
  // Use the local shutdown code path if the host is unspecified or if it exactly matches
  // the configured host/port. This avoids the possibility of RPC errors preventing
  // shutdown.
  if (!request.__isset.backend
      || (request.backend.hostname == FLAGS_hostname && port == FLAGS_krpc_port)) {
    ShutdownStatusPB shutdown_status;
    int64_t deadline_s = request.__isset.deadline_s ? request.deadline_s : -1;
    RETURN_IF_ERROR(parent_server_->StartShutdown(deadline_s, &shutdown_status));
    SetResultSet({ImpalaServer::ShutdownStatusToString(shutdown_status)});
    return Status::OK();
  }

  // KRPC relies on resolved IP address, so convert hostname.
  IpAddr ip_address;
  Status ip_status = HostnameToIpAddr(request.backend.hostname, &ip_address);
  if (!ip_status.ok()) {
    VLOG(1) << "Could not convert hostname " << request.backend.hostname
            << " to ip address, error: " << ip_status.GetDetail();
    return ip_status;
  }
  // Find BackendId for the given remote ip address and port from cluster membership.
  // The searching is not efficient, but Shutdown Requests are not called frequently.
  // The BackendId is used to generate UDS address for Unix domain socket. Leave the
  // Id value as 0 if it's not found in cluster membership.
  // Note that UDS is only used when FLAGS_rpc_use_unix_domain_socket is set as true.
  UniqueIdPB backend_id;
  backend_id.set_hi(0);
  backend_id.set_lo(0);
  if (ExecEnv::GetInstance()->rpc_mgr()->IsKrpcUsingUDS()) {
    if (ExecEnv::GetInstance()->rpc_mgr()->GetUdsAddressUniqueId()
        == UdsAddressUniqueIdPB::BACKEND_ID) {
      ClusterMembershipMgr::SnapshotPtr membership_snapshot =
          ExecEnv::GetInstance()->cluster_membership_mgr()->GetSnapshot();
      DCHECK(membership_snapshot.get() != nullptr);
      for (const auto& it : membership_snapshot->current_backends) {
        // Compare resolved IP addresses and ports.
        if (it.second.ip_address() == ip_address && it.second.address().port() == port) {
          DCHECK(it.second.has_backend_id());
          backend_id = it.second.backend_id();
          break;
        }
      }
    }
  }
  string krpc_error = "RemoteShutdown() RPC failed: Network error";
  string krpc_error2 = "RemoteShutdown() RPC failed: Timed out";
  NetworkAddressPB krpc_addr = MakeNetworkAddressPB(ip_address, port, backend_id,
      ExecEnv::GetInstance()->rpc_mgr()->GetUdsAddressUniqueId());
  std::unique_ptr<ControlServiceProxy> proxy;
  Status get_proxy_status =
      ControlService::GetProxy(krpc_addr, request.backend.hostname, &proxy);
  if (!get_proxy_status.ok()) {
    return Status(
        Substitute("Could not get Proxy to ControlService at $0 with error: $1.",
            NetworkAddressPBToString(krpc_addr), get_proxy_status.msg().msg()));
  }

  RemoteShutdownParamsPB params;
  if (request.__isset.deadline_s) params.set_deadline_s(request.deadline_s);
  RemoteShutdownResultPB resp;
  VLOG_QUERY << "Sending Shutdown RPC to " << NetworkAddressPBToString(krpc_addr);

  const int num_retries = 3;
  const int64_t timeout_ms = 10 * MILLIS_PER_SEC;
  const int64_t backoff_time_ms = 3 * MILLIS_PER_SEC;
  Status rpc_status = RpcMgr::DoRpcWithRetry(proxy, &ControlServiceProxy::RemoteShutdown,
      params, &resp, query_ctx_, "RemoteShutdown() RPC failed", num_retries, timeout_ms,
      backoff_time_ms, "CRS_SHUTDOWN_RPC");

  if (!rpc_status.ok()) {
    const string& msg = rpc_status.msg().msg();
    VLOG_QUERY << "RemoteShutdown query_id= " << PrintId(query_id())
               << " failed to send RPC to " << NetworkAddressPBToString(krpc_addr) << " :"
               << msg;
    string err_string = Substitute(
        "Rpc to $0 failed with error '$1'", NetworkAddressPBToString(krpc_addr), msg);
    // Attempt to detect if the the failure is because of not using a KRPC port.
    if (backend_port_specified &&
           (msg.find(krpc_error) != string::npos ||
            msg.find(krpc_error2) != string::npos)) {
      // Prior to IMPALA-7985 :shutdown() used the backend port.
      err_string.append(" This may be because the port specified is wrong. You may have"
                        " specified the backend (thrift) port which :shutdown() can no"
                        " longer use. Please make sure the correct KRPC port is being"
                        " used, or don't specify any port in the :shutdown() command.");
    }
    return Status(err_string);
  }
  Status shutdown_status(resp.status());
  RETURN_IF_ERROR(shutdown_status);
  SetResultSet({ImpalaServer::ShutdownStatusToString(resp.shutdown_status())});
  return Status::OK();
}

void ClientRequestState::Finalize(const Status* cause) {
  UnRegisterCompletedRPCs();
  Cancel(cause, /*wait_until_finalized=*/true);
  MarkActive();
  // Make sure we join on wait_thread_ before we finish (and especially before this object
  // is destroyed).
  int64_t block_on_wait_time_us = 0;
  BlockOnWait(0, &block_on_wait_time_us);
  DCHECK_EQ(block_on_wait_time_us, 0);

  // Update latest observed Kudu timestamp stored in the session from the coordinator.
  // Needs to take the session_ lock which must not be taken while holding lock_, so this
  // must happen before taking lock_ below.
  Coordinator* coordinator = GetCoordinator();
  if (coordinator != nullptr) {
    // This is safe to access on coord_ after Wait() has been called.
    uint64_t latest_kudu_ts =
        coordinator->dml_exec_state()->GetKuduLatestObservedTimestamp();
    if (latest_kudu_ts > 0) {
      VLOG_RPC << "Updating session (id=" << PrintId(session_id())  << ") with latest "
               << "observed Kudu timestamp: " << latest_kudu_ts;
      lock_guard<mutex> session_lock(session_->lock);
      session_->kudu_latest_observed_ts = std::max<uint64_t>(
          session_->kudu_latest_observed_ts, latest_kudu_ts);
    }
  }

  // If the transaction didn't get committed by this point then we should just abort it.
  if (InTransaction()) {
    AbortTransaction();
  } else if (InKuduTransaction()) {
    AbortKuduTransaction();
  }

  UpdateEndTime();

  {
    unique_lock<mutex> l(lock_);
    // Update result set cache metrics, and update mem limit accounting before tearing
    // down the coordinator.
    ClearResultCache();
  }
  // Wait until the audit events are flushed.
  if (wait_thread_.get() != nullptr) {
    wait_thread_->Join();
    wait_thread_.reset();
  } else {
    // The query failed in the fe even before a wait thread is launched. Synchronously
    // flush log events to audit authorization errors, if any.
    LogQueryEvents();
  }
  DCHECK(wait_thread_.get() == nullptr);

  // Update the timeline here so that all of the above work is captured in the timeline.
  query_events_->MarkEvent("Unregister query");
  UnRegisterRemainingRPCs();
}

Status ClientRequestState::Exec(const TMetadataOpRequest& exec_request) {
  TResultSet metadata_op_result;
  // Like the other Exec(), fill out as much profile information as we're able to.
  summary_profile_->AddInfoString("Query Type", PrintValue(TStmtType::DDL));
  RETURN_IF_ERROR(frontend_->ExecHiveServer2MetadataOp(exec_request,
      &metadata_op_result));
  result_metadata_ = metadata_op_result.schema;
  request_result_set_.reset(new vector<TResultRow>(metadata_op_result.rows));
  UpdateNonErrorExecState(ExecState::RUNNING);
  return Status::OK();
}

Status ClientRequestState::WaitAsync() {
  // TODO: IMPALA-7396: thread creation fault inject is disabled because it is not
  // handled correctly.
  return Thread::Create("query-exec-state", "wait-thread",
      &ClientRequestState::Wait, this, &wait_thread_, false);
}

bool ClientRequestState::BlockOnWait(int64_t timeout_us, int64_t* block_on_wait_time_us) {
  DCHECK_GE(timeout_us, 0);
  unique_lock<mutex> l(lock_);
  *block_on_wait_time_us = 0;
  // Some metadata operations like GET_COLUMNS do not rely on WaitAsync() to launch
  // the wait thread. In such cases this method is expected to be a no-op.
  if (wait_thread_.get() == nullptr) return true;
  while (!is_wait_done_) {
    if (timeout_us == 0) {
      block_on_wait_cv_.Wait(l);
      return true;
    } else {
      MonotonicStopWatch wait_timeout_timer;
      wait_timeout_timer.Start();
      bool notified = block_on_wait_cv_.WaitFor(l, timeout_us);
      if (notified) {
        *block_on_wait_time_us = wait_timeout_timer.ElapsedTime() / NANOS_PER_MICRO;
      }
      return notified;
    }
  }
  return true;
}

void ClientRequestState::Wait() {
  // block until results are ready
  Status status = WaitInternal();
  // Rows are available now (for SELECT statement), so start the 'wait' timer that tracks
  // how long Impala waits for the client to fetch rows. For other statements, track the
  // time until a Close() is received.
  MarkInactive();
  {
    lock_guard<mutex> l(lock_);
    if (returns_result_set()) {
      query_events()->MarkEvent("Rows available");
    } else {
      query_events()->MarkEvent("Request finished");
      UpdateEndTime();
    }
    discard_result(UpdateQueryStatus(status));
  }

  if (status.ok()) {
    if (stmt_type() == TStmtType::DDL) {
      DCHECK(catalog_op_type() != TCatalogOpType::DDL || request_result_set_ != nullptr);
    }
    // It is possible the query already failed at this point and ExecState is ERROR. In
    // this case, the call to UpdateNonErrorExecState(FINISHED) does not change the
    // ExecState.
    UpdateNonErrorExecState(ExecState::FINISHED);
  }
  // UpdateQueryStatus() or UpdateNonErrorExecState() have updated exec_state_.
  DCHECK(exec_state() == ExecState::FINISHED || exec_state() == ExecState::ERROR
      || retry_state() == RetryState::RETRYING || retry_state() == RetryState::RETRIED);
  // Notify all the threads blocked on Wait() to finish and then log the query events,
  // if any.
  {
    unique_lock<mutex> l(lock_);
    is_wait_done_ = true;
  }
  block_on_wait_cv_.NotifyAll();
  LogQueryEvents();
}

Status ClientRequestState::WaitInternal() {
  // Explain requests have already populated the result set. Nothing to do here.
  if (exec_request().stmt_type == TStmtType::EXPLAIN) {
    return Status::OK();
  }

  // Wait until the query has passed through admission control and is either running or
  // cancelled or encountered an error.
  if (async_exec_thread_.get() != nullptr) async_exec_thread_->Join();

  vector<ChildQuery*> child_queries;
  Status child_queries_status = child_query_executor_->WaitForAll(&child_queries);
  {
    lock_guard<mutex> l(lock_);
    RETURN_IF_ERROR(query_status_);
    RETURN_IF_ERROR(UpdateQueryStatus(child_queries_status));
  }
  if (!child_queries.empty()) query_events_->MarkEvent("Child queries finished");

  bool isCTAS = catalog_op_type() == TCatalogOpType::DDL
      && ddl_type() == TDdlType::CREATE_TABLE_AS_SELECT;

  if (GetCoordinator() != NULL) {
    Status status = GetCoordinator()->Wait();
    if (UNLIKELY(!status.ok())) {
      if (InKuduTransaction()) AbortKuduTransaction();
      return status;
    }
    RETURN_IF_ERROR(UpdateCatalog());
  } else {
    // When the coordinator is not available for CTAS that requires a coordinator, check
    // further if the query has been cancelled. If so, return immediately as there will
    // be no query result available (IMPALA-11006).
    if (isCTAS) {
      lock_guard<mutex> l(lock_);
      if (is_cancelled_) return Status::CANCELLED;
    }
  }

  if (catalog_op_type() == TCatalogOpType::DDL &&
      ddl_type() == TDdlType::COMPUTE_STATS && child_queries.size() > 0) {
    RETURN_IF_ERROR(UpdateTableAndColumnStats(child_queries));
  }

  if (!returns_result_set()) {
    // Queries that do not return a result are finished at this point. This includes
    // DML operations.
    eos_.Store(true);
  } else if (isCTAS) {
    SetCreateTableAsSelectResultSet();
  }
  return Status::OK();
}

Status ClientRequestState::FetchRows(const int32_t max_rows,
    QueryResultSet* fetched_rows, int64_t block_on_wait_time_us) {
  // Pause the wait timer, since the client has instructed us to do work on its behalf.
  MarkActive();

  // ImpalaServer::FetchInternal has already taken our lock_
  discard_result(UpdateQueryStatus(
      FetchRowsInternal(max_rows, fetched_rows, block_on_wait_time_us)));

  MarkInactive();
  return query_status_;
}

Status ClientRequestState::RestartFetch() {
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
  eos_.Store(false);
  num_rows_fetched_ = 0;
  return Status::OK();
}

void ClientRequestState::UpdateNonErrorExecState(ExecState new_state) {
  lock_guard<mutex> l(lock_);
  ExecState old_state = exec_state();
  static string error_msg = "Illegal state transition: $0 -> $1, query_id=$2";
  switch (new_state) {
    case ExecState::PENDING:
      DCHECK(old_state == ExecState::INITIALIZED)
          << Substitute(error_msg, ExecStateToString(old_state),
              ExecStateToString(new_state), PrintId(query_id()));
      UpdateExecState(new_state);
      break;
    case ExecState::RUNNING:
      // It is possible for FinishExecQueryOrDmlRequest() to attempt a transition to
      // running, even after the query has been cancelled with an error status (and is
      // thus in the ERROR ExecState). In this case, just ignore the transition attempt.
      if (old_state != ExecState::ERROR) {
        // DDL statements and metadata ops don't use the PENDING state, so a query can
        // transition directly from the INITIALIZED to RUNNING state.
        DCHECK(old_state == ExecState::INITIALIZED || old_state == ExecState::PENDING)
            << Substitute(error_msg, ExecStateToString(old_state),
                ExecStateToString(new_state), PrintId(query_id()));
        UpdateExecState(new_state);
      }
      break;
    case ExecState::FINISHED:
      // Only transition to the FINISHED state if the query has not failed. It is not
      // valid to transition from ERROR to FINISHED, so skip any attempt to do so.
      if (old_state != ExecState::ERROR) {
        // A query can transition from PENDING to FINISHED if it is cancelled by the
        // client.
        DCHECK(old_state == ExecState::PENDING || old_state == ExecState::RUNNING)
            << Substitute(error_msg, ExecStateToString(old_state),
                ExecStateToString(new_state), PrintId(query_id()));
        UpdateExecState(new_state);
      }
      break;
    default:
      DCHECK(false) << "A non-error state expected but got: "
                    << ExecStateToString(new_state);
  }
}

void ClientRequestState::SetOriginalId(const TUniqueId& original_id) {
  // Copy the TUniqueId query_id from the original query.
  original_id_ = make_unique<TUniqueId>(original_id);
  summary_profile_->AddInfoString("Original Query Id", PrintId(*original_id_));
}

void ClientRequestState::MarkAsRetrying(const Status& status) {
  retry_state_.Store(RetryState::RETRYING);
  summary_profile_->AddInfoString(
      RETRY_STATUS_KEY, RetryStateToString(RetryState::RETRYING));

  // Set the query status.
  query_status_ = status;
  summary_profile_->AddInfoStringRedacted(QUERY_STATUS_KEY, query_status_.GetDetail());
  // The Query Status might be overwritten later if the retry fails. "Retry Cause"
  // preserves the original error that triggered the retry.
  summary_profile_->AddInfoStringRedacted("Retry Cause", query_status_.GetDetail());
}

Status ClientRequestState::UpdateQueryStatus(const Status& status, bool log_error) {
  // Preserve the first non-ok status
  if (!status.ok() && query_status_.ok()) {
    UpdateExecState(ExecState::ERROR);
    query_status_ = status;
    summary_profile_->AddInfoStringRedacted(QUERY_STATUS_KEY, query_status_.GetDetail());
    if (log_error) VLOG_QUERY << status.GetDetail();
  }

  return status;
}

Status ClientRequestState::FetchRowsInternal(const int32_t max_rows,
    QueryResultSet* fetched_rows, int64_t block_on_wait_time_us) {
  // Wait() guarantees that we've transitioned at least to FINISHED state (and any
  // state beyond that should have a non-OK query_status_ set).
  DCHECK(exec_state() == ExecState::FINISHED);

  if (eos_.Load()) return Status::OK();

  if (request_result_set_ != NULL) {
    int num_rows = 0;
    const vector<TResultRow>& all_rows = (*(request_result_set_.get()));
    // max_rows <= 0 means no limit
    while ((num_rows < max_rows || max_rows <= 0)
        && num_rows_fetched_ < all_rows.size()) {
      RETURN_IF_ERROR(fetched_rows->AddOneRow(all_rows[num_rows_fetched_]));
      ++num_rows_fetched_;
      ++num_rows;
    }
    eos_.Store(num_rows_fetched_ == all_rows.size());
    return Status::OK();
  }

  Coordinator* coordinator = GetCoordinator();
  if (coordinator == nullptr) {
    return Status("Client tried to fetch rows on a query that produces no results.");
  }

  int32_t num_rows_fetched_from_cache = 0;
  if (result_cache_max_size_ > 0 && result_cache_ != NULL) {
    // Satisfy the fetch from the result cache if possible.
    int cache_fetch_size = (max_rows <= 0) ? result_cache_->size() : max_rows;
    num_rows_fetched_from_cache =
        fetched_rows->AddRows(result_cache_.get(), num_rows_fetched_, cache_fetch_size);
    num_rows_fetched_ += num_rows_fetched_from_cache;
    COUNTER_ADD(num_rows_fetched_from_cache_counter_, num_rows_fetched_from_cache);
    if (num_rows_fetched_from_cache >= max_rows) return Status::OK();
  }

  // Maximum number of rows to be fetched from the coord.
  int32_t max_coord_rows = max_rows;
  if (max_rows > 0) {
    DCHECK_LE(num_rows_fetched_from_cache, max_rows);
    max_coord_rows = max_rows - num_rows_fetched_from_cache;
  }
  {
    SCOPED_TIMER(row_materialization_timer_);
    size_t before = fetched_rows->size();
    bool eos = false;

    // Temporarily release lock so calls to Cancel() are not blocked. fetch_rows_lock_
    // (already held) ensures that we do not call coord_->GetNext() multiple times
    // concurrently.
    // TODO: Simplify this.
    lock_.unlock();
    Status status =
        coordinator->GetNext(fetched_rows, max_coord_rows, &eos, block_on_wait_time_us);
    lock_.lock();

    if (eos) eos_.Store(true);

    int num_fetched = fetched_rows->size() - before;
    DCHECK(max_coord_rows <= 0 || num_fetched <= max_coord_rows) << Substitute(
        "Fetched more rows ($0) than asked for ($1)", num_fetched, max_coord_rows);
    num_rows_fetched_ += num_fetched;
    COUNTER_ADD(num_rows_fetched_counter_, num_fetched);

    RETURN_IF_ERROR(status);
    // Check if query status has changed during GetNext() call
    if (!query_status_.ok()) {
      eos_.Store(true);
      return query_status_;
    }
  }

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
    MemTracker* query_mem_tracker = coordinator->query_mem_tracker();
    // Count the cached rows towards the mem limit.
    if (UNLIKELY(!query_mem_tracker->TryConsume(delta_bytes))) {
      string details("Failed to allocate memory for result cache.");
      return query_mem_tracker->MemLimitExceeded(nullptr, details, delta_bytes);
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

void ClientRequestState::Cancel(const Status* cause, bool wait_until_finalized) {
  {
    lock_guard<mutex> lock(lock_);
    // If the query has reached a terminal state, no need to update the state.
    bool already_done = eos_.Load() || exec_state() == ExecState::ERROR;
    if (!already_done && cause != NULL) {
      DCHECK(!cause->ok());
      discard_result(UpdateQueryStatus(*cause));
      query_events_->MarkEvent("Cancelled");
      DCHECK(exec_state() == ExecState::ERROR
          || retry_state() == RetryState::RETRYING);
    }

    // To avoid recalling RemoteAdmissionControlClient::CancelAdmission() since it will
    // send extra RPC.
    if (!is_cancelled_) {
      admission_control_client_->CancelAdmission();
      is_cancelled_ = true;
    }
  } // Release lock_ before doing cancellation work.

  // Cancel and close child queries before cancelling parent. 'lock_' should not be held
  // because a) ChildQuery::Cancel() calls back into ImpalaServer and b) cancellation
  // involves RPCs and can take quite some time.
  child_query_executor_->Cancel();

  // Ensure the parent query is cancelled if execution has started (if the query was not
  // started, cancellation is handled by the 'async-exec-thread' thread). 'lock_' should
  // not be held because cancellation involves RPCs and can block for a long time.
  if (GetCoordinator() != nullptr) GetCoordinator()->Cancel(wait_until_finalized);
}

Status ClientRequestState::UpdateCatalog() {
  const TExecRequest& exec_req = exec_request();
  if (!exec_req.__isset.query_exec_request ||
      exec_req.query_exec_request.stmt_type != TStmtType::DML) {
    return Status::OK();
  }

  query_events_->MarkEvent("DML data written");
  SCOPED_TIMER(ADD_TIMER(server_profile_, "MetastoreUpdateTimer"));

  const TQueryExecRequest& query_exec_request = exec_req.query_exec_request;
  if (query_exec_request.__isset.finalize_params) {
    const TFinalizeParams& finalize_params = query_exec_request.finalize_params;
    TUpdateCatalogRequest catalog_update;
    catalog_update.__set_sync_ddl(exec_req.query_options.sync_ddl);
    catalog_update.__set_header(GetCatalogServiceRequestHeader());
    if (exec_req.query_options.__isset.debug_action) {
      catalog_update.__set_debug_action(exec_req.query_options.debug_action);
    }
    DmlExecState* dml_exec_state = GetCoordinator()->dml_exec_state();
    if (!dml_exec_state->PrepareCatalogUpdate(&catalog_update)) {
      VLOG_QUERY << "No partitions altered, not updating metastore (query id: "
                 << PrintId(query_id()) << ")";
    } else {
      // TODO: We track partitions written to, not created, which means
      // that we do more work than is necessary, because written-to
      // partitions don't always require a metastore change.
      if (VLOG_IS_ON(1)) {
        vector<string> part_list;
        for (auto it : catalog_update.updated_partitions) part_list.push_back(it.first);
        VLOG_QUERY << "Updating metastore with "
                  << catalog_update.updated_partitions.size()
                  << " altered partitions ("
                  << join (part_list, ", ") << ")";
      }

      catalog_update.target_table = finalize_params.table_name;
      catalog_update.db_name = finalize_params.table_db;
      catalog_update.is_overwrite = finalize_params.is_overwrite;
      if (InTransaction()) {
        catalog_update.__set_transaction_id(finalize_params.transaction_id);
        catalog_update.__set_write_id(finalize_params.write_id);
      }
      if (finalize_params.__isset.iceberg_params) {
        TIcebergOperationParam& cat_ice_op = catalog_update.iceberg_operation;
        catalog_update.__isset.iceberg_operation = true;
        if (!CreateIcebergCatalogOps(finalize_params, &cat_ice_op)) {
          // No change, no need to update catalog.
          return Status::OK();
        }
      }

      Status cnxn_status;
      CatalogServiceConnection client(ExecEnv::GetInstance()->catalogd_client_cache(),
          *ExecEnv::GetInstance()->GetCatalogdAddress().get(), &cnxn_status);
      RETURN_IF_ERROR(cnxn_status);

      VLOG_QUERY << "Executing FinalizeDml() using CatalogService";
      TUpdateCatalogResponse resp;
      Status status = DebugAction(query_options(), "CLIENT_REQUEST_UPDATE_CATALOG");
      if (status.ok()) {
        status = client.DoRpc(
            &CatalogServiceClientWrapper::UpdateCatalog, catalog_update, &resp);
        query_events_->MarkEvent("UpdateCatalog finished");
      }
      if (resp.__isset.profile) {
        for (const TEventSequence& catalog_timeline : resp.profile.event_sequences) {
          string timeline_name = catalog_timeline.name;
          // For CTAS, we already have a timeline for the CreateTable execution.
          // Use another name for the INSERT timeline.
          if (summary_profile_->GetEventSequence(timeline_name) != nullptr) {
            timeline_name += " 2";
          }
          summary_profile_->AddEventSequence(timeline_name, catalog_timeline);
        }
      }
      if (status.ok()) status = Status(resp.result.status);
      if (!status.ok()) {
        if (InTransaction()) AbortTransaction();
        LOG(ERROR) << "ERROR Finalizing DML: " << status.GetDetail();
        return status;
      }
      if (InTransaction()) {
        // UpdateCatalog() succeeded and already committed the transaction for us.
        int64_t txn_id = GetTransactionId();
        if (!frontend_->UnregisterTransaction(txn_id).ok()) {
          LOG(ERROR) << Substitute("Failed to unregister transaction $0", txn_id);
        }
        ClearTransactionState();
        query_events_->MarkEvent("Transaction committed");
      }
      RETURN_IF_ERROR(parent_server_->ProcessCatalogUpdateResult(resp.result,
          exec_req.query_options.sync_ddl, query_options(), query_events_));
    }
  } else if (InKuduTransaction()) {
    // Commit the Kudu transaction. Clear transaction state if it's successful.
    // Otherwise, abort the Kudu transaction and clear transaction state.
    // Note that TQueryExecRequest.finalize_params is not set for inserting rows to Kudu
    // table.
    Status status = CommitKuduTransaction();
    if (UNLIKELY(!status.ok())) {
      AbortKuduTransaction();
      LOG(ERROR) << "ERROR Finalizing DML: " << status.GetDetail();
      return status;
    }
  }
  query_events_->MarkEvent("DML Metastore update finished");
  return Status::OK();
}

bool ClientRequestState::CreateIcebergCatalogOps(
    const TFinalizeParams& finalize_params, TIcebergOperationParam* cat_ice_op) {
  DCHECK(cat_ice_op != nullptr);
  const TIcebergDmlFinalizeParams& ice_finalize_params = finalize_params.iceberg_params;
  DmlExecState* dml_exec_state = GetCoordinator()->dml_exec_state();
  bool update_catalog = true;
  cat_ice_op->__set_operation(ice_finalize_params.operation);
  cat_ice_op->__set_initial_snapshot_id(
      ice_finalize_params.initial_snapshot_id);
  cat_ice_op->__set_spec_id(ice_finalize_params.spec_id);
  if (ice_finalize_params.operation == TIcebergOperation::INSERT) {
    cat_ice_op->__set_iceberg_data_files_fb(
        dml_exec_state->CreateIcebergDataFilesVector());
    cat_ice_op->__set_is_overwrite(finalize_params.is_overwrite);
    if (cat_ice_op->iceberg_data_files_fb.empty()) update_catalog = false;
  } else if (ice_finalize_params.operation == TIcebergOperation::DELETE) {
    cat_ice_op->__set_iceberg_delete_files_fb(
        dml_exec_state->CreateIcebergDeleteFilesVector());
    cat_ice_op->__set_data_files_referenced_by_position_deletes(
        dml_exec_state->DataFilesReferencedByPositionDeletes());
    if (cat_ice_op->iceberg_delete_files_fb.empty()) update_catalog = false;
  } else if (ice_finalize_params.operation == TIcebergOperation::UPDATE) {
    cat_ice_op->__set_iceberg_data_files_fb(
        dml_exec_state->CreateIcebergDataFilesVector());
    cat_ice_op->__set_iceberg_delete_files_fb(
        dml_exec_state->CreateIcebergDeleteFilesVector());
    cat_ice_op->__set_data_files_referenced_by_position_deletes(
        dml_exec_state->DataFilesReferencedByPositionDeletes());
    if (cat_ice_op->iceberg_delete_files_fb.empty()) {
      DCHECK(cat_ice_op->iceberg_data_files_fb.empty());
      update_catalog = false;
    }
  } else if (ice_finalize_params.operation == TIcebergOperation::OPTIMIZE) {
    cat_ice_op->__set_iceberg_data_files_fb(
        dml_exec_state->CreateIcebergDataFilesVector());
  }
  if (!update_catalog) query_events_->MarkEvent("No-op Iceberg DML statement");
  return update_catalog;
}

void ClientRequestState::SetResultSet(const TDdlExecResponse* ddl_resp) {
  if (ddl_resp != NULL && ddl_resp->__isset.result_set) {
    result_metadata_ = ddl_resp->result_set.schema;
    request_result_set_.reset(new vector<TResultRow>(ddl_resp->result_set.rows));
  }
}

void ClientRequestState::SetResultSet(const vector<string>& results) {
  request_result_set_.reset(new vector<TResultRow>);
  request_result_set_->resize(results.size());
  for (int i = 0; i < results.size(); ++i) {
    (*request_result_set_.get())[i].__isset.colVals = true;
    (*request_result_set_.get())[i].colVals.resize(1);
    (*request_result_set_.get())[i].colVals[0].__set_string_val(results[i]);
  }
}

void ClientRequestState::SetResultSet(const vector<string>& col1,
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

void ClientRequestState::SetResultSet(const vector<string>& col1,
    const vector<string>& col2, const vector<string>& col3) {
  DCHECK_EQ(col1.size(), col2.size());
  DCHECK_EQ(col1.size(), col3.size());

  request_result_set_.reset(new vector<TResultRow>);
  request_result_set_->resize(col1.size());
  for (int i = 0; i < col1.size(); ++i) {
    (*request_result_set_.get())[i].__isset.colVals = true;
    (*request_result_set_.get())[i].colVals.resize(3);
    (*request_result_set_.get())[i].colVals[0].__set_string_val(col1[i]);
    (*request_result_set_.get())[i].colVals[1].__set_string_val(col2[i]);
    (*request_result_set_.get())[i].colVals[2].__set_string_val(col3[i]);
  }
}

void ClientRequestState::SetResultSet(const vector<string>& col1,
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

void ClientRequestState::SetCreateTableAsSelectResultSet() {
  DCHECK(ddl_type() == TDdlType::CREATE_TABLE_AS_SELECT);
  int64_t total_num_rows_inserted = 0;
  // There will only be rows inserted in the case a new table was created as part of this
  // operation.
  if (catalog_op_executor_->ddl_exec_response()->new_table_created) {
    DCHECK(GetCoordinator());
    total_num_rows_inserted = GetCoordinator()->dml_exec_state()->GetNumModifiedRows();
  }
  const string& summary_msg = Substitute("Inserted $0 row(s)", total_num_rows_inserted);
  VLOG_QUERY << summary_msg;
  vector<string> results(1, summary_msg);
  SetResultSet(results);
}

void ClientRequestState::MarkInactive() {
  client_wait_sw_.Start();
  lock_guard<mutex> l(expiration_data_lock_);
  last_active_time_ms_ = UnixMillis();
  DCHECK(ref_count_ > 0) << "Invalid MarkInactive()";
  --ref_count_;
}

void ClientRequestState::MarkActive() {
  client_wait_sw_.Stop();
  int64_t elapsed_time = client_wait_sw_.ElapsedTime();
  // If we have reached eos, then the query is already complete,
  // and we should not accumulate more client wait time. This mostly
  // impacts the finalization step, where the client is closing the
  // query and does not need any more rows. Fetching may have already
  // completed prior to this point, so finalization time should not
  // count in that case. If the fetch was incomplete, then the client
  // time should be counted for finalization as well.
  if (!eos()) {
    client_wait_timer_->Set(elapsed_time);
    // The first call is before any MarkInactive() call has run and produces
    // a zero-length sample. Skip this zero-length sample (but not any later
    // zero-length samples).
    if (elapsed_time != 0 || last_client_wait_time_ != 0) {
      int64_t current_wait_time = elapsed_time - last_client_wait_time_;
      client_wait_time_stats_->UpdateCounter(current_wait_time);
    }
    last_client_wait_time_ = elapsed_time;
  }
  lock_guard<mutex> l(expiration_data_lock_);
  last_active_time_ms_ = UnixMillis();
  ++ref_count_;
}

Status ClientRequestState::UpdateTableAndColumnStats(
    const vector<ChildQuery*>& child_queries) {
  DCHECK_GE(child_queries.size(), 1);
  DCHECK_LE(child_queries.size(), 2);
  catalog_op_executor_.reset(
      new CatalogOpExecutor(ExecEnv::GetInstance(), frontend_, server_profile_));

  // If there was no column stats query, pass in empty thrift structures to
  // ExecComputeStats(). Otherwise pass in the column stats result.
  TTableSchema col_stats_schema;
  TRowSet col_stats_data;
  if (child_queries.size() > 1) {
    col_stats_schema = child_queries[1]->result_schema();
    col_stats_data = child_queries[1]->result_data();
  }

  const TExecRequest& exec_req = exec_request();
  Status status = catalog_op_executor_->ExecComputeStats(
      GetCatalogServiceRequestHeader(),
      exec_req.catalog_op_request,
      child_queries[0]->result_schema(),
      child_queries[0]->result_data(),
      col_stats_schema,
      col_stats_data);
  AddCatalogTimeline();
  {
    lock_guard<mutex> l(lock_);
    RETURN_IF_ERROR(UpdateQueryStatus(status));
  }
  RETURN_IF_ERROR(parent_server_->ProcessCatalogUpdateResult(
      *catalog_op_executor_->update_catalog_result(),
      exec_req.query_options.sync_ddl, query_options(), query_events_));

  // Set the results to be reported to the client.
  SetResultSet(catalog_op_executor_->ddl_exec_response());
  query_events_->MarkEvent("Metastore update finished");
  return Status::OK();
}

void ClientRequestState::ClearResultCache() {
  if (result_cache_ == nullptr) return;
  // Update result set cache metrics and mem limit accounting.
  ImpaladMetrics::RESULTSET_CACHE_TOTAL_NUM_ROWS->Increment(-result_cache_->size());
  int64_t total_bytes = result_cache_->ByteSize();
  ImpaladMetrics::RESULTSET_CACHE_TOTAL_BYTES->Increment(-total_bytes);
  Coordinator* coordinator = GetCoordinator();
  if (coordinator != nullptr) {
    DCHECK(coordinator->query_mem_tracker() != nullptr);
    coordinator->query_mem_tracker()->Release(total_bytes);
  }
  result_cache_.reset();
}

void ClientRequestState::UpdateExecState(ExecState exec_state) {
  exec_state_.Store(exec_state);
  summary_profile_->AddInfoString("Query State", PrintValue(BeeswaxQueryState()));
  summary_profile_->AddInfoString("Impala Query State", ExecStateToString(exec_state));
}

TOperationState::type ClientRequestState::TOperationState() const {
  switch (exec_state()) {
    case ExecState::INITIALIZED: return TOperationState::INITIALIZED_STATE;
    case ExecState::PENDING: return TOperationState::PENDING_STATE;
    case ExecState::RUNNING: return TOperationState::RUNNING_STATE;
    case ExecState::FINISHED: return TOperationState::FINISHED_STATE;
    case ExecState::ERROR: return TOperationState::ERROR_STATE;
    default: {
      DCHECK(false) << "Add explicit translation for all used ExecState values";
      return TOperationState::ERROR_STATE;
    }
  }
}

beeswax::QueryState::type ClientRequestState::BeeswaxQueryState() const {
  switch (exec_state()) {
    case ExecState::INITIALIZED: return beeswax::QueryState::CREATED;
    case ExecState::PENDING: return beeswax::QueryState::COMPILED;
    case ExecState::RUNNING: return beeswax::QueryState::RUNNING;
    case ExecState::FINISHED: return beeswax::QueryState::FINISHED;
    case ExecState::ERROR: return beeswax::QueryState::EXCEPTION;
    default: {
      DCHECK(false) << "Add explicit translation for all used ExecState values";
      return beeswax::QueryState::EXCEPTION;
    }
  }
}

// It is safe to use 'coord_' directly for the following two methods since they are safe
// to call concurrently with Coordinator::Exec(). See comments for 'coord_' and
// 'coord_exec_called_' for more details.
Status ClientRequestState::UpdateBackendExecStatus(
    const ReportExecStatusRequestPB& request,
    const TRuntimeProfileForest& thrift_profiles) {
  DCHECK(coord_.get());
  return coord_->UpdateBackendExecStatus(request, thrift_profiles);
}

void ClientRequestState::UpdateFilter(
    const UpdateFilterParamsPB& params, RpcContext* context) {
  DCHECK(coord_.get());
  coord_->UpdateFilter(params, context);
}

bool ClientRequestState::GetDmlStats(TDmlResult* dml_result, Status* query_status) {
  lock_guard<mutex> l(lock_);
  *query_status = query_status_;
  if (!query_status->ok()) return false;
  // Coord may be NULL for a SELECT with LIMIT 0.
  // Note that when IMPALA-87 is fixed (INSERT without FROM clause) we might
  // need to revisit this, since that might lead us to insert a row without a
  // coordinator, depending on how we choose to drive the table sink.
  Coordinator* coord = GetCoordinator();
  if (coord == nullptr) return false;
  coord->dml_exec_state()->ToTDmlResult(dml_result);
  return true;
}

void ClientRequestState::WaitUntilRetried() {
  unique_lock<mutex> l(lock_);
  DCHECK(retry_state() != RetryState::NOT_RETRIED);
  while (retry_state() == RetryState::RETRYING) {
    block_until_retried_cv_.Wait(l);
  }
  DCHECK(retry_state() == RetryState::RETRIED
      || exec_state() == ExecState::ERROR);
}

void ClientRequestState::MarkAsRetried(const TUniqueId& retried_id) {
  DCHECK(retry_state() == RetryState::RETRYING)
      << Substitute("Illegal retry state transition: $0 -> RETRYING, query_id=$2",
          RetryStateToString(retry_state()), PrintId(query_id()));
  retry_state_.Store(RetryState::RETRIED);
  summary_profile_->AddInfoStringRedacted(
      RETRY_STATUS_KEY, RetryStateToString(RetryState::RETRIED));
  summary_profile_->AddInfoString("Retried Query Id", PrintId(retried_id));
  UpdateExecState(ExecState::ERROR);
  block_until_retried_cv_.NotifyOne();
  retried_id_ = make_unique<TUniqueId>(retried_id);
}

const string& ClientRequestState::effective_user() const {
  return GetEffectiveUser(query_ctx_.session);
}

void ClientRequestState::UpdateEndTime() {
  // Update the query's end time only if it isn't set previously.
  if (end_time_us_.CompareAndSwap(0, UnixMicros())) {
    // Certain API clients expect Start Time and End Time to be date-time strings
    // of nanosecond precision, so we explicitly specify the precision here.
    summary_profile_->AddInfoString(
        "End Time", ToStringFromUnixMicros(end_time_us(), TimePrecision::Nanosecond));
    int64_t duration = end_time_us() - start_time_us();
    summary_profile_->AddInfoString("Duration", Substitute("$0 ($1 us)",
        PrettyPrinter::Print(duration, TUnit::TIME_US), duration));
  }
}

int64_t ClientRequestState::GetTransactionId() const {
  DCHECK(InTransaction());
  return exec_request().query_exec_request.finalize_params.transaction_id;
}

bool ClientRequestState::InTransaction() const {
  return exec_request().query_exec_request.finalize_params.__isset.transaction_id &&
      !transaction_closed_;
}

void ClientRequestState::AbortTransaction() {
  DCHECK(InTransaction());
  if (frontend_->AbortTransaction(GetTransactionId()).ok()) {
    query_events_->MarkEvent("Transaction aborted");
  } else {
    VLOG(1) << Substitute("Unable to abort transaction with id: $0", GetTransactionId());
  }
  ClearTransactionState();
}

void ClientRequestState::ClearTransactionState() {
  DCHECK(InTransaction());
  transaction_closed_ = true;
}

bool ClientRequestState::InKuduTransaction() const {
  // If Kudu transaction is opened, TQueryExecRequest.query_ctx.is_kudu_transactional
  // is set as true by Frontend.doCreateExecRequest().
  return (exec_request().query_exec_request.query_ctx.is_kudu_transactional
      && !transaction_closed_);
}

void ClientRequestState::AbortKuduTransaction() {
  DCHECK(InKuduTransaction());
  if (frontend_->AbortKuduTransaction(query_ctx_.query_id).ok()) {
    query_events_->MarkEvent("Kudu transaction aborted");
  } else {
    VLOG(1) << Substitute("Unable to abort Kudu transaction with query-id: $0",
        PrintId(query_ctx_.query_id));
  }
  transaction_closed_ = true;
}

Status ClientRequestState::CommitKuduTransaction() {
  DCHECK(InKuduTransaction());
  // Skip calling Commit() for Kudu Transaction with a debug action so that test code
  // could explicitly control over calling Commit().
  Status status = DebugAction(exec_request().query_options, "CRS_NOT_COMMIT_KUDU_TXN");
  if (UNLIKELY(!status.ok())) {
    VLOG(1) << Substitute("Skip to commit Kudu transaction with query-id: $0",
        PrintId(query_ctx_.query_id));
    transaction_closed_ = true;
    return Status::OK();
  }

  status = frontend_->CommitKuduTransaction(query_ctx_.query_id);
  if (status.ok()) {
    query_events_->MarkEvent("Kudu transaction committed");
    transaction_closed_ = true;
  } else {
    VLOG(1) << Substitute("Unable to commit Kudu transaction with query-id: $0",
        PrintId(query_ctx_.query_id));
  }
  return status;
}

void ClientRequestState::LogQueryEvents() {
  // Wait until the results are available. This guarantees the completion of non QUERY
  // statements like DDL/DML etc. Query events are logged if the query reaches a FINISHED
  // state. For certain query types, events are logged regardless of the query state.
  Status status;
  {
    lock_guard<mutex> l(lock_);
    status = query_status();
  }
  bool log_events = true;
  switch (stmt_type()) {
    case TStmtType::QUERY:
    case TStmtType::DML:
    case TStmtType::DDL:
    case TStmtType::UNKNOWN:
      log_events = status.ok();
      break;
    case TStmtType::EXPLAIN:
    case TStmtType::LOAD:
    case TStmtType::SET:
    case TStmtType::ADMIN_FN:
    default:
      break;
  }

  // Log audit events that are due to an AuthorizationException.
  if (parent_server_->IsAuditEventLoggingEnabled() &&
      (Frontend::IsAuthorizationError(status) || log_events)) {
    // TODO: deal with an error status
    discard_result(LogAuditRecord(status));
  }

  if (log_events && (parent_server_->AreQueryHooksEnabled() ||
      parent_server_->IsLineageLoggingEnabled())) {
    // TODO: deal with an error status
    discard_result(LogLineageRecord());
  }
}

Status ClientRequestState::LogAuditRecord(const Status& query_status) {
  const TExecRequest& request = exec_request();
  stringstream ss;
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

  writer.StartObject();
  // Each log entry is a timestamp mapped to a JSON object
  ss << UnixMillis();
  writer.String(ss.str().c_str());
  writer.StartObject();
  writer.String("query_id");
  writer.String(PrintId(query_id()).c_str());
  writer.String("session_id");
  writer.String(PrintId(session_id()).c_str());
  writer.String("start_time");
  writer.String(ToStringFromUnixMicros(start_time_us()).c_str());
  writer.String("authorization_failure");
  writer.Bool(Frontend::IsAuthorizationError(query_status));
  writer.String("status");
  writer.String(query_status.GetDetail().c_str());
  writer.String("user");
  writer.String(effective_user().c_str());
  writer.String("impersonator");
  if (do_as_user().empty()) {
    // If there is no do_as_user() is empty, the "impersonator" field should be Null.
    writer.Null();
  } else {
    // Otherwise, the delegator is the current connected user.
    writer.String(connected_user().c_str());
  }
  writer.String("statement_type");
  if (request.stmt_type == TStmtType::DDL) {
    if (request.catalog_op_request.op_type == TCatalogOpType::DDL) {
      writer.String(PrintValue(request.catalog_op_request.ddl_params.ddl_type).c_str());
    } else {
      writer.String(PrintValue(request.catalog_op_request.op_type).c_str());
    }
  } else {
    writer.String(PrintValue(request.stmt_type).c_str());
  }
  writer.String("network_address");
  writer.String(TNetworkAddressToString(
      session()->network_address).c_str());
  writer.String("sql_statement");
  string stmt = replace_all_copy(sql_stmt(), "\n", " ");
  Redact(&stmt);
  writer.String(stmt.c_str());
  writer.String("catalog_objects");

  writer.StartArray();
  for (const TAccessEvent& event: request.access_events) {
    writer.StartObject();
    writer.String("name");
    writer.String(event.name.c_str());
    writer.String("object_type");
    writer.String(PrintValue(event.object_type).c_str());
    writer.String("privilege");
    writer.String(event.privilege.c_str());
    writer.EndObject();
  }
  writer.EndArray();
  writer.EndObject();
  writer.EndObject();
  Status status = parent_server_->AppendAuditEntry(buffer.GetString());
  if (!status.ok()) {
    LOG(ERROR) << "Unable to record audit event record: " << status.GetDetail();
    if (FLAGS_abort_on_failed_audit_event) {
      CLEAN_EXIT_WITH_ERROR("Shutting down Impala Server due to "
          "abort_on_failed_audit_event=true");
    }
  }
  return status;
}

Status ClientRequestState::LogLineageRecord() {
  const TExecRequest& request = exec_request();
  if (request.stmt_type == TStmtType::EXPLAIN || (!request.__isset.query_exec_request &&
      !request.__isset.catalog_op_request)) {
    return Status::OK();
  }
  TLineageGraph lineage_graph;
  if (request.__isset.query_exec_request &&
      request.query_exec_request.__isset.lineage_graph) {
    lineage_graph = request.query_exec_request.lineage_graph;
  } else if (request.__isset.catalog_op_request &&
      request.catalog_op_request.__isset.lineage_graph) {
    lineage_graph = request.catalog_op_request.lineage_graph;
  } else {
    return Status::OK();
  }

  if (catalog_op_executor_ != nullptr && catalog_op_type() == TCatalogOpType::DDL) {
    const TDdlExecResponse* response = ddl_exec_response();
    //Set table location in the lineage graph. Currently, this is only set for external
    // tables in frontend.
    if (response->__isset.table_location) {
        lineage_graph.__set_table_location(response->table_location);
    }
    // Update vertices that have -1 table_create_time for a newly created table/view.
    if (response->__isset.table_name &&
        response->__isset.table_create_time) {
      for (auto &vertex: lineage_graph.vertices) {
        if (!vertex.__isset.metadata) continue;
        if (vertex.metadata.table_name == response->table_name &&
            vertex.metadata.table_create_time == -1) {
          vertex.metadata.__set_table_create_time(response->table_create_time);
        }
      }
    }
  }

  // Set the query end time in TLineageGraph. Must use UNIX time directly rather than
  // e.g. converting from end_time() (IMPALA-4440).
  lineage_graph.__set_ended(UnixMillis() / 1000);

  string lineage_record;
  LineageUtil::TLineageToJSON(lineage_graph, &lineage_record);

  if (parent_server_->AreQueryHooksEnabled()) {
    // invoke QueryEventHooks
    TQueryCompleteContext query_complete_context;
    query_complete_context.__set_lineage_string(lineage_record);
    const Status& status = ExecEnv::GetInstance()->frontend()->CallQueryCompleteHooks(
        query_complete_context);

    if (!status.ok()) {
      LOG(ERROR) << "Failed to send query lineage info to FE CallQueryCompleteHooks"
                 << status.GetDetail();
      if (FLAGS_abort_on_failed_lineage_event) {
        CLEAN_EXIT_WITH_ERROR("Shutting down Impala Server due to "
            "abort_on_failed_lineage_event=true");
      }
    }
  }

  // lineage logfile writing is deprecated in favor of the
  // QueryEventHooks (see FE).  this behavior is being retained
  // for now but may be removed in the future.
  if (parent_server_->IsLineageLoggingEnabled()) {
    const Status& status = parent_server_->AppendLineageEntry(lineage_record);
    if (!status.ok()) {
      LOG(ERROR) << "Unable to record query lineage record: " << status.GetDetail();
      if (FLAGS_abort_on_failed_lineage_event) {
        CLEAN_EXIT_WITH_ERROR("Shutting down Impala Server due to "
            "abort_on_failed_lineage_event=true");
      }
    }
    return status;
  }
  return Status::OK();
}

string ClientRequestState::ExecStateToString(ExecState state) {
  static const unordered_map<ClientRequestState::ExecState, const char*>
      exec_state_strings{{ClientRequestState::ExecState::INITIALIZED, "INITIALIZED"},
          {ClientRequestState::ExecState::PENDING, "PENDING"},
          {ClientRequestState::ExecState::RUNNING, "RUNNING"},
          {ClientRequestState::ExecState::FINISHED, "FINISHED"},
          {ClientRequestState::ExecState::ERROR, "ERROR"}};
  return exec_state_strings.at(state);
}

string ClientRequestState::RetryStateToString(RetryState state) {
  static const unordered_map<ClientRequestState::RetryState, const char*>
      retry_state_strings{{ClientRequestState::RetryState::NOT_RETRIED, "NOT_RETRIED"},
          {ClientRequestState::RetryState::RETRYING, "RETRYING"},
          {ClientRequestState::RetryState::RETRIED, "RETRIED"}};
  return retry_state_strings.at(state);
}

TCatalogServiceRequestHeader ClientRequestState::GetCatalogServiceRequestHeader() {
  TCatalogServiceRequestHeader header = TCatalogServiceRequestHeader();
  header.__set_requesting_user(effective_user());
  header.__set_client_ip(session()->network_address.hostname);
  header.__set_want_minimal_response(FLAGS_use_local_catalog);
  header.__set_redacted_sql_stmt(
      query_ctx_.client_request.__isset.redacted_stmt ?
          query_ctx_.client_request.redacted_stmt : query_ctx_.client_request.stmt);
  header.__set_query_id(query_ctx_.query_id);
  header.__set_coordinator_hostname(FLAGS_hostname);
  return header;
}

void ClientRequestState::RegisterRPC() {
  RpcEventHandler::InvocationContext* rpc_context =
      RpcEventHandler::GetThreadRPCContext();
  // The existence of rpc_context means that this is called from an RPC
  if (rpc_context) {
    lock_guard<mutex> l(lock_);
    if (track_rpcs_ && pending_rpcs_.find(rpc_context) == pending_rpcs_.end()) {
      rpc_context->Register();
      pending_rpcs_.insert(rpc_context);
      rpc_count_->Add(1);
    }
  }
}

void ClientRequestState::UnRegisterCompletedRPCs() {
  lock_guard<mutex> l(lock_);
  for (auto iter = pending_rpcs_.begin(); iter != pending_rpcs_.end();) {
    RpcEventHandler::InvocationContext* rpc_context = *iter;
    uint64_t read_ns = 0, write_ns = 0;
    if (rpc_context->UnRegisterCompleted(read_ns, write_ns)) {
      rpc_read_timer_->Add(read_ns);
      rpc_write_timer_->Add(write_ns);
      iter = pending_rpcs_.erase(iter);
    } else {
      ++iter;
    }
  }
}

void ClientRequestState::UnRegisterRemainingRPCs() {
  lock_guard<mutex> l(lock_);
  for (auto rpc_context: pending_rpcs_) {
    rpc_context->UnRegister();
  }
  track_rpcs_ = false;
  pending_rpcs_.clear();
}

void ClientRequestState::CopyRPCs(ClientRequestState& from_request) {
  lock_guard<mutex> l_to(lock_);
  lock_guard<mutex> l_from(from_request.lock_);
  rpc_read_timer_->Add(from_request.rpc_read_timer_->value());
  rpc_write_timer_->Add(from_request.rpc_write_timer_->value());
  rpc_count_->Add(from_request.rpc_count_->value());
  for (auto rpc_context: from_request.pending_rpcs_) {
    rpc_context->Register();
    pending_rpcs_.insert(rpc_context);
  }
}

Status ClientRequestState::ExecMigrateRequest() {
  ExecMigrateRequestImpl();
  SetResultSet({"Table has been migrated."});
  return query_status_;
}

void ClientRequestState::ExecMigrateRequestImpl() {
  // A convert table request holds the query strings for the sub-queries. These are
  // populated by ConvertTableToIcebergStmt in the Frontend during analysis.
  const TConvertTableRequest& params = exec_request().convert_table_request;
  {
    RuntimeProfile* child_profile =
        RuntimeProfile::Create(&profile_pool_, "Child Queries 1");
    profile_->AddChild(child_profile);
    vector<ChildQuery> child_queries;

    // Prepare: SET some table properties for the original table.
    RuntimeProfile* set_hdfs_table_profile = RuntimeProfile::Create(
        &profile_pool_, "Set properties for HDFS table query");
    child_profile->AddChild(set_hdfs_table_profile);
    child_queries.emplace_back(params.set_hdfs_table_properties_query, this,
        parent_server_, set_hdfs_table_profile, &profile_pool_);

    // Prepare: RENAME the HDFS table to a temporary HDFS table.
    RuntimeProfile* rename_hdfs_table_profile = RuntimeProfile::Create(
        &profile_pool_, "Rename HDFS table query");
    child_profile->AddChild(rename_hdfs_table_profile);
    child_queries.emplace_back(params.rename_hdfs_table_to_temporary_query,
        this, parent_server_, rename_hdfs_table_profile, &profile_pool_);

    // Prepare: REFRESH the temporary HDFS table.
    RuntimeProfile* refresh_hdfs_table_profile = RuntimeProfile::Create(
        &profile_pool_, "Refresh temporary HDFS table query");
    child_profile->AddChild(refresh_hdfs_table_profile);
    child_queries.emplace_back(params.refresh_temporary_hdfs_table_query, this,
        parent_server_, refresh_hdfs_table_profile, &profile_pool_);

    // Execute child queries
    unique_ptr<ChildQueryExecutor> query_executor(new ChildQueryExecutor());
    RETURN_VOID_IF_ERROR(query_executor->ExecAsync(move(child_queries)));
    vector<ChildQuery*>* completed_queries = new vector<ChildQuery*>();
    Status query_status = query_executor->WaitForAll(completed_queries);
    if (!query_status.ok()) AddTableResetHints(params, &query_status);
    {
      lock_guard<mutex> l(lock_);
      RETURN_VOID_IF_ERROR(UpdateQueryStatus(query_status));
    }
  }
  // Create an external Iceberg table using the data of the HDFS table.
  Status status = frontend_->Convert(exec_request());
  if (!status.ok()) AddTableResetHints(params, &status);
  {
    lock_guard<mutex> l(lock_);
    RETURN_VOID_IF_ERROR(UpdateQueryStatus(status));
  }
  {
    RuntimeProfile* child_profile =
        RuntimeProfile::Create(&profile_pool_, "Child Queries 2");
    profile_->AddChild(child_profile);
    vector<ChildQuery> child_queries;

    if (params.__isset.create_iceberg_table_query) {
      // Prepare: CREATE the Iceberg table that inherits HDFS table location.
      RuntimeProfile* create_iceberg_table_profile = RuntimeProfile::Create(
          &profile_pool_, "Create Iceberg table query");
      child_profile->AddChild(create_iceberg_table_profile);
      child_queries.emplace_back(params.create_iceberg_table_query, this,
          parent_server_, create_iceberg_table_profile, &profile_pool_);
    } else {
      // Prepare: Invalidate metadata for tables in Hive catalog to guarantee that it is
      // propagated instantly to all coordinators.
      RuntimeProfile* invalidate_metadata_profile = RuntimeProfile::Create(
          &profile_pool_, "Invalidate metadata Iceberg table query");
      child_profile->AddChild(invalidate_metadata_profile);
      child_queries.emplace_back(params.invalidate_metadata_query, this,
          parent_server_, invalidate_metadata_profile, &profile_pool_);
    }

    if (params.__isset.post_create_alter_table_query) {
      // Prepare: ALTER TABLE query after creating the Iceberg table.
      RuntimeProfile* post_create_alter_table_profile = RuntimeProfile::Create(
          &profile_pool_, "ALTER TABLE after create Iceberg table query");
      child_profile->AddChild(post_create_alter_table_profile);
      child_queries.emplace_back(params.post_create_alter_table_query, this,
          parent_server_, post_create_alter_table_profile, &profile_pool_);
    }

    // Prepare: DROP the temporary HDFS table.
    RuntimeProfile* drop_tmp_hdfs_table_profile = RuntimeProfile::Create(
        &profile_pool_, "Drop temporary HDFS table query");
    child_profile->AddChild(drop_tmp_hdfs_table_profile);
    child_queries.emplace_back(params.drop_temporary_hdfs_table_query, this,
        parent_server_, drop_tmp_hdfs_table_profile, &profile_pool_);

    // Execute queries
    unique_ptr<ChildQueryExecutor> query_executor(new ChildQueryExecutor());
    RETURN_VOID_IF_ERROR(query_executor->ExecAsync(move(child_queries)));
    vector<ChildQuery*>* completed_queries = new vector<ChildQuery*>();
    Status query_status = query_executor->WaitForAll(completed_queries);
    {
      lock_guard<mutex> l(lock_);
      RETURN_VOID_IF_ERROR(UpdateQueryStatus(query_status));
    }
  }
}

void ClientRequestState::AddTableResetHints(const TConvertTableRequest& params,
      Status* status) const {
  string table_reset_hint("Your table might have been renamed. To reset the name "
      "try running:\n" + params.reset_table_name_query + ";");
  status->MergeStatus(Status(table_reset_hint));
}

int64_t ClientRequestState::num_rows_fetched_counter() const {
  if (LIKELY(num_rows_fetched_counter_ != nullptr)) {
    return num_rows_fetched_counter_->value();
  }

  return 0;
}

int64_t ClientRequestState::row_materialization_rate() const {
  if (LIKELY(row_materialization_rate_ != nullptr)) {
    return row_materialization_rate_->value();
  }

  return 0;
}

int64_t ClientRequestState::row_materialization_timer() const {
  if (LIKELY(row_materialization_timer_ != nullptr)) {
    return row_materialization_timer_->value();
  }

  return 0;
}

void ClientRequestState::AddCatalogTimeline() {
  if (catalog_op_executor_ != nullptr
      && catalog_op_executor_->catalog_profile() != nullptr) {
    for (const TEventSequence& catalog_timeline :
        catalog_op_executor_->catalog_profile()->event_sequences) {
      summary_profile_->AddEventSequence(catalog_timeline.name, catalog_timeline);
    }
  }
}

}
