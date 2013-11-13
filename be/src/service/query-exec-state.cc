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

#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "service/impala-server.h"
#include "service/frontend.h"
#include "util/debug-util.h"
#include "util/time.h"

#include "gen-cpp/CatalogService.h"
#include "gen-cpp/CatalogService_types.h"

using namespace std;
using namespace boost;
using namespace boost::uuids;
using namespace beeswax;

DECLARE_int32(catalog_service_port);
DECLARE_string(catalog_service_host);

namespace impala {

ImpalaServer::QueryExecState::QueryExecState(
    ExecEnv* exec_env, Frontend* frontend,
    ImpalaServer* server,
    shared_ptr<SessionState> session,
    const TSessionState& query_session_state, const string& sql_stmt)
  : sql_stmt_(sql_stmt),
    last_active_time_(numeric_limits<int64_t>::max()),
    ref_count_(0L),
    exec_env_(exec_env),
    parent_session_(session),
    query_session_state_(query_session_state),
    coord_(NULL),
    profile_(&profile_pool_, "Query"),  // assign name w/ id after planning
    server_profile_(&profile_pool_, "ImpalaServer"),
    summary_profile_(&profile_pool_, "Summary"),
    eos_(false),
    query_state_(beeswax::QueryState::CREATED),
    current_batch_(NULL),
    current_batch_row_(0),
    num_rows_fetched_(0),
    frontend_(frontend),
    parent_server_(server),
    start_time_(TimestampValue::local_time_micros()) {
  row_materialization_timer_ = ADD_TIMER(&server_profile_, "RowMaterializationTimer");
  client_wait_timer_ = ADD_TIMER(&server_profile_, "ClientFetchWaitTimer");
  query_events_ = summary_profile_.AddEventSequence("Query Timeline");
  query_events_->Start();
  profile_.AddChild(&summary_profile_);

  // Creating a random_generator every time is not free, but
  // benchmarks show it to be slightly cheaper than contending for a
  // single generator under a lock (since random_generator is not
  // thread-safe).
  random_generator uuid_generator;
  uuid query_uuid = uuid_generator();
  UUIDToTUniqueId(query_uuid, &query_id_);

  profile_.set_name("Query (id=" + PrintId(query_id()) + ")");
  summary_profile_.AddInfoString("Session ID", PrintId(session_id()));
  summary_profile_.AddInfoString("Session Type", PrintTSessionType(session_type()));
  summary_profile_.AddInfoString("Start Time", start_time().DebugString());
  summary_profile_.AddInfoString("End Time", "");
  summary_profile_.AddInfoString("Query Type", "N/A");
  summary_profile_.AddInfoString("Query State", PrintQueryState(query_state_));
  summary_profile_.AddInfoString("Query Status", "OK");
  summary_profile_.AddInfoString("Impala Version", GetVersionString(/* compact */ true));
  summary_profile_.AddInfoString("User", user());
  summary_profile_.AddInfoString("Network Address",
      lexical_cast<string>(parent_session_->network_address));
  summary_profile_.AddInfoString("Default Db", default_db());
  summary_profile_.AddInfoString("Sql Statement", sql_stmt);
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
      return Status::OK;
    }
    case TStmtType::DDL: {
      string op_type = catalog_op_type() == TCatalogOpType::DDL ?
         PrintTDdlType(ddl_type()) : PrintTCatalogOpType(catalog_op_type());
      summary_profile_.AddInfoString("DDL Type", op_type);

      if (catalog_op_type() != TCatalogOpType::DDL &&
          catalog_op_type() != TCatalogOpType::RESET_METADATA) {
        Status status = ExecLocalCatalogOp(exec_request_.catalog_op_request);
        lock_guard<mutex> l(lock_);
        return UpdateQueryStatus(status);
      }

      catalog_op_executor_.reset(new CatalogOpExecutor());
      Status status = catalog_op_executor_->Exec(exec_request->catalog_op_request);
      {
        lock_guard<mutex> l(lock_);
        RETURN_IF_ERROR(UpdateQueryStatus(status));
      }

      // If this is a CTAS request, there will usually be more work to do
      // after executing the CREATE TABLE statement (the INSERT portion of the operation).
      // The exception is if the user specified IF NOT EXISTS and the table already
      // existed, in which case we do not execute the INSERT.
      if (catalog_op_type() == TCatalogOpType::DDL &&
          ddl_type() == TDdlType::CREATE_TABLE_AS_SELECT) {
        if (catalog_op_executor_->ddl_exec_response()->new_table_created) {
          // At this point, the remainder of the CTAS request executes
          // like a normal DML request. As with other DML requests, it will
          // wait for another catalog update if any partitions were altered as a result
          // of the operation.
          DCHECK(exec_request_.__isset.query_exec_request);
          RETURN_IF_ERROR(ExecQueryOrDmlRequest(exec_request_.query_exec_request));
        } else {
          DCHECK(exec_request_.catalog_op_request.
              ddl_params.create_table_params.if_not_exists);
        }
      } else {
        // CREATE TABLE AS SELECT performs its catalog update once the DML
        // portion of the operation has completed.
        RETURN_IF_ERROR(parent_server_->ProcessCatalogUpdateResult(
            *catalog_op_executor_->update_catalog_result(),
            exec_request_.query_options.synced_ddl));
      }
      return Status::OK;
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
      reset_req.reset_metadata_params.__set_is_refresh(true);
      reset_req.reset_metadata_params.__set_table_name(
          exec_request_.load_data_request.table_name);
      catalog_op_executor_.reset(new CatalogOpExecutor());
      RETURN_IF_ERROR(catalog_op_executor_->Exec(reset_req));
      RETURN_IF_ERROR(parent_server_->ProcessCatalogUpdateResult(
          *catalog_op_executor_->update_catalog_result(),
          exec_request_.query_options.synced_ddl));
      return Status::OK;
    }
    default:
      stringstream errmsg;
      errmsg << "Unknown  exec request stmt type: " << exec_request->stmt_type;
      return Status(errmsg.str());
  }
}

Status ImpalaServer::QueryExecState::ExecLocalCatalogOp(
    const TCatalogOpRequest& catalog_op) {
  switch (catalog_op.op_type) {
    case TCatalogOpType::USE: {
      lock_guard<mutex> l(parent_session_->lock);
      parent_session_->database = exec_request_.catalog_op_request.use_db_params.db;
      return Status::OK;
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
          &query_session_state_, &table_names));
      SetResultSet(table_names.tables);
      return Status::OK;
    }
    case TCatalogOpType::SHOW_DBS: {
      const TShowDbsParams* params = &catalog_op.show_dbs_params;
      TGetDbsResult db_names;
      const string* db_pattern =
          params->__isset.show_pattern ? (&params->show_pattern) : NULL;
      RETURN_IF_ERROR(
          frontend_->GetDbNames(db_pattern, &query_session_state_, &db_names));
      SetResultSet(db_names.dbs);
      return Status::OK;
    }
    case TCatalogOpType::SHOW_STATS: {
      const TShowStatsParams& params = catalog_op.show_stats_params;
      TResultSet response;
      RETURN_IF_ERROR(frontend_->GetStats(params, &response));
      // Set the result set and its schema from the response.
      request_result_set_.reset(new vector<TResultRow>(response.rows));
      result_metadata_ = response.schema;
      return Status::OK;
    }
    case TCatalogOpType::SHOW_FUNCTIONS: {
      const TShowFunctionsParams* params = &catalog_op.show_fns_params;
      TGetFunctionsResult functions;
      const string* fn_pattern =
          params->__isset.show_pattern ? (&params->show_pattern) : NULL;
      RETURN_IF_ERROR(frontend_->GetFunctions(
          params->type, params->db, fn_pattern, &query_session_state_, &functions));
      SetResultSet(functions.fn_signatures);
      return Status::OK;
    }
    case TCatalogOpType::DESCRIBE: {
      TDescribeTableResult response;
      RETURN_IF_ERROR(frontend_->DescribeTable(catalog_op.describe_table_params,
          &response));
      // Set the result set
      request_result_set_.reset(new vector<TResultRow>(response.results));
      return Status::OK;
    }
    case TCatalogOpType::SHOW_CREATE_TABLE: {
      string response;
      RETURN_IF_ERROR(frontend_->ShowCreateTable(catalog_op.show_create_table_params,
          &response));
      SetResultSet(vector<string>(1, response));
      return Status::OK;
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

  // If desc_tbl is not set, query has SELECT with no FROM. In that
  // case, the query can only have a single fragment, and that fragment needs to be
  // executed by the coordinator. This check confirms that.
  // If desc_tbl is set, the query may or may not have a coordinator fragment.
  bool has_coordinator_fragment =
      query_exec_request.fragments[0].partition.type == TPartitionType::UNPARTITIONED;
  DCHECK(has_coordinator_fragment || query_exec_request.__isset.desc_tbl);

  // If the first fragment has a "limit 0" and this is a query, simply set eos_
  // to true and return.
  // TODO: To be compatible with Hive, INSERT OVERWRITE must clear out target
  // tables / static partitions even if no rows are written.
  DCHECK(query_exec_request.fragments[0].__isset.plan);
  if (query_exec_request.fragments[0].plan.nodes[0].limit == 0 &&
      query_exec_request.stmt_type == TStmtType::QUERY) {
    eos_ = true;
    return Status::OK;
  }

  coord_.reset(new Coordinator(exec_env_));
  Status status = coord_->Exec(
      query_id_, query_exec_request, exec_request_.query_options, &output_exprs_);
  {
    lock_guard<mutex> l(lock_);
    RETURN_IF_ERROR(UpdateQueryStatus(status));
  }

  profile_.AddChild(coord_->query_profile());
  return Status::OK;
}

void ImpalaServer::QueryExecState::Done() {
  unique_lock<mutex> l(lock_);
  MarkActive();
  end_time_ = TimestampValue::local_time_micros();
  summary_profile_.AddInfoString("End Time", end_time().DebugString());
  summary_profile_.AddInfoString("Query State", PrintQueryState(query_state_));
  query_events_->MarkEvent("Unregister query");
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
  return Status::OK;
}

Status ImpalaServer::QueryExecState::Wait() {
  if (coord_.get() != NULL) {
    RETURN_IF_ERROR(coord_->Wait());
    RETURN_IF_ERROR(UpdateCatalog());
  }

  if (!returns_result_set()) {
    // Queries that do not return a result are finished at this point. This includes
    // DML operations and a subset of the DDL operations.
    eos_ = true;
  } else {
    if (ddl_type() == TDdlType::CREATE_TABLE_AS_SELECT) {
      SetCreateTableAsSelectResultSet();
    }
  }
  // Rows are available now (for SELECT statement), so start the 'wait' timer that tracks
  // how long Impala waits for the client to fetch rows. For other statements, track the
  // time until a Close() is received.
  MarkInactive();
  return Status::OK;
}

Status ImpalaServer::QueryExecState::FetchRows(const int32_t max_rows,
    QueryResultSet* fetched_rows) {
  // Pause the wait timer, since the client has instructed us to do work on its behalf.
  MarkActive();

  // FetchInternal has already taken our lock_
  UpdateQueryStatus(FetchRowsInternal(max_rows, fetched_rows));

  MarkInactive();
  return query_status_;
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
    summary_profile_.AddInfoString("Query Status", query_status_.GetErrorMsg());
  }

  return status;
}

Status ImpalaServer::QueryExecState::FetchRowsInternal(const int32_t max_rows,
    QueryResultSet* fetched_rows) {
  DCHECK(query_state_ != QueryState::EXCEPTION);

  if (eos_) return Status::OK;

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
    return Status::OK;
  }

  // List of expr values to hold evaluated rows from the query
  vector<void*> result_row;
  result_row.resize(output_exprs_.size());

  // List of scales for floating point values in result_row
  vector<int> scales;
  scales.resize(result_row.size());

  if (coord_ == NULL) {
    // query without FROM clause: we return exactly one row
    query_state_ = QueryState::FINISHED;
    eos_ = true;
    RETURN_IF_ERROR(GetRowValue(NULL, &result_row, &scales));
    return fetched_rows->AddOneRow(result_row, scales);
  }

  // query with a FROM clause
  lock_.unlock();
  Status status = coord_->Wait();
  lock_.lock();
  if (!status.ok()) return status;

  // Check if query_state_ changed during Wait() call
  if (query_state_ == QueryState::EXCEPTION) return query_status_;

  query_state_ = QueryState::FINISHED;  // results will be ready after this call
  // Fetch the next batch if we've returned the current batch entirely
  if (current_batch_ == NULL || current_batch_row_ >= current_batch_->num_rows()) {
    RETURN_IF_ERROR(FetchNextBatch());
  }
  if (current_batch_ == NULL) return Status::OK;

  {
    SCOPED_TIMER(row_materialization_timer_);
    // Convert the available rows, limited by max_rows
    int available = current_batch_->num_rows() - current_batch_row_;
    int fetched_count = available;
    // max_rows <= 0 means no limit
    if (max_rows > 0 && max_rows < available) fetched_count = max_rows;
    for (int i = 0; i < fetched_count; ++i) {
      TupleRow* row = current_batch_->GetRow(current_batch_row_);
      RETURN_IF_ERROR(GetRowValue(row, &result_row, &scales));
      RETURN_IF_ERROR(fetched_rows->AddOneRow(result_row, scales));
      ++num_rows_fetched_;
      ++current_batch_row_;
    }
  }
  return Status::OK;
}

Status ImpalaServer::QueryExecState::GetRowValue(TupleRow* row, vector<void*>* result,
                                                 vector<int>* scales) {
  DCHECK(result->size() >= output_exprs_.size());
  for (int i = 0; i < output_exprs_.size(); ++i) {
    (*result)[i] = output_exprs_[i]->GetValue(row);
    (*scales)[i] = output_exprs_[i]->output_scale();
  }
  return Status::OK;
}

void ImpalaServer::QueryExecState::Cancel(const Status* cause) {
  // If the query is completed, no need to cancel.
  if (eos_) return;
  // we don't want multiple concurrent cancel calls to end up executing
  // Coordinator::Cancel() multiple times
  if (query_state_ == QueryState::EXCEPTION) return;
  if (cause != NULL) UpdateQueryStatus(*cause);
  query_events_->MarkEvent("Cancelled");
  query_state_ = QueryState::EXCEPTION;
  if (coord_.get() != NULL) coord_->Cancel(cause);
}

Status ImpalaServer::QueryExecState::UpdateCatalog() {
  if (!exec_request().__isset.query_exec_request ||
      exec_request().query_exec_request.stmt_type != TStmtType::DML) {
    return Status::OK;
  }

  query_events_->MarkEvent("DML data written");
  SCOPED_TIMER(ADD_TIMER(&server_profile_, "MetastoreUpdateTimer"));

  TQueryExecRequest query_exec_request = exec_request().query_exec_request;
  if (query_exec_request.__isset.finalize_params) {
    const TFinalizeParams& finalize_params = query_exec_request.finalize_params;
    TUpdateCatalogRequest catalog_update;
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

      ThriftClient<CatalogServiceClient> client(FLAGS_catalog_service_host,
          FLAGS_catalog_service_port, NULL, false, ThriftServer::ThreadPool);
      RETURN_IF_ERROR(client.Open());

      VLOG_QUERY << "Executing FinalizeDml() using CatalogService";
      TUpdateCatalogResponse resp;
      client.iface()->UpdateCatalog(resp, catalog_update);

      Status status(resp.result.status);
      if (!status.ok()) LOG(ERROR) << "ERROR Finalizing DML: " << status.GetErrorMsg();
      RETURN_IF_ERROR(status);
      RETURN_IF_ERROR(parent_server_->ProcessCatalogUpdateResult(resp.result,
          exec_request_.query_options.synced_ddl));
    }
  }
  query_events_->MarkEvent("DML Metastore update finished");
  return Status::OK;
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

  // Check if query_state_ changed during GetNext() call
  if (query_state_ == QueryState::EXCEPTION) {
    current_batch_ = NULL;
    return query_status_;
  }

  current_batch_row_ = 0;
  eos_ = current_batch_ == NULL;
  return Status::OK;
}

void ImpalaServer::QueryExecState::SetResultSet(const vector<string>& results) {
  request_result_set_.reset(new vector<TResultRow>);
  request_result_set_->resize(results.size());
  for (int i = 0; i < results.size(); ++i) {
    (*request_result_set_.get())[i].__isset.colVals = true;
    (*request_result_set_.get())[i].colVals.resize(1);
    (*request_result_set_.get())[i].colVals[0].__set_stringVal(results[i]);
  }
}

void ImpalaServer::QueryExecState::SetCreateTableAsSelectResultSet() {
  DCHECK(ddl_type() == TDdlType::CREATE_TABLE_AS_SELECT);
  int total_num_rows_inserted = 0;
  // There will only be rows inserted in the case a new table was created
  // as part of this operation.
  if (catalog_op_executor_->ddl_exec_response()->new_table_created) {
    DCHECK(coord_.get());
    BOOST_FOREACH(const PartitionRowCount::value_type& p,
        coord_->partition_row_counts()) {
      total_num_rows_inserted += p.second;
    }
  }
  stringstream ss;
  ss << "Inserted " << total_num_rows_inserted << " row(s)";
  VLOG_QUERY << ss.str();
  vector<string> results(1, ss.str());
  SetResultSet(results);
}

void ImpalaServer::QueryExecState::MarkInactive() {
  client_wait_sw_.Start();
  lock_guard<mutex> l(expiration_data_lock_);
  last_active_time_ = ms_since_epoch();
  DCHECK(ref_count_ > 0) << "Invalid MarkInactive()";
  --ref_count_;
}

void ImpalaServer::QueryExecState::MarkActive() {
  client_wait_sw_.Stop();
  int64_t elapsed_time = client_wait_sw_.ElapsedTime();
  client_wait_timer_->Set(elapsed_time);
  lock_guard<mutex> l(expiration_data_lock_);
  last_active_time_ = ms_since_epoch();
  ++ref_count_;
}

}
