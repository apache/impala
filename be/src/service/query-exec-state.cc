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
#include "service/impala-server.h"
#include "service/frontend.h"

#include "exec/ddl-executor.h"
#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"

using namespace std;
using namespace boost;
using namespace boost::uuids;
using namespace beeswax;

namespace impala {

ImpalaServer::QueryExecState::QueryExecState(
    ExecEnv* exec_env, Frontend* frontend,
    shared_ptr<SessionState> session,
    const TSessionState& query_session_state, const string& sql_stmt)
  : sql_stmt_(sql_stmt),
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
    start_time_(TimestampValue::local_time_micros()) {
  row_materialization_timer_ = ADD_TIMER(&server_profile_, "RowMaterializationTimer");
  client_wait_timer_ = ADD_TIMER(&server_profile_, "ClientFetchWaitTimer");
  query_events_ = summary_profile_.AddEventSequence("Query Timeline");
  query_events_->Start();
  profile_.AddChild(&summary_profile_);
  profile_.AddChild(&server_profile_);

  // Creating a random_generator every time is not free, but
  // benchmarks show it to be slightly cheaper than contending for a
  // single generator under a lock (since random_generator is not
  // thread-safe).
  random_generator uuid_generator;
  uuid query_uuid = uuid_generator();
  query_id_.hi = *reinterpret_cast<uint64_t*>(&query_uuid.data[0]);
  query_id_.lo = *reinterpret_cast<uint64_t*>(&query_uuid.data[8]);

  summary_profile_.AddInfoString("Start Time", start_time().DebugString());
  summary_profile_.AddInfoString("End Time", "");
  summary_profile_.AddInfoString("Query Type", "N/A");
  summary_profile_.AddInfoString("Query State", PrintQueryState(query_state_));
  summary_profile_.AddInfoString("Query Status", "OK");
  summary_profile_.AddInfoString("Impala Version", GetVersionString(/* compact */ true));
  summary_profile_.AddInfoString("User", user());
  summary_profile_.AddInfoString("Default Db", default_db());
  summary_profile_.AddInfoString("Sql Statement", sql_stmt);
}

Status ImpalaServer::QueryExecState::Exec(TExecRequest* exec_request) {
  exec_request_ = *exec_request;
  profile_.set_name("Query (id=" + PrintId(query_id()) + ")");

  summary_profile_.AddInfoString("Query Type", PrintTStmtType(stmt_type()));
  summary_profile_.AddInfoString("Query State", PrintQueryState(query_state_));

  switch (exec_request->stmt_type) {
    case TStmtType::QUERY:
    case TStmtType::DML:
      return ExecQueryOrDmlRequest();
    case TStmtType::EXPLAIN: {
      request_result_set_.reset(new vector<TResultRow>(
          exec_request_.explain_result.results));
      return Status::OK;
    }
    case TStmtType::DDL: {
      if (exec_request_.ddl_exec_request.ddl_type == TDdlType::USE) {
        lock_guard<mutex> l(parent_session_->lock);
        parent_session_->database = exec_request_.ddl_exec_request.use_db_params.db;
        return Status::OK;
      }
      ddl_executor_.reset(new DdlExecutor(frontend_));
      Status status = ddl_executor_->Exec(exec_request_.ddl_exec_request,
          query_session_state_);
      {
        lock_guard<mutex> l(lock_);
        return UpdateQueryStatus(status);
      }
    }
    case TStmtType::LOAD: {
      DCHECK(exec_request_.__isset.load_data_request);
      TLoadDataResp response;
      RETURN_IF_ERROR(
          frontend_->LoadData(exec_request_.load_data_request, &response));
      request_result_set_.reset(new vector<TResultRow>);
      request_result_set_->push_back(response.load_summary);
      return Status::OK;
    }
    default:
      stringstream errmsg;
      errmsg << "Unknown  exec request stmt type: " << exec_request->stmt_type;
      return Status(errmsg.str());
  }
}

Status ImpalaServer::QueryExecState::ExecQueryOrDmlRequest() {
  DCHECK(exec_request_.__isset.query_exec_request);
  TQueryExecRequest& query_exec_request = exec_request_.query_exec_request;

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

  // If the first fragment has a "limit 0", simply set EOS to true and return.
  // TODO: Remove this check if this is an INSERT. To be compatible with
  // Hive, OVERWRITE inserts must clear out target tables / static
  // partitions even if no rows are written.
  DCHECK(query_exec_request.fragments[0].__isset.plan);
  if (query_exec_request.fragments[0].plan.nodes[0].limit == 0) {
    eos_ = true;
    return Status::OK;
  }

  coord_.reset(new Coordinator(exec_env_));
  Status status = coord_->Exec(
      query_id_, &query_exec_request, exec_request_.query_options, &output_exprs_);
  {
    lock_guard<mutex> l(lock_);
    RETURN_IF_ERROR(UpdateQueryStatus(status));
  }

  profile_.AddChild(coord_->query_profile());
  return Status::OK;
}

void ImpalaServer::QueryExecState::Done() {
  end_time_ = TimestampValue::local_time_micros();
  summary_profile_.AddInfoString("End Time", end_time().DebugString());
  summary_profile_.AddInfoString("Query State", PrintQueryState(query_state_));
  query_events_->MarkEvent("Unregister query");
}

Status ImpalaServer::QueryExecState::Exec(const TMetadataOpRequest& exec_request) {
  ddl_executor_.reset(new DdlExecutor(frontend_));
  RETURN_IF_ERROR(ddl_executor_->Exec(exec_request));
  result_metadata_ = ddl_executor_->result_set_metadata();
  return Status::OK;
}

Status ImpalaServer::QueryExecState::Wait() {
  if (coord_.get() != NULL) {
    RETURN_IF_ERROR(coord_->Wait());
    RETURN_IF_ERROR(UpdateMetastore());
  }

  if (!returns_result_set()) {
    // Queries that do not return a result are finished at this point. This includes
    // DML operations and a subset of the DDL operations.
    eos_ = true;
  } else {
    // Rows are available now, so start the 'wait' timer that tracks how
    // long Impala waits for the client to fetch rows.
    client_wait_sw_.Start();
  }

  return Status::OK;
}

Status ImpalaServer::QueryExecState::FetchRows(const int32_t max_rows,
    QueryResultSet* fetched_rows) {
  // Pause the wait timer, since the client has instructed us to do
  // work on its behalf.
  client_wait_sw_.Stop();
  int64_t elapsed_time = client_wait_sw_.ElapsedTime();
  client_wait_timer_->Set(elapsed_time);

  // FetchInternal has already taken our lock_
  UpdateQueryStatus(FetchRowsInternal(max_rows, fetched_rows));

  // If all rows have been returned, no point in continuing the timer
  // to wait for the next call to FetchRows, which should never come.
  if (!eos_) client_wait_sw_.Start();
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

  if (ddl_executor_ != NULL || request_result_set_ != NULL) {
    // DDL / EXPLAIN / LOAD
    DCHECK(ddl_executor_ == NULL || request_result_set_ == NULL);
    query_state_ = QueryState::FINISHED;
    int num_rows = 0;
    const vector<TResultRow>& all_rows = (ddl_executor_ != NULL) ?
        ddl_executor_->result_set() : (*(request_result_set_.get()));
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
  RETURN_IF_ERROR(coord_->Wait());
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

void ImpalaServer::QueryExecState::Cancel() {
  // If the query is completed, no need to cancel.
  if (eos_) return;
  // we don't want multiple concurrent cancel calls to end up executing
  // Coordinator::Cancel() multiple times
  if (query_state_ == QueryState::EXCEPTION) return;
  query_state_ = QueryState::EXCEPTION;
  if (coord_.get() != NULL) coord_->Cancel();
}

Status ImpalaServer::QueryExecState::UpdateMetastore() {
  if (stmt_type() != TStmtType::DML) return Status::OK;

  DCHECK(exec_request().__isset.query_exec_request);
  TQueryExecRequest query_exec_request = exec_request().query_exec_request;
  if (!query_exec_request.__isset.finalize_params) return Status::OK;

  TFinalizeParams& finalize_params = query_exec_request.finalize_params;
  TCatalogUpdate catalog_update;
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
    RETURN_IF_ERROR(frontend_->UpdateMetastore(catalog_update));
  }
  return Status::OK;
}

Status ImpalaServer::QueryExecState::FetchNextBatch() {
  DCHECK(!eos_);
  DCHECK(coord_.get() != NULL);

  RETURN_IF_ERROR(coord_->GetNext(&current_batch_, coord_->runtime_state()));
  current_batch_row_ = 0;
  eos_ = current_batch_ == NULL;
  return Status::OK;
}

}
