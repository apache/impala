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
#include <protocol/TBinaryProtocol.h>
#include <protocol/TDebugProtocol.h>
#include <transport/TSocket.h>
#include <server/TThreadPoolServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>
#include <server/TServer.h>
//#include <concurrency/Thread.h>
#include <concurrency/PosixThreadFactory.h>
#include <gtest/gtest.h>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>

#include "common/logging.h"
#include "common/service-ids.h"
#include "exprs/expr.h"
#include "exec/hdfs-table-sink.h"
#include "codegen/llvm-codegen.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/client-cache.h"
#include "runtime/descriptors.h"
#include "runtime/data-stream-sender.h"
#include "runtime/row-batch.h"
#include "runtime/plan-fragment-executor.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/exec-env.h"
#include "runtime/coordinator.h"
#include "exec/exec-node.h"
#include "exec/scan-node.h"
#include "exec/ddl-executor.h"
#include "sparrow/simple-scheduler.h"
#include "util/container-util.h"
#include "util/debug-util.h"
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
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/JavaConstants_constants.h"

using namespace std;
using namespace boost;
using namespace boost::algorithm;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;
using namespace beeswax;
using sparrow::Scheduler;
using sparrow::ServiceStateMap;
using sparrow::Membership;

DEFINE_bool(use_planservice, false, "Use external planservice if true");
DECLARE_string(planservice_host);
DECLARE_int32(planservice_port);
DEFINE_int32(fe_port, 21000, "port on which client requests are served");
DEFINE_int32(fe_service_threads, 64,
    "number of threads available to serve client requests");
DECLARE_int32(be_port);
DEFINE_int32(be_service_threads, 64,
    "(Advanced) number of threads available to serve backend execution requests");
DEFINE_bool(load_catalog_at_startup, false, "if true, load all catalog data at startup");
DEFINE_string(default_query_options, "", "key=value pair of default query options for"
    " impalad, separated by ','");

namespace impala {

ThreadManager* fe_tm;
ThreadManager* be_tm;

// Used for queries that execute instantly and always succeed
const string NO_QUERY_HANDLE = "no_query_handle";

const string NUM_QUERIES_METRIC = "impala-server.num.queries";

// Execution state of a query. This captures everything necessary
// to convert row batches received by the coordinator into results
// we can return to the client. It also captures all state required for
// servicing query-related requests from the client.
// Thread safety: this class is generally not thread-safe, callers need to
// synchronize access explicitly via lock().
// To avoid deadlocks, the caller must *not* acquire query_exec_state_map_lock_
// while holding the exec state's lock.
// TODO: Consider renaming to RequestExecState for consistency.
class ImpalaServer::QueryExecState {
 public:
  QueryExecState(ExecEnv* exec_env, ImpalaServer* server)
    : exec_env_(exec_env),
      coord_(NULL),
      profile_(&profile_pool_, "Query"),  // assign name w/ id after planning
      eos_(false),
      query_state_(QueryState::CREATED),
      current_batch_(NULL),
      current_batch_row_(0),
      num_rows_fetched_(0),
      impala_server_(server) {
    planner_timer_ = ADD_COUNTER(&profile_, "PlanningTime", TCounterType::CPU_TICKS);
  }

  ~QueryExecState() {
  }

  // Initiates execution of plan fragments, if there are any, and sets
  // up the output exprs for subsequent calls to FetchRowsAsAscii().
  // Also sets up profile and pre-execution counters.
  // Non-blocking.
  Status Exec(TExecRequest* exec_request);

  // Call this to ensure that rows are ready when calling FetchRowsAsAscii().
  // Must be preceded by call to Exec().
  Status Wait() {
    if (coord_.get() != NULL) { 
      RETURN_IF_ERROR(coord_->Wait());
      RETURN_IF_ERROR(UpdateMetastore());
    }
        
    return Status::OK;
  }

  // Return at most max_rows from the current batch. If the entire current batch has
  // been returned, fetch another batch first.
  // Caller should verify that EOS has not be reached before calling.
  // Always calls coord()->Wait() prior to getting a batch.
  // Also updates query_state_/status_ in case of error.
  Status FetchRowsAsAscii(const int32_t max_rows, vector<string>* fetched_rows);

  // Update query state if the requested state isn't already obsolete.
  void UpdateQueryState(QueryState::type query_state) {
    lock_guard<mutex> l(lock_);
    if (query_state_ < query_state) query_state_ = query_state;
  }

  void SetErrorStatus(const Status& status) {
    DCHECK(!status.ok());
    lock_guard<mutex> l(lock_);
    query_state_ = QueryState::EXCEPTION;
    query_status_ = status;
  }

  // Sets state to EXCEPTION and cancels coordinator.
  // Caller needs to hold lock().
  void Cancel();

  bool eos() { return eos_; }
  Coordinator* coord() const { return coord_.get(); }
  int num_rows_fetched() const { return num_rows_fetched_; }
  const TResultSetMetadata* result_metadata() { return &result_metadata_; }
  const TUniqueId& query_id() const { return query_id_; }
  const TExecRequest& exec_request() const { return exec_request_; }
  TStmtType::type stmt_type() const { return exec_request_.stmt_type; }
  mutex* lock() { return &lock_; }
  const QueryState::type query_state() const { return query_state_; }
  void set_query_state(QueryState::type state) { query_state_ = state; }
  const Status& query_status() const { return query_status_; }
  RuntimeProfile::Counter* planner_timer() { return planner_timer_; }
  void set_result_metadata(const TResultSetMetadata& md) { result_metadata_ = md; }

 private:
  TUniqueId query_id_;
  mutex lock_;  // protects all following fields
  ExecEnv* exec_env_;

  // not set for queries w/o FROM, ddl queries, or short-circuited (i.e. queries with
  // "limit 0")
  scoped_ptr<Coordinator> coord_;

  scoped_ptr<DdlExecutor> ddl_executor_; // Runs DDL queries, instead of coord_
  // local runtime_state_ in case we don't have a coord_
  RuntimeState local_runtime_state_;
  ObjectPool profile_pool_;
  RuntimeProfile profile_;
  RuntimeProfile::Counter* planner_timer_;
  vector<Expr*> output_exprs_;
  bool eos_;  // if true, there are no more rows to return
  QueryState::type query_state_;
  Status query_status_;
  TExecRequest exec_request_;

  TResultSetMetadata result_metadata_; // metadata for select query
  RowBatch* current_batch_; // the current row batch; only applicable if coord is set
  int current_batch_row_; // number of rows fetched within the current batch
  int num_rows_fetched_; // number of rows fetched by client for the entire query

  // To get access to UpdateMetastore
  ImpalaServer* impala_server_;

  // Core logic of FetchRowsAsAscii(). Does not update query_state_/status_.
  Status FetchRowsAsAsciiInternal(const int32_t max_rows, vector<string>* fetched_rows);

  // Fetch the next row batch and store the results in current_batch_. Only
  // called for non-DDL / DML queries.
  Status FetchNextBatch();

  // Evaluates output_exprs_ against at most max_rows in the current_batch_ starting
  // from current_batch_row_ and output the evaluated rows in Ascii form in
  // fetched_rows.
  Status ConvertRowBatchToAscii(const int32_t max_rows, vector<string>* fetched_rows);

  // Creates single result row in query_result by evaluating output_exprs_ without
  // a row (ie, the expressions are constants) and put it in query_results_;
  Status CreateConstantRowAsAscii(vector<string>* fetched_rows);

  // Creates a single string out of the ascii expr values and appends that string
  // to converted_rows.
  Status ConvertSingleRowToAscii(TupleRow* row, vector<string>* converted_rows);

  // Set output_exprs_, based on exprs.
  Status PrepareSelectListExprs(RuntimeState* runtime_state,
      const vector<TExpr>& exprs, const RowDescriptor& row_desc);

  // Gather and publish all required updates to the metastore
  Status UpdateMetastore();

};

Status ImpalaServer::QueryExecState::Exec(TExecRequest* exec_request) {
  exec_request_ = *exec_request;
  profile_.set_name("Query (id=" + PrintId(exec_request->request_id) + ")");
  query_id_ = exec_request->request_id;

  if (exec_request->stmt_type == TStmtType::QUERY || 
      exec_request->stmt_type == TStmtType::DML) {
    DCHECK(exec_request_.__isset.query_exec_request);
    TQueryExecRequest& query_exec_request = exec_request_.query_exec_request;

    // we always need at least one plan fragment
    DCHECK_GT(query_exec_request.fragments.size(), 0);

    // If desc_tbl is not set, query has SELECT with no FROM. In that
    // case, the query can only have a single fragment, and that fragment needs to be
    // executed by the coordinator. This check confirms that.
    // If desc_tbl is set, the query may or may not have a coordinator fragment.
    bool has_coordinator_fragment =
        query_exec_request.fragments[0].partition.type == TPartitionType::UNPARTITIONED;
    DCHECK(has_coordinator_fragment || query_exec_request.__isset.desc_tbl);
    bool has_from_clause = query_exec_request.__isset.desc_tbl;
    
    if (!has_from_clause) {
      // query without a FROM clause: only one fragment, and it doesn't have a plan
      DCHECK(!query_exec_request.fragments[0].__isset.plan);
      DCHECK_EQ(query_exec_request.fragments.size(), 1);
      // TODO: This does not handle INSERT INTO ... SELECT 1 because there is no
      // coordinator fragment actually executing to drive the sink. 
      // Workaround: INSERT INTO ... SELECT 1 FROM tbl LIMIT 1
      local_runtime_state_.Init(
          exec_request->request_id, exec_request->query_options,
          query_exec_request.query_globals.now_string,
          NULL /* = we don't expect to be executing anything here */);
      RETURN_IF_ERROR(PrepareSelectListExprs(&local_runtime_state_,
          query_exec_request.fragments[0].output_exprs, RowDescriptor()));
    } else {
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
      RETURN_IF_ERROR(coord_->Exec(
          exec_request->request_id, &query_exec_request, exec_request->query_options));

      if (has_coordinator_fragment) {
        RETURN_IF_ERROR(PrepareSelectListExprs(coord_->runtime_state(),
            query_exec_request.fragments[0].output_exprs, coord_->row_desc()));
      }
      profile_.AddChild(coord_->query_profile());
    }
  } else {
    // ODBC-187: Only tab supported as delimiter
    ddl_executor_.reset(new DdlExecutor(impala_server_, "\t"));
    RETURN_IF_ERROR(ddl_executor_->Exec(&exec_request_.ddl_exec_request));
  }

  return Status::OK;
}

Status ImpalaServer::QueryExecState::FetchRowsAsAscii(const int32_t max_rows,
    vector<string>* fetched_rows) {
  DCHECK(!eos_);
  DCHECK(query_state_ != QueryState::EXCEPTION);
  query_status_ = FetchRowsAsAsciiInternal(max_rows, fetched_rows);
  if (!query_status_.ok()) {
    query_state_ = QueryState::EXCEPTION;
  }
  return query_status_;
}

Status ImpalaServer::QueryExecState::FetchRowsAsAsciiInternal(const int32_t max_rows,
    vector<string>* fetched_rows) {
  if (coord_ == NULL && ddl_executor_ == NULL) {
    query_state_ = QueryState::FINISHED;  // results will be ready after this call
    // query without FROM clause: we return exactly one row
    return CreateConstantRowAsAscii(fetched_rows);
  } else {
    if (coord_ != NULL) {
      RETURN_IF_ERROR(coord_->Wait());
    }
    query_state_ = QueryState::FINISHED;  // results will be ready after this call
    if (coord_ != NULL) {
      // Fetch the next batch if we've returned the current batch entirely
      if (current_batch_ == NULL || current_batch_row_ >= current_batch_->num_rows()) {
        RETURN_IF_ERROR(FetchNextBatch());
      }
      return ConvertRowBatchToAscii(max_rows, fetched_rows);
    } else {
      DCHECK(ddl_executor_.get());
      int num_rows = 0;
      const vector<string>& all_rows = ddl_executor_->all_rows_ascii();

      // If max_rows < 0, there's no maximum on the number of rows to return
      while ((num_rows < max_rows || max_rows < 0)
          && num_rows_fetched_ < all_rows.size()) {
        fetched_rows->push_back(all_rows[num_rows_fetched_++]);
        ++num_rows;
      }
      
      eos_ = (num_rows_fetched_ == all_rows.size());

      return Status::OK;
    }
  }
}

void ImpalaServer::QueryExecState::Cancel() {
  // we don't want multiple concurrent cancel calls to end up executing
  // Coordinator::Cancel() multiple times
  if (query_state_ == QueryState::EXCEPTION) return;
  query_state_ = QueryState::EXCEPTION;
  if (coord_.get() != NULL) coord_->Cancel();
}

Status ImpalaServer::QueryExecState::PrepareSelectListExprs(
    RuntimeState* runtime_state,
    const vector<TExpr>& exprs, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(runtime_state->obj_pool(), exprs, &output_exprs_));
  for (int i = 0; i < output_exprs_.size(); ++i) {
    // Don't codegen these, they are unused anyway.
    // TODO: codegen this and the write values path
    RETURN_IF_ERROR(Expr::Prepare(output_exprs_[i], runtime_state, row_desc, true));
  }
  return Status::OK;
}

Status ImpalaServer::QueryExecState::UpdateMetastore() {
  if (stmt_type() != TStmtType::DML) {
    return Status::OK;
  }

  DCHECK(exec_request().__isset.query_exec_request);
  TQueryExecRequest query_exec_request = exec_request().query_exec_request;
  DCHECK(query_exec_request.__isset.finalize_params);

  // TODO: Doesn't handle INSERT with no FROM
  if (coord() == NULL) {
    stringstream ss;
    ss << "INSERT with no FROM clause not fully supported, not updating metastore"
       << " (query_id: " << query_id() << ")";
    VLOG_QUERY << ss.str();
    return Status(ss.str());
  }

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
    
    catalog_update.target_table = query_exec_request.finalize_params.table_name;
    catalog_update.db_name = query_exec_request.finalize_params.table_db;
    RETURN_IF_ERROR(impala_server_->UpdateMetastore(catalog_update));
  }

  // TODO: Reset only the updated table
  return impala_server_->ResetCatalogInternal(); 
} 

Status ImpalaServer::QueryExecState::FetchNextBatch() {
  DCHECK(!eos_);
  DCHECK(coord_.get() != NULL);

  RETURN_IF_ERROR(coord_->GetNext(&current_batch_, coord_->runtime_state()));
  current_batch_row_ = 0;
  eos_ = current_batch_ == NULL;
  return Status::OK;
}

Status ImpalaServer::QueryExecState::ConvertRowBatchToAscii(const int32_t max_rows,
    vector<string>* fetched_rows) {
  fetched_rows->clear();
  if (current_batch_ == NULL) return Status::OK;

  // Convert the available rows, limited by max_rows
  int available = current_batch_->num_rows() - current_batch_row_;
  int fetched_count = available;
  // max_rows <= 0 means no limit
  if (max_rows > 0 && max_rows < available) fetched_count = max_rows;
  fetched_rows->reserve(fetched_count);
  for (int i = 0; i < fetched_count; ++i) {
    TupleRow* row = current_batch_->GetRow(current_batch_row_);
    RETURN_IF_ERROR(ConvertSingleRowToAscii(row, fetched_rows));
    ++num_rows_fetched_;
    ++current_batch_row_;
  }
  return Status::OK;
}

Status ImpalaServer::QueryExecState::CreateConstantRowAsAscii(
    vector<string>* fetched_rows) {
  string out_str;
  fetched_rows->reserve(1);
  RETURN_IF_ERROR(ConvertSingleRowToAscii(NULL, fetched_rows));
  eos_ = true;
  ++num_rows_fetched_;
  return Status::OK;
}

Status ImpalaServer::QueryExecState::ConvertSingleRowToAscii(TupleRow* row,
    vector<string>* converted_rows) {
  stringstream out_stream;
  out_stream.precision(ASCII_PRECISION);
  for (int i = 0; i < output_exprs_.size(); ++i) {
    // ODBC-187 - ODBC can only take "\t" as the delimiter
    out_stream << (i > 0 ? "\t" : "");
    output_exprs_[i]->PrintValue(row, &out_stream);
  }
  converted_rows->push_back(out_stream.str());
  return Status::OK;
}

// Execution state of a single plan fragment.
class ImpalaServer::FragmentExecState {
 public:
  FragmentExecState(const TUniqueId& query_id, int backend_num,
                    const TUniqueId& fragment_instance_id, ExecEnv* exec_env,
                    const pair<string, int>& coord_hostport)
    : query_id_(query_id),
      backend_num_(backend_num),
      fragment_instance_id_(fragment_instance_id),
      executor_(exec_env,
          bind<void>(mem_fn(&ImpalaServer::FragmentExecState::ReportStatusCb),
                     this, _1, _2, _3)),
      client_cache_(exec_env->client_cache()),
      coord_hostport_(coord_hostport) {
  }

  // Calling the d'tor releases all memory and closes all data streams
  // held by executor_.
  ~FragmentExecState() {
  }

  // Returns current execution status, if there was an error. Otherwise cancels
  // the fragment and returns OK.
  Status Cancel();

  // Call Prepare() and create and initialize data sink.
  Status Prepare(const TExecPlanFragmentParams& exec_params);

  // Main loop of plan fragment execution. Blocks until execution finishes.
  void Exec();

  const TUniqueId& query_id() const { return query_id_; }
  const TUniqueId& fragment_instance_id() const { return fragment_instance_id_; }

  void set_exec_thread(thread* exec_thread) { exec_thread_.reset(exec_thread); }

 private:
  TUniqueId query_id_;
  int backend_num_;
  TUniqueId fragment_instance_id_;
  PlanFragmentExecutor executor_;
  BackendClientCache* client_cache_;
  TExecPlanFragmentParams exec_params_;

  // initiating coordinator to which we occasionally need to report back
  // (it's exported ImpalaInternalService)
  const pair<string, int> coord_hostport_;

  // the thread executing this plan fragment
  scoped_ptr<thread> exec_thread_;

  // protects exec_status_
  mutex status_lock_;

  // set in ReportStatusCb();
  // if set to anything other than OK, execution has terminated w/ an error
  Status exec_status_;

  // Callback for executor; updates exec_status_ if 'status' indicates an error
  // or if there was a thrift error.
  void ReportStatusCb(const Status& status, RuntimeProfile* profile, bool done);

  // Update exec_status_ w/ status, if the former isn't already an error.
  // Returns current exec_status_.
  Status UpdateStatus(const Status& status);
};

Status ImpalaServer::FragmentExecState::UpdateStatus(const Status& status) {
  lock_guard<mutex> l(status_lock_);
  if (!status.ok() && exec_status_.ok()) exec_status_ = status;
  return exec_status_;
}

Status ImpalaServer::FragmentExecState::Cancel() {
  lock_guard<mutex> l(status_lock_);
  RETURN_IF_ERROR(exec_status_);
  executor_.Cancel();
  return Status::OK;
}

Status ImpalaServer::FragmentExecState::Prepare(
    const TExecPlanFragmentParams& exec_params) {
  exec_params_ = exec_params;
  RETURN_IF_ERROR(executor_.Prepare(exec_params));
  return Status::OK;
}

void ImpalaServer::FragmentExecState::Exec() {
  // Open() does the full execution, because all plan fragments have sinks
  executor_.Open();
}

// There can only be one of these callbacks in-flight at any moment, because
// it is only invoked from the executor's reporting thread.
// Also, the reported status will always reflect the most recent execution status,
// including the final status when execution finishes.
void ImpalaServer::FragmentExecState::ReportStatusCb(
    const Status& status, RuntimeProfile* profile, bool done) {
  DCHECK(status.ok() || done);  // if !status.ok() => done
  Status exec_status = UpdateStatus(status);

  ImpalaInternalServiceClient* coord;
  if (!client_cache_->GetClient(coord_hostport_, &coord).ok()) {
    stringstream s;
    s << "couldn't get a client for " << coord_hostport_.first
      << ":" << coord_hostport_.second;
    UpdateStatus(Status(TStatusCode::INTERNAL_ERROR, s.str()));
    return;
  }
  DCHECK(coord != NULL);

  TReportExecStatusParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_query_id(query_id_);
  params.__set_backend_num(backend_num_);
  params.__set_fragment_instance_id(fragment_instance_id_);
  exec_status.SetTStatus(&params);
  params.__set_done(done);
  profile->ToThrift(&params.profile);
  params.__isset.profile = true;

  RuntimeState* runtime_state = executor_.runtime_state();
  DCHECK(runtime_state != NULL);
  // Only send updates to insert status if fragment is finished, the coordinator
  // waits until query execution is done to use them anyhow.
  if (done && runtime_state->hdfs_files_to_move()->size() > 0) {
    TInsertExecStatus insert_status;
    insert_status.__set_files_to_move(*runtime_state->hdfs_files_to_move());

    if (executor_.runtime_state()->num_appended_rows()->size() > 0) { 
      insert_status.__set_num_appended_rows(
          *executor_.runtime_state()->num_appended_rows());
    }

    params.__set_insert_exec_status(insert_status);
  }

  // Send new errors to coordinator
  runtime_state->GetUnreportedErrors(&(params.error_log));
  params.__isset.error_log = (params.error_log.size() > 0);

  TReportExecStatusResult res;
  Status rpc_status;
  try {
    coord->ReportExecStatus(res, params);
    rpc_status = Status(res.status);
  } catch (TException& e) {
    stringstream msg;
    msg << "ReportExecStatus() to " << coord_hostport_.first << ":"
        << coord_hostport_.second << " failed:\n" << e.what();
    VLOG_QUERY << msg.str();
    rpc_status = Status(TStatusCode::INTERNAL_ERROR, msg.str());
  }
  client_cache_->ReleaseClient(coord);

  if (!rpc_status.ok()) {
    // we need to cancel the execution of this fragment
    UpdateStatus(rpc_status);
    executor_.Cancel();
  }
}

const char* ImpalaServer::SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION = "42000";
const char* ImpalaServer::SQLSTATE_GENERAL_ERROR = "HY000";
const char* ImpalaServer::SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED = "HYC00";
const int ImpalaServer::ASCII_PRECISION = 16; // print 16 digits for double/float

ImpalaServer::ImpalaServer(ExecEnv* exec_env)
    : exec_env_(exec_env) {
  // Initialize default config
  InitializeConfigVariables();

  if (!FLAGS_use_planservice) {
    JNIEnv* jni_env = getJNIEnv();
    // create instance of java class JniFrontend
    jclass fe_class = jni_env->FindClass("com/cloudera/impala/service/JniFrontend");
    jmethodID fe_ctor = jni_env->GetMethodID(fe_class, "<init>", "(Z)V");
    EXIT_IF_EXC(jni_env);
    create_exec_request_id_ =
        jni_env->GetMethodID(fe_class, "createExecRequest", "([B)[B");
    EXIT_IF_EXC(jni_env);
    get_explain_plan_id_ = jni_env->GetMethodID(fe_class, "getExplainPlan",
        "([B)Ljava/lang/String;");
    EXIT_IF_EXC(jni_env);
    reset_catalog_id_ = jni_env->GetMethodID(fe_class, "resetCatalog", "()V");
    EXIT_IF_EXC(jni_env);
    get_hadoop_config_id_ = jni_env->GetMethodID(fe_class, "getHadoopConfigAsHtml",
        "()Ljava/lang/String;");
    EXIT_IF_EXC(jni_env);
    update_metastore_id_ = jni_env->GetMethodID(fe_class, "updateMetastore", "([B)V");
    EXIT_IF_EXC(jni_env);
    get_table_names_id_ = jni_env->GetMethodID(fe_class, "getTableNames", "([B)[B");
    EXIT_IF_EXC(jni_env);
    describe_table_id_ = jni_env->GetMethodID(fe_class, "describeTable", "([B)[B");
    EXIT_IF_EXC(jni_env);
    get_db_names_id_ = jni_env->GetMethodID(fe_class, "getDbNames", "([B)[B");
    EXIT_IF_EXC(jni_env);

    jboolean lazy = (FLAGS_load_catalog_at_startup ? false : true);
    jobject fe = jni_env->NewObject(fe_class, fe_ctor, lazy);
    EXIT_IF_EXC(jni_env);
    EXIT_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, fe, &fe_));
  } else {
    planservice_socket_.reset(new TSocket(FLAGS_planservice_host,
        FLAGS_planservice_port));
    planservice_transport_.reset(new TBufferedTransport(planservice_socket_));
    planservice_protocol_.reset(new TBinaryProtocol(planservice_transport_));
    planservice_client_.reset(new ImpalaPlanServiceClient(planservice_protocol_));
    planservice_transport_->open();
  }

  Webserver::PathHandlerCallback default_callback =
      bind<void>(mem_fn(&ImpalaServer::RenderHadoopConfigs), this, _1);
  exec_env->webserver()->RegisterPathHandler("/varz", default_callback);

  Webserver::PathHandlerCallback query_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryStatePathHandler), this, _1);
  exec_env->webserver()->RegisterPathHandler("/queries", query_callback);

  Webserver::PathHandlerCallback sessions_callback =
      bind<void>(mem_fn(&ImpalaServer::SessionPathHandler), this, _1);
  exec_env->webserver()->RegisterPathHandler("/sessions", sessions_callback);

  Webserver::PathHandlerCallback catalog_callback =
      bind<void>(mem_fn(&ImpalaServer::CatalogPathHandler), this, _1);
  exec_env->webserver()->RegisterPathHandler("/catalog", catalog_callback);

  Webserver::PathHandlerCallback backends_callback =
      bind<void>(mem_fn(&ImpalaServer::BackendsPathHandler), this, _1);
  exec_env->webserver()->RegisterPathHandler("/backends", backends_callback);

  num_queries_metric_ = 
      exec_env->metrics()->CreateAndRegisterPrimitiveMetric(NUM_QUERIES_METRIC, 0L);
}

void ImpalaServer::RenderHadoopConfigs(stringstream* output) {
  (*output) << "<h2>Hadoop Configuration</h2>";
  if (FLAGS_use_planservice) {
    (*output) << "Using external PlanService, no Hadoop configs available";
    return;
  }
  (*output) << "<pre>";
  JNIEnv* jni_env = getJNIEnv();
  jstring java_explain_string =
      static_cast<jstring>(jni_env->CallObjectMethod(fe_, get_hadoop_config_id_));
  RETURN_IF_EXC(jni_env);
  jboolean is_copy;
  const char *str = jni_env->GetStringUTFChars(java_explain_string, &is_copy);
  RETURN_IF_EXC(jni_env);
  (*output) << str;
  (*output) << "</pre>";
  jni_env->ReleaseStringUTFChars(java_explain_string, str);
  RETURN_IF_EXC(jni_env);
}

void ImpalaServer::QueryStatePathHandler(stringstream* output) {
  (*output) << "<h2>Queries</h2>";
  lock_guard<mutex> l(query_exec_state_map_lock_);
  (*output) << "This page lists all registered queries, i.e., those that are not closed "
    " nor cancelled.<br/>" << endl;
  (*output) << query_exec_state_map_.size() << " queries in flight" << endl;
  (*output) << "<table border=1><tr><th>Query Id</th>" << endl;
  (*output) << "<th>Statement</th>" << endl;
  (*output) << "<th>Query Type</th>" << endl;
  (*output) << "<th>Backend Progress</th>" << endl;
  (*output) << "<th>State</th>" << endl;
  (*output) << "<th># rows fetched</th>" << endl;
  (*output) << "</tr>";
  BOOST_FOREACH(const QueryExecStateMap::value_type& exec_state, query_exec_state_map_) {
    QueryHandle handle;
    TUniqueIdToQueryHandle(exec_state.first, &handle);
    const TExecRequest& request = exec_state.second->exec_request();
    const string& query_stmt = 
        (request.stmt_type != TStmtType::DDL && request.__isset.sql_stmt) ?
        request.sql_stmt : "N/A";
    (*output) << "<tr><td>" << handle.id << "</td>"
              << "<td>" << query_stmt << "</td>"
              << "<td>"
              << _TStmtType_VALUES_TO_NAMES.find(request.stmt_type)->second
              << "</td>"
              << "<td>";

    Coordinator* coord = exec_state.second->coord(); 
    if (coord == NULL) {
      (*output) << "N/A";
    } else {
      int64_t num_complete = coord->progress().num_complete();
      int64_t total = coord->progress().total();
      (*output) << num_complete << " / " << total << " (" << setw(4);
      if (total == 0) {
        (*output) << " (0%)";
      } else {
        (*output) << (100.0 * num_complete / (1.f * total)) << "%)";
      }
    }

    (*output) << "</td>"
              << "<td>" << _QueryState_VALUES_TO_NAMES.find(
                  exec_state.second->query_state())->second << "</td>"
              << "<td>" << exec_state.second->num_rows_fetched() << "</td>"
              << "</tr>" << endl;
  }

  (*output) << "</table>";

  // Print the query location counts.
  (*output) << "<h2>Query Locations</h2>";
  (*output) << "<table border=1><tr><th>Location</th><th>Query Ids</th></tr>" << endl;
  {
    lock_guard<mutex> l(query_locations_lock_);
    BOOST_FOREACH(const QueryLocations::value_type& location, query_locations_) {
      (*output) << "<tr><td>" << location.first.ipaddress << ":" << location.first.port 
                << "<td><b>" << location.second.size() << "</b></td></tr>";
    }
  }
  (*output) << "</table>";
}

void ImpalaServer::SessionPathHandler(stringstream* output) {
  (*output) << "<h2>Sessions</h2>" << endl;
  lock_guard<mutex> l_(session_state_map_lock_);
  (*output) << "There are " << session_state_map_.size() << " active sessions." << endl
            << "<table border=1><tr><th>Session Key</th>"
            << "<th>Default Database</th><th>Start Time</th></tr>" << endl;
  BOOST_FOREACH(const SessionStateMap::value_type& session, session_state_map_) {
    (*output) << "<tr>"
              << "<td>" << session.first << "</td>"
              << "<td>" << session.second.database << "</td>"
              << "<td>" << to_simple_string(session.second.start_time) << "</td>"
              << "</tr>";
  }

  (*output) << "</table>";
}

void ImpalaServer::CatalogPathHandler(stringstream* output) {
  (*output) << "<h2>Catalog</h2>" << endl;
  vector<string> db_names;
  Status status = GetDbNames(NULL, &db_names);
  if (!status.ok()) {
    (*output) << "Error: " << status.GetErrorMsg();
    return;
  }

  // Build a navigation string like [ default | tpch | ... ]
  vector<string> links;
  BOOST_FOREACH(const string& db, db_names) {
    stringstream ss;
    ss << "<a href='#" << db << "'>" << db << "</a>";
    links.push_back(ss.str());
  }
  (*output) << "[ " <<  join(links, " | ") << " ] ";

  BOOST_FOREACH(const string& db, db_names) {
    (*output) << "<a id='" << db << "'><h3>" << db << "</h3></a>";
    vector<string> table_names;
    Status status = GetTableNames(&db, NULL, &table_names);
    if (!status.ok()) {
      (*output) << "Error: " << status.GetErrorMsg();
      continue;
    }

    (*output) << "<p>" << db << " contains <b>" << table_names.size() 
              << "</b> tables</p>";

    (*output) << "<ul>" << endl;
    BOOST_FOREACH(const string& table, table_names) {
      (*output) << "<li>" << table << "</li>" << endl;
    }
    (*output) << "</ul>" << endl;
  }
}

void ImpalaServer::BackendsPathHandler(stringstream* output) {
  (*output) << "<h2>Known Backends</h2>";
  Scheduler::HostList backends;
  (*output) << "<pre>";
  exec_env_->scheduler()->GetAllKnownHosts(&backends);
  BOOST_FOREACH(const Scheduler::HostList::value_type& host, backends) {
    (*output) << host << endl;
  }
  (*output) << "</pre>";
}

ImpalaServer::~ImpalaServer() {}

void ImpalaServer::query(QueryHandle& query_handle, const Query& query) {
  VLOG_QUERY << "query(): query=" << query.query;
  TClientRequest query_request;
  Status status = QueryToTClientRequest(query, &query_request);
  if (!status.ok()) {
    // raise general error for request conversion error;
    RaiseBeeswaxException(status.GetErrorMsg(), SQLSTATE_GENERAL_ERROR);
  }

  shared_ptr<QueryExecState> exec_state;
  status = Execute(query_request, &exec_state);
  
  if (!status.ok()) {
    // raise Syntax error or access violation;
    // it's likely to be syntax/analysis error
    // TODO: that may not be true; fix this
    RaiseBeeswaxException(
        status.GetErrorMsg(), SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);
  }

  if (exec_state.get() == NULL) {
    // No execution required for this query (USE)
    query_handle.id = NO_QUERY_HANDLE;
    query_handle.log_context = NO_QUERY_HANDLE;
    return;    
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
  status = Execute(query_request, &exec_state);

  if (!status.ok()) {
    // raise Syntax error or access violation;
    // it's likely to be syntax/analysis error
    // TODO: that may not be true; fix this
    RaiseBeeswaxException(
        status.GetErrorMsg(), SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);
  }

  if (exec_state.get() == NULL) {
    // No execution required for this query (USE)
    query_handle.id = NO_QUERY_HANDLE;
    query_handle.log_context = NO_QUERY_HANDLE;
    return;
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

Status ImpalaServer::Execute(const TClientRequest& request,
    shared_ptr<QueryExecState>* exec_state) {
  bool registered_exec_state;
  num_queries_metric_->Increment(1L);
  Status status = ExecuteInternal(request, &registered_exec_state, exec_state);
  if (!status.ok() && registered_exec_state) {
    UnregisterQuery((*exec_state)->query_id());
  }
  return status;
}

Status ImpalaServer::ExecuteInternal(
    const TClientRequest& request, bool* registered_exec_state,
    shared_ptr<QueryExecState>* exec_state) {
  exec_state->reset(new QueryExecState(exec_env_, this));
  *registered_exec_state = false;

  TExecRequest result;
  {
    SCOPED_TIMER((*exec_state)->planner_timer());
    RETURN_IF_ERROR(GetExecRequest(request, &result));
  }

  if (result.stmt_type == TStmtType::DDL && 
      result.ddl_exec_request.ddl_type == TDdlType::USE) {
    {
      lock_guard<mutex> l_(session_state_map_lock_);
      ThriftServer::SessionKey* key = ThriftServer::GetThreadSessionKey();
      SessionStateMap::iterator it = session_state_map_.find(*key);
      DCHECK(it != session_state_map_.end());
      it->second.database = result.ddl_exec_request.database;
    }
    exec_state->reset();
    return Status::OK;
  }
  
  if (result.__isset.result_set_metadata) {
    (*exec_state)->set_result_metadata(result.result_set_metadata);
  }

  // register exec state before starting execution in order to handle incoming
  // status reports
  {
    lock_guard<mutex> l(query_exec_state_map_lock_);

    // there shouldn't be an active query with that same id
    // (query_id is globally unique)
    QueryExecStateMap::iterator entry =
        query_exec_state_map_.find(result.request_id);
    if (entry != query_exec_state_map_.end()) {
      stringstream ss;
      ss << "query id " << PrintId(result.request_id)
         << " already exists";
      return Status(TStatusCode::INTERNAL_ERROR, ss.str());
    }

    query_exec_state_map_.insert(make_pair(result.request_id, *exec_state));
    *registered_exec_state = true;
  }

  // start execution of query; also starts fragment status reports
  RETURN_IF_ERROR((*exec_state)->Exec(&result));

  if ((*exec_state)->coord() != NULL) {
    const unordered_set<THostPort>& unique_hosts = (*exec_state)->coord()->unique_hosts();
    if (!unique_hosts.empty()) {
      lock_guard<mutex> l(query_locations_lock_);
      BOOST_FOREACH(const THostPort& port, unique_hosts) {
        query_locations_[port].insert((*exec_state)->query_id());
      }
    }
  }

  return Status::OK;
}

bool ImpalaServer::UnregisterQuery(const TUniqueId& query_id) {
  VLOG_QUERY << "UnregisterQuery(): query_id=" << query_id;
  shared_ptr<QueryExecState> exec_state;
  {
    lock_guard<mutex> l(query_exec_state_map_lock_);
    QueryExecStateMap::iterator entry = query_exec_state_map_.find(query_id);
    if (entry == query_exec_state_map_.end()) {
      VLOG_QUERY << "unknown query id: " << PrintId(query_id);
      return false;
    } else {
      exec_state = entry->second;
    }      
    query_exec_state_map_.erase(entry);
  }
  

  if (exec_state->coord() != NULL) {
    const unordered_set<THostPort>& unique_hosts = 
        exec_state->coord()->unique_hosts();
    if (!unique_hosts.empty()) {
      lock_guard<mutex> l(query_locations_lock_);
      BOOST_FOREACH(const THostPort& hostport, unique_hosts) {
        // Query may have been removed already by cancellation path. In
        // particular, if node to fail was last sender to an exchange, the
        // coordinator will realise and fail the query at the same time the
        // failure detection path does the same thing. They will harmlessly race
        // to remove the query from this map.
        QueryLocations::iterator it = query_locations_.find(hostport);
        if (it != query_locations_.end()) {
          it->second.erase(exec_state->query_id());
        }
      }
    }
  }

  {
    lock_guard<mutex> l(*exec_state->lock());
    //exec_state->Cancel();
  }
  return true;
}

void ImpalaServer::Wait(boost::shared_ptr<QueryExecState> exec_state) {
  // block until results are ready
  Status status = exec_state->Wait();
  if (status.ok()) {
    exec_state->UpdateQueryState(QueryState::FINISHED);
  } else {
    UnregisterQuery(exec_state->query_id());
    exec_state->SetErrorStatus(status);
  }
}

Status ImpalaServer::UpdateMetastore(const TCatalogUpdate& catalog_update) {
  VLOG_QUERY << "UpdateMetastore()";
  if (!FLAGS_use_planservice) {
    JNIEnv* jni_env = getJNIEnv();
    jbyteArray request_bytes;
    RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &catalog_update, &request_bytes));
    jni_env->CallObjectMethod(fe_, update_metastore_id_, request_bytes);
    RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
  } else {
    try {
      planservice_client_->UpdateMetastore(catalog_update);
    } catch (TException& e) {
      return Status(e.what());
    }
  }

  return Status::OK;
}

Status ImpalaServer::DescribeTable(const string& db, const string& table, 
    vector<TColumnDesc>* columns) {
 if (!FLAGS_use_planservice) {
    JNIEnv* jni_env = getJNIEnv();
    jbyteArray request_bytes;
    TDescribeTableParams params;
    params.__set_db(db);
    params.__set_table_name(table);

    RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &params, &request_bytes));
    jbyteArray result_bytes = static_cast<jbyteArray>(
        jni_env->CallObjectMethod(fe_, describe_table_id_, request_bytes));

    RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());

    TDescribeTableResult result;
    RETURN_IF_ERROR(DeserializeThriftMsg(jni_env, result_bytes, &result));
    
    columns->insert(columns->begin(),
        result.columns.begin(), result.columns.end());
    return Status::OK;
 } else {
   return Status("DescribeTable not supported with external planservice");
 }
}

Status ImpalaServer::GetTableNames(const string* db, const string* pattern, 
    vector<string>* table_names) {
  if (!FLAGS_use_planservice) {
    JNIEnv* jni_env = getJNIEnv();
    jbyteArray request_bytes;
    TGetTablesParams params;
    if (db != NULL) {
      params.__set_db(*db);
    }
    if (pattern != NULL) {
      params.__set_pattern(*pattern);
    }

    RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &params, &request_bytes));
    jbyteArray result_bytes = static_cast<jbyteArray>(
        jni_env->CallObjectMethod(fe_, get_table_names_id_, request_bytes));

    RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());

    TGetTablesResult result;
    RETURN_IF_ERROR(DeserializeThriftMsg(jni_env, result_bytes, &result));
    
    table_names->insert(table_names->begin(),
        result.tables.begin(), result.tables.end());
    return Status::OK;
  } else {
    return Status("GetTableNames not supported with external planservice");
  }
}

Status ImpalaServer::GetDbNames(const string* pattern, vector<string>* db_names) {
  if (!FLAGS_use_planservice) {
    JNIEnv* jni_env = getJNIEnv();
    jbyteArray request_bytes;
    TGetDbsParams params;
    if (pattern != NULL) {
      params.__set_pattern(*pattern);
    }

    RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &params, &request_bytes));
    jbyteArray result_bytes = static_cast<jbyteArray>(
        jni_env->CallObjectMethod(fe_, get_db_names_id_, request_bytes));

    RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());

    TGetDbsResult result;
    RETURN_IF_ERROR(DeserializeThriftMsg(jni_env, result_bytes, &result));
    
    db_names->insert(db_names->begin(), result.dbs.begin(), result.dbs.end());
    return Status::OK;
  } else {
    return Status("GetDbNames not supported with external planservice");
  }
}

Status ImpalaServer::GetExecRequest(
    const TClientRequest& request, TExecRequest* result) {
  if (!FLAGS_use_planservice) {
    // TODO: figure out if repeated calls to
    // JNI_GetCreatedJavaVMs()/AttachCurrentThread() are too expensive
    JNIEnv* jni_env = getJNIEnv();
    jbyteArray request_bytes;
    RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &request, &request_bytes));
    jbyteArray result_bytes = static_cast<jbyteArray>(
        jni_env->CallObjectMethod(fe_, create_exec_request_id_, request_bytes));
    RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
    RETURN_IF_ERROR(DeserializeThriftMsg(jni_env, result_bytes, result));
    // TODO: dealloc result_bytes?
    // TODO: figure out if we should detach here
    //RETURN_IF_JNIERROR(jvm_->DetachCurrentThread());
    return Status::OK;
  } else {
    planservice_client_->CreateExecRequest(*result, request);
    return Status::OK;
  }
}

Status ImpalaServer::GetExplainPlan(
    const TClientRequest& query_request, string* explain_string) {
  if (!FLAGS_use_planservice) {
    // TODO: figure out if repeated calls to
    // JNI_GetCreatedJavaVMs()/AttachCurrentThread() are too expensive
    JNIEnv* jni_env = getJNIEnv();
    jbyteArray query_request_bytes;
    RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &query_request, &query_request_bytes));
    jstring java_explain_string = static_cast<jstring>(
        jni_env->CallObjectMethod(fe_, get_explain_plan_id_, query_request_bytes));
    RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
    jboolean is_copy;
    const char *str = jni_env->GetStringUTFChars(java_explain_string, &is_copy);
    RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
    *explain_string = str;
    jni_env->ReleaseStringUTFChars(java_explain_string, str);
    RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
    return Status::OK;
  } else {
    try {
      planservice_client_->GetExplainString(*explain_string, query_request);
    } catch (TException& e) {
      return Status(e.what());
    }
    return Status::OK;
  }
}

Status ImpalaServer::ResetCatalogInternal() {
  LOG(INFO) << "Refreshing catalog";
  if (!FLAGS_use_planservice) {
    JNIEnv* jni_env = getJNIEnv();
    jni_env->CallObjectMethod(fe_, reset_catalog_id_);
    RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
  } else {
    try {
      planservice_client_->RefreshMetadata();
    } catch (TTransportException& e) {
      stringstream msg;
      msg << "RefreshMetadata rpc failed: " << e.what();
      LOG(ERROR) << msg.str();
      // TODO: different error code here?
      return Status(msg.str());
    }
  }

  return Status::OK;
}

void ImpalaServer::ResetCatalog(impala::TStatus& status) {
  ResetCatalogInternal().ToThrift(&status);
}

Status ImpalaServer::CancelInternal(const TUniqueId& query_id) {
  VLOG_QUERY << "Cancel(): query_id=" << PrintId(query_id);
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
  if (exec_state == NULL) return Status("Invalid or unknown query handle");

  lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());
  // TODO: can we call Coordinator::Cancel() here while holding lock?
  exec_state->Cancel();
  return Status::OK;
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

  if (query_handle.id == NO_QUERY_HANDLE) {
    query_results.ready = true;
    query_results.has_more = false;    
    return;
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

Status ImpalaServer::FetchInternal(const TUniqueId& query_id,
    const bool start_over, const int32_t fetch_size, beeswax::Results* query_results) {
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
  if (exec_state == NULL) return Status("Invalid query handle");

  // make sure we release the lock on exec_state if we see any error
  lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());

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
  if (exec_state->eos()) {
    // if we hit EOS, return no rows and set has_more to false.
    query_results->data.clear();
  } else {
    fetch_rows_status = exec_state->FetchRowsAsAscii(fetch_size, &query_results->data);
  }
  query_results->__set_has_more(!exec_state->eos());
  query_results->__isset.data = true;

  return fetch_rows_status;
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
  if (handle.id == NO_QUERY_HANDLE) {
    return;
  }

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

Status ImpalaServer::CloseInsertInternal(const TUniqueId& query_id,
    TInsertResult* insert_result) {
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
  if (exec_state == NULL) return Status("Invalid query handle");

  {
    lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());
    // Coord may be NULL for a SELECT with LIMIT 0.
    // Note that when IMP-231 is fixed (INSERT without WHERE clause) we might
    // need to revisit this, since that might lead us to insert a row without a
    // coordinator, depending on how we choose to drive the table sink.
    if (exec_state->coord() != NULL) {
      insert_result->__set_rows_appended(exec_state->coord()->partition_row_counts());
    }
  }

  if (!UnregisterQuery(query_id)) {
    stringstream ss;
    ss << "Failed to unregister query ID '" << ThriftDebugString(query_id) << "'";
    return Status(ss.str());
  }
  return Status::OK;
}

QueryState::type ImpalaServer::get_state(const QueryHandle& handle) {
  if (handle.id == NO_QUERY_HANDLE) {
    return QueryState::FINISHED;
  }

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


void ImpalaServer::dump_config(string& config) {
  config = "";
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

Status ImpalaServer::QueryToTClientRequest(const Query& query,
    TClientRequest* request) {
  request->queryOptions = default_query_options_;
  request->queryOptions.return_as_ascii = true;
  request->stmt = query.query;
  {
    lock_guard<mutex> l_(session_state_map_lock_);
    SessionStateMap::iterator it = 
        session_state_map_.find(*ThriftServer::GetThreadSessionKey());
    DCHECK(it != session_state_map_.end());

    it->second.ToThrift(&request->sessionState);
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

Status ImpalaServer::ParseQueryOptions(const string& options,
    TQueryOptions* query_options) {
  if (options.length() == 0) return Status::OK;
  vector<string> kv_pairs;
  split(kv_pairs, options, is_any_of(","), token_compress_on );
  BOOST_FOREACH(string& kv_string, kv_pairs) {
    trim(kv_string);
    if (kv_string.length() == 0) continue;
    vector<string> key_value;
    split(key_value, kv_string, is_any_of("="), token_compress_on);
    if (key_value.size() != 2) {
      stringstream ss;
      ss << "Ignoring invalid configuration option " << kv_string
         << ": bad format (expected key=value)";
      return Status(ss.str());
    }

    int option = GetQueryOption(key_value[0]);
    if (option < 0) {
      stringstream ss;
      ss << "Ignoring invalid configuration option: " << key_value[0];
      LOG(WARNING) << ss.str();
      return Status(ss.str());
    } else {
      switch (option) {
        case TImpalaQueryOptions::ABORT_ON_ERROR:
          query_options->abort_on_error =
              iequals(key_value[1], "true") || iequals(key_value[1], "1");
          break;
        case TImpalaQueryOptions::MAX_ERRORS:
          query_options->max_errors = atoi(key_value[1].c_str());
          break;
        case TImpalaQueryOptions::DISABLE_CODEGEN:
          query_options->disable_codegen =
              iequals(key_value[1], "true") || iequals(key_value[1], "1");
          break;
        case TImpalaQueryOptions::BATCH_SIZE:
          query_options->batch_size = atoi(key_value[1].c_str());
          break;
        case TImpalaQueryOptions::NUM_NODES:
          query_options->num_nodes = atoi(key_value[1].c_str());
          break;
        case TImpalaQueryOptions::MAX_SCAN_RANGE_LENGTH:
          query_options->max_scan_range_length = atol(key_value[1].c_str());
          break;
        case TImpalaQueryOptions::MAX_IO_BUFFERS:
          query_options->max_io_buffers = atoi(key_value[1].c_str());
          break;
        case TImpalaQueryOptions::NUM_SCANNER_THREADS:
          query_options->num_scanner_threads = atoi(key_value[1].c_str());
          break;
        case TImpalaQueryOptions::PARTITION_AGG:
          query_options->partition_agg =
              iequals(key_value[1], "true") || iequals(key_value[1], "1");
            break;
        case TImpalaQueryOptions::ALLOW_UNSUPPORTED_FORMATS:
          query_options->allow_unsupported_formats =
              iequals(key_value[1], "true") || iequals(key_value[1], "1");
            break;
        default:
          // We hit this DCHECK(false) if we forgot to add the corresponding entry here
          // when we add a new query option.
          LOG(ERROR) << "Missing exec option implementation: " << kv_string;
          DCHECK(false);
          break;
      }
    }
  }
  return Status::OK;
}


void ImpalaServer::TUniqueIdToQueryHandle(const TUniqueId& query_id,
    QueryHandle* handle) {
  stringstream stringstream;
  stringstream << query_id.hi << " " << query_id.lo;
  handle->__set_id(stringstream.str());
  handle->__set_log_context(stringstream.str());
}

void ImpalaServer::QueryHandleToTUniqueId(const QueryHandle& handle,
    TUniqueId* query_id) {
  DCHECK_NE(handle.id, NO_QUERY_HANDLE);
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

inline shared_ptr<ImpalaServer::FragmentExecState> ImpalaServer::GetFragmentExecState(
    const TUniqueId& fragment_instance_id) {
  lock_guard<mutex> l(fragment_exec_state_map_lock_);
  FragmentExecStateMap::iterator i = fragment_exec_state_map_.find(fragment_instance_id);
  if (i == fragment_exec_state_map_.end()) {
    return shared_ptr<FragmentExecState>();
  } else {
    return i->second;
  }
}

inline shared_ptr<ImpalaServer::QueryExecState> ImpalaServer::GetQueryExecState(
    const TUniqueId& query_id, bool lock) {
  lock_guard<mutex> l(query_exec_state_map_lock_);
  QueryExecStateMap::iterator i = query_exec_state_map_.find(query_id);
  if (i == query_exec_state_map_.end()) {
    return shared_ptr<QueryExecState>();
  } else {
    if (lock) i->second->lock()->lock();
    return i->second;
  }
}

void ImpalaServer::ExecPlanFragment(
    TExecPlanFragmentResult& return_val, const TExecPlanFragmentParams& params) {
  VLOG_QUERY << "ExecPlanFragment() instance_id=" << params.params.fragment_instance_id
             << " coord=" << params.coord.ipaddress << ":" << params.coord.port
             << " backend#=" << params.backend_num;
  StartPlanFragmentExecution(params).SetTStatus(&return_val);
}

void ImpalaServer::ReportExecStatus(
    TReportExecStatusResult& return_val, const TReportExecStatusParams& params) {
  VLOG_FILE << "ReportExecStatus() query_id=" << params.query_id
            << " backend#=" << params.backend_num
            << " instance_id=" << params.fragment_instance_id
            << " done=" << (params.done ? "true" : "false");
  // TODO: implement something more efficient here, we're currently 
  // acquiring/releasing the map lock and doing a map lookup for
  // every report (assign each query a local int32_t id and use that to index into a
  // vector of QueryExecStates, w/o lookup or locking?)
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(params.query_id, false);
  if (exec_state.get() == NULL) {
    return_val.status.__set_status_code(TStatusCode::INTERNAL_ERROR);
    stringstream str;
    str << "unknown query id: " << params.query_id;
    return_val.status.error_msgs.push_back(str.str());
    LOG(ERROR) << str.str();
    return;
  }
  exec_state->coord()->UpdateFragmentExecStatus(params).SetTStatus(&return_val);
}

void ImpalaServer::CancelPlanFragment(
    TCancelPlanFragmentResult& return_val, const TCancelPlanFragmentParams& params) {
  VLOG_QUERY << "CancelPlanFragment(): instance_id=" << params.fragment_instance_id;
  shared_ptr<FragmentExecState> exec_state = GetFragmentExecState(params.fragment_instance_id);
  if (exec_state.get() == NULL) {
    stringstream str;
    str << "unknown fragment id: " << params.fragment_instance_id;
    Status status(TStatusCode::INTERNAL_ERROR, str.str());
    status.SetTStatus(&return_val);
    return;
  }
  // we only initiate cancellation here, the map entry as well as the exec state
  // are removed when fragment execution terminates (which is at present still
  // running in exec_state->exec_thread_)
  exec_state->Cancel().SetTStatus(&return_val);
}

void ImpalaServer::TransmitData(
    TTransmitDataResult& return_val, const TTransmitDataParams& params) {
  VLOG_ROW << "TransmitData(): instance_id=" << params.dest_fragment_instance_id
           << " node_id=" << params.dest_node_id
           << " #rows=" << params.row_batch.num_rows
           << " eos=" << (params.eos ? "true" : "false");
  // TODO: fix Thrift so we can simply take ownership of thrift_batch instead
  // of having to copy its data
  if (params.row_batch.num_rows > 0) {
    Status status = exec_env_->stream_mgr()->AddData(
        params.dest_fragment_instance_id, params.dest_node_id, params.row_batch);
    status.SetTStatus(&return_val);
    if (!status.ok()) {
      // should we close the channel here as well?
      return;
    }
  }

  if (params.eos) {
    exec_env_->stream_mgr()->CloseSender(
        params.dest_fragment_instance_id, params.dest_node_id).SetTStatus(&return_val);
  }
}

Status ImpalaServer::StartPlanFragmentExecution(
    const TExecPlanFragmentParams& exec_params) {
  if (!exec_params.fragment.__isset.output_sink) {
    return Status("missing sink in plan fragment");
  }

  const TPlanFragmentExecParams& params = exec_params.params;
  shared_ptr<FragmentExecState> exec_state(
      new FragmentExecState(
        params.query_id, exec_params.backend_num, params.fragment_instance_id,
        exec_env_, make_pair(exec_params.coord.ipaddress, exec_params.coord.port)));
  // Call Prepare() now, before registering the exec state, to avoid calling
  // exec_state->Cancel().
  // We might get an async cancellation, and the executor requires that Cancel() not
  // be called before Prepare() returns.
  RETURN_IF_ERROR(exec_state->Prepare(exec_params));

  {
    lock_guard<mutex> l(fragment_exec_state_map_lock_);
    // register exec_state before starting exec thread
    fragment_exec_state_map_.insert(make_pair(params.fragment_instance_id, exec_state));
  }

  // execute plan fragment in new thread
  // TODO: manage threads via global thread pool
  exec_state->set_exec_thread(
      new thread(&ImpalaServer::RunExecPlanFragment, this, exec_state.get()));
  return Status::OK;
}

void ImpalaServer::RunExecPlanFragment(FragmentExecState* exec_state) {
  exec_state->Exec();

  // we're done with this plan fragment
  {
    lock_guard<mutex> l(fragment_exec_state_map_lock_);
    FragmentExecStateMap::iterator i =
        fragment_exec_state_map_.find(exec_state->fragment_instance_id());
    if (i != fragment_exec_state_map_.end()) {
      // ends up calling the d'tor, if there are no async cancellations
      fragment_exec_state_map_.erase(i);
    } else {
      LOG(ERROR) << "missing entry in fragment exec state map: instance_id="
                 << exec_state->fragment_instance_id();
    }
  }
}

int ImpalaServer::GetQueryOption(const string& key) {
  map<int, const char*>::const_iterator itr =
      _TImpalaQueryOptions_VALUES_TO_NAMES.begin();
  for (; itr != _TImpalaQueryOptions_VALUES_TO_NAMES.end(); ++itr) {
    if (iequals(key, (*itr).second)) {
      return itr->first;
    }
  }
  return -1;
}

void ImpalaServer::InitializeConfigVariables() {
  Status status = ParseQueryOptions(FLAGS_default_query_options, &default_query_options_);
  if (!status.ok()) {
    // Log error and exit if the default query options are invalid.
    LOG(ERROR) << "Invalid default query options. Please check -default_query_options.\n"
               << status.GetErrorMsg();
    exit(1);
  }
  LOG(INFO) << "Default query options:" << ThriftDebugString(default_query_options_);

  map<int, const char*>::const_iterator itr =
      _TImpalaQueryOptions_VALUES_TO_NAMES.begin();
  for (; itr != _TImpalaQueryOptions_VALUES_TO_NAMES.end(); ++itr) {
    ConfigVariable option;
    stringstream value;
    switch (itr->first) {
      case TImpalaQueryOptions::ABORT_ON_ERROR:
        value << default_query_options_.abort_on_error;
        break;
      case TImpalaQueryOptions::MAX_ERRORS:
        value << default_query_options_.max_errors;
        break;
      case TImpalaQueryOptions::DISABLE_CODEGEN:
        value << default_query_options_.disable_codegen;
        break;
      case TImpalaQueryOptions::BATCH_SIZE:
        value << default_query_options_.batch_size;
        break;
      case TImpalaQueryOptions::NUM_NODES:
        value << default_query_options_.num_nodes;
        break;
      case TImpalaQueryOptions::MAX_SCAN_RANGE_LENGTH:
        value << default_query_options_.max_scan_range_length;
        break;
      case TImpalaQueryOptions::MAX_IO_BUFFERS:
        value << default_query_options_.max_io_buffers;
        break;
      case TImpalaQueryOptions::NUM_SCANNER_THREADS:
        value << default_query_options_.num_scanner_threads;
        break;
      case TImpalaQueryOptions::PARTITION_AGG:
        value << default_query_options_.partition_agg;
        break;
      case TImpalaQueryOptions::ALLOW_UNSUPPORTED_FORMATS:
        value << default_query_options_.allow_unsupported_formats;
        break;
      default:
        // We hit this DCHECK(false) if we forgot to add the corresponding entry here
        // when we add a new query option.
        LOG(ERROR) << "Missing exec option implementation: " << itr->second;
        DCHECK(false);
    }
    option.__set_key(itr->second);
    option.__set_value(value.str());
    default_configs_.push_back(option);
  }
  ConfigVariable support_start_over;
  support_start_over.__set_key("support_start_over");
  support_start_over.__set_value("false");
  default_configs_.push_back(support_start_over);
}


void ImpalaServer::SessionStart(const ThriftServer::SessionKey& session_key) {
  lock_guard<mutex> l_(session_state_map_lock_);

  // Currently Kerberos uses only one session id, so there can be dups.
  // TODO: Re-enable this check once IMP-391 is resolved
  // DCHECK(session_state_map_.find(session_key) == session_state_map_.end());

  SessionState& state = session_state_map_[session_key];
  state.start_time = second_clock::local_time();
  state.database = "default";
}

void ImpalaServer::SessionEnd(const ThriftServer::SessionKey& session_key) {
  lock_guard<mutex> l_(session_state_map_lock_);
  session_state_map_.erase(session_key);
}

void ImpalaServer::SessionState::ToThrift(TSessionState* state) {
  state->database = database;
}

void ImpalaServer::MembershipCallback(const ServiceStateMap& service_state) {
  // TODO: Consider rate-limiting this. In the short term, best to have
  // state-store heartbeat less frequently.
  ServiceStateMap::const_iterator it = service_state.find(IMPALA_SERVICE_ID);
  if (it != service_state.end()) {
    vector<THostPort> current_membership(it->second.membership.size());;
    // TODO: Why is Membership not just a set? Would save a copy.
    BOOST_FOREACH(const Membership::value_type& member, it->second.membership) {
      // This is ridiculous: have to clear out hostname so that THostPorts match. 
      current_membership.push_back(member.second);
      current_membership.back().hostname = "";
    }
    
    vector<THostPort> difference;
    sort(current_membership.begin(), current_membership.end());
    set_difference(last_membership_.begin(), last_membership_.end(),
                   current_membership.begin(), current_membership.end(), 
                   std::inserter(difference, difference.begin()));
    vector<TUniqueId> to_cancel;
    {
      lock_guard<mutex> l(query_locations_lock_);
      // Build a list of hosts that have currently executing queries but aren't
      // in the membership list. Cancel them in a separate loop to avoid holding
      // on to the location map lock too long.
      BOOST_FOREACH(const THostPort& hostport, difference) {        
        QueryLocations::iterator it = query_locations_.find(hostport);
        if (it != query_locations_.end()) {          
          to_cancel.insert(to_cancel.begin(), it->second.begin(), it->second.end());
        }
        // We can remove the location wholesale once we know it's failed. 
        query_locations_.erase(hostport);
        exec_env_->client_cache()->CloseConnections(
            make_pair(hostport.ipaddress, hostport.port));
      }
    }

    BOOST_FOREACH(const TUniqueId& query_id, to_cancel) {
      CancelInternal(query_id);
    }
    last_membership_ = current_membership;
  }
}

ImpalaServer* CreateImpalaServer(ExecEnv* exec_env, int fe_port, int be_port,
    ThriftServer** fe_server, ThriftServer** be_server) {
  DCHECK((fe_port == 0) == (fe_server == NULL));
  DCHECK((be_port == 0) == (be_server == NULL));

  shared_ptr<ImpalaServer> handler(new ImpalaServer(exec_env));
  // TODO: do we want a BoostThreadFactory?
  // TODO: we want separate thread factories here, so that fe requests can't starve
  // be requests
  shared_ptr<ThreadFactory> thread_factory(new PosixThreadFactory());

  if (fe_port != 0 && fe_server != NULL) {
    // FE must be a TThreadPoolServer because ODBC and Hue only support TThreadPoolServer.
    shared_ptr<TProcessor> fe_processor(new ImpalaServiceProcessor(handler));
    *fe_server = new ThriftServer("ImpalaServer Frontend", fe_processor, fe_port, 
        FLAGS_fe_service_threads, ThriftServer::ThreadPool);

    (*fe_server)->SetSessionHandler(handler.get());

    LOG(INFO) << "ImpalaService listening on " << fe_port;
  }

  if (be_port != 0 && be_server != NULL) {
    shared_ptr<TProcessor> be_processor(new ImpalaInternalServiceProcessor(handler));
    *be_server = new ThriftServer("ImpalaServer Backend", be_processor, be_port, 
        FLAGS_be_service_threads);

    LOG(INFO) << "ImpalaInternalService listening on " << be_port;
  }

  return handler.get();
}

}
