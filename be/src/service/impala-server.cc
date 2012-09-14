// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "service/impala-server.h"

#include <boost/algorithm/string/join.hpp>
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
#include <glog/logging.h>
// include gflags.h *after* logging.h, otherwise the linker will complain about
// undefined references to FLAGS_v
#include <gflags/gflags.h>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>

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
#include "exec/exec-stats.h"
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

DEFINE_bool(use_planservice, false, "Use external planservice if true");
DECLARE_string(planservice_host);
DECLARE_int32(planservice_port);
DEFINE_int32(fe_port, 21000, "port on which ImpalaService is exported");
DEFINE_int32(fe_service_threads, 10,
    "number of threads servicing ImpalaService requests");
DEFINE_int32(be_port, 22000, "port on which ImpalaInternalService is exported");
DEFINE_int32(be_service_threads, 10,
    "number of threads servicing ImpalaInternalService requests");
DEFINE_bool(load_catalog_at_startup, false, "if true, load all catalog data at startup");
DEFINE_int32(default_num_nodes, 1, "default degree of parallelism for all queries; query "
    "can override it by specifying num_nodes in beeswax.Query.Configuration");
DECLARE_int32(be_port);
DECLARE_int32(fe_port);
DECLARE_int32(be_port);

namespace impala {

ThreadManager* fe_tm;
ThreadManager* be_tm;

// Used for queries that execute instantly and always succeed
const string NO_QUERY_HANDLE = "no_query_handle";

// Execution state of a query. This captures everything necessary
// to convert row batches received by the coordinator into results
// we can return to the client. It also captures all state required for
// servicing query-related requests from the client.
// Thread safety: this class is generally not thread-safe, callers need to
// synchronize access explicitly via lock().
// To avoid deadlocks, the caller must *not* acquire query_exec_state_map_lock_
// while holding the exec state's lock.
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
      current_row_(0),
      impala_server_(server) {
    planner_timer_ = ADD_COUNTER(&profile_, "PlanningTime", TCounterType::CPU_TICKS);
  }

  ~QueryExecState() {
  }

  // Initiates execution of plan fragments, if there are any, and sets
  // up the output exprs for subsequent calls to FetchRowsAsAscii().
  // Also sets up profile and pre-execution counters.
  // Non-blocking.
  Status Exec(TQueryExecRequest* exec_request);

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
  int current_row() const { return current_row_; }
  const TResultSetMetadata* result_metadata() { return &result_metadata_; }
  const TUniqueId& query_id() const { return query_id_; }
  const TQueryExecRequest& query_exec_request() const { return exec_request_; }
  mutex* lock() { return &lock_; }
  const QueryState::type query_state() const { return query_state_; }
  void set_query_state(QueryState::type state) { query_state_ = state; }
  const Status& query_status() const { return query_status_; }
  RuntimeProfile::Counter* planner_timer() { return planner_timer_; }
  void set_result_metadata(const TResultSetMetadata& md) { result_metadata_ = md; }

 private:
  TUniqueId query_id_;
  mutex lock_;  // protects all following fields
  ExecStats exec_stats_;
  ExecEnv* exec_env_;
  scoped_ptr<Coordinator> coord_;  // not set for queries w/o FROM
  // local runtime_state_ in case we don't have a coord_
  RuntimeState local_runtime_state_;
  ObjectPool profile_pool_;
  RuntimeProfile profile_;
  RuntimeProfile::Counter* planner_timer_;
  vector<Expr*> output_exprs_;
  bool eos_;  // if true, there are no more rows to return
  QueryState::type query_state_;
  Status query_status_;
  TQueryExecRequest exec_request_;

  TResultSetMetadata result_metadata_; // metadata for select query
  RowBatch* current_batch_; // the current row batch; only applicable if coord is set
  int current_batch_row_; // num of rows fetched within the current batch
  int current_row_; // num of rows that has been fetched for the entire query

  // To get access to UpdateMetastore
  ImpalaServer* impala_server_;

  // Core logic of FetchRowsAsAscii(). Does not update query_state_/status_.
  Status FetchRowsAsAsciiInternal(const int32_t max_rows, vector<string>* fetched_rows);

  // Fetch the next row batch and store the results in current_batch_
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

Status ImpalaServer::QueryExecState::Exec(TQueryExecRequest* exec_request) {
  exec_request_ = *exec_request;
  profile_.set_name("Query (id=" + PrintId(exec_request->query_id) + ")");

  if (exec_request->has_coordinator_fragment 
      && !exec_request->fragment_requests[0].__isset.desc_tbl) {
    // query without a FROM clause
    // TODO: This does not handle INSERT INTO ... SELECT 1 because there is no
    // coordinator fragment actually executing to drive the sink. 
    // Workaround: INSERT INTO ... SELECT 1 FROM tbl LIMIT 1
    local_runtime_state_.Init(
        exec_request->query_id, exec_request->fragment_requests[0].query_options,
        exec_request->fragment_requests[0].query_globals.now_string,
        NULL /* = we don't expect to be executing anything here */);
    RETURN_IF_ERROR(PrepareSelectListExprs(&local_runtime_state_,
        exec_request->fragment_requests[0].output_exprs, RowDescriptor()));
  } else {
    coord_.reset(new Coordinator(exec_env_, &exec_stats_));
    RETURN_IF_ERROR(coord_->Exec(exec_request));
    if (exec_request->has_coordinator_fragment) {
      DCHECK(exec_request->fragment_requests[0].__isset.desc_tbl);
      RETURN_IF_ERROR(PrepareSelectListExprs(coord_->runtime_state(),
          exec_request->fragment_requests[0].output_exprs, coord_->row_desc()));
    }
    profile_.AddChild(coord_->query_profile());
  }

  query_id_ = exec_request->query_id;
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
  if (coord_ == NULL) {
    query_state_ = QueryState::FINISHED;  // results will be ready after this call
    // query without FROM clause: we return exactly one row
    return CreateConstantRowAsAscii(fetched_rows);
  } else {
    RETURN_IF_ERROR(coord_->Wait());
    query_state_ = QueryState::FINISHED;  // results will be ready after this call
    // Fetch the next batch if we've returned the current batch entirely
    if (current_batch_ == NULL || current_batch_row_ >= current_batch_->num_rows()) {
      RETURN_IF_ERROR(FetchNextBatch());
    }
    return ConvertRowBatchToAscii(max_rows, fetched_rows);
  }
}

void ImpalaServer::QueryExecState::Cancel() {
  // we don't want multiple concurrent cancel calls to end up executing
  // Coordinator::Cancel() multiple times
  if (query_state_ == QueryState::EXCEPTION) return;
  query_state_ = QueryState::EXCEPTION;
  coord_->Cancel();
}

Status ImpalaServer::QueryExecState::PrepareSelectListExprs(
    RuntimeState* runtime_state,
    const vector<TExpr>& exprs, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(runtime_state->obj_pool(), exprs, &output_exprs_));
  for (int i = 0; i < output_exprs_.size(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare(output_exprs_[i], runtime_state, row_desc));
  }
  return Status::OK;
}

Status ImpalaServer::QueryExecState::UpdateMetastore() {
  if (!query_exec_request().__isset.insert_table_name) {
    // insert_table_name is set iff this is an INSERT query
    return Status::OK;
  }

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
    DCHECK(query_exec_request().__isset.insert_table_name);

    // TODO: We track partitions written to, not created, which means
    // that we do more work than is necessary, because written-to
    // partitions don't always require a metastore change.
    VLOG_QUERY << "Updating metastore with " << catalog_update.created_partitions.size()
               << " altered partitions (" 
               << join (catalog_update.created_partitions, ", ") << ")";
    
    catalog_update.target_table = query_exec_request().insert_table_name;
    catalog_update.db_name = query_exec_request().insert_table_db;
    RETURN_IF_ERROR(impala_server_->UpdateMetastore(catalog_update));
  }

  // TODO: Reset only the updated table
  return impala_server_->ResetCatalogInternal(); 
}

Status ImpalaServer::QueryExecState::FetchNextBatch() {
  DCHECK(!eos_);
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
    ++current_row_;
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
  ++current_row_;
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
                    const TUniqueId& fragment_id, ExecEnv* exec_env,
                    const pair<string, int>& coord_hostport)
    : query_id_(query_id),
      backend_num_(backend_num),
      fragment_id_(fragment_id),
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
  Status Prepare(const TPlanExecRequest& request, const TPlanExecParams& params);

  // Main loop of plan fragment execution. Blocks until execution finishes.
  void Exec();

  const TUniqueId& query_id() const { return query_id_; }
  const TUniqueId& fragment_id() const { return fragment_id_; }

  void set_exec_thread(thread* exec_thread) { exec_thread_.reset(exec_thread); }

 private:
  TUniqueId query_id_;
  int backend_num_;
  TUniqueId fragment_id_;
  PlanFragmentExecutor executor_;
  BackendClientCache* client_cache_;
  TPlanExecRequest plan_exec_request_;

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
    const TPlanExecRequest& request, const TPlanExecParams& params) {
  plan_exec_request_ = request;
  RETURN_IF_ERROR(executor_.Prepare(request, params));
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
  params.__set_fragment_id(fragment_id_);
  exec_status.SetTStatus(&params);
  params.__set_done(done);
  profile->ToThrift(&params.profile);
  params.__isset.profile = true;

  if (executor_.runtime_state()->updated_hdfs_partitions().size() > 0) {
    params.partitions_to_create.insert(
        params.partitions_to_create.begin(),
        executor_.runtime_state()->updated_hdfs_partitions().begin(), 
        executor_.runtime_state()->updated_hdfs_partitions().end());

    params.__isset.partitions_to_create = true;
  }

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

ImpalaServer::~ImpalaServer() {}

void ImpalaServer::query(QueryHandle& query_handle, const Query& query) {
  TClientRequest query_request;
  QueryToTClientRequest(query, &query_request);
  VLOG_QUERY << "query(): query=" << query.query;
  shared_ptr<QueryExecState> exec_state;
  Status status = Execute(query_request, &exec_state);
  
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
  TClientRequest query_request;
  QueryToTClientRequest(query, &query_request);
  VLOG_QUERY << "executeAndWait(): query=" << query.query;

  shared_ptr<QueryExecState> exec_state;
  Status status = Execute(query_request, &exec_state);

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
  Status status = ExecuteInternal(request, &registered_exec_state, exec_state);
  if (!status.ok() && registered_exec_state) {
    UnregisterQuery((*exec_state)->query_id());
  }
  return status;
}

Status ImpalaServer::ExecuteInternal(
    const TClientRequest& request, bool* registered_exec_state,
    boost::shared_ptr<QueryExecState>* exec_state) {
  exec_state->reset(new QueryExecState(exec_env_, this));
  *registered_exec_state = false;

  TCreateExecRequestResult result;
  {
    SCOPED_TIMER((*exec_state)->planner_timer());
    RETURN_IF_ERROR(GetQueryExecRequest(request, &result));

    if (result.stmt_type == TStmtType::DDL) {
      // Only DDL command is USE at the moment.
      LOG(INFO) << "USE command ignored";
      exec_state.reset(NULL);
      return Status::OK;
    }
  }
  if (result.__isset.resultSetMetadata) {
    (*exec_state)->set_result_metadata(result.resultSetMetadata);
  }
  TQueryExecRequest& exec_request = result.queryExecRequest;
  // we always need at least one plan fragment
  DCHECK_GT(exec_request.fragment_requests.size(), 0);

  // If desc_tbl is not set, query has SELECT with no FROM. In that
  // case, the query must have a coordinator fragment. This check
  // confirms that. If desc_tbl is set, the query may or may not have
  // a coordinator fragment.
  DCHECK(exec_request.has_coordinator_fragment || 
      exec_request.fragment_requests[0].__isset.desc_tbl);

  if (exec_request.has_coordinator_fragment) {
    if (exec_request.fragment_requests[0].__isset.desc_tbl) {
      DCHECK_GT(exec_request.node_request_params.size(), 0);
    } else {
      // query without a FROM clause, first fragment request has no plan fragment
      DCHECK(!exec_request.fragment_requests[0].__isset.plan_fragment);
    }
  }

  // Set the query options across all fragments
  for (int i = 0; i < exec_request.fragment_requests.size(); ++i) {
    exec_request.fragment_requests[i].query_options = request.queryOptions;
  }

  // register exec state before starting execution in order to handle incoming
  // status reports
  {
    lock_guard<mutex> l(query_exec_state_map_lock_);

    // there shouldn't be an active query with that same id
    // (query_id is globally unique)
    QueryExecStateMap::iterator entry =
        query_exec_state_map_.find(exec_request.query_id);
    if (entry != query_exec_state_map_.end()) {
      stringstream ss;
      ss << "query id " << PrintId(exec_request.query_id)
         << " already exists";
      return Status(TStatusCode::INTERNAL_ERROR, ss.str());
    }

    query_exec_state_map_.insert(make_pair(exec_request.query_id, *exec_state));
    *registered_exec_state = true;
  }

  // start execution of query; also starts fragment status reports
  RETURN_IF_ERROR((*exec_state)->Exec(&exec_request));

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
      query_exec_state_map_.erase(entry);
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

Status ImpalaServer::GetQueryExecRequest(
    const TClientRequest& request, TCreateExecRequestResult* result) {
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

void ImpalaServer::Cancel(impala::TStatus& status,
    const beeswax::QueryHandle& query_handle) {
  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  QueryHandleToTUniqueId(query_handle, &query_id);
  VLOG_QUERY << "Cancel(): query_id=" << PrintId(query_id);
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
  if (exec_state == NULL) {
    RaiseBeeswaxException("Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }
  status.status_code = TStatusCode::OK;
  lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());
  // TODO: can we call Coordinator::Cancel() here while holding lock?
  exec_state->Cancel();
}

void ImpalaServer::explain(QueryExplanation& query_explanation, const Query& query) {
  // Translate Beeswax Query to Impala's QueryRequest and then set the explain plan bool
  // before shipping to FE
  TClientRequest query_request;
  QueryToTClientRequest(query, &query_request);
  VLOG_QUERY << "explain(): query=" << query.query;

  Status status = GetExplainPlan(query_request, &query_explanation.textual);
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
  query_results->__set_start_row(exec_state->current_row());

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
    DCHECK(exec_state->coord() != NULL);
    DCHECK(exec_state->coord()->runtime_state() != NULL);
    RuntimeState* runtime_state = exec_state->coord()->runtime_state();
    set<std::string>& updated_partitions = runtime_state->updated_hdfs_partitions();
    insert_result->modified_hdfs_partitions.insert(
        insert_result->modified_hdfs_partitions.begin(),
        updated_partitions.begin(), updated_partitions.end());
    insert_result->__set_rows_appended(runtime_state->num_appended_rows());
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
  VLOG_QUERY << "get_state(): query_id=" << PrintId(query_id);

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
  log = "Distributed log collection not supported.";
}

void ImpalaServer::get_default_configuration(vector<ConfigVariable> &configurations,
    const bool include_hadoop) {
  configurations.insert(configurations.end(), default_configs_.begin(),
      default_configs_.end());
}

void ImpalaServer::QueryToTClientRequest(const Query& query,
    TClientRequest* request) {
  request->queryOptions.num_nodes = FLAGS_default_num_nodes;
  request->queryOptions.return_as_ascii = true;
  request->stmt = query.query;

  // Convert Query.Configuration to TClientRequest.queryOptions
  if (query.__isset.configuration) {
    BOOST_FOREACH(string kv_string, query.configuration) {
      trim(kv_string);
      vector<string> key_value;
      split(key_value, kv_string, is_any_of(":"), token_compress_on);
      if (key_value.size() != 2) {
        LOG(WARNING) << "ignoring invalid configuration option " << kv_string
                     << ": bad format (expected key:value)";
        continue;
      }

      int option = GetQueryOption(key_value[0]);
      if (option < 0) {
        LOG(WARNING) << "ignoring invalid configuration option: " << key_value[0];
      } else {
        switch (option) {
          case TImpalaQueryOptions::ABORT_ON_ERROR:
            request->queryOptions.abort_on_error =
                iequals(key_value[1], "true") || iequals(key_value[1], "1");
            break;
          case TImpalaQueryOptions::MAX_ERRORS:
            request->queryOptions.max_errors = atoi(key_value[1].c_str());
            break;
          case TImpalaQueryOptions::DISABLE_CODEGEN:
            request->queryOptions.disable_codegen =
                iequals(key_value[1], "true") || iequals(key_value[1], "1");
            break;
          case TImpalaQueryOptions::BATCH_SIZE:
            request->queryOptions.batch_size = atoi(key_value[1].c_str());
            break;
          case TImpalaQueryOptions::NUM_NODES:
            request->queryOptions.num_nodes = atoi(key_value[1].c_str());
            break;
          case TImpalaQueryOptions::MAX_SCAN_RANGE_LENGTH:
            request->queryOptions.max_scan_range_length = atol(key_value[1].c_str());
            break;
          case TImpalaQueryOptions::FILE_BUFFER_SIZE:
            request->queryOptions.file_buffer_size = atoi(key_value[1].c_str());
            break;
          case TImpalaQueryOptions::MAX_IO_BUFFERS:
            request->queryOptions.max_io_buffers = atoi(key_value[1].c_str());
            break;
          case TImpalaQueryOptions::NUM_SCANNER_THREADS:
            request->queryOptions.num_scanner_threads = atoi(key_value[1].c_str());
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
    VLOG_QUERY << "TClientRequest.queryOptions: "
               << ThriftDebugString(request->queryOptions);
  }
}

void ImpalaServer::TUniqueIdToQueryHandle(const TUniqueId& query_id,
    QueryHandle* handle) {
  stringstream stringstream;
  stringstream << query_id.hi << " " << query_id.lo;
  handle->id = stringstream.str();
}

void ImpalaServer::QueryHandleToTUniqueId(const QueryHandle& handle,
    TUniqueId* query_id) {
  DCHECK_NE(handle.id, NO_QUERY_HANDLE);
  char_separator<char> sep(" ");
  tokenizer< char_separator<char> > tokens(handle.id, sep);
  int i = 0;
  BOOST_FOREACH(string t, tokens) {
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
    const TUniqueId& fragment_id) {
  lock_guard<mutex> l(fragment_exec_state_map_lock_);
  FragmentExecStateMap::iterator i = fragment_exec_state_map_.find(fragment_id);
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
  VLOG_QUERY << "ExecPlanFragment() fragment_id=" << params.request.fragment_id
             << " coord=" << params.coord.host << ":" << params.coord.port
             << " backend#=" << params.backend_num;
  StartPlanFragmentExecution(
      params.request, params.params, params.coord, params.backend_num)
      .SetTStatus(&return_val);
}

void ImpalaServer::ReportExecStatus(
    TReportExecStatusResult& return_val, const TReportExecStatusParams& params) {
  VLOG_FILE << "ReportExecStatus() query_id=" << params.query_id
            << " backend#=" << params.backend_num
            << " fragment_id=" << params.fragment_id
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
  VLOG_QUERY << "CancelPlanFragment(): fragment_id=" << params.fragment_id;
  shared_ptr<FragmentExecState> exec_state = GetFragmentExecState(params.fragment_id);
  if (exec_state.get() == NULL) {
    stringstream str;
    str << "unknown fragment id: " << params.fragment_id;
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
  VLOG_ROW << "TransmitData(): fragment_id=" << params.dest_fragment_id
           << " node_id=" << params.dest_node_id
           << " #rows=" << params.row_batch.num_rows
           << " eos=" << (params.eos ? "true" : "false");
  // TODO: fix Thrift so we can simply take ownership of thrift_batch instead
  // of having to copy its data
  if (params.row_batch.num_rows > 0) {
    Status status = exec_env_->stream_mgr()->AddData(
        params.dest_fragment_id, params.dest_node_id, params.row_batch);
    status.SetTStatus(&return_val);
    if (!status.ok()) {
      // should we close the channel here as well?
      return;
    }
  }

  if (params.eos) {
    exec_env_->stream_mgr()->CloseSender(
        params.dest_fragment_id, params.dest_node_id).SetTStatus(&return_val);
  }
}

Status ImpalaServer::StartPlanFragmentExecution(
    const TPlanExecRequest& request, const TPlanExecParams& params,
    const THostPort& coord_hostport, int backend_num) {
  if (!request.data_sink.__isset.dataStreamSink &&
      !request.data_sink.__isset.tableSink) {
    return Status("missing sink in slave plan fragment");
  }

  shared_ptr<FragmentExecState> exec_state(
      new FragmentExecState(
        request.query_id, backend_num, request.fragment_id, exec_env_,
        make_pair(coord_hostport.host, coord_hostport.port)));
  // Call Prepare() now, before registering the exec state, to avoid calling
  // exec_state->Cancel().
  // We might get an async cancellation, and the executor requires that Cancel() not
  // be called before Prepare() returns.
  RETURN_IF_ERROR(exec_state->Prepare(request, params));

  {
    lock_guard<mutex> l(fragment_exec_state_map_lock_);
    // register exec_state before starting exec thread
    fragment_exec_state_map_.insert(make_pair(request.fragment_id, exec_state));
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
        fragment_exec_state_map_.find(exec_state->fragment_id());
    if (i != fragment_exec_state_map_.end()) {
      // ends up calling the d'tor, if there are no async cancellations
      fragment_exec_state_map_.erase(i);
    } else {
      LOG(ERROR) << "missing entry in fragment exec state map: fragment_id="
                 << exec_state->fragment_id();
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
  TQueryOptions default_options;
  map<int, const char*>::const_iterator itr =
      _TImpalaQueryOptions_VALUES_TO_NAMES.begin();
  for (; itr != _TImpalaQueryOptions_VALUES_TO_NAMES.end(); ++itr) {
    ConfigVariable option;
    stringstream value;
    switch (itr->first) {
      case TImpalaQueryOptions::ABORT_ON_ERROR:
        value << default_options.abort_on_error;
        break;
      case TImpalaQueryOptions::MAX_ERRORS:
        value << default_options.max_errors;
        break;
      case TImpalaQueryOptions::DISABLE_CODEGEN:
        value << default_options.disable_codegen;
        break;
      case TImpalaQueryOptions::BATCH_SIZE:
        value << default_options.batch_size;
        break;
      case TImpalaQueryOptions::NUM_NODES:
        value << FLAGS_default_num_nodes;
        break;
      case TImpalaQueryOptions::MAX_SCAN_RANGE_LENGTH:
        value << default_options.max_scan_range_length;
        break;
      case TImpalaQueryOptions::FILE_BUFFER_SIZE:
        value << default_options.file_buffer_size;
        break;
      case TImpalaQueryOptions::MAX_IO_BUFFERS:
        value << default_options.max_io_buffers;
        break;
      case TImpalaQueryOptions::NUM_SCANNER_THREADS:
        value << default_options.num_scanner_threads;
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
